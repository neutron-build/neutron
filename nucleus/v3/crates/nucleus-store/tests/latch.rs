//! The latch's conformance tests (card A-4c0 Work 8; S-STORE §5.3 "Fair
//! latches").
//!
//! - `latch_core_spot_cases`: the policy's exact answers, the fixed queue's
//!   capacity, its overflow and the seq's wrap, called on [`LatchCore`].
//! - `latch_core_exhaustive`: an exhaustive enumeration of the policy's
//!   interleavings, with no model checker, checked by the monitor.
//! - `latch_core_equality`: the core's `Eq` and `Hash`, on which that
//!   enumeration memoises, keep the logical state and nothing else.
//! - `latch_model_exhaustive`, `latch_model_overflow`, `latch_model_pct` and
//!   `latch_model_random`: the blocking latch on the model checker's backend.
//! - `latch_threads` and `latch_api`: the production backend.
//! - `latch_forced_batch`: the blocking latch's batch path (R3's collection,
//!   W2's handed stamps) on the production backend, in a forced order.
//! - `sync_rwcell_std`, `sync_rwcell_model`, `sync_mutex_poison_std` and
//!   `sync_mutex_poison_model`: the sync layer's cell and both backends'
//!   poisoning.
//! - `sync_condvar_poison_std`, `sync_condvar_poison_model`,
//!   `sync_rwcell_poison_std` and `sync_rwcell_poison_model`: F5's other
//!   halves, a mutex poisoned while a thread waits on it and a poisoned
//!   cell, on both backends.
//! - `sync_atomic_std` and `sync_atomic_model`: the sync layer's atomic,
//!   including the value a failed `compare_exchange` answers, on both
//!   backends.
//!
//! Every helper returns `Result`: the workspace lints deny `unwrap`, `expect`
//! and `panic!` outside `#[test]` functions.

mod harness;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use harness::monitor::{LatchMonitor, Outcome, Request, Violation, check_trace};
use harness::suite::{Op, Scenario, new_latch, overflow, run};
use harness::{
    Bounds, EXHAUSTIVE, EXHAUSTIVE_CAP, Harness, PCT, RANDOM, Report, Shuttle, check, run_threads,
};
use nucleus_store::StoreError;
use nucleus_store::latch::{Batch, Holders, Kind, Latch, LatchCore, LatchInfo, Mode, TryLatch};
use nucleus_store::sync::{
    AtomicU64, Condvar, Mutex, Primitives, RwCell, StdPrimitives, yield_now,
};

/// A check that answers with its text instead of panicking.
fn require(condition: bool, what: impl Into<String>) -> Result<(), String> {
    if condition { Ok(()) } else { Err(what.into()) }
}

/// A latch error as text.
fn said<T: std::fmt::Debug>(result: Result<T, StoreError>) -> Result<T, String> {
    result.map_err(|error| format!("{error:?}"))
}

/// A monitor violation as text.
fn said_violation(result: Result<(), Violation>) -> Result<(), String> {
    result.map_err(|violation| violation.to_string())
}

/// The message of an `Err(StoreError::Invalid)`, or what came instead.
fn invalid<T: std::fmt::Debug>(result: Result<T, StoreError>) -> Result<String, String> {
    match result {
        Err(StoreError::Invalid(message)) => Ok(message),
        Err(error) => Err(format!("expected `Invalid`, got {error:?}")),
        Ok(value) => Err(format!("expected `Invalid`, got `Ok({value:?})`")),
    }
}

/// One blocking arrival on a core (card A-4c0 Work 8): `admits`, then the
/// grant or the enqueue that follows. Returns the seq if it was queued.
fn arrive(core: &mut LatchCore, mode: Mode) -> Result<Option<u32>, String> {
    if core.admits(mode, Kind::Blocking) {
        core.grant_arrival(mode);
        return Ok(None);
    }
    said(core.enqueue(mode)).map(Some)
}

/// `latch_core_spot_cases`: the policy's hand-checked answers (a)–(l).
fn spot_cases() -> Result<(), String> {
    // (a) K=0: a batch, then a collection.
    {
        let mut c = said(LatchCore::new(0, 8))?;
        require(
            c.admits(Mode::Exclusive, Kind::Blocking),
            "(a): 1:X is granted",
        )?;
        c.grant_arrival(Mode::Exclusive);
        require(!c.admits(Mode::Shared, Kind::Blocking), "(a): 2:S waits")?;
        let s2 = said(c.enqueue(Mode::Shared))?;
        let s3 = said(c.enqueue(Mode::Shared))?;
        said(c.release(Mode::Exclusive))?;
        require(
            c.holders() == Holders::Free,
            "(a): Exclusive releases to Free",
        )?;
        require(c.may_grant(s3), "(a): may_grant(s3)")?;
        require(
            said(c.grant_queued(s3))? == Batch { first: s2, len: 2 },
            "(a): the batch is { first: s2, len: 2 }",
        )?;
        require(
            c.holders() == Holders::Shared(2),
            "(a): Shared(2) after the batch",
        )?;
        require(c.may_grant(s2), "(a): may_grant(s2), a handed member")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 0 },
            "(a): a collection grants nothing",
        )?;
        require(c.waiting() == 0, "(a): waiting() is 0")?;
    }
    // (b) K=0: §5.3's rule on shared grants, then its rule on tries.
    {
        let mut c = said(LatchCore::new(0, 8))?;
        arrive(&mut c, Mode::Shared)?;
        let s2 = arrive(&mut c, Mode::Exclusive)?.ok_or("(b): 2:X was granted")?;
        let s3 = arrive(&mut c, Mode::Shared)?.ok_or("(b): 3:S was granted")?;
        require(
            !c.admits(Mode::Shared, Kind::Try),
            "(b): try 4:S is refused",
        )?;
        require(
            !c.admits(Mode::Exclusive, Kind::Try),
            "(b): try 5:X is refused",
        )?;
        said(c.release(Mode::Shared))?;
        require(!c.may_grant(s3), "(b): may_grant(s3) is false")?;
        require(c.may_grant(s2), "(b): may_grant(s2)")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 1 },
            "(b): the batch is { first: s2, len: 1 }",
        )?;
        require(c.holders() == Holders::Exclusive, "(b): Exclusive")?;
        require(c.waiting() == 1, "(b): waiting() is 1")?;
        require(
            !c.may_grant(s3),
            "(b): may_grant(s3) is false under the writer",
        )?;
        said(c.release(Mode::Exclusive))?;
        require(c.may_grant(s3), "(b): may_grant(s3) after the release")?;
        require(
            said(c.grant_queued(s3))? == Batch { first: s3, len: 1 },
            "(b): the batch is { first: s3, len: 1 }",
        )?;
        require(
            c.holders() == Holders::Shared(1),
            "(b): Shared(1) at the end",
        )?;
    }
    // (c) K=1: one barge, then the bound holds.
    {
        let mut c = said(LatchCore::new(1, 8))?;
        arrive(&mut c, Mode::Shared)?;
        let s2 = arrive(&mut c, Mode::Exclusive)?.ok_or("(c): 2:X was granted")?;
        said(c.release(Mode::Shared))?;
        require(c.admits(Mode::Exclusive, Kind::Blocking), "(c): 3:X barges")?;
        c.grant_arrival(Mode::Exclusive);
        said(c.release(Mode::Exclusive))?;
        let s4 = arrive(&mut c, Mode::Exclusive)?.ok_or("(c): 4:X was granted")?;
        require(
            !c.admits(Mode::Exclusive, Kind::Try),
            "(c): try 5:X is refused",
        )?;
        require(c.may_grant(s2), "(c): may_grant(s2)")?;
        require(!c.may_grant(s4), "(c): may_grant(s4) is false")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 1 },
            "(c): the batch is { first: s2, len: 1 }",
        )?;
    }
    // (d) K=1: no try succeeds over the queued writer, the release instant
    // included.
    {
        let mut c = said(LatchCore::new(1, 8))?;
        arrive(&mut c, Mode::Shared)?;
        arrive(&mut c, Mode::Exclusive)?;
        require(
            !c.admits(Mode::Shared, Kind::Try),
            "(d): try 3:S is refused",
        )?;
        require(
            !c.admits(Mode::Exclusive, Kind::Try),
            "(d): try 4:X is refused",
        )?;
        said(c.release(Mode::Shared))?;
        require(
            !c.admits(Mode::Shared, Kind::Try),
            "(d): try 5:S is refused at the release",
        )?;
        require(
            !c.admits(Mode::Exclusive, Kind::Try),
            "(d): try 6:X is refused at the release",
        )?;
        require(c.holders() == Holders::Free, "(d): the release leaves Free")?;
        arrive(&mut c, Mode::Shared)?;
        require(c.waiting() == 2, "(d): 7:S is queued behind the writer")?;
    }
    // (e) K=2: two barges, then the bound holds.
    {
        let mut c = said(LatchCore::new(2, 8))?;
        arrive(&mut c, Mode::Exclusive)?;
        let s2 = arrive(&mut c, Mode::Shared)?.ok_or("(e): 2:S was granted")?;
        said(c.release(Mode::Exclusive))?;
        require(c.admits(Mode::Exclusive, Kind::Blocking), "(e): 3:X barges")?;
        c.grant_arrival(Mode::Exclusive);
        said(c.release(Mode::Exclusive))?;
        require(c.admits(Mode::Exclusive, Kind::Blocking), "(e): 4:X barges")?;
        c.grant_arrival(Mode::Exclusive);
        said(c.release(Mode::Exclusive))?;
        arrive(&mut c, Mode::Exclusive)?;
        arrive(&mut c, Mode::Shared)?;
        require(c.waiting() == 3, "(e): three requests wait")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 1 },
            "(e): the batch is { first: s2, len: 1 }",
        )?;
        require(c.holders() == Holders::Shared(1), "(e): Shared(1)")?;
    }
    // (f) K=0: no barge, so the two readers leave in one batch.
    {
        let mut c = said(LatchCore::new(0, 8))?;
        arrive(&mut c, Mode::Exclusive)?;
        let s2 = arrive(&mut c, Mode::Shared)?.ok_or("(f): 2:S was granted")?;
        said(c.release(Mode::Exclusive))?;
        let s3 = arrive(&mut c, Mode::Shared)?.ok_or("(f): 3:S was granted")?;
        require(s3 == s2.wrapping_add(1), "(f): s3 = s2 + 1")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 2 },
            "(f): the batch is { first: s2, len: 2 }",
        )?;
    }
    // (g) K=1: a shared barge counts as an overtake, and a granted arrival
    // takes no seq.
    {
        let mut c = said(LatchCore::new(1, 8))?;
        arrive(&mut c, Mode::Exclusive)?;
        let s2 = arrive(&mut c, Mode::Shared)?.ok_or("(g): 2:S was granted")?;
        said(c.release(Mode::Exclusive))?;
        require(c.admits(Mode::Shared, Kind::Blocking), "(g): 3:S barges")?;
        arrive(&mut c, Mode::Shared)?;
        let s4 = arrive(&mut c, Mode::Shared)?.ok_or("(g): 4:S was granted")?;
        require(c.holders() == Holders::Shared(1), "(g): Shared(1)")?;
        require(s4 == s2.wrapping_add(1), "(g): s4 = s2 + 1")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 2 },
            "(g): the batch is { first: s2, len: 2 }",
        )?;
        require(c.holders() == Holders::Shared(3), "(g): Shared(3)")?;
    }
    // (h) Misuse is `Invalid`.
    {
        let mut c = said(LatchCore::new(1, 8))?;
        require(
            !invalid(c.release(Mode::Exclusive))?.is_empty(),
            "(h): release on Free",
        )?;
        require(
            !invalid(c.release(Mode::Shared))?.is_empty(),
            "(h): a shared release on Free",
        )?;
        arrive(&mut c, Mode::Shared)?;
        require(
            !invalid(c.release(Mode::Exclusive))?.is_empty(),
            "(h): an exclusive release under a reader",
        )?;
        require(
            !invalid(c.grant_queued(0))?.is_empty(),
            "(h): grant_queued of a seq never returned",
        )?;
        let s2 = arrive(&mut c, Mode::Exclusive)?.ok_or("(h): 2:X was granted")?;
        require(
            !invalid(c.grant_queued(s2))?.is_empty(),
            "(h): grant_queued of a head the holders do not admit",
        )?;
    }
    // (i) Capacity and overflow.
    {
        let mut c = said(LatchCore::new(1, 2))?;
        require(c.capacity() == 2, "(i): capacity() is 2")?;
        arrive(&mut c, Mode::Shared)?;
        let s2 = arrive(&mut c, Mode::Exclusive)?.ok_or("(i): 2:X was granted")?;
        let s3 = arrive(&mut c, Mode::Exclusive)?.ok_or("(i): 3:X was granted")?;
        require(
            !c.admits(Mode::Exclusive, Kind::Blocking),
            "(i): 4:X is not admitted",
        )?;
        let before = c.clone();
        let message = invalid(c.enqueue(Mode::Exclusive)).map_err(|e| format!("(i): {e}"))?;
        require(
            message.contains("capacity"),
            "(i): the overflow names the capacity",
        )?;
        require(c == before, "(i): the overflow changes nothing")?;
        require(c.waiting() == 2, "(i): waiting() is still 2")?;
        said(c.release(Mode::Shared))?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 1 },
            "(i): the batch is { first: s2, len: 1 }",
        )?;
        let s5 = arrive(&mut c, Mode::Exclusive)?.ok_or("(i): 5:X was granted")?;
        require(s5 == s3.wrapping_add(1), "(i): s5 = s3 + 1")?;
        require(c.waiting() == 2, "(i): waiting() is 2 again")?;
    }
    // (j) The wrap.
    {
        let mut c = said(LatchCore::new_at(0, 4, u32::MAX - 1))?;
        require(c.head_seq() == u32::MAX - 1, "(j): the first seq")?;
        arrive(&mut c, Mode::Exclusive)?;
        let s2 = said(c.enqueue(Mode::Shared))?;
        let s3 = said(c.enqueue(Mode::Shared))?;
        require(
            s2 == u32::MAX - 1 && s3 == u32::MAX,
            "(j): s2 and s3 at the wrap",
        )?;
        said(c.release(Mode::Exclusive))?;
        require(c.may_grant(s3), "(j): may_grant(s3)")?;
        require(
            said(c.grant_queued(s3))? == Batch { first: s2, len: 2 },
            "(j): the batch is { first: s2, len: 2 }",
        )?;
        require(c.head_seq() == 0, "(j): head_seq() wraps to 0")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 0 },
            "(j): the collection",
        )?;
        let s4 = arrive(&mut c, Mode::Exclusive)?.ok_or("(j): 4:X was granted")?;
        require(s4 == 0, "(j): s4 is 0")?;
        said(c.release(Mode::Shared))?;
        said(c.release(Mode::Shared))?;
        require(c.may_grant(0), "(j): may_grant(0)")?;
        require(
            said(c.grant_queued(0))? == Batch { first: 0, len: 1 },
            "(j): the batch is { first: 0, len: 1 }",
        )?;
        require(c.holders() == Holders::Exclusive, "(j): Exclusive")?;
    }
    // (k) Every thread queued at once: the capacity rule's tightness.
    {
        let mut c = said(LatchCore::new(0, 3))?;
        arrive(&mut c, Mode::Exclusive)?;
        let s2 = arrive(&mut c, Mode::Exclusive)?.ok_or("(k): 2:X was granted")?;
        arrive(&mut c, Mode::Exclusive)?;
        said(c.release(Mode::Exclusive))?;
        require(c.holders() == Holders::Free, "(k): Free after the release")?;
        require(
            !c.admits(Mode::Exclusive, Kind::Blocking),
            "(k): 1:X again is not admitted at K = 0",
        )?;
        arrive(&mut c, Mode::Exclusive)?;
        require(
            c.waiting() == 3 && c.waiting() == c.capacity(),
            "(k): waiting() is the capacity",
        )?;
        let before = c.clone();
        require(
            invalid(c.enqueue(Mode::Exclusive))?.contains("capacity"),
            "(k): a fourth enqueue is `Invalid` and names the capacity",
        )?;
        require(c == before, "(k): the fourth enqueue changes nothing")?;
        require(
            said(c.grant_queued(s2))? == Batch { first: s2, len: 1 },
            "(k): the batch is { first: s2, len: 1 }",
        )?;
    }
    // (l) The constructor's domain.
    {
        for (bound, capacity) in [(128u32, 1u32), (0, 0), (0, 65_536)] {
            // `invalid` fails the case unless the answer is `Invalid`.
            invalid(LatchCore::new(bound, capacity))
                .map_err(|e| format!("(l): new({bound}, {capacity}): {e}"))?;
        }
        let c = said(LatchCore::new(127, 65_535))?;
        require(
            c.bound() == 127 && c.capacity() == 65_535 && c.waiting() == 0,
            "(l): the domain's ends",
        )?;
    }
    Ok(())
}

#[test]
fn latch_core_spot_cases() {
    spot_cases().unwrap();
}

/// An actor's place in its script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Place {
    /// Ready for the script's next op, or done.
    Idle,
    /// Holds the latch in `mode` for the request `id`.
    Holding { mode: Mode, id: usize },
    /// Waits for the request `id`, named by the core's `seq`.
    Queued { mode: Mode, id: usize, seq: u32 },
    /// Holds the latch for the request `id` from a batch's grant, and has yet
    /// to collect that grant at its own re-check.
    Handed { mode: Mode, id: usize, seq: u32 },
}

#[derive(Debug, Clone)]
struct Actor {
    /// The next op's index in the script.
    pc: usize,
    place: Place,
}

/// One state of the enumeration: the core, the actors and the monitor.
#[derive(Debug, Clone)]
struct State {
    core: LatchCore,
    actors: Vec<Actor>,
    monitor: LatchMonitor,
}

/// The memo key (card A-4c0 Work 8): the core, the actors and the monitor,
/// with each actor's seq taken relative to the core's `head_seq`, since the
/// core's equality ignores `head_seq` itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    core: LatchCore,
    actors: Vec<(usize, Place)>,
    monitor: LatchMonitor,
}

impl State {
    fn key(&self) -> Key {
        let head = self.core.head_seq();
        Key {
            core: self.core.clone(),
            actors: self
                .actors
                .iter()
                .map(|a| {
                    let place = match a.place {
                        Place::Queued { mode, id, seq } => Place::Queued {
                            mode,
                            id,
                            seq: seq.wrapping_sub(head),
                        },
                        Place::Handed { mode, id, seq } => Place::Handed {
                            mode,
                            id,
                            seq: seq.wrapping_sub(head),
                        },
                        other => other,
                    };
                    (a.pc, place)
                })
                .collect(),
            monitor: self.monitor.clone(),
        }
    }

    /// The states one step reaches (card A-4c0 Work 8): an idle actor's next
    /// arrival, a holder's release, a woken waiter's re-check, or a handed
    /// member's collection. A waiter the core may grant is one the latch has
    /// woken (W4), since only a release, a batch or its own enqueue makes it
    /// grantable; a woken waiter whose re-check fails changes nothing, so the
    /// enumeration leaves its wake out. A batch wakes its other members,
    /// which collect when they re-check, and makes no further wake.
    fn successors(&self, scripts: &[&[Op]]) -> Result<Vec<State>, String> {
        let mut next = Vec::new();
        for (index, actor) in self.actors.iter().enumerate() {
            let ops = scripts.get(index).copied().unwrap_or(&[]);
            match actor.place {
                Place::Idle if actor.pc < ops.len() => {
                    next.push(self.arrive(index, ops[actor.pc])?);
                }
                Place::Holding { mode, id } => next.push(self.release(index, mode, id)?),
                Place::Queued { seq, .. } => {
                    if self.core.may_grant(seq) {
                        next.push(self.collect(index)?);
                    }
                }
                Place::Handed { .. } => next.push(self.take_hand(index)?),
                Place::Idle => {}
            }
        }
        Ok(next)
    }

    fn arrive(&self, index: usize, op: Op) -> Result<State, String> {
        let mut s = self.clone();
        let id = index * OPS_PER_ACTOR + s.actors[index].pc;
        let (mode, kind) = match op {
            Op::Hold(mode) => (mode, Kind::Blocking),
            Op::TryHold(mode) => (mode, Kind::Try),
        };
        said_violation(s.monitor.arrive(id, mode, kind))?;
        if s.core.admits(mode, kind) {
            s.core.grant_arrival(mode);
            said_violation(s.monitor.grant(id))?;
            s.actors[index].place = Place::Holding { mode, id };
            return Ok(s);
        }
        if kind == Kind::Try {
            said_violation(s.monitor.refuse(id))?;
            s.actors[index].pc += 1;
            s.actors[index].place = Place::Idle;
            return Ok(s);
        }
        let seq = said(s.core.enqueue(mode))?;
        s.actors[index].place = Place::Queued { mode, id, seq };
        Ok(s)
    }

    fn release(&self, index: usize, mode: Mode, id: usize) -> Result<State, String> {
        let mut s = self.clone();
        said(s.core.release(mode))?;
        said_violation(s.monitor.release(id))?;
        s.actors[index].pc += 1;
        s.actors[index].place = Place::Idle;
        Ok(s)
    }

    fn collect(&self, index: usize) -> Result<State, String> {
        let mut s = self.clone();
        let (_, _, seq) = match s.actors[index].place {
            Place::Queued { seq, .. } => (0, 0, seq),
            _ => return Err("collect: the actor is not queued".to_string()),
        };
        let batch = said(s.core.grant_queued(seq))?;
        // A batch grants every member at once (R3), each with its stamp
        // fixed there in queue order (W2), so the monitor sees the grants in
        // that order, whichever member's re-check granted the batch. The
        // caller holds from now; each other member's grant is handed to it,
        // and its own re-check collects it.
        let mut members: Vec<(u32, usize)> = Vec::new();
        for (other, actor) in s.actors.iter().enumerate() {
            if let Place::Queued { seq, .. } = actor.place {
                let offset = seq.wrapping_sub(batch.first);
                if offset < batch.len {
                    members.push((offset, other));
                }
            }
        }
        members.sort_unstable();
        for (_, other) in members {
            let (mode, id, seq) = match s.actors[other].place {
                Place::Queued { mode, id, seq } => (mode, id, seq),
                _ => return Err("collect: a batch member is not queued".to_string()),
            };
            said_violation(s.monitor.grant(id))?;
            s.actors[other].place = if other == index {
                Place::Holding { mode, id }
            } else {
                Place::Handed { mode, id, seq }
            };
        }
        Ok(s)
    }

    /// A handed member's re-check: it collects its grant, which grants
    /// nothing new.
    fn take_hand(&self, index: usize) -> Result<State, String> {
        let mut s = self.clone();
        let (mode, id, seq) = match s.actors[index].place {
            Place::Handed { mode, id, seq } => (mode, id, seq),
            _ => return Err("take_hand: the actor holds no handed grant".to_string()),
        };
        let collected = said(s.core.grant_queued(seq))?;
        if collected != (Batch { first: seq, len: 0 }) {
            return Err(format!(
                "a handed member's re-check granted {collected:?}, where nothing new is expected"
            ));
        }
        s.actors[index].place = Place::Holding { mode, id };
        Ok(s)
    }
}

/// Each actor's request ids start at a multiple of this, so no two requests
/// of a configuration share an id.
const OPS_PER_ACTOR: usize = 4;

/// What one configuration's enumeration found.
struct Found {
    states: usize,
    most_waiting: u32,
}

/// Enumerates every interleaving of the configuration's scripts on
/// `LatchCore::new(k, actors)`, feeding the monitor each event, and fails on
/// a monitor violation, an `Invalid` from the core, or a stuck state.
fn explore(k: u32, scripts: &[&[Op]]) -> Result<Found, String> {
    let core = said(LatchCore::new(k, scripts.len() as u32))?;
    let first = State {
        core,
        actors: vec![
            Actor {
                pc: 0,
                place: Place::Idle,
            };
            scripts.len()
        ],
        monitor: LatchMonitor::new(k),
    };
    let mut seen = HashSet::new();
    let mut stack = vec![first];
    let mut found = Found {
        states: 0,
        most_waiting: 0,
    };
    while let Some(state) = stack.pop() {
        if !seen.insert(state.key()) {
            continue;
        }
        found.states += 1;
        found.most_waiting = found.most_waiting.max(state.core.waiting());
        let next = state.successors(scripts)?;
        if next.is_empty() {
            // A state with no step enabled: every script must be done, and
            // the monitor must agree that nobody waits or holds.
            let done = state
                .actors
                .iter()
                .zip(scripts)
                .all(|(actor, ops)| actor.pc == ops.len());
            if !done {
                return Err(format!("a stuck state at K = {k}: {:?}", state.actors));
            }
            said_violation(state.monitor.finish())?;
        }
        stack.extend(next);
    }
    Ok(found)
}

/// `latch_core_exhaustive`: 1,017 configurations, each enumerated
/// exhaustively and deterministically, with no model checker.
fn core_exhaustive() -> Result<(), String> {
    const HOLD_S: Op = Op::Hold(Mode::Shared);
    const HOLD_X: Op = Op::Hold(Mode::Exclusive);
    const OPS: [Op; 4] = [
        HOLD_S,
        HOLD_X,
        Op::TryHold(Mode::Shared),
        Op::TryHold(Mode::Exclusive),
    ];
    const THREE_X: &[Op] = &[HOLD_X, HOLD_X, HOLD_X];
    const S_X_S: &[Op] = &[HOLD_S, HOLD_X, HOLD_S];
    const TWO_S: &[Op] = &[HOLD_S, HOLD_S];
    const TWO_X: &[Op] = &[HOLD_X, HOLD_X];
    const TRY_X_S_TRY_S: &[Op] = &[
        Op::TryHold(Mode::Exclusive),
        HOLD_S,
        Op::TryHold(Mode::Shared),
    ];
    const X_S: &[Op] = &[HOLD_X, HOLD_S];
    const ONE_S: &[Op] = &[HOLD_S];
    const ONE_X: &[Op] = &[HOLD_X];
    const ONE_TRY_S: &[Op] = &[Op::TryHold(Mode::Shared)];
    const ONE_TRY_X: &[Op] = &[Op::TryHold(Mode::Exclusive)];

    let mut tally = Tally::default();
    for k in [0, 1, 2] {
        for actors in [2, 3, 4] {
            // Every assignment of one op from the four to each actor.
            let count = OPS.len().pow(actors as u32);
            for pattern in 0..count {
                let mut digits = pattern;
                let mut scripts = Vec::with_capacity(actors);
                for _ in 0..actors {
                    scripts.push(match digits % OPS.len() {
                        0 => ONE_S,
                        1 => ONE_X,
                        2 => ONE_TRY_S,
                        _ => ONE_TRY_X,
                    });
                    digits /= OPS.len();
                }
                let found = explore(k, &scripts).map_err(|e| {
                    format!("{e} [K = {k}, one op each for {actors} actors, pattern {pattern}]")
                })?;
                tally.note(found, actors);
            }
        }
        // The three multi-op configurations.
        const CONFIGS: [&[&[Op]]; 3] = [
            &[ONE_X, ONE_X, THREE_X],
            &[ONE_S, ONE_X, S_X_S],
            &[TWO_S, TWO_X, TRY_X_S_TRY_S, X_S],
        ];
        for scripts in CONFIGS {
            let found = explore(k, scripts)?;
            tally.note(found, scripts.len());
        }
    }
    let Tally {
        configurations,
        states,
        most_waiting,
    } = tally;
    println!(
        "latch_core_exhaustive: {configurations} configurations, {states} states; the most any state waited, by actors: 2 -> {}, 3 -> {}, 4 -> {}",
        most_waiting[2], most_waiting[3], most_waiting[4]
    );
    require(
        configurations == 1_017,
        format!("{configurations} configurations, where 1,017 are expected"),
    )?;
    Ok(())
}

/// What the whole enumeration found.
#[derive(Default)]
struct Tally {
    configurations: usize,
    states: usize,
    /// The most any state waited, by the configuration's actor count.
    most_waiting: [u32; 5],
}

impl Tally {
    fn note(&mut self, found: Found, actors: usize) {
        self.configurations += 1;
        self.states += found.states;
        self.most_waiting[actors] = self.most_waiting[actors].max(found.most_waiting);
    }
}

#[test]
fn latch_core_exhaustive() {
    core_exhaustive().unwrap();
}

/// Runs one scenario of the suite on `Latch<Shuttle>` under `bounds`, and
/// names the scenario and its bound in any failure.
fn model(bounds: Bounds, scenario: Scenario, k: u32) -> Result<Report, String> {
    check(bounds, move || {
        run::<Shuttle, Latch<Shuttle>>(scenario, new_latch, k)
    })
    .map_err(|failure| format!("{} at K = {k}: {failure}", scenario.name()))
}

/// `latch_model_exhaustive`: S3, S5, S6, S7 and S9 on `Latch<Shuttle>` for K
/// in {0, 1}, under `EXHAUSTIVE`: each passes, search complete.
fn model_exhaustive() -> Result<(), String> {
    let mut line = String::from("latch_model_exhaustive executions:");
    for k in [0, 1] {
        for scenario in [
            Scenario::S3,
            Scenario::S5,
            Scenario::S6,
            Scenario::S7,
            Scenario::S9,
        ] {
            let report = model(EXHAUSTIVE, scenario, k)?;
            require(
                report.executions < EXHAUSTIVE_CAP,
                format!("{} at K = {k}: the search reached the cap", scenario.name()),
            )?;
            line.push_str(&format!(" {}@K={k}={}", scenario.name(), report.executions));
        }
    }
    println!("{line} (cap {EXHAUSTIVE_CAP})");
    Ok(())
}

#[test]
fn latch_model_exhaustive() {
    model_exhaustive().unwrap();
}

/// `latch_model_overflow`: a latch of capacity 1 overflows in every schedule,
/// reported and never absorbed.
fn model_overflow() -> Result<(), String> {
    let mut line = String::from("latch_model_overflow executions:");
    for k in [0, 1] {
        let report = check(EXHAUSTIVE, move || overflow::<Shuttle>(k))
            .map_err(|failure| format!("overflow at K = {k}: {failure}"))?;
        require(
            report.executions < EXHAUSTIVE_CAP,
            format!("overflow at K = {k}: the search reached the cap"),
        )?;
        line.push_str(&format!(" K={k}={}", report.executions));
    }
    println!("{line} (cap {EXHAUSTIVE_CAP})");
    Ok(())
}

#[test]
fn latch_model_overflow() {
    model_overflow().unwrap();
}

/// S1–S9 for K in {0, 1, 2} under `PCT` (card A-4c0 Work 8).
fn model_pct() -> Result<(), String> {
    for scenario in Scenario::ALL {
        for k in [0, 1, 2] {
            model(PCT, scenario, k)?;
        }
    }
    Ok(())
}

#[test]
fn latch_model_pct() {
    model_pct().unwrap();
}

/// S1–S9 for K in {0, 1, 2} under `RANDOM` (card A-4c0 Work 8).
fn model_random() -> Result<(), String> {
    for scenario in Scenario::ALL {
        for k in [0, 1, 2] {
            model(RANDOM, scenario, k)?;
        }
    }
    Ok(())
}

#[test]
fn latch_model_random() {
    model_random().unwrap();
}

/// S1–S9 for K in {0, 1, 2}, and the overflow scenario for K in {0, 1}, on
/// `Latch<StdPrimitives>` through `run_threads(20, …)`.
fn threads() -> Result<(), String> {
    for scenario in Scenario::ALL {
        for k in [0, 1, 2] {
            run_threads(20, move || {
                run::<StdPrimitives, Latch<StdPrimitives>>(scenario, new_latch, k)
            })
            .map_err(|failure| format!("{} at K = {k}: {failure}", scenario.name()))?;
        }
    }
    for k in [0, 1] {
        run_threads(20, move || overflow::<StdPrimitives>(k))
            .map_err(|failure| format!("overflow at K = {k}: {failure}"))?;
    }
    Ok(())
}

#[test]
fn latch_threads() {
    threads().unwrap();
}

/// The stamps `latch_api` has seen, which must strictly increase.
#[derive(Default)]
struct Seen(Vec<u64>);

impl Seen {
    fn push(&mut self, stamps: impl IntoIterator<Item = u64>) -> Result<(), String> {
        for stamp in stamps {
            if self.0.last().is_some_and(|last| stamp <= *last) {
                let last = self.0.last().copied();
                return Err(format!(
                    "the stamp {stamp} does not exceed {last:?} in {:?}",
                    self.0
                ));
            }
            self.0.push(stamp);
        }
        Ok(())
    }
}

/// `latch_api`: direct single-thread calls on `Latch<StdPrimitives>`.
fn api() -> Result<(), String> {
    let mut seen = Seen::default();
    let latch: Latch<StdPrimitives> = said(Latch::new(1, 4))?;
    require(
        said(latch.info())?
            == LatchInfo {
                holders: Holders::Free,
                waiting: 0,
                arrivals: 0,
                bound: 1,
                capacity: 4,
            },
        "info() before any call is { Free, 0, 0, 1, 4 }",
    )?;

    // Stamps strictly increase across arrive, grant and release.
    let guard = said(latch.latch(Mode::Exclusive))?;
    require(guard.mode() == Mode::Exclusive, "mode()")?;
    let (arrived, granted) = guard.stamps();
    seen.push([arrived, granted])?;
    // While an Exclusive guard is held, a try in either mode is refused with
    // an arrival stamp greater than the guard's grant stamp.
    for mode in [Mode::Shared, Mode::Exclusive] {
        match said(latch.try_latch(mode))? {
            TryLatch::Refused(stamp) => {
                require(
                    stamp > granted,
                    "a refusal's stamp is greater than the grant's",
                )?;
                seen.push([stamp])?;
            }
            TryLatch::Granted(_) => {
                return Err("a try was granted over an exclusive holder".to_string());
            }
        }
    }
    seen.push([said(guard.release())?])?;
    require(
        said(latch.info())?.arrivals == 3,
        "arrivals counts each try, granted or refused",
    )?;

    // Dropping a guard releases it, so a following try_latch(Exclusive) is
    // granted.
    let guard = said(latch.latch(Mode::Shared))?;
    seen.push([guard.stamps().0, guard.stamps().1])?;
    match said(latch.try_latch(Mode::Exclusive))? {
        TryLatch::Refused(stamp) => seen.push([stamp])?,
        TryLatch::Granted(_) => {
            return Err("an exclusive try was granted over a reader".to_string());
        }
    }
    drop(guard);
    match said(latch.try_latch(Mode::Exclusive))? {
        TryLatch::Granted(guard) => {
            require(guard.mode() == Mode::Exclusive, "mode() of the try's guard")?;
            seen.push([guard.stamps().0, guard.stamps().1])?;
            drop(guard);
        }
        TryLatch::Refused(_) => {
            return Err("try_latch(Exclusive) was refused on a free latch".to_string());
        }
    }
    require(
        said(latch.info())?.holders == Holders::Free,
        "a dropped guard leaves Free",
    )?;
    require(
        said(latch.bound())? == 1 && said(latch.capacity())? == 4,
        "bound() and capacity()",
    )?;
    for (bound, capacity) in [(128u32, 1u32), (0, 0)] {
        // `invalid` fails the case unless the answer is `Invalid`.
        invalid(Latch::<StdPrimitives>::new(bound, capacity))
            .map_err(|e| format!("Latch::new({bound}, {capacity}): {e}"))?;
    }
    Ok(())
}

#[test]
fn latch_api() {
    api().unwrap();
}

/// `sync_rwcell_std` on `RwCell<StdPrimitives, u32>`.
fn rwcell_std() -> Result<(), String> {
    let cell: RwCell<StdPrimitives, u32> = RwCell::new(0);
    // Two reads held together are both granted.
    let first = said(cell.try_read())?.ok_or("the first try_read was busy")?;
    let second = said(cell.try_read())?.ok_or("the second try_read was busy")?;
    require(*first == 0 && *second == 0, "both read 0")?;
    // While a read is held, try_write is Ok(None).
    require(
        said(cell.try_write())?.is_none(),
        "try_write under a read is Ok(None)",
    )?;
    drop((first, second));
    // After both drop, try_write is granted; under it both tries are busy.
    let mut write = said(cell.try_write())?.ok_or("try_write was busy with no guard held")?;
    *write = 7;
    require(
        said(cell.try_read())?.is_none(),
        "try_read under a write is Ok(None)",
    )?;
    require(
        said(cell.try_write())?.is_none(),
        "try_write under a write is Ok(None)",
    )?;
    drop(write);
    // A value written through it is read afterwards.
    let read = said(cell.try_read())?.ok_or("try_read was busy with no guard held")?;
    require(*read == 7, "the value written is read")?;
    Ok(())
}

#[test]
fn sync_rwcell_std() {
    rwcell_std().unwrap();
}

/// `sync_rwcell_model`: two model threads each try the cell's write side
/// once, under `EXHAUSTIVE`; every execution ends with 1 or 2, and both
/// occur across the search.
fn rwcell_model() -> Result<(), String> {
    let ones = Arc::new(AtomicUsize::new(0));
    let twos = Arc::new(AtomicUsize::new(0));
    let (ones_in, twos_in) = (Arc::clone(&ones), Arc::clone(&twos));
    let report = check(EXHAUSTIVE, move || {
        let cell: Arc<RwCell<Shuttle, u32>> = Arc::new(RwCell::new(0));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let cell = Arc::clone(&cell);
            handles.push(Shuttle::spawn(move || {
                // A try reports a poison or a busy cell; neither is an error
                // of this scenario, which tries once.
                if let Ok(Some(mut write)) = cell.try_write() {
                    *write += 1;
                    yield_now::<Shuttle>();
                }
            }));
        }
        for handle in handles {
            Shuttle::join(handle)?;
        }
        let read = match cell.try_write() {
            Ok(Some(read)) => read,
            Ok(None) => return Err("the cell was busy with both threads joined".to_string()),
            Err(error) => return Err(error.to_string()),
        };
        let value = *read;
        match value {
            1 => {
                ones_in.fetch_add(1, Ordering::SeqCst);
            }
            2 => {
                twos_in.fetch_add(1, Ordering::SeqCst);
            }
            _ => return Err(format!("the cell holds {value}, where 1 or 2 is expected")),
        }
        Ok(())
    })
    .map_err(|failure| format!("sync_rwcell_model: {failure}"))?;
    println!(
        "sync_rwcell_model executions: {} (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    require(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap",
    )?;
    let (ones, twos) = (ones.load(Ordering::SeqCst), twos.load(Ordering::SeqCst));
    require(
        ones > 0 && twos > 0,
        format!("the values 1 and 2 occurred {ones} and {twos} times: both must occur"),
    )?;
    Ok(())
}

#[test]
fn sync_rwcell_model() {
    rwcell_model().unwrap();
}

/// `sync_mutex_poison_std` (F5; S-STORE §9.2's "in every backend"): a spawned
/// OS thread panics while holding `Mutex<StdPrimitives, u32>`; after its
/// join returns the panic, `lock()` is `Poisoned`, twice.
#[test]
fn sync_mutex_poison_std() {
    let mutex = Arc::new(Mutex::<StdPrimitives, u32>::new(0));
    let theirs = Arc::clone(&mutex);
    let handle = std::thread::spawn(move || {
        let _guard = theirs.lock().unwrap();
        panic!("a panic under the platform's mutex");
    });
    assert!(
        handle.join().is_err(),
        "the thread's panic is returned by its join"
    );
    assert!(
        matches!(mutex.lock(), Err(StoreError::Poisoned(_))),
        "lock() after the panic"
    );
    assert!(
        matches!(mutex.lock(), Err(StoreError::Poisoned(_))),
        "a second lock()"
    );
}

/// `sync_mutex_poison_model`: a model thread unwinds while holding
/// `Mutex<Shuttle, u32>`; the panic is caught inside the thread, so the
/// execution does not fail; main then finds the mutex poisoned in every
/// execution.
fn poison_model() -> Result<Report, String> {
    check(EXHAUSTIVE, move || {
        let mutex: Arc<Mutex<Shuttle, u32>> = Arc::new(Mutex::new(0));
        let theirs = Arc::clone(&mutex);
        let handle = Shuttle::spawn(move || {
            // The guard is dropped as the closure unwinds, which poisons the
            // mutex; the panic is caught here, inside the thread.
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = match theirs.lock() {
                    Ok(guard) => guard,
                    Err(error) => return error.to_string(),
                };
                std::panic::resume_unwind(Box::new("a panic under the model's mutex"));
            }));
            match caught {
                Ok(text) => Err(text),
                Err(_) => Ok(()),
            }
        });
        Shuttle::join(handle)??;
        match mutex.lock() {
            Err(StoreError::Poisoned(_)) => Ok(()),
            Ok(_) => Err("the mutex was not poisoned by a panic under it".to_string()),
            Err(error) => Err(format!("a poisoned mutex answered {error:?}")),
        }
    })
    .map_err(|failure| format!("sync_mutex_poison_model: {failure}"))
}

#[test]
fn sync_mutex_poison_model() {
    let report = poison_model().unwrap();
    println!(
        "sync_mutex_poison_model executions: {} (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// `sync_atomic_std` and `sync_atomic_model` (F1, F3): every operation of the
/// sync layer's atomic answers the same on both backends. A failed
/// `compare_exchange` answers `Err` with the value it compared against and
/// stores nothing; a successful one answers `Ok` with the old value.
fn atomic_ops<P: Primitives>() -> Result<(), String> {
    let a: AtomicU64<P> = AtomicU64::new(5);
    require(a.load() == 5, "load after new(5)")?;
    a.store(7);
    require(a.load() == 7, "load after store(7)")?;
    require(a.fetch_add(3) == 7, "fetch_add answers the old value")?;
    require(a.load() == 10, "fetch_add(3) adds 3")?;
    require(a.fetch_sub(4) == 10, "fetch_sub answers the old value")?;
    require(a.load() == 6, "fetch_sub(4) subtracts 4")?;
    require(
        a.compare_exchange(6, 9) == Ok(6),
        "a compare_exchange that matches answers Ok with the old value",
    )?;
    require(a.load() == 9, "a compare_exchange that matches stores")?;
    let failed = a.compare_exchange(6, 1);
    require(
        failed == Err(9),
        format!(
            "a compare_exchange that does not match answers {failed:?}, where Err(9) is expected"
        ),
    )?;
    require(
        a.load() == 9,
        "a compare_exchange that does not match stores nothing",
    )?;
    Ok(())
}

#[test]
fn sync_atomic_std() {
    atomic_ops::<StdPrimitives>().unwrap();
}

#[test]
fn sync_atomic_model() {
    let report = check(EXHAUSTIVE, atomic_ops::<Shuttle>).unwrap();
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// What a poison scenario's thread answers: `Ok` once its panic was caught,
/// the text of anything that went wrong before it.
fn caught_panic(caught: std::thread::Result<Result<(), String>>) -> Result<(), String> {
    match caught {
        // The closure ends by unwinding, so it never returns `Ok`.
        Ok(Ok(())) => Err("the closure returned where it unwinds".to_string()),
        Ok(Err(text)) => Err(text),
        Err(_) => Ok(()),
    }
}

/// The state `condvar_poison` shares: main is ready to wait, and the other
/// thread has notified it.
type PoisonSignals<H> = (Mutex<H, (bool, bool)>, Condvar<H>, Condvar<H>);

/// `sync_condvar_poison_std` and `sync_condvar_poison_model` (F5: a backend's
/// `condvar_wait` maps a poisoned lock to `Poisoned` and never takes the
/// guard out of the poisoning; S-STORE §9.2's "in every backend"). Main
/// waits on a condition variable; the other thread takes the mutex once main
/// waits, notifies, and unwinds while holding it, which poisons it. Main's
/// wait must then answer `StoreError::Poisoned`, never the guard, and so must
/// a later `lock()`. The panic is caught inside the thread, so it does not
/// fail a model execution.
fn condvar_poison<H: Harness>() -> Result<(), String> {
    let shared: Arc<PoisonSignals<H>> =
        Arc::new((Mutex::new((false, false)), Condvar::new(), Condvar::new()));
    let theirs = Arc::clone(&shared);
    let handle = H::spawn(move || {
        let (mutex, ready, wake) = &*theirs;
        caught_panic(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> Result<(), String> {
                let mut guard = mutex.lock().map_err(|e| e.to_string())?;
                while !guard.0 {
                    guard = ready.wait(guard).map_err(|e| e.to_string())?;
                }
                guard.1 = true;
                wake.notify_all();
                // The guard drops as this unwinds, which poisons the mutex
                // with main waiting on it.
                std::panic::resume_unwind(Box::new("a panic under the mutex, a waiter woken"));
            },
        )))
    });
    let (mutex, ready, wake) = &*shared;
    let mut guard = mutex.lock().map_err(|e| e.to_string())?;
    guard.0 = true;
    ready.notify_all();
    loop {
        match wake.wait(guard) {
            Err(StoreError::Poisoned(_)) => break,
            Err(error) => return Err(format!("a wait on a poisoned mutex answered {error:?}")),
            Ok(back) if back.1 => {
                return Err(
                    "a wait answered the guard of a mutex poisoned while it waited".to_string(),
                );
            }
            // A spurious wakeup (the platform's backend may have them): the
            // other thread has not notified yet, so wait again.
            Ok(back) => guard = back,
        }
    }
    H::join(handle)??;
    match mutex.lock() {
        Err(StoreError::Poisoned(_)) => Ok(()),
        Ok(_) => Err("lock() after the poisoned wait answered the guard".to_string()),
        Err(error) => Err(format!("lock() after the poisoned wait answered {error:?}")),
    }
}

#[test]
fn sync_condvar_poison_std() {
    condvar_poison::<StdPrimitives>().unwrap();
}

#[test]
fn sync_condvar_poison_model() {
    let report = check(EXHAUSTIVE, condvar_poison::<Shuttle>).unwrap();
    println!(
        "sync_condvar_poison_model executions: {} (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// `sync_rwcell_poison_std` and `sync_rwcell_poison_model` (F5 for the cell):
/// a thread unwinds while it holds the cell's exclusive borrow, which
/// poisons it; then both tries answer `StoreError::Poisoned`, never a borrow
/// and never `Ok(None)`.
fn rwcell_poison<H: Harness>() -> Result<(), String> {
    let cell: Arc<RwCell<H, u32>> = Arc::new(RwCell::new(0));
    let theirs = Arc::clone(&cell);
    let handle = H::spawn(move || {
        caught_panic(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> Result<(), String> {
                let _write = theirs
                    .try_write()
                    .map_err(|e| e.to_string())?
                    .ok_or("try_write was busy on an unshared cell")?;
                std::panic::resume_unwind(Box::new("a panic under the cell's exclusive borrow"));
            },
        )))
    });
    H::join(handle)??;
    match cell.try_read() {
        Err(StoreError::Poisoned(_)) => {}
        Ok(read) => {
            return Err(format!(
                "try_read on a poisoned cell answered Ok({:?})",
                read.map(|r| *r)
            ));
        }
        Err(error) => return Err(format!("try_read on a poisoned cell answered {error:?}")),
    }
    match cell.try_write() {
        Err(StoreError::Poisoned(_)) => Ok(()),
        Ok(write) => Err(format!(
            "try_write on a poisoned cell answered Ok({:?})",
            write.map(|w| *w)
        )),
        Err(error) => Err(format!("try_write on a poisoned cell answered {error:?}")),
    }
}

#[test]
fn sync_rwcell_poison_std() {
    rwcell_poison::<StdPrimitives>().unwrap();
}

#[test]
fn sync_rwcell_poison_model() {
    let report = check(EXHAUSTIVE, rwcell_poison::<Shuttle>).unwrap();
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// The core's hash under the standard library's fixed-key hasher.
fn hash_of(core: &LatchCore) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    core.hash(&mut hasher);
    hasher.finish()
}

/// Two cores are equal and hash alike.
fn same(a: &LatchCore, b: &LatchCore, what: &str) -> Result<(), String> {
    require(a == b, format!("{what}: equal, where {a:?} and {b:?}"))?;
    require(
        hash_of(a) == hash_of(b),
        format!("{what}: equal cores hash alike"),
    )
}

/// Two cores differ.
fn differ(a: &LatchCore, b: &LatchCore, what: &str) -> Result<(), String> {
    require(a != b, format!("{what}: not equal, where {a:?} and {b:?}"))
}

/// `latch_core_equality` (card A-4c0 Work 3, "Equality and hashing"): `Eq`
/// and `Hash` compare the bound, the capacity, the holders, `queue[0..len]`,
/// `handed_len`, `handed_left` and `handed_first − head_seq`; neither
/// `head_seq` itself nor the bytes past `len` enter. `latch_core_exhaustive`
/// memoises on them, so an equality that ignored a live field would shrink
/// its search silently, and one that kept `head_seq` would grow it.
fn core_equality() -> Result<(), String> {
    use Mode::{Exclusive as X, Shared as S};

    // `head_seq` does not enter: two cores whose first seqs differ, and the
    // same arrivals on each.
    let mut a = said(LatchCore::new_at(1, 4, 0))?;
    let mut b = said(LatchCore::new_at(1, 4, 1_000))?;
    same(&a, &b, "new cores whose first seqs differ")?;
    for core in [&mut a, &mut b] {
        require(arrive(core, X)?.is_none(), "1:X is granted")?;
        require(arrive(core, S)?.is_some(), "2:S is queued")?;
        require(arrive(core, X)?.is_some(), "3:X is queued")?;
    }
    require(a.head_seq() != b.head_seq(), "the two heads' seqs differ")?;
    same(&a, &b, "the same arrivals from different first seqs")?;

    // The bytes past `len` do not enter. Both cores grant 1:S, queue 2:X,
    // release S and grant 2:X from the queue, so both hold Exclusive with an
    // empty queue; in `d` a barging 3:X raised 2:X's count before its grant,
    // so the stale byte at index 0 differs.
    let mut c = said(LatchCore::new(1, 4))?;
    require(arrive(&mut c, S)?.is_none(), "c: 1:S is granted")?;
    let sc = arrive(&mut c, X)?.ok_or("c: 2:X is queued")?;
    said(c.release(S))?;
    said(c.grant_queued(sc))?;
    let mut d = said(LatchCore::new(1, 4))?;
    require(arrive(&mut d, S)?.is_none(), "d: 1:S is granted")?;
    let sd = arrive(&mut d, X)?.ok_or("d: 2:X is queued")?;
    said(d.release(S))?;
    require(arrive(&mut d, X)?.is_none(), "d: 3:X barges")?;
    said(d.release(X))?;
    said(d.grant_queued(sd))?;
    require(
        c.holders() == Holders::Exclusive && c.waiting() == 0 && d.waiting() == 0,
        "c and d hold Exclusive with no waiter",
    )?;
    same(&c, &d, "cores that differ only past `len`")?;

    // Each logical field enters.
    differ(
        &said(LatchCore::new(0, 4))?,
        &said(LatchCore::new(1, 4))?,
        "the bound",
    )?;
    differ(
        &said(LatchCore::new(1, 4))?,
        &said(LatchCore::new(1, 5))?,
        "the capacity",
    )?;
    let free = said(LatchCore::new(1, 4))?;
    let mut shared = free.clone();
    shared.grant_arrival(S);
    differ(&free, &shared, "the holders")?;

    // A queued entry's mode.
    let mut e1 = said(LatchCore::new(0, 4))?;
    let mut e2 = e1.clone();
    for (core, mode) in [(&mut e1, S), (&mut e2, X)] {
        require(arrive(core, X)?.is_none(), "1:X is granted")?;
        require(arrive(core, mode)?.is_some(), "2 is queued")?;
    }
    differ(&e1, &e2, "a queued entry's mode")?;

    // A queued entry's count: both hold Shared(2) with one X queued, which a
    // later shared grant overtook in `f1` only.
    let mut f1 = said(LatchCore::new(1, 4))?;
    f1.grant_arrival(S);
    said(f1.enqueue(X))?;
    f1.grant_arrival(S);
    let mut f2 = said(LatchCore::new(1, 4))?;
    f2.grant_arrival(S);
    f2.grant_arrival(S);
    said(f2.enqueue(X))?;
    require(
        f1.holders() == Holders::Shared(2) && f2.holders() == Holders::Shared(2),
        "f1 and f2 hold Shared(2)",
    )?;
    differ(&f1, &f2, "a queued entry's count")?;

    // The queue's length.
    let mut g1 = said(LatchCore::new(0, 4))?;
    g1.grant_arrival(X);
    said(g1.enqueue(S))?;
    let mut g2 = g1.clone();
    said(g2.enqueue(S))?;
    differ(&g1, &g2, "the queue's length")?;

    // The uncollected members of the last batch: a batch of two readers,
    // before and after its second member collects.
    let mut h1 = said(LatchCore::new(0, 4))?;
    h1.grant_arrival(X);
    let s1 = said(h1.enqueue(S))?;
    let s2 = said(h1.enqueue(S))?;
    said(h1.release(X))?;
    require(
        said(h1.grant_queued(s1))? == Batch { first: s1, len: 2 },
        "the batch of two",
    )?;
    let mut h2 = h1.clone();
    said(h2.grant_queued(s2))?;
    differ(&h1, &h2, "the last batch's uncollected members")?;
    Ok(())
}

#[test]
fn latch_core_equality() {
    core_equality().unwrap();
}

/// How many readers `latch_forced_batch` queues together. Three, so that a
/// handed member's stamp is checked whichever reader grants the batch: with
/// two, the member the batch hands to may be the oldest, whose offset is 0.
const READERS: u32 = 3;

/// How many rounds `latch_forced_batch`'s main thread yields, at most, for
/// its readers to queue. A count, not a time (Limit 6): in a correct
/// build each reader queues within its first few instructions, so the bound
/// only turns a reader that never queues into a failure instead of a hang.
const QUEUE_ROUNDS: usize = 10_000_000;

/// One reader's stamps in `latch_forced_batch`: arrived, granted, released.
type ReaderStamps = (u64, u64, u64);

/// `latch_forced_batch`: the batch path of the blocking latch, which no
/// scenario of `latch_model_exhaustive` reaches (none queues two readers
/// together; a three-thread batch scenario does not end within the cap).
/// It forces the order the model tests can only sample, with no timing:
/// on `Latch<StdPrimitives>` with K = 0, main holds Exclusive while
/// [`READERS`] readers queue (it reads `info().waiting`, as A-3b's
/// forced-order tests do), then releases. Whichever reader re-checks first
/// grants the batch of all of them (R3), the others collect their handed
/// grants, and W2 gives the grants consecutive stamps in queue order after
/// main's release. The monitor then checks the whole trace.
fn forced_batch() -> Result<(), String> {
    let latch: Arc<Latch<StdPrimitives>> = Arc::new(said(Latch::new(0, READERS + 1))?);
    let main = said(latch.latch(Mode::Exclusive))?;
    let (main_arrived, main_granted) = main.stamps();
    let mut readers = Vec::new();
    for id in (1..=READERS as usize).map(|n| n * 8) {
        let latch = Arc::clone(&latch);
        let handle = std::thread::spawn(move || -> Result<ReaderStamps, String> {
            let guard = latch.latch(Mode::Shared).map_err(|e| e.to_string())?;
            let (arrived, granted) = guard.stamps();
            let released = guard.release().map_err(|e| e.to_string())?;
            Ok((arrived, granted, released))
        });
        readers.push((id, handle));
    }
    let mut rounds = 0;
    while said(latch.info())?.waiting < READERS {
        if readers.iter().any(|(_, handle)| handle.is_finished()) {
            return Err("a reader returned while main held the latch exclusive".to_string());
        }
        rounds += 1;
        require(
            rounds < QUEUE_ROUNDS,
            "the readers did not queue behind the exclusive holder",
        )?;
        yield_now::<StdPrimitives>();
    }
    let main_released = said(main.release())?;
    let mut requests = vec![Request {
        id: 0,
        mode: Mode::Exclusive,
        kind: Kind::Blocking,
        arrived: main_arrived,
        outcome: Outcome::Granted {
            grant: main_granted,
            release: Some(main_released),
        },
    }];
    let mut grants = Vec::new();
    for (id, handle) in readers {
        let (arrived, granted, released) = handle
            .join()
            .map_err(|_| "a reader panicked".to_string())??;
        grants.push((arrived, granted));
        requests.push(Request {
            id,
            mode: Mode::Shared,
            kind: Kind::Blocking,
            arrived,
            outcome: Outcome::Granted {
                grant: granted,
                release: Some(released),
            },
        });
    }
    // Queue order is arrival order.
    grants.sort_unstable();
    let first = grants.first().map_or(0, |&(_, granted)| granted);
    require(
        first > main_released,
        format!("the batch's first grant {first} follows main's release {main_released}"),
    )?;
    let expected: Vec<u64> = (0..u64::from(READERS)).map(|n| first + n).collect();
    let granted: Vec<u64> = grants.iter().map(|&(_, granted)| granted).collect();
    require(
        granted == expected,
        format!(
            "the batch's grants, in arrival order, are {granted:?}, where one tick per member in queue order (W2) gives {expected:?}"
        ),
    )?;
    check_trace(&requests, 0).map_err(|v| v.to_string())?;
    let info = said(latch.info())?;
    require(
        info == LatchInfo {
            holders: Holders::Free,
            waiting: 0,
            arrivals: u64::from(READERS) + 1,
            bound: 0,
            capacity: READERS + 1,
        },
        format!("after the batch, info() is {info:?}"),
    )?;
    Ok(())
}

#[test]
fn latch_forced_batch() {
    forced_batch().unwrap();
}
