//! The monitor of S-STORE §5.3's latch rules (card A-4c0 Work 6): the oracle
//! of every latch test and of every planted bug.
//!
//! It is written from §5.3's sentences ("a waiting exclusive request blocks
//! new shared grants"; "a waiter is overtaken by at most `K` later requests";
//! "`try_latch` never succeeds over a queued exclusive waiter") and from
//! `t_root_latch_fair`'s definition ("the later requests granted before it").
//! It never calls, copies or mirrors the latch's own policy: its count of
//! later grants is its own, kept per request.
//!
//! The rules, each named at the start of its violation's text:
//! - **M1, mutual exclusion:** an exclusive grant while anyone holds, or a
//!   shared grant while an exclusive holder holds.
//! - **M2, shared over a waiting exclusive** (§5.3): a grant to a shared
//!   request while a blocking exclusive request that arrived before it is
//!   still waiting.
//! - **M3, try over a waiting exclusive** (§5.3): the same, for a try of
//!   either mode.
//! - **M4, bypass:** a waiting request with more than `K` later requests
//!   granted before it.
//! - **M5, completeness:** at the end, a request still waiting or holding.
//! - **M6, well-formed:** an event out of order for its request (arrive, then
//!   grant or refuse, then release), a stamp or an id used by two requests,
//!   or a release by a request that does not hold.
//!
//! When one grant breaks several rules, the monitor reports the first of M1,
//! M2, M3 and M4, in that order.

use std::fmt;

use nucleus_store::latch::{Kind, Mode};

/// A request's identity in a trace: small and dense, since the monitor keeps
/// each request's state at its id.
pub type Id = usize;

/// A violation of one of the monitor's rules (M1–M6). Its text starts with
/// the rule's name, so a test can tell the classes apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The rule's name, `M1` to `M6`.
    pub rule: &'static str,
    /// What happened.
    pub text: String,
}

impl Violation {
    /// A violation of `rule` (M6): an event the trace could not feed.
    fn malformed(text: String) -> Self {
        Self { rule: "M6", text }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.rule, self.text)
    }
}

/// What became of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Granted at stamp `grant`; released at `release` once it has.
    Granted {
        /// The grant's stamp (W2).
        grant: u64,
        /// The release's stamp (W2), `None` while the hold lasts.
        release: Option<u64>,
    },
    /// Refused at arrival.
    Refused,
}

/// One request as its thread recorded it (card A-4c0 Work 6): an id, a mode,
/// a kind, an arrival stamp and its outcome. A request that overflowed the
/// waiter queue never arrived and is not a `Request`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    /// The request's id, which no two requests of a trace share.
    pub id: Id,
    /// The mode asked for.
    pub mode: Mode,
    /// Blocking or try.
    pub kind: Kind,
    /// The arrival's stamp (W2).
    pub arrived: u64,
    /// The grant and release stamps, or the refusal.
    pub outcome: Outcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Arrive(Id, Mode, Kind),
    Grant(Id),
    Refuse(Id),
    Release(Id),
}

/// Orders every arrival, grant, refusal and release of `requests` by its
/// stamp, feeds them to a fresh [`LatchMonitor`] of bound `bound` in that
/// order, and ends with [`LatchMonitor::finish`].
///
/// A refusal shares its arrival's stamp (W2) and is fed right after it; that
/// is the only stamp two events may share.
pub fn check_trace(requests: &[Request], bound: u32) -> Result<(), Violation> {
    // (stamp, order at one stamp, event): an arrival, then its refusal, then
    // a grant, then a release.
    let mut events = Vec::with_capacity(requests.len() * 2);
    for r in requests {
        events.push((r.arrived, 0u8, Event::Arrive(r.id, r.mode, r.kind)));
        match r.outcome {
            Outcome::Refused => events.push((r.arrived, 1, Event::Refuse(r.id))),
            Outcome::Granted { grant, release } => {
                events.push((grant, 2, Event::Grant(r.id)));
                if let Some(release) = release {
                    events.push((release, 3, Event::Release(r.id)));
                }
            }
        }
    }
    events.sort_by_key(|e| (e.0, e.1));
    for pair in events.windows(2) {
        let (first, second) = (pair[0], pair[1]);
        let arrival_and_refusal =
            matches!((first.2, second.2), (Event::Arrive(a, _, _), Event::Refuse(b)) if a == b);
        if first.0 == second.0 && !arrival_and_refusal {
            return Err(Violation::malformed(format!(
                "stamp {} is used by two events ({:?} and {:?})",
                first.0, first.2, second.2
            )));
        }
    }
    let mut monitor = LatchMonitor::new(bound);
    for (_, _, event) in events {
        match event {
            Event::Arrive(id, mode, kind) => monitor.arrive(id, mode, kind)?,
            Event::Grant(id) => monitor.grant(id)?,
            Event::Refuse(id) => monitor.refuse(id)?,
            Event::Release(id) => monitor.release(id)?,
        }
    }
    monitor.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum State {
    Arrived { mode: Mode, kind: Kind },
    Granted { mode: Mode },
    Refused,
    Released,
}

/// A blocking request that has arrived and not yet taken its grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Waiter {
    id: Id,
    mode: Mode,
    /// The later requests granted before it: the monitor's own count (M4).
    overtaken: u32,
}

/// The monitor (M1–M6). It holds no stamps, so an enumerator can memoise on
/// it: its state is the requests still waiting, in arrival order, each with
/// its own count, and the holders.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LatchMonitor {
    bound: u32,
    /// The blocking requests not yet granted, in arrival order.
    waiting: Vec<Waiter>,
    /// The exclusive holder, if any.
    exclusive: Option<Id>,
    /// The shared holders.
    shared: u32,
    /// Every id that has arrived, at its index, and its state.
    states: Vec<Option<State>>,
}

impl LatchMonitor {
    /// A monitor of a latch whose bypass bound is `bound` (`K`).
    pub fn new(bound: u32) -> Self {
        Self {
            bound,
            waiting: Vec::new(),
            exclusive: None,
            shared: 0,
            states: Vec::new(),
        }
    }

    fn state(&self, id: Id) -> Option<State> {
        self.states.get(id).and_then(|s| *s)
    }

    /// The request `id` arrived, asking for `mode`. A blocking request waits
    /// from its arrival until its grant; a try is answered at once.
    pub fn arrive(&mut self, id: Id, mode: Mode, kind: Kind) -> Result<(), Violation> {
        if self.state(id).is_some() {
            return Err(Violation::malformed(format!(
                "id {id} is used by two requests"
            )));
        }
        if id >= self.states.len() {
            self.states.resize(id + 1, None);
        }
        if let Some(slot) = self.states.get_mut(id) {
            *slot = Some(State::Arrived { mode, kind });
        }
        if kind == Kind::Blocking {
            self.waiting.push(Waiter {
                id,
                mode,
                overtaken: 0,
            });
        }
        Ok(())
    }

    /// The request `id` was granted.
    pub fn grant(&mut self, id: Id) -> Result<(), Violation> {
        let (mode, kind) = match self.state(id) {
            Some(State::Arrived { mode, kind }) => (mode, kind),
            _ => {
                return Err(Violation::malformed(format!(
                    "grant of request {id}, which has not arrived or is already answered"
                )));
            }
        };
        // M1, mutual exclusion.
        let anyone_holds = self.exclusive.is_some() || self.shared > 0;
        if (mode == Mode::Exclusive && anyone_holds)
            || (mode == Mode::Shared && self.exclusive.is_some())
        {
            return Err(Violation {
                rule: "M1",
                text: format!(
                    "{mode:?} grant to request {id} while the latch is held: {}",
                    self.describe_holders()
                ),
            });
        }
        // The granted request's place among the waiters. A request granted at
        // its arrival is not among them: every waiter arrived before it.
        let position = self.waiting.iter().position(|w| w.id == id);
        let before = position.unwrap_or(self.waiting.len());
        // M2: a shared grant over a blocking exclusive that arrived earlier.
        if mode == Mode::Shared
            && let Some(earlier) = self
                .waiting
                .get(..before)
                .and_then(|ws| ws.iter().find(|w| w.mode == Mode::Exclusive))
        {
            return Err(Violation {
                rule: "M2",
                text: format!(
                    "shared request {id} granted while blocking exclusive request {} waits",
                    earlier.id
                ),
            });
        }
        // M3: a try of either mode over a waiting exclusive.
        if kind == Kind::Try
            && let Some(waiting) = self.waiting.iter().find(|w| w.mode == Mode::Exclusive)
        {
            return Err(Violation {
                rule: "M3",
                text: format!(
                    "try request {id} ({mode:?}) granted while blocking exclusive request {} waits",
                    waiting.id
                ),
            });
        }
        // M4: this grant overtakes every request that arrived earlier and still waits.
        for w in self.waiting.iter_mut().take(before) {
            w.overtaken = w.overtaken.saturating_add(1);
        }
        if let Some(overtaken) = self
            .waiting
            .iter()
            .take(before)
            .find(|w| w.overtaken > self.bound)
        {
            return Err(Violation {
                rule: "M4",
                text: format!(
                    "request {} is overtaken by {} later grants, more than K = {}",
                    overtaken.id, overtaken.overtaken, self.bound
                ),
            });
        }
        // The grant stands.
        if let Some(position) = position {
            self.waiting.remove(position);
        }
        match mode {
            Mode::Exclusive => self.exclusive = Some(id),
            Mode::Shared => self.shared += 1,
        }
        if let Some(slot) = self.states.get_mut(id) {
            *slot = Some(State::Granted { mode });
        }
        Ok(())
    }

    /// The try `id` was refused. A blocking request is never refused: it is
    /// queued (W3), or it overflows and never arrived.
    pub fn refuse(&mut self, id: Id) -> Result<(), Violation> {
        match self.state(id) {
            Some(State::Arrived {
                kind: Kind::Try, ..
            }) => {}
            _ => {
                return Err(Violation::malformed(format!(
                    "refusal of request {id}, which is not a try awaiting its answer"
                )));
            }
        }
        if let Some(slot) = self.states.get_mut(id) {
            *slot = Some(State::Refused);
        }
        Ok(())
    }

    /// The holder `id` released the latch.
    pub fn release(&mut self, id: Id) -> Result<(), Violation> {
        let mode = match self.state(id) {
            Some(State::Granted { mode }) => mode,
            _ => {
                return Err(Violation::malformed(format!(
                    "release by request {id}, which does not hold the latch"
                )));
            }
        };
        match mode {
            Mode::Exclusive => {
                if self.exclusive != Some(id) {
                    return Err(Violation::malformed(format!(
                        "release by request {id}, which does not hold the latch exclusively"
                    )));
                }
                self.exclusive = None;
            }
            Mode::Shared => {
                if self.shared == 0 {
                    return Err(Violation::malformed(format!(
                        "release by request {id}, which does not hold the latch shared"
                    )));
                }
                self.shared -= 1;
            }
        }
        if let Some(slot) = self.states.get_mut(id) {
            *slot = Some(State::Released);
        }
        Ok(())
    }

    /// M5: every request has been answered, and nobody waits or holds.
    pub fn finish(&self) -> Result<(), Violation> {
        let unanswered = self
            .states
            .iter()
            .flatten()
            .filter(|s| matches!(s, State::Arrived { .. }))
            .count();
        if !self.waiting.is_empty() || self.exclusive.is_some() || self.shared > 0 || unanswered > 0
        {
            return Err(Violation {
                rule: "M5",
                text: format!(
                    "at the end {} request(s) still wait, the holders are ({}), and {} arrived without an answer",
                    self.waiting.len(),
                    self.describe_holders(),
                    unanswered
                ),
            });
        }
        Ok(())
    }

    fn describe_holders(&self) -> String {
        let mut holders = Vec::new();
        if let Some(id) = self.exclusive {
            holders.push(format!("exclusive holder {id}"));
        }
        if self.shared > 0 {
            holders.push(format!(
                "{} shared holder{}",
                self.shared,
                if self.shared == 1 { "" } else { "s" }
            ));
        }
        if holders.is_empty() {
            "nobody".to_string()
        } else {
            holders.join(" and ")
        }
    }
}
