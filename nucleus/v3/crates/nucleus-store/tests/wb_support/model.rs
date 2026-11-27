//! A pauser for the interleaving harness (card A-3c, Work 13.5): the same
//! calls as A-3a's `buf_support::Pauser` (`pause_at`, `report`,
//! `next_event`, `release`, `release_all`, `hook`), built only on the sync
//! facade over `P` (`nucleus_store::sync::{Mutex, Condvar}`), so that the
//! model checker schedules every pause and every wait. Events carry
//! `(point, key, frame)`. Nothing here has a timed wait: a paused thread
//! waits on a condition variable that only `release` signals.

use std::sync::Arc;

use nucleus_store::buf::{FrameId, Hook, HookPoint, PageKey};
use nucleus_store::sync::{Condvar, Mutex, Primitives, StdPrimitives};

/// An event the controller receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// A thread reached an armed pause point and is now blocked there.
    Arrived(HookPoint, PageKey, FrameId),
    /// A reported point fired (the thread did not block).
    Seen(HookPoint, PageKey, FrameId),
    /// A thread's closure ended (sent on its way out, even by a panic).
    Done(&'static str),
}

/// A pause's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pause {
    Armed,
    Paused,
    Released,
}

/// The state the hook and the controller share.
struct Shared<P: Primitives> {
    pauses: Mutex<P, Vec<(HookPoint, PageKey, Pause)>>,
    reports: Mutex<P, Vec<(HookPoint, PageKey)>>,
    events: Mutex<P, Vec<Event>>,
    wake: Condvar<P>,
}

impl<P: Primitives> Shared<P> {
    /// Send `e` to the controller and wake it.
    fn send(&self, e: Event) -> Result<(), nucleus_store::StoreError> {
        let mut events = self.events.lock()?;
        events.push(e);
        self.wake.notify_all();
        Ok(())
    }

    /// Whether `(point, key)` is reported.
    fn reported(&self, point: HookPoint, key: PageKey) -> bool {
        self.reports
            .lock()
            .is_ok_and(|reports| reports.contains(&(point, key)))
    }

    /// The hook: a firing that an armed pause holds sends `Arrived` only and
    /// blocks until released; every other firing of a reported point sends
    /// `Seen`. An error of the facade's (a mutex a panic poisoned) makes the
    /// hook return, so that the execution ends as the harness's `Deadlock`,
    /// never as a pass.
    fn fire(&self, point: HookPoint, key: PageKey, frame: FrameId) {
        let mut pauses = match self.pauses.lock() {
            Ok(pauses) => pauses,
            Err(_) => return,
        };
        let armed = pauses.iter_mut().any(|(p, k, state)| {
            if *p == point && *k == key && *state == Pause::Armed {
                *state = Pause::Paused;
                true
            } else {
                false
            }
        });
        if !armed {
            drop(pauses);
            if self.reported(point, key) {
                let _ = self.send(Event::Seen(point, key, frame));
            }
            return;
        }
        drop(pauses);
        if self.send(Event::Arrived(point, key, frame)).is_err() {
            return;
        }
        let Ok(mut pauses) = self.pauses.lock() else {
            return;
        };
        while pauses
            .iter()
            .any(|(p, k, state)| *p == point && *k == key && *state == Pause::Paused)
        {
            pauses = match self.wake.wait(pauses) {
                Ok(pauses) => pauses,
                Err(_) => return,
            };
        }
    }
}

/// The race tests' controller under the harness: one-shot pauses at named
/// hook points, reports, and an event queue, all on the facade over `P`.
pub struct ModelPauser<P: Primitives = StdPrimitives> {
    shared: Arc<Shared<P>>,
}

impl<P: Primitives> ModelPauser<P> {
    /// A controller with nothing armed.
    pub fn new() -> ModelPauser<P> {
        ModelPauser {
            shared: Arc::new(Shared {
                pauses: Mutex::new(Vec::new()),
                reports: Mutex::new(Vec::new()),
                events: Mutex::new(Vec::new()),
                wake: Condvar::new(),
            }),
        }
    }

    /// Arm a one-shot pause: the first thread to fire `point` for `key` sends
    /// `Arrived` and blocks until `release(point, key)`.
    pub fn pause_at(&self, point: HookPoint, key: PageKey) -> Result<(), String> {
        self.shared
            .pauses
            .lock()
            .map(|mut pauses| pauses.push((point, key, Pause::Armed)))
            .map_err(|e| format!("{e:?}"))
    }

    /// Make every firing of `point` for `key` send `Seen`, without blocking.
    pub fn report(&self, point: HookPoint, key: PageKey) -> Result<(), String> {
        self.shared
            .reports
            .lock()
            .map(|mut reports| reports.push((point, key)))
            .map_err(|e| format!("{e:?}"))
    }

    /// Send `Done(name)`.
    pub fn done(&self, name: &'static str) {
        let _ = self.shared.send(Event::Done(name));
    }

    /// The next event, waiting until one comes.
    pub fn next_event(&self) -> Result<Event, String> {
        let mut events = self.shared.events.lock().map_err(|e| format!("{e:?}"))?;
        loop {
            if !events.is_empty() {
                return Ok(events.remove(0));
            }
            events = self
                .shared
                .wake
                .wait(events)
                .map_err(|e| format!("{e:?}"))?;
        }
    }

    /// Every event sent and not yet taken, without blocking.
    pub fn drain(&self) -> Result<Vec<Event>, String> {
        let mut events = self.shared.events.lock().map_err(|e| format!("{e:?}"))?;
        Ok(core::mem::take(&mut *events))
    }

    /// The hook to install with `Pool::set_hook`.
    pub fn hook(&self) -> Hook {
        let shared = Arc::clone(&self.shared);
        Arc::new(move |point, key, frame| shared.fire(point, key, frame))
    }

    /// Unblock the thread paused at `(point, key)`, or disarm the pause if no
    /// thread has reached it yet.
    pub fn release(&self, point: HookPoint, key: PageKey) -> Result<(), String> {
        let released = self
            .shared
            .pauses
            .lock()
            .map(|mut pauses| {
                let mut any = false;
                for (p, k, state) in pauses.iter_mut() {
                    if *p == point && *k == key {
                        *state = Pause::Released;
                        any = true;
                    }
                }
                any
            })
            .map_err(|e| format!("{e:?}"))?;
        self.shared.wake.notify_all();
        let _ = released;
        Ok(())
    }

    /// `release` every armed or paused point.
    pub fn release_all(&self) -> Result<(), String> {
        self.shared
            .pauses
            .lock()
            .map(|mut pauses| {
                for (_, _, state) in pauses.iter_mut() {
                    *state = Pause::Released;
                }
            })
            .map_err(|e| format!("{e:?}"))?;
        self.shared.wake.notify_all();
        Ok(())
    }
}
