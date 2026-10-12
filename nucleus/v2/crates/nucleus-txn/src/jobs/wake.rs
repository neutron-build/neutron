//! The wake primitive of the job runner (§9.3 "wake on enqueue"): a
//! pending flag plus a condvar. `notify` sets pending and wakes every
//! waiter; the runner takes the pending flag **before** each step, so a
//! notify that lands during a step re-arms the wake and forces another
//! step instead of being consumed by the park that follows.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// What [`Wake::wait`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeResult {
    /// Pending was set (and is now cleared).
    Woken,
    /// The timeout elapsed with no notify.
    TimedOut,
    /// The stop flag was set.
    Stopped,
}

/// A pending flag + condvar pair, one per [`JobKind`](crate::jobs::JobKind).
pub struct Wake {
    pending: Mutex<bool>,
    cv: Condvar,
}

impl Wake {
    pub(crate) fn new() -> Wake {
        Wake {
            pending: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, bool> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets pending and wakes every current and future waiter until the
    /// flag is taken.
    pub(crate) fn notify(&self) {
        *self.lock() = true;
        self.cv.notify_all();
    }

    /// Takes the pending flag: `true` iff a notify landed since the last
    /// take. The runner calls this before each step (a notify during the
    /// step survives until the next take).
    pub(crate) fn take_pending(&self) -> bool {
        let mut p = self.lock();
        if *p {
            *p = false;
            true
        } else {
            false
        }
    }

    /// Parks until pending is set, `stop` is set, or `timeout` elapses —
    /// returning without sleeping at all if pending or stop is already
    /// set. A `Woken` return consumes the pending flag.
    pub(crate) fn wait(&self, timeout: Duration, stop: &AtomicBool) -> WakeResult {
        let deadline = Instant::now() + timeout;
        let mut pending = self.lock();
        loop {
            if *pending {
                *pending = false;
                return WakeResult::Woken;
            }
            if stop.load(Ordering::SeqCst) {
                return WakeResult::Stopped;
            }
            let now = Instant::now();
            if now >= deadline {
                return WakeResult::TimedOut;
            }
            let (p2, timed_out) = self
                .cv
                .wait_timeout(pending, deadline - now)
                .unwrap_or_else(PoisonError::into_inner);
            pending = p2;
            if timed_out.timed_out() && Instant::now() >= deadline {
                // The loop head re-checks pending and stop first.
                continue;
            }
        }
    }
}
