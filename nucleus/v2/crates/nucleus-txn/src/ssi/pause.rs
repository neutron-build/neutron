//! Test-only pause points inside SSI critical sections (seeds 20, 21): an
//! armed point parks the thread that reaches it until the test releases it.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

#[derive(Default)]
struct PauseState {
    armed: bool,
    arrived: bool,
    released: bool,
}

#[derive(Default)]
pub(crate) struct Pause {
    st: Mutex<PauseState>,
    cv: Condvar,
}

impl Pause {
    fn lock(&self) -> MutexGuard<'_, PauseState> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The next thread to reach the point parks there.
    pub(crate) fn arm(&self) {
        let mut st = self.lock();
        st.armed = true;
        st.arrived = false;
        st.released = false;
    }

    /// The pause point. No-op unless armed; fires once per arm.
    pub(crate) fn hit(&self) {
        let mut st = self.lock();
        if !st.armed {
            return;
        }
        st.armed = false;
        st.arrived = true;
        self.cv.notify_all();
        while !st.released {
            st = self.cv.wait(st).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Blocks until a thread parked at the point (up to `limit`); returns
    /// whether one did.
    pub(crate) fn wait_arrived(&self, limit: Duration) -> bool {
        let st = self.lock();
        let (st, _) = self
            .cv
            .wait_timeout_while(st, limit, |s| !s.arrived)
            .unwrap_or_else(PoisonError::into_inner);
        st.arrived
    }

    pub(crate) fn release(&self) {
        let mut st = self.lock();
        st.released = true;
        self.cv.notify_all();
    }
}
