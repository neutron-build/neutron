//! The one place `buf` takes its synchronisation from (card A-3a).
//!
//! Every mutex, condition variable, latch and shared pointer the buffer
//! manager uses is named here and nowhere else in `buf`, so that a model
//! checker's primitives can be put in their place by editing this one module
//! (card A-4c0's interleaving harness, S-STORE §15 "The interleaving
//! harness": "every lock, wait, wake-up, atomic and yield of the store code
//! that such a test runs goes through one sync layer"; A-3b moves `buf` onto
//! that layer). It holds no logic and no `cfg`.
//!
//! The yield is card A-3b-0's (Work 4 and A6): the frame acquisition yields
//! between two of its attempts instead of spinning on a latch it must not
//! block on (S-STORE §5.2), and S-STORE §15 counts that attempt as a step.

pub(crate) use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
    TryLockError,
};
pub(crate) use std::thread::yield_now;
