//! The one place `buf` takes its synchronisation from (card A-3a).
//!
//! Every mutex, condition variable, latch and shared pointer the buffer
//! manager uses is named here and nowhere else in `buf`, so that a model
//! checker's primitives can be put in their place by editing this one module
//! (card A-4c0's interleaving harness, S-STORE §15 "The interleaving
//! harness": "every lock, wait, wake-up, atomic and yield of the store code
//! that such a test runs goes through one sync layer"; A-3b moves `buf` onto
//! that layer). It holds no logic and no `cfg`.

pub(crate) use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
    TryLockError,
};
