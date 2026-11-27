//! The one place `buf` takes its synchronisation from (card A-3a; card
//! A-3b, Work 5).
//!
//! Every mutex, condition variable, cell and yield the buffer manager uses
//! is the store's sync layer's, named here and nowhere else in `buf`
//! (S-STORE §15 "The interleaving harness": "every lock, wait, wake-up,
//! atomic and yield of the store code that such a test runs goes through
//! one sync layer"), so the harness runs the pool's own code and a model
//! checker schedules its waits. `buf` names no `std::sync` item but `Arc`,
//! whose reference count never blocks and orders nothing the store code
//! reads (card A-3b, SF-3), and no `std::thread` item. The frame latch is
//! card A-4c0's `latch::Latch`, which takes its own primitives from the
//! same layer. It holds no logic and no `cfg`.
//!
//! The backend is the pool's type parameter `P` (card A-3b, Work 5): the
//! platform's in production, a model checker's in a harness test.
//!
//! The yield is card A-3b-0's (Work 4 and A6): the frame acquisition yields
//! between two of its attempts instead of spinning on a latch it must not
//! block on (S-STORE §5.2), and S-STORE §15 counts that attempt as a step.

pub(crate) use std::sync::Arc;

pub(crate) use crate::sync::{Condvar, Mutex, MutexGuard, Primitives, StdPrimitives, yield_now};
