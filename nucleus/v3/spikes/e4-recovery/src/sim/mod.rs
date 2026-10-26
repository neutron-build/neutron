#![deny(clippy::indexing_slicing)]
//! The simulated environment: disk, scheduler and fault model (card X-5
//! Work 1).
//!
//! Invariants owned here:
//! - S-DET: the schedule is a pure function of the seed. Exactly one task
//!   runs at a time (the baton); every handoff is chosen by the seed's
//!   PRNG; no decision reads wall-clock time, thread identity or
//!   `HashMap`/`HashSet` iteration order. The same `(seed, mutant)` gives
//!   a byte-identical digest across runs and across `--threads` (Work 0).
//! - S-BATON: the running task holds the world lock between scheduler
//!   calls. Every scheduler entry point (`yield_baton`, `wait_for`,
//!   `finish`) and every `E4Env` call is a yield point where the baton may
//!   pass; no world borrow survives across one, which the code structure
//!   enforces (primitives lock, work, unlock).
//! - S-CRASH: at the crash point the scheduler stops the world; every task
//!   receives [`SimFail::Crashed`] at its next yield and unwinds. `DEADLOCK`
//!   (all tasks blocked) and `PANIC` (a task unwound by panic) are per-seed
//!   failures, never passes (card 1.2).
//!
//! The `nucleus-env` SimEnv (cards A-1a/A-1b) had not merged when this card
//! launched, so per the card's "Substrate" clause this module is the
//! spike-local simulated-I/O layer behind the narrow `E4Env` trait, written
//! so the swap is mechanical.

mod disk;
mod env;
mod sched;

pub use disk::{
    DWrite, DirOp, DiskEvent, FaultClass, FaultKnobs, MediaFault, MediaKind,
    PAGE_FILE as DISK_PAGE_FILE, ROOT_DIR as DISK_ROOT_DIR, ReadRec, SECTOR, SimDisk,
    WAL_DIR as DISK_WAL_DIR,
};
pub use env::{E4Env, EnvRef, FileId, HasDisk, PAGE_FILE, ROOT_DIR, WAL_DIR};
pub use sched::{
    HARNESS, Sched, SchedCore, SchedHost, SimFail, TaskId, WaitKey, WorldGuard, world_held_by_me,
};
