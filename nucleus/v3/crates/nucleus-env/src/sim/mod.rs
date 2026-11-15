#![deny(clippy::indexing_slicing)]
//! The simulated environment: disk, scheduler and fault model, behind
//! A-1a's `Env`/`File` traits (card A-1b Work 4).
//!
//! Provenance: `disk.rs` and `sched.rs` are copied from E4
//! (`nucleus/v3/spikes/e4-recovery` at `48ac9920`) with the provenance
//! blocks naming their departures; the module doc's first four
//! invariants are E4's `sim/mod.rs` lines 5–26 kept as text; `env.rs`
//! is rewritten against the traits (E4's `E4Env`/`EnvRef` adapter
//! shape, generalised). The spike stays E4's evidence record. PLAN-v3
//! §3.2 (the SimEnv bullet: "deterministic, with a seeded scheduler
//! … drops, tears, loses or misdirects unsynced writes"); S-STORE §15
//! (the SimEnv-named invariant rows, `t_seg_ack_durable`'s
//! I-RO-OPEN clause especially); §12.5's writer rule is what
//! `sim_segment_ack_substrate` pins (card A-1b Work 6).
//!
//! Invariants owned here:
//! - S-DET: the schedule is a pure function of the seed. Exactly one task
//!   runs at a time (the baton); every handoff is chosen by the seed's
//!   PRNG; no decision reads wall-clock time, thread identity or
//!   `HashMap`/`HashSet` iteration order. The same `(seed, mutant)` gives
//!   a byte-identical digest across runs and across `--threads` (Work 0).
//! - S-BATON: the running task holds the world lock between scheduler
//!   calls. Every scheduler entry point (`yield_baton`, `wait_for`,
//!   `finish`) and every SimEnv call is a yield point where the baton may
//!   pass; no world borrow survives across one, which the code structure
//!   enforces (primitives lock, work, unlock).
//! - S-CRASH: at the crash point the scheduler stops the world; every task
//!   receives [`SimFail::Crashed`] at its next yield and unwinds. `DEADLOCK`
//!   (all tasks blocked) and `PANIC` (a task unwound by panic) are per-seed
//!   failures, never passes (card 1.2).
//! - S-LIVE (X5-R13 (b)): a world whose yield count exceeds
//!   [`YIELD_BUDGET`] is livelocked — a task that keeps yielding and never
//!   parks can never trip the deadlock detector. The scheduler fails the
//!   world exactly as the deadlock path does and returns
//!   [`SimFail::Livelock`] naming the tripping site; the harness records it
//!   on the round, folded into the `deadlock` flags. A hang is never a
//!   pass, and never a hang.
//! - **S-ENV** (this card's layer, stated in full in `env.rs`): every
//!   `Env`/`File` method except `now`, `rand`, `SimFile::accepted` and
//!   `Drop` is a yield point and then one atomic step on the `SimDisk`
//!   (a crashed operation never executes); `Read` refusals and E-PATH
//!   errors come before the yield; disk failures map to the A-1a error
//!   variants, never to `Crashed`; the namespace is S-STORE §2's layout
//!   (root files, `wal`, files in `wal/`); a handle names its file by
//!   `FileId`, never its path (E-HANDLE); locks live on a per-life table
//!   keyed by `FileId`, so a crash releases every lock; the clock is
//!   virtual and `rand` draws from a per-life stream; every stream is
//!   `rng::derive(seed, TAG)` so streams never perturb one another.
//!
//! The `nucleus-env` SimEnv layer (card A-1b) is new code between E4's
//! proven parts and A-1a's trait: E4's spike-local `E4Env`/`EnvRef`
//! pair is replaced by [`SimEnv`]/[`SimFile`] implementing
//! `crate::Env`/`crate::File`, with the calling task found in a
//! thread-local (the trait's `&self` methods carry no task argument).

mod disk;
mod env;
mod sched;

pub use disk::{
    DWrite, DirOp, DiskEvent, FaultClass, FaultKnobs, MediaFault, MediaKind, PAGE_FILE,
    PAGE_FILE as DISK_PAGE_FILE, ROOT_DIR, ROOT_DIR as DISK_ROOT_DIR, ReadRec, SECTOR, SimDisk,
    WAL_DIR, WAL_DIR as DISK_WAL_DIR,
};
pub use env::{FileId, LifeOutcome, SIM_MAX_LEN, Sim, SimEnv, SimFile};
pub use sched::{
    HARNESS, Sched, SchedCore, SchedHost, SimFail, TaskId, WaitKey, WorldGuard, YIELD_BUDGET,
    world_held_by_me,
};

/// A host that carries the simulated disk (the world's disk slice).
/// Verbatim from E4's `src/sim/env.rs` lines 38–41 (card A-1b).
pub trait HasDisk {
    /// The disk slice.
    fn disk(&mut self) -> &mut SimDisk;
}
