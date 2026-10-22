//! The arm factories of the measurement driver — B-SAME's construction
//! half (card X-1b Work 1–2, Do-not "same pool, same page size, same
//! thread counts").
//!
//! Both arms are built by one code shape: a factory receives the pool
//! size the workload chose and forwards it — with the page size and undo
//! budget from the shared [`Config`] — **verbatim** into the arm's
//! constructor. There is deliberately no per-arm arithmetic between the
//! workload's pool and the constructor: the X-1b gate's POOL-PER-ARM
//! mutant (`make_c` gets `pool / 4`) lived exactly there, invisible to
//! every test, so the construction path is now library code that
//! `tests/measurement_integrity.rs` pins down twice:
//!
//! - [`make_b`]`(&cfg)(1)` and [`make_c`]`(&cfg, root)(1)` must build
//!   **working** engines: pool 1 is a legal (minimum) pool, while a
//!   factory that shrinks its argument turns 1 into 0 — and a zero pool
//!   is a broken configuration both arm constructors document (every
//!   later call returns `TxnErr::Engine`);
//! - [`make_c`] must give arm (c) exactly `cfg.undo_budget_bytes`: an
//!   engine that writes more than a quarter of the configured budget of
//!   undo, but less than the budget itself, must show
//!   `undo_spilled_bytes == 0`.
//!
//! Invariants owned here:
//! - E-SAME: the pool argument, page size and undo budget an arm is
//!   constructed with are a function of `(Config, pool)` alone — never
//!   of the arm — so two arms built for one workload configuration are
//!   built identically (B-SAME).
//! - E-FRESH: every arm (c) engine gets its own spill directory under
//!   the run's scratch root (the engines remove their spill files on
//!   drop; the directories are the caller's to reuse).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use e1_arm_b::ArmB;
use e1_arm_c::ArmC;

use crate::BenchError;
use crate::Config;

/// The arm (b) factory (E-SAME): a fresh
/// [`ArmB`](e1_arm_b::ArmB) for every `pool_frames`, at the shared
/// config's page size. `main` hands this to every W1–W6 workload.
pub fn make_b(cfg: &Config) -> impl Fn(usize) -> Result<ArmB, BenchError> {
    let page_size = cfg.page_size;
    move |pool_frames| Ok(ArmB::new(pool_frames, page_size))
}

/// The arm (c) factory (E-SAME, E-FRESH): a fresh
/// [`ArmC`](e1_arm_c::ArmC) for every `pool_frames`, at the shared
/// config's page size and undo budget, spilling into a fresh directory
/// under `root`.
pub fn make_c(cfg: &Config, root: &Path) -> impl Fn(usize) -> Result<ArmC, BenchError> {
    let page_size = cfg.page_size;
    let undo_budget_bytes = cfg.undo_budget_bytes;
    let root = root.to_path_buf();
    let n = AtomicU64::new(0);
    move |pool_frames| {
        let dir = root.join(format!(
            "armc-{}-{}",
            std::process::id(),
            n.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).map_err(|e| BenchError::new("arm c spill dir", e))?;
        Ok(ArmC::new(pool_frames, page_size, undo_budget_bytes, &dir))
    }
}
