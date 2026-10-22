//! The pre-measurement check (card X-1b Work 3): before anything is
//! measured, both arms must pass [`e1_mvcc::suite::run_all`] **in this
//! binary** — the same behaviour suite (RC/RR visibility, EPQ, unique,
//! locks, savepoints, atomicity, long-reader GC, the S13 differential,
//! the 100k-write txn) their own cards ran, executed here so a stale or
//! mis-built arm cannot be measured (B-ABORT).
//!
//! The engine shapes are the arms' own suite shapes (4096-frame pool,
//! 8192-byte pages, arm (c) at the 64 MiB undo budget), and arm (c)'s
//! spill directory sits under the bench scratch root.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use e1_arm_b::ArmB;
use e1_arm_c::ArmC;
use e1_mvcc::suite;

use crate::BenchError;
use crate::json::{Json, obj};

/// Allocate a fresh spill directory for one arm (c) verification engine
/// (each engine names its own file inside; the engines remove their
/// files on drop, the directory remains and is the caller's to reuse).
pub fn spill_dir(root: &std::path::Path) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = root.join(format!(
        "verify-c-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    if std::fs::create_dir_all(&d).is_err() {
        // Fall back to the system temp dir rather than fail the check:
        // the location is irrelevant to the suite's behaviour.
        return std::env::temp_dir().join(format!("e1-bench-verify-c-{}", std::process::id()));
    }
    d
}

/// Run [`suite::run_all`] on both arms; `Ok(report)` carries each arm's
/// pass/fail, `Err` only for a construction failure. An arm that fails
/// makes the caller abort the bench (B-ABORT).
pub fn run(root: &std::path::Path) -> Result<Json, BenchError> {
    let b_failures = suite::run_all(&|| ArmB::new(4096, 8192));
    let c_dir = spill_dir(root);
    let c_failures = suite::run_all(&|| ArmC::new(4096, 8192, 64 << 20, &c_dir));
    Ok(obj(vec![
        ("arm_b_failures", Json::from(b_failures.join("; "))),
        ("arm_b_passed", b_failures.is_empty().into()),
        ("arm_c_failures", Json::from(c_failures.join("; "))),
        ("arm_c_passed", c_failures.is_empty().into()),
        (
            "note",
            "e1_mvcc::suite::run_all in this binary, both arms at the suite shapes (4096 frames, 8192-byte pages, 64 MiB undo budget)"
                .into(),
        ),
    ]))
}
