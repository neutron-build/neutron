//! Card X-2 Work 9: arm (b) against the e1-mvcc behaviour suite.
//!
//! - `run_all(&|| ArmB::new(4096, 8192))` must return no failures: the
//!   whole behavioural subset (RC/RR visibility, EPQ, unique, locks,
//!   savepoints, atomicity, long-reader GC, the 500-seed S13
//!   differential, the 100k-write txn) on this arm's version layout.
//! - The S13 differential again with **pool = 64 frames** (the card's
//!   word: so that eviction is exercised — the run reports what the
//!   pool actually sees; see the RESULT for the measured page counts).
//!
//! The suite's hooks are per-engine state (card X-1a-F3), so these two
//! tests — and any concurrent `run_all` in another binary — cannot
//! satisfy each other's barriers or park each other's commits.

use e1_arm_b::ArmB;
use e1_mvcc::suite;

/// The full behaviour suite on the default pool (card Work 9).
#[test]
fn arm_b_passes_the_behaviour_suite() {
    let failures = suite::run_all(&|| ArmB::new(4096, 8192));
    assert!(failures.is_empty(), "failures: {failures:#?}");
}

/// The S13 differential with a 64-frame pool (card Work 9).
#[test]
fn arm_b_passes_the_s13_differential_with_a_64_frame_pool() {
    let failures = suite::s13_differential(&|| ArmB::new(64, 8192));
    assert!(failures.is_empty(), "failures: {failures:#?}");
}
