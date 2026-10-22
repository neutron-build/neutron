//! Card X-2 Work 9: arm (b) against the e1-mvcc behaviour suite.
//!
//! - `run_all(&|| ArmB::new(4096, 8192))` must return no failures: the
//!   whole behavioural subset (RC/RR visibility, EPQ, unique, locks,
//!   savepoints, atomicity, long-reader GC, the 500-seed S13
//!   differential, the 100k-write txn) on this arm's version layout.
//! - The S13 differential again with **pool = 64 frames**, as the card
//!   words it "so that eviction is exercised". Measured on this arm,
//!   that run does **not** exercise eviction: all 500 per-seed engines
//!   hold their whole tree in one page (max 33 versions — the tree's
//!   minimum page is 8132 bytes, so a whole S13 schedule fits one
//!   leaf), and 63 of the 64 frames stay empty. The run is kept exactly
//!   as the card mandates and reported in the RESULT.
//! - The purpose the card named — eviction under the differential-era
//!   suite — is exercised by the s14 run below instead: its one commit
//!   grows the tree to 854 pages (measured), ~53× a 16-frame pool, so
//!   the commit's inserts and the final full scan both run through
//!   constant dirty and clean eviction.
//!
//! The suite's hooks are per-engine state (card X-1a-F3), so these
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

/// The 100k-write scenario on a pool ~53× smaller than its tree, which
/// is where this arm actually exercises buffer-pool eviction (see the
/// module docs for why the 64-frame differential cannot).
#[test]
fn arm_b_passes_s14_with_a_pool_smaller_than_the_tree() {
    let failures = suite::s14_large_txn(&|| ArmB::new(16, 8192));
    assert!(failures.is_empty(), "failures: {failures:#?}");
}
