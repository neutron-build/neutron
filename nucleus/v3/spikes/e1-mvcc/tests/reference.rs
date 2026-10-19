//! Card X-1a Work 6: `run_all` on the reference engine must return no
//! failures. This validates the suite itself — the scenarios are only a
//! usable oracle for the arms if the obviously-correct engine passes
//! every one of them.

use e1_mvcc::reference::RefEngine;
use e1_mvcc::suite;

#[test]
fn reference_engine_passes_the_behaviour_suite() {
    let failures = suite::run_all(&RefEngine::new);
    assert!(failures.is_empty(), "failures: {failures:#?}");
}
