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
//!
//! The pass/fail decision is split into named library pieces —
//! [`check_arm`] (run the suite on one factory), [`report`] (build the
//! report object from the two checks) and [`report_passed`] (read the
//! decision back) — because the X-1b gate's VERIFY-BYPASS mutant
//! ("`arm_c_passed` forced true") passed every existing test.
//! `tests/measurement_integrity.rs` now drives a deliberately broken
//! engine through [`check_arm`] and asserts the decision is `false` at
//! each step, and `main` aborts exactly on [`report_passed`].

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

/// The outcome of checking one arm: whether the behaviour suite passed,
/// and its failures joined into one string when it did not.
#[derive(Clone, Debug)]
pub struct ArmCheck {
    /// Whether [`suite::run_all`] reported zero failures.
    pub passed: bool,
    /// The failures, joined with `"; "` (empty when `passed`).
    pub failures: String,
}

/// Run [`suite::run_all`] on one engine factory (B-ABORT's per-arm
/// half). Generic so the integrity test can check a deliberately broken
/// engine the same way the bench checks the real arms.
pub fn check_arm<E: e1_mvcc::api::TxnEngine>(make: &dyn Fn() -> E) -> ArmCheck {
    let failures = suite::run_all(make);
    ArmCheck {
        passed: failures.is_empty(),
        failures: failures.join("; "),
    }
}

/// Build the verification report from the two arms' checks (the
/// `arm_b_passed` / `arm_c_passed` fields are exactly `check.passed`).
pub fn report(b: ArmCheck, c: ArmCheck) -> Json {
    obj(vec![
        ("arm_b_failures", Json::from(b.failures)),
        ("arm_b_passed", b.passed.into()),
        ("arm_c_failures", Json::from(c.failures)),
        ("arm_c_passed", c.passed.into()),
        (
            "note",
            "e1_mvcc::suite::run_all in this binary, both arms at the suite shapes (4096 frames, 8192-byte pages, 64 MiB undo budget)"
                .into(),
        ),
    ])
}

/// The B-ABORT decision: the bench may measure only when **both** arms
/// passed. `main` aborts on exactly this function's word.
pub fn report_passed(v: &Json) -> bool {
    matches!(
        (
            crate::json::field(v, "arm_b_passed"),
            crate::json::field(v, "arm_c_passed"),
        ),
        (Some(Json::Bool(true)), Some(Json::Bool(true)))
    )
}

/// Run [`suite::run_all`] on both real arms; `Ok(report)` carries each
/// arm's pass/fail, `Err` only for a construction failure. An arm that
/// fails makes the caller abort the bench (B-ABORT).
pub fn run(root: &std::path::Path) -> Result<Json, BenchError> {
    let b = check_arm(&|| ArmB::new(4096, 8192));
    let c_dir = spill_dir(root);
    let c = check_arm(&|| ArmC::new(4096, 8192, 64 << 20, &c_dir));
    Ok(report(b, c))
}

#[cfg(test)]
mod tests {
    use super::{ArmCheck, report, report_passed};
    use crate::json::{Json, obj};

    /// The decision is the conjunction of both arms' fields — a report
    /// with either arm failed (or a field missing) is not passed. This
    /// is the test the gate's VERIFY-BYPASS mutant ("arm_c_passed
    /// forced true") now fails.
    #[test]
    fn report_passed_requires_both_arms() {
        let ok = ArmCheck {
            passed: true,
            failures: String::new(),
        };
        let bad = ArmCheck {
            passed: false,
            failures: "S4: write-write went wrong".into(),
        };
        assert!(report_passed(&report(
            ok.clone(),
            ArmCheck {
                passed: true,
                failures: String::new(),
            }
        )));
        assert!(!report_passed(&report(bad.clone(), ok.clone())));
        assert!(!report_passed(&report(ok, bad)));
        assert!(!report_passed(&obj(vec![])));
        assert!(!report_passed(&Json::Null));
    }

    /// The report carries the failures verbatim (an operator reading the
    /// JSON sees what the suite saw).
    #[test]
    fn report_carries_failures() {
        let r = report(
            ArmCheck {
                passed: false,
                failures: "S2: snapshot drifted".into(),
            },
            ArmCheck {
                passed: true,
                failures: String::new(),
            },
        );
        let s = r.render();
        assert!(s.contains("S2: snapshot drifted"), "report: {s}");
        assert!(s.contains("\"arm_b_passed\":false"), "report: {s}");
    }
}
