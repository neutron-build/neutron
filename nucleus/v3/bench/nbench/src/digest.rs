//! Digest expectation protocol (PLAN-v3 §5.2 rule 4, card B-0 rework).
//!
//! Invariants owned here:
//! - Every result-set digest is compared against a *recorded
//!   expectation* — the comparison itself is a pure function
//!   ([`compare`]) so an inverted comparison cannot survive the unit
//!   tests (gate nv3-B-0-gate-1 defect 3: the "mismatch voids the run"
//!   rule previously had no tested seam and mutant M2 passed).
//! - The first run of a strict phase *records* its digest loudly; a
//!   missing **or blank** expectation file counts as not-yet-recorded
//!   (blanking the file is the documented way to re-record after a
//!   deliberate protocol change).
//! - A strict mismatch voids the run and stops measurement (§5.2 rule
//!   4); a non-strict mismatch is evidence only — [`Decision`] carries
//!   that decision, and the runner's [`crate::runner`] applies it.
//! - Smoke runs use `<phase>-smoke.txt` expectations, so a smoke digest
//!   can never become (or be compared against) the production
//!   expectation for a row.

use crate::error::{Error, Result};
use crate::json::Value;
use std::path::{Path, PathBuf};

/// What comparing a measured digest against its expectation concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// No expectation existed. `recorded` is true when this evaluation
    /// wrote the measured value as the new expectation (strict runs
    /// record; non-strict runs and smoke runs of non-strict phases do
    /// not, so the file stays absent).
    FirstRun {
        /// Whether the expectation file was written by this call.
        recorded: bool,
    },
    /// The expectation exists and equals the measured digest.
    Match,
    /// The expectation exists and differs (§5.2 rule 4 evidence).
    Mismatch {
        /// The previously recorded expectation.
        expected: String,
        /// The digest just measured.
        got: String,
    },
}

impl Decision {
    /// Whether nothing contradicted the measured digest. A first run
    /// reports `true` (nothing to contradict) so the results document
    /// matches the historical evidence shape.
    pub fn matched(&self) -> bool {
        !matches!(self, Decision::Mismatch { .. })
    }

    /// Whether this decision voids the run — true only for a mismatch.
    /// Strictness is the runner's call: the same mismatch on a
    /// non-strict phase is recorded but does not void.
    pub fn voids_run(&self) -> bool {
        matches!(self, Decision::Mismatch { .. })
    }

    /// The expectation as it was *before* this evaluation (for the
    /// results document; `None` on a first run, matching the historical
    /// `expected: null` evidence shape).
    pub fn expected(&self) -> Option<String> {
        match self {
            Decision::FirstRun { .. } => None,
            Decision::Match => None,
            Decision::Mismatch { expected, .. } => Some(expected.clone()),
        }
    }

    /// JSON evidence object for the results document (`value`,
    /// `expected`, `match` — the shape every committed results file
    /// already carries).
    pub fn evidence(&self, value: &str) -> Value {
        Value::obj(vec![
            ("value", Value::s(value)),
            (
                "expected",
                match self.expected() {
                    Some(e) => Value::s(&e),
                    None => Value::Null,
                },
            ),
            ("match", Value::Bool(self.matched())),
        ])
    }
}

/// Pure comparison of a measured digest against an expectation
/// (absence is *not* a mismatch — it is a first run).
pub fn compare(expected: Option<&str>, value: &str) -> Decision {
    match expected {
        None => Decision::FirstRun { recorded: false },
        Some(e) if e == value => Decision::Match,
        Some(e) => Decision::Mismatch {
            expected: e.to_string(),
            got: value.to_string(),
        },
    }
}

/// Path of the expectation file for one (row, phase, smoke) triple.
/// Smoke runs get `<phase>-smoke.txt` inside the same directory, which
/// `.gitignore` keeps out of the tree.
pub fn expectation_path(root: &Path, row_id: &str, phase: &str, smoke: bool) -> PathBuf {
    let mut name = phase.to_string();
    if smoke {
        name.push_str("-smoke");
    }
    name.push_str(".txt");
    root.join("workloads")
        .join(row_id)
        .join("expected")
        .join(name)
}

/// Load an expectation. A missing **or blank** file counts as
/// not-yet-recorded (blanking the file re-records after a deliberate
/// digest-protocol change).
pub fn load_expectation(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Record `value` as the expectation at `path`.
pub fn record(path: &Path, value: &str) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(Error::Io)?;
    }
    std::fs::write(path, value).map_err(Error::Io)?;
    Ok(())
}

/// Full expectation decision for one digest phase: load the
/// expectation, compare, and — on a first run of a *strict* phase —
/// record the measured value loudly.
pub fn decide(path: &Path, value: &str, strict: bool) -> Result<Decision> {
    let expected = load_expectation(path);
    match compare(expected.as_deref(), value) {
        Decision::FirstRun { .. } => {
            if strict {
                record(path, value)?;
                eprintln!(
                    "RECORDED initial digest for {} -> {}",
                    path.display(),
                    path.file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_default()
                );
                Ok(Decision::FirstRun { recorded: true })
            } else {
                Ok(Decision::FirstRun { recorded: false })
            }
        }
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("nbench-tests").join(format!(
            "{}-{}",
            std::process::id(),
            name
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    #[test]
    fn first_strict_run_records_then_matches() {
        let dir = tmpdir("digest-first-run");
        let path = dir.join("init.txt");
        let _ = std::fs::remove_file(&path);
        // first strict run: records
        let d = decide(&path, "abc", true).expect("decide");
        assert_eq!(d, Decision::FirstRun { recorded: true });
        assert!(d.matched());
        assert!(!d.voids_run());
        assert_eq!(load_expectation(&path).as_deref(), Some("abc"));
        // second strict run with the same digest: matches, no rewrite
        let d2 = decide(&path, "abc", true).expect("decide");
        assert_eq!(d2, Decision::Match);
        assert!(d2.matched());
        assert!(!d2.voids_run());
    }

    #[test]
    fn non_strict_first_run_does_not_record() {
        let dir = tmpdir("digest-nonstrict");
        let path = dir.join("final.txt");
        let _ = std::fs::remove_file(&path);
        let d = decide(&path, "v1", false).expect("decide");
        assert_eq!(d, Decision::FirstRun { recorded: false });
        assert!(
            !path.exists(),
            "non-strict run must not write an expectation"
        );
    }

    #[test]
    fn strict_mismatch_voids_and_carries_both_values() {
        let dir = tmpdir("digest-mismatch");
        let path = dir.join("init.txt");
        std::fs::write(&path, "expected-digest").expect("write");
        let d = decide(&path, "measured-digest", true).expect("decide");
        assert_eq!(
            d,
            Decision::Mismatch {
                expected: "expected-digest".into(),
                got: "measured-digest".into()
            }
        );
        assert!(d.voids_run());
        assert!(!d.matched());
        // evidence keeps both sides
        assert_eq!(d.expected().as_deref(), Some("expected-digest"));
        // the expectation file is untouched by a mismatch
        assert_eq!(load_expectation(&path).as_deref(), Some("expected-digest"));
    }

    #[test]
    fn compare_distinguishes_all_three_cases() {
        // This is the seam the inverted-comparison mutant (gate M2)
        // targets: swapping Match and Mismatch must fail here.
        assert_eq!(compare(None, "x"), Decision::FirstRun { recorded: false });
        assert_eq!(compare(Some("x"), "x"), Decision::Match);
        assert_eq!(
            compare(Some("x"), "y"),
            Decision::Mismatch {
                expected: "x".into(),
                got: "y".into()
            }
        );
        // Blank expectations: `compare` is the *pure* layer and treats
        // any Some as an expectation; the blank-as-absent (re-record)
        // rule lives in `load_expectation` and is covered by the
        // decide-path tests below.
        assert_eq!(
            compare(Some(""), "y"),
            Decision::Mismatch {
                expected: "".into(),
                got: "y".into()
            }
        );
        assert_eq!(load_expectation(Path::new("/nonexistent-no-file")), None);
    }

    #[test]
    fn smoke_expectations_are_isolated_from_production() {
        let root = tmpdir("digest-smoke-iso");
        let prod = expectation_path(&root, "some-row", "init", false);
        let smoke = expectation_path(&root, "some-row", "init", true);
        assert_ne!(prod, smoke);
        assert!(
            prod.to_string_lossy().ends_with("expected/init.txt"),
            "{prod:?}"
        );
        assert!(
            smoke.to_string_lossy().ends_with("expected/init-smoke.txt"),
            "{smoke:?}"
        );
        // recording one never creates or touches the other
        let _ = std::fs::remove_file(&prod);
        let _ = std::fs::remove_file(&smoke);
        let d = decide(&smoke, "s", true).expect("decide");
        assert_eq!(d, Decision::FirstRun { recorded: true });
        assert!(smoke.exists());
        assert!(!prod.exists(), "smoke recording leaked into production");
        // and a production recording leaves the smoke file alone
        let d2 = decide(&prod, "p", true).expect("decide");
        assert_eq!(d2, Decision::FirstRun { recorded: true });
        assert_eq!(load_expectation(&smoke).as_deref(), Some("s"));
        assert_eq!(load_expectation(&prod).as_deref(), Some("p"));
    }

    #[test]
    fn evidence_shape_matches_the_results_document() {
        let m = Decision::Mismatch {
            expected: "e".into(),
            got: "g".into(),
        };
        let v = m.evidence("g");
        assert_eq!(v.get("value").and_then(Value::as_str), Some("g"));
        assert_eq!(v.get("expected").and_then(Value::as_str), Some("e"));
        assert_eq!(v.get("match").and_then(Value::as_bool), Some(false));
        let f = Decision::FirstRun { recorded: true };
        let v = f.evidence("g");
        assert!(matches!(v.get("expected"), Some(Value::Null)));
        assert_eq!(v.get("match").and_then(Value::as_bool), Some(true));
    }

    #[test]
    fn blank_expectation_file_rerecords_on_strict_run() {
        // The documented re-record path: blank the file, next strict
        // run records the (new) digest instead of failing.
        let dir = tmpdir("digest-blank");
        let path = dir.join("init.txt");
        std::fs::write(&path, "\n").expect("blank");
        let d = decide(&path, "new-protocol", true).expect("decide");
        assert_eq!(d, Decision::FirstRun { recorded: true });
        assert_eq!(load_expectation(&path).as_deref(), Some("new-protocol"));
    }
}
