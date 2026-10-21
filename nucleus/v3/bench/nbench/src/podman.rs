//! podman lifecycle through the user systemd manager.
//!
//! Invariant owned here (card B-0 environment note): *every* podman call
//! goes through `systemd-run --user --wait --pipe --quiet podman ...`,
//! because agent sessions run with `NoNewPrivs` set and plain podman
//! cannot invoke its setuid helpers. Long-running servers start as their
//! own user unit (`nbench-<system>`, `--collect`, no `--wait`) and are
//! stopped with `systemctl --user stop`, so a crashed run never leaks a
//! competitor container.
//!
//! Stopping a unit that is not loaded is *expected* — it happens before
//! every row, successful or not — so [`unit_stop`] keeps that case
//! silent instead of printing "Failed to stop … Unit … not loaded."
//! ahead of every result (card B-0-F6 Work item 2). Every *other* stop
//! outcome keeps its reason: [`unit_stop`] captures the stop call's
//! output, classifies it and prints whatever is not the expected
//! not-loaded case, so a genuine stop failure is no longer silent (card
//! B-0-F6-F1, verdict B-0-F6 findings 1 and 2).

use crate::error::{Error, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Output of a finished foreground command (success implies exit 0).
pub struct CmdOut {
    /// Stdout of the command.
    pub stdout: String,
}

/// Bound, in bytes, of every output tail this module puts in an error
/// message — stderr and the log-tail fallback alike. One constant, one
/// truncation seam (card B-0-F6-F1 Work item 4, verdict B-0-F6
/// finding 2).
const STDERR_TAIL_MAX: usize = 4000;

fn tail(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let start = s.len() - max;
        // do not split inside a character
        let start = s
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| i >= start)
            .unwrap_or(start);
        format!("...{}", &s[start..])
    }
}

/// A short tail of a command's output — its stderr, or an `NB_LOG`
/// file the command wrote through `2>&1 | tee` — for error messages:
/// the last [`STDERR_TAIL_MAX`] bytes, cut at a character boundary
/// (card B-0-F6 uses it for both stderr and the log-tail fallback).
///
/// The truncation itself lives only in the private [`tail`] helper:
/// before B-0-F6-F1 this function duplicated its body, and a mutant on
/// `tail` was invisible to every caller of this copy.
pub fn stderr_tail(bytes: &[u8]) -> String {
    tail(&String::from_utf8_lossy(bytes), STDERR_TAIL_MAX)
}

/// Run `podman <args>` to completion under the user manager.
pub fn podman(args: &[&str]) -> Result<CmdOut> {
    let mut cmd = Command::new("systemd-run");
    cmd.args(["--user", "--wait", "--pipe", "--quiet", "podman"])
        .args(args);
    let out = cmd.output().map_err(Error::Io)?;
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    if !out.status.success() {
        return Err(Error::Command {
            name: format!("podman {}", args.join(" ")),
            status: out.status.code().unwrap_or(-1),
            stderr_tail: tail(&stderr, STDERR_TAIL_MAX),
        });
    }
    let _ = &stderr; // only used for the error path above
    Ok(CmdOut { stdout })
}

/// Like [`podman`] but tolerant of failure; returns the exit code.
pub fn try_podman(args: &[&str]) -> i32 {
    let mut cmd = Command::new("systemd-run");
    cmd.args(["--user", "--wait", "--pipe", "--quiet", "podman"])
        .args(args);
    match cmd.output() {
        Ok(out) => out.status.code().unwrap_or(-1),
        Err(_) => -1,
    }
}

/// Start a long-running server container as user unit `<unit>`.
///
/// `--collect` makes systemd garbage-collect the unit when it stops; no
/// `--wait` and no `--pipe` — card B-0's environment note: both would
/// block until the server exits — so this returns as soon as systemd
/// accepts the unit, and the runner polls readiness separately. The
/// call's output is captured (capturing cannot block, for the same
/// reason) and a refused start carries its stderr in the error instead
/// of an empty tail (card B-0-F6-F1 Work item 3).
pub fn unit_start(unit: &str, args: &[&str]) -> Result<()> {
    let out = Command::new("systemd-run")
        .args(["--user", "--unit", unit, "--collect", "--quiet"])
        .arg("podman")
        .args(args)
        .output()
        .map_err(Error::Io)?;
    if !out.status.success() {
        return Err(Error::Command {
            name: format!("systemd-run --unit {unit} podman {}", args.join(" ")),
            status: out.status.code().unwrap_or(-1),
            stderr_tail: stderr_tail(&out.stderr),
        });
    }
    Ok(())
}

/// Is this stderr, for this unit, exactly systemd's not-loaded message?
///
/// systemd prints `Failed to stop <unit>.service: Unit <unit>.service
/// not loaded.` when a stop hits a unit that was never loaded — the
/// expected case before every row. The message must be the *only*
/// non-blank stderr line, it must end with `not loaded.`, and it must
/// name *this* unit (with or without the `.service` suffix systemd
/// appends). The match sits on a unit-name boundary, so a longer unit
/// that merely starts with `unit` (`nbench-a` vs `nbench-ab.service`)
/// does not pass for this one.
fn only_not_loaded_line(unit: &str, stderr: &str) -> bool {
    let mut non_blank = stderr.lines().map(str::trim).filter(|l| !l.is_empty());
    let Some(line) = non_blank.next() else {
        return false;
    };
    if non_blank.next().is_some() || !line.ends_with("not loaded.") {
        return false;
    }
    [unit.to_string(), format!("{unit}.service")]
        .iter()
        .any(|name| {
            let needle = format!("Unit {name}");
            line.match_indices(needle.as_str())
                .any(|(at, _)| &line[at + needle.len()..] == " not loaded.")
        })
}

/// Classify the result of a `systemctl --user stop <unit>` call: `None`
/// exactly when the stop is the expected case, `Some(reason)` for every
/// real failure (card B-0-F6-F1 Work item 1, verdict B-0-F6 finding 1).
///
/// Expected — silent — is exit 0, or the only non-blank stderr line
/// being systemd's not-loaded message for this very unit
/// ([`only_not_loaded_line`]). Everything else keeps a reason: a
/// non-zero exit with any other text or with none, more than one line,
/// a not-loaded line for a *different* unit, and a stop process killed
/// by a signal (`code == None`). The reason names the unit, the exit
/// code (or "killed by a signal") and a bounded tail of stderr; empty
/// stderr becomes the literal `(no output)`, never an empty string.
fn stop_report(unit: &str, code: Option<i32>, stderr: &str) -> Option<String> {
    if code == Some(0) || only_not_loaded_line(unit, stderr) {
        return None;
    }
    let how = match code {
        Some(c) => format!("exit {c}"),
        None => "killed by a signal".to_string(),
    };
    let said = {
        let t = tail(stderr, STDERR_TAIL_MAX);
        if t.trim().is_empty() {
            "(no output)".to_string()
        } else {
            t
        }
    };
    Some(format!(
        "systemctl --user stop {unit} failed ({how}): {said}"
    ))
}

/// Stop a user unit and wait until it is gone (bounded).
///
/// The expected case stays silent: stopping a unit that is not loaded
/// is the normal first-stop case, and systemd's "Failed to stop … Unit
/// … not loaded." would otherwise precede every row, successful or not,
/// as noise the operator cannot act on (card B-0-F6 Work item 2). The
/// stop call's output is *captured*, not inherited, so nothing leaks in
/// the expected case — and it is *kept*: any other outcome is printed
/// with its reason (a failed stop job on a loaded unit, a stop killed
/// by a signal, a failure to spawn systemctl at all, or a unit that is
/// still active when the poll below gives up), because silence there
/// hides the cause of the *next* start's failure on the same unit (card
/// B-0-F6-F1 Work items 1-2, verdict B-0-F6 finding 1). Still returns
/// `()`: a stop problem never aborts a row by itself.
pub fn unit_stop(unit: &str) {
    let stop = Command::new("systemctl")
        .args(["--user", "stop", unit])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    match stop {
        Ok(out) => {
            if let Some(reason) = stop_report(
                unit,
                out.status.code(),
                &String::from_utf8_lossy(&out.stderr),
            ) {
                eprintln!("nbench: {reason}");
            }
        }
        Err(e) => eprintln!("nbench: cannot run systemctl --user stop {unit}: {e}"),
    }
    // Wait for the unit to fully disappear so a restart cannot race the
    // old container's cgroup cleanup.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let active = Command::new("systemctl")
            .args(["--user", "is-active", "--quiet", unit])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !active {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    eprintln!(
        "nbench: unit {unit} is still active after waiting 60s; \
         the next start of this unit may race its cleanup"
    );
}

/// Run a command inside a container to completion (via the user manager).
pub fn try_podman_exec(container: &str, args: &[&str]) -> i32 {
    let mut full = vec!["exec", container];
    full.extend_from_slice(args);
    try_podman(&full)
}

/// Poll `podman exec` until it succeeds, with a bounded deadline.
pub fn wait_until_exec_ok(container: &str, args: &[&str], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if try_podman_exec(container, args) == 0 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_keeps_the_end_and_cuts_on_char_boundary() {
        assert_eq!(tail("hello", 10), "hello");
        // "héllo wörld" is 13 bytes; the cut at byte 8 must move to a
        // character boundary (the 'ö'), yielding its tail.
        assert_eq!(tail("héllo wörld", 5), "...örld");
        assert_eq!(tail("abcdefghij", 3), "...hij");
    }

    /// Child-mode marker for [`unit_stop_discards_expected_chatter`]:
    /// set on a re-execution of this very test binary so only the
    /// `unit_stop` call below runs, with its stdio inherited from the
    /// child (the parent captures the child's stderr).
    const UNIT_STOP_CHILD: &str = "NB_UNIT_STOP_SILENCE_CHILD";

    #[test]
    fn unit_stop_discards_expected_chatter() {
        // Card B-0-F6 Work item 2, mutant MUTED-STOP: `systemctl --user
        // stop` on a never-loaded unit prints "Failed to stop … Unit …
        // not loaded." on stderr — expected before every row — and
        // `unit_stop` must discard it. stdio config cannot be read back
        // from a `Command`, so the assertion observes the real thing:
        // the parent spawns this test binary (`--exact`, so only this
        // test runs) with the child marker; the child performs one real
        // `unit_stop` and exits; the parent asserts the child's stderr
        // carries none of the chatter. Without the `Stdio::null()`
        // redirection, the inherited stderr leaks the message and this
        // test fails.
        if std::env::var_os(UNIT_STOP_CHILD).is_some() {
            unit_stop("nbench-silence-probe");
            std::process::exit(0);
        }
        let exe = std::env::current_exe().expect("test binary path");
        let out = Command::new(&exe)
            .args([
                "--exact",
                "podman::tests::unit_stop_discards_expected_chatter",
                "--nocapture",
            ])
            .env(UNIT_STOP_CHILD, "1")
            .output()
            .expect("spawn child test binary");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("Failed to stop") && !stderr.contains("not loaded"),
            "unit_stop leaked systemd chatter to stderr: {stderr:?}"
        );
    }

    #[test]
    fn stop_report_expected_cases_are_silent() {
        // Card B-0-F6-F1 Work item 1, mutant STOP-LOUD-ALL: exit 0, and
        // the expected not-loaded message — the only non-blank line, for
        // this very unit, with or without the `.service` suffix systemd
        // appends, with or without a trailing blank line — are silence.
        assert_eq!(stop_report("nbench-x", Some(0), ""), None);
        let not_loaded = "Failed to stop nbench-x.service: Unit nbench-x.service not loaded.\n";
        assert_eq!(stop_report("nbench-x", Some(5), not_loaded), None);
        assert_eq!(
            stop_report("nbench-x", Some(5), &format!("{not_loaded}\n")),
            None
        );
        assert_eq!(
            stop_report(
                "nbench-x",
                Some(5),
                "Failed to stop nbench-x: Unit nbench-x not loaded.\n"
            ),
            None
        );
    }

    #[test]
    fn stop_report_keeps_real_failures() {
        // Card B-0-F6-F1 Work item 1, mutants STOP-SILENT-ALL,
        // STOP-ANY-NOT-LOADED and STOP-EMPTY-REASON: everything that is
        // not the expected case keeps a reason naming the unit, the exit
        // code (or the signal) and a tail of the stderr.
        let reason = stop_report(
            "nbench-x",
            Some(1),
            "Job for nbench-x.service failed because the control process exited with error code.\n",
        )
        .expect("a failed stop job on a loaded unit is a real failure");
        assert!(reason.contains("nbench-x"), "names the unit: {reason}");
        assert!(reason.contains("exit 1"), "names the exit code: {reason}");
        assert!(
            reason.contains("Job for nbench-x.service failed"),
            "keeps the stderr tail: {reason}"
        );
        let reason = stop_report("nbench-x", Some(1), "").expect("empty stderr is still a failure");
        assert!(
            reason.contains("(no output)"),
            "empty stderr becomes (no output), never an empty reason: {reason}"
        );
        let reason = stop_report("nbench-x", None, "").expect("a signalled stop is a real failure");
        assert!(
            reason.contains("killed by a signal"),
            "names the signal instead of an exit code: {reason}"
        );
        // a not-loaded line for a *different* unit is not our expected
        // case — including a longer unit that merely starts with ours
        let reason = stop_report(
            "nbench-a",
            Some(5),
            "Failed to stop nbench-ab.service: Unit nbench-ab.service not loaded.\n",
        )
        .expect("a not-loaded line for another unit is a real failure");
        assert!(
            reason.contains("nbench-ab.service"),
            "keeps the other unit's line: {reason}"
        );
        let reason = stop_report(
            "nbench-a",
            Some(5),
            "Failed to stop nbench-z.service: Unit nbench-z.service not loaded.\n",
        )
        .expect("a not-loaded line for an unrelated unit is a real failure");
        assert!(
            reason.contains("nbench-z.service"),
            "keeps the unrelated unit's line: {reason}"
        );
        // ...and so is the expected line followed by a second, different
        // one (what a `See user logs` follow-up looks like)
        let reason = stop_report(
            "nbench-x",
            Some(5),
            "Failed to stop nbench-x.service: Unit nbench-x.service not loaded.\n\
             See user logs and 'systemctl --user status' for details.\n",
        )
        .expect("two stderr lines are a real failure");
        assert!(
            reason.contains("See user logs"),
            "keeps both lines: {reason}"
        );
    }

    /// Child-mode marker for [`unit_stop_reports_a_real_failure`], the
    /// same re-exec pattern as [`UNIT_STOP_CHILD`]: the child performs
    /// one real `unit_stop` and exits; the parent captures the child's
    /// stderr.
    const UNIT_STOP_REPORT_CHILD: &str = "NB_UNIT_STOP_REPORT_CHILD";

    #[test]
    fn unit_stop_reports_a_real_failure() {
        // Card B-0-F6-F1 Work item 2, mutant STOP-SILENT-ALL (today's
        // behaviour: every stop outcome discarded). `systemctl --user
        // stop nbench@.service` refuses the instance-less template name
        // with a message that is *not* not-loaded, so this is a real
        // failure `unit_stop` must report. Probe on this host (re-run
        // for RESULT): exit 1, "Failed to stop nbench@.service: Unit
        // name nbench@.service is missing the instance name." plus a
        // "See user logs …" line.
        if std::env::var_os(UNIT_STOP_REPORT_CHILD).is_some() {
            unit_stop("nbench@.service");
            std::process::exit(0);
        }
        let exe = std::env::current_exe().expect("test binary path");
        let out = Command::new(&exe)
            .args([
                "--exact",
                "podman::tests::unit_stop_reports_a_real_failure",
                "--nocapture",
            ])
            .env(UNIT_STOP_REPORT_CHILD, "1")
            .output()
            .expect("spawn child test binary");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("nbench:") && stderr.contains("nbench@.service"),
            "unit_stop must print why a real stop failed: {stderr:?}"
        );
    }

    #[test]
    fn unit_start_failure_carries_stderr() {
        // Card B-0-F6-F1 Work item 3, mutant START-EMPTY-TAIL: a start
        // systemd refuses must carry its stderr, not an empty tail.
        // `nbench@.service` is a template name without an instance, so
        // systemd-run refuses it *before* starting anything and podman
        // never runs. Probe on this host (re-run with `true` for
        // RESULT): exit 1, "Failed to start transient service unit: Unit
        // name nbench@.service is missing the instance name."
        let err = unit_start("nbench@.service", &["version"])
            .expect_err("systemd-run refuses the instance-less template name");
        match err {
            Error::Command { stderr_tail, .. } => assert!(
                !stderr_tail.trim().is_empty(),
                "the refused start must carry its stderr: {stderr_tail:?}"
            ),
            other => panic!("expected Error::Command, got {other:?}"),
        }
    }
}
