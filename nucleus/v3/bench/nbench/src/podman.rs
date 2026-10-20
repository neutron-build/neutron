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
//! every row, successful or not — so [`unit_stop`] discards that
//! chatter instead of printing "Failed to stop … Unit … not loaded."
//! ahead of every result (card B-0-F6 Work item 2).

use crate::error::{Error, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Output of a finished foreground command (success implies exit 0).
pub struct CmdOut {
    /// Stdout of the command.
    pub stdout: String,
}

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
/// the last 4000 bytes, cut at a character boundary (card B-0-F6 uses
/// it for both stderr and the log-tail fallback).
pub fn stderr_tail(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    let max = 4000;
    if s.len() <= max {
        s.to_string()
    } else {
        let start = s.len() - max;
        let start = s
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| i >= start)
            .unwrap_or(start);
        format!("...{}", &s[start..])
    }
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
            stderr_tail: tail(&stderr, 4000),
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
/// accepts the unit, and the runner polls readiness separately.
pub fn unit_start(unit: &str, args: &[&str]) -> Result<()> {
    let status = Command::new("systemd-run")
        .args(["--user", "--unit", unit, "--collect", "--quiet"])
        .arg("podman")
        .args(args)
        .status()
        .map_err(Error::Io)?;
    if !status.success() {
        return Err(Error::Command {
            name: format!("systemd-run --unit {unit} podman {}", args.join(" ")),
            status: status.code().unwrap_or(-1),
            stderr_tail: String::new(),
        });
    }
    Ok(())
}

/// Stop a user unit and wait until it is gone (bounded).
///
/// The stop call's output is discarded: stopping a unit that is not
/// loaded is the normal first-stop case, and systemd's "Failed to stop
/// … Unit … not loaded." would otherwise precede every row, successful
/// or not, as noise the operator cannot act on (card B-0-F6 Work item
/// 2). Real stop failures are surfaced by the unit failing to
/// disappear, which the polling loop below bounds.
pub fn unit_stop(unit: &str) {
    let _ = Command::new("systemctl")
        .args(["--user", "stop", unit])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
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
}
