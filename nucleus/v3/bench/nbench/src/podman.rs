//! podman lifecycle through the user systemd manager.
//!
//! Invariant owned here (card B-0 environment note): *every* podman call
//! goes through `systemd-run --user --wait --pipe --quiet podman ...`,
//! because agent sessions run with `NoNewPrivs` set and plain podman
//! cannot invoke its setuid helpers. Long-running servers start as their
//! own user unit (`nbench-<system>`, `--collect`, no `--wait`) and are
//! stopped with `systemctl --user stop`, so a crashed run never leaks a
//! competitor container.

use crate::error::{Error, Result};
use std::process::Command;
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

/// A short tail of a command's stderr, for error messages.
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
/// `--wait` so the call returns as soon as the container is launched.
pub fn unit_start(unit: &str, args: &[&str]) -> Result<()> {
    let status = Command::new("systemd-run")
        .args(["--user", "--unit", unit, "--collect", "--pipe", "--quiet"])
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
pub fn unit_stop(unit: &str) {
    let _ = Command::new("systemctl")
        .args(["--user", "stop", unit])
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
}
