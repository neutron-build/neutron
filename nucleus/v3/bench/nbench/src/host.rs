//! Host provenance capture (PLAN-v3 §5.2 rule 6).
//!
//! Invariant owned here: a results file names the exact host, kernel,
//! CPU count, RAM, container runtime and free disk it was produced on, so
//! two runs can never be silently compared across machines (rule 1).

use crate::error::Result;
use crate::json::Value;
use std::process::Command;

fn run_capture(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn first_line_of(path: &str) -> String {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.lines().next().map(|l| l.trim().to_string()))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

/// Collect host facts into a JSON object.
pub fn collect() -> Result<Value> {
    let mem_total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines().next().and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|kib| kib.parse::<u64>().ok())
                    .map(|kib| format!("{:.1} GiB", kib as f64 / 1024.0 / 1024.0))
            })
        })
        .unwrap_or_else(|| "unknown".into());
    let nproc = std::thread::available_parallelism()
        .map(|n| n.get().to_string())
        .unwrap_or_else(|_| "unknown".into());
    let disk_free = run_capture("df", &["-h", "--output=size,used,avail", "/"]);
    // The runtime version goes through the same wrapper as every other
    // podman call (uniform provenance; the wrapper needs no privileges).
    let podman_version = crate::podman::podman(&["--version"])
        .map(|o| o.stdout.trim().to_string())
        .unwrap_or_else(|_| "unknown".into());
    Ok(Value::obj(vec![
        (
            "hostname",
            Value::s(&first_line_of("/proc/sys/kernel/hostname")),
        ),
        (
            "kernel",
            Value::s(&first_line_of("/proc/sys/kernel/osrelease")),
        ),
        ("cpu_model", Value::s(&cpu_model())),
        ("cpus", Value::s(&nproc)),
        ("mem_total", Value::s(&mem_total)),
        ("root_fs", Value::s(&disk_free)),
        ("podman", Value::s(&podman_version)),
        (
            "container_runtime_note",
            Value::s(
                "rootless podman via systemd-run --user (PLAN-v3 §5.2 rule 1; card B-0 environment note)",
            ),
        ),
    ]))
}

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split(':').nth(1).map(|m| m.trim().to_string()))
        })
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provenance_has_the_required_fields() {
        let v = collect().expect("collect");
        for key in [
            "hostname",
            "kernel",
            "cpu_model",
            "cpus",
            "mem_total",
            "root_fs",
            "podman",
            "container_runtime_note",
        ] {
            let val = v.get(key).and_then(Value::as_str).expect(key);
            assert!(!val.is_empty(), "{key} empty");
        }
    }
}
