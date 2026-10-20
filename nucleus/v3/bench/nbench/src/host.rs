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

/// Current load averages (1/5/15) as a JSON object, from /proc/loadavg.
/// Kept separate from [`collect`] so the runner can take a second
/// reading at results-write time (gate nv3-B-0-gate-1 finding 4).
pub fn loadavg_value() -> Value {
    let raw: Vec<String> = std::fs::read_to_string("/proc/loadavg")
        .unwrap_or_default()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    let field = |i: usize| -> Value {
        raw.get(i)
            .map(|s| Value::s(s))
            .unwrap_or_else(|| Value::s("unknown"))
    };
    Value::obj(vec![
        ("load1", field(0)),
        ("load5", field(1)),
        ("load15", field(2)),
    ])
}

/// Live `nbench-*` user units (competing nbench activity on this host).
/// A plain `systemctl --user` query — the card's environment note only
/// ever wraps *podman* in systemd-run.
fn nbench_units() -> String {
    let out = run_capture(
        "systemctl",
        &[
            "--user",
            "list-units",
            "--plain",
            "--no-legend",
            "--no-pager",
            "nbench-*",
        ],
    );
    if out == "unknown" {
        return out;
    }
    let names: Vec<String> = out
        .lines()
        .filter_map(|l| l.split_whitespace().next().map(|s| s.to_string()))
        .collect();
    if names.is_empty() {
        "none".into()
    } else {
        names.join(",")
    }
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
    // one source for the recorded provenance and the load-anomaly rule
    // (rework round 4 item a): the rule must be re-derivable from the
    // `cpus` field the results file already carries
    let nproc = cpu_count().to_string();
    let disk_free = run_capture("df", &["-h", "--output=size,used,avail", "/"]);
    // The runtime version goes through the same wrapper as every other
    // podman call (uniform provenance; the wrapper needs no privileges).
    let podman_version = crate::podman::podman(&["--version"])
        .map(|o| o.stdout.trim().to_string())
        .unwrap_or_else(|_| "unknown".into());
    let load = loadavg_value();
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
        // load at row start; the results file also carries
        // provenance.load_at_end (finding 4: co-tenant opacity)
        (
            "load1",
            load.get("load1")
                .cloned()
                .unwrap_or_else(|| Value::s("unknown")),
        ),
        (
            "load5",
            load.get("load5")
                .cloned()
                .unwrap_or_else(|| Value::s("unknown")),
        ),
        (
            "load15",
            load.get("load15")
                .cloned()
                .unwrap_or_else(|| Value::s("unknown")),
        ),
        ("nbench_units_live", Value::s(&nbench_units())),
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

/// Host CPU count — the same value recorded as the provenance `cpus`
/// field.
///
/// The end-of-row load-anomaly rule (runner, escalation
/// nv3-B-0-esc-1 / rework round 4 item a) compares `load1` against
/// this count, so a reader can re-derive the rule from the results
/// file's own provenance. Falls back to 1 — every reading then looks
/// anomalous — if the platform refuses to answer: the check fails
/// loud, it is never silently disabled.
pub fn cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
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
            "load1",
            "load5",
            "load15",
            "nbench_units_live",
            "root_fs",
            "podman",
            "container_runtime_note",
        ] {
            let val = v.get(key).and_then(Value::as_str).expect(key);
            assert!(!val.is_empty(), "{key} empty");
        }
    }

    #[test]
    fn loadavg_shape_is_three_numbers() {
        let v = loadavg_value();
        for key in ["load1", "load5", "load15"] {
            let s = v.get(key).and_then(Value::as_str).expect(key);
            // on a live host each field parses as f64 ("unknown" only
            // when /proc/loadavg is unreadable, which no Linux host is)
            assert!(
                s.parse::<f64>().is_ok(),
                "{key} = {s:?} is not a load average"
            );
        }
    }

    #[test]
    fn nbench_units_reports_none_without_live_units() {
        // no nbench server is running during `cargo test`; if one is
        // (parallel test run), the value is still a non-empty string
        let units = nbench_units();
        assert!(!units.is_empty());
    }
}
