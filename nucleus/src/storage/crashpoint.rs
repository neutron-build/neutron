//! Deterministic crash injection at named durability boundaries (M3).
//!
//! `probe_crash_subprocess` kills a child at a RANDOM instant, which gives
//! probabilistic coverage: over enough iterations it eventually lands in each
//! dangerous window, but it never proves any particular window is safe. This
//! module makes each window individually addressable — the harness names a
//! boundary, the child dies exactly there, and recovery is asserted for that
//! specific point. Probabilistic becomes exhaustive.
//!
//! A reached crashpoint calls `std::process::abort()`: no unwinding, no `Drop`,
//! no buffer flush, no destructor-driven fsync — the same observable outcome as
//! `kill -9` or power loss, but at a chosen instruction.
//!
//! # Cost when disabled
//!
//! The active point is read from the environment exactly once into a
//! `OnceLock`. With `NUCLEUS_CRASHPOINT` unset, `reach()` is one load of an
//! already-initialized `Option<&'static str>` plus a null check, returning
//! before any string comparison. Measured, not assumed: bulk-loading 500k
//! rows five times with these hooks compiled in versus stubbed to no-ops gave
//! the same 2.28s mean either way. That is cheap enough to keep in the
//! shipping binary, which matters — durability must be proven on the artifact
//! that actually runs, not on a specially-compiled one.
//!
//! # Usage
//!
//! ```sh
//! NUCLEUS_CRASHPOINT=wal.after_append        # die the first time that point is reached
//! NUCLEUS_CRASHPOINT=wal.after_fsync \
//! NUCLEUS_CRASHPOINT_SKIP=5                  # let 5 arrivals pass, die on the 6th
//! ```

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// The armed crashpoint name, or `None` when injection is off.
static ARMED: OnceLock<Option<String>> = OnceLock::new();
/// How many arrivals to let pass before aborting.
static SKIP: OnceLock<u64> = OnceLock::new();
/// Arrivals seen so far at the armed point.
static SEEN: AtomicU64 = AtomicU64::new(0);

fn armed() -> Option<&'static str> {
    ARMED
        .get_or_init(|| std::env::var("NUCLEUS_CRASHPOINT").ok())
        .as_deref()
}

fn skip() -> u64 {
    *SKIP.get_or_init(|| {
        std::env::var("NUCLEUS_CRASHPOINT_SKIP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    })
}

/// Every crashpoint the engine declares. Kept as one list so the harness can
/// enumerate the full set instead of hard-coding names that silently rot when
/// a point is renamed or removed.
pub const ALL_POINTS: &[&str] = &[
    "wal.before_append",
    "wal.after_append",
    "wal.before_fsync",
    "wal.after_fsync",
    "wal.before_commit_record",
    "wal.after_commit_record",
    "checkpoint.before",
    "checkpoint.mid_rewrite",
    "checkpoint.after",
    "meta.before_rename",
    "meta.after_rename",
    // Between the two commit-time durability forces for a cross-model
    // transaction: specialty stores (KV/timeseries/vector/graph/streams) are
    // already fsynced, the SQL WAL is not yet. See the ordering comment on
    // `force_specialty_durability`/`force_wal_durability` in executor/mod.rs
    // for why that is the deliberately SAFE half of this window — a crash
    // here must leave an orphaned specialty write, never a durable SQL row
    // referencing a specialty write that was never made durable.
    "commit.after_specialty_before_sql",
];

/// Raft durability boundaries, kept separate from [`ALL_POINTS`] because a SQL
/// workload never reaches them — listing them there would make
/// `probe_crash_points` report six permanent, bogus coverage gaps. They are
/// driven by `probe_raft_crash` instead.
///
/// These are the windows Raft's safety proof depends on: a vote or an
/// acknowledged entry lost across one of them lets a single term elect two
/// leaders, or drops a write a client was already told was committed.
pub const ALL_RAFT_POINTS: &[&str] = &[
    "raft.before_hardstate_fsync",
    "raft.after_hardstate_fsync",
    "raft.before_hardstate_rename",
    "raft.after_hardstate_rename",
    "raft.before_log_fsync",
    "raft.after_log_fsync",
];

// ============================================================================
// I/O fault injection
// ============================================================================

/// The armed I/O fault point, or `None` when injection is off.
static IO_ARMED: OnceLock<Option<String>> = OnceLock::new();
/// Which error kind to inject.
static IO_KIND: OnceLock<String> = OnceLock::new();
/// Arrivals seen so far at the armed I/O fault point.
static IO_SEEN: AtomicU64 = AtomicU64::new(0);
/// How many arrivals to let pass before failing.
static IO_SKIP: OnceLock<u64> = OnceLock::new();

/// Every I/O fault point the engine declares.
pub const ALL_IO_POINTS: &[&str] = &[
    "wal.append",
    "wal.fsync",
    "meta.write",
    // A Raft node that cannot persist must refuse to vote or acknowledge
    // rather than answer from memory; these points make that path testable.
    "raft.hardstate_write",
    "raft.log_append",
    // Specialty-store WALs (KV / collections / timeseries / vector / graph /
    // streams) reach durability through `force_specialty_durability`, forced
    // BEFORE the SQL WAL at the same commit boundary — see the ordering
    // comment on that call in executor/mod.rs. `kv.wal_fsync` makes that
    // ordering directly testable: fail it and assert the SQL WAL was never
    // forced either, instead of trusting an unobservable crash race.
    "kv.wal_fsync",
];

fn io_armed() -> Option<&'static str> {
    IO_ARMED
        .get_or_init(|| std::env::var("NUCLEUS_IOFAULT").ok())
        .as_deref()
}

/// An injected `io::Error` for `name`, or `None` to proceed normally.
///
/// Where `reach()` models power loss, this models *failing hardware*: a disk
/// that is full, a filesystem gone read-only, an fsync that reports failure.
/// Those paths are otherwise nearly impossible to exercise portably, and they
/// are exactly where a database is most tempted to continue with suspect data.
///
/// ```sh
/// NUCLEUS_IOFAULT=wal.fsync NUCLEUS_IOFAULT_KIND=full   # ENOSPC on fsync
/// NUCLEUS_IOFAULT=wal.append NUCLEUS_IOFAULT_SKIP=10    # fail the 11th append
/// ```
#[inline]
pub fn io_fault(name: &str) -> Option<std::io::Error> {
    let active = io_armed()?;
    if active != name {
        return None;
    }
    let skip = *IO_SKIP.get_or_init(|| {
        std::env::var("NUCLEUS_IOFAULT_SKIP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    });
    if IO_SEEN.fetch_add(1, Ordering::SeqCst) < skip {
        return None;
    }
    let kind = IO_KIND.get_or_init(|| {
        std::env::var("NUCLEUS_IOFAULT_KIND").unwrap_or_else(|_| "full".to_string())
    });
    let (k, msg) = match kind.as_str() {
        "perm" => (std::io::ErrorKind::PermissionDenied, "injected permission denied"),
        "ro" => (std::io::ErrorKind::PermissionDenied, "injected read-only filesystem"),
        "io" => (std::io::ErrorKind::Other, "injected I/O error"),
        _ => (std::io::ErrorKind::StorageFull, "injected no space left on device"),
    };
    Some(std::io::Error::new(k, msg))
}

/// Convenience for call sites returning `io::Result`: fail if armed.
///
/// Gated to match its only consumer (`storage::mvcc_wal`, server-only) so a
/// core-only build does not warn about an unused macro.
#[cfg(feature = "server")]
macro_rules! io_fault_check {
    ($name:expr) => {
        if let Some(e) = $crate::storage::crashpoint::io_fault($name) {
            return Err(e);
        }
    };
}
#[cfg(feature = "server")]
pub(crate) use io_fault_check;

/// Abort the process if `name` is the armed crashpoint and its skip count is
/// exhausted. Otherwise return immediately.
#[inline]
pub fn reach(name: &str) {
    // Fast path: nothing armed.
    let Some(active) = armed() else { return };
    if active != name {
        return;
    }
    if SEEN.fetch_add(1, Ordering::SeqCst) < skip() {
        return;
    }
    // Emit before dying so a harness reading the child's stderr can confirm
    // the crash was the injected one and not an unrelated failure.
    eprintln!("NUCLEUS_CRASHPOINT_HIT {name}");
    // Flush only our own diagnostic; the database's buffers are deliberately
    // left unflushed — that is the whole point.
    use std::io::Write;
    let _ = std::io::stderr().flush();
    std::process::abort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_crashpoints_are_inert() {
        // With nothing armed (the default in the test process), every declared
        // point must be a no-op. If this ever aborts, the suite dies loudly.
        for p in ALL_POINTS.iter().chain(ALL_RAFT_POINTS) {
            reach(p);
        }
    }

    #[test]
    fn disabled_io_faults_are_inert() {
        for p in ALL_IO_POINTS {
            assert!(io_fault(p).is_none(), "{p} injected with nothing armed");
        }
    }

    #[test]
    fn point_names_are_unique_and_namespaced() {
        let mut seen = std::collections::HashSet::new();
        for p in ALL_POINTS.iter().chain(ALL_RAFT_POINTS) {
            assert!(seen.insert(*p), "duplicate crashpoint name: {p}");
            assert!(
                p.contains('.'),
                "crashpoint {p} should be namespaced as <subsystem>.<boundary>"
            );
        }
    }
}
