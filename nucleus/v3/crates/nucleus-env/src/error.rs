//! `EnvError`: the one error enum of this crate (PLAN-v3 §6.1).
//!
//! Invariant owned: **the error surface**. Every fallible call in the
//! [`Env`](crate::Env) and [`File`](crate::File) traits returns one of
//! these variants and nothing else — no backend leaks a raw I/O error, so
//! engine code above this crate sees one shape on every platform
//! (PLAN-v3 §3.2; S-STORE §2 "All I/O goes through nucleus-env").
//!
//! The SQLSTATE column of S-STORE §1 is assigned at the session boundary
//! (PLAN-v3 §6.1 "one `thiserror` enum per crate, mapped to SQLSTATE at
//! the session boundary"); the per-variant docs name the mapping where a
//! normative document fixes it.

use std::io::ErrorKind;

/// Everything that can go wrong through the [`crate::Env`] surface.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvError {
    /// The named file or directory does not exist, or a parent directory
    /// of the named path does not (E-OPEN, E-NAMESPACE).
    #[error("not found: {path}")]
    NotFound {
        /// The path that was asked for.
        path: String,
    },
    /// The named file or directory already exists where creation was
    /// requested (E-OPEN's `CreateNew`, E-NAMESPACE's `create_dir` and
    /// `rename`).
    #[error("already exists: {path}")]
    AlreadyExists {
        /// The path that already exists.
        path: String,
    },
    /// Another open handle holds a conflicting lock on the file. Both
    /// lock modes are non-blocking, so a conflict is reported at once
    /// (E-LOCK; S-STORE §14.1 "No writer": the read-only open "fails
    /// rather than wait when a writer holds the lock").
    #[error("locked by another handle: {path}")]
    Locked {
        /// The path of the file whose lock is held.
        path: String,
    },
    /// A write-class operation (`write_at`, `set_len`, `sync_data`,
    /// `sync_all`) was attempted on a handle opened
    /// [`OpenMode::Read`](crate::OpenMode::Read). Refused before any
    /// I/O, so the read-only open of S-STORE §14.1 "What it refuses"
    /// fails loudly at the Env instead of silently at the device.
    #[error("handle is read-only: {path}")]
    ReadOnly {
        /// The path of the file the read-only handle names.
        path: String,
    },
    /// The path violates the E-PATH rules of this crate: it must be
    /// relative, with `/` separators, without a leading `/`, and each
    /// component non-empty, not `.` or `..`, and free of `\` and NUL.
    /// Checked before any I/O on every backend.
    #[error("invalid path {path:?}: {why}")]
    InvalidPath {
        /// The rejected path.
        path: String,
        /// Which E-PATH clause it violated.
        why: &'static str,
    },
    /// The underlying storage reported no space left (ENOSPC, EDQUOT).
    /// Maps to SQLSTATE **53100** at the session boundary (S-STORE §4.5;
    /// §14.1 "A stopped store opens read-only with zero free bytes").
    #[error("no space left for {op} on {path}")]
    NoSpace {
        /// The operation that ran out of space.
        op: &'static str,
        /// The path it was applied to.
        path: String,
    },
    /// The simulated power cut or process crash has happened
    /// (A-1b's SimEnv). A backend in this crate never returns it; it is
    /// declared here so A-1b need not edit this file.
    #[error("the simulated crash has happened")]
    Crashed,
    /// The simulated world failed: deadlock, livelock or a baton
    /// violation (A-1b's SimEnv). A backend in this crate never returns
    /// it; it is declared here so A-1b need not edit this file.
    #[error("simulated world halted: {what}")]
    Halted {
        /// What the scheduler was doing when the world halted.
        what: String,
    },
    /// Any other failure of the underlying platform. Maps to SQLSTATE
    /// **58030** at the session boundary (S-STORE §9.2).
    #[error("{op} on {path} failed ({kind}): {detail}")]
    Io {
        /// The operation that failed.
        op: &'static str,
        /// The path it was applied to.
        path: String,
        /// The platform's error classification.
        kind: ErrorKind,
        /// The platform's message.
        detail: String,
    },
}
