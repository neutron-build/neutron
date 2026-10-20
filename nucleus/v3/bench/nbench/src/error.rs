//! Errors for the nbench orchestrator.
//!
//! Invariant owned here: every failure mode of the harness — subprocess
//! failure, malformed tool output, digest mismatch, bad CLI usage — is
//! reported as an [`Error`] value and never as a panic (PLAN-v3 §6.1).
//! The digest-mismatch arm exists so §5.2 rule 4 ("a mismatch voids the
//! run") is a typed outcome the runner records, not a crash.

use std::fmt;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Anything the nbench runner can fail on.
#[derive(Debug)]
pub enum Error {
    /// Wrapped I/O failure (files, pipes, sockets).
    Io(std::io::Error),
    /// A subprocess exited non-zero.
    Command {
        /// The command line that failed, for the log.
        name: String,
        /// Exit status code (non-zero by construction).
        status: i32,
        /// Tail of the subprocess stderr, kept small.
        stderr_tail: String,
    },
    /// JSON encode/decode failure.
    Json(String),
    /// A tool produced output we could not parse.
    Parse {
        /// What was being parsed.
        what: &'static str,
        /// The offending line or context.
        line: String,
    },
    /// A value was missing or of the wrong type in a JSON document.
    Missing(String),
    /// A numeric sample list was empty where it must not be.
    EmptySamples,
    /// A result digest mismatched its expectation: the run is void
    /// (PLAN-v3 §5.2 rule 4).
    DigestMismatch {
        /// Which phase (init/final) mismatched.
        phase: String,
        /// The digest recorded previously (expected).
        expected: String,
        /// The digest just measured.
        got: String,
    },
    /// A row id unknown to the registry.
    UnknownRow(String),
    /// Bad CLI usage; the message is printed with the usage text.
    Usage(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Command {
                name,
                status,
                stderr_tail,
            } => write!(f, "command failed ({name}, exit {status}): {stderr_tail}"),
            Error::Json(m) => write!(f, "json error: {m}"),
            Error::Parse { what, line } => write!(f, "parse error in {what}: {line}"),
            Error::Missing(m) => write!(f, "missing field or wrong type: {m}"),
            Error::EmptySamples => write!(f, "empty sample list"),
            Error::DigestMismatch {
                phase,
                expected,
                got,
            } => write!(
                f,
                "digest mismatch at {phase}: expected {expected}, got {got}"
            ),
            Error::UnknownRow(r) => write!(f, "unknown row {r}"),
            Error::Usage(m) => write!(f, "usage: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
