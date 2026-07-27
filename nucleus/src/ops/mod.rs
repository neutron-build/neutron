//! Operational governance: service admission mode, disk watermarks, graceful
//! shutdown drain, and secret redaction.
//!
//! These are the pieces an operator needs to keep a long-running database
//! bounded and safe:
//!
//! * [`mode`] — the process-wide read-write / degraded read-only admission
//!   gate. Every write statement passes through it.
//! * [`disk`] — filesystem watermarks that drive the gate into read-only
//!   before the data directory fills, instead of discovering ENOSPC halfway
//!   through a write.
//! * [`shutdown`] — a drain coordinator that makes "stop accepting, finish
//!   in-flight work, then persist" an ordered, observable sequence rather
//!   than a race between a signal handler and the accept loop.
//! * [`redact`] — one place that decides what a secret looks like, so
//!   passwords and keys never reach logs or status output.

pub mod disk;
pub mod mode;
pub mod redact;
pub mod shutdown;

pub use disk::{DiskGuard, DiskLevel, DiskObservation, DiskWatermarks, SpaceInfo, SpaceProbe};
pub use mode::{DegradeReason, ServiceMode, ServiceState};
pub use redact::{
    REDACTED, is_secret_key, redact_connection_string, redact_line, redact_sql, redact_value,
};
pub use shutdown::{DrainOutcome, InflightGuard, ShutdownCoordinator};
