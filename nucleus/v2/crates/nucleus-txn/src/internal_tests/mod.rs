//! C-T1a's tests for the status table, registry, removal, truncation, boot
//! and the read path, moved inside the crate by C-T1b's rework: they set up
//! txn state directly (`set_committed`, `advance_visible_ts`, …), which is
//! exactly what the rework made crate-private, and they predate the commit
//! pipeline that is now the public way to drive state. The files are
//! otherwise verbatim; C-T1b's own tests stay in `tests/` and use only the
//! public API.
#![cfg(test)]

mod boot;
mod concurrency;
mod read_path;
mod registry;
mod removal;
mod truncation;
