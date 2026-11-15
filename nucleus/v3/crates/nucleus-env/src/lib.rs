//! Environment abstraction for Nucleus v3 (PLAN-v3 §3.2).
//!
//! Every file, clock and randomness access made by the engine goes
//! through this crate, so the same engine runs on a real filesystem, in
//! memory, in the deterministic simulator and in the browser. S-STORE
//! §2: "All I/O goes through `nucleus-env`"; §14.1: the read-only open
//! takes its file lock "through `Env`". Normative design:
//! `docs/S-STORE.md` (card S-1); this crate is card A-1a.
//!
//! # Backends
//!
//! - [`StdEnv`]: the real filesystem, pread/pwrite, advisory locks.
//!   Unix only (`#[cfg(unix)]`); on other targets the crate builds
//!   without it (Windows is an open point from A-1a's report).
//! - [`MemEnv`]: an in-memory backend for unit tests, deterministic and
//!   wasm-safe (no filesystem, no platform clock, no OS randomness).
//! - SimEnv (A-1b): MemEnv plus a seeded scheduler whose power cut
//!   drops, tears, loses or misdirects unsynced writes.
//!
//! # The contract (card A-1a, Work 2)
//!
//! - **E-PATH** — a path is relative, with `/` separators; each
//!   component is non-empty and not `.` or `..`; no component contains
//!   `\` or NUL; no leading `/`. `""` names the root and only
//!   `sync_dir`/`list_dir` accept it. A violation is
//!   [`EnvError::InvalidPath`] on every backend, before any I/O. One
//!   namespace for all backends, OPFS included.
//! - **E-OPEN** — `Read` opens read-only, `ReadWrite` opens read-write,
//!   `CreateNew` creates and refuses an existing file
//!   ([`EnvError::AlreadyExists`]). A missing file or parent is
//!   [`EnvError::NotFound`]. A `Read` handle refuses `write_at`,
//!   `set_len`, `sync_data` and `sync_all` with [`EnvError::ReadOnly`],
//!   before any I/O (S-STORE §14.1 "What it refuses").
//! - **E-IO** — `read_at` is short only at end-of-file, 0 at or past
//!   it, and never zero-fills past it (§12.5's short segment must be
//!   visible); `write_at` writes all of `buf`, extending the file;
//!   `set_len` truncates or zero-extends; `len` is the current length.
//! - **E-DURABLE** — written bytes are durable only after a `sync_data`
//!   or `sync_all` of that file; a `set_len` change only after
//!   `sync_all`; a `create_dir`, `CreateNew`, `rename` or `remove` only
//!   after `sync_dir` of its parent; `sync_dir` makes that directory's
//!   entries durable and nothing else. This is §12.5's writer rule in
//!   Env terms: the segment "`sync_all`'d and its `wal/` directory
//!   entry synced through `Env`". StdEnv provides it; MemEnv has no
//!   crash, so its syncs are no-ops; A-1b's SimEnv makes the losses
//!   happen.
//! - **E-NAMESPACE** — `rename` moves a file within one directory
//!   (cross-directory is [`EnvError::InvalidPath`]) and never replaces
//!   an existing target ([`EnvError::AlreadyExists`]); `remove` deletes
//!   a file; `exists` is true for files and directories; `list_dir`
//!   returns bytewise-sorted names; `create_dir` creates one directory
//!   whose parent exists.
//! - **E-LOCK** (N11; S-STORE §14.1 "No writer") — a lock belongs to an
//!   open handle, is advisory, on the file itself, and taking it
//!   creates and writes no file. `lock_exclusive` succeeds only if no
//!   other handle holds any lock; `lock_shared` only if no other handle
//!   holds the exclusive lock; several handles may hold the shared lock
//!   (flock semantics). Both are non-blocking: a conflict is
//!   [`EnvError::Locked`] at once. `lock_shared` works on a `Read`
//!   handle. `unlock` is `Ok` when the handle holds none; dropping a
//!   handle releases its lock.
//! - **E-CLOCK/RAND** — `now` is monotonic since the backend was
//!   created; `sleep(d)` returns once `now` has advanced by at least
//!   `d`; `rand` is a 64-bit value, not cryptographic, for ids such as
//!   S-STORE §2.1's `creation_uuid`. Engine code calls these, never
//!   platform time (§3.2's rule that no engine code touches the
//!   platform's filesystem or time APIs directly; only `std_env.rs`
//!   may name them, and that rule's lint is not this card's).
#![deny(clippy::indexing_slicing)]

mod env;
mod error;
mod path;

pub use env::{Env, File, OpenMode};
pub use error::EnvError;
