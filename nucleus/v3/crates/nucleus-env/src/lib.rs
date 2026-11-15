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
//! SimEnv (card A-1b, `--features sim`) is E4's simulator, copied with
//! provenance from `spikes/e4-recovery` at commit `48ac9920` — the
//! baton scheduler, `SimDisk` with its fault model, and the self-tests
//! of card X-5's rework pins — behind this crate's [`Env`] and
//! [`File`] traits (`sim::SimEnv`). The spike stays E4's evidence
//! record. The feature is off by default: the simulator runs OS
//! threads and keeps E4's opt-in diagnostic log, so no wasm32 or
//! production build compiles it; tests enable it through the crate's
//! self dev-dependency, as `test-hooks` does elsewhere.
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
//!   it — including offsets no platform file can reach, past `i64::MAX`
//!   and past `usize` on a 32-bit target — and never zero-fills past it
//!   (§12.5's short segment must be visible); `write_at` writes all of
//!   `buf`, extending the file, and a zero-length write changes nothing
//!   at any offset and returns `Ok` (after the `Read` refusal);
//!   `set_len` truncates or zero-extends; `len` is the current length.
//!   The length bound (A-1b-0-F2): a `write_at` whose `off + buf.len()`
//!   exceeds `i64::MAX` — u64 overflow included — or a `set_len` whose
//!   `len` exceeds it, is [`EnvError::Io`] `{ kind: InvalidInput }` on
//!   every backend, checked before any I/O, after the `Read` refusal
//!   and after the empty-write no-op, so an empty write stays `Ok` at
//!   any offset. Below the bound, a backend that cannot hold the
//!   length refuses with `Io`: `FileTooLarge` (a platform file-size
//!   limit, or SimEnv's cap) or `OutOfMemory` (MemEnv's failed
//!   allocation); no backend panics on a length.
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
//! - **E-HANDLE** — a handle names the file it opened, not the path it
//!   opened it by. A `rename` rebinds the name; the handle keeps
//!   reaching the file under its new name. A `remove` unbinds the name;
//!   the handle keeps reaching the now-unnamed file, and every `File`
//!   method works on it, until the handle is dropped. A later
//!   `CreateNew` of the same name makes a different file, which the old
//!   handle never reaches. A handle's errors name its open-time path,
//!   for the message only. Why: E4's SimDisk `FileId` model and its
//!   WAL recycling, which writes through the id it held before the
//!   rename (S-STORE §12.1); E-LOCK's "on the file itself"; POSIX
//!   open-file semantics, which StdEnv already has. S-STORE leaves the
//!   case open. A future `OpfsEnv` meets this rule or names its
//!   departure in its own card.
//! - **E-KIND** — file/directory mismatches are one behaviour on every
//!   backend. A file operation on a directory: `open` (`Read`,
//!   `ReadWrite`) is [`EnvError::Io`] `{ kind: IsADirectory }`; `open`
//!   (`CreateNew`) is [`EnvError::AlreadyExists`], because the name is
//!   taken; `remove` and `rename`'s source are
//!   `Io { kind: IsADirectory }`. A directory operation on a file:
//!   `sync_dir` and `list_dir` are `Io { kind: NotADirectory }`;
//!   `create_dir` of a file's name is `AlreadyExists`. A path under a
//!   file names nothing: `exists` is `Ok(false)` and every other method
//!   treats it as missing (`NotFound`). `rename` checks in order:
//!   E-PATH, cross-directory, the source (`NotFound`, or
//!   `IsADirectory`), then the target (`AlreadyExists`). Each refusal's
//!   `path` names the path the refusal is about.
//! - **E-LOCK** (N11; S-STORE §14.1 "No writer") — a lock belongs to an
//!   open handle, is advisory, on the file itself, and taking it
//!   creates and writes no file. The lock lives on the file: it moves
//!   with a `rename` and is released by `unlock` or by dropping the
//!   holding handle, whatever name the file has by then.
//!   `lock_exclusive` succeeds only if no other handle holds any lock;
//!   `lock_shared` only if no other handle holds the exclusive lock;
//!   several handles may hold the shared lock (flock semantics). Both
//!   are non-blocking: a conflict is [`EnvError::Locked`] at once.
//!   `lock_shared` works on a `Read` handle. `unlock` is `Ok` when the
//!   handle holds none; dropping a handle releases its lock. The
//!   same-handle rule (A-1b-0-F3): on a handle that already holds a
//!   lock, taking the mode it holds again is `Ok` and changes nothing;
//!   taking the other mode is [`EnvError::Locked`], and the handle
//!   keeps the lock it holds — no backend converts a lock, because
//!   flock conversion is not atomic and a failed shared-to-exclusive
//!   upgrade on Linux drops the shared lock.
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
mod mem;
mod path;
#[cfg(feature = "sim")]
pub mod rng;
#[cfg(feature = "sim")]
pub mod sim;
#[cfg(unix)]
mod std_env;

pub use env::{Env, File, OpenMode};
pub use error::EnvError;
pub use mem::{MemEnv, MemFile};
#[cfg(unix)]
pub use std_env::{StdEnv, StdFile};
