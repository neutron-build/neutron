//! The [`Env`] and [`File`] traits: the I/O, clock and randomness surface
//! every engine call goes through (PLAN-v3 §3.2; card A-1a Work 2).
//!
//! Invariants owned here: **E-OPEN**, **E-IO**, **E-DURABLE**,
//! **E-NAMESPACE**, **E-LOCK** and **E-CLOCK/RAND**, stated per item
//! below and restated in the crate root. E-PATH lives in
//! [`crate::path`]. Every implementation — [`crate::MemEnv`],
//! [`crate::StdEnv`], and A-1b's SimEnv — offers the same semantics, so
//! engine code is written once and tested against each backend
//! (S-STORE §2 "All I/O goes through nucleus-env"; §12.5's writer rule;
//! §14.1 "No writer").
//!
//! There are deliberately no `Send`/`Sync` supertraits: engine code adds
//! the bound where it shares an [`Env`] across threads, and a future
//! `OpfsEnv`, whose JS handles are not `Send`, stays possible. All
//! backends in this crate are `Send + Sync` (test `backends_are_send_sync`).

use crate::error::EnvError;
use std::time::Duration;

/// How [`Env::open`] opens a file (E-OPEN).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Opens an existing file read-only. A handle in this mode refuses
    /// `write_at`, `set_len`, `sync_data` and `sync_all` with
    /// [`EnvError::ReadOnly`], before any I/O. It may take a shared
    /// lock: S-STORE §14.1's read-only open locks the page file
    /// "opened read-only by a read-only open".
    Read,
    /// Opens an existing file for reading and writing. A missing file is
    /// [`EnvError::NotFound`].
    ReadWrite,
    /// Creates the file and opens it for reading and writing. An
    /// existing file is [`EnvError::AlreadyExists`], never truncated. A
    /// missing parent directory is [`EnvError::NotFound`].
    CreateNew,
}

/// An open file: positioned I/O on a handle, never a cursor (E-IO).
///
/// A handle has no file-position state: every read and write names its
/// offset, so one handle is safely shared by sequential engine steps
/// without seek/position hazards.
pub trait File {
    /// Reads from `off` into `buf` and returns how many bytes were read.
    ///
    /// E-IO: the read is short only at end-of-file and counts only real
    /// bytes; it returns 0 at or past end-of-file. It never zero-fills
    /// past end-of-file: S-STORE §12.5's short segment must be visible
    /// as a short read, not padded zeros.
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError>;

    /// Writes all of `buf` at `off`, extending the file if needed. A gap
    /// the write leaves below `off` reads back as zeros. On a
    /// [`OpenMode::Read`] handle this is [`EnvError::ReadOnly`], before
    /// any I/O (E-OPEN).
    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError>;

    /// Makes the file's written bytes durable (E-DURABLE: this is the
    /// `fdatasync` reading — data plus the length growth that data
    /// needs). On a [`OpenMode::Read`] handle this is
    /// [`EnvError::ReadOnly`], before any I/O.
    fn sync_data(&self) -> Result<(), EnvError>;

    /// Makes the file's bytes, length and metadata durable (E-DURABLE:
    /// S-STORE §12.5's writer rule — a writer appends to a WAL segment
    /// only after it "`sync_all`'d"). On a [`OpenMode::Read`] handle
    /// this is [`EnvError::ReadOnly`], before any I/O.
    fn sync_all(&self) -> Result<(), EnvError>;

    /// The current file length in bytes.
    fn len(&self) -> Result<u64, EnvError>;

    /// Whether [`File::len`] is 0.
    fn is_empty(&self) -> Result<bool, EnvError> {
        Ok(self.len()? == 0)
    }

    /// Sets the file length: truncates, or zero-extends (E-IO; S-STORE
    /// §4.5 grows the page file with it). A `set_len` change is durable
    /// only after [`File::sync_all`] (E-DURABLE). On a
    /// [`OpenMode::Read`] handle this is [`EnvError::ReadOnly`], before
    /// any I/O.
    fn set_len(&self, len: u64) -> Result<(), EnvError>;

    /// Takes the exclusive advisory lock on this file, non-blocking
    /// (E-LOCK; N11; S-STORE §14.1 "No writer").
    ///
    /// Succeeds only if no other handle holds any lock on the file. A
    /// conflict is [`EnvError::Locked`] at once — this never waits.
    /// Taking a lock creates and writes no file: it is an advisory lock
    /// on the file this handle already has open (S-STORE §14.1, mutant
    /// RO-LOCK-FILE's substrate).
    fn lock_exclusive(&self) -> Result<(), EnvError>;

    /// Takes the shared (read) advisory lock on this file, non-blocking
    /// (E-LOCK; S-STORE §14.1).
    ///
    /// Succeeds only if no other handle holds the exclusive lock;
    /// several handles may hold the shared lock at once (flock
    /// semantics — S-STORE §14.1 leaves the several-readers case to the
    /// lock mode's choice). Works on a [`OpenMode::Read`] handle: an
    /// O_RDONLY descriptor can take a shared lock, which is what the
    /// read-only open needs. A conflict is [`EnvError::Locked`] at
    /// once. Taking a lock creates and writes no file.
    fn lock_shared(&self) -> Result<(), EnvError>;

    /// Releases this handle's lock, of either mode, and is
    /// [`Ok(())`][Ok] when the handle holds none. Dropping the handle
    /// releases its lock (E-LOCK).
    fn unlock(&self) -> Result<(), EnvError>;
}

/// The I/O, clock and randomness surface every engine call goes through
/// (PLAN-v3 §3.2).
///
/// E-NAMESPACE, stated once here and per item below: `rename` moves a
/// file within one directory and never replaces; `remove` deletes a
/// file; `exists` covers files and directories; `list_dir` returns
/// sorted names; `create_dir` creates one directory whose parent must
/// exist.
pub trait Env {
    /// The handle type this backend hands out.
    type File: File;

    /// Opens `path` per `mode` (E-OPEN). A missing file, or a missing
    /// parent directory, is [`EnvError::NotFound`]. `CreateNew` on an
    /// existing file is [`EnvError::AlreadyExists`].
    fn open(&self, path: &str, mode: OpenMode) -> Result<Self::File, EnvError>;

    /// Creates one directory whose parent exists (E-NAMESPACE; S-STORE
    /// §2: the `wal/` directory). An existing file or directory of that
    /// name is [`EnvError::AlreadyExists`]; a missing parent is
    /// [`EnvError::NotFound`].
    fn create_dir(&self, path: &str) -> Result<(), EnvError>;

    /// Makes a directory's entries durable (E-DURABLE; S-STORE §12.5:
    /// the segment's "`wal/` directory entry synced through `Env`").
    ///
    /// A `create_dir`, a `CreateNew`, a `rename` or a `remove` is
    /// durable only after this returns for its parent directory, and
    /// `sync_dir` makes that directory's entries durable and nothing
    /// else. `""` names the root.
    fn sync_dir(&self, path: &str) -> Result<(), EnvError>;

    /// Moves a file **within one directory** (E-NAMESPACE; S-STORE §12.1
    /// recycles segments by rename inside `wal/`).
    ///
    /// A rename across directories is [`EnvError::InvalidPath`]. An
    /// existing target is [`EnvError::AlreadyExists`], never replaced —
    /// a silent replace would hide a store bug that a refusal catches.
    /// A missing source is [`EnvError::NotFound`].
    fn rename(&self, from: &str, to: &str) -> Result<(), EnvError>;

    /// Deletes a file (E-NAMESPACE; C-T0 v8 §2.1's temp files are
    /// "deleted at open"). Removing a directory is not part of this
    /// trait. A missing file is [`EnvError::NotFound`].
    fn remove(&self, path: &str) -> Result<(), EnvError>;

    /// Whether `path` names a file or a directory. A missing path is
    /// `Ok(false)`, never an error (E-NAMESPACE).
    fn exists(&self, path: &str) -> Result<bool, EnvError>;

    /// The names of the files and directories directly in `path`,
    /// sorted bytewise (E-NAMESPACE). `""` names the root. A missing
    /// directory is [`EnvError::NotFound`].
    fn list_dir(&self, path: &str) -> Result<Vec<String>, EnvError>;

    /// A monotonic duration since this `Env` was created (E-CLOCK/RAND).
    /// Engine code reads the clock only here, never the platform clock
    /// (PLAN-v3 §3.2).
    fn now(&self) -> Duration;

    /// Returns once [`Env::now`] has advanced by at least `d`
    /// (E-CLOCK/RAND). MemEnv advances its virtual clock by exactly `d`
    /// and yields; StdEnv sleeps the calling thread.
    fn sleep(&self, d: Duration);

    /// A 64-bit random value (E-CLOCK/RAND). Not cryptographic: it
    /// serves ids such as S-STORE §2.1's `creation_uuid`. MemEnv is
    /// seeded, so test sequences replay.
    fn rand(&self) -> u64;
}
