//! The [`Env`] and [`File`] traits: the I/O, clock and randomness surface
//! every engine call goes through (PLAN-v3 §3.2; card A-1a Work 2).
//!
//! Invariants owned here: **E-OPEN**, **E-IO**, **E-DURABLE**,
//! **E-NAMESPACE**, **E-LOCK**, **E-HANDLE**, **E-KIND** and
//! **E-CLOCK/RAND**, stated per item below and restated in the crate
//! root. E-PATH lives in [`crate::path`]. Every implementation — [`crate::MemEnv`],
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
    /// missing parent directory is [`EnvError::NotFound`]. On a name
    /// whose old file an open handle still holds, this makes a
    /// **different** file, which that handle never reaches (E-HANDLE).
    CreateNew,
}

/// An open file: positioned I/O on a handle, never a cursor (E-IO).
///
/// A handle has no file-position state: every read and write names its
/// offset, so one handle is safely shared by sequential engine steps
/// without seek/position hazards.
///
/// **E-HANDLE:** a handle names **the file it opened, not the path** it
/// opened it by. After an [`Env::rename`] the handle still reaches that
/// file, now under its new name. After an [`Env::remove`] it still
/// reaches the file, which no name reaches any more, until the handle
/// is dropped; every `File` method works on it. A later
/// [`OpenMode::CreateNew`] of the same name makes a **different** file,
/// which the old handle never reaches. A handle's errors name the path
/// it was opened by, even after a rename: the path is for the message
/// only — the handle never resolves it again.
///
/// Why identity, not refusal of a `rename`/`remove` under an open
/// handle: E4's SimDisk keeps an inode table, and its `rename` and
/// `unlink` change only the directory bindings, so a handle reaches its
/// file across a rename and after an unlink; E4's WAL recycling writes
/// through the `FileId` it held before the rename (S-STORE §12.1
/// recycles segments the same way); E-LOCK's lock lives "on the file
/// itself"; and StdEnv already has POSIX open-file semantics. S-STORE
/// (§12.1, §12.5, §14.1) leaves the case open. A future `OpfsEnv`
/// meets this rule or names its departure in its own card.
pub trait File {
    /// Reads from `off` into `buf` and returns how many bytes were read.
    ///
    /// E-IO: the read is short only at end-of-file and counts only real
    /// bytes; it returns 0 at or past end-of-file — including offsets
    /// no platform file can reach, past `i64::MAX` and past `usize` on
    /// a 32-bit target. It never zero-fills past end-of-file: S-STORE
    /// §12.5's short segment must be visible as a short read, not
    /// padded zeros.
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError>;

    /// Writes all of `buf` at `off`, extending the file if needed. A gap
    /// the write leaves below `off` reads back as zeros. A zero-length
    /// write changes nothing at any offset — no extension — and returns
    /// [`Ok(())`][Ok] (E-IO). On a [`OpenMode::Read`] handle this is
    /// [`EnvError::ReadOnly`], before any I/O (E-OPEN: the refusal
    /// comes before the empty-write no-op).
    ///
    /// E-IO's length bound (A-1b-0-F2): a write whose `off + buf.len()`
    /// exceeds `i64::MAX` — u64 overflow included — is
    /// [`EnvError::Io`] with `kind: InvalidInput` on every backend. It
    /// is checked before any I/O, after the `Read` refusal and after
    /// the empty-write no-op, so an empty write stays `Ok` at any
    /// offset. Below that bound, a backend that cannot hold the length
    /// refuses with `Io`: `FileTooLarge` (a platform file-size limit,
    /// or SimEnv's cap) or `OutOfMemory` (MemEnv's failed allocation).
    /// No backend panics on a length.
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
    ///
    /// E-IO's length bound (A-1b-0-F2): a `set_len` whose `len`
    /// exceeds `i64::MAX` is [`EnvError::Io`] with `kind:
    /// InvalidInput` on every backend, checked before any I/O and
    /// after the `Read` refusal. Below that bound, a backend that
    /// cannot hold the length refuses with `Io`: `FileTooLarge` (a
    /// platform limit, or SimEnv's cap) or `OutOfMemory` (MemEnv's
    /// failed allocation). No backend panics on a length.
    fn set_len(&self, len: u64) -> Result<(), EnvError>;

    /// Takes the exclusive advisory lock on this file, non-blocking
    /// (E-LOCK; N11; S-STORE §14.1 "No writer").
    ///
    /// Succeeds only if no other handle holds any lock on the file. A
    /// conflict is [`EnvError::Locked`] at once — this never waits.
    /// Taking a lock creates and writes no file: it is an advisory lock
    /// on the file this handle already has open (S-STORE §14.1, mutant
    /// RO-LOCK-FILE's substrate). The lock lives on the file, so it
    /// moves with an [`Env::rename`] and is released by
    /// [`File::unlock`] or by dropping the holding handle, whatever
    /// name the file has by then.
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
///
/// E-KIND, stated once here and per item below: a file operation on a
/// directory and a directory operation on a file are refused the same
/// way on every backend — a directory where a file is required is
/// [`EnvError::Io`] with `kind: IsADirectory`, and a file where a
/// directory is required is `Io` with `kind: NotADirectory`. A path
/// under a file names nothing: `exists` is `Ok(false)` and every other
/// method treats it as missing ([`EnvError::NotFound`]). Each refusal's
/// `path` names the path that refusal is about.
pub trait Env {
    /// The handle type this backend hands out.
    type File: File;

    /// Opens `path` per `mode` (E-OPEN). A missing file, a missing
    /// parent directory, or a path under a file, is
    /// [`EnvError::NotFound`]. `CreateNew` on an existing name — file
    /// or directory — is [`EnvError::AlreadyExists`]. `Read` and
    /// `ReadWrite` of a directory are `Io` with `kind: IsADirectory`
    /// (E-KIND).
    fn open(&self, path: &str, mode: OpenMode) -> Result<Self::File, EnvError>;

    /// Creates one directory whose parent exists (E-NAMESPACE; S-STORE
    /// §2: the `wal/` directory). An existing file or directory of that
    /// name is [`EnvError::AlreadyExists`]; a missing parent, or a path
    /// under a file, is [`EnvError::NotFound`] (E-KIND).
    fn create_dir(&self, path: &str) -> Result<(), EnvError>;

    /// Makes a directory's entries durable (E-DURABLE; S-STORE §12.5:
    /// the segment's "`wal/` directory entry synced through `Env`").
    ///
    /// A `create_dir`, a `CreateNew`, a `rename` or a `remove` is
    /// durable only after this returns for its parent directory, and
    /// `sync_dir` makes that directory's entries durable and nothing
    /// else. `""` names the root. A file is refused with `Io` `kind:
    /// NotADirectory`, and a path under a file with
    /// [`EnvError::NotFound`] (E-KIND) — a caller that passes
    /// `wal/<seg>.log` where it means `wal` is told so; it never gets
    /// `Ok` with no directory sync.
    fn sync_dir(&self, path: &str) -> Result<(), EnvError>;

    /// Moves a file **within one directory** (E-NAMESPACE; S-STORE §12.1
    /// recycles segments by rename inside `wal/`).
    ///
    /// A rename across directories is [`EnvError::InvalidPath`]. An
    /// existing target is [`EnvError::AlreadyExists`], never replaced —
    /// a silent replace would hide a store bug that a refusal catches.
    /// The checks run in order (E-KIND): E-PATH, cross-directory, the
    /// source — [`EnvError::NotFound`] if it is missing or under a
    /// file, `Io` with `kind: IsADirectory` if it is a directory,
    /// because `rename` moves a file — then the target. The file any
    /// open handle holds moves with the rename: the handle keeps
    /// reaching it under the new name, and its lock moves with it
    /// (E-HANDLE, E-LOCK).
    fn rename(&self, from: &str, to: &str) -> Result<(), EnvError>;

    /// Deletes a file (E-NAMESPACE; C-T0 v8 §2.1's temp files are
    /// "deleted at open"). Removing a directory is not part of this
    /// trait: it is `Io` with `kind: IsADirectory` (E-KIND). A missing
    /// file, or a path under a file, is [`EnvError::NotFound`]. A
    /// handle that holds the removed file keeps reaching it, unnamed,
    /// until the handle is dropped (E-HANDLE).
    fn remove(&self, path: &str) -> Result<(), EnvError>;

    /// Whether `path` names a file or a directory. A missing path — or
    /// a path under a file, which names nothing — is `Ok(false)`, never
    /// an error (E-NAMESPACE, E-KIND).
    fn exists(&self, path: &str) -> Result<bool, EnvError>;

    /// The names of the files and directories directly in `path`,
    /// sorted bytewise (E-NAMESPACE). `""` names the root. A missing
    /// directory is [`EnvError::NotFound`]. A file is `Io` with `kind:
    /// NotADirectory`, and a path under a file is
    /// [`EnvError::NotFound`] (E-KIND).
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
