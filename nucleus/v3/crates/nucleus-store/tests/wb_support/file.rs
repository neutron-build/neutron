//! `WbFile` (card A-3c, Work 13.1): a `nucleus_env::File` over a `MemFile`
//! that logs every read and every page write with the kind, id and epoch the
//! written header carries, lets a test check every page write before it is
//! made (I-NO-INPLACE's write hook), and can fail one write or every
//! `set_len`.
//!
//! Its own locks are std's, held only inside one call and with no facade
//! operation inside, so a model execution sees each call as one step. The
//! on-write check never panics and never fails the write: its `Err` is
//! appended to a violations list the test reads afterwards.

use std::io::ErrorKind;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use nucleus_env::{Env, EnvError, File, MemEnv, MemFile, OpenMode};

use super::PS;

/// A page write the file was asked for: where, how long, and what its
/// header says (kind at byte 4, id at byte 8, epoch at byte 16, read as raw
/// little-endian, independent of `page::read_header`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Write {
    /// The byte offset.
    pub offset: u64,
    /// The length in bytes.
    pub len: usize,
    /// The header's kind byte.
    pub kind: u8,
    /// The header's id.
    pub id: u64,
    /// The header's epoch.
    pub epoch: u32,
}

/// The check a test sets on every page write: called with the slot
/// (`offset / PS`) and the bytes before the write is delegated.
pub type OnWrite = Arc<dyn Fn(u64, &[u8]) -> Result<(), String> + Send + Sync>;

/// The page file of every fixture.
pub struct WbFile {
    inner: MemFile,
    reads: Mutex<Vec<(u64, usize)>>,
    writes: Mutex<Vec<Write>>,
    on_write: Mutex<Option<OnWrite>>,
    violations: Mutex<Vec<String>>,
    fail_write_at: Mutex<Option<u64>>,
    fail_set_len: Mutex<bool>,
}

/// Lock a test mutex, recovering from poison: a panicking test thread must
/// not turn a fixture's lock into a second panic.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The injected error of a failed write or `set_len`.
fn injected(op: &'static str) -> EnvError {
    EnvError::Io {
        op,
        path: "data".into(),
        kind: ErrorKind::Other,
        detail: "injected".into(),
    }
}

/// A byte of `b`, or 0 past its end.
fn at(b: &[u8], i: usize) -> u8 {
    b.get(i).copied().unwrap_or(0)
}

/// The little-endian `u32` at `i`.
fn le_u32(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes([at(b, i), at(b, i + 1), at(b, i + 2), at(b, i + 3)])
}

/// The little-endian `u64` at `i`.
fn le_u64(b: &[u8], i: usize) -> u64 {
    let mut bytes = [0u8; 8];
    for (k, slot) in bytes.iter_mut().enumerate() {
        *slot = at(b, i + k);
    }
    u64::from_le_bytes(bytes)
}

impl WbFile {
    /// A new, empty page file `data` in a fresh `MemEnv`.
    pub fn new() -> Result<WbFile, EnvError> {
        Ok(WbFile {
            inner: MemEnv::new(1).open("data", OpenMode::CreateNew)?,
            reads: Mutex::new(Vec::new()),
            writes: Mutex::new(Vec::new()),
            on_write: Mutex::new(None),
            violations: Mutex::new(Vec::new()),
            fail_write_at: Mutex::new(None),
            fail_set_len: Mutex::new(false),
        })
    }

    /// Install or remove the on-write check.
    pub fn set_on_write(&self, check: Option<OnWrite>) {
        *lock(&self.on_write) = check;
    }

    /// Make the write at byte offset `off` fail with the injected error;
    /// `None` stops it. A failed write is not logged.
    pub fn fail_write_at(&self, off: Option<u64>) {
        *lock(&self.fail_write_at) = off;
    }

    /// Make every later `set_len` fail with the injected error.
    pub fn fail_set_len(&self, yes: bool) {
        *lock(&self.fail_set_len) = yes;
    }

    /// Every page write made, in order.
    pub fn writes(&self) -> Vec<Write> {
        lock(&self.writes).clone()
    }

    /// Every `read_at`, as `(offset, length)`, in order.
    pub fn reads(&self) -> Vec<(u64, usize)> {
        lock(&self.reads).clone()
    }

    /// The on-write check's violations, in order.
    pub fn violations(&self) -> Vec<String> {
        lock(&self.violations).clone()
    }

    /// Write `bytes` at `off` through the file itself, bypassing the log
    /// and the check: a test's own damage or setup, not a page write.
    pub fn poke(&self, off: u64, bytes: &[u8]) -> Result<(), EnvError> {
        self.inner.write_at(off, bytes)
    }

    /// The file's bytes at `off`, `len` long, read without a log entry.
    pub fn peek(&self, off: u64, len: usize) -> Result<Vec<u8>, EnvError> {
        let mut buf = vec![0u8; len];
        self.inner.read_at(off, &mut buf)?;
        Ok(buf)
    }
}

impl File for WbFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError> {
        lock(&self.reads).push((off, buf.len()));
        self.inner.read_at(off, buf)
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        if *lock(&self.fail_write_at) == Some(off) {
            return Err(injected("write_at"));
        }
        lock(&self.writes).push(Write {
            offset: off,
            len: buf.len(),
            kind: at(buf, 4),
            id: le_u64(buf, 8),
            epoch: le_u32(buf, 16),
        });
        let check = lock(&self.on_write).clone();
        if let Some(check) = check
            && let Err(violation) = check(off / u64::from(PS), buf)
        {
            lock(&self.violations).push(violation);
        }
        self.inner.write_at(off, buf)
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        self.inner.sync_data()
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        self.inner.sync_all()
    }

    fn len(&self) -> Result<u64, EnvError> {
        self.inner.len()
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        if *lock(&self.fail_set_len) {
            return Err(injected("set_len"));
        }
        self.inner.set_len(len)
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        self.inner.lock_exclusive()
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        self.inner.lock_shared()
    }

    fn unlock(&self) -> Result<(), EnvError> {
        self.inner.unlock()
    }
}
