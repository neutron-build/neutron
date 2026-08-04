//! Spill-to-disk harness for blocking query operators (Tranche B / B2).
//!
//! Phase 3 of the streaming refactor makes sort / hash-aggregate / hash-join /
//! DISTINCT / COPY spill their intermediate results to disk when they cross the
//! memory reservation that today returns `MemoryExceeded`. This module is the
//! **safety substrate** those operators build on, landed ahead of them so the
//! rules are fixed before the first external algorithm is written — a wrong
//! default here (a leaked plaintext copy of encrypted rows, an unbounded spill
//! that fills the volume, an orphaned file after a crash) is a data-exposure or
//! availability bug, not a perf miss. See `_internal/SPILL_SAFETY_DESIGN.md`.
//!
//! Guarantees provided here, each covered by a test:
//! - **Serialization** reuses the one canonical `Value` codec
//!   ([`crate::storage::value_codec`]) — no second on-disk format to drift.
//! - **Lifecycle**: a spill file is deleted on normal finish, on error, on drop
//!   (cancel), and — for files a crash leaves behind — by [`SpillManager::sweep_orphans`]
//!   at startup. Nothing survives a clean shutdown, so any file found at startup
//!   is by definition an orphan.
//! - **Accounting**: every stored byte is reserved against a [`DiskBudget`];
//!   crossing it is a clean [`SpillError::DiskBudgetExceeded`], never an ENOSPC
//!   crash.
//! - **Sensitive data**: rows from at-rest-encrypted tables reach the executor
//!   as plaintext (encryption is transparent at the storage layer). Spilling
//!   them plaintext would write encrypted data to disk in the clear, so a
//!   [`Sensitivity::Sensitive`] run **fails closed** unless the manager holds an
//!   encryptor, and when it does the spill stream is encrypted with the same
//!   AES-256-GCM path as pages.
//!
//! No operator spills yet — this is the mechanism only. Phase 3 wires the
//! `MemoryExceeded` trigger to `create_run` and maps [`SpillError`] onto
//! `ExecError` at that boundary.
//!
//! Most of this surface has no non-test caller until Phase 3 (the startup orphan
//! sweep is the one live use), so the module carries a scoped `dead_code` allow —
//! the same groundwork convention as `row_batch.rs`. It lifts as operators adopt
//! the harness.
#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::encryption::PageEncryptor;
use crate::storage::value_codec;
use crate::types::Row;

/// Cap on a single stored block's length, guarding the reader against a corrupt
/// length prefix turning into an allocation bomb. Far above any real batch
/// (millions of wide rows) yet bounded.
const MAX_BLOCK_LEN: u32 = 1 << 30; // 1 GiB

/// Errors from the spill path. Deliberately its **own** type, not `ExecError`:
/// no operator spills in B2, so nothing maps this onto the wire taxonomy yet.
/// Phase 3 performs that mapping (`DiskBudgetExceeded` → 53100 disk_full,
/// `EncryptionRequired` → fall back to `MemoryExceeded`) at the call site.
#[derive(Debug)]
pub enum SpillError {
    /// A write would push spilled bytes past the disk budget.
    DiskBudgetExceeded {
        requested: u64,
        used: u64,
        limit: u64,
    },
    /// A sensitive run was requested but the manager holds no encryptor, so a
    /// plaintext spill would leak at-rest-encrypted data. Fail closed.
    EncryptionRequired,
    /// A spilled block could not be decrypted (wrong key / tampering).
    Decryption,
    /// A spill file was truncated or its framing was corrupt.
    Corrupt(String),
    /// Underlying filesystem error.
    Io(std::io::Error),
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::DiskBudgetExceeded {
                requested,
                used,
                limit,
            } => write!(
                f,
                "spill disk budget exceeded: requested {requested} bytes, {used} in use, limit {limit}"
            ),
            SpillError::EncryptionRequired => write!(
                f,
                "refusing to spill sensitive rows without an encryptor (fail-closed)"
            ),
            SpillError::Decryption => write!(f, "spill block decryption failed"),
            SpillError::Corrupt(msg) => write!(f, "corrupt spill file: {msg}"),
            SpillError::Io(e) => write!(f, "spill io error: {e}"),
        }
    }
}

impl std::error::Error for SpillError {}

impl From<std::io::Error> for SpillError {
    fn from(e: std::io::Error) -> Self {
        SpillError::Io(e)
    }
}

/// Whether a run may contain rows that must never touch disk in the clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sensitivity {
    /// Ordinary rows — spill plaintext.
    Plain,
    /// Rows sourced from an at-rest-encrypted table — spill only encrypted, and
    /// fail closed if no encryptor is configured.
    Sensitive,
}

// ============================================================================
// DiskBudget — spill-byte accounting (mirrors allocator::MemoryBudget)
// ============================================================================

/// Atomic accounting for bytes currently occupied by live spill files, with a
/// hard ceiling. Crossing the ceiling is a clean error so spill can never fill
/// the volume out from under the rest of the process.
#[derive(Debug)]
pub struct DiskBudget {
    used_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    limit_bytes: u64,
}

impl DiskBudget {
    /// A budget with a hard ceiling of `limit_bytes`.
    pub fn new(limit_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            limit_bytes,
        })
    }

    /// An effectively unbounded budget (tests, or configurations that trust the
    /// volume). Still tracked, just never denied.
    pub fn unlimited() -> Arc<Self> {
        Self::new(u64::MAX)
    }

    fn try_add(&self, bytes: u64) -> Result<(), SpillError> {
        // CAS loop so concurrent operators can't both pass the check and
        // over-commit past the ceiling.
        loop {
            let current = self.used_bytes.load(Ordering::Acquire);
            let next = current.saturating_add(bytes);
            if next > self.limit_bytes {
                return Err(SpillError::DiskBudgetExceeded {
                    requested: bytes,
                    used: current,
                    limit: self.limit_bytes,
                });
            }
            if self
                .used_bytes
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.peak_bytes.fetch_max(next, Ordering::Relaxed);
                return Ok(());
            }
        }
    }

    fn sub(&self, bytes: u64) {
        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Bytes currently occupied by live spill files.
    pub fn used(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// High-water mark of spilled bytes since creation.
    pub fn peak(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }
}

/// RAII claim on `DiskBudget` bytes. Grows as a run is written and releases the
/// whole claim when dropped — so budget is freed exactly when the spill file it
/// accounts for is deleted.
struct DiskReservation {
    budget: Arc<DiskBudget>,
    bytes: u64,
}

impl DiskReservation {
    fn grow(&mut self, extra: u64) -> Result<(), SpillError> {
        self.budget.try_add(extra)?;
        self.bytes += extra;
        Ok(())
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.sub(self.bytes);
        }
    }
}

// ============================================================================
// SpillFile — the deletion + reservation guard shared by writer and reader
// ============================================================================

/// Owns a spill file's on-disk lifetime: deletes the file and releases its disk
/// reservation on drop. Held by the writer while producing and handed to the
/// reader on `finish`, so the file lives exactly as long as something is using
/// it and is reclaimed on every exit path (finish, error, cancel, panic).
struct SpillFile {
    path: PathBuf,
    reservation: DiskReservation,
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        // Best-effort: a leaked file is swept at next startup, so a failed
        // unlink here is not fatal. The reservation releases when it drops next.
        let _ = std::fs::remove_file(&self.path);
    }
}

// ============================================================================
// SpillManager — spill directory owner + orphan sweeper
// ============================================================================

/// Prefix every spill file shares, so the orphan sweep can recognize its own
/// files and never touch anything else that lands in the directory.
const SPILL_PREFIX: &str = "spill-";
const SPILL_SUFFIX: &str = ".tmp";

/// Owns the spill directory, the shared [`DiskBudget`], and the optional
/// encryptor. Hands out [`SpillWriter`]s for individual runs.
pub struct SpillManager {
    dir: PathBuf,
    budget: Arc<DiskBudget>,
    encryptor: Option<PageEncryptor>,
    seq: AtomicU64,
}

impl SpillManager {
    /// Open (creating if needed) the spill directory under `dir`, with a disk
    /// ceiling of `budget_limit` bytes and an optional encryptor for sensitive
    /// runs. Does **not** sweep — call [`SpillManager::sweep_orphans`] once at
    /// startup, before any query can spill.
    pub fn new(
        dir: impl AsRef<Path>,
        budget_limit: u64,
        encryptor: Option<PageEncryptor>,
    ) -> Result<Self, SpillError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            budget: DiskBudget::new(budget_limit),
            encryptor,
            seq: AtomicU64::new(0),
        })
    }

    /// The shared disk budget (for metrics / SHOW-style introspection).
    pub fn budget(&self) -> &Arc<DiskBudget> {
        &self.budget
    }

    /// The spill directory this manager owns.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Delete every spill file left in the directory. Since spill files never
    /// survive a clean shutdown (writer/reader guards unlink on drop), any file
    /// matching the spill naming scheme at startup is an orphan from a crashed
    /// process. Returns the number reclaimed. Foreign files are left untouched.
    pub fn sweep_orphans(&self) -> Result<usize, SpillError> {
        let mut reclaimed = 0;
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(SPILL_PREFIX)
                && name.ends_with(SPILL_SUFFIX)
                && std::fs::remove_file(entry.path()).is_ok()
            {
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }

    /// Begin a new spill run. `owner_id` (typically a query/txn id) is woven into
    /// the file name for debuggability; uniqueness is guaranteed by an internal
    /// sequence so concurrent runs never collide. A [`Sensitivity::Sensitive`]
    /// run fails closed unless an encryptor is configured.
    pub fn create_run(
        &self,
        owner_id: &str,
        sensitivity: Sensitivity,
    ) -> Result<SpillWriter, SpillError> {
        let encryptor = match sensitivity {
            Sensitivity::Plain => None,
            Sensitivity::Sensitive => match &self.encryptor {
                Some(e) => Some(e.clone()),
                None => return Err(SpillError::EncryptionRequired),
            },
        };

        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let safe_owner: String = owner_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .take(48)
            .collect();
        let file_name = format!(
            "{SPILL_PREFIX}{}-{}-{safe_owner}{SPILL_SUFFIX}",
            std::process::id(),
            seq
        );
        let path = self.dir.join(file_name);

        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;

        Ok(SpillWriter {
            writer: BufWriter::new(file),
            guard: SpillFile {
                path,
                reservation: DiskReservation {
                    budget: Arc::clone(&self.budget),
                    bytes: 0,
                },
            },
            encryptor,
            scratch: Vec::new(),
        })
    }
}

// ============================================================================
// SpillWriter / SpillReader — block-framed run I/O
// ============================================================================

/// On-disk framing per block (one `write_batch` call):
///   `[stored_len: u32 LE] [stored_bytes …]`
/// where `stored_bytes` is the (optionally AES-256-GCM-encrypted) payload
///   `[nrows: u32 LE] [value_codec row …]`.
///
/// A run is a sequence of such blocks; EOF marks the end. The reader deletes the
/// file when dropped, so a run is single-pass by design (write fully, then read
/// fully) — exactly the external-sort run / merge shape.
pub struct SpillWriter {
    writer: BufWriter<File>,
    guard: SpillFile,
    encryptor: Option<PageEncryptor>,
    scratch: Vec<u8>,
}

impl SpillWriter {
    /// Encode and append a batch of rows as one block. Reserves disk budget for
    /// the stored bytes **before** writing; on budget exhaustion nothing is
    /// written and the run can still be finished/dropped cleanly.
    pub fn write_batch(&mut self, rows: &[Row]) -> Result<(), SpillError> {
        self.scratch.clear();
        self.scratch
            .extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            value_codec::write_row(&mut self.scratch, row);
        }

        let stored: Vec<u8> = match &self.encryptor {
            Some(enc) => enc.encrypt_bytes(&self.scratch),
            None => std::mem::take(&mut self.scratch),
        };

        // The reader rejects any block above MAX_BLOCK_LEN, and the writer had
        // no matching cap — so a single skewed group large enough to fill the
        // query budget produced a block this process could write and then
        // refused to read back, surfacing as "corrupt spill file" on data it
        // had just written. Reachable whenever max_memory_mb > 1024, which the
        // cgroup auto-derivation reaches on its own in an 8 GB container. Above
        // 4 GiB the `as u32` cast below would truncate and desynchronise the
        // frame instead.
        if stored.len() as u64 > MAX_BLOCK_LEN as u64 {
            return Err(SpillError::Corrupt(format!(
                "spill block of {} bytes exceeds the {MAX_BLOCK_LEN}-byte frame limit; \
                 lower the query memory budget so batches flush more often",
                stored.len()
            )));
        }
        let framed = 4u64 + stored.len() as u64;
        self.guard.reservation.grow(framed)?;

        self.writer
            .write_all(&(stored.len() as u32).to_le_bytes())?;
        self.writer.write_all(&stored)?;

        // Return the buffer to `scratch` for reuse when not encrypting.
        if self.encryptor.is_none() {
            self.scratch = stored;
        }
        Ok(())
    }

    /// Bytes reserved for this run so far (the framed on-disk size).
    pub fn bytes_spilled(&self) -> u64 {
        self.guard.reservation.bytes
    }

    /// Finish writing and reopen the run for a single sequential read pass. The
    /// file-deletion + budget-release guard transfers to the reader, so the file
    /// lives until the reader is dropped.
    pub fn finish(mut self) -> Result<SpillReader, SpillError> {
        self.writer.flush()?;
        // Intentionally no fsync: spill files are ephemeral and swept on crash,
        // so durability across a crash is neither needed nor wanted.
        let path = self.guard.path.clone();
        let file = File::open(&path)?;
        Ok(SpillReader {
            reader: BufReader::new(file),
            guard: self.guard,
            encryptor: self.encryptor,
        })
    }
}

/// Sequential reader over a finished run. Deletes the backing file and releases
/// its disk reservation when dropped.
pub struct SpillReader {
    reader: BufReader<File>,
    #[allow(dead_code)] // held for its Drop (unlink + budget release)
    guard: SpillFile,
    encryptor: Option<PageEncryptor>,
}

impl SpillReader {
    /// Read the next block of rows, or `Ok(None)` at end of run.
    pub fn read_batch(&mut self) -> Result<Option<Vec<Row>>, SpillError> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(SpillError::Io(e)),
        }
        let stored_len = u32::from_le_bytes(len_buf);
        if stored_len > MAX_BLOCK_LEN {
            return Err(SpillError::Corrupt(format!(
                "block length {stored_len} exceeds cap {MAX_BLOCK_LEN}"
            )));
        }

        let mut stored = vec![0u8; stored_len as usize];
        self.reader
            .read_exact(&mut stored)
            .map_err(|e| SpillError::Corrupt(format!("truncated block payload: {e}")))?;

        let payload = match &self.encryptor {
            Some(enc) => enc
                .decrypt_bytes(&stored)
                .map_err(|_| SpillError::Decryption)?,
            None => stored,
        };

        if payload.len() < 4 {
            return Err(SpillError::Corrupt("missing row count".into()));
        }
        let nrows = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        // MAX_BLOCK_LEN guards the OUTER frame against a corrupt length prefix,
        // but this inner count was unchecked: a block whose `nrows` field is
        // corrupted to 0xFFFFFFFF asked for a ~103 GB allocation, which fails
        // through `handle_alloc_error` and ABORTS the process rather than
        // unwinding into `SpillError::Corrupt`. Every row needs at least one
        // byte, so a count that cannot fit in what remains is corrupt.
        if nrows > payload.len().saturating_sub(4) {
            return Err(SpillError::Corrupt(format!(
                "block claims {nrows} rows but holds only {} bytes",
                payload.len() - 4
            )));
        }
        let mut pos = 4usize;
        let mut rows = Vec::with_capacity(nrows);
        for _ in 0..nrows {
            let row = value_codec::read_row(&payload, &mut pos)
                .ok_or_else(|| SpillError::Corrupt("truncated row in block".into()))?;
            rows.push(row);
        }
        Ok(Some(rows))
    }

    /// Drain the whole run into a single `Vec<Row>`.
    pub fn read_all(&mut self) -> Result<Vec<Row>, SpillError> {
        let mut out = Vec::new();
        while let Some(batch) = self.read_batch()? {
            out.extend(batch);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::encryption::PageEncryptor;
    use crate::types::Value;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn sample_rows(n: usize) -> Vec<Row> {
        (0..n)
            .map(|i| {
                vec![
                    Value::Int64(i as i64),
                    Value::Text(format!("row-{i}")),
                    if i % 3 == 0 {
                        Value::Null
                    } else {
                        Value::Bool(true)
                    },
                    Value::Array(vec![Value::Int32(i as i32), Value::Null]),
                ]
            })
            .collect()
    }

    #[test]
    fn plaintext_run_roundtrips_across_batches() {
        let dir = tmp();
        let mgr = SpillManager::new(dir.path(), 1 << 30, None).unwrap();
        let mut w = mgr.create_run("q1", Sensitivity::Plain).unwrap();
        w.write_batch(&sample_rows(3)).unwrap();
        w.write_batch(&sample_rows(5)).unwrap();
        w.write_batch(&[]).unwrap(); // empty batch is a valid block
        let mut r = w.finish().unwrap();
        let mut all = Vec::new();
        all.extend(r.read_batch().unwrap().unwrap());
        all.extend(r.read_batch().unwrap().unwrap());
        assert!(r.read_batch().unwrap().unwrap().is_empty());
        assert!(r.read_batch().unwrap().is_none());
        let mut expected = sample_rows(3);
        expected.extend(sample_rows(5));
        assert_eq!(all, expected);
    }

    #[test]
    fn encrypted_run_roundtrips_and_ciphertext_differs_from_plaintext() {
        let dir = tmp();
        let enc = PageEncryptor::from_key(&[7u8; 32]);
        let mgr = SpillManager::new(dir.path(), 1 << 30, Some(enc)).unwrap();
        let mut w = mgr.create_run("secret", Sensitivity::Sensitive).unwrap();
        let rows = sample_rows(4);
        w.write_batch(&rows).unwrap();
        let path = w.guard.path.clone();
        // Read the raw file bytes and confirm the plaintext marker is absent.
        let on_disk = std::fs::read(&path).unwrap();
        assert!(
            !on_disk.windows(5).any(|win| win == b"row-0"),
            "plaintext leaked into an encrypted spill file"
        );
        let mut r = w.finish().unwrap();
        assert_eq!(r.read_all().unwrap(), rows);
    }

    #[test]
    fn sensitive_run_without_encryptor_fails_closed() {
        let dir = tmp();
        let mgr = SpillManager::new(dir.path(), 1 << 30, None).unwrap();
        match mgr.create_run("secret", Sensitivity::Sensitive) {
            Err(SpillError::EncryptionRequired) => {}
            Err(other) => panic!("expected fail-closed, got {other:?}"),
            Ok(_) => panic!("expected fail-closed, got a writer"),
        }
    }

    #[test]
    fn file_is_deleted_on_reader_drop_and_budget_released() {
        let dir = tmp();
        let mgr = SpillManager::new(dir.path(), 1 << 30, None).unwrap();
        let path;
        {
            let mut w = mgr.create_run("q", Sensitivity::Plain).unwrap();
            w.write_batch(&sample_rows(10)).unwrap();
            path = w.guard.path.clone();
            assert!(path.exists());
            assert!(mgr.budget().used() > 0);
            let r = w.finish().unwrap();
            assert!(path.exists());
            drop(r);
        }
        assert!(!path.exists(), "spill file must be gone after reader drop");
        assert_eq!(mgr.budget().used(), 0, "budget released on cleanup");
    }

    #[test]
    fn file_is_deleted_when_writer_dropped_without_finish() {
        let dir = tmp();
        let mgr = SpillManager::new(dir.path(), 1 << 30, None).unwrap();
        let mut w = mgr.create_run("cancelled", Sensitivity::Plain).unwrap();
        w.write_batch(&sample_rows(2)).unwrap();
        let path = w.guard.path.clone();
        assert!(path.exists());
        drop(w); // simulates query cancel / error mid-build
        assert!(!path.exists());
        assert_eq!(mgr.budget().used(), 0);
    }

    #[test]
    fn disk_budget_exceeded_is_clean_error() {
        let dir = tmp();
        // Tiny ceiling: the first block's framed size overshoots it.
        let mgr = SpillManager::new(dir.path(), 16, None).unwrap();
        let mut w = mgr.create_run("q", Sensitivity::Plain).unwrap();
        match w.write_batch(&sample_rows(100)) {
            Err(SpillError::DiskBudgetExceeded { limit, .. }) => assert_eq!(limit, 16),
            other => panic!("expected budget error, got {other:?}"),
        }
        // Nothing committed to the budget; the run drops cleanly.
        drop(w);
        assert_eq!(mgr.budget().used(), 0);
    }

    #[test]
    fn sweep_reclaims_orphans_but_spares_foreign_files() {
        let dir = tmp();
        let mgr = SpillManager::new(dir.path(), 1 << 30, None).unwrap();
        // Simulate a crash: a spill-named file left behind, no live guard.
        let orphan = dir
            .path()
            .join(format!("{SPILL_PREFIX}999-0-q{SPILL_SUFFIX}"));
        std::fs::write(&orphan, b"leftover").unwrap();
        let foreign = dir.path().join("important.dat");
        std::fs::write(&foreign, b"keep me").unwrap();

        let reclaimed = mgr.sweep_orphans().unwrap();
        assert_eq!(reclaimed, 1);
        assert!(!orphan.exists());
        assert!(foreign.exists(), "sweep must not touch foreign files");
    }

    #[test]
    fn concurrent_runs_get_distinct_files() {
        let dir = tmp();
        let mgr = SpillManager::new(dir.path(), 1 << 30, None).unwrap();
        let a = mgr.create_run("q", Sensitivity::Plain).unwrap();
        let b = mgr.create_run("q", Sensitivity::Plain).unwrap();
        assert_ne!(a.guard.path, b.guard.path);
    }

    #[test]
    fn corrupt_length_prefix_declines_without_panic() {
        let dir = tmp();
        let mgr = SpillManager::new(dir.path(), 1 << 30, None).unwrap();
        let w = mgr.create_run("q", Sensitivity::Plain).unwrap();
        let path = w.guard.path.clone();
        let mut r = w.finish().unwrap();
        // Empty file → immediate clean end.
        assert!(r.read_batch().unwrap().is_none());
        drop(r);
        // Hand-write a bogus oversized length prefix into a fresh file and read.
        std::fs::write(&path, u32::MAX.to_le_bytes()).unwrap();
        let file = File::open(&path).unwrap();
        let mut r2 = SpillReader {
            reader: BufReader::new(file),
            guard: SpillFile {
                path: path.clone(),
                reservation: DiskReservation {
                    budget: DiskBudget::unlimited(),
                    bytes: 0,
                },
            },
            encryptor: None,
        };
        assert!(matches!(r2.read_batch(), Err(SpillError::Corrupt(_))));
    }
}
