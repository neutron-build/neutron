//! Write-Ahead Log (WAL) — type-agnostic page-level logging.
//!
//! Principle 3: The WAL logs page-level changes. It does not know what kind of
//! data is stored on a page. It just logs bytes. This means adding new subsystems
//! (vector indexes, columnar storage, etc.) never requires modifying WAL code.
//!
//! Record format (on disk):
//!   [record_len: u32] [lsn: u64] [txn_id: u64] [record_type: u8]
//!   [page_id: u32] [page_image: PAGE_SIZE bytes] [crc: u32]
//!
//! Record types:
//!   0 = PAGE_WRITE   — full page image after modification
//!   1 = COMMIT       — transaction committed (no page data)
//!   2 = ABORT        — transaction aborted (no page data)
//!   3 = CHECKPOINT   — marks a consistent point (no page data)

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::page::{PAGE_SIZE, PageBuf};

// ============================================================================
// Record types
// ============================================================================

pub const RECORD_PAGE_WRITE: u8 = 0;
pub const RECORD_COMMIT: u8 = 1;
pub const RECORD_ABORT: u8 = 2;
pub const RECORD_CHECKPOINT: u8 = 3;
/// A page's image from BEFORE an uncommitted transaction first modified it.
///
/// Written only on the steal path: the buffer pool evicting (or force-logging)
/// a page that the transaction currently applying has dirtied. Without it,
/// recovery has no way back — a redo-only page WAL can replay an uncommitted
/// page image but cannot take it back, which is how a `kill -9` mid-COMMIT
/// left a partial transaction durable. Carries a full page image, like
/// RECORD_PAGE_WRITE.
pub const RECORD_PAGE_UNDO: u8 = 4;

/// Does this record type carry a full page image after its header?
pub fn carries_page_image(record_type: u8) -> bool {
    record_type == RECORD_PAGE_WRITE || record_type == RECORD_PAGE_UNDO
}

/// How the WAL should sync data to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Full fsync: flushes data + metadata (default, safest).
    Fsync,
    /// fdatasync: flushes data only, skipping metadata like timestamps.
    /// Faster than fsync on most filesystems.
    Fdatasync,
    /// Flush to the OS without forcing the drive's write cache.
    ///
    /// A plain `fsync(2)`. This is the mode that was missing, and on macOS it
    /// is the only one that differs from the other two: Rust's `sync_all` and
    /// `sync_data` both issue `fcntl(F_FULLFSYNC)` there — a true drive-cache
    /// barrier measured at 4,253 us against 41 us for `fsync`, 104x. That is
    /// also why `Fdatasync` is a knob that does nothing on macOS.
    ///
    /// **Durability:** survives a process crash, an OS panic, and `kill -9`,
    /// because the data is in the kernel's hands. It does NOT survive sudden
    /// power loss, because the drive may still hold it in a volatile cache.
    /// That is the same guarantee PostgreSQL gives with
    /// `wal_sync_method = fsync/open_datasync` on macOS, which is what makes a
    /// like-for-like write comparison against that configuration possible.
    ///
    /// On Linux `fsync(2)` normally does flush the device cache, so this mode
    /// is effectively equivalent to `Fsync` there rather than weaker.
    FlushOs,
    /// No sync: let the OS decide when to flush. Fast but unsafe.
    None,
}

/// `fsync(2)` on the file, without the macOS drive-cache barrier that
/// `sync_all`/`sync_data` issue. See [`SyncMode::FlushOs`].
#[cfg(unix)]
fn flush_to_os(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: `file` is an open File, so its fd is valid for the call.
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Non-unix has no separate weaker barrier to reach for, so this is `sync_data`.
#[cfg(not(unix))]
fn flush_to_os(file: &std::fs::File) -> std::io::Result<()> {
    file.sync_data()
}

impl SyncMode {
    /// Parse a sync mode string from config. Case-insensitive.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "fdatasync" => SyncMode::Fdatasync,
            "flush_os" | "flush-os" | "os" => SyncMode::FlushOs,
            "none" | "off" => SyncMode::None,
            _ => SyncMode::Fsync, // default
        }
    }

    /// Force this mode's durability barrier on `file`.
    ///
    /// The caller must have flushed any userspace buffer (a `BufWriter`) first
    /// — this only issues the kernel/device barrier, and syncing a file whose
    /// bytes are still sitting in a `BufWriter` is a no-op that looks like
    /// durability.
    ///
    /// This is the one place the mode is interpreted. It exists because the
    /// same four-arm match was written out at each call site, and the specialty
    /// WALs (document, FTS, CDC) had no arm at all: they ended their appends
    /// with `Write::flush`, which for a bare `std::fs::File` is defined to do
    /// nothing whatsoever. An ack meant "the kernel has it" at best and
    /// "nothing has it" at worst, while reading exactly like a durable write.
    /// Returns whether a barrier was actually issued -- `false` only for
    /// [`SyncMode::None`]. Callers count the `true`s, so that a "syncs" metric
    /// means barriers reached rather than calls made. Counting calls would
    /// report a healthy sync rate for a database configured never to sync,
    /// which is the failure this whole change is about.
    pub fn apply(self, file: &std::fs::File) -> std::io::Result<bool> {
        match self {
            SyncMode::Fsync => file.sync_all().map(|()| true),
            SyncMode::Fdatasync => file.sync_data().map(|()| true),
            SyncMode::FlushOs => flush_to_os(file).map(|()| true),
            SyncMode::None => Ok(false),
        }
    }
}

/// Header size for a WAL record (before page data).
/// record_len(4) + lsn(8) + txn_id(8) + record_type(1) + page_id(4) = 25
const RECORD_HEADER_SIZE: usize = 25;

/// CRC trailer size.
const RECORD_CRC_SIZE: usize = 4;

/// Full record size for a page write.
const PAGE_WRITE_RECORD_SIZE: usize = RECORD_HEADER_SIZE + PAGE_SIZE + RECORD_CRC_SIZE;

/// Record size for control records (commit, abort, checkpoint) — no page data.
const CONTROL_RECORD_SIZE: usize = RECORD_HEADER_SIZE + RECORD_CRC_SIZE;

/// CRC over a page-write record's authenticated bytes: the header
/// (lsn, txn_id, record_type, page_id) followed by the page image. Covering the
/// header — not just the page — means a corrupt page_id or txn_id is detected on
/// replay instead of being silently applied to the wrong page / attributed to the
/// wrong transaction. (Control records already CRC their header fields.)
///
/// The record type is part of the authenticated bytes, so a RECORD_PAGE_UNDO
/// whose type byte flipped to RECORD_PAGE_WRITE fails the CRC rather than
/// being replayed as a redo — which would reinstate exactly the uncommitted
/// image the undo record exists to remove. RECORD_PAGE_WRITE is 0, so page
/// writes hash identically to before this became a parameter and the on-disk
/// format is unchanged.
fn page_image_crc(lsn: u64, txn_id: u64, record_type: u8, page_id: u32, page_image: &[u8]) -> u32 {
    let mut crc = crc32c::crc32c(&lsn.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &txn_id.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &[record_type]);
    crc = crc32c::crc32c_append(crc, &page_id.to_le_bytes());
    crc32c::crc32c_append(crc, page_image)
}

// ============================================================================
// WAL record (in-memory representation)
// ============================================================================

#[derive(Debug)]
pub struct WalRecord {
    pub lsn: u64,
    pub txn_id: u64,
    pub record_type: u8,
    pub page_id: u32,
    /// Full page image (only for PAGE_WRITE records).
    pub page_image: Option<Box<PageBuf>>,
}

// ============================================================================
// WAL writer
// ============================================================================

/// Trait abstracting WAL operations used by the buffer pool.
/// Both single-file `Wal` and `SegmentedWal` implement this.
pub trait WalBackend: Send + Sync {
    /// Log a full page image write. Returns the assigned LSN.
    fn log_page_write(
        &self,
        txn_id: u64,
        page_id: u32,
        page_image: &PageBuf,
    ) -> std::io::Result<u64>;

    /// Log a page's pre-modification image so recovery can undo an
    /// uncommitted write that reached the data file. See [`Wal::log_page_undo`].
    ///
    /// No default: a backend that silently dropped undo records would let a
    /// caller believe an uncommitted page could be taken back when it could
    /// not, which is the failure this whole path exists to remove. Every
    /// backend states its answer.
    fn log_page_undo(
        &self,
        txn_id: u64,
        page_id: u32,
        before_image: &PageBuf,
    ) -> std::io::Result<u64>;
    /// Force buffered WAL data to stable storage.
    fn sync(&self) -> std::io::Result<()>;
    /// Get WAL stats: (bytes_written, syncs).
    fn wal_stats(&self) -> (u64, u64) {
        (0, 0)
    }
    /// Group-commit sync: leader performs the actual sync, followers piggyback.
    fn group_sync(&self) {
        if let Err(e) = self.sync() {
            tracing::error!("WAL group_sync failed: {e}");
        }
    }
    /// Durability-grade group sync: return only once a completed sync covers
    /// `lsn`. Unlike `group_sync`, the result is propagated — commit acks
    /// must not be sent if the WAL could not be made durable.
    fn sync_up_to(&self, _lsn: u64) -> std::io::Result<()> {
        self.sync()
    }
    /// Log a COMMIT record for the given transaction. Returns the assigned LSN.
    fn log_commit(&self, _txn_id: u64) -> std::io::Result<u64> {
        Ok(0)
    }
    /// Log an ABORT record for the given transaction. Returns the assigned LSN.
    fn log_abort(&self, _txn_id: u64) -> std::io::Result<u64> {
        Ok(0)
    }
    /// Log a checkpoint record and return the checkpoint LSN.
    fn log_checkpoint(&self) -> std::io::Result<u64> {
        Ok(0)
    }
    /// Truncate WAL segments before the given LSN to reclaim disk space.
    fn truncate_before(&self, _before_lsn: u64) -> std::io::Result<usize> {
        Ok(0)
    }
    /// Raise the next-LSN floor. Crash recovery calls this after replaying
    /// (and disposing of) WAL content: a freshly opened backend must never
    /// mint LSNs at or below what is already stamped on data pages, or the
    /// NEXT recovery's page-vs-record LSN comparison silently discards the
    /// new records.
    fn bump_next_lsn(&self, min_next: u64);
    /// Start a fresh segment (no-op for single-file backends). Recovery
    /// rotates after replay so every pre-recovery segment — including ones
    /// carrying legacy-format records that log CRC errors on each re-parse
    /// — becomes inactive and prunable at the next checkpoint.
    fn rotate(&self) -> std::io::Result<()> {
        Ok(())
    }
    /// Seal and archive the segment being written, so everything committed so
    /// far is recoverable from the WAL archive alone. Returns whether a segment
    /// was actually archived.
    ///
    /// The default is `Ok(false)` and that is the honest answer here rather
    /// than a stub: only the segmented backend has an archive at all, and PITR
    /// is documented as segmented-only. A backend that cannot archive must say
    /// so — reporting `true` would let a caller believe the tail was preserved.
    fn archive_active(&self) -> std::io::Result<bool> {
        Ok(false)
    }
    /// The next LSN this backend will assign. `0` when the backend does not
    /// track LSNs.
    fn current_lsn(&self) -> u64 {
        0
    }
    /// Hold every segment carrying a record at or after `lsn`, regardless of
    /// what a concurrent checkpoint asks `truncate_before` to reclaim.
    ///
    /// An online physical backup pins retention for the duration of the copy:
    /// the snapshot's data file may lag the WAL by the whole copy window, so
    /// the records that bring it forward must still exist when the copy
    /// finishes. Without the pin a checkpoint mid-backup silently deletes the
    /// only records that could repair the snapshot. Returns `false` when the
    /// backend has nothing to pin (single-file WAL: it is never reclaimed
    /// while open).
    fn pin_retention(&self, _lsn: u64) -> bool {
        false
    }
    /// Release a retention pin taken by [`WalBackend::pin_retention`].
    fn unpin_retention(&self) {}
}

/// The write-ahead log.
pub struct Wal {
    /// The WAL file, buffered for performance.
    writer: Mutex<BufWriter<File>>,
    /// Monotonically increasing log sequence number.
    next_lsn: AtomicU64,
    /// Path to the WAL file (for recovery).
    path: std::path::PathBuf,
    /// Number of records written (all types).
    pub writes: AtomicU64,
    /// Total bytes written to disk (record_len prefix + record body).
    pub bytes_written: AtomicU64,
    /// Number of sync (fsync) operations performed.
    pub syncs: AtomicU64,
    /// How to sync data to disk.
    sync_mode: SyncMode,
    /// Group commit coordinator.
    committer: GroupCommitter,
}

impl Wal {
    /// Open or create a WAL file.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        // Determine next LSN by scanning existing WAL records, and position
        // the writer AFTER the last valid record. The old code left the
        // write cursor at byte 0, so every reopen+append overwrote the head
        // of the existing log — shredding old records and producing CRC
        // "corruption" at fixed LSNs on every subsequent startup replay.
        let mut file = file;
        let file_len = file.metadata()?.len();
        let next_lsn = if file_len == 0 {
            1
        } else {
            // NOT `unwrap_or_default()`. That collapsed ANY scan error into
            // "empty WAL, valid_end 0", and the truncation below then took the
            // whole log to zero — logged as routine torn-tail repair. A
            // transient open/read failure at startup was enough to destroy
            // every acknowledged commit in the file.
            let scan = scan_wal(path)?;
            match scan.tail {
                TailState::TornEof { valid_end } if valid_end < file_len => {
                    // Ordinary crash mid-append: repair so subsequent appends
                    // never land after invalid bytes.
                    tracing::warn!(
                        "WAL: truncating {} bytes of torn tail after last valid record (offset {valid_end})",
                        file_len - valid_end
                    );
                    file.set_len(valid_end)?;
                }
                TailState::InteriorCorruption { offset, ref reason } => {
                    // Quarantine BEFORE truncating. Truncating alone would
                    // destroy the evidence; leaving the file intact is worse —
                    // the writer would append after the corrupt region and
                    // every subsequent scan would stop before those new
                    // records, losing them silently. Copy the original aside,
                    // then cut back to the valid prefix so the log stays
                    // append-able.
                    let quarantine = path.with_extension(format!("wal.corrupt-{offset}"));
                    match std::fs::copy(path, &quarantine) {
                        Ok(_) => tracing::error!(
                            "WAL: interior corruption at offset {offset} ({reason}). \
                             Recovered {} record(s) before it. Original preserved at {}; \
                             truncating the live log to the valid prefix.",
                            scan.records.len(),
                            quarantine.display()
                        ),
                        Err(e) => {
                            // Without a copy, truncating is unrecoverable data
                            // destruction. Refuse.
                            tracing::error!(
                                "WAL: interior corruption at offset {offset} ({reason}), and the \
                                 quarantine copy to {} failed: {e}. Refusing to truncate.",
                                quarantine.display()
                            );
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!(
                                    "WAL corruption at offset {offset} ({reason}); \
                                     could not quarantine the file for repair: {e}"
                                ),
                            ));
                        }
                    }
                    file.set_len(scan.valid_end)?;
                }
                _ => {}
            }
            max_lsn(&scan.records) + 1
        };
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            next_lsn: AtomicU64::new(next_lsn),
            path: path.to_path_buf(),
            writes: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
            sync_mode: SyncMode::Fsync,
            committer: GroupCommitter::new(),
        })
    }

    /// Open a WAL with a specific sync mode.
    pub fn open_with_sync_mode(path: &Path, sync_mode: SyncMode) -> std::io::Result<Self> {
        let mut wal = Self::open(path)?;
        wal.sync_mode = sync_mode;
        Ok(wal)
    }

    /// Log a page write. Returns the LSN assigned to this record.
    /// Must be called BEFORE the dirty page is flushed to the data file.
    pub fn log_page_write(
        &self,
        txn_id: u64,
        page_id: u32,
        page_image: &PageBuf,
    ) -> std::io::Result<u64> {
        self.log_page_image(RECORD_PAGE_WRITE, txn_id, page_id, page_image)
    }

    /// Log a page's pre-modification image, so recovery can undo an
    /// uncommitted write that eviction already pushed to the data file.
    ///
    /// Must be appended BEFORE the uncommitted image is written to disk — the
    /// write-ahead rule applies to undo exactly as it does to redo, and in the
    /// other order a crash between the two leaves the uncommitted bytes on
    /// disk with nothing recording what they replaced.
    pub fn log_page_undo(
        &self,
        txn_id: u64,
        page_id: u32,
        before_image: &PageBuf,
    ) -> std::io::Result<u64> {
        self.log_page_image(RECORD_PAGE_UNDO, txn_id, page_id, before_image)
    }

    fn log_page_image(
        &self,
        record_type: u8,
        txn_id: u64,
        page_id: u32,
        page_image: &PageBuf,
    ) -> std::io::Result<u64> {
        let mut writer = self.writer.lock();
        // LSN allocated under the writer lock: every LSN below next_lsn is
        // fully appended once the lock is held, so sync_covering() can report
        // an exact durable-coverage LSN with no gaps from in-flight appends.
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let record_len = PAGE_WRITE_RECORD_SIZE as u32;
        writer.write_all(&record_len.to_le_bytes())?;
        writer.write_all(&lsn.to_le_bytes())?;
        writer.write_all(&txn_id.to_le_bytes())?;
        writer.write_all(&[record_type])?;
        writer.write_all(&page_id.to_le_bytes())?;
        writer.write_all(page_image)?;

        // CRC over header (lsn/txn_id/type/page_id) + page image — see page_image_crc.
        let crc = page_image_crc(lsn, txn_id, record_type, page_id, page_image);
        writer.write_all(&crc.to_le_bytes())?;

        self.writes.fetch_add(1, Ordering::Relaxed);
        // record_len is the full on-disk size (it already includes the 4-byte
        // length prefix — see RECORD_HEADER_SIZE), so the bytes written equal
        // record_len exactly.
        self.bytes_written
            .fetch_add(record_len as u64, Ordering::Relaxed);

        Ok(lsn)
    }

    /// Log a commit record.
    pub fn log_commit(&self, txn_id: u64) -> std::io::Result<u64> {
        self.log_control(RECORD_COMMIT, txn_id)
    }

    /// Log an abort record.
    pub fn log_abort(&self, txn_id: u64) -> std::io::Result<u64> {
        self.log_control(RECORD_ABORT, txn_id)
    }

    /// Log a checkpoint record.
    pub fn log_checkpoint(&self) -> std::io::Result<u64> {
        self.log_control(RECORD_CHECKPOINT, 0)
    }

    /// Force WAL to disk using the configured sync mode.
    /// Must be called after commit for durability.
    pub fn sync(&self) -> std::io::Result<()> {
        self.sync_covering().map(|_| ())
    }

    /// Sync and report the highest LSN durably covered by this sync.
    ///
    /// The coverage LSN is captured under the writer lock, where LSN
    /// allocation also happens — so every record at or below it is fully
    /// appended and therefore flushed+synced by this call.
    pub fn sync_covering(&self) -> std::io::Result<u64> {
        let mut writer = self.writer.lock();
        let covered = self.next_lsn.load(Ordering::SeqCst).saturating_sub(1);
        writer.flush()?;
        let _ = self.sync_mode.apply(writer.get_ref())?;
        self.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(covered)
    }

    /// Block until a completed sync covers `lsn` (group commit).
    pub fn sync_up_to(&self, lsn: u64) -> std::io::Result<()> {
        self.committer.sync_up_to(lsn, || self.sync_covering())
    }

    /// Get the current (next to be assigned) LSN.
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Acquire)
    }

    /// Convenience constructor: create a new WAL file with a specific sync mode.
    pub fn new(path: &Path, sync_mode: SyncMode) -> std::io::Result<Self> {
        Self::open_with_sync_mode(path, sync_mode)
    }

    /// Perform a group-commit sync. The leader calls fsync; followers piggyback.
    pub fn group_sync(&self) {
        self.committer.group_sync(|| {
            if let Err(e) = self.sync() {
                tracing::error!("WAL group_sync failed: {e}");
            }
        });
    }
}

impl WalBackend for Wal {
    fn log_page_write(
        &self,
        txn_id: u64,
        page_id: u32,
        page_image: &PageBuf,
    ) -> std::io::Result<u64> {
        Wal::log_page_write(self, txn_id, page_id, page_image)
    }

    fn log_page_undo(
        &self,
        txn_id: u64,
        page_id: u32,
        before_image: &PageBuf,
    ) -> std::io::Result<u64> {
        Wal::log_page_undo(self, txn_id, page_id, before_image)
    }

    fn sync(&self) -> std::io::Result<()> {
        Wal::sync(self)
    }

    fn wal_stats(&self) -> (u64, u64) {
        (
            self.bytes_written.load(Ordering::Relaxed),
            self.syncs.load(Ordering::Relaxed),
        )
    }

    fn group_sync(&self) {
        Wal::group_sync(self)
    }

    fn log_commit(&self, txn_id: u64) -> std::io::Result<u64> {
        Wal::log_commit(self, txn_id)
    }

    fn log_abort(&self, txn_id: u64) -> std::io::Result<u64> {
        Wal::log_abort(self, txn_id)
    }

    fn log_checkpoint(&self) -> std::io::Result<u64> {
        Wal::log_checkpoint(self)
    }
    fn bump_next_lsn(&self, min_next: u64) {
        self.next_lsn.fetch_max(min_next, Ordering::SeqCst);
    }
    fn sync_up_to(&self, lsn: u64) -> std::io::Result<()> {
        Wal::sync_up_to(self, lsn)
    }
    fn current_lsn(&self) -> u64 {
        Wal::current_lsn(self)
    }
}

impl Wal {
    // Internal: write a control record (commit/abort/checkpoint).
    fn log_control(&self, record_type: u8, txn_id: u64) -> std::io::Result<u64> {
        let mut writer = self.writer.lock();
        // LSN allocated under the writer lock — see log_page_write.
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let record_len = CONTROL_RECORD_SIZE as u32;
        writer.write_all(&record_len.to_le_bytes())?;
        writer.write_all(&lsn.to_le_bytes())?;
        writer.write_all(&txn_id.to_le_bytes())?;
        writer.write_all(&[record_type])?;
        writer.write_all(&0u32.to_le_bytes())?; // page_id = 0 (not applicable)

        // CRC over the header fields — stack array avoids heap allocation
        let mut crc_buf = [0u8; 17]; // lsn(8) + txn_id(8) + record_type(1)
        crc_buf[..8].copy_from_slice(&lsn.to_le_bytes());
        crc_buf[8..16].copy_from_slice(&txn_id.to_le_bytes());
        crc_buf[16] = record_type;
        let crc = crc32c::crc32c(&crc_buf);
        writer.write_all(&crc.to_le_bytes())?;

        self.writes.fetch_add(1, Ordering::Relaxed);
        // record_len already includes its own 4-byte length prefix.
        self.bytes_written
            .fetch_add(record_len as u64, Ordering::Relaxed);

        Ok(lsn)
    }
}

// ============================================================================
// WAL recovery (reader)
// ============================================================================

/// Read all WAL records from a file for crash recovery.
pub fn read_wal_records(path: &Path) -> std::io::Result<Vec<WalRecord>> {
    Ok(read_wal_records_with_end(path)?.0)
}

/// Read all WAL records plus the byte offset just past the last VALID
/// record. `Wal::open` truncates the file to that offset and appends from
/// there — repairing a torn tail from a crash instead of leaving garbage
/// that later replays report as corruption.
/// Why a record failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalCorruption {
    /// CRC over the record's authenticated bytes did not match.
    Crc {
        lsn: u64,
        stored: u32,
        computed: u32,
    },
    /// Declared record length is impossible (zero, or shorter than a header).
    BadLength { declared: u32 },
}

impl std::fmt::Display for WalCorruption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Crc {
                lsn,
                stored,
                computed,
            } => write!(
                f,
                "CRC mismatch at LSN {lsn}: stored={stored:#x}, computed={computed:#x}"
            ),
            Self::BadLength { declared } => write!(f, "impossible record length {declared}"),
        }
    }
}

/// What the scan found where it stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TailState {
    /// Every byte parsed into a valid record.
    Clean,
    /// The final record is physically incomplete — the ordinary result of a
    /// crash mid-append. Safe to truncate to `valid_end`.
    TornEof { valid_end: u64 },
    /// A record failed validation with more bytes after it. The log is not a
    /// contiguous history from this point on. NEVER truncate on this.
    InteriorCorruption { offset: u64, reason: WalCorruption },
}

/// The result of scanning a WAL file.
///
/// `records` is always the **valid prefix** — every record up to the point the
/// scan stopped, and nothing after it. This is the invariant that matters: a
/// reader must never assemble state from records that straddle a gap, because
/// the missing record may be what makes the suffix meaningful (a COMMIT, or an
/// earlier image of the same page).
pub struct WalScan {
    pub records: Vec<WalRecord>,
    /// Byte offset just past the last fully validated record.
    pub valid_end: u64,
    pub tail: TailState,
}

/// Scan a WAL file into its valid prefix plus a classification of why the scan
/// stopped.
///
/// Replaces the previous skip-and-continue behaviour, which logged a CRC
/// mismatch, advanced by the declared length, and kept replaying. That made
/// recovery a *selection* of records rather than an acknowledged prefix.
/// Distinguishing a torn tail from interior corruption is what lets
/// [`Wal::open`] repair the first and refuse to touch the second.
pub fn scan_wal(path: &Path) -> std::io::Result<WalScan> {
    let (records, valid_end, tail) = scan_inner(path)?;
    Ok(WalScan {
        records,
        valid_end,
        tail,
    })
}

/// Read every record in the valid prefix, discarding tail classification.
///
/// Returns `(records, valid_end)`. Interior corruption is **not** an error here
/// — the caller gets the prefix — but the records after the corruption are
/// never included. Callers that must distinguish the cases use [`scan_wal`].
pub fn read_wal_records_with_end(path: &Path) -> std::io::Result<(Vec<WalRecord>, u64)> {
    let (records, valid_end, _) = scan_inner(path)?;
    Ok((records, valid_end))
}

fn scan_inner(path: &Path) -> std::io::Result<(Vec<WalRecord>, u64, TailState)> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut records = Vec::new();
    let mut last_good_end: u64 = 0;
    let mut pos: u64 = 0;
    let mut tail = TailState::Clean;

    /// A validation failure at `pos` is a torn tail only when the record runs
    /// to physical EOF; anything with bytes after it is interior corruption.
    fn classify(pos: u64, record_end: u64, file_len: u64, reason: WalCorruption) -> TailState {
        if record_end >= file_len {
            TailState::TornEof { valid_end: pos }
        } else {
            TailState::InteriorCorruption {
                offset: pos,
                reason,
            }
        }
    }

    while pos + 4 <= file_len {
        file.seek(SeekFrom::Start(pos))?;

        // Read record length
        let mut len_buf = [0u8; 4];
        if file.read_exact(&mut len_buf).is_err() {
            break; // Truncated record — ignore
        }
        let record_len = u32::from_le_bytes(len_buf) as usize;

        if record_len < RECORD_HEADER_SIZE + RECORD_CRC_SIZE {
            // A length this small cannot describe a record. Zero is what a
            // pre-allocated or partially written tail looks like.
            tail = classify(
                pos,
                file_len,
                file_len,
                WalCorruption::BadLength {
                    declared: record_len as u32,
                },
            );
            break;
        }
        if (pos + record_len as u64) > file_len {
            // Declared length overruns the file: the record was never fully
            // written. This is the ordinary torn tail.
            tail = TailState::TornEof { valid_end: pos };
            break;
        }

        // Read record header
        let mut lsn_buf = [0u8; 8];
        let mut txn_buf = [0u8; 8];
        let mut type_buf = [0u8; 1];
        let mut pid_buf = [0u8; 4];

        file.read_exact(&mut lsn_buf)?;
        file.read_exact(&mut txn_buf)?;
        file.read_exact(&mut type_buf)?;
        file.read_exact(&mut pid_buf)?;

        let lsn = u64::from_le_bytes(lsn_buf);
        let txn_id = u64::from_le_bytes(txn_buf);
        let record_type = type_buf[0];
        let page_id = u32::from_le_bytes(pid_buf);

        let page_image = if carries_page_image(record_type) {
            let mut img = Box::new([0u8; PAGE_SIZE]);
            file.read_exact(img.as_mut())?;
            Some(img)
        } else {
            None
        };

        // Read and validate CRC
        let mut crc_buf = [0u8; 4];
        file.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        if carries_page_image(record_type) {
            // For page-image records, CRC is over the header
            // (lsn/txn_id/type/page_id) plus the page image — see page_image_crc.
            if let Some(ref img) = page_image {
                let computed = page_image_crc(lsn, txn_id, record_type, page_id, img.as_ref());
                if computed != stored_crc {
                    // Stop. Replaying past this would apply a page image whose
                    // predecessor is missing — see `scan_wal`.
                    tracing::error!(
                        "WAL CORRUPTION: CRC mismatch at LSN {lsn} (page write) at offset {pos}: \
                         stored={stored_crc:#x}, computed={computed:#x}. Stopping replay here; \
                         {} record(s) recovered before it.",
                        records.len()
                    );
                    tail = classify(
                        pos,
                        pos + record_len as u64,
                        file_len,
                        WalCorruption::Crc {
                            lsn,
                            stored: stored_crc,
                            computed,
                        },
                    );
                    break;
                }
            }
        } else {
            // For control records, CRC is over header fields — stack array avoids heap alloc
            let mut crc_data = [0u8; 17];
            crc_data[..8].copy_from_slice(&lsn_buf);
            crc_data[8..16].copy_from_slice(&txn_buf);
            crc_data[16] = record_type;
            let computed = crc32c::crc32c(&crc_data);
            if computed != stored_crc {
                // Stop rather than skip. A control record carries transaction
                // state; continuing past one means replaying records whose
                // COMMIT/ABORT context was the thing that got lost.
                tracing::error!(
                    "WAL CORRUPTION: CRC mismatch at LSN {lsn} (control record) at offset {pos}: \
                     stored={stored_crc:#x}, computed={computed:#x}. Stopping replay here; \
                     {} record(s) recovered before it.",
                    records.len()
                );
                tail = classify(
                    pos,
                    pos + record_len as u64,
                    file_len,
                    WalCorruption::Crc {
                        lsn,
                        stored: stored_crc,
                        computed,
                    },
                );
                break;
            }
        }

        records.push(WalRecord {
            lsn,
            txn_id,
            record_type,
            page_id,
            page_image,
        });

        pos += record_len as u64;
        last_good_end = pos;
    }

    // Trailing bytes that never even reached a length prefix are a torn tail.
    if matches!(tail, TailState::Clean) && last_good_end < file_len {
        tail = TailState::TornEof {
            valid_end: last_good_end,
        };
    }

    Ok((records, last_good_end, tail))
}

/// Determine the maximum LSN in a set of WAL records.
pub fn max_lsn(records: &[WalRecord]) -> u64 {
    records.iter().map(|r| r.lsn).max().unwrap_or(0)
}

/// Get the current WAL file byte size, or 0 if the file can't be read.
pub fn wal_file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("path", &self.path)
            .field("next_lsn", &self.next_lsn.load(Ordering::Relaxed))
            .finish()
    }
}

// ============================================================================
// Segmented WAL
// ============================================================================

/// A segmented WAL that splits records across numbered segment files.
///
/// Segment files are named `wal-NNNNNN.log` (e.g., `wal-000001.log`).
/// When the active segment exceeds `max_segment_size`, a new segment is opened.
/// Old segments can be truncated after checkpointing.
pub struct SegmentedWal {
    /// Directory containing WAL segment files.
    dir: std::path::PathBuf,
    /// Maximum size in bytes for a single segment before rotation.
    max_segment_size: u64,
    /// The active (current) WAL segment.
    active: Mutex<ActiveSegment>,
    /// Monotonically increasing LSN counter.
    next_lsn: AtomicU64,
    /// The most recent checkpoint LSN.
    checkpoint_lsn: AtomicU64,
    /// Number of records written (all types).
    pub writes: AtomicU64,
    /// Total bytes written to disk (record_len prefix + record body).
    pub bytes_written_total: AtomicU64,
    /// Number of sync (fsync) operations performed.
    pub syncs: AtomicU64,
    /// How to sync data to disk.
    sync_mode: SyncMode,
    /// Group commit coordinator.
    committer: GroupCommitter,
    /// Optional continuous-archiving destination (PITR). When `Some`, every
    /// segment is copied here the moment it is sealed (rotation) and, as a
    /// last-resort safety net, again just before `truncate_before` would delete
    /// it — so no WAL segment is ever reclaimed without first being preserved.
    /// `None` (the default) means no archiving and zero behavior change.
    archive_dir: Option<std::path::PathBuf>,
    /// Retention floor held by an in-progress online backup. `0` = unpinned.
    /// While non-zero, `truncate_before` never reclaims a segment holding a
    /// record at or after this LSN — see [`WalBackend::pin_retention`].
    retention_pin: AtomicU64,
}

struct ActiveSegment {
    /// Segment number (1-indexed).
    segment_number: u64,
    /// Buffered writer for the active segment.
    writer: BufWriter<File>,
    /// Bytes written to the current segment so far.
    bytes_written: u64,
}

impl SegmentedWal {
    /// Open or create a segmented WAL in the given directory.
    ///
    /// Scans existing segments to determine the next LSN and active segment.
    pub fn open(dir: &Path, max_segment_size: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        // Find existing segments
        let mut segments = list_segments(dir)?;
        segments.sort();

        let (next_lsn, checkpoint_lsn) = if segments.is_empty() {
            (1u64, 0u64)
        } else {
            let mut max = 0u64;
            let mut cp = 0u64;
            for &seg_num in &segments {
                let path = segment_path(dir, seg_num);
                if let Ok(records) = read_wal_records(&path) {
                    for r in &records {
                        if r.lsn > max {
                            max = r.lsn;
                        }
                        if r.record_type == RECORD_CHECKPOINT && r.lsn > cp {
                            cp = r.lsn;
                        }
                    }
                }
            }
            (max + 1, cp)
        };

        // Open or create the active segment
        let active_seg_num = segments.last().copied().unwrap_or(1);
        let seg_path = segment_path(dir, active_seg_num);
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&seg_path)?;
        let bytes_written = file.metadata()?.len();

        Ok(Self {
            dir: dir.to_path_buf(),
            max_segment_size,
            active: Mutex::new(ActiveSegment {
                segment_number: active_seg_num,
                writer: BufWriter::new(file),
                bytes_written,
            }),
            next_lsn: AtomicU64::new(next_lsn),
            checkpoint_lsn: AtomicU64::new(checkpoint_lsn),
            writes: AtomicU64::new(0),
            bytes_written_total: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
            sync_mode: SyncMode::Fsync,
            committer: GroupCommitter::new(),
            // Opt-in continuous archiving: `NUCLEUS_WAL_ARCHIVE_DIR=<root>`
            // enables PITR archiving into a per-WAL subdirectory of <root>
            // (named after this WAL's directory), so multiple databases in one
            // process never collide. Unset → no archiving.
            archive_dir: Self::archive_dir_from_env(dir),
            retention_pin: AtomicU64::new(0),
        })
    }

    /// Open a segmented WAL with a specific sync mode.
    pub fn open_with_sync_mode(
        dir: &Path,
        max_segment_size: u64,
        sync_mode: SyncMode,
    ) -> std::io::Result<Self> {
        let mut wal = Self::open(dir, max_segment_size)?;
        wal.sync_mode = sync_mode;
        Ok(wal)
    }

    /// Open a segmented WAL with continuous archiving to an explicit directory
    /// (bypasses the `NUCLEUS_WAL_ARCHIVE_DIR` env lookup). Primarily for
    /// tests and embedded callers that manage their own archive layout.
    pub fn open_with_archive(
        dir: &Path,
        max_segment_size: u64,
        sync_mode: SyncMode,
        archive_dir: &Path,
    ) -> std::io::Result<Self> {
        let mut wal = Self::open(dir, max_segment_size)?;
        wal.sync_mode = sync_mode;
        wal.archive_dir = Some(archive_dir.to_path_buf());
        Ok(wal)
    }

    /// Resolve the per-WAL archive directory from `NUCLEUS_WAL_ARCHIVE_DIR`.
    /// The env names an archive *root*; each WAL archives under
    /// `<root>/<wal-dir-basename>` so distinct databases stay separated.
    fn archive_dir_from_env(wal_dir: &Path) -> Option<std::path::PathBuf> {
        let root = std::env::var_os("NUCLEUS_WAL_ARCHIVE_DIR")?;
        if root.is_empty() {
            return None;
        }
        let basename = wal_dir
            .file_name()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("wal"));
        Some(std::path::Path::new(&root).join(basename))
    }

    /// Log a page write. Returns the assigned LSN.
    pub fn log_page_write(
        &self,
        txn_id: u64,
        page_id: u32,
        page_image: &PageBuf,
    ) -> std::io::Result<u64> {
        self.log_page_image(RECORD_PAGE_WRITE, txn_id, page_id, page_image)
    }

    /// Log a page's pre-modification image. See [`Wal::log_page_undo`].
    pub fn log_page_undo(
        &self,
        txn_id: u64,
        page_id: u32,
        before_image: &PageBuf,
    ) -> std::io::Result<u64> {
        self.log_page_image(RECORD_PAGE_UNDO, txn_id, page_id, before_image)
    }

    fn log_page_image(
        &self,
        record_type: u8,
        txn_id: u64,
        page_id: u32,
        page_image: &PageBuf,
    ) -> std::io::Result<u64> {
        let mut active = self.active.lock();
        // LSN allocated under the segment lock — see Wal::log_page_write.
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let record_len = PAGE_WRITE_RECORD_SIZE as u32;
        active.writer.write_all(&record_len.to_le_bytes())?;
        active.writer.write_all(&lsn.to_le_bytes())?;
        active.writer.write_all(&txn_id.to_le_bytes())?;
        active.writer.write_all(&[record_type])?;
        active.writer.write_all(&page_id.to_le_bytes())?;
        active.writer.write_all(page_image)?;
        let crc = page_image_crc(lsn, txn_id, record_type, page_id, page_image);
        active.writer.write_all(&crc.to_le_bytes())?;

        active.bytes_written += record_len as u64;

        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written_total
            .fetch_add(4 + record_len as u64, Ordering::Relaxed);

        // Check if rotation is needed
        if active.bytes_written >= self.max_segment_size {
            self.rotate_inner(&mut active)?;
        }

        Ok(lsn)
    }

    /// Log a commit record.
    pub fn log_commit(&self, txn_id: u64) -> std::io::Result<u64> {
        self.log_control(RECORD_COMMIT, txn_id)
    }

    /// Log an abort record.
    pub fn log_abort(&self, txn_id: u64) -> std::io::Result<u64> {
        self.log_control(RECORD_ABORT, txn_id)
    }

    /// Log a checkpoint record and update the checkpoint LSN.
    pub fn log_checkpoint(&self) -> std::io::Result<u64> {
        let lsn = self.log_control(RECORD_CHECKPOINT, 0)?;
        self.checkpoint_lsn.store(lsn, Ordering::SeqCst);
        Ok(lsn)
    }

    /// Force all buffered WAL data to disk using the configured sync mode.
    pub fn sync(&self) -> std::io::Result<()> {
        self.sync_covering().map(|_| ())
    }

    /// Sync and report the highest LSN durably covered by this sync.
    ///
    /// Coverage is exact: LSNs are allocated under the segment lock, so every
    /// LSN below `next_lsn` is fully appended (to this or an already-fsynced
    /// rotated-out segment) by the time we hold the lock here.
    pub fn sync_covering(&self) -> std::io::Result<u64> {
        let mut active = self.active.lock();
        let covered = self.next_lsn.load(Ordering::SeqCst).saturating_sub(1);
        active.writer.flush()?;
        let _ = self.sync_mode.apply(active.writer.get_ref())?;
        self.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(covered)
    }

    /// Block until a completed sync covers `lsn` (group commit).
    pub fn sync_up_to(&self, lsn: u64) -> std::io::Result<()> {
        self.committer.sync_up_to(lsn, || self.sync_covering())
    }

    /// Manually rotate to a new segment.
    pub fn rotate(&self) -> std::io::Result<()> {
        let mut active = self.active.lock();
        self.rotate_inner(&mut active)
    }

    /// Seal and archive the segment currently being written, so everything
    /// committed up to now is recoverable from the archive alone.
    ///
    /// Segments are otherwise archived only when they fill up, which makes the
    /// PITR recovery point the last *rollover* rather than the last commit. At
    /// the default 64 MiB segment a low-write database can run for days without
    /// rolling over, and every one of those commits is missing from the archive
    /// — including across a clean shutdown, where nothing was lost and nothing
    /// crashed. Worse, `restore-pitr` replays what it has and reports success,
    /// so the gap is invisible exactly when someone is relying on it.
    ///
    /// Returns `Ok(true)` if a segment was sealed and archived, `Ok(false)` if
    /// there was nothing to do (no archive configured, or the active segment is
    /// empty). Rotating an empty segment is skipped deliberately: called on a
    /// timer it would otherwise litter the archive with empty files.
    pub fn archive_active(&self) -> std::io::Result<bool> {
        if self.archive_dir.is_none() {
            return Ok(false);
        }
        let mut active = self.active.lock();
        if active.bytes_written == 0 {
            return Ok(false);
        }
        self.rotate_inner(&mut active)?;
        Ok(true)
    }

    /// Copy a sealed segment into the archive directory (idempotent) and record
    /// its LSN range + archive time in the archive index. This is the PITR
    /// durability primitive: a segment that has been archived can be replayed
    /// long after it is reclaimed from the live WAL.
    ///
    /// Returns `Ok(true)` when archiving is configured and the segment is now
    /// present in the archive (freshly copied, already present, or already
    /// reclaimed from the live dir), `Ok(false)` when no archive is configured.
    /// The copy is staged through a temp file + atomic rename, so a crash mid
    /// copy never leaves a truncated segment a restore would trust.
    fn archive_segment(&self, seg_num: u64) -> std::io::Result<bool> {
        let Some(archive) = self.archive_dir.as_ref() else {
            return Ok(false);
        };
        std::fs::create_dir_all(archive)?;
        let src = segment_path(&self.dir, seg_num);
        if !src.exists() {
            // Nothing to archive — already reclaimed. Treat as success so the
            // caller (truncate_before) does not block on a vanished segment.
            return Ok(true);
        }
        let src_len = std::fs::metadata(&src)?.len();
        let dst = segment_path(archive, seg_num);
        // Idempotent: a same-size archived copy already exists.
        if let Ok(m) = std::fs::metadata(&dst)
            && m.len() == src_len
        {
            return Ok(true);
        }
        let tmp = dst.with_extension("log.tmp");
        std::fs::copy(&src, &tmp)?;
        std::fs::rename(&tmp, &dst)?;
        // Record the LSN range + archive time for time-based PITR. The index is
        // an optimization; LSN-based restore reads the segment files directly,
        // so a missing/partial index never loses recoverability.
        if let Some((min_lsn, max_lsn)) = segment_lsn_bounds(&src) {
            let unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let mut idx = OpenOptions::new()
                .create(true)
                .append(true)
                .open(archive.join(ARCHIVE_INDEX_NAME))?;
            writeln!(idx, "{seg_num} {min_lsn} {max_lsn} {unix}")?;
            idx.sync_all()?;
        }
        Ok(true)
    }

    /// Truncate (delete) all segments whose records are fully before `before_lsn`.
    ///
    /// This reclaims disk space after checkpointing. Segments that contain any
    /// record with LSN >= `before_lsn` are kept. When continuous archiving is
    /// enabled, a segment is archived before deletion and is NEVER deleted if
    /// archiving fails — the "no acknowledged write is ever unrecoverable"
    /// guarantee takes precedence over reclaiming disk.
    ///
    /// An active retention pin (see [`SegmentedWal::pin_retention`]) clamps
    /// `before_lsn`: a checkpoint that fires during an online backup must not
    /// reclaim the records that backup still needs to reach consistency.
    pub fn truncate_before(&self, before_lsn: u64) -> std::io::Result<usize> {
        let pin = self.retention_pin.load(Ordering::Acquire);
        let before_lsn = if pin == 0 {
            before_lsn
        } else {
            before_lsn.min(pin)
        };
        let active = self.active.lock();
        let active_seg = active.segment_number;
        drop(active);

        let mut segments = list_segments(&self.dir)?;
        segments.sort();

        let mut removed = 0;
        for seg_num in segments {
            // Never remove the active segment
            if seg_num >= active_seg {
                break;
            }
            let path = segment_path(&self.dir, seg_num);
            // Fail CLOSED on an unreadable segment, matching the archive guard
            // below ("keeping it rather than losing it").
            //
            // `unwrap_or_default()` collapsed an I/O error — EMFILE under fd
            // pressure, a transient EIO — into an empty record list, hence
            // max_seg_lsn = 0, which is below any `before_lsn`, so the segment
            // was DELETED UNREAD. Segments numbered above the one holding the
            // checkpoint record legitimately carry records past `cp_lsn`, and
            // those cover pages dirtied after the checkpoint's flush: they are
            // the only copy. Continuous archiving would preserve the bytes
            // first, but it is opt-in and off by default.
            let records = match read_wal_records(&path) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        segment = seg_num,
                        "WAL truncation: could not read segment, keeping it rather than \
                         deleting it unread: {e}"
                    );
                    break;
                }
            };
            let max_seg_lsn = records.iter().map(|r| r.lsn).max().unwrap_or(0);

            if max_seg_lsn < before_lsn {
                // Last-resort archiving safety net: never delete an un-archived
                // segment. If it was already archived on rotation this is a
                // cheap idempotent no-op; if archiving fails, keep the segment.
                if self.archive_dir.is_some() {
                    match self.archive_segment(seg_num) {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(
                                "WAL archive of segment {seg_num} failed; \
                                 keeping it rather than losing it: {e}"
                            );
                            continue;
                        }
                    }
                }
                std::fs::remove_file(&path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Get the current (next to be assigned) LSN.
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Acquire)
    }

    /// Hold every segment carrying a record at or after `lsn` until
    /// [`SegmentedWal::unpin_retention`]. Idempotent; a lower pin wins, so
    /// overlapping backups all keep the records they need.
    pub fn pin_retention(&self, lsn: u64) {
        let lsn = lsn.max(1);
        let mut cur = self.retention_pin.load(Ordering::Acquire);
        loop {
            if cur != 0 && cur <= lsn {
                return;
            }
            match self.retention_pin.compare_exchange_weak(
                cur,
                lsn,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Release the retention pin.
    pub fn unpin_retention(&self) {
        self.retention_pin.store(0, Ordering::Release);
    }

    /// The current retention pin (`0` when unpinned). Test/introspection hook.
    pub fn retention_pin(&self) -> u64 {
        self.retention_pin.load(Ordering::Acquire)
    }

    /// Get the most recent checkpoint LSN.
    pub fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn.load(Ordering::Acquire)
    }

    /// Get the active segment number.
    pub fn active_segment(&self) -> u64 {
        self.active.lock().segment_number
    }

    /// Read all WAL records across all segments in LSN order.
    pub fn read_all_records(&self) -> std::io::Result<Vec<WalRecord>> {
        let mut segments = list_segments(&self.dir)?;
        segments.sort();

        let mut all_records = Vec::new();
        for seg_num in segments {
            let path = segment_path(&self.dir, seg_num);
            if let Ok(records) = read_wal_records(&path) {
                all_records.extend(records);
            }
        }
        all_records.sort_by_key(|r| r.lsn);
        Ok(all_records)
    }

    // Internal: write a control record.
    fn log_control(&self, record_type: u8, txn_id: u64) -> std::io::Result<u64> {
        let mut active = self.active.lock();
        // LSN allocated under the segment lock — see Wal::log_page_write.
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let record_len = CONTROL_RECORD_SIZE as u32;
        active.writer.write_all(&record_len.to_le_bytes())?;
        active.writer.write_all(&lsn.to_le_bytes())?;
        active.writer.write_all(&txn_id.to_le_bytes())?;
        active.writer.write_all(&[record_type])?;
        active.writer.write_all(&0u32.to_le_bytes())?;

        let mut crc_buf = [0u8; 17];
        crc_buf[..8].copy_from_slice(&lsn.to_le_bytes());
        crc_buf[8..16].copy_from_slice(&txn_id.to_le_bytes());
        crc_buf[16] = record_type;
        let crc = crc32c::crc32c(&crc_buf);
        active.writer.write_all(&crc.to_le_bytes())?;

        active.bytes_written += record_len as u64;

        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written_total
            .fetch_add(4 + record_len as u64, Ordering::Relaxed);

        if active.bytes_written >= self.max_segment_size {
            self.rotate_inner(&mut active)?;
        }

        Ok(lsn)
    }

    // Internal: rotate to a new segment (called with lock held).
    fn rotate_inner(&self, active: &mut ActiveSegment) -> std::io::Result<()> {
        active.writer.flush()?;
        match self.sync_mode {
            SyncMode::Fsync => active.writer.get_ref().sync_all()?,
            SyncMode::Fdatasync => active.writer.get_ref().sync_data()?,
            SyncMode::FlushOs => flush_to_os(active.writer.get_ref())?,
            SyncMode::None => {}
        }

        // Continuous archiving: preserve the just-sealed segment immediately, so
        // PITR sees committed data without waiting for the next checkpoint. A
        // failure here is logged, not fatal — the segment stays on disk and
        // truncate_before will guard it (refusing to delete it un-archived).
        if self.archive_dir.is_some() {
            let sealed = active.segment_number;
            if let Err(e) = self.archive_segment(sealed) {
                tracing::warn!("WAL archive of sealed segment {sealed} failed: {e}");
            }
        }

        let new_seg_num = active.segment_number + 1;
        let new_path = segment_path(&self.dir, new_seg_num);
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&new_path)?;

        active.segment_number = new_seg_num;
        active.writer = BufWriter::new(file);
        active.bytes_written = 0;

        Ok(())
    }

    /// Perform a group-commit sync. The leader calls fsync; followers piggyback.
    pub fn group_sync(&self) {
        self.committer.group_sync(|| {
            if let Err(e) = self.sync() {
                tracing::error!("SegmentedWal group_sync failed: {e}");
            }
        });
    }
}

impl WalBackend for SegmentedWal {
    fn log_page_write(
        &self,
        txn_id: u64,
        page_id: u32,
        page_image: &PageBuf,
    ) -> std::io::Result<u64> {
        SegmentedWal::log_page_write(self, txn_id, page_id, page_image)
    }

    fn log_page_undo(
        &self,
        txn_id: u64,
        page_id: u32,
        before_image: &PageBuf,
    ) -> std::io::Result<u64> {
        SegmentedWal::log_page_undo(self, txn_id, page_id, before_image)
    }

    fn sync(&self) -> std::io::Result<()> {
        SegmentedWal::sync(self)
    }

    fn archive_active(&self) -> std::io::Result<bool> {
        SegmentedWal::archive_active(self)
    }

    fn wal_stats(&self) -> (u64, u64) {
        (
            self.bytes_written_total.load(Ordering::Relaxed),
            self.syncs.load(Ordering::Relaxed),
        )
    }

    fn group_sync(&self) {
        SegmentedWal::group_sync(self)
    }

    fn log_commit(&self, txn_id: u64) -> std::io::Result<u64> {
        SegmentedWal::log_commit(self, txn_id)
    }

    fn log_abort(&self, txn_id: u64) -> std::io::Result<u64> {
        SegmentedWal::log_abort(self, txn_id)
    }

    fn log_checkpoint(&self) -> std::io::Result<u64> {
        SegmentedWal::log_checkpoint(self)
    }

    fn truncate_before(&self, before_lsn: u64) -> std::io::Result<usize> {
        SegmentedWal::truncate_before(self, before_lsn)
    }
    fn bump_next_lsn(&self, min_next: u64) {
        self.next_lsn.fetch_max(min_next, Ordering::SeqCst);
    }
    fn rotate(&self) -> std::io::Result<()> {
        SegmentedWal::rotate(self)
    }
    fn sync_up_to(&self, lsn: u64) -> std::io::Result<()> {
        SegmentedWal::sync_up_to(self, lsn)
    }
    fn current_lsn(&self) -> u64 {
        SegmentedWal::current_lsn(self)
    }
    fn pin_retention(&self, lsn: u64) -> bool {
        SegmentedWal::pin_retention(self, lsn);
        true
    }
    fn unpin_retention(&self) {
        SegmentedWal::unpin_retention(self);
    }
}

impl std::fmt::Debug for SegmentedWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentedWal")
            .field("dir", &self.dir)
            .field("next_lsn", &self.next_lsn.load(Ordering::Relaxed))
            .field(
                "checkpoint_lsn",
                &self.checkpoint_lsn.load(Ordering::Relaxed),
            )
            .finish()
    }
}

// ============================================================================
// Group commit
// ============================================================================

/// Batched sync for group commit optimization.
///
/// Uses a leader-follower pattern with parking_lot::Condvar:
/// - First caller becomes the leader, performs the actual sync
/// - Subsequent callers wait on the condvar for the leader to finish
/// - After sync, leader increments epoch and wakes all followers
pub struct GroupCommitter {
    state: parking_lot::Mutex<GroupCommitState>,
    done: parking_lot::Condvar,
}

struct GroupCommitState {
    syncing: bool,
    epoch: u64,
    waiters: u64,
    sync_count: u64,
    /// Highest LSN known to be covered by a completed sync. Only advanced by
    /// `sync_up_to`, whose sync closure reports the coverage it achieved.
    synced_lsn: u64,
}

impl Default for GroupCommitter {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupCommitter {
    pub fn new() -> Self {
        Self {
            state: parking_lot::Mutex::new(GroupCommitState {
                syncing: false,
                epoch: 0,
                waiters: 0,
                sync_count: 0,
                synced_lsn: 0,
            }),
            done: parking_lot::Condvar::new(),
        }
    }

    /// Perform a group sync. Only the leader calls `sync_fn`; followers wait.
    pub fn group_sync<F: FnOnce()>(&self, sync_fn: F) -> u64 {
        let mut state = self.state.lock();
        if state.syncing {
            let my_epoch = state.epoch;
            state.waiters += 1;
            while state.epoch == my_epoch && state.syncing {
                self.done.wait(&mut state);
            }
            state.waiters -= 1;
            if state.epoch > my_epoch {
                return state.epoch;
            }
        }
        state.syncing = true;
        drop(state);
        sync_fn();
        let mut state = self.state.lock();
        state.syncing = false;
        state.epoch += 1;
        state.sync_count += 1;
        let epoch = state.epoch;
        drop(state);
        self.done.notify_all();
        epoch
    }

    /// Durability-grade group sync: returns only once a completed sync covers
    /// `target_lsn`.
    ///
    /// Unlike `group_sync`, a caller arriving while a sync is in flight does
    /// NOT piggyback on it blindly — that sync may have started before the
    /// caller's records were appended and therefore not cover them. Instead the
    /// caller re-checks `synced_lsn` after every completed sync and becomes the
    /// next leader if its records still aren't covered.
    ///
    /// `sync_fn` must perform the sync and return the highest LSN it durably
    /// covered (capture it under the WAL writer lock before syncing). Errors
    /// from the leader's own sync attempt are returned to that leader; waiting
    /// followers simply retry as leader on the next loop iteration.
    pub fn sync_up_to<F: Fn() -> std::io::Result<u64>>(
        &self,
        target_lsn: u64,
        sync_fn: F,
    ) -> std::io::Result<()> {
        loop {
            let mut state = self.state.lock();
            if state.synced_lsn >= target_lsn {
                return Ok(());
            }
            if state.syncing {
                let my_epoch = state.epoch;
                state.waiters += 1;
                while state.epoch == my_epoch && state.syncing {
                    self.done.wait(&mut state);
                }
                state.waiters -= 1;
                // Re-check coverage on the next loop iteration.
                continue;
            }
            state.syncing = true;
            drop(state);
            let result = sync_fn();
            let mut state = self.state.lock();
            state.syncing = false;
            state.epoch += 1;
            state.sync_count += 1;
            if let Ok(covered) = result {
                state.synced_lsn = state.synced_lsn.max(covered);
            }
            drop(state);
            self.done.notify_all();
            // Propagate our own sync failure; on success the loop re-checks
            // coverage (a rotation mid-append can leave target > covered).
            result?;
        }
    }

    pub fn sync_count(&self) -> u64 {
        self.state.lock().sync_count
    }

    /// Number of callers currently queued as followers behind the in-flight
    /// leader. Lets a test deterministically wait until every peer has batched
    /// before the leader completes, instead of relying on thread-scheduling
    /// timing (which is what made the batching tests flaky under CPU contention).
    pub fn waiters(&self) -> u64 {
        self.state.lock().waiters
    }

    pub fn epoch(&self) -> u64 {
        self.state.lock().epoch
    }

    pub fn pending_count(&self) -> u64 {
        self.state.lock().waiters
    }
}

/// Read every record from every segment of a segmented-WAL directory, in
/// segment order. A missing directory reads as empty. Used by crash
/// recovery, which must read the same storage the segmented writer used —
/// recovery used to read only the single-file WAL and silently replayed
/// nothing for segmented (default) deployments.
pub fn read_wal_dir_records(dir: &Path) -> std::io::Result<Vec<WalRecord>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut segments = list_segments(dir)?;
    segments.sort_unstable();
    let mut all = Vec::new();
    for seg in segments {
        // Propagate rather than skip. A torn tail is NOT an error here --
        // `read_wal_records_with_end` repairs that internally and returns the
        // records it could parse -- so an Err from this call is a genuine I/O
        // failure to read a segment that exists. Skipping it silently dropped
        // every commit in that segment while the open went on to succeed, which
        // is the failure the single-file arm of the caller explicitly refuses.
        let path = segment_path(dir, seg);
        let mut records = read_wal_records(&path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "WAL recovery could not read segment {}: {e}. Refusing to open and \
                     silently discard its commits.",
                    path.display()
                ),
            )
        })?;
        all.append(&mut records);
    }
    Ok(all)
}

// ============================================================================
// Segment helpers
// ============================================================================

/// Name of the append-only archive index (one line per archived segment:
/// `<seg_num> <min_lsn> <max_lsn> <archived_unix>`). Used to resolve
/// time-based PITR targets; LSN-based restore reads the segment files directly.
pub const ARCHIVE_INDEX_NAME: &str = "archive.index";

/// Generate the path for a WAL segment file.
fn segment_path(dir: &Path, segment_number: u64) -> std::path::PathBuf {
    dir.join(format!("wal-{segment_number:06}.log"))
}

/// Return the `(min_lsn, max_lsn)` spanned by a segment file, or `None` if it
/// holds no parseable records.
pub(crate) fn segment_lsn_bounds(path: &Path) -> Option<(u64, u64)> {
    let records = read_wal_records(path).ok()?;
    let mut min = u64::MAX;
    let mut max = 0u64;
    for r in &records {
        min = min.min(r.lsn);
        max = max.max(r.lsn);
    }
    if records.is_empty() {
        None
    } else {
        Some((min, max))
    }
}

/// List archived segment numbers in an archive directory (same naming as the
/// live WAL). Public within the crate for the PITR restore path.
pub(crate) fn list_archive_segments(dir: &Path) -> std::io::Result<Vec<u64>> {
    list_segments(dir)
}

/// What an archive prune removed and, more importantly, what it kept.
#[derive(Debug, Clone, Default)]
pub struct ArchivePruneReport {
    /// Segment numbers deleted, ascending.
    pub removed: Vec<u64>,
    /// Segments still present afterwards.
    pub kept: usize,
    /// Lowest LSN still recoverable from the archive, if anything is left.
    pub oldest_retained_lsn: Option<u64>,
    /// Bytes reclaimed.
    pub bytes_freed: u64,
    /// Segments left alone because their LSN range could not be read. Never
    /// deleted: a segment we cannot prove is below the horizon might be the one
    /// a restore needs.
    pub skipped_unreadable: Vec<u64>,
}

/// Delete archived WAL segments that lie entirely below `keep_from_lsn`.
///
/// Continuous archiving had no retention story at all: `archive_segment` copies
/// every sealed segment in and nothing ever took one out, so an archive on a
/// busy database grows until the disk does not. This is the missing half. It is
/// deliberately **not** automatic and there is no default policy -- deleting
/// recovery data on a timer, with no knowledge of which base backups still
/// exist, trades a disk-space problem for an unrecoverable one.
///
/// Safety rules, in order of how badly each would hurt if wrong:
///
/// 1. A segment is removed only when its **max** LSN is strictly below
///    `keep_from_lsn`. The segment *containing* the horizon is kept, because a
///    restore to that LSN has to replay it.
/// 2. Bounds come from the segment file itself, not the index. The index is
///    documented as an optimization that a restore can do without; letting it
///    decide deletions would promote an advisory file into the authority on
///    what is recoverable.
/// 3. A segment whose bounds cannot be read is kept and reported, never
///    removed. Unreadable is not the same as unneeded.
///
/// The index is rewritten to match, so a later time-based restore does not
/// resolve a target onto a segment that is gone.
pub fn prune_archive(
    archive_dir: &Path,
    keep_from_lsn: u64,
) -> std::io::Result<ArchivePruneReport> {
    let report = plan_prune_archive(archive_dir, keep_from_lsn)?;
    for seg in &report.removed {
        std::fs::remove_file(segment_path(archive_dir, *seg))?;
    }
    if !report.removed.is_empty() {
        rewrite_archive_index(archive_dir, &report.removed)?;
    }
    Ok(report)
}

/// Decide what [`prune_archive`] would remove, without removing it.
///
/// The real prune is this function plus the deletions, so a dry run cannot
/// disagree with the run it is previewing. Computing the preview separately
/// would make it a second implementation of the rule that decides whether
/// recovery data is expendable, and the two would drift.
pub fn plan_prune_archive(
    archive_dir: &Path,
    keep_from_lsn: u64,
) -> std::io::Result<ArchivePruneReport> {
    if !archive_dir.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("archive directory {} does not exist", archive_dir.display()),
        ));
    }
    let mut report = ArchivePruneReport::default();
    let mut retained_min: Option<u64> = None;

    for seg in list_archive_segments(archive_dir)? {
        let path = segment_path(archive_dir, seg);
        match segment_lsn_bounds(&path) {
            Some((min_lsn, max_lsn)) => {
                if max_lsn < keep_from_lsn {
                    report.bytes_freed += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    report.removed.push(seg);
                } else {
                    report.kept += 1;
                    retained_min = Some(retained_min.map_or(min_lsn, |m: u64| m.min(min_lsn)));
                }
            }
            None => {
                report.kept += 1;
                report.skipped_unreadable.push(seg);
            }
        }
    }
    report.removed.sort_unstable();
    report.oldest_retained_lsn = retained_min;
    Ok(report)
}

/// Drop `removed` segments from the archive index, atomically.
///
/// Written to a temp file and renamed, so a crash mid-rewrite leaves the old
/// index rather than a half one -- the same discipline `atomic_replace_wal`
/// applies to the logs themselves.
fn rewrite_archive_index(archive_dir: &Path, removed: &[u64]) -> std::io::Result<()> {
    let idx_path = archive_dir.join(ARCHIVE_INDEX_NAME);
    let Ok(contents) = std::fs::read_to_string(&idx_path) else {
        return Ok(()); // no index is a supported state
    };
    let gone: std::collections::HashSet<u64> = removed.iter().copied().collect();
    let mut kept = String::new();
    for line in contents.lines() {
        let seg = line
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<u64>().ok());
        match seg {
            Some(n) if gone.contains(&n) => {}
            // An unparseable line is kept rather than dropped: this rewrite
            // exists to remove entries for files that are gone, not to tidy.
            _ => {
                kept.push_str(line);
                kept.push('\n');
            }
        }
    }
    let tmp = idx_path.with_extension("index.tmp");
    std::fs::write(&tmp, kept.as_bytes())?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, &idx_path)?;
    Ok(())
}

/// Copy the byte-exact prefix of `src` holding every record with `lsn <=
/// target_lsn` into `dst`, stopping at the first record beyond the target.
///
/// Records are appended in strictly increasing LSN order under the WAL lock, so
/// a prefix cut at a record boundary yields a valid, replayable segment WITHOUT
/// re-serializing (preserving every CRC exactly). Returns the highest LSN
/// copied, or `None` if no record qualified (nothing written).
pub(crate) fn copy_segment_prefix_upto_lsn(
    src: &Path,
    dst: &Path,
    target_lsn: u64,
) -> std::io::Result<Option<u64>> {
    let data = std::fs::read(src)?;
    let mut pos: usize = 0;
    let mut cutoff: usize = 0;
    let mut max_copied: Option<u64> = None;
    while pos + 4 <= data.len() {
        let record_len =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        // record_len is the FULL on-disk record size, including its own 4-byte
        // length prefix (see read_wal_records_with_end). 0 or overrun = torn
        // tail; stop.
        if record_len < RECORD_HEADER_SIZE + RECORD_CRC_SIZE || pos + record_len > data.len() {
            break;
        }
        // LSN sits immediately after the length prefix.
        let lsn = u64::from_le_bytes([
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
            data[pos + 8],
            data[pos + 9],
            data[pos + 10],
            data[pos + 11],
        ]);
        if lsn <= target_lsn {
            pos += record_len;
            cutoff = pos;
            max_copied = Some(lsn);
        } else {
            break;
        }
    }
    if cutoff == 0 {
        return Ok(None);
    }
    std::fs::write(dst, &data[..cutoff])?;
    Ok(max_copied)
}

/// Path of a segment file inside a WAL/archive directory. Crate-visible for the
/// PITR restore path, which reconstructs a WAL directory from the archive.
pub(crate) fn segment_file_path(dir: &Path, segment_number: u64) -> std::path::PathBuf {
    segment_path(dir, segment_number)
}

/// List all segment numbers in a WAL directory.
pub(crate) fn list_segments(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stripped) = name.strip_prefix("wal-")
            && let Some(num_str) = stripped.strip_suffix(".log")
            && let Ok(n) = num_str.parse::<u64>()
        {
            segments.push(n);
        }
    }
    Ok(segments)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── Undo records (CAMPAIGN-02) ──────────────────────────────────────

    #[test]
    fn undo_record_roundtrips_with_its_page_image() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("undo.wal");
        let wal = Wal::open(&wal_path).unwrap();

        let before = [7u8; PAGE_SIZE];
        let after = [9u8; PAGE_SIZE];
        let undo_lsn = wal.log_page_undo(42, 3, &before).unwrap();
        let write_lsn = wal.log_page_write(42, 3, &after).unwrap();
        wal.sync().unwrap();

        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(records.len(), 2, "both records must survive the round trip");

        assert_eq!(records[0].record_type, RECORD_PAGE_UNDO);
        assert_eq!(records[0].lsn, undo_lsn);
        assert_eq!(records[0].txn_id, 42);
        assert_eq!(records[0].page_id, 3);
        assert_eq!(
            records[0]
                .page_image
                .as_ref()
                .expect("undo carries an image")
                .as_ref(),
            &before,
            "the undo record must carry the BEFORE image, not the new one"
        );

        assert_eq!(records[1].record_type, RECORD_PAGE_WRITE);
        assert_eq!(records[1].lsn, write_lsn);
        assert_eq!(records[1].page_image.as_ref().unwrap().as_ref(), &after);
    }

    /// The record type is inside the CRC, so an undo record cannot decay into
    /// a redo record. If it could, a single flipped bit would reinstate
    /// exactly the uncommitted page image the undo record exists to remove.
    #[test]
    fn an_undo_record_retyped_as_a_write_fails_its_crc() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("retype.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_page_undo(1, 5, &[3u8; PAGE_SIZE]).unwrap();
            wal.sync().unwrap();
        }

        // The type byte sits after the 4-byte length, 8-byte LSN and 8-byte
        // txn id — see the record layout in `log_page_image`.
        const TYPE_OFFSET: u64 = 4 + 8 + 8;
        let mut bytes = std::fs::read(&wal_path).unwrap();
        assert_eq!(bytes[TYPE_OFFSET as usize], RECORD_PAGE_UNDO);
        bytes[TYPE_OFFSET as usize] = RECORD_PAGE_WRITE;
        std::fs::write(&wal_path, &bytes).unwrap();

        let records = read_wal_records(&wal_path).unwrap();
        assert!(
            records.is_empty(),
            "a retyped undo record must fail its CRC and stop replay, not be \
             replayed as a redo; got {records:?}"
        );
    }

    // ── Single-file WAL tests ───────────────────────────────────────────

    #[test]
    fn wal_write_and_read_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Wal::open(&wal_path).unwrap();

        let page = [42u8; PAGE_SIZE];
        let lsn1 = wal.log_page_write(1, 10, &page).unwrap();
        let lsn2 = wal.log_commit(1).unwrap();
        wal.sync().unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);

        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record_type, RECORD_PAGE_WRITE);
        assert_eq!(records[0].page_id, 10);
        assert_eq!(records[1].record_type, RECORD_COMMIT);
    }

    // Regression: a corrupt control record must be skipped by exactly its own
    // length so the records AFTER it still recover. The skip used to advance by
    // `4 + record_len`, over-shooting by 4 and silently losing the rest of the WAL.
    #[test]
    fn wal_open_repairs_torn_tail_and_appends_cleanly() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("torn.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(1).unwrap();
            wal.log_commit(2).unwrap();
            wal.sync().unwrap();
        }
        // Simulate a crash mid-append: valid length prefix, torn payload.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            f.write_all(&(CONTROL_RECORD_SIZE as u32).to_le_bytes())
                .unwrap();
            f.write_all(&[0xAB; 7]).unwrap(); // partial header, then crash
            f.sync_all().unwrap();
        }
        // Reopen: the torn tail is truncated, the next append lands cleanly,
        // and replay recovers every record with no CRC corruption.
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(3).unwrap();
            wal.sync().unwrap();
        }
        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records.iter().map(|r| r.txn_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        // And it stays clean across further restarts (the fixed-LSN symptom).
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(4).unwrap();
            wal.sync().unwrap();
        }
        assert_eq!(read_wal_records(&wal_path).unwrap().len(), 4);
    }

    #[test]
    fn wal_reopen_append_must_not_overwrite_existing_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("reopen.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(1).unwrap();
            wal.log_commit(2).unwrap();
            wal.sync().unwrap();
        }
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(3).unwrap();
            wal.sync().unwrap();
        }
        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(
            records.len(),
            3,
            "reopen+append clobbered earlier records: got {:?}",
            records.iter().map(|r| r.txn_id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn wal_replay_stops_at_corrupt_control_record() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("corrupt.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(1).unwrap();
            wal.log_commit(2).unwrap();
            wal.log_commit(3).unwrap();
            wal.sync().unwrap();
        }

        // Corrupt the CRC trailer of the FIRST control record (its last 4 bytes).
        let crc_off = (CONTROL_RECORD_SIZE - RECORD_CRC_SIZE) as u64;
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.seek(SeekFrom::Start(crc_off)).unwrap();
            f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            f.sync_all().unwrap();
        }

        // This test previously asserted the OPPOSITE — that records after the
        // corrupt one still recover, "proving the skip realigned correctly".
        // Realigning was the right fix for the misalignment bug, but skipping
        // is the wrong policy: recovery then returns a SELECTION of records
        // rather than an acknowledged prefix, and the missing record may be
        // exactly what makes the suffix meaningful. Postgres stops at the
        // first invalid record; so do we now.
        let scan = scan_wal(&wal_path).unwrap();
        assert!(
            scan.records.is_empty(),
            "replay must stop at the corrupt record, not skip past it"
        );
        assert_eq!(scan.valid_end, 0);
        match scan.tail {
            TailState::InteriorCorruption { offset, .. } => assert_eq!(offset, 0),
            other => panic!("expected interior corruption, got {other:?}"),
        }
    }

    #[test]
    fn wal_torn_final_record_is_not_interior_corruption() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("torn.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(1).unwrap();
            wal.log_commit(2).unwrap();
            wal.sync().unwrap();
        }
        // A record whose declared length overruns the file: crash mid-append.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            f.write_all(&(CONTROL_RECORD_SIZE as u32).to_le_bytes())
                .unwrap();
            f.write_all(&[0xAB; 3]).unwrap();
            f.sync_all().unwrap();
        }
        let scan = scan_wal(&wal_path).unwrap();
        assert_eq!(scan.records.len(), 2, "the valid prefix must survive");
        assert!(
            matches!(scan.tail, TailState::TornEof { .. }),
            "a record running to physical EOF is a torn tail, not corruption: {:?}",
            scan.tail
        );
    }

    #[test]
    fn wal_open_quarantines_interior_corruption_instead_of_zeroing() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("quarantine.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(1).unwrap();
            wal.log_commit(2).unwrap();
            wal.log_commit(3).unwrap();
            wal.sync().unwrap();
        }
        // Corrupt the SECOND record, leaving a third after it.
        let crc_off = (CONTROL_RECORD_SIZE * 2 - RECORD_CRC_SIZE) as u64;
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.seek(SeekFrom::Start(crc_off)).unwrap();
            f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            f.sync_all().unwrap();
        }
        let before_len = std::fs::metadata(&wal_path).unwrap().len();

        // Reopening must keep the valid prefix and preserve the original.
        // The bug this guards: `unwrap_or_default()` turned any scan failure
        // into `(vec![], 0)`, and `0 < file_len` then truncated the log to
        // zero under a message that read like routine tail repair.
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(4).unwrap();
            wal.sync().unwrap();
        }
        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(
            records.iter().map(|r| r.txn_id).collect::<Vec<_>>(),
            vec![1, 4],
            "the prefix before the corruption must survive and stay append-able"
        );
        let quarantine = wal_path.with_extension(format!("wal.corrupt-{}", CONTROL_RECORD_SIZE));
        let saved = std::fs::metadata(&quarantine).expect("original must be quarantined");
        assert_eq!(
            saved.len(),
            before_len,
            "quarantine must be a byte-exact copy of the corrupt log"
        );
    }

    #[test]
    fn wal_page_write_detects_header_corruption() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("hdr.wal");
        {
            let wal = Wal::open(&wal_path).unwrap();
            let page = Box::new([0u8; PAGE_SIZE]);
            wal.log_page_write(42, 7, &page).unwrap(); // txn_id=42, page_id=7
            wal.sync().unwrap();
        }
        // Sanity: the record reads back.
        assert_eq!(read_wal_records(&wal_path).unwrap().len(), 1);

        // Corrupt the page_id field in the HEADER (not the page body). Before the
        // header-CRC fix the CRC covered only the page image, so this corruption
        // went undetected and the page would be applied to the wrong page_id.
        let page_id_off = (RECORD_HEADER_SIZE - 4) as u64; // 25 - 4 = 21
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.seek(SeekFrom::Start(page_id_off)).unwrap();
            f.write_all(&[0xAB]).unwrap();
            f.sync_all().unwrap();
        }

        let records = read_wal_records(&wal_path).unwrap();
        assert!(
            records.is_empty(),
            "page-write header corruption must now be caught by the CRC"
        );
    }

    #[test]
    fn wal_checkpoint_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Wal::open(&wal_path).unwrap();

        let lsn = wal.log_checkpoint().unwrap();
        wal.sync().unwrap();

        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, RECORD_CHECKPOINT);
        assert_eq!(records[0].lsn, lsn);
    }

    #[test]
    fn wal_reopen_continues_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_commit(1).unwrap();
            wal.log_commit(2).unwrap();
            wal.sync().unwrap();
        }

        let wal2 = Wal::open(&wal_path).unwrap();
        assert_eq!(wal2.current_lsn(), 3);
    }

    // ── Segmented WAL tests ─────────────────────────────────────────────

    #[test]
    fn segmented_wal_basic_operations() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let wal = SegmentedWal::open(&wal_dir, 1024 * 1024).unwrap();
        let page = [0u8; PAGE_SIZE];

        let lsn1 = wal.log_page_write(1, 0, &page).unwrap();
        let lsn2 = wal.log_commit(1).unwrap();
        wal.sync().unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(wal.current_lsn(), 3);
    }

    #[test]
    fn segmented_wal_auto_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Each page write record = 4 + 25 + 16384 + 4 = 16417 bytes (record_len prefix + header + page + crc).
        // Actually record_len includes header+page+crc = 25 + 16384 + 4 = 16413.
        // On disk: 4 (len prefix) + 16413 (record) = 16417 bytes total.
        // Set max segment to 16500 so it rotates after each page write.
        let wal = SegmentedWal::open(&wal_dir, 16_500).unwrap();
        let page = [0u8; PAGE_SIZE];

        wal.log_page_write(1, 0, &page).unwrap();
        // bytes_written = 16413, which >= 16500? No, 16413 < 16500.
        // The bytes_written tracks record_len (the value written), not the 4-byte prefix.
        // Actually let's check: record_len = PAGE_WRITE_RECORD_SIZE = 25 + 16384 + 4 = 16413
        // bytes_written += 16413, but the actual disk write is 4 + 16413 = 16417
        // So bytes_written = 16413 after first write. 16413 < 16500, no rotation.
        // Let's use a smaller threshold to guarantee rotation.
        drop(wal);

        let wal = SegmentedWal::open(&wal_dir, 100).unwrap();
        let page = [0u8; PAGE_SIZE];

        wal.log_page_write(1, 0, &page).unwrap();
        let seg_after_first = wal.active_segment();

        wal.log_page_write(2, 1, &page).unwrap();
        let seg_after_second = wal.active_segment();

        wal.sync().unwrap();

        // Should have rotated at least once
        assert!(
            seg_after_first > 1 || seg_after_second > seg_after_first,
            "should rotate: seg1={seg_after_first}, seg2={seg_after_second}"
        );

        let segments = list_segments(&wal_dir).unwrap();
        assert!(
            segments.len() >= 2,
            "should have multiple segment files: {}",
            segments.len()
        );
    }

    #[test]
    fn segmented_wal_read_all_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let wal = SegmentedWal::open(&wal_dir, 20_000).unwrap();
        let page = [0u8; PAGE_SIZE];

        // Write records across multiple segments
        for i in 0..5 {
            wal.log_page_write(i, i as u32, &page).unwrap();
        }
        wal.sync().unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 5);
        // Should be in LSN order
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.lsn, (i + 1) as u64);
        }
    }

    #[test]
    fn segmented_wal_checkpoint_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let wal = SegmentedWal::open(&wal_dir, 20_000).unwrap();
        let page = [0u8; PAGE_SIZE];

        // Write 3 page records (each triggers rotation)
        wal.log_page_write(1, 0, &page).unwrap();
        wal.log_page_write(2, 1, &page).unwrap();
        wal.log_page_write(3, 2, &page).unwrap();
        let cp_lsn = wal.log_checkpoint().unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.checkpoint_lsn(), cp_lsn);

        let segs_before = list_segments(&wal_dir).unwrap().len();
        let removed = wal.truncate_before(cp_lsn).unwrap();
        let segs_after = list_segments(&wal_dir).unwrap().len();

        assert!(removed > 0, "should have removed some segments");
        assert!(segs_after < segs_before, "fewer segments after truncation");
    }

    #[test]
    fn retention_pin_survives_a_checkpoint_truncate() {
        // The sharp failure this prevents: an online backup copies the data
        // file over some window; a checkpoint fires mid-copy and truncates the
        // WAL past the window's start; the records that would have brought the
        // copied pages forward are gone, and the snapshot is silently
        // unrecoverable. The pin must beat the truncate.
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let wal = SegmentedWal::open(&wal_dir, 20_000).unwrap();
        let page = [7u8; PAGE_SIZE];

        // Records that predate the backup window — these are reclaimable.
        wal.log_page_write(1, 0, &page).unwrap();
        wal.log_page_write(2, 1, &page).unwrap();

        // Backup begins here.
        let pin_lsn = wal.current_lsn();
        wal.pin_retention(pin_lsn);
        assert_eq!(wal.retention_pin(), pin_lsn);

        // Writes during the copy window — the snapshot needs every one of them.
        let mut window_lsns = Vec::new();
        for txn in 3..8u64 {
            window_lsns.push(wal.log_page_write(txn, txn as u32, &page).unwrap());
        }
        let cp_lsn = wal.log_checkpoint().unwrap();
        wal.sync().unwrap();

        // A checkpoint asks to reclaim everything below it — including the
        // whole window.
        wal.truncate_before(cp_lsn).unwrap();

        let surviving: std::collections::HashSet<u64> = wal
            .read_all_records()
            .unwrap()
            .into_iter()
            .map(|r| r.lsn)
            .collect();
        for lsn in &window_lsns {
            assert!(
                surviving.contains(lsn),
                "checkpoint reclaimed LSN {lsn}, which the in-progress backup still needs \
                 (pin was {pin_lsn})"
            );
        }

        // Once the backup releases the pin, the same request reclaims freely.
        wal.unpin_retention();
        assert_eq!(wal.retention_pin(), 0);
        wal.truncate_before(cp_lsn).unwrap();
        let after: std::collections::HashSet<u64> = wal
            .read_all_records()
            .unwrap()
            .into_iter()
            .map(|r| r.lsn)
            .collect();
        assert!(
            after.len() < surviving.len(),
            "releasing the pin must let the checkpoint reclaim: {} -> {}",
            surviving.len(),
            after.len()
        );
    }

    #[test]
    fn retention_pin_keeps_the_lowest_of_overlapping_pins() {
        // Two backups in flight: the older one's needs must win, or the
        // younger one's begin silently shortens the older one's retention.
        let dir = tempfile::tempdir().unwrap();
        let wal = SegmentedWal::open(&dir.path().join("wal"), 20_000).unwrap();
        wal.pin_retention(50);
        wal.pin_retention(90);
        assert_eq!(
            wal.retention_pin(),
            50,
            "a later, higher pin must not raise the floor"
        );
        wal.pin_retention(20);
        assert_eq!(wal.retention_pin(), 20, "a lower pin must lower the floor");
        wal.unpin_retention();
        assert_eq!(wal.retention_pin(), 0);
    }

    #[test]
    fn segmented_wal_reopen_continues() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let wal = SegmentedWal::open(&wal_dir, 1024 * 1024).unwrap();
            wal.log_commit(1).unwrap();
            wal.log_commit(2).unwrap();
            wal.log_commit(3).unwrap();
            wal.sync().unwrap();
        }

        let wal2 = SegmentedWal::open(&wal_dir, 1024 * 1024).unwrap();
        assert_eq!(wal2.current_lsn(), 4);
    }

    #[test]
    fn segmented_wal_manual_rotate() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let wal = SegmentedWal::open(&wal_dir, 1024 * 1024).unwrap();
        assert_eq!(wal.active_segment(), 1);

        wal.rotate().unwrap();
        assert_eq!(wal.active_segment(), 2);

        wal.rotate().unwrap();
        assert_eq!(wal.active_segment(), 3);
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    use proptest::prelude::*;

    proptest! {
        /// A single page write record roundtrips through WAL write + read.
        #[test]
        fn prop_wal_page_write_roundtrip(
            txn_id in any::<u64>(),
            page_id in any::<u32>(),
            fill_byte in any::<u8>(),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let wal_path = dir.path().join("prop_pw.wal");
            let wal = Wal::open(&wal_path).unwrap();

            let page = [fill_byte; PAGE_SIZE];
            let lsn = wal.log_page_write(txn_id, page_id, &page).unwrap();
            wal.sync().unwrap();

            let records = read_wal_records(&wal_path).unwrap();
            prop_assert_eq!(records.len(), 1);
            prop_assert_eq!(records[0].lsn, lsn);
            prop_assert_eq!(records[0].txn_id, txn_id);
            prop_assert_eq!(records[0].record_type, RECORD_PAGE_WRITE);
            prop_assert_eq!(records[0].page_id, page_id);
            let img = records[0].page_image.as_ref().expect("page_image should be Some for PAGE_WRITE");
            prop_assert_eq!(img.as_ref(), &page);
        }

        /// Commit records roundtrip correctly.
        #[test]
        fn prop_wal_commit_roundtrip(txn_id in any::<u64>()) {
            let dir = tempfile::tempdir().unwrap();
            let wal_path = dir.path().join("prop_commit.wal");
            let wal = Wal::open(&wal_path).unwrap();

            let lsn = wal.log_commit(txn_id).unwrap();
            wal.sync().unwrap();

            let records = read_wal_records(&wal_path).unwrap();
            prop_assert_eq!(records.len(), 1);
            prop_assert_eq!(records[0].lsn, lsn);
            prop_assert_eq!(records[0].txn_id, txn_id);
            prop_assert_eq!(records[0].record_type, RECORD_COMMIT);
            prop_assert!(records[0].page_image.is_none());
        }

        /// Abort records roundtrip correctly.
        #[test]
        fn prop_wal_abort_roundtrip(txn_id in any::<u64>()) {
            let dir = tempfile::tempdir().unwrap();
            let wal_path = dir.path().join("prop_abort.wal");
            let wal = Wal::open(&wal_path).unwrap();

            let lsn = wal.log_abort(txn_id).unwrap();
            wal.sync().unwrap();

            let records = read_wal_records(&wal_path).unwrap();
            prop_assert_eq!(records.len(), 1);
            prop_assert_eq!(records[0].lsn, lsn);
            prop_assert_eq!(records[0].txn_id, txn_id);
            prop_assert_eq!(records[0].record_type, RECORD_ABORT);
            prop_assert!(records[0].page_image.is_none());
        }

        /// Mixed sequences of WAL records preserve all fields and have monotonically increasing LSNs.
        #[test]
        fn prop_wal_mixed_sequence_roundtrip(
            txn_ids in proptest::collection::vec(any::<u64>(), 1..10),
            page_ids in proptest::collection::vec(any::<u32>(), 1..10),
            fill_byte in any::<u8>(),
            record_types in proptest::collection::vec(0u8..4u8, 1..10),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let wal_path = dir.path().join("prop_mixed.wal");
            let wal = Wal::open(&wal_path).unwrap();

            let page = [fill_byte; PAGE_SIZE];
            let count = record_types.len();
            let mut expected_lsns = Vec::with_capacity(count);
            let mut expected_types = Vec::with_capacity(count);
            let mut expected_txns = Vec::with_capacity(count);
            let mut expected_pids = Vec::with_capacity(count);

            for i in 0..count {
                let txn = txn_ids[i % txn_ids.len()];
                let pid = page_ids[i % page_ids.len()];
                let rt = record_types[i];
                let lsn = match rt {
                    0 => wal.log_page_write(txn, pid, &page).unwrap(),
                    1 => wal.log_commit(txn).unwrap(),
                    2 => wal.log_abort(txn).unwrap(),
                    _ => wal.log_checkpoint().unwrap(),
                };
                expected_lsns.push(lsn);
                expected_types.push(rt);
                expected_txns.push(if rt == 3 { 0 } else { txn });
                expected_pids.push(if rt == 0 { pid } else { 0 });
            }
            wal.sync().unwrap();

            let records = read_wal_records(&wal_path).unwrap();
            prop_assert_eq!(records.len(), count);

            // Verify LSNs are monotonically increasing.
            for i in 1..records.len() {
                prop_assert!(records[i].lsn > records[i - 1].lsn,
                    "LSNs must be monotonically increasing: {} vs {}",
                    records[i - 1].lsn, records[i].lsn);
            }

            // Verify all fields match.
            for (i, rec) in records.iter().enumerate() {
                prop_assert_eq!(rec.lsn, expected_lsns[i]);
                prop_assert_eq!(rec.record_type, expected_types[i]);
                prop_assert_eq!(rec.txn_id, expected_txns[i]);
                prop_assert_eq!(rec.page_id, expected_pids[i]);

                if rec.record_type == RECORD_PAGE_WRITE {
                    let img = rec.page_image.as_ref().expect("PAGE_WRITE must have page_image");
                    prop_assert_eq!(img.as_ref(), &page);
                } else {
                    prop_assert!(rec.page_image.is_none());
                }
            }
        }

        /// LSNs are always monotonically increasing across writes.
        #[test]
        fn prop_wal_lsns_monotonic(num_records in 2usize..20) {
            let dir = tempfile::tempdir().unwrap();
            let wal_path = dir.path().join("prop_mono.wal");
            let wal = Wal::open(&wal_path).unwrap();

            let mut lsns = Vec::with_capacity(num_records);
            for i in 0..num_records {
                let lsn = wal.log_commit(i as u64).unwrap();
                lsns.push(lsn);
            }
            wal.sync().unwrap();

            // Verify LSNs are strictly increasing.
            for i in 1..lsns.len() {
                prop_assert!(lsns[i] > lsns[i - 1],
                    "LSN {} ({}) must be greater than LSN {} ({})",
                    i, lsns[i], i - 1, lsns[i - 1]);
            }

            // Verify the same ordering after reading back.
            let records = read_wal_records(&wal_path).unwrap();
            prop_assert_eq!(records.len(), num_records);
            for i in 1..records.len() {
                prop_assert!(records[i].lsn > records[i - 1].lsn);
            }
        }

        /// Write N random page images to the WAL, then replay all records and verify
        /// byte-for-byte match of every page image.
        #[test]
        fn prop_wal_write_then_replay(
            entries in proptest::collection::vec(
                (any::<u64>(), any::<u32>(), any::<u8>()), 1..20
            ),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let wal_path = dir.path().join("prop_replay.wal");
            let wal = Wal::open(&wal_path).unwrap();

            // Build expected page images and write them to the WAL.
            let mut expected: Vec<(u64, u32, Box<PageBuf>)> = Vec::with_capacity(entries.len());
            for (txn_id, page_id, fill_byte) in &entries {
                let page = [*fill_byte; PAGE_SIZE];
                let lsn = wal.log_page_write(*txn_id, *page_id, &page).unwrap();
                expected.push((lsn, *page_id, Box::new(page)));
            }
            wal.sync().unwrap();

            // Replay and verify.
            let records = read_wal_records(&wal_path).unwrap();
            prop_assert_eq!(records.len(), expected.len(),
                "record count mismatch: expected {}, got {}", expected.len(), records.len());

            for (i, rec) in records.iter().enumerate() {
                let (exp_lsn, exp_pid, ref exp_page) = expected[i];
                prop_assert_eq!(rec.lsn, exp_lsn,
                    "LSN mismatch at record {}: expected {}, got {}", i, exp_lsn, rec.lsn);
                prop_assert_eq!(rec.page_id, exp_pid,
                    "page_id mismatch at record {}: expected {}, got {}", i, exp_pid, rec.page_id);
                prop_assert_eq!(rec.record_type, RECORD_PAGE_WRITE);
                let img = rec.page_image.as_ref()
                    .expect("PAGE_WRITE record must have page_image");
                prop_assert_eq!(img.as_ref(), exp_page.as_ref(),
                    "page image mismatch at record {}", i);
            }
        }

        /// Page images with random byte patterns at specific offsets roundtrip correctly.
        #[test]
        fn prop_wal_page_image_partial_random(
            txn_id in any::<u64>(),
            page_id in any::<u32>(),
            offset in 0usize..PAGE_SIZE,
            patch in proptest::collection::vec(any::<u8>(), 0..256),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let wal_path = dir.path().join("prop_partial.wal");
            let wal = Wal::open(&wal_path).unwrap();

            let mut page = [0u8; PAGE_SIZE];
            let end = (offset + patch.len()).min(PAGE_SIZE);
            let copy_len = end - offset;
            page[offset..offset + copy_len].copy_from_slice(&patch[..copy_len]);

            wal.log_page_write(txn_id, page_id, &page).unwrap();
            wal.sync().unwrap();

            let records = read_wal_records(&wal_path).unwrap();
            prop_assert_eq!(records.len(), 1);
            let img = records[0].page_image.as_ref().unwrap();
            prop_assert_eq!(img.as_ref(), &page);
        }
    }

    // ── SyncMode tests ─────────────────────────────────────────────────

    #[test]
    fn sync_mode_from_str() {
        assert_eq!(SyncMode::from_str("fsync"), SyncMode::Fsync);
        assert_eq!(SyncMode::from_str("FSYNC"), SyncMode::Fsync);
        assert_eq!(SyncMode::from_str("fdatasync"), SyncMode::Fdatasync);
        assert_eq!(SyncMode::from_str("FDATASYNC"), SyncMode::Fdatasync);
        assert_eq!(SyncMode::from_str("flush_os"), SyncMode::FlushOs);
        assert_eq!(SyncMode::from_str("flush-os"), SyncMode::FlushOs);
        assert_eq!(SyncMode::from_str("OS"), SyncMode::FlushOs);
        assert_eq!(SyncMode::from_str("none"), SyncMode::None);
        assert_eq!(SyncMode::from_str("off"), SyncMode::None);
        assert_eq!(SyncMode::from_str("anything_else"), SyncMode::Fsync); // default
    }

    #[test]
    fn wal_with_fdatasync_mode() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("fdatasync.wal");
        let wal = Wal::open_with_sync_mode(&wal_path, SyncMode::Fdatasync).unwrap();

        let page = [7u8; PAGE_SIZE];
        wal.log_page_write(1, 0, &page).unwrap();
        wal.sync().unwrap(); // should use sync_data instead of sync_all

        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
    }

    /// `FlushOs` gives up the drive-cache barrier, not the write. Records must
    /// still be on disk and readable after `sync()` — that is the whole point
    /// of it being distinct from `None`, which skips the syscall entirely.
    #[test]
    fn wal_with_flush_os_mode_still_persists_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("flushos.wal");
        let wal = Wal::open_with_sync_mode(&wal_path, SyncMode::FlushOs).unwrap();

        let page = [11u8; PAGE_SIZE];
        wal.log_page_write(1, 0, &page).unwrap();
        wal.log_page_write(1, 1, &page).unwrap();
        wal.sync().unwrap(); // plain fsync(2), no F_FULLFSYNC

        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(
            records.len(),
            2,
            "FlushOs must still push records to the OS; it only declines the \
             drive-cache barrier"
        );
    }

    #[test]
    fn wal_with_none_sync_mode() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("nosync.wal");
        let wal = Wal::open_with_sync_mode(&wal_path, SyncMode::None).unwrap();

        let page = [9u8; PAGE_SIZE];
        wal.log_page_write(1, 0, &page).unwrap();
        wal.sync().unwrap(); // should skip sync entirely

        let records = read_wal_records(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn segmented_wal_with_fdatasync_mode() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("seg_fdatasync");
        let wal =
            SegmentedWal::open_with_sync_mode(&wal_dir, 1024 * 1024, SyncMode::Fdatasync).unwrap();

        let page = [5u8; PAGE_SIZE];
        wal.log_page_write(1, 0, &page).unwrap();
        wal.sync().unwrap();

        let seg_path = segment_path(&wal_dir, 1);
        let records = read_wal_records(&seg_path).unwrap();
        assert_eq!(records.len(), 1);
    }
}

#[cfg(test)]
mod group_commit_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    #[test]
    fn group_commit_single_thread() {
        let gc = GroupCommitter::new();
        let count = Arc::new(AtomicU64::new(0));
        let c = count.clone();
        gc.group_sync(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(gc.epoch(), 1);
        assert_eq!(gc.sync_count(), 1);
    }

    #[test]
    fn group_commit_epoch_advances() {
        let gc = GroupCommitter::new();
        assert_eq!(gc.epoch(), 0);
        gc.group_sync(|| {});
        assert_eq!(gc.epoch(), 1);
        gc.group_sync(|| {});
        assert_eq!(gc.epoch(), 2);
    }

    #[test]
    fn group_commit_concurrent_batching() {
        use std::sync::Barrier;
        let gc = Arc::new(GroupCommitter::new());
        let sync_count = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(10));
        let mut handles = vec![];
        for _ in 0..10 {
            let gc = gc.clone();
            let gc_inner = gc.clone();
            let sc = sync_count.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                gc.group_sync(|| {
                    // The leader holds the sync open until the other 9 callers
                    // are queued as followers, so they are GUARANTEED to batch
                    // behind this one sync — deterministic regardless of OS
                    // scheduling. (The old `sleep(10ms)` + `total < 10` assertion
                    // flaked under CPU contention: if threads ran serially each
                    // became its own leader and `total == 10`.)
                    while gc_inner.waiters() < 9 {
                        std::thread::yield_now();
                    }
                    sc.fetch_add(1, Ordering::SeqCst);
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Exactly one leader synced; the other 9 batched behind it.
        assert_eq!(
            sync_count.load(Ordering::SeqCst),
            1,
            "all 10 concurrent callers must batch into one sync"
        );
        assert_eq!(gc.sync_count(), 1);
    }

    #[test]
    fn group_commit_followers_unblock() {
        use std::sync::Barrier;
        let gc = Arc::new(GroupCommitter::new());
        let barrier = Arc::new(Barrier::new(5));
        let done = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];
        for _ in 0..5 {
            let gc = gc.clone();
            let b = barrier.clone();
            let d = done.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                gc.group_sync(|| {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                });
                d.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(done.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn group_commit_sequential_calls() {
        let gc = GroupCommitter::new();
        let count = Arc::new(AtomicU64::new(0));
        for _ in 0..5 {
            let c = count.clone();
            gc.group_sync(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        assert_eq!(count.load(Ordering::SeqCst), 5);
        assert_eq!(gc.sync_count(), 5);
    }

    #[test]
    fn group_commit_stress_100_threads() {
        use std::sync::Barrier;
        let gc = Arc::new(GroupCommitter::new());
        let sync_count = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(100));
        let mut handles = vec![];
        for _ in 0..100 {
            let gc = gc.clone();
            let gc_inner = gc.clone();
            let sc = sync_count.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                gc.group_sync(|| {
                    // Leader waits until the other 99 are queued as followers →
                    // deterministic single batch (see group_commit_concurrent_batching).
                    while gc_inner.waiters() < 99 {
                        std::thread::yield_now();
                    }
                    sc.fetch_add(1, Ordering::SeqCst);
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // All 100 concurrent callers batch behind a single leader sync.
        assert_eq!(
            sync_count.load(Ordering::SeqCst),
            1,
            "all 100 concurrent callers must batch into one sync"
        );
    }

    #[test]
    fn group_commit_no_waiters_initially() {
        let gc = GroupCommitter::new();
        assert_eq!(gc.pending_count(), 0);
        assert_eq!(gc.epoch(), 0);
        assert_eq!(gc.sync_count(), 0);
    }

    #[test]
    fn group_commit_wal_has_group_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path, SyncMode::None).unwrap();
        wal.group_sync();
        // Should work without error
    }

    // ── Commit-time durability: LSN-aware group commit ──────────────────

    #[test]
    fn sync_covering_reports_last_appended_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(&dir.path().join("t.wal"), SyncMode::Fsync).unwrap();
        let page = [0u8; PAGE_SIZE];
        let mut last = 0;
        for _ in 0..3 {
            last = wal.log_page_write(0, 1, &page).unwrap();
        }
        assert_eq!(wal.sync_covering().unwrap(), last);

        let seg = SegmentedWal::open(&dir.path().join("t.wal.d"), 64 * 1024 * 1024).unwrap();
        let mut last = 0;
        for _ in 0..3 {
            last = seg.log_page_write(0, 1, &page).unwrap();
        }
        assert_eq!(seg.sync_covering().unwrap(), last);
    }

    #[test]
    fn sync_up_to_skips_when_already_covered() {
        let gc = GroupCommitter::new();
        let calls = Arc::new(AtomicU64::new(0));
        // First call covers up to 10.
        let c = calls.clone();
        gc.sync_up_to(5, move || {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(10)
        })
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Target already covered — sync_fn must not run again.
        let c = calls.clone();
        gc.sync_up_to(10, move || {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(10)
        })
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Uncovered target syncs again.
        let c = calls.clone();
        gc.sync_up_to(11, move || {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(20)
        })
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn sync_up_to_never_returns_before_coverage() {
        // Stress: N threads "append" (bump an appended counter) then demand
        // coverage. The sync closure copies appended -> durable. Every thread
        // must observe durable >= its own append mark on return — a follower
        // piggybacking on an fsync that started before its append would trip
        // the assert.
        let gc = Arc::new(GroupCommitter::new());
        let appended = Arc::new(AtomicU64::new(0));
        let durable = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let gc = gc.clone();
            let appended = appended.clone();
            let durable = durable.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    let my_mark = appended.fetch_add(1, Ordering::SeqCst) + 1;
                    let a = appended.clone();
                    let d = durable.clone();
                    gc.sync_up_to(my_mark, move || {
                        // Simulated fsync: everything appended so far is
                        // durable once this returns.
                        let covered = a.load(Ordering::SeqCst);
                        std::thread::yield_now();
                        d.fetch_max(covered, Ordering::SeqCst);
                        Ok(covered)
                    })
                    .unwrap();
                    assert!(
                        durable.load(Ordering::SeqCst) >= my_mark,
                        "sync_up_to returned before the caller's append was durable"
                    );
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Group commit must have batched at least some of the 3200 requests.
        assert!(
            gc.sync_count() < 3200,
            "no batching happened at all ({} syncs)",
            gc.sync_count()
        );
    }

    #[test]
    fn sync_up_to_propagates_leader_error() {
        let gc = GroupCommitter::new();
        let err = gc
            .sync_up_to(1, || Err(std::io::Error::other("disk gone")))
            .unwrap_err();
        assert_eq!(err.to_string(), "disk gone");
        // A later successful sync still works and covers.
        gc.sync_up_to(1, || Ok(5)).unwrap();
    }
}

#[cfg(test)]
mod archive_tests {
    use super::*;

    /// Build an archive of several sealed segments and return their LSN bounds.
    fn archived_segments(dir: &Path) -> (std::path::PathBuf, Vec<(u64, u64, u64)>) {
        let wal_dir = dir.join("t.wal.d");
        let archive = dir.join("archive");
        // Segments must hold SEVERAL records, not one. With one LSN per segment
        // a horizon can never fall strictly inside a segment, and the assertion
        // this fixture exists to support -- that the segment CONTAINING the
        // horizon survives -- passes against a deliberately wrong
        // implementation. It did, on the first version of this test.
        let wal =
            SegmentedWal::open_with_archive(&wal_dir, 24 * 1024, SyncMode::None, &archive).unwrap();
        let page = [3u8; PAGE_SIZE];
        for txn in 1..=18u64 {
            wal.log_page_write(txn, txn as u32, &page).unwrap();
            wal.log_commit(txn).unwrap();
        }
        wal.sync().unwrap();
        wal.archive_active().unwrap();
        let mut out = Vec::new();
        for seg in list_archive_segments(&archive).unwrap() {
            if let Some((lo, hi)) = segment_lsn_bounds(&segment_path(&archive, seg)) {
                out.push((seg, lo, hi));
            }
        }
        // Ascending by segment number. `list_archive_segments` does not promise
        // an order, and assuming one made the first version of this test assert
        // against the wrong segment.
        out.sort_by_key(|(seg, _, _)| *seg);
        (archive, out)
    }

    /// Pruning must never remove a segment a restore to the horizon needs.
    ///
    /// Continuous archiving copied every sealed segment in and nothing ever
    /// took one out, so the archive grew without bound. That is the gap this
    /// closes -- but the dangerous direction is the fix, not the bug: deleting
    /// one segment too many turns a disk-space problem into an unrecoverable
    /// one, silently, and only during an actual recovery.
    ///
    /// The load-bearing assertion is the *second* one. Removing everything
    /// below the horizon is easy; keeping the segment that CONTAINS the horizon
    /// is the part that a naive `max_lsn <= keep_from` would get wrong.
    #[test]
    fn prune_archive_keeps_the_segment_containing_the_horizon() {
        let dir = tempfile::tempdir().unwrap();
        let (archive, segs) = archived_segments(dir.path());
        assert!(
            segs.len() >= 3,
            "need several archived segments to prune between, got {segs:?}"
        );

        // Aim at an LSN in the middle of the second segment.
        let (target_seg, lo, hi) = segs[1];
        let horizon = lo + (hi - lo) / 2;

        let report = prune_archive(&archive, horizon).unwrap();

        assert!(
            report.removed.contains(&segs[0].0),
            "a segment entirely below the horizon must go: {report:?}"
        );
        assert!(
            !report.removed.contains(&target_seg),
            "the segment CONTAINING the horizon must be kept -- a restore to \
             LSN {horizon} has to replay it: {report:?}"
        );
        for (seg, seg_lo, _) in &segs[2..] {
            assert!(
                !report.removed.contains(seg),
                "segment {seg} starts at {seg_lo}, above the horizon, and must be kept"
            );
        }

        // The files on disk agree with the report.
        let left: std::collections::HashSet<u64> = list_archive_segments(&archive)
            .unwrap()
            .into_iter()
            .collect();
        for seg in &report.removed {
            assert!(
                !left.contains(seg),
                "segment {seg} reported removed but still present"
            );
        }
        assert_eq!(
            left.len(),
            report.kept,
            "kept count disagrees with the directory"
        );

        // The index no longer refers to files that are gone, or a later
        // time-based restore resolves onto a segment that does not exist.
        let idx = std::fs::read_to_string(archive.join(ARCHIVE_INDEX_NAME)).unwrap_or_default();
        for seg in &report.removed {
            for line in idx.lines() {
                let first = line
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u64>().ok());
                assert_ne!(first, Some(*seg), "index still lists removed segment {seg}");
            }
        }
    }

    /// A horizon below everything must delete nothing. The negative control:
    /// without it, a prune that removed everything unconditionally would still
    /// pass the test above.
    #[test]
    fn prune_archive_with_a_horizon_below_everything_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let (archive, segs) = archived_segments(dir.path());
        let before = list_archive_segments(&archive).unwrap();

        let report = prune_archive(&archive, 0).unwrap();

        assert!(
            report.removed.is_empty(),
            "nothing is below LSN 0: {report:?}"
        );
        assert_eq!(report.bytes_freed, 0);
        assert_eq!(list_archive_segments(&archive).unwrap(), before);
        let true_min = segs.iter().map(|(_, lo, _)| *lo).min().unwrap();
        assert_eq!(report.oldest_retained_lsn, Some(true_min));
    }

    // A segment that has not filled is not in the archive, so everything
    // committed into it is unreachable by PITR. That is the whole gap: at the
    // default 64 MiB a quiet database can go days without a rollover, and
    // `restore-pitr` replays what it has and reports success either way, so the
    // loss only shows up during an actual recovery. `archive_active` is what a
    // clean shutdown and the archive timeout call to close it.
    #[test]
    fn archive_active_seals_a_partial_segment() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("t.wal.d");
        let archive = dir.path().join("archive");

        // A segment far larger than what gets written, so nothing rotates.
        let wal =
            SegmentedWal::open_with_archive(&wal_dir, 64 * 1024 * 1024, SyncMode::None, &archive)
                .unwrap();

        let page = [7u8; PAGE_SIZE];
        wal.log_page_write(1, 1, &page).unwrap();
        wal.log_commit(1).unwrap();
        wal.sync().unwrap();

        assert!(
            list_archive_segments(&archive)
                .unwrap_or_default()
                .is_empty(),
            "a partial segment must not be archived on its own — if it were, \
             this test would not be measuring anything"
        );

        assert!(
            wal.archive_active().unwrap(),
            "archive_active must report that it sealed a segment holding commits"
        );
        assert!(
            !list_archive_segments(&archive)
                .unwrap_or_default()
                .is_empty(),
            "committed records were left out of the archive, so PITR cannot reach them"
        );

        // Idempotent: with nothing newly written there is nothing to seal, and
        // saying so keeps a timer from filling the archive with empty segments.
        assert!(
            !wal.archive_active().unwrap(),
            "an empty active segment must not be archived"
        );
    }

    // Without an archive configured there is nothing to archive to, and the
    // answer must be `false` rather than an optimistic `true`: a caller uses
    // this to decide whether the tail is safe.
    #[test]
    fn archive_active_reports_false_without_an_archive() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("t.wal.d");
        let wal =
            SegmentedWal::open_with_sync_mode(&wal_dir, 64 * 1024 * 1024, SyncMode::None).unwrap();

        let page = [7u8; PAGE_SIZE];
        wal.log_page_write(1, 1, &page).unwrap();
        wal.sync().unwrap();

        assert!(!wal.archive_active().unwrap());
    }
}
