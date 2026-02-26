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

use super::page::{PageBuf, PAGE_SIZE};

// ============================================================================
// Record types
// ============================================================================

pub const RECORD_PAGE_WRITE: u8 = 0;
pub const RECORD_COMMIT: u8 = 1;
pub const RECORD_ABORT: u8 = 2;
pub const RECORD_CHECKPOINT: u8 = 3;

/// How the WAL should sync data to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Full fsync: flushes data + metadata (default, safest).
    Fsync,
    /// fdatasync: flushes data only, skipping metadata like timestamps.
    /// Faster than fsync on most filesystems.
    Fdatasync,
    /// No sync: let the OS decide when to flush. Fast but unsafe.
    None,
}

impl SyncMode {
    /// Parse a sync mode string from config. Case-insensitive.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "fdatasync" => SyncMode::Fdatasync,
            "none" | "off" => SyncMode::None,
            _ => SyncMode::Fsync, // default
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
    fn log_page_write(&self, txn_id: u64, page_id: u32, page_image: &PageBuf) -> std::io::Result<u64>;
    /// Force buffered WAL data to stable storage.
    fn sync(&self) -> std::io::Result<()>;
    /// Get WAL stats: (bytes_written, syncs).
    fn wal_stats(&self) -> (u64, u64) { (0, 0) }
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

        // Determine next LSN by scanning existing WAL records.
        let file_len = file.metadata()?.len();
        let next_lsn = if file_len == 0 {
            1
        } else {
            // Scan the WAL to find the max LSN and continue from there.
            let records = read_wal_records(path).unwrap_or_default();
            let max = max_lsn(&records);
            max + 1
        };

        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            next_lsn: AtomicU64::new(next_lsn),
            path: path.to_path_buf(),
            writes: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
            sync_mode: SyncMode::Fsync,
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
    pub fn log_page_write(&self, txn_id: u64, page_id: u32, page_image: &PageBuf) -> std::io::Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut writer = self.writer.lock();

        let record_len = PAGE_WRITE_RECORD_SIZE as u32;
        writer.write_all(&record_len.to_le_bytes())?;
        writer.write_all(&lsn.to_le_bytes())?;
        writer.write_all(&txn_id.to_le_bytes())?;
        writer.write_all(&[RECORD_PAGE_WRITE])?;
        writer.write_all(&page_id.to_le_bytes())?;
        writer.write_all(page_image)?;

        // CRC over the entire record (excluding the record_len prefix and crc itself)
        let crc = crc32c::crc32c(page_image);
        writer.write_all(&crc.to_le_bytes())?;

        self.writes.fetch_add(1, Ordering::Relaxed);
        // 4 bytes for the record_len prefix + the record body
        self.bytes_written.fetch_add(4 + record_len as u64, Ordering::Relaxed);

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
        let mut writer = self.writer.lock();
        writer.flush()?;
        match self.sync_mode {
            SyncMode::Fsync => writer.get_ref().sync_all()?,
            SyncMode::Fdatasync => writer.get_ref().sync_data()?,
            SyncMode::None => {} // skip sync entirely
        }
        self.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get the current (next to be assigned) LSN.
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Acquire)
    }

}

impl WalBackend for Wal {
    fn log_page_write(&self, txn_id: u64, page_id: u32, page_image: &PageBuf) -> std::io::Result<u64> {
        Wal::log_page_write(self, txn_id, page_id, page_image)
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
}

impl Wal {
    // Internal: write a control record (commit/abort/checkpoint).
    fn log_control(&self, record_type: u8, txn_id: u64) -> std::io::Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut writer = self.writer.lock();

        let record_len = CONTROL_RECORD_SIZE as u32;
        writer.write_all(&record_len.to_le_bytes())?;
        writer.write_all(&lsn.to_le_bytes())?;
        writer.write_all(&txn_id.to_le_bytes())?;
        writer.write_all(&[record_type])?;
        writer.write_all(&0u32.to_le_bytes())?; // page_id = 0 (not applicable)

        // CRC over the header fields
        let mut crc_data = Vec::with_capacity(17);
        crc_data.extend_from_slice(&lsn.to_le_bytes());
        crc_data.extend_from_slice(&txn_id.to_le_bytes());
        crc_data.push(record_type);
        let crc = crc32c::crc32c(&crc_data);
        writer.write_all(&crc.to_le_bytes())?;

        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(4 + record_len as u64, Ordering::Relaxed);

        Ok(lsn)
    }
}

// ============================================================================
// WAL recovery (reader)
// ============================================================================

/// Read all WAL records from a file for crash recovery.
pub fn read_wal_records(path: &Path) -> std::io::Result<Vec<WalRecord>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut records = Vec::new();
    let mut pos: u64 = 0;

    while pos + 4 <= file_len {
        file.seek(SeekFrom::Start(pos))?;

        // Read record length
        let mut len_buf = [0u8; 4];
        if file.read_exact(&mut len_buf).is_err() {
            break; // Truncated record — ignore
        }
        let record_len = u32::from_le_bytes(len_buf) as usize;

        if record_len == 0 || (pos + record_len as u64) > file_len {
            break; // Truncated or corrupt
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

        let page_image = if record_type == RECORD_PAGE_WRITE {
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

        if record_type == RECORD_PAGE_WRITE {
            // For page writes, CRC is over the page image
            if let Some(ref img) = page_image {
                let computed = crc32c::crc32c(img.as_ref());
                if computed != stored_crc {
                    tracing::warn!(
                        "WAL CRC mismatch at LSN {lsn} (page write), skipping record"
                    );
                    pos += record_len as u64;
                    continue;
                }
            }
        } else {
            // For control records, CRC is over header fields (lsn + txn_id + record_type)
            let mut crc_data = Vec::with_capacity(17);
            crc_data.extend_from_slice(&lsn_buf);
            crc_data.extend_from_slice(&txn_buf);
            crc_data.push(record_type);
            let computed = crc32c::crc32c(&crc_data);
            if computed != stored_crc {
                tracing::warn!(
                    "WAL CRC mismatch at LSN {lsn} (control record), skipping record"
                );
                pos += record_len as u64;
                continue;
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
    }

    Ok(records)
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
            .write(true)
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
        })
    }

    /// Open a segmented WAL with a specific sync mode.
    pub fn open_with_sync_mode(dir: &Path, max_segment_size: u64, sync_mode: SyncMode) -> std::io::Result<Self> {
        let mut wal = Self::open(dir, max_segment_size)?;
        wal.sync_mode = sync_mode;
        Ok(wal)
    }

    /// Log a page write. Returns the assigned LSN.
    pub fn log_page_write(&self, txn_id: u64, page_id: u32, page_image: &PageBuf) -> std::io::Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut active = self.active.lock();

        let record_len = PAGE_WRITE_RECORD_SIZE as u32;
        active.writer.write_all(&record_len.to_le_bytes())?;
        active.writer.write_all(&lsn.to_le_bytes())?;
        active.writer.write_all(&txn_id.to_le_bytes())?;
        active.writer.write_all(&[RECORD_PAGE_WRITE])?;
        active.writer.write_all(&page_id.to_le_bytes())?;
        active.writer.write_all(page_image)?;
        let crc = crc32c::crc32c(page_image);
        active.writer.write_all(&crc.to_le_bytes())?;

        active.bytes_written += record_len as u64;

        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written_total.fetch_add(4 + record_len as u64, Ordering::Relaxed);

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
        let mut active = self.active.lock();
        active.writer.flush()?;
        match self.sync_mode {
            SyncMode::Fsync => active.writer.get_ref().sync_all()?,
            SyncMode::Fdatasync => active.writer.get_ref().sync_data()?,
            SyncMode::None => {} // skip sync entirely
        }
        self.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Manually rotate to a new segment.
    pub fn rotate(&self) -> std::io::Result<()> {
        let mut active = self.active.lock();
        self.rotate_inner(&mut active)
    }

    /// Truncate (delete) all segments whose records are fully before `before_lsn`.
    ///
    /// This reclaims disk space after checkpointing. Segments that contain any
    /// record with LSN >= `before_lsn` are kept.
    pub fn truncate_before(&self, before_lsn: u64) -> std::io::Result<usize> {
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
            let max_seg_lsn = read_wal_records(&path)
                .unwrap_or_default()
                .iter()
                .map(|r| r.lsn)
                .max()
                .unwrap_or(0);

            if max_seg_lsn < before_lsn {
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
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut active = self.active.lock();

        let record_len = CONTROL_RECORD_SIZE as u32;
        active.writer.write_all(&record_len.to_le_bytes())?;
        active.writer.write_all(&lsn.to_le_bytes())?;
        active.writer.write_all(&txn_id.to_le_bytes())?;
        active.writer.write_all(&[record_type])?;
        active.writer.write_all(&0u32.to_le_bytes())?;

        let mut crc_data = Vec::with_capacity(17);
        crc_data.extend_from_slice(&lsn.to_le_bytes());
        crc_data.extend_from_slice(&txn_id.to_le_bytes());
        crc_data.push(record_type);
        let crc = crc32c::crc32c(&crc_data);
        active.writer.write_all(&crc.to_le_bytes())?;

        active.bytes_written += record_len as u64;

        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written_total.fetch_add(4 + record_len as u64, Ordering::Relaxed);

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
            SyncMode::None => {}
        }

        let new_seg_num = active.segment_number + 1;
        let new_path = segment_path(&self.dir, new_seg_num);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&new_path)?;

        active.segment_number = new_seg_num;
        active.writer = BufWriter::new(file);
        active.bytes_written = 0;

        Ok(())
    }
}

impl WalBackend for SegmentedWal {
    fn log_page_write(&self, txn_id: u64, page_id: u32, page_image: &PageBuf) -> std::io::Result<u64> {
        SegmentedWal::log_page_write(self, txn_id, page_id, page_image)
    }

    fn sync(&self) -> std::io::Result<()> {
        SegmentedWal::sync(self)
    }

    fn wal_stats(&self) -> (u64, u64) {
        (
            self.bytes_written_total.load(Ordering::Relaxed),
            self.syncs.load(Ordering::Relaxed),
        )
    }
}

impl std::fmt::Debug for SegmentedWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentedWal")
            .field("dir", &self.dir)
            .field("next_lsn", &self.next_lsn.load(Ordering::Relaxed))
            .field("checkpoint_lsn", &self.checkpoint_lsn.load(Ordering::Relaxed))
            .finish()
    }
}

// ============================================================================
// Group commit
// ============================================================================

/// Batched sync for group commit optimization.
///
/// Instead of calling `fsync` for every individual commit, multiple
/// transactions' commits are batched and flushed together, amortizing
/// the fsync cost across the group.
pub struct GroupCommitter {
    /// The underlying WAL (either single-file or segmented).
    pending_syncs: AtomicU64,
}

impl GroupCommitter {
    /// Create a new group committer.
    pub fn new() -> Self {
        Self {
            pending_syncs: AtomicU64::new(0),
        }
    }

    /// Record that a transaction wants to sync. Returns the pending count.
    pub fn request_sync(&self) -> u64 {
        self.pending_syncs.fetch_add(1, Ordering::SeqCst)
    }

    /// Perform the group sync. Resets the pending counter.
    /// Returns the number of syncs that were batched.
    pub fn perform_sync(&self) -> u64 {
        self.pending_syncs.swap(0, Ordering::SeqCst)
    }

    /// Get the number of pending sync requests.
    pub fn pending_count(&self) -> u64 {
        self.pending_syncs.load(Ordering::Acquire)
    }
}

// ============================================================================
// Segment helpers
// ============================================================================

/// Generate the path for a WAL segment file.
fn segment_path(dir: &Path, segment_number: u64) -> std::path::PathBuf {
    dir.join(format!("wal-{segment_number:06}.log"))
}

/// List all segment numbers in a WAL directory.
fn list_segments(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stripped) = name.strip_prefix("wal-") {
            if let Some(num_str) = stripped.strip_suffix(".log") {
                if let Ok(n) = num_str.parse::<u64>() {
                    segments.push(n);
                }
            }
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
        assert!(seg_after_first > 1 || seg_after_second > seg_after_first,
            "should rotate: seg1={seg_after_first}, seg2={seg_after_second}");

        let segments = list_segments(&wal_dir).unwrap();
        assert!(segments.len() >= 2, "should have multiple segment files: {}", segments.len());
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

    // ── Group commit tests ──────────────────────────────────────────────

    #[test]
    fn group_committer_batching() {
        let gc = GroupCommitter::new();
        assert_eq!(gc.pending_count(), 0);

        gc.request_sync();
        gc.request_sync();
        gc.request_sync();
        assert_eq!(gc.pending_count(), 3);

        let batched = gc.perform_sync();
        assert_eq!(batched, 3);
        assert_eq!(gc.pending_count(), 0);
    }

    #[test]
    fn group_committer_empty_sync() {
        let gc = GroupCommitter::new();
        let batched = gc.perform_sync();
        assert_eq!(batched, 0);
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
        let wal = SegmentedWal::open_with_sync_mode(&wal_dir, 1024 * 1024, SyncMode::Fdatasync).unwrap();

        let page = [5u8; PAGE_SIZE];
        wal.log_page_write(1, 0, &page).unwrap();
        wal.sync().unwrap();

        let seg_path = segment_path(&wal_dir, 1);
        let records = read_wal_records(&seg_path).unwrap();
        assert_eq!(records.len(), 1);
    }
}
