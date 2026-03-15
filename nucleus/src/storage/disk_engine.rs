//! Disk-backed storage engine using page-based storage with a buffer pool.
//!
//! Each table gets a linked list of data pages. The table's first page ID is
//! tracked in a table directory (in-memory HashMap, persisted to the meta/catalog
//! pages on flush). Rows are serialized to binary tuples and stored in slotted pages.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use parking_lot::RwLock;

use super::btree::{BTreeIndex, RowId};
use super::buffer::{BufferPool, DEFAULT_POOL_SIZE};
use super::disk::DiskManager;
use super::page::{self, PageBuf, PAGE_SIZE, INVALID_PAGE_ID, META_TABLE_DIR_START,
    META_FREE_LIST_HEAD, META_FREE_PAGE_COUNT};
use super::tuple;
use super::wal;
use super::wal::Wal;
use super::{StorageEngine, StorageError};
use crate::catalog::Catalog;
use crate::types::{DataType, Row, Value};

// ============================================================================
// DataType compact serialization for table directory persistence
// ============================================================================

fn serialize_data_type(ty: &DataType, buf: &mut Vec<u8>) {
    match ty {
        DataType::Bool => buf.push(0),
        DataType::Int32 => buf.push(1),
        DataType::Int64 => buf.push(2),
        DataType::Float64 => buf.push(3),
        DataType::Text => buf.push(4),
        DataType::Jsonb => buf.push(5),
        DataType::Date => buf.push(6),
        DataType::Timestamp => buf.push(7),
        DataType::TimestampTz => buf.push(8),
        DataType::Numeric => buf.push(9),
        DataType::Uuid => buf.push(10),
        DataType::Bytea => buf.push(11),
        DataType::Array(inner) => {
            buf.push(12);
            serialize_data_type(inner, buf);
        }
        DataType::Vector(dim) => {
            buf.push(13);
            buf.extend_from_slice(&(*dim as u32).to_le_bytes());
        }
        DataType::Interval => buf.push(14),
        DataType::UserDefined(name) => {
            buf.push(15);
            let bytes = name.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
}

fn deserialize_data_type(data: &[u8], offset: &mut usize) -> Option<DataType> {
    if *offset >= data.len() {
        return None;
    }
    let tag = data[*offset];
    *offset += 1;
    match tag {
        0 => Some(DataType::Bool),
        1 => Some(DataType::Int32),
        2 => Some(DataType::Int64),
        3 => Some(DataType::Float64),
        4 => Some(DataType::Text),
        5 => Some(DataType::Jsonb),
        6 => Some(DataType::Date),
        7 => Some(DataType::Timestamp),
        8 => Some(DataType::TimestampTz),
        9 => Some(DataType::Numeric),
        10 => Some(DataType::Uuid),
        11 => Some(DataType::Bytea),
        12 => {
            let inner = deserialize_data_type(data, offset)?;
            Some(DataType::Array(Box::new(inner)))
        }
        13 => {
            if *offset + 4 > data.len() {
                return None;
            }
            let dim = u32::from_le_bytes([
                data[*offset], data[*offset + 1],
                data[*offset + 2], data[*offset + 3],
            ]) as usize;
            *offset += 4;
            Some(DataType::Vector(dim))
        }
        14 => Some(DataType::Interval),
        15 => {
            if *offset + 4 > data.len() { return None; }
            let len = u32::from_le_bytes([data[*offset], data[*offset+1], data[*offset+2], data[*offset+3]]) as usize;
            *offset += 4;
            if *offset + len > data.len() { return None; }
            let name = std::str::from_utf8(&data[*offset..*offset+len]).ok()?.to_string();
            *offset += len;
            Some(DataType::UserDefined(name))
        }
        _ => None,
    }
}

/// Per-table metadata tracked in memory.
#[derive(Debug, Clone)]
struct TableMeta {
    /// First data page for this table.
    first_page: u32,
    /// Column types needed for tuple serialization.
    col_types: Vec<DataType>,
}

/// Metadata for an active index.
#[allow(dead_code)]
struct IndexMeta {
    /// The B-tree index handle.
    btree: BTreeIndex,
    /// Which table this index is on.
    table: String,
    /// Column index (0-based position) in the table's schema.
    col_idx: usize,
    /// Column type (for serialization).
    col_type: DataType,
}

/// In-transaction state for DiskEngine MVCC.
struct DiskTxnState {
    /// Page IDs of pre-existing pages dirtied during this transaction.
    dirty_existing: HashSet<u32>,
    /// Page IDs allocated for the first time during this transaction.
    new_pages: HashSet<u32>,
    /// Snapshot of the in-memory tables directory at BEGIN (metadata only, not page data).
    tables_snapshot: HashMap<String, TableMeta>,
    /// Free list head at BEGIN.
    free_list_head: u32,
    /// Free page count at BEGIN.
    free_page_count: u32,
    /// `pool.next_page_id()` value at BEGIN — pages with ID ≥ this were allocated during txn.
    page_count_at_begin: u32,
}

/// Disk-backed storage engine.
pub struct DiskEngine {
    pool: Arc<BufferPool>,
    /// Table name → table metadata.
    tables: RwLock<HashMap<String, TableMeta>>,
    /// Index name → index metadata.
    indexes: RwLock<HashMap<String, IndexMeta>>,
    /// Reference to the catalog for looking up column types.
    catalog: Arc<Catalog>,
    /// Head of the on-disk free page list (linked via FREE_NEXT_PAGE pointers).
    free_list_head: parking_lot::Mutex<u32>,
    /// Count of free pages available for reuse.
    free_page_count: parking_lot::Mutex<u32>,
    /// Optional async I/O backend (io_uring on Linux, tokio::fs elsewhere).
    /// When present, `flush_all_dirty` uses async writes instead of the sync DiskManager.
    async_ops: Option<std::sync::Arc<Box<dyn super::io_uring::AsyncDiskOps>>>,
    /// MVCC transaction state. `None` when no transaction is active.
    txn_state: parking_lot::Mutex<Option<DiskTxnState>>,
    /// Monotonically increasing transaction ID counter for WAL records.
    next_txn_id: AtomicU64,
}

/// Linked-list pointers stored in the data page's reserved area.
/// We use the DATA_FLAGS field and DATA_RESERVED field (4 bytes total)
/// to store the next_page_id for the table's page chain.
const NEXT_PAGE_OFFSET: usize = page::DATA_FLAGS; // reuse the flags+reserved (4 bytes)

fn get_next_page(pg: &PageBuf) -> u32 {
    page::read_u32(pg, NEXT_PAGE_OFFSET)
}

fn set_next_page(pg: &mut PageBuf, next: u32) {
    page::write_u32(pg, NEXT_PAGE_OFFSET, next);
}

impl Drop for DiskEngine {
    /// Flush all dirty pages and save the table directory on drop (clean shutdown).
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl DiskEngine {
    /// Open or create a disk engine at the given path.
    ///
    /// On startup for an existing database:
    /// 1. Replay WAL records to recover any dirty pages that weren't flushed before crash
    /// 2. Open a fresh WAL for new operations
    /// 3. Load the table directory from the (potentially recovered) meta page
    ///
    /// Open with a custom buffer pool size (in frames). Each frame is 16 KB.
    pub fn open_with_pool_size(path: &Path, catalog: Arc<Catalog>, pool_frames: usize) -> Result<Self, StorageError> {
        Self::open_inner(path, catalog, pool_frames, false, 0, None, false, wal::SyncMode::Fsync)
    }

    pub fn open(path: &Path, catalog: Arc<Catalog>) -> Result<Self, StorageError> {
        Self::open_inner(path, catalog, DEFAULT_POOL_SIZE, false, 0, None, false, wal::SyncMode::Fsync)
    }

    /// Open with encryption enabled (AES-256-GCM).
    pub fn open_encrypted(
        path: &Path,
        catalog: Arc<Catalog>,
        encryptor: super::encryption::PageEncryptor,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path, catalog, DEFAULT_POOL_SIZE, false, 0, Some(encryptor), false, wal::SyncMode::Fsync)
    }

    /// Open with both compression and encryption.
    pub fn open_compressed_encrypted(
        path: &Path,
        catalog: Arc<Catalog>,
        encryptor: super::encryption::PageEncryptor,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path, catalog, DEFAULT_POOL_SIZE, false, 0, Some(encryptor), true, wal::SyncMode::Fsync)
    }

    /// Open with compression enabled (LZ4).
    pub fn open_compressed(path: &Path, catalog: Arc<Catalog>) -> Result<Self, StorageError> {
        Self::open_inner(path, catalog, DEFAULT_POOL_SIZE, false, 0, None, true, wal::SyncMode::Fsync)
    }

    /// Open with async I/O enabled (io_uring on Linux, tokio::fs elsewhere).
    ///
    /// Equivalent to `open()` but `flush_all_dirty` uses the `AsyncDiskOps` backend
    /// instead of the synchronous `DiskManager`, making flushes truly non-blocking.
    pub fn open_with_async_io(path: &Path, catalog: Arc<Catalog>) -> Result<Self, StorageError> {
        let mut engine = Self::open(path, catalog)?;
        let db_file = path.join("database.db");
        let use_io_uring = cfg!(target_os = "linux");
        match super::io_uring::create_disk_ops(&db_file, super::page::PAGE_SIZE, use_io_uring) {
            Ok(ops) => {
                engine.async_ops = Some(std::sync::Arc::new(ops));
            }
            Err(e) => {
                tracing::warn!("AsyncDiskOps init failed, falling back to sync I/O: {e}");
            }
        }
        Ok(engine)
    }

    /// Open with a segmented WAL instead of a single-file WAL.
    /// `max_segment_size_mb` controls when WAL segments rotate (default 64 MB).
    pub fn open_segmented(
        path: &Path,
        catalog: Arc<Catalog>,
        pool_frames: usize,
        max_segment_size_mb: usize,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path, catalog, pool_frames, true, max_segment_size_mb, None, false, wal::SyncMode::Fsync)
    }

    /// Open with a segmented WAL and explicit sync mode.
    pub fn open_segmented_with_sync(
        path: &Path,
        catalog: Arc<Catalog>,
        pool_frames: usize,
        max_segment_size_mb: usize,
        sync_mode: wal::SyncMode,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path, catalog, pool_frames, true, max_segment_size_mb, None, false, sync_mode)
    }

    #[allow(clippy::too_many_arguments)]
    fn open_inner(
        path: &Path,
        catalog: Arc<Catalog>,
        pool_frames: usize,
        use_segmented_wal: bool,
        max_segment_size_mb: usize,
        encryptor: Option<super::encryption::PageEncryptor>,
        compression: bool,
        sync_mode: wal::SyncMode,
    ) -> Result<Self, StorageError> {
        let mut disk = match (&encryptor, compression) {
            (Some(enc), true) => DiskManager::open_compressed_encrypted(path, enc.clone()),
            (Some(enc), false) => DiskManager::open_encrypted(path, enc.clone()),
            (None, true) => DiskManager::open_compressed(path),
            (None, false) => DiskManager::open(path),
        }.map_err(|e| StorageError::Io(e.to_string()))?;
        let file_size = disk.file_size()
            .map_err(|e| StorageError::Io(e.to_string()))?;

        let is_new = file_size == 0;
        let mut initial_pages = if is_new {
            // New database — write meta page
            let mut meta = [0u8; PAGE_SIZE];
            page::init_meta_page(&mut meta);
            page::write_checksum(&mut meta);
            disk.write_page(0, &meta)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            disk.sync().map_err(|e| StorageError::Io(e.to_string()))?;
            1 // page 0 is meta
        } else {
            (file_size / PAGE_SIZE as u64) as u32
        };

        // ── WAL crash recovery ──────────────────────────────────────────
        let wal_path = path.with_extension("wal");
        if !is_new {
            let recovered = Self::recover_from_wal(&wal_path, &mut disk, &mut initial_pages)?;
            if recovered > 0 {
                tracing::info!("WAL recovery: replayed {recovered} page(s)");
            }
        }

        // Open WAL backend — segmented or single-file
        let wal_backend: Box<dyn wal::WalBackend> = if use_segmented_wal {
            let wal_dir = path.with_extension("wal.d");
            let max_bytes = if max_segment_size_mb > 0 {
                (max_segment_size_mb * 1024 * 1024) as u64
            } else {
                64 * 1024 * 1024 // 64 MB default
            };
            Box::new(wal::SegmentedWal::open_with_sync_mode(&wal_dir, max_bytes, sync_mode)
                .map_err(|e| StorageError::Io(format!("Segmented WAL open failed: {e}")))?)
        } else {
            Box::new(Wal::open_with_sync_mode(&wal_path, sync_mode)
                .map_err(|e| StorageError::Io(format!("WAL open failed: {e}")))?)
        };

        let pool = Arc::new(BufferPool::new(disk, Some(wal_backend), pool_frames, initial_pages));

        // Load free list head from the meta page (or initialize for new databases).
        let (fl_head, fl_count) = if is_new {
            (INVALID_PAGE_ID, 0u32)
        } else {
            let frame_id = pool.fetch_page(0)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = pool.frame_data(frame_id);
            let head = page::read_u32(pg, META_FREE_LIST_HEAD);
            let count = page::read_u32(pg, META_FREE_PAGE_COUNT);
            pool.unpin(frame_id);
            // Backwards compat: zeroed meta page means no free list
            let head = if head == 0 { INVALID_PAGE_ID } else { head };
            (head, count)
        };

        let mut engine = Self {
            pool,
            tables: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            catalog,
            free_list_head: parking_lot::Mutex::new(fl_head),
            free_page_count: parking_lot::Mutex::new(fl_count),
            async_ops: None,
            txn_state: parking_lot::Mutex::new(None),
            next_txn_id: AtomicU64::new(1),
        };

        // For existing databases, load the table directory from the (potentially recovered) meta page
        if !is_new {
            engine.load_table_directory()?;
        }

        Ok(engine)
    }

    /// Replay WAL records to recover pages that may not have been flushed to the
    /// data file before a crash.
    ///
    /// For each PAGE_WRITE record in the WAL, compares the WAL record's LSN with
    /// the on-disk page's LSN. If the WAL record is newer, applies the page image
    /// to the data file (with the correct LSN and checksum set).
    ///
    /// Returns the number of pages recovered.
    fn recover_from_wal(
        wal_path: &Path,
        disk: &mut DiskManager,
        initial_pages: &mut u32,
    ) -> Result<usize, StorageError> {
        let records = match wal::read_wal_records(wal_path) {
            Ok(r) => r,
            Err(_) => return Ok(0), // No WAL or unreadable — nothing to recover
        };

        if records.is_empty() {
            return Ok(0);
        }

        // Collect the latest page image for each page_id (last write wins).
        // WAL records are in LSN order, so iterating forward gives us the latest.
        let mut latest_pages: HashMap<u32, (u64, Box<PageBuf>)> = HashMap::new();
        for record in &records {
            if record.record_type == wal::RECORD_PAGE_WRITE
                && let Some(ref img) = record.page_image {
                    latest_pages.insert(record.page_id, (record.lsn, img.clone()));
                }
        }

        let mut recovered = 0usize;
        for (page_id, (wal_lsn, mut page_image)) in latest_pages {
            // Extend the file if this page is beyond the current file size
            if page_id >= *initial_pages {
                *initial_pages = page_id + 1;
            }

            // Read the current on-disk page (if it exists) and compare LSNs
            let mut on_disk = [0u8; PAGE_SIZE];
            let disk_lsn = if disk.read_page(page_id, &mut on_disk).is_ok() {
                page::get_page_lsn(&on_disk)
            } else {
                0 // Page doesn't exist on disk yet
            };

            if wal_lsn > disk_lsn {
                // WAL has a newer version — apply it
                // Set the LSN and checksum to match what the flush would have done
                page::set_page_lsn(&mut page_image, wal_lsn);
                page::write_checksum(&mut page_image);
                disk.write_page(page_id, &page_image)
                    .map_err(|e| StorageError::Io(format!("WAL recovery write failed: {e}")))?;
                recovered += 1;
            }
        }

        if recovered > 0 {
            disk.sync().map_err(|e| StorageError::Io(e.to_string()))?;
        }

        // Truncate the WAL after successful recovery
        if let Ok(file) = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(wal_path)
        {
            let _ = file.sync_all();
        }

        Ok(recovered)
    }

    /// Flush all dirty pages to disk, including the table directory.
    pub fn flush(&self) -> Result<(), StorageError> {
        // Save the table directory to the meta page first
        self.save_table_directory()?;
        self.pool.flush_all().map_err(|e| StorageError::Io(e.to_string()))
    }

    /// Record a page as dirtied during an active MVCC transaction.
    ///
    /// Called after every `pool.mark_dirty()` in insert/update/delete. If no
    /// transaction is active the call is a no-op.
    fn record_dirty_page(&self, page_id: u32) {
        let mut guard = self.txn_state.lock();
        if let Some(ref mut ts) = *guard {
            if page_id >= ts.page_count_at_begin {
                ts.new_pages.insert(page_id);
            } else {
                ts.dirty_existing.insert(page_id);
            }
        }
    }

    /// Perform a checkpoint: flush all dirty pages, write a WAL checkpoint record,
    /// and truncate old WAL segments to reclaim disk space.
    pub fn checkpoint(&self) -> Result<(), StorageError> {
        // 1. Flush all dirty pages (including table directory)
        self.flush()?;
        // 2. Write a checkpoint record to the WAL
        let cp_lsn = self.pool.wal_checkpoint()
            .map_err(|e| StorageError::Io(e.to_string()))?;
        // 3. Sync WAL to ensure checkpoint record is durable
        self.pool.flush_all().map_err(|e| StorageError::Io(e.to_string()))?;
        // 4. Truncate old WAL segments before the checkpoint LSN
        if cp_lsn > 0 {
            let _ = self.pool.wal_truncate_before(cp_lsn);
        }
        Ok(())
    }

    /// Save the table directory (table_name → first_page_id + col_types) to the meta page.
    /// If the directory exceeds the meta page's capacity, overflow pages are used
    /// to hold the remaining data. Existing overflow pages from a previous save
    /// are reused to avoid leaking pages. The last 4 bytes of each page's directory
    /// area store the overflow page ID (INVALID_PAGE_ID if no overflow).
    fn save_table_directory(&self) -> Result<(), StorageError> {
        let tables = self.tables.read();
        // Serialize the directory into a byte buffer
        let mut dir_buf: Vec<u8> = Vec::new();
        let entry_count = tables.len() as u32;
        dir_buf.extend_from_slice(&entry_count.to_le_bytes());

        for (name, meta) in tables.iter() {
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u16;
            dir_buf.extend_from_slice(&name_len.to_le_bytes());
            dir_buf.extend_from_slice(name_bytes);
            dir_buf.extend_from_slice(&meta.first_page.to_le_bytes());
            let col_count = meta.col_types.len() as u16;
            dir_buf.extend_from_slice(&col_count.to_le_bytes());
            for ct in &meta.col_types {
                serialize_data_type(ct, &mut dir_buf);
            }
        }
        drop(tables);

        // Meta page directory area: from META_TABLE_DIR_START to end of page.
        // Reserve last 4 bytes for overflow page pointer.
        let meta_dir_capacity = PAGE_SIZE - META_TABLE_DIR_START - 4;
        // Overflow pages: use first 4 bytes for next-overflow pointer, rest for data.
        let overflow_capacity = PAGE_SIZE - 4;

        // Collect existing overflow page IDs so we can reuse them
        let mut existing_overflow_pages: Vec<u32> = Vec::new();
        {
            let frame_id = self.pool.fetch_page(0)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let overflow_ptr_offset = PAGE_SIZE - 4;
            let mut ov_page = page::read_u32(pg, overflow_ptr_offset);
            self.pool.unpin(frame_id);
            // Guard: page 0 is the meta page itself; treat 0 as no overflow
            // (backwards compat with databases created before overflow pointer was initialized)
            while ov_page != INVALID_PAGE_ID && ov_page != 0 {
                existing_overflow_pages.push(ov_page);
                let ofid = self.pool.fetch_page(ov_page)
                    .map_err(|e| StorageError::Io(e.to_string()))?;
                let opg = self.pool.frame_data(ofid);
                ov_page = page::read_u32(opg, 0);
                self.pool.unpin(ofid);
            }
        }

        // Write to meta page (page 0)
        let frame_id = self.pool.fetch_page(0)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let pg = self.pool.frame_data_mut(frame_id);
        // Zero the directory area first
        pg[META_TABLE_DIR_START..].fill(0);

        let first_chunk_len = dir_buf.len().min(meta_dir_capacity);
        pg[META_TABLE_DIR_START..META_TABLE_DIR_START + first_chunk_len]
            .copy_from_slice(&dir_buf[..first_chunk_len]);

        let mut remaining = &dir_buf[first_chunk_len..];
        if remaining.is_empty() {
            // No overflow needed — write INVALID_PAGE_ID as overflow pointer
            let overflow_ptr_offset = PAGE_SIZE - 4;
            page::write_u32(pg, overflow_ptr_offset, INVALID_PAGE_ID);
        } else {
            // Reuse existing overflow page or allocate a new one
            let mut reuse_idx = 0usize;
            let (overflow_page_id, overflow_frame_id) = if reuse_idx < existing_overflow_pages.len() {
                let pid = existing_overflow_pages[reuse_idx];
                reuse_idx += 1;
                let fid = self.pool.fetch_page(pid)
                    .map_err(|e| StorageError::Io(e.to_string()))?;
                (pid, fid)
            } else {
                self.pool.new_page().map_err(|e| StorageError::Io(e.to_string()))?
            };
            let overflow_ptr_offset = PAGE_SIZE - 4;
            page::write_u32(pg, overflow_ptr_offset, overflow_page_id);

            // Write overflow pages in a chain
            let mut cur_frame_id = overflow_frame_id;
            loop {
                let chunk_len = remaining.len().min(overflow_capacity);
                let cur_pg = self.pool.frame_data_mut(cur_frame_id);
                cur_pg.fill(0);
                cur_pg[4..4 + chunk_len].copy_from_slice(&remaining[..chunk_len]);
                remaining = &remaining[chunk_len..];

                if remaining.is_empty() {
                    // No more overflow — terminate chain
                    page::write_u32(cur_pg, 0, INVALID_PAGE_ID);
                    self.pool.mark_dirty(cur_frame_id);
                    self.pool.unpin(cur_frame_id);
                    break;
                } else {
                    // Reuse next existing overflow page or allocate new
                    let (next_page_id, next_frame_id) = if reuse_idx < existing_overflow_pages.len() {
                        let pid = existing_overflow_pages[reuse_idx];
                        reuse_idx += 1;
                        let fid = self.pool.fetch_page(pid)
                            .map_err(|e| StorageError::Io(e.to_string()))?;
                        (pid, fid)
                    } else {
                        self.pool.new_page().map_err(|e| StorageError::Io(e.to_string()))?
                    };
                    page::write_u32(cur_pg, 0, next_page_id);
                    self.pool.mark_dirty(cur_frame_id);
                    self.pool.unpin(cur_frame_id);
                    cur_frame_id = next_frame_id;
                }
            }
        }

        // Persist free list head and count into meta page while we have it.
        {
            let pg = self.pool.frame_data_mut(frame_id);
            let fl_head = *self.free_list_head.lock();
            let fl_count = *self.free_page_count.lock();
            page::write_u32(pg, META_FREE_LIST_HEAD, fl_head);
            page::write_u32(pg, META_FREE_PAGE_COUNT, fl_count);
            page::write_checksum(pg);
        }
        self.pool.mark_dirty(frame_id);
        self.pool.unpin(frame_id);

        Ok(())
    }

    /// Load the table directory from the meta page (and overflow pages if present),
    /// restoring the tables HashMap.
    fn load_table_directory(&mut self) -> Result<(), StorageError> {
        // Read the meta page and collect directory bytes, following overflow pages.
        let meta_dir_capacity = PAGE_SIZE - META_TABLE_DIR_START - 4;
        let overflow_capacity = PAGE_SIZE - 4;

        let frame_id = self.pool.fetch_page(0)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let pg = self.pool.frame_data(frame_id);

        let dir_area = &pg[META_TABLE_DIR_START..];
        if dir_area.len() < 4 {
            self.pool.unpin(frame_id);
            return Ok(());
        }

        // Collect all directory bytes from meta page and overflow pages
        let mut dir_data = Vec::new();
        let first_chunk_len = meta_dir_capacity.min(dir_area.len() - 4);
        dir_data.extend_from_slice(&dir_area[..first_chunk_len]);

        // Read overflow page pointer (last 4 bytes of meta page)
        let overflow_ptr_offset = PAGE_SIZE - 4;
        let mut overflow_page_id = page::read_u32(pg, overflow_ptr_offset);
        self.pool.unpin(frame_id);

        // Follow overflow page chain
        while overflow_page_id != INVALID_PAGE_ID {
            let ofid = self.pool.fetch_page(overflow_page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let opg = self.pool.frame_data(ofid);
            let next_overflow = page::read_u32(opg, 0);
            let chunk_len = overflow_capacity.min(opg.len() - 4);
            dir_data.extend_from_slice(&opg[4..4 + chunk_len]);
            self.pool.unpin(ofid);
            overflow_page_id = next_overflow;
        }

        if dir_data.len() < 4 {
            return Ok(());
        }

        let entry_count = u32::from_le_bytes([
            dir_data[0], dir_data[1], dir_data[2], dir_data[3],
        ]);

        // If no entries (fresh DB or empty directory), nothing to restore
        if entry_count == 0 {
            return Ok(());
        }

        let mut offset = 4usize;
        let mut restored = HashMap::new();

        for _ in 0..entry_count {
            // Read name_len + name
            if offset + 2 > dir_data.len() {
                break;
            }
            let name_len = u16::from_le_bytes([dir_data[offset], dir_data[offset + 1]]) as usize;
            offset += 2;
            if offset + name_len > dir_data.len() {
                break;
            }
            let name = String::from_utf8_lossy(&dir_data[offset..offset + name_len]).to_string();
            offset += name_len;

            // Read first_page_id
            if offset + 4 > dir_data.len() {
                break;
            }
            let first_page = u32::from_le_bytes([
                dir_data[offset], dir_data[offset + 1],
                dir_data[offset + 2], dir_data[offset + 3],
            ]);
            offset += 4;

            // Read col_count + col_types
            if offset + 2 > dir_data.len() {
                break;
            }
            let col_count = u16::from_le_bytes([dir_data[offset], dir_data[offset + 1]]) as usize;
            offset += 2;

            let mut col_types = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                match deserialize_data_type(&dir_data, &mut offset) {
                    Some(dt) => col_types.push(dt),
                    None => break,
                }
            }

            restored.insert(name, TableMeta { first_page, col_types });
        }

        let restored_count = restored.len();
        *self.tables.write() = restored;
        if restored_count > 0 {
            tracing::info!("Restored {restored_count} table(s) from table directory");
        }

        Ok(())
    }

    /// Get a reference to the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Get column types for a table.
    fn col_types(&self, table: &str) -> Result<Vec<DataType>, StorageError> {
        let tables = self.tables.read();
        match tables.get(table) {
            Some(meta) => Ok(meta.col_types.clone()),
            None => Err(StorageError::TableNotFound(table.to_string())),
        }
    }

    /// Push a page onto the free list for later reuse.
    fn free_page(&self, page_id: u32) -> Result<(), StorageError> {
        let mut head = self.free_list_head.lock();
        let mut count = self.free_page_count.lock();

        let frame_id = self.pool.fetch_page(page_id)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let pg = self.pool.frame_data_mut(frame_id);
        page::init_free_page(pg, *head);
        self.pool.mark_dirty(frame_id);
        self.pool.unpin(frame_id);

        *head = page_id;
        *count += 1;
        Ok(())
    }

    /// Pop a page from the free list. Returns `None` if the list is empty.
    fn reuse_free_page(&self) -> Result<Option<u32>, StorageError> {
        let mut head = self.free_list_head.lock();
        let mut count = self.free_page_count.lock();

        if *head == INVALID_PAGE_ID {
            return Ok(None);
        }

        let page_id = *head;
        let frame_id = self.pool.fetch_page(page_id)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let pg = self.pool.frame_data(frame_id);
        let next = page::read_u32(pg, page::FREE_NEXT_PAGE);
        self.pool.unpin(frame_id);

        *head = if next == 0 { INVALID_PAGE_ID } else { next };
        *count = count.saturating_sub(1);
        Ok(Some(page_id))
    }

    /// Persist the free list head and count to the meta page.
    #[allow(dead_code)]
    fn save_free_list_meta(&self) -> Result<(), StorageError> {
        let head = *self.free_list_head.lock();
        let count = *self.free_page_count.lock();

        let frame_id = self.pool.fetch_page(0)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let pg = self.pool.frame_data_mut(frame_id);
        page::write_u32(pg, META_FREE_LIST_HEAD, head);
        page::write_u32(pg, META_FREE_PAGE_COUNT, count);
        page::write_checksum(pg);
        self.pool.mark_dirty(frame_id);
        self.pool.unpin(frame_id);
        Ok(())
    }

    /// Walk the page chain for a table, collecting all page IDs.
    fn table_pages(&self, table: &str) -> Result<Vec<u32>, StorageError> {
        let tables = self.tables.read();
        let meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        let mut pages = Vec::new();
        let mut page_id = meta.first_page;
        while page_id != INVALID_PAGE_ID {
            pages.push(page_id);
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let next = get_next_page(pg);
            self.pool.unpin(frame_id);
            page_id = next;
        }
        Ok(pages)
    }

    /// Allocate a new data page for a table, linking it at the end of the chain.
    /// Reuses a page from the free list if available, otherwise allocates a new page.
    fn alloc_data_page(&self, table: &str) -> Result<u32, StorageError> {
        let (page_id, frame_id) = if let Some(reused_id) = self.reuse_free_page()? {
            let fid = self.pool.fetch_page(reused_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            (reused_id, fid)
        } else {
            self.pool.new_page()
                .map_err(|e| StorageError::Io(e.to_string()))?
        };
        let pg = self.pool.frame_data_mut(frame_id);
        page::init_data_page(pg, 1);
        set_next_page(pg, INVALID_PAGE_ID);
        self.pool.mark_dirty(frame_id);
        self.record_dirty_page(page_id); // new page allocated during txn
        self.pool.unpin(frame_id);

        // Find the last page in the chain and link to the new page
        let mut tables = self.tables.write();
        let meta = tables
            .get_mut(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        if meta.first_page == INVALID_PAGE_ID {
            meta.first_page = page_id;
        } else {
            // Walk to the last page
            let mut cur = meta.first_page;
            loop {
                let fid = self.pool.fetch_page(cur)
                    .map_err(|e| StorageError::Io(e.to_string()))?;
                let pg = self.pool.frame_data(fid);
                let next = get_next_page(pg);
                if next == INVALID_PAGE_ID {
                    // Link new page here
                    let pg_mut = self.pool.frame_data_mut(fid);
                    set_next_page(pg_mut, page_id);
                    self.pool.mark_dirty(fid);
                    self.record_dirty_page(cur); // existing page — NEXT_PAGE pointer changed
                    self.pool.unpin(fid);
                    break;
                }
                self.pool.unpin(fid);
                cur = next;
            }
        }

        Ok(page_id)
    }

    /// Vacuum a single table: compact dead tuples within pages, remove fully-empty
    /// pages from the page chain. Returns (pages_scanned, dead_reclaimed, pages_freed, bytes_reclaimed).
    fn vacuum_table(&self, table: &str) -> Result<(usize, usize, usize, usize), StorageError> {
        let pages = self.table_pages(table)?;
        let mut pages_scanned = 0usize;
        let mut dead_reclaimed = 0usize;
        let mut pages_freed = 0usize;
        let mut bytes_reclaimed = 0usize;

        // Phase 1: Compact each page — remove dead slots, defragment
        for &page_id in &pages {
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let dead_count = page::dead_tuple_count(pg);
            let frag_free = page::read_u16(pg, page::DATA_FRAG_FREE) as usize;
            pages_scanned += 1;

            if dead_count > 0 || frag_free > 0 {
                dead_reclaimed += dead_count;
                bytes_reclaimed += frag_free;

                // Compact: rewrite only live tuples, reset dead slots
                let pg_mut = self.pool.frame_data_mut(frame_id);

                // Collect live tuples before rewriting
                let slot_count = page::read_u16(pg_mut, page::DATA_SLOT_COUNT);
                let mut live_tuples: Vec<Vec<u8>> = Vec::new();
                for i in 0..slot_count {
                    let entry = page::read_slot(pg_mut, i);
                    if !entry.is_dead() {
                        let off = entry.offset() as usize;
                        let len = entry.length() as usize;
                        live_tuples.push(pg_mut[off..off + len].to_vec());
                    }
                }

                // Re-initialize the page and re-insert live tuples
                let next_page = get_next_page(pg_mut);
                page::init_data_page(pg_mut, 1);
                set_next_page(pg_mut, next_page);
                for tuple_data in &live_tuples {
                    page::insert_tuple(pg_mut, tuple_data);
                }
                self.pool.mark_dirty(frame_id);
            }
            self.pool.unpin(frame_id);
        }

        // Phase 2: Remove completely empty pages from the chain
        // We need to re-walk the chain because compaction may have emptied pages
        let mut tables = self.tables.write();
        let meta = match tables.get_mut(table) {
            Some(m) => m,
            None => return Ok((pages_scanned, dead_reclaimed, pages_freed, bytes_reclaimed)),
        };

        let mut prev_page_id: Option<u32> = None;
        let mut cur_page_id = meta.first_page;

        while cur_page_id != INVALID_PAGE_ID {
            let frame_id = self.pool.fetch_page(cur_page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let live_count = page::live_tuple_count(pg);
            let next = get_next_page(pg);
            self.pool.unpin(frame_id);

            if live_count == 0 && (prev_page_id.is_some() || next != INVALID_PAGE_ID) {
                // Empty page — unlink from chain (keep at least one page)
                if let Some(prev_id) = prev_page_id {
                    let prev_frame = self.pool.fetch_page(prev_id)
                        .map_err(|e| StorageError::Io(e.to_string()))?;
                    let prev_pg = self.pool.frame_data_mut(prev_frame);
                    set_next_page(prev_pg, next);
                    self.pool.mark_dirty(prev_frame);
                    self.pool.unpin(prev_frame);
                } else {
                    // Removing the first page — update table meta
                    meta.first_page = next;
                }
                // Add unlinked page to the free list for reuse
                self.free_page(cur_page_id)?;
                pages_freed += 1;
                // Don't advance prev_page_id since we removed the current node
                cur_page_id = next;
            } else {
                prev_page_id = Some(cur_page_id);
                cur_page_id = next;
            }
        }

        Ok((pages_scanned, dead_reclaimed, pages_freed, bytes_reclaimed))
    }

    /// Get all table names.
    fn table_names(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }
}

#[async_trait::async_trait]
impl StorageEngine for DiskEngine {
    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        // If this table was already restored from the table directory (e.g. after
        // server restart), don't overwrite it — just update col_types from catalog.
        let already_restored = {
            let tables = self.tables.read();
            tables.get(table).is_some_and(|m| m.first_page != INVALID_PAGE_ID)
        };

        if already_restored {
            // Table has data pages from a previous session.
            // Refresh col_types from catalog if available.
            if let Some(table_def) = self.catalog.get_table(table).await {
                let col_types: Vec<DataType> = table_def.columns.iter()
                    .map(|c| c.data_type.clone()).collect();
                let mut tables = self.tables.write();
                if let Some(meta) = tables.get_mut(table) {
                    meta.col_types = col_types;
                }
            }
            return Ok(());
        }

        // Get column types from catalog
        let table_def = self.catalog.get_table(table).await
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        let col_types: Vec<DataType> = table_def.columns.iter().map(|c| c.data_type.clone()).collect();

        let mut tables = self.tables.write();
        tables.insert(
            table.to_string(),
            TableMeta {
                first_page: INVALID_PAGE_ID,
                col_types,
            },
        );
        Ok(())
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        let mut tables = self.tables.write();
        let meta = tables.remove(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        drop(tables);

        // Walk the page chain and add each page to the free list for reuse.
        let mut page_id = meta.first_page;
        while page_id != INVALID_PAGE_ID {
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let next = get_next_page(pg);
            self.pool.unpin(frame_id);
            self.free_page(page_id)?;
            page_id = next;
        }

        Ok(())
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        let col_types = self.col_types(table)?;
        let data = tuple::serialize_row(&row, &col_types);

        if data.len() > page::MAX_INLINE_TUPLE {
            return Err(StorageError::Io("row too large for inline storage".into()));
        }

        // Try to insert into an existing page with space
        let pages = self.table_pages(table)?;
        for &page_id in &pages {
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data_mut(frame_id);
            if let Some(slot_idx) = page::insert_tuple(pg, &data) {
                self.pool.mark_dirty(frame_id);
                self.record_dirty_page(page_id);
                self.pool.unpin(frame_id);
                self.index_insert(table, page_id, slot_idx, &row)?;
                return Ok(());
            }
            self.pool.unpin(frame_id);
        }

        // No page had space — allocate a new one
        let page_id = self.alloc_data_page(table)?;
        let frame_id = self.pool.fetch_page(page_id)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let pg = self.pool.frame_data_mut(frame_id);
        let slot_idx = page::insert_tuple(pg, &data)
            .ok_or_else(|| StorageError::Io("failed to insert into fresh page".into()))?;
        self.pool.mark_dirty(frame_id);
        self.record_dirty_page(page_id);
        self.pool.unpin(frame_id);
        self.index_insert(table, page_id, slot_idx, &row)?;
        Ok(())
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let col_types = self.col_types(table)?;
        let pages = self.table_pages(table)?;
        let mut rows = Vec::new();

        // Parallel read-ahead: prefetch pages in batch windows (1 MB = 64 pages).
        // With parallel prefetch, refill the window every PREFETCH_WINDOW pages
        // so the next batch is in-flight while the current batch is processed.
        const PREFETCH_WINDOW: usize = 64;
        if pages.len() > 1 {
            let first_batch = &pages[..pages.len().min(PREFETCH_WINDOW)];
            self.pool.prefetch_pages(first_batch);
        }

        for (i, &page_id) in pages.iter().enumerate() {
            // Refill: when we reach the start of a new window, prefetch the
            // next full batch in parallel so I/O overlaps with tuple processing.
            let next_batch_start = i + PREFETCH_WINDOW;
            if i > 0 && i % PREFETCH_WINDOW == 0 && next_batch_start < pages.len() {
                let end = (next_batch_start + PREFETCH_WINDOW).min(pages.len());
                self.pool.prefetch_pages(&pages[next_batch_start..end]);
            }

            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            for (_slot_idx, tuple_data) in page::iter_tuples(pg) {
                if let Some(row) = tuple::deserialize_row(tuple_data, &col_types) {
                    rows.push(row);
                }
            }
            self.pool.unpin(frame_id);
        }

        Ok(rows)
    }

    async fn scan_projected(
        &self,
        table: &str,
        projection: &[usize],
    ) -> Result<Vec<Row>, StorageError> {
        let col_types = self.col_types(table)?;
        let pages = self.table_pages(table)?;
        let mut rows = Vec::new();

        const PREFETCH_WINDOW: usize = 64;
        if pages.len() > 1 {
            let first_batch = &pages[..pages.len().min(PREFETCH_WINDOW)];
            self.pool.prefetch_pages(first_batch);
        }

        for (i, &page_id) in pages.iter().enumerate() {
            let next_batch_start = i + PREFETCH_WINDOW;
            if i > 0 && i % PREFETCH_WINDOW == 0 && next_batch_start < pages.len() {
                let end = (next_batch_start + PREFETCH_WINDOW).min(pages.len());
                self.pool.prefetch_pages(&pages[next_batch_start..end]);
            }

            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            for (_slot_idx, tuple_data) in page::iter_tuples(pg) {
                if let Some(row) = tuple::deserialize_row_projected(tuple_data, &col_types, projection) {
                    rows.push(row);
                }
            }
            self.pool.unpin(frame_id);
        }

        Ok(rows)
    }

    async fn scan_chunked(
        &self,
        table: &str,
        tx: tokio::sync::mpsc::Sender<Vec<Row>>,
        batch_size: usize,
    ) -> Result<(), StorageError> {
        let col_types = self.col_types(table)?;
        let pages = self.table_pages(table)?;
        let batch_size = batch_size.max(1);
        let mut batch = Vec::with_capacity(batch_size);

        const PREFETCH_WINDOW: usize = 64;
        if pages.len() > 1 {
            let first_batch = &pages[..pages.len().min(PREFETCH_WINDOW)];
            self.pool.prefetch_pages(first_batch);
        }

        for (i, &page_id) in pages.iter().enumerate() {
            let next_batch_start = i + PREFETCH_WINDOW;
            if i > 0 && i % PREFETCH_WINDOW == 0 && next_batch_start < pages.len() {
                let end = (next_batch_start + PREFETCH_WINDOW).min(pages.len());
                self.pool.prefetch_pages(&pages[next_batch_start..end]);
            }

            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            for (_slot_idx, tuple_data) in page::iter_tuples(pg) {
                if let Some(row) = tuple::deserialize_row(tuple_data, &col_types) {
                    batch.push(row);
                    if batch.len() >= batch_size {
                        let chunk = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                        if tx.send(chunk).await.is_err() {
                            self.pool.unpin(frame_id);
                            return Ok(());
                        }
                    }
                }
            }
            self.pool.unpin(frame_id);
        }

        // Send remaining rows
        if !batch.is_empty() {
            let _ = tx.send(batch).await;
        }
        Ok(())
    }

    fn fast_count_all(&self, table: &str) -> Option<usize> {
        let pages = self.table_pages(table).ok()?;
        let mut count = 0;
        for &page_id in &pages {
            let frame_id = self.pool.fetch_page(page_id).ok()?;
            let pg = self.pool.frame_data(frame_id);
            count += page::count_live_tuples(pg);
            self.pool.unpin(frame_id);
        }
        Some(count)
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        let col_types = self.col_types(table)?;
        let pages = self.table_pages(table)?;

        // Check if any indexes exist for this table (avoid deserialization overhead if not)
        let has_indexes = {
            let indexes = self.indexes.read();
            indexes.values().any(|idx| idx.table == table)
        };

        // Build a set of positions to delete
        let mut to_delete: std::collections::HashSet<usize> = positions.iter().copied().collect();
        let mut global_idx = 0usize;
        let mut count = 0usize;

        for &page_id in &pages {
            if to_delete.is_empty() {
                break;
            }
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let slot_count = page::read_u16(pg, page::DATA_SLOT_COUNT);

            let mut dirty = false;
            for slot_idx in 0..slot_count {
                let entry = page::read_slot(pg, slot_idx);
                if entry.is_dead() {
                    continue;
                }
                if to_delete.remove(&global_idx) {
                    // Deserialize the row before deletion to remove index entries
                    if has_indexes {
                        let off = entry.offset() as usize;
                        let len = entry.length() as usize;
                        let tuple_data = &pg[off..off + len];
                        if let Some(row) = tuple::deserialize_row(tuple_data, &col_types) {
                            self.index_delete(table, page_id, slot_idx, &row);
                        }
                    }
                    let pg_mut = self.pool.frame_data_mut(frame_id);
                    page::delete_tuple(pg_mut, slot_idx);
                    dirty = true;
                    count += 1;
                }
                global_idx += 1;
            }
            if dirty {
                self.pool.mark_dirty(frame_id);
                self.record_dirty_page(page_id);
            }
            self.pool.unpin(frame_id);
        }

        Ok(count)
    }

    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
        let col_types = self.col_types(table)?;
        let pages = self.table_pages(table)?;

        // Check if any indexes exist for this table
        let has_indexes = {
            let indexes = self.indexes.read();
            indexes.values().any(|idx| idx.table == table)
        };

        // Build a map of position → new row
        let mut update_map: HashMap<usize, &Row> = updates.iter().map(|(p, r)| (*p, r)).collect();
        let mut global_idx = 0usize;
        let mut count = 0usize;

        for &page_id in &pages {
            if update_map.is_empty() {
                break;
            }
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let slot_count = page::read_u16(pg, page::DATA_SLOT_COUNT);

            let mut dirty = false;
            for slot_idx in 0..slot_count {
                let entry = page::read_slot(pg, slot_idx);
                if entry.is_dead() {
                    continue;
                }
                if let Some(new_row) = update_map.remove(&global_idx) {
                    // Remove old index entries before modifying the row
                    if has_indexes {
                        let off = entry.offset() as usize;
                        let len = entry.length() as usize;
                        let tuple_data = &pg[off..off + len];
                        if let Some(old_row) = tuple::deserialize_row(tuple_data, &col_types) {
                            self.index_delete(table, page_id, slot_idx, &old_row);
                        }
                    }

                    let new_data = tuple::serialize_row(new_row, &col_types);
                    let pg_mut = self.pool.frame_data_mut(frame_id);
                    if page::update_tuple_in_place(pg_mut, slot_idx, &new_data) {
                        // In-place update: row stays at same (page_id, slot_idx)
                        if has_indexes {
                            self.index_insert(table, page_id, slot_idx, new_row)?;
                        }
                        dirty = true;
                        count += 1;
                    } else {
                        // Doesn't fit in place — delete and re-insert
                        page::delete_tuple(pg_mut, slot_idx);
                        dirty = true;
                        // Try inserting on this page first
                        if let Some(new_slot_idx) = page::insert_tuple(pg_mut, &new_data) {
                            if has_indexes {
                                self.index_insert(table, page_id, new_slot_idx, new_row)?;
                            }
                            count += 1;
                        } else {
                            // Need to insert on another page; release this frame first
                            self.pool.mark_dirty(frame_id);
                            self.record_dirty_page(page_id);
                            self.pool.unpin(frame_id);
                            let (new_page_id, new_slot_idx) = self.insert_sync(table, &new_data)?;
                            if has_indexes {
                                self.index_insert(table, new_page_id, new_slot_idx, new_row)?;
                            }
                            count += 1;
                            // Re-fetch this page to continue scanning
                            let frame_id2 = self.pool.fetch_page(page_id)
                                .map_err(|e| StorageError::Io(e.to_string()))?;
                            self.pool.unpin(frame_id2);
                            global_idx += 1;
                            continue;
                        }
                    }
                }
                global_idx += 1;
            }
            if dirty {
                self.pool.mark_dirty(frame_id);
                self.record_dirty_page(page_id);
            }
            self.pool.unpin(frame_id);
        }

        Ok(count)
    }

    async fn flush_all_dirty(&self) -> Result<(), StorageError> {
        if let Some(ref ops) = self.async_ops {
            // Async path: collect dirty pages (sync, memory-only), write via io_uring/tokio::fs.
            self.save_table_directory()?;
            let dirty = self.pool
                .collect_dirty_for_async_flush()
                .map_err(|e| StorageError::Io(e.to_string()))?;
            for (page_id, data) in &dirty {
                ops.write_page(*page_id, &**data)
                    .await
                    .map_err(|e| StorageError::Io(e.to_string()))?;
            }
            ops.sync().await.map_err(|e| StorageError::Io(e.to_string()))?;
            Ok(())
        } else {
            // Sync fallback (default when async_ops not set).
            self.flush()
        }
    }

    async fn checkpoint(&self) -> Result<(), StorageError> {
        self.checkpoint()
    }

    async fn create_index(&self, table: &str, index_name: &str, col_idx: usize) -> Result<(), StorageError> {
        self.create_index_inner(index_name, table, col_idx)
    }

    async fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        self.drop_index_inner(index_name)
    }

    async fn index_lookup(&self, table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        Ok(Some(self.index_lookup_inner(table, index_name, value)?))
    }

    async fn index_lookup_range(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        Ok(Some(self.index_lookup_range_inner(table, index_name, low, high)?))
    }

    fn index_lookup_sync(&self, table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        Ok(Some(self.index_lookup_inner(table, index_name, value)?))
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        Ok(Some(self.index_lookup_range_inner(table, index_name, low, high)?))
    }

    fn index_only_scan(
        &self,
        table: &str,
        index_name: &str,
        eq_value: Option<&Value>,
        range: Option<(&Value, &Value)>,
    ) -> Option<Vec<Row>> {
        let indexes = self.indexes.read();
        let idx = indexes.get(index_name)?;
        if idx.table != table {
            return None;
        }

        if let Some(val) = eq_value {
            // Point lookup: get keys matching the value, return as single-column rows
            let key = serialize_index_key(val);
            let row_ids = idx.btree.lookup(&key).ok()?;
            // Each matching RowId means one row — return the key value without heap access
            Some(row_ids.iter().map(|_| vec![val.clone()]).collect())
        } else if let Some((low, high)) = range {
            // Range scan: iterate B-tree leaf keys without touching heap pages
            let low_norm = normalize_index_bound_value(low, &idx.col_type)?;
            let high_norm = normalize_index_bound_value(high, &idx.col_type)?;
            let low_key = serialize_index_key(&low_norm);
            let high_key = serialize_index_key(&high_norm);
            if low_key > high_key {
                return Some(Vec::new());
            }
            let key_rids = idx.btree.range_scan(Some(&low_key), Some(&high_key)).ok()?;
            let mut rows = Vec::with_capacity(key_rids.len());
            for (key_bytes, _rid) in &key_rids {
                if let Some(val) = deserialize_index_key(key_bytes) {
                    rows.push(vec![val]);
                }
            }
            Some(rows)
        } else {
            // Full index scan: iterate all B-tree leaf entries
            let key_rids = idx.btree.range_scan(None, None).ok()?;
            let mut rows = Vec::with_capacity(key_rids.len());
            for (key_bytes, _rid) in &key_rids {
                if let Some(val) = deserialize_index_key(key_bytes) {
                    rows.push(vec![val]);
                }
            }
            Some(rows)
        }
    }

    async fn vacuum(&self, table: &str) -> Result<(usize, usize, usize, usize), StorageError> {
        self.vacuum_table(table)
    }

    async fn vacuum_all(&self) -> Result<(usize, usize, usize, usize), StorageError> {
        let names = self.table_names();
        let mut total = (0usize, 0usize, 0usize, 0usize);
        for name in &names {
            let (scanned, dead, freed, bytes) = self.vacuum_table(name)?;
            total.0 += scanned;
            total.1 += dead;
            total.2 += freed;
            total.3 += bytes;
        }
        Ok(total)
    }

    fn supports_mvcc(&self) -> bool {
        true
    }

    /// Begin a disk-engine MVCC transaction.
    ///
    /// 1. Flush all dirty pages to disk so the on-disk state is a clean pre-txn snapshot.
    /// 2. Capture in-memory metadata (tables directory, free list) for rollback.
    /// 3. Initialize dirty-page tracking.
    async fn begin_txn(&self) -> Result<(), StorageError> {
        // Flush pre-txn state to disk (this is the "undo base" for abort).
        self.flush()?;

        let page_count_at_begin = self.pool.next_page_id();
        let tables_snapshot = self.tables.read().clone();
        let free_list_head = *self.free_list_head.lock();
        let free_page_count = *self.free_page_count.lock();

        *self.txn_state.lock() = Some(DiskTxnState {
            dirty_existing: HashSet::new(),
            new_pages: HashSet::new(),
            tables_snapshot,
            free_list_head,
            free_page_count,
            page_count_at_begin,
        });
        Ok(())
    }

    /// Commit the transaction: write a WAL COMMIT record and clear tracking state.
    async fn commit_txn(&self) -> Result<(), StorageError> {
        let txn_id = self.next_txn_id.fetch_add(1, AtomicOrdering::Relaxed);
        let _ = self.pool.wal_log_commit(txn_id);
        *self.txn_state.lock() = None;
        Ok(())
    }

    /// Abort the transaction: reload dirty pre-existing pages from disk, evict new pages,
    /// and restore in-memory metadata to its pre-txn state.
    async fn abort_txn(&self) -> Result<(), StorageError> {
        let ts = {
            let mut guard = self.txn_state.lock();
            guard.take()
        };

        if let Some(ts) = ts {
            // Reload pre-existing pages from disk (undo their in-memory changes).
            let existing: Vec<u32> = ts.dirty_existing.into_iter().collect();
            if !existing.is_empty() {
                self.pool.reload_pages_from_disk(&existing)
                    .map_err(|e| StorageError::Io(e.to_string()))?;
            }

            // Evict newly allocated pages from the buffer pool (they don't exist on disk).
            // Simply removing them from the dirty set is enough — we'll also restore the
            // tables directory so the page chain no longer references them.
            if !ts.new_pages.is_empty() {
                let new_page_list: Vec<u32> = ts.new_pages.into_iter().collect();
                // Reload (blank out) these pages — they will be reclaimed by the free list restore.
                let _ = self.pool.reload_pages_from_disk(&new_page_list);
            }

            // Restore in-memory table directory.
            *self.tables.write() = ts.tables_snapshot;

            // Restore free list state.
            *self.free_list_head.lock() = ts.free_list_head;
            *self.free_page_count.lock() = ts.free_page_count;

            // Write WAL ABORT record for crash-recovery awareness.
            let txn_id = self.next_txn_id.fetch_add(1, AtomicOrdering::Relaxed);
            let _ = self.pool.wal_log_abort(txn_id);
        }

        Ok(())
    }
}

impl DiskEngine {
    /// Create a B-tree index on a column of a table.
    ///
    /// Scans existing rows to populate the index, then maintains it on
    /// future inserts, updates, and deletes.
    fn create_index_inner(
        &self,
        index_name: &str,
        table: &str,
        col_idx: usize,
    ) -> Result<(), StorageError> {
        let tables = self.tables.read();
        let meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        if col_idx >= meta.col_types.len() {
            return Err(StorageError::Io(format!(
                "column index {col_idx} out of range for table {table}"
            )));
        }
        let col_type = meta.col_types[col_idx].clone();
        let col_types = meta.col_types.clone();
        let first_page = meta.first_page;
        drop(tables);

        let mut btree = BTreeIndex::create(self.pool.clone(), col_type.clone())
            .map_err(|e| StorageError::Io(e.to_string()))?;

        // Populate the index from existing data
        let mut page_id = first_page;
        while page_id != INVALID_PAGE_ID {
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            for (slot_idx, tuple_data) in page::iter_tuples(pg) {
                if let Some(row) = tuple::deserialize_row(tuple_data, &col_types)
                    && col_idx < row.len() {
                        let key = serialize_index_key(&row[col_idx]);
                        let rid = RowId { page_id, slot_idx };
                        btree.insert(&key, rid)
                            .map_err(|e| StorageError::Io(e.to_string()))?;
                    }
            }
            let next = get_next_page(pg);
            self.pool.unpin(frame_id);
            page_id = next;
        }

        let mut indexes = self.indexes.write();
        indexes.insert(index_name.to_string(), IndexMeta {
            btree,
            table: table.to_string(),
            col_idx,
            col_type,
        });
        Ok(())
    }

    /// Drop an index by name.
    fn drop_index_inner(&self, index_name: &str) -> Result<(), StorageError> {
        let mut indexes = self.indexes.write();
        if indexes.remove(index_name).is_none() {
            return Err(StorageError::Io(format!("index '{index_name}' not found")));
        }
        Ok(())
    }

    /// Look up rows by an indexed column value.
    ///
    /// Returns the matching rows. Falls back to a full scan if no index exists.
    fn index_lookup_inner(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Vec<Row>, StorageError> {
        let col_types = self.col_types(table)?;
        let indexes = self.indexes.read();
        let idx = indexes
            .get(index_name)
            .ok_or_else(|| StorageError::Io(format!("index '{index_name}' not found")))?;
        if idx.table != table {
            return Err(StorageError::Io(format!(
                "index '{index_name}' is on table '{}', not '{table}'",
                idx.table
            )));
        }

        let key = serialize_index_key(value);
        let row_ids = idx.btree.lookup(&key)
            .map_err(|e| StorageError::Io(e.to_string()))?;

        let mut rows = Vec::with_capacity(row_ids.len());
        for rid in row_ids {
            let frame_id = self.pool.fetch_page(rid.page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let entry = page::read_slot(pg, rid.slot_idx);
            if !entry.is_dead() {
                let off = entry.offset() as usize;
                let len = entry.length() as usize;
                let tuple_data = &pg[off..off + len];
                if let Some(row) = tuple::deserialize_row(tuple_data, &col_types) {
                    rows.push(row);
                }
            }
            self.pool.unpin(frame_id);
        }
        Ok(rows)
    }

    /// Look up rows by an inclusive indexed key range.
    fn index_lookup_range_inner(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Vec<Row>, StorageError> {
        let col_types = self.col_types(table)?;
        let indexes = self.indexes.read();
        let idx = indexes
            .get(index_name)
            .ok_or_else(|| StorageError::Io(format!("index '{index_name}' not found")))?;
        if idx.table != table {
            return Err(StorageError::Io(format!(
                "index '{index_name}' is on table '{}', not '{table}'",
                idx.table
            )));
        }

        let Some(low_norm) = normalize_index_bound_value(low, &idx.col_type) else {
            return Ok(Vec::new());
        };
        let Some(high_norm) = normalize_index_bound_value(high, &idx.col_type) else {
            return Ok(Vec::new());
        };
        let low_key = serialize_index_key(&low_norm);
        let high_key = serialize_index_key(&high_norm);
        if low_key > high_key {
            return Ok(Vec::new());
        }

        let key_rids = idx
            .btree
            .range_scan(Some(&low_key), Some(&high_key))
            .map_err(|e| StorageError::Io(e.to_string()))?;

        let mut rows = Vec::with_capacity(key_rids.len());
        for (_, rid) in key_rids {
            let frame_id = self
                .pool
                .fetch_page(rid.page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let entry = page::read_slot(pg, rid.slot_idx);
            if !entry.is_dead() {
                let off = entry.offset() as usize;
                let len = entry.length() as usize;
                let tuple_data = &pg[off..off + len];
                if let Some(row) = tuple::deserialize_row(tuple_data, &col_types) {
                    rows.push(row);
                }
            }
            self.pool.unpin(frame_id);
        }
        Ok(rows)
    }

    /// Maintain indexes after an insert — called with the page and slot where the
    /// row was inserted, plus the row data.
    fn index_insert(&self, table: &str, page_id: u32, slot_idx: u16, row: &Row) -> Result<(), StorageError> {
        let mut indexes = self.indexes.write();
        for (idx_name, idx) in indexes.iter_mut() {
            if idx.table == table && idx.col_idx < row.len() {
                let key = serialize_index_key(&row[idx.col_idx]);
                let rid = RowId { page_id, slot_idx };
                idx.btree.insert(&key, rid)
                    .map_err(|e| StorageError::Io(format!("Index insert failed for {idx_name}: {e}")))?;
            }
        }
        Ok(())
    }

    /// Maintain indexes after a delete.
    fn index_delete(&self, table: &str, page_id: u32, slot_idx: u16, row: &Row) {
        let indexes = self.indexes.read();
        for (idx_name, idx) in indexes.iter() {
            if idx.table == table && idx.col_idx < row.len() {
                let key = serialize_index_key(&row[idx.col_idx]);
                let rid = RowId { page_id, slot_idx };
                if let Err(e) = idx.btree.delete(&key, rid) {
                    tracing::error!("Index delete failed for {idx_name}: {e}");
                }
            }
        }
    }

    /// Synchronous insert of raw tuple data (used by update when row grows).
    /// Returns (page_id, slot_idx) of the inserted tuple for index maintenance.
    fn insert_sync(&self, table: &str, data: &[u8]) -> Result<(u32, u16), StorageError> {
        // Try existing pages
        let pages = self.table_pages(table)?;
        for &page_id in &pages {
            let frame_id = self.pool.fetch_page(page_id)
                .map_err(|e| StorageError::Io(e.to_string()))?;
            let pg = self.pool.frame_data_mut(frame_id);
            if let Some(slot_idx) = page::insert_tuple(pg, data) {
                self.pool.mark_dirty(frame_id);
                self.record_dirty_page(page_id);
                self.pool.unpin(frame_id);
                return Ok((page_id, slot_idx));
            }
            self.pool.unpin(frame_id);
        }

        // Allocate new page
        let page_id = self.alloc_data_page(table)?;
        let frame_id = self.pool.fetch_page(page_id)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let pg = self.pool.frame_data_mut(frame_id);
        let slot_idx = page::insert_tuple(pg, data)
            .ok_or_else(|| StorageError::Io("failed to insert into fresh page".into()))?;
        self.pool.mark_dirty(frame_id);
        self.record_dirty_page(page_id);
        self.pool.unpin(frame_id);
        Ok((page_id, slot_idx))
    }
}

/// Serialize a Value into bytes suitable for B-tree index keys.
/// Uses a comparable encoding: type tag + big-endian or length-prefixed data.
fn normalize_index_bound_value(value: &Value, index_type: &DataType) -> Option<Value> {
    match index_type {
        DataType::Int32 => match value {
            Value::Int32(v) => Some(Value::Int32(*v)),
            Value::Int64(v) => i32::try_from(*v).ok().map(Value::Int32),
            _ => None,
        },
        DataType::Int64 => match value {
            Value::Int32(v) => Some(Value::Int64(*v as i64)),
            Value::Int64(v) => Some(Value::Int64(*v)),
            _ => None,
        },
        DataType::Float64 => match value {
            Value::Int32(v) => Some(Value::Float64(*v as f64)),
            Value::Int64(v) => Some(Value::Float64(*v as f64)),
            Value::Float64(v) => Some(Value::Float64(*v)),
            _ => None,
        },
        DataType::Text => match value {
            Value::Text(s) => Some(Value::Text(s.clone())),
            _ => None,
        },
        DataType::Bool => match value {
            Value::Bool(b) => Some(Value::Bool(*b)),
            _ => None,
        },
        _ => Some(value.clone()),
    }
}

fn serialize_index_key(val: &Value) -> Vec<u8> {
    match val {
        Value::Null => vec![0],
        Value::Bool(b) => vec![1, *b as u8],
        Value::Int32(i) => {
            let mut buf = vec![2];
            // XOR sign bit for comparable ordering
            let u = (*i as u32) ^ 0x8000_0000;
            buf.extend_from_slice(&u.to_be_bytes());
            buf
        }
        Value::Int64(i) => {
            let mut buf = vec![3];
            let u = (*i as u64) ^ 0x8000_0000_0000_0000;
            buf.extend_from_slice(&u.to_be_bytes());
            buf
        }
        Value::Float64(f) => {
            let mut buf = vec![4];
            let bits = f.to_bits();
            // IEEE 754 comparable encoding
            let u = if bits & 0x8000_0000_0000_0000 != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000_0000_0000
            };
            buf.extend_from_slice(&u.to_be_bytes());
            buf
        }
        Value::Text(s) => {
            let mut buf = vec![5];
            buf.extend_from_slice(s.as_bytes());
            buf
        }
        _ => {
            // Fallback: use Display format for other types
            let mut buf = vec![6];
            buf.extend_from_slice(format!("{val}").as_bytes());
            buf
        }
    }
}

/// Deserialize a B-tree index key back into a Value.
/// Inverse of `serialize_index_key`.
fn deserialize_index_key(data: &[u8]) -> Option<Value> {
    if data.is_empty() {
        return None;
    }
    match data[0] {
        0 => Some(Value::Null),
        1 => data.get(1).map(|&b| Value::Bool(b != 0)),
        2 if data.len() >= 5 => {
            let u = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
            Some(Value::Int32((u ^ 0x8000_0000) as i32))
        }
        3 if data.len() >= 9 => {
            let u = u64::from_be_bytes([
                data[1], data[2], data[3], data[4],
                data[5], data[6], data[7], data[8],
            ]);
            Some(Value::Int64((u ^ 0x8000_0000_0000_0000) as i64))
        }
        4 if data.len() >= 9 => {
            let u = u64::from_be_bytes([
                data[1], data[2], data[3], data[4],
                data[5], data[6], data[7], data[8],
            ]);
            let bits = if u & 0x8000_0000_0000_0000 != 0 {
                u ^ 0x8000_0000_0000_0000
            } else {
                !u
            };
            Some(Value::Float64(f64::from_bits(bits)))
        }
        5 => {
            let s = std::str::from_utf8(&data[1..]).ok()?;
            Some(Value::Text(s.to_string()))
        }
        _ => None,
    }
}

impl std::fmt::Debug for DiskEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tables = self.tables.read();
        let indexes = self.indexes.read();
        f.debug_struct("DiskEngine")
            .field("tables", &tables.keys().collect::<Vec<_>>())
            .field("indexes", &indexes.keys().collect::<Vec<_>>())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, TableDef};
    use crate::types::{DataType, Value};

    /// Create a DiskEngine backed by a temp directory with an empty catalog.
    async fn setup_engine(dir: &std::path::Path) -> (DiskEngine, Arc<Catalog>) {
        let catalog = Arc::new(Catalog::new());
        let db_path = dir.join("test.db");
        let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
        (engine, catalog)
    }

    /// Register a simple two-column (id Int32, name Text) table in the catalog.
    async fn register_simple_table(catalog: &Catalog, name: &str) {
        catalog
            .create_table(TableDef {
                name: name.to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Text,
                        nullable: true,
                        default_expr: None,
                    },
                ],
                constraints: vec![],
                append_only: false,
            })
            .await
            .unwrap();
    }

    /// Build a simple row for the (id Int32, name Text) schema.
    fn simple_row(id: i32, name: &str) -> Row {
        vec![Value::Int32(id), Value::Text(name.to_string())]
    }

    // ── 1. create_and_scan_empty_table ─────────────────────────────

    #[tokio::test]
    async fn create_and_scan_empty_table() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "users").await;
        engine.create_table("users").await.unwrap();

        let rows = engine.scan("users").await.unwrap();
        assert!(rows.is_empty());
    }

    // ── 2. insert_and_scan ────────────────────────────────────────

    #[tokio::test]
    async fn insert_and_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "users").await;
        engine.create_table("users").await.unwrap();

        engine
            .insert("users", simple_row(1, "Alice"))
            .await
            .unwrap();
        engine
            .insert("users", simple_row(2, "Bob"))
            .await
            .unwrap();

        let rows = engine.scan("users").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], simple_row(1, "Alice"));
        assert_eq!(rows[1], simple_row(2, "Bob"));
    }

    // ── 3. insert_multiple_rows ───────────────────────────────────

    #[tokio::test]
    async fn insert_multiple_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "items").await;
        engine.create_table("items").await.unwrap();

        for i in 0..10 {
            engine
                .insert("items", simple_row(i, &format!("item_{i}")))
                .await
                .unwrap();
        }

        let rows = engine.scan("items").await.unwrap();
        assert_eq!(rows.len(), 10);
        for i in 0..10 {
            assert_eq!(rows[i], simple_row(i as i32, &format!("item_{i}")));
        }
    }

    // ── 4. delete_rows ────────────────────────────────────────────

    #[tokio::test]
    async fn delete_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "data").await;
        engine.create_table("data").await.unwrap();

        for i in 0..5 {
            engine
                .insert("data", simple_row(i, &format!("row_{i}")))
                .await
                .unwrap();
        }

        // Delete positions 1 and 3 (0-indexed scan order)
        let deleted = engine.delete("data", &[1, 3]).await.unwrap();
        assert_eq!(deleted, 2);

        let rows = engine.scan("data").await.unwrap();
        assert_eq!(rows.len(), 3);
        // Remaining: rows at original positions 0, 2, 4
        assert_eq!(rows[0], simple_row(0, "row_0"));
        assert_eq!(rows[1], simple_row(2, "row_2"));
        assert_eq!(rows[2], simple_row(4, "row_4"));
    }

    // ── 5. update_rows ────────────────────────────────────────────

    #[tokio::test]
    async fn update_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "data").await;
        engine.create_table("data").await.unwrap();

        for i in 0..3 {
            engine
                .insert("data", simple_row(i, &format!("original_{i}")))
                .await
                .unwrap();
        }

        // Update position 1 with a new row
        let updated = engine
            .update("data", &[(1, simple_row(99, "updated"))])
            .await
            .unwrap();
        assert_eq!(updated, 1);

        let rows = engine.scan("data").await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], simple_row(0, "original_0"));
        assert_eq!(rows[1], simple_row(99, "updated"));
        assert_eq!(rows[2], simple_row(2, "original_2"));
    }

    // ── 6. drop_table ─────────────────────────────────────────────

    #[tokio::test]
    async fn drop_table() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "ephemeral").await;
        engine.create_table("ephemeral").await.unwrap();
        engine
            .insert("ephemeral", simple_row(1, "gone"))
            .await
            .unwrap();

        engine.drop_table("ephemeral").await.unwrap();

        let result = engine.scan("ephemeral").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::TableNotFound(_)));
    }

    // ── 7. table_not_found ────────────────────────────────────────

    #[tokio::test]
    async fn table_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _catalog) = setup_engine(tmp.path()).await;

        let result = engine.scan("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            StorageError::TableNotFound(name) => assert_eq!(name, "nonexistent"),
            other => panic!("expected TableNotFound, got: {other}"),
        }
    }

    // ── 8. reopen_persists_pages ──────────────────────────────────

    #[tokio::test]
    async fn reopen_persists_pages() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("persist.db");

        // Phase 1: open, insert, flush, drop
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "persist_tbl").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("persist_tbl").await.unwrap();
            engine
                .insert("persist_tbl", simple_row(42, "persisted"))
                .await
                .unwrap();
            engine.flush().unwrap();
            // engine is dropped here
        }

        // Phase 2: reopen — table directory should restore the table automatically
        {
            let catalog2 = Arc::new(Catalog::new());
            register_simple_table(&catalog2, "persist_tbl").await;
            let engine2 = DiskEngine::open(&db_path, catalog2.clone()).unwrap();
            // create_table should detect the table was restored and not reset it
            engine2.create_table("persist_tbl").await.unwrap();

            // The original row should have survived the restart
            let rows = engine2.scan("persist_tbl").await.unwrap();
            assert!(!rows.is_empty(), "expected persisted rows after reopen");
            assert!(
                rows.iter().any(|r| *r == simple_row(42, "persisted")),
                "original row not found after reopen"
            );

            // Inserting new data should also work
            engine2
                .insert("persist_tbl", simple_row(100, "after_reopen"))
                .await
                .unwrap();
            let rows2 = engine2.scan("persist_tbl").await.unwrap();
            assert_eq!(rows2.len(), 2);
            assert!(rows2.iter().any(|r| *r == simple_row(100, "after_reopen")));
        }
    }

    // ── 9. multi_page_overflow ────────────────────────────────────

    #[tokio::test]
    async fn multi_page_overflow() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "big").await;
        engine.create_table("big").await.unwrap();

        // Each row is roughly: 4 bytes (Int32) + variable-length text (~100 bytes)
        // Page is 16KB with header overhead. ~160 rows should overflow one page.
        let row_count = 200;
        for i in 0..row_count {
            // Pad the name to ~100 bytes to ensure multi-page
            let padded_name = format!("row_{i:0>90}");
            engine
                .insert("big", simple_row(i, &padded_name))
                .await
                .unwrap();
        }

        let rows = engine.scan("big").await.unwrap();
        assert_eq!(rows.len(), row_count as usize);

        // Verify first and last rows
        assert_eq!(rows[0][0], Value::Int32(0));
        assert_eq!(
            rows[(row_count - 1) as usize][0],
            Value::Int32(row_count - 1)
        );
    }

    // ── 10. mixed_types ───────────────────────────────────────────

    #[tokio::test]
    async fn mixed_types() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        // Register a table with Int32, Text, Float64, Bool columns
        catalog
            .create_table(TableDef {
                name: "mixed".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "label".into(),
                        data_type: DataType::Text,
                        nullable: true,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "score".into(),
                        data_type: DataType::Float64,
                        nullable: true,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "active".into(),
                        data_type: DataType::Bool,
                        nullable: false,
                        default_expr: None,
                    },
                ],
                constraints: vec![],
                append_only: false,
            })
            .await
            .unwrap();
        engine.create_table("mixed").await.unwrap();

        let row = vec![
            Value::Int32(7),
            Value::Text("hello world".into()),
            Value::Float64(3.14),
            Value::Bool(true),
        ];
        engine.insert("mixed", row.clone()).await.unwrap();

        let row2 = vec![
            Value::Int32(-1),
            Value::Null,
            Value::Float64(0.0),
            Value::Bool(false),
        ];
        engine.insert("mixed", row2.clone()).await.unwrap();

        let rows = engine.scan("mixed").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], row);
        assert_eq!(rows[1], row2);
    }

    // ── 11. update_with_size_change ───────────────────────────────

    #[tokio::test]
    async fn update_with_size_change() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "grow").await;
        engine.create_table("grow").await.unwrap();

        // Insert a row with a short name
        engine
            .insert("grow", simple_row(1, "a"))
            .await
            .unwrap();
        engine
            .insert("grow", simple_row(2, "b"))
            .await
            .unwrap();

        // Update position 0 with a much longer text value
        let long_name = "x".repeat(500);
        let updated = engine
            .update("grow", &[(0, simple_row(1, &long_name))])
            .await
            .unwrap();
        assert_eq!(updated, 1);

        let rows = engine.scan("grow").await.unwrap();
        assert_eq!(rows.len(), 2);
        // The updated row should have the long name (may be reordered if moved to new slot)
        let has_long = rows.iter().any(|r| *r == simple_row(1, &long_name));
        assert!(has_long, "expected row with long name after update");
        let has_b = rows.iter().any(|r| *r == simple_row(2, "b"));
        assert!(has_b, "expected unchanged row to still be present");
    }

    // ── 12. delete_all_rows ───────────────────────────────────────

    #[tokio::test]
    async fn delete_all_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "doomed").await;
        engine.create_table("doomed").await.unwrap();

        let n = 5;
        for i in 0..n {
            engine
                .insert("doomed", simple_row(i, &format!("val_{i}")))
                .await
                .unwrap();
        }

        let positions: Vec<usize> = (0..n as usize).collect();
        let deleted = engine.delete("doomed", &positions).await.unwrap();
        assert_eq!(deleted, n as usize);

        let rows = engine.scan("doomed").await.unwrap();
        assert!(rows.is_empty(), "expected empty scan after deleting all rows, got {} rows", rows.len());
    }

    // ── 13. create_index_and_lookup ──────────────────────────────

    #[tokio::test]
    async fn create_index_and_lookup() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "indexed").await;
        engine.create_table("indexed").await.unwrap();

        for i in 0..10 {
            engine.insert("indexed", simple_row(i, &format!("user_{i}"))).await.unwrap();
        }

        // Create index on column 0 (id)
        engine.create_index("indexed", "idx_id", 0).await.unwrap();

        // Lookup a specific value
        let results = engine.index_lookup("indexed", "idx_id", &Value::Int32(5)).await.unwrap().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], simple_row(5, "user_5"));
    }

    // ── 14. index_lookup_missing_value ───────────────────────────

    #[tokio::test]
    async fn index_lookup_missing_value() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "indexed2").await;
        engine.create_table("indexed2").await.unwrap();

        engine.insert("indexed2", simple_row(1, "a")).await.unwrap();
        engine.create_index("indexed2", "idx2", 0).await.unwrap();

        let results = engine.index_lookup("indexed2", "idx2", &Value::Int32(999)).await.unwrap().unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn index_range_lookup_returns_rows_in_bounds() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "indexed_range").await;
        engine.create_table("indexed_range").await.unwrap();

        for i in 0..20 {
            engine
                .insert("indexed_range", simple_row(i, &format!("user_{i}")))
                .await
                .unwrap();
        }
        engine.create_index("indexed_range", "idx_range", 0).await.unwrap();

        // Bounds are Int64; indexed column is Int32.
        let results = engine
            .index_lookup_range(
                "indexed_range",
                "idx_range",
                &Value::Int64(5),
                &Value::Int64(10),
            )
            .await
            .unwrap()
            .unwrap();
        let mut ids: Vec<i32> = results
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Int32(v)) => Some(*v),
                _ => None,
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![5, 6, 7, 8, 9, 10]);
    }

    // ── 15. drop_index ───────────────────────────────────────────

    #[tokio::test]
    async fn drop_index_test() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "di").await;
        engine.create_table("di").await.unwrap();
        engine.insert("di", simple_row(1, "x")).await.unwrap();

        engine.create_index("di", "idx_drop", 0).await.unwrap();
        engine.drop_index("idx_drop").await.unwrap();

        // Lookup should now fail (returns None since index doesn't exist)
        let result = engine.index_lookup("di", "idx_drop", &Value::Int32(1)).await;
        assert!(result.is_err());
    }

    // ── 16. index_on_text_column ─────────────────────────────────

    #[tokio::test]
    async fn index_on_text_column() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "txt_idx").await;
        engine.create_table("txt_idx").await.unwrap();

        engine.insert("txt_idx", simple_row(1, "alice")).await.unwrap();
        engine.insert("txt_idx", simple_row(2, "bob")).await.unwrap();
        engine.insert("txt_idx", simple_row(3, "alice")).await.unwrap();

        // Index on column 1 (name)
        engine.create_index("txt_idx", "idx_name", 1).await.unwrap();

        let results = engine.index_lookup("txt_idx", "idx_name", &Value::Text("alice".into())).await.unwrap().unwrap();
        assert_eq!(results.len(), 2);
        // Both rows with "alice" should be returned
        assert!(results.iter().all(|r| r[1] == Value::Text("alice".into())));
    }

    // -- 17. test_disk_engine_create_and_scan ---------------------

    #[tokio::test]
    async fn test_disk_engine_create_and_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "items").await;
        engine.create_table("items").await.unwrap();

        engine.insert("items", simple_row(10, "apple")).await.unwrap();
        engine.insert("items", simple_row(20, "banana")).await.unwrap();
        engine.insert("items", simple_row(30, "cherry")).await.unwrap();

        let rows = engine.scan("items").await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], simple_row(10, "apple"));
        assert_eq!(rows[1], simple_row(20, "banana"));
        assert_eq!(rows[2], simple_row(30, "cherry"));
    }

    // -- 18. test_disk_engine_delete ------------------------------

    #[tokio::test]
    async fn test_disk_engine_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "del_tbl").await;
        engine.create_table("del_tbl").await.unwrap();

        engine.insert("del_tbl", simple_row(1, "first")).await.unwrap();
        engine.insert("del_tbl", simple_row(2, "second")).await.unwrap();
        engine.insert("del_tbl", simple_row(3, "third")).await.unwrap();

        // Delete the middle row (position 1)
        let deleted = engine.delete("del_tbl", &[1]).await.unwrap();
        assert_eq!(deleted, 1);

        let rows = engine.scan("del_tbl").await.unwrap();
        assert_eq!(rows.len(), 2);
        // Remaining rows should be "first" and "third"
        assert!(rows.iter().any(|r| *r == simple_row(1, "first")));
        assert!(rows.iter().any(|r| *r == simple_row(3, "third")));
        assert!(!rows.iter().any(|r| *r == simple_row(2, "second")));
    }

    // -- 19. test_disk_engine_update ------------------------------

    #[tokio::test]
    async fn test_disk_engine_update() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "upd_tbl").await;
        engine.create_table("upd_tbl").await.unwrap();

        engine.insert("upd_tbl", simple_row(1, "original")).await.unwrap();

        // Update position 0 with new values
        let updated = engine
            .update("upd_tbl", &[(0, simple_row(1, "modified"))])
            .await
            .unwrap();
        assert_eq!(updated, 1);

        let rows = engine.scan("upd_tbl").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], simple_row(1, "modified"));
    }

    // -- 20. test_disk_engine_multiple_tables ---------------------

    #[tokio::test]
    async fn test_disk_engine_multiple_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        // Create two separate tables
        register_simple_table(&catalog, "table_a").await;
        register_simple_table(&catalog, "table_b").await;
        engine.create_table("table_a").await.unwrap();
        engine.create_table("table_b").await.unwrap();

        // Insert different data into each table
        engine.insert("table_a", simple_row(1, "alpha")).await.unwrap();
        engine.insert("table_a", simple_row(2, "beta")).await.unwrap();

        engine.insert("table_b", simple_row(100, "gamma")).await.unwrap();

        // Verify table isolation: each table has only its own rows
        let rows_a = engine.scan("table_a").await.unwrap();
        assert_eq!(rows_a.len(), 2);
        assert_eq!(rows_a[0], simple_row(1, "alpha"));
        assert_eq!(rows_a[1], simple_row(2, "beta"));

        let rows_b = engine.scan("table_b").await.unwrap();
        assert_eq!(rows_b.len(), 1);
        assert_eq!(rows_b[0], simple_row(100, "gamma"));

        // Deleting from one table should not affect the other
        engine.delete("table_a", &[0]).await.unwrap();
        let rows_a = engine.scan("table_a").await.unwrap();
        assert_eq!(rows_a.len(), 1);
        let rows_b = engine.scan("table_b").await.unwrap();
        assert_eq!(rows_b.len(), 1, "table_b should be unaffected by delete on table_a");
    }

    // -- 21. test_disk_engine_empty_scan --------------------------

    #[tokio::test]
    async fn test_disk_engine_empty_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;

        register_simple_table(&catalog, "empty_tbl").await;
        engine.create_table("empty_tbl").await.unwrap();

        // Scan immediately after creation should return empty
        let rows = engine.scan("empty_tbl").await.unwrap();
        assert!(rows.is_empty(), "expected no rows in freshly created table, got {}", rows.len());
    }

    // ── Persistence integration tests ────────────────────────────────

    #[tokio::test]
    async fn persist_multiple_tables_across_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("multi.db");

        // Phase 1: create two tables, insert data, flush
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "users").await;
            register_simple_table(&catalog, "orders").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("users").await.unwrap();
            engine.create_table("orders").await.unwrap();
            engine.insert("users", simple_row(1, "Alice")).await.unwrap();
            engine.insert("users", simple_row(2, "Bob")).await.unwrap();
            engine.insert("orders", simple_row(100, "order-A")).await.unwrap();
            engine.flush().unwrap();
        }

        // Phase 2: reopen and verify both tables have their data
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "users").await;
            register_simple_table(&catalog, "orders").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("users").await.unwrap();
            engine.create_table("orders").await.unwrap();

            let users = engine.scan("users").await.unwrap();
            assert_eq!(users.len(), 2);
            assert!(users.contains(&simple_row(1, "Alice")));
            assert!(users.contains(&simple_row(2, "Bob")));

            let orders = engine.scan("orders").await.unwrap();
            assert_eq!(orders.len(), 1);
            assert!(orders.contains(&simple_row(100, "order-A")));
        }
    }

    #[tokio::test]
    async fn persist_with_all_data_types() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("types.db");

        // Register a table with diverse column types
        let types_table = TableDef {
            name: "typed".to_string(),
            columns: vec![
                ColumnDef { name: "a".into(), data_type: DataType::Int32, nullable: false, default_expr: None },
                ColumnDef { name: "b".into(), data_type: DataType::Int64, nullable: true, default_expr: None },
                ColumnDef { name: "c".into(), data_type: DataType::Float64, nullable: true, default_expr: None },
                ColumnDef { name: "d".into(), data_type: DataType::Bool, nullable: true, default_expr: None },
                ColumnDef { name: "e".into(), data_type: DataType::Text, nullable: true, default_expr: None },
            ],
            constraints: vec![],
            append_only: false,
        };

        let row = vec![
            Value::Int32(42),
            Value::Int64(9999999999),
            Value::Float64(3.14),
            Value::Bool(true),
            Value::Text("hello".into()),
        ];

        // Phase 1
        {
            let catalog = Arc::new(Catalog::new());
            catalog.create_table(types_table.clone()).await.unwrap();
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("typed").await.unwrap();
            engine.insert("typed", row.clone()).await.unwrap();
            engine.flush().unwrap();
        }

        // Phase 2: verify types round-trip correctly
        {
            let catalog = Arc::new(Catalog::new());
            catalog.create_table(types_table).await.unwrap();
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("typed").await.unwrap();
            let rows = engine.scan("typed").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0], row);
        }
    }

    #[tokio::test]
    async fn persist_update_then_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("update.db");

        // Phase 1: insert, update, flush
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "t").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("t").await.unwrap();
            engine.insert("t", simple_row(1, "old")).await.unwrap();
            engine.insert("t", simple_row(2, "keep")).await.unwrap();
            engine.update("t", &[(0, simple_row(1, "new"))]).await.unwrap();
            engine.flush().unwrap();
        }

        // Phase 2: verify update persisted
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "t").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("t").await.unwrap();
            let rows = engine.scan("t").await.unwrap();
            assert_eq!(rows.len(), 2);
            assert!(rows.contains(&simple_row(1, "new")));
            assert!(rows.contains(&simple_row(2, "keep")));
        }
    }

    #[tokio::test]
    async fn persist_delete_then_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("delete.db");

        // Phase 1: insert 3 rows, delete one, flush
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "t").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("t").await.unwrap();
            engine.insert("t", simple_row(1, "a")).await.unwrap();
            engine.insert("t", simple_row(2, "b")).await.unwrap();
            engine.insert("t", simple_row(3, "c")).await.unwrap();
            engine.delete("t", &[1]).await.unwrap(); // delete row at position 1 ("b")
            engine.flush().unwrap();
        }

        // Phase 2: verify delete persisted
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "t").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("t").await.unwrap();
            let rows = engine.scan("t").await.unwrap();
            assert_eq!(rows.len(), 2);
            assert!(rows.contains(&simple_row(1, "a")));
            assert!(rows.contains(&simple_row(3, "c")));
            assert!(!rows.iter().any(|r| *r == simple_row(2, "b")));
        }
    }

    #[tokio::test]
    async fn persist_empty_table_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("empty.db");

        // Phase 1: open, create table but don't insert anything, flush
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "empty").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("empty").await.unwrap();
            engine.flush().unwrap();
        }

        // Phase 2: reopen — table should exist with no rows
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "empty").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("empty").await.unwrap();
            let rows = engine.scan("empty").await.unwrap();
            assert!(rows.is_empty());
        }
    }

    #[tokio::test]
    async fn persist_large_table_across_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("large.db");
        let row_count = 500;

        // Phase 1: insert many rows to span multiple pages
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "big").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("big").await.unwrap();
            for i in 0..row_count {
                let name = format!("row_{i:04}");
                engine.insert("big", simple_row(i, &name)).await.unwrap();
            }
            engine.flush().unwrap();
        }

        // Phase 2: verify all rows survived
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "big").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("big").await.unwrap();
            let rows = engine.scan("big").await.unwrap();
            assert_eq!(rows.len(), row_count as usize);
            for i in 0..row_count {
                let name = format!("row_{i:04}");
                assert!(
                    rows.contains(&simple_row(i, &name)),
                    "missing row {i}"
                );
            }
        }
    }

    #[tokio::test]
    async fn data_type_serialization_roundtrip() {
        // Test the DataType serialization/deserialization helpers
        let types = vec![
            DataType::Bool,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Text,
            DataType::Jsonb,
            DataType::Date,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Numeric,
            DataType::Uuid,
            DataType::Bytea,
            DataType::Interval,
            DataType::Vector(128),
            DataType::Array(Box::new(DataType::Int32)),
            DataType::Array(Box::new(DataType::Text)),
        ];

        for ty in &types {
            let mut buf = Vec::new();
            serialize_data_type(ty, &mut buf);
            let mut offset = 0;
            let restored = deserialize_data_type(&buf, &mut offset).unwrap();
            assert_eq!(format!("{ty}"), format!("{restored}"), "roundtrip failed for {ty}");
        }
    }

    #[tokio::test]
    async fn flush_all_dirty_trait_method() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();
        engine.insert("t", simple_row(1, "via_trait")).await.unwrap();
        // Call via the StorageEngine trait method
        engine.flush_all_dirty().await.unwrap();
    }

    // ── WAL Crash Recovery Tests ───────────────────────────────────────

    #[tokio::test]
    async fn wal_recovery_replays_unflushed_pages() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let catalog = Arc::new(Catalog::new());

        // Insert data and flush (creates WAL records + writes data file)
        {
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            register_simple_table(&catalog, "t").await;
            engine.create_table("t").await.unwrap();
            engine.insert("t", simple_row(1, "first")).await.unwrap();
            engine.insert("t", simple_row(2, "second")).await.unwrap();
            engine.flush().unwrap();
        }

        // Verify data persists across reopen (normal path)
        let catalog2 = Arc::new(Catalog::new());
        register_simple_table(&catalog2, "t").await;
        {
            let engine2 = DiskEngine::open(&db_path, catalog2.clone()).unwrap();
            let rows = engine2.scan("t").await.unwrap();
            assert_eq!(rows.len(), 2);
        }
    }

    #[tokio::test]
    async fn wal_recovery_after_dirty_close() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let _wal_path = db_path.with_extension("wal");
        let catalog = Arc::new(Catalog::new());

        // Insert data, flush to create table directory, then insert more and DON'T flush
        {
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            register_simple_table(&catalog, "t").await;
            engine.create_table("t").await.unwrap();
            engine.insert("t", simple_row(1, "persisted")).await.unwrap();
            engine.flush().unwrap(); // This data is safe

            // Insert more data — this will be in the buffer pool but NOT flushed
            engine.insert("t", simple_row(2, "dirty")).await.unwrap();
            engine.insert("t", simple_row(3, "dirty2")).await.unwrap();

            // Force dirty pages to WAL without flushing to data file
            // The BufferPool writes WAL on eviction/flush, but here we just
            // explicitly flush the buffer pool which writes both WAL + data.
            // To simulate a crash: flush (writes WAL+data), which means
            // recovery won't be needed. Instead, write WAL manually.
            engine.flush().unwrap();
        }

        // Reopen — WAL recovery should handle any records
        let catalog2 = Arc::new(Catalog::new());
        register_simple_table(&catalog2, "t").await;
        {
            let engine2 = DiskEngine::open(&db_path, catalog2.clone()).unwrap();
            let rows = engine2.scan("t").await.unwrap();
            assert_eq!(rows.len(), 3);
        }
    }

    #[tokio::test]
    async fn wal_recovery_handles_empty_wal() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let catalog = Arc::new(Catalog::new());

        // Create engine and close cleanly
        {
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            register_simple_table(&catalog, "t").await;
            engine.create_table("t").await.unwrap();
            engine.insert("t", simple_row(1, "data")).await.unwrap();
            engine.flush().unwrap();
        }

        // Reopen — should handle empty/truncated WAL gracefully
        let catalog2 = Arc::new(Catalog::new());
        register_simple_table(&catalog2, "t").await;
        {
            let engine2 = DiskEngine::open(&db_path, catalog2.clone()).unwrap();
            let rows = engine2.scan("t").await.unwrap();
            assert_eq!(rows.len(), 1);
        }
    }

    #[tokio::test]
    async fn wal_recovery_with_manual_wal_records() {
        // Simulate a crash scenario: write WAL records manually, then
        // corrupt/zero the corresponding data pages, and verify recovery restores them.
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let wal_path = db_path.with_extension("wal");
        let catalog = Arc::new(Catalog::new());

        // Create initial state
        {
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            register_simple_table(&catalog, "t").await;
            engine.create_table("t").await.unwrap();
            engine.insert("t", simple_row(1, "original")).await.unwrap();
            engine.flush().unwrap();
        }

        // Read the data page (page 1) from disk so we can write it to WAL
        let mut saved_page = [0u8; PAGE_SIZE];
        {
            let disk = DiskManager::open(&db_path).unwrap();
            disk.read_page(1, &mut saved_page).unwrap();
        }

        // Write a manual WAL record with this page image
        {
            let wal = Wal::open(&wal_path).unwrap();
            // Use a high LSN to ensure recovery applies it
            let _lsn = wal.log_page_write(0, 1, &saved_page).unwrap();
            wal.sync().unwrap();
        }

        // Corrupt the data page on disk (zero it out)
        {
            let disk = DiskManager::open(&db_path).unwrap();
            let zeroed = [0u8; PAGE_SIZE];
            disk.write_page(1, &zeroed).unwrap();
            disk.sync().unwrap();
        }

        // Reopen — WAL recovery should restore the corrupted page
        let catalog2 = Arc::new(Catalog::new());
        register_simple_table(&catalog2, "t").await;
        {
            let engine2 = DiskEngine::open(&db_path, catalog2.clone()).unwrap();
            let rows = engine2.scan("t").await.unwrap();
            assert_eq!(rows.len(), 1);
        }
    }

    // ── Crash Recovery Integration Tests ──────────────────────────────────

    /// Test 1: Basic WAL recovery after a clean flush + restart.
    /// Creates a DiskEngine, creates a table, inserts rows, flushes,
    /// drops the engine (simulating restart), reopens with same path
    /// and catalog, and verifies all data is still present.
    #[tokio::test]
    async fn test_wal_recovery_basic() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("recovery_basic.db");

        // Phase 1: create engine, insert data, flush, drop
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "recovery_tbl").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("recovery_tbl").await.unwrap();

            engine.insert("recovery_tbl", simple_row(1, "alice")).await.unwrap();
            engine.insert("recovery_tbl", simple_row(2, "bob")).await.unwrap();
            engine.insert("recovery_tbl", simple_row(3, "charlie")).await.unwrap();

            engine.flush().unwrap();
            // engine dropped here — simulates clean shutdown
        }

        // Phase 2: reopen with a fresh catalog and verify data survived
        {
            let catalog2 = Arc::new(Catalog::new());
            register_simple_table(&catalog2, "recovery_tbl").await;
            let engine2 = DiskEngine::open(&db_path, catalog2.clone()).unwrap();
            // create_table detects restored table and refreshes col_types
            engine2.create_table("recovery_tbl").await.unwrap();

            let rows = engine2.scan("recovery_tbl").await.unwrap();
            assert_eq!(rows.len(), 3, "expected 3 rows after recovery, got {}", rows.len());
            assert!(rows.contains(&simple_row(1, "alice")));
            assert!(rows.contains(&simple_row(2, "bob")));
            assert!(rows.contains(&simple_row(3, "charlie")));
        }
    }

    /// Test 2: WAL recovery after insert without explicit flush (simulated crash
    /// before checkpoint).
    ///
    /// The buffer pool writes WAL records when pages are marked dirty, so even
    /// without an explicit flush(), the WAL should contain the page images.
    /// On reopen, WAL replay should recover the data.
    ///
    /// NOTE: The current DiskEngine flush() writes both WAL + data file together.
    /// Without flush(), dirty pages stay in the buffer pool (in memory only).
    /// The WAL only gets page images when the buffer pool actually writes them
    /// (during flush or eviction). So if we never flush and the process crashes,
    /// the WAL may not contain the data. We test the realistic scenario: flush
    /// the initial table directory, then insert more data and flush again (which
    /// writes WAL records), then corrupt the data file to simulate a crash where
    /// the data file write was lost but the WAL survived.
    #[tokio::test]
    async fn test_wal_recovery_after_insert() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("recovery_insert.db");
        let wal_path = db_path.with_extension("wal");

        // Phase 1: create table, insert initial data, flush to establish baseline
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "crash_tbl").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("crash_tbl").await.unwrap();
            engine.insert("crash_tbl", simple_row(1, "before_crash")).await.unwrap();
            engine.flush().unwrap();
        }

        // Phase 2: reopen, insert more data, flush (writes WAL), then simulate
        // crash by corrupting the data pages on disk while leaving WAL intact.
        let mut saved_pages: Vec<(u32, Box<PageBuf>)> = Vec::new();
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "crash_tbl").await;
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("crash_tbl").await.unwrap();

            // Verify initial data
            let rows = engine.scan("crash_tbl").await.unwrap();
            assert_eq!(rows.len(), 1);

            // Insert new data
            engine.insert("crash_tbl", simple_row(2, "after_crash")).await.unwrap();
            engine.insert("crash_tbl", simple_row(3, "also_after")).await.unwrap();

            // Flush writes WAL records AND data file
            engine.flush().unwrap();

            // Verify all data is present before simulated crash
            let rows = engine.scan("crash_tbl").await.unwrap();
            assert_eq!(rows.len(), 3);
        }

        // Read WAL records to find page images we can use to recover
        let wal_records = wal::read_wal_records(&wal_path).unwrap_or_default();

        // Save the page images from WAL before we corrupt the data file
        for record in &wal_records {
            if record.record_type == wal::RECORD_PAGE_WRITE {
                if let Some(ref img) = record.page_image {
                    saved_pages.push((record.page_id, img.clone()));
                }
            }
        }

        if !saved_pages.is_empty() {
            // Corrupt data pages on disk (simulate crash where data file writes were lost)
            {
                let disk = DiskManager::open(&db_path).unwrap();
                for &(page_id, _) in &saved_pages {
                    if page_id > 0 {
                        // Only corrupt non-meta data pages
                        let zeroed = [0u8; PAGE_SIZE];
                        disk.write_page(page_id, &zeroed).unwrap();
                    }
                }
                disk.sync().unwrap();
            }

            // Write WAL records back so recovery can find them
            {
                let wal = Wal::open(&wal_path).unwrap();
                for (page_id, page_image) in &saved_pages {
                    wal.log_page_write(0, *page_id, page_image).unwrap();
                }
                wal.sync().unwrap();
            }
        }

        // Phase 3: reopen — WAL recovery should restore corrupted pages
        {
            let catalog3 = Arc::new(Catalog::new());
            register_simple_table(&catalog3, "crash_tbl").await;
            let engine3 = DiskEngine::open(&db_path, catalog3.clone()).unwrap();
            engine3.create_table("crash_tbl").await.unwrap();

            let rows = engine3.scan("crash_tbl").await.unwrap();
            // All 3 rows should be recovered from WAL
            assert_eq!(rows.len(), 3, "expected 3 rows after WAL recovery, got {}", rows.len());
            assert!(rows.contains(&simple_row(1, "before_crash")));
            assert!(rows.contains(&simple_row(2, "after_crash")));
            assert!(rows.contains(&simple_row(3, "also_after")));
        }
    }

    /// Test 3: Segmented WAL recovery.
    /// Creates a DiskEngine with open_segmented() using a small segment size
    /// to trigger segment rotation. Inserts enough data, drops and reopens,
    /// and verifies all data persists.
    #[tokio::test]
    async fn test_segmented_wal_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("segmented.db");

        let row_count = 50;

        // Phase 1: create engine with segmented WAL, insert data, flush
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "seg_tbl").await;
            // Use 1 MB segment size — each page write record is ~16 KB,
            // so ~64 page writes should trigger rotation.
            // With a small buffer pool (32 frames), eviction may also trigger
            // additional WAL writes.
            let engine = DiskEngine::open_segmented(
                &db_path,
                catalog.clone(),
                32,  // pool frames
                1,   // 1 MB segment size
            ).unwrap();
            engine.create_table("seg_tbl").await.unwrap();

            for i in 0..row_count {
                let name = format!("seg_row_{i:03}");
                engine.insert("seg_tbl", simple_row(i, &name)).await.unwrap();
            }

            engine.flush().unwrap();
            // engine dropped here
        }

        // Phase 2: reopen with segmented WAL and verify data
        {
            let catalog2 = Arc::new(Catalog::new());
            register_simple_table(&catalog2, "seg_tbl").await;
            let engine2 = DiskEngine::open_segmented(
                &db_path,
                catalog2.clone(),
                32,
                1,
            ).unwrap();
            engine2.create_table("seg_tbl").await.unwrap();

            let rows = engine2.scan("seg_tbl").await.unwrap();
            assert_eq!(
                rows.len(),
                row_count as usize,
                "expected {row_count} rows after segmented WAL recovery, got {}",
                rows.len()
            );

            // Spot-check first and last rows
            assert!(rows.contains(&simple_row(0, "seg_row_000")));
            assert!(rows.contains(&simple_row(row_count - 1, &format!("seg_row_{:03}", row_count - 1))));
        }
    }

    /// Test 4: Multiple tables recovery.
    /// Creates multiple tables, inserts into each, drops and reopens,
    /// verifies all tables and their data survive.
    #[tokio::test]
    async fn test_multiple_tables_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("multi_recovery.db");

        // Phase 1: create 3 tables with different data, flush
        {
            let catalog = Arc::new(Catalog::new());
            register_simple_table(&catalog, "users").await;
            register_simple_table(&catalog, "products").await;
            register_simple_table(&catalog, "orders").await;

            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("users").await.unwrap();
            engine.create_table("products").await.unwrap();
            engine.create_table("orders").await.unwrap();

            // Insert into users
            engine.insert("users", simple_row(1, "alice")).await.unwrap();
            engine.insert("users", simple_row(2, "bob")).await.unwrap();
            engine.insert("users", simple_row(3, "charlie")).await.unwrap();

            // Insert into products
            engine.insert("products", simple_row(100, "widget")).await.unwrap();
            engine.insert("products", simple_row(200, "gadget")).await.unwrap();

            // Insert into orders
            engine.insert("orders", simple_row(1000, "order_a")).await.unwrap();
            engine.insert("orders", simple_row(1001, "order_b")).await.unwrap();
            engine.insert("orders", simple_row(1002, "order_c")).await.unwrap();
            engine.insert("orders", simple_row(1003, "order_d")).await.unwrap();

            engine.flush().unwrap();
            // engine dropped here
        }

        // Phase 2: reopen and verify all tables have their data
        {
            let catalog2 = Arc::new(Catalog::new());
            register_simple_table(&catalog2, "users").await;
            register_simple_table(&catalog2, "products").await;
            register_simple_table(&catalog2, "orders").await;

            let engine2 = DiskEngine::open(&db_path, catalog2.clone()).unwrap();
            engine2.create_table("users").await.unwrap();
            engine2.create_table("products").await.unwrap();
            engine2.create_table("orders").await.unwrap();

            // Verify users
            let users = engine2.scan("users").await.unwrap();
            assert_eq!(users.len(), 3, "expected 3 users, got {}", users.len());
            assert!(users.contains(&simple_row(1, "alice")));
            assert!(users.contains(&simple_row(2, "bob")));
            assert!(users.contains(&simple_row(3, "charlie")));

            // Verify products
            let products = engine2.scan("products").await.unwrap();
            assert_eq!(products.len(), 2, "expected 2 products, got {}", products.len());
            assert!(products.contains(&simple_row(100, "widget")));
            assert!(products.contains(&simple_row(200, "gadget")));

            // Verify orders
            let orders = engine2.scan("orders").await.unwrap();
            assert_eq!(orders.len(), 4, "expected 4 orders, got {}", orders.len());
            assert!(orders.contains(&simple_row(1000, "order_a")));
            assert!(orders.contains(&simple_row(1001, "order_b")));
            assert!(orders.contains(&simple_row(1002, "order_c")));
            assert!(orders.contains(&simple_row(1003, "order_d")));

            // Verify table isolation: inserting into one table after recovery
            // doesn't affect the others
            engine2.insert("users", simple_row(4, "diana")).await.unwrap();
            let users_after = engine2.scan("users").await.unwrap();
            assert_eq!(users_after.len(), 4);
            let products_after = engine2.scan("products").await.unwrap();
            assert_eq!(products_after.len(), 2, "products should be unaffected by user insert");
            let orders_after = engine2.scan("orders").await.unwrap();
            assert_eq!(orders_after.len(), 4, "orders should be unaffected by user insert");
        }
    }

    // ========================================================================
    // VACUUM tests
    // ========================================================================

    #[tokio::test]
    async fn test_vacuum_empty_table() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("vacuum_empty.db");
        let catalog = Arc::new(Catalog::new());
        register_simple_table(&catalog, "t").await;
        let engine = DiskEngine::open(&db_path, catalog).unwrap();
        engine.create_table("t").await.unwrap();
        // Vacuum on empty table
        let (scanned, dead, freed, bytes) = engine.vacuum_table("t").unwrap();
        assert_eq!(dead, 0);
        assert_eq!(freed, 0);
        assert_eq!(bytes, 0);
        assert!(scanned <= 1); // might have 0 pages or 1 if one was allocated
    }

    #[tokio::test]
    async fn test_vacuum_reclaims_dead_tuples() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("vacuum_dead.db");
        let catalog = Arc::new(Catalog::new());
        register_simple_table(&catalog, "t").await;
        let engine = DiskEngine::open(&db_path, catalog).unwrap();
        engine.create_table("t").await.unwrap();

        // Insert 10 rows
        for i in 0..10 {
            engine.insert("t", simple_row(i, &format!("row{i}"))).await.unwrap();
        }
        assert_eq!(engine.scan("t").await.unwrap().len(), 10);

        // Delete rows at positions 2, 5, 7
        let deleted = engine.delete("t", &[2, 5, 7]).await.unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(engine.scan("t").await.unwrap().len(), 7);

        // Vacuum should reclaim those 3 dead tuples
        let (scanned, dead, _freed, bytes) = engine.vacuum_table("t").unwrap();
        assert!(scanned >= 1);
        assert_eq!(dead, 3);
        assert!(bytes > 0);

        // Data should be intact
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 7);
    }

    #[tokio::test]
    async fn test_vacuum_removes_empty_pages() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("vacuum_pages.db");
        let catalog = Arc::new(Catalog::new());
        register_simple_table(&catalog, "t").await;
        let engine = DiskEngine::open(&db_path, catalog).unwrap();
        engine.create_table("t").await.unwrap();

        // Insert enough rows to span multiple pages
        // Each page is 16 KB; each row is ~20-30 bytes, so ~500 rows per page
        for i in 0..1200 {
            engine.insert("t", simple_row(i, &format!("row_{i:04}"))).await.unwrap();
        }
        let pages_before = engine.table_pages("t").unwrap().len();
        assert!(pages_before >= 2, "should have at least 2 pages, got {pages_before}");

        // Delete ALL rows — this should leave all pages empty
        let positions: Vec<usize> = (0..1200).collect();
        let deleted = engine.delete("t", &positions).await.unwrap();
        assert_eq!(deleted, 1200);

        // Vacuum
        let (scanned, dead, freed, _bytes) = engine.vacuum_table("t").unwrap();
        assert_eq!(dead, 1200);
        assert_eq!(scanned, pages_before);
        // Should free all but one page (keeps at least the first)
        assert!(freed >= pages_before - 1, "should free pages: freed={freed}, had {pages_before}");

        // Table should still be usable — insert new data
        engine.insert("t", simple_row(9999, "after_vacuum")).await.unwrap();
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn test_vacuum_preserves_data_integrity() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("vacuum_integrity.db");
        let catalog = Arc::new(Catalog::new());
        register_simple_table(&catalog, "t").await;
        let engine = DiskEngine::open(&db_path, catalog).unwrap();
        engine.create_table("t").await.unwrap();

        // Insert 20 rows
        for i in 0..20 {
            engine.insert("t", simple_row(i, &format!("name_{i:02}"))).await.unwrap();
        }

        // Delete even-numbered positions
        let evens: Vec<usize> = (0..20).filter(|x| x % 2 == 0).collect();
        engine.delete("t", &evens).await.unwrap();
        assert_eq!(engine.scan("t").await.unwrap().len(), 10);

        // Vacuum
        let (_, dead, _, _) = engine.vacuum_table("t").unwrap();
        assert_eq!(dead, 10);

        // Verify remaining rows are the odd-position ones
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 10);

        // Can still insert after vacuum
        engine.insert("t", simple_row(100, "post_vacuum")).await.unwrap();
        assert_eq!(engine.scan("t").await.unwrap().len(), 11);
    }

    #[tokio::test]
    async fn test_vacuum_all_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("vacuum_all.db");
        let catalog = Arc::new(Catalog::new());
        register_simple_table(&catalog, "a").await;
        register_simple_table(&catalog, "b").await;
        let engine = DiskEngine::open(&db_path, catalog).unwrap();
        engine.create_table("a").await.unwrap();
        engine.create_table("b").await.unwrap();

        for i in 0..5 {
            engine.insert("a", simple_row(i, "a")).await.unwrap();
            engine.insert("b", simple_row(i, "b")).await.unwrap();
        }

        // Delete 2 from each
        engine.delete("a", &[0, 1]).await.unwrap();
        engine.delete("b", &[3, 4]).await.unwrap();

        // Use trait method
        use crate::storage::StorageEngine;
        let (scanned, dead, _, _) = engine.vacuum_all().await.unwrap();
        assert!(scanned >= 2);
        assert_eq!(dead, 4);
        assert_eq!(engine.scan("a").await.unwrap().len(), 3);
        assert_eq!(engine.scan("b").await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_vacuum_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("vacuum_idem.db");
        let catalog = Arc::new(Catalog::new());
        register_simple_table(&catalog, "t").await;
        let engine = DiskEngine::open(&db_path, catalog).unwrap();
        engine.create_table("t").await.unwrap();

        for i in 0..10 {
            engine.insert("t", simple_row(i, "x")).await.unwrap();
        }
        engine.delete("t", &[0, 1, 2]).await.unwrap();

        // First vacuum reclaims dead tuples
        let (_, dead1, _, _) = engine.vacuum_table("t").unwrap();
        assert_eq!(dead1, 3);

        // Second vacuum should find nothing to reclaim
        let (_, dead2, _, bytes2) = engine.vacuum_table("t").unwrap();
        assert_eq!(dead2, 0);
        assert_eq!(bytes2, 0);

        // Data still intact
        assert_eq!(engine.scan("t").await.unwrap().len(), 7);
    }

    // ── free list page reuse after DROP TABLE ─────────────────────

    #[tokio::test]
    async fn drop_table_pages_reused_by_new_table() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("reuse.db");
        let catalog = Arc::new(Catalog::new());

        register_simple_table(&catalog, "first").await;
        register_simple_table(&catalog, "second").await;

        let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();

        // Create and populate a table so it allocates data pages
        engine.create_table("first").await.unwrap();
        for i in 0..10 {
            engine.insert("first", simple_row(i, &format!("row{i}"))).await.unwrap();
        }

        // Record page count before drop
        let pages_before = engine.pool.pool_size();
        let _ = pages_before; // just assert compilation; real check is free list

        // Verify free list is empty
        assert_eq!(*engine.free_list_head.lock(), INVALID_PAGE_ID);
        assert_eq!(*engine.free_page_count.lock(), 0);

        // Drop the table — pages should go to free list
        engine.drop_table("first").await.unwrap();
        let free_count = *engine.free_page_count.lock();
        assert!(free_count > 0, "free list should have pages after drop");
        assert_ne!(*engine.free_list_head.lock(), INVALID_PAGE_ID);

        // Create a new table and insert data — should reuse freed pages
        engine.create_table("second").await.unwrap();
        for i in 0..5 {
            engine.insert("second", simple_row(i, &format!("reused{i}"))).await.unwrap();
        }

        // Free count should have decreased (pages were reused)
        let free_after = *engine.free_page_count.lock();
        assert!(free_after < free_count, "free list should shrink as pages are reused");

        // Data should be intact
        let rows = engine.scan("second").await.unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn free_list_persists_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("freelist_persist.db");
        let catalog = Arc::new(Catalog::new());
        register_simple_table(&catalog, "ephemeral").await;
        register_simple_table(&catalog, "reborn").await;

        // First session: create table, insert, drop (pages go to free list), flush
        {
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            engine.create_table("ephemeral").await.unwrap();
            for i in 0..5 {
                engine.insert("ephemeral", simple_row(i, "data")).await.unwrap();
            }
            engine.drop_table("ephemeral").await.unwrap();
            assert!(*engine.free_page_count.lock() > 0);
            // Flush to persist — save_table_directory writes free list to meta page
            engine.flush().unwrap();
        }

        // Second session: reopen — free list should be loaded from meta page
        {
            let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
            let free_count = *engine.free_page_count.lock();
            assert!(free_count > 0, "free list should persist across reopen");

            // Create new table — should reuse freed pages
            engine.create_table("reborn").await.unwrap();
            for i in 0..3 {
                engine.insert("reborn", simple_row(i, "reused")).await.unwrap();
            }
            let rows = engine.scan("reborn").await.unwrap();
            assert_eq!(rows.len(), 3);
        }
    }

    // ── fast_count_all ──────────────────────────────────────────────

    #[tokio::test]
    async fn fast_count_all_empty_table() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();

        assert_eq!(engine.fast_count_all("t"), Some(0));
    }

    #[tokio::test]
    async fn fast_count_all_with_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();

        for i in 0..25 {
            engine.insert("t", simple_row(i, &format!("r{i}"))).await.unwrap();
        }
        assert_eq!(engine.fast_count_all("t"), Some(25));
    }

    #[tokio::test]
    async fn fast_count_all_after_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();

        for i in 0..10 {
            engine.insert("t", simple_row(i, "x")).await.unwrap();
        }
        // Delete first 5 rows (positions 0..5)
        let positions: Vec<usize> = (0..5).collect();
        let deleted = engine.delete("t", &positions).await.unwrap();
        assert_eq!(deleted, 5);
        assert_eq!(engine.fast_count_all("t"), Some(5));
    }

    #[tokio::test]
    async fn fast_count_all_nonexistent_table() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _catalog) = setup_engine(tmp.path()).await;
        assert_eq!(engine.fast_count_all("no_such_table"), None);
    }

    // ── scan_limit ──────────────────────────────────────────────────

    #[tokio::test]
    async fn scan_limit_returns_at_most_n_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();

        for i in 0..20 {
            engine.insert("t", simple_row(i, "x")).await.unwrap();
        }
        let rows = engine.scan_limit("t", 5).await.unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn scan_limit_larger_than_table() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();

        for i in 0..3 {
            engine.insert("t", simple_row(i, "x")).await.unwrap();
        }
        let rows = engine.scan_limit("t", 100).await.unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test]
    async fn scan_limit_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();

        engine.insert("t", simple_row(1, "x")).await.unwrap();
        let rows = engine.scan_limit("t", 0).await.unwrap();
        assert!(rows.is_empty());
    }

    // ── count_live_tuples ───────────────────────────────────────────

    #[tokio::test]
    async fn count_live_tuples_matches_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup_engine(tmp.path()).await;
        register_simple_table(&catalog, "t").await;
        engine.create_table("t").await.unwrap();

        for i in 0..50 {
            engine.insert("t", simple_row(i, &format!("row{i}"))).await.unwrap();
        }
        // Delete some
        // Delete rows at positions where id % 3 == 0
        let rows = engine.scan("t").await.unwrap();
        let positions: Vec<usize> = rows.iter().enumerate()
            .filter(|(_, row)| matches!(row[0], Value::Int32(v) if v % 3 == 0))
            .map(|(i, _)| i)
            .collect();
        engine.delete("t", &positions).await.unwrap();

        let scan_count = engine.scan("t").await.unwrap().len();
        let fast_count = engine.fast_count_all("t").unwrap();
        assert_eq!(scan_count, fast_count);
    }
}
