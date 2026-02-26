//! Direct I/O abstraction layer with io_uring support on Linux.
//!
//! Provides `AsyncDiskOps` -- an async trait for page-level disk I/O.
//! On Linux (kernel 5.1+), `IoUringDiskOps` uses io_uring for async I/O
//! via submission/completion queues with `tokio::task::spawn_blocking`.
//! On other platforms, it delegates to `StandardDiskOps` (tokio::fs).

use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Async trait for page-level disk I/O.
///
/// All operations are page-aligned: page N lives at byte offset
/// `N * page_size` in the backing file.
#[async_trait::async_trait]
pub trait AsyncDiskOps: Send + Sync {
    /// Read a full page into `buf`. The buffer must be at least `page_size()` bytes.
    async fn read_page(&self, page_id: u32, buf: &mut [u8]) -> io::Result<()>;

    /// Write `data` as a full page. `data` must be exactly `page_size()` bytes.
    async fn write_page(&self, page_id: u32, data: &[u8]) -> io::Result<()>;

    /// Flush all pending writes to durable storage.
    async fn sync(&self) -> io::Result<()>;

    /// The page size used by this I/O backend.
    fn page_size(&self) -> usize;
}

// ---------------------------------------------------------------------------
// StandardDiskOps -- tokio::fs based implementation
// ---------------------------------------------------------------------------

/// Standard disk I/O using `tokio::fs`. Works on all platforms.
pub struct StandardDiskOps {
    path: PathBuf,
    page_size: usize,
}

impl StandardDiskOps {
    /// Open (or create) the backing file and return a new `StandardDiskOps`.
    pub fn new(path: impl AsRef<Path>, page_size: usize) -> io::Result<Self> {
        // Eagerly create the file so later reads/writes don't fail on missing file.
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            page_size,
        })
    }
}

#[async_trait::async_trait]
impl AsyncDiskOps for StandardDiskOps {
    async fn read_page(&self, page_id: u32, buf: &mut [u8]) -> io::Result<()> {
        let offset = page_id as u64 * self.page_size as u64;
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await?;
        file.seek(SeekFrom::Start(offset)).await?;

        let bytes_read = file.read(buf).await?;
        // Zero-fill the remainder if the file is shorter than the requested page.
        if bytes_read < self.page_size {
            buf[bytes_read..self.page_size].fill(0);
        }
        Ok(())
    }

    async fn write_page(&self, page_id: u32, data: &[u8]) -> io::Result<()> {
        let offset = page_id as u64 * self.page_size as u64;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .await?;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }

    async fn sync(&self) -> io::Result<()> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .await?;
        file.sync_all().await?;
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }
}

// ---------------------------------------------------------------------------
// IoUringDiskOps -- real io_uring on Linux, fallback on other platforms
// ---------------------------------------------------------------------------

// ---- Linux: real io_uring implementation ----

#[cfg(target_os = "linux")]
/// Direct I/O backend using Linux io_uring for kernel-bypassing async I/O.
///
/// Operations are submitted to the io_uring submission queue and completed
/// via `spawn_blocking` to avoid blocking the tokio runtime. Each operation
/// does a single SQE submit + CQE wait, which is efficient because the
/// kernel performs the I/O asynchronously.
pub struct IoUringDiskOps {
    ring: std::sync::Arc<std::sync::Mutex<io_uring::IoUring>>,
    file: std::sync::Arc<std::fs::File>,
    page_size: usize,
    /// When true, O_DIRECT is used for bypassing the page cache.
    /// Currently reserved for future use (requires aligned buffer allocation).
    #[allow(dead_code)]
    use_direct_io: bool,
}

#[cfg(target_os = "linux")]
impl IoUringDiskOps {
    /// Create a new `IoUringDiskOps` with the default queue depth (256).
    pub fn new(
        path: impl AsRef<Path>,
        page_size: usize,
        use_direct_io: bool,
    ) -> io::Result<Self> {
        Self::new_with_queue_depth(path, page_size, use_direct_io, 256)
    }

    /// Create a new `IoUringDiskOps` with a custom submission queue depth.
    pub fn new_with_queue_depth(
        path: impl AsRef<Path>,
        page_size: usize,
        use_direct_io: bool,
        queue_depth: u32,
    ) -> io::Result<Self> {
        // Open the backing file. O_DIRECT support is reserved for future use
        // (requires aligned buffer allocation via posix_memalign).
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;

        let ring = io_uring::IoUring::new(queue_depth)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("io_uring init failed: {e}")))?;

        Ok(Self {
            ring: std::sync::Arc::new(std::sync::Mutex::new(ring)),
            file: std::sync::Arc::new(file),
            page_size,
            use_direct_io,
        })
    }
}

#[cfg(target_os = "linux")]
#[async_trait::async_trait]
impl AsyncDiskOps for IoUringDiskOps {
    async fn read_page(&self, page_id: u32, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;

        let ring = self.ring.clone();
        let file = self.file.clone();
        let page_size = self.page_size;
        let offset = page_id as u64 * page_size as u64;

        let data = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let mut read_buf = vec![0u8; page_size];
            let fd = io_uring::types::Fd(file.as_raw_fd());

            let read_e = io_uring::opcode::Read::new(
                fd,
                read_buf.as_mut_ptr(),
                read_buf.len() as _,
            )
            .offset(offset as _)
            .build()
            .user_data(0x42);

            let mut ring_guard = ring.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("ring lock poisoned: {e}"))
            })?;

            unsafe {
                ring_guard
                    .submission()
                    .push(&read_e)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring SQ full"))?;
            }

            ring_guard.submit_and_wait(1)?;

            if let Some(cqe) = ring_guard.completion().next() {
                let ret = cqe.result();
                if ret < 0 {
                    return Err(io::Error::from_raw_os_error(-ret));
                }
                let bytes_read = ret as usize;
                if bytes_read < page_size {
                    read_buf[bytes_read..page_size].fill(0);
                }
            }

            Ok(read_buf)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("spawn_blocking join: {e}")))?
        ?;

        let copy_len = buf.len().min(data.len());
        buf[..copy_len].copy_from_slice(&data[..copy_len]);
        Ok(())
    }

    async fn write_page(&self, page_id: u32, data: &[u8]) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;

        let ring = self.ring.clone();
        let file = self.file.clone();
        let page_size = self.page_size;
        let offset = page_id as u64 * page_size as u64;
        let write_data = data.to_vec();

        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let fd = io_uring::types::Fd(file.as_raw_fd());

            let write_e = io_uring::opcode::Write::new(
                fd,
                write_data.as_ptr(),
                write_data.len().min(page_size) as _,
            )
            .offset(offset as _)
            .build()
            .user_data(0x43);

            let mut ring_guard = ring.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("ring lock poisoned: {e}"))
            })?;

            unsafe {
                ring_guard
                    .submission()
                    .push(&write_e)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring SQ full"))?;
            }

            ring_guard.submit_and_wait(1)?;

            if let Some(cqe) = ring_guard.completion().next() {
                let ret = cqe.result();
                if ret < 0 {
                    return Err(io::Error::from_raw_os_error(-ret));
                }
            }

            Ok(())
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("spawn_blocking join: {e}")))?
    }

    async fn sync(&self) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;

        let ring = self.ring.clone();
        let file = self.file.clone();

        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let fd = io_uring::types::Fd(file.as_raw_fd());

            let fsync_e = io_uring::opcode::Fsync::new(fd)
                .build()
                .user_data(0x44);

            let mut ring_guard = ring.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("ring lock poisoned: {e}"))
            })?;

            unsafe {
                ring_guard
                    .submission()
                    .push(&fsync_e)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring SQ full"))?;
            }

            ring_guard.submit_and_wait(1)?;

            if let Some(cqe) = ring_guard.completion().next() {
                let ret = cqe.result();
                if ret < 0 {
                    return Err(io::Error::from_raw_os_error(-ret));
                }
            }

            Ok(())
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("spawn_blocking join: {e}")))?
    }

    fn page_size(&self) -> usize {
        self.page_size
    }
}

// ---- Non-Linux: fallback to StandardDiskOps ----

#[cfg(not(target_os = "linux"))]
/// Fallback I/O backend for non-Linux platforms. Delegates all operations
/// to `StandardDiskOps` since io_uring is Linux-only.
pub struct IoUringDiskOps {
    inner: StandardDiskOps,
    #[allow(dead_code)]
    use_direct_io: bool,
}

#[cfg(not(target_os = "linux"))]
impl IoUringDiskOps {
    /// Create a new `IoUringDiskOps`. On non-Linux platforms this delegates
    /// to `StandardDiskOps`.
    pub fn new(
        path: impl AsRef<Path>,
        page_size: usize,
        use_direct_io: bool,
    ) -> io::Result<Self> {
        let inner = StandardDiskOps::new(path, page_size)?;
        Ok(Self {
            inner,
            use_direct_io,
        })
    }

    /// Create with explicit queue depth (ignored on non-Linux).
    pub fn new_with_queue_depth(
        path: impl AsRef<Path>,
        page_size: usize,
        use_direct_io: bool,
        _queue_depth: u32,
    ) -> io::Result<Self> {
        Self::new(path, page_size, use_direct_io)
    }
}

#[cfg(not(target_os = "linux"))]
#[async_trait::async_trait]
impl AsyncDiskOps for IoUringDiskOps {
    async fn read_page(&self, page_id: u32, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_page(page_id, buf).await
    }

    async fn write_page(&self, page_id: u32, data: &[u8]) -> io::Result<()> {
        self.inner.write_page(page_id, data).await
    }

    async fn sync(&self) -> io::Result<()> {
        self.inner.sync().await
    }

    fn page_size(&self) -> usize {
        self.inner.page_size()
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Create an `AsyncDiskOps` backend.
///
/// When `use_direct_io` is true, returns an `IoUringDiskOps` (which uses
/// real io_uring on Linux, or delegates to `StandardDiskOps` elsewhere).
/// Otherwise returns a plain `StandardDiskOps`.
pub fn create_disk_ops(
    path: impl AsRef<Path>,
    page_size: usize,
    use_direct_io: bool,
) -> io::Result<Box<dyn AsyncDiskOps>> {
    if use_direct_io {
        Ok(Box::new(IoUringDiskOps::new(path, page_size, true)?))
    } else {
        Ok(Box::new(StandardDiskOps::new(path, page_size)?))
    }
}

/// Create an `AsyncDiskOps` backend from a `DiskOpsConfig`.
///
/// Uses `queue_depth` from the config for io_uring ring sizing on Linux.
pub fn create_disk_ops_with_config(
    path: impl AsRef<Path>,
    config: &DiskOpsConfig,
) -> io::Result<Box<dyn AsyncDiskOps>> {
    if config.use_direct_io {
        Ok(Box::new(IoUringDiskOps::new_with_queue_depth(
            path,
            config.page_size,
            true,
            config.queue_depth,
        )?))
    } else {
        Ok(Box::new(StandardDiskOps::new(path, config.page_size)?))
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the disk I/O backend.
pub struct DiskOpsConfig {
    /// Page size in bytes (default 16 KiB).
    pub page_size: usize,
    /// Whether to attempt Direct I/O / io_uring (default false).
    pub use_direct_io: bool,
    /// io_uring submission queue depth (default 256).
    pub queue_depth: u32,
}

impl Default for DiskOpsConfig {
    fn default() -> Self {
        Self {
            page_size: 16384,
            use_direct_io: false,
            queue_depth: 256,
        }
    }
}

// ---------------------------------------------------------------------------
// Batched I/O submission
// ---------------------------------------------------------------------------

/// An I/O request for batch submission.
#[derive(Debug, Clone)]
pub enum IoRequest {
    Read { page_id: u32 },
    Write { page_id: u32, data: Vec<u8> },
    Sync,
}

/// Result of a single I/O operation in a batch.
#[derive(Debug)]
pub enum IoResult {
    ReadComplete { page_id: u32, data: Vec<u8> },
    WriteComplete { page_id: u32 },
    SyncComplete,
    Error { request_index: usize, error: String },
}

/// Batched I/O submission queue that collects multiple I/O requests
/// and executes them through the underlying `AsyncDiskOps` backend.
///
/// On Linux with io_uring, this maps naturally to the submission/completion
/// queue model. On other platforms, requests are executed sequentially.
pub struct IoBatchQueue {
    pending: Vec<IoRequest>,
    completed_count: u64,
    submitted_count: u64,
    queue_depth: u32,
}

impl IoBatchQueue {
    /// Create a new batch queue with the given depth limit.
    pub fn new(queue_depth: u32) -> Self {
        Self {
            pending: Vec::new(),
            completed_count: 0,
            submitted_count: 0,
            queue_depth,
        }
    }

    /// Submit a read request to the queue.
    pub fn submit_read(&mut self, page_id: u32) -> usize {
        let idx = self.pending.len();
        self.pending.push(IoRequest::Read { page_id });
        idx
    }

    /// Submit a write request to the queue.
    pub fn submit_write(&mut self, page_id: u32, data: Vec<u8>) -> usize {
        let idx = self.pending.len();
        self.pending.push(IoRequest::Write { page_id, data });
        idx
    }

    /// Submit a sync (fsync) request to the queue.
    pub fn submit_sync(&mut self) -> usize {
        let idx = self.pending.len();
        self.pending.push(IoRequest::Sync);
        idx
    }

    /// Number of pending requests.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Whether the queue has reached its depth limit.
    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.queue_depth as usize
    }

    /// Execute all pending requests against the given backend.
    pub async fn execute(&mut self, ops: &dyn AsyncDiskOps) -> Vec<IoResult> {
        let requests = std::mem::take(&mut self.pending);
        self.submitted_count += requests.len() as u64;
        let mut results = Vec::with_capacity(requests.len());

        for (i, req) in requests.iter().enumerate() {
            match req {
                IoRequest::Read { page_id } => {
                    let mut buf = vec![0u8; ops.page_size()];
                    match ops.read_page(*page_id, &mut buf).await {
                        Ok(()) => {
                            self.completed_count += 1;
                            results.push(IoResult::ReadComplete {
                                page_id: *page_id,
                                data: buf,
                            });
                        }
                        Err(e) => {
                            results.push(IoResult::Error {
                                request_index: i,
                                error: e.to_string(),
                            });
                        }
                    }
                }
                IoRequest::Write { page_id, data } => {
                    match ops.write_page(*page_id, data).await {
                        Ok(()) => {
                            self.completed_count += 1;
                            results.push(IoResult::WriteComplete { page_id: *page_id });
                        }
                        Err(e) => {
                            results.push(IoResult::Error {
                                request_index: i,
                                error: e.to_string(),
                            });
                        }
                    }
                }
                IoRequest::Sync => {
                    match ops.sync().await {
                        Ok(()) => {
                            self.completed_count += 1;
                            results.push(IoResult::SyncComplete);
                        }
                        Err(e) => {
                            results.push(IoResult::Error {
                                request_index: i,
                                error: e.to_string(),
                            });
                        }
                    }
                }
            }
        }
        results
    }

    /// Total requests that have been submitted.
    pub fn submitted_count(&self) -> u64 { self.submitted_count }
    /// Total requests that completed successfully.
    pub fn completed_count(&self) -> u64 { self.completed_count }
    /// The configured queue depth.
    pub fn queue_depth(&self) -> u32 { self.queue_depth }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const TEST_PAGE_SIZE: usize = 4096;

    /// Helper: create a StandardDiskOps in a temp directory.
    fn setup_standard(dir: &TempDir) -> StandardDiskOps {
        let path = dir.path().join("test.db");
        StandardDiskOps::new(&path, TEST_PAGE_SIZE).expect("failed to create StandardDiskOps")
    }

    #[tokio::test]
    async fn standard_read_write_roundtrip() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);

        let page: Vec<u8> = (0..TEST_PAGE_SIZE).map(|i| (i % 256) as u8).collect();
        ops.write_page(0, &page).await.unwrap();

        let mut buf = vec![0u8; TEST_PAGE_SIZE];
        ops.read_page(0, &mut buf).await.unwrap();
        assert_eq!(buf, page);
    }

    #[tokio::test]
    async fn standard_sync_does_not_panic() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);
        ops.sync().await.unwrap();
    }

    #[tokio::test]
    async fn write_then_read_multiple_pages() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);

        for page_id in 0..5u32 {
            let data = vec![page_id as u8; TEST_PAGE_SIZE];
            ops.write_page(page_id, &data).await.unwrap();
        }

        for page_id in 0..5u32 {
            let mut buf = vec![0u8; TEST_PAGE_SIZE];
            ops.read_page(page_id, &mut buf).await.unwrap();
            assert!(buf.iter().all(|&b| b == page_id as u8));
        }
    }

    #[tokio::test]
    async fn page_size_accessor() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);
        assert_eq!(ops.page_size(), TEST_PAGE_SIZE);
    }

    #[tokio::test]
    async fn factory_creates_standard_when_direct_io_false() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("factory_std.db");
        let ops = create_disk_ops(&path, TEST_PAGE_SIZE, false).unwrap();
        assert_eq!(ops.page_size(), TEST_PAGE_SIZE);

        // Verify it works by doing a write/read roundtrip.
        let data = vec![0xAB; TEST_PAGE_SIZE];
        ops.write_page(0, &data).await.unwrap();
        let mut buf = vec![0u8; TEST_PAGE_SIZE];
        ops.read_page(0, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn factory_creates_io_uring_when_direct_io_true() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("factory_uring.db");
        let ops = create_disk_ops(&path, TEST_PAGE_SIZE, true).unwrap();
        assert_eq!(ops.page_size(), TEST_PAGE_SIZE);

        // Verify it works (on non-Linux, delegates to StandardDiskOps).
        let data = vec![0xCD; TEST_PAGE_SIZE];
        ops.write_page(0, &data).await.unwrap();
        let mut buf = vec![0u8; TEST_PAGE_SIZE];
        ops.read_page(0, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn read_past_eof_returns_zeros() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);

        // File is empty -- reading page 99 should yield all zeros.
        let mut buf = vec![0xFFu8; TEST_PAGE_SIZE];
        ops.read_page(99, &mut buf).await.unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn write_at_various_offsets() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);

        // Write pages 0, 5, 10 (with gaps).
        for &page_id in &[0u32, 5, 10] {
            let data = vec![page_id as u8; TEST_PAGE_SIZE];
            ops.write_page(page_id, &data).await.unwrap();
        }

        for &page_id in &[0u32, 5, 10] {
            let mut buf = vec![0u8; TEST_PAGE_SIZE];
            ops.read_page(page_id, &mut buf).await.unwrap();
            assert!(buf.iter().all(|&b| b == page_id as u8));
        }
    }

    #[tokio::test]
    async fn disk_ops_config_defaults() {
        let cfg = DiskOpsConfig::default();
        assert_eq!(cfg.page_size, 16384);
        assert!(!cfg.use_direct_io);
        assert_eq!(cfg.queue_depth, 256);
    }

    #[tokio::test]
    async fn concurrent_read_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("concurrent.db");
        let ops = std::sync::Arc::new(create_disk_ops(&path, TEST_PAGE_SIZE, false).unwrap());

        // Spawn several writers in parallel, each writing a distinct page.
        let mut handles = Vec::new();
        for page_id in 0..8u32 {
            let ops = ops.clone();
            handles.push(tokio::spawn(async move {
                let data = vec![page_id as u8; TEST_PAGE_SIZE];
                ops.write_page(page_id, &data).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Now read them all back and verify.
        for page_id in 0..8u32 {
            let mut buf = vec![0u8; TEST_PAGE_SIZE];
            ops.read_page(page_id, &mut buf).await.unwrap();
            assert!(buf.iter().all(|&b| b == page_id as u8));
        }
    }

    // -- IoBatchQueue tests --

    #[tokio::test]
    async fn batch_queue_submit_and_execute() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);
        let mut queue = IoBatchQueue::new(64);

        // Write 3 pages via batch
        queue.submit_write(0, vec![0xAA; TEST_PAGE_SIZE]);
        queue.submit_write(1, vec![0xBB; TEST_PAGE_SIZE]);
        queue.submit_write(2, vec![0xCC; TEST_PAGE_SIZE]);
        assert_eq!(queue.pending_count(), 3);

        let results = queue.execute(&ops).await;
        assert_eq!(results.len(), 3);
        assert!(matches!(results[0], IoResult::WriteComplete { page_id: 0 }));
        assert!(matches!(results[1], IoResult::WriteComplete { page_id: 1 }));
        assert!(matches!(results[2], IoResult::WriteComplete { page_id: 2 }));
        assert_eq!(queue.completed_count(), 3);
        assert_eq!(queue.submitted_count(), 3);
        assert_eq!(queue.pending_count(), 0);

        // Read them back via batch
        queue.submit_read(0);
        queue.submit_read(1);
        queue.submit_read(2);
        let results = queue.execute(&ops).await;
        assert_eq!(results.len(), 3);
        for (i, result) in results.iter().enumerate() {
            match result {
                IoResult::ReadComplete { page_id, data } => {
                    assert_eq!(*page_id, i as u32);
                    assert!(data.iter().all(|&b| b == [0xAA, 0xBB, 0xCC][i]));
                }
                _ => panic!("expected ReadComplete"),
            }
        }
    }

    #[tokio::test]
    async fn batch_queue_sync() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);
        let mut queue = IoBatchQueue::new(64);

        queue.submit_sync();
        let results = queue.execute(&ops).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], IoResult::SyncComplete));
    }

    #[tokio::test]
    async fn batch_queue_full_detection() {
        let mut queue = IoBatchQueue::new(3);
        assert!(!queue.is_full());
        queue.submit_read(0);
        queue.submit_read(1);
        assert!(!queue.is_full());
        queue.submit_read(2);
        assert!(queue.is_full());
    }

    #[tokio::test]
    async fn batch_queue_mixed_operations() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);
        let mut queue = IoBatchQueue::new(64);

        // Write, read, sync in one batch
        queue.submit_write(5, vec![0xDD; TEST_PAGE_SIZE]);
        queue.submit_read(5);
        queue.submit_sync();

        let results = queue.execute(&ops).await;
        assert_eq!(results.len(), 3);
        assert!(matches!(results[0], IoResult::WriteComplete { page_id: 5 }));
        match &results[1] {
            IoResult::ReadComplete { page_id, data } => {
                assert_eq!(*page_id, 5);
                assert!(data.iter().all(|&b| b == 0xDD));
            }
            _ => panic!("expected ReadComplete"),
        }
        assert!(matches!(results[2], IoResult::SyncComplete));
        assert_eq!(queue.completed_count(), 3);
    }

    #[tokio::test]
    async fn batch_queue_empty_execute() {
        let dir = TempDir::new().unwrap();
        let ops = setup_standard(&dir);
        let mut queue = IoBatchQueue::new(64);

        let results = queue.execute(&ops).await;
        assert!(results.is_empty());
        assert_eq!(queue.submitted_count(), 0);
    }

    // -- IoUringDiskOps constructor tests (work on all platforms) --

    #[tokio::test]
    async fn io_uring_ops_new_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("uring_test.db");
        let _ops = IoUringDiskOps::new(&path, TEST_PAGE_SIZE, false).unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn io_uring_ops_new_with_queue_depth() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("uring_qd.db");
        let ops = IoUringDiskOps::new_with_queue_depth(&path, 8192, false, 128).unwrap();
        assert_eq!(ops.page_size(), 8192);
    }

    #[tokio::test]
    async fn io_uring_ops_read_write_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("uring_rw.db");
        let ops = IoUringDiskOps::new(&path, TEST_PAGE_SIZE, false).unwrap();

        let data = vec![0xEE; TEST_PAGE_SIZE];
        ops.write_page(0, &data).await.unwrap();

        let mut buf = vec![0u8; TEST_PAGE_SIZE];
        ops.read_page(0, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn io_uring_ops_multiple_pages() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("uring_multi.db");
        let ops = IoUringDiskOps::new(&path, TEST_PAGE_SIZE, false).unwrap();

        for pg in 0..4u32 {
            let data = vec![(pg + 1) as u8; TEST_PAGE_SIZE];
            ops.write_page(pg, &data).await.unwrap();
        }

        for pg in 0..4u32 {
            let mut buf = vec![0u8; TEST_PAGE_SIZE];
            ops.read_page(pg, &mut buf).await.unwrap();
            assert!(buf.iter().all(|&b| b == (pg + 1) as u8));
        }
    }

    #[tokio::test]
    async fn io_uring_ops_sync() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("uring_sync.db");
        let ops = IoUringDiskOps::new(&path, TEST_PAGE_SIZE, false).unwrap();
        ops.sync().await.unwrap();
    }

    #[tokio::test]
    async fn create_disk_ops_with_config_standard() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config_std.db");
        let config = DiskOpsConfig {
            page_size: 8192,
            use_direct_io: false,
            queue_depth: 64,
        };
        let ops = create_disk_ops_with_config(&path, &config).unwrap();
        assert_eq!(ops.page_size(), 8192);
    }

    #[tokio::test]
    async fn create_disk_ops_with_config_direct_io() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config_dio.db");
        let config = DiskOpsConfig {
            page_size: TEST_PAGE_SIZE,
            use_direct_io: true,
            queue_depth: 128,
        };
        let ops = create_disk_ops_with_config(&path, &config).unwrap();
        assert_eq!(ops.page_size(), TEST_PAGE_SIZE);

        // Roundtrip test
        let data = vec![0xFF; TEST_PAGE_SIZE];
        ops.write_page(0, &data).await.unwrap();
        let mut buf = vec![0u8; TEST_PAGE_SIZE];
        ops.read_page(0, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }
}
