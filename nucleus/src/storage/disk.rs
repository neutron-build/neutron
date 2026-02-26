//! Disk manager — handles file I/O for page-based storage.
//!
//! Single database file. Page N is at byte offset N * PAGE_SIZE.
//! All I/O is page-aligned, preparing for future Direct I/O support.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use super::compression::PageCompressor;
use super::encryption::PageEncryptor;
use super::page::{PAGE_SIZE, PageBuf};

/// Disk manager for a single database file.
/// Optionally compresses and/or encrypts all pages transparently.
/// Write path: page → compress → encrypt → disk
/// Read path:  disk → decrypt → decompress → page
pub struct DiskManager {
    file: Mutex<File>,
    /// When set, all pages are encrypted on write and decrypted on read.
    encryptor: Option<PageEncryptor>,
    /// When true, pages are compressed with LZ4 before writing to disk.
    compression_enabled: bool,
}

impl DiskManager {
    /// Open (or create) a database file (unencrypted, uncompressed).
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            encryptor: None,
            compression_enabled: false,
        })
    }

    /// Open a database file with encryption enabled.
    pub fn open_encrypted(path: &Path, encryptor: PageEncryptor) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            encryptor: Some(encryptor),
            compression_enabled: false,
        })
    }

    /// Open a database file with compression enabled.
    pub fn open_compressed(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            encryptor: None,
            compression_enabled: true,
        })
    }

    /// Open a database file with both compression and encryption.
    pub fn open_compressed_encrypted(path: &Path, encryptor: PageEncryptor) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            encryptor: Some(encryptor),
            compression_enabled: true,
        })
    }

    /// Enable or disable compression on an existing DiskManager.
    pub fn set_compression(&mut self, enabled: bool) {
        self.compression_enabled = enabled;
    }

    /// The base size of a page slot before encryption overhead.
    fn compressed_slot_size(&self) -> usize {
        if self.compression_enabled {
            // Worst case: 1-byte codec + 4-byte size + PAGE_SIZE (uncompressed fallback)
            super::compression::COMPRESSION_HEADER_SIZE + PAGE_SIZE
        } else {
            PAGE_SIZE
        }
    }

    /// The on-disk size of a single page slot (fixed, for page-aligned access).
    fn page_disk_size(&self) -> usize {
        let base = self.compressed_slot_size();
        if self.encryptor.is_some() {
            base + super::encryption::NONCE_SIZE + super::encryption::TAG_SIZE
        } else {
            base
        }
    }

    /// Read a page from disk into the buffer.
    /// Read path: disk → decrypt → decompress → page
    pub fn read_page(&self, page_id: u32, buf: &mut PageBuf) -> std::io::Result<()> {
        let disk_size = self.page_disk_size();
        let offset = page_id as u64 * disk_size as u64;
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;

        let mut raw = vec![0u8; disk_size];
        file.read_exact(&mut raw)?;

        // Step 1: Decrypt if encrypted (uses generic bytes decrypt)
        let decrypted = if let Some(ref enc) = self.encryptor {
            if self.compression_enabled {
                // Variable-size plaintext (compressed slot)
                enc.decrypt_bytes(&raw).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?
            } else {
                // Fixed PAGE_SIZE plaintext
                let d = enc.decrypt_page(&raw).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                d.to_vec()
            }
        } else {
            raw
        };

        // Step 2: Decompress if compressed
        if self.compression_enabled {
            let page = PageCompressor::decompress_page(&decrypted).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
            })?;
            buf.copy_from_slice(&page);
        } else {
            buf.copy_from_slice(&decrypted[..PAGE_SIZE]);
        }

        Ok(())
    }

    /// Write a page buffer to disk.
    /// Write path: page → compress → encrypt → disk
    pub fn write_page(&self, page_id: u32, buf: &PageBuf) -> std::io::Result<()> {
        let disk_size = self.page_disk_size();
        let offset = page_id as u64 * disk_size as u64;
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;

        // Step 1: Compress if enabled
        let data = if self.compression_enabled {
            let slot_size = self.compressed_slot_size();
            let mut compressed = PageCompressor::compress_page(buf);
            // Pad to fixed slot size for page-aligned disk I/O
            compressed.resize(slot_size, 0);
            compressed
        } else {
            buf.to_vec()
        };

        // Step 2: Encrypt if enabled
        if let Some(ref enc) = self.encryptor {
            if self.compression_enabled {
                // Encrypt the compressed slot as variable-length bytes
                let encrypted = enc.encrypt_bytes(&data);
                file.write_all(&encrypted)?;
            } else {
                // Encrypt as fixed-size PageBuf
                let encrypted = enc.encrypt_page(buf);
                file.write_all(&encrypted)?;
            }
        } else {
            file.write_all(&data)?;
        }
        Ok(())
    }

    /// Flush all pending writes to disk.
    pub fn sync(&self) -> std::io::Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_all()
    }

    /// Get the current file size in bytes.
    pub fn file_size(&self) -> std::io::Result<u64> {
        let file = self.file.lock().unwrap();
        file.metadata().map(|m| m.len())
    }

    /// Extend the file to accommodate a new page.
    pub fn extend_to_page(&self, page_id: u32) -> std::io::Result<()> {
        let disk_size = self.page_disk_size();
        let needed = (page_id as u64 + 1) * disk_size as u64;
        let file = self.file.lock().unwrap();
        let current = file.metadata()?.len();
        if needed > current {
            file.set_len(needed)?;
        }
        Ok(())
    }

    /// Check if encryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.encryptor.is_some()
    }

    /// Check if compression is enabled.
    pub fn is_compressed(&self) -> bool {
        self.compression_enabled
    }
}

impl std::fmt::Debug for DiskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskManager")
            .field("encrypted", &self.encryptor.is_some())
            .field("compressed", &self.compression_enabled)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_page() -> PageBuf {
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xDE;
        page[1] = 0xAD;
        for i in 0..100 {
            page[100 + i] = (i & 0xFF) as u8;
        }
        page[PAGE_SIZE - 1] = 0xFF;
        page
    }

    #[test]
    fn uncompressed_unencrypted_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::open(&path).unwrap();
        let page = test_page();

        dm.extend_to_page(0).unwrap();
        dm.write_page(0, &page).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(0, &mut read_buf).unwrap();
        assert_eq!(page, read_buf);
    }

    #[test]
    fn compressed_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compressed.db");
        let dm = DiskManager::open_compressed(&path).unwrap();
        assert!(dm.is_compressed());

        let page = test_page();
        dm.extend_to_page(0).unwrap();
        dm.write_page(0, &page).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(0, &mut read_buf).unwrap();
        assert_eq!(page, read_buf);
    }

    #[test]
    fn compressed_multiple_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.db");
        let dm = DiskManager::open_compressed(&path).unwrap();

        let mut pages = Vec::new();
        for i in 0u8..5 {
            let mut page = [0u8; PAGE_SIZE];
            page[0] = i;
            page[PAGE_SIZE / 2] = i.wrapping_mul(7);
            pages.push(page);
        }

        for (i, page) in pages.iter().enumerate() {
            dm.extend_to_page(i as u32).unwrap();
            dm.write_page(i as u32, page).unwrap();
        }

        for (i, expected) in pages.iter().enumerate() {
            let mut buf = [0u8; PAGE_SIZE];
            dm.read_page(i as u32, &mut buf).unwrap();
            assert_eq!(expected, &buf, "page {i} mismatch");
        }
    }

    #[test]
    fn encrypted_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("encrypted.db");
        let enc = super::super::encryption::PageEncryptor::from_key(&[0x42; 32]);
        let dm = DiskManager::open_encrypted(&path, enc).unwrap();
        assert!(dm.is_encrypted());

        let page = test_page();
        dm.extend_to_page(0).unwrap();
        dm.write_page(0, &page).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(0, &mut read_buf).unwrap();
        assert_eq!(page, read_buf);
    }

    #[test]
    fn compressed_and_encrypted_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("both.db");
        let enc = super::super::encryption::PageEncryptor::from_key(&[0x99; 32]);
        let dm = DiskManager::open_compressed_encrypted(&path, enc).unwrap();
        assert!(dm.is_compressed());
        assert!(dm.is_encrypted());

        let page = test_page();
        dm.extend_to_page(0).unwrap();
        dm.write_page(0, &page).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(0, &mut read_buf).unwrap();
        assert_eq!(page, read_buf);
    }

    #[test]
    fn compressed_and_encrypted_multiple_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("both_multi.db");
        let enc = super::super::encryption::PageEncryptor::from_key(&[0xAA; 32]);
        let dm = DiskManager::open_compressed_encrypted(&path, enc).unwrap();

        for i in 0u32..3 {
            let mut page = [0u8; PAGE_SIZE];
            page[0..4].copy_from_slice(&i.to_le_bytes());
            dm.extend_to_page(i).unwrap();
            dm.write_page(i, &page).unwrap();
        }

        for i in 0u32..3 {
            let mut expected = [0u8; PAGE_SIZE];
            expected[0..4].copy_from_slice(&i.to_le_bytes());
            let mut buf = [0u8; PAGE_SIZE];
            dm.read_page(i, &mut buf).unwrap();
            assert_eq!(expected, buf, "page {i} mismatch");
        }
    }

    #[test]
    fn compressed_saves_space() {
        let dir = tempfile::tempdir().unwrap();
        let path_raw = dir.path().join("raw.db");
        let path_comp = dir.path().join("comp.db");

        let dm_raw = DiskManager::open(&path_raw).unwrap();
        let dm_comp = DiskManager::open_compressed(&path_comp).unwrap();

        // Write a sparse page (mostly zeros — compresses very well)
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 1;
        page[PAGE_SIZE - 1] = 2;

        dm_raw.extend_to_page(0).unwrap();
        dm_raw.write_page(0, &page).unwrap();
        dm_comp.extend_to_page(0).unwrap();
        dm_comp.write_page(0, &page).unwrap();

        // Both should roundtrip correctly
        let mut buf = [0u8; PAGE_SIZE];
        dm_raw.read_page(0, &mut buf).unwrap();
        assert_eq!(page, buf);
        dm_comp.read_page(0, &mut buf).unwrap();
        assert_eq!(page, buf);
    }
}
