//! Page format definitions and slotted page operations.
//!
//! Every page in Nucleus is exactly PAGE_SIZE bytes. Every page starts with a
//! common 16-byte header containing page_type, format_version, checksum, and LSN.
//!
//! Data pages use a slotted layout: slot directory grows forward from the header,
//! tuple data grows backward from the end of the page.

/// Page size: 16 KB. Optimal for NVMe SSDs.
pub const PAGE_SIZE: usize = 16_384;

/// Invalid page ID sentinel.
pub const INVALID_PAGE_ID: u32 = u32::MAX;

// ============================================================================
// Page type constants
// ============================================================================

pub const PAGE_TYPE_FREE: u16 = 0;
pub const PAGE_TYPE_DATA: u16 = 1;
pub const PAGE_TYPE_INDEX: u16 = 2;
pub const PAGE_TYPE_OVERFLOW: u16 = 3;
pub const PAGE_TYPE_FSM: u16 = 4;
pub const PAGE_TYPE_META: u16 = 5;

// ============================================================================
// Common page header (16 bytes, present on every page)
// ============================================================================

/// Byte offset and sizes for the common header fields.
pub const HEADER_PAGE_TYPE: usize = 0; // u16
pub const HEADER_FORMAT_VERSION: usize = 2; // u16
pub const HEADER_CHECKSUM: usize = 4; // u32
pub const HEADER_LSN: usize = 8; // u64
pub const COMMON_HEADER_SIZE: usize = 16;

// ============================================================================
// Data page sub-header (12 bytes, follows common header)
// ============================================================================

pub const DATA_SLOT_COUNT: usize = COMMON_HEADER_SIZE; // u16
pub const DATA_FREE_START: usize = COMMON_HEADER_SIZE + 2; // u16
pub const DATA_FREE_END: usize = COMMON_HEADER_SIZE + 4; // u16
pub const DATA_FRAG_FREE: usize = COMMON_HEADER_SIZE + 6; // u16
pub const DATA_FLAGS: usize = COMMON_HEADER_SIZE + 8; // u16
pub const DATA_RESERVED: usize = COMMON_HEADER_SIZE + 10; // u16
pub const DATA_HEADER_SIZE: usize = COMMON_HEADER_SIZE + 12; // = 28

/// Slot entry size in bytes.
pub const SLOT_SIZE: usize = 4;

// ============================================================================
// Meta page layout (page 0)
// ============================================================================

pub const META_MAGIC: usize = COMMON_HEADER_SIZE; // [u8; 8]
pub const META_DB_VERSION: usize = COMMON_HEADER_SIZE + 8; // u32
pub const META_PAGE_SIZE: usize = COMMON_HEADER_SIZE + 12; // u32
pub const META_TOTAL_PAGES: usize = COMMON_HEADER_SIZE + 16; // u32
pub const META_FREE_LIST_HEAD: usize = COMMON_HEADER_SIZE + 20; // u32
pub const META_FREE_PAGE_COUNT: usize = COMMON_HEADER_SIZE + 24; // u32
pub const META_CATALOG_ROOT: usize = COMMON_HEADER_SIZE + 28; // u32
pub const META_FSM_ROOT: usize = COMMON_HEADER_SIZE + 32; // u32
pub const META_LAST_TXN_ID: usize = COMMON_HEADER_SIZE + 36; // u64
pub const META_CHECKPOINT_LSN: usize = COMMON_HEADER_SIZE + 44; // u64

/// Start of the inline table directory in the meta page (after META_CHECKPOINT_LSN + 8).
/// Format: u32 entry_count, then for each entry:
///   u16 name_len, [u8; name_len] name, u32 first_page_id, u16 col_count,
///   then col_count serialized DataType bytes.
pub const META_TABLE_DIR_START: usize = COMMON_HEADER_SIZE + 52;

pub const MAGIC_BYTES: &[u8; 8] = b"NUCLEUS\0";
pub const DB_FORMAT_VERSION: u32 = 1;

// ============================================================================
// Overflow page layout
// ============================================================================

pub const OVERFLOW_NEXT_PAGE: usize = COMMON_HEADER_SIZE; // u32
pub const OVERFLOW_PAYLOAD_LEN: usize = COMMON_HEADER_SIZE + 4; // u16
pub const OVERFLOW_HEADER_SIZE: usize = COMMON_HEADER_SIZE + 8; // = 24
pub const OVERFLOW_PAYLOAD_CAP: usize = PAGE_SIZE - OVERFLOW_HEADER_SIZE;

// ============================================================================
// Free page layout
// ============================================================================

pub const FREE_NEXT_PAGE: usize = COMMON_HEADER_SIZE; // u32

// ============================================================================
// Page buffer type
// ============================================================================

/// A raw page buffer. All page operations work on this type.
pub type PageBuf = [u8; PAGE_SIZE];

// ============================================================================
// Byte helpers (little-endian)
// ============================================================================

#[inline]
pub fn read_u16(page: &PageBuf, offset: usize) -> u16 {
    u16::from_le_bytes([page[offset], page[offset + 1]])
}

#[inline]
pub fn write_u16(page: &mut PageBuf, offset: usize, val: u16) {
    let bytes = val.to_le_bytes();
    page[offset] = bytes[0];
    page[offset + 1] = bytes[1];
}

#[inline]
pub fn read_u32(page: &PageBuf, offset: usize) -> u32 {
    u32::from_le_bytes([
        page[offset],
        page[offset + 1],
        page[offset + 2],
        page[offset + 3],
    ])
}

#[inline]
pub fn write_u32(page: &mut PageBuf, offset: usize, val: u32) {
    let bytes = val.to_le_bytes();
    page[offset..offset + 4].copy_from_slice(&bytes);
}

#[inline]
pub fn read_u64(page: &PageBuf, offset: usize) -> u64 {
    u64::from_le_bytes([
        page[offset],
        page[offset + 1],
        page[offset + 2],
        page[offset + 3],
        page[offset + 4],
        page[offset + 5],
        page[offset + 6],
        page[offset + 7],
    ])
}

#[inline]
pub fn write_u64(page: &mut PageBuf, offset: usize, val: u64) {
    let bytes = val.to_le_bytes();
    page[offset..offset + 8].copy_from_slice(&bytes);
}

// ============================================================================
// Common header operations
// ============================================================================

pub fn get_page_type(page: &PageBuf) -> u16 {
    read_u16(page, HEADER_PAGE_TYPE)
}

pub fn get_format_version(page: &PageBuf) -> u16 {
    read_u16(page, HEADER_FORMAT_VERSION)
}

pub fn get_page_lsn(page: &PageBuf) -> u64 {
    read_u64(page, HEADER_LSN)
}

pub fn set_page_lsn(page: &mut PageBuf, lsn: u64) {
    write_u64(page, HEADER_LSN, lsn);
}

/// Compute and write checksum for the page.
pub fn write_checksum(page: &mut PageBuf) {
    // Zero the checksum field before computing
    write_u32(page, HEADER_CHECKSUM, 0);
    let crc = crc32c::crc32c(page);
    write_u32(page, HEADER_CHECKSUM, crc);
}

/// Verify page checksum. Returns true if valid.
pub fn verify_checksum(page: &PageBuf) -> bool {
    let stored = read_u32(page, HEADER_CHECKSUM);
    let mut tmp = *page;
    write_u32(&mut tmp, HEADER_CHECKSUM, 0);
    let computed = crc32c::crc32c(&tmp);
    stored == computed
}

// ============================================================================
// Data page initialization
// ============================================================================

/// Initialize a fresh data page.
pub fn init_data_page(page: &mut PageBuf, format_version: u16) {
    page.fill(0);
    write_u16(page, HEADER_PAGE_TYPE, PAGE_TYPE_DATA);
    write_u16(page, HEADER_FORMAT_VERSION, format_version);
    write_u16(page, DATA_SLOT_COUNT, 0);
    write_u16(page, DATA_FREE_START, DATA_HEADER_SIZE as u16);
    write_u16(page, DATA_FREE_END, PAGE_SIZE as u16);
    write_u16(page, DATA_FRAG_FREE, 0);
}

// ============================================================================
// Slotted page operations
// ============================================================================

/// Usable space on a fresh data page.
pub const DATA_USABLE: usize = PAGE_SIZE - DATA_HEADER_SIZE;

/// Maximum inline tuple size (anything bigger must overflow).
/// We leave room for at least one slot entry.
pub const MAX_INLINE_TUPLE: usize = DATA_USABLE - SLOT_SIZE;

/// Slot entry: packed u32.
///   bits 0-14:  byte offset from page start (max 16383)
///   bit 15:     dead flag
///   bits 16-30: tuple length (max 16383)
///   bit 31:     overflow flag
#[derive(Clone, Copy)]
pub struct SlotEntry(u32);

impl SlotEntry {
    pub fn new(offset: u16, length: u16, dead: bool, overflow: bool) -> Self {
        let mut packed = (offset as u32) & 0x7FFF;
        if dead {
            packed |= 0x8000;
        }
        packed |= ((length as u32) & 0x7FFF) << 16;
        if overflow {
            packed |= 0x8000_0000;
        }
        Self(packed)
    }

    pub fn offset(self) -> u16 {
        (self.0 & 0x7FFF) as u16
    }

    pub fn is_dead(self) -> bool {
        (self.0 & 0x8000) != 0
    }

    pub fn length(self) -> u16 {
        ((self.0 >> 16) & 0x7FFF) as u16
    }

    pub fn is_overflow(self) -> bool {
        (self.0 & 0x8000_0000) != 0
    }

    pub fn to_u32(self) -> u32 {
        self.0
    }

    pub fn from_u32(v: u32) -> Self {
        Self(v)
    }
}

/// Read a slot entry from the page.
pub fn read_slot(page: &PageBuf, slot_idx: u16) -> SlotEntry {
    let off = DATA_HEADER_SIZE + (slot_idx as usize) * SLOT_SIZE;
    SlotEntry::from_u32(read_u32(page, off))
}

/// Write a slot entry to the page.
fn write_slot(page: &mut PageBuf, slot_idx: u16, entry: SlotEntry) {
    let off = DATA_HEADER_SIZE + (slot_idx as usize) * SLOT_SIZE;
    write_u32(page, off, entry.to_u32());
}

/// Available free space on a data page (contiguous only, ignoring fragmented).
pub fn free_space(page: &PageBuf) -> usize {
    let start = read_u16(page, DATA_FREE_START) as usize;
    let end = read_u16(page, DATA_FREE_END) as usize;
    end.saturating_sub(start)
}

/// Total reclaimable space (contiguous + fragmented).
pub fn total_free(page: &PageBuf) -> usize {
    free_space(page) + read_u16(page, DATA_FRAG_FREE) as usize
}

/// Insert a tuple into a data page. Returns the slot index, or None if no space.
pub fn insert_tuple(page: &mut PageBuf, data: &[u8]) -> Option<u16> {
    let needed = data.len() + SLOT_SIZE;
    let avail = free_space(page);

    if needed > avail {
        // Check if compaction would help
        if needed <= avail + read_u16(page, DATA_FRAG_FREE) as usize {
            compact_page(page);
            return insert_tuple(page, data);
        }
        return None; // page full
    }

    let free_end = read_u16(page, DATA_FREE_END) as usize;
    let tuple_offset = free_end - data.len();
    page[tuple_offset..tuple_offset + data.len()].copy_from_slice(data);

    // Find a dead slot to reuse, or append new slot
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    let slot_idx = {
        let mut reuse = None;
        for i in 0..slot_count {
            let entry = read_slot(page, i);
            if entry.is_dead() {
                reuse = Some(i);
                break;
            }
        }
        if let Some(idx) = reuse {
            idx
        } else {
            // Append new slot
            write_u16(page, DATA_SLOT_COUNT, slot_count + 1);
            let new_start = read_u16(page, DATA_FREE_START) + SLOT_SIZE as u16;
            write_u16(page, DATA_FREE_START, new_start);
            slot_count
        }
    };

    let entry = SlotEntry::new(tuple_offset as u16, data.len() as u16, false, false);
    write_slot(page, slot_idx, entry);
    write_u16(page, DATA_FREE_END, tuple_offset as u16);

    Some(slot_idx)
}

/// Read tuple data for a given slot. Returns None if slot is dead.
pub fn read_tuple(page: &PageBuf, slot_idx: u16) -> Option<&[u8]> {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    if slot_idx >= slot_count {
        return None;
    }
    let entry = read_slot(page, slot_idx);
    if entry.is_dead() {
        return None;
    }
    let off = entry.offset() as usize;
    let len = entry.length() as usize;
    Some(&page[off..off + len])
}

/// Mark a slot as dead (logical delete). Returns the freed tuple length.
pub fn delete_tuple(page: &mut PageBuf, slot_idx: u16) -> Option<u16> {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    if slot_idx >= slot_count {
        return None;
    }
    let entry = read_slot(page, slot_idx);
    if entry.is_dead() {
        return None;
    }
    let len = entry.length();
    let dead_entry = SlotEntry::new(entry.offset(), len, true, entry.is_overflow());
    write_slot(page, slot_idx, dead_entry);

    // Track fragmented free space
    let frag = read_u16(page, DATA_FRAG_FREE);
    write_u16(page, DATA_FRAG_FREE, frag + len);

    Some(len)
}

/// Update a tuple in place if the new data fits, otherwise returns false.
pub fn update_tuple_in_place(page: &mut PageBuf, slot_idx: u16, new_data: &[u8]) -> bool {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    if slot_idx >= slot_count {
        return false;
    }
    let entry = read_slot(page, slot_idx);
    if entry.is_dead() {
        return false;
    }

    let old_len = entry.length() as usize;
    if new_data.len() <= old_len {
        // Fits in existing space
        let off = entry.offset() as usize;
        page[off..off + new_data.len()].copy_from_slice(new_data);
        // If shorter, the leftover becomes fragmented free
        if new_data.len() < old_len {
            let diff = (old_len - new_data.len()) as u16;
            let new_entry =
                SlotEntry::new(entry.offset(), new_data.len() as u16, false, false);
            write_slot(page, slot_idx, new_entry);
            let frag = read_u16(page, DATA_FRAG_FREE);
            write_u16(page, DATA_FRAG_FREE, frag + diff);
        }
        true
    } else {
        // Doesn't fit — caller must delete + re-insert (possibly on a different page)
        false
    }
}

/// Compact a data page: defragment by moving all live tuples to the end.
pub fn compact_page(page: &mut PageBuf) {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);

    // Collect live tuples
    let mut live: Vec<(u16, Vec<u8>)> = Vec::new(); // (slot_idx, data)
    for i in 0..slot_count {
        let entry = read_slot(page, i);
        if !entry.is_dead() {
            let off = entry.offset() as usize;
            let len = entry.length() as usize;
            live.push((i, page[off..off + len].to_vec()));
        }
    }

    // Rewrite tuples from end of page
    let mut write_pos = PAGE_SIZE;
    for (slot_idx, data) in &live {
        write_pos -= data.len();
        page[write_pos..write_pos + data.len()].copy_from_slice(data);
        let new_entry = SlotEntry::new(write_pos as u16, data.len() as u16, false, false);
        write_slot(page, *slot_idx, new_entry);
    }

    write_u16(page, DATA_FREE_END, write_pos as u16);
    write_u16(page, DATA_FRAG_FREE, 0);
}

/// Iterator over live tuples: yields (slot_idx, tuple_bytes).
pub fn iter_tuples(page: &PageBuf) -> Vec<(u16, &[u8])> {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    let mut result = Vec::new();
    for i in 0..slot_count {
        let entry = read_slot(page, i);
        if !entry.is_dead() {
            let off = entry.offset() as usize;
            let len = entry.length() as usize;
            result.push((i, &page[off..off + len]));
        }
    }
    result
}

/// Count live (non-dead) tuples on the page without allocating.
pub fn count_live_tuples(page: &PageBuf) -> usize {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    let mut count = 0;
    for i in 0..slot_count {
        let entry = read_slot(page, i);
        if !entry.is_dead() {
            count += 1;
        }
    }
    count
}

/// Number of dead (deleted) tuples on the page.
pub fn dead_tuple_count(page: &PageBuf) -> usize {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    let mut count = 0;
    for i in 0..slot_count {
        if read_slot(page, i).is_dead() {
            count += 1;
        }
    }
    count
}

/// Total slot count (live + dead) on the page.
pub fn slot_count(page: &PageBuf) -> u16 {
    read_u16(page, DATA_SLOT_COUNT)
}

/// Number of live (non-dead) tuples on the page.
pub fn live_tuple_count(page: &PageBuf) -> usize {
    let slot_count = read_u16(page, DATA_SLOT_COUNT);
    let mut count = 0;
    for i in 0..slot_count {
        if !read_slot(page, i).is_dead() {
            count += 1;
        }
    }
    count
}

// ============================================================================
// Meta page operations
// ============================================================================

/// Initialize the meta page (page 0).
pub fn init_meta_page(page: &mut PageBuf) {
    page.fill(0);
    write_u16(page, HEADER_PAGE_TYPE, PAGE_TYPE_META);
    write_u16(page, HEADER_FORMAT_VERSION, 1);
    page[META_MAGIC..META_MAGIC + 8].copy_from_slice(MAGIC_BYTES);
    write_u32(page, META_DB_VERSION, DB_FORMAT_VERSION);
    write_u32(page, META_PAGE_SIZE, PAGE_SIZE as u32);
    write_u32(page, META_TOTAL_PAGES, 1); // just the meta page itself
    write_u32(page, META_FREE_LIST_HEAD, INVALID_PAGE_ID);
    write_u32(page, META_FREE_PAGE_COUNT, 0);
    write_u32(page, META_CATALOG_ROOT, INVALID_PAGE_ID);
    write_u32(page, META_FSM_ROOT, INVALID_PAGE_ID);
    // Initialize table directory overflow pointer to INVALID_PAGE_ID (no overflow)
    write_u32(page, PAGE_SIZE - 4, INVALID_PAGE_ID);
}

// ============================================================================
// Free page operations
// ============================================================================

/// Initialize a free page pointing to the next free page.
pub fn init_free_page(page: &mut PageBuf, next_page_id: u32) {
    page.fill(0);
    write_u16(page, HEADER_PAGE_TYPE, PAGE_TYPE_FREE);
    write_u32(page, FREE_NEXT_PAGE, next_page_id);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_basic_insert_read() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);
        let data = b"hello world";
        let slot = insert_tuple(&mut page, data).expect("insert should succeed");
        let read_back = read_tuple(&page, slot).expect("read should succeed");
        assert_eq!(read_back, data);
    }

    #[test]
    fn test_varying_tuple_sizes() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);

        let tuples: Vec<Vec<u8>> = vec![
            vec![0xAA; 1],       // minimal
            vec![0xBB; 100],     // medium
            vec![0xCC; 1000],    // large
            vec![0xDD; 255],     // odd size
        ];

        let mut slots = Vec::new();
        for data in &tuples {
            let slot = insert_tuple(&mut page, data).expect("insert should succeed");
            slots.push(slot);
        }

        for (i, slot) in slots.iter().enumerate() {
            let read_back = read_tuple(&page, *slot).expect("should read back");
            assert_eq!(read_back.len(), tuples[i].len());
            assert_eq!(read_back, tuples[i].as_slice());
        }
        assert_eq!(live_tuple_count(&page), tuples.len());
    }

    #[test]
    fn test_delete_marks_slot_dead() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);

        let slot0 = insert_tuple(&mut page, b"first").unwrap();
        let slot1 = insert_tuple(&mut page, b"second").unwrap();
        let slot2 = insert_tuple(&mut page, b"third").unwrap();

        assert_eq!(live_tuple_count(&page), 3);

        delete_tuple(&mut page, slot1);

        let entry = read_slot(&page, slot1);
        assert!(entry.is_dead());
        assert!(read_tuple(&page, slot1).is_none());

        assert_eq!(read_tuple(&page, slot0), Some(b"first".as_slice()));
        assert_eq!(read_tuple(&page, slot2), Some(b"third".as_slice()));
        assert_eq!(live_tuple_count(&page), 2);

        // Deleting again returns None
        assert_eq!(delete_tuple(&mut page, slot1), None);
    }

    #[test]
    fn test_update_tuple_in_place_same_size() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);

        let original = b"hello world!"; // 12 bytes
        let updated  = b"HELLO WORLD!"; // 12 bytes

        let slot = insert_tuple(&mut page, original).unwrap();
        assert_eq!(read_tuple(&page, slot), Some(original.as_slice()));

        let ok = update_tuple_in_place(&mut page, slot, updated);
        assert!(ok);
        assert_eq!(read_tuple(&page, slot), Some(updated.as_slice()));

        // Shorter data also works
        let shorter = b"hi";
        let ok = update_tuple_in_place(&mut page, slot, shorter);
        assert!(ok);
        assert_eq!(read_tuple(&page, slot), Some(shorter.as_slice()));
    }

    #[test]
    fn test_update_tuple_larger_fails() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);

        let original = b"short";
        let larger = b"this is a much longer replacement that does not fit in place";

        let slot = insert_tuple(&mut page, original).unwrap();

        let ok = update_tuple_in_place(&mut page, slot, larger);
        assert!(!ok);
        assert_eq!(read_tuple(&page, slot), Some(original.as_slice()));

        // Update on dead slot returns false
        delete_tuple(&mut page, slot);
        assert!(!update_tuple_in_place(&mut page, slot, original));

        // Update on out-of-range slot returns false
        assert!(!update_tuple_in_place(&mut page, 9999, original));
    }

    #[test]
    fn test_fill_page_completely() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);

        let tuple_data = [0xABu8; 100];
        let mut count = 0u32;
        loop {
            match insert_tuple(&mut page, &tuple_data) {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert!(count > 0);
        // After filling with 100-byte tuples, can't fit another 100-byte tuple
        assert!(insert_tuple(&mut page, &tuple_data).is_none());
        assert_eq!(live_tuple_count(&page), count as usize);
        // Remaining free space is less than tuple size + slot overhead
        assert!(free_space(&page) < tuple_data.len() + SLOT_SIZE);
    }

    #[test]
    fn test_iter_tuples_skips_dead() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);

        let slot_a = insert_tuple(&mut page, b"aaa").unwrap();
        let slot_b = insert_tuple(&mut page, b"bbb").unwrap();
        let slot_c = insert_tuple(&mut page, b"ccc").unwrap();

        delete_tuple(&mut page, slot_b);

        let live: Vec<(u16, &[u8])> = iter_tuples(&page);
        assert_eq!(live.len(), 2);
        let live_slots: Vec<u16> = live.iter().map(|(idx, _)| *idx).collect();
        assert!(live_slots.contains(&slot_a));
        assert!(live_slots.contains(&slot_c));
        assert!(!live_slots.contains(&slot_b));
    }

    #[test]
    fn test_checksum_roundtrip() {
        let mut page = [0u8; PAGE_SIZE];
        init_data_page(&mut page, 1);

        insert_tuple(&mut page, b"checksum test").unwrap();

        write_checksum(&mut page);
        assert!(verify_checksum(&page));

        // Corrupt a byte
        let saved = page[PAGE_SIZE - 1];
        page[PAGE_SIZE - 1] ^= 0xFF;
        assert!(!verify_checksum(&page));

        // Restore
        page[PAGE_SIZE - 1] = saved;
        assert!(verify_checksum(&page));
    }

    #[test]
    fn test_meta_page_initialization() {
        let mut page = [0u8; PAGE_SIZE];
        init_meta_page(&mut page);

        assert_eq!(get_page_type(&page), PAGE_TYPE_META);
        assert_eq!(get_format_version(&page), 1);
        assert_eq!(&page[META_MAGIC..META_MAGIC + 8], MAGIC_BYTES);
        assert_eq!(read_u32(&page, META_DB_VERSION), DB_FORMAT_VERSION);
        assert_eq!(read_u32(&page, META_PAGE_SIZE), PAGE_SIZE as u32);
        assert_eq!(read_u32(&page, META_TOTAL_PAGES), 1);
        assert_eq!(read_u32(&page, META_FREE_LIST_HEAD), INVALID_PAGE_ID);
        assert_eq!(read_u32(&page, META_FREE_PAGE_COUNT), 0);
    }

    proptest! {
        #[test]
        fn prop_insert_and_read_back_tuples(
            tuples in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 4..256),
                1..50
            )
        ) {
            let mut page = [0u8; PAGE_SIZE];
            init_data_page(&mut page, 1);

            let mut inserted: Vec<(u16, Vec<u8>)> = Vec::new();
            for tuple_data in &tuples {
                match insert_tuple(&mut page, tuple_data) {
                    Some(slot_idx) => {
                        inserted.push((slot_idx, tuple_data.clone()));
                    }
                    None => break, // page full
                }
            }

            // Verify all inserted tuples can be read back correctly
            for (slot_idx, expected_data) in &inserted {
                let read_back = read_tuple(&page, *slot_idx)
                    .expect("inserted tuple must be readable");
                prop_assert_eq!(read_back, expected_data.as_slice());
            }
        }

        #[test]
        fn prop_live_tuple_count_matches_inserts(
            tuples in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 4..256),
                1..50
            )
        ) {
            let mut page = [0u8; PAGE_SIZE];
            init_data_page(&mut page, 1);

            let mut insert_count = 0u16;
            for tuple_data in &tuples {
                match insert_tuple(&mut page, tuple_data) {
                    Some(_) => insert_count += 1,
                    None => break,
                }
            }

            let count = live_tuple_count(&page);
            prop_assert_eq!(count, insert_count as usize);
        }

        #[test]
        fn prop_delete_then_read_returns_none(
            tuples in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 4..128),
                2..30
            )
        ) {
            let mut page = [0u8; PAGE_SIZE];
            init_data_page(&mut page, 1);

            let mut slots: Vec<u16> = Vec::new();
            for tuple_data in &tuples {
                match insert_tuple(&mut page, tuple_data) {
                    Some(slot_idx) => slots.push(slot_idx),
                    None => break,
                }
            }

            // Delete every other tuple
            let mut deleted = std::collections::HashSet::new();
            for (i, &slot) in slots.iter().enumerate() {
                if i % 2 == 0 {
                    delete_tuple(&mut page, slot);
                    deleted.insert(slot);
                }
            }

            // Verify deleted tuples return None, live tuples still readable
            for &slot in &slots {
                if deleted.contains(&slot) {
                    prop_assert!(read_tuple(&page, slot).is_none(),
                        "deleted slot {} should return None", slot);
                } else {
                    prop_assert!(read_tuple(&page, slot).is_some(),
                        "live slot {} should still be readable", slot);
                }
            }
        }
    }

    proptest! {
        #[test]
        fn prop_slot_entry_roundtrip(
            offset in 0u16..0x7FFF,
            length in 0u16..0x7FFF,
            dead in any::<bool>(),
            overflow in any::<bool>()
        ) {
            let entry = SlotEntry::new(offset, length, dead, overflow);
            prop_assert_eq!(entry.offset(), offset);
            prop_assert_eq!(entry.length(), length);
            prop_assert_eq!(entry.is_dead(), dead);
            prop_assert_eq!(entry.is_overflow(), overflow);

            // u32 roundtrip
            let raw = entry.to_u32();
            let restored = SlotEntry::from_u32(raw);
            prop_assert_eq!(restored.offset(), offset);
            prop_assert_eq!(restored.length(), length);
            prop_assert_eq!(restored.is_dead(), dead);
            prop_assert_eq!(restored.is_overflow(), overflow);
        }
    }
}
