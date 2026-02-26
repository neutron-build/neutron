//! B-tree index implementation.
//!
//! Uses the page-based storage engine. Index pages are marked with PAGE_TYPE_INDEX.
//! Leaf pages store (key, RowId) entries in sorted order.
//! Internal pages store (key, child_page_id) entries.
//!
//! The B-tree supports:
//!   - Point lookups: find all RowIds matching a key
//!   - Range scans: find all RowIds in a key range
//!   - Insert: add a new (key, RowId) entry
//!   - Delete: remove a (key, RowId) entry
//!
//! Keys are serialized using the same tuple format as row data, making
//! the B-tree type-agnostic (any column type can be indexed).

use std::sync::Arc;

use super::buffer::BufferPool;
use super::page::{self, PageBuf, PAGE_SIZE, INVALID_PAGE_ID};
use crate::types::{DataType, Value};

// ============================================================================
// Index page layout
// ============================================================================

// Index page sub-header (follows common 16-byte header):
//   is_leaf: u8           (offset 16)
//   entry_count: u16      (offset 17)
//   right_sibling: u32    (offset 19) — for leaf pages, next leaf in chain
//   parent: u32           (offset 23) — parent page (for splits)
//   _reserved: u8         (offset 27)
// Total sub-header: 12 bytes → entries start at offset 28

const IDX_IS_LEAF: usize = page::COMMON_HEADER_SIZE; // u8
const IDX_ENTRY_COUNT: usize = page::COMMON_HEADER_SIZE + 1; // u16
const IDX_RIGHT_SIBLING: usize = page::COMMON_HEADER_SIZE + 3; // u32
const IDX_PARENT: usize = page::COMMON_HEADER_SIZE + 7; // u32
const IDX_HEADER_SIZE: usize = page::COMMON_HEADER_SIZE + 12; // = 28

/// Maximum key size. Keys larger than this are not indexable.
pub const MAX_KEY_SIZE: usize = 256;

/// A row identifier: (page_id, slot_index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowId {
    pub page_id: u32,
    pub slot_idx: u16,
}

impl RowId {
    pub fn encode(&self) -> [u8; 6] {
        let mut buf = [0u8; 6];
        buf[0..4].copy_from_slice(&self.page_id.to_le_bytes());
        buf[4..6].copy_from_slice(&self.slot_idx.to_le_bytes());
        buf
    }

    pub fn decode(data: &[u8]) -> Self {
        Self {
            page_id: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            slot_idx: u16::from_le_bytes([data[4], data[5]]),
        }
    }
}

// ============================================================================
// Leaf entry format:
//   [key_len: u16] [key_data: key_len bytes] [row_id: 6 bytes]
//
// Internal entry format:
//   [key_len: u16] [key_data: key_len bytes] [child_page_id: u32]
//
// Internal pages also have a "leftmost child" stored right after the header.
// ============================================================================

const LEFTMOST_CHILD_OFFSET: usize = IDX_HEADER_SIZE; // u32, for internal nodes
const INTERNAL_ENTRIES_START: usize = IDX_HEADER_SIZE + 4; // after leftmost child

/// An in-memory B-tree index handle.
pub struct BTreeIndex {
    /// Root page ID.
    root_page: u32,
    /// Column data type (for typed operations like key decoding).
    #[allow(dead_code)]
    key_type: DataType,
    /// Buffer pool reference.
    pool: Arc<BufferPool>,
}

impl BTreeIndex {
    /// Create a new empty B-tree index. Allocates a root leaf page.
    pub fn create(pool: Arc<BufferPool>, key_type: DataType) -> Result<Self, BTreeError> {
        let (page_id, frame_id) = pool.new_page().map_err(|e| BTreeError::Io(e.to_string()))?;
        let pg = pool.frame_data_mut(frame_id);
        init_index_page(pg, true); // leaf
        pool.mark_dirty(frame_id);
        pool.unpin(frame_id);

        Ok(Self {
            root_page: page_id,
            key_type,
            pool,
        })
    }

    /// Open an existing B-tree index from a root page.
    pub fn open(pool: Arc<BufferPool>, root_page: u32, key_type: DataType) -> Self {
        Self {
            root_page,
            key_type,
            pool,
        }
    }

    /// Get the root page ID.
    pub fn root_page(&self) -> u32 {
        self.root_page
    }

    // ========================================================================
    // Point lookup
    // ========================================================================

    /// Find all RowIds matching the given key.
    pub fn lookup(&self, key: &[u8]) -> Result<Vec<RowId>, BTreeError> {
        let leaf = self.find_leaf(key)?;
        let frame_id = self.pool.fetch_page(leaf).map_err(|e| BTreeError::Io(e.to_string()))?;
        let pg = self.pool.frame_data(frame_id);
        let results = scan_leaf_for_key(pg, key);
        self.pool.unpin(frame_id);
        Ok(results)
    }

    // ========================================================================
    // Range scan
    // ========================================================================

    /// Scan all entries where key is in [start_key, end_key] (inclusive).
    /// If start_key is None, scan from the beginning.
    /// If end_key is None, scan to the end.
    pub fn range_scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, RowId)>, BTreeError> {
        let leaf = match start_key {
            Some(k) => self.find_leaf(k)?,
            None => self.find_leftmost_leaf()?,
        };

        let mut results = Vec::new();
        let mut page_id = leaf;

        while page_id != INVALID_PAGE_ID {
            let frame_id = self.pool.fetch_page(page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let count = page::read_u16(pg, IDX_ENTRY_COUNT);
            let mut pos = IDX_HEADER_SIZE;

            for _ in 0..count {
                let (key, row_id, next_pos) = read_leaf_entry(pg, pos);
                pos = next_pos;

                // Check range
                if let Some(sk) = start_key {
                    if key.as_slice() < sk {
                        continue;
                    }
                }
                if let Some(ek) = end_key {
                    if key.as_slice() > ek {
                        self.pool.unpin(frame_id);
                        return Ok(results);
                    }
                }
                results.push((key, row_id));
            }

            let next = page::read_u32(pg, IDX_RIGHT_SIBLING);
            self.pool.unpin(frame_id);
            page_id = next;
        }

        Ok(results)
    }

    // ========================================================================
    // Insert
    // ========================================================================

    /// Insert a (key, RowId) entry into the B-tree.
    pub fn insert(&mut self, key: &[u8], row_id: RowId) -> Result<(), BTreeError> {
        let leaf = self.find_leaf(key)?;
        let frame_id = self.pool.fetch_page(leaf).map_err(|e| BTreeError::Io(e.to_string()))?;

        // Try to insert into the leaf
        let pg = self.pool.frame_data_mut(frame_id);
        if try_insert_leaf(pg, key, &row_id) {
            self.pool.mark_dirty(frame_id);
            self.pool.unpin(frame_id);
            return Ok(());
        }

        // Leaf is full — need to split
        self.pool.unpin(frame_id);
        self.split_and_insert(leaf, key, row_id)?;
        Ok(())
    }

    // ========================================================================
    // Delete
    // ========================================================================

    /// Delete a (key, RowId) entry from the B-tree.
    pub fn delete(&self, key: &[u8], row_id: RowId) -> Result<bool, BTreeError> {
        let leaf = self.find_leaf(key)?;
        let frame_id = self.pool.fetch_page(leaf).map_err(|e| BTreeError::Io(e.to_string()))?;
        let pg = self.pool.frame_data_mut(frame_id);
        let deleted = delete_leaf_entry(pg, key, &row_id);
        if deleted {
            self.pool.mark_dirty(frame_id);
        }
        self.pool.unpin(frame_id);
        Ok(deleted)
    }

    // ========================================================================
    // Tree navigation
    // ========================================================================

    /// Find the leaf page where a key should live.
    fn find_leaf(&self, key: &[u8]) -> Result<u32, BTreeError> {
        let mut page_id = self.root_page;
        loop {
            let frame_id = self.pool.fetch_page(page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let is_leaf = pg[IDX_IS_LEAF] != 0;
            if is_leaf {
                self.pool.unpin(frame_id);
                return Ok(page_id);
            }
            // Internal node: find the child to descend into
            let child = find_child(pg, key);
            self.pool.unpin(frame_id);
            page_id = child;
        }
    }

    /// Find the leftmost leaf page (for full scans).
    fn find_leftmost_leaf(&self) -> Result<u32, BTreeError> {
        let mut page_id = self.root_page;
        loop {
            let frame_id = self.pool.fetch_page(page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
            let pg = self.pool.frame_data(frame_id);
            let is_leaf = pg[IDX_IS_LEAF] != 0;
            if is_leaf {
                self.pool.unpin(frame_id);
                return Ok(page_id);
            }
            // Go to leftmost child
            let child = page::read_u32(pg, LEFTMOST_CHILD_OFFSET);
            self.pool.unpin(frame_id);
            page_id = child;
        }
    }

    // ========================================================================
    // Split
    // ========================================================================

    /// Split a full leaf and insert the new entry.
    fn split_and_insert(
        &mut self,
        leaf_page_id: u32,
        key: &[u8],
        row_id: RowId,
    ) -> Result<(), BTreeError> {
        // Read all entries from the leaf
        let frame_id = self.pool.fetch_page(leaf_page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
        let pg = self.pool.frame_data(frame_id);
        let mut entries = collect_leaf_entries(pg);
        let old_sibling = page::read_u32(pg, IDX_RIGHT_SIBLING);
        let parent = page::read_u32(pg, IDX_PARENT);
        self.pool.unpin(frame_id);

        // Add the new entry and sort
        let entry_bytes = encode_leaf_entry(key, &row_id);
        entries.push(entry_bytes);
        entries.sort_by(|a, b| {
            let ka = extract_key(a);
            let kb = extract_key(b);
            ka.cmp(&kb)
        });

        // Split at midpoint
        let mid = entries.len() / 2;
        let left_entries = &entries[..mid];
        let right_entries = &entries[mid..];

        // Get the split key (first key in right page)
        let split_key = extract_key(&right_entries[0]).to_vec();

        // Allocate new right page
        let (right_page_id, right_frame) = self.pool.new_page().map_err(|e| BTreeError::Io(e.to_string()))?;
        let rpg = self.pool.frame_data_mut(right_frame);
        init_index_page(rpg, true);
        page::write_u32(rpg, IDX_RIGHT_SIBLING, old_sibling);
        page::write_u32(rpg, IDX_PARENT, parent);
        write_leaf_entries(rpg, right_entries);
        self.pool.mark_dirty(right_frame);
        self.pool.unpin(right_frame);

        // Rewrite left page
        let left_frame = self.pool.fetch_page(leaf_page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
        let lpg = self.pool.frame_data_mut(left_frame);
        init_index_page(lpg, true);
        page::write_u32(lpg, IDX_RIGHT_SIBLING, right_page_id);
        page::write_u32(lpg, IDX_PARENT, parent);
        write_leaf_entries(lpg, left_entries);
        self.pool.mark_dirty(left_frame);
        self.pool.unpin(left_frame);

        // Insert split key into parent
        if parent == INVALID_PAGE_ID {
            // Root was a leaf — create new root
            let (new_root_id, root_frame) = self.pool.new_page().map_err(|e| BTreeError::Io(e.to_string()))?;
            let rpg = self.pool.frame_data_mut(root_frame);
            init_index_page(rpg, false); // internal node
            page::write_u32(rpg, LEFTMOST_CHILD_OFFSET, leaf_page_id);
            insert_internal_entry(rpg, &split_key, right_page_id);
            self.pool.mark_dirty(root_frame);
            self.pool.unpin(root_frame);

            // Update parent pointers
            self.set_parent(leaf_page_id, new_root_id)?;
            self.set_parent(right_page_id, new_root_id)?;

            self.root_page = new_root_id;
        } else {
            // Insert into parent (may cascade splits, but for now we assume parent has space)
            self.insert_into_parent(parent, &split_key, right_page_id)?;
        }

        Ok(())
    }

    fn insert_into_parent(
        &mut self,
        parent_page_id: u32,
        key: &[u8],
        right_child: u32,
    ) -> Result<(), BTreeError> {
        // Check if the parent has space for this entry
        let frame_id = self.pool.fetch_page(parent_page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
        let pg = self.pool.frame_data(frame_id);
        let has_space = internal_has_space(pg, key.len());
        self.pool.unpin(frame_id);

        if has_space {
            // Simple case: parent has room, just insert
            let frame_id = self.pool.fetch_page(parent_page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
            let pg = self.pool.frame_data_mut(frame_id);
            insert_internal_entry(pg, key, right_child);
            self.pool.mark_dirty(frame_id);
            self.pool.unpin(frame_id);

            self.set_parent(right_child, parent_page_id)?;
            return Ok(());
        }

        // Parent is full — split the internal node
        self.split_internal_and_insert(parent_page_id, key, right_child)
    }

    /// Split a full internal node and insert the new (key, right_child) entry.
    /// The middle key is promoted to the grandparent (or a new root is created).
    fn split_internal_and_insert(
        &mut self,
        internal_page_id: u32,
        key: &[u8],
        right_child: u32,
    ) -> Result<(), BTreeError> {
        // Read all existing entries and the leftmost child from this internal node
        let frame_id = self.pool.fetch_page(internal_page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
        let pg = self.pool.frame_data(frame_id);
        let mut entries = collect_internal_entries(pg);
        let _leftmost_child = page::read_u32(pg, LEFTMOST_CHILD_OFFSET);
        let grandparent = page::read_u32(pg, IDX_PARENT);
        self.pool.unpin(frame_id);

        // Add the new entry and sort by key
        entries.push((key.to_vec(), right_child));
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Split at midpoint: left entries stay, middle key promoted, right entries go to new page
        let mid = entries.len() / 2;
        let left_entries = entries[..mid].to_vec();
        let (promoted_key, promoted_child) = entries[mid].clone();
        let right_entries = entries[mid + 1..].to_vec();

        // The new right page's leftmost child is the child pointer from the promoted entry.
        // This is because the promoted key separates left from right:
        //   - All children in left_entries have keys < promoted_key
        //   - promoted_child is the child that was to the right of promoted_key
        //   - All children in right_entries have keys > promoted_key
        // So promoted_child becomes the leftmost child of the new right page.
        let new_right_leftmost = promoted_child;

        // Allocate the new right internal page
        let (right_page_id, right_frame) = self.pool.new_page().map_err(|e| BTreeError::Io(e.to_string()))?;
        let rpg = self.pool.frame_data_mut(right_frame);
        init_index_page(rpg, false); // internal node
        page::write_u32(rpg, LEFTMOST_CHILD_OFFSET, new_right_leftmost);
        page::write_u32(rpg, IDX_PARENT, grandparent);
        write_internal_entries(rpg, &right_entries);
        self.pool.mark_dirty(right_frame);
        self.pool.unpin(right_frame);

        // Rewrite the left (original) page with only the left entries
        let left_frame = self.pool.fetch_page(internal_page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
        let lpg = self.pool.frame_data_mut(left_frame);
        // Preserve leftmost child (unchanged) and parent
        // Clear entries area and rewrite
        write_internal_entries(lpg, &left_entries);
        // leftmost_child stays the same (was already set)
        self.pool.mark_dirty(left_frame);
        self.pool.unpin(left_frame);

        // Update parent pointers for children that moved to the new right page
        self.set_parent(new_right_leftmost, right_page_id)?;
        for (_, child_id) in &right_entries {
            self.set_parent(*child_id, right_page_id)?;
        }

        // Promote the middle key to the grandparent
        if grandparent == INVALID_PAGE_ID {
            // We're splitting the root — create a new root
            let (new_root_id, root_frame) = self.pool.new_page().map_err(|e| BTreeError::Io(e.to_string()))?;
            let rpg = self.pool.frame_data_mut(root_frame);
            init_index_page(rpg, false); // internal node
            page::write_u32(rpg, LEFTMOST_CHILD_OFFSET, internal_page_id);
            insert_internal_entry(rpg, &promoted_key, right_page_id);
            self.pool.mark_dirty(root_frame);
            self.pool.unpin(root_frame);

            self.set_parent(internal_page_id, new_root_id)?;
            self.set_parent(right_page_id, new_root_id)?;

            self.root_page = new_root_id;
        } else {
            // Recursively insert into the grandparent (may cascade further)
            self.insert_into_parent(grandparent, &promoted_key, right_page_id)?;
        }

        Ok(())
    }

    fn set_parent(&self, page_id: u32, parent: u32) -> Result<(), BTreeError> {
        let frame_id = self.pool.fetch_page(page_id).map_err(|e| BTreeError::Io(e.to_string()))?;
        let pg = self.pool.frame_data_mut(frame_id);
        page::write_u32(pg, IDX_PARENT, parent);
        self.pool.mark_dirty(frame_id);
        self.pool.unpin(frame_id);
        Ok(())
    }
}

// ============================================================================
// Page-level helpers
// ============================================================================

fn init_index_page(pg: &mut PageBuf, is_leaf: bool) {
    pg.fill(0);
    page::write_u16(pg, page::HEADER_PAGE_TYPE, page::PAGE_TYPE_INDEX);
    page::write_u16(pg, page::HEADER_FORMAT_VERSION, 1);
    pg[IDX_IS_LEAF] = if is_leaf { 1 } else { 0 };
    page::write_u16(pg, IDX_ENTRY_COUNT, 0);
    page::write_u32(pg, IDX_RIGHT_SIBLING, INVALID_PAGE_ID);
    page::write_u32(pg, IDX_PARENT, INVALID_PAGE_ID);
}

/// Available space for entries on an index page.
fn index_free_space(pg: &PageBuf) -> usize {
    let count = page::read_u16(pg, IDX_ENTRY_COUNT) as usize;
    let is_leaf = pg[IDX_IS_LEAF] != 0;
    let start = if is_leaf { IDX_HEADER_SIZE } else { INTERNAL_ENTRIES_START };
    // Calculate used space by scanning entries
    let mut pos = start;
    for _ in 0..count {
        let key_len = page::read_u16(pg, pos) as usize;
        pos += 2 + key_len;
        if is_leaf {
            pos += 6; // RowId
        } else {
            pos += 4; // child page_id
        }
    }
    PAGE_SIZE - pos
}

// ---- Leaf operations ----

fn read_leaf_entry(pg: &PageBuf, pos: usize) -> (Vec<u8>, RowId, usize) {
    let key_len = page::read_u16(pg, pos) as usize;
    let key = pg[pos + 2..pos + 2 + key_len].to_vec();
    let rid_start = pos + 2 + key_len;
    let row_id = RowId::decode(&pg[rid_start..rid_start + 6]);
    (key, row_id, rid_start + 6)
}

fn scan_leaf_for_key(pg: &PageBuf, key: &[u8]) -> Vec<RowId> {
    let count = page::read_u16(pg, IDX_ENTRY_COUNT);
    let mut pos = IDX_HEADER_SIZE;
    let mut results = Vec::new();
    for _ in 0..count {
        let (k, rid, next) = read_leaf_entry(pg, pos);
        if k == key {
            results.push(rid);
        } else if k.as_slice() > key {
            break; // entries are sorted
        }
        pos = next;
    }
    results
}

fn try_insert_leaf(pg: &mut PageBuf, key: &[u8], row_id: &RowId) -> bool {
    let entry_size = 2 + key.len() + 6;
    if index_free_space(pg) < entry_size {
        return false;
    }

    // Collect entries, insert in sorted position
    let mut entries: Vec<Vec<u8>> = collect_leaf_entries(pg);
    let new_entry = encode_leaf_entry(key, row_id);

    // Find insertion position
    let pos = entries
        .iter()
        .position(|e| extract_key(e) >= key)
        .unwrap_or(entries.len());
    entries.insert(pos, new_entry);

    // Rewrite all entries
    write_leaf_entries(pg, &entries);
    true
}

fn delete_leaf_entry(pg: &mut PageBuf, key: &[u8], row_id: &RowId) -> bool {
    let entries = collect_leaf_entries(pg);
    let rid_bytes = row_id.encode();
    let mut new_entries: Vec<Vec<u8>> = Vec::new();
    let mut found = false;

    for entry in &entries {
        let ek = extract_key(entry);
        let erid = &entry[2 + ek.len()..2 + ek.len() + 6];
        if ek == key && erid == rid_bytes && !found {
            found = true;
            continue; // skip this entry
        }
        new_entries.push(entry.clone());
    }

    if found {
        write_leaf_entries(pg, &new_entries);
    }
    found
}

fn collect_leaf_entries(pg: &PageBuf) -> Vec<Vec<u8>> {
    let count = page::read_u16(pg, IDX_ENTRY_COUNT);
    let mut pos = IDX_HEADER_SIZE;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key_len = page::read_u16(pg, pos) as usize;
        let entry_len = 2 + key_len + 6;
        entries.push(pg[pos..pos + entry_len].to_vec());
        pos += entry_len;
    }
    entries
}

fn write_leaf_entries(pg: &mut PageBuf, entries: &[Vec<u8>]) {
    let mut pos = IDX_HEADER_SIZE;
    for entry in entries {
        pg[pos..pos + entry.len()].copy_from_slice(entry);
        pos += entry.len();
    }
    page::write_u16(pg, IDX_ENTRY_COUNT, entries.len() as u16);
    // Zero remaining space
    if pos < PAGE_SIZE {
        pg[pos..PAGE_SIZE].fill(0);
    }
}

fn encode_leaf_entry(key: &[u8], row_id: &RowId) -> Vec<u8> {
    let mut entry = Vec::with_capacity(2 + key.len() + 6);
    entry.extend_from_slice(&(key.len() as u16).to_le_bytes());
    entry.extend_from_slice(key);
    entry.extend_from_slice(&row_id.encode());
    entry
}

fn extract_key(entry: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes([entry[0], entry[1]]) as usize;
    &entry[2..2 + key_len]
}

// ---- Internal node operations ----

/// Check whether an internal page has enough space for a new entry with the given key length.
fn internal_has_space(pg: &PageBuf, key_len: usize) -> bool {
    let entry_size = 2 + key_len + 4; // key_len(u16) + key + child_page_id(u32)
    index_free_space(pg) >= entry_size
}

/// Collect all (key, child_page_id) entries from an internal page.
fn collect_internal_entries(pg: &PageBuf) -> Vec<(Vec<u8>, u32)> {
    let count = page::read_u16(pg, IDX_ENTRY_COUNT) as usize;
    let mut entries = Vec::with_capacity(count);
    let mut pos = INTERNAL_ENTRIES_START;
    for _ in 0..count {
        let key_len = page::read_u16(pg, pos) as usize;
        let key = pg[pos + 2..pos + 2 + key_len].to_vec();
        let child = page::read_u32(pg, pos + 2 + key_len);
        entries.push((key, child));
        pos = pos + 2 + key_len + 4;
    }
    entries
}

/// Rewrite internal page entries (does NOT touch leftmost_child or header fields other than entry count).
fn write_internal_entries(pg: &mut PageBuf, entries: &[(Vec<u8>, u32)]) {
    let mut wpos = INTERNAL_ENTRIES_START;
    for (k, c) in entries {
        page::write_u16(pg, wpos, k.len() as u16);
        wpos += 2;
        pg[wpos..wpos + k.len()].copy_from_slice(k);
        wpos += k.len();
        page::write_u32(pg, wpos, *c);
        wpos += 4;
    }
    page::write_u16(pg, IDX_ENTRY_COUNT, entries.len() as u16);
    // Zero remaining space
    if wpos < PAGE_SIZE {
        pg[wpos..PAGE_SIZE].fill(0);
    }
}

fn find_child(pg: &PageBuf, key: &[u8]) -> u32 {
    let count = page::read_u16(pg, IDX_ENTRY_COUNT);
    let mut pos = INTERNAL_ENTRIES_START;

    // Internal node format: leftmost_child, then (separator_key, right_child) pairs.
    // If key < separator[0], go to leftmost_child.
    // If separator[i-1] <= key < separator[i], go to child[i-1] (= right_child of entry i-1).
    // If key >= all separators, go to the last right_child.

    let mut prev_child = page::read_u32(pg, LEFTMOST_CHILD_OFFSET);

    for _ in 0..count {
        let key_len = page::read_u16(pg, pos) as usize;
        let entry_key = &pg[pos + 2..pos + 2 + key_len];
        let child_offset = pos + 2 + key_len;
        let right_child = page::read_u32(pg, child_offset);

        if key < entry_key {
            return prev_child;
        }

        prev_child = right_child;
        pos = child_offset + 4;
    }

    prev_child
}

fn insert_internal_entry(pg: &mut PageBuf, key: &[u8], child_page_id: u32) {
    let count = page::read_u16(pg, IDX_ENTRY_COUNT) as usize;

    // Collect existing entries
    let mut entries: Vec<(Vec<u8>, u32)> = Vec::new();
    let mut pos = INTERNAL_ENTRIES_START;
    for _ in 0..count {
        let key_len = page::read_u16(pg, pos) as usize;
        let ek = pg[pos + 2..pos + 2 + key_len].to_vec();
        let child = page::read_u32(pg, pos + 2 + key_len);
        entries.push((ek, child));
        pos = pos + 2 + key_len + 4;
    }

    // Insert in sorted order
    let ins_pos = entries
        .iter()
        .position(|(k, _)| k.as_slice() >= key)
        .unwrap_or(entries.len());
    entries.insert(ins_pos, (key.to_vec(), child_page_id));

    // Rewrite entries
    let mut wpos = INTERNAL_ENTRIES_START;
    for (k, c) in &entries {
        page::write_u16(pg, wpos, k.len() as u16);
        wpos += 2;
        pg[wpos..wpos + k.len()].copy_from_slice(k);
        wpos += k.len();
        page::write_u32(pg, wpos, *c);
        wpos += 4;
    }
    page::write_u16(pg, IDX_ENTRY_COUNT, entries.len() as u16);
}

// ============================================================================
// Key serialization (for indexing Values)
// ============================================================================

/// Serialize a Value into a comparable byte key.
/// The encoding preserves sort order for memcmp-based comparison.
pub fn value_to_key(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00], // NULL sorts first
        Value::Bool(false) => vec![0x01, 0x00],
        Value::Bool(true) => vec![0x01, 0x01],
        Value::Int32(n) => {
            let mut buf = vec![0x02];
            // Flip sign bit for memcmp ordering
            let encoded = (*n as u32) ^ 0x80000000;
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        Value::Int64(n) => {
            let mut buf = vec![0x03];
            let encoded = (*n as u64) ^ 0x8000000000000000;
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        Value::Float64(n) => {
            let mut buf = vec![0x04];
            let bits = n.to_bits();
            // IEEE 754 ordering trick: flip all bits if negative, flip sign bit if positive
            let encoded = if bits & 0x8000000000000000 != 0 {
                !bits
            } else {
                bits ^ 0x8000000000000000
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        Value::Text(s) => {
            let mut buf = vec![0x05];
            buf.extend_from_slice(s.as_bytes());
            buf
        }
        Value::Jsonb(v) => {
            let mut buf = vec![0x06];
            let s = serde_json::to_string(v).unwrap_or_default();
            buf.extend_from_slice(s.as_bytes());
            buf
        }
        Value::Date(d) => {
            let mut buf = vec![0x07];
            let encoded = (*d as u32) ^ 0x80000000;
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        Value::Timestamp(us) => {
            let mut buf = vec![0x08];
            let encoded = (*us as u64) ^ 0x8000000000000000;
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        Value::TimestampTz(us) => {
            let mut buf = vec![0x09];
            let encoded = (*us as u64) ^ 0x8000000000000000;
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        Value::Numeric(s) => {
            let mut buf = vec![0x0A];
            buf.extend_from_slice(s.as_bytes());
            buf
        }
        Value::Uuid(b) => {
            let mut buf = vec![0x0B];
            buf.extend_from_slice(b);
            buf
        }
        Value::Bytea(b) => {
            let mut buf = vec![0x0C];
            buf.extend_from_slice(b);
            buf
        }
        Value::Array(_) => {
            let mut buf = vec![0x0D];
            let s = format!("{value}");
            buf.extend_from_slice(s.as_bytes());
            buf
        }
        Value::Vector(vec) => {
            let mut buf = vec![0x0E];
            // Encode vector as packed floats (not sortable, but unique)
            for f in vec {
                buf.extend_from_slice(&f.to_le_bytes());
            }
            buf
        }
        Value::Interval { months, days, microseconds } => {
            let mut buf = vec![0x0F];
            // Encode as months(i32) + days(i32) + microseconds(i64) with sign-flip for ordering
            let em = (*months as u32) ^ 0x80000000;
            let ed = (*days as u32) ^ 0x80000000;
            let eus = (*microseconds as u64) ^ 0x8000000000000000;
            buf.extend_from_slice(&em.to_be_bytes());
            buf.extend_from_slice(&ed.to_be_bytes());
            buf.extend_from_slice(&eus.to_be_bytes());
            buf
        }
    }
}

/// Deserialize a key back to a Value (approximate — for display purposes).
pub fn key_to_value(key: &[u8], dtype: &DataType) -> Value {
    if key.is_empty() || key[0] == 0x00 {
        return Value::Null;
    }
    match dtype {
        DataType::Bool => {
            if key.len() >= 2 {
                Value::Bool(key[1] != 0)
            } else {
                Value::Null
            }
        }
        DataType::Int32 => {
            if key.len() >= 5 {
                let encoded = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
                Value::Int32((encoded ^ 0x80000000) as i32)
            } else {
                Value::Null
            }
        }
        DataType::Int64 => {
            if key.len() >= 9 {
                let encoded = u64::from_be_bytes([
                    key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8],
                ]);
                Value::Int64((encoded ^ 0x8000000000000000) as i64)
            } else {
                Value::Null
            }
        }
        DataType::Float64 => {
            if key.len() >= 9 {
                let encoded = u64::from_be_bytes([
                    key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8],
                ]);
                let bits = if encoded & 0x8000000000000000 != 0 {
                    encoded ^ 0x8000000000000000
                } else {
                    !encoded
                };
                Value::Float64(f64::from_bits(bits))
            } else {
                Value::Null
            }
        }
        DataType::Text => {
            if key.len() >= 2 {
                Value::Text(String::from_utf8_lossy(&key[1..]).to_string())
            } else {
                Value::Null
            }
        }
        DataType::Jsonb => {
            if key.len() >= 2 {
                match serde_json::from_slice(&key[1..]) {
                    Ok(v) => Value::Jsonb(v),
                    Err(_) => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        DataType::Date => {
            if key.len() >= 5 {
                let encoded = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
                Value::Date((encoded ^ 0x80000000) as i32)
            } else {
                Value::Null
            }
        }
        DataType::Timestamp => {
            if key.len() >= 9 {
                let encoded = u64::from_be_bytes([
                    key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8],
                ]);
                Value::Timestamp((encoded ^ 0x8000000000000000) as i64)
            } else {
                Value::Null
            }
        }
        DataType::TimestampTz => {
            if key.len() >= 9 {
                let encoded = u64::from_be_bytes([
                    key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8],
                ]);
                Value::TimestampTz((encoded ^ 0x8000000000000000) as i64)
            } else {
                Value::Null
            }
        }
        DataType::Numeric => {
            if key.len() >= 2 {
                Value::Numeric(String::from_utf8_lossy(&key[1..]).to_string())
            } else {
                Value::Null
            }
        }
        DataType::Uuid => {
            if key.len() >= 17 {
                let mut b = [0u8; 16];
                b.copy_from_slice(&key[1..17]);
                Value::Uuid(b)
            } else {
                Value::Null
            }
        }
        DataType::Bytea => {
            if key.len() >= 2 {
                Value::Bytea(key[1..].to_vec())
            } else {
                Value::Null
            }
        }
        DataType::Array(_) => {
            if key.len() >= 2 {
                Value::Text(String::from_utf8_lossy(&key[1..]).to_string())
            } else {
                Value::Null
            }
        }
        DataType::Vector(dim) => {
            if key.len() >= 1 + dim * 4 {
                let mut vec = Vec::with_capacity(*dim);
                for i in 0..*dim {
                    let pos = 1 + i * 4;
                    let f = f32::from_le_bytes([key[pos], key[pos + 1], key[pos + 2], key[pos + 3]]);
                    vec.push(f);
                }
                Value::Vector(vec)
            } else {
                Value::Null
            }
        }
        DataType::Interval => {
            if key.len() >= 17 {
                let em = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
                let ed = u32::from_be_bytes([key[5], key[6], key[7], key[8]]);
                let eus = u64::from_be_bytes([key[9], key[10], key[11], key[12], key[13], key[14], key[15], key[16]]);
                Value::Interval {
                    months: (em ^ 0x80000000) as i32,
                    days: (ed ^ 0x80000000) as i32,
                    microseconds: (eus ^ 0x8000000000000000) as i64,
                }
            } else {
                Value::Null
            }
        }
        DataType::UserDefined(_) => {
            // Enum values are stored as text keys.
            if key.len() >= 2 {
                match std::str::from_utf8(&key[1..]) {
                    Ok(s) => Value::Text(s.to_string()),
                    Err(_) => Value::Null,
                }
            } else {
                Value::Null
            }
        }
    }
}

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("key too large: {0} bytes (max {MAX_KEY_SIZE})")]
    KeyTooLarge(usize),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_key_ordering_ints() {
        let keys: Vec<Vec<u8>> = vec![
            value_to_key(&Value::Int32(-100)),
            value_to_key(&Value::Int32(-1)),
            value_to_key(&Value::Int32(0)),
            value_to_key(&Value::Int32(1)),
            value_to_key(&Value::Int32(100)),
        ];
        for i in 0..keys.len() - 1 {
            assert!(keys[i] < keys[i + 1], "key ordering failed at index {i}");
        }
    }

    #[test]
    fn value_key_ordering_floats() {
        let keys: Vec<Vec<u8>> = vec![
            value_to_key(&Value::Float64(-100.0)),
            value_to_key(&Value::Float64(-0.001)),
            value_to_key(&Value::Float64(0.0)),
            value_to_key(&Value::Float64(0.001)),
            value_to_key(&Value::Float64(100.0)),
        ];
        for i in 0..keys.len() - 1 {
            assert!(keys[i] < keys[i + 1], "key ordering failed at index {i}");
        }
    }

    #[test]
    fn value_key_ordering_text() {
        let keys: Vec<Vec<u8>> = vec![
            value_to_key(&Value::Text("aaa".into())),
            value_to_key(&Value::Text("bbb".into())),
            value_to_key(&Value::Text("zzz".into())),
        ];
        for i in 0..keys.len() - 1 {
            assert!(keys[i] < keys[i + 1], "key ordering failed at index {i}");
        }
    }

    #[test]
    fn value_key_null_sorts_first() {
        let null_key = value_to_key(&Value::Null);
        let int_key = value_to_key(&Value::Int32(0));
        assert!(null_key < int_key);
    }

    #[test]
    fn value_key_roundtrip() {
        let val = Value::Int32(42);
        let key = value_to_key(&val);
        let decoded = key_to_value(&key, &DataType::Int32);
        assert_eq!(val, decoded);

        let val = Value::Float64(3.14);
        let key = value_to_key(&val);
        let decoded = key_to_value(&key, &DataType::Float64);
        assert_eq!(val, decoded);
    }

    // ========================================================================
    // B-tree structural tests (require DiskManager + BufferPool)
    // ========================================================================

    use crate::storage::disk::DiskManager;
    use crate::storage::buffer::BufferPool;

    /// Helper: create a BTreeIndex backed by a temporary database file.
    /// Returns the index and a temp dir handle (must be kept alive).
    fn make_test_btree() -> (BTreeIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("btree_test.db");
        let disk = DiskManager::open(&db_path).unwrap();
        let pool = Arc::new(BufferPool::new(disk, None, 1024, 0));
        let idx = BTreeIndex::create(pool, DataType::Int64).unwrap();
        (idx, dir)
    }

    /// Helper: encode an i64 as a sortable 8-byte key (same encoding as value_to_key for Int64).
    fn int_key(n: i64) -> Vec<u8> {
        value_to_key(&Value::Int64(n))
    }

    /// Helper: make a dummy RowId from a sequence number.
    fn dummy_rid(n: u32) -> RowId {
        RowId { page_id: n, slot_idx: 0 }
    }

    #[test]
    fn btree_insert_causes_leaf_split() {
        let (mut idx, _dir) = make_test_btree();
        let count = 200u32;
        for i in 0..count {
            let key = int_key(i as i64);
            idx.insert(&key, dummy_rid(i)).unwrap();
        }
        // Verify all entries are findable
        for i in 0..count {
            let key = int_key(i as i64);
            let results = idx.lookup(&key).unwrap();
            assert!(
                !results.is_empty(),
                "lookup failed for key {i} after leaf split"
            );
            assert_eq!(results[0], dummy_rid(i));
        }
    }

    #[test]
    fn btree_insert_causes_cascading_split() {
        let (mut idx, _dir) = make_test_btree();
        let count = 500u32;
        for i in 0..count {
            let key = int_key(i as i64);
            idx.insert(&key, dummy_rid(i)).unwrap();
        }
        // Verify all entries are findable
        for i in 0..count {
            let key = int_key(i as i64);
            let results = idx.lookup(&key).unwrap();
            assert!(
                !results.is_empty(),
                "lookup failed for key {i} after cascading splits"
            );
            assert_eq!(results[0], dummy_rid(i));
        }
    }

    #[test]
    fn btree_range_scan_after_splits() {
        let (mut idx, _dir) = make_test_btree();
        let count = 300u32;
        for i in 0..count {
            let key = int_key(i as i64);
            idx.insert(&key, dummy_rid(i)).unwrap();
        }
        // Full range scan — should return all entries in sorted order
        let results = idx.range_scan(None, None).unwrap();
        assert_eq!(results.len(), count as usize);
        for i in 0..results.len() - 1 {
            assert!(
                results[i].0 <= results[i + 1].0,
                "range scan not sorted at index {i}: {:?} > {:?}",
                results[i].0,
                results[i + 1].0,
            );
        }
        // Partial range scan
        let start = int_key(100);
        let end = int_key(199);
        let partial = idx.range_scan(Some(&start), Some(&end)).unwrap();
        assert_eq!(partial.len(), 100, "partial range scan returned wrong count");
    }

    #[test]
    fn btree_delete_after_splits() {
        let (mut idx, _dir) = make_test_btree();
        let count = 300u32;
        for i in 0..count {
            let key = int_key(i as i64);
            idx.insert(&key, dummy_rid(i)).unwrap();
        }
        // Delete every other entry
        for i in (0..count).step_by(2) {
            let key = int_key(i as i64);
            let deleted = idx.delete(&key, dummy_rid(i)).unwrap();
            assert!(deleted, "delete returned false for key {i}");
        }
        // Verify deleted entries are gone, remaining are present
        for i in 0..count {
            let key = int_key(i as i64);
            let results = idx.lookup(&key).unwrap();
            if i % 2 == 0 {
                assert!(results.is_empty(), "key {i} should have been deleted");
            } else {
                assert!(!results.is_empty(), "key {i} should still exist");
                assert_eq!(results[0], dummy_rid(i));
            }
        }
    }

    #[test]
    fn btree_insert_sequential_keys() {
        let (mut idx, _dir) = make_test_btree();
        let count = 1000u32;
        for i in 0..count {
            let key = int_key(i as i64);
            idx.insert(&key, dummy_rid(i)).unwrap();
        }
        // Verify all 1000 entries
        for i in 0..count {
            let key = int_key(i as i64);
            let results = idx.lookup(&key).unwrap();
            assert!(
                !results.is_empty(),
                "lookup failed for sequential key {i}"
            );
            assert_eq!(results[0], dummy_rid(i));
        }
        // Verify range scan returns all in order
        let all = idx.range_scan(None, None).unwrap();
        assert_eq!(all.len(), count as usize);
        for i in 0..all.len() - 1 {
            assert!(all[i].0 <= all[i + 1].0, "not sorted at index {i}");
        }
    }
}
