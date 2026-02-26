//! Free Space Map (FSM) — tracks free space per data page.
//!
//! The FSM avoids sequential scans when looking for pages with enough free
//! space for insertion. Each entry is a single byte encoding the free space
//! category (0..=255), where each unit represents ~64 bytes of free space
//! (64 * 255 = 16320, close to PAGE_SIZE).
//!
//! The FSM is stored as a flat array in memory and can be persisted to
//! dedicated FSM pages on disk.


/// Bytes of free space represented by one FSM category unit.
const BYTES_PER_CATEGORY: usize = 64;

/// Maximum number of pages tracked by a single FSM instance.
const MAX_FSM_PAGES: usize = 1024 * 1024; // 1M pages = 1MB of FSM data

/// Free Space Map for tracking available space in data pages.
pub struct FreeSpaceMap {
    /// One byte per page: category representing free space.
    /// Category 0 = full, 255 = empty (16320+ bytes free).
    entries: Vec<u8>,
}

impl Default for FreeSpaceMap {
    fn default() -> Self {
        Self::new()
    }
}

impl FreeSpaceMap {
    /// Create a new empty FSM.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create an FSM pre-sized for `num_pages` pages, all marked as empty.
    pub fn with_capacity(num_pages: usize) -> Self {
        let n = num_pages.min(MAX_FSM_PAGES);
        Self {
            entries: vec![255; n],
        }
    }

    /// Convert free bytes to a category value (0..=255).
    fn bytes_to_category(free_bytes: usize) -> u8 {
        let cat = free_bytes / BYTES_PER_CATEGORY;
        cat.min(255) as u8
    }

    /// Convert a category value back to minimum free bytes it represents.
    fn category_to_bytes(category: u8) -> usize {
        category as usize * BYTES_PER_CATEGORY
    }

    /// Update the free space for a given page.
    pub fn update(&mut self, page_id: u32, free_bytes: usize) {
        let idx = page_id as usize;
        if idx >= MAX_FSM_PAGES {
            return;
        }
        // Extend entries if needed
        if idx >= self.entries.len() {
            self.entries.resize(idx + 1, 0);
        }
        self.entries[idx] = Self::bytes_to_category(free_bytes);
    }

    /// Find a page with at least `needed_bytes` of free space.
    /// Returns the page_id of the first suitable page, or None if no page has enough space.
    pub fn find_page(&self, needed_bytes: usize) -> Option<u32> {
        let needed_cat = Self::bytes_to_category(needed_bytes);
        // Need at least this category to have enough space
        let min_cat = if needed_bytes > 0 && needed_bytes % BYTES_PER_CATEGORY != 0 {
            needed_cat + 1
        } else {
            needed_cat
        };

        for (idx, &cat) in self.entries.iter().enumerate() {
            if cat >= min_cat {
                return Some(idx as u32);
            }
        }
        None
    }

    /// Find the page with the most free space. Returns (page_id, estimated_free_bytes).
    pub fn find_most_free(&self) -> Option<(u32, usize)> {
        let (idx, &max_cat) = self.entries.iter().enumerate().max_by_key(|&(_, &c)| c)?;
        if max_cat == 0 {
            return None;
        }
        Some((idx as u32, Self::category_to_bytes(max_cat)))
    }

    /// Get the free space category for a page (0 = full, 255 = empty).
    pub fn get(&self, page_id: u32) -> u8 {
        let idx = page_id as usize;
        if idx < self.entries.len() {
            self.entries[idx]
        } else {
            0
        }
    }

    /// Get the estimated free bytes for a page.
    pub fn free_bytes(&self, page_id: u32) -> usize {
        Self::category_to_bytes(self.get(page_id))
    }

    /// Number of pages tracked.
    pub fn page_count(&self) -> usize {
        self.entries.len()
    }

    /// Serialize the FSM to bytes for persistence.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.entries.len());
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.entries);
        out
    }

    /// Deserialize an FSM from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, &'static str> {
        if data.len() < 4 {
            return Err("FSM data too short");
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if data.len() < 4 + count {
            return Err("FSM data truncated");
        }
        Ok(Self {
            entries: data[4..4 + count].to_vec(),
        })
    }

    /// Get summary statistics: (total_pages, full_pages, empty_pages, avg_free_category).
    pub fn stats(&self) -> (usize, usize, usize, f64) {
        let total = self.entries.len();
        let full = self.entries.iter().filter(|&&c| c == 0).count();
        let empty = self.entries.iter().filter(|&&c| c == 255).count();
        let avg = if total > 0 {
            self.entries.iter().map(|&c| c as f64).sum::<f64>() / total as f64
        } else {
            0.0
        };
        (total, full, empty, avg)
    }
}

impl std::fmt::Debug for FreeSpaceMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (total, full, empty, avg) = self.stats();
        f.debug_struct("FreeSpaceMap")
            .field("pages", &total)
            .field("full", &full)
            .field("empty", &empty)
            .field("avg_category", &format!("{avg:.1}"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::page::PAGE_SIZE;

    #[test]
    fn new_fsm_is_empty() {
        let fsm = FreeSpaceMap::new();
        assert_eq!(fsm.page_count(), 0);
        assert_eq!(fsm.find_page(100), None);
    }

    #[test]
    fn with_capacity_marks_empty() {
        let fsm = FreeSpaceMap::with_capacity(10);
        assert_eq!(fsm.page_count(), 10);
        // All pages should be marked as empty (255)
        for i in 0..10 {
            assert_eq!(fsm.get(i), 255);
        }
    }

    #[test]
    fn update_and_get() {
        let mut fsm = FreeSpaceMap::new();
        fsm.update(0, PAGE_SIZE); // Full page of free space
        fsm.update(1, 0);         // No free space
        fsm.update(2, 1000);      // Some free space

        assert_eq!(fsm.get(0), 255); // PAGE_SIZE / 64 = 256, capped at 255
        assert_eq!(fsm.get(1), 0);
        assert_eq!(fsm.get(2), (1000u16 / 64) as u8); // 15
    }

    #[test]
    fn find_page_with_enough_space() {
        let mut fsm = FreeSpaceMap::new();
        fsm.update(0, 100);   // ~100 bytes free
        fsm.update(1, 500);   // ~500 bytes free
        fsm.update(2, 2000);  // ~2000 bytes free
        fsm.update(3, 50);    // ~50 bytes free

        // Need 400 bytes → page 1 has 500
        assert_eq!(fsm.find_page(400), Some(1));
        // Need 1500 bytes → page 2 has 2000
        assert_eq!(fsm.find_page(1500), Some(2));
        // Need 5000 bytes → no page has enough
        assert_eq!(fsm.find_page(5000), None);
    }

    #[test]
    fn find_most_free() {
        let mut fsm = FreeSpaceMap::new();
        fsm.update(0, 100);
        fsm.update(1, 5000);
        fsm.update(2, 2000);

        let (page_id, free) = fsm.find_most_free().unwrap();
        assert_eq!(page_id, 1);
        assert!(free >= 4992); // 5000 / 64 * 64 = 4992
    }

    #[test]
    fn find_most_free_empty_fsm() {
        let fsm = FreeSpaceMap::new();
        assert_eq!(fsm.find_most_free(), None);
    }

    #[test]
    fn find_most_free_all_full() {
        let mut fsm = FreeSpaceMap::new();
        fsm.update(0, 0);
        fsm.update(1, 0);
        assert_eq!(fsm.find_most_free(), None);
    }

    #[test]
    fn serialization_roundtrip() {
        let mut fsm = FreeSpaceMap::new();
        fsm.update(0, 100);
        fsm.update(5, 5000);
        fsm.update(10, PAGE_SIZE);

        let bytes = fsm.to_bytes();
        let fsm2 = FreeSpaceMap::from_bytes(&bytes).unwrap();

        assert_eq!(fsm.page_count(), fsm2.page_count());
        for i in 0..fsm.page_count() as u32 {
            assert_eq!(fsm.get(i), fsm2.get(i));
        }
    }

    #[test]
    fn serialization_error_cases() {
        assert!(FreeSpaceMap::from_bytes(&[]).is_err());
        assert!(FreeSpaceMap::from_bytes(&[1, 0, 0, 0]).is_err()); // claims 1 entry but no data
        assert!(FreeSpaceMap::from_bytes(&[0, 0, 0, 0]).is_ok()); // 0 entries
    }

    #[test]
    fn auto_extend_entries() {
        let mut fsm = FreeSpaceMap::new();
        // Update a high page_id — should auto-extend
        fsm.update(100, 500);
        assert_eq!(fsm.page_count(), 101);
        assert_eq!(fsm.get(100), (500 / 64) as u8);
        // Intermediate pages should be 0 (full)
        assert_eq!(fsm.get(50), 0);
    }

    #[test]
    fn stats_calculation() {
        let mut fsm = FreeSpaceMap::with_capacity(5);
        fsm.update(0, 0);          // full
        fsm.update(1, 0);          // full
        fsm.update(2, PAGE_SIZE);  // empty
        // pages 3 and 4 are initially 255 (empty)

        let (total, full, empty, _avg) = fsm.stats();
        assert_eq!(total, 5);
        assert_eq!(full, 2);
        assert_eq!(empty, 3); // pages 2, 3, 4
    }

    #[test]
    fn category_boundaries() {
        // 0 bytes → category 0
        assert_eq!(FreeSpaceMap::bytes_to_category(0), 0);
        // 63 bytes → category 0
        assert_eq!(FreeSpaceMap::bytes_to_category(63), 0);
        // 64 bytes → category 1
        assert_eq!(FreeSpaceMap::bytes_to_category(64), 1);
        // PAGE_SIZE → category 255 (capped)
        assert_eq!(FreeSpaceMap::bytes_to_category(PAGE_SIZE), 255);
    }

    #[test]
    fn free_bytes_accessor() {
        let mut fsm = FreeSpaceMap::new();
        fsm.update(0, 1000);
        // 1000 / 64 = 15, 15 * 64 = 960
        assert_eq!(fsm.free_bytes(0), 960);
    }
}
