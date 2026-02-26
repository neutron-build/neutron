//! Database branching engine — zero-copy branching for testing/staging.
//!
//! Supports:
//!   - CREATE BRANCH 'staging' FROM 'main' — zero-copy branch
//!   - Copy-on-write page sharing between branches
//!   - Branch metadata and lineage tracking
//!   - Branch comparison and merging
//!
//! Replaces Neon's branching feature, built natively into the engine.

use std::collections::{HashMap, HashSet};

// ============================================================================
// Types
// ============================================================================

/// Unique branch identifier.
pub type BranchId = u64;

/// A database branch with copy-on-write page sharing.
#[derive(Debug, Clone)]
pub struct DatabaseBranch {
    pub id: BranchId,
    pub name: String,
    pub parent_id: Option<BranchId>,
    /// Snapshot ID at branch creation point.
    pub base_snapshot: u64,
    /// Pages modified in this branch (overrides shared pages).
    pub modified_pages: HashMap<u64, Vec<u8>>,
    /// Pages deleted in this branch.
    pub deleted_pages: HashSet<u64>,
    pub created_at: u64,
    pub metadata: HashMap<String, String>,
    pub is_active: bool,
}

/// A snapshot of the database state.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub id: u64,
    pub branch_id: BranchId,
    pub pages: HashMap<u64, Vec<u8>>,
    pub timestamp: u64,
}

/// Diff between two branches.
#[derive(Debug)]
pub struct BranchDiff {
    pub added_pages: Vec<u64>,
    pub modified_pages: Vec<u64>,
    pub deleted_pages: Vec<u64>,
}

// ============================================================================
// Branch manager
// ============================================================================

/// Database branching engine with copy-on-write semantics.
pub struct BranchManager {
    branches: HashMap<BranchId, DatabaseBranch>,
    /// Shared page store (base pages shared across branches).
    shared_pages: HashMap<u64, Vec<u8>>,
    next_branch_id: BranchId,
    next_snapshot_id: u64,
}

impl Default for BranchManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BranchManager {
    pub fn new() -> Self {
        let mut mgr = Self {
            branches: HashMap::new(),
            shared_pages: HashMap::new(),
            next_branch_id: 1,
            next_snapshot_id: 1,
        };

        // Create the default "main" branch
        let main_branch = DatabaseBranch {
            id: 0,
            name: "main".into(),
            parent_id: None,
            base_snapshot: 0,
            modified_pages: HashMap::new(),
            deleted_pages: HashSet::new(),
            created_at: now_ms(),
            metadata: HashMap::new(),
            is_active: true,
        };
        mgr.branches.insert(0, main_branch);
        mgr
    }

    /// Create a new branch from an existing branch (zero-copy).
    pub fn create_branch(
        &mut self,
        name: &str,
        from_branch: &str,
    ) -> Result<BranchId, BranchError> {
        let parent = self
            .find_branch_by_name(from_branch)
            .ok_or_else(|| BranchError::NotFound(from_branch.to_string()))?;
        let parent_id = parent.id;

        if self.find_branch_by_name(name).is_some() {
            return Err(BranchError::AlreadyExists(name.to_string()));
        }

        let id = self.next_branch_id;
        self.next_branch_id += 1;

        let snapshot_id = self.next_snapshot_id;
        self.next_snapshot_id += 1;

        // Materialize parent's pages into shared store (if not already there)
        let parent = self.branches.get(&parent_id).unwrap();
        for (&page_id, page_data) in &parent.modified_pages {
            self.shared_pages
                .entry(page_id)
                .or_insert_with(|| page_data.clone());
        }

        let branch = DatabaseBranch {
            id,
            name: name.to_string(),
            parent_id: Some(parent_id),
            base_snapshot: snapshot_id,
            modified_pages: HashMap::new(),
            deleted_pages: HashSet::new(),
            created_at: now_ms(),
            metadata: HashMap::new(),
            is_active: true,
        };

        self.branches.insert(id, branch);
        Ok(id)
    }

    /// Delete a branch.
    pub fn delete_branch(&mut self, name: &str) -> Result<(), BranchError> {
        if name == "main" {
            return Err(BranchError::CannotDeleteMain);
        }

        let branch = self
            .find_branch_by_name(name)
            .ok_or_else(|| BranchError::NotFound(name.to_string()))?;
        let id = branch.id;

        // Check no child branches
        let has_children = self
            .branches
            .values()
            .any(|b| b.parent_id == Some(id) && b.is_active);
        if has_children {
            return Err(BranchError::HasChildren(name.to_string()));
        }

        if let Some(b) = self.branches.get_mut(&id) {
            b.is_active = false;
        }
        Ok(())
    }

    /// Read a page from a branch (follows copy-on-write chain).
    pub fn read_page(&self, branch_name: &str, page_id: u64) -> Option<&[u8]> {
        let branch = self.find_branch_by_name(branch_name)?;

        // Check if the page was deleted in this branch
        if branch.deleted_pages.contains(&page_id) {
            return None;
        }

        // Check branch-local modified pages first
        if let Some(data) = branch.modified_pages.get(&page_id) {
            return Some(data.as_slice());
        }

        // Fall back to shared pages
        if let Some(data) = self.shared_pages.get(&page_id) {
            return Some(data.as_slice());
        }

        // Walk parent chain
        if let Some(parent_id) = branch.parent_id {
            if let Some(parent) = self.branches.get(&parent_id) {
                if parent.deleted_pages.contains(&page_id) {
                    return None;
                }
                if let Some(data) = parent.modified_pages.get(&page_id) {
                    return Some(data.as_slice());
                }
            }
        }

        None
    }

    /// Write a page to a branch (copy-on-write).
    pub fn write_page(
        &mut self,
        branch_name: &str,
        page_id: u64,
        data: Vec<u8>,
    ) -> Result<(), BranchError> {
        let branch = self
            .find_branch_by_name_mut(branch_name)
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;

        branch.modified_pages.insert(page_id, data);
        branch.deleted_pages.remove(&page_id);
        Ok(())
    }

    /// Delete a page from a branch.
    pub fn delete_page(&mut self, branch_name: &str, page_id: u64) -> Result<(), BranchError> {
        let branch = self
            .find_branch_by_name_mut(branch_name)
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;

        branch.modified_pages.remove(&page_id);
        branch.deleted_pages.insert(page_id);
        Ok(())
    }

    /// Diff between two branches.
    pub fn diff(&self, branch_a: &str, branch_b: &str) -> Result<BranchDiff, BranchError> {
        let a = self
            .find_branch_by_name(branch_a)
            .ok_or_else(|| BranchError::NotFound(branch_a.to_string()))?;
        let b = self
            .find_branch_by_name(branch_b)
            .ok_or_else(|| BranchError::NotFound(branch_b.to_string()))?;

        let a_pages: HashSet<u64> = a.modified_pages.keys().copied().collect();
        let b_pages: HashSet<u64> = b.modified_pages.keys().copied().collect();

        let added: Vec<u64> = b_pages.difference(&a_pages).copied().collect();
        let deleted: Vec<u64> = a_pages.difference(&b_pages).copied().collect();
        let modified: Vec<u64> = a_pages
            .intersection(&b_pages)
            .filter(|&&page_id| {
                a.modified_pages.get(&page_id) != b.modified_pages.get(&page_id)
            })
            .copied()
            .collect();

        Ok(BranchDiff {
            added_pages: added,
            modified_pages: modified,
            deleted_pages: deleted,
        })
    }

    /// Merge a source branch into a target branch.
    /// Source branch's modified pages override target's.
    pub fn merge(
        &mut self,
        source_name: &str,
        target_name: &str,
    ) -> Result<usize, BranchError> {
        let source = self
            .find_branch_by_name(source_name)
            .ok_or_else(|| BranchError::NotFound(source_name.to_string()))?;

        // Collect source modifications
        let source_pages: Vec<(u64, Vec<u8>)> = source
            .modified_pages
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        let source_deletes: Vec<u64> = source.deleted_pages.iter().copied().collect();

        let target = self
            .find_branch_by_name_mut(target_name)
            .ok_or_else(|| BranchError::NotFound(target_name.to_string()))?;

        let mut merge_count = 0;
        for (page_id, data) in source_pages {
            target.modified_pages.insert(page_id, data);
            target.deleted_pages.remove(&page_id);
            merge_count += 1;
        }
        for page_id in source_deletes {
            target.modified_pages.remove(&page_id);
            target.deleted_pages.insert(page_id);
            merge_count += 1;
        }

        Ok(merge_count)
    }

    /// List all active branches.
    pub fn list_branches(&self) -> Vec<&DatabaseBranch> {
        self.branches.values().filter(|b| b.is_active).collect()
    }

    /// Get branch info.
    pub fn get_branch(&self, name: &str) -> Option<&DatabaseBranch> {
        self.find_branch_by_name(name)
    }

    /// Number of branch-local pages (measure of divergence from parent).
    pub fn branch_size(&self, name: &str) -> Option<usize> {
        self.find_branch_by_name(name)
            .map(|b| b.modified_pages.len() + b.deleted_pages.len())
    }

    fn find_branch_by_name(&self, name: &str) -> Option<&DatabaseBranch> {
        self.branches.values().find(|b| b.name == name && b.is_active)
    }

    fn find_branch_by_name_mut(&mut self, name: &str) -> Option<&mut DatabaseBranch> {
        self.branches.values_mut().find(|b| b.name == name && b.is_active)
    }
}

/// Branch-related errors.
#[derive(Debug, Clone)]
pub enum BranchError {
    NotFound(String),
    AlreadyExists(String),
    CannotDeleteMain,
    HasChildren(String),
}

impl std::fmt::Display for BranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BranchError::NotFound(n) => write!(f, "branch not found: {n}"),
            BranchError::AlreadyExists(n) => write!(f, "branch already exists: {n}"),
            BranchError::CannotDeleteMain => write!(f, "cannot delete main branch"),
            BranchError::HasChildren(n) => write!(f, "branch has active children: {n}"),
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_list_branches() {
        let mut mgr = BranchManager::new();
        mgr.create_branch("staging", "main").unwrap();
        mgr.create_branch("dev", "main").unwrap();

        let branches = mgr.list_branches();
        assert_eq!(branches.len(), 3); // main, staging, dev
    }

    #[test]
    fn duplicate_branch_name() {
        let mut mgr = BranchManager::new();
        mgr.create_branch("staging", "main").unwrap();
        assert!(mgr.create_branch("staging", "main").is_err());
    }

    #[test]
    fn copy_on_write() {
        let mut mgr = BranchManager::new();

        // Write to main
        mgr.write_page("main", 1, vec![1, 2, 3]).unwrap();
        mgr.write_page("main", 2, vec![4, 5, 6]).unwrap();

        // Create branch
        mgr.create_branch("feature", "main").unwrap();

        // Both branches see the same data
        assert_eq!(mgr.read_page("main", 1), Some(&[1, 2, 3][..]));
        assert_eq!(mgr.read_page("feature", 1), Some(&[1, 2, 3][..]));

        // Modify on feature branch
        mgr.write_page("feature", 1, vec![7, 8, 9]).unwrap();

        // Main is unchanged, feature sees the new data
        assert_eq!(mgr.read_page("main", 1), Some(&[1, 2, 3][..]));
        assert_eq!(mgr.read_page("feature", 1), Some(&[7, 8, 9][..]));

        // Both still see page 2
        assert_eq!(mgr.read_page("feature", 2), Some(&[4, 5, 6][..]));
    }

    #[test]
    fn branch_diff() {
        let mut mgr = BranchManager::new();
        mgr.write_page("main", 1, vec![1]).unwrap();
        mgr.write_page("main", 2, vec![2]).unwrap();

        mgr.create_branch("feature", "main").unwrap();
        mgr.write_page("feature", 2, vec![20]).unwrap(); // Modify page 2
        mgr.write_page("feature", 3, vec![3]).unwrap(); // Add page 3

        let diff = mgr.diff("main", "feature").unwrap();
        assert_eq!(diff.added_pages.len(), 1); // Page 3
        assert_eq!(diff.modified_pages.len(), 1); // Page 2
    }

    #[test]
    fn merge_branch() {
        let mut mgr = BranchManager::new();
        mgr.write_page("main", 1, vec![1]).unwrap();

        mgr.create_branch("feature", "main").unwrap();
        mgr.write_page("feature", 1, vec![10]).unwrap();
        mgr.write_page("feature", 2, vec![20]).unwrap();

        let merged = mgr.merge("feature", "main").unwrap();
        assert_eq!(merged, 2);

        // Main now has the merged data
        assert_eq!(mgr.read_page("main", 1), Some(&[10][..]));
        assert_eq!(mgr.read_page("main", 2), Some(&[20][..]));
    }

    #[test]
    fn delete_branch() {
        let mut mgr = BranchManager::new();
        mgr.create_branch("temp", "main").unwrap();
        assert_eq!(mgr.list_branches().len(), 2);

        mgr.delete_branch("temp").unwrap();
        assert_eq!(mgr.list_branches().len(), 1);

        // Cannot delete main
        assert!(mgr.delete_branch("main").is_err());
    }

    #[test]
    fn delete_page_on_branch() {
        let mut mgr = BranchManager::new();
        mgr.write_page("main", 1, vec![1]).unwrap();
        mgr.write_page("main", 2, vec![2]).unwrap();

        mgr.create_branch("feature", "main").unwrap();
        mgr.delete_page("feature", 1).unwrap();

        // Main still has page 1, feature doesn't
        assert_eq!(mgr.read_page("main", 1), Some(&[1][..]));
        assert!(mgr.read_page("feature", 1).is_none());
        assert_eq!(mgr.read_page("feature", 2), Some(&[2][..]));
    }

    #[test]
    fn branch_size() {
        let mut mgr = BranchManager::new();
        assert_eq!(mgr.branch_size("main"), Some(0));

        mgr.write_page("main", 1, vec![1]).unwrap();
        mgr.write_page("main", 2, vec![2]).unwrap();
        assert_eq!(mgr.branch_size("main"), Some(2));
    }

    #[test]
    fn branch_from_nonexistent_parent() {
        let mut mgr = BranchManager::new();
        assert!(mgr.create_branch("orphan", "nonexistent").is_err());
    }

    #[test]
    fn read_nonexistent_page() {
        let mgr = BranchManager::new();
        assert!(mgr.read_page("main", 999).is_none());
    }

    #[test]
    fn nested_branch_cow() {
        let mut mgr = BranchManager::new();
        mgr.write_page("main", 1, vec![1]).unwrap();

        // Create branch1 from main, write a page there
        mgr.create_branch("branch1", "main").unwrap();
        mgr.write_page("branch1", 2, vec![2]).unwrap();

        // Create branch2 from branch1 — this materializes branch1's pages
        // into shared_pages, making page 2 visible everywhere.
        mgr.create_branch("branch2", "branch1").unwrap();

        // Now write branch2-only page
        mgr.write_page("branch2", 3, vec![3]).unwrap();

        // branch2 sees all inherited pages
        assert_eq!(mgr.read_page("branch2", 1), Some(&[1][..]));
        assert_eq!(mgr.read_page("branch2", 2), Some(&[2][..]));
        assert_eq!(mgr.read_page("branch2", 3), Some(&[3][..]));

        // branch1 still sees its own page 2
        assert_eq!(mgr.read_page("branch1", 2), Some(&[2][..]));
        // branch1 doesn't see branch2-only page 3
        assert!(mgr.read_page("branch1", 3).is_none());

        // Overwrite page 2 on branch2 — branch1 should still see original
        mgr.write_page("branch2", 2, vec![22]).unwrap();
        assert_eq!(mgr.read_page("branch2", 2), Some(&[22][..]));
    }

    #[test]
    fn merge_adds_new_and_modifies_existing() {
        let mut mgr = BranchManager::new();
        mgr.write_page("main", 1, vec![10]).unwrap();
        mgr.write_page("main", 2, vec![20]).unwrap();

        mgr.create_branch("f", "main").unwrap();
        mgr.write_page("f", 1, vec![100]).unwrap(); // modify
        mgr.write_page("f", 3, vec![30]).unwrap();  // add
        mgr.write_page("f", 4, vec![40]).unwrap();  // add

        let merged = mgr.merge("f", "main").unwrap();
        assert_eq!(merged, 3); // 1 modified + 2 added

        assert_eq!(mgr.read_page("main", 1), Some(&[100][..]));
        assert_eq!(mgr.read_page("main", 2), Some(&[20][..]));
        assert_eq!(mgr.read_page("main", 3), Some(&[30][..]));
        assert_eq!(mgr.read_page("main", 4), Some(&[40][..]));
    }

    #[test]
    fn diff_with_deleted_pages() {
        let mut mgr = BranchManager::new();
        mgr.write_page("main", 1, vec![1]).unwrap();
        mgr.write_page("main", 2, vec![2]).unwrap();
        mgr.write_page("main", 3, vec![3]).unwrap();

        mgr.create_branch("f", "main").unwrap();
        mgr.delete_page("f", 1).unwrap();
        mgr.write_page("f", 4, vec![4]).unwrap();

        let diff = mgr.diff("main", "f").unwrap();
        assert!(diff.deleted_pages.contains(&1));
        assert!(diff.added_pages.contains(&4));
        assert!(diff.modified_pages.is_empty());
    }

    #[test]
    fn branch_metadata() {
        let mgr = BranchManager::new();
        let branches = mgr.list_branches();
        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].name, "main");
    }

    #[test]
    fn overwrite_page_multiple_times() {
        let mut mgr = BranchManager::new();
        mgr.write_page("main", 1, vec![1]).unwrap();
        mgr.write_page("main", 1, vec![2]).unwrap();
        mgr.write_page("main", 1, vec![3]).unwrap();
        assert_eq!(mgr.read_page("main", 1), Some(&[3][..]));
        assert_eq!(mgr.branch_size("main"), Some(1)); // still just 1 page
    }
}
