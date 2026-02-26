//! Git-like data versioning engine.
//!
//! Supports:
//!   - Branch, commit, merge, diff, time-travel at row granularity
//!   - SELECT * FROM table AS OF 'v2.3'
//!   - Point-in-time queries (temporal/AS-OF joins)
//!
//! Replaces DVC, LakeFS, Dolt for data versioning.

use std::collections::HashMap;

// ============================================================================
// Types
// ============================================================================

/// Unique commit identifier (simplified — production would use SHA-256).
pub type CommitId = u64;

/// A row identified by table and primary key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowKey {
    pub table: String,
    pub pk: String,
}

/// A row value (column_name → value).
pub type RowData = HashMap<String, String>;

/// A change to a single row.
#[derive(Debug, Clone)]
pub enum RowChange {
    Insert(RowData),
    Update { old: RowData, new: RowData },
    Delete(RowData),
}

/// A commit in the version history.
#[derive(Debug, Clone)]
pub struct Commit {
    pub id: CommitId,
    pub parent: Option<CommitId>,
    pub branch: String,
    pub message: String,
    pub changes: HashMap<RowKey, RowChange>,
    pub timestamp: u64,
}

/// A branch pointing to a commit.
#[derive(Debug, Clone)]
pub struct Branch {
    pub name: String,
    pub head: CommitId,
}

/// Diff between two versions.
#[derive(Debug, Clone)]
pub struct VersionDiff {
    pub inserted: Vec<(RowKey, RowData)>,
    pub updated: Vec<(RowKey, RowData, RowData)>, // (key, old, new)
    pub deleted: Vec<(RowKey, RowData)>,
}

// ============================================================================
// Version store
// ============================================================================

/// Git-like version control for database rows.
pub struct VersionStore {
    commits: HashMap<CommitId, Commit>,
    branches: HashMap<String, Branch>,
    /// Materialized state per commit: (table, pk) → row_data.
    /// Computed lazily and cached.
    snapshots: HashMap<CommitId, HashMap<RowKey, RowData>>,
    next_commit_id: CommitId,
}

impl Default for VersionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionStore {
    pub fn new() -> Self {
        let mut store = Self {
            commits: HashMap::new(),
            branches: HashMap::new(),
            snapshots: HashMap::new(),
            next_commit_id: 1,
        };

        // Create initial empty commit on "main" branch
        let init_commit = Commit {
            id: 0,
            parent: None,
            branch: "main".into(),
            message: "initial commit".into(),
            changes: HashMap::new(),
            timestamp: now_ms(),
        };
        store.commits.insert(0, init_commit);
        store.snapshots.insert(0, HashMap::new());
        store.branches.insert(
            "main".into(),
            Branch {
                name: "main".into(),
                head: 0,
            },
        );

        store
    }

    /// Create a new branch from an existing branch's HEAD.
    pub fn create_branch(&mut self, name: &str, from: &str) -> Result<(), VersionError> {
        let source = self
            .branches
            .get(from)
            .ok_or(VersionError::BranchNotFound(from.to_string()))?;
        let head = source.head;

        if self.branches.contains_key(name) {
            return Err(VersionError::BranchExists(name.to_string()));
        }

        self.branches.insert(
            name.to_string(),
            Branch {
                name: name.to_string(),
                head,
            },
        );
        Ok(())
    }

    /// Commit changes to a branch. Returns the commit ID.
    pub fn commit(
        &mut self,
        branch: &str,
        message: &str,
        changes: HashMap<RowKey, RowChange>,
    ) -> Result<CommitId, VersionError> {
        let branch_data = self
            .branches
            .get(branch)
            .ok_or(VersionError::BranchNotFound(branch.to_string()))?
            .clone();

        let id = self.next_commit_id;
        self.next_commit_id += 1;

        let commit = Commit {
            id,
            parent: Some(branch_data.head),
            branch: branch.to_string(),
            message: message.to_string(),
            changes: changes.clone(),
            timestamp: now_ms(),
        };

        // Materialize snapshot
        let mut snapshot = self.get_snapshot(branch_data.head)?;
        for (key, change) in &changes {
            match change {
                RowChange::Insert(data) => {
                    snapshot.insert(key.clone(), data.clone());
                }
                RowChange::Update { new, .. } => {
                    snapshot.insert(key.clone(), new.clone());
                }
                RowChange::Delete(_) => {
                    snapshot.remove(key);
                }
            }
        }

        self.commits.insert(id, commit);
        self.snapshots.insert(id, snapshot);
        self.branches.get_mut(branch).unwrap().head = id;

        Ok(id)
    }

    /// Get the materialized snapshot at a commit.
    fn get_snapshot(&self, commit_id: CommitId) -> Result<HashMap<RowKey, RowData>, VersionError> {
        self.snapshots
            .get(&commit_id)
            .cloned()
            .ok_or(VersionError::CommitNotFound(commit_id))
    }

    /// Query rows at a specific version (commit or branch HEAD).
    pub fn query_at(
        &self,
        branch_or_commit: &str,
        table: &str,
    ) -> Result<Vec<(String, RowData)>, VersionError> {
        let commit_id = self.resolve_ref(branch_or_commit)?;
        let snapshot = self.get_snapshot(commit_id)?;

        let rows: Vec<(String, RowData)> = snapshot
            .iter()
            .filter(|(key, _)| key.table == table)
            .map(|(key, data)| (key.pk.clone(), data.clone()))
            .collect();

        Ok(rows)
    }

    /// Resolve a reference (branch name or commit ID string) to a commit ID.
    fn resolve_ref(&self, ref_str: &str) -> Result<CommitId, VersionError> {
        if let Some(branch) = self.branches.get(ref_str) {
            Ok(branch.head)
        } else if let Ok(id) = ref_str.parse::<CommitId>() {
            if self.commits.contains_key(&id) {
                Ok(id)
            } else {
                Err(VersionError::CommitNotFound(id))
            }
        } else {
            Err(VersionError::BranchNotFound(ref_str.to_string()))
        }
    }

    /// Diff between two versions.
    pub fn diff(&self, from: &str, to: &str) -> Result<VersionDiff, VersionError> {
        let from_id = self.resolve_ref(from)?;
        let to_id = self.resolve_ref(to)?;

        let from_snap = self.get_snapshot(from_id)?;
        let to_snap = self.get_snapshot(to_id)?;

        let mut diff = VersionDiff {
            inserted: Vec::new(),
            updated: Vec::new(),
            deleted: Vec::new(),
        };

        // Find inserts and updates
        for (key, to_data) in &to_snap {
            match from_snap.get(key) {
                None => diff.inserted.push((key.clone(), to_data.clone())),
                Some(from_data) => {
                    if from_data != to_data {
                        diff.updated
                            .push((key.clone(), from_data.clone(), to_data.clone()));
                    }
                }
            }
        }

        // Find deletes
        for (key, from_data) in &from_snap {
            if !to_snap.contains_key(key) {
                diff.deleted.push((key.clone(), from_data.clone()));
            }
        }

        Ok(diff)
    }

    /// Merge a source branch into a target branch (fast-forward or auto-merge).
    /// Simple strategy: source changes override target on conflicts.
    pub fn merge(
        &mut self,
        source: &str,
        target: &str,
        message: &str,
    ) -> Result<CommitId, VersionError> {
        let source_id = self.resolve_ref(source)?;
        let target_id = self.resolve_ref(target)?;

        let source_snap = self.get_snapshot(source_id)?;
        let target_snap = self.get_snapshot(target_id)?;

        // Compute changes needed to bring target to merged state
        let mut changes = HashMap::new();

        // Add/update rows from source
        for (key, source_data) in &source_snap {
            match target_snap.get(key) {
                None => {
                    changes.insert(
                        key.clone(),
                        RowChange::Insert(source_data.clone()),
                    );
                }
                Some(target_data) => {
                    if target_data != source_data {
                        changes.insert(
                            key.clone(),
                            RowChange::Update {
                                old: target_data.clone(),
                                new: source_data.clone(),
                            },
                        );
                    }
                }
            }
        }

        self.commit(target, message, changes)
    }

    /// Get commit history for a branch (newest first).
    pub fn log(&self, branch: &str) -> Result<Vec<&Commit>, VersionError> {
        let mut result = Vec::new();
        let mut current = self.resolve_ref(branch)?;

        loop {
            let commit = self
                .commits
                .get(&current)
                .ok_or(VersionError::CommitNotFound(current))?;
            result.push(commit);
            match commit.parent {
                Some(parent) => current = parent,
                None => break,
            }
        }

        Ok(result)
    }

    /// List all branches.
    pub fn list_branches(&self) -> Vec<&str> {
        self.branches.keys().map(|s| s.as_str()).collect()
    }

    /// Delete a branch (cannot delete "main").
    pub fn delete_branch(&mut self, name: &str) -> Result<(), VersionError> {
        if name == "main" {
            return Err(VersionError::CannotDeleteMain);
        }
        self.branches
            .remove(name)
            .ok_or(VersionError::BranchNotFound(name.to_string()))?;
        Ok(())
    }
}

/// Version store errors.
#[derive(Debug, Clone)]
pub enum VersionError {
    BranchNotFound(String),
    BranchExists(String),
    CommitNotFound(CommitId),
    CannotDeleteMain,
}

impl std::fmt::Display for VersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionError::BranchNotFound(b) => write!(f, "branch not found: {b}"),
            VersionError::BranchExists(b) => write!(f, "branch already exists: {b}"),
            VersionError::CommitNotFound(id) => write!(f, "commit not found: {id}"),
            VersionError::CannotDeleteMain => write!(f, "cannot delete main branch"),
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ============================================================================
// Temporal / AS-OF join support
// ============================================================================

/// A row with a validity interval for temporal queries.
///
/// Each `TemporalRow` represents a version of a row that was active during
/// the interval `[valid_from, valid_to)`.  When `valid_to` is `None` the
/// row is the current (latest) version.
#[derive(Debug, Clone)]
pub struct TemporalRow {
    pub key: RowKey,
    pub data: RowData,
    /// Timestamp (inclusive) when this version became active.
    pub valid_from: u64,
    /// Timestamp (exclusive) when this version was superseded, or `None` if
    /// this is the current version.
    pub valid_to: Option<u64>,
}

/// A table that stores the full history of every row, enabling point-in-time
/// queries and AS-OF joins.
///
/// Internally rows are stored in a flat `Vec` sorted by `(key, valid_from)`.
/// This keeps the implementation simple while still allowing efficient binary
/// search within a key's history.
pub struct TemporalTable {
    pub name: String,
    /// All row versions, sorted by `(key.table, key.pk, valid_from)`.
    rows: Vec<TemporalRow>,
}

impl TemporalTable {
    /// Create a new, empty temporal table.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            rows: Vec::new(),
        }
    }

    /// Insert a new version of a row at `timestamp`.
    ///
    /// If a current (open-ended) version for the same key exists, its
    /// `valid_to` is set to `timestamp` to close it before the new version
    /// is appended.
    pub fn insert(&mut self, key: RowKey, data: RowData, timestamp: u64) {
        // Close the previous current version for this key, if any.
        for row in self.rows.iter_mut().rev() {
            if row.key == key && row.valid_to.is_none() {
                row.valid_to = Some(timestamp);
                break;
            }
        }

        self.rows.push(TemporalRow {
            key,
            data,
            valid_from: timestamp,
            valid_to: None,
        });

        // Keep rows sorted by (table, pk, valid_from) so binary search works.
        self.rows.sort_by(|a, b| {
            a.key
                .table
                .cmp(&b.key.table)
                .then_with(|| a.key.pk.cmp(&b.key.pk))
                .then_with(|| a.valid_from.cmp(&b.valid_from))
        });
    }

    /// Return the row data that was active for `key` at the given `timestamp`.
    ///
    /// Uses binary search to find the first row for the key whose
    /// `valid_from <= timestamp`, then verifies the row was still active
    /// (i.e. `valid_to` is `None` or `> timestamp`).
    pub fn as_of(&self, key: &RowKey, timestamp: u64) -> Option<&RowData> {
        // Collect the slice of rows belonging to this key.
        let key_rows: Vec<&TemporalRow> = self
            .rows
            .iter()
            .filter(|r| r.key == *key)
            .collect();

        if key_rows.is_empty() {
            return None;
        }

        // Binary search: find the latest row whose valid_from <= timestamp.
        // `partition_point` returns the first index where the predicate is false,
        // so we search for valid_from <= timestamp.
        let idx = key_rows.partition_point(|r| r.valid_from <= timestamp);

        if idx == 0 {
            // All rows start after `timestamp`.
            return None;
        }

        let row = key_rows[idx - 1];

        // Verify the row was still active at `timestamp`.
        match row.valid_to {
            Some(end) if timestamp >= end => None,
            _ => Some(&row.data),
        }
    }

    /// Return the full version history for `key`, ordered by `valid_from`.
    pub fn history(&self, key: &RowKey) -> Vec<&TemporalRow> {
        self.rows.iter().filter(|r| r.key == *key).collect()
    }
}

/// Perform an AS-OF join between a list of events and a `TemporalTable`.
///
/// For each `(event_timestamp, key)` pair in `events`, the function looks up
/// the row state in `temporal_table` that was active **at or before**
/// `event_timestamp`.  Events with no matching row are silently skipped.
///
/// This is the standard technique to prevent *data leakage* in ML feature
/// engineering: features are always looked up as of the event time, so no
/// future information can leak into the training set.
pub fn as_of_join(
    events: &[(u64, RowKey)],
    temporal_table: &TemporalTable,
) -> Vec<(u64, RowData)> {
    let mut results = Vec::new();

    for (event_ts, key) in events {
        if let Some(data) = temporal_table.as_of(key, *event_ts) {
            results.push((*event_ts, data.clone()));
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(pairs: &[(&str, &str)]) -> RowData {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn basic_commit_and_query() {
        let mut vs = VersionStore::new();

        let mut changes = HashMap::new();
        changes.insert(
            RowKey { table: "users".into(), pk: "1".into() },
            RowChange::Insert(make_row(&[("name", "Alice"), ("age", "30")])),
        );
        changes.insert(
            RowKey { table: "users".into(), pk: "2".into() },
            RowChange::Insert(make_row(&[("name", "Bob"), ("age", "25")])),
        );

        vs.commit("main", "add users", changes).unwrap();

        let rows = vs.query_at("main", "users").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn time_travel_query() {
        let mut vs = VersionStore::new();

        // Commit 1: add Alice
        let mut c1 = HashMap::new();
        c1.insert(
            RowKey { table: "users".into(), pk: "1".into() },
            RowChange::Insert(make_row(&[("name", "Alice")])),
        );
        let commit1 = vs.commit("main", "add alice", c1).unwrap();

        // Commit 2: add Bob
        let mut c2 = HashMap::new();
        c2.insert(
            RowKey { table: "users".into(), pk: "2".into() },
            RowChange::Insert(make_row(&[("name", "Bob")])),
        );
        vs.commit("main", "add bob", c2).unwrap();

        // Query at current HEAD: should see both
        let current = vs.query_at("main", "users").unwrap();
        assert_eq!(current.len(), 2);

        // Query at commit1: should see only Alice
        let past = vs.query_at(&commit1.to_string(), "users").unwrap();
        assert_eq!(past.len(), 1);
        assert_eq!(past[0].1["name"], "Alice");
    }

    #[test]
    fn branching_and_merge() {
        let mut vs = VersionStore::new();

        // Add data on main
        let mut c1 = HashMap::new();
        c1.insert(
            RowKey { table: "data".into(), pk: "1".into() },
            RowChange::Insert(make_row(&[("value", "original")])),
        );
        vs.commit("main", "initial data", c1).unwrap();

        // Create feature branch
        vs.create_branch("feature", "main").unwrap();

        // Modify on feature branch
        let mut c2 = HashMap::new();
        c2.insert(
            RowKey { table: "data".into(), pk: "1".into() },
            RowChange::Update {
                old: make_row(&[("value", "original")]),
                new: make_row(&[("value", "modified")]),
            },
        );
        c2.insert(
            RowKey { table: "data".into(), pk: "2".into() },
            RowChange::Insert(make_row(&[("value", "new_row")])),
        );
        vs.commit("feature", "modify on feature", c2).unwrap();

        // Main still has original
        let main_rows = vs.query_at("main", "data").unwrap();
        assert_eq!(main_rows.len(), 1);
        assert_eq!(main_rows[0].1["value"], "original");

        // Feature has modified
        let feature_rows = vs.query_at("feature", "data").unwrap();
        assert_eq!(feature_rows.len(), 2);

        // Merge feature into main
        vs.merge("feature", "main", "merge feature").unwrap();

        let merged = vs.query_at("main", "data").unwrap();
        assert_eq!(merged.len(), 2);
        let row1 = merged.iter().find(|(pk, _)| pk == "1").unwrap();
        assert_eq!(row1.1["value"], "modified");
    }

    #[test]
    fn diff_between_versions() {
        let mut vs = VersionStore::new();

        let mut c1 = HashMap::new();
        c1.insert(
            RowKey { table: "t".into(), pk: "1".into() },
            RowChange::Insert(make_row(&[("v", "a")])),
        );
        c1.insert(
            RowKey { table: "t".into(), pk: "2".into() },
            RowChange::Insert(make_row(&[("v", "b")])),
        );
        let commit1 = vs.commit("main", "c1", c1).unwrap();

        let mut c2 = HashMap::new();
        c2.insert(
            RowKey { table: "t".into(), pk: "2".into() },
            RowChange::Update {
                old: make_row(&[("v", "b")]),
                new: make_row(&[("v", "b2")]),
            },
        );
        c2.insert(
            RowKey { table: "t".into(), pk: "3".into() },
            RowChange::Insert(make_row(&[("v", "c")])),
        );
        c2.insert(
            RowKey { table: "t".into(), pk: "1".into() },
            RowChange::Delete(make_row(&[("v", "a")])),
        );
        vs.commit("main", "c2", c2).unwrap();

        let diff = vs.diff(&commit1.to_string(), "main").unwrap();
        assert_eq!(diff.inserted.len(), 1); // pk=3
        assert_eq!(diff.updated.len(), 1); // pk=2
        assert_eq!(diff.deleted.len(), 1); // pk=1
    }

    #[test]
    fn commit_log() {
        let mut vs = VersionStore::new();
        vs.commit("main", "first", HashMap::new()).unwrap();
        vs.commit("main", "second", HashMap::new()).unwrap();

        let log = vs.log("main").unwrap();
        assert_eq!(log.len(), 3); // initial + first + second
        assert_eq!(log[0].message, "second");
        assert_eq!(log[1].message, "first");
        assert_eq!(log[2].message, "initial commit");
    }

    #[test]
    fn branch_management() {
        let mut vs = VersionStore::new();
        vs.create_branch("dev", "main").unwrap();
        vs.create_branch("staging", "main").unwrap();

        let branches = vs.list_branches();
        assert_eq!(branches.len(), 3); // main, dev, staging

        assert!(vs.delete_branch("dev").is_ok());
        assert!(vs.delete_branch("main").is_err()); // Can't delete main
    }

    // ========================================================================
    // Temporal / AS-OF join tests
    // ========================================================================

    #[test]
    fn temporal_table_basic() {
        let mut tt = TemporalTable::new("prices");
        let key = RowKey { table: "prices".into(), pk: "AAPL".into() };

        // t=100: price = 150
        tt.insert(key.clone(), make_row(&[("price", "150")]), 100);
        // t=200: price = 160
        tt.insert(key.clone(), make_row(&[("price", "160")]), 200);
        // t=300: price = 155
        tt.insert(key.clone(), make_row(&[("price", "155")]), 300);

        // Before any version exists
        assert!(tt.as_of(&key, 50).is_none());

        // Exactly at t=100
        assert_eq!(tt.as_of(&key, 100).unwrap()["price"], "150");

        // Between t=100 and t=200
        assert_eq!(tt.as_of(&key, 150).unwrap()["price"], "150");

        // Exactly at t=200
        assert_eq!(tt.as_of(&key, 200).unwrap()["price"], "160");

        // Between t=200 and t=300
        assert_eq!(tt.as_of(&key, 250).unwrap()["price"], "160");

        // At t=300 (current version)
        assert_eq!(tt.as_of(&key, 300).unwrap()["price"], "155");

        // Well into the future — current version is still valid
        assert_eq!(tt.as_of(&key, 99999).unwrap()["price"], "155");
    }

    #[test]
    fn temporal_table_history() {
        let mut tt = TemporalTable::new("accounts");
        let key = RowKey { table: "accounts".into(), pk: "acct_1".into() };

        tt.insert(key.clone(), make_row(&[("balance", "100")]), 10);
        tt.insert(key.clone(), make_row(&[("balance", "250")]), 20);
        tt.insert(key.clone(), make_row(&[("balance", "175")]), 30);

        let hist = tt.history(&key);
        assert_eq!(hist.len(), 3);

        // Ordered by valid_from
        assert_eq!(hist[0].valid_from, 10);
        assert_eq!(hist[0].data["balance"], "100");
        assert_eq!(hist[0].valid_to, Some(20));

        assert_eq!(hist[1].valid_from, 20);
        assert_eq!(hist[1].data["balance"], "250");
        assert_eq!(hist[1].valid_to, Some(30));

        assert_eq!(hist[2].valid_from, 30);
        assert_eq!(hist[2].data["balance"], "175");
        assert_eq!(hist[2].valid_to, None); // current

        // History for a non-existent key is empty
        let other = RowKey { table: "accounts".into(), pk: "nope".into() };
        assert!(tt.history(&other).is_empty());
    }

    #[test]
    fn as_of_join_basic() {
        let mut tt = TemporalTable::new("features");
        let key_a = RowKey { table: "features".into(), pk: "user_1".into() };
        let key_b = RowKey { table: "features".into(), pk: "user_2".into() };

        // user_1 feature versions
        tt.insert(key_a.clone(), make_row(&[("score", "0.5")]), 100);
        tt.insert(key_a.clone(), make_row(&[("score", "0.8")]), 200);

        // user_2 feature versions
        tt.insert(key_b.clone(), make_row(&[("score", "0.3")]), 150);

        let events = vec![
            (120, key_a.clone()), // should match user_1 @ score=0.5
            (180, key_b.clone()), // should match user_2 @ score=0.3
            (250, key_a.clone()), // should match user_1 @ score=0.8
        ];

        let joined = as_of_join(&events, &tt);
        assert_eq!(joined.len(), 3);

        assert_eq!(joined[0].0, 120);
        assert_eq!(joined[0].1["score"], "0.5");

        assert_eq!(joined[1].0, 180);
        assert_eq!(joined[1].1["score"], "0.3");

        assert_eq!(joined[2].0, 250);
        assert_eq!(joined[2].1["score"], "0.8");
    }

    #[test]
    fn as_of_join_no_future_leak() {
        // This is the critical ML correctness test: an event at t=150 must
        // NOT see data that was only inserted at t=200.
        let mut tt = TemporalTable::new("features");
        let key = RowKey { table: "features".into(), pk: "item_1".into() };

        // Version 1 at t=100
        tt.insert(key.clone(), make_row(&[("flag", "old")]), 100);
        // Version 2 at t=200 (future relative to the event below)
        tt.insert(key.clone(), make_row(&[("flag", "new")]), 200);

        let events = vec![
            (150, key.clone()), // between v1 and v2 — must see "old"
            (50, key.clone()),  // before any version — must NOT match
        ];

        let joined = as_of_join(&events, &tt);

        // Only the t=150 event should match (t=50 has no data yet).
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].0, 150);
        assert_eq!(joined[0].1["flag"], "old"); // NOT "new"
    }
}
