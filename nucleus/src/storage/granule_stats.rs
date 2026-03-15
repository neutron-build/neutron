//! Zone map statistics and granule-level pruning for analytics.
//!
//! Tracks min/max values per column per 8K-row granule to enable
//! skip decisions on filtered scans. Expected speedup: 5-10x on selective queries,
//! skip 50-90% of granules.
//!
//! Key reference: ClickHouse zone maps achieve 12-23x speedup on BETWEEN/WHERE queries.
//! Our implementation aims for 10-50x via granule-aware filtering.

use std::collections::HashMap;
use std::sync::Arc;

use crate::types::{Row, Value};

/// Statistics for a single column within a granule.
/// Tracks min/max values and NULL count for pruning decisions.
#[derive(Clone, Debug)]
pub struct ColumnStats {
    /// Minimum value in the column (NULL if all rows are NULL).
    pub min_value: Value,
    /// Maximum value in the column (NULL if all rows are NULL).
    pub max_value: Value,
    /// Count of NULL values in this column.
    pub null_count: u32,
    /// Total values in column (including NULLs).
    pub total_count: u32,
}

impl ColumnStats {
    /// Create new column stats from initial value.
    fn new(value: Value) -> Self {
        let is_null = value == Value::Null;
        Self {
            min_value: value.clone(),
            max_value: value,
            null_count: if is_null { 1 } else { 0 },
            total_count: 1,
        }
    }

    /// Update stats with a new value.
    fn update(&mut self, value: &Value) {
        if *value == Value::Null {
            self.null_count += 1;
        } else {
            if value < &self.min_value || self.min_value == Value::Null {
                self.min_value = value.clone();
            }
            if value > &self.max_value || self.max_value == Value::Null {
                self.max_value = value.clone();
            }
        }
        self.total_count += 1;
    }

    /// Check if a value is potentially within this column's range.
    fn contains(&self, value: &Value) -> bool {
        if *value == Value::Null {
            self.null_count > 0
        } else {
            value >= &self.min_value && value <= &self.max_value
        }
    }

    /// Check if a range [min, max] overlaps with this column's range.
    fn overlaps_range(&self, min: &Value, max: &Value) -> bool {
        // Range [min, max] overlaps with [self.min, self.max] if
        // max >= self.min AND min <= self.max.
        if *min == Value::Null || *max == Value::Null {
            // NULL range overlaps if there are NULLs
            return self.null_count > 0;
        }
        max >= &self.min_value && min <= &self.max_value
    }
}

/// Statistics for a single 8K-row granule across all indexed columns.
/// Used for zone map filtering in sequential scans.
#[derive(Clone, Debug)]
pub struct GranuleStats {
    /// Table ID for this granule.
    pub table_id: u64,
    /// Granule ID (typically row_num / 8192).
    pub granule_id: u32,
    /// Number of rows in this granule.
    pub row_count: u32,
    /// Column-specific statistics, indexed by column_id.
    pub column_stats: HashMap<u32, ColumnStats>,
}

impl GranuleStats {
    /// Create a new empty granule.
    pub fn new(table_id: u64, granule_id: u32) -> Self {
        Self {
            table_id,
            granule_id,
            row_count: 0,
            column_stats: HashMap::new(),
        }
    }

    /// Update granule stats with a new row.
    pub fn add_row(&mut self, row: &Row, column_ids: &[u32]) {
        self.row_count += 1;
        for (col_idx, col_id) in column_ids.iter().enumerate() {
            if col_idx < row.len() {
                let value = &row[col_idx];
                self.column_stats
                    .entry(*col_id)
                    .and_modify(|stats| stats.update(value))
                    .or_insert_with(|| ColumnStats::new(value.clone()));
            }
        }
    }

    /// Merge another granule's stats into this one (for batch updates).
    pub fn merge(&mut self, other: &GranuleStats) {
        self.row_count += other.row_count;
        for (col_id, other_stats) in &other.column_stats {
            self.column_stats
                .entry(*col_id)
                .and_modify(|stats| {
                    // Update min/max with the other granule's extremes
                    if other_stats.min_value != Value::Null
                        && (stats.min_value == Value::Null
                            || other_stats.min_value < stats.min_value)
                        {
                            stats.min_value = other_stats.min_value.clone();
                        }
                    if other_stats.max_value != Value::Null
                        && (stats.max_value == Value::Null
                            || other_stats.max_value > stats.max_value)
                        {
                            stats.max_value = other_stats.max_value.clone();
                        }
                    stats.null_count += other_stats.null_count;
                    stats.total_count += other_stats.total_count;
                })
                .or_insert_with(|| other_stats.clone());
        }
    }
}

/// Zone map index: HashMap<table_id, Vec<GranuleStats>>.
/// Thread-safe wrapper for zone map storage across all tables.
pub struct ZoneMapIndex {
    /// Maps table_id → vector of granule statistics.
    /// Typically indexed by granule_id = row_index / 8192.
    stats_by_table: Arc<parking_lot::RwLock<HashMap<u64, Vec<GranuleStats>>>>,
}

impl ZoneMapIndex {
    /// Create a new zone map index.
    pub fn new() -> Self {
        Self {
            stats_by_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    /// Register or update granule stats for a table.
    pub fn update_granule(
        &self,
        table_id: u64,
        granule_id: u32,
        stats: GranuleStats,
    ) {
        let mut map = self.stats_by_table.write();
        let granules = map.entry(table_id).or_default();

        // Expand vector if necessary
        let granule_idx = granule_id as usize;
        if granule_idx >= granules.len() {
            granules.resize_with(granule_idx + 1, || {
                GranuleStats::new(table_id, granule_idx as u32)
            });
        }

        granules[granule_idx] = stats;
    }

    /// Get stats for a specific granule.
    pub fn get_granule(&self, table_id: u64, granule_id: u32) -> Option<GranuleStats> {
        let map = self.stats_by_table.read();
        map.get(&table_id)
            .and_then(|granules| granules.get(granule_id as usize))
            .cloned()
    }

    /// Get all granules for a table.
    pub fn get_table_granules(&self, table_id: u64) -> Vec<GranuleStats> {
        let map = self.stats_by_table.read();
        map.get(&table_id).cloned()
            .unwrap_or_default()
    }

    /// Clear all stats for a table (e.g., on DROP TABLE or VACUUM).
    pub fn clear_table(&self, table_id: u64) {
        let mut map = self.stats_by_table.write();
        map.remove(&table_id);
    }

    /// Clear all stats (used in tests).
    #[cfg(test)]
    pub fn clear_all(&self) {
        self.stats_by_table.write().clear();
    }
}

impl Clone for ZoneMapIndex {
    fn clone(&self) -> Self {
        Self {
            stats_by_table: Arc::clone(&self.stats_by_table),
        }
    }
}

impl Default for ZoneMapIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute zone map stats for a batch of rows.
///
/// # Arguments
/// * `rows` - Slice of rows to analyze
/// * `column_ids` - Column indices to track
/// * `table_id` - Table identifier
/// * `granule_id` - Granule identifier (typically row_index / 8192)
///
/// # Returns
/// GranuleStats with min/max/null counts for each column.
pub fn compute_granule_stats(
    rows: &[Row],
    column_ids: &[u32],
    table_id: u64,
    granule_id: u32,
) -> GranuleStats {
    let mut stats = GranuleStats::new(table_id, granule_id);
    for row in rows {
        stats.add_row(row, column_ids);
    }
    stats
}

/// Filter decision: can we skip this granule based on a WHERE filter?
///
/// Returns `true` if the granule can be safely skipped (i.e., no rows
/// in the granule can satisfy the filter). Returns `false` if the
/// granule might contain matching rows (conservative approach).
///
/// Handles filter expressions: col > X, col = X, col IN (X, Y, Z), col BETWEEN X AND Y.
pub fn can_skip_granule(granule: &GranuleStats, col_id: u32, filter: &FilterPredicate) -> bool {
    let stats = match granule.column_stats.get(&col_id) {
        Some(s) => s,
        None => return false, // No stats for this column: don't skip.
    };

    use FilterPredicate::*;
    match filter {
        // col = X: skip if X is outside [min, max]
        Equal(val) => !stats.contains(val),

        // col > X: skip if max <= X
        GreaterThan(val) => {
            if stats.max_value == Value::Null {
                true // All NULLs, no row > X
            } else {
                stats.max_value <= *val
            }
        }

        // col >= X: skip if max < X
        GreaterThanOrEqual(val) => {
            if stats.max_value == Value::Null {
                true
            } else {
                stats.max_value < *val
            }
        }

        // col < X: skip if min >= X
        LessThan(val) => {
            if stats.min_value == Value::Null {
                true
            } else {
                stats.min_value >= *val
            }
        }

        // col <= X: skip if min > X
        LessThanOrEqual(val) => {
            if stats.min_value == Value::Null {
                true
            } else {
                stats.min_value > *val
            }
        }

        // col IN (X, Y, Z): skip if none of X, Y, Z are in [min, max]
        In(values) => {
            !values.iter().any(|v| {
                if *v == Value::Null {
                    stats.null_count > 0
                } else {
                    stats.contains(v)
                }
            })
        }

        // col BETWEEN X AND Y: skip if no overlap
        Between { min, max } => !stats.overlaps_range(min, max),

        // col IS NULL: skip if no NULLs in granule
        IsNull => stats.null_count == 0,

        // col IS NOT NULL: skip if all rows are NULL
        IsNotNull => stats.null_count == stats.total_count,

        // col LIKE pattern: no zone map optimization (too complex)
        Like { .. } => false, // Don't skip: conservative
    }
}

/// Filter predicate for zone map evaluation.
#[derive(Clone, Debug)]
pub enum FilterPredicate {
    Equal(Value),
    GreaterThan(Value),
    GreaterThanOrEqual(Value),
    LessThan(Value),
    LessThanOrEqual(Value),
    In(Vec<Value>),
    Between { min: Value, max: Value },
    IsNull,
    IsNotNull,
    Like { pattern: String },
}

/// Apply zone map filtering to a set of granules.
///
/// Returns a boolean vector: `result[i] = true` if granule `i` should be scanned,
/// `false` if it should be skipped.
pub fn apply_zone_map_filter(
    granules: &[GranuleStats],
    col_id: u32,
    filter: &FilterPredicate,
) -> Vec<bool> {
    granules
        .iter()
        .map(|g| !can_skip_granule(g, col_id, filter))
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn row(vals: &[Value]) -> Row {
        vals.to_vec()
    }

    #[test]
    fn column_stats_creation() {
        let stats = ColumnStats::new(Value::Int64(42));
        assert_eq!(stats.min_value, Value::Int64(42));
        assert_eq!(stats.max_value, Value::Int64(42));
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.total_count, 1);
    }

    #[test]
    fn column_stats_null() {
        let stats = ColumnStats::new(Value::Null);
        assert_eq!(stats.min_value, Value::Null);
        assert_eq!(stats.max_value, Value::Null);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.total_count, 1);
    }

    #[test]
    fn column_stats_update() {
        let mut stats = ColumnStats::new(Value::Int64(10));
        stats.update(&Value::Int64(20));
        stats.update(&Value::Int64(5));
        stats.update(&Value::Null);

        assert_eq!(stats.min_value, Value::Int64(5));
        assert_eq!(stats.max_value, Value::Int64(20));
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.total_count, 4);
    }

    #[test]
    fn column_stats_contains() {
        let mut stats = ColumnStats::new(Value::Int64(10));
        stats.update(&Value::Int64(20));
        stats.update(&Value::Int64(30));

        assert!(stats.contains(&Value::Int64(15)));
        assert!(stats.contains(&Value::Int64(10)));
        assert!(stats.contains(&Value::Int64(30)));
        assert!(!stats.contains(&Value::Int64(5)));
        assert!(!stats.contains(&Value::Int64(35)));
        assert!(!stats.contains(&Value::Null)); // No NULLs in stats
    }

    #[test]
    fn column_stats_overlaps_range() {
        let mut stats = ColumnStats::new(Value::Int64(10));
        stats.update(&Value::Int64(20));

        // [5, 15] overlaps [10, 20]
        assert!(stats.overlaps_range(&Value::Int64(5), &Value::Int64(15)));

        // [15, 25] overlaps [10, 20]
        assert!(stats.overlaps_range(&Value::Int64(15), &Value::Int64(25)));

        // [10, 20] overlaps [10, 20]
        assert!(stats.overlaps_range(&Value::Int64(10), &Value::Int64(20)));

        // [25, 30] doesn't overlap [10, 20]
        assert!(!stats.overlaps_range(&Value::Int64(25), &Value::Int64(30)));

        // [0, 5] doesn't overlap [10, 20]
        assert!(!stats.overlaps_range(&Value::Int64(0), &Value::Int64(5)));
    }

    #[test]
    fn granule_stats_creation() {
        let granule = GranuleStats::new(1, 0);
        assert_eq!(granule.table_id, 1);
        assert_eq!(granule.granule_id, 0);
        assert_eq!(granule.row_count, 0);
        assert!(granule.column_stats.is_empty());
    }

    #[test]
    fn granule_stats_add_row() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Int64(1), Value::Text("a".into())]), &[0, 1]);
        granule.add_row(&row(&[Value::Int64(2), Value::Text("b".into())]), &[0, 1]);

        assert_eq!(granule.row_count, 2);
        assert_eq!(granule.column_stats.len(), 2);

        let col0 = &granule.column_stats[&0];
        assert_eq!(col0.min_value, Value::Int64(1));
        assert_eq!(col0.max_value, Value::Int64(2));
        assert_eq!(col0.null_count, 0);

        let col1 = &granule.column_stats[&1];
        assert_eq!(col1.min_value, Value::Text("a".into()));
        assert_eq!(col1.max_value, Value::Text("b".into()));
    }

    #[test]
    fn granule_stats_merge() {
        let mut g1 = GranuleStats::new(1, 0);
        g1.add_row(&row(&[Value::Int64(10)]), &[0]);
        g1.add_row(&row(&[Value::Int64(20)]), &[0]);

        let mut g2 = GranuleStats::new(1, 1);
        g2.add_row(&row(&[Value::Int64(5)]), &[0]);
        g2.add_row(&row(&[Value::Int64(30)]), &[0]);

        g1.merge(&g2);

        assert_eq!(g1.row_count, 4);
        let col0 = &g1.column_stats[&0];
        assert_eq!(col0.min_value, Value::Int64(5));
        assert_eq!(col0.max_value, Value::Int64(30));
    }

    #[test]
    fn can_skip_equal() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Int64(10), Value::Int64(20)]), &[0]);
        granule.add_row(&row(&[Value::Int64(15), Value::Int64(25)]), &[0]);

        // Value 12 is in [10, 15]: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::Equal(Value::Int64(12))
        ));

        // Value 20 is outside [10, 15]: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::Equal(Value::Int64(20))
        ));
    }

    #[test]
    fn can_skip_greater_than() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Int64(10)]), &[0]);
        granule.add_row(&row(&[Value::Int64(20)]), &[0]);

        // max=20 > 15: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::GreaterThan(Value::Int64(15))
        ));

        // max=20 <= 20: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::GreaterThan(Value::Int64(20))
        ));

        // max=20 <= 100: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::GreaterThan(Value::Int64(100))
        ));
    }

    #[test]
    fn can_skip_less_than() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Int64(10)]), &[0]);
        granule.add_row(&row(&[Value::Int64(20)]), &[0]);

        // min=10 < 15: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::LessThan(Value::Int64(15))
        ));

        // min=10 >= 10: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::LessThan(Value::Int64(10))
        ));

        // min=10 >= 0: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::LessThan(Value::Int64(0))
        ));
    }

    #[test]
    fn can_skip_in() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Int64(10), Value::Int64(20)]), &[0]);
        granule.add_row(&row(&[Value::Int64(15), Value::Int64(25)]), &[0]);

        // 12, 13, 14 all in [10, 15]: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::In(vec![Value::Int64(12), Value::Int64(13)])
        ));

        // 50, 60, 70 all outside [10, 15]: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::In(vec![Value::Int64(50), Value::Int64(60)])
        ));

        // Mix: 5 is outside, 12 is inside: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::In(vec![Value::Int64(5), Value::Int64(12)])
        ));
    }

    #[test]
    fn can_skip_between() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Int64(10)]), &[0]);
        granule.add_row(&row(&[Value::Int64(20)]), &[0]);

        // BETWEEN 5 AND 15: overlaps [10, 20]: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::Between {
                min: Value::Int64(5),
                max: Value::Int64(15)
            }
        ));

        // BETWEEN 25 AND 30: no overlap: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::Between {
                min: Value::Int64(25),
                max: Value::Int64(30)
            }
        ));

        // BETWEEN 0 AND 5: no overlap: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::Between {
                min: Value::Int64(0),
                max: Value::Int64(5)
            }
        ));
    }

    #[test]
    fn can_skip_is_null() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Int64(10)]), &[0]);
        granule.add_row(&row(&[Value::Int64(20)]), &[0]);

        // No NULLs in granule: skip IS NULL
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::IsNull
        ));

        // Add a NULL
        granule.add_row(&row(&[Value::Null]), &[0]);

        // Now has NULLs: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::IsNull
        ));
    }

    #[test]
    fn can_skip_is_not_null() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Null]), &[0]);
        granule.add_row(&row(&[Value::Null]), &[0]);

        // All NULLs: skip IS NOT NULL
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::IsNotNull
        ));

        // Add a non-NULL
        granule.add_row(&row(&[Value::Int64(10)]), &[0]);

        // Has non-NULLs: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::IsNotNull
        ));
    }

    #[test]
    fn zone_map_index_operations() {
        let index = ZoneMapIndex::new();

        let mut g1 = GranuleStats::new(1, 0);
        g1.add_row(&row(&[Value::Int64(10)]), &[0]);

        index.update_granule(1, 0, g1);

        let retrieved = index.get_granule(1, 0).unwrap();
        assert_eq!(retrieved.granule_id, 0);
        assert_eq!(retrieved.row_count, 1);
    }

    #[test]
    fn zone_map_index_get_table_granules() {
        let index = ZoneMapIndex::new();

        let mut g1 = GranuleStats::new(1, 0);
        g1.add_row(&row(&[Value::Int64(10)]), &[0]);

        let mut g2 = GranuleStats::new(1, 1);
        g2.add_row(&row(&[Value::Int64(20)]), &[0]);

        index.update_granule(1, 0, g1);
        index.update_granule(1, 1, g2);

        let granules = index.get_table_granules(1);
        assert_eq!(granules.len(), 2);
        assert_eq!(granules[0].granule_id, 0);
        assert_eq!(granules[1].granule_id, 1);
    }

    #[test]
    fn zone_map_index_clear_table() {
        let index = ZoneMapIndex::new();

        let mut g1 = GranuleStats::new(1, 0);
        g1.add_row(&row(&[Value::Int64(10)]), &[0]);

        index.update_granule(1, 0, g1);
        assert!(index.get_granule(1, 0).is_some());

        index.clear_table(1);
        assert!(index.get_granule(1, 0).is_none());
    }

    #[test]
    fn apply_zone_map_filter_range() {
        let mut g1 = GranuleStats::new(1, 0);
        g1.add_row(&row(&[Value::Int64(10)]), &[0]);
        g1.add_row(&row(&[Value::Int64(20)]), &[0]);

        let mut g2 = GranuleStats::new(1, 1);
        g2.add_row(&row(&[Value::Int64(50)]), &[0]);
        g2.add_row(&row(&[Value::Int64(60)]), &[0]);

        let granules = vec![g1, g2];

        // BETWEEN 15 AND 55: g1 overlaps, g2 overlaps
        let result = apply_zone_map_filter(
            &granules,
            0,
            &FilterPredicate::Between {
                min: Value::Int64(15),
                max: Value::Int64(55),
            },
        );
        assert_eq!(result, vec![true, true]);

        // BETWEEN 25 AND 55: g1 doesn't overlap (25 > max 20), g2 overlaps (55 >= 50 && 25 <= 60)
        let result = apply_zone_map_filter(
            &granules,
            0,
            &FilterPredicate::Between {
                min: Value::Int64(25),
                max: Value::Int64(55),
            },
        );
        assert_eq!(result, vec![false, true]);

        // BETWEEN 70 AND 80: neither overlaps
        let result = apply_zone_map_filter(
            &granules,
            0,
            &FilterPredicate::Between {
                min: Value::Int64(70),
                max: Value::Int64(80),
            },
        );
        assert_eq!(result, vec![false, false]);
    }

    #[test]
    fn apply_zone_map_filter_in() {
        let mut g1 = GranuleStats::new(1, 0);
        g1.add_row(&row(&[Value::Int64(10)]), &[0]);
        g1.add_row(&row(&[Value::Int64(20)]), &[0]);

        let mut g2 = GranuleStats::new(1, 1);
        g2.add_row(&row(&[Value::Int64(50)]), &[0]);
        g2.add_row(&row(&[Value::Int64(60)]), &[0]);

        let granules = vec![g1, g2];

        // IN (5, 12, 15): 12, 15 in g1 range: g1 scanned, g2 skipped
        let result = apply_zone_map_filter(
            &granules,
            0,
            &FilterPredicate::In(vec![
                Value::Int64(5),
                Value::Int64(12),
                Value::Int64(15),
            ]),
        );
        assert_eq!(result, vec![true, false]);

        // IN (50, 60): in g2 range: g1 skipped, g2 scanned
        let result = apply_zone_map_filter(
            &granules,
            0,
            &FilterPredicate::In(vec![Value::Int64(50), Value::Int64(60)]),
        );
        assert_eq!(result, vec![false, true]);
    }

    #[test]
    fn compute_granule_stats_batch() {
        let rows = vec![
            row(&[Value::Int64(1), Value::Text("a".into())]),
            row(&[Value::Int64(2), Value::Text("b".into())]),
            row(&[Value::Int64(3), Value::Text("c".into())]),
        ];

        let stats = compute_granule_stats(&rows, &[0, 1], 1, 0);

        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.table_id, 1);
        assert_eq!(stats.granule_id, 0);

        let col0 = &stats.column_stats[&0];
        assert_eq!(col0.min_value, Value::Int64(1));
        assert_eq!(col0.max_value, Value::Int64(3));

        let col1 = &stats.column_stats[&1];
        assert_eq!(col1.min_value, Value::Text("a".into()));
        assert_eq!(col1.max_value, Value::Text("c".into()));
    }

    #[test]
    fn granule_stats_with_mixed_nulls() {
        let rows = vec![
            row(&[Value::Int64(10)]),
            row(&[Value::Null]),
            row(&[Value::Int64(20)]),
            row(&[Value::Null]),
        ];

        let stats = compute_granule_stats(&rows, &[0], 1, 0);

        let col0 = &stats.column_stats[&0];
        assert_eq!(col0.min_value, Value::Int64(10));
        assert_eq!(col0.max_value, Value::Int64(20));
        assert_eq!(col0.null_count, 2);
        assert_eq!(col0.total_count, 4);
    }

    #[test]
    fn can_skip_granule_with_text_column() {
        let mut granule = GranuleStats::new(1, 0);
        granule.add_row(&row(&[Value::Text("apple".into())]), &[0]);
        granule.add_row(&row(&[Value::Text("zebra".into())]), &[0]);

        // "banana" is in [apple, zebra]: don't skip
        assert!(!can_skip_granule(
            &granule,
            0,
            &FilterPredicate::Equal(Value::Text("banana".into()))
        ));

        // "zulu" is outside [apple, zebra]: skip
        assert!(can_skip_granule(
            &granule,
            0,
            &FilterPredicate::Equal(Value::Text("zulu".into()))
        ));
    }
}
