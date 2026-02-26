//! Auto-indexing advisor and query energy metrics.
//!
//! Supports:
//!   - Track query patterns and frequency
//!   - Recommend indexes based on observed workload
//!   - Estimate query energy cost (per-query carbon metrics)
//!   - Index usage statistics
//!
//! Replaces manual index tuning and provides sustainability metrics.

use std::collections::HashMap;

// ============================================================================
// Query pattern tracking
// ============================================================================

/// A normalized query pattern (parameterized form).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryPattern {
    pub table: String,
    pub columns_in_where: Vec<String>,
    pub columns_in_order_by: Vec<String>,
    pub columns_in_group_by: Vec<String>,
    pub has_join: bool,
}

/// Statistics about a query pattern.
#[derive(Debug, Clone)]
pub struct PatternStats {
    pub pattern: QueryPattern,
    pub execution_count: u64,
    pub total_rows_scanned: u64,
    pub total_duration_us: u64,
    pub avg_rows_scanned: f64,
    pub avg_duration_us: f64,
    pub last_seen: u64,
}

/// An index recommendation.
#[derive(Debug, Clone)]
pub struct IndexRecommendation {
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: RecommendedIndexType,
    pub estimated_speedup: f64,
    pub reason: String,
    pub priority: RecommendationPriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecommendedIndexType {
    BTree,
    Hash,
    Gin,
    Gist,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecommendationPriority {
    Low,
    Medium,
    High,
    Critical,
}

/// Query workload advisor — observes patterns and recommends indexes.
pub struct IndexAdvisor {
    patterns: HashMap<QueryPattern, PatternStats>,
    /// Existing indexes: (table, columns)
    existing_indexes: Vec<(String, Vec<String>)>,
    /// Minimum executions before recommending an index.
    min_executions: u64,
    /// Minimum avg rows scanned to trigger recommendation.
    min_rows_threshold: f64,
}

impl Default for IndexAdvisor {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexAdvisor {
    pub fn new() -> Self {
        Self {
            patterns: HashMap::new(),
            existing_indexes: Vec::new(),
            min_executions: 10,
            min_rows_threshold: 100.0,
        }
    }

    /// Record a query execution.
    pub fn record_query(
        &mut self,
        pattern: QueryPattern,
        rows_scanned: u64,
        duration_us: u64,
    ) {
        let ts = now_ms();
        let stats = self.patterns.entry(pattern.clone()).or_insert_with(|| PatternStats {
            pattern,
            execution_count: 0,
            total_rows_scanned: 0,
            total_duration_us: 0,
            avg_rows_scanned: 0.0,
            avg_duration_us: 0.0,
            last_seen: 0,
        });

        stats.execution_count += 1;
        stats.total_rows_scanned += rows_scanned;
        stats.total_duration_us += duration_us;
        stats.avg_rows_scanned = stats.total_rows_scanned as f64 / stats.execution_count as f64;
        stats.avg_duration_us = stats.total_duration_us as f64 / stats.execution_count as f64;
        stats.last_seen = ts;
    }

    /// Register an existing index.
    pub fn register_index(&mut self, table: &str, columns: Vec<String>) {
        self.existing_indexes
            .push((table.to_string(), columns));
    }

    /// Check if an index already exists for a table and columns.
    fn has_index(&self, table: &str, columns: &[String]) -> bool {
        self.existing_indexes.iter().any(|(t, cols)| {
            t == table && cols.len() >= columns.len() && columns.iter().zip(cols).all(|(a, b)| a == b)
        })
    }

    /// Generate index recommendations based on observed workload.
    pub fn recommend(&self) -> Vec<IndexRecommendation> {
        let mut recommendations = Vec::new();

        for stats in self.patterns.values() {
            if stats.execution_count < self.min_executions {
                continue;
            }
            if stats.avg_rows_scanned < self.min_rows_threshold {
                continue;
            }

            let pattern = &stats.pattern;

            // Recommend index on WHERE columns
            if !pattern.columns_in_where.is_empty()
                && !self.has_index(&pattern.table, &pattern.columns_in_where)
            {
                let priority = if stats.execution_count > 1000 && stats.avg_rows_scanned > 10000.0 {
                    RecommendationPriority::Critical
                } else if stats.execution_count > 100 {
                    RecommendationPriority::High
                } else {
                    RecommendationPriority::Medium
                };

                let estimated_speedup = stats.avg_rows_scanned / 10.0; // Rough estimate

                recommendations.push(IndexRecommendation {
                    table: pattern.table.clone(),
                    columns: pattern.columns_in_where.clone(),
                    index_type: RecommendedIndexType::BTree,
                    estimated_speedup,
                    reason: format!(
                        "Query executed {} times, scanning avg {:.0} rows. Index would reduce to ~10 rows.",
                        stats.execution_count, stats.avg_rows_scanned
                    ),
                    priority,
                });
            }

            // Recommend covering index for ORDER BY
            if !pattern.columns_in_order_by.is_empty() {
                let mut combined = pattern.columns_in_where.clone();
                for col in &pattern.columns_in_order_by {
                    if !combined.contains(col) {
                        combined.push(col.clone());
                    }
                }
                if !self.has_index(&pattern.table, &combined) {
                    recommendations.push(IndexRecommendation {
                        table: pattern.table.clone(),
                        columns: combined,
                        index_type: RecommendedIndexType::BTree,
                        estimated_speedup: 2.0,
                        reason: "Covering index for ORDER BY avoids sort operation".into(),
                        priority: RecommendationPriority::Low,
                    });
                }
            }
        }

        recommendations.sort_by(|a, b| b.priority.cmp(&a.priority));
        recommendations
    }

    /// Get all tracked patterns.
    pub fn patterns(&self) -> Vec<&PatternStats> {
        self.patterns.values().collect()
    }

    /// Get the top N most frequent patterns.
    pub fn top_patterns(&self, n: usize) -> Vec<&PatternStats> {
        let mut patterns: Vec<&PatternStats> = self.patterns.values().collect();
        patterns.sort_by(|a, b| b.execution_count.cmp(&a.execution_count));
        patterns.truncate(n);
        patterns
    }
}

// ============================================================================
// Query energy metrics
// ============================================================================

/// Estimated energy cost of a query.
#[derive(Debug, Clone)]
pub struct EnergyCost {
    /// Estimated CPU energy in microjoules.
    pub cpu_uj: f64,
    /// Estimated disk I/O energy in microjoules.
    pub io_uj: f64,
    /// Estimated network energy in microjoules (for distributed queries).
    pub network_uj: f64,
    /// Total estimated energy in microjoules.
    pub total_uj: f64,
    /// Estimated CO2 in micrograms (depends on grid carbon intensity).
    pub co2_ug: f64,
}

/// Energy cost estimator for queries.
pub struct EnergyEstimator {
    /// CPU energy per row processed (microjoules).
    pub cpu_uj_per_row: f64,
    /// Disk I/O energy per page read (microjoules).
    pub io_uj_per_page: f64,
    /// Network energy per kilobyte (microjoules).
    pub net_uj_per_kb: f64,
    /// Carbon intensity (grams CO2 per kWh). US average ~400, France ~50.
    pub carbon_intensity_g_per_kwh: f64,
}

impl Default for EnergyEstimator {
    fn default() -> Self {
        Self::new()
    }
}

impl EnergyEstimator {
    /// Default estimator with typical x86 server values.
    pub fn new() -> Self {
        Self {
            cpu_uj_per_row: 0.5,     // ~0.5 microjoules per row
            io_uj_per_page: 10.0,    // ~10 microjoules per 8KB page read (NVMe)
            net_uj_per_kb: 5.0,      // ~5 microjoules per KB network
            carbon_intensity_g_per_kwh: 400.0, // US average
        }
    }

    /// Estimate energy cost for a query.
    pub fn estimate(
        &self,
        rows_processed: u64,
        pages_read: u64,
        network_kb: u64,
    ) -> EnergyCost {
        let cpu = self.cpu_uj_per_row * rows_processed as f64;
        let io = self.io_uj_per_page * pages_read as f64;
        let net = self.net_uj_per_kb * network_kb as f64;
        let total = cpu + io + net;

        // Convert microjoules to kWh: 1 kWh = 3.6e12 microjoules
        let kwh = total / 3.6e12;
        let co2_g = kwh * self.carbon_intensity_g_per_kwh;
        let co2_ug = co2_g * 1e6;

        EnergyCost {
            cpu_uj: cpu,
            io_uj: io,
            network_uj: net,
            total_uj: total,
            co2_ug,
        }
    }

    /// Format energy cost as human-readable string.
    pub fn format_cost(cost: &EnergyCost) -> String {
        if cost.total_uj < 1000.0 {
            format!("{:.1} µJ ({:.3} µg CO₂)", cost.total_uj, cost.co2_ug)
        } else if cost.total_uj < 1_000_000.0 {
            format!("{:.1} mJ ({:.3} µg CO₂)", cost.total_uj / 1000.0, cost.co2_ug)
        } else {
            format!("{:.3} J ({:.3} mg CO₂)", cost.total_uj / 1_000_000.0, cost.co2_ug / 1000.0)
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
// Auto-indexing execution (checklist 7.6)
// ============================================================================

/// Configuration for automatic index management.
#[derive(Debug, Clone)]
pub struct AutoIndexConfig {
    pub enabled: bool,
    /// Minimum estimated speedup to auto-create an index.
    pub creation_threshold: f64,
    /// Drop indexes unused for this many milliseconds.
    pub drop_unused_after_ms: u64,
    /// Maximum number of auto-created indexes.
    pub max_auto_indexes: usize,
}

impl Default for AutoIndexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            creation_threshold: 50.0,
            drop_unused_after_ms: 5000,
            max_auto_indexes: 64,
        }
    }
}

/// An auto-created index.
#[derive(Debug, Clone)]
pub struct AutoIndex {
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: RecommendedIndexType,
    pub created_at_ms: u64,
    pub last_used_at_ms: u64,
    pub query_count: u64,
}

/// Actions taken by the auto-indexer.
#[derive(Debug, Clone)]
pub enum AutoIndexAction {
    Created { table: String, columns: Vec<String>, timestamp_ms: u64 },
    Dropped { table: String, columns: Vec<String>, reason: String, timestamp_ms: u64 },
    Skipped { table: String, columns: Vec<String>, reason: String },
}

/// Manages automatic index creation and retirement.
pub struct AutoIndexer {
    config: AutoIndexConfig,
    auto_indexes: Vec<AutoIndex>,
    actions_log: Vec<AutoIndexAction>,
}

impl AutoIndexer {
    pub fn new(config: AutoIndexConfig) -> Self {
        Self { config, auto_indexes: Vec::new(), actions_log: Vec::new() }
    }

    /// Evaluate recommendations and auto-create indexes that meet the threshold.
    pub fn evaluate_recommendations(
        &mut self,
        recs: &[IndexRecommendation],
        timestamp_ms: u64,
    ) -> Vec<AutoIndexAction> {
        let mut actions = Vec::new();
        if !self.config.enabled { return actions; }

        for rec in recs {
            let already_exists = self.auto_indexes.iter().any(|idx| {
                idx.table == rec.table && idx.columns == rec.columns
            });
            if already_exists { continue; }

            if rec.estimated_speedup < self.config.creation_threshold {
                let action = AutoIndexAction::Skipped {
                    table: rec.table.clone(), columns: rec.columns.clone(),
                    reason: format!("speedup {:.1} below threshold {:.0}", rec.estimated_speedup, self.config.creation_threshold),
                };
                actions.push(action.clone());
                self.actions_log.push(action);
                continue;
            }

            if self.auto_indexes.len() >= self.config.max_auto_indexes {
                let action = AutoIndexAction::Skipped {
                    table: rec.table.clone(), columns: rec.columns.clone(),
                    reason: format!("max auto-indexes limit ({}) reached", self.config.max_auto_indexes),
                };
                actions.push(action.clone());
                self.actions_log.push(action);
                continue;
            }

            self.auto_indexes.push(AutoIndex {
                table: rec.table.clone(), columns: rec.columns.clone(),
                index_type: rec.index_type, created_at_ms: timestamp_ms,
                last_used_at_ms: timestamp_ms, query_count: 0,
            });
            let action = AutoIndexAction::Created {
                table: rec.table.clone(), columns: rec.columns.clone(), timestamp_ms,
            };
            actions.push(action.clone());
            self.actions_log.push(action);
        }
        actions
    }

    /// Mark an auto-index as recently used.
    pub fn mark_index_used(&mut self, table: &str, columns: &[String], timestamp_ms: u64) {
        for idx in &mut self.auto_indexes {
            if idx.table == table && idx.columns == columns {
                idx.last_used_at_ms = timestamp_ms;
                idx.query_count += 1;
                return;
            }
        }
    }

    /// Sweep for unused auto-indexes older than the configured threshold.
    pub fn sweep_unused(&mut self, timestamp_ms: u64) -> Vec<AutoIndexAction> {
        let mut actions = Vec::new();
        let threshold_ms = self.config.drop_unused_after_ms;
        let mut kept = Vec::new();

        for idx in self.auto_indexes.drain(..) {
            let age_ms = timestamp_ms.saturating_sub(idx.last_used_at_ms);
            if age_ms >= threshold_ms {
                let action = AutoIndexAction::Dropped {
                    table: idx.table.clone(), columns: idx.columns.clone(),
                    reason: format!("unused for {age_ms}ms (threshold: {threshold_ms}ms)"),
                    timestamp_ms,
                };
                actions.push(action.clone());
                self.actions_log.push(action);
            } else {
                kept.push(idx);
            }
        }
        self.auto_indexes = kept;
        actions
    }

    pub fn auto_index_count(&self) -> usize { self.auto_indexes.len() }
    pub fn actions_log(&self) -> &[AutoIndexAction] { &self.actions_log }
    pub fn get_auto_indexes(&self) -> &[AutoIndex] { &self.auto_indexes }
    pub fn is_enabled(&self) -> bool { self.config.enabled }
    pub fn set_enabled(&mut self, enabled: bool) { self.config.enabled = enabled; }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_tracking() {
        let mut advisor = IndexAdvisor::new();

        let pattern = QueryPattern {
            table: "users".into(),
            columns_in_where: vec!["email".into()],
            columns_in_order_by: vec![],
            columns_in_group_by: vec![],
            has_join: false,
        };

        for _ in 0..20 {
            advisor.record_query(pattern.clone(), 50_000, 10_000);
        }

        let top = advisor.top_patterns(5);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].execution_count, 20);
        assert!((top[0].avg_rows_scanned - 50_000.0).abs() < 1e-6);
    }

    #[test]
    fn index_recommendation() {
        let mut advisor = IndexAdvisor::new();

        let pattern = QueryPattern {
            table: "orders".into(),
            columns_in_where: vec!["user_id".into(), "status".into()],
            columns_in_order_by: vec!["created_at".into()],
            columns_in_group_by: vec![],
            has_join: false,
        };

        // Record enough executions to trigger recommendation
        for _ in 0..50 {
            advisor.record_query(pattern.clone(), 10_000, 5_000);
        }

        let recs = advisor.recommend();
        assert!(!recs.is_empty());

        // Should recommend index on (user_id, status)
        let where_rec = recs.iter().find(|r| r.columns == vec!["user_id", "status"]);
        assert!(where_rec.is_some());
    }

    #[test]
    fn existing_index_suppresses_recommendation() {
        let mut advisor = IndexAdvisor::new();
        advisor.register_index("orders", vec!["user_id".into(), "status".into()]);

        let pattern = QueryPattern {
            table: "orders".into(),
            columns_in_where: vec!["user_id".into(), "status".into()],
            columns_in_order_by: vec![],
            columns_in_group_by: vec![],
            has_join: false,
        };

        for _ in 0..50 {
            advisor.record_query(pattern.clone(), 10_000, 5_000);
        }

        let recs = advisor.recommend();
        // No WHERE-column recommendation since index exists
        let where_rec = recs.iter().find(|r| r.columns == vec!["user_id", "status"]);
        assert!(where_rec.is_none());
    }

    #[test]
    fn energy_estimation() {
        let estimator = EnergyEstimator::new();
        let cost = estimator.estimate(100_000, 1_000, 0);

        assert!(cost.cpu_uj > 0.0);
        assert!(cost.io_uj > 0.0);
        assert_eq!(cost.network_uj, 0.0);
        assert!((cost.total_uj - (cost.cpu_uj + cost.io_uj + cost.network_uj)).abs() < 1e-6);
        assert!(cost.co2_ug > 0.0);
    }

    #[test]
    fn energy_format() {
        let estimator = EnergyEstimator::new();

        // Small query
        let small = estimator.estimate(100, 10, 0);
        let formatted = EnergyEstimator::format_cost(&small);
        assert!(formatted.contains("µJ"));

        // Large query
        let large = estimator.estimate(10_000_000, 100_000, 1000);
        let formatted = EnergyEstimator::format_cost(&large);
        assert!(formatted.contains("J") || formatted.contains("mJ"));
    }

    // ================================================================
    // New comprehensive tests
    // ================================================================

    #[test]
    fn high_frequency_pattern_gets_critical_priority() {
        let mut advisor = IndexAdvisor::new();
        let pattern = QueryPattern {
            table: "events".into(),
            columns_in_where: vec!["user_id".into()],
            columns_in_order_by: vec![],
            columns_in_group_by: vec![],
            has_join: false,
        };
        for _ in 0..2000 {
            advisor.record_query(pattern.clone(), 50_000, 20_000);
        }
        let recs = advisor.recommend();
        assert!(!recs.is_empty());
        let rec = recs.iter().find(|r| r.table == "events").unwrap();
        assert_eq!(rec.priority, RecommendationPriority::Critical);
        assert_eq!(rec.columns, vec!["user_id"]);
        assert_eq!(rec.index_type, RecommendedIndexType::BTree);
    }

    #[test]
    fn index_recommendation_for_composite_where() {
        let mut advisor = IndexAdvisor::new();
        let pattern = QueryPattern {
            table: "orders".into(),
            columns_in_where: vec!["customer_id".into(), "status".into(), "region".into()],
            columns_in_order_by: vec![],
            columns_in_group_by: vec![],
            has_join: false,
        };
        for _ in 0..100 {
            advisor.record_query(pattern.clone(), 5_000, 3_000);
        }
        let recs = advisor.recommend();
        let rec = recs.iter().find(|r| r.columns == vec!["customer_id", "status", "region"]).unwrap();
        assert_eq!(rec.table, "orders");
        assert!(rec.estimated_speedup > 1.0);
    }

    #[test]
    fn multiple_table_patterns_tracked_independently() {
        let mut advisor = IndexAdvisor::new();
        let p1 = QueryPattern {
            table: "users".into(),
            columns_in_where: vec!["email".into()],
            columns_in_order_by: vec![], columns_in_group_by: vec![], has_join: false,
        };
        let p2 = QueryPattern {
            table: "orders".into(),
            columns_in_where: vec!["user_id".into()],
            columns_in_order_by: vec![], columns_in_group_by: vec![], has_join: false,
        };
        let p3 = QueryPattern {
            table: "products".into(),
            columns_in_where: vec!["category".into()],
            columns_in_order_by: vec![], columns_in_group_by: vec![], has_join: false,
        };
        for _ in 0..30 {
            advisor.record_query(p1.clone(), 10_000, 5_000);
        }
        for _ in 0..20 {
            advisor.record_query(p2.clone(), 8_000, 4_000);
        }
        for _ in 0..15 {
            advisor.record_query(p3.clone(), 6_000, 3_000);
        }
        let all = advisor.patterns();
        assert_eq!(all.len(), 3);
        let top = advisor.top_patterns(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].execution_count, 30);
        assert_eq!(top[1].execution_count, 20);
        let recs = advisor.recommend();
        let tables: Vec<&str> = recs.iter().map(|r| r.table.as_str()).collect();
        assert!(tables.contains(&"users"));
        assert!(tables.contains(&"orders"));
        assert!(tables.contains(&"products"));
    }

    #[test]
    fn below_threshold_no_recommendation() {
        let mut advisor = IndexAdvisor::new();
        let pattern = QueryPattern {
            table: "tiny".into(),
            columns_in_where: vec!["id".into()],
            columns_in_order_by: vec![], columns_in_group_by: vec![], has_join: false,
        };
        // Below min_executions (10)
        for _ in 0..5 {
            advisor.record_query(pattern.clone(), 50_000, 10_000);
        }
        let recs = advisor.recommend();
        assert!(recs.iter().find(|r| r.table == "tiny").is_none());
    }

    #[test]
    fn low_row_scan_no_recommendation() {
        let mut advisor = IndexAdvisor::new();
        let pattern = QueryPattern {
            table: "small".into(),
            columns_in_where: vec!["id".into()],
            columns_in_order_by: vec![], columns_in_group_by: vec![], has_join: false,
        };
        // Below min_rows_threshold (100)
        for _ in 0..50 {
            advisor.record_query(pattern.clone(), 50, 100);
        }
        let recs = advisor.recommend();
        assert!(recs.iter().find(|r| r.table == "small").is_none());
    }

    #[test]
    fn covering_index_for_order_by() {
        let mut advisor = IndexAdvisor::new();
        let pattern = QueryPattern {
            table: "logs".into(),
            columns_in_where: vec!["level".into()],
            columns_in_order_by: vec!["timestamp".into()],
            columns_in_group_by: vec![],
            has_join: false,
        };
        for _ in 0..50 {
            advisor.record_query(pattern.clone(), 10_000, 5_000);
        }
        let recs = advisor.recommend();
        let covering = recs.iter().find(|r| r.columns == vec!["level", "timestamp"]);
        assert!(covering.is_some());
        assert_eq!(covering.unwrap().reason, "Covering index for ORDER BY avoids sort operation");
    }

    #[test]
    fn energy_estimation_with_network() {
        let estimator = EnergyEstimator::new();
        let cost = estimator.estimate(1_000, 100, 500);
        assert!(cost.cpu_uj > 0.0);
        assert!(cost.io_uj > 0.0);
        assert!(cost.network_uj > 0.0);
        assert!((cost.total_uj - (cost.cpu_uj + cost.io_uj + cost.network_uj)).abs() < 1e-6);
        assert!(cost.co2_ug > 0.0);
    }

    #[test]
    fn energy_estimator_custom_carbon_intensity() {
        let mut estimator = EnergyEstimator::new();
        estimator.carbon_intensity_g_per_kwh = 50.0; // France nuclear
        let cost_fr = estimator.estimate(100_000, 1_000, 0);
        estimator.carbon_intensity_g_per_kwh = 400.0; // US average
        let cost_us = estimator.estimate(100_000, 1_000, 0);
        assert!(cost_us.co2_ug > cost_fr.co2_ug);
        assert!((cost_us.co2_ug / cost_fr.co2_ug - 8.0).abs() < 0.01);
        assert!((cost_us.total_uj - cost_fr.total_uj).abs() < 1e-6);
    }

    #[test]
    fn energy_zero_query() {
        let estimator = EnergyEstimator::new();
        let cost = estimator.estimate(0, 0, 0);
        assert_eq!(cost.cpu_uj, 0.0);
        assert_eq!(cost.io_uj, 0.0);
        assert_eq!(cost.network_uj, 0.0);
        assert_eq!(cost.total_uj, 0.0);
        assert_eq!(cost.co2_ug, 0.0);
    }

    #[test]
    fn recommendation_priority_ordering() {
        let mut advisor = IndexAdvisor::new();
        let p_medium = QueryPattern {
            table: "t1".into(),
            columns_in_where: vec!["a".into()],
            columns_in_order_by: vec![], columns_in_group_by: vec![], has_join: false,
        };
        let p_high = QueryPattern {
            table: "t2".into(),
            columns_in_where: vec!["b".into()],
            columns_in_order_by: vec![], columns_in_group_by: vec![], has_join: false,
        };
        for _ in 0..20 {
            advisor.record_query(p_medium.clone(), 500, 1_000);
        }
        for _ in 0..200 {
            advisor.record_query(p_high.clone(), 500, 1_000);
        }
        let recs = advisor.recommend();
        assert!(recs.len() >= 2);
        assert!(recs[0].priority >= recs[1].priority);
    }

    // ── Auto-indexing tests ────────────────────────────────────────

    fn sample_auto_config() -> AutoIndexConfig {
        AutoIndexConfig {
            enabled: true,
            creation_threshold: 10.0,
            drop_unused_after_ms: 5000,
            max_auto_indexes: 4,
        }
    }

    fn make_rec(table: &str, columns: Vec<&str>, speedup: f64) -> IndexRecommendation {
        IndexRecommendation {
            table: table.to_string(),
            columns: columns.into_iter().map(String::from).collect(),
            index_type: RecommendedIndexType::BTree,
            estimated_speedup: speedup,
            reason: "test".into(),
            priority: RecommendationPriority::High,
        }
    }

    #[test]
    fn auto_indexer_creates_above_threshold() {
        let mut indexer = AutoIndexer::new(sample_auto_config());
        let recs = vec![make_rec("users", vec!["email"], 500.0)];
        let actions = indexer.evaluate_recommendations(&recs, 1000);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], AutoIndexAction::Created { table, .. } if table == "users"));
        assert_eq!(indexer.auto_index_count(), 1);
    }

    #[test]
    fn auto_indexer_skips_below_threshold() {
        let mut indexer = AutoIndexer::new(sample_auto_config());
        let recs = vec![make_rec("users", vec!["email"], 5.0)];
        let actions = indexer.evaluate_recommendations(&recs, 1000);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], AutoIndexAction::Skipped { .. }));
        assert_eq!(indexer.auto_index_count(), 0);
    }

    #[test]
    fn auto_indexer_respects_max_limit() {
        let config = AutoIndexConfig { max_auto_indexes: 2, ..sample_auto_config() };
        let mut indexer = AutoIndexer::new(config);
        let recs = vec![
            make_rec("t1", vec!["a"], 100.0),
            make_rec("t2", vec!["b"], 200.0),
            make_rec("t3", vec!["c"], 300.0),
        ];
        let actions = indexer.evaluate_recommendations(&recs, 1000);
        let created = actions.iter().filter(|a| matches!(a, AutoIndexAction::Created { .. })).count();
        let skipped = actions.iter().filter(|a| matches!(a, AutoIndexAction::Skipped { .. })).count();
        assert_eq!(created, 2);
        assert_eq!(skipped, 1);
    }

    #[test]
    fn auto_indexer_no_duplicate() {
        let mut indexer = AutoIndexer::new(sample_auto_config());
        let recs = vec![make_rec("users", vec!["email"], 500.0)];
        indexer.evaluate_recommendations(&recs, 1000);
        let actions = indexer.evaluate_recommendations(&recs, 2000);
        assert!(actions.is_empty());
        assert_eq!(indexer.auto_index_count(), 1);
    }

    #[test]
    fn auto_indexer_mark_used() {
        let mut indexer = AutoIndexer::new(sample_auto_config());
        let recs = vec![make_rec("users", vec!["email"], 500.0)];
        indexer.evaluate_recommendations(&recs, 1000);
        let cols = vec!["email".to_string()];
        indexer.mark_index_used("users", &cols, 5000);
        assert_eq!(indexer.get_auto_indexes()[0].last_used_at_ms, 5000);
        assert_eq!(indexer.get_auto_indexes()[0].query_count, 1);
    }

    #[test]
    fn auto_indexer_sweep_removes_stale() {
        let mut indexer = AutoIndexer::new(sample_auto_config());
        let recs = vec![
            make_rec("t1", vec!["a"], 100.0),
            make_rec("t2", vec!["b"], 200.0),
        ];
        indexer.evaluate_recommendations(&recs, 1000);
        // Mark t2 as used recently
        indexer.mark_index_used("t2", &["b".to_string()], 4000);
        // Sweep at 7000: t1 age=6000 >= 5000 (dropped), t2 age=3000 < 5000 (kept)
        let actions = indexer.sweep_unused(7000);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], AutoIndexAction::Dropped { table, .. } if table == "t1"));
        assert_eq!(indexer.auto_index_count(), 1);
    }

    #[test]
    fn auto_indexer_disabled_does_nothing() {
        let config = AutoIndexConfig { enabled: false, ..sample_auto_config() };
        let mut indexer = AutoIndexer::new(config);
        let recs = vec![make_rec("users", vec!["email"], 500.0)];
        let actions = indexer.evaluate_recommendations(&recs, 1000);
        assert!(actions.is_empty());
    }

    #[test]
    fn auto_indexer_toggle_enabled() {
        let mut indexer = AutoIndexer::new(sample_auto_config());
        indexer.set_enabled(false);
        assert!(!indexer.is_enabled());
        let recs = vec![make_rec("users", vec!["email"], 500.0)];
        assert!(indexer.evaluate_recommendations(&recs, 1000).is_empty());
        indexer.set_enabled(true);
        assert_eq!(indexer.evaluate_recommendations(&recs, 2000).len(), 1);
    }

    #[test]
    fn auto_indexer_actions_log() {
        let mut indexer = AutoIndexer::new(sample_auto_config());
        let recs = vec![
            make_rec("t1", vec!["a"], 500.0),
            make_rec("t2", vec!["b"], 3.0),
        ];
        indexer.evaluate_recommendations(&recs, 1000);
        assert_eq!(indexer.actions_log().len(), 2); // 1 created + 1 skipped
        indexer.sweep_unused(7000);
        assert_eq!(indexer.actions_log().len(), 3); // + 1 dropped
    }

    #[test]
    fn auto_indexer_end_to_end() {
        let mut advisor = IndexAdvisor::new();
        let pattern = QueryPattern {
            table: "orders".into(),
            columns_in_where: vec!["user_id".into()],
            columns_in_order_by: vec![],
            columns_in_group_by: vec![],
            has_join: false,
        };
        for _ in 0..100 {
            advisor.record_query(pattern.clone(), 10_000, 5_000);
        }
        let recs = advisor.recommend();
        assert!(!recs.is_empty());

        let mut indexer = AutoIndexer::new(AutoIndexConfig {
            enabled: true,
            creation_threshold: 10.0,
            drop_unused_after_ms: 10_000,
            max_auto_indexes: 8,
        });
        let actions = indexer.evaluate_recommendations(&recs, 1000);
        let created: Vec<_> = actions.iter().filter(|a| matches!(a, AutoIndexAction::Created { .. })).collect();
        assert!(!created.is_empty());
        assert_eq!(indexer.get_auto_indexes()[0].table, "orders");
    }

}
