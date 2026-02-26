//! Tiered storage engine — automatic hot/warm/cold/archive data placement.
//!
//! This module manages automatic data placement across storage tiers based on
//! data age and access patterns. Segments are placed in the tier that best
//! balances performance and cost: recently created or frequently accessed data
//! lives on the fastest (and most expensive) tier, while stale data migrates
//! progressively toward cheaper archival storage.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// StorageTier
// ---------------------------------------------------------------------------

/// Represents the physical storage class a data segment resides on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageTier {
    /// NVMe / RAM — fastest, most expensive.
    Hot,
    /// SSD — moderate speed, moderate cost.
    Warm,
    /// HDD or object storage — slow, cheap.
    Cold,
    /// Compressed cold storage — slowest, cheapest.
    Archive,
}

impl StorageTier {
    /// Returns a human-readable label for the tier.
    pub fn label(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
            StorageTier::Archive => "archive",
        }
    }
}

// ---------------------------------------------------------------------------
// TierConfig
// ---------------------------------------------------------------------------

/// Per-tier capacity and cost configuration.
#[derive(Debug, Clone)]
pub struct TierConfig {
    pub tier: StorageTier,
    /// Maximum capacity in bytes.
    pub capacity_bytes: usize,
    /// Currently used bytes.
    pub used_bytes: usize,
    /// Estimated cost in dollars per gigabyte per month.
    pub cost_per_gb_month: f64,
    /// Average access latency in microseconds.
    pub avg_latency_us: u64,
}

// ---------------------------------------------------------------------------
// TierPolicy
// ---------------------------------------------------------------------------

/// Rules that govern when data migrates between tiers for a given table.
#[derive(Debug, Clone)]
pub struct TierPolicy {
    pub table_name: String,
    /// Data newer than this many days stays in the Hot tier.
    pub hot_threshold_days: u32,
    /// Data older than `hot_threshold_days` but newer than this stays Warm.
    pub warm_threshold_days: u32,
    /// Data older than `warm_threshold_days` but newer than this stays Cold.
    /// Beyond this threshold the data is archived.
    pub cold_threshold_days: u32,
    /// If set, any segment whose `access_count` exceeds this value is promoted
    /// back to Hot regardless of age.
    pub access_count_override: Option<u64>,
}

// ---------------------------------------------------------------------------
// DataSegment
// ---------------------------------------------------------------------------

/// A chunk of data whose lifecycle is managed by the tiered storage engine.
#[derive(Debug, Clone)]
pub struct DataSegment {
    pub segment_id: u64,
    pub table_name: String,
    pub current_tier: StorageTier,
    /// Logical size in bytes (before compression, if any).
    pub size_bytes: usize,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at: u64,
    /// Last-access timestamp in milliseconds since the Unix epoch.
    pub last_accessed_at: u64,
    /// Total number of times this segment has been accessed.
    pub access_count: u64,
    /// Compression ratio where `1.0` means uncompressed and values < 1.0
    /// indicate the fraction of original size after compression.
    pub compression_ratio: f64,
}

// ---------------------------------------------------------------------------
// MigrationAction
// ---------------------------------------------------------------------------

/// Describes a single tier-migration operation.
#[derive(Debug, Clone)]
pub struct MigrationAction {
    pub segment_id: u64,
    pub from_tier: StorageTier,
    pub to_tier: StorageTier,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// TierStat
// ---------------------------------------------------------------------------

/// Aggregate statistics for a single storage tier.
#[derive(Debug, Clone)]
pub struct TierStat {
    pub tier: StorageTier,
    pub segment_count: usize,
    pub total_bytes: usize,
    pub utilization_pct: f64,
}

// ---------------------------------------------------------------------------
// TierManager
// ---------------------------------------------------------------------------

/// Core engine that tracks data segments, evaluates placement policies, and
/// plans migrations across storage tiers.
pub struct TierManager {
    configs: HashMap<StorageTier, TierConfig>,
    segments: HashMap<u64, DataSegment>,
    policies: HashMap<String, TierPolicy>,
}

const MS_PER_DAY: u64 = 86_400_000;
const BYTES_PER_GB: f64 = 1_073_741_824.0; // 2^30

impl TierManager {
    /// Creates a new `TierManager` with sensible default tier configurations.
    ///
    /// Default capacities:
    /// - Hot:     64 GiB, $3.00/GB/mo, 10 us latency
    /// - Warm:   256 GiB, $0.50/GB/mo, 200 us latency
    /// - Cold:     1 TiB, $0.02/GB/mo, 5 000 us latency
    /// - Archive:  4 TiB, $0.004/GB/mo, 50 000 us latency
    pub fn new() -> Self {
        let mut configs = HashMap::new();

        configs.insert(StorageTier::Hot, TierConfig {
            tier: StorageTier::Hot,
            capacity_bytes: 64 * 1024 * 1024 * 1024,   // 64 GiB
            used_bytes: 0,
            cost_per_gb_month: 3.0,
            avg_latency_us: 10,
        });
        configs.insert(StorageTier::Warm, TierConfig {
            tier: StorageTier::Warm,
            capacity_bytes: 256 * 1024 * 1024 * 1024,  // 256 GiB
            used_bytes: 0,
            cost_per_gb_month: 0.50,
            avg_latency_us: 200,
        });
        configs.insert(StorageTier::Cold, TierConfig {
            tier: StorageTier::Cold,
            capacity_bytes: 1024 * 1024 * 1024 * 1024, // 1 TiB
            used_bytes: 0,
            cost_per_gb_month: 0.02,
            avg_latency_us: 5_000,
        });
        configs.insert(StorageTier::Archive, TierConfig {
            tier: StorageTier::Archive,
            capacity_bytes: 4 * 1024 * 1024 * 1024 * 1024, // 4 TiB
            used_bytes: 0,
            cost_per_gb_month: 0.004,
            avg_latency_us: 50_000,
        });

        Self {
            configs,
            segments: HashMap::new(),
            policies: HashMap::new(),
        }
    }

    /// Registers (or replaces) a tier policy for the given table.
    pub fn set_policy(&mut self, policy: TierPolicy) {
        self.policies.insert(policy.table_name.clone(), policy);
    }

    /// Registers a data segment with the manager and accounts for its size on
    /// the segment's current tier.
    pub fn add_segment(&mut self, segment: DataSegment) {
        if let Some(cfg) = self.configs.get_mut(&segment.current_tier) {
            cfg.used_bytes += segment.size_bytes;
        }
        self.segments.insert(segment.segment_id, segment);
    }

    /// Records an access for `segment_id`, bumping its access count and
    /// updating `last_accessed_at` to `now_ms`.
    ///
    /// Returns `true` if the segment was found, `false` otherwise.
    pub fn record_access(&mut self, segment_id: u64, now_ms: u64) -> bool {
        if let Some(seg) = self.segments.get_mut(&segment_id) {
            seg.access_count += 1;
            seg.last_accessed_at = now_ms;
            true
        } else {
            false
        }
    }

    /// Determines the ideal tier for the given segment based on the table's
    /// policy, the segment's age, and its access frequency.
    ///
    /// Returns `None` if the segment or its table's policy is not registered,
    /// or if the segment is already on the ideal tier.
    pub fn evaluate_segment(&self, segment_id: u64, now_ms: u64) -> Option<StorageTier> {
        let seg = self.segments.get(&segment_id)?;
        let policy = self.policies.get(&seg.table_name)?;

        let age_days = now_ms.saturating_sub(seg.created_at) / MS_PER_DAY;

        // Access-count promotion override: if the segment has been accessed
        // more than the threshold, it belongs in Hot regardless of age.
        if let Some(threshold) = policy.access_count_override {
            if seg.access_count > threshold {
                return if seg.current_tier != StorageTier::Hot {
                    Some(StorageTier::Hot)
                } else {
                    None
                };
            }
        }

        let ideal = if age_days < policy.hot_threshold_days as u64 {
            StorageTier::Hot
        } else if age_days < policy.warm_threshold_days as u64 {
            StorageTier::Warm
        } else if age_days < policy.cold_threshold_days as u64 {
            StorageTier::Cold
        } else {
            StorageTier::Archive
        };

        if ideal != seg.current_tier {
            Some(ideal)
        } else {
            None
        }
    }

    /// Scans every registered segment and produces a list of migration actions
    /// for segments that are not on their ideal tier.
    pub fn plan_migrations(&self, now_ms: u64) -> Vec<MigrationAction> {
        let mut actions = Vec::new();

        for (&seg_id, seg) in &self.segments {
            if let Some(ideal) = self.evaluate_segment(seg_id, now_ms) {
                let reason = Self::migration_reason(seg, ideal);
                actions.push(MigrationAction {
                    segment_id: seg_id,
                    from_tier: seg.current_tier,
                    to_tier: ideal,
                    reason,
                });
            }
        }

        // Sort by segment_id for deterministic output.
        actions.sort_by_key(|a| a.segment_id);
        actions
    }

    /// Applies a migration action by moving the segment to the target tier and
    /// updating the used-bytes accounting on both tiers.
    ///
    /// Returns `true` if the migration was applied, `false` if the segment
    /// does not exist.
    pub fn apply_migration(&mut self, action: &MigrationAction) -> bool {
        let seg = match self.segments.get_mut(&action.segment_id) {
            Some(s) => s,
            None => return false,
        };

        let size = seg.size_bytes;

        // Decrement old tier usage.
        if let Some(cfg) = self.configs.get_mut(&action.from_tier) {
            cfg.used_bytes = cfg.used_bytes.saturating_sub(size);
        }

        // Increment new tier usage.
        if let Some(cfg) = self.configs.get_mut(&action.to_tier) {
            cfg.used_bytes += size;
        }

        seg.current_tier = action.to_tier;
        true
    }

    /// Returns aggregate statistics for every configured tier.
    pub fn tier_stats(&self) -> Vec<TierStat> {
        let mut stats: HashMap<StorageTier, TierStat> = HashMap::new();

        // Initialise from configs so we include tiers that have zero segments.
        for (tier, cfg) in &self.configs {
            stats.insert(*tier, TierStat {
                tier: *tier,
                segment_count: 0,
                total_bytes: 0,
                utilization_pct: 0.0,
            });
            // We will compute utilization after counting bytes.
            let _ = cfg; // suppress unused warning in this block
        }

        for seg in self.segments.values() {
            if let Some(stat) = stats.get_mut(&seg.current_tier) {
                stat.segment_count += 1;
                stat.total_bytes += seg.size_bytes;
            }
        }

        // Compute utilization percentages.
        for (tier, stat) in stats.iter_mut() {
            if let Some(cfg) = self.configs.get(tier) {
                if cfg.capacity_bytes > 0 {
                    stat.utilization_pct =
                        (stat.total_bytes as f64 / cfg.capacity_bytes as f64) * 100.0;
                }
            }
        }

        // Return in tier order: Hot, Warm, Cold, Archive.
        let order = [
            StorageTier::Hot,
            StorageTier::Warm,
            StorageTier::Cold,
            StorageTier::Archive,
        ];
        order
            .iter()
            .filter_map(|t| stats.remove(t))
            .collect()
    }

    /// Estimates the total monthly storage cost across all tiers in dollars.
    pub fn estimated_monthly_cost(&self) -> f64 {
        let mut total = 0.0;
        for (tier, cfg) in &self.configs {
            // Sum actual segment bytes on this tier rather than using
            // `cfg.used_bytes` — they should agree, but iterating segments is
            // the source of truth.
            let tier_bytes: usize = self
                .segments
                .values()
                .filter(|s| s.current_tier == *tier)
                .map(|s| s.size_bytes)
                .sum();

            let gb = tier_bytes as f64 / BYTES_PER_GB;
            total += gb * cfg.cost_per_gb_month;
        }
        total
    }

    // -- private helpers ----------------------------------------------------

    fn migration_reason(seg: &DataSegment, ideal: StorageTier) -> String {
        if ideal == StorageTier::Hot {
            format!(
                "segment {} promoted to hot — high access count ({})",
                seg.segment_id, seg.access_count,
            )
        } else {
            format!(
                "segment {} demoted from {} to {} — age-based policy",
                seg.segment_id,
                seg.current_tier.label(),
                ideal.label(),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: returns a default TierPolicy for table "events".
    fn default_policy() -> TierPolicy {
        TierPolicy {
            table_name: "events".into(),
            hot_threshold_days: 7,
            warm_threshold_days: 30,
            cold_threshold_days: 90,
            access_count_override: Some(100),
        }
    }

    /// Helper: builds a DataSegment with sensible defaults.
    fn make_segment(id: u64, created_at: u64, tier: StorageTier) -> DataSegment {
        DataSegment {
            segment_id: id,
            table_name: "events".into(),
            current_tier: tier,
            size_bytes: 1024 * 1024 * 100, // 100 MiB
            created_at,
            last_accessed_at: created_at,
            access_count: 0,
            compression_ratio: 1.0,
        }
    }

    // -- Test 1: segment placement by age -----------------------------------

    #[test]
    fn test_segment_placement_by_age() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy());

        let now = 200 * MS_PER_DAY; // "now" is day 200

        // Created 3 days ago — should stay Hot.
        let seg_hot = make_segment(1, now - 3 * MS_PER_DAY, StorageTier::Hot);
        mgr.add_segment(seg_hot);
        assert_eq!(mgr.evaluate_segment(1, now), None); // already correct

        // Created 15 days ago — should be Warm.
        let seg_warm = make_segment(2, now - 15 * MS_PER_DAY, StorageTier::Hot);
        mgr.add_segment(seg_warm);
        assert_eq!(mgr.evaluate_segment(2, now), Some(StorageTier::Warm));

        // Created 60 days ago — should be Cold.
        let seg_cold = make_segment(3, now - 60 * MS_PER_DAY, StorageTier::Hot);
        mgr.add_segment(seg_cold);
        assert_eq!(mgr.evaluate_segment(3, now), Some(StorageTier::Cold));

        // Created 120 days ago — should be Archive.
        let seg_archive = make_segment(4, now - 120 * MS_PER_DAY, StorageTier::Hot);
        mgr.add_segment(seg_archive);
        assert_eq!(mgr.evaluate_segment(4, now), Some(StorageTier::Archive));
    }

    // -- Test 2: hot-to-warm migration --------------------------------------

    #[test]
    fn test_hot_to_warm_migration() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy());

        let now = 100 * MS_PER_DAY;
        // 10 days old, still on Hot — should migrate to Warm.
        let seg = make_segment(10, now - 10 * MS_PER_DAY, StorageTier::Hot);
        mgr.add_segment(seg);

        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].segment_id, 10);
        assert_eq!(actions[0].from_tier, StorageTier::Hot);
        assert_eq!(actions[0].to_tier, StorageTier::Warm);

        // Apply the migration.
        assert!(mgr.apply_migration(&actions[0]));
        let seg = mgr.segments.get(&10).unwrap();
        assert_eq!(seg.current_tier, StorageTier::Warm);
    }

    // -- Test 3: warm-to-cold migration -------------------------------------

    #[test]
    fn test_warm_to_cold_migration() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy());

        let now = 200 * MS_PER_DAY;
        // 50 days old, sitting on Warm — should migrate to Cold.
        let seg = make_segment(20, now - 50 * MS_PER_DAY, StorageTier::Warm);
        mgr.add_segment(seg);

        let ideal = mgr.evaluate_segment(20, now);
        assert_eq!(ideal, Some(StorageTier::Cold));

        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].to_tier, StorageTier::Cold);

        mgr.apply_migration(&actions[0]);
        assert_eq!(
            mgr.segments.get(&20).unwrap().current_tier,
            StorageTier::Cold,
        );
    }

    // -- Test 4: access-count promotion override ----------------------------

    #[test]
    fn test_access_count_promotion_override() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy()); // override threshold = 100

        let now = 200 * MS_PER_DAY;
        // 120 days old, on Cold tier — would normally go to Archive.
        let mut seg = make_segment(30, now - 120 * MS_PER_DAY, StorageTier::Cold);
        seg.access_count = 50; // below threshold
        mgr.add_segment(seg);

        // With 50 accesses it should go to Archive (age wins).
        assert_eq!(mgr.evaluate_segment(30, now), Some(StorageTier::Archive));

        // Simulate heavy access — push count above threshold.
        for _ in 0..60 {
            mgr.record_access(30, now);
        }
        // Now access_count = 110 > 100 threshold — should promote to Hot.
        assert_eq!(mgr.evaluate_segment(30, now), Some(StorageTier::Hot));

        // Apply the migration.
        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].to_tier, StorageTier::Hot);
        assert!(actions[0].reason.contains("promoted to hot"));
    }

    // -- Test 5: tier stats calculation -------------------------------------

    #[test]
    fn test_tier_stats_calculation() {
        let mut mgr = TierManager::new();

        let seg1 = DataSegment {
            segment_id: 1,
            table_name: "events".into(),
            current_tier: StorageTier::Hot,
            size_bytes: 1_000_000,
            created_at: 0,
            last_accessed_at: 0,
            access_count: 0,
            compression_ratio: 1.0,
        };
        let seg2 = DataSegment {
            segment_id: 2,
            table_name: "events".into(),
            current_tier: StorageTier::Hot,
            size_bytes: 2_000_000,
            created_at: 0,
            last_accessed_at: 0,
            access_count: 0,
            compression_ratio: 1.0,
        };
        let seg3 = DataSegment {
            segment_id: 3,
            table_name: "events".into(),
            current_tier: StorageTier::Cold,
            size_bytes: 5_000_000,
            created_at: 0,
            last_accessed_at: 0,
            access_count: 0,
            compression_ratio: 1.0,
        };

        mgr.add_segment(seg1);
        mgr.add_segment(seg2);
        mgr.add_segment(seg3);

        let stats = mgr.tier_stats();
        assert_eq!(stats.len(), 4); // Hot, Warm, Cold, Archive

        let hot = &stats[0];
        assert_eq!(hot.tier, StorageTier::Hot);
        assert_eq!(hot.segment_count, 2);
        assert_eq!(hot.total_bytes, 3_000_000);

        let warm = &stats[1];
        assert_eq!(warm.tier, StorageTier::Warm);
        assert_eq!(warm.segment_count, 0);
        assert_eq!(warm.total_bytes, 0);

        let cold = &stats[2];
        assert_eq!(cold.tier, StorageTier::Cold);
        assert_eq!(cold.segment_count, 1);
        assert_eq!(cold.total_bytes, 5_000_000);
    }

    // -- Test 6: cost estimation --------------------------------------------

    #[test]
    fn test_cost_estimation() {
        let mut mgr = TierManager::new();

        // Place exactly 1 GiB on Hot ($3.00/GB/mo) and 1 GiB on Cold ($0.02/GB/mo).
        let one_gib: usize = 1024 * 1024 * 1024;

        let seg_hot = DataSegment {
            segment_id: 1,
            table_name: "t".into(),
            current_tier: StorageTier::Hot,
            size_bytes: one_gib,
            created_at: 0,
            last_accessed_at: 0,
            access_count: 0,
            compression_ratio: 1.0,
        };
        let seg_cold = DataSegment {
            segment_id: 2,
            table_name: "t".into(),
            current_tier: StorageTier::Cold,
            size_bytes: one_gib,
            created_at: 0,
            last_accessed_at: 0,
            access_count: 0,
            compression_ratio: 1.0,
        };

        mgr.add_segment(seg_hot);
        mgr.add_segment(seg_cold);

        let cost = mgr.estimated_monthly_cost();
        // 1 GiB = exactly 1.0 GB in our constant (BYTES_PER_GB = 2^30).
        // Expected: 1.0 * 3.0 + 1.0 * 0.02 = 3.02
        let expected = 3.0 + 0.02;
        assert!(
            (cost - expected).abs() < 1e-6,
            "expected cost ~{expected}, got {cost}",
        );
    }

    // -- Test 7: migration plan generation ----------------------------------

    #[test]
    fn test_migration_plan_generation() {
        let mut mgr = TierManager::new();
        mgr.set_policy(TierPolicy {
            table_name: "logs".into(),
            hot_threshold_days: 3,
            warm_threshold_days: 14,
            cold_threshold_days: 60,
            access_count_override: None,
        });

        let now = 100 * MS_PER_DAY;

        // Segment on Hot, 5 days old — should demote to Warm.
        let s1 = DataSegment {
            segment_id: 1,
            table_name: "logs".into(),
            current_tier: StorageTier::Hot,
            size_bytes: 500,
            created_at: now - 5 * MS_PER_DAY,
            last_accessed_at: now - 5 * MS_PER_DAY,
            access_count: 0,
            compression_ratio: 1.0,
        };

        // Segment on Warm, 20 days old — should demote to Cold.
        let s2 = DataSegment {
            segment_id: 2,
            table_name: "logs".into(),
            current_tier: StorageTier::Warm,
            size_bytes: 1000,
            created_at: now - 20 * MS_PER_DAY,
            last_accessed_at: now - 20 * MS_PER_DAY,
            access_count: 0,
            compression_ratio: 1.0,
        };

        // Segment on Cold, 80 days old — should demote to Archive.
        let s3 = DataSegment {
            segment_id: 3,
            table_name: "logs".into(),
            current_tier: StorageTier::Cold,
            size_bytes: 2000,
            created_at: now - 80 * MS_PER_DAY,
            last_accessed_at: now - 80 * MS_PER_DAY,
            access_count: 0,
            compression_ratio: 1.0,
        };

        // Segment already on correct tier (Hot, 1 day old) — no migration.
        let s4 = DataSegment {
            segment_id: 4,
            table_name: "logs".into(),
            current_tier: StorageTier::Hot,
            size_bytes: 300,
            created_at: now - 1 * MS_PER_DAY,
            last_accessed_at: now - 1 * MS_PER_DAY,
            access_count: 0,
            compression_ratio: 1.0,
        };

        mgr.add_segment(s1);
        mgr.add_segment(s2);
        mgr.add_segment(s3);
        mgr.add_segment(s4);

        let actions = mgr.plan_migrations(now);

        // Expect exactly 3 migrations (segments 1, 2, 3). Segment 4 stays.
        assert_eq!(actions.len(), 3);

        // Actions are sorted by segment_id.
        assert_eq!(actions[0].segment_id, 1);
        assert_eq!(actions[0].from_tier, StorageTier::Hot);
        assert_eq!(actions[0].to_tier, StorageTier::Warm);

        assert_eq!(actions[1].segment_id, 2);
        assert_eq!(actions[1].from_tier, StorageTier::Warm);
        assert_eq!(actions[1].to_tier, StorageTier::Cold);

        assert_eq!(actions[2].segment_id, 3);
        assert_eq!(actions[2].from_tier, StorageTier::Cold);
        assert_eq!(actions[2].to_tier, StorageTier::Archive);

        // Apply all migrations and verify segment tiers.
        for action in &actions {
            mgr.apply_migration(action);
        }
        assert_eq!(mgr.segments.get(&1).unwrap().current_tier, StorageTier::Warm);
        assert_eq!(mgr.segments.get(&2).unwrap().current_tier, StorageTier::Cold);
        assert_eq!(mgr.segments.get(&3).unwrap().current_tier, StorageTier::Archive);
        assert_eq!(mgr.segments.get(&4).unwrap().current_tier, StorageTier::Hot);
    }

    // -- Test 8: promotion from cold tier to hot tier -------------------------

    #[test]
    fn test_cold_to_hot_promotion_via_access_count() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy()); // access_count_override = Some(100)

        let now = 300 * MS_PER_DAY;

        // A segment that is 200 days old, sitting on Cold tier.
        // Age alone would dictate Archive, but high access count should
        // override and promote it all the way to Hot.
        let mut seg = make_segment(50, now - 200 * MS_PER_DAY, StorageTier::Cold);
        seg.access_count = 101; // above the 100 threshold
        mgr.add_segment(seg);

        // Evaluation should say Hot (promotion override).
        assert_eq!(mgr.evaluate_segment(50, now), Some(StorageTier::Hot));

        // Plan and apply the migration.
        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].from_tier, StorageTier::Cold);
        assert_eq!(actions[0].to_tier, StorageTier::Hot);
        assert!(actions[0].reason.contains("promoted to hot"));

        mgr.apply_migration(&actions[0]);
        let seg = mgr.segments.get(&50).unwrap();
        assert_eq!(seg.current_tier, StorageTier::Hot);

        // After promotion, re-evaluation should return None (already on Hot).
        assert_eq!(mgr.evaluate_segment(50, now), None);
    }

    // -- Test 9: demotion from hot to cold (access-count-based demotion) ------

    #[test]
    fn test_hot_to_cold_demotion_when_access_count_below_threshold() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy()); // access_count_override = Some(100)

        let now = 300 * MS_PER_DAY;

        // A segment that is 60 days old (Cold-age range) with a low access
        // count. Even though it is currently on Hot, the access count (5) is
        // well below the 100 threshold, so age-based policy applies and it
        // should be demoted to Cold.
        let mut seg = make_segment(60, now - 60 * MS_PER_DAY, StorageTier::Hot);
        seg.access_count = 5; // far below the override threshold
        mgr.add_segment(seg);

        assert_eq!(mgr.evaluate_segment(60, now), Some(StorageTier::Cold));

        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].from_tier, StorageTier::Hot);
        assert_eq!(actions[0].to_tier, StorageTier::Cold);
        assert!(actions[0].reason.contains("demoted"));

        mgr.apply_migration(&actions[0]);
        assert_eq!(
            mgr.segments.get(&60).unwrap().current_tier,
            StorageTier::Cold,
        );

        // Verify used_bytes accounting: Hot tier should have decreased,
        // Cold tier should have increased.
        let hot_cfg = mgr.configs.get(&StorageTier::Hot).unwrap();
        assert_eq!(hot_cfg.used_bytes, 0);
        let cold_cfg = mgr.configs.get(&StorageTier::Cold).unwrap();
        assert_eq!(cold_cfg.used_bytes, 100 * 1024 * 1024); // 100 MiB
    }

    // -- Test 10: data flowing through all four tiers -------------------------

    #[test]
    fn test_segment_flows_through_all_tiers() {
        let mut mgr = TierManager::new();
        mgr.set_policy(TierPolicy {
            table_name: "events".into(),
            hot_threshold_days: 7,
            warm_threshold_days: 30,
            cold_threshold_days: 90,
            access_count_override: None, // no override — pure age-based
        });

        // Start with a brand-new segment on Hot.
        let seg = make_segment(100, 0, StorageTier::Hot);
        mgr.add_segment(seg);

        // Phase 1: Day 3 — still Hot, no migration needed.
        let day3 = 3 * MS_PER_DAY;
        assert_eq!(mgr.evaluate_segment(100, day3), None);

        // Phase 2: Day 10 — should move to Warm.
        let day10 = 10 * MS_PER_DAY;
        assert_eq!(mgr.evaluate_segment(100, day10), Some(StorageTier::Warm));
        let actions = mgr.plan_migrations(day10);
        assert_eq!(actions.len(), 1);
        mgr.apply_migration(&actions[0]);
        assert_eq!(mgr.segments.get(&100).unwrap().current_tier, StorageTier::Warm);

        // Phase 3: Day 50 — should move to Cold.
        let day50 = 50 * MS_PER_DAY;
        assert_eq!(mgr.evaluate_segment(100, day50), Some(StorageTier::Cold));
        let actions = mgr.plan_migrations(day50);
        assert_eq!(actions.len(), 1);
        mgr.apply_migration(&actions[0]);
        assert_eq!(mgr.segments.get(&100).unwrap().current_tier, StorageTier::Cold);

        // Phase 4: Day 100 — should move to Archive.
        let day100 = 100 * MS_PER_DAY;
        assert_eq!(mgr.evaluate_segment(100, day100), Some(StorageTier::Archive));
        let actions = mgr.plan_migrations(day100);
        assert_eq!(actions.len(), 1);
        mgr.apply_migration(&actions[0]);
        assert_eq!(mgr.segments.get(&100).unwrap().current_tier, StorageTier::Archive);

        // Phase 5: Day 500 — still Archive, no further migration.
        let day500 = 500 * MS_PER_DAY;
        assert_eq!(mgr.evaluate_segment(100, day500), None);
        let actions = mgr.plan_migrations(day500);
        assert!(actions.is_empty());
    }

    // -- Test 11: edge cases — key not found, empty tier, max capacity --------

    #[test]
    fn test_edge_case_key_not_found() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy());

        // Evaluate a segment that does not exist.
        assert_eq!(mgr.evaluate_segment(999, 0), None);

        // Record access on a non-existent segment.
        assert!(!mgr.record_access(999, 0));

        // Apply migration on a non-existent segment.
        let action = MigrationAction {
            segment_id: 999,
            from_tier: StorageTier::Hot,
            to_tier: StorageTier::Cold,
            reason: "test".into(),
        };
        assert!(!mgr.apply_migration(&action));
    }

    #[test]
    fn test_edge_case_empty_tiers() {
        let mgr = TierManager::new();

        // Stats should still report all four tiers with zero segments.
        let stats = mgr.tier_stats();
        assert_eq!(stats.len(), 4);
        for stat in &stats {
            assert_eq!(stat.segment_count, 0);
            assert_eq!(stat.total_bytes, 0);
            assert!((stat.utilization_pct - 0.0).abs() < f64::EPSILON);
        }

        // No migrations should be planned.
        let actions = mgr.plan_migrations(0);
        assert!(actions.is_empty());

        // Monthly cost should be zero.
        assert!((mgr.estimated_monthly_cost() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_edge_case_segment_without_policy() {
        let mut mgr = TierManager::new();
        // Do NOT set any policy.

        let seg = make_segment(1, 0, StorageTier::Hot);
        mgr.add_segment(seg);

        // Without a policy, evaluate_segment should return None.
        assert_eq!(mgr.evaluate_segment(1, 100 * MS_PER_DAY), None);

        // plan_migrations should produce nothing.
        let actions = mgr.plan_migrations(100 * MS_PER_DAY);
        assert!(actions.is_empty());
    }

    // -- Test 12: stats and metrics tracking ----------------------------------

    #[test]
    fn test_stats_and_metrics_after_migrations() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy());

        let now = 200 * MS_PER_DAY;
        let seg_size: usize = 1024 * 1024 * 100; // 100 MiB each

        // Add 3 segments: all start on Hot but have varying ages.
        // Segment 1: 3 days old (stays Hot)
        // Segment 2: 15 days old (should go to Warm)
        // Segment 3: 60 days old (should go to Cold)
        mgr.add_segment(make_segment(1, now - 3 * MS_PER_DAY, StorageTier::Hot));
        mgr.add_segment(make_segment(2, now - 15 * MS_PER_DAY, StorageTier::Hot));
        mgr.add_segment(make_segment(3, now - 60 * MS_PER_DAY, StorageTier::Hot));

        // Before migration: all 3 on Hot.
        let stats_before = mgr.tier_stats();
        let hot_before = stats_before.iter().find(|s| s.tier == StorageTier::Hot).unwrap();
        assert_eq!(hot_before.segment_count, 3);
        assert_eq!(hot_before.total_bytes, 3 * seg_size);

        // Apply migrations.
        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 2); // segments 2 and 3 need to move
        for action in &actions {
            mgr.apply_migration(action);
        }

        // After migration: 1 on Hot, 1 on Warm, 1 on Cold.
        let stats_after = mgr.tier_stats();
        let hot_after = stats_after.iter().find(|s| s.tier == StorageTier::Hot).unwrap();
        let warm_after = stats_after.iter().find(|s| s.tier == StorageTier::Warm).unwrap();
        let cold_after = stats_after.iter().find(|s| s.tier == StorageTier::Cold).unwrap();

        assert_eq!(hot_after.segment_count, 1);
        assert_eq!(hot_after.total_bytes, seg_size);
        assert_eq!(warm_after.segment_count, 1);
        assert_eq!(warm_after.total_bytes, seg_size);
        assert_eq!(cold_after.segment_count, 1);
        assert_eq!(cold_after.total_bytes, seg_size);

        // Utilization percentage: Hot capacity is 64 GiB.
        // 100 MiB / 64 GiB = 100 / 65536 ~ 0.00152587890625 * 100
        let expected_util = (seg_size as f64 / (64.0 * 1024.0 * 1024.0 * 1024.0)) * 100.0;
        assert!(
            (hot_after.utilization_pct - expected_util).abs() < 1e-9,
            "expected utilization ~{expected_util}, got {}",
            hot_after.utilization_pct,
        );

        // Verify cost decreased after migration (moving data from Hot to
        // cheaper tiers reduces total cost).
        // Re-create the pre-migration state cost for comparison.
        // Pre: 3 * 100MiB on Hot at $3.00/GB = 3 * (100/1024) * 3.0
        // Post: 1 on Hot, 1 on Warm ($0.50), 1 on Cold ($0.02)
        let cost_after = mgr.estimated_monthly_cost();
        let gb_per_seg = seg_size as f64 / BYTES_PER_GB;
        let cost_before_expected = 3.0 * gb_per_seg * 3.0;
        let cost_after_expected =
            gb_per_seg * 3.0 + gb_per_seg * 0.50 + gb_per_seg * 0.02;
        assert!(cost_after < cost_before_expected);
        assert!(
            (cost_after - cost_after_expected).abs() < 1e-9,
            "expected cost ~{cost_after_expected}, got {cost_after}",
        );
    }

    // -- Test 13: concurrent-style access patterns ----------------------------

    #[test]
    fn test_concurrent_access_patterns() {
        // Simulates what happens when many segments are accessed in rapid
        // succession — e.g., a burst of reads that promote cold segments
        // back to hot, while other segments age out simultaneously.
        let mut mgr = TierManager::new();
        mgr.set_policy(TierPolicy {
            table_name: "events".into(),
            hot_threshold_days: 7,
            warm_threshold_days: 30,
            cold_threshold_days: 90,
            access_count_override: Some(50),
        });

        let now = 200 * MS_PER_DAY;

        // Create 10 segments, all 100 days old, on Archive tier.
        for id in 1..=10u64 {
            let seg = DataSegment {
                segment_id: id,
                table_name: "events".into(),
                current_tier: StorageTier::Archive,
                size_bytes: 1024 * 1024, // 1 MiB each
                created_at: now - 100 * MS_PER_DAY,
                last_accessed_at: now - 100 * MS_PER_DAY,
                access_count: 0,
                compression_ratio: 0.5,
            };
            mgr.add_segment(seg);
        }

        // Burst of reads on segments 1-5 (51 accesses each, above threshold).
        for id in 1..=5u64 {
            for _ in 0..51 {
                assert!(mgr.record_access(id, now));
            }
        }
        // Segments 6-10 get no accesses — they stay Archive (already correct).

        // Plan migrations: segments 1-5 should be promoted to Hot.
        // Segments 6-10 are already on Archive which matches age policy.
        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 5);

        // All 5 actions should be promotions to Hot.
        for action in &actions {
            assert!(action.segment_id >= 1 && action.segment_id <= 5);
            assert_eq!(action.from_tier, StorageTier::Archive);
            assert_eq!(action.to_tier, StorageTier::Hot);
        }

        // Apply all migrations.
        for action in &actions {
            assert!(mgr.apply_migration(action));
        }

        // Verify final state: 5 on Hot, 0 on Warm, 0 on Cold, 5 on Archive.
        let stats = mgr.tier_stats();
        let hot = stats.iter().find(|s| s.tier == StorageTier::Hot).unwrap();
        let archive = stats.iter().find(|s| s.tier == StorageTier::Archive).unwrap();
        assert_eq!(hot.segment_count, 5);
        assert_eq!(archive.segment_count, 5);

        // Verify that access counts and timestamps were updated correctly.
        for id in 1..=5u64 {
            let seg = mgr.segments.get(&id).unwrap();
            assert_eq!(seg.access_count, 51);
            assert_eq!(seg.last_accessed_at, now);
            assert_eq!(seg.current_tier, StorageTier::Hot);
        }
        for id in 6..=10u64 {
            let seg = mgr.segments.get(&id).unwrap();
            assert_eq!(seg.access_count, 0);
            assert_eq!(seg.current_tier, StorageTier::Archive);
        }
    }

    // -- Test 14: used_bytes accounting across multiple migrations -------------

    #[test]
    fn test_used_bytes_accounting_round_trip() {
        let mut mgr = TierManager::new();
        mgr.set_policy(default_policy());

        let seg_size = 1024 * 1024 * 100; // 100 MiB
        let now = 200 * MS_PER_DAY;

        // Start a segment on Hot.
        let seg = make_segment(1, now - 60 * MS_PER_DAY, StorageTier::Hot);
        mgr.add_segment(seg);

        assert_eq!(mgr.configs.get(&StorageTier::Hot).unwrap().used_bytes, seg_size);
        assert_eq!(mgr.configs.get(&StorageTier::Cold).unwrap().used_bytes, 0);

        // Demote to Cold (age = 60 days).
        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 1);
        mgr.apply_migration(&actions[0]);

        assert_eq!(mgr.configs.get(&StorageTier::Hot).unwrap().used_bytes, 0);
        assert_eq!(mgr.configs.get(&StorageTier::Cold).unwrap().used_bytes, seg_size);

        // Now simulate heavy access to promote back to Hot.
        for _ in 0..101 {
            mgr.record_access(1, now);
        }
        let actions = mgr.plan_migrations(now);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].to_tier, StorageTier::Hot);
        mgr.apply_migration(&actions[0]);

        // Bytes should be back on Hot, Cold should be zero again.
        assert_eq!(mgr.configs.get(&StorageTier::Hot).unwrap().used_bytes, seg_size);
        assert_eq!(mgr.configs.get(&StorageTier::Cold).unwrap().used_bytes, 0);
    }
}
