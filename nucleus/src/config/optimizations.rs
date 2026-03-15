//! Phase 4 optimization configuration.
//!
//! Runtime flags for controlling binary protocol, zone maps, GROUP BY specialization,
//! lazy materialization, and SIMD aggregates.

use clap::{Parser, ValueEnum};

/// Optimization configuration for Phase 4.
#[derive(Parser, Debug, Clone)]
pub struct OptimizationConfig {
    /// Binary protocol settings
    #[command(flatten)]
    pub binary_protocol: BinaryProtocolConfig,

    /// Zone map settings
    #[command(flatten)]
    pub zone_maps: ZoneMapConfig,

    /// GROUP BY specialization settings
    #[command(flatten)]
    pub group_by: GroupByConfig,

    /// Lazy materialization settings
    #[command(flatten)]
    pub lazy_materialization: LazyMaterializationConfig,

    /// SIMD settings
    #[command(flatten)]
    pub simd: SimdConfig,

    /// Metrics and monitoring
    #[command(flatten)]
    pub metrics: MetricsConfig,
}

/// Binary protocol configuration.
#[derive(Parser, Debug, Clone)]
pub struct BinaryProtocolConfig {
    /// Disable binary protocol (use SQL/PostgreSQL protocol only)
    #[arg(long, default_value = "false")]
    pub disable_binary_protocol: bool,

    /// Port for binary protocol server (in addition to SQL on 5432)
    #[arg(long, default_value = "5433")]
    pub binary_port: u16,

    /// Maximum message size for binary protocol (bytes)
    #[arg(long, default_value = "67108864")]  // 64 MB
    pub binary_max_message_size: u32,

    /// Number of binary protocol handler threads
    #[arg(long, default_value = "4")]
    pub binary_thread_pool_size: usize,

    /// Enable binary protocol handshake compression
    #[arg(long, default_value = "true")]
    pub binary_compression_enabled: bool,

    /// Binary protocol read timeout (seconds)
    #[arg(long, default_value = "30")]
    pub binary_read_timeout_secs: u64,
}

/// Zone map configuration.
#[derive(Parser, Debug, Clone)]
pub struct ZoneMapConfig {
    /// Disable zone maps (granule-level min/max pruning)
    #[arg(long, default_value = "false")]
    pub disable_zone_maps: bool,

    /// Minimum table size to enable zone maps (rows)
    #[arg(long, default_value = "10000000")]  // 10M rows
    pub zone_map_threshold_rows: u64,

    /// Granule size for zone map statistics (bytes)
    #[arg(long, default_value = "1048576")]  // 1 MB
    pub zone_map_granule_size_bytes: u64,

    /// Recompute zone maps after this many inserts
    #[arg(long, default_value = "1000000")]
    pub zone_map_recompute_threshold: u64,

    /// Skip zone map recomputation if skip ratio >this percentage
    #[arg(long, default_value = "90")]
    pub zone_map_max_skip_ratio_percent: u8,
}

/// GROUP BY specialization configuration.
#[derive(Parser, Debug, Clone)]
pub struct GroupByConfig {
    /// Disable GROUP BY type specialization
    #[arg(long, default_value = "false")]
    pub disable_group_by_specialization: bool,

    /// Enable GROUP BY column pruning
    #[arg(long, default_value = "true")]
    pub group_by_column_pruning: bool,

    /// Threshold for specialized vs generic path (milliseconds)
    #[arg(long, default_value = "100")]
    pub group_by_specialization_threshold_ms: u64,

    /// Maximum distinct values for in-memory GROUP BY hash table
    #[arg(long, default_value = "10000000")]
    pub group_by_hash_table_max_size: usize,
}

/// Lazy materialization configuration.
#[derive(Parser, Debug, Clone)]
pub struct LazyMaterializationConfig {
    /// Disable lazy materialization (always materialize all rows)
    #[arg(long, default_value = "false")]
    pub disable_lazy_materialization: bool,

    /// Minimum result set size to enable lazy materialization (rows)
    #[arg(long, default_value = "1000")]
    pub lazy_materialization_threshold_rows: usize,

    /// Memory threshold before forcing materialization (bytes)
    #[arg(long, default_value = "536870912")]  // 512 MB
    pub lazy_materialization_memory_limit_bytes: u64,

    /// Enable deferred row cleanup
    #[arg(long, default_value = "true")]
    pub lazy_materialization_cleanup_enabled: bool,

    /// Cleanup batch size (rows to clean at once)
    #[arg(long, default_value = "10000")]
    pub lazy_materialization_cleanup_batch_size: usize,
}

/// SIMD configuration.
#[derive(Parser, Debug, Clone)]
pub struct SimdConfig {
    /// Disable SIMD aggregates (use scalar path only)
    #[arg(long, default_value = "false")]
    pub disable_simd: bool,

    /// SIMD CPU target
    #[arg(long, value_enum, default_value = "auto")]
    pub simd_target: SimdTarget,

    /// Enable SIMD correctness checking (compare SIMD vs scalar)
    #[arg(long, default_value = "false")]
    pub simd_correctness_check: bool,

    /// Sample ratio for SIMD correctness checking (1 in N queries)
    #[arg(long, default_value = "1000")]
    pub simd_correctness_check_sample_ratio: u32,

    /// Minimum result set size for SIMD (avoids overhead on small sets)
    #[arg(long, default_value = "1000")]
    pub simd_min_rows: usize,
}

/// SIMD CPU target selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SimdTarget {
    /// Automatically detect available features
    #[value(name = "auto")]
    Auto,

    /// AVX-512 (Skylake-X and newer, requires explicit CPU flag)
    #[value(name = "avx512")]
    Avx512,

    /// AVX-2 (Haswell and newer)
    #[value(name = "avx2")]
    Avx2,

    /// SSE4.2 (Nehalem and newer)
    #[value(name = "sse42")]
    Sse42,

    /// Scalar (fallback, always available)
    #[value(name = "scalar")]
    Scalar,
}

/// Metrics and monitoring configuration.
#[derive(Parser, Debug, Clone)]
pub struct MetricsConfig {
    /// Port for Prometheus metrics endpoint
    #[arg(long, default_value = "9090")]
    pub metrics_port: u16,

    /// Enable Prometheus /metrics endpoint
    #[arg(long, default_value = "true")]
    pub metrics_enabled: bool,

    /// Enable /health endpoint
    #[arg(long, default_value = "true")]
    pub health_check_enabled: bool,

    /// Include detailed optimization metrics in health check
    #[arg(long, default_value = "true")]
    pub health_check_detailed: bool,
}

impl OptimizationConfig {
    /// Create default configuration.
    pub fn default() -> Self {
        Self {
            binary_protocol: BinaryProtocolConfig::default(),
            zone_maps: ZoneMapConfig::default(),
            group_by: GroupByConfig::default(),
            lazy_materialization: LazyMaterializationConfig::default(),
            simd: SimdConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }

    /// All optimizations enabled.
    pub fn all_enabled() -> Self {
        Self {
            binary_protocol: BinaryProtocolConfig {
                disable_binary_protocol: false,
                binary_port: 5433,
                binary_max_message_size: 67108864,
                binary_thread_pool_size: 4,
                binary_compression_enabled: true,
                binary_read_timeout_secs: 30,
            },
            zone_maps: ZoneMapConfig {
                disable_zone_maps: false,
                zone_map_threshold_rows: 1000000,  // 1M rows for full optimization
                zone_map_granule_size_bytes: 1048576,
                zone_map_recompute_threshold: 1000000,
                zone_map_max_skip_ratio_percent: 90,
            },
            group_by: GroupByConfig {
                disable_group_by_specialization: false,
                group_by_column_pruning: true,
                group_by_specialization_threshold_ms: 100,
                group_by_hash_table_max_size: 10000000,
            },
            lazy_materialization: LazyMaterializationConfig {
                disable_lazy_materialization: false,
                lazy_materialization_threshold_rows: 1000,
                lazy_materialization_memory_limit_bytes: 536870912,
                lazy_materialization_cleanup_enabled: true,
                lazy_materialization_cleanup_batch_size: 10000,
            },
            simd: SimdConfig {
                disable_simd: false,
                simd_target: SimdTarget::Auto,
                simd_correctness_check: false,
                simd_correctness_check_sample_ratio: 1000,
                simd_min_rows: 1000,
            },
            metrics: MetricsConfig {
                metrics_port: 9090,
                metrics_enabled: true,
                health_check_enabled: true,
                health_check_detailed: true,
            },
        }
    }

    /// All optimizations disabled (baseline).
    pub fn all_disabled() -> Self {
        Self {
            binary_protocol: BinaryProtocolConfig {
                disable_binary_protocol: true,
                ..BinaryProtocolConfig::default()
            },
            zone_maps: ZoneMapConfig {
                disable_zone_maps: true,
                ..ZoneMapConfig::default()
            },
            group_by: GroupByConfig {
                disable_group_by_specialization: true,
                ..GroupByConfig::default()
            },
            lazy_materialization: LazyMaterializationConfig {
                disable_lazy_materialization: true,
                ..LazyMaterializationConfig::default()
            },
            simd: SimdConfig {
                disable_simd: true,
                ..SimdConfig::default()
            },
            metrics: MetricsConfig::default(),
        }
    }

    /// Check if all optimizations are enabled.
    pub fn all_enabled_check(&self) -> bool {
        !self.binary_protocol.disable_binary_protocol
            && !self.zone_maps.disable_zone_maps
            && !self.group_by.disable_group_by_specialization
            && !self.lazy_materialization.disable_lazy_materialization
            && !self.simd.disable_simd
    }

    /// Check if all optimizations are disabled.
    pub fn all_disabled_check(&self) -> bool {
        self.binary_protocol.disable_binary_protocol
            && self.zone_maps.disable_zone_maps
            && self.group_by.disable_group_by_specialization
            && self.lazy_materialization.disable_lazy_materialization
            && self.simd.disable_simd
    }
}

// Implement Default for sub-configs
impl Default for BinaryProtocolConfig {
    fn default() -> Self {
        Self {
            disable_binary_protocol: false,
            binary_port: 5433,
            binary_max_message_size: 67108864,
            binary_thread_pool_size: 4,
            binary_compression_enabled: true,
            binary_read_timeout_secs: 30,
        }
    }
}

impl Default for ZoneMapConfig {
    fn default() -> Self {
        Self {
            disable_zone_maps: false,
            zone_map_threshold_rows: 10000000,
            zone_map_granule_size_bytes: 1048576,
            zone_map_recompute_threshold: 1000000,
            zone_map_max_skip_ratio_percent: 90,
        }
    }
}

impl Default for GroupByConfig {
    fn default() -> Self {
        Self {
            disable_group_by_specialization: false,
            group_by_column_pruning: true,
            group_by_specialization_threshold_ms: 100,
            group_by_hash_table_max_size: 10000000,
        }
    }
}

impl Default for LazyMaterializationConfig {
    fn default() -> Self {
        Self {
            disable_lazy_materialization: false,
            lazy_materialization_threshold_rows: 1000,
            lazy_materialization_memory_limit_bytes: 536870912,
            lazy_materialization_cleanup_enabled: true,
            lazy_materialization_cleanup_batch_size: 10000,
        }
    }
}

impl Default for SimdConfig {
    fn default() -> Self {
        Self {
            disable_simd: false,
            simd_target: SimdTarget::Auto,
            simd_correctness_check: false,
            simd_correctness_check_sample_ratio: 1000,
            simd_min_rows: 1000,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            metrics_port: 9090,
            metrics_enabled: true,
            health_check_enabled: true,
            health_check_detailed: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_enabled_config() {
        let cfg = OptimizationConfig::all_enabled();
        assert!(cfg.all_enabled_check());
    }

    #[test]
    fn test_all_disabled_config() {
        let cfg = OptimizationConfig::all_disabled();
        assert!(cfg.all_disabled_check());
    }

    #[test]
    fn test_default_config() {
        let cfg = OptimizationConfig::default();
        // Default should have most optimizations enabled
        assert!(!cfg.binary_protocol.disable_binary_protocol);
        assert!(!cfg.simd.disable_simd);
    }

    #[test]
    fn test_simd_target_variants() {
        assert_eq!(SimdTarget::Auto, SimdTarget::Auto);
        assert_ne!(SimdTarget::Avx512, SimdTarget::Scalar);
    }
}
