//! Optimization-specific metrics for Phase 4.
//!
//! Tracks performance and effectiveness of:
//! - Binary protocol (wire optimization)
//! - Zone maps (granule-level pruning)
//! - GROUP BY specialization (type-specialized paths)
//! - Lazy materialization (deferred evaluation)
//! - SIMD aggregates (vectorized operations)

use crate::metrics::{Counter, Gauge, Histogram};

/// Binary protocol metrics.
pub struct BinaryProtocolMetrics {
    pub connections_active: Gauge,
    pub latency_histogram: Histogram,  // microseconds: [1, 5, 10, 50, 100, 500, 1000, 5000]
    pub parse_errors_total: Counter,
    pub message_size_bytes: Histogram,  // bytes: [100, 1K, 10K, 100K, 1M]
    pub handshake_failures: Counter,
    pub protocol_violations: Counter,
}

impl BinaryProtocolMetrics {
    pub fn new() -> Self {
        Self {
            connections_active: Gauge::new(
                "nucleus_binary_connections_active",
                "Active binary protocol connections",
            ),
            latency_histogram: Histogram::new(
                "nucleus_binary_latency_microseconds",
                "Binary protocol query latency in microseconds",
                vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0],
            ),
            parse_errors_total: Counter::new(
                "nucleus_binary_parse_errors_total",
                "Total binary protocol parse errors",
            ),
            message_size_bytes: Histogram::new(
                "nucleus_binary_message_size_bytes",
                "Binary protocol message size in bytes",
                vec![100.0, 1000.0, 10000.0, 100000.0, 1000000.0],
            ),
            handshake_failures: Counter::new(
                "nucleus_binary_handshake_failures_total",
                "Binary protocol handshake failures",
            ),
            protocol_violations: Counter::new(
                "nucleus_binary_protocol_violations_total",
                "Binary protocol violations detected",
            ),
        }
    }

    pub fn record_query(&self, latency_us: f64) {
        self.latency_histogram.observe(latency_us);
    }

    pub fn record_message(&self, size_bytes: u64) {
        self.message_size_bytes.observe(size_bytes as f64);
    }
}

impl Default for BinaryProtocolMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Zone map metrics.
pub struct ZoneMapMetrics {
    pub granules_scanned_total: Counter,
    pub granules_skipped_total: Counter,
    pub skip_ratio_gauge: Gauge,  // percentage: 0-100
    pub recompute_operations: Counter,
    pub recompute_duration_seconds: Histogram,
}

impl ZoneMapMetrics {
    pub fn new() -> Self {
        Self {
            granules_scanned_total: Counter::new(
                "nucleus_zone_map_granules_scanned_total",
                "Total granules scanned",
            ),
            granules_skipped_total: Counter::new(
                "nucleus_zone_map_granules_skipped_total",
                "Total granules skipped by zone map",
            ),
            skip_ratio_gauge: Gauge::new(
                "nucleus_zone_map_skip_ratio_percent",
                "Percentage of granules skipped (0-100)",
            ),
            recompute_operations: Counter::new(
                "nucleus_zone_map_recompute_operations_total",
                "Zone map recomputation operations",
            ),
            recompute_duration_seconds: Histogram::new(
                "nucleus_zone_map_recompute_duration_seconds",
                "Zone map recomputation duration in seconds",
                vec![0.01, 0.1, 1.0, 10.0, 60.0],
            ),
        }
    }

    pub fn record_granule_scan(&self, skipped: bool) {
        self.granules_scanned_total.inc();
        if skipped {
            self.granules_skipped_total.inc();
        }
    }

    pub fn update_skip_ratio(&self, total: u64, skipped: u64) {
        if total > 0 {
            let ratio = (skipped as f64 / total as f64) * 100.0;
            self.skip_ratio_gauge.set(ratio as i64);
        }
    }

    pub fn record_recompute(&self, duration_secs: f64) {
        self.recompute_operations.inc();
        self.recompute_duration_seconds.observe(duration_secs);
    }

    pub fn record_granule_batch(&self, total: u64, skipped: u64) {
        for _ in 0..total {
            self.granules_scanned_total.inc();
        }
        for _ in 0..skipped {
            self.granules_skipped_total.inc();
        }
        self.update_skip_ratio(total, skipped);
    }
}

impl Default for ZoneMapMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// GROUP BY specialization metrics.
pub struct GroupByMetrics {
    pub specialized_queries_total: Counter,
    pub generic_fallback_total: Counter,
    pub specialization_type_int: Counter,
    pub specialization_type_string: Counter,
    pub specialization_type_float: Counter,
    pub specialization_type_decimal: Counter,
    pub mixed_type_fallback: Counter,
}

impl GroupByMetrics {
    pub fn new() -> Self {
        Self {
            specialized_queries_total: Counter::new(
                "nucleus_group_by_specialized_total",
                "GROUP BY queries using specialized path",
            ),
            generic_fallback_total: Counter::new(
                "nucleus_group_by_generic_fallback_total",
                "GROUP BY queries using generic fallback",
            ),
            specialization_type_int: Counter::new(
                "nucleus_group_by_int_specialization_total",
                "INTEGER GROUP BY specializations",
            ),
            specialization_type_string: Counter::new(
                "nucleus_group_by_string_specialization_total",
                "STRING GROUP BY specializations",
            ),
            specialization_type_float: Counter::new(
                "nucleus_group_by_float_specialization_total",
                "FLOAT GROUP BY specializations",
            ),
            specialization_type_decimal: Counter::new(
                "nucleus_group_by_decimal_specialization_total",
                "DECIMAL GROUP BY specializations",
            ),
            mixed_type_fallback: Counter::new(
                "nucleus_group_by_mixed_type_fallback_total",
                "GROUP BY with mixed types (fallback)",
            ),
        }
    }

    pub fn record_specialized(&self, data_type: &str) {
        self.specialized_queries_total.inc();
        match data_type {
            "int" | "bigint" | "smallint" => self.specialization_type_int.inc(),
            "varchar" | "text" | "char" => self.specialization_type_string.inc(),
            "float" | "double" => self.specialization_type_float.inc(),
            "decimal" | "numeric" => self.specialization_type_decimal.inc(),
            _ => {}
        }
    }

    pub fn record_fallback(&self, mixed_types: bool) {
        self.generic_fallback_total.inc();
        if mixed_types {
            self.mixed_type_fallback.inc();
        }
    }
}

impl Default for GroupByMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Lazy materialization metrics.
pub struct LazyMaterializationMetrics {
    pub materialized_rows_total: Counter,
    pub deferred_rows_total: Counter,
    pub memory_saved_bytes_total: Counter,
    pub materialization_ratio_gauge: Gauge,  // percentage
    pub materialization_duration_seconds: Histogram,
    pub deferred_cleanup_duration_seconds: Histogram,
}

impl LazyMaterializationMetrics {
    pub fn new() -> Self {
        Self {
            materialized_rows_total: Counter::new(
                "nucleus_lazy_materialization_materialized_rows_total",
                "Rows actually materialized",
            ),
            deferred_rows_total: Counter::new(
                "nucleus_lazy_materialization_deferred_rows_total",
                "Rows deferred from materialization",
            ),
            memory_saved_bytes_total: Counter::new(
                "nucleus_lazy_materialization_memory_saved_bytes_total",
                "Memory saved by deferring materialization",
            ),
            materialization_ratio_gauge: Gauge::new(
                "nucleus_lazy_materialization_ratio_percent",
                "Percentage of rows deferred (0-100)",
            ),
            materialization_duration_seconds: Histogram::new(
                "nucleus_lazy_materialization_duration_seconds",
                "Materialization duration in seconds",
                vec![0.001, 0.01, 0.1, 1.0, 10.0],
            ),
            deferred_cleanup_duration_seconds: Histogram::new(
                "nucleus_lazy_materialization_cleanup_duration_seconds",
                "Deferred row cleanup duration in seconds",
                vec![0.001, 0.01, 0.1, 1.0],
            ),
        }
    }

    pub fn record_materialization(&self, materialized: u64, deferred: u64, duration_secs: f64) {
        self.materialized_rows_total.inc_by(materialized);
        self.deferred_rows_total.inc_by(deferred);
        self.materialization_duration_seconds.observe(duration_secs);

        let total = materialized + deferred;
        if total > 0 {
            let ratio = (deferred as f64 / total as f64) * 100.0;
            self.materialization_ratio_gauge.set(ratio as i64);
        }
    }

    pub fn record_memory_saved(&self, bytes: u64) {
        self.memory_saved_bytes_total.inc_by(bytes);
    }

    pub fn record_cleanup(&self, duration_secs: f64) {
        self.deferred_cleanup_duration_seconds.observe(duration_secs);
    }
}

impl Default for LazyMaterializationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// SIMD aggregate metrics.
pub struct SimdMetrics {
    pub aggregates_executed_total: Counter,
    pub cpu_dispatch_avx512_total: Counter,
    pub cpu_dispatch_avx2_total: Counter,
    pub cpu_dispatch_sse42_total: Counter,
    pub cpu_dispatch_scalar_total: Counter,
    pub cpu_dispatch_ratio_avx512_gauge: Gauge,  // percentage
    pub cpu_dispatch_ratio_scalar_gauge: Gauge,  // percentage
    pub aggregate_duration_seconds: Histogram,
    pub correctness_mismatches_total: Counter,
}

impl SimdMetrics {
    pub fn new() -> Self {
        Self {
            aggregates_executed_total: Counter::new(
                "nucleus_simd_aggregates_executed_total",
                "Total SIMD aggregate operations",
            ),
            cpu_dispatch_avx512_total: Counter::new(
                "nucleus_simd_cpu_dispatch_avx512_total",
                "SIMD operations using AVX-512",
            ),
            cpu_dispatch_avx2_total: Counter::new(
                "nucleus_simd_cpu_dispatch_avx2_total",
                "SIMD operations using AVX-2",
            ),
            cpu_dispatch_sse42_total: Counter::new(
                "nucleus_simd_cpu_dispatch_sse42_total",
                "SIMD operations using SSE4.2",
            ),
            cpu_dispatch_scalar_total: Counter::new(
                "nucleus_simd_cpu_dispatch_scalar_total",
                "SIMD fallback to scalar operations",
            ),
            cpu_dispatch_ratio_avx512_gauge: Gauge::new(
                "nucleus_simd_cpu_dispatch_avx512_ratio_percent",
                "Percentage of SIMD using AVX-512",
            ),
            cpu_dispatch_ratio_scalar_gauge: Gauge::new(
                "nucleus_simd_cpu_dispatch_scalar_ratio_percent",
                "Percentage of SIMD fallback to scalar",
            ),
            aggregate_duration_seconds: Histogram::new(
                "nucleus_simd_aggregate_duration_seconds",
                "SIMD aggregate duration in seconds",
                vec![0.0001, 0.001, 0.01, 0.1, 1.0],
            ),
            correctness_mismatches_total: Counter::new(
                "nucleus_simd_correctness_mismatches_total",
                "SIMD results differing from scalar (ERROR)",
            ),
        }
    }

    pub fn record_aggregate(&self, dispatch: &str, duration_secs: f64) {
        self.aggregates_executed_total.inc();
        match dispatch {
            "avx512" => self.cpu_dispatch_avx512_total.inc(),
            "avx2" => self.cpu_dispatch_avx2_total.inc(),
            "sse42" => self.cpu_dispatch_sse42_total.inc(),
            "scalar" => self.cpu_dispatch_scalar_total.inc(),
            _ => {}
        }
        self.aggregate_duration_seconds.observe(duration_secs);
    }

    pub fn update_dispatch_ratios(&self, total: u64, avx512: u64, scalar: u64) {
        if total > 0 {
            let avx512_ratio = (avx512 as f64 / total as f64) * 100.0;
            let scalar_ratio = (scalar as f64 / total as f64) * 100.0;
            self.cpu_dispatch_ratio_avx512_gauge.set(avx512_ratio as i64);
            self.cpu_dispatch_ratio_scalar_gauge.set(scalar_ratio as i64);
        }
    }

    pub fn record_correctness_mismatch(&self) {
        self.correctness_mismatches_total.inc();
    }
}

impl Default for SimdMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter lazy materialization metrics (Phase 2C).
pub struct FilterLazyMetrics {
    pub filter_evaluations_total: Counter,
    pub positions_returned_total: Counter,
    pub filter_positions_memory_saved_bytes_total: Counter,
    pub filter_evaluation_time_us: Histogram,
    pub lazy_materialization_hit_rate_gauge: Gauge,  // percentage: (matches / total) * 100
}

impl FilterLazyMetrics {
    pub fn new() -> Self {
        Self {
            filter_evaluations_total: Counter::new(
                "nucleus_filter_lazy_evaluations_total",
                "Total WHERE filter evaluations using lazy materialization",
            ),
            positions_returned_total: Counter::new(
                "nucleus_filter_lazy_positions_returned_total",
                "Total row positions returned (instead of full rows)",
            ),
            filter_positions_memory_saved_bytes_total: Counter::new(
                "nucleus_filter_lazy_memory_saved_bytes_total",
                "Memory saved by returning positions instead of full rows",
            ),
            filter_evaluation_time_us: Histogram::new(
                "nucleus_filter_lazy_evaluation_time_us",
                "Filter evaluation time in microseconds",
                vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0],
            ),
            lazy_materialization_hit_rate_gauge: Gauge::new(
                "nucleus_filter_lazy_hit_rate_percent",
                "Percentage of rows matching filter (0-100)",
            ),
        }
    }

    pub fn record_filter_evaluation(&self, total_rows: u64, matching_rows: u64, duration_us: f64) {
        self.filter_evaluations_total.inc();
        self.positions_returned_total.inc_by(matching_rows);

        // Estimate: 100 bytes per non-matching row saved
        let non_matching = total_rows.saturating_sub(matching_rows);
        let memory_saved = non_matching * 100;
        self.filter_positions_memory_saved_bytes_total.inc_by(memory_saved);

        self.filter_evaluation_time_us.observe(duration_us);

        // Hit rate: matching / total * 100
        if total_rows > 0 {
            let hit_rate = (matching_rows as f64 / total_rows as f64) * 100.0;
            self.lazy_materialization_hit_rate_gauge.set(hit_rate as i64);
        }
    }
}

impl Default for FilterLazyMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined Phase 4 optimization metrics.
pub struct Phase4Metrics {
    pub binary_protocol: BinaryProtocolMetrics,
    pub zone_maps: ZoneMapMetrics,
    pub group_by: GroupByMetrics,
    pub lazy_materialization: LazyMaterializationMetrics,
    pub filter_lazy: FilterLazyMetrics,
    pub simd: SimdMetrics,
}

impl Phase4Metrics {
    pub fn new() -> Self {
        Self {
            binary_protocol: BinaryProtocolMetrics::new(),
            zone_maps: ZoneMapMetrics::new(),
            group_by: GroupByMetrics::new(),
            lazy_materialization: LazyMaterializationMetrics::new(),
            filter_lazy: FilterLazyMetrics::new(),
            simd: SimdMetrics::new(),
        }
    }
}

impl Default for Phase4Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_protocol_metrics() {
        let m = BinaryProtocolMetrics::new();
        m.connections_active.set(5);
        m.record_query(12.3);
        m.record_message(1024);
        m.parse_errors_total.inc();

        assert_eq!(m.connections_active.get(), 5);
        assert_eq!(m.parse_errors_total.get(), 1);
        assert_eq!(m.latency_histogram.count(), 1);
    }

    #[test]
    fn zone_map_metrics() {
        let m = ZoneMapMetrics::new();
        m.record_granule_scan(false);
        m.record_granule_scan(true);
        m.record_granule_scan(true);
        m.update_skip_ratio(3, 2);
        m.record_recompute(1.5);

        assert_eq!(m.granules_scanned_total.get(), 3);
        assert_eq!(m.granules_skipped_total.get(), 2);
        assert_eq!(m.skip_ratio_gauge.get(), 66);  // 2/3 * 100
        assert_eq!(m.recompute_operations.get(), 1);
    }

    #[test]
    fn group_by_metrics() {
        let m = GroupByMetrics::new();
        m.record_specialized("int");
        m.record_specialized("string");
        m.record_fallback(false);
        m.record_fallback(true);

        assert_eq!(m.specialized_queries_total.get(), 2);
        assert_eq!(m.generic_fallback_total.get(), 2);
        assert_eq!(m.specialization_type_int.get(), 1);
        assert_eq!(m.mixed_type_fallback.get(), 1);
    }

    #[test]
    fn lazy_materialization_metrics() {
        let m = LazyMaterializationMetrics::new();
        m.record_materialization(100, 900, 0.05);
        m.record_memory_saved(1024 * 1024);  // 1 MB
        m.record_cleanup(0.01);

        assert_eq!(m.materialized_rows_total.get(), 100);
        assert_eq!(m.deferred_rows_total.get(), 900);
        assert_eq!(m.memory_saved_bytes_total.get(), 1024 * 1024);
        assert_eq!(m.materialization_ratio_gauge.get(), 90);  // 900/1000 * 100
    }

    #[test]
    fn filter_lazy_metrics() {
        let m = FilterLazyMetrics::new();
        m.record_filter_evaluation(1000, 300, 50.5);
        m.record_filter_evaluation(1000, 100, 45.2);

        assert_eq!(m.filter_evaluations_total.get(), 2);
        assert_eq!(m.positions_returned_total.get(), 400);
        // 1000 - 300 = 700 non-matching * 100 = 70000 bytes
        // 1000 - 100 = 900 non-matching * 100 = 90000 bytes
        // Total: 160000 bytes saved
        assert_eq!(m.filter_positions_memory_saved_bytes_total.get(), 160000);
        assert_eq!(m.lazy_materialization_hit_rate_gauge.get(), 10);  // 100/1000 * 100 = 10%
    }

    #[test]
    fn simd_metrics() {
        let m = SimdMetrics::new();
        m.record_aggregate("avx512", 0.001);
        m.record_aggregate("avx512", 0.002);
        m.record_aggregate("scalar", 0.005);
        m.update_dispatch_ratios(3, 2, 1);

        assert_eq!(m.aggregates_executed_total.get(), 3);
        assert_eq!(m.cpu_dispatch_avx512_total.get(), 2);
        assert_eq!(m.cpu_dispatch_scalar_total.get(), 1);
        assert_eq!(m.cpu_dispatch_ratio_avx512_gauge.get(), 66);
        assert_eq!(m.cpu_dispatch_ratio_scalar_gauge.get(), 33);
    }
}
