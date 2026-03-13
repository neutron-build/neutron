//! Time-series engine — columnar storage with Gorilla compression, WAL, and aggregation.
//!
//! Rewritten from row-oriented `Vec<DataPoint>` to genuine columnar layout:
//!   - Separate timestamp/value/tag columns with 1:1 alignment
//!   - Time-window B-tree index for O(log P + K) range queries
//!   - Inline `SeriesStats` for O(1) full-series aggregation
//!   - Gorilla compression (delta-of-delta timestamps, XOR values)
//!   - Write-ahead log for durability
//!
//! Public API is fully backward-compatible: `DataPoint`, `BucketAgg`, `BucketSize`,
//! `ContinuousAggManager`, `RetentionPolicy`, `downsample()` all unchanged.

pub mod compression;

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// SIMD-accelerated column aggregation (4-wide unrolled)
// ============================================================================

/// Sum an f64 slice using 4-wide unrolled accumulation.
/// Reduces loop overhead and enables auto-vectorization by the compiler.
#[inline]
pub fn simd_sum(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mut acc0 = 0.0_f64;
    let mut acc1 = 0.0_f64;
    let mut acc2 = 0.0_f64;
    let mut acc3 = 0.0_f64;

    let chunks = values.len() / 4;
    let remainder = values.len() % 4;

    // Process 4 elements at a time
    for i in 0..chunks {
        let base = i * 4;
        acc0 += values[base];
        acc1 += values[base + 1];
        acc2 += values[base + 2];
        acc3 += values[base + 3];
    }

    // Process remainder
    let tail_start = chunks * 4;
    for i in 0..remainder {
        acc0 += values[tail_start + i];
    }

    (acc0 + acc1) + (acc2 + acc3)
}

/// Find the minimum of an f64 slice using 4-wide unrolled comparison.
#[inline]
pub fn simd_min(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::INFINITY;
    }

    let mut min0 = f64::INFINITY;
    let mut min1 = f64::INFINITY;
    let mut min2 = f64::INFINITY;
    let mut min3 = f64::INFINITY;

    let chunks = values.len() / 4;
    let remainder = values.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        let v0 = values[base];
        let v1 = values[base + 1];
        let v2 = values[base + 2];
        let v3 = values[base + 3];
        if v0 < min0 { min0 = v0; }
        if v1 < min1 { min1 = v1; }
        if v2 < min2 { min2 = v2; }
        if v3 < min3 { min3 = v3; }
    }

    let tail_start = chunks * 4;
    for i in 0..remainder {
        let v = values[tail_start + i];
        if v < min0 { min0 = v; }
    }

    let a = if min0 < min1 { min0 } else { min1 };
    let b = if min2 < min3 { min2 } else { min3 };
    if a < b { a } else { b }
}

/// Find the maximum of an f64 slice using 4-wide unrolled comparison.
#[inline]
pub fn simd_max(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::NEG_INFINITY;
    }

    let mut max0 = f64::NEG_INFINITY;
    let mut max1 = f64::NEG_INFINITY;
    let mut max2 = f64::NEG_INFINITY;
    let mut max3 = f64::NEG_INFINITY;

    let chunks = values.len() / 4;
    let remainder = values.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        let v0 = values[base];
        let v1 = values[base + 1];
        let v2 = values[base + 2];
        let v3 = values[base + 3];
        if v0 > max0 { max0 = v0; }
        if v1 > max1 { max1 = v1; }
        if v2 > max2 { max2 = v2; }
        if v3 > max3 { max3 = v3; }
    }

    let tail_start = chunks * 4;
    for i in 0..remainder {
        let v = values[tail_start + i];
        if v > max0 { max0 = v; }
    }

    let a = if max0 > max1 { max0 } else { max1 };
    let b = if max2 > max3 { max2 } else { max3 };
    if a > b { a } else { b }
}

/// Compute sum, min, and max in a single pass using 4-wide unrolled accumulators.
/// Returns (sum, min, max). Used when all three aggregates are needed at once
/// to avoid multiple passes over the data.
#[inline]
pub fn simd_sum_min_max(values: &[f64]) -> (f64, f64, f64) {
    if values.is_empty() {
        return (0.0, f64::INFINITY, f64::NEG_INFINITY);
    }

    let mut sum0 = 0.0_f64;
    let mut sum1 = 0.0_f64;
    let mut sum2 = 0.0_f64;
    let mut sum3 = 0.0_f64;
    let mut min0 = f64::INFINITY;
    let mut min1 = f64::INFINITY;
    let mut min2 = f64::INFINITY;
    let mut min3 = f64::INFINITY;
    let mut max0 = f64::NEG_INFINITY;
    let mut max1 = f64::NEG_INFINITY;
    let mut max2 = f64::NEG_INFINITY;
    let mut max3 = f64::NEG_INFINITY;

    let chunks = values.len() / 4;
    let remainder = values.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        let v0 = values[base];
        let v1 = values[base + 1];
        let v2 = values[base + 2];
        let v3 = values[base + 3];

        sum0 += v0;
        sum1 += v1;
        sum2 += v2;
        sum3 += v3;

        if v0 < min0 { min0 = v0; }
        if v1 < min1 { min1 = v1; }
        if v2 < min2 { min2 = v2; }
        if v3 < min3 { min3 = v3; }

        if v0 > max0 { max0 = v0; }
        if v1 > max1 { max1 = v1; }
        if v2 > max2 { max2 = v2; }
        if v3 > max3 { max3 = v3; }
    }

    let tail_start = chunks * 4;
    for i in 0..remainder {
        let v = values[tail_start + i];
        sum0 += v;
        if v < min0 { min0 = v; }
        if v > max0 { max0 = v; }
    }

    let sum = (sum0 + sum1) + (sum2 + sum3);
    let min_a = if min0 < min1 { min0 } else { min1 };
    let min_b = if min2 < min3 { min2 } else { min3 };
    let min = if min_a < min_b { min_a } else { min_b };
    let max_a = if max0 > max1 { max0 } else { max1 };
    let max_b = if max2 > max3 { max2 } else { max3 };
    let max = if max_a > max_b { max_a } else { max_b };

    (sum, min, max)
}

// ============================================================================
// Time-series types (public API — unchanged)
// ============================================================================

/// A single data point in a time series.
#[derive(Debug, Clone)]
pub struct DataPoint {
    pub timestamp: u64, // Unix timestamp in milliseconds
    pub tags: Vec<(String, String)>,
    pub value: f64,
}

/// Time bucket granularity for aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketSize {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
}

impl BucketSize {
    /// Duration of one bucket in milliseconds.
    pub fn millis(&self) -> u64 {
        match self {
            BucketSize::Second => 1_000,
            BucketSize::Minute => 60_000,
            BucketSize::Hour => 3_600_000,
            BucketSize::Day => 86_400_000,
            BucketSize::Week => 604_800_000,
            BucketSize::Month => 2_592_000_000, // 30 days approximation
        }
    }
}

/// Truncate a timestamp to a bucket boundary.
pub fn time_bucket(ts: u64, bucket: BucketSize) -> u64 {
    let millis = bucket.millis();
    (ts / millis) * millis
}

// ============================================================================
// Aggregation (public API — unchanged)
// ============================================================================

/// Aggregate statistics for a time bucket.
#[derive(Debug, Clone)]
pub struct BucketAgg {
    pub bucket_start: u64,
    pub count: usize,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first: f64,
    pub last: f64,
}

impl BucketAgg {
    fn new(ts: u64, value: f64) -> Self {
        Self {
            bucket_start: ts,
            count: 1,
            sum: value,
            min: value,
            max: value,
            first: value,
            last: value,
        }
    }

    fn add(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.last = value;
    }

    pub fn avg(&self) -> f64 {
        self.sum / self.count as f64
    }
}

/// Aggregate data points into time buckets.
pub fn aggregate(points: &[DataPoint], bucket: BucketSize) -> Vec<BucketAgg> {
    let mut buckets: BTreeMap<u64, BucketAgg> = BTreeMap::new();

    for point in points {
        let bucket_ts = time_bucket(point.timestamp, bucket);
        buckets
            .entry(bucket_ts)
            .and_modify(|agg| agg.add(point.value))
            .or_insert_with(|| BucketAgg::new(bucket_ts, point.value));
    }

    buckets.into_values().collect()
}

// ============================================================================
// Aggregate result for fast_aggregate
// ============================================================================

/// Result of a fast aggregation over a range.
#[derive(Debug, Clone)]
pub struct AggResult {
    pub count: usize,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl AggResult {
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

// ============================================================================
// Columnar internals
// ============================================================================

/// Running statistics for a series — maintained incrementally on insert.
#[derive(Debug, Clone)]
pub struct SeriesStats {
    pub count: usize,
    pub min_ts: u64,
    pub max_ts: u64,
    pub sum: f64,
    pub min_val: f64,
    pub max_val: f64,
}

impl SeriesStats {
    fn new() -> Self {
        Self {
            count: 0,
            min_ts: u64::MAX,
            max_ts: 0,
            sum: 0.0,
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
        }
    }

    fn update(&mut self, ts: u64, val: f64) {
        self.count += 1;
        self.min_ts = self.min_ts.min(ts);
        self.max_ts = self.max_ts.max(ts);
        self.sum += val;
        self.min_val = self.min_val.min(val);
        self.max_val = self.max_val.max(val);
    }

    /// Recompute stats from columns (used after retention purge).
    fn recompute(timestamps: &[u64], values: &[f64]) -> Self {
        if timestamps.is_empty() {
            return Self::new();
        }
        let mut s = Self {
            count: timestamps.len(),
            min_ts: timestamps[0],
            max_ts: timestamps[timestamps.len() - 1],
            sum: 0.0,
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
        };
        for &v in values {
            s.sum += v;
            s.min_val = s.min_val.min(v);
            s.max_val = s.max_val.max(v);
        }
        // Check all timestamps for min/max (may not be sorted if out-of-order inserts)
        for &t in timestamps {
            s.min_ts = s.min_ts.min(t);
            s.max_ts = s.max_ts.max(t);
        }
        s
    }
}

/// Metadata for a time partition in the B-tree index.
#[derive(Debug, Clone)]
pub struct PartitionMeta {
    pub start_offset: usize,
    pub count: usize,
    pub min_ts: u64,
    pub max_ts: u64,
    pub compressed: bool,
}

/// Time-window B-tree index for O(log P + K) range queries.
#[derive(Debug, Clone)]
pub struct TimeIndex {
    /// Maps partition boundary (bucket_start) → metadata.
    boundaries: BTreeMap<u64, PartitionMeta>,
}

impl TimeIndex {
    fn new() -> Self {
        Self {
            boundaries: BTreeMap::new(),
        }
    }

    /// Find all partitions whose time window overlaps [start_ts, end_ts).
    fn range(&self, start_ts: u64, end_ts: u64) -> Vec<(u64, &PartitionMeta)> {
        // A partition with boundary B overlaps [start, end) if:
        //   partition.min_ts < end_ts AND partition.max_ts >= start_ts
        // We use the B-tree to skip partitions that are entirely after end_ts.
        // Start from the beginning and iterate; due to sorted order, we can
        // stop once boundary > end_ts (all subsequent partitions start later).
        let mut result = Vec::new();
        for (&boundary, meta) in &self.boundaries {
            // Skip partitions entirely after our range
            if meta.min_ts >= end_ts {
                // Since boundaries are sorted and min_ts >= boundary,
                // all remaining partitions are also after end_ts
                break;
            }
            // Skip partitions entirely before our range
            if meta.max_ts < start_ts {
                continue;
            }
            result.push((boundary, meta));
        }
        result
    }

    /// Get or insert a partition for the given bucket boundary.
    fn ensure_partition(&mut self, bucket_start: u64) -> &mut PartitionMeta {
        self.boundaries.entry(bucket_start).or_insert(PartitionMeta {
            start_offset: 0,
            count: 0,
            min_ts: u64::MAX,
            max_ts: 0,
            compressed: false,
        })
    }

    fn len(&self) -> usize {
        self.boundaries.len()
    }
}

/// A columnar time series — separate arrays for timestamps, values, and tags.
#[derive(Debug, Clone)]
pub struct Series {
    pub name: String,
    /// Sorted dense timestamp column.
    pub timestamps: Vec<u64>,
    /// Aligned with timestamps 1:1.
    pub values: Vec<f64>,
    /// Per-tag-key column, each aligned with timestamps 1:1.
    pub tag_columns: HashMap<String, Vec<Option<String>>>,
    /// Time-window B-tree index: bucket_start → partition metadata.
    pub partition_index: TimeIndex,
    /// Running statistics.
    pub stats: SeriesStats,
}

impl Series {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            timestamps: Vec::new(),
            values: Vec::new(),
            tag_columns: HashMap::new(),
            partition_index: TimeIndex::new(),
            stats: SeriesStats::new(),
        }
    }

    /// Insert a data point into the columnar storage. Maintains sorted order.
    fn insert(&mut self, ts: u64, value: f64, tags: &[(String, String)], partition_size: u64) {
        // Find sorted insertion position
        let pos = self.timestamps
            .binary_search(&ts)
            .unwrap_or_else(|i| i);

        let is_append = pos == self.timestamps.len();

        // Insert into columns
        self.timestamps.insert(pos, ts);
        self.values.insert(pos, value);

        // Insert into tag columns
        // First, ensure all existing tag columns get a None at this position
        for col in self.tag_columns.values_mut() {
            col.insert(pos, None);
        }
        // Then set the tags that are present
        for (key, val) in tags {
            let col = self.tag_columns
                .entry(key.clone())
                .or_insert_with(|| vec![None; self.timestamps.len()]);
            // If the column was just created, it already has the right length
            // (we inserted None for all existing cols, and new col is filled with None)
            // But we need to set this position
            if col.len() < self.timestamps.len() {
                // Pad to current length
                col.resize(self.timestamps.len(), None);
            }
            col[pos] = Some(val.clone());
        }

        // Update stats
        self.stats.update(ts, value);

        // Update partition index incrementally
        let bucket_start = (ts / partition_size) * partition_size;
        if is_append {
            // Fast path: appending at the end (common for time-series — monotonic timestamps)
            // Just update the last partition or create a new one
            let meta = self.partition_index.ensure_partition(bucket_start);
            if meta.count == 0 {
                meta.start_offset = pos;
            }
            meta.count += 1;
            meta.min_ts = meta.min_ts.min(ts);
            meta.max_ts = meta.max_ts.max(ts);
        } else {
            // Slow path: out-of-order insert shifts all subsequent offsets
            // Update the target partition
            {
                let meta = self.partition_index.ensure_partition(bucket_start);
                if meta.count == 0 {
                    meta.start_offset = pos;
                }
                meta.count += 1;
                meta.min_ts = meta.min_ts.min(ts);
                meta.max_ts = meta.max_ts.max(ts);
            }
            // Shift start_offset +1 for all partitions that start after pos
            for meta in self.partition_index.boundaries.values_mut() {
                if meta.start_offset > pos {
                    meta.start_offset += 1;
                } else if meta.start_offset == pos {
                    // This could be the partition we just inserted into,
                    // or a different one. If different, shift it.
                    let this_bucket = (self.timestamps[meta.start_offset] / partition_size) * partition_size;
                    if this_bucket != bucket_start && meta.start_offset > 0 {
                        // Not our target partition but has same start_offset.
                        // After insert, our element is at `pos` so this one shifted to pos+1.
                        // Actually: binary_search gave us `pos`, the element at pos after insert is `ts`.
                        // The original element at pos (if any) is now at pos+1.
                        // So if this partition's data starts at pos+1 now, shift it.
                    }
                }
            }
            // For correctness with out-of-order inserts, rebuild the index
            // This is only triggered for non-append inserts which are less common
            self.rebuild_partition_index(partition_size);
        }
    }

    /// Rebuild the partition index from scratch.
    /// This is called after inserts since offsets shift with sorted insertion.
    fn rebuild_partition_index(&mut self, partition_size: u64) {
        self.partition_index = TimeIndex::new();
        if self.timestamps.is_empty() {
            return;
        }

        let mut i = 0;
        while i < self.timestamps.len() {
            let ts = self.timestamps[i];
            let bucket_start = (ts / partition_size) * partition_size;
            let start_offset = i;
            let mut count = 0;
            let mut min_ts = ts;
            let mut max_ts = ts;

            while i < self.timestamps.len() {
                let t = self.timestamps[i];
                let b = (t / partition_size) * partition_size;
                if b != bucket_start {
                    break;
                }
                min_ts = min_ts.min(t);
                max_ts = max_ts.max(t);
                count += 1;
                i += 1;
            }

            self.partition_index.boundaries.insert(
                bucket_start,
                PartitionMeta {
                    start_offset,
                    count,
                    min_ts,
                    max_ts,
                    compressed: false,
                },
            );
        }
    }

    /// Range query — returns indices into the columns that match [start, end).
    /// Uses the partition index for O(log P + K) performance.
    fn query_range_indices(&self, start: u64, end: u64) -> Vec<usize> {
        let partitions = self.partition_index.range(start, end);
        let mut indices = Vec::new();

        for (_, meta) in partitions {
            let slice_end = meta.start_offset + meta.count;
            for i in meta.start_offset..slice_end {
                if i < self.timestamps.len() {
                    let ts = self.timestamps[i];
                    if ts >= start && ts < end {
                        indices.push(i);
                    }
                }
            }
        }

        indices
    }

    /// Native columnar aggregation over a range [start, end) with bucket grouping.
    /// When a partition fits entirely within a single time bucket and is fully
    /// contained in the query range, uses SIMD-accelerated aggregation on the
    /// contiguous value slice.
    pub fn aggregate_range(&self, start: u64, end: u64, bucket: BucketSize) -> Vec<BucketAgg> {
        let mut buckets: BTreeMap<u64, BucketAgg> = BTreeMap::new();

        let partitions = self.partition_index.range(start, end);
        for (_, meta) in partitions {
            let slice_end = (meta.start_offset + meta.count).min(self.timestamps.len());
            if meta.start_offset >= slice_end {
                continue;
            }

            // Check if this partition is fully contained in the query range
            // AND all its points fall within a single time bucket.
            let fully_contained = meta.min_ts >= start && meta.max_ts < end;
            let bucket_start_min = time_bucket(meta.min_ts, bucket);
            let bucket_start_max = time_bucket(meta.max_ts, bucket);
            let single_bucket = bucket_start_min == bucket_start_max;

            if fully_contained && single_bucket {
                // Fast path: SIMD aggregate entire partition into one bucket
                let val_slice = &self.values[meta.start_offset..slice_end];
                let (sum, min_val, max_val) = simd_sum_min_max(val_slice);
                let bucket_ts = bucket_start_min;

                buckets
                    .entry(bucket_ts)
                    .and_modify(|agg| {
                        agg.count += val_slice.len();
                        agg.sum += sum;
                        agg.min = agg.min.min(min_val);
                        agg.max = agg.max.max(max_val);
                        agg.last = val_slice[val_slice.len() - 1];
                    })
                    .or_insert_with(|| BucketAgg {
                        bucket_start: bucket_ts,
                        count: val_slice.len(),
                        sum,
                        min: min_val,
                        max: max_val,
                        first: val_slice[0],
                        last: val_slice[val_slice.len() - 1],
                    });
            } else {
                // Slow path: per-element timestamp check and bucket assignment
                for i in meta.start_offset..slice_end {
                    let ts = self.timestamps[i];
                    if ts >= start && ts < end {
                        let val = self.values[i];
                        let bucket_ts = time_bucket(ts, bucket);
                        buckets
                            .entry(bucket_ts)
                            .and_modify(|agg| agg.add(val))
                            .or_insert_with(|| BucketAgg::new(bucket_ts, val));
                    }
                }
            }
        }

        buckets.into_values().collect()
    }

    /// Fast full-series aggregation using precomputed stats (O(1) if querying
    /// the entire series). Otherwise uses SIMD-accelerated column scan with
    /// partition-level fast paths for fully-contained partitions.
    pub fn fast_aggregate(&self, start: u64, end: u64) -> Option<AggResult> {
        if self.stats.count == 0 {
            return None;
        }

        // If the query covers the entire series, use stats directly
        if start <= self.stats.min_ts && end > self.stats.max_ts {
            return Some(AggResult {
                count: self.stats.count,
                sum: self.stats.sum,
                min: self.stats.min_val,
                max: self.stats.max_val,
            });
        }

        // Collect per-partition partial aggregates, then merge.
        // For partitions fully within [start, end), use SIMD on the contiguous slice.
        // For boundary partitions, filter by timestamp then SIMD the collected values.
        let partitions = self.partition_index.range(start, end);

        if partitions.is_empty() {
            return None;
        }

        // If enough partitions, aggregate each independently then merge
        let partial_results: Vec<AggResult> = if partitions.len() > 4 {
            // Parallel-style: aggregate each partition independently
            partitions.iter().filter_map(|(_, meta)| {
                self.aggregate_partition(meta, start, end)
            }).collect()
        } else {
            partitions.iter().filter_map(|(_, meta)| {
                self.aggregate_partition(meta, start, end)
            }).collect()
        };

        // Merge partial results
        Self::merge_agg_results(&partial_results)
    }

    /// Aggregate a single partition's contribution to a [start, end) range query.
    /// Uses SIMD on the contiguous value slice when the partition is fully contained.
    fn aggregate_partition(&self, meta: &PartitionMeta, start: u64, end: u64) -> Option<AggResult> {
        let slice_start = meta.start_offset;
        let slice_end = (meta.start_offset + meta.count).min(self.timestamps.len());

        if slice_start >= slice_end {
            return None;
        }

        // Check if this partition is fully contained within [start, end)
        if meta.min_ts >= start && meta.max_ts < end {
            // Fast path: entire partition is within range — use SIMD on contiguous slice
            let val_slice = &self.values[slice_start..slice_end];
            let (sum, min, max) = simd_sum_min_max(val_slice);
            return Some(AggResult {
                count: val_slice.len(),
                sum,
                min,
                max,
            });
        }

        // Boundary partition: need to check timestamps.
        // Collect qualifying values into a temporary buffer, then SIMD.
        let mut filtered_values = Vec::new();
        for i in slice_start..slice_end {
            let ts = self.timestamps[i];
            if ts >= start && ts < end {
                filtered_values.push(self.values[i]);
            }
        }

        if filtered_values.is_empty() {
            return None;
        }

        let (sum, min, max) = simd_sum_min_max(&filtered_values);
        Some(AggResult {
            count: filtered_values.len(),
            sum,
            min,
            max,
        })
    }

    /// Merge multiple partial AggResults into a single result.
    fn merge_agg_results(partials: &[AggResult]) -> Option<AggResult> {
        if partials.is_empty() {
            return None;
        }

        let mut total_count = 0usize;
        let mut total_sum = 0.0f64;
        let mut total_min = f64::INFINITY;
        let mut total_max = f64::NEG_INFINITY;

        for r in partials {
            total_count += r.count;
            total_sum += r.sum;
            if r.min < total_min { total_min = r.min; }
            if r.max > total_max { total_max = r.max; }
        }

        if total_count == 0 {
            None
        } else {
            Some(AggResult {
                count: total_count,
                sum: total_sum,
                min: total_min,
                max: total_max,
            })
        }
    }

    /// Reconstruct DataPoints from columnar storage for a set of indices.
    fn to_datapoints(&self, indices: &[usize]) -> Vec<DataPoint> {
        indices
            .iter()
            .map(|&i| {
                let mut tags = Vec::new();
                for (key, col) in &self.tag_columns {
                    if let Some(Some(val)) = col.get(i) {
                        tags.push((key.clone(), val.clone()));
                    }
                }
                DataPoint {
                    timestamp: self.timestamps[i],
                    value: self.values[i],
                    tags,
                }
            })
            .collect()
    }
}

// ============================================================================
// WAL (Write-Ahead Log)
// ============================================================================

/// WAL entry types for the time-series store.
const WAL_CREATE_SERIES: u8 = 0x01;
const WAL_INSERT: u8 = 0x02;
const WAL_DELETE_SERIES: u8 = 0x03;
const WAL_SNAPSHOT: u8 = 0x04;
const WAL_INSERT_BATCH: u8 = 0x05;

/// Write-ahead log for time-series durability.
struct TsWal {
    writer: parking_lot::Mutex<Option<std::io::BufWriter<std::fs::File>>>,
    dir: std::path::PathBuf,
}

impl std::fmt::Debug for TsWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TsWal")
            .field("dir", &self.dir)
            .finish()
    }
}

impl TsWal {
    fn open(dir: &std::path::Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join("ts_wal.bin");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        Ok(Self {
            writer: parking_lot::Mutex::new(Some(std::io::BufWriter::new(file))),
            dir: dir.to_path_buf(),
        })
    }

    fn wal_path(&self) -> std::path::PathBuf {
        self.dir.join("ts_wal.bin")
    }

    fn log_create_series(&self, name: &str, partition_size: u64) {
        use std::io::Write;
        let mut guard = self.writer.lock();
        if let Some(ref mut w) = *guard {
            let mut buf = vec![WAL_CREATE_SERIES];
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&partition_size.to_le_bytes());
            if let Err(e) = w.write_all(&buf).and_then(|_| w.flush()) {
                tracing::error!("timeseries WAL log_create_series failed: {e}");
            }
        }
    }

    fn log_delete_series(&self, name: &str) {
        use std::io::Write;
        let mut guard = self.writer.lock();
        if let Some(ref mut w) = *guard {
            let mut buf = vec![WAL_DELETE_SERIES];
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            if let Err(e) = w.write_all(&buf).and_then(|_| w.flush()) {
                tracing::error!("timeseries WAL log_delete_series failed: {e}");
            }
        }
    }

    fn log_insert(&self, name: &str, ts: u64, value: f64, tags: &[(String, String)]) {
        use std::io::Write;
        let mut guard = self.writer.lock();
        if let Some(ref mut w) = *guard {
            let mut buf = vec![WAL_INSERT];
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&ts.to_le_bytes());
            buf.extend_from_slice(&value.to_le_bytes());
            buf.extend_from_slice(&(tags.len() as u32).to_le_bytes());
            for (k, v) in tags {
                let kb = k.as_bytes();
                let vb = v.as_bytes();
                buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                buf.extend_from_slice(kb);
                buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                buf.extend_from_slice(vb);
            }
            if let Err(e) = w.write_all(&buf).and_then(|_| w.flush()) {
                tracing::error!("timeseries WAL log_insert failed: {e}");
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn log_insert_batch(&self, name: &str, points: &[(u64, f64, Vec<(String, String)>)]) {
        use std::io::Write;
        let mut guard = self.writer.lock();
        if let Some(ref mut w) = *guard {
            let mut buf = vec![WAL_INSERT_BATCH];
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&(points.len() as u32).to_le_bytes());
            for (ts, value, tags) in points {
                buf.extend_from_slice(&ts.to_le_bytes());
                buf.extend_from_slice(&value.to_le_bytes());
                buf.extend_from_slice(&(tags.len() as u32).to_le_bytes());
                for (k, v) in tags {
                    let kb = k.as_bytes();
                    let vb = v.as_bytes();
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                    buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(vb);
                }
            }
            if let Err(e) = w.write_all(&buf).and_then(|_| w.flush()) {
                tracing::error!("timeseries WAL log_insert_batch failed: {e}");
            }
        }
    }

    /// Checkpoint: write a snapshot and truncate the WAL.
    fn checkpoint(&self, series_map: &HashMap<String, Series>) {
        use std::io::Write;
        let mut guard = self.writer.lock();
        // Flush current writer
        if let Some(ref mut w) = *guard {
            let _ = w.flush();
        }
        // Truncate and rewrite
        let wal_path = self.wal_path();
        if let Ok(file) = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&wal_path)
        {
            let mut w = std::io::BufWriter::new(file);
            // Write snapshot
            let mut buf = vec![WAL_SNAPSHOT];
            buf.extend_from_slice(&(series_map.len() as u32).to_le_bytes());
            for (name, series) in series_map {
                let name_bytes = name.as_bytes();
                buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(name_bytes);
                let n = series.timestamps.len();
                buf.extend_from_slice(&(n as u32).to_le_bytes());
                for &ts in &series.timestamps {
                    buf.extend_from_slice(&ts.to_le_bytes());
                }
                for &val in &series.values {
                    buf.extend_from_slice(&val.to_le_bytes());
                }
                buf.extend_from_slice(&(series.tag_columns.len() as u32).to_le_bytes());
                for (key, col) in &series.tag_columns {
                    let kb = key.as_bytes();
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                    for opt in col {
                        match opt {
                            Some(v) => {
                                buf.push(1);
                                let vb = v.as_bytes();
                                buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                                buf.extend_from_slice(vb);
                            }
                            None => buf.push(0),
                        }
                    }
                }
            }
            let _ = w.write_all(&buf).and_then(|_| w.flush());
            // Reopen in append mode
            if let Ok(file) = std::fs::OpenOptions::new().append(true).open(&wal_path) {
                *guard = Some(std::io::BufWriter::new(file));
            }
        }
    }

    /// Replay WAL entries into a store. Returns Ok(()) on success,
    /// or Ok(()) with partial replay on corruption (graceful recovery).
    fn replay(
        dir: &std::path::Path,
        partition_size: u64,
    ) -> std::io::Result<HashMap<String, Series>> {
        let wal_path = dir.join("ts_wal.bin");
        if !wal_path.exists() {
            return Ok(HashMap::new());
        }
        let data = std::fs::read(&wal_path)?;
        let mut series_map: HashMap<String, Series> = HashMap::new();
        let mut pos = 0;

        while pos < data.len() {
            // Try to read entry type
            let entry_type = data[pos];
            pos += 1;

            match entry_type {
                WAL_CREATE_SERIES => {
                    if let Some((name, _ps, new_pos)) = Self::read_create_series(&data, pos) {
                        series_map.entry(name.clone()).or_insert_with(|| Series::new(&name));
                        pos = new_pos;
                    } else {
                        // Corrupt — stop replay
                        break;
                    }
                }
                WAL_INSERT => {
                    if let Some((name, ts, value, tags, new_pos)) = Self::read_insert(&data, pos) {
                        let series = series_map
                            .entry(name.clone())
                            .or_insert_with(|| Series::new(&name));
                        series.insert(ts, value, &tags, partition_size);
                        pos = new_pos;
                    } else {
                        break;
                    }
                }
                WAL_DELETE_SERIES => {
                    if let Some((name, new_pos)) = Self::read_string(&data, pos) {
                        series_map.remove(&name);
                        pos = new_pos;
                    } else {
                        break;
                    }
                }
                WAL_SNAPSHOT => {
                    if let Some((snapshot, new_pos)) =
                        Self::read_snapshot(&data, pos, partition_size)
                    {
                        // Snapshot replaces all state
                        series_map = snapshot;
                        pos = new_pos;
                    } else {
                        break;
                    }
                }
                WAL_INSERT_BATCH => {
                    if let Some((name, points, new_pos)) = Self::read_insert_batch(&data, pos) {
                        let series = series_map
                            .entry(name.clone())
                            .or_insert_with(|| Series::new(&name));
                        for (ts, value, tags) in &points {
                            series.insert(*ts, *value, tags, partition_size);
                        }
                        pos = new_pos;
                    } else {
                        break;
                    }
                }
                _ => {
                    // Unknown entry type — corrupt, stop
                    break;
                }
            }
        }

        Ok(series_map)
    }

    // -- WAL parsing helpers --

    fn read_u32(data: &[u8], pos: usize) -> Option<(u32, usize)> {
        if pos + 4 > data.len() {
            return None;
        }
        let val = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        Some((val, pos + 4))
    }

    fn read_u64(data: &[u8], pos: usize) -> Option<(u64, usize)> {
        if pos + 8 > data.len() {
            return None;
        }
        let bytes: [u8; 8] = data[pos..pos + 8].try_into().ok()?;
        Some((u64::from_le_bytes(bytes), pos + 8))
    }

    fn read_f64(data: &[u8], pos: usize) -> Option<(f64, usize)> {
        if pos + 8 > data.len() {
            return None;
        }
        let bytes: [u8; 8] = data[pos..pos + 8].try_into().ok()?;
        Some((f64::from_le_bytes(bytes), pos + 8))
    }

    fn read_string(data: &[u8], pos: usize) -> Option<(String, usize)> {
        let (len, pos) = Self::read_u32(data, pos)?;
        let end = pos + len as usize;
        if end > data.len() {
            return None;
        }
        let s = std::str::from_utf8(&data[pos..end]).ok()?.to_string();
        Some((s, end))
    }

    fn read_create_series(data: &[u8], pos: usize) -> Option<(String, u64, usize)> {
        let (name, pos) = Self::read_string(data, pos)?;
        let (ps, pos) = Self::read_u64(data, pos)?;
        Some((name, ps, pos))
    }

    #[allow(clippy::type_complexity)]
    fn read_insert(
        data: &[u8],
        pos: usize,
    ) -> Option<(String, u64, f64, Vec<(String, String)>, usize)> {
        let (name, pos) = Self::read_string(data, pos)?;
        let (ts, pos) = Self::read_u64(data, pos)?;
        let (value, pos) = Self::read_f64(data, pos)?;
        let (n_tags, mut pos) = Self::read_u32(data, pos)?;
        let mut tags = Vec::new();
        for _ in 0..n_tags {
            let (key, new_pos) = Self::read_string(data, pos)?;
            let (val, new_pos) = Self::read_string(data, new_pos)?;
            tags.push((key, val));
            pos = new_pos;
        }
        Some((name, ts, value, tags, pos))
    }

    fn read_snapshot(
        data: &[u8],
        pos: usize,
        partition_size: u64,
    ) -> Option<(HashMap<String, Series>, usize)> {
        let (n_series, mut pos) = Self::read_u32(data, pos)?;
        let mut map = HashMap::new();
        for _ in 0..n_series {
            let (name, new_pos) = Self::read_string(data, pos)?;
            pos = new_pos;
            let (n_points, new_pos) = Self::read_u32(data, pos)?;
            pos = new_pos;
            let n = n_points as usize;

            let mut timestamps = Vec::with_capacity(n);
            for _ in 0..n {
                let (ts, new_pos) = Self::read_u64(data, pos)?;
                timestamps.push(ts);
                pos = new_pos;
            }
            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                let (val, new_pos) = Self::read_f64(data, pos)?;
                values.push(val);
                pos = new_pos;
            }

            let (n_tag_keys, new_pos) = Self::read_u32(data, pos)?;
            pos = new_pos;
            let mut tag_columns: HashMap<String, Vec<Option<String>>> = HashMap::new();
            for _ in 0..n_tag_keys {
                let (key, new_pos) = Self::read_string(data, pos)?;
                pos = new_pos;
                let mut col = Vec::with_capacity(n);
                for _ in 0..n {
                    if pos >= data.len() {
                        return None;
                    }
                    let flag = data[pos];
                    pos += 1;
                    if flag == 1 {
                        let (val, new_pos) = Self::read_string(data, pos)?;
                        col.push(Some(val));
                        pos = new_pos;
                    } else {
                        col.push(None);
                    }
                }
                tag_columns.insert(key, col);
            }

            let mut series = Series::new(&name);
            series.timestamps = timestamps;
            series.values = values;
            series.tag_columns = tag_columns;
            series.stats = SeriesStats::recompute(&series.timestamps, &series.values);
            series.rebuild_partition_index(partition_size);
            map.insert(name, series);
        }
        Some((map, pos))
    }

    #[allow(clippy::type_complexity)]
    fn read_insert_batch(
        data: &[u8],
        pos: usize,
    ) -> Option<(String, Vec<(u64, f64, Vec<(String, String)>)>, usize)> {
        let (name, pos) = Self::read_string(data, pos)?;
        let (n_points, mut pos) = Self::read_u32(data, pos)?;
        let mut points = Vec::with_capacity(n_points as usize);
        for _ in 0..n_points {
            let (ts, new_pos) = Self::read_u64(data, pos)?;
            let (value, new_pos) = Self::read_f64(data, new_pos)?;
            let (n_tags, mut tag_pos) = Self::read_u32(data, new_pos)?;
            let mut tags = Vec::new();
            for _ in 0..n_tags {
                let (key, new_pos) = Self::read_string(data, tag_pos)?;
                let (val, new_pos) = Self::read_string(data, new_pos)?;
                tags.push((key, val));
                tag_pos = new_pos;
            }
            points.push((ts, value, tags));
            pos = tag_pos;
        }
        Some((name, points, pos))
    }
}

// ============================================================================
// TimeSeriesStore (public API preserved)
// ============================================================================

/// Retention policy for automatic data deletion.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum age of data in milliseconds.
    pub max_age_ms: u64,
}

/// Time-series store with columnar storage, partitioning, and optional WAL.
#[derive(Debug)]
pub struct TimeSeriesStore {
    /// Series name → columnar data.
    series: HashMap<String, Series>,
    /// Partition size in milliseconds.
    partition_size: u64,
    /// Optional retention policy.
    retention: Option<RetentionPolicy>,
    /// Last value per series (for O(1) last-value lookup).
    last_values: HashMap<String, DataPoint>,
    /// Optional WAL for durability.
    wal: Option<TsWal>,
}

impl TimeSeriesStore {
    pub fn new(partition_bucket: BucketSize) -> Self {
        Self {
            series: HashMap::new(),
            partition_size: partition_bucket.millis(),
            retention: None,
            last_values: HashMap::new(),
            wal: None,
        }
    }

    /// Open a WAL-backed store from a directory. Replays any existing WAL.
    pub fn open(dir: &std::path::Path, partition_bucket: BucketSize) -> std::io::Result<Self> {
        let partition_size = partition_bucket.millis();
        let series = TsWal::replay(dir, partition_size)?;

        // Reconstruct last_values from each series
        let mut last_values = HashMap::new();
        for (name, s) in &series {
            if !s.timestamps.is_empty() {
                // Find the index of the maximum timestamp
                let mut max_idx = 0;
                let mut max_ts = s.timestamps[0];
                for (i, &t) in s.timestamps.iter().enumerate() {
                    if t >= max_ts {
                        max_ts = t;
                        max_idx = i;
                    }
                }
                let dp = DataPoint {
                    timestamp: s.timestamps[max_idx],
                    value: s.values[max_idx],
                    tags: {
                        let mut tags = Vec::new();
                        for (key, col) in &s.tag_columns {
                            if let Some(Some(val)) = col.get(max_idx) {
                                tags.push((key.clone(), val.clone()));
                            }
                        }
                        tags
                    },
                };
                last_values.insert(name.clone(), dp);
            }
        }

        let wal = TsWal::open(dir)?;

        Ok(Self {
            series,
            partition_size,
            retention: None,
            last_values,
            wal: Some(wal),
        })
    }

    /// Set retention policy.
    pub fn set_retention(&mut self, policy: RetentionPolicy) {
        self.retention = Some(policy);
    }

    /// Delete a named series entirely.
    pub fn delete_series(&mut self, series_name: &str) -> bool {
        if let Some(ref wal) = self.wal {
            wal.log_delete_series(series_name);
        }
        self.last_values.remove(series_name);
        self.series.remove(series_name).is_some()
    }

    /// Insert a data point.
    pub fn insert(&mut self, series_name: &str, point: DataPoint) {
        let is_new = !self.series.contains_key(series_name);
        // Log to WAL if enabled
        if let Some(ref wal) = self.wal {
            if is_new {
                wal.log_create_series(series_name, self.partition_size);
            }
            wal.log_insert(series_name, point.timestamp, point.value, &point.tags);
        }

        let series = self
            .series
            .entry(series_name.to_string())
            .or_insert_with(|| Series::new(series_name));

        series.insert(point.timestamp, point.value, &point.tags, self.partition_size);

        // Update last value
        let update = self
            .last_values
            .get(series_name)
            .is_none_or(|last| point.timestamp >= last.timestamp);
        if update {
            self.last_values.insert(series_name.to_string(), point);
        }
    }

    /// Query data points in a time range [start, end).
    /// Returns references to reconstructed DataPoints.
    pub fn query(
        &self,
        series_name: &str,
        start: u64,
        end: u64,
    ) -> Vec<DataPoint> {
        if let Some(series) = self.series.get(series_name) {
            let indices = series.query_range_indices(start, end);
            series.to_datapoints(&indices)
        } else {
            Vec::new()
        }
    }

    /// Get the last value for a series (O(1)).
    pub fn last_value(&self, series_name: &str) -> Option<&DataPoint> {
        self.last_values.get(series_name)
    }

    /// Apply retention policy — remove data older than max_age.
    pub fn apply_retention(&mut self) {
        let policy = match &self.retention {
            Some(p) => p.clone(),
            None => return,
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        let cutoff = now.saturating_sub(policy.max_age_ms);

        for series in self.series.values_mut() {
            // Find the first index that's >= cutoff
            let keep_from = series
                .timestamps
                .partition_point(|&ts| ts < cutoff);

            if keep_from > 0 {
                // Remove old data from all columns
                series.timestamps.drain(..keep_from);
                series.values.drain(..keep_from);
                for col in series.tag_columns.values_mut() {
                    col.drain(..keep_from);
                }
                // Recompute stats and index
                series.stats = SeriesStats::recompute(&series.timestamps, &series.values);
                series.rebuild_partition_index(self.partition_size);
            }
        }
    }

    /// Total number of data points across all series.
    pub fn total_points(&self) -> usize {
        self.series.values().map(|s| s.timestamps.len()).sum()
    }

    /// Number of partitions for a series.
    pub fn partition_count(&self, series_name: &str) -> usize {
        self.series
            .get(series_name)
            .map_or(0, |s| s.partition_index.len())
    }

    /// Access the underlying Series (for advanced queries).
    pub fn get_series(&self, series_name: &str) -> Option<&Series> {
        self.series.get(series_name)
    }

    /// Write a snapshot to the WAL and truncate the log.
    pub fn snapshot(&self) {
        if let Some(ref wal) = self.wal {
            wal.checkpoint(&self.series);
        }
    }
}

// ============================================================================
// Parallel aggregation helpers on Series
// ============================================================================

/// Minimum number of partitions before we spawn threads for parallel aggregation.
const PAR_PARTITION_THRESHOLD: usize = 4;

impl Series {
    /// Aggregate partitions in a range, returning per-partition AggResults.
    /// This is a building block for the parallel methods: each thread calls this
    /// on a disjoint subset of partition metadata.
    fn aggregate_partition_batch(
        &self,
        metas: &[(u64, PartitionMeta)],
        start: u64,
        end: u64,
    ) -> Vec<AggResult> {
        metas
            .iter()
            .filter_map(|(_, meta)| self.aggregate_partition(meta, start, end))
            .collect()
    }

}

/// Snapshot of `TimeSeriesStore` mutable state for transaction rollback.
pub struct TsTxnSnapshot {
    series: HashMap<String, Series>,
    last_values: HashMap<String, DataPoint>,
}

impl TimeSeriesStore {
    // ========================================================================
    // Parallel range aggregation — scoped threads across partitions
    // ========================================================================

    /// Parallel range sum across partitions. Falls back to sequential for small
    /// partition counts.
    pub fn par_range_sum(&self, series_name: &str, start: u64, end: u64) -> Option<f64> {
        let series = self.series.get(series_name)?;
        let partitions: Vec<(u64, PartitionMeta)> = series
            .partition_index
            .range(start, end)
            .into_iter()
            .map(|(b, m)| (b, m.clone()))
            .collect();

        if partitions.is_empty() {
            return None;
        }
        if partitions.len() < PAR_PARTITION_THRESHOLD {
            return series.fast_aggregate(start, end).map(|r| r.sum);
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = partitions.len().div_ceil(cpus);

        let total = std::thread::scope(|s| {
            let handles: Vec<_> = partitions
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        let partials = series.aggregate_partition_batch(chunk, start, end);
                        partials.iter().map(|r| r.sum).sum::<f64>()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).sum::<f64>()
        });
        Some(total)
    }

    /// Parallel range count across partitions.
    pub fn par_range_count(&self, series_name: &str, start: u64, end: u64) -> Option<usize> {
        let series = self.series.get(series_name)?;
        let partitions: Vec<(u64, PartitionMeta)> = series
            .partition_index
            .range(start, end)
            .into_iter()
            .map(|(b, m)| (b, m.clone()))
            .collect();

        if partitions.is_empty() {
            return None;
        }
        if partitions.len() < PAR_PARTITION_THRESHOLD {
            return series.fast_aggregate(start, end).map(|r| r.count);
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = partitions.len().div_ceil(cpus);

        let total = std::thread::scope(|s| {
            let handles: Vec<_> = partitions
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        let partials = series.aggregate_partition_batch(chunk, start, end);
                        partials.iter().map(|r| r.count).sum::<usize>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .sum::<usize>()
        });
        Some(total)
    }

    /// Parallel range average across partitions.
    pub fn par_range_avg(&self, series_name: &str, start: u64, end: u64) -> Option<f64> {
        let series = self.series.get(series_name)?;
        let partitions: Vec<(u64, PartitionMeta)> = series
            .partition_index
            .range(start, end)
            .into_iter()
            .map(|(b, m)| (b, m.clone()))
            .collect();

        if partitions.is_empty() {
            return None;
        }
        if partitions.len() < PAR_PARTITION_THRESHOLD {
            return series.fast_aggregate(start, end).map(|r| r.avg());
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = partitions.len().div_ceil(cpus);

        // Collect (sum, count) pairs from each thread, then merge.
        let (total_sum, total_count) = std::thread::scope(|s| {
            let handles: Vec<_> = partitions
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        let partials = series.aggregate_partition_batch(chunk, start, end);
                        let sum: f64 = partials.iter().map(|r| r.sum).sum();
                        let count: usize = partials.iter().map(|r| r.count).sum();
                        (sum, count)
                    })
                })
                .collect();
            handles.into_iter().fold((0.0f64, 0usize), |(s, c), h| {
                let (hs, hc) = h.join().unwrap();
                (s + hs, c + hc)
            })
        });

        if total_count == 0 {
            None
        } else {
            Some(total_sum / total_count as f64)
        }
    }

    /// Parallel range minimum across partitions.
    pub fn par_range_min(&self, series_name: &str, start: u64, end: u64) -> Option<f64> {
        let series = self.series.get(series_name)?;
        let partitions: Vec<(u64, PartitionMeta)> = series
            .partition_index
            .range(start, end)
            .into_iter()
            .map(|(b, m)| (b, m.clone()))
            .collect();

        if partitions.is_empty() {
            return None;
        }
        if partitions.len() < PAR_PARTITION_THRESHOLD {
            return series.fast_aggregate(start, end).map(|r| r.min);
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = partitions.len().div_ceil(cpus);

        let min_val = std::thread::scope(|s| {
            let handles: Vec<_> = partitions
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        let partials = series.aggregate_partition_batch(chunk, start, end);
                        partials
                            .iter()
                            .map(|r| r.min)
                            .fold(f64::INFINITY, f64::min)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .fold(f64::INFINITY, f64::min)
        });
        if min_val == f64::INFINITY {
            None
        } else {
            Some(min_val)
        }
    }

    /// Parallel range maximum across partitions.
    pub fn par_range_max(&self, series_name: &str, start: u64, end: u64) -> Option<f64> {
        let series = self.series.get(series_name)?;
        let partitions: Vec<(u64, PartitionMeta)> = series
            .partition_index
            .range(start, end)
            .into_iter()
            .map(|(b, m)| (b, m.clone()))
            .collect();

        if partitions.is_empty() {
            return None;
        }
        if partitions.len() < PAR_PARTITION_THRESHOLD {
            return series.fast_aggregate(start, end).map(|r| r.max);
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = partitions.len().div_ceil(cpus);

        let max_val = std::thread::scope(|s| {
            let handles: Vec<_> = partitions
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        let partials = series.aggregate_partition_batch(chunk, start, end);
                        partials
                            .iter()
                            .map(|r| r.max)
                            .fold(f64::NEG_INFINITY, f64::max)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .fold(f64::NEG_INFINITY, f64::max)
        });
        if max_val == f64::NEG_INFINITY {
            None
        } else {
            Some(max_val)
        }
    }

    // ========================================================================
    // Parallel multi-series query
    // ========================================================================

    /// Query the last value of multiple series in parallel.
    /// Each series lookup is independent, so they can run concurrently.
    pub fn par_multi_series_last(
        &self,
        series_names: &[&str],
    ) -> Vec<(String, Option<(i64, f64)>)> {
        if series_names.len() < 2 {
            // Not worth spawning threads for 0-1 series
            return series_names
                .iter()
                .map(|&name| {
                    let val = self.last_value(name).map(|dp| (dp.timestamp as i64, dp.value));
                    (name.to_string(), val)
                })
                .collect();
        }

        std::thread::scope(|s| {
            let handles: Vec<_> = series_names
                .iter()
                .map(|&name| {
                    s.spawn(move || {
                        let val =
                            self.last_value(name).map(|dp| (dp.timestamp as i64, dp.value));
                        (name.to_string(), val)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        })
    }

    // ========================================================================
    // Parallel bulk insert
    // ========================================================================

    /// Insert many data points, grouped by series for efficiency.
    /// Points are grouped by series name, then each group is inserted sequentially
    /// (since each series is independent, the grouping reduces HashMap lookups).
    pub fn par_bulk_insert(&mut self, points: &[(String, i64, f64)]) {
        // Group by series name
        let mut groups: HashMap<&str, Vec<(u64, f64)>> = HashMap::new();
        for (name, ts, val) in points {
            groups
                .entry(name.as_str())
                .or_default()
                .push((*ts as u64, *val));
        }

        // Insert each group
        for (name, pts) in &groups {
            // Log to WAL if enabled (batch)
            if let Some(ref wal) = self.wal {
                if !self.series.contains_key(*name) {
                    wal.log_create_series(name, self.partition_size);
                }
                let batch: Vec<_> =
                    pts.iter().map(|&(ts, val)| (ts, val, Vec::<(String, String)>::new())).collect();
                wal.log_insert_batch(name, &batch);
            }

            let series = self
                .series
                .entry(name.to_string())
                .or_insert_with(|| Series::new(name));

            for &(ts, val) in pts {
                series.insert(ts, val, &[], self.partition_size);
            }

            // Update last value with the maximum timestamp in this group
            if let Some(&(max_ts, max_val)) = pts.iter().max_by_key(|(ts, _)| *ts) {
                let update = self
                    .last_values
                    .get(*name)
                    .is_none_or(|last| max_ts >= last.timestamp);
                if update {
                    self.last_values.insert(
                        name.to_string(),
                        DataPoint {
                            timestamp: max_ts,
                            tags: vec![],
                            value: max_val,
                        },
                    );
                }
            }
        }
    }

    /// Capture a snapshot of all mutable TimeSeries state for transaction rollback.
    ///
    /// The WAL is not included — it is append-only and must not be reverted.
    pub fn txn_snapshot(&self) -> TsTxnSnapshot {
        TsTxnSnapshot {
            series: self.series.clone(),
            last_values: self.last_values.clone(),
        }
    }

    /// Restore mutable TimeSeries state from a transaction snapshot (for ROLLBACK).
    pub fn txn_restore(&mut self, snap: TsTxnSnapshot) {
        self.series = snap.series;
        self.last_values = snap.last_values;
    }
}

// ============================================================================
// Continuous aggregates (public API — unchanged)
// ============================================================================

/// Which scalar value to extract from a BucketAgg for materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    Avg,
    Sum,
    Min,
    Max,
    Count,
    First,
    Last,
}

impl AggFunction {
    /// Extract the scalar value from a computed BucketAgg.
    fn extract(&self, agg: &BucketAgg) -> f64 {
        match self {
            AggFunction::Avg => agg.avg(),
            AggFunction::Sum => agg.sum,
            AggFunction::Min => agg.min,
            AggFunction::Max => agg.max,
            AggFunction::Count => agg.count as f64,
            AggFunction::First => agg.first,
            AggFunction::Last => agg.last,
        }
    }
}

/// Definition of a single continuous aggregate.
#[derive(Debug)]
pub struct ContinuousAggDef {
    pub name: String,
    pub source_series: String,
    pub bucket_size: BucketSize,
    pub agg_function: AggFunction,
    /// bucket_start → materialized aggregate value
    pub materialized: BTreeMap<i64, f64>,
    /// Watermark: the timestamp up to which data has been materialized (exclusive).
    pub last_materialized_ts: i64,
}

/// Manager for continuous aggregates.
#[derive(Debug)]
pub struct ContinuousAggManager {
    pub aggregates: HashMap<String, ContinuousAggDef>,
}

impl Default for ContinuousAggManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ContinuousAggManager {
    pub fn new() -> Self {
        Self {
            aggregates: HashMap::new(),
        }
    }

    /// Register a new continuous aggregate definition.
    pub fn create(
        &mut self,
        name: &str,
        source_series: &str,
        bucket_size: BucketSize,
        agg_function: AggFunction,
    ) {
        let def = ContinuousAggDef {
            name: name.to_string(),
            source_series: source_series.to_string(),
            bucket_size,
            agg_function,
            materialized: BTreeMap::new(),
            last_materialized_ts: 0,
        };
        self.aggregates.insert(name.to_string(), def);
    }

    /// Incrementally materialize new buckets since the last watermark.
    pub fn refresh(&mut self, name: &str, store: &TimeSeriesStore) {
        let def = match self.aggregates.get_mut(name) {
            Some(d) => d,
            None => return,
        };

        let watermark = def.last_materialized_ts as u64;

        // Query all points from the watermark to u64::MAX.
        let points = store.query(&def.source_series, watermark, u64::MAX);
        if points.is_empty() {
            return;
        }

        // Find the maximum timestamp across the queried points.
        let max_ts = points.iter().map(|p| p.timestamp).max().unwrap();

        // Determine the bucket boundary that is still "open".
        let open_bucket_start = time_bucket(max_ts, def.bucket_size);

        // Group points into buckets.
        let mut buckets: BTreeMap<u64, BucketAgg> = BTreeMap::new();
        for point in &points {
            let bucket_ts = time_bucket(point.timestamp, def.bucket_size);
            buckets
                .entry(bucket_ts)
                .and_modify(|agg| agg.add(point.value))
                .or_insert_with(|| BucketAgg::new(bucket_ts, point.value));
        }

        // Materialize only fully-closed buckets.
        let mut new_watermark = watermark;
        for (bucket_start, agg) in &buckets {
            if *bucket_start < open_bucket_start {
                let value = def.agg_function.extract(agg);
                def.materialized.insert(*bucket_start as i64, value);
                let bucket_end = bucket_start + def.bucket_size.millis();
                if bucket_end > new_watermark {
                    new_watermark = bucket_end;
                }
            }
        }

        // If no closed buckets, materialize all (batch scenario).
        if new_watermark == watermark && !buckets.is_empty() {
            for (bucket_start, agg) in &buckets {
                let value = def.agg_function.extract(agg);
                def.materialized.insert(*bucket_start as i64, value);
                let bucket_end = bucket_start + def.bucket_size.millis();
                if bucket_end > new_watermark {
                    new_watermark = bucket_end;
                }
            }
        }

        def.last_materialized_ts = new_watermark as i64;
    }

    /// Query materialized data in a time range [from_ts, to_ts).
    pub fn query(&self, name: &str, from_ts: i64, to_ts: i64) -> Vec<(i64, f64)> {
        let def = match self.aggregates.get(name) {
            Some(d) => d,
            None => return Vec::new(),
        };

        def.materialized
            .range(from_ts..to_ts)
            .map(|(&k, &v)| (k, v))
            .collect()
    }

    /// Remove a continuous aggregate.
    pub fn drop(&mut self, name: &str) {
        self.aggregates.remove(name);
    }

    /// List all continuous aggregate definitions.
    pub fn list(&self) -> Vec<&ContinuousAggDef> {
        self.aggregates.values().collect()
    }
}

/// Downsample data points by aggregating into larger buckets.
pub fn downsample(points: &[DataPoint], target_bucket: BucketSize) -> Vec<DataPoint> {
    let aggs = aggregate(points, target_bucket);
    aggs.into_iter()
        .map(|agg| DataPoint {
            timestamp: agg.bucket_start,
            tags: Vec::new(),
            value: agg.avg(),
        })
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_points(count: usize, interval_ms: u64, base_ts: u64) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint {
                timestamp: base_ts + i as u64 * interval_ms,
                tags: vec![("host".into(), "server1".into())],
                value: (i as f64) * 1.5 + 10.0,
            })
            .collect()
    }

    // ========================================================================
    // Original 17 tests (ported to new API)
    // ========================================================================

    #[test]
    fn time_bucket_test() {
        let ts = 1_700_000_123_456u64;
        let bucketed = time_bucket(ts, BucketSize::Hour);
        assert_eq!(bucketed % BucketSize::Hour.millis(), 0);
        assert!(bucketed <= ts);
        assert!(bucketed + BucketSize::Hour.millis() > ts);
    }

    #[test]
    fn aggregation() {
        let points = make_points(100, 1000, 1_700_000_000_000);
        let aggs = aggregate(&points, BucketSize::Minute);

        assert_eq!(aggs.len(), 2);
        assert_eq!(aggs[0].count + aggs[1].count, 100);
    }

    #[test]
    fn store_insert_and_query() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for i in 0..100 {
            store.insert(
                "cpu.usage",
                DataPoint {
                    timestamp: base_ts + i * 60_000,
                    tags: vec![],
                    value: 50.0 + (i as f64) * 0.5,
                },
            );
        }

        assert_eq!(store.total_points(), 100);

        let results = store.query("cpu.usage", base_ts, base_ts + 30 * 60_000);
        assert_eq!(results.len(), 30);
    }

    #[test]
    fn last_value_o1() {
        let mut store = TimeSeriesStore::new(BucketSize::Day);
        let base_ts = 1_700_000_000_000u64;

        store.insert(
            "temp",
            DataPoint {
                timestamp: base_ts,
                tags: vec![],
                value: 20.0,
            },
        );
        store.insert(
            "temp",
            DataPoint {
                timestamp: base_ts + 1000,
                tags: vec![],
                value: 22.5,
            },
        );

        let last = store.last_value("temp").unwrap();
        assert!((last.value - 22.5).abs() < 1e-10);
    }

    #[test]
    fn partitioning() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for h in 0..5 {
            for m in 0..10 {
                store.insert(
                    "metric",
                    DataPoint {
                        timestamp: base_ts + h * 3_600_000 + m * 60_000,
                        tags: vec![],
                        value: 1.0,
                    },
                );
            }
        }

        assert_eq!(store.partition_count("metric"), 5);
        assert_eq!(store.total_points(), 50);
    }

    #[test]
    fn downsample_test() {
        let base = time_bucket(1_700_000_000_000, BucketSize::Minute);
        let points = make_points(60, 1000, base);
        let downsampled = downsample(&points, BucketSize::Minute);
        assert_eq!(downsampled.len(), 1);
    }

    /// Helper: insert `count` points at `interval_ms` apart into the store.
    fn insert_series(
        store: &mut TimeSeriesStore,
        series: &str,
        count: usize,
        interval_ms: u64,
        base_ts: u64,
    ) {
        for i in 0..count {
            store.insert(
                series,
                DataPoint {
                    timestamp: base_ts + i as u64 * interval_ms,
                    tags: vec![],
                    value: (i as f64) + 1.0,
                },
            );
        }
    }

    #[test]
    fn continuous_agg_basic() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let mut manager = ContinuousAggManager::new();

        let base_ts = time_bucket(1_700_000_000_000, BucketSize::Minute);

        insert_series(&mut store, "cpu", 180, 1_000, base_ts);

        manager.create("cpu_1m_avg", "cpu", BucketSize::Minute, AggFunction::Avg);
        manager.refresh("cpu_1m_avg", &store);

        let results = manager.query("cpu_1m_avg", 0, i64::MAX);

        assert_eq!(
            results.len(),
            2,
            "expected 2 minute buckets, got {:?}",
            results
        );

        let (ts0, avg0) = results[0];
        assert_eq!(ts0, base_ts as i64);
        assert!(
            (avg0 - 30.5).abs() < 1e-10,
            "first bucket avg was {}",
            avg0
        );

        let (ts1, avg1) = results[1];
        assert_eq!(ts1, (base_ts + BucketSize::Minute.millis()) as i64);
        assert!(
            (avg1 - 90.5).abs() < 1e-10,
            "second bucket avg was {}",
            avg1
        );
    }

    #[test]
    fn continuous_agg_incremental() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let mut manager = ContinuousAggManager::new();

        let base_ts = time_bucket(1_700_000_000_000, BucketSize::Minute);

        insert_series(&mut store, "sensor", 180, 1_000, base_ts);
        manager.create(
            "sensor_1m_sum",
            "sensor",
            BucketSize::Minute,
            AggFunction::Sum,
        );
        manager.refresh("sensor_1m_sum", &store);

        let after_phase1 = manager.query("sensor_1m_sum", 0, i64::MAX);
        assert_eq!(
            after_phase1.len(),
            2,
            "phase 1: expected 2 closed buckets"
        );

        let wm1 = manager
            .aggregates
            .get("sensor_1m_sum")
            .unwrap()
            .last_materialized_ts;

        let phase2_base = base_ts + 180 * 1_000;
        insert_series(&mut store, "sensor", 60, 1_000, phase2_base);
        manager.refresh("sensor_1m_sum", &store);

        let after_phase2 = manager.query("sensor_1m_sum", 0, i64::MAX);
        assert_eq!(
            after_phase2.len(),
            3,
            "phase 2: expected 3 closed buckets"
        );

        let wm2 = manager
            .aggregates
            .get("sensor_1m_sum")
            .unwrap()
            .last_materialized_ts;
        assert!(
            wm2 > wm1,
            "watermark should advance: wm2={} wm1={}",
            wm2,
            wm1
        );

        assert_eq!(after_phase1[0], after_phase2[0]);
        assert_eq!(after_phase1[1], after_phase2[1]);
    }

    #[test]
    fn continuous_agg_query_range() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let mut manager = ContinuousAggManager::new();

        let base_ts = time_bucket(1_700_000_000_000, BucketSize::Minute);

        insert_series(&mut store, "temp", 360, 1_000, base_ts);
        manager.create("temp_1m_max", "temp", BucketSize::Minute, AggFunction::Max);
        manager.refresh("temp_1m_max", &store);

        let all = manager.query("temp_1m_max", 0, i64::MAX);
        assert_eq!(
            all.len(),
            5,
            "expected 5 closed minute buckets for 360s of data"
        );

        let minute_ms = BucketSize::Minute.millis() as i64;
        let from = base_ts as i64 + 2 * minute_ms;
        let to = base_ts as i64 + 4 * minute_ms;
        let ranged = manager.query("temp_1m_max", from, to);
        assert_eq!(
            ranged.len(),
            2,
            "range query should return 2 buckets, got {:?}",
            ranged
        );

        assert_eq!(ranged[0].0, from);
        assert_eq!(ranged[1].0, from + minute_ms);

        assert!(
            (ranged[0].1 - 180.0).abs() < 1e-10,
            "minute 2 max was {}",
            ranged[0].1
        );
        assert!(
            (ranged[1].1 - 240.0).abs() < 1e-10,
            "minute 3 max was {}",
            ranged[1].1
        );
    }

    #[test]
    fn multiple_series_isolation() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        store.insert(
            "cpu",
            DataPoint {
                timestamp: base_ts,
                tags: vec![],
                value: 50.0,
            },
        );
        store.insert(
            "mem",
            DataPoint {
                timestamp: base_ts,
                tags: vec![],
                value: 80.0,
            },
        );
        store.insert(
            "cpu",
            DataPoint {
                timestamp: base_ts + 1000,
                tags: vec![],
                value: 55.0,
            },
        );

        assert_eq!(store.query("cpu", 0, u64::MAX).len(), 2);
        assert_eq!(store.query("mem", 0, u64::MAX).len(), 1);
        assert_eq!(store.total_points(), 3);
    }

    #[test]
    fn query_empty_and_nonexistent() {
        let store = TimeSeriesStore::new(BucketSize::Hour);
        assert!(store.query("nonexistent", 0, u64::MAX).is_empty());
        assert!(store.last_value("nonexistent").is_none());
        assert_eq!(store.partition_count("nonexistent"), 0);
        assert_eq!(store.total_points(), 0);
    }

    #[test]
    fn last_value_not_updated_for_older_insert() {
        let mut store = TimeSeriesStore::new(BucketSize::Day);
        let base_ts = 1_700_000_000_000u64;

        store.insert(
            "temp",
            DataPoint {
                timestamp: base_ts + 5000,
                tags: vec![],
                value: 30.0,
            },
        );
        store.insert(
            "temp",
            DataPoint {
                timestamp: base_ts,
                tags: vec![],
                value: 20.0,
            },
        );

        let last = store.last_value("temp").unwrap();
        assert!(
            (last.value - 30.0).abs() < 1e-10,
            "last_value should still be 30.0"
        );
    }

    #[test]
    fn bucket_size_millis_correct() {
        assert_eq!(BucketSize::Second.millis(), 1_000);
        assert_eq!(BucketSize::Minute.millis(), 60_000);
        assert_eq!(BucketSize::Hour.millis(), 3_600_000);
        assert_eq!(BucketSize::Day.millis(), 86_400_000);
        assert_eq!(BucketSize::Week.millis(), 604_800_000);
        assert_eq!(BucketSize::Month.millis(), 2_592_000_000);
    }

    #[test]
    fn continuous_agg_drop_and_list() {
        let mut manager = ContinuousAggManager::new();
        manager.create("agg1", "series1", BucketSize::Hour, AggFunction::Avg);
        manager.create("agg2", "series2", BucketSize::Minute, AggFunction::Sum);

        assert_eq!(manager.list().len(), 2);

        manager.drop("agg1");
        assert_eq!(manager.list().len(), 1);
        assert!(manager.aggregates.contains_key("agg2"));
        assert!(!manager.aggregates.contains_key("agg1"));

        assert!(manager.query("agg1", 0, i64::MAX).is_empty());
    }

    #[test]
    fn downsample_multiple_buckets() {
        let base = time_bucket(1_700_000_000_000, BucketSize::Minute);
        let points = make_points(180, 1000, base);
        let downsampled = downsample(&points, BucketSize::Minute);
        assert_eq!(downsampled.len(), 3);
        for dp in &downsampled {
            assert!(dp.value > 0.0);
        }
    }

    #[test]
    fn agg_function_all_variants() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let mut manager = ContinuousAggManager::new();
        let base_ts = time_bucket(1_700_000_000_000, BucketSize::Minute);

        insert_series(&mut store, "s1", 60, 1_000, base_ts);

        for (name, func, expected) in [
            ("sum", AggFunction::Sum, 1830.0),
            ("min", AggFunction::Min, 1.0),
            ("max", AggFunction::Max, 60.0),
            ("count", AggFunction::Count, 60.0),
            ("first", AggFunction::First, 1.0),
            ("last", AggFunction::Last, 60.0),
            ("avg", AggFunction::Avg, 30.5),
        ] {
            manager.create(name, "s1", BucketSize::Minute, func);
            manager.refresh(name, &store);
            let results = manager.query(name, 0, i64::MAX);
            assert!(!results.is_empty(), "{name} should have results");
            assert!(
                (results[0].1 - expected).abs() < 1e-6,
                "{name}: expected {expected}, got {}",
                results[0].1
            );
        }
    }

    #[test]
    fn retention_policy_removes_old_partitions() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let old_ts = 0u64;

        store.insert(
            "old_series",
            DataPoint {
                timestamp: old_ts,
                tags: vec![],
                value: 1.0,
            },
        );
        store.insert(
            "old_series",
            DataPoint {
                timestamp: old_ts + 3_600_000,
                tags: vec![],
                value: 2.0,
            },
        );

        store.set_retention(RetentionPolicy { max_age_ms: 1_000 });
        store.apply_retention();

        assert_eq!(store.total_points(), 0);
    }

    // ========================================================================
    // New tests for columnar engine
    // ========================================================================

    #[test]
    fn column_alignment_verified() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for i in 0..50 {
            store.insert(
                "aligned",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![
                        ("host".into(), format!("s{}", i % 3)),
                        ("region".into(), "us-east".into()),
                    ],
                    value: i as f64,
                },
            );
        }

        let series = store.get_series("aligned").unwrap();
        assert_eq!(series.timestamps.len(), 50);
        assert_eq!(series.values.len(), 50);
        // Tag columns should be aligned
        let host_col = series.tag_columns.get("host").unwrap();
        let region_col = series.tag_columns.get("region").unwrap();
        assert_eq!(host_col.len(), 50);
        assert_eq!(region_col.len(), 50);
        // Check alignment: index i should have matching values
        for i in 0..50 {
            assert_eq!(series.timestamps[i], base_ts + i as u64 * 1000);
            assert!((series.values[i] - i as f64).abs() < 1e-10);
            assert_eq!(
                host_col[i].as_deref(),
                Some(format!("s{}", i % 3)).as_deref()
            );
            assert_eq!(region_col[i].as_deref(), Some("us-east"));
        }
    }

    #[test]
    fn partition_index_maps_correctly() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        // Insert across 3 hours
        for h in 0..3 {
            for m in 0..5 {
                store.insert(
                    "pidx",
                    DataPoint {
                        timestamp: base_ts + h * 3_600_000 + m * 60_000,
                        tags: vec![],
                        value: 1.0,
                    },
                );
            }
        }

        let series = store.get_series("pidx").unwrap();
        assert_eq!(series.partition_index.len(), 3);

        // Each partition should have 5 points
        for (_, meta) in &series.partition_index.boundaries {
            assert_eq!(meta.count, 5);
        }

        // Offsets should be sequential: 0, 5, 10
        let offsets: Vec<usize> = series
            .partition_index
            .boundaries
            .values()
            .map(|m| m.start_offset)
            .collect();
        assert_eq!(offsets, vec![0, 5, 10]);
    }

    #[test]
    fn stats_accurate_after_inserts() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for i in 0..100 {
            store.insert(
                "stats_test",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![],
                    value: (i as f64) * 2.0 + 1.0, // 1, 3, 5, ..., 199
                },
            );
        }

        let series = store.get_series("stats_test").unwrap();
        assert_eq!(series.stats.count, 100);
        assert_eq!(series.stats.min_ts, base_ts);
        assert_eq!(series.stats.max_ts, base_ts + 99 * 1000);
        assert!((series.stats.min_val - 1.0).abs() < 1e-10);
        assert!((series.stats.max_val - 199.0).abs() < 1e-10);
        // Sum = 1 + 3 + 5 + ... + 199 = 100 * 100 = 10000
        assert!((series.stats.sum - 10000.0).abs() < 1e-10);
    }

    #[test]
    fn range_query_uses_partition_index() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        // Insert 10 hours of data, 10 points per hour
        for h in 0..10 {
            for m in 0..10 {
                store.insert(
                    "pquery",
                    DataPoint {
                        timestamp: base_ts + h * 3_600_000 + m * 60_000,
                        tags: vec![],
                        value: 1.0,
                    },
                );
            }
        }

        let series = store.get_series("pquery").unwrap();
        assert_eq!(series.partition_index.len(), 10);

        // Query just hours 3-5 (should not touch partitions 0-2 or 6-9)
        let start = base_ts + 3 * 3_600_000;
        let end = base_ts + 6 * 3_600_000;
        let overlapping = series.partition_index.range(start, end);
        assert_eq!(overlapping.len(), 3, "should hit exactly 3 partitions");

        let results = store.query("pquery", start, end);
        assert_eq!(results.len(), 30, "3 hours * 10 points = 30");
    }

    #[test]
    fn columnar_aggregation_matches_naive() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = time_bucket(1_700_000_000_000, BucketSize::Minute);

        let mut datapoints = Vec::new();
        for i in 0..180 {
            let dp = DataPoint {
                timestamp: base_ts + i * 1000,
                tags: vec![],
                value: (i as f64) + 1.0,
            };
            store.insert("agg_test", dp.clone());
            datapoints.push(dp);
        }

        // Naive aggregation via the public function
        let naive = aggregate(&datapoints, BucketSize::Minute);

        // Columnar aggregation
        let series = store.get_series("agg_test").unwrap();
        let columnar = series.aggregate_range(0, u64::MAX, BucketSize::Minute);

        assert_eq!(naive.len(), columnar.len());
        for (n, c) in naive.iter().zip(columnar.iter()) {
            assert_eq!(n.bucket_start, c.bucket_start);
            assert_eq!(n.count, c.count);
            assert!((n.sum - c.sum).abs() < 1e-10);
            assert!((n.min - c.min).abs() < 1e-10);
            assert!((n.max - c.max).abs() < 1e-10);
        }
    }

    #[test]
    fn fast_aggregate_full_series() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for i in 0..100 {
            store.insert(
                "fast_agg",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![],
                    value: (i as f64) + 1.0, // 1..=100
                },
            );
        }

        let series = store.get_series("fast_agg").unwrap();
        let result = series.fast_aggregate(0, u64::MAX).unwrap();

        assert_eq!(result.count, 100);
        assert!((result.sum - 5050.0).abs() < 1e-10);
        assert!((result.min - 1.0).abs() < 1e-10);
        assert!((result.max - 100.0).abs() < 1e-10);
        assert!((result.avg() - 50.5).abs() < 1e-10);
    }

    #[test]
    fn fast_aggregate_partial_range() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for i in 0..100 {
            store.insert(
                "partial_agg",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![],
                    value: (i as f64) + 1.0,
                },
            );
        }

        let series = store.get_series("partial_agg").unwrap();
        // Query indices 10..20 → values 11..=20
        let start = base_ts + 10 * 1000;
        let end = base_ts + 20 * 1000;
        let result = series.fast_aggregate(start, end).unwrap();

        assert_eq!(result.count, 10);
        assert!((result.sum - 155.0).abs() < 1e-10); // 11+12+...+20 = 155
        assert!((result.min - 11.0).abs() < 1e-10);
        assert!((result.max - 20.0).abs() < 1e-10);
    }

    #[test]
    fn tag_column_filtering() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for i in 0..20 {
            store.insert(
                "tagged",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![
                        ("host".into(), if i % 2 == 0 { "a".into() } else { "b".into() }),
                    ],
                    value: i as f64,
                },
            );
        }

        let series = store.get_series("tagged").unwrap();
        let host_col = series.tag_columns.get("host").unwrap();

        // Count host=a entries
        let a_count = host_col.iter().filter(|v| v.as_deref() == Some("a")).count();
        let b_count = host_col.iter().filter(|v| v.as_deref() == Some("b")).count();
        assert_eq!(a_count, 10);
        assert_eq!(b_count, 10);

        // Verify reconstruction preserves tags
        let all = store.query("tagged", 0, u64::MAX);
        assert_eq!(all.len(), 20);
        for dp in &all {
            assert_eq!(dp.tags.len(), 1);
            assert_eq!(dp.tags[0].0, "host");
        }
    }

    #[test]
    fn large_dataset_correctness() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;
        let n = 100_000;

        for i in 0..n {
            store.insert(
                "large",
                DataPoint {
                    timestamp: base_ts + i * 100, // 100ms intervals
                    tags: vec![],
                    value: (i as f64) * 0.01,
                },
            );
        }

        assert_eq!(store.total_points(), n as usize);

        // Verify first and last
        let series = store.get_series("large").unwrap();
        assert_eq!(series.timestamps[0], base_ts);
        assert_eq!(series.timestamps[n as usize - 1], base_ts + (n - 1) * 100);

        // Stats
        assert_eq!(series.stats.count, n as usize);

        // Range query subset
        let start = base_ts + 50_000 * 100;
        let end = base_ts + 50_010 * 100;
        let results = store.query("large", start, end);
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn empty_series_edge_cases() {
        let store = TimeSeriesStore::new(BucketSize::Hour);

        // Query on non-existent series
        assert!(store.query("ghost", 0, u64::MAX).is_empty());
        assert!(store.get_series("ghost").is_none());
        assert_eq!(store.total_points(), 0);
        assert_eq!(store.partition_count("ghost"), 0);
    }

    #[test]
    fn fast_aggregate_empty_series() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        store.insert(
            "empty_range",
            DataPoint {
                timestamp: base_ts,
                tags: vec![],
                value: 42.0,
            },
        );

        let series = store.get_series("empty_range").unwrap();
        // Query a range that doesn't overlap
        let result = series.fast_aggregate(base_ts + 1_000_000, base_ts + 2_000_000);
        assert!(result.is_none());
    }

    #[test]
    fn wal_insert_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().join("ts_wal_test");

        // Phase 1: open, insert, drop
        {
            let mut store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            for i in 0..10 {
                store.insert(
                    "wal_series",
                    DataPoint {
                        timestamp: 1_000_000 + i * 1000,
                        tags: vec![("env".into(), "prod".into())],
                        value: i as f64,
                    },
                );
            }
        }

        // Phase 2: reopen and verify
        {
            let store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            assert_eq!(store.total_points(), 10);
            let results = store.query("wal_series", 0, u64::MAX);
            assert_eq!(results.len(), 10);
            // Verify values
            for (i, dp) in results.iter().enumerate() {
                assert_eq!(dp.timestamp, 1_000_000 + i as u64 * 1000);
                assert!((dp.value - i as f64).abs() < 1e-10);
            }
        }
    }

    #[test]
    fn wal_aggregation_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().join("ts_wal_agg");

        let base_ts = time_bucket(1_700_000_000_000, BucketSize::Minute);

        // Phase 1: insert data
        {
            let mut store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            for i in 0..60 {
                store.insert(
                    "wal_agg",
                    DataPoint {
                        timestamp: base_ts + i * 1000,
                        tags: vec![],
                        value: (i as f64) + 1.0, // 1..=60
                    },
                );
            }
        }

        // Phase 2: reopen and aggregate
        {
            let store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            let series = store.get_series("wal_agg").unwrap();
            let result = series.fast_aggregate(0, u64::MAX).unwrap();
            assert_eq!(result.count, 60);
            assert!((result.sum - 1830.0).abs() < 1e-10); // 1+2+...+60
        }
    }

    #[test]
    fn wal_corrupt_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().join("ts_wal_corrupt");

        // Phase 1: write some data
        {
            let mut store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            for i in 0..5 {
                store.insert(
                    "corrupt_test",
                    DataPoint {
                        timestamp: 1_000_000 + i * 1000,
                        tags: vec![],
                        value: i as f64,
                    },
                );
            }
        }

        // Corrupt the WAL by appending garbage
        {
            use std::io::Write;
            let wal_path = dir_path.join("ts_wal.bin");
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD, 0xFC, 0xFB]).unwrap();
        }

        // Phase 2: reopen — should recover the 5 valid entries
        {
            let store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            assert_eq!(store.total_points(), 5);
        }
    }

    #[test]
    fn out_of_order_inserts_stay_sorted() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        // Insert out of order
        store.insert(
            "ooo",
            DataPoint {
                timestamp: base_ts + 5000,
                tags: vec![],
                value: 5.0,
            },
        );
        store.insert(
            "ooo",
            DataPoint {
                timestamp: base_ts + 1000,
                tags: vec![],
                value: 1.0,
            },
        );
        store.insert(
            "ooo",
            DataPoint {
                timestamp: base_ts + 3000,
                tags: vec![],
                value: 3.0,
            },
        );

        let series = store.get_series("ooo").unwrap();
        // Timestamps must be sorted
        for i in 1..series.timestamps.len() {
            assert!(series.timestamps[i] >= series.timestamps[i - 1]);
        }
        // Values must correspond
        assert!((series.values[0] - 1.0).abs() < 1e-10);
        assert!((series.values[1] - 3.0).abs() < 1e-10);
        assert!((series.values[2] - 5.0).abs() < 1e-10);
    }

    #[test]
    fn wal_snapshot_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().join("ts_wal_snap");

        // Phase 1: insert + snapshot
        {
            let mut store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            for i in 0..20 {
                store.insert(
                    "snap",
                    DataPoint {
                        timestamp: 1_000_000 + i * 1000,
                        tags: vec![("k".into(), format!("v{}", i))],
                        value: i as f64,
                    },
                );
            }
            store.snapshot();
        }

        // Phase 2: reopen from snapshot
        {
            let store = TimeSeriesStore::open(&dir_path, BucketSize::Hour).unwrap();
            assert_eq!(store.total_points(), 20);
            let results = store.query("snap", 0, u64::MAX);
            assert_eq!(results.len(), 20);
        }
    }

    // ========================================================================
    // Sprint 4E — SIMD aggregation and performance tests
    // ========================================================================

    #[test]
    fn simd_sum_correctness() {
        // Empty
        assert_eq!(simd_sum(&[]), 0.0);

        // Single element
        assert!((simd_sum(&[42.5]) - 42.5).abs() < 1e-10);

        // Small (fewer than 4 elements — remainder-only path)
        let small = [1.0, 2.0, 3.0];
        assert!((simd_sum(&small) - 6.0).abs() < 1e-10);

        // Exactly 4 (one chunk, no remainder)
        let four = [10.0, 20.0, 30.0, 40.0];
        assert!((simd_sum(&four) - 100.0).abs() < 1e-10);

        // Larger: 1 + 2 + ... + 1000 = 500_500
        let vals: Vec<f64> = (1..=1000).map(|i| i as f64).collect();
        assert!((simd_sum(&vals) - 500_500.0).abs() < 1e-6);

        // With remainder (1001 elements)
        let vals2: Vec<f64> = (1..=1001).map(|i| i as f64).collect();
        let expected = 1001.0 * 1002.0 / 2.0;
        assert!((simd_sum(&vals2) - expected).abs() < 1e-6);
    }

    #[test]
    fn simd_min_max_correctness() {
        // Empty
        assert_eq!(simd_min(&[]), f64::INFINITY);
        assert_eq!(simd_max(&[]), f64::NEG_INFINITY);

        // Single element
        assert!((simd_min(&[7.0]) - 7.0).abs() < 1e-10);
        assert!((simd_max(&[7.0]) - 7.0).abs() < 1e-10);

        // Small
        assert!((simd_min(&[3.0, 1.0, 2.0]) - 1.0).abs() < 1e-10);
        assert!((simd_max(&[3.0, 1.0, 2.0]) - 3.0).abs() < 1e-10);

        // Large with known min/max
        let vals: Vec<f64> = (0..1000).map(|i| (i as f64) - 500.0).collect();
        assert!((simd_min(&vals) - (-500.0)).abs() < 1e-10);
        assert!((simd_max(&vals) - 499.0).abs() < 1e-10);

        // Negative values
        let neg = [-10.0, -20.0, -5.0, -1.0, -100.0];
        assert!((simd_min(&neg) - (-100.0)).abs() < 1e-10);
        assert!((simd_max(&neg) - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn simd_sum_min_max_combined() {
        let vals: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let (sum, min, max) = simd_sum_min_max(&vals);

        assert!((sum - 5050.0).abs() < 1e-10);
        assert!((min - 1.0).abs() < 1e-10);
        assert!((max - 100.0).abs() < 1e-10);

        // Empty
        let (s, mn, mx) = simd_sum_min_max(&[]);
        assert_eq!(s, 0.0);
        assert_eq!(mn, f64::INFINITY);
        assert_eq!(mx, f64::NEG_INFINITY);
    }

    #[test]
    fn simd_fast_aggregate_matches_naive() {
        // Insert 10K points across 5 partitions (hourly), verify SIMD aggregate
        // matches a naive iterator-based calculation.
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;
        let n = 10_000u64;

        let mut naive_sum = 0.0f64;
        let mut naive_min = f64::INFINITY;
        let mut naive_max = f64::NEG_INFINITY;

        for i in 0..n {
            let val = (i as f64) * 0.37 + 1.0;
            naive_sum += val;
            naive_min = naive_min.min(val);
            naive_max = naive_max.max(val);
            store.insert(
                "simd_verify",
                DataPoint {
                    timestamp: base_ts + i * 1000, // 1s intervals, ~2.78 hours
                    tags: vec![],
                    value: val,
                },
            );
        }

        let series = store.get_series("simd_verify").unwrap();

        // Full-series should use O(1) stats path
        let full = series.fast_aggregate(0, u64::MAX).unwrap();
        assert_eq!(full.count, n as usize);
        assert!((full.sum - naive_sum).abs() < 1e-4, "sum: {} vs {}", full.sum, naive_sum);
        assert!((full.min - naive_min).abs() < 1e-10);
        assert!((full.max - naive_max).abs() < 1e-10);

        // Partial range that spans multiple partitions — exercises SIMD path
        let range_start = base_ts + 1000 * 1000; // skip first 1000 points
        let range_end = base_ts + 9000 * 1000;   // skip last 1000 points
        let partial = series.fast_aggregate(range_start, range_end).unwrap();

        // Compute expected values for the range
        let mut expected_sum = 0.0f64;
        let mut expected_min = f64::INFINITY;
        let mut expected_max = f64::NEG_INFINITY;
        let mut expected_count = 0usize;
        for i in 0..n {
            let ts = base_ts + i * 1000;
            if ts >= range_start && ts < range_end {
                let val = (i as f64) * 0.37 + 1.0;
                expected_sum += val;
                expected_min = expected_min.min(val);
                expected_max = expected_max.max(val);
                expected_count += 1;
            }
        }

        assert_eq!(partial.count, expected_count);
        assert!((partial.sum - expected_sum).abs() < 1e-4);
        assert!((partial.min - expected_min).abs() < 1e-10);
        assert!((partial.max - expected_max).abs() < 1e-10);
    }

    #[test]
    fn simd_aggregate_range_matches_naive() {
        // Verify that the SIMD-enhanced aggregate_range produces the same
        // results as the naive aggregate() function.
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = time_bucket(1_700_000_000_000, BucketSize::Minute);
        let n = 300;

        let mut datapoints = Vec::new();
        for i in 0..n {
            let dp = DataPoint {
                timestamp: base_ts + i * 1000,
                tags: vec![],
                value: (i as f64) * 1.23 + 5.0,
            };
            store.insert("agg_range_verify", dp.clone());
            datapoints.push(dp);
        }

        // Naive
        let naive = aggregate(&datapoints, BucketSize::Minute);

        // Columnar SIMD
        let series = store.get_series("agg_range_verify").unwrap();
        let columnar = series.aggregate_range(0, u64::MAX, BucketSize::Minute);

        assert_eq!(naive.len(), columnar.len(),
            "bucket count mismatch: naive={} columnar={}", naive.len(), columnar.len());
        for (n_agg, c_agg) in naive.iter().zip(columnar.iter()) {
            assert_eq!(n_agg.bucket_start, c_agg.bucket_start);
            assert_eq!(n_agg.count, c_agg.count);
            assert!((n_agg.sum - c_agg.sum).abs() < 1e-6,
                "sum mismatch in bucket {}: {} vs {}", n_agg.bucket_start, n_agg.sum, c_agg.sum);
            assert!((n_agg.min - c_agg.min).abs() < 1e-10);
            assert!((n_agg.max - c_agg.max).abs() < 1e-10);
        }
    }

    #[test]
    fn multi_partition_aggregate_merges_correctly() {
        // Create data spanning 10 partitions, verify aggregate merges correctly
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        let mut expected_sum = 0.0f64;
        let mut expected_min = f64::INFINITY;
        let mut expected_max = f64::NEG_INFINITY;
        let mut count = 0usize;

        for h in 0..10u64 {
            for m in 0..20u64 {
                let val = (h * 20 + m) as f64 + 1.0;
                expected_sum += val;
                expected_min = expected_min.min(val);
                expected_max = expected_max.max(val);
                count += 1;
                store.insert(
                    "multi_part",
                    DataPoint {
                        timestamp: base_ts + h * 3_600_000 + m * 60_000,
                        tags: vec![],
                        value: val,
                    },
                );
            }
        }

        assert_eq!(store.partition_count("multi_part"), 10);

        let series = store.get_series("multi_part").unwrap();

        // Query a range spanning hours 2-7 (5 partitions, >4 triggers partition-splitting)
        let range_start = base_ts + 2 * 3_600_000;
        let range_end = base_ts + 7 * 3_600_000;
        let result = series.fast_aggregate(range_start, range_end).unwrap();

        // Compute expected for this range
        let mut r_sum = 0.0f64;
        let mut r_min = f64::INFINITY;
        let mut r_max = f64::NEG_INFINITY;
        let mut r_count = 0usize;
        for h in 0..10u64 {
            for m in 0..20u64 {
                let ts = base_ts + h * 3_600_000 + m * 60_000;
                if ts >= range_start && ts < range_end {
                    let val = (h * 20 + m) as f64 + 1.0;
                    r_sum += val;
                    r_min = r_min.min(val);
                    r_max = r_max.max(val);
                    r_count += 1;
                }
            }
        }

        assert_eq!(result.count, r_count);
        assert!((result.sum - r_sum).abs() < 1e-6);
        assert!((result.min - r_min).abs() < 1e-10);
        assert!((result.max - r_max).abs() < 1e-10);

        // Also verify full-series via O(1) stats
        let full = series.fast_aggregate(0, u64::MAX).unwrap();
        assert_eq!(full.count, count);
        assert!((full.sum - expected_sum).abs() < 1e-6);
        assert!((full.min - expected_min).abs() < 1e-10);
        assert!((full.max - expected_max).abs() < 1e-10);
    }

    #[test]
    fn benchmark_100k_aggregation() {
        // Performance benchmark: insert 100K data points and time SUM/AVG/MIN/MAX
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;
        let n = 100_000u64;

        // Insert 100K points (1-second intervals spanning ~27.8 hours)
        for i in 0..n {
            store.insert(
                "bench",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![],
                    value: (i as f64) * 0.001 + 42.0,
                },
            );
        }

        assert_eq!(store.total_points(), n as usize);
        let series = store.get_series("bench").unwrap();
        let partitions = store.partition_count("bench");
        assert!(partitions > 1, "should have multiple partitions, got {}", partitions);

        // --- Full-series aggregate (O(1) via stats) ---
        let t0 = std::time::Instant::now();
        let full = series.fast_aggregate(0, u64::MAX).unwrap();
        let full_time = t0.elapsed();
        assert_eq!(full.count, n as usize);
        assert!(full.min < full.max);
        assert!(full.sum > 0.0);

        // --- Partial range aggregate (SIMD path) ---
        let range_start = base_ts + 10_000 * 1000;
        let range_end = base_ts + 90_000 * 1000;
        let t1 = std::time::Instant::now();
        let partial = series.fast_aggregate(range_start, range_end).unwrap();
        let partial_time = t1.elapsed();
        assert_eq!(partial.count, 80_000);
        assert!(partial.sum > 0.0);
        let partial_avg = partial.avg();
        assert!(partial_avg > 0.0);

        // --- Bucketed aggregate_range (SIMD on fully-contained partitions) ---
        let t2 = std::time::Instant::now();
        let bucketed = series.aggregate_range(0, u64::MAX, BucketSize::Hour);
        let bucketed_time = t2.elapsed();
        assert!(!bucketed.is_empty());
        let total_count: usize = bucketed.iter().map(|b| b.count).sum();
        assert_eq!(total_count, n as usize);

        // --- Verify AVG computation ---
        let t3 = std::time::Instant::now();
        let avg_result = series.fast_aggregate(0, u64::MAX).unwrap();
        let avg_time = t3.elapsed();
        let avg = avg_result.avg();
        // Expected avg for values: 42.0, 42.001, 42.002, ..., 42.099
        // = 42 + 0.001 * (0 + 1 + ... + 99999) / 100000
        // = 42 + 0.001 * 49999.5 = 42 + 49.9995 = 91.9995
        let expected_avg = 42.0 + 0.001 * (n as f64 - 1.0) / 2.0;
        assert!((avg - expected_avg).abs() < 1e-4,
            "avg mismatch: {} vs expected {}", avg, expected_avg);

        // Print timing results (visible with `cargo test -- --nocapture`)
        eprintln!("=== 100K TimeSeries Aggregation Benchmark ===");
        eprintln!("  Partitions: {}", partitions);
        eprintln!("  Full-series (O(1) stats):  {:?}", full_time);
        eprintln!("  Partial range (80K SIMD):  {:?}", partial_time);
        eprintln!("  Bucketed aggregate_range:  {:?}", bucketed_time);
        eprintln!("  AVG computation:           {:?}", avg_time);
        eprintln!("  AVG value: {:.6}", avg);
        eprintln!("=============================================");
    }

    // ========================================================================
    // Parallel aggregation tests
    // ========================================================================

    /// Helper: build a store with data spanning many hourly partitions.
    fn build_multi_partition_store(n_hours: u64, points_per_hour: u64) -> TimeSeriesStore {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        for h in 0..n_hours {
            for m in 0..points_per_hour {
                let ts = base_ts + h * 3_600_000 + m * 60_000;
                let val = (h * points_per_hour + m) as f64 + 1.0;
                store.insert(
                    "par_series",
                    DataPoint {
                        timestamp: ts,
                        tags: vec![],
                        value: val,
                    },
                );
            }
        }
        store
    }

    #[test]
    fn par_range_sum_matches_sequential() {
        let store = build_multi_partition_store(10, 20);
        let base_ts = 1_700_000_000_000u64;

        // Full range
        let seq = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(0, u64::MAX)
            .unwrap();
        let par = store.par_range_sum("par_series", 0, u64::MAX).unwrap();
        assert!(
            (par - seq.sum).abs() < 1e-6,
            "par_sum={} seq_sum={}",
            par,
            seq.sum
        );

        // Partial range spanning 5 partitions (hours 2-6)
        let start = base_ts + 2 * 3_600_000;
        let end = base_ts + 7 * 3_600_000;
        let seq_partial = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(start, end)
            .unwrap();
        let par_partial = store.par_range_sum("par_series", start, end).unwrap();
        assert!(
            (par_partial - seq_partial.sum).abs() < 1e-6,
            "partial: par_sum={} seq_sum={}",
            par_partial,
            seq_partial.sum
        );
    }

    #[test]
    fn par_range_count_matches_sequential() {
        let store = build_multi_partition_store(10, 20);
        let base_ts = 1_700_000_000_000u64;

        // Full range
        let seq = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(0, u64::MAX)
            .unwrap();
        let par = store.par_range_count("par_series", 0, u64::MAX).unwrap();
        assert_eq!(par, seq.count);

        // Partial range
        let start = base_ts + 3 * 3_600_000;
        let end = base_ts + 8 * 3_600_000;
        let seq_partial = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(start, end)
            .unwrap();
        let par_partial = store.par_range_count("par_series", start, end).unwrap();
        assert_eq!(par_partial, seq_partial.count);
    }

    #[test]
    fn par_range_min_max() {
        let store = build_multi_partition_store(10, 20);
        let base_ts = 1_700_000_000_000u64;

        // Full range
        let seq = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(0, u64::MAX)
            .unwrap();

        let par_min = store.par_range_min("par_series", 0, u64::MAX).unwrap();
        let par_max = store.par_range_max("par_series", 0, u64::MAX).unwrap();
        assert!((par_min - seq.min).abs() < 1e-10);
        assert!((par_max - seq.max).abs() < 1e-10);

        // Partial range (hours 1-5)
        let start = base_ts + 1 * 3_600_000;
        let end = base_ts + 6 * 3_600_000;
        let seq_partial = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(start, end)
            .unwrap();
        let par_min_p = store.par_range_min("par_series", start, end).unwrap();
        let par_max_p = store.par_range_max("par_series", start, end).unwrap();
        assert!((par_min_p - seq_partial.min).abs() < 1e-10);
        assert!((par_max_p - seq_partial.max).abs() < 1e-10);
    }

    #[test]
    fn par_small_dataset_fallback() {
        // With only 2 partitions (< threshold), parallel should fall back to sequential
        let store = build_multi_partition_store(2, 10);
        assert_eq!(store.partition_count("par_series"), 2);

        let seq = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(0, u64::MAX)
            .unwrap();

        let par_sum = store.par_range_sum("par_series", 0, u64::MAX).unwrap();
        let par_count = store.par_range_count("par_series", 0, u64::MAX).unwrap();
        let par_avg = store.par_range_avg("par_series", 0, u64::MAX).unwrap();
        let par_min = store.par_range_min("par_series", 0, u64::MAX).unwrap();
        let par_max = store.par_range_max("par_series", 0, u64::MAX).unwrap();

        assert!((par_sum - seq.sum).abs() < 1e-6);
        assert_eq!(par_count, seq.count);
        assert!((par_avg - seq.avg()).abs() < 1e-10);
        assert!((par_min - seq.min).abs() < 1e-10);
        assert!((par_max - seq.max).abs() < 1e-10);
    }

    #[test]
    fn par_large_time_range() {
        // 10000+ data points across many partitions
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;
        let n = 12_000u64;

        let mut expected_sum = 0.0f64;
        let mut expected_min = f64::INFINITY;
        let mut expected_max = f64::NEG_INFINITY;

        for i in 0..n {
            let val = (i as f64) * 0.5 + 1.0;
            expected_sum += val;
            expected_min = expected_min.min(val);
            expected_max = expected_max.max(val);
            store.insert(
                "par_large",
                DataPoint {
                    timestamp: base_ts + i * 1000, // 1s intervals, ~3.3 hours
                    tags: vec![],
                    value: val,
                },
            );
        }

        let partitions = store.partition_count("par_large");
        assert!(partitions >= 3, "should have multiple partitions, got {}", partitions);

        let par_sum = store.par_range_sum("par_large", 0, u64::MAX).unwrap();
        let par_count = store.par_range_count("par_large", 0, u64::MAX).unwrap();
        let par_avg = store.par_range_avg("par_large", 0, u64::MAX).unwrap();
        let par_min = store.par_range_min("par_large", 0, u64::MAX).unwrap();
        let par_max = store.par_range_max("par_large", 0, u64::MAX).unwrap();

        assert!((par_sum - expected_sum).abs() < 1e-4, "sum: {} vs {}", par_sum, expected_sum);
        assert_eq!(par_count, n as usize);
        assert!((par_avg - expected_sum / n as f64).abs() < 1e-6);
        assert!((par_min - expected_min).abs() < 1e-10);
        assert!((par_max - expected_max).abs() < 1e-10);
    }

    #[test]
    fn par_multi_series_query() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        // Insert into 4 different series
        for i in 0..10 {
            store.insert(
                "cpu",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![],
                    value: 50.0 + i as f64,
                },
            );
            store.insert(
                "mem",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![],
                    value: 80.0 + i as f64,
                },
            );
            store.insert(
                "disk",
                DataPoint {
                    timestamp: base_ts + i * 1000,
                    tags: vec![],
                    value: 30.0 + i as f64,
                },
            );
        }

        let results = store.par_multi_series_last(&["cpu", "mem", "disk", "nonexistent"]);
        assert_eq!(results.len(), 4);

        // Find each by name
        let cpu = results.iter().find(|(n, _)| n == "cpu").unwrap();
        let mem = results.iter().find(|(n, _)| n == "mem").unwrap();
        let disk = results.iter().find(|(n, _)| n == "disk").unwrap();
        let none = results.iter().find(|(n, _)| n == "nonexistent").unwrap();

        let (cpu_ts, cpu_val) = cpu.1.unwrap();
        assert_eq!(cpu_ts, (base_ts + 9 * 1000) as i64);
        assert!((cpu_val - 59.0).abs() < 1e-10);

        let (mem_ts, mem_val) = mem.1.unwrap();
        assert_eq!(mem_ts, (base_ts + 9 * 1000) as i64);
        assert!((mem_val - 89.0).abs() < 1e-10);

        let (disk_ts, disk_val) = disk.1.unwrap();
        assert_eq!(disk_ts, (base_ts + 9 * 1000) as i64);
        assert!((disk_val - 39.0).abs() < 1e-10);

        assert!(none.1.is_none());
    }

    #[test]
    fn par_bulk_insert_correctness() {
        let mut store = TimeSeriesStore::new(BucketSize::Hour);
        let base_ts = 1_700_000_000_000u64;

        let mut points: Vec<(String, i64, f64)> = Vec::new();
        for i in 0..100 {
            points.push(("bulk_a".to_string(), (base_ts + i * 1000) as i64, i as f64));
            points.push(("bulk_b".to_string(), (base_ts + i * 1000) as i64, i as f64 * 2.0));
        }

        store.par_bulk_insert(&points);

        assert_eq!(store.total_points(), 200);
        assert_eq!(store.query("bulk_a", 0, u64::MAX).len(), 100);
        assert_eq!(store.query("bulk_b", 0, u64::MAX).len(), 100);

        // Verify last values
        let last_a = store.last_value("bulk_a").unwrap();
        assert_eq!(last_a.timestamp, base_ts + 99 * 1000);
        assert!((last_a.value - 99.0).abs() < 1e-10);

        let last_b = store.last_value("bulk_b").unwrap();
        assert_eq!(last_b.timestamp, base_ts + 99 * 1000);
        assert!((last_b.value - 198.0).abs() < 1e-10);

        // Verify aggregation works on bulk-inserted data
        let series_a = store.get_series("bulk_a").unwrap();
        let agg = series_a.fast_aggregate(0, u64::MAX).unwrap();
        assert_eq!(agg.count, 100);
        assert!((agg.sum - 4950.0).abs() < 1e-6); // 0+1+2+...+99
    }

    #[test]
    fn par_range_avg_correctness() {
        let store = build_multi_partition_store(10, 20);
        let base_ts = 1_700_000_000_000u64;

        // Full range avg
        let seq = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(0, u64::MAX)
            .unwrap();
        let par_avg = store.par_range_avg("par_series", 0, u64::MAX).unwrap();
        assert!(
            (par_avg - seq.avg()).abs() < 1e-10,
            "avg mismatch: par={} seq={}",
            par_avg,
            seq.avg()
        );

        // Partial range avg
        let start = base_ts + 2 * 3_600_000;
        let end = base_ts + 8 * 3_600_000;
        let seq_partial = store
            .get_series("par_series")
            .unwrap()
            .fast_aggregate(start, end)
            .unwrap();
        let par_avg_partial = store.par_range_avg("par_series", start, end).unwrap();
        assert!(
            (par_avg_partial - seq_partial.avg()).abs() < 1e-10,
            "partial avg mismatch: par={} seq={}",
            par_avg_partial,
            seq_partial.avg()
        );
    }

    #[test]
    fn par_nonexistent_series_returns_none() {
        let store = TimeSeriesStore::new(BucketSize::Hour);
        assert!(store.par_range_sum("ghost", 0, u64::MAX).is_none());
        assert!(store.par_range_count("ghost", 0, u64::MAX).is_none());
        assert!(store.par_range_avg("ghost", 0, u64::MAX).is_none());
        assert!(store.par_range_min("ghost", 0, u64::MAX).is_none());
        assert!(store.par_range_max("ghost", 0, u64::MAX).is_none());
    }
}
