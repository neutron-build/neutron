//! Unified cost model for cross-engine query planning (Principle 4).
//!
//! Every access method — B-tree, hash, HNSW vector index, FTS inverted
//! index, R-tree spatial index — reports cost in the same units. The
//! query planner compares costs across engines to build optimal plans.

use std::fmt;
use std::ops::Add;

// ============================================================================
// Cost primitives
// ============================================================================

/// Decomposed cost in uniform units.
///
/// Each component tracks a different resource dimension so the planner can
/// reason about bottlenecks (I/O-bound vs CPU-bound vs network-bound).
/// The `total()` method collapses them into a single comparable number.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    /// CPU work (comparisons, hashing, arithmetic).
    pub cpu: f64,
    /// Disk / page I/O.
    pub io: f64,
    /// Network round-trips (relevant for distributed plans).
    pub network: f64,
}

impl Cost {
    /// Create a new cost with the given components.
    pub fn new(cpu: f64, io: f64, network: f64) -> Self {
        Self { cpu, io, network }
    }

    /// Zero cost.
    pub fn zero() -> Self {
        Self { cpu: 0.0, io: 0.0, network: 0.0 }
    }

    /// Weighted total — single scalar for comparison.
    ///
    /// Weights approximate the relative latency of each resource on
    /// modern NVMe hardware:
    ///   - CPU op  ~ 1 ns  (weight 1.0)
    ///   - IO page ~ 10 us (weight 10.0)
    ///   - Network ~ 1 ms  (weight 1000.0)
    pub fn total(&self) -> f64 {
        self.cpu + self.io * 10.0 + self.network * 1000.0
    }
}

impl Add for Cost {
    type Output = Cost;

    fn add(self, rhs: Cost) -> Cost {
        Cost {
            cpu: self.cpu + rhs.cpu,
            io: self.io + rhs.io,
            network: self.network + rhs.network,
        }
    }
}

impl fmt::Display for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cost(cpu={:.2}, io={:.2}, net={:.2}, total={:.2})",
            self.cpu,
            self.io,
            self.network,
            self.total(),
        )
    }
}

// ============================================================================
// Row estimates
// ============================================================================

/// Estimated cardinality after applying an access method.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowEstimate {
    /// Expected number of output rows.
    pub rows: f64,
    /// Fraction of input rows that survive (0.0..=1.0).
    pub selectivity: f64,
}

impl RowEstimate {
    pub fn new(rows: f64, selectivity: f64) -> Self {
        Self { rows, selectivity }
    }
}

// ============================================================================
// Sort order
// ============================================================================

/// Whether an access method can deliver rows in a useful order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
    Unsorted,
}

// ============================================================================
// Predicate classification
// ============================================================================

/// Logical category of a WHERE-clause predicate.
///
/// The planner classifies each predicate so that access methods can
/// advertise which kinds they support efficiently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateType {
    /// `col = value`
    Equality,
    /// `col > lo AND col < hi` or `BETWEEN`
    Range,
    /// `col LIKE 'foo%'`
    Prefix,
    /// `to_tsvector(col) @@ to_tsquery('...')`
    FullTextMatch,
    /// `col <-> vector` (nearest-neighbor)
    VectorSimilarity,
    /// `ST_Contains(geom, point)`
    SpatialContains,
    /// `ST_DWithin(geom, point, radius)`
    SpatialDistance,
    /// `MATCH (a)-[*1..3]->(b)`
    GraphTraversal,
}

// ============================================================================
// Access method trait
// ============================================================================

/// A single access path the planner can choose from.
///
/// Every engine (B-tree, HNSW, FTS, R-tree, ...) implements this so
/// the planner can compare costs in uniform units.
pub trait AccessMethod {
    /// Human-readable name (e.g. "btree", "hnsw", "seq_scan").
    fn name(&self) -> &str;

    /// Estimate the cost of scanning `rows_in` rows with the given
    /// selectivity through this access method.
    fn estimate_cost(&self, rows_in: f64, selectivity: f64) -> Cost;

    /// Estimate the output cardinality.
    fn estimate_rows(&self, total_rows: f64, selectivity: f64) -> RowEstimate;

    /// If this method naturally delivers ordered output, return the order.
    fn can_provide_ordering(&self) -> Option<SortOrder>;

    /// Whether this access method can efficiently evaluate the given
    /// predicate type.
    fn supports_predicate(&self, predicate_type: &PredicateType) -> bool;
}

// ============================================================================
// Concrete access methods
// ============================================================================

/// B-tree index scan — efficient for equality and range predicates.
///
/// Cost is O(log N) for the tree descent plus O(K) for leaf-page
/// scanning where K is the number of matching rows.
#[derive(Debug)]
pub struct BTreeAccess {
    /// Branching factor of the B-tree (typically 100-500).
    pub fanout: f64,
}

impl BTreeAccess {
    pub fn new(fanout: f64) -> Self {
        Self { fanout }
    }
}

impl AccessMethod for BTreeAccess {
    fn name(&self) -> &str {
        "btree"
    }

    fn estimate_cost(&self, rows_in: f64, selectivity: f64) -> Cost {
        // Tree height: log_fanout(rows_in), each level is one random I/O.
        let height = if rows_in > 1.0 {
            rows_in.log(self.fanout).ceil()
        } else {
            1.0
        };
        let matching = (rows_in * selectivity).max(1.0);
        // Leaf pages scanned (assume ~100 rows per leaf).
        let leaf_pages = (matching / 100.0).max(1.0);
        Cost {
            cpu: matching * 0.01,    // comparison per matching row
            io: height + leaf_pages, // tree descent + leaf scan
            network: 0.0,
        }
    }

    fn estimate_rows(&self, total_rows: f64, selectivity: f64) -> RowEstimate {
        RowEstimate::new((total_rows * selectivity).max(1.0), selectivity)
    }

    fn can_provide_ordering(&self) -> Option<SortOrder> {
        Some(SortOrder::Ascending)
    }

    fn supports_predicate(&self, predicate_type: &PredicateType) -> bool {
        matches!(predicate_type, PredicateType::Equality | PredicateType::Range | PredicateType::Prefix)
    }
}

/// Hash index — O(1) point lookups for equality predicates only.
#[derive(Debug)]
pub struct HashAccess;

impl AccessMethod for HashAccess {
    fn name(&self) -> &str {
        "hash"
    }

    fn estimate_cost(&self, rows_in: f64, selectivity: f64) -> Cost {
        let matching = (rows_in * selectivity).max(1.0);
        Cost {
            cpu: matching * 0.02, // hash + compare
            io: matching * 0.1,   // one page per match (random)
            network: 0.0,
        }
    }

    fn estimate_rows(&self, total_rows: f64, selectivity: f64) -> RowEstimate {
        RowEstimate::new((total_rows * selectivity).max(1.0), selectivity)
    }

    fn can_provide_ordering(&self) -> Option<SortOrder> {
        None
    }

    fn supports_predicate(&self, predicate_type: &PredicateType) -> bool {
        matches!(predicate_type, PredicateType::Equality)
    }
}

/// Sequential (full table) scan — the fallback access method.
///
/// Reads every page; supports all predicate types (via brute-force
/// evaluation) but at O(N) cost.
#[derive(Debug)]
pub struct SeqScanAccess;

impl AccessMethod for SeqScanAccess {
    fn name(&self) -> &str {
        "seq_scan"
    }

    fn estimate_cost(&self, rows_in: f64, _selectivity: f64) -> Cost {
        // Read every row; sequential I/O is cheap per-page.
        let pages = (rows_in / 100.0).max(1.0);
        Cost {
            cpu: rows_in * 0.01,
            io: pages * 1.0, // sequential page cost
            network: 0.0,
        }
    }

    fn estimate_rows(&self, total_rows: f64, selectivity: f64) -> RowEstimate {
        RowEstimate::new((total_rows * selectivity).max(1.0), selectivity)
    }

    fn can_provide_ordering(&self) -> Option<SortOrder> {
        Some(SortOrder::Unsorted)
    }

    fn supports_predicate(&self, _predicate_type: &PredicateType) -> bool {
        // Sequential scan can evaluate any predicate (just slowly).
        true
    }
}

/// HNSW (Hierarchical Navigable Small World) vector index.
///
/// Cost is O(log N) hops through the graph layers, excellent for
/// approximate nearest-neighbor searches.
#[derive(Debug)]
pub struct HnswAccess {
    /// Expected number of hops per layer (typically `ef_search`).
    pub ef_search: f64,
    /// Number of layers in the HNSW graph (~ log N).
    pub layers: f64,
}

impl HnswAccess {
    pub fn new(ef_search: f64, layers: f64) -> Self {
        Self { ef_search, layers }
    }

    /// Build an HNSW access method with reasonable defaults for a
    /// given dataset size.
    pub fn for_dataset(num_vectors: f64) -> Self {
        let layers = if num_vectors > 1.0 {
            num_vectors.ln().ceil()
        } else {
            1.0
        };
        Self {
            ef_search: 64.0,
            layers,
        }
    }
}

impl AccessMethod for HnswAccess {
    fn name(&self) -> &str {
        "hnsw"
    }

    fn estimate_cost(&self, rows_in: f64, _selectivity: f64) -> Cost {
        // Each layer requires `ef_search` distance computations.
        // Total hops ~ layers * ef_search.
        let layers = if rows_in > 1.0 {
            rows_in.ln().ceil()
        } else {
            1.0
        };
        let comparisons = layers * self.ef_search;
        Cost {
            cpu: comparisons * 0.05, // distance calc is heavier than a scalar compare
            io: layers * 1.0,        // one random page per layer hop
            network: 0.0,
        }
    }

    fn estimate_rows(&self, _total_rows: f64, selectivity: f64) -> RowEstimate {
        // ANN queries typically return a fixed top-K, but we model it
        // via selectivity for uniformity.
        let k = (1.0 / selectivity).max(1.0);
        RowEstimate::new(k, selectivity)
    }

    fn can_provide_ordering(&self) -> Option<SortOrder> {
        // Results are ordered by distance (ascending).
        Some(SortOrder::Ascending)
    }

    fn supports_predicate(&self, predicate_type: &PredicateType) -> bool {
        matches!(predicate_type, PredicateType::VectorSimilarity)
    }
}

/// FTS inverted index — efficient for full-text match predicates.
///
/// Cost depends on the number of terms in the query and the length
/// of posting lists (term frequency).
#[derive(Debug)]
pub struct FtsAccess {
    /// Average posting-list length for a single term.
    pub avg_postings: f64,
    /// Number of query terms.
    pub query_terms: f64,
}

impl FtsAccess {
    pub fn new(avg_postings: f64, query_terms: f64) -> Self {
        Self { avg_postings, query_terms }
    }
}

impl AccessMethod for FtsAccess {
    fn name(&self) -> &str {
        "fts_inverted"
    }

    fn estimate_cost(&self, rows_in: f64, selectivity: f64) -> Cost {
        // Read one posting list per query term, then intersect.
        let postings_read = self.query_terms * self.avg_postings;
        let matching = (rows_in * selectivity).max(1.0);
        Cost {
            cpu: postings_read * 0.01 + matching * 0.02, // decode + BM25 score
            io: self.query_terms * 1.0,                   // one page per term's posting list
            network: 0.0,
        }
    }

    fn estimate_rows(&self, total_rows: f64, selectivity: f64) -> RowEstimate {
        RowEstimate::new((total_rows * selectivity).max(1.0), selectivity)
    }

    fn can_provide_ordering(&self) -> Option<SortOrder> {
        // FTS returns results ranked by relevance (descending score).
        Some(SortOrder::Descending)
    }

    fn supports_predicate(&self, predicate_type: &PredicateType) -> bool {
        matches!(predicate_type, PredicateType::FullTextMatch)
    }
}

/// R-tree spatial index — efficient for containment and distance
/// queries on 2-D / 3-D geometries.
///
/// Cost is O(log N) for the tree search with some fan-out penalty
/// for overlapping bounding boxes.
#[derive(Debug)]
pub struct RTreeAccess {
    /// Average branching factor of the R-tree.
    pub fanout: f64,
}

impl RTreeAccess {
    pub fn new(fanout: f64) -> Self {
        Self { fanout }
    }
}

impl AccessMethod for RTreeAccess {
    fn name(&self) -> &str {
        "rtree"
    }

    fn estimate_cost(&self, rows_in: f64, selectivity: f64) -> Cost {
        let height = if rows_in > 1.0 {
            rows_in.log(self.fanout).ceil()
        } else {
            1.0
        };
        let matching = (rows_in * selectivity).max(1.0);
        // R-tree searches may visit multiple branches (overlap penalty).
        let overlap_factor = 1.5;
        Cost {
            cpu: matching * 0.02 + height * overlap_factor,
            io: height * overlap_factor + (matching / 50.0).max(1.0),
            network: 0.0,
        }
    }

    fn estimate_rows(&self, total_rows: f64, selectivity: f64) -> RowEstimate {
        RowEstimate::new((total_rows * selectivity).max(1.0), selectivity)
    }

    fn can_provide_ordering(&self) -> Option<SortOrder> {
        None
    }

    fn supports_predicate(&self, predicate_type: &PredicateType) -> bool {
        matches!(
            predicate_type,
            PredicateType::SpatialContains | PredicateType::SpatialDistance
        )
    }
}

// ============================================================================
// Plan candidate selection
// ============================================================================

/// A candidate access path paired with its estimated cost.
pub struct PlanCandidate<'a> {
    pub method: &'a dyn AccessMethod,
    pub cost: Cost,
}

impl<'a> PlanCandidate<'a> {
    pub fn new(method: &'a dyn AccessMethod, cost: Cost) -> Self {
        Self { method, cost }
    }
}

impl fmt::Debug for PlanCandidate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanCandidate")
            .field("method", &self.method.name())
            .field("cost", &self.cost)
            .finish()
    }
}

/// Choose the cheapest access method from a slice of candidates.
///
/// Returns the index of the winner, or `None` if the slice is empty.
pub fn choose_best_access(candidates: &[PlanCandidate<'_>]) -> Option<usize> {
    if candidates.is_empty() {
        return None;
    }
    let mut best_idx = 0;
    let mut best_total = candidates[0].cost.total();
    for (i, c) in candidates.iter().enumerate().skip(1) {
        let t = c.cost.total();
        if t < best_total {
            best_total = t;
            best_idx = i;
        }
    }
    Some(best_idx)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_cost_sublinear() {
        let btree = BTreeAccess::new(200.0);
        let cost_1k = btree.estimate_cost(1_000.0, 0.01);
        let cost_1m = btree.estimate_cost(1_000_000.0, 0.01);

        // B-tree is O(log N); cost should grow much slower than 1000x
        // when input grows 1000x.
        let ratio = cost_1m.total() / cost_1k.total();
        assert!(
            ratio < 100.0,
            "B-tree cost ratio should be sublinear: 1M/1K total ratio = {ratio:.2}"
        );
    }

    #[test]
    fn test_seqscan_cost_linear() {
        let seq = SeqScanAccess;
        let cost_1k = seq.estimate_cost(1_000.0, 1.0);
        let cost_10k = seq.estimate_cost(10_000.0, 1.0);

        // Sequential scan is O(N); 10x input should give ~10x cost.
        let ratio = cost_10k.total() / cost_1k.total();
        assert!(
            (ratio - 10.0).abs() < 1.0,
            "SeqScan cost should scale linearly: ratio = {ratio:.2}"
        );
    }

    #[test]
    fn test_hnsw_beats_seqscan_for_vectors() {
        let rows = 1_000_000.0;
        let selectivity = 0.0001; // top-100 out of 1M

        let hnsw = HnswAccess::for_dataset(rows);
        let seq = SeqScanAccess;

        let hnsw_cost = hnsw.estimate_cost(rows, selectivity);
        let seq_cost = seq.estimate_cost(rows, selectivity);

        assert!(
            hnsw_cost.total() < seq_cost.total(),
            "HNSW ({:.2}) should beat seq scan ({:.2}) for vector similarity on 1M rows",
            hnsw_cost.total(),
            seq_cost.total(),
        );
    }

    #[test]
    fn test_fts_supports_fulltext() {
        let fts = FtsAccess::new(500.0, 2.0);

        assert!(fts.supports_predicate(&PredicateType::FullTextMatch));
        assert!(!fts.supports_predicate(&PredicateType::Equality));
        assert!(!fts.supports_predicate(&PredicateType::VectorSimilarity));
        assert!(!fts.supports_predicate(&PredicateType::Range));
    }

    #[test]
    fn test_choose_best_access() {
        let btree = BTreeAccess::new(200.0);
        let seq = SeqScanAccess;
        let hash = HashAccess;

        let rows = 100_000.0;
        let selectivity = 0.001; // very selective

        let candidates = vec![
            PlanCandidate::new(&seq, seq.estimate_cost(rows, selectivity)),
            PlanCandidate::new(&btree, btree.estimate_cost(rows, selectivity)),
            PlanCandidate::new(&hash, hash.estimate_cost(rows, selectivity)),
        ];

        let best = choose_best_access(&candidates);
        assert!(best.is_some());

        let winner = best.unwrap();
        // For a highly selective equality lookup, hash or btree should
        // beat sequential scan (index 0).
        assert_ne!(winner, 0, "Sequential scan should not win for selective lookups");
    }

    #[test]
    fn test_cost_addition() {
        let a = Cost::new(1.0, 2.0, 3.0);
        let b = Cost::new(0.5, 1.5, 0.0);
        let sum = a + b;

        assert!((sum.cpu - 1.5).abs() < f64::EPSILON);
        assert!((sum.io - 3.5).abs() < f64::EPSILON);
        assert!((sum.network - 3.0).abs() < f64::EPSILON);

        // total = cpu + io*10 + network*1000
        let expected_total = 1.5 + 3.5 * 10.0 + 3.0 * 1000.0;
        assert!(
            (sum.total() - expected_total).abs() < f64::EPSILON,
            "total() = {}, expected {expected_total}",
            sum.total()
        );
    }

    #[test]
    fn test_btree_provides_ordering() {
        let btree = BTreeAccess::new(200.0);
        assert_eq!(btree.can_provide_ordering(), Some(SortOrder::Ascending));
    }

    #[test]
    fn test_hash_no_ordering() {
        let hash = HashAccess;
        assert_eq!(hash.can_provide_ordering(), None);
    }

    #[test]
    fn test_seqscan_supports_all_predicates() {
        let seq = SeqScanAccess;
        let all_types = [
            PredicateType::Equality,
            PredicateType::Range,
            PredicateType::Prefix,
            PredicateType::FullTextMatch,
            PredicateType::VectorSimilarity,
            PredicateType::SpatialContains,
            PredicateType::SpatialDistance,
            PredicateType::GraphTraversal,
        ];
        for pt in &all_types {
            assert!(
                seq.supports_predicate(pt),
                "SeqScan should support {pt:?}"
            );
        }
    }

    #[test]
    fn test_rtree_supports_spatial() {
        let rtree = RTreeAccess::new(50.0);
        assert!(rtree.supports_predicate(&PredicateType::SpatialContains));
        assert!(rtree.supports_predicate(&PredicateType::SpatialDistance));
        assert!(!rtree.supports_predicate(&PredicateType::Equality));
        assert!(!rtree.supports_predicate(&PredicateType::FullTextMatch));
    }

    #[test]
    fn test_choose_best_access_empty() {
        let candidates: Vec<PlanCandidate<'_>> = vec![];
        assert!(choose_best_access(&candidates).is_none());
    }

    #[test]
    fn test_cost_display() {
        let c = Cost::new(1.0, 2.0, 0.5);
        let s = format!("{c}");
        assert!(s.contains("cpu=1.00"));
        assert!(s.contains("io=2.00"));
        assert!(s.contains("net=0.50"));
    }

    #[test]
    fn test_row_estimate() {
        let btree = BTreeAccess::new(200.0);
        let est = btree.estimate_rows(100_000.0, 0.01);
        assert!((est.rows - 1_000.0).abs() < 1.0);
        assert!((est.selectivity - 0.01).abs() < f64::EPSILON);
    }
}
