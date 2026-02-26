//! Geospatial engine — R-tree index, point/polygon types, distance calculations.
//!
//! PostGIS-compatible function signatures:
//!   - ST_Distance(a, b) → distance in meters (Haversine for geographic, Euclidean for Cartesian)
//!   - ST_DWithin(a, b, distance) → boolean
//!   - ST_Contains(polygon, point) → boolean
//!   - ST_Area(polygon) → area
//!   - ST_MakePoint(x, y) → Point

use std::collections::HashMap;
use std::fmt;

// ============================================================================
// Geometry types
// ============================================================================

/// A 2D point (x, y) or (longitude, latitude).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POINT({} {})", self.x, self.y)
    }
}

/// A 2D bounding box (minimum bounding rectangle).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl BBox {
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self {
            min_x,
            min_y,
            max_x,
            max_y,
        }
    }

    /// Create a bounding box from a single point.
    pub fn from_point(p: &Point) -> Self {
        Self {
            min_x: p.x,
            min_y: p.y,
            max_x: p.x,
            max_y: p.y,
        }
    }

    /// Expand this bbox to include another.
    pub fn union(&self, other: &BBox) -> BBox {
        BBox {
            min_x: self.min_x.min(other.min_x),
            min_y: self.min_y.min(other.min_y),
            max_x: self.max_x.max(other.max_x),
            max_y: self.max_y.max(other.max_y),
        }
    }

    /// Check if this bbox intersects another.
    pub fn intersects(&self, other: &BBox) -> bool {
        self.min_x <= other.max_x
            && self.max_x >= other.min_x
            && self.min_y <= other.max_y
            && self.max_y >= other.min_y
    }

    /// Check if this bbox contains a point.
    pub fn contains_point(&self, p: &Point) -> bool {
        p.x >= self.min_x && p.x <= self.max_x && p.y >= self.min_y && p.y <= self.max_y
    }

    /// Area of the bounding box.
    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }

    /// Enlargement needed to include another bbox.
    pub fn enlargement(&self, other: &BBox) -> f64 {
        self.union(other).area() - self.area()
    }
}

/// A polygon defined by an outer ring of points (counter-clockwise).
#[derive(Debug, Clone, PartialEq)]
pub struct Polygon {
    pub exterior: Vec<Point>,
}

impl Polygon {
    pub fn new(points: Vec<Point>) -> Self {
        Self { exterior: points }
    }

    /// Bounding box of this polygon.
    pub fn bbox(&self) -> BBox {
        let mut bb = BBox::new(f64::MAX, f64::MAX, f64::MIN, f64::MIN);
        for p in &self.exterior {
            bb.min_x = bb.min_x.min(p.x);
            bb.min_y = bb.min_y.min(p.y);
            bb.max_x = bb.max_x.max(p.x);
            bb.max_y = bb.max_y.max(p.y);
        }
        bb
    }

    /// Check if a point is inside this polygon using ray casting algorithm.
    pub fn contains(&self, p: &Point) -> bool {
        let n = self.exterior.len();
        if n < 3 {
            return false;
        }

        let mut inside = false;
        let mut j = n - 1;
        for i in 0..n {
            let pi = &self.exterior[i];
            let pj = &self.exterior[j];

            if ((pi.y > p.y) != (pj.y > p.y))
                && (p.x < (pj.x - pi.x) * (p.y - pi.y) / (pj.y - pi.y) + pi.x)
            {
                inside = !inside;
            }
            j = i;
        }
        inside
    }

    /// Compute the area using the shoelace formula.
    pub fn area(&self) -> f64 {
        let n = self.exterior.len();
        if n < 3 {
            return 0.0;
        }

        let mut sum = 0.0;
        let mut j = n - 1;
        for i in 0..n {
            sum += (self.exterior[j].x + self.exterior[i].x)
                * (self.exterior[j].y - self.exterior[i].y);
            j = i;
        }
        (sum / 2.0).abs()
    }
}

impl fmt::Display for Polygon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POLYGON((")?;
        for (i, p) in self.exterior.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} {}", p.x, p.y)?;
        }
        write!(f, "))")
    }
}

// ============================================================================
// Distance functions
// ============================================================================

/// Euclidean distance between two points.
pub fn euclidean_distance(a: &Point, b: &Point) -> f64 {
    ((a.x - b.x).powi(2) + (a.y - b.y).powi(2)).sqrt()
}

/// Haversine distance between two geographic points (lat/lon in degrees).
/// Returns distance in meters.
pub fn haversine_distance(a: &Point, b: &Point) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;

    let lat1 = a.y.to_radians();
    let lat2 = b.y.to_radians();
    let dlat = (b.y - a.y).to_radians();
    let dlon = (b.x - a.x).to_radians();

    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * h.sqrt().asin();

    EARTH_RADIUS_M * c
}

/// Check if two points are within a given distance (meters, using Haversine).
pub fn st_dwithin(a: &Point, b: &Point, distance_m: f64) -> bool {
    haversine_distance(a, b) <= distance_m
}

// ============================================================================
// R-tree index
// ============================================================================

const RTREE_MAX_ENTRIES: usize = 16;
const _RTREE_MIN_ENTRIES: usize = 4;

/// Entry in an R-tree leaf node.
#[derive(Debug, Clone)]
struct RTreeEntry {
    bbox: BBox,
    doc_id: u64,
}

/// An R-tree node.
#[derive(Debug)]
enum RTreeNode {
    Leaf {
        entries: Vec<RTreeEntry>,
    },
    Internal {
        children: Vec<(BBox, Box<RTreeNode>)>,
    },
}

/// R-tree spatial index.
#[derive(Debug)]
pub struct RTree {
    root: RTreeNode,
    count: usize,
    /// Point lookup for haversine filtering in radius queries.
    points: HashMap<u64, Point>,
}

impl Default for RTree {
    fn default() -> Self {
        Self::new()
    }
}

impl RTree {
    pub fn new() -> Self {
        Self {
            root: RTreeNode::Leaf {
                entries: Vec::new(),
            },
            count: 0,
            points: HashMap::new(),
        }
    }

    /// Insert a point into the R-tree.
    pub fn insert(&mut self, point: &Point, doc_id: u64) {
        let bbox = BBox::from_point(point);
        let entry = RTreeEntry { bbox, doc_id };
        self.points.insert(doc_id, *point);
        let split = Self::insert_into(&mut self.root, entry);
        if let Some((left_bbox, left, right_bbox, right)) = split {
            // Root split — create new root
            self.root = RTreeNode::Internal {
                children: vec![
                    (left_bbox, Box::new(left)),
                    (right_bbox, Box::new(right)),
                ],
            };
        }
        self.count += 1;
    }

    /// Insert into a node, returning a split if the node overflows.
    fn insert_into(
        node: &mut RTreeNode,
        entry: RTreeEntry,
    ) -> Option<(BBox, RTreeNode, BBox, RTreeNode)> {
        match node {
            RTreeNode::Leaf { entries } => {
                entries.push(entry);
                if entries.len() > RTREE_MAX_ENTRIES {
                    let (left, right) = Self::split_leaf(entries);
                    let left_bbox = Self::compute_leaf_bbox(&left);
                    let right_bbox = Self::compute_leaf_bbox(&right);
                    Some((
                        left_bbox,
                        RTreeNode::Leaf { entries: left },
                        right_bbox,
                        RTreeNode::Leaf { entries: right },
                    ))
                } else {
                    None
                }
            }
            RTreeNode::Internal { children } => {
                // Choose the child with minimum enlargement
                let best_idx = children
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| {
                        let ea = a.0.enlargement(&entry.bbox);
                        let eb = b.0.enlargement(&entry.bbox);
                        ea.partial_cmp(&eb).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(i, _)| i)
                    .unwrap_or(0);

                // Update bbox
                children[best_idx].0 = children[best_idx].0.union(&entry.bbox);

                let split = Self::insert_into(&mut children[best_idx].1, entry);
                if let Some((left_bbox, left, right_bbox, right)) = split {
                    // Replace the child that split
                    children[best_idx] = (left_bbox, Box::new(left));
                    children.push((right_bbox, Box::new(right)));

                    if children.len() > RTREE_MAX_ENTRIES {
                        let (left_children, right_children) = Self::split_internal(children);
                        let lb = Self::compute_internal_bbox(&left_children);
                        let rb = Self::compute_internal_bbox(&right_children);
                        Some((
                            lb,
                            RTreeNode::Internal {
                                children: left_children,
                            },
                            rb,
                            RTreeNode::Internal {
                                children: right_children,
                            },
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    /// Split a leaf node using simple midpoint split on x-axis.
    fn split_leaf(entries: &mut Vec<RTreeEntry>) -> (Vec<RTreeEntry>, Vec<RTreeEntry>) {
        entries.sort_by(|a, b| {
            a.bbox
                .min_x
                .partial_cmp(&b.bbox.min_x)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mid = entries.len() / 2;
        let right = entries.split_off(mid);
        let left = std::mem::take(entries);
        (left, right)
    }

    #[allow(clippy::type_complexity)]
    fn split_internal(
        children: &mut Vec<(BBox, Box<RTreeNode>)>,
    ) -> (Vec<(BBox, Box<RTreeNode>)>, Vec<(BBox, Box<RTreeNode>)>) {
        children.sort_by(|a, b| {
            a.0.min_x
                .partial_cmp(&b.0.min_x)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mid = children.len() / 2;
        let right = children.split_off(mid);
        let left = std::mem::take(children);
        (left, right)
    }

    fn compute_leaf_bbox(entries: &[RTreeEntry]) -> BBox {
        entries
            .iter()
            .fold(entries[0].bbox, |acc, e| acc.union(&e.bbox))
    }

    fn compute_internal_bbox(children: &[(BBox, Box<RTreeNode>)]) -> BBox {
        children
            .iter()
            .fold(children[0].0, |acc, (bb, _)| acc.union(bb))
    }

    /// Search for all points within a bounding box.
    pub fn search_bbox(&self, query: &BBox) -> Vec<u64> {
        let mut results = Vec::new();
        Self::search_node(&self.root, query, &mut results);
        results
    }

    fn search_node(node: &RTreeNode, query: &BBox, results: &mut Vec<u64>) {
        match node {
            RTreeNode::Leaf { entries } => {
                for entry in entries {
                    if query.intersects(&entry.bbox) {
                        results.push(entry.doc_id);
                    }
                }
            }
            RTreeNode::Internal { children } => {
                for (bbox, child) in children {
                    if query.intersects(bbox) {
                        Self::search_node(child, query, results);
                    }
                }
            }
        }
    }

    /// Find all points within a given radius (meters) of a center point.
    /// Uses a bounding-box pre-filter with latitude-corrected longitude scaling,
    /// then applies exact haversine distance filtering on candidates.
    pub fn search_radius(&self, center: &Point, radius_m: f64) -> Vec<u64> {
        // Compute a bounding box that encompasses the radius.
        // Latitude: 1 degree ≈ 111,320 meters (constant).
        // Longitude: varies by cos(latitude), clamped to avoid division by zero near poles.
        let lat_deg = radius_m / 111_320.0;
        let lon_deg = radius_m / (111_320.0 * center.y.to_radians().cos().abs().max(0.01));
        let query_bbox = BBox::new(
            center.x - lon_deg,
            center.y - lat_deg,
            center.x + lon_deg,
            center.y + lat_deg,
        );

        // Get candidates from R-tree bounding-box search
        let candidates = self.search_bbox(&query_bbox);

        // Filter by actual Haversine distance using stored point coordinates
        candidates
            .into_iter()
            .filter(|doc_id| {
                if let Some(point) = self.points.get(doc_id) {
                    haversine_distance(center, point) <= radius_m
                } else {
                    // Point not in lookup map (shouldn't happen); include as candidate
                    true
                }
            })
            .collect()
    }

    /// Number of indexed entries.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn euclidean() {
        let a = Point::new(0.0, 0.0);
        let b = Point::new(3.0, 4.0);
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn haversine() {
        // New York to London ≈ 5,570 km
        let ny = Point::new(-74.006, 40.7128); // lon, lat
        let london = Point::new(-0.1278, 51.5074);
        let dist = haversine_distance(&ny, &london);
        assert!(dist > 5_500_000.0 && dist < 5_600_000.0, "dist = {dist}");
    }

    #[test]
    fn polygon_contains() {
        // Simple square: (0,0) → (10,0) → (10,10) → (0,10)
        let poly = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
        ]);

        assert!(poly.contains(&Point::new(5.0, 5.0))); // Inside
        assert!(!poly.contains(&Point::new(15.0, 5.0))); // Outside
        assert!(!poly.contains(&Point::new(-1.0, 5.0))); // Outside
    }

    #[test]
    fn polygon_area() {
        // Unit square
        let poly = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(1.0, 0.0),
            Point::new(1.0, 1.0),
            Point::new(0.0, 1.0),
        ]);
        assert!((poly.area() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn rtree_insert_and_search() {
        let mut tree = RTree::new();

        // Insert some points in a grid
        for i in 0..100 {
            let x = (i % 10) as f64;
            let y = (i / 10) as f64;
            tree.insert(&Point::new(x, y), i as u64);
        }

        assert_eq!(tree.len(), 100);

        // Search for points in a region
        let query = BBox::new(2.0, 2.0, 5.0, 5.0);
        let results = tree.search_bbox(&query);
        // Should find points (2,2), (3,2), (4,2), (5,2), (2,3), ..., (5,5) = 4×4 = 16 points
        assert_eq!(results.len(), 16, "results = {results:?}");
    }

    #[test]
    fn rtree_with_splits() {
        let mut tree = RTree::new();

        // Insert more points than RTREE_MAX_ENTRIES to trigger splits
        for i in 0..1000 {
            let x = (i as f64) * 0.1;
            let y = (i as f64) * 0.05;
            tree.insert(&Point::new(x, y), i as u64);
        }

        assert_eq!(tree.len(), 1000);

        // Should still find all points
        let all = tree.search_bbox(&BBox::new(-1.0, -1.0, 200.0, 200.0));
        assert_eq!(all.len(), 1000);

        // Narrow search
        let narrow = tree.search_bbox(&BBox::new(0.0, 0.0, 1.0, 1.0));
        assert!(!narrow.is_empty());
        assert!(narrow.len() < 100);
    }

    #[test]
    fn dwithin() {
        let a = Point::new(-74.006, 40.7128); // NYC
        let b = Point::new(-73.935, 40.730); // ~6km away in Brooklyn

        assert!(st_dwithin(&a, &b, 10_000.0)); // Within 10km
        assert!(!st_dwithin(&a, &b, 1_000.0)); // Not within 1km
    }

    #[test]
    fn bbox_operations() {
        let a = BBox::new(0.0, 0.0, 5.0, 5.0);
        let b = BBox::new(3.0, 3.0, 8.0, 8.0);

        assert!(a.intersects(&b));
        assert!(b.intersects(&a));

        let c = BBox::new(10.0, 10.0, 15.0, 15.0);
        assert!(!a.intersects(&c));

        let u = a.union(&b);
        assert_eq!(u, BBox::new(0.0, 0.0, 8.0, 8.0));
        assert!((a.area() - 25.0).abs() < 1e-10);
    }

    #[test]
    fn point_display() {
        let p = Point::new(-73.935, 40.730);
        assert_eq!(format!("{p}"), "POINT(-73.935 40.73)");
    }

    #[test]
    fn polygon_display() {
        let poly = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(1.0, 0.0),
            Point::new(1.0, 1.0),
        ]);
        assert_eq!(format!("{poly}"), "POLYGON((0 0, 1 0, 1 1))");
    }

    #[test]
    fn bbox_from_point_and_contains() {
        let p = Point::new(5.0, 10.0);
        let bb = BBox::from_point(&p);
        assert!(bb.contains_point(&p));
        assert!(!bb.contains_point(&Point::new(5.1, 10.0)));
        assert!((bb.area() - 0.0).abs() < 1e-10);
    }

    #[test]
    fn bbox_enlargement() {
        let a = BBox::new(0.0, 0.0, 2.0, 2.0); // area=4
        let b = BBox::new(1.0, 1.0, 3.0, 3.0); // union area=9
        let enlarge = a.enlargement(&b);
        assert!((enlarge - 5.0).abs() < 1e-10); // 9-4=5

        // No enlargement needed if b is inside a
        let c = BBox::new(0.5, 0.5, 1.5, 1.5);
        assert!((a.enlargement(&c) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn polygon_degenerate_cases() {
        // Fewer than 3 points
        let empty = Polygon::new(vec![]);
        assert!(!empty.contains(&Point::new(0.0, 0.0)));
        assert!((empty.area() - 0.0).abs() < 1e-10);

        let line = Polygon::new(vec![Point::new(0.0, 0.0), Point::new(1.0, 1.0)]);
        assert!(!line.contains(&Point::new(0.5, 0.5)));
        assert!((line.area() - 0.0).abs() < 1e-10);
    }

    #[test]
    fn haversine_same_point_is_zero() {
        let p = Point::new(-122.4194, 37.7749); // San Francisco
        let dist = haversine_distance(&p, &p);
        assert!((dist - 0.0).abs() < 1e-6);
    }

    #[test]
    fn euclidean_negative_coords() {
        let a = Point::new(-3.0, -4.0);
        let b = Point::new(0.0, 0.0);
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn rtree_empty_search() {
        let tree = RTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        let results = tree.search_bbox(&BBox::new(-100.0, -100.0, 100.0, 100.0));
        assert!(results.is_empty());
    }

    #[test]
    fn polygon_triangle_area() {
        // Right triangle with legs 6 and 8 → area = 24
        let tri = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(6.0, 0.0),
            Point::new(0.0, 8.0),
        ]);
        assert!((tri.area() - 24.0).abs() < 1e-10);
    }

    #[test]
    fn rtree_search_radius() {
        let mut tree = RTree::new();
        // Insert points at known locations
        tree.insert(&Point::new(0.0, 0.0), 1);
        tree.insert(&Point::new(0.001, 0.001), 2); // ~157m away
        tree.insert(&Point::new(1.0, 1.0), 3); // ~157km away

        // Search with 1km radius should find nearby points
        let results = tree.search_radius(&Point::new(0.0, 0.0), 1000.0);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
        // Point 3 is far away and should not be in results
        assert!(!results.contains(&3));
    }
}
