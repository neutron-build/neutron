//! Geo-model differential fuzzer.
//!
//! Drives every Nucleus geo function through the SQL surface and checks results
//! against brute-force planar/Haversine math written directly in this file.
//! Small integer coordinates keep intermediate values exact.
//!
//! Functions tested:
//!   ST_MAKEPOINT(x, y)               → 'POINT(x y)'  text
//!   ST_X(point_wkt)                  → x coordinate
//!   ST_Y(point_wkt)                  → y coordinate
//!   ST_DISTANCE_EUCLIDEAN(x1,y1,x2,y2) → Euclidean distance (no subsystem gate)
//!   ST_DISTANCE(lat1,lon1,lat2,lon2) → Haversine distance in metres
//!   ST_DWITHIN(lat1,lon1,lat2,lon2,r) → bool (Haversine ≤ r)
//!   ST_AREA(x1,y1, …)               → shoelace area (abs)
//!   ST_CONTAINS(polygon_wkt, pt_wkt)→ bool (ray-cast)
//!
//! Build:
//!   cargo build --release --features server --bin probe_geo
//! Run:
//!   cargo run  --release --features server --bin probe_geo
#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift64) ──────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    /// Random integer in [lo, hi] inclusive.
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

// ─── Reference math ───────────────────────────────────────────────────────────

/// Euclidean distance (same formula as geo::euclidean_distance).
fn ref_euclidean(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    ((x1 - x2).powi(2) + (y1 - y2).powi(2)).sqrt()
}

/// Haversine distance in metres (same formula as geo::haversine_distance).
/// Note: Nucleus maps args as (lat, lon) → Point { x: lon, y: lat }.
fn ref_haversine(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let lat1 = lat1_deg.to_radians();
    let lat2 = lat2_deg.to_radians();
    let dlat = (lat2_deg - lat1_deg).to_radians();
    let dlon = (lon2_deg - lon1_deg).to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * h.sqrt().asin();
    R * c
}

/// Shoelace area (absolute value) — same as geo::Polygon::area().
fn ref_area(pts: &[(f64, f64)]) -> f64 {
    let n = pts.len();
    if n < 3 {
        return 0.0;
    }
    let mut sum = 0.0f64;
    let mut j = n - 1;
    for i in 0..n {
        sum += (pts[j].0 + pts[i].0) * (pts[j].1 - pts[i].1);
        j = i;
    }
    (sum / 2.0).abs()
}

/// Ray-cast point-in-polygon — same algorithm as geo::Polygon::contains().
fn ref_contains(poly: &[(f64, f64)], px: f64, py: f64) -> bool {
    let n = poly.len();
    if n < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let (ix, iy) = poly[i];
        let (jx, jy) = poly[j];
        if ((iy > py) != (jy > py)) && (px < (jx - ix) * (py - iy) / (jy - iy) + ix) {
            inside = !inside;
        }
        j = i;
    }
    inside
}

// ─── Float tolerance ──────────────────────────────────────────────────────────

/// Absolute + relative tolerance for float comparisons.
fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-6 + 1e-6 * a.abs().max(b.abs())
}

// ─── Executor helpers ─────────────────────────────────────────────────────────

fn run_f64(ex: &Executor, sql: &str) -> Result<f64, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Float64(f)) => Ok(*f),
                Some(Value::Int32(n)) => Ok(*n as f64),
                Some(Value::Int64(n)) => Ok(*n as f64),
                other => Err(format!("unexpected value: {:?}", other)),
            },
            other => Err(format!("unexpected result: {:?}", other)),
        },
        Ok(Err(e)) => Err(format!("exec error: {e}")),
        Err(_) => Err("PANIC".into()),
    }
}

fn run_bool(ex: &Executor, sql: &str) -> Result<bool, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Bool(b)) => Ok(*b),
                other => Err(format!("expected bool, got: {:?}", other)),
            },
            other => Err(format!("unexpected result: {:?}", other)),
        },
        Ok(Err(e)) => Err(format!("exec error: {e}")),
        Err(_) => Err("PANIC".into()),
    }
}

fn run_text(ex: &Executor, sql: &str) -> Result<String, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Text(s)) => Ok(s.clone()),
                other => Err(format!("expected text, got: {:?}", other)),
            },
            other => Err(format!("unexpected result: {:?}", other)),
        },
        Ok(Err(e)) => Err(format!("exec error: {e}")),
        Err(_) => Err("PANIC".into()),
    }
}

// ─── Divergence reporter ──────────────────────────────────────────────────────

fn report(divs: &mut usize, max: usize, label: &str, sql: &str, exp: &str, got: &str) {
    *divs += 1;
    if *divs <= max {
        println!("─── GEO DIVERGENCE #{divs} ({label}) ───");
        println!("  sql      : {sql}");
        println!("  expected : {exp}");
        println!("  nucleus  : {got}\n");
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xF00D_CAF3;
    let mut iterations = 5_000usize;
    let mut max_report = 20usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args[i].parse().unwrap();
            }
            "--iterations" => {
                i += 1;
                iterations = args[i].parse().unwrap();
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus geo differential fuzzer (vs reference math)");
    println!("seed={seed} iterations={iterations}\n");

    // Single shared executor for functions that don't require state.
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    let mut total = 0usize;
    let mut divergences = 0usize;

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        // ── Coordinate domains ────────────────────────────────────────────────
        // Euclidean: small integers (exact distance arithmetic).
        let (x1, y1) = (rng.int(-20, 20) as f64, rng.int(-20, 20) as f64);
        let (x2, y2) = (rng.int(-20, 20) as f64, rng.int(-20, 20) as f64);

        // Geographic: valid latitude [-89, 89], longitude [-179, 179].
        let lat1 = rng.int(-89, 89) as f64;
        let lon1 = rng.int(-179, 179) as f64;
        let lat2 = rng.int(-89, 89) as f64;
        let lon2 = rng.int(-179, 179) as f64;

        // ── 1. ST_MAKEPOINT / ST_X / ST_Y ─────────────────────────────────────
        {
            total += 1;
            let sql = format!("SELECT ST_MAKEPOINT({x1},{y1})");
            let expected = format!("POINT({x1} {y1})");
            match run_text(&ex, &sql) {
                Ok(got) if got == expected => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_MAKEPOINT text",
                    &sql,
                    &expected,
                    &got,
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_MAKEPOINT error",
                    &sql,
                    &expected,
                    &e,
                ),
            }
        }
        {
            total += 1;
            // Build the point WKT the same way ST_MAKEPOINT does, then extract X.
            let pt_wkt = format!("POINT({x1} {y1})");
            let sql = format!("SELECT ST_X('{pt_wkt}')");
            match run_f64(&ex, &sql) {
                Ok(got) if close(got, x1) => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_X",
                    &sql,
                    &x1.to_string(),
                    &got.to_string(),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_X error",
                    &sql,
                    &x1.to_string(),
                    &e,
                ),
            }
        }
        {
            total += 1;
            let pt_wkt = format!("POINT({x1} {y1})");
            let sql = format!("SELECT ST_Y('{pt_wkt}')");
            match run_f64(&ex, &sql) {
                Ok(got) if close(got, y1) => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_Y",
                    &sql,
                    &y1.to_string(),
                    &got.to_string(),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_Y error",
                    &sql,
                    &y1.to_string(),
                    &e,
                ),
            }
        }

        // ── 2. ST_X/ST_Y round-trip through ST_MAKEPOINT output ───────────────
        {
            total += 1;
            // Nest: ST_X(ST_MAKEPOINT(x,y)) should equal x.
            let sql = format!("SELECT ST_X(ST_MAKEPOINT({x2},{y2}))");
            match run_f64(&ex, &sql) {
                Ok(got) if close(got, x2) => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_X(ST_MAKEPOINT)",
                    &sql,
                    &x2.to_string(),
                    &got.to_string(),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_X(ST_MAKEPOINT) error",
                    &sql,
                    &x2.to_string(),
                    &e,
                ),
            }
        }
        {
            total += 1;
            let sql = format!("SELECT ST_Y(ST_MAKEPOINT({x2},{y2}))");
            match run_f64(&ex, &sql) {
                Ok(got) if close(got, y2) => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_Y(ST_MAKEPOINT)",
                    &sql,
                    &y2.to_string(),
                    &got.to_string(),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_Y(ST_MAKEPOINT) error",
                    &sql,
                    &y2.to_string(),
                    &e,
                ),
            }
        }

        // ── 3. ST_DISTANCE_EUCLIDEAN ──────────────────────────────────────────
        // Signature: ST_DISTANCE_EUCLIDEAN(x1, y1, x2, y2)
        {
            total += 1;
            let expected = ref_euclidean(x1, y1, x2, y2);
            let sql = format!("SELECT ST_DISTANCE_EUCLIDEAN({x1},{y1},{x2},{y2})");
            match run_f64(&ex, &sql) {
                Ok(got) if close(got, expected) => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_DISTANCE_EUCLIDEAN",
                    &sql,
                    &format!("{expected:.9}"),
                    &format!("{got:.9}"),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_DISTANCE_EUCLIDEAN error",
                    &sql,
                    &format!("{expected:.9}"),
                    &e,
                ),
            }
        }

        // ── 4. ST_DISTANCE (Haversine) ────────────────────────────────────────
        // Signature: ST_DISTANCE(lat1, lon1, lat2, lon2) → metres
        {
            total += 1;
            let expected = ref_haversine(lat1, lon1, lat2, lon2);
            let sql = format!("SELECT ST_DISTANCE({lat1},{lon1},{lat2},{lon2})");
            match run_f64(&ex, &sql) {
                Ok(got) if close(got, expected) => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_DISTANCE(haversine)",
                    &sql,
                    &format!("{expected:.3}"),
                    &format!("{got:.3}"),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_DISTANCE error",
                    &sql,
                    &format!("{expected:.3}"),
                    &e,
                ),
            }
        }

        // ── 5. Symmetry: ST_DISTANCE(a,b) == ST_DISTANCE(b,a) ────────────────
        {
            total += 1;
            let sql_ab = format!("SELECT ST_DISTANCE({lat1},{lon1},{lat2},{lon2})");
            let sql_ba = format!("SELECT ST_DISTANCE({lat2},{lon2},{lat1},{lon1})");
            let d_ab = run_f64(&ex, &sql_ab);
            let d_ba = run_f64(&ex, &sql_ba);
            match (&d_ab, &d_ba) {
                (Ok(a), Ok(b)) if close(*a, *b) => {}
                (Ok(a), Ok(b)) => {
                    let s = format!(
                        "ST_DISTANCE({lat1},{lon1},{lat2},{lon2}) = {a:.3} != {b:.3} = ST_DISTANCE({lat2},{lon2},{lat1},{lon1})"
                    );
                    report(
                        &mut divergences,
                        max_report,
                        "ST_DISTANCE symmetry",
                        &s,
                        "symmetric",
                        "asymmetric",
                    );
                }
                _ => {}
            }
        }

        // ── 6. Symmetry: ST_DISTANCE_EUCLIDEAN(a,b) == ST_DISTANCE_EUCLIDEAN(b,a)
        {
            total += 1;
            let sql_ab = format!("SELECT ST_DISTANCE_EUCLIDEAN({x1},{y1},{x2},{y2})");
            let sql_ba = format!("SELECT ST_DISTANCE_EUCLIDEAN({x2},{y2},{x1},{y1})");
            let d_ab = run_f64(&ex, &sql_ab);
            let d_ba = run_f64(&ex, &sql_ba);
            match (&d_ab, &d_ba) {
                (Ok(a), Ok(b)) if close(*a, *b) => {}
                (Ok(a), Ok(b)) => {
                    let s = format!(
                        "euclidean({x1},{y1},{x2},{y2})={a:.6} vs ({x2},{y2},{x1},{y1})={b:.6}"
                    );
                    report(
                        &mut divergences,
                        max_report,
                        "ST_DISTANCE_EUCLIDEAN symmetry",
                        &s,
                        "symmetric",
                        "asymmetric",
                    );
                }
                _ => {}
            }
        }

        // ── 7. Self-distance == 0 ─────────────────────────────────────────────
        {
            total += 1;
            let sql = format!("SELECT ST_DISTANCE_EUCLIDEAN({x1},{y1},{x1},{y1})");
            match run_f64(&ex, &sql) {
                Ok(got) if got.abs() < 1e-9 => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "self-euclidean=0",
                    &sql,
                    "0",
                    &got.to_string(),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "self-euclidean error",
                    &sql,
                    "0",
                    &e,
                ),
            }
        }
        {
            total += 1;
            let sql = format!("SELECT ST_DISTANCE({lat1},{lon1},{lat1},{lon1})");
            match run_f64(&ex, &sql) {
                Ok(got) if got.abs() < 1e-6 => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "self-haversine=0",
                    &sql,
                    "0",
                    &got.to_string(),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "self-haversine error",
                    &sql,
                    "0",
                    &e,
                ),
            }
        }

        // ── 8. ST_DWITHIN ─────────────────────────────────────────────────────
        // Signature: ST_DWITHIN(lat1, lon1, lat2, lon2, radius_m)
        {
            total += 1;
            let haversine_d = ref_haversine(lat1, lon1, lat2, lon2);
            // Use a radius that is strictly above or below the actual distance.
            let above = haversine_d + 1.0;
            let below = (haversine_d - 1.0).max(0.0);

            let sql_above = format!("SELECT ST_DWITHIN({lat1},{lon1},{lat2},{lon2},{above})");
            let sql_below = format!("SELECT ST_DWITHIN({lat1},{lon1},{lat2},{lon2},{below})");

            match run_bool(&ex, &sql_above) {
                Ok(true) => {}
                Ok(false) => report(
                    &mut divergences,
                    max_report,
                    "ST_DWITHIN above",
                    &sql_above,
                    "true",
                    "false",
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_DWITHIN above error",
                    &sql_above,
                    "true",
                    &e,
                ),
            }
            // When the two points are distinct, d > 0, so d-1 < d, so should be false.
            if haversine_d > 1.0 {
                total += 1;
                match run_bool(&ex, &sql_below) {
                    Ok(false) => {}
                    Ok(true) => report(
                        &mut divergences,
                        max_report,
                        "ST_DWITHIN below",
                        &sql_below,
                        "false",
                        "true",
                    ),
                    Err(e) => report(
                        &mut divergences,
                        max_report,
                        "ST_DWITHIN below error",
                        &sql_below,
                        "false",
                        &e,
                    ),
                }
            }
        }

        // ── 9. ST_DWITHIN vs ST_DISTANCE consistency ──────────────────────────
        // pick a random radius and verify both agree
        {
            total += 1;
            let radius = rng.int(0, 2_000_000) as f64; // 0..2000 km
            let expected_within = ref_haversine(lat1, lon1, lat2, lon2) <= radius;
            let sql = format!("SELECT ST_DWITHIN({lat1},{lon1},{lat2},{lon2},{radius})");
            match run_bool(&ex, &sql) {
                Ok(got) if got == expected_within => {}
                Ok(got) => {
                    let dist = ref_haversine(lat1, lon1, lat2, lon2);
                    let detail = format!(
                        "haversine={dist:.1}m radius={radius:.1}m => expected {expected_within} got {got}"
                    );
                    report(
                        &mut divergences,
                        max_report,
                        "ST_DWITHIN vs formula",
                        &detail,
                        &expected_within.to_string(),
                        &got.to_string(),
                    );
                }
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_DWITHIN error",
                    &sql,
                    &expected_within.to_string(),
                    &e,
                ),
            }
        }

        // ── 10. ST_AREA (shoelace, 3 to 6 vertices) ───────────────────────────
        {
            let nv = 3 + rng.below(4); // 3..6 vertices
            let mut verts: Vec<(f64, f64)> = (0..nv)
                .map(|_| (rng.int(-10, 10) as f64, rng.int(-10, 10) as f64))
                .collect();
            // Deduplicate consecutive identical vertices (degenerate case handled by ref already)
            verts.dedup();
            if verts.len() < 3 {
                continue;
            }

            let expected_area = ref_area(&verts);
            total += 1;

            // Build SQL: ST_AREA(x1,y1, x2,y2, ...)
            let coord_args: Vec<String> = verts.iter().map(|(x, y)| format!("{x},{y}")).collect();
            let sql = format!("SELECT ST_AREA({})", coord_args.join(","));
            match run_f64(&ex, &sql) {
                Ok(got) if close(got, expected_area) => {}
                Ok(got) => report(
                    &mut divergences,
                    max_report,
                    "ST_AREA",
                    &sql,
                    &format!("{expected_area:.9}"),
                    &format!("{got:.9}"),
                ),
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_AREA error",
                    &sql,
                    &format!("{expected_area:.9}"),
                    &e,
                ),
            }
        }

        // ── 11. ST_CONTAINS ───────────────────────────────────────────────────
        // Use a simple axis-aligned rectangle so the oracle is unambiguous.
        // Rectangle corners: (0,0)-(W,0)-(W,H)-(0,H) where W,H > 0.
        {
            let w = 1 + rng.below(10); // 1..10
            let h = 1 + rng.below(10);
            let (wf, hf) = (w as f64, h as f64);

            // Test point: random, possibly inside or outside.
            let px = rng.int(-2, w as i64 + 2) as f64;
            let py = rng.int(-2, h as i64 + 2) as f64;

            let poly_wkt = format!("POLYGON((0 0, {wf} 0, {wf} {hf}, 0 {hf}, 0 0))");
            let pt_wkt = format!("POINT({px} {py})");
            let poly_pts = vec![(0.0, 0.0), (wf, 0.0), (wf, hf), (0.0, hf), (0.0, 0.0)];
            let expected = ref_contains(&poly_pts, px, py);

            total += 1;
            let sql = format!("SELECT ST_CONTAINS('{poly_wkt}','{pt_wkt}')");
            match run_bool(&ex, &sql) {
                Ok(got) if got == expected => {}
                Ok(got) => {
                    let detail = format!(
                        "poly=({wf}x{hf} rect) pt=({px},{py}) expected={expected} got={got}"
                    );
                    report(
                        &mut divergences,
                        max_report,
                        "ST_CONTAINS",
                        &detail,
                        &expected.to_string(),
                        &got.to_string(),
                    );
                }
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_CONTAINS error",
                    &sql,
                    &expected.to_string(),
                    &e,
                ),
            }
        }

        // ── 12. ST_CONTAINS centroid must be inside ───────────────────────────
        // A convex polygon always contains its centroid.
        {
            let w = 2 + rng.below(8);
            let h = 2 + rng.below(8);
            let (wf, hf) = (w as f64, h as f64);
            // centroid of rectangle
            let cx = wf / 2.0;
            let cy = hf / 2.0;
            let poly_wkt = format!("POLYGON((0 0, {wf} 0, {wf} {hf}, 0 {hf}, 0 0))");
            let pt_wkt = format!("POINT({cx} {cy})");

            total += 1;
            let sql = format!("SELECT ST_CONTAINS('{poly_wkt}','{pt_wkt}')");
            match run_bool(&ex, &sql) {
                Ok(true) => {}
                Ok(false) => {
                    let detail =
                        format!("centroid ({cx},{cy}) of ({wf}x{hf} rect) should be inside");
                    report(
                        &mut divergences,
                        max_report,
                        "ST_CONTAINS centroid",
                        &detail,
                        "true",
                        "false",
                    );
                }
                Err(e) => report(
                    &mut divergences,
                    max_report,
                    "ST_CONTAINS centroid error",
                    &sql,
                    "true",
                    &e,
                ),
            }
        }
    }

    // ── Additional fixed regression cases ────────────────────────────────────

    // R1: Unit square area = 1
    {
        let sql = "SELECT ST_AREA(0,0, 1,0, 1,1, 0,1)";
        total += 1;
        match run_f64(&ex, sql) {
            Ok(got) if close(got, 1.0) => {}
            Ok(got) => report(
                &mut divergences,
                max_report,
                "R1 unit-square area",
                sql,
                "1.0",
                &got.to_string(),
            ),
            Err(e) => report(
                &mut divergences,
                max_report,
                "R1 unit-square error",
                sql,
                "1.0",
                &e,
            ),
        }
    }
    // R2: Equilateral right triangle area = 0.5
    {
        let sql = "SELECT ST_AREA(0,0, 1,0, 0,1)";
        total += 1;
        match run_f64(&ex, sql) {
            Ok(got) if close(got, 0.5) => {}
            Ok(got) => report(
                &mut divergences,
                max_report,
                "R2 right-triangle area",
                sql,
                "0.5",
                &got.to_string(),
            ),
            Err(e) => report(
                &mut divergences,
                max_report,
                "R2 right-triangle error",
                sql,
                "0.5",
                &e,
            ),
        }
    }
    // R3: ST_DISTANCE origin→origin = 0
    {
        let sql = "SELECT ST_DISTANCE(0,0,0,0)";
        total += 1;
        match run_f64(&ex, sql) {
            Ok(got) if got.abs() < 1e-9 => {}
            Ok(got) => report(
                &mut divergences,
                max_report,
                "R3 self-distance=0",
                sql,
                "0",
                &got.to_string(),
            ),
            Err(e) => report(
                &mut divergences,
                max_report,
                "R3 self-distance error",
                sql,
                "0",
                &e,
            ),
        }
    }
    // R4: Euclidean (3,4) → (0,0) = 5
    {
        let sql = "SELECT ST_DISTANCE_EUCLIDEAN(3,4,0,0)";
        total += 1;
        match run_f64(&ex, sql) {
            Ok(got) if close(got, 5.0) => {}
            Ok(got) => report(
                &mut divergences,
                max_report,
                "R4 pythagorean 3-4-5",
                sql,
                "5.0",
                &got.to_string(),
            ),
            Err(e) => report(
                &mut divergences,
                max_report,
                "R4 pythagorean error",
                sql,
                "5.0",
                &e,
            ),
        }
    }
    // R5: ST_X/ST_Y of POINT(7 -3) = 7 / -3
    {
        let sql = "SELECT ST_X('POINT(7 -3)')";
        total += 1;
        match run_f64(&ex, sql) {
            Ok(got) if close(got, 7.0) => {}
            Ok(got) => report(
                &mut divergences,
                max_report,
                "R5 ST_X=7",
                sql,
                "7.0",
                &got.to_string(),
            ),
            Err(e) => report(
                &mut divergences,
                max_report,
                "R5 ST_X error",
                sql,
                "7.0",
                &e,
            ),
        }
    }
    {
        let sql = "SELECT ST_Y('POINT(7 -3)')";
        total += 1;
        match run_f64(&ex, sql) {
            Ok(got) if close(got, -3.0) => {}
            Ok(got) => report(
                &mut divergences,
                max_report,
                "R5 ST_Y=-3",
                sql,
                "-3.0",
                &got.to_string(),
            ),
            Err(e) => report(
                &mut divergences,
                max_report,
                "R5 ST_Y error",
                sql,
                "-3.0",
                &e,
            ),
        }
    }
    // R6: Point strictly outside rectangle should not be contained
    {
        let sql = "SELECT ST_CONTAINS('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))','POINT(6 3)')";
        total += 1;
        match run_bool(&ex, sql) {
            Ok(false) => {}
            Ok(true) => report(
                &mut divergences,
                max_report,
                "R6 outside-rect",
                sql,
                "false",
                "true",
            ),
            Err(e) => report(
                &mut divergences,
                max_report,
                "R6 outside error",
                sql,
                "false",
                &e,
            ),
        }
    }

    println!("\n════ SUMMARY ════");
    println!("checks run         : {total}");
    println!("divergences        : {divergences}");
    if divergences == 0 {
        println!("\nNo geo divergences vs reference math.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
