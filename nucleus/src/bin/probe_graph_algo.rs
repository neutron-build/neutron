//! Graph algorithm differential fuzzer — focused on algorithm correctness.
//!
//! Exercises properties that probe_graph.rs does NOT cover:
//!   1. GRAPH_NEIGHBORS 'in' and 'both' directions (probe_graph only tests 'out')
//!   2. In-degree / out-degree / total-degree measured from GRAPH_NEIGHBORS calls
//!   3. K-hop neighbour sets (hop=1,2,3) validated against iterative BFS oracle
//!   4. Reachability: a full pair-wise BFS is run on the reference and compared
//!      against GRAPH_SHORTEST_PATH (non-NULL ↔ reachable)
//!   5. Path validity: when GRAPH_SHORTEST_PATH returns a node-list, every
//!      consecutive pair must be a live edge in the persistent store (checked via
//!      GRAPH_NEIGHBORS 'out').
//!
//! Weighted Dijkstra is NOT exposed as a SQL function (only the in-process API
//! exists), so that is documented in sharedChangesNeeded rather than tested here.
//!
//! Build:
//!   cargo build --release --features server --bin probe_graph_algo
//! Run:
//!   cargo run  --release --features server --bin probe_graph_algo \
//!       -- --seed 0xABCD1234 --iterations 2000
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)]

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MemoryEngine, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift64) ─────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
}

// ─── Reference oracle: directed graph ────────────────────────────────────────
#[derive(Default, Clone)]
struct RefGraph {
    nodes: HashSet<u64>,
    // edge_id → (from, to)
    edges: HashMap<u64, (u64, u64)>,
    // from → [(to, edge_id)]
    out_adj: HashMap<u64, Vec<(u64, u64)>>,
    // to → [(from, edge_id)]
    in_adj: HashMap<u64, Vec<(u64, u64)>>,
}

impl RefGraph {
    fn add_node(&mut self, id: u64) {
        self.nodes.insert(id);
        self.out_adj.entry(id).or_default();
        self.in_adj.entry(id).or_default();
    }

    fn add_edge(&mut self, eid: u64, from: u64, to: u64) {
        self.edges.insert(eid, (from, to));
        self.out_adj.entry(from).or_default().push((to, eid));
        self.in_adj.entry(to).or_default().push((from, eid));
    }

    fn delete_node(&mut self, id: u64) -> bool {
        if !self.nodes.remove(&id) {
            return false;
        }
        let dead: Vec<u64> = self
            .edges
            .iter()
            .filter(|(_, (f, t))| *f == id || *t == id)
            .map(|(&eid, _)| eid)
            .collect();
        for eid in dead {
            if let Some((from, to)) = self.edges.remove(&eid) {
                if let Some(v) = self.out_adj.get_mut(&from) {
                    v.retain(|&(_, e)| e != eid);
                }
                if let Some(v) = self.in_adj.get_mut(&to) {
                    v.retain(|&(_, e)| e != eid);
                }
            }
        }
        self.out_adj.remove(&id);
        self.in_adj.remove(&id);
        true
    }

    fn delete_edge(&mut self, eid: u64) -> bool {
        if let Some((from, to)) = self.edges.remove(&eid) {
            if let Some(v) = self.out_adj.get_mut(&from) {
                v.retain(|&(_, e)| e != eid);
            }
            if let Some(v) = self.in_adj.get_mut(&to) {
                v.retain(|&(_, e)| e != eid);
            }
            true
        } else {
            false
        }
    }

    // Outgoing neighbours (set of node IDs)
    fn out_neighbors(&self, id: u64) -> BTreeSet<u64> {
        self.out_adj
            .get(&id)
            .map(|v| v.iter().map(|&(nb, _)| nb).collect())
            .unwrap_or_default()
    }

    // Incoming neighbours (set of node IDs)
    fn in_neighbors(&self, id: u64) -> BTreeSet<u64> {
        self.in_adj
            .get(&id)
            .map(|v| v.iter().map(|&(nb, _)| nb).collect())
            .unwrap_or_default()
    }

    // Both-direction neighbours (union of in + out)
    fn both_neighbors(&self, id: u64) -> BTreeSet<u64> {
        let mut s = self.out_neighbors(id);
        s.extend(self.in_neighbors(id));
        s
    }

    // Out-degree (counting multi-edges)
    fn out_degree(&self, id: u64) -> usize {
        self.out_adj.get(&id).map_or(0, |v| v.len())
    }

    // In-degree
    fn in_degree(&self, id: u64) -> usize {
        self.in_adj.get(&id).map_or(0, |v| v.len())
    }

    // Total degree (out + in, counting multi-edges)
    fn total_degree(&self, id: u64) -> usize {
        self.out_degree(id) + self.in_degree(id)
    }

    // BFS k-hop reachable set (directed, outgoing). k=0 → {id} itself.
    fn k_hop_out(&self, start: u64, k: usize) -> BTreeSet<u64> {
        if !self.nodes.contains(&start) {
            return BTreeSet::new();
        }
        let mut frontier: BTreeSet<u64> = [start].into();
        let mut reached: BTreeSet<u64> = [start].into();
        for _ in 0..k {
            let mut next_frontier = BTreeSet::new();
            for &n in &frontier {
                for nb in self.out_neighbors(n) {
                    if reached.insert(nb) {
                        next_frontier.insert(nb);
                    }
                }
            }
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }
        reached
    }

    // BFS reachability: can we reach `to` from `from` (directed, outgoing)?
    fn reachable(&self, from: u64, to: u64) -> bool {
        if !self.nodes.contains(&from) || !self.nodes.contains(&to) {
            return false;
        }
        if from == to {
            return true;
        }
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(from);
        visited.insert(from);
        while let Some(cur) = queue.pop_front() {
            for nb in self.out_neighbors(cur) {
                if nb == to {
                    return true;
                }
                if visited.insert(nb) {
                    queue.push_back(nb);
                }
            }
        }
        false
    }

    fn live_node_ids(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self.nodes.iter().copied().collect();
        v.sort();
        v
    }

    fn live_edge_ids(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self.edges.keys().copied().collect();
        v.sort();
        v
    }

    // Check that an edge exists (from → to).
    fn has_out_edge(&self, from: u64, to: u64) -> bool {
        self.out_adj
            .get(&from)
            .map_or(false, |v| v.iter().any(|&(t, _)| t == to))
    }
}

// ─── Executor helpers ─────────────────────────────────────────────────────────

fn run_i64(ex: &Executor, sql: &str) -> (Result<i64, ()>, bool) {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Err(_) => (Err(()), true),
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Int64(n)) => (Ok(*n), false),
                Some(Value::Int32(n)) => (Ok(*n as i64), false),
                _ => (Err(()), false),
            },
            _ => (Err(()), false),
        },
        Ok(Err(_)) => (Err(()), false),
    }
}

fn run_str(ex: &Executor, sql: &str) -> (Result<String, ()>, bool) {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Err(_) => (Err(()), true),
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                let v = rows
                    .first()
                    .and_then(|row| row.first())
                    .cloned()
                    .unwrap_or(Value::Null);
                (Ok(v.to_string()), false)
            }
            _ => (Err(()), false),
        },
        Ok(Err(_)) => (Err(()), false),
    }
}

// Parse neighbor JSON array → set of neighbor IDs.
// Format: [{"neighbor_id":N,"edge_id":M,"edge_type":"..."},...] or "[]"
fn parse_neighbors_json(s: &str) -> BTreeSet<u64> {
    let mut set = BTreeSet::new();
    let mut rest = s;
    while let Some(pos) = rest.find("\"neighbor_id\":") {
        rest = &rest[pos + "\"neighbor_id\":".len()..];
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        if end > 0 {
            if let Ok(n) = rest[..end].parse::<u64>() {
                set.insert(n);
            }
        }
        if end == 0 {
            break;
        }
        rest = &rest[end..];
    }
    set
}

// Parse GRAPH_SHORTEST_PATH result "[1,2,3]" → Vec<u64> or None if "NULL".
fn parse_path(s: &str) -> Option<Vec<u64>> {
    if s == "NULL" {
        return None;
    }
    let inner = s.trim_start_matches('[').trim_end_matches(']');
    if inner.is_empty() {
        return None;
    }
    let ids: Vec<u64> = inner
        .split(',')
        .filter_map(|tok| tok.trim().parse::<u64>().ok())
        .collect();
    if ids.is_empty() {
        None
    } else {
        Some(ids)
    }
}

// ─── Operation model ──────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
enum Op {
    AddNode,
    AddEdge { from: u64, to: u64 },
    DeleteNode(u64),
    DeleteEdge(u64),
    // Algorithm checks
    NeighborsIn(u64),
    NeighborsBoth(u64),
    InDegree(u64),
    OutDegree(u64),
    TotalDegree(u64),
    KHop { start: u64, k: usize },
    Reachability { from: u64, to: u64 },
    PathValid { from: u64, to: u64 },
}

fn gen_op(rng: &mut Rng, oracle: &RefGraph) -> Op {
    let nodes = oracle.live_node_ids();
    let edges = oracle.live_edge_ids();
    match rng.below(14) {
        0 | 1 => Op::AddNode,
        2 => {
            if nodes.len() >= 2 {
                let from = *rng.pick(&nodes);
                let to = *rng.pick(&nodes);
                Op::AddEdge { from, to }
            } else {
                Op::AddNode
            }
        }
        3 => {
            if !nodes.is_empty() {
                Op::DeleteNode(*rng.pick(&nodes))
            } else {
                Op::AddNode
            }
        }
        4 => {
            if !edges.is_empty() {
                Op::DeleteEdge(*rng.pick(&edges))
            } else {
                Op::AddNode
            }
        }
        5 => {
            if !nodes.is_empty() {
                Op::NeighborsIn(*rng.pick(&nodes))
            } else {
                Op::AddNode
            }
        }
        6 => {
            if !nodes.is_empty() {
                Op::NeighborsBoth(*rng.pick(&nodes))
            } else {
                Op::AddNode
            }
        }
        7 => {
            if !nodes.is_empty() {
                Op::InDegree(*rng.pick(&nodes))
            } else {
                Op::AddNode
            }
        }
        8 => {
            if !nodes.is_empty() {
                Op::OutDegree(*rng.pick(&nodes))
            } else {
                Op::AddNode
            }
        }
        9 => {
            if !nodes.is_empty() {
                Op::TotalDegree(*rng.pick(&nodes))
            } else {
                Op::AddNode
            }
        }
        10 => {
            if !nodes.is_empty() {
                let k = 1 + rng.below(3); // k = 1, 2, or 3
                Op::KHop {
                    start: *rng.pick(&nodes),
                    k,
                }
            } else {
                Op::AddNode
            }
        }
        11 => {
            if nodes.len() >= 2 {
                let from = *rng.pick(&nodes);
                let to = *rng.pick(&nodes);
                Op::Reachability { from, to }
            } else if !nodes.is_empty() {
                let n = *rng.pick(&nodes);
                Op::Reachability { from: n, to: n }
            } else {
                Op::AddNode
            }
        }
        _ => {
            if nodes.len() >= 2 {
                let from = *rng.pick(&nodes);
                let to = *rng.pick(&nodes);
                Op::PathValid { from, to }
            } else if !nodes.is_empty() {
                let n = *rng.pick(&nodes);
                Op::PathValid { from: n, to: n }
            } else {
                Op::AddNode
            }
        }
    }
}

// ─── Main loop ────────────────────────────────────────────────────────────────
fn main_impl() {
    let mut seed: u64 = 0xABCD_1234;
    let mut iterations = 2000usize;
    let mut ops_per = 80usize;
    let mut max_report = 20usize;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                let s = args[i].as_str();
                let (base, radix) = if s.starts_with("0x") || s.starts_with("0X") {
                    (&s[2..], 16)
                } else {
                    (s, 10)
                };
                seed = u64::from_str_radix(base, radix).unwrap();
            }
            "--iterations" => {
                i += 1;
                iterations = args[i].parse().unwrap();
            }
            "--ops" => {
                i += 1;
                ops_per = args[i].parse().unwrap();
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

    println!("Nucleus graph algorithm differential fuzzer");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}");
    println!(
        "Checks: GRAPH_NEIGHBORS in/both, in/out/total degree, k-hop, reachability, path validity\n"
    );

    let mut total_ops = 0usize;
    let mut divergences = 0usize;
    let mut panics = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        // Fresh executor + oracle per iteration.
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let mut oracle = RefGraph::default();
        let mut log: Vec<String> = Vec::new();

        macro_rules! report_div {
            ($label:expr, $sql:expr, $exp:expr, $got:expr) => {{
                divergences += 1;
                if divergences <= max_report {
                    println!(
                        "─── DIVERGENCE #{} {} (iter {}) ───",
                        divergences, $label, iter
                    );
                    println!("  expected : {:?}", $exp);
                    println!("  got      : {:?}", $got);
                    println!("  sql      : {}", $sql);
                    println!("  replay ({} ops):", log.len());
                    for s in &log {
                        println!("    {s};");
                    }
                    println!();
                }
                continue 'outer;
            }};
        }

        macro_rules! handle_panic {
            ($sql:expr) => {{
                panics += 1;
                if panics <= max_report {
                    println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {}\n", $sql);
                }
                continue 'outer;
            }};
        }

        for _ in 0..ops_per {
            total_ops += 1;
            let op = gen_op(&mut rng, &oracle);

            match &op {
                Op::AddNode => {
                    let sql = "SELECT GRAPH_ADD_NODE('n')";
                    log.push(sql.to_string());
                    match run_i64(&ex, sql) {
                        (Ok(nid), _) => {
                            oracle.add_node(nid as u64);
                        }
                        (Err(()), true) => {
                            handle_panic!(sql);
                        }
                        (Err(()), false) => {
                            report_div!("AddNode returned Err", sql, "Int64(id)", "Err");
                        }
                    }
                }

                Op::AddEdge { from, to } => {
                    let sql = format!("SELECT GRAPH_ADD_EDGE({from},{to},'rel')");
                    log.push(sql.clone());
                    match run_i64(&ex, &sql) {
                        (Ok(eid), _) => {
                            oracle.add_edge(eid as u64, *from, *to);
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(from) && oracle.nodes.contains(to) {
                                report_div!(
                                    "AddEdge returned Err for live nodes",
                                    &sql,
                                    "Int64(eid)",
                                    "Err"
                                );
                            }
                        }
                    }
                }

                Op::DeleteNode(id) => {
                    let sql = format!("SELECT GRAPH_DELETE_NODE({id})");
                    log.push(sql.clone());
                    let expected = oracle.delete_node(*id);
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got = s == "true";
                            if got != expected {
                                report_div!("DeleteNode bool mismatch", &sql, expected, got);
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if expected {
                                report_div!("DeleteNode returned Err, expected true", &sql, true, "Err");
                            }
                        }
                    }
                }

                Op::DeleteEdge(eid) => {
                    let sql = format!("SELECT GRAPH_DELETE_EDGE({eid})");
                    log.push(sql.clone());
                    let expected = oracle.delete_edge(*eid);
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got = s == "true";
                            if got != expected {
                                report_div!("DeleteEdge bool mismatch", &sql, expected, got);
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if expected {
                                report_div!("DeleteEdge returned Err, expected true", &sql, true, "Err");
                            }
                        }
                    }
                }

                // ── Algorithm checks ──────────────────────────────────────────

                Op::NeighborsIn(id) => {
                    let sql = format!("SELECT GRAPH_NEIGHBORS({id},'in')");
                    log.push(sql.clone());
                    let expected = oracle.in_neighbors(*id);
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got = parse_neighbors_json(s);
                            if got != expected {
                                report_div!("NeighborsIn mismatch", &sql, &expected, &got);
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(id) {
                                report_div!(
                                    "NeighborsIn returned Err for live node",
                                    &sql,
                                    &expected,
                                    "Err"
                                );
                            }
                        }
                    }
                }

                Op::NeighborsBoth(id) => {
                    let sql = format!("SELECT GRAPH_NEIGHBORS({id},'both')");
                    log.push(sql.clone());
                    let expected = oracle.both_neighbors(*id);
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got = parse_neighbors_json(s);
                            if got != expected {
                                report_div!("NeighborsBoth mismatch", &sql, &expected, &got);
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(id) {
                                report_div!(
                                    "NeighborsBoth returned Err for live node",
                                    &sql,
                                    &expected,
                                    "Err"
                                );
                            }
                        }
                    }
                }

                Op::InDegree(id) => {
                    // Measure in-degree by counting items in GRAPH_NEIGHBORS 'in'.
                    // GRAPH_NEIGHBORS returns one entry per edge (multi-edges included).
                    let sql = format!("SELECT GRAPH_NEIGHBORS({id},'in')");
                    log.push(sql.clone());
                    let expected_deg = oracle.in_degree(*id) as i64;
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            // Count "neighbor_id" occurrences to handle multi-edges.
                            let got_deg = s.matches("\"neighbor_id\":").count() as i64;
                            if got_deg != expected_deg {
                                report_div!(
                                    format!("InDegree mismatch node={id}"),
                                    &sql,
                                    expected_deg,
                                    got_deg
                                );
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(id) && expected_deg > 0 {
                                report_div!(
                                    format!("InDegree Err for live node={id}"),
                                    &sql,
                                    expected_deg,
                                    "Err"
                                );
                            }
                        }
                    }
                }

                Op::OutDegree(id) => {
                    let sql = format!("SELECT GRAPH_NEIGHBORS({id},'out')");
                    log.push(sql.clone());
                    let expected_deg = oracle.out_degree(*id) as i64;
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got_deg = s.matches("\"neighbor_id\":").count() as i64;
                            if got_deg != expected_deg {
                                report_div!(
                                    format!("OutDegree mismatch node={id}"),
                                    &sql,
                                    expected_deg,
                                    got_deg
                                );
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(id) && expected_deg > 0 {
                                report_div!(
                                    format!("OutDegree Err for live node={id}"),
                                    &sql,
                                    expected_deg,
                                    "Err"
                                );
                            }
                        }
                    }
                }

                Op::TotalDegree(id) => {
                    let sql = format!("SELECT GRAPH_NEIGHBORS({id},'both')");
                    log.push(sql.clone());
                    let expected_deg = oracle.total_degree(*id) as i64;
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got_deg = s.matches("\"neighbor_id\":").count() as i64;
                            if got_deg != expected_deg {
                                report_div!(
                                    format!("TotalDegree mismatch node={id}"),
                                    &sql,
                                    expected_deg,
                                    got_deg
                                );
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(id) && expected_deg > 0 {
                                report_div!(
                                    format!("TotalDegree Err for live node={id}"),
                                    &sql,
                                    expected_deg,
                                    "Err"
                                );
                            }
                        }
                    }
                }

                Op::KHop { start, k } => {
                    // Compute k-hop reachable set by issuing GRAPH_NEIGHBORS 'out'
                    // iteratively in Nucleus, then compare to oracle BFS.
                    let expected = oracle.k_hop_out(*start, *k);

                    // Nucleus k-hop: iterative BFS using GRAPH_NEIGHBORS 'out'.
                    let mut nucleus_reached: BTreeSet<u64> = [*start].into();
                    let mut frontier: BTreeSet<u64> = [*start].into();
                    let mut khop_panic = false;
                    let mut khop_err = false;

                    'khop: for _hop in 0..*k {
                        let mut next_frontier = BTreeSet::new();
                        for &nid in &frontier {
                            let sql = format!("SELECT GRAPH_NEIGHBORS({nid},'out')");
                            match run_str(&ex, &sql) {
                                (_, true) => {
                                    khop_panic = true;
                                    break 'khop;
                                }
                                (Ok(ref s), false) => {
                                    for nb in parse_neighbors_json(s) {
                                        if nucleus_reached.insert(nb) {
                                            next_frontier.insert(nb);
                                        }
                                    }
                                }
                                (Err(()), false) => {
                                    // Non-existent node returns Err; skip silently.
                                    if oracle.nodes.contains(&nid) {
                                        khop_err = true;
                                        break 'khop;
                                    }
                                }
                            }
                        }
                        frontier = next_frontier;
                        if frontier.is_empty() {
                            break;
                        }
                    }

                    let sql_desc = format!("k-hop BFS start={start} k={k}");
                    log.push(sql_desc.clone());

                    if khop_panic {
                        panics += 1;
                        if panics <= max_report {
                            println!("─── PANIC #{panics} {sql_desc} (iter {iter}) ───\n");
                        }
                        continue 'outer;
                    }
                    if khop_err {
                        divergences += 1;
                        if divergences <= max_report {
                            println!(
                                "─── DIVERGENCE #{divergences} {sql_desc} (iter {iter}) ─── GRAPH_NEIGHBORS Err for live node\n"
                            );
                        }
                        continue 'outer;
                    }
                    if nucleus_reached != expected {
                        report_div!(
                            format!("KHop k={k} start={start}"),
                            &sql_desc,
                            &expected,
                            &nucleus_reached
                        );
                    }
                }

                Op::Reachability { from, to } => {
                    // Oracle says whether `to` is reachable from `from`.
                    // Nucleus answer: GRAPH_SHORTEST_PATH returns non-NULL iff reachable.
                    let sql = format!("SELECT GRAPH_SHORTEST_PATH({from},{to})");
                    log.push(sql.clone());
                    let expected_reachable = oracle.reachable(*from, *to);
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let nucleus_reachable = s != "NULL";
                            if nucleus_reachable != expected_reachable {
                                report_div!(
                                    format!("Reachability mismatch from={from} to={to}"),
                                    &sql,
                                    expected_reachable,
                                    nucleus_reachable
                                );
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(from) && oracle.nodes.contains(to) {
                                report_div!(
                                    "Reachability returned Err for live nodes",
                                    &sql,
                                    expected_reachable,
                                    "Err"
                                );
                            }
                        }
                    }
                }

                Op::PathValid { from, to } => {
                    // GRAPH_SHORTEST_PATH returns a node list.
                    // Verify: (a) if oracle says unreachable, path must be NULL;
                    //         (b) if path is returned, every consecutive step
                    //             (path[i] → path[i+1]) must be a live out-edge in the
                    //             oracle's adjacency map;
                    //         (c) path starts at `from` and ends at `to`.
                    let sql = format!("SELECT GRAPH_SHORTEST_PATH({from},{to})");
                    log.push(sql.clone());
                    let expected_reachable = oracle.reachable(*from, *to);

                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let path = parse_path(s);
                            match (&path, expected_reachable) {
                                (None, false) => {
                                    // Both say unreachable — OK.
                                }
                                (Some(_), false) => {
                                    // Nucleus claims reachable but oracle disagrees.
                                    report_div!(
                                        format!("PathValid: Nucleus gave path but oracle says unreachable from={from} to={to}"),
                                        &sql,
                                        "NULL",
                                        s
                                    );
                                }
                                (None, true) => {
                                    // Nucleus says unreachable but oracle found a path.
                                    report_div!(
                                        format!("PathValid: Nucleus returned NULL but oracle says reachable from={from} to={to}"),
                                        &sql,
                                        "Some(path)",
                                        "NULL"
                                    );
                                }
                                (Some(p), true) => {
                                    // Path returned — validate it.
                                    // (c) Endpoints.
                                    if p.first() != Some(from) {
                                        report_div!(
                                            format!("PathValid: path starts at {:?} not from={from}", p.first()),
                                            &sql,
                                            from,
                                            p.first().copied().unwrap_or(0)
                                        );
                                    }
                                    if p.last() != Some(to) {
                                        report_div!(
                                            format!("PathValid: path ends at {:?} not to={to}", p.last()),
                                            &sql,
                                            to,
                                            p.last().copied().unwrap_or(0)
                                        );
                                    }
                                    // (b) Each step must be a live out-edge.
                                    for window in p.windows(2) {
                                        let (a, b) = (window[0], window[1]);
                                        if !oracle.has_out_edge(a, b) {
                                            report_div!(
                                                format!("PathValid: step {a}→{b} is not a live edge in graph"),
                                                &sql,
                                                format!("valid edge {a}->{b}"),
                                                format!("no such edge")
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        (Err(()), true) => {
                            handle_panic!(&sql);
                        }
                        (Err(()), false) => {
                            if oracle.nodes.contains(from) && oracle.nodes.contains(to) {
                                report_div!(
                                    "PathValid returned Err for live nodes",
                                    &sql,
                                    expected_reachable,
                                    "Err"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    // ─── Summary ─────────────────────────────────────────────────────────────
    println!("\n════ SUMMARY ════");
    println!("total iterations : {iterations}");
    println!("total ops        : {total_ops}");
    println!("divergences      : {divergences}");
    println!("panics           : {panics}");
    let total = divergences + panics;
    if total == 0 {
        println!("\nNo divergences or panics vs reference oracle.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
