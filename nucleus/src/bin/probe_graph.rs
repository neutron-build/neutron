//! Graph-model differential fuzzer.
//!
//! Drives Nucleus's persistent graph store through its SQL surface
//! (GRAPH_ADD_NODE / GRAPH_ADD_EDGE / GRAPH_DELETE_NODE / GRAPH_DELETE_EDGE /
//! GRAPH_NEIGHBORS / GRAPH_NODE_COUNT / GRAPH_EDGE_COUNT / GRAPH_SHORTEST_PATH)
//! and compares every observable result against a plain-Rust adjacency-map oracle.
//!
//! Nucleus assigns its own node/edge IDs (u64). The harness captures those IDs
//! from GRAPH_ADD_NODE / GRAPH_ADD_EDGE return values and uses them for all
//! subsequent SQL calls. The oracle maintains a parallel graph keyed on the
//! exact same Nucleus IDs so counts and neighbor sets can be compared directly.
//!
//! Also exercises GRAPH_SHORTEST_PATH_LENGTH and GRAPH_NODE_DEGREE (JSON-based
//! utility functions) against a BFS oracle.
//!
//! Build:
//!   cargo build --release --features server --bin probe_graph
//! Run:
//!   cargo run  --release --features server --bin probe_graph -- --seed 1234 --iterations 3000
#![cfg(feature = "server")]

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
}

// ─── Reference oracle: directed adjacency map ────────────────────────────────
//
// We mirror exactly what the Nucleus graph store does, using the same node IDs
// that Nucleus returns. This way counts and neighbor sets are directly comparable.
#[derive(Default)]
struct RefGraph {
    // node_id → alive
    nodes: HashSet<u64>,
    // edge_id → (from, to)
    edges: HashMap<u64, (u64, u64)>,
    // adjacency: from → Vec<(to, edge_id)>
    out_adj: HashMap<u64, Vec<(u64, u64)>>,
}

impl RefGraph {
    fn add_node(&mut self, id: u64) {
        self.nodes.insert(id);
        self.out_adj.entry(id).or_default();
    }

    fn add_edge(&mut self, eid: u64, from: u64, to: u64) {
        self.edges.insert(eid, (from, to));
        self.out_adj.entry(from).or_default().push((to, eid));
    }

    fn delete_node(&mut self, id: u64) -> bool {
        if !self.nodes.remove(&id) {
            return false;
        }
        // Cascade: remove all edges touching this node
        let dead: Vec<u64> = self
            .edges
            .iter()
            .filter(|(_, (f, t))| *f == id || *t == id)
            .map(|(&eid, _)| eid)
            .collect();
        for eid in dead {
            if let Some((from, _to)) = self.edges.remove(&eid) {
                if let Some(v) = self.out_adj.get_mut(&from) {
                    v.retain(|&(_, e)| e != eid);
                }
            }
        }
        self.out_adj.remove(&id);
        true
    }

    fn delete_edge(&mut self, eid: u64) -> bool {
        if let Some((from, _to)) = self.edges.remove(&eid) {
            if let Some(v) = self.out_adj.get_mut(&from) {
                v.retain(|&(_, e)| e != eid);
            }
            true
        } else {
            false
        }
    }

    fn node_count(&self) -> i64 {
        self.nodes.len() as i64
    }

    fn edge_count(&self) -> i64 {
        self.edges.len() as i64
    }

    fn out_neighbors(&self, node_id: u64) -> BTreeSet<u64> {
        self.out_adj
            .get(&node_id)
            .map(|v| v.iter().map(|&(nb, _)| nb).collect())
            .unwrap_or_default()
    }

    /// BFS shortest path length (directed). Returns None if unreachable.
    fn bfs_path_len(&self, from: u64, to: u64) -> Option<usize> {
        if !self.nodes.contains(&from) || !self.nodes.contains(&to) {
            return None;
        }
        if from == to {
            return Some(0);
        }
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back((from, 0usize));
        visited.insert(from);
        while let Some((cur, dist)) = queue.pop_front() {
            if let Some(v) = self.out_adj.get(&cur) {
                for &(nb, _) in v {
                    if nb == to {
                        return Some(dist + 1);
                    }
                    if visited.insert(nb) {
                        queue.push_back((nb, dist + 1));
                    }
                }
            }
        }
        None
    }

    fn live_node_ids(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self.nodes.iter().copied().collect();
        v.sort(); // deterministic ordering
        v
    }

    fn live_edge_ids(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self.edges.keys().copied().collect();
        v.sort();
        v
    }
}

// ─── Executor helpers ─────────────────────────────────────────────────────────
//
// Each helper executes the SQL EXACTLY ONCE and returns both the parsed result
// and a `panicked` flag.  Using a separate `is_panic_sql` pre-flight would
// execute every mutation twice, corrupting stateful graph operations.

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

fn run_bool(ex: &Executor, sql: &str) -> (Result<bool, ()>, bool) {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Err(_) => (Err(()), true),
        Ok(Ok(mut r)) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Bool(b)) => (Ok(*b), false),
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

// Parse "[{"neighbor_id":N,...},...]" → set of neighbor IDs
fn parse_neighbors_json(s: &str) -> BTreeSet<u64> {
    let mut set = BTreeSet::new();
    let mut rest = s;
    while let Some(pos) = rest.find("\"neighbor_id\":") {
        rest = &rest[pos + "\"neighbor_id\":".len()..];
        let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
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

// Parse "[1,2,3]" or "NULL" → path length
fn parse_path_len(s: &str) -> Option<usize> {
    if s == "NULL" {
        return None;
    }
    let inner = s.trim_start_matches('[').trim_end_matches(']');
    if inner.is_empty() {
        return None; // empty array = no path
    }
    let node_count = inner.split(',').count();
    Some(node_count.saturating_sub(1))
}

// ─── Operation model ──────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
enum Op {
    AddNode,
    AddEdge { from: u64, to: u64 },
    DeleteNode(u64),
    DeleteEdge(u64),
    NodeCount,
    EdgeCount,
    Neighbors(u64),
    ShortestPath { from: u64, to: u64 },
}

fn gen_op(rng: &mut Rng, oracle: &RefGraph) -> Op {
    let nodes = oracle.live_node_ids();
    let edges = oracle.live_edge_ids();
    match rng.below(10) {
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
        5 => Op::NodeCount,
        6 => Op::EdgeCount,
        7 => {
            if !nodes.is_empty() {
                Op::Neighbors(*rng.pick(&nodes))
            } else {
                Op::NodeCount
            }
        }
        _ => {
            if nodes.len() >= 2 {
                let from = *rng.pick(&nodes);
                let to = *rng.pick(&nodes);
                Op::ShortestPath { from, to }
            } else if !nodes.is_empty() {
                let n = *rng.pick(&nodes);
                Op::ShortestPath { from: n, to: n }
            } else {
                Op::NodeCount
            }
        }
    }
}

// ─── Main loop ────────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xABCD_1234;
    let mut iterations = 3000usize;
    let mut ops_per = 60usize;
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

    println!("Nucleus graph differential fuzzer (persistent store vs adjacency-map oracle)");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

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
                    println!("─── DIVERGENCE #{} {} (iter {}) ───", divergences, $label, iter);
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

        // Note: there is no separate `check_panic!` pre-flight here.
        // Each run_* helper executes SQL exactly once and returns a `panicked`
        // flag alongside the result. A pre-flight call would double-execute
        // every mutation and corrupt the graph state.

        for _ in 0..ops_per {
            total_ops += 1;
            let op = gen_op(&mut rng, &oracle);

            match &op {
                Op::AddNode => {
                    let sql = "SELECT GRAPH_ADD_NODE('n')";
                    log.push(sql.to_string());
                    match run_i64(&ex, sql) {
                        (Ok(nid), _) => {
                            // Register the nucleus-assigned ID in our oracle so future
                            // ops use the same IDs as nucleus.
                            oracle.add_node(nid as u64);
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
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
                            // Nucleus accepted the edge; register it in oracle.
                            oracle.add_edge(eid as u64, *from, *to);
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
                        }
                        (Err(()), false) => {
                            // Both nodes exist in oracle, so nucleus should succeed.
                            if oracle.nodes.contains(from) && oracle.nodes.contains(to) {
                                report_div!(
                                    "AddEdge returned Err for live nodes",
                                    &sql,
                                    "Int64(eid)",
                                    "Err"
                                );
                            }
                            // Otherwise (stale oracle ref) — tolerated.
                        }
                    }
                }

                Op::DeleteNode(id) => {
                    let sql = format!("SELECT GRAPH_DELETE_NODE({id})");
                    log.push(sql.clone());
                    let expected = oracle.delete_node(*id);
                    match run_bool(&ex, &sql) {
                        (Ok(got), _) if got == expected => {}
                        (Ok(got), _) => {
                            report_div!(
                                "DeleteNode bool mismatch",
                                &sql,
                                expected,
                                got
                            );
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
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
                    match run_bool(&ex, &sql) {
                        (Ok(got), _) if got == expected => {}
                        (Ok(got), _) => {
                            report_div!(
                                "DeleteEdge bool mismatch",
                                &sql,
                                expected,
                                got
                            );
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
                        }
                        (Err(()), false) => {
                            if expected {
                                report_div!("DeleteEdge returned Err, expected true", &sql, true, "Err");
                            }
                        }
                    }
                }

                Op::NodeCount => {
                    let sql = "SELECT GRAPH_NODE_COUNT()";
                    log.push(sql.to_string());
                    let expected = oracle.node_count();
                    match run_i64(&ex, sql) {
                        (Ok(got), _) if got == expected => {}
                        (Ok(got), _) => {
                            report_div!("NodeCount mismatch", sql, expected, got);
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
                        }
                        (Err(()), false) => {
                            report_div!("NodeCount returned Err", sql, expected, "Err");
                        }
                    }
                }

                Op::EdgeCount => {
                    let sql = "SELECT GRAPH_EDGE_COUNT()";
                    log.push(sql.to_string());
                    let expected = oracle.edge_count();
                    match run_i64(&ex, sql) {
                        (Ok(got), _) if got == expected => {}
                        (Ok(got), _) => {
                            report_div!("EdgeCount mismatch", sql, expected, got);
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
                        }
                        (Err(()), false) => {
                            report_div!("EdgeCount returned Err", sql, expected, "Err");
                        }
                    }
                }

                Op::Neighbors(id) => {
                    let sql = format!("SELECT GRAPH_NEIGHBORS({id},'out')");
                    log.push(sql.clone());
                    let expected = oracle.out_neighbors(*id);
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got = parse_neighbors_json(s);
                            if got != expected {
                                report_div!("Neighbors mismatch", &sql, &expected, &got);
                            }
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
                        }
                        (Err(()), false) => {
                            // If node doesn't exist in oracle, Err is acceptable.
                            if oracle.nodes.contains(id) {
                                report_div!("Neighbors returned Err for live node", &sql, &expected, "Err");
                            }
                        }
                    }
                }

                Op::ShortestPath { from, to } => {
                    let sql = format!("SELECT GRAPH_SHORTEST_PATH({from},{to})");
                    log.push(sql.clone());
                    let expected_len = oracle.bfs_path_len(*from, *to);
                    match run_str(&ex, &sql) {
                        (Ok(ref s), _) => {
                            let got_len = parse_path_len(s);
                            if got_len != expected_len {
                                report_div!(
                                    format!("ShortestPath length mismatch from={from} to={to}"),
                                    &sql,
                                    expected_len,
                                    got_len
                                );
                            }
                        }
                        (Err(()), true) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC #{panics} (iter {iter}) ───\n  sql: {sql}\n");
                            }
                            continue 'outer;
                        }
                        (Err(()), false) => {
                            // Acceptable only if both nodes are not live.
                            if oracle.nodes.contains(from) && oracle.nodes.contains(to) {
                                report_div!("ShortestPath returned Err for live nodes", &sql, expected_len, "Err");
                            }
                        }
                    }
                }
            }
        }
    }

    // ─── JSON-utility section: GRAPH_SHORTEST_PATH_LENGTH + GRAPH_NODE_DEGREE ──
    //
    // These two functions take inline edges_json and do not touch the persistent
    // store, so they are tested separately against a BFS oracle.
    println!("\n── JSON-utility section (GRAPH_SHORTEST_PATH_LENGTH / GRAPH_NODE_DEGREE) ──\n");

    let util_iters = (iterations / 5).max(200);
    let mut util_div = 0usize;
    let mut util_panics = 0usize;

    let cat2 = Arc::new(Catalog::new());
    let st2: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let uex = Arc::new(Executor::new(cat2, st2));

    for iter in 0..util_iters {
        let mut rng = Rng(
            (seed ^ 0xFEED_CAFE)
                .wrapping_add(iter as u64)
                .wrapping_mul(0x100000001B3),
        );

        // Build a small random directed graph.
        let n_nodes = 3 + rng.below(5); // 3..7 nodes; IDs 1..=n_nodes
        let node_ids: Vec<u64> = (1..=(n_nodes as u64)).collect();
        let n_edges = rng.below(n_nodes * 2 + 1);
        let mut edge_list: Vec<(u64, u64)> = Vec::new();
        for _ in 0..n_edges {
            let f = *rng.pick(&node_ids);
            let t = *rng.pick(&node_ids);
            edge_list.push((f, t));
        }

        // Build oracle adjacency for BFS.
        let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
        for &(f, t) in &edge_list {
            adj.entry(f).or_default().push(t);
        }

        let edges_json_str = {
            let parts: Vec<String> = edge_list
                .iter()
                .map(|(f, t)| format!(r#"{{"from":{f},"to":{t}}}"#))
                .collect();
            format!("[{}]", parts.join(","))
        };
        let edges_json_sql = format!("'{edges_json_str}'");

        // GRAPH_SHORTEST_PATH_LENGTH: pick from/to within the node space
        let from_id = *rng.pick(&node_ids);
        let to_id = *rng.pick(&node_ids);

        // BFS oracle for GRAPH_SHORTEST_PATH_LENGTH
        let expected_len: Option<i64> = {
            if from_id == to_id {
                // Self-path: the function requires node to be reachable.
                // Nucleus maps nodes from the JSON; if neither from nor to appears
                // as a node in the edge list, they won't be in the graph.
                // We expect NULL since node doesn't exist in the internal graph.
                // But per graph semantics, a node CAN reach itself (path length 0).
                // Check if node appears in the JSON edge list.
                let in_json = edge_list.iter().any(|(f, t)| *f == from_id || *t == to_id);
                if in_json { Some(0) } else { None }
            } else {
                // BFS
                let mut visited = HashSet::new();
                let mut queue = VecDeque::new();
                queue.push_back((from_id, 0i64));
                visited.insert(from_id);
                let mut result = None;
                'bfs: while let Some((cur, dist)) = queue.pop_front() {
                    if let Some(neighbors) = adj.get(&cur) {
                        for &nb in neighbors {
                            if nb == to_id {
                                result = Some(dist + 1);
                                break 'bfs;
                            }
                            if visited.insert(nb) {
                                queue.push_back((nb, dist + 1));
                            }
                        }
                    }
                }
                // If either node is not in the JSON edge list, nucleus returns NULL
                let from_in = edge_list.iter().any(|(f, t)| *f == from_id || *t == from_id);
                let to_in = edge_list.iter().any(|(f, t)| *f == to_id || *t == to_id);
                if !from_in || !to_in { None } else { result }
            }
        };

        let spl_sql = format!(
            "SELECT GRAPH_SHORTEST_PATH_LENGTH({edges_json_sql},{from_id},{to_id})"
        );
        match run_str(&uex, &spl_sql) {
            (_, true) => {
                util_panics += 1;
                if util_panics <= max_report {
                    println!("─── PANIC #{util_panics} GRAPH_SHORTEST_PATH_LENGTH (util iter {iter}) ───");
                    println!("  sql: {spl_sql}\n");
                }
            }
            (Ok(ref s), false) => {
                let got_len: Option<i64> = if s == "NULL" { None } else { s.parse::<i64>().ok() };
                if got_len != expected_len {
                    util_div += 1;
                    if util_div <= max_report {
                        println!(
                            "─── DIVERGENCE #{util_div} GRAPH_SHORTEST_PATH_LENGTH (util iter {iter}) ───"
                        );
                        println!("  edges_json: {edges_json_str}");
                        println!("  from={from_id} to={to_id}");
                        println!("  expected: {expected_len:?}");
                        println!("  got:      {got_len:?}");
                        println!("  sql: {spl_sql}\n");
                    }
                }
            }
            (Err(()), false) => {
                util_div += 1;
                if util_div <= max_report {
                    println!(
                        "─── DIVERGENCE #{util_div} GRAPH_SHORTEST_PATH_LENGTH Err (util iter {iter}) ───"
                    );
                    println!("  expected: {expected_len:?}  got: Err");
                    println!("  sql: {spl_sql}\n");
                }
            }
        }

        // GRAPH_NODE_DEGREE: pick a node from the full node space (may or may not appear in edges)
        let probe_node = *rng.pick(&node_ids);
        let expected_degree = edge_list
            .iter()
            .filter(|(f, t)| *f == probe_node || *t == probe_node)
            .count() as i64;

        let deg_sql = format!("SELECT GRAPH_NODE_DEGREE({edges_json_sql},{probe_node})");
        match run_i64(&uex, &deg_sql) {
            (_, true) => {
                util_panics += 1;
                if util_panics <= max_report {
                    println!("─── PANIC #{util_panics} GRAPH_NODE_DEGREE (util iter {iter}) ───");
                    println!("  sql: {deg_sql}\n");
                }
            }
            (Ok(d), false) if d == expected_degree => {}
            (Ok(d), false) => {
                util_div += 1;
                if util_div <= max_report {
                    println!(
                        "─── DIVERGENCE #{util_div} GRAPH_NODE_DEGREE (util iter {iter}) ───"
                    );
                    println!("  edges_json: {edges_json_str}");
                    println!("  probe_node={probe_node} expected={expected_degree} got={d}");
                    println!("  sql: {deg_sql}\n");
                }
            }
            (Err(()), false) => {
                util_div += 1;
                if util_div <= max_report {
                    println!(
                        "─── DIVERGENCE #{util_div} GRAPH_NODE_DEGREE Err (util iter {iter}) ───"
                    );
                    println!("  expected={expected_degree}  got=Err");
                    println!("  sql: {deg_sql}\n");
                }
            }
        }
    }

    // ─── Summary ─────────────────────────────────────────────────────────────
    println!("\n════ SUMMARY ════");
    println!("persistent-store ops   : {total_ops}");
    println!("persistent divergences : {divergences}");
    println!("persistent panics      : {panics}");
    println!("util iterations        : {util_iters}");
    println!("util divergences       : {util_div}");
    println!("util panics            : {util_panics}");
    let total = divergences + util_div + panics + util_panics;
    if total == 0 {
        println!("\nNo graph divergences or panics vs reference.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
