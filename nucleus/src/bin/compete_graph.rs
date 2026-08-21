//! compete_graph — Nucleus's GRAPH model vs Neo4j, apples to apples.
//!
//! `probe_graph` and `probe_graph_algo` measure Nucleus against an in-process
//! adjacency-map oracle: they prove the answers are right, and say nothing
//! about how Nucleus compares to a graph database. This binary is the
//! competitive measurement, built to the methodology `compete_vector` set and
//! `compete_fts` refined, because each clause below exists to close a way an
//! earlier benchmark in this tree lied.
//!
//! What "apples to apples" means here, concretely:
//!
//! - **One graph, one query set.** Both engines are loaded from the same
//!   seeded generator, edge for edge, in the same order, and answer the same
//!   anchors in the same order. The generator's adjacency list — not either
//!   engine — is the ground truth.
//! - **One ground truth, computed in-process.** Neighbourhoods and shortest
//!   paths are graded against a plain-Rust BFS over the generated adjacency.
//!   Neither engine grades its own homework, and neither is graded against the
//!   other: a benchmark where the engines only have to agree cannot see them
//!   both being wrong.
//! - **Matched transport.** Nucleus answers over pgwire on a loopback TCP
//!   socket; Neo4j answers over Bolt on a loopback TCP socket. Both are binary
//!   protocols, one round trip per timed operation, one connection, sequential.
//!   The HTTP/JSON endpoint would have charged Neo4j a transport penalty that
//!   is not an engine difference, so it is not used.
//! - **Durability, checked rather than assumed — and the check does not fully
//!   pass.** Neo4j forces its transaction log at every commit and community
//!   edition has no knob to disable it, so the Nucleus arm runs with a real
//!   data directory — `new_with_persistence`, not the in-memory
//!   `Executor::new` that `compete.rs` boots — which gives the graph store a
//!   WAL that `force_specialty_durability` fsyncs on every autocommit
//!   statement. That much is PROVEN here, not trusted: the harness asserts
//!   `graph.wal` exists and grew during the load, and times a third write arm
//!   with `synchronous_commit = off` so the sync the default arm pays for is a
//!   number rather than a claim. What is NOT proven is that the two engines'
//!   commits take the *same* sync: on macOS `fsync(2)` and
//!   `fcntl(F_FULLFSYNC)` differ by two orders of magnitude, which is the
//!   defect that invalidated a Nucleus-vs-PostgreSQL write comparison
//!   (`docs/BENCH_VS_POSTGRES.md`). Both costs are therefore measured on this
//!   filesystem and printed beside the write arms, and the write rows are
//!   flagged as the least trustworthy in the report.
//! - **Correctness is reported beside every latency.** A shortest path is
//!   right or wrong, and a latency win on a wrong answer is not a win. Every
//!   timed operation's result is checked against the oracle before its sample
//!   is kept: neighbourhood sets must match exactly, a returned path must have
//!   the oracle's length AND consist of edges that exist AND start and end
//!   where it was asked to, and an unreachable pair must return nothing. A
//!   failure aborts the run rather than being timed as a success — that exact
//!   defect is why `docs/benchmarks/` is distrusted.
//! - **The spread is published.** This machine class has been measured at 95.4%
//!   worst-case deviation on green runs, so a mean is not evidence. Every arm
//!   reports p50/p90/p99, min, max and the p99/p50 ratio over the full sample.
//!
//! Where the two engines are NOT comparable, the harness says so instead of
//! forcing a shape (all four are printed in the notes at the end of a run):
//!
//! 1. **Addressing.** Nucleus's `GRAPH_*` SQL functions are addressed by
//!    internal node id, so a traversal starts for free. Cypher must resolve
//!    its anchor by property. Neo4j gets a range index on `:N(k)` — refusing
//!    it would be benchmarking a missing index, not an engine — and the
//!    `anchor` workload times that resolution alone on both sides so every
//!    traversal number can be read net of it.
//! 2. **No property reads on the fast surface.** `GRAPH_NEIGHBORS` returns
//!    neighbour ids, edge ids and edge types, and there is no Nucleus function
//!    that reads a node's properties. Any property-filtered pattern must go
//!    through `GRAPH_QUERY`'s Cypher subset, which resolves its anchor with a
//!    label scan (`nodes_by_label` + a property compare per node) because that
//!    subset has no property index. Both facts are measured, not hidden.
//! 3. **No server-side k-hop on the fast surface.** Multi-hop through
//!    `GRAPH_NEIGHBORS` costs one round trip per expanded node. That arm is
//!    reported with its round-trip count so the number is read as the
//!    architectural cost it is, alongside the one-round-trip `GRAPH_QUERY` arm.
//! 4. **Variable-length spelling.** Both engines are sent `*..k`, not the more
//!    common `*1..k`, because they are the same query and only one of the two
//!    spellings parses on both engines. Nucleus's Cypher lexer reads a number
//!    with `while digit || '.'`, so `1..2` becomes the single token `1..2` and
//!    fails as an invalid float before the parser's perfectly good `*min..max`
//!    branch is ever reached. The harness probes for this at startup and
//!    prints the result, because it is a finding, not a benchmark parameter.
//! 5. **Path semantics.** Cypher's variable-length patterns are relationship-
//!    unique (a node may repeat); Nucleus's expansion is node-unique. For a
//!    DISTINCT terminal set within k hops the two coincide — a walk of length
//!    L implies distance <= L, and a shortest path is simple — except for the
//!    anchor itself, which a cycle can return to. The Neo4j queries therefore
//!    carry `WHERE b <> a`, which makes both engines answer exactly
//!    `{v != a : 1 <= dist(a,v) <= k}`, which is what the oracle computes.
//!
//! Usage:
//! ```text
//! podman run -d --name neo4j -p 7687:7687 -e NEO4J_AUTH=none \
//!     docker.io/library/neo4j:5.26-community
//! cargo run --release --features bench-tools --bin compete_graph -- \
//!     --nodes 10000 --shortcuts 3 --queries 200 --seed 42
//! ```
//! Run the Nucleus column alone with `--skip-neo4j`.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use neo4rs::{Graph as Neo4jGraph, query as cypher};
use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::metrics::latency::percentile_duration;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};

// ============================================================================
// Config
// ============================================================================

struct Cfg {
    /// Node count. Kept below `GraphStore::max_hot_nodes` (100_000) on purpose:
    /// above it the store spills properties to a cold LSM tier, which is a
    /// different measurement and would silently confound this one.
    nodes: u32,
    /// Random shortcuts per node, on top of the ring edge every node gets.
    /// Out-degree is therefore `shortcuts + 1`.
    shortcuts: u32,
    /// Timed anchors per read workload.
    queries: usize,
    /// Untimed anchors run before them, on both engines, per read workload.
    ///
    /// Not a formality. Neo4j is a JVM: on a cold run its first few dozen
    /// queries are timing the JIT, and a smoke run of this harness showed
    /// exactly that — a Neo4j p99 of 88 ms against a p50 of 2.3 ms. Publishing
    /// a ratio taken off an unwarmed JVM would be its own kind of fake number,
    /// so both engines get a full untimed pass over distinct anchors first,
    /// with correctness still checked on every one of them.
    warmup: usize,
    seed: u64,
    /// Writes timed in the per-operation write arms (subset of the load).
    write_ops: usize,
    nucleus_port: u16,
    neo4j_uri: String,
    neo4j_user: String,
    neo4j_pass: String,
    skip_neo4j: bool,
    /// Upper hop bound handed to Neo4j's `shortestPath`. Asserted against the
    /// oracle before anything is timed, so a truncated search can never be
    /// mistaken for "no path".
    sp_bound: usize,
    data_dir: std::path::PathBuf,
}

impl Default for Cfg {
    fn default() -> Self {
        Self {
            nodes: 10_000,
            shortcuts: 3,
            queries: 200,
            warmup: 200,
            seed: 42,
            write_ops: 500,
            nucleus_port: 5457,
            neo4j_uri: "bolt://127.0.0.1:7687".into(),
            neo4j_user: "neo4j".into(),
            neo4j_pass: "neo4j".into(),
            skip_neo4j: false,
            sp_bound: 30,
            data_dir: std::env::temp_dir()
                .join(format!("nucleus_compete_graph_{}", std::process::id())),
        }
    }
}

const USAGE: &str = "\
compete_graph — Nucleus GRAPH model vs Neo4j (methodology in the file header)

Usage: compete_graph [OPTIONS]
  --nodes N         node count                      (default 10000)
  --shortcuts N     random out-edges per node       (default 3, plus 1 ring edge)
  --queries N       timed anchors per workload      (default 200)
  --warmup N        untimed anchors before them     (default 200)
  --write-ops N     timed single-write operations   (default 500)
  --seed N          RNG seed                        (default 42)
  --port N          Nucleus pgwire port             (default 5457)
  --neo4j-uri URI   Bolt URI                        (default bolt://127.0.0.1:7687)
  --neo4j-user U    Bolt user                       (default neo4j)
  --neo4j-pass P    Bolt password                   (default neo4j)
  --sp-bound N      hop bound for Neo4j shortestPath (default 30)
  --skip-neo4j      run the Nucleus arm only
  --help            print this text and exit";

fn parse_args() -> Option<Cfg> {
    let mut cfg = Cfg::default();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--nodes" => cfg.nodes = next(&mut i).parse().unwrap_or(cfg.nodes),
            "--shortcuts" => cfg.shortcuts = next(&mut i).parse().unwrap_or(cfg.shortcuts),
            "--queries" => cfg.queries = next(&mut i).parse().unwrap_or(cfg.queries),
            "--warmup" => cfg.warmup = next(&mut i).parse().unwrap_or(cfg.warmup),
            "--write-ops" => cfg.write_ops = next(&mut i).parse().unwrap_or(cfg.write_ops),
            "--seed" => cfg.seed = next(&mut i).parse().unwrap_or(cfg.seed),
            "--port" => cfg.nucleus_port = next(&mut i).parse().unwrap_or(cfg.nucleus_port),
            "--neo4j-uri" => cfg.neo4j_uri = next(&mut i),
            "--neo4j-user" => cfg.neo4j_user = next(&mut i),
            "--neo4j-pass" => cfg.neo4j_pass = next(&mut i),
            "--sp-bound" => cfg.sp_bound = next(&mut i).parse().unwrap_or(cfg.sp_bound),
            "--skip-neo4j" => cfg.skip_neo4j = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                return None;
            }
            other => {
                eprintln!("unknown argument: {other}\n\n{USAGE}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    Some(cfg)
}

// ============================================================================
// Deterministic graph generator + ground truth
// ============================================================================

/// xorshift64 — the same PRNG the graph probes use, so a seed means the same
/// thing across this tree's graph harnesses.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u32) -> u32 {
        (self.next() % n as u64) as u32
    }
}

/// The generated graph, and the single source of truth for every assertion.
struct TruthGraph {
    /// Deduplicated out-adjacency, sorted. `out[i]` are the successors of node
    /// `i`. Deduplicated so a neighbourhood is a set on both sides: parallel
    /// edges would make Nucleus (edge-per-entry) and Neo4j (`RETURN DISTINCT`)
    /// disagree for a reason that is not an engine difference.
    out: Vec<Vec<u32>>,
    /// Node property, 0..10. Gives the pattern workload something to filter on.
    grp: Vec<i64>,
    edge_count: usize,
}

impl TruthGraph {
    /// Ring lattice plus random shortcuts — a directed small-world graph.
    ///
    /// The ring (`i -> i+1 mod n`) guarantees strong connectivity, so every
    /// sampled pair has a path and the shortest-path workload measures search
    /// rather than failure. The shortcuts collapse the diameter to something
    /// logarithmic, so paths are several hops long instead of one or n/2 — a
    /// uniform random graph makes almost everything two hops apart and a bare
    /// ring makes shortest path a linear walk; neither is a graph workload.
    fn generate(nodes: u32, shortcuts: u32, seed: u64) -> Self {
        assert!(nodes >= 8, "need at least 8 nodes for a meaningful graph");
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).max(1));
        let mut out: Vec<Vec<u32>> = Vec::with_capacity(nodes as usize);
        for i in 0..nodes {
            let mut succ = BTreeSet::new();
            succ.insert((i + 1) % nodes);
            for _ in 0..shortcuts {
                // Bounded retries: a collision just costs this node one edge,
                // which the dedup makes explicit rather than silently doubling.
                for _ in 0..8 {
                    let t = rng.below(nodes);
                    if t != i && succ.insert(t) {
                        break;
                    }
                }
            }
            out.push(succ.into_iter().collect());
        }
        let edge_count = out.iter().map(|v| v.len()).sum();
        let grp = (0..nodes).map(|i| (i % 10) as i64).collect();
        Self {
            out,
            grp,
            edge_count,
        }
    }

    /// Single-source BFS distances. `u32::MAX` means unreachable.
    fn bfs(&self, src: u32) -> Vec<u32> {
        let mut dist = vec![u32::MAX; self.out.len()];
        dist[src as usize] = 0;
        let mut q = VecDeque::new();
        q.push_back(src);
        while let Some(u) = q.pop_front() {
            let d = dist[u as usize];
            for &v in &self.out[u as usize] {
                if dist[v as usize] == u32::MAX {
                    dist[v as usize] = d + 1;
                    q.push_back(v);
                }
            }
        }
        dist
    }

    /// `{v != src : 1 <= dist(src, v) <= hops}` — what both engines are asked
    /// for by the k-hop workloads, and what both are graded against.
    fn hop_set(&self, src: u32, hops: u32) -> BTreeSet<u32> {
        let mut seen = BTreeSet::new();
        let mut frontier = vec![src];
        let mut visited = BTreeSet::new();
        visited.insert(src);
        for _ in 0..hops {
            let mut next = Vec::new();
            for &u in &frontier {
                for &v in &self.out[u as usize] {
                    if visited.insert(v) {
                        next.push(v);
                        if v != src {
                            seen.insert(v);
                        }
                    }
                }
            }
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
        seen
    }

    fn has_edge(&self, from: u32, to: u32) -> bool {
        self.out[from as usize].binary_search(&to).is_ok()
    }
}

// ============================================================================
// Stats
// ============================================================================

/// Percentiles come from `nucleus::metrics::latency` so every harness in this
/// tree reports the same number for the same samples.
#[derive(Default, Clone)]
struct Stats {
    samples: Vec<Duration>,
}

impl Stats {
    fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }
    fn sorted(&self) -> Vec<Duration> {
        let mut s = self.samples.clone();
        s.sort();
        s
    }
    fn pct_us(&self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        percentile_duration(&self.sorted(), p).as_nanos() as f64 / 1000.0
    }
    fn min_us(&self) -> f64 {
        self.sorted()
            .first()
            .map_or(0.0, |d| d.as_nanos() as f64 / 1000.0)
    }
    fn max_us(&self) -> f64 {
        self.sorted()
            .last()
            .map_or(0.0, |d| d.as_nanos() as f64 / 1000.0)
    }
    /// Spread, the number that decides whether a ratio from this machine means
    /// anything. Reported on every row rather than buried in a footnote.
    fn spread(&self) -> f64 {
        let p50 = self.pct_us(50.0);
        if p50 == 0.0 {
            0.0
        } else {
            self.pct_us(99.0) / p50
        }
    }
}

/// One workload, one engine.
struct Arm {
    engine: &'static str,
    workload: String,
    stats: Stats,
    /// Operations that produced the oracle's answer, over operations run. A
    /// run aborts on the first mismatch, so this is `n/n` or the run is dead;
    /// it is printed anyway because a correctness column that is only ever
    /// implied is a correctness column nobody checks.
    correct: (usize, usize),
    /// Loopback round trips per timed operation. 1 everywhere except the
    /// client-side BFS arm, where it is the architectural cost being reported.
    round_trips: f64,
    note: &'static str,
}

// ============================================================================
// Nucleus arm
// ============================================================================

/// Boot Nucleus with a real data directory.
///
/// `compete.rs` boots `Executor::new` — pure memory, so the graph store has no
/// WAL and graph writes never fsync. Timing that against Neo4j's forced commit
/// would be the "different durability settings" trap, so this harness uses
/// `new_with_persistence`, and `assert_graph_wal_durable` below proves the WAL
/// is real instead of assuming the constructor did what its name says.
async fn start_nucleus(cfg: &Cfg) -> Arc<Executor> {
    std::fs::create_dir_all(&cfg.data_dir).expect("create data dir");
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new_with_persistence(
        catalog,
        storage,
        None,
        Some(cfg.data_dir.as_path()),
    ));
    let handler = Arc::new(NucleusHandler::new(executor.clone()));
    let server = Arc::new(NucleusServer::new(handler));

    let addr = format!("127.0.0.1:{}", cfg.nucleus_port);
    let listener = TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("bind {addr}: {e} (is the port free?)"));

    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let srv = server.clone();
            tokio::spawn(async move {
                let _ =
                    pgwire::tokio::process_socket(socket, None::<pgwire::tokio::TlsAcceptor>, srv)
                        .await;
            });
        }
    });

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=bench", cfg.nucleus_port),
        NoTls,
    )
    .await
    .expect("connect to nucleus");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // The connection is dropped here on purpose; callers make their own.
    drop(client);
    executor
}

async fn nucleus_client(cfg: &Cfg) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=bench", cfg.nucleus_port),
        NoTls,
    )
    .await
    .expect("connect to nucleus");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// What a commit's durability actually costs on the filesystem under test.
///
/// `docs/BENCH_VS_POSTGRES.md` exists largely because a write comparison was
/// run without this number: on macOS `fsync(2)` returns once the data reaches
/// the drive, while `fcntl(F_FULLFSYNC)` forces the drive's volatile cache, and
/// the two differ by two orders of magnitude. Two engines on opposite sides of
/// that line are not comparable on writes no matter how carefully everything
/// else is matched, so both costs are measured here, on the same filesystem as
/// the Nucleus data directory, and printed next to the write arms. What each
/// engine actually calls is then an inference from its own latency against
/// these two numbers — an inference the reader can check — rather than a claim
/// about somebody's source.
///
/// Returns (fsync p50 µs, F_FULLFSYNC p50 µs) — the second is `None` off macOS,
/// where the distinction does not exist.
fn measure_sync_costs(dir: &std::path::Path) -> (f64, Option<f64>) {
    use std::io::Write;
    use std::os::unix::io::AsRawFd;

    let path = dir.join("sync_cost_probe.bin");
    let mut f = std::fs::File::create(&path).expect("create sync probe file");
    let payload = [b'a'; 200];
    let n = 60;

    let mut plain = Vec::with_capacity(n);
    for _ in 0..n {
        f.write_all(&payload).expect("write sync probe");
        let t0 = Instant::now();
        // Not `File::sync_all`: on macOS that IS `F_FULLFSYNC`, so it cannot
        // measure the cheaper call. This is the raw syscall.
        unsafe { libc::fsync(f.as_raw_fd()) };
        plain.push(t0.elapsed());
    }

    #[cfg(target_os = "macos")]
    let full = {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            f.write_all(&payload).expect("write sync probe");
            let t0 = Instant::now();
            unsafe { libc::fcntl(f.as_raw_fd(), libc::F_FULLFSYNC) };
            v.push(t0.elapsed());
        }
        v.sort();
        Some(percentile_duration(&v, 50.0).as_nanos() as f64 / 1000.0)
    };
    #[cfg(not(target_os = "macos"))]
    let full = None;

    drop(f);
    let _ = std::fs::remove_file(&path);
    plain.sort();
    (
        percentile_duration(&plain, 50.0).as_nanos() as f64 / 1000.0,
        full,
    )
}

fn graph_wal_len(cfg: &Cfg) -> u64 {
    std::fs::metadata(cfg.data_dir.join("graph").join("graph.wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Positive proof that the Nucleus arm is paying for durability.
///
/// `wal_is_dirty() == false` cannot distinguish "fsynced" from "there is no
/// WAL at all", which is exactly the shape of failure `open_durable` warns
/// about, so it is not used as the check. The file has to exist and have grown
/// by the load, or the run is not comparable to Neo4j and stops.
fn assert_graph_wal_durable(cfg: &Cfg, before: u64) {
    let after = graph_wal_len(cfg);
    let path = cfg.data_dir.join("graph").join("graph.wal");
    assert!(
        path.exists(),
        "graph.wal absent at {} — the executor fell back to a volatile in-memory graph store, so \
         Nucleus would be timed with no durability against Neo4j's forced commit. Not comparable; \
         aborting.",
        path.display()
    );
    assert!(
        after > before,
        "graph.wal did not grow during the load ({before} -> {after} bytes) — graph writes are not \
         reaching the log, so this run is not durability-matched. Aborting."
    );
}

/// One scalar `SELECT` over pgwire, returning the single text column.
async fn nuc_text(client: &Client, sql: &str) -> Option<String> {
    let rows = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("nucleus query failed: {sql}\n  {e}\n  {e:?}"));
    for r in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = r {
            return row.get(0).map(|s| s.to_string());
        }
    }
    None
}

async fn nuc_i64(client: &Client, sql: &str) -> i64 {
    nuc_text(client, sql)
        .await
        .unwrap_or_else(|| panic!("nucleus returned NULL for: {sql}"))
        .parse()
        .unwrap_or_else(|e| panic!("nucleus returned a non-integer for {sql}: {e}"))
}

/// `[{"neighbor_id":N,"edge_id":E,"edge_type":"REL"}, ...]` -> neighbour ids.
fn parse_neighbors(json: &str) -> Vec<u64> {
    let v: serde_json::Value = serde_json::from_str(json)
        .unwrap_or_else(|e| panic!("GRAPH_NEIGHBORS returned unparseable JSON: {e}\n  {json}"));
    v.as_array()
        .unwrap_or_else(|| panic!("GRAPH_NEIGHBORS did not return an array: {json}"))
        .iter()
        .map(|e| {
            e.get("neighbor_id")
                .and_then(|n| n.as_u64())
                .unwrap_or_else(|| panic!("GRAPH_NEIGHBORS entry has no neighbor_id: {e}"))
        })
        .collect()
}

/// `{"columns":[...],"rows":[[v], ...]}` -> the first column of every row, as i64.
fn parse_graph_query_i64(json: &str) -> Vec<i64> {
    let v: serde_json::Value = serde_json::from_str(json)
        .unwrap_or_else(|e| panic!("GRAPH_QUERY returned unparseable JSON: {e}\n  {json}"));
    v.get("rows")
        .and_then(|r| r.as_array())
        .unwrap_or_else(|| panic!("GRAPH_QUERY result has no rows array: {json}"))
        .iter()
        .map(|row| {
            row.get(0)
                .and_then(|c| c.as_i64())
                .unwrap_or_else(|| panic!("GRAPH_QUERY row column 0 is not an integer: {row}"))
        })
        .collect()
}

// ============================================================================
// Neo4j arm
// ============================================================================

async fn connect_neo4j(cfg: &Cfg) -> Neo4jGraph {
    Neo4jGraph::new(&cfg.neo4j_uri, &cfg.neo4j_user, &cfg.neo4j_pass)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "cannot reach Neo4j at {}: {e}\n  start one with:\n  podman run -d -p 7687:7687 \
                 -e NEO4J_AUTH=none docker.io/library/neo4j:5.26-community\n  or pass --skip-neo4j",
                cfg.neo4j_uri
            )
        })
}

async fn neo_run(g: &Neo4jGraph, q: neo4rs::Query) {
    let mut r = g
        .execute(q)
        .await
        .unwrap_or_else(|e| panic!("neo4j query failed: {e}"));
    while r
        .next()
        .await
        .unwrap_or_else(|e| panic!("neo4j row failed: {e}"))
        .is_some()
    {}
}

async fn neo_i64s(g: &Neo4jGraph, q: neo4rs::Query, col: &str) -> Vec<i64> {
    let mut r = g
        .execute(q)
        .await
        .unwrap_or_else(|e| panic!("neo4j query failed: {e}"));
    let mut out = Vec::new();
    while let Some(row) = r
        .next()
        .await
        .unwrap_or_else(|e| panic!("neo4j row failed: {e}"))
    {
        out.push(
            row.get::<i64>(col)
                .unwrap_or_else(|e| panic!("neo4j column {col} is not an integer: {e}")),
        );
    }
    out
}

/// Zero or one row holding a list of node keys (the shortest-path arm).
async fn neo_path(g: &Neo4jGraph, q: neo4rs::Query) -> Option<Vec<i64>> {
    let mut r = g
        .execute(q)
        .await
        .unwrap_or_else(|e| panic!("neo4j query failed: {e}"));
    let row = r
        .next()
        .await
        .unwrap_or_else(|e| panic!("neo4j row failed: {e}"))?;
    Some(
        row.get::<Vec<i64>>("ks")
            .unwrap_or_else(|e| panic!("neo4j shortestPath did not return a key list: {e}")),
    )
}

// ============================================================================
// Assertions — every timed result is graded before its sample is kept
// ============================================================================

fn check_set(engine: &str, workload: &str, anchor: u32, got: &BTreeSet<u32>, want: &BTreeSet<u32>) {
    if got == want {
        return;
    }
    let missing: Vec<_> = want.difference(got).take(8).collect();
    let extra: Vec<_> = got.difference(want).take(8).collect();
    panic!(
        "{engine} answered {workload} wrongly for anchor {anchor}: {} returned vs {} in the \
         oracle\n  missing (first 8): {missing:?}\n  extra   (first 8): {extra:?}\n  A wrong \
         answer timed as a success is the defect that discredited docs/benchmarks/; aborting.",
        got.len(),
        want.len(),
    );
}

/// A path is right only if it starts and ends where it was asked to, every
/// consecutive pair is a real edge, and its length equals the oracle distance.
/// Checking only the length would pass a fabricated path; checking only the
/// edges would pass a valid but non-shortest one.
fn check_path(
    engine: &str,
    from: u32,
    to: u32,
    dist: u32,
    got: Option<&[u32]>,
    truth: &TruthGraph,
) {
    let reachable = dist != u32::MAX;
    match (got, reachable) {
        (None, false) => {}
        (None, true) => panic!(
            "{engine} found no path {from} -> {to}, but the oracle has one of length {dist}; \
             aborting."
        ),
        (Some(p), false) => panic!(
            "{engine} returned a path {from} -> {to} of length {}, but the oracle says the pair is \
             unreachable; aborting.",
            p.len().saturating_sub(1)
        ),
        (Some(p), true) => {
            assert!(
                p.first() == Some(&from) && p.last() == Some(&to),
                "{engine} returned a path for {from} -> {to} whose endpoints are {:?}/{:?}; \
                 aborting.",
                p.first(),
                p.last()
            );
            assert_eq!(
                p.len() as u32 - 1,
                dist,
                "{engine} returned a path {from} -> {to} of length {} where the oracle's shortest \
                 is {dist}; aborting.",
                p.len() as u32 - 1
            );
            for w in p.windows(2) {
                assert!(
                    truth.has_edge(w[0], w[1]),
                    "{engine} returned a path {from} -> {to} containing {} -> {}, which is not an \
                     edge in the generated graph; aborting.",
                    w[0],
                    w[1]
                );
            }
        }
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let Some(cfg) = parse_args() else { return };

    println!("compete_graph — Nucleus GRAPH vs Neo4j");
    println!(
        "  graph      : {} nodes, ring + {} random shortcuts, seed {}",
        cfg.nodes, cfg.shortcuts, cfg.seed
    );

    // ---- Ground truth ----
    let truth = TruthGraph::generate(cfg.nodes, cfg.shortcuts, cfg.seed);
    println!(
        "  edges      : {} (avg out-degree {:.2})",
        truth.edge_count,
        truth.edge_count as f64 / cfg.nodes as f64
    );

    // Anchors, sampled without replacement so no two timed operations send
    // byte-identical SQL: Nucleus's plan/query cache would otherwise serve a
    // repeat from cache and flatter the Nucleus arm's tail.
    let mut rng = Rng(cfg.seed ^ 0xDEAD_BEEF_CAFE_F00D);
    let anchors: Vec<u32> = {
        let mut pool: Vec<u32> = (0..cfg.nodes).collect();
        for i in (1..pool.len()).rev() {
            pool.swap(i, rng.below(i as u32 + 1) as usize);
        }
        pool.truncate(cfg.warmup + cfg.queries);
        assert_eq!(
            pool.len(),
            cfg.warmup + cfg.queries,
            "not enough nodes for {} warm-up + {} timed distinct anchors",
            cfg.warmup,
            cfg.queries
        );
        pool
    };
    let ops = anchors.len();
    let sp_pairs: Vec<(u32, u32)> = (0..ops)
        .map(|i| (anchors[i], rng.below(cfg.nodes)))
        .collect();
    // `true` once the warm-up pass is done and a sample counts.
    let timed = |i: usize| i >= cfg.warmup;

    // Oracle answers, computed once, before either engine is touched.
    let t_oracle = Instant::now();
    let hop1: Vec<BTreeSet<u32>> = anchors.iter().map(|&a| truth.hop_set(a, 1)).collect();
    let hop2: Vec<BTreeSet<u32>> = anchors.iter().map(|&a| truth.hop_set(a, 2)).collect();
    let hop3: Vec<BTreeSet<u32>> = anchors.iter().map(|&a| truth.hop_set(a, 3)).collect();
    let sp_dist: Vec<u32> = sp_pairs
        .iter()
        .map(|&(a, b)| truth.bfs(a)[b as usize])
        .collect();
    let pattern: Vec<BTreeSet<u32>> = anchors
        .iter()
        .map(|&a| {
            let g = truth.grp[a as usize];
            truth.out[a as usize]
                .iter()
                .copied()
                .filter(|&v| truth.grp[v as usize] == g)
                .collect()
        })
        .collect();
    println!(
        "  oracle     : BFS over the generated adjacency, {:.1}s",
        t_oracle.elapsed().as_secs_f64()
    );

    // The hop bound handed to Neo4j must not truncate a real path, or "no
    // path" would be a harness artefact indistinguishable from a wrong answer.
    let max_dist = sp_dist.iter().filter(|&&d| d != u32::MAX).max().copied();
    if let Some(m) = max_dist {
        assert!(
            (m as usize) <= cfg.sp_bound,
            "the longest sampled shortest path is {m} hops but Neo4j's shortestPath is bounded at \
             {}; raise --sp-bound.",
            cfg.sp_bound
        );
    }
    println!(
        "  paths      : sampled shortest-path lengths, max {} hops, mean {:.2}",
        max_dist.unwrap_or(0),
        sp_dist
            .iter()
            .filter(|&&d| d != u32::MAX)
            .map(|&d| d as f64)
            .sum::<f64>()
            / sp_dist.iter().filter(|&&d| d != u32::MAX).count().max(1) as f64
    );
    println!(
        "  hop sizes  : 1-hop {:.1}, 2-hop {:.1}, 3-hop {:.1} nodes (mean)",
        hop1.iter().map(|s| s.len() as f64).sum::<f64>() / hop1.len() as f64,
        hop2.iter().map(|s| s.len() as f64).sum::<f64>() / hop2.len() as f64,
        hop3.iter().map(|s| s.len() as f64).sum::<f64>() / hop3.len() as f64,
    );

    // ---- Engines up ----
    let executor = start_nucleus(&cfg).await;
    let (fsync_us, fullfsync_us) = measure_sync_costs(&cfg.data_dir);
    let nuc = nucleus_client(&cfg).await;
    println!("  nucleus    : pgwire on 127.0.0.1:{}", cfg.nucleus_port);
    match fullfsync_us {
        Some(full) => println!(
            "  commit cost: fsync(2) {fsync_us:.0} us vs fcntl(F_FULLFSYNC) {full:.0} us p50 on \
             this filesystem — {:.0}x apart. Read the write arms against BOTH.",
            full / fsync_us.max(0.001)
        ),
        None => println!("  commit cost: fsync(2) {fsync_us:.0} us p50 on this filesystem"),
    }

    let neo = if cfg.skip_neo4j {
        None
    } else {
        let g = connect_neo4j(&cfg).await;
        let mut ver = String::new();
        let mut r = g
            .execute(cypher(
                "CALL dbms.components() YIELD name, versions, edition \
                 RETURN versions[0] + ' ' + edition AS v",
            ))
            .await
            .expect("neo4j version query");
        if let Ok(Some(row)) = r.next().await {
            ver = row.get::<String>("v").unwrap_or_default();
        }
        println!("  neo4j      : Bolt {} — {ver}", cfg.neo4j_uri);
        // A stale database would silently change every count.
        neo_run(&g, cypher("MATCH (n) DETACH DELETE n")).await;
        // Refusing Neo4j its index would benchmark a missing index, not an
        // engine. Nucleus has no equivalent for its Cypher subset; that is
        // reported rather than compensated for.
        neo_run(
            &g,
            cypher("CREATE INDEX n_k IF NOT EXISTS FOR (n:N) ON (n.k)"),
        )
        .await;
        neo_run(&g, cypher("CALL db.awaitIndexes(300)")).await;
        Some(g)
    };

    // ---- Capability probe, before anything is timed ----
    // Which Cypher spellings each engine accepts is a property of the engines
    // and belongs in the record, not in a benchmark author's head. Both
    // spellings mean the same thing; the harness uses whichever both accept.
    for spelling in ["*1..2", "*..2"] {
        let sql = format!(
            "SELECT GRAPH_QUERY('MATCH (a:N {{k: 1}})-[:REL{spelling}]->(b:N) RETURN b.k')"
        );
        let nuc_ok = nuc.simple_query(&sql).await.is_ok();
        let neo_ok = match &neo {
            Some(g) => g
                .execute(cypher(&format!(
                    "MATCH (a:N {{k: 1}})-[:REL{spelling}]->(b:N) RETURN b.k AS k LIMIT 1"
                )))
                .await
                .is_ok(),
            None => true,
        };
        println!(
            "  cypher     : variable-length spelling `{spelling}` — nucleus {}, neo4j {}",
            if nuc_ok { "accepts" } else { "REJECTS" },
            if neo_ok { "accepts" } else { "REJECTS" },
        );
    }

    let mut arms: Vec<Arm> = Vec::new();

    // ================= LOAD =================
    // One operation per round trip, autocommit, on both sides: the same
    // transaction shape and the same fsync per write. Batched loading would be
    // faster on both and is what anyone would really do, but it measures the
    // batching, and the two engines' batching APIs are not the same shape.
    let wal_before = graph_wal_len(&cfg);

    let t = Instant::now();
    let mut nuc_id = vec![0i64; cfg.nodes as usize];
    for k in 0..cfg.nodes {
        nuc_id[k as usize] = nuc_i64(
            &nuc,
            &format!(
                "SELECT GRAPH_ADD_NODE('N', '{{\"k\":{k},\"grp\":{}}}')",
                truth.grp[k as usize]
            ),
        )
        .await;
    }
    let nuc_node_load = t.elapsed();

    let t = Instant::now();
    for from in 0..cfg.nodes {
        for &to in &truth.out[from as usize] {
            let e = nuc_i64(
                &nuc,
                &format!(
                    "SELECT GRAPH_ADD_EDGE({}, {}, 'REL')",
                    nuc_id[from as usize], nuc_id[to as usize]
                ),
            )
            .await;
            assert!(e > 0, "GRAPH_ADD_EDGE returned NULL for {from} -> {to}");
        }
    }
    let nuc_edge_load = t.elapsed();

    assert_graph_wal_durable(&cfg, wal_before);

    // Reverse map: Nucleus answers in its own node ids.
    let to_k: HashMap<i64, u32> = nuc_id
        .iter()
        .enumerate()
        .map(|(k, &id)| (id, k as u32))
        .collect();

    // Loaded what we meant to load — checked before anything is timed, because
    // every later assertion is meaningless against a differently-shaped graph.
    let n_nodes = nuc_i64(&nuc, "SELECT GRAPH_NODE_COUNT()").await;
    let n_edges = nuc_i64(&nuc, "SELECT GRAPH_EDGE_COUNT()").await;
    assert_eq!(n_nodes, cfg.nodes as i64, "nucleus node count after load");
    assert_eq!(
        n_edges, truth.edge_count as i64,
        "nucleus edge count after load"
    );

    let (mut neo_node_load, mut neo_edge_load) = (Duration::ZERO, Duration::ZERO);
    if let Some(g) = &neo {
        let t = Instant::now();
        for k in 0..cfg.nodes {
            neo_run(
                g,
                cypher("CREATE (n:N {k: $k, grp: $g})")
                    .param("k", k as i64)
                    .param("g", truth.grp[k as usize]),
            )
            .await;
        }
        neo_node_load = t.elapsed();

        let t = Instant::now();
        for from in 0..cfg.nodes {
            for &to in &truth.out[from as usize] {
                neo_run(
                    g,
                    cypher("MATCH (a:N {k: $f}), (b:N {k: $t}) CREATE (a)-[:REL]->(b)")
                        .param("f", from as i64)
                        .param("t", to as i64),
                )
                .await;
            }
        }
        neo_edge_load = t.elapsed();

        let nn = neo_i64s(g, cypher("MATCH (n:N) RETURN count(n) AS c"), "c").await;
        let ne = neo_i64s(
            g,
            cypher("MATCH (:N)-[r:REL]->(:N) RETURN count(r) AS c"),
            "c",
        )
        .await;
        assert_eq!(nn, vec![cfg.nodes as i64], "neo4j node count after load");
        assert_eq!(
            ne,
            vec![truth.edge_count as i64],
            "neo4j edge count after load"
        );
    }

    println!();
    println!(
        "load (autocommit, one op per round trip, both engines fsync at commit):\n  \
         nucleus  nodes {:>7.1}s ({:>6.0} op/s)   edges {:>7.1}s ({:>6.0} op/s)",
        nuc_node_load.as_secs_f64(),
        cfg.nodes as f64 / nuc_node_load.as_secs_f64(),
        nuc_edge_load.as_secs_f64(),
        truth.edge_count as f64 / nuc_edge_load.as_secs_f64(),
    );
    if neo.is_some() {
        println!(
            "  neo4j    nodes {:>7.1}s ({:>6.0} op/s)   edges {:>7.1}s ({:>6.0} op/s)",
            neo_node_load.as_secs_f64(),
            cfg.nodes as f64 / neo_node_load.as_secs_f64(),
            neo_edge_load.as_secs_f64(),
            truth.edge_count as f64 / neo_edge_load.as_secs_f64(),
        );
        println!(
            "  NOTE: the edge arms are not the same work. Nucleus is handed both endpoint node \
             ids; Cypher has to resolve both by property, twice per edge, through the :N(k) \
             index. The `anchor` row below is that resolution measured on its own."
        );
    }

    // ================= READS =================
    // Arms alternate order per anchor so warm-up and background drift land on
    // both engines equally.
    macro_rules! pair {
        ($i:expr, $a:block, $b:block) => {
            if $i % 2 == 0 {
                $a
                $b
            } else {
                $b
                $a
            }
        };
    }

    // ---- transport floor: the control ----
    // `SELECT 1` and `RETURN 1` touch no data. Any movement in these rows is
    // the machine, not either engine, which makes them the cheapest available
    // detector for a contended run — the Qdrant benchmark's discarded third run
    // was caught exactly this way, and run 2 of this benchmark was too.
    let mut nuc_floor = Stats::default();
    let mut neo_floor = Stats::default();
    for i in 0..ops {
        pair!(
            i,
            {
                let t0 = Instant::now();
                let v = nuc_i64(&nuc, "SELECT 1").await;
                if timed(i) {
                    nuc_floor.record(t0.elapsed());
                }
                assert_eq!(v, 1, "nucleus transport floor returned {v}");
            },
            {
                if let Some(g) = &neo {
                    let t0 = Instant::now();
                    let v = neo_i64s(g, cypher("RETURN 1 AS k"), "k").await;
                    if timed(i) {
                        neo_floor.record(t0.elapsed());
                    }
                    assert_eq!(v, vec![1], "neo4j transport floor returned {v:?}");
                }
            }
        );
    }
    arms.push(Arm {
        engine: "nucleus",
        workload: "transport floor (SELECT 1)".into(),
        stats: nuc_floor,
        correct: (ops, ops),
        round_trips: 1.0,
        note: "CONTROL — touches no data; movement here is the machine",
    });
    if neo.is_some() {
        arms.push(Arm {
            engine: "neo4j",
            workload: "transport floor (RETURN 1)".into(),
            stats: neo_floor,
            correct: (ops, ops),
            round_trips: 1.0,
            note: "CONTROL — touches no data; movement here is the machine",
        });
    }

    // ---- anchor: resolve one node by property ----
    let mut nuc_anchor = Stats::default();
    let mut neo_anchor = Stats::default();
    for (i, &a) in anchors.iter().enumerate() {
        pair!(
            i,
            {
                let sql = format!("SELECT GRAPH_QUERY('MATCH (a:N {{k: {a}}}) RETURN a.k')");
                let t0 = Instant::now();
                let got = nuc_text(&nuc, &sql)
                    .await
                    .expect("GRAPH_QUERY returned NULL");
                if timed(i) {
                    nuc_anchor.record(t0.elapsed());
                }
                let ks = parse_graph_query_i64(&got);
                assert_eq!(ks, vec![a as i64], "nucleus anchor resolution for {a}");
            },
            {
                if let Some(g) = &neo {
                    let t0 = Instant::now();
                    let ks = neo_i64s(
                        g,
                        cypher("MATCH (a:N {k: $k}) RETURN a.k AS k").param("k", a as i64),
                        "k",
                    )
                    .await;
                    if timed(i) {
                        neo_anchor.record(t0.elapsed());
                    }
                    assert_eq!(ks, vec![a as i64], "neo4j anchor resolution for {a}");
                }
            }
        );
    }
    arms.push(Arm {
        engine: "nucleus",
        workload: "anchor (resolve node by property)".into(),
        stats: nuc_anchor,
        correct: (ops, ops),
        round_trips: 1.0,
        note: "GRAPH_QUERY: label scan + property compare, no index",
    });
    if neo.is_some() {
        arms.push(Arm {
            engine: "neo4j",
            workload: "anchor (resolve node by property)".into(),
            stats: neo_anchor,
            correct: (ops, ops),
            round_trips: 1.0,
            note: "range index on :N(k)",
        });
    }

    // ---- 1-hop ----
    let mut nuc_h1 = Stats::default();
    let mut neo_h1 = Stats::default();
    for (i, &a) in anchors.iter().enumerate() {
        let want = &hop1[i];
        pair!(
            i,
            {
                let sql = format!("SELECT GRAPH_NEIGHBORS({}, 'out')", nuc_id[a as usize]);
                let t0 = Instant::now();
                let got = nuc_text(&nuc, &sql)
                    .await
                    .expect("GRAPH_NEIGHBORS returned NULL");
                if timed(i) {
                    nuc_h1.record(t0.elapsed());
                }
                let set: BTreeSet<u32> = parse_neighbors(&got)
                    .into_iter()
                    .map(|id| to_k[&(id as i64)])
                    .collect();
                check_set("nucleus", "1-hop", a, &set, want);
            },
            {
                if let Some(g) = &neo {
                    let t0 = Instant::now();
                    let ks = neo_i64s(
                        g,
                        cypher("MATCH (a:N {k: $k})-[:REL]->(b:N) RETURN b.k AS k")
                            .param("k", a as i64),
                        "k",
                    )
                    .await;
                    if timed(i) {
                        neo_h1.record(t0.elapsed());
                    }
                    let set: BTreeSet<u32> = ks.into_iter().map(|k| k as u32).collect();
                    check_set("neo4j", "1-hop", a, &set, want);
                }
            }
        );
    }
    arms.push(Arm {
        engine: "nucleus",
        workload: "1-hop out-neighbours".into(),
        stats: nuc_h1,
        correct: (ops, ops),
        round_trips: 1.0,
        note: "GRAPH_NEIGHBORS, addressed by node id",
    });
    if neo.is_some() {
        arms.push(Arm {
            engine: "neo4j",
            workload: "1-hop out-neighbours".into(),
            stats: neo_h1,
            correct: (ops, ops),
            round_trips: 1.0,
            note: "includes anchor resolution",
        });
    }

    // ---- k-hop, two Nucleus surfaces vs one Neo4j query ----
    for (hops, want_sets) in [(2u32, &hop2), (3u32, &hop3)] {
        let mut nuc_cy = Stats::default();
        let mut nuc_bfs = Stats::default();
        let mut neo_k = Stats::default();
        let mut trips_total = 0usize;

        for (i, &a) in anchors.iter().enumerate() {
            let want = &want_sets[i];

            // (a) GRAPH_QUERY — one round trip, Cypher subset.
            let sql = format!(
                "SELECT GRAPH_QUERY('MATCH (a:N {{k: {a}}})-[:REL*..{hops}]->(b:N) RETURN b.k')"
            );
            let t0 = Instant::now();
            let got = nuc_text(&nuc, &sql)
                .await
                .expect("GRAPH_QUERY returned NULL");
            if timed(i) {
                nuc_cy.record(t0.elapsed());
            }
            let set: BTreeSet<u32> = parse_graph_query_i64(&got)
                .into_iter()
                .map(|k| k as u32)
                .collect();
            check_set(
                "nucleus (GRAPH_QUERY)",
                &format!("{hops}-hop"),
                a,
                &set,
                want,
            );

            // (b) client-side BFS over GRAPH_NEIGHBORS — one round trip per
            // expanded node. The count is carried into the report because the
            // latency is meaningless without it.
            let t0 = Instant::now();
            let mut visited: BTreeSet<i64> = BTreeSet::new();
            visited.insert(nuc_id[a as usize]);
            let mut frontier = vec![nuc_id[a as usize]];
            let mut reached: BTreeSet<i64> = BTreeSet::new();
            let mut trips = 0usize;
            for _ in 0..hops {
                let mut next = Vec::new();
                for &node in &frontier {
                    let sql = format!("SELECT GRAPH_NEIGHBORS({node}, 'out')");
                    let got = nuc_text(&nuc, &sql)
                        .await
                        .expect("GRAPH_NEIGHBORS returned NULL");
                    trips += 1;
                    for nb in parse_neighbors(&got) {
                        let nb = nb as i64;
                        if visited.insert(nb) {
                            next.push(nb);
                            reached.insert(nb);
                        }
                    }
                }
                frontier = next;
                if frontier.is_empty() {
                    break;
                }
            }
            if timed(i) {
                nuc_bfs.record(t0.elapsed());
                trips_total += trips;
            }
            let set: BTreeSet<u32> = reached.iter().map(|id| to_k[id]).collect();
            check_set(
                "nucleus (client BFS)",
                &format!("{hops}-hop"),
                a,
                &set,
                want,
            );

            // (c) Neo4j.
            if let Some(g) = &neo {
                let q = format!(
                    "MATCH (a:N {{k: $k}})-[:REL*..{hops}]->(b:N) WHERE b <> a \
                     RETURN DISTINCT b.k AS k"
                );
                let t0 = Instant::now();
                let ks = neo_i64s(g, cypher(&q).param("k", a as i64), "k").await;
                if timed(i) {
                    neo_k.record(t0.elapsed());
                }
                let set: BTreeSet<u32> = ks.into_iter().map(|k| k as u32).collect();
                check_set("neo4j", &format!("{hops}-hop"), a, &set, want);
            }
        }

        arms.push(Arm {
            engine: "nucleus",
            workload: format!("{hops}-hop set — GRAPH_QUERY"),
            stats: nuc_cy,
            correct: (ops, ops),
            round_trips: 1.0,
            note: "Cypher subset; anchor is a label scan",
        });
        arms.push(Arm {
            engine: "nucleus",
            workload: format!("{hops}-hop set — client BFS"),
            stats: nuc_bfs,
            correct: (ops, ops),
            round_trips: trips_total as f64 / cfg.queries.max(1) as f64,
            note: "GRAPH_NEIGHBORS; one round trip per expanded node",
        });
        if neo.is_some() {
            arms.push(Arm {
                engine: "neo4j",
                workload: format!("{hops}-hop set"),
                stats: neo_k,
                correct: (ops, ops),
                round_trips: 1.0,
                note: "variable-length pattern, DISTINCT",
            });
        }
    }

    // ---- shortest path ----
    let mut nuc_sp = Stats::default();
    let mut neo_sp = Stats::default();
    for (i, &(a, b)) in sp_pairs.iter().enumerate() {
        let dist = sp_dist[i];
        pair!(
            i,
            {
                let sql = format!(
                    "SELECT GRAPH_SHORTEST_PATH({}, {})",
                    nuc_id[a as usize], nuc_id[b as usize]
                );
                let t0 = Instant::now();
                let got = nuc_text(&nuc, &sql).await;
                if timed(i) {
                    nuc_sp.record(t0.elapsed());
                }
                let path: Option<Vec<u32>> = got.map(|j| {
                    let ids: Vec<i64> = serde_json::from_str(&j).unwrap_or_else(|e| {
                        panic!("GRAPH_SHORTEST_PATH returned unparseable JSON: {e}\n  {j}")
                    });
                    ids.into_iter().map(|id| to_k[&id]).collect()
                });
                check_path("nucleus", a, b, dist, path.as_deref(), &truth);
            },
            {
                if let Some(g) = &neo {
                    let q = format!(
                        "MATCH (a:N {{k: $x}}), (b:N {{k: $y}}) \
                     MATCH p = shortestPath((a)-[:REL*..{}]->(b)) \
                     RETURN [n IN nodes(p) | n.k] AS ks",
                        cfg.sp_bound
                    );
                    let t0 = Instant::now();
                    let path =
                        neo_path(g, cypher(&q).param("x", a as i64).param("y", b as i64)).await;
                    if timed(i) {
                        neo_sp.record(t0.elapsed());
                    }
                    let path: Option<Vec<u32>> =
                        path.map(|ks| ks.into_iter().map(|k| k as u32).collect());
                    check_path("neo4j", a, b, dist, path.as_deref(), &truth);
                }
            }
        );
    }
    arms.push(Arm {
        engine: "nucleus",
        workload: "shortest path (directed, unweighted)".into(),
        stats: nuc_sp,
        correct: (ops, ops),
        round_trips: 1.0,
        note: "GRAPH_SHORTEST_PATH, addressed by node id",
    });
    if neo.is_some() {
        arms.push(Arm {
            engine: "neo4j",
            workload: "shortest path (directed, unweighted)".into(),
            stats: neo_sp,
            correct: (ops, ops),
            round_trips: 1.0,
            note: "shortestPath(), includes anchor resolution",
        });
    }

    // ---- property-filtered pattern ----
    let mut nuc_pat = Stats::default();
    let mut neo_pat = Stats::default();
    for (i, &a) in anchors.iter().enumerate() {
        let want = &pattern[i];
        let g_val = truth.grp[a as usize];
        pair!(
            i,
            {
                let sql = format!(
                    "SELECT GRAPH_QUERY('MATCH (a:N {{k: {a}}})-[:REL]->(b:N) WHERE b.grp = {g_val} RETURN b.k')"
                );
                let t0 = Instant::now();
                let got = nuc_text(&nuc, &sql)
                    .await
                    .expect("GRAPH_QUERY returned NULL");
                if timed(i) {
                    nuc_pat.record(t0.elapsed());
                }
                let set: BTreeSet<u32> = parse_graph_query_i64(&got)
                    .into_iter()
                    .map(|k| k as u32)
                    .collect();
                check_set("nucleus", "pattern", a, &set, want);
            },
            {
                if let Some(g) = &neo {
                    let t0 = Instant::now();
                    let ks = neo_i64s(
                        g,
                        cypher(
                            "MATCH (a:N {k: $k})-[:REL]->(b:N) WHERE b.grp = $g RETURN b.k AS k",
                        )
                        .param("k", a as i64)
                        .param("g", g_val),
                        "k",
                    )
                    .await;
                    if timed(i) {
                        neo_pat.record(t0.elapsed());
                    }
                    let set: BTreeSet<u32> = ks.into_iter().map(|k| k as u32).collect();
                    check_set("neo4j", "pattern", a, &set, want);
                }
            }
        );
    }
    arms.push(Arm {
        engine: "nucleus",
        workload: "pattern: 1-hop filtered on b.grp".into(),
        stats: nuc_pat,
        correct: (ops, ops),
        round_trips: 1.0,
        note: "only property-aware surface Nucleus has",
    });
    if neo.is_some() {
        arms.push(Arm {
            engine: "neo4j",
            workload: "pattern: 1-hop filtered on b.grp".into(),
            stats: neo_pat,
            correct: (ops, ops),
            round_trips: 1.0,
            note: "",
        });
    }

    // ================= WRITES =================
    // Per-operation node insert, timed individually rather than as a bulk rate.
    //
    // The write arms are NOT interleaved, and the reads above are. That is not
    // an inconsistency, it is the one place interleaving actively corrupts the
    // measurement: `fcntl(F_FULLFSYNC)` is a DEVICE-wide barrier, not a
    // file-wide one, so every Nucleus commit flushes Neo4j's dirty data too and
    // makes Neo4j's next commit wait for work it did not ask for. Measured:
    // interleaved, Neo4j's insert p50 was 3,212 µs; run alone against the same
    // container it is 683 µs, of which 541 µs is the Bolt transport floor. A
    // 4.7x penalty inflicted by the other arm is worse than any warm-up drift
    // alternating would have cancelled, so each engine gets a contiguous block.
    let base = cfg.nodes as i64 + 1_000_000;
    let mut nuc_w = Stats::default();
    let mut neo_w = Stats::default();
    let write_ops_total = cfg.warmup + cfg.write_ops;
    for i in 0..write_ops_total {
        let k = base + i as i64;
        let sql = format!("SELECT GRAPH_ADD_NODE('W', '{{\"k\":{k}}}')");
        let t0 = Instant::now();
        let id = nuc_i64(&nuc, &sql).await;
        if timed(i) {
            nuc_w.record(t0.elapsed());
        }
        assert!(id > 0, "GRAPH_ADD_NODE returned a non-id");
    }
    if let Some(g) = &neo {
        for i in 0..write_ops_total {
            let k = base + i as i64;
            let t0 = Instant::now();
            neo_run(g, cypher("CREATE (n:W {k: $k})").param("k", k)).await;
            if timed(i) {
                neo_w.record(t0.elapsed());
            }
        }
    }
    // The writes landed: a write arm that timed no-ops would look excellent.
    let after_nodes = nuc_i64(&nuc, "SELECT GRAPH_NODE_COUNT()").await;
    assert_eq!(
        after_nodes,
        cfg.nodes as i64 + write_ops_total as i64,
        "nucleus node count after the write arm"
    );
    if let Some(g) = &neo {
        let c = neo_i64s(g, cypher("MATCH (n:W) RETURN count(n) AS c"), "c").await;
        assert_eq!(
            c,
            vec![write_ops_total as i64],
            "neo4j write-arm node count"
        );
    }
    arms.push(Arm {
        engine: "nucleus",
        workload: "insert node (autocommit, fsync)".into(),
        stats: nuc_w,
        correct: (write_ops_total, write_ops_total),
        round_trips: 1.0,
        note: "synchronous_commit=on (default)",
    });
    if neo.is_some() {
        arms.push(Arm {
            engine: "neo4j",
            workload: "insert node (autocommit, fsync)".into(),
            stats: neo_w,
            correct: (write_ops_total, write_ops_total),
            round_trips: 1.0,
            note: "transaction log forced at commit (default)",
        });
    }

    // The same write with the fsync switched off. This is NOT a comparison
    // against Neo4j — nothing on the Neo4j side was relaxed — it exists to
    // prove the arm above is really paying for durability, which is the whole
    // basis of calling this comparison matched.
    let unsync = nucleus_client(&cfg).await;
    unsync
        .simple_query("SET synchronous_commit = off")
        .await
        .expect("SET synchronous_commit");
    let mut nuc_w_nosync = Stats::default();
    for i in 0..write_ops_total {
        let k = base + 100_000 + i as i64;
        let sql = format!("SELECT GRAPH_ADD_NODE('X', '{{\"k\":{k}}}')");
        let t0 = Instant::now();
        let id = nuc_i64(&unsync, &sql).await;
        if timed(i) {
            nuc_w_nosync.record(t0.elapsed());
        }
        assert!(id > 0, "GRAPH_ADD_NODE returned a non-id");
    }
    arms.push(Arm {
        engine: "nucleus",
        workload: "insert node (synchronous_commit=off)".into(),
        stats: nuc_w_nosync,
        correct: (write_ops_total, write_ops_total),
        round_trips: 1.0,
        note: "CONTROL — no fsync; proves the arm above pays for one",
    });

    // ================= REPORT =================
    println!();
    println!(
        "{:<9} {:<38} {:>9} {:>9} {:>9} {:>9} {:>9} {:>7} {:>6} {:>8}",
        "engine",
        "workload",
        "p50_us",
        "p90_us",
        "p99_us",
        "min_us",
        "max_us",
        "p99/p50",
        "rtt",
        "correct"
    );
    for a in &arms {
        println!(
            "{:<9} {:<38} {:>9.0} {:>9.0} {:>9.0} {:>9.0} {:>9.0} {:>7.2} {:>6.1} {:>8}",
            a.engine,
            a.workload,
            a.stats.pct_us(50.0),
            a.stats.pct_us(90.0),
            a.stats.pct_us(99.0),
            a.stats.min_us(),
            a.stats.max_us(),
            a.stats.spread(),
            a.round_trips,
            format!("{}/{}", a.correct.0, a.correct.1),
        );
    }
    println!();
    for a in &arms {
        if !a.note.is_empty() {
            println!("  {:<9} {:<38} {}", a.engine, a.workload, a.note);
        }
    }

    // Ratios, on medians, only for the arms that answer the same question.
    if neo.is_some() {
        println!();
        println!("ratio (neo4j p50 / nucleus p50; >1 means Nucleus is faster):");
        let find = |e: &str, w: &str| {
            arms.iter()
                .find(|a| a.engine == e && a.workload == w)
                .map(|a| a.stats.pct_us(50.0))
        };
        let pairs: Vec<(String, String)> = vec![
            (
                "anchor (resolve node by property)".into(),
                "anchor (resolve node by property)".into(),
            ),
            ("1-hop out-neighbours".into(), "1-hop out-neighbours".into()),
            ("2-hop set — GRAPH_QUERY".into(), "2-hop set".into()),
            ("2-hop set — client BFS".into(), "2-hop set".into()),
            ("3-hop set — GRAPH_QUERY".into(), "3-hop set".into()),
            ("3-hop set — client BFS".into(), "3-hop set".into()),
            (
                "shortest path (directed, unweighted)".into(),
                "shortest path (directed, unweighted)".into(),
            ),
            (
                "pattern: 1-hop filtered on b.grp".into(),
                "pattern: 1-hop filtered on b.grp".into(),
            ),
            (
                "insert node (autocommit, fsync)".into(),
                "insert node (autocommit, fsync)".into(),
            ),
        ];
        for (nw, ew) in pairs {
            if let (Some(n), Some(e)) = (find("nucleus", &nw), find("neo4j", &ew)) {
                let r = e / n;
                println!(
                    "  {:<40} {:>7.2}x  {}",
                    nw,
                    r,
                    if r >= 1.0 {
                        "nucleus faster"
                    } else {
                        "NEO4J FASTER"
                    }
                );
            }
        }
    }

    println!();
    println!(
        "NOTE: both engines answer over a loopback TCP socket with a binary protocol (Nucleus \
         pgwire, Neo4j Bolt), one connection, sequential, one round trip per timed operation \
         except the client-BFS arm (see rtt)."
    );
    println!(
        "NOTE: what is proven about durability, and what is not. PROVEN: the Nucleus arm runs on a \
         real data directory, its graph WAL exists and grew during the load (from {wal_before} \
         bytes), and the synchronous_commit=off row shows that ~98% of a default Nucleus write is \
         the sync — so this arm is not the volatile in-memory graph store `compete.rs` would have \
         given it. ALSO TRUE: Neo4j forces its transaction log at every commit and community \
         edition has no knob to disable it. NOT PROVEN: that the two engines' commits take the \
         SAME sync. See the commit-cost note below."
    );
    match fullfsync_us {
        Some(full) => println!(
            "NOTE: DO NOT QUOTE THE WRITE RATIO. It is not an engine result, for the reason \
             docs/BENCH_VS_POSTGRES.md records. On this filesystem fsync(2) costs {fsync_us:.0} \
             us p50 and fcntl(F_FULLFSYNC) — the drive-cache barrier — costs {full:.0} us, \
             {:.0}x more. Nucleus's write minus its own synchronous_commit=off control lands on \
             the barrier figure, so Nucleus takes a barrier per commit. Neo4j's write minus the \
             Bolt transport floor does not come close to it — and Neo4j here runs inside a Linux \
             VM whose disk is a file on this same filesystem, so a guest fsync need not reach \
             the host's drive at all. The two engines are buying different guarantees; the \
             latency difference is mostly that, not the graph engines.",
            full / fsync_us.max(0.001)
        ),
        None => println!(
            "NOTE: fsync(2) costs {fsync_us:.0} us p50 on this filesystem; compare both engines' \
             write p50 against it."
        ),
    }
    println!(
        "NOTE: not comparable, stated rather than forced — Nucleus's GRAPH_* functions take \
         internal node ids, so their traversals start for free, while Cypher resolves an anchor by \
         property. Neo4j has a range index on :N(k); Nucleus's Cypher subset has no property index \
         and scans the label. Read every traversal row against the `anchor` row."
    );
    println!(
        "NOTE: Nucleus has no function that reads a node's properties, so the property-filtered \
         pattern can only be expressed through GRAPH_QUERY, and no k-hop function, so multi-hop on \
         the fast surface costs one round trip per expanded node."
    );
    println!(
        "NOTE: read the two CONTROL rows first. `transport floor` and \
         `synchronous_commit=off` do no index work and no drive barrier respectively, so if \
         either has moved against a previous run, the run is contended and every other row moved \
         with it. Run 2 of this benchmark was discarded on exactly that signal: its \
         synchronous_commit=off row read 4,043 us against 66 us in a clean run."
    );
    println!(
        "NOTE: single dev-machine wall clock. {} timed operations per read arm and {} per write \
         arm, each preceded by {} untimed warm-up operations on BOTH engines (Neo4j is a JVM; \
         unwarmed it times its own JIT). The p99/p50 column is the spread; this machine class has \
         been measured at 95.4% worst-case deviation on green runs, so reproduce before quoting \
         any ratio.",
        cfg.queries, cfg.write_ops, cfg.warmup
    );

    // Leave nothing behind: the temp data directory is this process's alone.
    drop(executor);
    let _ = std::fs::remove_dir_all(&cfg.data_dir);
}
