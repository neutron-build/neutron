//! Raft consensus invariant simulator (Tier 2 — distributed/replication).
//!
//! The real distributed stack (`src/distributed/replicator.rs`) is glued to a
//! TCP transport (`TcpTransport`) and an async executor apply-channel, which
//! cannot be exercised in-process without standing up real sockets and an
//! executor per node. BUT the consensus core it drives — `nucleus::raft::RaftNode`
//! — is a pure, deterministic, I/O-free state machine. This harness builds an
//! in-process cluster of N `RaftNode`s and a deterministic message-passing
//! simulator that injects faults (leader-timeout elections, asymmetric network
//! partitions, node pauses, message reordering/loss) and then asserts the four
//! core Raft safety properties after every step:
//!
//!   1. ELECTION SAFETY  — at most one leader per term across the whole cluster.
//!   2. LOG MATCHING     — if two logs hold an entry with the same (index,term),
//!                         then all preceding entries are identical.
//!   3. STATE-MACHINE / COMMIT DURABILITY — once an index is committed on ANY
//!                         node with a given (term,command), no node ever holds a
//!                         DIFFERENT (term,command) at that index, now or after any
//!                         later leadership change. (A committed entry is never
//!                         lost, overwritten, or reordered.)
//!   4. LEADER COMPLETENESS — every committed entry is present (identical) in the
//!                         log of any node that is currently leader.
//!
//! There is NO external oracle: these are the Raft paper's own safety theorems,
//! which the implementation claims to provide (and which `lean4/` and `quint/`
//! purport to prove). A violation is a real consensus bug. The simulator is fully
//! deterministic given --seed, so any finding replays exactly.
//!
//! Build: `cargo build --release --features "server rusqlite" --bin probe_distributed`
//! Run  : `cargo run   --release --features server --bin probe_distributed -- --seed 1`
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)] // internal fuzz harness

use std::collections::HashMap;

use nucleus::raft::{
    AppendEntriesRequest, AppendEntriesResponse, Command, NodeId, RaftNode, RequestVoteRequest,
    RequestVoteResponse, Role,
};

// ─── Deterministic PRNG ───────────────────────────────────────────────────────
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
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
}

// ─── Canonicalize a Command for value-equality (Command has no PartialEq) ──────
fn cmd_canon(c: &Command) -> String {
    match c {
        Command::Sql(s) => format!("S:{s}"),
        Command::Noop => "N".into(),
        Command::AddNode(id) => format!("+{id}"),
        Command::RemoveNode(id) => format!("-{id}"),
    }
}

// ─── In-flight RPCs (point-to-point, deterministic) ────────────────────────────
enum Rpc {
    Vote {
        to: NodeId,
        from: NodeId,
        req: RequestVoteRequest,
    },
    VoteResp {
        to: NodeId,
        from: NodeId,
        resp: RequestVoteResponse,
    },
    Append {
        to: NodeId,
        from: NodeId,
        req: AppendEntriesRequest,
    },
    AppendResp {
        to: NodeId,
        from: NodeId,
        resp: AppendEntriesResponse,
    },
}
impl Rpc {
    fn to(&self) -> NodeId {
        match self {
            Rpc::Vote { to, .. }
            | Rpc::VoteResp { to, .. }
            | Rpc::Append { to, .. }
            | Rpc::AppendResp { to, .. } => *to,
        }
    }
    fn from(&self) -> NodeId {
        match self {
            Rpc::Vote { from, .. }
            | Rpc::VoteResp { from, .. }
            | Rpc::Append { from, .. }
            | Rpc::AppendResp { from, .. } => *from,
        }
    }
}

struct Cluster {
    nodes: HashMap<NodeId, RaftNode>,
    ids: Vec<NodeId>,
    inflight: Vec<Rpc>,
    /// Per-index record of the FIRST committed (term, command-canon) we ever
    /// observed on any node. The durability invariant: this must never change.
    committed_seen: HashMap<u64, (u64, String)>,
}

/// Dump full per-node state — used by --trace to judge whether a flagged entry
/// was genuinely committed (real bug) or recorded prematurely (harness bug).
fn dump_cluster(c: &Cluster, tag: &str) {
    println!("=== STATE DUMP ({tag}) ===");
    for &id in &c.ids {
        let n = &c.nodes[&id];
        let logs: Vec<String> = n
            .log
            .iter()
            .map(|e| format!("[{}:t{} {}]", e.index, e.term, cmd_canon(&e.command)))
            .collect();
        println!(
            "  node {id}: role={:?} term={} commit={} applied={} leader={:?}",
            n.role, n.current_term, n.commit_index, n.last_applied, n.leader_id
        );
        println!("    log: {}", logs.join(" "));
        if n.role == Role::Leader {
            println!(
                "    match_index={:?} next_index={:?}",
                n.match_index, n.next_index
            );
        }
    }
    let mut led: Vec<_> = c.committed_seen.iter().collect();
    led.sort_by_key(|(k, _)| **k);
    println!("  committed_seen ledger: {led:?}");
}

impl Cluster {
    fn new(n: usize) -> Self {
        let ids: Vec<NodeId> = (1..=n as u64).collect();
        let mut nodes = HashMap::new();
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            nodes.insert(id, RaftNode::new(id, peers));
        }
        Cluster {
            nodes,
            ids,
            inflight: Vec::new(),
            committed_seen: HashMap::new(),
        }
    }

    /// Trigger an election from `cand`: it becomes candidate and emits RequestVotes.
    fn start_election(&mut self, cand: NodeId) {
        let reqs = self.nodes.get_mut(&cand).unwrap().start_election();
        for (peer, req) in reqs {
            self.inflight.push(Rpc::Vote {
                to: peer,
                from: cand,
                req,
            });
        }
    }

    /// Leader heartbeat / replication: emit AppendEntries to all peers.
    fn replicate(&mut self, leader: NodeId) {
        let node = self.nodes.get(&leader).unwrap();
        if node.role != Role::Leader {
            return;
        }
        let appends = node.build_append_entries();
        for (peer, req) in appends {
            self.inflight.push(Rpc::Append {
                to: leader_dummy(peer),
                from: leader,
                req,
            });
        }
    }

    /// Leader client write.
    fn client_write(&mut self, leader: NodeId, cmd: Command) {
        let node = self.nodes.get_mut(&leader).unwrap();
        if node.role == Role::Leader {
            node.append_entry(cmd);
        }
    }

    /// Deliver one RPC. `partition(a,b)` returns true if the link a<->b is up.
    fn deliver(&mut self, rpc: Rpc) {
        let (to, from) = (rpc.to(), rpc.from());
        let _ = (to, from);
        match rpc {
            Rpc::Vote { to, from, req } => {
                let resp = self.nodes.get_mut(&to).unwrap().handle_request_vote(&req);
                self.inflight.push(Rpc::VoteResp {
                    to: from,
                    from: to,
                    resp,
                });
            }
            Rpc::VoteResp { to, from, resp } => {
                let became = self
                    .nodes
                    .get_mut(&to)
                    .unwrap()
                    .handle_vote_response(from, &resp);
                if became {
                    // New leader replicates immediately (establishes leadership noop).
                    self.replicate(to);
                }
            }
            Rpc::Append { to, from, req } => {
                let resp = self.nodes.get_mut(&to).unwrap().handle_append_entries(&req);
                self.inflight.push(Rpc::AppendResp {
                    to: from,
                    from: to,
                    resp,
                });
            }
            Rpc::AppendResp { to, from, resp } => {
                self.nodes
                    .get_mut(&to)
                    .unwrap()
                    .handle_append_response(from, &resp);
            }
        }
    }

    fn leaders(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self
            .ids
            .iter()
            .copied()
            .filter(|id| self.nodes[id].role == Role::Leader)
            .collect();
        v.sort();
        v
    }
}

// `to` for an Append is the peer; helper keeps intent obvious.
fn leader_dummy(peer: NodeId) -> NodeId {
    peer
}

// ─── Invariant checks ─────────────────────────────────────────────────────────

/// Election Safety: at most one leader per term.
fn check_one_leader_per_term(c: &Cluster) -> Option<String> {
    let mut by_term: HashMap<u64, Vec<NodeId>> = HashMap::new();
    for &id in &c.ids {
        let n = &c.nodes[&id];
        if n.role == Role::Leader {
            by_term.entry(n.current_term).or_default().push(id);
        }
    }
    for (term, leaders) in &by_term {
        if leaders.len() > 1 {
            return Some(format!(
                "SPLIT-BRAIN: term {term} has {} leaders: {:?}",
                leaders.len(),
                leaders
            ));
        }
    }
    None
}

/// Log Matching: for any two nodes, if they share (index,term) at some index,
/// every preceding entry must be identical (term + command).
fn check_log_matching(c: &Cluster) -> Option<String> {
    let nodes: Vec<&RaftNode> = c.ids.iter().map(|id| &c.nodes[id]).collect();
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            let (a, b) = (nodes[i], nodes[j]);
            let maxlen = a.log.len().min(b.log.len());
            // Find the highest common index where (index,term) agree; below it
            // all entries must match. We just verify the whole common prefix that
            // shares terms: classic check is "if same index & term, prefixes equal".
            for idx in 1..maxlen {
                let ea = &a.log[idx];
                let eb = &b.log[idx];
                if ea.term == eb.term {
                    // Same (index, term) ⇒ same command, AND identical prefix.
                    if cmd_canon(&ea.command) != cmd_canon(&eb.command) {
                        return Some(format!(
                            "LOG-MATCH VIOLATION: nodes {} & {} at index {idx} term {} differ: {:?} vs {:?}",
                            a.id,
                            b.id,
                            ea.term,
                            cmd_canon(&ea.command),
                            cmd_canon(&eb.command)
                        ));
                    }
                    for k in 1..idx {
                        let ka = &a.log[k];
                        let kb = &b.log[k];
                        if ka.term != kb.term || cmd_canon(&ka.command) != cmd_canon(&kb.command) {
                            return Some(format!(
                                "LOG-MATCH VIOLATION: nodes {} & {} agree at idx {idx}(term {}) but differ at prefix idx {k}: ({},{:?}) vs ({},{:?})",
                                a.id,
                                b.id,
                                ea.term,
                                ka.term,
                                cmd_canon(&ka.command),
                                kb.term,
                                cmd_canon(&kb.command)
                            ));
                        }
                    }
                }
            }
        }
    }
    None
}

/// Commit Durability + State-Machine Safety: record each committed entry; if a
/// previously-committed index ever shows a DIFFERENT (term,command) on any node,
/// a committed entry was lost/overwritten/reordered.
fn check_commit_durability(c: &mut Cluster) -> Option<String> {
    // First, record/verify against every node's committed prefix.
    let mut violation = None;
    let mut to_record: Vec<(u64, (u64, String))> = Vec::new();
    for &id in &c.ids {
        let n = &c.nodes[&id];
        let upto = n.commit_index.min(n.last_log_index());
        for idx in 1..=upto {
            let e = match n.log_at(idx) {
                Some(e) => e,
                None => continue,
            };
            let val = (e.term, cmd_canon(&e.command));
            match c.committed_seen.get(&idx) {
                Some(prev) => {
                    if *prev != val {
                        violation = Some(format!(
                            "COMMIT DURABILITY VIOLATION at index {idx}: previously committed as (term {}, {}), node {} now holds committed (term {}, {})",
                            prev.0, prev.1, id, val.0, val.1
                        ));
                    }
                }
                None => to_record.push((idx, val)),
            }
        }
        if violation.is_some() {
            break;
        }
    }
    for (idx, val) in to_record {
        c.committed_seen.entry(idx).or_insert(val);
    }
    violation
}

/// Leader Completeness: a committed entry (per our recorded ledger) must be
/// present, identical, in the log of every LEGITIMATE current leader.
///
/// IMPORTANT: a *stale* leader — one partitioned in an old term while the rest
/// of the cluster moved on — is permitted by Raft to lag and even be missing
/// committed entries; it simply cannot commit anything because its AppendEntries
/// are rejected by the higher-term majority. The Leader Completeness Property
/// constrains the leader that could actually act (the one at the maximum term).
/// So we only check leaders whose term is not superseded by any node.
fn check_leader_completeness(c: &Cluster) -> Option<String> {
    let max_term = c
        .ids
        .iter()
        .map(|id| c.nodes[id].current_term)
        .max()
        .unwrap_or(0);
    for &id in &c.ids {
        let n = &c.nodes[&id];
        if n.role != Role::Leader {
            continue;
        }
        // Skip stale leaders superseded by a higher term elsewhere in the cluster.
        if n.current_term < max_term {
            continue;
        }
        for (&idx, (term, cmd)) in &c.committed_seen {
            match n.log_at(idx) {
                None => {
                    return Some(format!(
                        "LEADER COMPLETENESS VIOLATION: leader {} (term {}) is missing committed index {idx} (term {term}, {cmd})",
                        id, n.current_term
                    ));
                }
                Some(e) => {
                    if e.term != *term || &cmd_canon(&e.command) != cmd {
                        return Some(format!(
                            "LEADER COMPLETENESS VIOLATION: leader {} at committed index {idx} holds (term {}, {}) but committed value is (term {term}, {cmd})",
                            id,
                            e.term,
                            cmd_canon(&e.command)
                        ));
                    }
                }
            }
        }
    }
    None
}

fn check_all(c: &mut Cluster) -> Option<String> {
    if let Some(v) = check_one_leader_per_term(c) {
        return Some(v);
    }
    if let Some(v) = check_log_matching(c) {
        return Some(v);
    }
    if let Some(v) = check_commit_durability(c) {
        return Some(v);
    }
    if let Some(v) = check_leader_completeness(c) {
        return Some(v);
    }
    None
}

// ─── One simulated cluster run ─────────────────────────────────────────────────
struct RunResult {
    violation: Option<String>,
    steps_done: usize,
}

fn run_once(seed: u64, iter: usize, n: usize, steps: usize) -> RunResult {
    run_once_inner(seed, iter, n, steps, false)
}

fn run_once_inner(seed: u64, iter: usize, n: usize, steps: usize, trace: bool) -> RunResult {
    let mut rng = Rng(seed
        .wrapping_add(iter as u64)
        .wrapping_mul(0x100000001B3)
        .wrapping_add(1));
    let mut c = Cluster::new(n);
    // Partition state: a partition splits nodes into two groups; links across
    // the boundary drop. `part` maps node→group(0/1); None means no partition.
    let mut part: Option<HashMap<NodeId, u8>> = None;
    // Paused nodes: deliveries to/from them are dropped (simulates crash/hang).
    let mut paused: Vec<NodeId> = Vec::new();
    let mut next_key: u64 = 1;

    let link_up =
        |part: &Option<HashMap<NodeId, u8>>, paused: &[NodeId], a: NodeId, b: NodeId| -> bool {
            if paused.contains(&a) || paused.contains(&b) {
                return false;
            }
            match part {
                None => true,
                Some(m) => m.get(&a) == m.get(&b),
            }
        };

    for step in 0..steps {
        // ── Inject faults ──
        // Election: a random non-paused node times out and runs for office.
        if rng.chance(22) {
            let cand = c.ids[rng.below(c.ids.len())];
            if !paused.contains(&cand) {
                c.start_election(cand);
            }
        }
        // Leader heartbeat / replication burst.
        if rng.chance(60) {
            let leaders = c.leaders();
            for l in leaders {
                if !paused.contains(&l) {
                    c.replicate(l);
                }
            }
        }
        // Client write to a current leader.
        if rng.chance(45) {
            let leaders = c.leaders();
            if !leaders.is_empty() {
                let l = leaders[rng.below(leaders.len())];
                if !paused.contains(&l) {
                    let k = next_key;
                    next_key += 1;
                    c.client_write(l, Command::Sql(format!("INSERT INTO t VALUES ({k})")));
                }
            }
        }
        // Toggle a network partition.
        if rng.chance(10) {
            if part.is_some() && rng.chance(50) {
                part = None; // heal
            } else {
                // Random split into two groups (each side may be a minority).
                let mut m = HashMap::new();
                for &id in &c.ids {
                    m.insert(id, (rng.next() & 1) as u8);
                }
                part = Some(m);
            }
        }
        // Pause / resume a node (crash-stop then restart — log preserved, which
        // models a durable-log restart; the strongest test of commit durability).
        if rng.chance(8) {
            let v = c.ids[rng.below(c.ids.len())];
            if let Some(pos) = paused.iter().position(|&x| x == v) {
                paused.remove(pos);
            } else if paused.len() < n.saturating_sub(1) {
                // Never pause everyone — keep at least one alive.
                paused.push(v);
            }
        }

        // ── Deliver a batch of in-flight RPCs (with reordering & loss) ──
        let deliveries = 1 + rng.below(6);
        for _ in 0..deliveries {
            if c.inflight.is_empty() {
                break;
            }
            let pick = rng.below(c.inflight.len());
            let rpc = c.inflight.swap_remove(pick);
            let (to, from) = (rpc.to(), rpc.from());
            // Drop if the link is down (partition) or either endpoint paused, or
            // randomly (lossy network).
            if !link_up(&part, &paused, to, from) || rng.chance(6) {
                continue; // message lost
            }
            c.deliver(rpc);
        }

        // Occasionally let the in-flight queue grow unbounded → bound it by
        // dropping the oldest (models buffer overflow / very lossy net).
        if c.inflight.len() > 400 {
            c.inflight.drain(0..200);
        }

        // ── Check invariants ──
        if let Some(v) = check_all(&mut c) {
            if trace {
                println!("\n>>> FLAG at step {step}: {v}");
                dump_cluster(&c, "at flag");
            }
            return RunResult {
                violation: Some(v),
                steps_done: step + 1,
            };
        }
    }

    // Final convergence pass: heal everything, resume all, and flush the message
    // queue + periodic replication so commits propagate; invariants must still
    // hold (and committed entries must survive the chaos).
    part = None;
    paused.clear();
    for _ in 0..(n * 60) {
        // Make sure exactly the highest-term leader keeps replicating.
        let leaders = c.leaders();
        for l in leaders {
            c.replicate(l);
        }
        let mut delivered = false;
        while !c.inflight.is_empty() {
            let rpc = c.inflight.remove(0);
            c.deliver(rpc);
            delivered = true;
            if let Some(v) = check_all(&mut c) {
                return RunResult {
                    violation: Some(v),
                    steps_done: steps,
                };
            }
        }
        if !delivered { /* nothing pending; replicate again next round */ }
        if let Some(v) = check_all(&mut c) {
            return RunResult {
                violation: Some(v),
                steps_done: steps,
            };
        }
    }

    RunResult {
        violation: None,
        steps_done: steps,
    }
}

fn main_impl() {
    let mut seed: u64 = 0xD157_B00B;
    let mut iterations = 4000usize;
    let mut steps = 120usize;
    let mut max_report = 12usize;
    let mut nodes_opt: Option<usize> = None;
    let mut trace_iter: Option<usize> = None;
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
            "--steps" => {
                i += 1;
                steps = args[i].parse().unwrap();
            }
            "--nodes" => {
                i += 1;
                nodes_opt = Some(args[i].parse().unwrap());
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            "--trace" => {
                i += 1;
                trace_iter = Some(args[i].parse().unwrap());
            }
            _ => {}
        }
        i += 1;
    }

    // Trace mode: replay one specific run with full state dumps on the first flag.
    if let Some(ti) = trace_iter {
        let n = nodes_opt.unwrap_or(3);
        println!("TRACE seed={seed} iter={ti} nodes={n} steps={steps}");
        let r = run_once_inner(seed, ti, n, steps, true);
        match r.violation {
            Some(v) => println!("\nflag: {v} (step {})", r.steps_done),
            None => println!("no flag in this run"),
        }
        return;
    }

    std::panic::set_hook(Box::new(|_| {}));
    println!(
        "Nucleus Raft consensus invariant simulator\nseed={seed} iterations={iterations} steps/iter={steps}\n\
         invariants: election-safety · log-matching · commit-durability · leader-completeness\n"
    );

    // Cluster sizes 3 and 5 (odd, standard Raft). If --nodes given, only that.
    let sizes: Vec<usize> = match nodes_opt {
        Some(n) => vec![n],
        None => vec![3, 5],
    };
    let mut total_runs = 0usize;
    let mut violations = 0usize;

    for &n in &sizes {
        println!("── cluster size {n} ──");
        for iter in 0..iterations {
            total_runs += 1;
            let res = std::panic::catch_unwind(|| run_once(seed, iter, n, steps));
            match res {
                Ok(r) => {
                    if let Some(v) = r.violation {
                        violations += 1;
                        if violations <= max_report {
                            println!(
                                "─── VIOLATION #{violations} (n={n}, iter {iter}, seed {seed}, step {}) ───\n  {v}\n  replay: --seed {seed} --nodes {n} --iterations {} (offset {iter})\n",
                                r.steps_done,
                                iter + 1
                            );
                        }
                    }
                }
                Err(_) => {
                    violations += 1;
                    if violations <= max_report {
                        println!("─── PANIC #{violations} (n={n}, iter {iter}, seed {seed}) ───\n");
                    }
                }
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("cluster runs : {total_runs}");
    println!("violations   : {violations}");
    if violations == 0 {
        println!("\nAll Raft safety invariants held across every fault schedule.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
