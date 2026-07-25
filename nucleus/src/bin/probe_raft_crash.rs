//! Deterministic crash coverage for Raft's persistent state.
//!
//! An in-process "crash" (drop the node, reopen the directory) cannot prove
//! much: the page cache survives, `Drop` runs, buffered writers flush. This
//! harness spawns a **separate OS process** that performs one Raft operation and
//! dies at a *named* durability boundary via `NUCLEUS_CRASHPOINT`, which calls
//! `std::process::abort()` — no unwinding, no `Drop`, no buffer flush. The
//! parent then reopens the same directory and asserts what a restarted node is
//! entitled to see.
//!
//! # The invariants under test
//!
//! **A. No double vote.** If the child *granted* a vote (printed `GRANTED`), the
//! restarted node must still refuse a different candidate in that term. A node
//! that forgets its vote lets one term elect two leaders, and two leaders accept
//! conflicting writes at the same log index — committed entries get overwritten.
//! This is the invariant that makes the whole exercise worth doing.
//!
//! **B. No phantom acknowledgement.** If the child answered `success: true` to
//! AppendEntries (printed `ACKED n`), the restarted node must still hold those n
//! entries. The leader may have counted that ack toward the quorum that
//! committed them and already told a client the write was durable.
//!
//! **C. Never worse than silence.** If the child died *before* responding, it
//! promised nothing, so either outcome is acceptable — but the state must still
//! load cleanly and must never be self-contradictory (a term that went
//! backwards, a torn record decoded as real, a commit index above the log).
//!
//! # What this does and does not prove
//!
//! It proves the state survives process death without relying on `Drop`,
//! unwinding, or a flushed `BufWriter`. It does **not** prove survival of power
//! loss: the OS page cache outlives an aborted process, so a missing `fsync`
//! would still pass here. The `fsync` calls are present at each boundary and the
//! `raft.before_log_fsync` / `raft.after_log_fsync` points bracket them, but
//! only real power loss or a fault-injecting filesystem can demonstrate that
//! difference.
//!
//! Build/run:
//! ```sh
//! cargo run --features server --bin probe_raft_crash
//! ```
#![cfg(feature = "server")]

use std::path::{Path, PathBuf};
use std::process::Command as OsCommand;

use nucleus::raft::{AppendEntriesRequest, Command, LogEntry, RaftNode, RequestVoteRequest};
use nucleus::storage::crashpoint::ALL_RAFT_POINTS;

/// Boundaries at which the child is killed, split by the operation that reaches
/// them. Derived from the engine's own declaration so a renamed or added point
/// cannot silently drop out of coverage.
fn vote_points() -> Vec<&'static str> {
    ALL_RAFT_POINTS
        .iter()
        .copied()
        .filter(|p| p.contains("hardstate"))
        .collect()
}

fn append_points() -> Vec<&'static str> {
    ALL_RAFT_POINTS
        .iter()
        .copied()
        .filter(|p| p.contains("log"))
        .collect()
}

fn tmp_root() -> PathBuf {
    std::env::temp_dir().join(format!("nucleus_raft_crash_{}", std::process::id()))
}

// ── Child modes ──────────────────────────────────────────────────────────────

/// Grant a vote to candidate 2 in term 5 and *report it*, then start a second
/// hard-state write that the crash interrupts.
///
/// The two-step shape is deliberate. If the child died during the very first
/// write it would never print `GRANTED`, nothing would have been promised, and
/// the interesting assertion would silently never run — a vacuous pass. Running
/// the crashpoint with `SKIP=1` lets the first vote complete and be
/// acknowledged, and puts the crash inside a *later* write. That is the real
/// shape of the danger: durable state from an answered RPC being lost while a
/// subsequent write is in flight.
fn child_vote(dir: &Path) -> ! {
    let mut node = RaftNode::open(1, vec![2, 3], dir).expect("child: open raft state");

    let first = node.handle_request_vote(&RequestVoteRequest {
        term: 5,
        candidate_id: 2,
        last_log_index: 0,
        last_log_term: 0,
    });
    if first.vote_granted {
        println!("GRANTED");
    } else {
        println!("REFUSED");
    }
    // Force stdout out before the abort: an aborted process does not flush.
    use std::io::Write;
    let _ = std::io::stdout().flush();

    // Second write — this is where the armed crashpoint (SKIP=1) fires.
    let _ = node.handle_request_vote(&RequestVoteRequest {
        term: 6,
        candidate_id: 3,
        last_log_index: 0,
        last_log_term: 0,
    });
    println!("SURVIVED");
    let _ = std::io::stdout().flush();
    std::process::exit(0);
}

/// Acknowledge three entries, report it, then start a second append that the
/// crash interrupts. Same reasoning as `child_vote`.
fn child_append(dir: &Path) -> ! {
    let mut node = RaftNode::open(2, vec![1, 3], dir).expect("child: open raft state");
    let entries: Vec<LogEntry> = (1..=3)
        .map(|i| LogEntry {
            index: i,
            term: 3,
            command: Command::Sql(format!("INSERT INTO t VALUES ({i})")),
        })
        .collect();
    let resp = node.handle_append_entries(&AppendEntriesRequest {
        term: 3,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries,
        leader_commit: 3,
    });
    if resp.success {
        println!("ACKED {}", resp.match_index);
    } else {
        println!("REFUSED");
    }
    use std::io::Write;
    let _ = std::io::stdout().flush();

    // Second batch — the armed crashpoint (SKIP=1) fires inside this one.
    let more: Vec<LogEntry> = (4..=6)
        .map(|i| LogEntry {
            index: i,
            term: 3,
            command: Command::Sql(format!("INSERT INTO t VALUES ({i})")),
        })
        .collect();
    let _ = node.handle_append_entries(&AppendEntriesRequest {
        term: 3,
        leader_id: 1,
        prev_log_index: 3,
        prev_log_term: 3,
        entries: more,
        leader_commit: 6,
    });
    println!("SURVIVED");
    let _ = std::io::stdout().flush();
    std::process::exit(0);
}

// ── Parent ───────────────────────────────────────────────────────────────────

struct Outcome {
    responded: Option<String>,
    killed: bool,
}

fn run_child(mode: &str, dir: &Path, crashpoint: &str) -> Outcome {
    let exe = std::env::current_exe().expect("current exe");
    let output = OsCommand::new(exe)
        .arg("--child")
        .arg(mode)
        .arg(dir)
        .env("NUCLEUS_CRASHPOINT", crashpoint)
        // Let the first operation complete and be acknowledged; crash inside
        // the second. Without this the child would die before answering
        // anything and the binding-promise assertions would never run.
        .env("NUCLEUS_CRASHPOINT_SKIP", "1")
        .output()
        .expect("spawn child");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let responded = stdout
        .lines()
        .find(|l| l.starts_with("GRANTED") || l.starts_with("ACKED"))
        .map(|s| s.to_string());
    Outcome {
        responded,
        killed: !output.status.success() && !stdout.contains("SURVIVED"),
    }
}

/// Invariant A: a granted vote is binding across a crash.
fn check_vote(point: &str, failures: &mut Vec<String>) {
    let dir = tmp_root().join(format!("vote_{}", point.replace('.', "_")));
    let _ = std::fs::remove_dir_all(&dir);

    let outcome = run_child("vote", &dir, point);

    let mut restarted = match RaftNode::open(1, vec![2, 3], &dir) {
        Ok(n) => n,
        Err(e) => {
            failures.push(format!("[{point}] restart could not load Raft state: {e}"));
            return;
        }
    };

    // Never self-contradictory, whatever happened.
    if restarted.commit_index > restarted.last_log_index() {
        failures.push(format!(
            "[{point}] restart shows commit_index {} above last log index {}",
            restarted.commit_index,
            restarted.last_log_index()
        ));
    }

    if outcome.responded.as_deref() != Some("GRANTED") {
        // If the promise branch never runs, the interesting assertion never
        // runs, and a green result would mean nothing. Treat it as a failure of
        // the harness rather than a pass.
        failures.push(format!(
            "[{point}] child never reported GRANTED, so the double-vote invariant was \
             NOT exercised at this window"
        ));
        return;
    }

    // The child told candidate 2 it had the term-5 vote. Whatever the crash did
    // to the in-flight second write, that promise is binding: candidate 3 must
    // not be able to collect a term-5 vote from this node.
    if restarted.current_term < 5 {
        failures.push(format!(
            "[{point}] term went BACKWARDS across the crash: granted in term 5, \
             restarted at term {}",
            restarted.current_term
        ));
    }
    if restarted.current_term == 5 && restarted.voted_for != Some(2) {
        failures.push(format!(
            "[{point}] still in term 5 but the granted vote came back as {:?}",
            restarted.voted_for
        ));
    }

    let second = restarted.handle_request_vote(&RequestVoteRequest {
        term: 5,
        candidate_id: 3,
        last_log_index: 0,
        last_log_term: 0,
    });
    if second.vote_granted {
        failures.push(format!(
            "[{point}] DOUBLE VOTE: node granted term 5 to candidate 2, crashed, and \
             after restart granted term 5 to candidate 3 as well — term 5 can now \
             elect two leaders and committed entries can be overwritten"
        ));
    }

    if !outcome.killed {
        failures.push(format!(
            "[{point}] child exited normally — the crashpoint was never reached, so \
             this window is UNTESTED"
        ));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Invariant B: an acknowledged append is binding across a crash.
fn check_append(point: &str, failures: &mut Vec<String>) {
    let dir = tmp_root().join(format!("append_{}", point.replace('.', "_")));
    let _ = std::fs::remove_dir_all(&dir);

    let outcome = run_child("append", &dir, point);

    let restarted = match RaftNode::open(2, vec![1, 3], &dir) {
        Ok(n) => n,
        Err(e) => {
            failures.push(format!("[{point}] restart could not load Raft state: {e}"));
            return;
        }
    };

    let Some(acked) = outcome
        .responded
        .as_deref()
        .and_then(|r| r.strip_prefix("ACKED "))
        .and_then(|n| n.trim().parse::<u64>().ok())
    else {
        failures.push(format!(
            "[{point}] child never reported ACKED, so the phantom-ack invariant was \
             NOT exercised at this window"
        ));
        return;
    };
    {
        if restarted.last_log_index() < acked {
            failures.push(format!(
                "[{point}] PHANTOM ACK: follower acknowledged up to index {acked}, crashed, \
                 and came back with only {} — the leader may already have reported those \
                 writes committed to a client",
                restarted.last_log_index()
            ));
        }
        for i in 1..=acked {
            match restarted.log_at(i).map(|e| &e.command) {
                Some(Command::Sql(sql)) if sql == &format!("INSERT INTO t VALUES ({i})") => {}
                other => failures.push(format!(
                    "[{point}] entry {i} came back as {other:?}, not the acknowledged command"
                )),
            }
        }
    }

    // A torn tail must never decode into a real entry, whatever happened.
    for i in 1..=restarted.last_log_index() {
        match restarted.log_at(i).map(|e| &e.command) {
            Some(Command::Sql(sql)) if sql.starts_with("INSERT INTO t VALUES (") => {}
            Some(Command::Noop) => {}
            other => failures.push(format!(
                "[{point}] recovered a corrupt entry at index {i}: {other:?}"
            )),
        }
    }

    if !outcome.killed {
        failures.push(format!(
            "[{point}] child exited normally — the crashpoint was never reached, so \
             this window is UNTESTED"
        ));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A node whose disk refuses writes must refuse to vote, not answer from memory.
fn check_io_fault_refusal(failures: &mut Vec<String>) {
    let dir = tmp_root().join("iofault");
    let _ = std::fs::remove_dir_all(&dir);
    let exe = std::env::current_exe().expect("current exe");
    let output = OsCommand::new(exe)
        .arg("--child")
        .arg("vote")
        .arg(&dir)
        .env("NUCLEUS_IOFAULT", "raft.hardstate_write")
        .env("NUCLEUS_IOFAULT_KIND", "full")
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&output.stdout);

    if stdout.contains("GRANTED") {
        failures.push(
            "[io-fault] node granted a vote although the hard-state write failed — it \
             answered from memory for state that is not on disk"
                .to_string(),
        );
    } else if !stdout.contains("REFUSED") {
        failures.push(format!(
            "[io-fault] child neither granted nor refused; output was {stdout:?}"
        ));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 4 && args[1] == "--child" {
        let dir = PathBuf::from(&args[3]);
        match args[2].as_str() {
            "vote" => child_vote(&dir),
            "append" => child_append(&dir),
            other => panic!("unknown child mode {other}"),
        }
    }

    println!("probe_raft_crash: deterministic crash coverage for Raft persistent state");
    let mut failures = Vec::new();

    let (votes, appends) = (vote_points(), append_points());
    assert_eq!(
        votes.len() + appends.len(),
        ALL_RAFT_POINTS.len(),
        "a declared Raft crashpoint is not covered by any operation in this harness"
    );
    for point in &votes {
        println!("  vote   @ {point}");
        check_vote(point, &mut failures);
    }
    for point in &appends {
        println!("  append @ {point}");
        check_append(point, &mut failures);
    }
    println!("  io-fault @ raft.hardstate_write");
    check_io_fault_refusal(&mut failures);

    let _ = std::fs::remove_dir_all(tmp_root());

    if failures.is_empty() {
        println!(
            "\nOK: {} crash windows + 1 I/O-fault path, no double vote, no phantom ack",
            ALL_RAFT_POINTS.len()
        );
    } else {
        println!("\n{} FAILURES:", failures.len());
        for f in &failures {
            println!("  - {f}");
        }
        std::process::exit(1);
    }
}
