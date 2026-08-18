//! Session authority and guard-state probe (NU-217 / NU-218 class).
//!
//! Two sections, both about the same defect family: **a permissive default on
//! a failure path**. A lock-contention error was interpreted as "not in a
//! transaction"; an unknown session id fell back to the bootstrap superuser —
//! and the calls that install authority WROTE to that fallback.
//!
//! **Section `authority` (NU-218).** Reads may fall back to the default
//! session — that is a documented single-user convenience. Authority must
//! not: binding an authenticated principal or a tenant claim to an id that
//! names no session must fail, and the process-wide fallback identity must
//! come out byte-identical. The original defect stamped an attacker-supplied
//! principal onto the identity every later fallback runs as.
//!
//! **Section `guards` (NU-217).** While a session is inside an explicit
//! transaction, `session_in_transaction` must answer `true` on EVERY
//! observation, under concurrent churn on the same executor — and a
//! non-superuser session with a globally enabled RLS policy must be reported
//! by `session_has_active_rls` on every observation while its own transaction
//! state is being flipped. Both guards gate the wire layer's autocommit fast
//! paths; answering the unsafe direction under contention is how a guarded
//! path silently reopens.
//!
//! `--negative-control <authority|guards>` runs both sections twice at one
//! seed — clean, then with that section's model perturbed the way the
//! original bug perturbed the outcome (the unknown-id bind expected to
//! succeed; one guard observation expected to answer the unsafe direction).
//! It passes only if the perturbation adds divergences to that section and
//! none to the other. A check nobody has watched fail is not a check.
//!
//! Build: `cargo run --release --features server --bin probe_sessions`
//!        `... --bin probe_sessions -- --negative-control authority`
//!        `... --bin probe_sessions -- --negative-control guards`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::collections::BTreeMap;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};

// ─── Divergence bookkeeping ──────────────────────────────────────────────────

#[derive(Default)]
struct Sections {
    counts: BTreeMap<&'static str, usize>,
    findings: Vec<(&'static str, String)>,
}

impl Sections {
    fn push(&mut self, section: &'static str, detail: String) {
        *self.counts.entry(section).or_insert(0) += 1;
        if self.findings.len() < 40 {
            self.findings.push((section, detail));
        }
    }
    fn count(&self, section: &str) -> usize {
        self.counts.get(section).copied().unwrap_or(0)
    }
    fn total(&self) -> usize {
        self.counts.values().sum()
    }
}

fn make_executor() -> Executor {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    Executor::new(catalog, storage)
}

async fn exec(ex: &Executor, session: u64, sql: &str) -> Result<String, String> {
    let mut results = ex
        .execute_with_session(session, sql)
        .await
        .map_err(|e| format!("{e:?}"))?;
    match results.pop() {
        Some(nucleus::executor::ExecResult::Select { rows, .. }) => Ok(rows
            .first()
            .and_then(|r| r.first())
            .map(|v| v.to_string())
            .unwrap_or_default()),
        Some(nucleus::executor::ExecResult::Command { rows_affected, .. }) => {
            Ok(rows_affected.to_string())
        }
        other => Err(format!("unexpected result shape: {other:?}")),
    }
}

/// `SELECT CURRENT_USER` on a session — the identity the session runs as.
async fn current_user(ex: &Executor, session: u64) -> String {
    exec(ex, session, "SELECT CURRENT_USER")
        .await
        .unwrap_or_else(|e| format!("<error {e}>"))
}

// ═════════════════════════════════════════════════════════════════════════════
// Section 1 — authority never falls back (NU-218)
// ═════════════════════════════════════════════════════════════════════════════

async fn section_authority(perturb: bool, sec: &mut Sections) {
    let ex = make_executor();

    // Seed: two login-capable roles and an RLS-protected table, so the
    // fallback identity has something to be compromised against.
    for sql in [
        "CREATE TABLE docs (id INT, owner TEXT)",
        "INSERT INTO docs VALUES (1, 'alice'), (2, 'mallory'), (3, 'other')",
        "CREATE ROLE alice LOGIN PASSWORD 'a'",
        "CREATE ROLE mallory LOGIN PASSWORD 'm'",
        "CREATE POLICY own ON docs FOR SELECT USING (owner = CURRENT_USER)",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
    ] {
        // Policy DDL through the default (superuser) session.
        if let Err(e) = exec(&ex, 0, sql).await {
            sec.push("authority", format!("seed failed ({sql}): {e}"));
            return;
        }
    }

    let default_before = current_user(&ex, 0).await;

    // A legitimate bind works and is scoped to its session.
    let alice_sid = ex.create_session();
    let bind = ex.bind_authenticated_session(alice_sid, "alice").await;
    if bind.is_err() {
        sec.push(
            "authority",
            format!("legitimate bind of alice failed: {:?}", bind.err()),
        );
    }
    let alice_user = current_user(&ex, alice_sid).await;
    if alice_user != "alice" {
        sec.push(
            "authority",
            format!("alice's session runs as {alice_user:?} after a successful bind"),
        );
    }

    // Attack A: bind a principal to an id that names no session. The old code
    // resolved the id to the default session and wrote the principal onto it.
    let unknown_id = 999_999u64;
    let attack_a = ex.bind_authenticated_session(unknown_id, "mallory").await;
    if perturb {
        // Model of the bug: the bind succeeds. The fixed engine refuses,
        // which is the divergence the control needs.
        if attack_a.is_err() {
            sec.push(
                "authority",
                "unknown-id bind of mallory REFUSED (perturbed model expects the NU-218 \
                 bug: the bind succeeding against the fallback session)"
                    .to_string(),
            );
        }
    } else if attack_a.is_ok() {
        sec.push(
            "authority",
            "bind_authenticated_session(999_999, 'mallory') SUCCEEDED — the principal was \
             installed on the fallback session every unknown id resolves to (NU-218)"
                .to_string(),
        );
    }

    // Attack B: a real session id that has been dropped (stale id).
    let stale_sid = ex.create_session();
    ex.drop_session(stale_sid);
    let attack_b = ex.bind_authenticated_session(stale_sid, "mallory").await;
    if !perturb && attack_b.is_ok() {
        sec.push(
            "authority",
            format!("bind to a DROPPED session id ({stale_sid}) succeeded (NU-218)"),
        );
    }
    if perturb && attack_b.is_err() {
        sec.push(
            "authority",
            "stale-id bind REFUSED (perturbed model expects the bug)".to_string(),
        );
    }

    // Attack C: tenant claims are authority too.
    let attack_c = ex.bind_trusted_tenant(unknown_id, Some("tenant-mallory".into()));
    if !perturb && attack_c.is_ok() {
        sec.push(
            "authority",
            "bind_trusted_tenant on an unknown session id succeeded (NU-218)".to_string(),
        );
    }

    // The fallback identity must be untouched by any of the attacks.
    let default_after = current_user(&ex, 0).await;
    if !perturb && default_after != default_before {
        sec.push(
            "authority",
            format!(
                "the default session identity changed from {default_before:?} to \
                 {default_after:?} — every later unknown-id fallback now runs as the \
                 attacker's principal (NU-218)"
            ),
        );
    }
    if perturb && default_after == default_before {
        sec.push(
            "authority",
            "default session identity UNCHANGED (perturbed model expects the bug)".to_string(),
        );
    }

    // And alice's own session is unaffected by the attacks.
    let alice_after = current_user(&ex, alice_sid).await;
    if !perturb && alice_after != alice_user {
        sec.push(
            "authority",
            format!(
                "alice's session identity drifted from {alice_user:?} to {alice_after:?} \
                 during attacks on other ids"
            ),
        );
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Section 2 — guard state never answers the unsafe direction (NU-217)
// ═════════════════════════════════════════════════════════════════════════════

async fn section_guards(perturb: bool, sec: &mut Sections) {
    let ex = Arc::new(make_executor());

    for sql in [
        "CREATE TABLE g (id INT, owner TEXT)",
        "CREATE ROLE bob LOGIN PASSWORD 'b'",
        "CREATE POLICY gpol ON g FOR SELECT USING (owner = CURRENT_USER)",
        "ALTER TABLE g ENABLE ROW LEVEL SECURITY",
    ] {
        if let Err(e) = exec(&ex, 0, sql).await {
            sec.push("guards", format!("seed failed ({sql}): {e}"));
            return;
        }
    }

    // s_txn sits inside an open transaction for the whole storm: the ground
    // truth of `session_in_transaction(s_txn)` is invariantly TRUE.
    let s_txn = ex.create_session();
    if let Err(e) = exec(&ex, s_txn, "BEGIN").await {
        sec.push("guards", format!("BEGIN on s_txn failed: {e}"));
        return;
    }
    if let Err(e) = exec(&ex, s_txn, "INSERT INTO g VALUES (1, 'bob')").await {
        sec.push("guards", format!("staging INSERT on s_txn failed: {e}"));
        return;
    }

    // bob is a non-superuser and a policy is globally enabled, so the ground
    // truth of `session_has_active_rls(s_rls)` is invariantly TRUE — in and
    // out of transactions, under any contention.
    let s_rls = ex.create_session();
    if let Err(e) = ex.bind_authenticated_session(s_rls, "bob").await {
        sec.push("guards", format!("bind of bob failed: {e}"));
        return;
    }

    let ex2 = ex.clone();
    let churn = tokio::task::spawn(async move {
        // Flip s_rls's own transaction state (BEGIN/ROLLBACK) and churn the
        // sessions map, so any try_read-with-default in the guards has real
        // write-lock contention to lose against.
        for i in 0..600u32 {
            if i % 3 == 0 {
                let sid = ex2.create_session();
                ex2.drop_session(sid);
            }
            let _ = exec(&ex2, s_rls, "BEGIN").await;
            let _ = exec(&ex2, s_rls, "ROLLBACK").await;
        }
    });

    // Checkers: every observation must answer the SAFE direction. The unsafe
    // answer is what the pre-fix code produced under contention.
    let mut unsafe_txn_answers = 0usize;
    let mut unsafe_rls_answers = 0usize;
    let mut observations = 0usize;
    for _ in 0..40_000usize {
        observations += 1;
        if !ex.session_in_transaction(s_txn) {
            unsafe_txn_answers += 1;
        }
        if !ex.session_has_active_rls(s_rls) {
            unsafe_rls_answers += 1;
        }
    }
    let _ = churn.await;

    if !perturb {
        if unsafe_txn_answers > 0 {
            sec.push(
                "guards",
                format!(
                    "session_in_transaction answered FALSE {unsafe_txn_answers}/{observations} \
                     times while the session sat inside an open transaction — contention was \
                     read as 'not in a transaction', which is how an autocommit fast path \
                     reopens inside someone else's transaction (NU-217)"
                ),
            );
        }
        if unsafe_rls_answers > 0 {
            sec.push(
                "guards",
                format!(
                    "session_has_active_rls answered FALSE {unsafe_rls_answers}/{observations} \
                     times for a bound non-superuser with a policy enabled — the guard fails \
                     OPEN under contention (NU-217)"
                ),
            );
        }
    } else if unsafe_txn_answers == 0 && unsafe_rls_answers == 0 {
        // Model of the bug: at least one guard observation answers the unsafe
        // direction. The fixed engine never does — that is the divergence the
        // control needs.
        sec.push(
            "guards",
            "every guard observation answered the safe direction (perturbed model \
             expects at least one NU-217 misreport)"
                .to_string(),
        );
    }

    // Sanity mirrors, so a green guards run cannot mean "the checks checked
    // nothing": after ROLLBACK the txn answer must flip, and the superuser
    // default session must report no active RLS.
    let _ = exec(&ex, s_txn, "ROLLBACK").await;
    if ex.session_in_transaction(s_txn) {
        sec.push(
            "guards",
            "session_in_transaction stayed TRUE after ROLLBACK — the check answers a \
             constant, not the state"
                .to_string(),
        );
    }
    if ex.session_has_active_rls(0) {
        sec.push(
            "guards",
            "the bootstrap superuser session reports active RLS — the check cannot \
             distinguish bypass from enforcement"
                .to_string(),
        );
    }
}

// ─── Driver ──────────────────────────────────────────────────────────────────

fn run_sections(perturb: Option<&str>) -> Sections {
    let mut sec = Sections::default();
    let rt = tokio::runtime::Handle::current();
    let r1 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(section_authority(perturb == Some("authority"), &mut sec));
    }));
    if r1.is_err() {
        sec.push("authority", "PANIC during section".to_string());
    }
    let r2 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(section_guards(perturb == Some("guards"), &mut sec));
    }));
    if r2.is_err() {
        sec.push("guards", "PANIC during section".to_string());
    }
    sec
}

fn main_impl() {
    let mut negative: Option<String> = None;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--negative-control" => {
                i += 1;
                let section = args[i].clone();
                if !["authority", "guards"].contains(&section.as_str()) {
                    eprintln!(
                        "--negative-control takes one of: authority, guards (got {section:?})"
                    );
                    std::process::exit(2);
                }
                negative = Some(section);
            }
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    if let Some(section) = &negative {
        println!(
            "NEGATIVE CONTROL: the {section} model is deliberately wrong; that section MUST report"
        );
        let base = run_sections(None);
        let pert = run_sections(Some(section.as_str()));
        println!("\n════ SUMMARY (control) ════");
        for s in ["authority", "guards"] {
            println!(
                "{s:<10}: {} divergence(s)  (clean baseline: {})",
                pert.count(s),
                base.count(s)
            );
        }
        let gained = pert.count(section) as i64 - base.count(section) as i64;
        let spilled: i64 = ["authority", "guards"]
            .iter()
            .filter(|s| **s != section.as_str())
            .map(|s| pert.count(s) as i64 - base.count(s) as i64)
            .sum();
        if gained > 0 && spilled == 0 {
            println!(
                "\nNEGATIVE CONTROL PASSED: perturbing the {section} model added {gained} \
                 divergence(s) to {section} and none to the other section."
            );
            std::process::exit(0);
        }
        println!(
            "\nNEGATIVE CONTROL FAILED: perturbing the {section} model changed {section} by \
             {gained} and the other section by {spilled}. A check that cannot fail is not a \
             check, and a check that fires for something else is worse."
        );
        std::process::exit(1);
    }

    println!("Nucleus session authority + guard-state probe (NU-217 / NU-218 class)");
    let sec = run_sections(None);
    for (section, detail) in &sec.findings {
        println!("─── [{section}] {detail}");
    }
    println!("\n════ SUMMARY ════");
    for s in ["authority", "guards"] {
        println!("{s:<10}: {} divergence(s)", sec.count(s));
    }
    if sec.total() == 0 {
        println!("\nAuthority never fell back and the guards never answered the unsafe direction.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
