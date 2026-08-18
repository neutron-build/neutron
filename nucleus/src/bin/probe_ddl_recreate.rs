//! Re-registration probe (NU-251 class).
//!
//! The class is **a create applied twice destroying what the first create
//! made, and reporting success**. NU-251's instance was `CREATE TABLE` on the
//! memory engine: the second statement replaced the live rows and returned
//! OK, reachable through the shipped embedded API rather than only from tests.
//! Nothing about that is specific to tables, or to one engine — a duplicate
//! `CREATE INDEX`, `CREATE SEQUENCE` or `CREATE VIEW` has exactly the same
//! opportunity, and each is registered by different code.
//!
//! So the invariant is deliberately NOT "a duplicate create must error". Some
//! of them legitimately succeed (`IF NOT EXISTS`, `OR REPLACE`), and pinning
//! the error would test the parser rather than the defect. The invariant is
//! **whatever the second statement answers, the state the first one created is
//! still there afterwards** — the rows, the index's answers, the sequence's
//! position, the view's definition — and it is still there after a third
//! attempt, because a destructive path that needs two tries is the same bug.
//!
//! **Section `tables`** sweeps table re-creation (exact repeat, repeat with a
//! different schema, and both `IF NOT EXISTS` forms) across **every** engine,
//! because the original defect lived in one engine's implementation while the
//! others were correct — a single-engine check would have passed on the day
//! NU-251 shipped.
//!
//! **Section `objects`** sweeps the rest of the CREATE surface — index, view,
//! sequence, enum type, role, policy — on the same engines. A case whose setup
//! the engine does not support is SKIPPED and counted, and the count is
//! printed on every run: a probe that silently covered two of six objects
//! reads exactly like one that covered six.
//!
//! `--negative-control <tables|objects>` runs both sections twice at one seed,
//! clean, then with that section's state deliberately damaged between the two
//! observations. It passes only if the perturbation adds divergences to that
//! section and none to the other. This control tests the thing most likely to
//! be silently wrong here — not the expectation, but whether the observation
//! can see state loss at all. An observation that cannot is a probe that
//! passes forever.
//!
//! Build: `cargo run --release --features server --bin probe_ddl_recreate`
//!        `... --bin probe_ddl_recreate -- --negative-control tables`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::collections::BTreeMap;
use std::path::PathBuf;

use nucleus::executor::ExecResult;
use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb};

// ─── Divergence bookkeeping ──────────────────────────────────────────────────

#[derive(Default)]
struct Sections {
    counts: BTreeMap<&'static str, usize>,
    findings: Vec<(&'static str, String)>,
    stats: BTreeMap<String, usize>,
}

impl Sections {
    fn push(&mut self, section: &'static str, detail: String) {
        *self.counts.entry(section).or_insert(0) += 1;
        if self.findings.len() < 60 {
            self.findings.push((section, detail));
        }
    }
    fn bump(&mut self, key: String) {
        *self.stats.entry(key).or_insert(0) += 1;
    }
    fn count(&self, section: &str) -> usize {
        self.counts.get(section).copied().unwrap_or(0)
    }
    fn total(&self) -> usize {
        self.counts.values().sum()
    }
}

const SECTIONS: [&str; 2] = ["tables", "objects"];

// ─── Harness plumbing ────────────────────────────────────────────────────────

struct TmpDir(PathBuf);
impl TmpDir {
    fn new(tag: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("nucleus_ddl_recreate_{tag}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("mkdir temp");
        TmpDir(p)
    }
}
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open_harness(kind: EngineKind, dir: &std::path::Path) -> Result<HarnessDb, String> {
    let rt = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| rt.block_on(HarnessDb::open(kind, dir, EngineConfig::default())))
        .map_err(|e| format!("open: {e:?}"))
}

/// Run a statement and render whatever it produced as a stable string, so a
/// SELECT can be used as "the state that must survive" without the probe
/// caring what shape the state has.
fn exec(db: &HarnessDb, sql: &str) -> Result<String, String> {
    let rt = tokio::runtime::Handle::current();
    let res = tokio::task::block_in_place(|| rt.block_on(db.executor().execute(sql)));
    let results = res.map_err(|e| format!("{e:?}"))?;
    let mut out = String::new();
    for r in &results {
        match r {
            ExecResult::Select { rows, .. } => {
                for row in rows {
                    let cells: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                    out.push_str(&cells.join("|"));
                    out.push(';');
                }
            }
            ExecResult::Command { rows_affected, .. } => {
                out.push_str(&format!("cmd:{rows_affected};"));
            }
            other => out.push_str(&format!("{other:?};")),
        }
    }
    Ok(out)
}

// ─── Cases ───────────────────────────────────────────────────────────────────

struct Case {
    what: &'static str,
    setup: &'static [&'static str],
    /// A query whose result IS the state the re-registration must not touch.
    observe: &'static str,
    /// The duplicate registration.
    again: &'static str,
    /// True when the second statement is *supposed* to succeed (IF NOT EXISTS,
    /// OR REPLACE). Used only to describe the outcome, never to excuse loss.
    success_expected: bool,
    /// A statement that removes some of the observed state. Used ONLY by the
    /// negative control, to prove `observe` can see loss at all.
    damage: &'static str,
}

const TABLE_SETUP: &[&str] = &[
    "CREATE TABLE t (id INT, v TEXT)",
    "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
];

fn table_cases() -> Vec<Case> {
    vec![
        Case {
            what: "CREATE TABLE, identical definition",
            setup: TABLE_SETUP,
            observe: "SELECT id, v FROM t ORDER BY id",
            again: "CREATE TABLE t (id INT, v TEXT)",
            success_expected: false,
            damage: "DELETE FROM t WHERE id = 2",
        },
        Case {
            what: "CREATE TABLE, different definition",
            setup: TABLE_SETUP,
            observe: "SELECT id, v FROM t ORDER BY id",
            again: "CREATE TABLE t (x TEXT, y INT, z INT)",
            success_expected: false,
            damage: "DELETE FROM t WHERE id = 2",
        },
        Case {
            what: "CREATE TABLE IF NOT EXISTS, identical definition",
            setup: TABLE_SETUP,
            observe: "SELECT id, v FROM t ORDER BY id",
            again: "CREATE TABLE IF NOT EXISTS t (id INT, v TEXT)",
            success_expected: true,
            damage: "DELETE FROM t WHERE id = 2",
        },
        Case {
            what: "CREATE TABLE IF NOT EXISTS, different definition",
            setup: TABLE_SETUP,
            observe: "SELECT id, v FROM t ORDER BY id",
            again: "CREATE TABLE IF NOT EXISTS t (x TEXT, y INT, z INT)",
            success_expected: true,
            damage: "DELETE FROM t WHERE id = 2",
        },
        Case {
            // The schema is state too: a re-create that leaves the rows but
            // adopts the new column list is the same defect one layer up.
            what: "CREATE TABLE, different definition (schema preserved)",
            setup: TABLE_SETUP,
            observe: "SELECT v FROM t WHERE id = 3",
            again: "CREATE TABLE t (x TEXT)",
            success_expected: false,
            damage: "UPDATE t SET v = 'damaged' WHERE id = 3",
        },
    ]
}

fn object_cases() -> Vec<Case> {
    vec![
        Case {
            what: "CREATE INDEX, identical",
            setup: &[
                "CREATE TABLE t (id INT, v TEXT)",
                "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "CREATE INDEX ix_t_id ON t (id)",
            ],
            observe: "SELECT v FROM t WHERE id = 2",
            again: "CREATE INDEX ix_t_id ON t (id)",
            success_expected: false,
            damage: "DELETE FROM t WHERE id = 2",
        },
        Case {
            what: "CREATE INDEX IF NOT EXISTS",
            setup: &[
                "CREATE TABLE t (id INT, v TEXT)",
                "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "CREATE INDEX ix_t_id ON t (id)",
            ],
            observe: "SELECT v FROM t WHERE id = 2",
            again: "CREATE INDEX IF NOT EXISTS ix_t_id ON t (id)",
            success_expected: true,
            damage: "DELETE FROM t WHERE id = 2",
        },
        Case {
            what: "CREATE VIEW, identical",
            setup: &[
                "CREATE TABLE t (id INT, v TEXT)",
                "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "CREATE VIEW v_t AS SELECT id, v FROM t WHERE id > 1",
            ],
            observe: "SELECT id, v FROM v_t ORDER BY id",
            again: "CREATE VIEW v_t AS SELECT id, v FROM t WHERE id > 1",
            success_expected: false,
            damage: "DELETE FROM t WHERE id = 2",
        },
        Case {
            // A sequence's position is its state. Re-registering it must not
            // rewind the counter — a silently reset sequence hands out
            // primary keys that already exist.
            what: "CREATE SEQUENCE, identical",
            setup: &[
                "CREATE SEQUENCE s_probe",
                "SELECT NEXTVAL('s_probe')",
                "SELECT NEXTVAL('s_probe')",
                "SELECT NEXTVAL('s_probe')",
            ],
            observe: "SELECT CURRVAL('s_probe')",
            again: "CREATE SEQUENCE s_probe",
            success_expected: false,
            damage: "SELECT SETVAL('s_probe', 1)",
        },
        Case {
            what: "CREATE SEQUENCE IF NOT EXISTS",
            setup: &[
                "CREATE SEQUENCE s_probe",
                "SELECT NEXTVAL('s_probe')",
                "SELECT NEXTVAL('s_probe')",
                "SELECT NEXTVAL('s_probe')",
            ],
            observe: "SELECT CURRVAL('s_probe')",
            again: "CREATE SEQUENCE IF NOT EXISTS s_probe",
            success_expected: true,
            damage: "SELECT SETVAL('s_probe', 1)",
        },
        Case {
            what: "CREATE TYPE AS ENUM, identical",
            setup: &[
                "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
                "CREATE TABLE t (id INT, m mood)",
                "INSERT INTO t VALUES (1, 'happy')",
            ],
            observe: "SELECT id, m FROM t ORDER BY id",
            again: "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
            success_expected: false,
            damage: "DELETE FROM t WHERE id = 1",
        },
        Case {
            // A re-created role that silently drops its grants is a
            // privilege change disguised as a no-op.
            what: "CREATE ROLE, identical",
            setup: &[
                "CREATE TABLE t (id INT, v TEXT)",
                "INSERT INTO t VALUES (1, 'a')",
                "CREATE ROLE probe_role LOGIN PASSWORD 'p'",
                "GRANT SELECT ON t TO probe_role",
            ],
            observe: "SELECT id, v FROM t ORDER BY id",
            again: "CREATE ROLE probe_role LOGIN PASSWORD 'p'",
            success_expected: false,
            damage: "DELETE FROM t WHERE id = 1",
        },
        Case {
            what: "CREATE POLICY, identical",
            setup: &[
                "CREATE TABLE t (id INT, v TEXT)",
                "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
                "CREATE POLICY p_t ON t FOR SELECT USING (v = CURRENT_USER)",
                "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
            ],
            observe: "SELECT COUNT(*) FROM t",
            again: "CREATE POLICY p_t ON t FOR SELECT USING (v = CURRENT_USER)",
            success_expected: false,
            damage: "DELETE FROM t WHERE id = 1",
        },
    ]
}

// ─── One case, one engine ────────────────────────────────────────────────────

fn run_case(
    section: &'static str,
    kind: EngineKind,
    idx: usize,
    case: &Case,
    perturb: bool,
    sec: &mut Sections,
) {
    let tmp = TmpDir::new(&format!("{}_{}_{}", section, kind.name(), idx));
    let db = match open_harness(kind, &tmp.0) {
        Ok(d) => d,
        Err(e) => {
            sec.push(
                section,
                format!("[{}] could not open engine: {e}", kind.name()),
            );
            return;
        }
    };

    for stmt in case.setup {
        if let Err(e) = exec(&db, stmt) {
            // Unsupported DDL is a coverage gap, not a divergence — but it is
            // counted and printed, because a silently skipped case reads
            // exactly like a passing one.
            sec.bump(format!("skipped/{}/{}", kind.name(), case.what));
            let _ = e;
            return;
        }
    }

    let before = match exec(&db, case.observe) {
        Ok(v) => v,
        Err(e) => {
            sec.push(
                section,
                format!(
                    "[{}] {}: the observation query failed before the duplicate ran ({e}) — \
                     this case checks nothing",
                    kind.name(),
                    case.what
                ),
            );
            return;
        }
    };

    let again = exec(&db, case.again);
    let outcome = if again.is_ok() {
        "SUCCEEDED"
    } else {
        "errored"
    };
    sec.bump(format!(
        "duplicate-{}/{}",
        if again.is_ok() { "accepted" } else { "refused" },
        case.what
    ));
    if again.is_ok() && !case.success_expected {
        // Not a divergence — nothing was destroyed, which is this probe's
        // subject — but PostgreSQL raises `relation already exists` for these
        // and a client written against it will not see the error it handles.
        sec.bump(format!("accepted-though-postgres-errors/{}", case.what));
    }

    // The control damages the state HERE — after the duplicate, before the
    // second observation — so what it proves is that `observe` can see loss.
    if perturb {
        let _ = exec(&db, case.damage);
    }

    let after = match exec(&db, case.observe) {
        Ok(v) => v,
        Err(e) => {
            sec.push(
                section,
                format!(
                    "[{}] {}: the state became UNREADABLE after the duplicate ({outcome}): {e}",
                    kind.name(),
                    case.what
                ),
            );
            return;
        }
    };

    if before != after {
        sec.push(
            section,
            format!(
                "[{}] {}: the duplicate registration {outcome} and the state it re-registered \
                 did not survive it{} — was {before:?}, now {after:?} (NU-251 class)",
                kind.name(),
                case.what,
                if perturb { " (PERTURBED RUN)" } else { "" }
            ),
        );
        return;
    }

    if perturb {
        sec.push(
            section,
            format!(
                "[{}] {}: the observation returned {before:?} both before and after the state \
                 was deliberately damaged — it cannot see loss, so a clean result from this \
                 case means nothing",
                kind.name(),
                case.what
            ),
        );
        return;
    }

    // A destructive path that needs two tries is the same bug.
    let _ = exec(&db, case.again);
    match exec(&db, case.observe) {
        Ok(third) if third != before => sec.push(
            section,
            format!(
                "[{}] {}: state survived the SECOND registration but not the third — was \
                 {before:?}, now {third:?}",
                kind.name(),
                case.what
            ),
        ),
        Ok(_) => {}
        Err(e) => sec.push(
            section,
            format!(
                "[{}] {}: the state became unreadable after a third registration: {e}",
                kind.name(),
                case.what
            ),
        ),
    }
}

fn section_tables(perturb: bool, sec: &mut Sections) {
    for kind in EngineKind::ALL {
        for (i, case) in table_cases().iter().enumerate() {
            run_case("tables", kind, i, case, perturb, sec);
        }
    }
}

fn section_objects(perturb: bool, sec: &mut Sections) {
    for kind in EngineKind::ALL {
        for (i, case) in object_cases().iter().enumerate() {
            run_case("objects", kind, i + 100, case, perturb, sec);
        }
    }
}

// ─── Driver ──────────────────────────────────────────────────────────────────

fn run_sections(perturb: Option<&str>) -> Sections {
    let mut sec = Sections::default();
    let r1 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        section_tables(perturb == Some("tables"), &mut sec);
    }));
    if r1.is_err() {
        sec.push("tables", "PANIC during section".to_string());
    }
    let r2 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        section_objects(perturb == Some("objects"), &mut sec);
    }));
    if r2.is_err() {
        sec.push("objects", "PANIC during section".to_string());
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
                let section = args.get(i).cloned().unwrap_or_default();
                if !SECTIONS.contains(&section.as_str()) {
                    eprintln!(
                        "--negative-control takes one of: {} (got {section:?})",
                        SECTIONS.join(", ")
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
            "NEGATIVE CONTROL: the {section} state is deliberately damaged between the two \
             observations; that section MUST report"
        );
        let base = run_sections(None);
        let pert = run_sections(Some(section.as_str()));
        println!("\n════ SUMMARY (control) ════");
        for s in SECTIONS {
            println!(
                "{s:<8}: {} divergence(s)  (clean baseline: {})",
                pert.count(s),
                base.count(s)
            );
        }
        let gained = pert.count(section) as i64 - base.count(section) as i64;
        let spilled: i64 = SECTIONS
            .iter()
            .filter(|s| **s != section.as_str())
            .map(|s| pert.count(s) as i64 - base.count(s) as i64)
            .sum();
        if gained > 0 && spilled == 0 {
            println!(
                "\nNEGATIVE CONTROL PASSED: damaging the {section} state added {gained} \
                 divergence(s) to {section} and none to the other section."
            );
            std::process::exit(0);
        }
        println!(
            "\nNEGATIVE CONTROL FAILED: damaging the {section} state changed {section} by \
             {gained} and the other section by {spilled}. An observation that cannot see \
             state loss is a probe that passes forever."
        );
        std::process::exit(1);
    }

    println!("Nucleus re-registration probe (NU-251 class)");
    let sec = run_sections(None);
    for (section, detail) in &sec.findings {
        println!("─── [{section}] {detail}");
    }
    println!("\n════ SUMMARY ════");
    for (k, v) in &sec.stats {
        println!("  {k:<64} {v}");
    }
    for s in SECTIONS {
        println!("{s:<8}: {} divergence(s)", sec.count(s));
    }
    if sec.total() == 0 {
        println!(
            "\nEvery duplicate registration left the state its first form created intact, on \
             every engine."
        );
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
