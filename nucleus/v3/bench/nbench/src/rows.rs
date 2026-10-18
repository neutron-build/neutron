//! Row registry and run planning.
//!
//! Invariants owned here (PLAN-v3 §5.2 fairness rules, card B-0 Work item 3/4):
//! - Every row's plan is built by [`plan`] from the row definition and the
//!   parameters, so the sequencing rules live in *one* place and are unit
//!   tested: >= `runs` warm executions after `warmup_runs` warmup runs and a
//!   cold run that follows a container restart (rule 5), an init digest
//!   check before any measurement (rule 4), and a final digest afterwards.
//! - Durability variants are *named server start variants* (`sync-on`,
//!   `aof-always`, ...), so a pair of runs can never accidentally mix
//!   durability settings (rule 3).
//! - Sweeps are cross products taken in a fixed order, so a row's series
//!   list is reproducible.

use crate::params::Params;

/// How a workload's raw log is turned into metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parser {
    /// PostgreSQL pgbench (tps from the summary, percentiles from --log).
    Pgbench,
    /// sysbench (per-sec rates from the final stats block).
    Sysbench,
    /// memtier_benchmark (Totals block).
    Memtier,
    /// SQLite speedtest1 (TOTAL wall time, --sha3 digest line).
    Speedtest1,
    /// Vector search helper output (already JSON metrics).
    VectorSearch,
}

/// Metric direction, for honest ratio reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Higher is better (throughput, recall, QPS).
    HigherIsBetter,
    /// Lower is better (latency, run time, memory).
    LowerIsBetter,
}

/// Label per PLAN-v3 §5.3: beats >= 1.0x, competitive >= 0.7x, else behind.
pub fn label_for_ratio(ratio: f64) -> &'static str {
    if ratio >= 1.0 {
        "beats"
    } else if ratio >= 0.7 {
        "competitive"
    } else {
        "behind"
    }
}

/// One config point of a row's sweep (e.g. clients=16, synchronous_commit=on).
pub type Series = Vec<(String, String)>;

/// Server start variants: named extra args appended to the container command.
pub const SYNC_ON: &str = "sync-on";
/// `synchronous_commit=off` start variant for PostgreSQL.
pub const SYNC_OFF: &str = "sync-off";
/// AOF fsync per write variant for Valkey/Redis.
pub const AOF_ALWAYS: &str = "aof-always";
/// AOF fsync once per second variant for Valkey/Redis.
pub const AOF_EVERYSEC: &str = "aof-everysec";

/// Readiness probe for a server system.
#[derive(Clone, Copy, Debug)]
pub enum Ready {
    /// `pg_isready -U postgres -d <db>` via podman exec.
    PgIsready { db: &'static str },
    /// `<cli> ping` expecting PONG via podman exec.
    RespPing { cli: &'static str },
    /// HTTP GET /healthz expecting 200 (Qdrant).
    Http { port: u16 },
    /// Transient system: nothing to wait for.
    None,
}

/// Which kind of container lifecycle a system needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SystemKind {
    /// A long-lived server started under a systemd user unit (card B-0
    /// environment note: `systemd-run --user --unit nbench-<system> --collect`).
    Server,
    /// No server; driver scripts run throwaway containers themselves.
    Transient,
}

/// A competitor (or client tool) container setup.
pub struct SystemDef {
    /// Registry name (`systems/<name>/`).
    pub name: &'static str,
    /// Server (`Server`) or throwaway (`Transient`).
    pub kind: SystemKind,
    /// Named volume for durable state.
    pub volume: &'static str,
    /// Where the volume is mounted in the container.
    pub mount: &'static str,
    /// podman `--memory` limit (recorded in results; §5.2 rule 6).
    pub mem_limit: &'static str,
    /// podman `--cpus` limit (recorded in results; §5.2 rule 6).
    pub cpus: &'static str,
    /// Base args appended to the image command.
    pub base_args: &'static [&'static str],
    /// Named variants: extra args appended after the base args.
    pub variants: &'static [(&'static str, &'static [&'static str])],
    /// How to wait for readiness.
    pub ready: Ready,
    /// Extra `podman run` arguments (env, mounts) before the image name.
    pub run_extra: &'static [&'static str],
}

/// A benchmark row: one line of the §5.1 matrix.
pub struct RowDef {
    /// Row id, also `workloads/<id>/` and the results file name.
    pub id: &'static str,
    /// System registry name.
    pub system: &'static str,
    /// Log parser.
    pub parser: Parser,
    /// Short description for the report.
    pub description: &'static str,
    /// Metric names this row reports, with their better-direction.
    pub metrics: &'static [(&'static str, Direction)],
    /// Build the sweep: ordered list of config points.
    pub sweep: fn(&Params) -> Vec<Series>,
    /// Map a series point to a server start variant (durability pairing).
    pub variant_of: fn(&Series) -> Option<&'static str>,
    /// Compare the init digest against the committed expectation.
    pub strict_init_digest: bool,
    /// Compare the final digest against the committed expectation.
    pub strict_final_digest: bool,
}

fn kv_pair(k: &str, v: String) -> (String, String) {
    (k.to_string(), v)
}

/// Cross product of (key, values) lists, preserving order.
fn cross(dims: &[(&str, Vec<String>)]) -> Vec<Series> {
    let mut out: Vec<Series> = vec![Vec::new()];
    for (k, vs) in dims {
        let mut next = Vec::with_capacity(out.len() * vs.len());
        for point in &out {
            for v in vs {
                let mut p = point.clone();
                p.push(kv_pair(k, v.clone()));
                next.push(p);
            }
        }
        out = next;
    }
    out
}

fn sweep_pgbench(p: &Params) -> Vec<Series> {
    cross(&[
        ("synchronous_commit", p.modes.clone()),
        ("clients", p.clients.iter().map(|c| c.to_string()).collect()),
    ])
}

fn variant_pgbench(s: &Series) -> Option<&'static str> {
    let mode = s
        .iter()
        .find(|(k, _)| k == "synchronous_commit")
        .map(|(_, v)| v.as_str())?;
    match mode {
        "on" => Some(SYNC_ON),
        "off" => Some(SYNC_OFF),
        _ => None,
    }
}

fn sweep_sysbench(p: &Params) -> Vec<Series> {
    cross(&[
        ("workload", p.workloads.clone()),
        ("threads", p.threads.iter().map(|t| t.to_string()).collect()),
    ])
}

fn sweep_kv(p: &Params) -> Vec<Series> {
    cross(&[
        ("appendfsync", p.fsync_modes.clone()),
        (
            "pipeline",
            p.pipelines.iter().map(|v| v.to_string()).collect(),
        ),
    ])
}

fn variant_kv(s: &Series) -> Option<&'static str> {
    let mode = s
        .iter()
        .find(|(k, _)| k == "appendfsync")
        .map(|(_, v)| v.as_str())?;
    match mode {
        "always" => Some(AOF_ALWAYS),
        "everysec" => Some(AOF_EVERYSEC),
        _ => None,
    }
}

fn sweep_ef(p: &Params) -> Vec<Series> {
    p.ef.iter()
        .map(|e| vec![kv_pair("ef_search", e.to_string())])
        .collect()
}

fn sweep_single(_: &Params) -> Vec<Series> {
    vec![Vec::new()]
}

fn no_variant(_: &Series) -> Option<&'static str> {
    None
}

macro_rules! row {
    (
        $id:literal, $system:literal, $parser:ident, $desc:literal,
        [$($metric:literal => $dir:ident),* $(,)?],
        $sweep:ident, $variant:ident,
        init: $init:literal, final: $final:literal
    ) => {
        RowDef {
            id: $id,
            system: $system,
            parser: Parser::$parser,
            description: $desc,
            metrics: &[$(($metric, Direction::$dir)),*],
            sweep: $sweep,
            variant_of: $variant,
            strict_init_digest: $init,
            strict_final_digest: $final,
        }
    };
}

/// All rows the harness knows (card B-0 Work item 3; Nucleus columns are
/// filled by later cards C-9/D-*).
pub const ROWS: &[RowDef] = &[
    row!(
        "oltp-pgbench-pg17", "postgres17", Pgbench,
        "pgbench TPC-B (scale 100), clients 1/16/64, synchronous_commit on/off",
        ["tps" => HigherIsBetter, "latency_avg_ms" => LowerIsBetter,
         "p50_us" => LowerIsBetter, "p99_us" => LowerIsBetter,
         "p999_us" => LowerIsBetter],
        sweep_pgbench, variant_pgbench, init: true, final: false
    ),
    row!(
        "oltp-sysbench-pg17", "postgres17", Sysbench,
        "sysbench oltp_read_write + oltp_point_select on PG17, threads 1/16/64",
        ["tps" => HigherIsBetter, "qps" => HigherIsBetter,
         "latency_avg_ms" => LowerIsBetter, "p99_ms" => LowerIsBetter],
        sweep_sysbench, no_variant, init: true, final: false
    ),
    row!(
        "kv-memtier-valkey8", "valkey8", Memtier,
        "memtier GET/SET 1:10 against Valkey 8, pipeline 1/16, AOF always/everysec",
        ["ops_sec" => HigherIsBetter, "p50_ms" => LowerIsBetter,
         "p99_ms" => LowerIsBetter, "kb_per_sec" => HigherIsBetter],
        sweep_kv, variant_kv, init: true, final: true
    ),
    row!(
        "kv-memtier-redis7", "redis7", Memtier,
        "memtier GET/SET 1:10 against Redis 7, pipeline 1/16, AOF always/everysec",
        ["ops_sec" => HigherIsBetter, "p50_ms" => LowerIsBetter,
         "p99_ms" => LowerIsBetter, "kb_per_sec" => HigherIsBetter],
        sweep_kv, variant_kv, init: true, final: true
    ),
    row!(
        "vector-sift1m-qdrant", "qdrant", VectorSearch,
        "ann-benchmarks SIFT1M (HNSW m=16, ef_construction=200), recall@10 vs QPS",
        ["qps" => HigherIsBetter, "recall_at_10" => HigherIsBetter,
         "p50_ms" => LowerIsBetter, "p99_ms" => LowerIsBetter],
        sweep_ef, no_variant, init: true, final: true
    ),
    row!(
        "vector-sift1m-pgvector", "pgvector17", VectorSearch,
        "ann-benchmarks SIFT1M via pgvector HNSW (m=16, ef_construction=200), recall@10 vs QPS",
        ["qps" => HigherIsBetter, "recall_at_10" => HigherIsBetter],
        sweep_ef, no_variant, init: true, final: true
    ),
    row!(
        "embedded-sqlite-speedtest1", "sqlite", Speedtest1,
        "SQLite speedtest1 from the amalgamation (in container)",
        ["wall_s" => LowerIsBetter, "user_s" => LowerIsBetter, "sys_s" => LowerIsBetter],
        sweep_single, no_variant, init: true, final: true
    ),
];

impl RowDef {
    /// Look up a row by id.
    pub fn find(id: &str) -> Option<&'static RowDef> {
        ROWS.iter().find(|r| r.id == id)
    }
}

/// The container registry.
pub const SYSTEMS: &[SystemDef] = &[
    SystemDef {
        name: "postgres17",
        kind: SystemKind::Server,
        volume: "nbench-pg17-data",
        mount: "/var/lib/postgresql/data",
        mem_limit: "24g",
        cpus: "16",
        // Tuning per PostgreSQL documentation production guidance:
        // shared_buffers ~25% of machine RAM (38 GiB host) rounded down;
        // max_wal_size raised to reduce checkpoint frequency during bulk
        // load and the bench (PG docs §30.5 WAL configuration).
        base_args: &[
            "-c",
            "shared_buffers=6GB",
            "-c",
            "max_wal_size=8GB",
            "-c",
            "checkpoint_timeout=15min",
        ],
        variants: &[
            (SYNC_ON, &["-c", "synchronous_commit=on"]),
            (SYNC_OFF, &["-c", "synchronous_commit=off"]),
        ],
        ready: Ready::PgIsready { db: "nbench" },
        run_extra: &[
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "-e",
            "POSTGRES_DB=nbench",
        ],
    },
    SystemDef {
        name: "pgvector17",
        kind: SystemKind::Server,
        volume: "nbench-pgvec-data",
        mount: "/var/lib/postgresql/data",
        mem_limit: "24g",
        cpus: "16",
        // Same PostgreSQL tuning guidance as postgres17 (same engine);
        // separate port so both can coexist on the host network.
        base_args: &[
            "-c",
            "port=5433",
            "-c",
            "shared_buffers=6GB",
            "-c",
            "max_wal_size=8GB",
            "-c",
            "checkpoint_timeout=15min",
            "-c",
            "maintenance_work_mem=2GB",
        ],
        variants: &[],
        ready: Ready::PgIsready { db: "nbench" },
        run_extra: &[
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "-e",
            "POSTGRES_DB=nbench",
            "-v",
            "nbench-pgvec-init:/docker-entrypoint-initdb.d:ro",
        ],
    },
    SystemDef {
        name: "valkey8",
        kind: SystemKind::Server,
        volume: "nbench-valkey8-data",
        mount: "/data",
        mem_limit: "12g",
        cpus: "16",
        // Valkey production guidance (valkey.io docs: persistence + memory):
        // AOF for durability with the fsync policy as the matched pair; RDB
        // snapshots disabled to isolate AOF semantics; explicit maxmemory
        // with the default noeviction policy stated.
        base_args: &[
            "--appendonly",
            "yes",
            "--save",
            "",
            "--maxmemory",
            "6gb",
            "--maxmemory-policy",
            "noeviction",
        ],
        variants: &[
            (AOF_ALWAYS, &["--appendfsync", "always"]),
            (AOF_EVERYSEC, &["--appendfsync", "everysec"]),
        ],
        ready: Ready::RespPing { cli: "valkey-cli" },
        run_extra: &[],
    },
    SystemDef {
        name: "redis7",
        kind: SystemKind::Server,
        volume: "nbench-redis7-data",
        mount: "/data",
        mem_limit: "12g",
        cpus: "16",
        // Same guidance as Valkey; redis.io persistence + memory docs.
        // Non-default port so Valkey and Redis runs can coexist.
        base_args: &[
            "--port",
            "6380",
            "--appendonly",
            "yes",
            "--save",
            "",
            "--maxmemory",
            "6gb",
            "--maxmemory-policy",
            "noeviction",
        ],
        variants: &[
            (AOF_ALWAYS, &["--appendfsync", "always"]),
            (AOF_EVERYSEC, &["--appendfsync", "everysec"]),
        ],
        ready: Ready::RespPing { cli: "redis-cli" },
        run_extra: &[],
    },
    SystemDef {
        name: "qdrant",
        kind: SystemKind::Server,
        volume: "nbench-qdrant-storage",
        mount: "/qdrant/storage",
        mem_limit: "24g",
        cpus: "16",
        // Defaults are Qdrant's documented production defaults; HNSW
        // parameters are set per-collection at load time.
        base_args: &[],
        variants: &[],
        ready: Ready::Http { port: 6333 },
        run_extra: &[],
    },
    SystemDef {
        name: "sqlite",
        kind: SystemKind::Transient,
        volume: "nbench-sqlite-artifacts",
        mount: "/opt/nb",
        mem_limit: "24g",
        cpus: "16",
        base_args: &[],
        variants: &[],
        ready: Ready::None,
        run_extra: &[],
    },
];

impl SystemDef {
    /// Look up a system by name.
    pub fn find(name: &str) -> Option<&'static SystemDef> {
        SYSTEMS.iter().find(|s| s.name == name)
    }

    /// Host-side port the service listens on (`--network host`).
    pub fn port(&self) -> u16 {
        match self.name {
            "postgres17" => 5432,
            "pgvector17" => 5433,
            "valkey8" => 6379,
            "redis7" => 6380,
            "qdrant" => 6333,
            _ => 0,
        }
    }

    /// Container name (also the systemd unit base name).
    pub fn container(&self) -> String {
        format!("nbench-{}", self.name)
    }
}

/// One planned action.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    /// Start (or restart) the system container, optionally with a variant.
    Start { variant: Option<String> },
    /// Execute the workload driver once.
    Exec {
        /// init / cold / warmup / run.
        mode: &'static str,
        /// The config point this execution belongs to.
        series: Series,
        /// Index of the execution within its (series, mode) group.
        index: usize,
    },
    /// Run the digest script and compare when strict.
    Digest { phase: &'static str, strict: bool },
}

/// Build the full ordered plan for a row (the §5.2 sequencing, in one place).
pub fn plan(row: &RowDef, p: &Params) -> Vec<Step> {
    let mut steps = Vec::new();
    if let Some(sys) = SystemDef::find(row.system)
        && sys.kind == SystemKind::Server
    {
        steps.push(Step::Start { variant: None });
    }
    steps.push(Step::Exec {
        mode: "init",
        series: Vec::new(),
        index: 0,
    });
    if row.strict_init_digest || true {
        // The init digest is always recorded; strictness only controls
        // whether a mismatch voids the run (handled by the runner).
        steps.push(Step::Digest {
            phase: "init",
            strict: row.strict_init_digest,
        });
    }
    for series in (row.sweep)(p) {
        let variant = (row.variant_of)(&series).map(|v| v.to_string());
        steps.push(Step::Start { variant });
        for i in 0..p.cold_runs.max(1) {
            steps.push(Step::Exec {
                mode: "cold",
                series: series.clone(),
                index: i,
            });
        }
        for i in 0..p.warmup_runs {
            steps.push(Step::Exec {
                mode: "warmup",
                series: series.clone(),
                index: i,
            });
        }
        for i in 0..p.runs {
            steps.push(Step::Exec {
                mode: "run",
                series: series.clone(),
                index: i,
            });
        }
    }
    steps.push(Step::Digest {
        phase: "final",
        strict: row.strict_final_digest,
    });
    steps
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::Params;

    #[test]
    fn every_row_plan_has_five_warm_runs_and_a_cold_restart() {
        let p = Params::default();
        for row in ROWS {
            let steps = plan(row, &p);
            let warm = steps
                .iter()
                .filter(|s| matches!(s, Step::Exec { mode: "run", .. }))
                .count();
            assert!(warm >= 5, "{} has {warm} warm runs", row.id);
            // Every cold execution must directly follow a Start (fresh process).
            for (i, s) in steps.iter().enumerate() {
                if let Step::Exec { mode: "cold", .. } = s {
                    let prev = steps
                        .get(i.wrapping_sub(1))
                        .map(|s| matches!(s, Step::Start { .. }))
                        .unwrap_or(false);
                    assert!(prev, "{}: cold run not preceded by a restart", row.id);
                }
            }
            // An init digest runs before any measurement (§5.2 rule 4).
            let first_digest = steps
                .iter()
                .position(|s| matches!(s, Step::Digest { .. }))
                .expect("row has a digest step");
            let first_exec = steps
                .iter()
                .position(|s| matches!(s, Step::Exec { .. }))
                .expect("row has an exec step");
            assert!(
                first_exec < first_digest,
                "{}: digest does not follow init",
                row.id
            );
            // And a final digest closes the row.
            assert!(
                matches!(steps.last(), Some(Step::Digest { phase: "final", .. })),
                "{}: no final digest",
                row.id
            );
        }
    }

    #[test]
    fn pgbench_plan_sweeps_both_durability_modes() {
        let p = Params::default();
        let row = RowDef::find("oltp-pgbench-pg17").expect("row");
        let steps = plan(row, &p);
        let starts: Vec<_> = steps
            .iter()
            .filter_map(|s| match s {
                Step::Start { variant } => Some(variant.clone()),
                _ => None,
            })
            .collect();
        assert!(starts.contains(&Some(SYNC_ON.to_string())));
        assert!(starts.contains(&Some(SYNC_OFF.to_string())));
        // 2 modes x 3 client counts = 6 config points
        assert_eq!((row.sweep)(&p).len(), 6);
    }

    #[test]
    fn kv_plan_pairs_fsync_with_pipeline() {
        let p = Params::default();
        let row = RowDef::find("kv-memtier-valkey8").expect("row");
        let series = (row.sweep)(&p);
        assert_eq!(series.len(), 4);
        for s in &series {
            let fsync = s.iter().find(|(k, _)| k == "appendfsync").expect("fsync");
            assert!(
                fsync.1 == "always" || fsync.1 == "everysec",
                "unexpected fsync {fsync:?}"
            );
            assert!((row.variant_of)(s).is_some());
        }
        // each config point restarts into its own durability variant, plus
        // the initial start before init
        let steps = plan(row, &p);
        let restarts = steps
            .iter()
            .filter(|s| matches!(s, Step::Start { .. }))
            .count();
        assert_eq!(restarts, 4 + 1);
    }

    #[test]
    fn sweep_respects_overrides() {
        let p = Params::with_overrides(&[
            ("runs", "2"),
            ("cold_runs", "1"),
            ("warmup_runs", "0"),
            ("clients", "1"),
            ("modes", "on"),
        ])
        .expect("overrides");
        let row = RowDef::find("oltp-pgbench-pg17").expect("row");
        let steps = plan(row, &p);
        assert_eq!(
            steps
                .iter()
                .filter(|s| matches!(s, Step::Exec { mode: "run", .. }))
                .count(),
            2
        );
        assert_eq!(
            steps
                .iter()
                .filter(|s| matches!(s, Step::Exec { mode: "warmup", .. }))
                .count(),
            0
        );
        assert_eq!((row.sweep)(&p).len(), 1);
    }

    #[test]
    fn labels_follow_the_plan_thresholds() {
        assert_eq!(label_for_ratio(1.0), "beats");
        assert_eq!(label_for_ratio(1.5), "beats");
        assert_eq!(label_for_ratio(0.99), "competitive");
        assert_eq!(label_for_ratio(0.7), "competitive");
        assert_eq!(label_for_ratio(0.69), "behind");
    }

    #[test]
    fn cross_product_order_is_stable() {
        let dims: Vec<(&str, Vec<String>)> = vec![
            ("a", vec!["1".into(), "2".into()]),
            ("b", vec!["x".into(), "y".into()]),
        ];
        let got = cross(&dims);
        let flat: Vec<String> = got
            .iter()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        assert_eq!(flat, vec!["a=1,b=x", "a=1,b=y", "a=2,b=x", "a=2,b=y"]);
    }

    #[test]
    fn system_ports_are_distinct() {
        let mut seen = std::collections::HashSet::new();
        for s in SYSTEMS {
            if s.kind == SystemKind::Server {
                assert!(seen.insert(s.port()), "duplicate port for {}", s.name);
            }
        }
    }
}
