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
    /// SQLite speedtest1 (TOTAL wall time, --verify hash line).
    Speedtest1,
    /// Vector search helper output (already JSON metrics).
    VectorSearch,
    /// DuckDB TPC-H driver (TPCH_* summary lines from the driver log;
    /// card NB-3 Work 5).
    DuckdbTpch,
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
    /// `pg_isready -U postgres -d <db> -p <port>` via podman exec (the
    /// port must match the server's configured port).
    PgIsready { db: &'static str, port: u16 },
    /// `<cli> -p <port> ping` expecting PONG via podman exec. The port is
    /// part of the probe because Redis and Valkey coexist on the host
    /// network with distinct ports (6379/6380); a probe that ignores the
    /// configured port pings the wrong (or no) server.
    RespPing { cli: &'static str, port: u16 },
    /// `mysqladmin -h 127.0.0.1 -P <port> ping` via podman exec. The port
    /// must be carried over TCP (never the container's unix socket): the
    /// mysql entrypoint's first-time init runs a temporary server with
    /// `--skip-networking`, so a socket probe would report ready while
    /// the real server is not listening yet — the same lesson as
    /// [`Ready::RespPing`]'s (card NB-3 Work 2).
    MysqlPing { port: u16 },
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
    /// Re-create the dataset at the start of every config point, so each
    /// series is measured from the same known state (§5.2 fairness: no
    /// order-dependent drift between durability variants or client
    /// counts). False for rows whose dataset is immutable (vector) or
    /// private to each run (sqlite).
    pub reinit_per_series: bool,
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

fn sweep_ycsb(p: &Params) -> Vec<Series> {
    // Card NB-2: the YCSB row crosses the same two dimensions as the
    // sysbench row — workload letters (a–f, the stock core workloads)
    // x thread counts — in the fixed cross order, so the series list
    // is reproducible (18 points at the defaults).
    cross(&[
        ("workload", p.ycsb_workloads.clone()),
        (
            "threads",
            p.ycsb_threads.iter().map(|t| t.to_string()).collect(),
        ),
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

/// The Dragonfly sweep (card NB-3 Work 3): pipeline only, one fixed
/// durability configuration. The pinned Dragonfly release (v1.27.1)
/// exposes no AOF-equivalent fsync flag (verified against its own
/// `--help`, recorded in `systems/dragonfly/README.md`), so there is no
/// durability dimension to sweep and the row's numbers are
/// durability-unmatched against the Redis/Valkey AOF pairs — stated, per
/// PLAN-v3 §5.2 rule 3, never silently relaxed.
fn sweep_kv_fixed(p: &Params) -> Vec<Series> {
    cross(&[(
        "pipeline",
        p.pipelines.iter().map(|v| v.to_string()).collect(),
    )])
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
        init: $init:literal, final: $final:literal, reinit: $reinit:literal
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
            reinit_per_series: $reinit,
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
        sweep_pgbench, variant_pgbench, init: true, final: false, reinit: true
    ),
    row!(
        "oltp-sysbench-pg17", "postgres17", Sysbench,
        "sysbench oltp_read_write + oltp_point_select on PG17, threads 1/16/64",
        ["tps" => HigherIsBetter, "qps" => HigherIsBetter,
         "latency_avg_ms" => LowerIsBetter, "p99_ms" => LowerIsBetter],
        sweep_sysbench, no_variant, init: true, final: false, reinit: true
    ),
    // NB-2 scope note: this row parses driver-side through the
    // NB_METRICS protocol (the `VectorSearch` arm — the harness's one
    // mechanism for driver-written metrics), because the runner's
    // log-parser dispatch table is outside this card's Touch only
    // list (the match there is exhaustive; the scope ruling reverted
    // the one-line arm the first attempt added). The driver extracts
    // the fixed-format `[OVERALL]`/`[READ]` lines that `parse_ycsb` —
    // the fixture-pinned reference parser in parse.rs — reads, fails
    // loudly when the mandatory throughput line is missing, and is
    // itself under test: parse.rs's `driver` module executes the real
    // script over the recorded fixtures and holds its output to the
    // reference parser (verdict NB-2, rework 1).
    row!(
        "oltp-ycsb-pg17", "postgres17", VectorSearch,
        "YCSB core A–F over pgwire (JDBC) on PG17, threads 1/16/64 — the C-9 comparison row, Nucleus speaks pgwire (§3.7)",
        ["ops_sec" => HigherIsBetter, "p99_read_us" => LowerIsBetter,
         "avg_read_us" => LowerIsBetter],
        sweep_ycsb, no_variant, init: true, final: false, reinit: true
    ),
    row!(
        "kv-memtier-valkey8", "valkey8", Memtier,
        "memtier GET/SET 1:10 against Valkey 8, pipeline 1/16, AOF always/everysec",
        ["ops_sec" => HigherIsBetter, "p50_ms" => LowerIsBetter,
         "p99_ms" => LowerIsBetter, "kb_per_sec" => HigherIsBetter],
        sweep_kv, variant_kv, init: true, final: true, reinit: true
    ),
    row!(
        "kv-memtier-redis7", "redis7", Memtier,
        "memtier GET/SET 1:10 against Redis 7, pipeline 1/16, AOF always/everysec",
        ["ops_sec" => HigherIsBetter, "p50_ms" => LowerIsBetter,
         "p99_ms" => LowerIsBetter, "kb_per_sec" => HigherIsBetter],
        sweep_kv, variant_kv, init: true, final: true, reinit: true
    ),
    row!(
        "vector-sift1m-qdrant", "qdrant", VectorSearch,
        "ann-benchmarks SIFT1M (HNSW m=16, ef_construction=200), recall@10 vs QPS",
        ["qps" => HigherIsBetter, "recall_at_10" => HigherIsBetter,
         "p50_ms" => LowerIsBetter, "p99_ms" => LowerIsBetter],
        sweep_ef, no_variant, init: true, final: true, reinit: false
    ),
    row!(
        "vector-sift1m-pgvector", "pgvector17", VectorSearch,
        "ann-benchmarks SIFT1M via pgvector HNSW (m=16, ef_construction=200), recall@10 vs QPS",
        ["qps" => HigherIsBetter, "recall_at_10" => HigherIsBetter],
        sweep_ef, no_variant, init: true, final: true, reinit: false
    ),
    row!(
        "embedded-sqlite-speedtest1", "sqlite", Speedtest1,
        "SQLite speedtest1 from the amalgamation (in container)",
        ["wall_s" => LowerIsBetter, "user_s" => LowerIsBetter, "sys_s" => LowerIsBetter],
        sweep_single, no_variant, init: true, final: true, reinit: false
    ),
    row!(
        "oltp-sysbench-mysql84", "mysql84", Sysbench,
        "sysbench oltp_read_write + oltp_point_select on MySQL 8.4 (InnoDB), threads 1/16/64",
        ["tps" => HigherIsBetter, "qps" => HigherIsBetter,
         "latency_avg_ms" => LowerIsBetter, "p99_ms" => LowerIsBetter],
        sweep_sysbench, no_variant, init: true, final: false, reinit: true
    ),
    row!(
        "kv-memtier-dragonfly", "dragonfly", Memtier,
        "memtier GET/SET 1:10 against Dragonfly, pipeline 1/16 — durability-unmatched: \
         the pinned v1.27.1 release has no AOF-equivalent fsync flag (snapshots only; \
         systems/dragonfly/README.md), so this number must not be read against the \
         AOF-matched Redis/Valkey pairs (PLAN-v3 §5.2 rule 3)",
        ["ops_sec" => HigherIsBetter, "p50_ms" => LowerIsBetter,
         "p99_ms" => LowerIsBetter, "kb_per_sec" => HigherIsBetter],
        sweep_kv_fixed, no_variant, init: true, final: true, reinit: true
    ),
    row!(
        "embedded-duckdb-tpch1", "duckdb", DuckdbTpch,
        "DuckDB 2.0.1 CLI, embedded TPC-H SF1: dbgen data persisted to a file, 22 queries",
        ["wall_s" => LowerIsBetter, "user_s" => LowerIsBetter,
         "sys_s" => LowerIsBetter, "rss_kb" => LowerIsBetter],
        sweep_single, no_variant, init: true, final: true, reinit: false
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
        ready: Ready::PgIsready {
            db: "nbench",
            port: 5432,
        },
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
        ready: Ready::PgIsready {
            db: "nbench",
            port: 5433,
        },
        run_extra: &[
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "-e",
            "POSTGRES_DB=nbench",
            // Parallel HNSW index builds coordinate through POSIX shared
            // memory, which podman caps at 64MB by default (the postgres
            // image docs tell heavy-shared-memory workloads to raise
            // --shm-size). Without it the 2026-10-18 init failed with
            // "could not resize shared memory segment ... to 2144374272
            // bytes: No space left on device". 8g covers the 2GB
            // maintenance_work_mem build with headroom; it is container
            // plumbing, not engine tuning.
            "--shm-size",
            "8g",
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
        ready: Ready::RespPing {
            cli: "valkey-cli",
            port: 6379,
        },
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
        ready: Ready::RespPing {
            cli: "redis-cli",
            port: 6380,
        },
        run_extra: &[],
    },
    SystemDef {
        name: "qdrant",
        kind: SystemKind::Server,
        volume: "nbench-qdrant-storage",
        mount: "/qdrant/storage",
        mem_limit: "24g",
        cpus: "16",
        // No server flags: the container runs stock Qdrant; HNSW parameters are
        // set per collection at load time. Static-load rows (`vec-qdrant-load`)
        // also PATCH the collection to `indexing_threshold = 1` KB
        // (`qdrant_index_tail`; judgement J3, card B-0-F7-F1) — an unindexed
        // tail made latency non-deterministic, so it is always HNSW-indexed.
        // Not a Qdrant default; a collection built any other way never gets it.
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
    SystemDef {
        name: "mysql84",
        kind: SystemKind::Server,
        volume: "nbench-mysql84-data",
        mount: "/var/lib/mysql",
        mem_limit: "24g",
        cpus: "16",
        // Tuning per MySQL production guidance, durability-matched to the
        // PG row (PLAN-v3 §5.2 rule 3):
        // - innodb_buffer_pool_size ~25% of machine RAM (38 GiB host),
        //   the same guidance the postgres17 row follows;
        // - innodb_flush_log_at_trx_commit=1 is the per-commit flush —
        //   the pair of PG's synchronous_commit=on (rule 3 made checkable
        //   by the provenance test below);
        // - skip-log-bin disables the binary log: replication is not this
        //   row's subject. The card named `--log_bin=0`, which MySQL 8.4
        //   silently ignores (verified 2026-11-27: started with
        //   `--log_bin=0`, @@log_bin stayed 1); `--skip-log-bin` is the
        //   documented spelling and actually sets @@log_bin=0.
        base_args: &[
            "--port=3307",
            "--innodb_buffer_pool_size=6GB",
            "--innodb_flush_log_at_trx_commit=1",
            "--skip-log-bin",
        ],
        // No variants: the row is durability-matched through the base
        // args (per-commit flush), mirroring oltp-sysbench-pg17's shape
        // (no off-variant; the pairing is exercised by the pgbench row).
        variants: &[],
        ready: Ready::MysqlPing { port: 3307 },
        run_extra: &[
            "-e",
            "MYSQL_ROOT_PASSWORD=nbench",
            "-e",
            "MYSQL_DATABASE=nbench",
        ],
    },
    SystemDef {
        name: "dragonfly",
        kind: SystemKind::Server,
        volume: "nbench-dragonfly-data",
        mount: "/data",
        mem_limit: "12g",
        cpus: "16",
        // Base args per the image's documented run shape (the entrypoint
        // itself prepends `dragonfly --logtostderr`; stating it here
        // keeps the provenance explicit) and the same CPU budget the
        // other KV systems get (16 proactor threads on 16 --cpus).
        // No AOF-style durability variants exist in the pinned release
        // (v1.27.1, verified against the image's own --help: snapshots
        // only — `snapshot_cron`/`dbfilename`/`dir`, no fsync control),
        // so `variants` is empty and the row is durability-unmatched
        // against the AOF pairs (PLAN-v3 §5.2 rule 3, stated in the row
        // description the report prints).
        base_args: &["--logtostderr", "--port=6382", "--proactor_threads=16"],
        variants: &[],
        // RESP-compatible server; the image ships redis-cli 6.0.16
        // (Ubuntu redis-tools), so the in-container probe and digest use
        // it exactly like the redis7 row (card NB-3 Work 3).
        ready: Ready::RespPing {
            cli: "redis-cli",
            port: 6382,
        },
        run_extra: &[],
    },
    SystemDef {
        name: "duckdb",
        kind: SystemKind::Transient,
        volume: "nbench-duckdb-artifacts",
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
            "mysql84" => 3307,
            "dragonfly" => 6382,
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
///
/// Per config point: restart into the durability variant, re-create the
/// dataset (`reinit`) when the row asks for it, digest-check that fresh
/// state, then one cold run, warmup runs and `>= 5` warm runs. The
/// reinit digest uses the same expectation as the init digest: a
/// deterministic dataset reproduces it, and a mismatch voids the run.
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
    // The init digest is always recorded; strictness only controls
    // whether a mismatch voids the run (handled by the runner).
    steps.push(Step::Digest {
        phase: "init",
        strict: row.strict_init_digest,
    });
    for series in (row.sweep)(p) {
        let variant = (row.variant_of)(&series).map(|v| v.to_string());
        steps.push(Step::Start { variant });
        if row.reinit_per_series {
            steps.push(Step::Exec {
                mode: "reinit",
                series: series.clone(),
                index: 0,
            });
            steps.push(Step::Digest {
                phase: "init",
                strict: row.strict_init_digest,
            });
        }
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
            // Every cold execution must run against a freshly started
            // container: the step right before it is a Start (rows
            // without reinit) or the digest of a reinit that itself
            // directly follows a Start.
            for (i, s) in steps.iter().enumerate() {
                if let Step::Exec { mode: "cold", .. } = s {
                    let prev_ok = match steps.get(i.wrapping_sub(1)) {
                        Some(Step::Start { .. }) => true,
                        Some(Step::Digest { .. }) => {
                            // find the preceding reinit and check it follows a Start
                            let mut j = i;
                            let mut ok = false;
                            while j > 0 {
                                j -= 1;
                                match &steps[j] {
                                    Step::Exec { mode: "reinit", .. } => {
                                        ok = matches!(
                                            steps.get(j.wrapping_sub(1)),
                                            Some(Step::Start { .. })
                                        );
                                        break;
                                    }
                                    Step::Start { .. } => break,
                                    _ => {}
                                }
                            }
                            ok
                        }
                        _ => false,
                    };
                    assert!(prev_ok, "{}: cold run not on a fresh container", row.id);
                }
            }
            // Reinit rows re-create the dataset before every cold run and
            // digest-check it; immutable-dataset rows never reinit.
            let reinits = steps
                .iter()
                .filter(|s| matches!(s, Step::Exec { mode: "reinit", .. }))
                .count();
            let colds = steps
                .iter()
                .filter(|s| matches!(s, Step::Exec { mode: "cold", .. }))
                .count();
            if row.reinit_per_series {
                assert_eq!(
                    reinits,
                    (row.sweep)(&p).len(),
                    "{}: missing reinits",
                    row.id
                );
            } else {
                assert_eq!(reinits, 0, "{}: unexpected reinits", row.id);
                let _ = colds;
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
    fn sysbench_and_ycsb_rows_fix_synchronous_commit_by_container_default() {
        // The sysbench row's README states the rule; the YCSB row
        // (card NB-2) inherits it: `variant_of` is `no_variant`, so the
        // server always starts with the image default
        // (synchronous_commit=on) — the durability pairing is owned by
        // the pgbench row, and provenance records the fixed setting.
        let p = Params::default();
        for id in ["oltp-sysbench-pg17", "oltp-ycsb-pg17"] {
            let row = RowDef::find(id).expect("row");
            for s in (row.sweep)(&p) {
                assert!((row.variant_of)(&s).is_none(), "{id}: unexpected variant");
            }
            let starts: Vec<_> = plan(row, &p)
                .iter()
                .filter_map(|s| match s {
                    Step::Start { variant } => Some(variant.clone()),
                    _ => None,
                })
                .collect();
            assert!(
                starts.iter().all(|v| v.is_none()),
                "{id}: a variant leaked into a start step"
            );
        }
    }

    #[test]
    fn ycsb_plan_crosses_workloads_with_threads_and_reinits_each_series() {
        // Card NB-2 mutants SWEEP-NOT-CROSS and REINIT-DROPPED: the
        // sweep is the full cross product (6 workload letters x 3 thread
        // counts = 18 series) in the fixed cross order — zipping instead
        // of crossing, or dropping the thread dimension, fails the exact
        // order list below — and every series gets its own reinit, so
        // series 2+ never inherit the previous series' mutated table
        // (YCSB load generates fresh random field values; the digest
        // pins the row-count shape, the reinit restores it).
        let p = Params::default();
        let row = RowDef::find("oltp-ycsb-pg17").expect("row");
        let series = (row.sweep)(&p);
        assert_eq!(series.len(), 18);
        let expected: Vec<Series> = ["a", "b", "c", "d", "e", "f"]
            .iter()
            .flat_map(|w| {
                [1u64, 16, 64].iter().map(move |t| {
                    vec![
                        kv_pair("workload", w.to_string()),
                        kv_pair("threads", t.to_string()),
                    ]
                })
            })
            .collect();
        assert_eq!(series, expected, "series order must be the fixed cross");
        let steps = plan(row, &p);
        let reinits = steps
            .iter()
            .filter(|s| matches!(s, Step::Exec { mode: "reinit", .. }))
            .count();
        assert_eq!(reinits, 18, "one reinit per series");
        // Overrides narrow the cross the same way as every other row.
        let tiny = Params::with_overrides(&[
            ("ycsb_workloads", "a"),
            ("ycsb_threads", "1"),
            ("runs", "1"),
        ])
        .expect("overrides");
        assert_eq!((row.sweep)(&tiny).len(), 1);
        assert_eq!(
            plan(row, &tiny)
                .iter()
                .filter(|s| matches!(s, Step::Exec { mode: "reinit", .. }))
                .count(),
            1
        );
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

    #[test]
    fn resp_probes_carry_their_configured_port() {
        // Regression (card B-0 bring-up): the redis7 readiness probe once
        // ran `redis-cli ping` against the default port 6379 while the
        // server was configured for 6380, so the row never became ready.
        // A RESP system's probe port must equal the port in its base args
        // (its SystemDef::port()).
        for s in SYSTEMS {
            if let Ready::RespPing { port, .. } = s.ready {
                assert_eq!(
                    port,
                    s.port(),
                    "{}: RESP probe port {} != server port {}",
                    s.name,
                    port,
                    s.port()
                );
            }
        }
    }

    #[test]
    fn mysql_probes_carry_their_configured_port() {
        // The MysqlPing lesson (card NB-3, RespPing's): the probe must
        // carry the server's port. MYSQL-PROBE-PORTLESS (a probe that
        // omits the port) pings the wrong or no server; the SystemDef's
        // probe port must equal the configured server port.
        for s in SYSTEMS {
            if let Ready::MysqlPing { port } = s.ready {
                assert_eq!(
                    port,
                    s.port(),
                    "{}: mysql probe port {} != server port {}",
                    s.name,
                    port,
                    s.port()
                );
            }
        }
        let mysql = SystemDef::find("mysql84").expect("mysql84 system");
        assert_eq!(mysql.port(), 3307);
        assert!(matches!(mysql.ready, Ready::MysqlPing { port: 3307 }));
    }

    #[test]
    fn mysql84_base_args_carry_the_per_commit_flush() {
        // MYSQL-DURABILITY-SOFT (base args using
        // --innodb_flush_log_at_trx_commit=2) must fail here: rule 3's
        // durability match to the PG row (synchronous_commit=on, a
        // per-commit flush) is made checkable by pinning the flush
        // setting — and the two settings that shape the row's provenance
        // with it.
        let mysql = SystemDef::find("mysql84").expect("mysql84 system");
        assert!(
            mysql
                .base_args
                .contains(&"--innodb_flush_log_at_trx_commit=1"),
            "mysql84 must run the per-commit flush (§5.2 rule 3): {:?}",
            mysql.base_args
        );
        assert!(
            !mysql.base_args.iter().any(|a| a.contains("trx_commit=2")),
            "mysql84 must not soften the flush: {:?}",
            mysql.base_args
        );
        // no binlog (replication is not this row's subject)
        assert!(
            mysql.base_args.contains(&"--skip-log-bin"),
            "mysql84 must disable the binary log: {:?}",
            mysql.base_args
        );
        // the buffer pool follows the same ~25%-of-RAM guidance as the
        // PG row's shared_buffers
        assert!(
            mysql.base_args.contains(&"--innodb_buffer_pool_size=6GB"),
            "mysql84 buffer pool must follow the PG row's guidance: {:?}",
            mysql.base_args
        );
        // the server port is set by the container args, not the image
        // default (3306), so the row coexists on the host network
        assert!(
            mysql.base_args.contains(&"--port=3307"),
            "mysql84 must listen on its configured port: {:?}",
            mysql.base_args
        );
        // the durability match is in the base args, so no off variant
        // exists to pair (mirrors oltp-sysbench-pg17's shape)
        assert!(mysql.variants.is_empty());
    }

    #[test]
    fn dragonfly_row_is_durability_unmatched_and_says_so() {
        // PLAN-v3 §5.2 rule 3 is never silently relaxed: the pinned
        // Dragonfly release (v1.27.1) has no AOF-equivalent fsync flag
        // (verified against the image's own --help; snapshots only), so
        // the row runs with a fixed durability configuration and its
        // published line must carry the unmatched note verbatim.
        let df = SystemDef::find("dragonfly").expect("dragonfly system");
        assert!(
            df.variants.is_empty(),
            "dragonfly must not invent durability variants: {:?}",
            df.variants
        );
        let row = RowDef::find("kv-memtier-dragonfly").expect("row");
        assert!(
            row.description.contains("durability-unmatched"),
            "the row's report line must state the unmatched durability: {}",
            row.description
        );
        // the sweep is pipeline-only: no durability dimension exists
        let p = Params::default();
        for s in (row.sweep)(&p) {
            assert!(
                !s.iter().any(|(k, _)| k == "appendfsync"),
                "dragonfly sweep must not carry a durability dimension: {s:?}"
            );
            assert!(s.iter().any(|(k, _)| k == "pipeline"));
        }
        // one fixed configuration per pipeline depth
        assert_eq!((row.sweep)(&p).len(), p.pipelines.len());
    }

    #[test]
    fn sweep_kv_fixed_ignores_the_fsync_dimension() {
        // sweep_kv_fixed (the new fn card NB-3 names): the sweep keeps
        // the pipeline dimension of sweep_kv and drops the fsync
        // dimension entirely — a fixed durability configuration.
        let p = Params::with_overrides(&[("pipelines", "4,8")]).expect("overrides");
        let series = sweep_kv_fixed(&p);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0], vec![("pipeline".to_string(), "4".to_string())]);
        assert_eq!(series[1], vec![("pipeline".to_string(), "8".to_string())]);
    }

    #[test]
    fn embedded_duckdb_row_mirrors_the_sqlite_shape() {
        // The embedded row (PLAN-v3 §5.1): a transient system, one config
        // point, a dataset created at init and digested at init and
        // final, no per-series reinit (the database file persists on the
        // volume across the row's runs).
        let row = RowDef::find("embedded-duckdb-tpch1").expect("row");
        let sys = SystemDef::find(row.system).expect("system");
        assert_eq!(sys.kind, SystemKind::Transient);
        assert!(matches!(row.parser, Parser::DuckdbTpch));
        assert!(row.strict_init_digest && row.strict_final_digest);
        assert!(!row.reinit_per_series);
        let p = Params::default();
        assert_eq!((row.sweep)(&p).len(), 1);
        // the four §5.1 embedded metrics, efficiency included (rss_kb)
        let names: Vec<&str> = row.metrics.iter().map(|(m, _)| *m).collect();
        assert_eq!(names, vec!["wall_s", "user_s", "sys_s", "rss_kb"]);
    }

    #[test]
    fn mysql84_row_mirrors_the_pg17_sysbench_row() {
        // The row mirrors oltp-sysbench-pg17 cell for cell (card NB-3
        // Work 2): the same sweep, no variants, the same digest
        // discipline and the same metrics.
        let pg = RowDef::find("oltp-sysbench-pg17").expect("pg row");
        let my = RowDef::find("oltp-sysbench-mysql84").expect("mysql row");
        assert_eq!(my.system, "mysql84");
        assert!(matches!(my.parser, Parser::Sysbench));
        assert_eq!(my.metrics, pg.metrics);
        assert_eq!(my.strict_init_digest, pg.strict_init_digest);
        assert_eq!(my.strict_final_digest, pg.strict_final_digest);
        assert_eq!(my.reinit_per_series, pg.reinit_per_series);
        let p = Params::default();
        assert_eq!((my.sweep)(&p), (pg.sweep)(&p));
    }
}
