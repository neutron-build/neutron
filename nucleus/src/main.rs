// Custom global allocator selection. `mimalloc-allocator` is the default
// (universal, builds clean on macOS / Linux / Windows) shipped in the
// `server` feature. `jemalloc` is available as opt-in for Linux production
// tuning. Pick exactly one — the cfg gates are mutually exclusive: if
// `mimalloc-allocator` is on, jemalloc is suppressed even if also enabled.
#[cfg(feature = "mimalloc-allocator")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(feature = "jemalloc", not(feature = "mimalloc-allocator")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use nucleus::catalog::Catalog;
use nucleus::config::NucleusConfig;
use nucleus::executor::Executor;
use nucleus::metrics::MetricsRegistry;
use nucleus::pool::PoolConfig as SyncPoolConfig;
use nucleus::pool::async_pool::AsyncConnectionPool;
use nucleus::runtime::{ConnectionRouter, CoreConfig, NucleusRuntime};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::{DiskEngine, MvccStorageAdapter, StorageEngine, wal::SyncMode};
use nucleus::tls;
use nucleus::transport::TcpTransport;
use nucleus::wire::{AuthMethod, NucleusHandler, NucleusServer};

// ============================================================================
// CLI definition
// ============================================================================

/// Nucleus -- The Definitive Database
///
/// One database engine that replaces every data system a modern application needs.
/// Embedded or server mode. Single file or sharded cluster. Postgres wire protocol.
#[derive(Parser)]
#[command(name = "nucleus", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Start the Nucleus database server.
    ///
    /// Without flags: standalone server mode.
    /// With --join: cluster mode, joins an existing cluster.
    Start {
        /// Port to listen on (default: 5432).
        #[arg(short, long, default_value_t = 5432)]
        port: u16,

        /// Bind address (default: 127.0.0.1). Use 0.0.0.0 for all interfaces.
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Data directory for persistent storage.
        #[arg(short, long, default_value = "nucleus_data")]
        data: PathBuf,

        /// Path to a nucleus.toml config file. Defaults to <data-dir>/nucleus.toml.
        /// Use this to keep configuration outside the data directory, or to point
        /// one binary at different configs. A path given here must exist —
        /// startup fails rather than silently falling back to defaults, because
        /// a config that is silently ignored is worse than one that is absent.
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,

        /// Use in-memory storage (no persistence). Useful for testing.
        #[arg(long)]
        memory: bool,

        /// Join an existing cluster at this address (e.g., node1:5432).
        #[arg(long)]
        join: Option<String>,

        /// Region tag for geo-distributed deployments (e.g., us-east, eu-west).
        #[arg(long)]
        region: Option<String>,

        /// Cluster communication port (default: 5433). Used for inter-node Raft messaging.
        #[arg(long, default_value_t = 5433)]
        cluster_port: u16,

        /// Replicate from a primary node at this address (e.g., 192.168.1.1:5434).
        /// When specified, the server starts in replica mode and connects to the
        /// primary's replication port to receive WAL records via streaming replication.
        #[arg(long)]
        replicate_from: Option<String>,

        /// Port for the replication server (default: 5434). The primary listens on
        /// this port for incoming replica connections. Ignored in replica mode.
        #[arg(long, default_value_t = 5434)]
        replication_port: u16,

        /// Require password authentication. Can also be set via NUCLEUS_PASSWORD env var.
        #[arg(long)]
        password: Option<String>,

        /// SQL authentication method when password auth is enabled.
        /// Defaults to SCRAM-SHA-256. Use cleartext only for legacy clients.
        #[arg(long, value_enum)]
        auth_method: Option<CliAuthMethod>,

        /// Disable TLS (connections will be unencrypted).
        #[arg(long)]
        no_tls: bool,

        /// Path to TLS certificate file (PEM).
        #[arg(long)]
        tls_cert: Option<PathBuf>,

        /// Path to TLS private key file (PEM).
        #[arg(long)]
        tls_key: Option<PathBuf>,

        /// Path to trusted client CA bundle (PEM) to require mTLS client certs.
        /// Can also be set via NUCLEUS_TLS_CLIENT_CA.
        #[arg(long)]
        tls_client_ca: Option<PathBuf>,

        /// Enable page-level encryption at rest (AES-256-GCM).
        /// Requires NUCLEUS_ENCRYPT_KEY env var (hex-encoded 32-byte key) or
        /// NUCLEUS_ENCRYPT_PASSPHRASE env var (passphrase for Argon2 key derivation).
        #[arg(long)]
        encrypt: bool,

        /// Enable page-level LZ4 compression for on-disk pages.
        #[arg(long)]
        compress: bool,

        /// Port for the RESP2 (Redis protocol) server (default: 6379).
        /// Set to 0 to disable the RESP server.
        #[arg(long, default_value_t = 6379)]
        resp_port: u16,

        /// Port for the S3-compatible gateway (default: 0 = disabled).
        /// Requires NUCLEUS_S3_ACCESS_KEY and NUCLEUS_S3_SECRET_KEY.
        #[arg(long, default_value_t = 0)]
        s3_port: u16,

        /// OpenTelemetry OTLP endpoint for distributed tracing.
        /// Example: http://localhost:4317 (gRPC) or http://localhost:4318 (HTTP).
        /// Requires the 'otel' feature to be enabled at compile time.
        #[arg(long)]
        otlp_endpoint: Option<String>,

        /// Maximum memory usage in MB. All subsystems (buffer pool, cache, KV,
        /// FTS, columnar) share this budget. Default: 512 MB.
        #[arg(long, default_value_t = 512)]
        max_memory: usize,
    },

    /// Initialize a new Nucleus data directory.
    Init {
        /// Data directory to initialize.
        #[arg(short, long, default_value = "nucleus_data")]
        data: PathBuf,
    },

    /// Show Nucleus server version and build info.
    Version,

    /// Show status of a running Nucleus instance.
    Status {
        /// Host to query (default: 127.0.0.1:5432).
        #[arg(short = 'H', long, default_value = "127.0.0.1:5432")]
        host: String,
    },

    /// Interactive SQL shell (psql-like REPL).
    Shell {
        /// Host to connect to.
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,

        /// Port to connect to.
        #[arg(short, long, default_value_t = 5432)]
        port: u16,

        /// Execute a single SQL statement and exit (non-interactive).
        /// Exits 1 if the statement errors. Enables scripted access, e.g.
        /// `docker exec <c> nucleus shell -c "SELECT KV_GET('k')"`.
        #[arg(short = 'c', long)]
        command: Option<String>,

        /// With -c: print SELECT results as a JSON array of objects
        /// (machine-readable) instead of an ASCII table.
        #[arg(long)]
        json: bool,
    },

    /// Back up a data directory to a portable snapshot (physical).
    ///
    /// Refuses to run against a data directory a live instance holds: a plain
    /// copy of a database being written to is torn, and a torn snapshot that
    /// reports success is worse than no snapshot. Restore requires a build on
    /// the same on-disk format version.
    Backup {
        /// Data directory to back up.
        #[arg(short, long, default_value = "nucleus_data")]
        data: PathBuf,

        /// Destination snapshot directory.
        #[arg(short, long)]
        output: PathBuf,

        /// Overwrite the destination if it already exists.
        #[arg(long)]
        force: bool,

        /// Take a coordinated snapshot (checkpoint + WAL-pinned copy cut at a
        /// named LSN) by opening the database. Plaintext, uncompressed data
        /// files only — a plain copy needs no key, this does.
        #[arg(long)]
        online: bool,

        /// Copy a data directory that a live instance holds. The result may be
        /// TORN; it is recorded as inconsistent in the snapshot's manifest.
        #[arg(long)]
        allow_in_use: bool,
    },

    /// Restore a data directory from a snapshot created by `backup`.
    Restore {
        /// Snapshot directory produced by `nucleus backup`.
        #[arg(short, long)]
        input: PathBuf,

        /// Data directory to restore into. Must be empty unless --force.
        #[arg(short, long, default_value = "nucleus_data")]
        data: PathBuf,

        /// Overwrite the data directory if it already exists.
        #[arg(long)]
        force: bool,
    },

    /// Logical (portable SQL) dump of every table — survives format/schema
    /// changes and replays through the constraint-safe executor, unlike the
    /// physical `backup`. Take it against a stopped or quiesced instance.
    Dump {
        /// Data directory to dump from.
        #[arg(short, long, default_value = "nucleus_data")]
        data: PathBuf,

        /// Output .sql file (writes to stdout if omitted).
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Restore a logical dump produced by `nucleus dump` into a data directory
    /// (creates it if missing) by replaying the SQL through the executor.
    Load {
        /// The .sql dump file to replay.
        #[arg(short, long)]
        input: PathBuf,

        /// Data directory to restore into.
        #[arg(short, long, default_value = "nucleus_data")]
        data: PathBuf,
    },

    /// Point-in-time recovery: restore a physical base snapshot and replay the
    /// archived WAL (see `NUCLEUS_WAL_ARCHIVE_DIR`) forward to a target LSN,
    /// wall-clock time, or the latest archived point. Segmented-WAL databases
    /// only.
    RestorePitr {
        /// Physical base snapshot directory (from `nucleus backup`).
        #[arg(short, long)]
        base: PathBuf,

        /// WAL archive directory (the per-database subdirectory of
        /// `NUCLEUS_WAL_ARCHIVE_DIR`).
        #[arg(short, long)]
        archive: PathBuf,

        /// Data directory to restore into. Must be empty unless --force.
        #[arg(short, long, default_value = "nucleus_data")]
        data: PathBuf,

        /// Primary data file name inside the data dir.
        #[arg(long, default_value = "nucleus.db")]
        db_file: String,

        /// Replay through this exact LSN (inclusive). Mutually exclusive with
        /// --time; if neither is given, replays to the latest archived point.
        #[arg(long)]
        lsn: Option<u64>,

        /// Replay through the last segment archived at or before this Unix time
        /// (seconds). Segment granularity.
        #[arg(long)]
        time: Option<u64>,

        /// Overwrite the data directory if it already exists.
        #[arg(long)]
        force: bool,
    },

    /// Delete archived WAL segments that lie entirely below an LSN.
    ///
    /// Continuous archiving never removed anything, so an archive grows until
    /// the disk does not. This is the retention half, and it is deliberately
    /// manual: there is no policy and no timer, because deleting recovery data
    /// on a schedule -- with no knowledge of which base snapshots still exist
    /// -- trades a disk-space problem for an unrecoverable one. Pick the
    /// horizon from the `consistent_lsn` of the oldest base snapshot you still
    /// intend to restore from, and run `--dry-run` first.
    PruneArchive {
        /// WAL archive directory (the per-database subdirectory of
        /// `NUCLEUS_WAL_ARCHIVE_DIR`).
        #[arg(short, long)]
        archive: PathBuf,

        /// Keep every segment that can serve a restore to this LSN or later.
        /// A segment is removed only when ALL of its records are below it.
        #[arg(long)]
        keep_from_lsn: u64,

        /// Report what would be deleted and delete nothing.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum CliAuthMethod {
    ScramSha256,
    Cleartext,
}

impl CliAuthMethod {
    fn to_wire(self) -> AuthMethod {
        match self {
            Self::ScramSha256 => AuthMethod::ScramSha256,
            Self::Cleartext => AuthMethod::Cleartext,
        }
    }
}

// ============================================================================
// StartConfig — groups all `cmd_start` parameters
// ============================================================================

struct StartConfig {
    port: u16,
    host: String,
    data: PathBuf,
    config: Option<PathBuf>,
    memory: bool,
    join: Option<String>,
    region: Option<String>,
    replicate_from: Option<String>,
    replication_port: u16,
    password: Option<String>,
    auth_method: Option<CliAuthMethod>,
    no_tls: bool,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_client_ca: Option<PathBuf>,
    cluster_port: u16,
    encrypt: bool,
    compress: bool,
    resp_port: u16,
    s3_port: u16,
    otlp_endpoint: Option<String>,
    max_memory: usize,
}

// ============================================================================
// Entry point
// ============================================================================

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Start {
            port,
            host,
            data,
            config,
            memory,
            join,
            region,
            replicate_from,
            replication_port,
            password,
            auth_method,
            no_tls,
            tls_cert,
            tls_key,
            tls_client_ca,
            cluster_port,
            encrypt,
            compress,
            resp_port,
            s3_port,
            otlp_endpoint,
            max_memory,
        }) => {
            cmd_start(StartConfig {
                port,
                host,
                data,
                config,
                memory,
                join,
                region,
                replicate_from,
                replication_port,
                password,
                auth_method,
                no_tls,
                tls_cert,
                tls_key,
                tls_client_ca,
                cluster_port,
                encrypt,
                compress,
                resp_port,
                s3_port,
                otlp_endpoint,
                max_memory,
            })
            .await;
        }
        Some(Commands::Init { data }) => {
            cmd_init(data);
        }
        Some(Commands::Version) => {
            cmd_version();
        }
        Some(Commands::Status { host }) => {
            cmd_status(&host).await;
        }
        Some(Commands::Shell {
            host,
            port,
            command,
            json,
        }) => {
            if let Some(sql) = command {
                cmd_shell_exec(&host, port, &sql, json).await;
            } else {
                cmd_shell(&host, port).await;
            }
        }
        Some(Commands::Backup {
            data,
            output,
            force,
            online,
            allow_in_use,
        }) => {
            cmd_backup(data, output, force, online, allow_in_use);
        }
        Some(Commands::Restore { input, data, force }) => {
            cmd_restore(input, data, force);
        }
        Some(Commands::Dump { data, output }) => {
            cmd_dump(data, output).await;
        }
        Some(Commands::Load { input, data }) => {
            cmd_load(input, data).await;
        }
        Some(Commands::RestorePitr {
            base,
            archive,
            data,
            db_file,
            lsn,
            time,
            force,
        }) => {
            cmd_restore_pitr(base, archive, data, db_file, lsn, time, force);
        }
        Some(Commands::PruneArchive {
            archive,
            keep_from_lsn,
            dry_run,
        }) => {
            cmd_prune_archive(archive, keep_from_lsn, dry_run);
        }
        None => {
            // Default: start in server mode (same as `nucleus start`)
            cmd_start(StartConfig {
                port: 5432,
                host: "127.0.0.1".into(),
                data: PathBuf::from("nucleus_data"),
                config: None,
                memory: false,
                join: None,
                region: None,
                replicate_from: None,
                replication_port: 5434,
                password: None,
                auth_method: None,
                no_tls: false,
                tls_cert: None,
                tls_key: None,
                tls_client_ca: None,
                cluster_port: 5433,
                encrypt: false,
                compress: false,
                resp_port: 6379,
                s3_port: 0,
                otlp_endpoint: None,
                max_memory: 512,
            })
            .await;
        }
    }
}

// ============================================================================
// Commands
// ============================================================================

async fn cmd_start(cfg: StartConfig) {
    let StartConfig {
        port,
        host,
        data,
        memory,
        join,
        region,
        replicate_from,
        replication_port,
        password,
        auth_method,
        no_tls,
        tls_cert,
        tls_key,
        tls_client_ca,
        cluster_port,
        encrypt,
        compress,
        resp_port,
        s3_port,
        otlp_endpoint,
        max_memory,
        config,
    } = cfg;
    // Load config early so we can use logging.level for tracing.
    //
    // An explicit --config is REQUIRED to load: if the operator named a file,
    // silently falling back to defaults would apply a configuration they did
    // not ask for, and the symptom (a setting that "does nothing") is the
    // hardest kind to diagnose. The implicit <data>/nucleus.toml stays
    // best-effort, since not having one is the normal case.
    let explicit_config = config.is_some();
    let config_path = config.unwrap_or_else(|| data.join("nucleus.toml"));
    let mut config = match NucleusConfig::load(&config_path) {
        Ok(cfg) => {
            // `NucleusConfig::load` already overlays NUCLEUS_* env vars.
            eprintln!("Loaded config from {}", config_path.display());
            cfg
        }
        Err(e) if explicit_config => {
            eprintln!(
                "error: could not load config file {}: {e}",
                config_path.display()
            );
            std::process::exit(1);
        }
        Err(_) => {
            let mut cfg = NucleusConfig::default();
            cfg.apply_env_overrides();
            cfg
        }
    };

    // CLI args override TOML + env values.
    // Treat clap defaults as "unspecified" so config files/env can still drive runtime.
    let default_host = "127.0.0.1";
    let default_port = 5432;
    let default_data_dir = PathBuf::from("nucleus_data");
    let data_override = if data != default_data_dir {
        Some(data.to_string_lossy().to_string())
    } else {
        None
    };
    config.merge_cli_args(
        if host != default_host {
            Some(&host)
        } else {
            None
        },
        if port != default_port {
            Some(port)
        } else {
            None
        },
        data_override.as_deref(),
        if memory { Some(true) } else { None },
        // Like host/port/data above, treat the clap default as "unspecified"
        // so NUCLEUS_MAX_MEMORY_MB / nucleus.toml can drive the budget when
        // --max-memory isn't explicitly passed.
        if max_memory != 512 {
            Some(max_memory)
        } else {
            None
        },
    );

    // No explicit budget anywhere? Inside a container, size to the cgroup
    // limit instead of the 512 MB default (finding #33: the default budget
    // silently rejected writes in an 8 GB container).
    let cgroup_derived_mb = config.apply_cgroup_memory_default();

    // Derive subsystem budgets from the global memory limit
    config.apply_memory_budget();

    // Refuse to start on a configuration that cannot do what it says. Doing
    // this before the listener binds means an inverted watermark or a typo'd
    // enum surfaces immediately, not the first time the safety net was
    // supposed to catch something.
    if let Err(problems) = config.validate() {
        eprintln!("Refusing to start: invalid configuration");
        for p in &problems {
            eprintln!("  - {p}");
        }
        eprintln!(
            "Fix {} (or the corresponding NUCLEUS_* environment variables) and retry.",
            config_path.display()
        );
        std::process::exit(1);
    }

    // Configure tracing with config-driven log level
    let log_directive = format!("nucleus={}", config.logging.level);

    #[cfg(feature = "otel")]
    {
        if let Some(ref endpoint) = otlp_endpoint {
            // Initialize OpenTelemetry OTLP exporter
            use opentelemetry::trace::TracerProvider;
            use opentelemetry_otlp::{SpanExporter, WithExportConfig};
            use opentelemetry_sdk::trace::SdkTracerProvider;
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;

            let exporter = SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .expect("failed to create OTLP exporter");

            let provider = SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .build();

            let tracer = provider.tracer("nucleus");
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(
                    EnvFilter::from_default_env().add_directive(
                        log_directive
                            .parse()
                            .unwrap_or_else(|_| "nucleus=info".parse().unwrap()),
                    ),
                )
                .with(tracing_subscriber::fmt::layer())
                .with(otel_layer)
                .init();

            eprintln!("OpenTelemetry tracing enabled → {endpoint}");
        } else {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::from_default_env().add_directive(
                        log_directive
                            .parse()
                            .unwrap_or_else(|_| "nucleus=info".parse().unwrap()),
                    ),
                )
                .init();
        }
    }

    #[cfg(not(feature = "otel"))]
    {
        let _ = &otlp_endpoint; // suppress unused warning
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env().add_directive(
                    log_directive
                        .parse()
                        .unwrap_or_else(|_| "nucleus=info".parse().unwrap()),
                ),
            )
            .init();
    }

    // Effective runtime values are sourced from merged config.
    let host = config.server.host.clone();
    let port = config.server.port;
    let data = PathBuf::from(config.storage.data_dir.clone());
    let memory = config.storage.memory_mode;

    // Allow config-driven replica mode when CLI --replicate-from is not supplied.
    let replicate_from = replicate_from.or_else(|| {
        if config.replication.mode.eq_ignore_ascii_case("replica")
            || config.replication.primary_host.is_some()
        {
            config
                .replication
                .primary_host
                .as_ref()
                .map(|primary_host| {
                    if let Some(primary_port) = config.replication.primary_port {
                        format!("{primary_host}:{primary_port}")
                    } else {
                        primary_host.clone()
                    }
                })
        } else {
            None
        }
    });

    let cluster_token = std::env::var("NUCLEUS_CLUSTER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let replication_token = std::env::var("NUCLEUS_REPLICATION_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let allow_insecure_cluster = env_var_truthy("NUCLEUS_ALLOW_INSECURE_CLUSTER");
    let allow_insecure_replication = env_var_truthy("NUCLEUS_ALLOW_INSECURE_REPLICATION");

    if !is_loopback_host(&host) && cluster_token.is_none() && !allow_insecure_cluster {
        tracing::error!(
            "Refusing to start with non-loopback cluster transport and no NUCLEUS_CLUSTER_TOKEN. \
             Set NUCLEUS_CLUSTER_TOKEN or NUCLEUS_ALLOW_INSECURE_CLUSTER=1 for development."
        );
        std::process::exit(1);
    }
    if !is_loopback_host(&host) && replication_token.is_none() && !allow_insecure_replication {
        tracing::error!(
            "Refusing to start with non-loopback replication transport and no NUCLEUS_REPLICATION_TOKEN. \
             Set NUCLEUS_REPLICATION_TOKEN or NUCLEUS_ALLOW_INSECURE_REPLICATION=1 for development."
        );
        std::process::exit(1);
    }

    // Determine deployment mode
    let is_replica = replicate_from.is_some();
    let mode = if is_replica {
        "replica"
    } else if join.is_some() {
        "cluster"
    } else {
        "standalone"
    };

    tracing::info!(
        "Nucleus v{} starting in {mode} mode",
        env!("CARGO_PKG_VERSION")
    );
    tracing::info!(
        "Memory budget: {} MB (buffer pool: {} MB, cache: {} MB){}",
        config.server.max_memory_mb,
        config.storage.buffer_pool_size_mb,
        config.cache.max_memory_mb,
        if cgroup_derived_mb.is_some() {
            " — derived from cgroup limit (set --max-memory / NUCLEUS_MAX_MEMORY_MB to override)"
        } else {
            ""
        },
    );

    if let Some(ref region) = region {
        tracing::info!("Region: {region}");
    }

    tracing::info!("Log level: {}", config.logging.level);

    // Set up storage
    let catalog = Arc::new(Catalog::new());

    // Keep a separate Arc<DiskEngine> for shutdown flushing
    let disk_engine: Option<Arc<DiskEngine>>;
    // At-rest key copy for the query spill manager (external sort), if encrypted.
    let spill_encryptor: Option<nucleus::storage::encryption::PageEncryptor>;

    // Kept so the metrics registry — built after storage — can be attached to
    // the lock table. Lock waits are otherwise invisible.
    let mut buffered_for_metrics: Option<Arc<BufferedDiskEngine>> = None;
    let storage: Arc<dyn StorageEngine> = if memory {
        tracing::info!("Storage: in-memory with MVCC snapshot isolation");
        disk_engine = None;
        spill_encryptor = None;
        Arc::new(MvccStorageAdapter::new())
    } else {
        // Ensure data directory exists
        if !data.exists() {
            if let Err(e) = std::fs::create_dir_all(&data) {
                eprintln!(
                    "{}",
                    nucleus::ops::disk::data_dir_permission_help(&data, &e)
                );
                std::process::exit(1);
            }
            tracing::info!("Created data directory: {}", data.display());
        }

        // Confirm we can write here BEFORE opening anything. Without this the
        // first failure was a panic inside the storage open — exit 101 with no
        // mention of permissions, which an orchestrator turns into a silent
        // restart loop. The common cause is an upgrade from an image that ran
        // as root (v0.1.1 and earlier) to one that runs as uid 10001 (v0.1.2+)
        // over a data directory nothing re-owned.
        if let Err(e) = nucleus::ops::disk::ensure_data_dir_writable(&data) {
            eprintln!(
                "{}",
                nucleus::ops::disk::data_dir_permission_help(&data, &e)
            );
            std::process::exit(1);
        }

        // Announce that this directory is live. `nucleus backup` observes this
        // lock and refuses to take a plain directory copy out from under a
        // running writer — a copy that would look successful and restore into
        // a torn database. Held for the process lifetime; released by the
        // kernel on exit, so a crash never leaves a lock that blocks recovery.
        match nucleus::backup::DataDirLock::acquire(&data) {
            Ok(Some(lock)) => {
                // Deliberately leaked: the lock must outlive every scope in
                // this function and die only with the process.
                std::mem::forget(lock);
            }
            Ok(None) => {
                tracing::warn!(
                    "{} is already locked by another Nucleus instance; continuing, but two \
                     writers on one data directory will corrupt it",
                    data.display()
                );
            }
            Err(e) => {
                tracing::warn!("could not lock data directory {}: {e}", data.display());
            }
        }
        // Give the database a stable identity so a restore can tell this
        // database from a different one.
        let _ = nucleus::backup::database_id(&data);

        // Load persisted catalog (table/index definitions) from previous session
        let catalog_path = data.join("catalog.json");
        let catalog_persistence = CatalogPersistence::new(&catalog_path);
        match catalog_persistence.load_catalog(&catalog).await {
            Ok(()) => {
                let table_count = catalog.table_names().await.len();
                if table_count > 0 {
                    tracing::info!("Restored {table_count} table(s) from catalog");
                }
            }
            Err(e) => {
                tracing::warn!("Failed to load catalog: {e} (starting fresh)");
            }
        }

        let db_path = data.join("nucleus.db");
        // Convert config MB → buffer pool frames (each frame = 16 KB)
        let pool_frames = (config.storage.buffer_pool_size_mb * 1024 * 1024) / 16384;
        let use_segmented_wal = config.wal.segment_size_mb > 0;

        // Derive encryption key from env var if --encrypt is set
        let encryptor = if encrypt {
            use nucleus::storage::encryption::PageEncryptor;
            if let Ok(hex_key) = std::env::var("NUCLEUS_ENCRYPT_KEY") {
                if hex_key.len() != 64 || hex_key.len() % 2 != 0 {
                    tracing::error!(
                        "NUCLEUS_ENCRYPT_KEY must be exactly 64 hex characters (32 bytes)"
                    );
                    std::process::exit(1);
                }
                let mut key_bytes = Vec::with_capacity(32);
                for i in (0..hex_key.len()).step_by(2) {
                    match u8::from_str_radix(&hex_key[i..i + 2], 16) {
                        Ok(b) => key_bytes.push(b),
                        Err(_) => {
                            tracing::error!("NUCLEUS_ENCRYPT_KEY must be valid hex");
                            std::process::exit(1);
                        }
                    }
                }
                let mut key = [0u8; 32];
                key.copy_from_slice(&key_bytes);
                tracing::info!("Encryption: AES-256-GCM (key from env)");
                Some(PageEncryptor::from_key(&key))
            } else if let Ok(passphrase) = std::env::var("NUCLEUS_ENCRYPT_PASSPHRASE") {
                let salt_path = data.join("encrypt.salt");
                let salt = if salt_path.exists() {
                    let bytes = match std::fs::read(&salt_path) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::error!(
                                "failed to read salt file {}: {e}",
                                salt_path.display()
                            );
                            std::process::exit(1);
                        }
                    };
                    if bytes.len() != 16 {
                        tracing::error!(
                            "corrupt encrypt.salt file {} (expected 16 bytes, got {})",
                            salt_path.display(),
                            bytes.len()
                        );
                        std::process::exit(1);
                    }
                    let mut salt = [0u8; 16];
                    salt.copy_from_slice(&bytes);
                    salt
                } else {
                    let salt = PageEncryptor::generate_salt();
                    if let Err(e) = std::fs::write(&salt_path, salt) {
                        tracing::error!("failed to write salt file {}: {e}", salt_path.display());
                        std::process::exit(1);
                    }
                    salt
                };
                tracing::info!("Encryption: AES-256-GCM (passphrase + Argon2)");
                Some(PageEncryptor::from_passphrase(passphrase.as_bytes(), &salt))
            } else {
                tracing::error!(
                    "--encrypt requires NUCLEUS_ENCRYPT_KEY or NUCLEUS_ENCRYPT_PASSPHRASE env var"
                );
                std::process::exit(1);
            }
        } else {
            None
        };

        if compress {
            tracing::info!("Compression: LZ4 page-level compression enabled");
        }

        // Keep a copy of the at-rest key for the query spill manager so blocking
        // operators (external sort) spill ciphertext, not plaintext.
        spill_encryptor = encryptor.clone();
        let engine = Arc::new(match (encryptor, compress, use_segmented_wal) {
            (Some(enc), true, _) => {
                DiskEngine::open_compressed_encrypted(&db_path, catalog.clone(), enc)
                    .expect("failed to open database file")
            }
            (Some(enc), false, _) => DiskEngine::open_encrypted(&db_path, catalog.clone(), enc)
                .expect("failed to open database file"),
            (None, true, _) => DiskEngine::open_compressed(&db_path, catalog.clone())
                .expect("failed to open database file"),
            (None, false, true) => DiskEngine::open_segmented_with_sync(
                &db_path,
                catalog.clone(),
                pool_frames,
                config.wal.segment_size_mb,
                SyncMode::from_str(&config.wal.sync_mode),
            )
            .expect("failed to open database file"),
            (None, false, false) => {
                DiskEngine::open_with_pool_size(&db_path, catalog.clone(), pool_frames)
                    .expect("failed to open database file")
            }
        });
        tracing::info!(
            "Storage: disk ({}) — buffer pool {} MB ({} frames), WAL: {}{}{}",
            db_path.display(),
            config.storage.buffer_pool_size_mb,
            pool_frames,
            if use_segmented_wal {
                format!("segmented ({}MB segments)", config.wal.segment_size_mb)
            } else {
                "single-file".to_string()
            },
            if encrypt { ", encrypted" } else { "" },
            if compress { ", compressed" } else { "" },
        );

        // Re-register tables restored from catalog so DiskEngine knows about
        // them — and let create_table reconcile epochs (a directory entry whose
        // generation differs from the catalog's is a stale drop+recreate, whose
        // first_page is abandoned rather than trusted; T0.3).
        for table_name in catalog.table_names().await {
            if let Err(e) = engine.create_table(&table_name).await {
                tracing::warn!("Failed to re-register table {table_name}: {e}");
            }
        }

        // Bidirectional reconciliation: reclaim storage-ahead orphans — tables
        // the on-disk directory still holds but the (authoritative,
        // persisted-last) catalog does not. Because DDL forces storage durable
        // *before* the catalog, the only crash-window residue is storage-ahead:
        // an uncommitted CREATE or a half-applied DROP. Dropping the orphan
        // frees its pages and keeps the two sides convergent. This replaces the
        // old purely one-directional re-register (which could only ever leak).
        //
        // SAFETY GUARD: only reclaim when the catalog is NON-empty. A missing or
        // corrupt catalog.json loads as an empty catalog (load_catalog returns
        // Ok with no tables), and reclaiming against that would drop EVERY
        // storage table — turning a recoverable catalog problem into permanent
        // data loss. An empty catalog beside populated storage is treated as
        // "catalog needs recovery", not "everything is an orphan": leave the
        // tables intact (they are invisible to SQL until the catalog is restored,
        // but preserved).
        {
            let cataloged: std::collections::HashSet<String> =
                catalog.table_names().await.into_iter().collect();
            let storage_tables = engine.table_names();
            if cataloged.is_empty() {
                if !storage_tables.is_empty() {
                    tracing::warn!(
                        "reconcile: catalog is empty but storage holds {} table(s) — NOT \
                         reclaiming (likely a missing/corrupt catalog, not orphans); tables are \
                         preserved but invisible until the catalog is restored",
                        storage_tables.len()
                    );
                }
            } else {
                let orphans: Vec<String> = storage_tables
                    .into_iter()
                    .filter(|t| !cataloged.contains(t))
                    .collect();
                let reclaimed = !orphans.is_empty();
                for orphan in orphans {
                    tracing::warn!(
                        "reconcile: reclaiming orphan storage table '{orphan}' (absent from catalog)"
                    );
                    if let Err(e) = engine.drop_table(&orphan).await {
                        tracing::warn!("reconcile: failed to reclaim orphan '{orphan}': {e}");
                    }
                }
                // Persist the reclaimed directory so orphans don't reappear next boot.
                if reclaimed && let Err(e) = engine.flush_schema().await {
                    tracing::warn!("reconcile: failed to persist directory after reclaim: {e}");
                }
            }
        }

        disk_engine = Some(engine.clone());

        // Wrap DiskEngine in BufferedDiskEngine for transaction atomicity + rollback
        let buffered = Arc::new(BufferedDiskEngine::new(engine));
        tracing::info!(
            "Transaction support: buffered write-ahead (atomicity + rollback), \
             SERIALIZABLE via table-level strict 2PL"
        );
        buffered_for_metrics = Some(buffered.clone());
        buffered as Arc<dyn StorageEngine>
    };

    // Set up shared metrics registry
    let metrics = Arc::new(MetricsRegistry::new());
    if let Some(ref buffered) = buffered_for_metrics {
        buffered.set_metrics(metrics.clone());
    }

    // Set up replication manager — mode depends on --replicate-from flag
    let replication = Arc::new(parking_lot::RwLock::new(if is_replica {
        // Replica mode: we'll connect to the primary for WAL streaming
        nucleus::replication::ReplicationManager::new_replica(1, 0)
    } else {
        // Primary or standalone mode
        nucleus::replication::ReplicationManager::new_standalone(1)
    }));

    // Set up async connection pool using config values
    let pool_config = SyncPoolConfig {
        max_connections: config.server.max_connections as u32,
        min_idle: config.pool.min_idle as u32,
        max_idle_time_ms: config.pool.max_idle_time_secs * 1000,
        max_lifetime_ms: config.pool.max_lifetime_secs * 1000,
        acquire_timeout_ms: config.pool.acquire_timeout_secs * 1000,
        validation_interval_ms: config.pool.validation_interval_secs * 1000,
    };
    let conn_pool = Arc::new(AsyncConnectionPool::new(pool_config));
    tracing::info!(
        "Connection pool: max {} connections",
        config.server.max_connections
    );

    // Generate a stable node ID from the SQL listen address
    let node_id: u64 = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{}:{}", host, port).hash(&mut hasher);
        hasher.finish()
    };

    // Set up cluster coordinator (always, standalone by default)
    let cluster = Arc::new(parking_lot::RwLock::new(
        nucleus::distributed::ClusterCoordinator::new_standalone(node_id),
    ));

    // Set up executor and wire protocol handler
    let catalog_path = if memory {
        None
    } else {
        Some(data.join("catalog.json"))
    };
    let cache_bytes = config.cache.max_memory_mb * 1024 * 1024;
    let store_dir = if memory { None } else { Some(data.as_path()) };
    let mut executor_build =
        Executor::new_with_persistence(catalog, storage, catalog_path, store_dir)
            .with_cache_size(cache_bytes)
            .with_allocator_budget(config.server.max_memory_mb * 1024 * 1024)
            .with_metrics(metrics.clone())
            .with_replication(replication.clone())
            .with_conn_pool(conn_pool.clone())
            .with_cluster(cluster.clone());
    if let Some(enc) = spill_encryptor {
        // Encrypted deployment: spill runs must be ciphertext (fail-closed).
        executor_build = executor_build.with_spill_encryptor(enc);
    }
    let executor = Arc::new(executor_build);
    // Install the weak self-reference so streaming producers can hold an owned
    // Arc<Executor> across the wire-drain boundary (streaming WHERE filter, etc.).
    executor.install_self_ref();
    // Seed the sync column cache for restored tables so size-keyed fast paths
    // (O(1) COUNT most visibly) work from the first query after a restart.
    executor.warm_table_caches_sync();
    tracing::info!("Cache: {} MB", config.cache.max_memory_mb);

    // Query execution memory budget (T1.2): make the operator's configured
    // memory limit the ceiling for the hash-join result circuit-breaker, instead
    // of the hardcoded 256 MB default that ignored config. 0 → unlimited.
    //
    // A FRACTION of the RSS cap, not all of it. Set equal, the working-set
    // limit can never fire first: one query is permitted to reserve the whole
    // cap, so the RSS watchdog trips and the blanket write-reject fires while
    // no single query has done anything the budget considers wrong. A query
    // that is genuinely too big should get a 53200 that names the query.
    let query_mem_mb = config
        .server
        .max_memory_mb
        .saturating_mul(config.server.query_memory_percent.min(100))
        / 100;
    executor.set_query_memory_limit((query_mem_mb as u64) * 1024 * 1024);
    executor.set_reject_writes_on_memory_critical(config.server.reject_writes_on_memory_critical);
    if config.server.max_memory_mb > 0 {
        tracing::info!(
            "Query memory budget: {} MB ({}% of the {} MB server limit)",
            query_mem_mb,
            config.server.query_memory_percent,
            config.server.max_memory_mb
        );
    }

    // Disk watermark monitor: sample free space on the data directory's
    // filesystem and degrade the executor's write-admission gate to read-only
    // *before* the filesystem fills, rather than discovering ENOSPC partway
    // through a write. Writes resume automatically once free space climbs back
    // above the (higher) resume watermark. In-memory mode has no data
    // directory to watch.
    if !memory && config.storage.disk_check_interval_secs > 0 {
        let marks = config.storage.disk_watermarks();
        let guard = Arc::new(nucleus::ops::DiskGuard::with_fs_probe(
            data.clone(),
            marks,
            executor.service().clone(),
        ));
        // Evaluate once synchronously: starting up on an already-full disk
        // must come up read-only, not accept writes until the first tick.
        let first = guard.evaluate();
        tracing::info!(
            "Disk watermarks: warn<{:.1}% readonly<{:.1}% resume>{:.1}% min-free={} MB — {}",
            marks.warn_free_pct,
            marks.readonly_free_pct,
            marks.resume_free_pct,
            config.storage.disk_min_free_mb,
            first.detail
        );
        let interval = std::time::Duration::from_secs(config.storage.disk_check_interval_secs);
        // Supervised in `DiskGuard` rather than inline here: read-only is
        // latched and ONLY a later reading clears it, so if this loop stops the
        // server refuses writes until it is restarted — and an inline
        // `tokio::spawn` swallowed a panicking reading without a trace.
        guard.spawn_monitor(interval);
    }

    // Commit-time durability default (config wal.synchronous_commit;
    // sessions override with SET synchronous_commit = on|off).
    let sync_commit_on = !matches!(
        config.wal.synchronous_commit.to_ascii_lowercase().as_str(),
        "off" | "false" | "0"
    );
    executor.set_synchronous_commit_default(sync_commit_on);
    tracing::info!(
        "Durability: synchronous_commit={} (commit-time WAL force{})",
        if sync_commit_on { "on" } else { "off" },
        if sync_commit_on {
            ""
        } else {
            " disabled — loss window bounded by checkpoint interval"
        },
    );

    // Load persisted ANALYZE statistics so the optimizer is warm on restart.
    executor.load_stats().await;

    // Load persisted executor metadata (views, sequences, triggers, roles, functions).
    //
    // Fail closed. `security.rls` and `security.masking` are the only parts of
    // this load installed unconditionally — every other catalog is
    // `is_empty()`-guarded — so a meta.json that exists and cannot be read used
    // to start the server with row-level security and column masking silently
    // switched off, and the next DDL wrote that empty catalog back over the
    // file. Serving with an unknown security posture is worse than not serving,
    // and the operator's remedy is explicit: restore the file, or move it aside
    // to declare the empty catalog intentional. An ABSENT meta.json is an
    // ordinary first boot and is not an error.
    if let Err(e) = executor.load_meta_checked().await {
        tracing::error!("{e}");
        eprintln!(
            "nucleus: refusing to start — {e}\n\
             \n\
             meta.json holds the row-level-security policies and column-masking rules.\n\
             Starting without it would serve every table with those protections off.\n\
             \n\
             Restore it from backup, or move it aside to start with an explicitly\n\
             empty policy catalog."
        );
        std::process::exit(1);
    }

    // Re-register per-table engine overrides (mergetree/columnar tables) from
    // engines.json: reopens their WAL-backed storage and restores
    // replacing-dedup configs. Without this, engine tables silently fell back
    // to the default heap engine after every restart.
    executor.restore_table_engines().await;

    // Rebuild specialty indexes (IvfFlat, encrypted) from table data after restart.
    executor.rebuild_specialty_indexes().await;

    // Resolve password: CLI arg takes priority, then NUCLEUS_PASSWORD env var.
    let resolved_password = password.or_else(|| std::env::var("NUCLEUS_PASSWORD").ok());
    let resolved_auth_method = auth_method
        .map(CliAuthMethod::to_wire)
        .or_else(|| {
            std::env::var("NUCLEUS_AUTH_METHOD")
                .ok()
                .and_then(|value| parse_auth_method_env(&value))
        })
        .unwrap_or(AuthMethod::ScramSha256);
    let auth_enabled = resolved_password.is_some();
    let allow_insecure_auth = env_var_truthy("NUCLEUS_ALLOW_INSECURE_AUTH");
    let allow_no_auth = env_var_truthy("NUCLEUS_ALLOW_NO_AUTH");
    if !is_loopback_host(&host) && !auth_enabled && !allow_no_auth {
        tracing::error!(
            "Refusing to start with non-loopback SQL bind and no authentication. \
             Set --password/NUCLEUS_PASSWORD or NUCLEUS_ALLOW_NO_AUTH=1 for development."
        );
        std::process::exit(1);
    }
    if auth_enabled {
        tracing::info!(
            "Authentication enabled (password required, method: {:?})",
            resolved_auth_method
        );
        if no_tls {
            if allow_insecure_auth {
                tracing::warn!(
                    "Password authentication is enabled while TLS is disabled \
                     (NUCLEUS_ALLOW_INSECURE_AUTH=1)"
                );
            } else {
                tracing::error!(
                    "Refusing to start: password authentication requires TLS by default. \
                     Use --no-tls only with NUCLEUS_ALLOW_INSECURE_AUTH=1 for development."
                );
                std::process::exit(1);
            }
        }
        if resolved_auth_method != AuthMethod::ScramSha256 {
            tracing::error!(
                "Catalog-backed multi-user authentication requires SCRAM-SHA-256; \
                 cleartext authentication is no longer accepted by the production server"
            );
            std::process::exit(1);
        }
    }
    let resolved_password_for_resp = resolved_password.clone();
    let handler = if let Some(ref bootstrap_password) = resolved_password {
        executor.set_bootstrap_password(bootstrap_password).await;
        Arc::new(NucleusHandler::with_catalog_auth(executor.clone()))
    } else {
        Arc::new(NucleusHandler::new(executor.clone()))
    };
    let handler_ref = handler.clone();
    let server = Arc::new(NucleusServer::new(handler));
    let resolved_tls_client_ca = tls_client_ca.or_else(|| {
        std::env::var("NUCLEUS_TLS_CLIENT_CA")
            .ok()
            .map(PathBuf::from)
    });

    // Set up TLS
    let tls_acceptor = if no_tls {
        tracing::warn!("TLS disabled -- connections will be unencrypted");
        None
    } else if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        match tls::load_tls_config_with_client_ca(&cert, &key, resolved_tls_client_ca.as_deref()) {
            Ok(acceptor) => {
                if let Some(client_ca) = resolved_tls_client_ca.as_ref() {
                    tracing::info!(
                        "TLS enabled with mTLS client certificate verification (CA: {})",
                        client_ca.display()
                    );
                } else {
                    tracing::info!("TLS enabled (user-provided certificate)");
                }
                Some(acceptor)
            }
            Err(e) => {
                tracing::error!("Failed to load TLS certificate: {e}");
                std::process::exit(1);
            }
        }
    } else {
        match tls::setup_tls_with_client_ca(resolved_tls_client_ca.as_deref()) {
            Ok(acceptor) => {
                if acceptor.is_some() {
                    tracing::info!("TLS enabled (auto-generated self-signed certificate)");
                } else {
                    tracing::warn!("TLS disabled by configuration");
                }
                acceptor
            }
            Err(e) => {
                tracing::error!("TLS setup failed: {e}");
                std::process::exit(1);
            }
        }
    };

    if auth_enabled && tls_acceptor.is_none() && !allow_insecure_auth {
        tracing::error!("Refusing to start: password authentication is enabled without TLS.");
        std::process::exit(1);
    }

    let internal_tls = match load_internal_tls_from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::error!("Internal transport TLS configuration error: {e}");
            std::process::exit(1);
        }
    };
    if internal_tls.is_some() {
        tracing::info!("Internal node-to-node TLS is enabled (cluster + replication)");
    }

    // Set up cluster transport — always listen so other nodes can join us
    let cluster_listen = format!("{}:{}", host, cluster_port);
    let transport = Arc::new(TcpTransport::new_with_auth_and_tls(
        node_id,
        &cluster_listen,
        cluster_token.clone(),
        internal_tls.clone(),
    ));
    match transport.listen().await {
        Ok(addr) => tracing::info!("Cluster transport on {addr} (node_id={node_id:#x})"),
        Err(e) => tracing::warn!("Failed to bind cluster port {cluster_listen}: {e}"),
    }

    // If --join, perform the join handshake
    if let Some(ref peer_addr) = join {
        tracing::info!("Joining cluster via {peer_addr}...");

        // Use a hash of the peer address as a temporary node ID for the peer
        let peer_id: u64 = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            peer_addr.hash(&mut hasher);
            hasher.finish()
        };
        transport.register_peer(peer_id, peer_addr).await;

        // Send JoinCluster request
        let join_msg = nucleus::transport::Message::JoinCluster {
            node_id,
            address: format!("{}:{}", host, cluster_port),
        };
        if let Err(e) = transport.send_message(peer_id, join_msg).await {
            tracing::error!("Failed to send join request to {peer_addr}: {e}");
        } else {
            // Wait for JoinClusterResponse (10s timeout)
            match tokio::time::timeout(std::time::Duration::from_secs(10), transport.recv()).await {
                Ok(Some(env)) => {
                    if let nucleus::transport::Message::JoinClusterResponse {
                        success,
                        cluster_nodes,
                    } = env.message
                    {
                        if success {
                            tracing::info!("Joined cluster ({} nodes)", cluster_nodes.len());
                            let mut coord = cluster.write();
                            for (nid, addr) in &cluster_nodes {
                                if *nid != node_id {
                                    coord.add_node(*nid, addr.clone());
                                }
                            }
                        } else {
                            tracing::error!("Cluster join rejected by {peer_addr}");
                        }
                    }
                }
                Ok(None) => tracing::error!("Cluster connection closed before join response"),
                Err(_) => tracing::error!("Timeout waiting for join response from {peer_addr}"),
            }
        }
    }

    // Build RaftReplicator from peers discovered during join (or empty for standalone).
    let initial_peers: Vec<(u64, String)> = {
        let coord = cluster.read();
        coord
            .peer_node_ids()
            .into_iter()
            .filter_map(|id| {
                coord
                    .cluster_nodes()
                    .get(&id)
                    .cloned()
                    .map(|addr| (id, addr))
            })
            .collect()
    };
    // A server with a data directory gets durable Raft state; memory mode has
    // nowhere to put it and is explicitly not restart-safe.
    let raft_dir = if memory {
        None
    } else {
        Some(data.join("raft"))
    };
    let (raft_replicator, apply_rx) = nucleus::distributed::RaftReplicator::with_storage(
        node_id,
        initial_peers,
        transport.clone(),
        raft_dir,
    );
    let raft_replicator = Arc::new(raft_replicator);

    // Register initial peers with the transport.
    let peers: Vec<_> = {
        let coord = cluster.read();
        coord
            .peer_node_ids()
            .iter()
            .filter_map(|peer_id| {
                coord
                    .cluster_nodes()
                    .get(peer_id)
                    .map(|addr| (*peer_id, addr.clone()))
            })
            .collect()
    };
    for (peer_id, addr) in peers {
        transport.register_peer(peer_id, &addr).await;
    }

    // Attach the replicator to the executor (set_raft_replicator works post-construction).
    executor.set_raft_replicator(raft_replicator.clone());

    // Spawn apply task: execute committed SQL from followers on this node.
    let executor_for_apply = executor.clone();
    tokio::spawn(async move {
        let mut rx = apply_rx;
        while let Some(sql) = rx.recv().await {
            if let Err(e) = executor_for_apply.apply_replicated_sql(&sql).await {
                tracing::warn!("Failed to apply Raft-committed SQL: {e}: sql={sql}");
            }
        }
    });

    // Spawn cluster message receive loop (handles JoinCluster, heartbeats, Raft RPCs, etc.)
    let transport_for_recv = transport.clone();
    let cluster_for_recv = cluster.clone();
    let executor_for_recv = executor.clone();
    let replicator_for_recv = raft_replicator.clone();
    tokio::spawn(async move {
        loop {
            match transport_for_recv.recv().await {
                Some(env) => {
                    handle_cluster_message(
                        &cluster_for_recv,
                        &transport_for_recv,
                        env,
                        &cluster_listen,
                        &executor_for_recv,
                        Some(&replicator_for_recv),
                    )
                    .await;
                }
                None => {
                    tracing::debug!("Cluster transport inbox closed");
                    break;
                }
            }
        }
    });

    // Spawn Raft tick loop: heartbeats every 100 ms, election timeout every 50 ms.
    let replicator_for_tick = raft_replicator.clone();
    tokio::spawn(async move {
        let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_millis(100));
        let mut election_interval = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    replicator_for_tick.tick_heartbeat().await;
                }
                _ = election_interval.tick() => {
                    replicator_for_tick.tick_election().await;
                }
            }
        }
    });

    // Legacy cluster heartbeat loop for non-Raft connectivity checks (every 5s).
    let transport_for_hb = transport.clone();
    let cluster_for_hb = cluster.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let (peer_ids, term) = {
                let coord = cluster_for_hb.read();
                (coord.peer_node_ids(), coord.epoch())
            };
            if peer_ids.is_empty() {
                continue; // No peers to heartbeat
            }
            for peer in peer_ids {
                let _ = transport_for_hb
                    .send_message(
                        peer,
                        nucleus::transport::Message::Heartbeat { node_id, term },
                    )
                    .await;
            }
        }
    });

    // Start background worker pool with system maintenance tasks
    let workers = Arc::new(nucleus::background::BackgroundWorkerPool::new(2));
    workers.submit_recurring(
        nucleus::background::BackgroundTask::WalCheckpoint,
        nucleus::background::Priority::High,
        std::time::Duration::from_secs(config.wal.checkpoint_interval_secs),
    );
    workers.submit_recurring(
        nucleus::background::BackgroundTask::BufferFlush,
        nucleus::background::Priority::Normal,
        std::time::Duration::from_secs(60),
    );
    workers.submit_recurring(
        nucleus::background::BackgroundTask::CacheCleanup,
        nucleus::background::Priority::Low,
        std::time::Duration::from_secs(10),
    );
    workers.submit_recurring(
        nucleus::background::BackgroundTask::ReplicationSync,
        nucleus::background::Priority::Normal,
        std::time::Duration::from_secs(5),
    );
    // WAL archive timeout: seal and archive the active segment on a timer so
    // the point-in-time recovery window is bounded by wall-clock rather than by
    // write volume.
    //
    // A segment otherwise reaches the archive only when it fills. At the
    // default 64 MiB that makes the recovery point the last rollover, so a
    // quiet database can accumulate days of commits that `restore-pitr` cannot
    // reach — and it reports success regardless, so the gap only surfaces
    // during an actual recovery. Only spawned when an archive is configured,
    // since without one there is nothing to archive to.
    let archive_timeout_secs = config.wal.archive_timeout_secs;
    if archive_timeout_secs > 0
        && std::env::var("NUCLEUS_WAL_ARCHIVE_DIR").is_ok_and(|v| !v.is_empty())
        && let Some(engine) = disk_engine.clone()
    {
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(archive_timeout_secs));
            // The first tick fires immediately; skip it so startup does not
            // archive an empty segment.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let engine = engine.clone();
                match tokio::task::spawn_blocking(move || engine.archive_active_wal()).await {
                    Ok(Ok(true)) => {
                        tracing::debug!("WAL archive timeout: sealed and archived active segment")
                    }
                    Ok(Ok(false)) => {}
                    Ok(Err(e)) => tracing::warn!("WAL archive timeout failed: {e}"),
                    Err(e) => tracing::warn!("WAL archive timeout task panicked: {e}"),
                }
            }
        });
        tracing::info!(
            "WAL archive timeout: active segment archived every {archive_timeout_secs}s \
             (recovery-point objective)"
        );
    }

    // Idle-in-transaction sweep (T1.3): roll back transactions left open and
    // idle past the configured timeout so their MVCC snapshots are released and
    // GC can advance. Only spawned when enabled (timeout > 0), so the default
    // deployment is unchanged.
    let idle_txn_timeout_secs = config.server.idle_in_transaction_timeout_secs;
    if idle_txn_timeout_secs > 0 {
        let executor_for_idle = executor.clone();
        // Sweep on a cadence bounded to [1s, 30s] so an abandoned transaction is
        // reclaimed promptly without busy-looping on very large timeouts.
        let sweep_secs = idle_txn_timeout_secs.clamp(1, 30);
        let timeout_ms = idle_txn_timeout_secs.saturating_mul(1000);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(sweep_secs));
            loop {
                ticker.tick().await;
                let n = executor_for_idle
                    .sweep_idle_in_transaction(timeout_ms)
                    .await;
                if n > 0 {
                    tracing::info!("Idle-in-transaction sweep rolled back {n} transaction(s)");
                }
            }
        });
        tracing::info!("Idle-in-transaction timeout: {idle_txn_timeout_secs}s");
    }

    // Set up WAL notifier for streaming replication broadcast channel.
    // The notifier bridges the storage WAL to the TCP replication transport.
    let wal_notifier = Arc::new(tokio::sync::Mutex::new(
        nucleus::replication::WalNotifier::new(4096),
    ));

    // Spawn worker drain loop -- actually executes tasks
    let workers_loop = workers.clone();
    let disk_for_workers = disk_engine.clone();
    let repl_for_workers = replication.clone();
    let wal_path_for_workers = if memory {
        None
    } else {
        Some(data.join("nucleus.wal"))
    };
    let mut wal_bridge = nucleus::replication::WalBridge::new();
    let executor_for_workers = executor.clone();
    let notifier_for_workers = wal_notifier.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if !workers_loop.is_running() {
                break;
            }
            let tasks = workers_loop.drain_pending().await;
            for task in tasks {
                tracing::debug!(
                    "Background task: {:?} (priority={:?})",
                    task.task,
                    task.priority
                );
                match &task.task {
                    nucleus::background::BackgroundTask::BufferFlush => {
                        if let Some(ref engine) = disk_for_workers
                            && let Err(e) = engine.flush()
                        {
                            tracing::warn!("Background flush failed: {e}");
                        }
                    }
                    nucleus::background::BackgroundTask::WalCheckpoint => {
                        if let Some(ref engine) = disk_for_workers
                            && let Err(e) = engine.flush()
                        {
                            tracing::warn!("Background flush failed: {e}");
                        }
                        // Truncate the specialty-store WALs (CDC, KV,
                        // collections, blob, graph, document, streams, FTS)
                        // to their current in-memory snapshot. Each of these
                        // logs every write unconditionally with no consumer
                        // or reader required — without periodic
                        // checkpointing they grow one record per write
                        // forever, on disk, regardless of activity. This is
                        // what caused the 2026-06-30 observe-nucleus OOM
                        // (CDC log in memory) and was already visibly
                        // inflating kv.wal (493MB from a ~1.2GB dataset) on
                        // that same host. Vector, timeseries, and columnar are
                        // checkpointed alongside the rest below; the geo WAL is
                        // opened for recovery but never appended to (geo data
                        // persists as ordinary SQL columns), so it needs no
                        // checkpoint here.
                        // SQL disk engine: flush dirty pages, checkpoint the
                        // WAL, and prune fully-checkpointed segments. With
                        // synchronous_commit=on (default) acked commits are
                        // already WAL-forced at commit time, so this interval
                        // is about data-page flushing + segment pruning; it is
                        // the crash-loss bound ONLY for sessions running
                        // synchronous_commit=off (wal.checkpoint_interval_secs).
                        if let Some(ref engine) = disk_for_workers
                            && let Err(e) = engine.checkpoint()
                        {
                            tracing::warn!("SQL WAL checkpoint failed: {e}");
                        }
                        if let Err(e) = executor_for_workers.checkpoint_cdc_wal() {
                            tracing::warn!("CDC WAL checkpoint failed: {e}");
                        }
                        if let Err(e) = executor_for_workers.checkpoint_streams_wal() {
                            tracing::warn!("Streams WAL checkpoint failed: {e}");
                        }
                        if let Err(e) = executor_for_workers.kv_store().checkpoint() {
                            tracing::warn!("KV WAL checkpoint failed: {e}");
                        }
                        if let Err(e) = executor_for_workers.blob_store().read().checkpoint() {
                            tracing::warn!("Blob WAL checkpoint failed: {e}");
                        }
                        if let Err(e) = executor_for_workers.graph_store().read().checkpoint_wal() {
                            tracing::warn!("Graph WAL checkpoint failed: {e}");
                        }
                        if let Err(e) = executor_for_workers.doc_store().read().checkpoint() {
                            tracing::warn!("Document WAL checkpoint failed: {e}");
                        }
                        if let Err(e) = executor_for_workers.fts_index().read().checkpoint_wal() {
                            tracing::warn!("FTS WAL checkpoint failed: {e}");
                        }
                        // Vector index WAL: HNSW inserts/deletes log one record
                        // each; snapshot every live HNSW index (IvfFlat rebuilds
                        // from base-table data, never logged here).
                        if let Err(e) = executor_for_workers.checkpoint_vector_wal() {
                            tracing::warn!("Vector WAL checkpoint failed: {e}");
                        }
                        // TimeSeries retention (T1.3): purge points older than the
                        // configured TS_RETENTION policy BEFORE snapshotting, so the
                        // WAL is truncated to the retained state and old data does
                        // not grow the store forever. No-op when no policy is set.
                        executor_for_workers.ts_store().write().apply_retention();
                        // TimeSeries WAL: every insert appends a record; snapshot
                        // truncates it to the current series state.
                        executor_for_workers.ts_store().read().snapshot();
                        // Columnar WAL: every append/create logs a record; snapshot
                        // truncates it to the current table state.
                        if let Err(e) = executor_for_workers.columnar_store().write().checkpoint() {
                            tracing::warn!("Columnar WAL checkpoint failed: {e}");
                        }
                        // Per-table storage engines (WITH (engine='columnar'
                        // |'mergetree'|'lsm')): distinct from the columnar
                        // MODEL checkpointed just above. Each has its own WAL
                        // that otherwise grows unbounded — see
                        // `checkpoint_table_engines`'s doc comment.
                        executor_for_workers.checkpoint_table_engines().await;
                    }
                    nucleus::background::BackgroundTask::ReplicationSync => {
                        if let Some(ref wal_path) = wal_path_for_workers {
                            {
                                let mut repl = repl_for_workers.write();
                                let forwarded = wal_bridge.forward_new_records(wal_path, &mut repl);
                                if forwarded > 0 {
                                    tracing::debug!(
                                        "Replication: forwarded {forwarded} WAL records"
                                    );
                                }
                            } // drop parking_lot guard before await

                            // Also notify the broadcast channel so connected replicas
                            // receive records via TCP streaming.
                            let mut notifier = notifier_for_workers.lock().await;
                            notifier.notify_from_storage_wal(wal_path);
                        }
                    }
                    nucleus::background::BackgroundTask::CacheCleanup => {
                        executor_for_workers.cleanup_expired_cache();
                    }
                    _ => {}
                }
            }
        }
    });
    tracing::info!(
        "Background workers started ({} workers)",
        workers.num_workers
    );

    // Background eviction of expired idle connections
    let pool_for_evict = conn_pool.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            pool_for_evict.evict_expired().await;
        }
    });

    // ---- Memory watchdog (RSS-based hard cap) --------------------------------
    // Reads /proc/self/statm on Linux to get actual RSS and triggers memory
    // pressure when approaching the --max-memory limit.
    {
        let executor_for_mem = executor.clone();
        let memory_flag = executor.memory_critical_flag().clone();
        let metrics_for_mem = metrics.clone();
        let max_memory_bytes = config.server.max_memory_mb as u64 * 1024 * 1024;
        metrics.memory_limit_bytes.set(max_memory_bytes as i64);
        tokio::spawn(async move {
            let warn_threshold = (max_memory_bytes as f64 * 0.60) as u64;
            let pressure_threshold = (max_memory_bytes as f64 * 0.75) as u64;
            let critical_threshold = (max_memory_bytes as f64 * 0.90) as u64;
            // Mirror the write-reject flag into the metrics gauge + health
            // registry so the state is observable instead of silent
            // (finding #33: writes were rejected with no external signal).
            let mut was_rejecting = false;
            let mut surface_reject_state = |rejecting: bool, rss: u64| {
                metrics_for_mem
                    .memory_writes_rejected
                    .set(if rejecting { 1 } else { 0 });
                if rejecting != was_rejecting {
                    let mut health = executor_for_mem.health_registry().write();
                    if rejecting {
                        health.mark_degraded(
                            "memory",
                            &format!(
                                "writes rejected: RSS {} MB >= 90% of {} MB budget",
                                rss / (1024 * 1024),
                                max_memory_bytes / (1024 * 1024)
                            ),
                        );
                    } else {
                        health.mark_healthy("memory");
                    }
                    was_rejecting = rejecting;
                }
            };
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let rss_bytes = read_rss_bytes();
                metrics_for_mem.memory_rss_bytes.set(rss_bytes as i64);
                if rss_bytes == 0 {
                    continue; // /proc/self/statm not available (macOS, etc.)
                }
                if rss_bytes > pressure_threshold {
                    tracing::warn!(
                        "Memory pressure: RSS {} MB / {} MB limit — triggering eviction",
                        rss_bytes / (1024 * 1024),
                        max_memory_bytes / (1024 * 1024),
                    );
                    // Evict cache, sweep KV expired, compact FTS, flush columnar to disk
                    executor_for_mem.cleanup_expired_cache();
                    executor_for_mem.kv_store().sweep_expired();
                    {
                        let mut fts = executor_for_mem.fts_index().write();
                        fts.shrink_postings();
                        let _ = fts.checkpoint_wal();
                    }
                    // Flush ALL MergeTree hot segments to cold (disk) storage
                    let col_freed = executor_for_mem
                        .columnar_store()
                        .write()
                        .flush_all_hot_to_disk();
                    if col_freed > 0 {
                        tracing::info!(
                            "Columnar flush: freed {} MB of hot segments to disk",
                            col_freed / (1024 * 1024),
                        );
                    }
                    // Refresh allocator tracking
                    {
                        let mut alloc = executor_for_mem.memory_allocator().lock();
                        // Reset all subsystems to measured values
                        for name in ["cache", "fts", "kv", "columnar", "sparse", "doc", "graph"] {
                            let old = alloc.allocation(name).map(|a| a.current_bytes).unwrap_or(0);
                            alloc.release(name, old);
                        }
                    }
                    // If still critical after eviction, reject writes via executor flag
                    let rss_after = read_rss_bytes();
                    if rss_after > critical_threshold {
                        memory_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        surface_reject_state(true, rss_after);
                        tracing::error!(
                            "CRITICAL: RSS {} MB exceeds 90% of {} MB limit after eviction. \
                             New writes will be rejected until memory drops.",
                            rss_after / (1024 * 1024),
                            max_memory_bytes / (1024 * 1024),
                        );
                    } else {
                        // Eviction brought us below critical — allow writes again
                        memory_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                        surface_reject_state(false, rss_after);
                    }
                } else {
                    // Below pressure threshold — ensure writes are allowed
                    memory_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                    surface_reject_state(false, rss_bytes);
                    if rss_bytes > warn_threshold {
                        // Diagnostic: report per-subsystem estimates
                        let col_bytes = executor_for_mem
                            .columnar_store()
                            .read()
                            .estimated_memory_bytes();
                        let fts_bytes = {
                            use nucleus::memory::Pressurable;
                            executor_for_mem.fts_index().read().current_usage()
                        };
                        let kv_entries = executor_for_mem.kv_store().dbsize();
                        tracing::info!(
                            "Memory: RSS {} MB / {} MB (columnar {} MB, FTS {} MB, KV {} entries)",
                            rss_bytes / (1024 * 1024),
                            max_memory_bytes / (1024 * 1024),
                            col_bytes / (1024 * 1024),
                            fts_bytes / (1024 * 1024),
                            kv_entries,
                        );
                    }
                }
            }
        });
        tracing::info!(
            "Memory watchdog: monitoring RSS, pressure at {}% of {} MB",
            85,
            config.server.max_memory_mb
        );
    }

    // ---- Streaming replication transport ------------------------------------
    if let Some(ref primary_addr) = replicate_from {
        // Replica mode: connect to the primary's replication port
        tracing::info!("Replica mode: connecting to primary at {primary_addr}");
        let client = nucleus::replication::ReplicationClient::new_with_tls(
            primary_addr.clone(),
            replication_token.clone(),
            internal_tls.clone(),
        );
        let repl_for_client = replication.clone();
        tokio::spawn(async move {
            loop {
                match client.run(repl_for_client.clone()).await {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::error!("Replication client error: {e}, reconnecting in 5s...");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });
    } else {
        // Primary / standalone mode: start replication server for replicas
        let repl_listen = format!("{host}:{replication_port}");
        let repl_server = nucleus::replication::ReplicationServer::new_with_tls(
            repl_listen.clone(),
            wal_notifier.clone(),
            replication_token.clone(),
            internal_tls.clone(),
        );
        tracing::info!("Replication server listening on {repl_listen}");
        tokio::spawn(async move {
            if let Err(e) = repl_server.run().await {
                tracing::error!("Replication server error: {e}");
            }
        });
    }

    // Set up thread-per-core runtime with connection routing
    let core_config = CoreConfig::default();
    let num_cores = core_config.num_cores;
    let runtime = Arc::new(NucleusRuntime::new(core_config));
    let router = Arc::new(ConnectionRouter::new(runtime.clone()));
    tracing::info!("Runtime: {num_cores} cores, round-robin connection routing");

    // Graceful shutdown handler: flush dirty pages on Ctrl+C / SIGTERM.
    //
    // Bounded shutdown contract (see #16):
    //   1. On signal, notify the accept loop to stop taking new work.
    //   2. Arm a hard-exit watchdog on a real OS thread — if the in-tokio
    //      cleanup is still running after `SHUTDOWN_DEADLINE_SECS`, the
    //      watchdog calls `std::process::exit` so we never hang past the
    //      budget regardless of dropped runtime / stuck spawn_blocking /
    //      lock contention in the buffer pool.
    //   3. Run synchronous work (disk flush) on a blocking thread so it
    //      cannot stall a tokio worker.
    const SHUTDOWN_DEADLINE_SECS: u64 = 5;
    /// How long to wait for in-flight connections before flushing. Must stay
    /// comfortably under `SHUTDOWN_DEADLINE_SECS` so the flush and the
    /// watchdog both still have room.
    const DRAIN_BUDGET_SECS: u64 = 2;
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let shutdown_for_handler = shutdown_notify.clone();
    // Drain coordinator: makes "stop accepting → finish in-flight work →
    // persist → exit" an enforced order. Before this, the signal handler
    // flushed and called `process::exit(0)` immediately after notifying the
    // accept loop, so the accept loop's own drain never got to run and a
    // request could be executing while the flush ran and the process died.
    let drain = nucleus::ops::ShutdownCoordinator::new();
    let drain_for_handler = drain.clone();
    let drain_for_accept = drain.clone();
    let disk_for_shutdown = disk_engine.clone();
    let workers_for_shutdown = workers.clone();
    let runtime_for_shutdown = runtime.clone();
    let transport_for_shutdown = transport.clone();
    tokio::spawn(async move {
        // Wait for either Ctrl+C (SIGINT) or SIGTERM (on Unix).
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
        tracing::info!("Shutdown signal received, flushing data...");

        // Arm the hard-exit watchdog FIRST. This runs on a dedicated OS
        // thread that is invisible to the tokio runtime, so it fires even
        // if every tokio worker is parked or the runtime drop is wedged.
        std::thread::Builder::new()
            .name("nucleus-shutdown-watchdog".into())
            .spawn(|| {
                std::thread::sleep(std::time::Duration::from_secs(
                    SHUTDOWN_DEADLINE_SECS,
                ));
                eprintln!(
                    "[nucleus] graceful shutdown exceeded {SHUTDOWN_DEADLINE_SECS}s budget, forcing exit"
                );
                std::process::exit(0);
            })
            .expect("failed to spawn shutdown watchdog thread");

        // Tell the accept loop and protocol servers to stop immediately so
        // ports are released and no new work is admitted.
        shutdown_for_handler.notify_waiters();

        // Wait (bounded) for in-flight connections to finish BEFORE tearing
        // down the runtime and flushing. An idle client costs the full budget,
        // which is the price of not cutting off a request mid-write.
        match drain_for_handler
            .await_drain(std::time::Duration::from_secs(DRAIN_BUDGET_SECS))
            .await
        {
            nucleus::ops::DrainOutcome::Drained { started_with } => {
                if started_with > 0 {
                    tracing::info!("Drained {started_with} in-flight connection(s)");
                }
            }
            nucleus::ops::DrainOutcome::TimedOut { remaining } => {
                tracing::warn!(
                    "Shutdown drain timed out after {DRAIN_BUDGET_SECS}s with {remaining} connection(s) still active; continuing to flush"
                );
            }
        }

        // Log runtime stats before shutdown
        let stats = runtime_for_shutdown.stats();
        tracing::info!(
            "Runtime stats: {} total tasks across {} cores",
            stats.total_tasks,
            stats.core_count
        );
        for cs in &stats.per_core {
            if cs.connections > 0 || cs.tasks > 0 {
                tracing::info!(
                    "  Core {}: {} active connections, {} tasks processed",
                    cs.core_id,
                    cs.connections,
                    cs.tasks
                );
            }
        }
        runtime_for_shutdown.shutdown();

        workers_for_shutdown.shutdown();
        transport_for_shutdown.shutdown().await;

        // Run the synchronous flush on a blocking thread. Disk flushes can
        // contend with the buffer pool's writer paths and block for several
        // hundred ms; doing it inline on a tokio worker has been observed
        // to stall the runtime drop sequence under load.
        if let Some(engine) = disk_for_shutdown {
            let flush_result = tokio::task::spawn_blocking(move || {
                let flushed = engine.flush();
                // Seal and archive the active WAL segment on the way out.
                // Segments are otherwise archived only when they fill, which
                // makes the PITR recovery point the last rollover rather than
                // the last commit — so a clean, planned shutdown would leave
                // every commit since the segment began unreachable by
                // restore-pitr, which would then report success anyway.
                let archived = engine.archive_active_wal();
                (flushed, archived)
            })
            .await;
            match flush_result {
                Ok((flushed, archived)) => {
                    match flushed {
                        Ok(()) => tracing::info!("Data flushed to disk successfully"),
                        Err(e) => tracing::error!("Failed to flush data on shutdown: {e}"),
                    }
                    match archived {
                        Ok(true) => tracing::info!("Active WAL segment archived for PITR"),
                        Ok(false) => {}
                        Err(e) => tracing::error!(
                            "Failed to archive the active WAL segment on shutdown: {e}. \
                             Point-in-time recovery can only reach the last completed \
                             segment; commits after it are not in the archive."
                        ),
                    }
                }
                Err(e) => tracing::error!("Flush task panicked: {e}"),
            }
        }
        drain_for_handler.mark_persisted();
        tracing::info!("Nucleus stopped.");
        // Hard-exit immediately; do not wait for the runtime to drop
        // background tasks (some of which never check a shutdown flag).
        std::process::exit(0);
    });

    // Spawn RESP2 (Redis protocol) server
    if resp_port > 0 {
        let resp_addr = format!("{host}:{resp_port}");
        let kv = std::sync::Arc::clone(executor.kv_store());
        let resp_pw = resolved_password_for_resp.clone();
        let resp_shutdown = shutdown_notify.clone();
        tokio::spawn(async move {
            if let Err(e) =
                nucleus::resp::server::start_resp_server(resp_addr, kv, resp_pw, resp_shutdown)
                    .await
            {
                tracing::error!("RESP server error: {e}");
            }
        });
        tracing::info!("RESP server on port {resp_port} (redis-cli compatible)");
    }

    // Spawn S3-compatible gateway
    if s3_port > 0 {
        let access_key = std::env::var("NUCLEUS_S3_ACCESS_KEY").unwrap_or_default();
        let secret_key = std::env::var("NUCLEUS_S3_SECRET_KEY").unwrap_or_default();
        if access_key.is_empty() || secret_key.is_empty() {
            tracing::error!(
                "--s3-port set but NUCLEUS_S3_ACCESS_KEY / NUCLEUS_S3_SECRET_KEY missing; \
                 S3 gateway NOT started"
            );
        } else {
            let max_object_bytes = std::env::var("NUCLEUS_S3_MAX_OBJECT_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1024 * 1024 * 1024);
            let s3_addr = format!("{host}:{s3_port}");
            let s3_exec = executor.clone();
            let s3_config = std::sync::Arc::new(nucleus::s3::S3Config {
                access_key,
                secret_key,
                max_object_bytes,
            });
            let s3_shutdown = shutdown_notify.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    nucleus::s3::start_s3_server(s3_addr, s3_exec, s3_config, s3_shutdown).await
                {
                    tracing::error!("S3 gateway error: {e}");
                }
            });
            tracing::info!("S3 gateway on port {s3_port} (path-style, SigV4)");
        }
    }

    // Spawn metrics HTTP endpoint
    let metrics_port = config.metrics.port;
    let metrics_enabled = config.metrics.enabled;
    let metrics_endpoint = normalize_metrics_endpoint(&config.metrics.endpoint);
    if metrics_enabled {
        let metrics_for_http = metrics.clone();
        let endpoint_for_http = metrics_endpoint.clone();
        tokio::spawn(async move {
            serve_metrics_http(metrics_for_http, metrics_port, endpoint_for_http).await;
        });
    } else {
        tracing::info!("Metrics HTTP endpoint disabled (metrics.enabled = false)");
    }

    // Spawn periodic metrics sync task: buffer pool, WAL, and connection pool stats
    {
        let metrics_sync = metrics.clone();
        let disk_engine_sync = disk_engine.clone();
        let pool_sync = conn_pool.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            let mut prev_wal_bytes: u64 = 0;
            let mut prev_wal_syncs: u64 = 0;
            let mut prev_cache_hits: u64 = 0;
            let mut prev_cache_misses: u64 = 0;
            loop {
                interval.tick().await;
                if let Some(ref engine) = disk_engine_sync {
                    let bp = engine.buffer_pool();
                    let (hits, misses, _evictions, dirty) = bp.stats().snapshot();
                    // Buffer pool gauges
                    metrics_sync.buffer_pool_pages.set(bp.pool_size() as i64);
                    metrics_sync.buffer_pool_dirty_pages.set(dirty as i64);
                    // Cache hit/miss counters (delta since last sync)
                    let hit_delta = hits.saturating_sub(prev_cache_hits);
                    let miss_delta = misses.saturating_sub(prev_cache_misses);
                    if hit_delta > 0 {
                        metrics_sync.cache_hits.inc_by(hit_delta);
                    }
                    if miss_delta > 0 {
                        metrics_sync.cache_misses.inc_by(miss_delta);
                    }
                    prev_cache_hits = hits;
                    prev_cache_misses = misses;
                    // WAL counters (delta since last sync)
                    let (wal_bytes, wal_syncs) = bp.wal_stats();
                    let bytes_delta = wal_bytes.saturating_sub(prev_wal_bytes);
                    let syncs_delta = wal_syncs.saturating_sub(prev_wal_syncs);
                    if bytes_delta > 0 {
                        metrics_sync.wal_bytes_written.inc_by(bytes_delta);
                    }
                    if syncs_delta > 0 {
                        metrics_sync.wal_syncs.inc_by(syncs_delta);
                    }
                    prev_wal_bytes = wal_bytes;
                    prev_wal_syncs = wal_syncs;
                }
                // Connection pool idle count
                let pool_stats = pool_sync.stats().await;
                metrics_sync
                    .idle_connections
                    .set(pool_stats.idle_connections as i64);
            }
        });
    }

    // Start listening
    let addr = format!("{host}:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("Failed to bind SQL listener on {addr}: {e}");
            std::process::exit(1);
        }
    };

    println!();
    println!("  Nucleus is ready.");
    println!("  Listening on: {addr}");
    println!("  Mode:         {mode}");
    println!("  Cores:        {num_cores}");
    println!("  Buffer pool:  {} MB", config.storage.buffer_pool_size_mb);
    println!("  Cache:        {} MB", config.cache.max_memory_mb);
    println!("  Log level:    {}", config.logging.level);
    println!("  Cluster port: {cluster_port}");
    println!("  Node ID:      {node_id:#x}");
    if let Some(ref primary_addr) = replicate_from {
        println!("  Replicating:  from {primary_addr}");
    } else {
        println!("  Repl port:    {replication_port}");
    }
    if metrics_enabled {
        println!("  Metrics:      http://{host}:{metrics_port}{metrics_endpoint}");
    } else {
        println!("  Metrics:      disabled");
    }
    if resp_port > 0 {
        println!("  RESP port:    {resp_port}");
    }
    if s3_port > 0 {
        println!("  S3 port:      {s3_port}");
    }
    println!("  Connect:      psql -h {host} -p {port}");
    println!();

    tracing::info!("Listening on {addr}");

    let mut connection_tasks = tokio::task::JoinSet::new();

    loop {
        let (socket, peer_addr) = tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!("Accept error: {e}");
                        continue;
                    }
                }
            }
            _ = shutdown_notify.notified() => {
                tracing::info!("Accept loop exiting due to shutdown");
                break;
            }
        };

        // Acquire a connection slot from the pool.
        //
        // `try_acquire`, not `acquire`: awaiting a slot here would block the
        // accept loop for the whole acquire timeout (30 s by default) the
        // moment the limit is reached, so a single over-limit client would
        // stall every other connection — a limit that turns into an outage.
        // And a refused client gets a real `FATAL 53300` frame instead of a
        // silently dropped socket, which clients report as "server closed the
        // connection unexpectedly".
        let pool_ref = conn_pool.clone();
        let conn_id = match pool_ref.try_acquire(&peer_addr.to_string()).await {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(
                    "Rejected connection from {peer_addr}: {e} (limit {})",
                    config.server.max_connections
                );
                metrics.connections_rejected.inc();
                let limit = config.server.max_connections;
                tokio::spawn(async move {
                    nucleus::wire::overload::refuse_too_many_connections(socket, limit).await;
                });
                continue;
            }
        };

        // Register the connection with the drain coordinator. `None` means
        // shutdown already began between accept and here — refuse rather than
        // start work the drain would then have to wait for.
        let Some(inflight) = drain_for_accept.try_admit() else {
            tracing::debug!("Refusing connection from {peer_addr}: server is shutting down");
            pool_ref.release_with_metadata_cleanup(conn_id).await;
            continue;
        };

        let core = router.route();
        router.connection_started(core);
        tracing::debug!(
            "Connection from {peer_addr} -> core {} (slot {})",
            core.0,
            conn_id
        );
        let server_ref = server.clone();
        let tls_ref = tls_acceptor.clone();
        let metrics_ref = metrics.clone();
        let router_ref = router.clone();
        let handler_cleanup = handler_ref.clone();
        let peer_addr_str = peer_addr.to_string();
        metrics_ref.active_connections.inc();
        connection_tasks.spawn(async move {
            // Not pgwire's own `process_socket`: that one ignores Terminate and
            // never closes the socket, so a client waiting for the close waits
            // forever and this task's cleanup below never runs. See
            // `wire::process_socket_closing_on_terminate` — it goes away when
            // pgwire reaches 0.40.1.
            if let Err(e) =
                nucleus::wire::process_socket_closing_on_terminate(socket, tls_ref, server_ref)
                    .await
            {
                tracing::error!("Connection error from {peer_addr}: {e}");
            }
            // Clean up the per-connection session state.
            handler_cleanup.cleanup_session(&peer_addr_str);
            metrics_ref.active_connections.dec();
            router_ref.connection_ended(core);
            pool_ref.release_with_metadata_cleanup(conn_id).await;
            // Dropped last so the drain coordinator only considers this
            // connection finished after its cleanup has run. Dropping on a
            // panic unwind too, so a blown-up connection cannot wedge
            // shutdown.
            drop(inflight);
        });
    }

    // The signal handler owns the ordered shutdown sequence (drain → flush →
    // exit) and terminates the process itself. Returning from `main` here
    // would end the process *before* that flush ran — which is what used to
    // happen: this function's own 2 s drain finished first and dropped off the
    // end of `main`, so "Data flushed to disk successfully" never appeared on
    // a SIGTERM and durability rested entirely on the WAL.
    //
    // Wait for the handler to report persistence instead. The wait is bounded
    // by the same deadline as the handler's hard-exit watchdog, so a wedged
    // flush still cannot hang the process. Draining is *not* repeated here:
    // two sequential 2 s drains would nearly exhaust the 5 s budget.
    let persist_deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(SHUTDOWN_DEADLINE_SECS);
    while !drain.is_persisted() && std::time::Instant::now() < persist_deadline {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    if !drain.is_persisted() {
        tracing::warn!(
            "Exiting without a confirmed flush: shutdown exceeded {SHUTDOWN_DEADLINE_SECS}s"
        );
    }
    connection_tasks.abort_all();
}

/// Process an incoming cluster message (JoinCluster, Heartbeat, Raft RPCs, ForwardDml, etc.).
async fn handle_cluster_message(
    cluster: &Arc<parking_lot::RwLock<nucleus::distributed::ClusterCoordinator>>,
    transport: &Arc<TcpTransport>,
    env: nucleus::transport::Envelope,
    local_address: &str,
    executor: &Arc<Executor>,
    replicator: Option<&Arc<nucleus::distributed::RaftReplicator>>,
) {
    use nucleus::transport::Message;

    match env.message {
        // ── Raft consensus RPCs — dispatch to the replicator ─────────────────
        Message::RequestVote { .. }
        | Message::RequestVoteResponse { .. }
        | Message::AppendEntries { .. }
        | Message::AppendEntriesResponse { .. } => {
            if let Some(rep) = replicator {
                if let Some((to, reply)) = rep.handle_raft_message(&env.message, env.from).await {
                    let _ = transport.send_message(to, reply).await;
                }
            } else {
                tracing::debug!("Raft message from {} ignored (no replicator)", env.from);
            }
        }

        Message::JoinCluster { node_id, address } => {
            tracing::info!("Node {node_id:#x} requesting to join from {address}");

            // Synchronous lock work — no awaits while guard is held
            let (self_node_id, node_count, epoch) = {
                let mut coord = cluster.write();
                coord.add_node(node_id, address.clone());
                let s = coord.status();
                (s.node_id, s.node_count, s.epoch)
            };

            // Async work — register peer and send response
            transport.register_peer(node_id, &address).await;

            // Inform the Raft replicator about the new peer.
            if let Some(rep) = replicator {
                rep.add_peer(node_id, address.clone()).await;
            }

            let response = Message::JoinClusterResponse {
                success: true,
                cluster_nodes: vec![
                    (self_node_id, local_address.to_string()),
                    (node_id, address),
                ],
            };
            if let Err(e) = transport.send_message(node_id, response).await {
                tracing::error!("Failed to send join response to {node_id:#x}: {e}");
            } else {
                tracing::info!(
                    "Node {node_id:#x} joined cluster (now {node_count} nodes, epoch {epoch})"
                );
            }
        }
        Message::Heartbeat { node_id, term } => {
            tracing::trace!("Heartbeat from {node_id:#x} (term={term})");
            let (our_id, our_epoch) = {
                let coord = cluster.read();
                (coord.status().node_id, coord.epoch())
            };
            let _ = transport
                .send_message(
                    node_id,
                    Message::HeartbeatResponse {
                        node_id: our_id,
                        term: our_epoch,
                    },
                )
                .await;
        }
        Message::HeartbeatResponse { node_id, term } => {
            tracing::trace!("HeartbeatResponse from {node_id:#x} (term={term})");
        }
        Message::ForwardQuery { query, shard_id } => {
            tracing::debug!(
                "ForwardQuery from {}: shard={shard_id} query={query}",
                env.from
            );
            match executor.execute_principal_less_forward(&query).await {
                Ok(results) => {
                    // Serialize result rows as JSON-encoded bytes
                    let mut encoded_rows = Vec::new();
                    for result in &results {
                        if let nucleus::executor::ExecResult::Select { rows, .. } = result {
                            for row in rows {
                                let json = serde_json::to_vec(
                                    &row.iter().map(|v| format!("{v}")).collect::<Vec<_>>(),
                                )
                                .unwrap_or_default();
                                encoded_rows.push(json);
                            }
                        }
                    }
                    let _ = transport
                        .send_message(
                            env.from,
                            Message::ForwardQueryResponse {
                                success: true,
                                rows: encoded_rows,
                                error: None,
                            },
                        )
                        .await;
                }
                Err(e) => {
                    let _ = transport
                        .send_message(
                            env.from,
                            Message::ForwardQueryResponse {
                                success: false,
                                rows: vec![],
                                error: Some(e.to_string()),
                            },
                        )
                        .await;
                }
            }
        }
        Message::ForwardDml { sql, shard_id: _ } => {
            tracing::debug!("ForwardDml from {}: sql={sql}", env.from);
            let request_id = env.id; // Preserve for send_request() correlation.
            let from = env.from;
            let (response_msg, self_id) = match executor.execute_principal_less_forward(&sql).await
            {
                Ok(results) => {
                    let rows_affected: usize = results
                        .iter()
                        .filter_map(|r| {
                            if let nucleus::executor::ExecResult::Command {
                                rows_affected, ..
                            } = r
                            {
                                Some(*rows_affected)
                            } else {
                                None
                            }
                        })
                        .sum();
                    (
                        Message::ForwardDmlResponse {
                            success: true,
                            rows_affected,
                            error: None,
                        },
                        transport.local_node_id(),
                    )
                }
                Err(e) => (
                    Message::ForwardDmlResponse {
                        success: false,
                        rows_affected: 0,
                        error: Some(e.to_string()),
                    },
                    transport.local_node_id(),
                ),
            };
            // Reply with the same envelope ID so send_request() can correlate it.
            let reply_envelope = nucleus::transport::Envelope {
                id: request_id,
                from: self_id,
                to: from,
                message: response_msg,
            };
            let _ = transport.send(from, &reply_envelope).await;
        }
        other => {
            tracing::debug!("Unhandled cluster message from {}: {:?}", env.from, other);
        }
    }
}

// Helper: empty replicator for the existing call site that predates the replicator.
// (Removed — the call site now always passes `Some(&replicator_for_recv)`.)

fn cmd_init(data: PathBuf) {
    if data.exists() {
        eprintln!("Data directory already exists: {}", data.display());
        std::process::exit(1);
    }

    std::fs::create_dir_all(&data).expect("failed to create data directory");
    println!("Initialized Nucleus data directory: {}", data.display());
    println!("Start with: nucleus start --data {}", data.display());
}

fn cmd_backup(data: PathBuf, output: PathBuf, force: bool, online: bool, allow_in_use: bool) {
    let version = env!("CARGO_PKG_VERSION");
    let result = if online {
        backup_online_via_engine(&data, &output, force, version, allow_in_use)
    } else {
        nucleus::backup::backup_data_dir_opts(&data, &output, force, version, allow_in_use)
    };
    match result {
        Ok(manifest) => {
            println!(
                "Backup complete: {} -> {}",
                data.display(),
                output.display()
            );
            println!("  Nucleus version: {}", manifest.nucleus_version);
            println!("  On-disk format:  v{}", manifest.format_version);
            println!("  Database id:     {}", manifest.database_id);
            println!(
                "  Files:           {} (BLAKE3 checksummed)",
                manifest.files.len()
            );
            if manifest.online {
                println!(
                    "  Consistency:     online, consistent through LSN {}",
                    manifest.consistent_lsn
                );
            } else if manifest.taken_while_in_use {
                println!(
                    "  Consistency:     NONE — copied while the database was in use. \
                     This snapshot may be torn and is recorded as such in its manifest."
                );
            } else {
                println!("  Consistency:     offline copy of a quiesced directory");
            }
            if manifest.encryption.encrypted {
                println!(
                    "  At rest:         encrypted ({}) — restoring needs the same key",
                    manifest
                        .encryption
                        .algorithm
                        .as_deref()
                        .unwrap_or("unknown")
                );
            }
            println!(
                "  Restore with: nucleus restore --input {} --data <dir>",
                output.display()
            );
        }
        Err(e) => {
            eprintln!("Backup failed: {e}");
            std::process::exit(1);
        }
    }
}

/// Take a coordinated online snapshot by opening the data directory's SQL
/// engine ourselves: checkpoint, pin WAL retention, copy the data file with
/// every page slot validated, then cut the WAL at a named LSN.
///
/// Opt-in rather than the default for one reason: this process must open the
/// data file, and it has no way to know an encrypted or compressed database's
/// settings from the outside. Guessing wrong is caught by page checksums (the
/// open fails loudly rather than snapshotting garbage), but a plain directory
/// copy needs no such guess, so it stays the default.
fn backup_online_via_engine(
    data: &std::path::Path,
    output: &std::path::Path,
    force: bool,
    version: &str,
    allow_in_use: bool,
) -> std::io::Result<nucleus::backup::BackupManifest> {
    use nucleus::backup::{DataDirLock, backup_data_dir_opts, backup_online};

    if !data.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("data directory does not exist: {}", data.display()),
        ));
    }

    // Take the directory lock for the duration of the backup. Failing to get
    // it means a live instance owns the directory.
    let _lock = match DataDirLock::acquire(data)? {
        Some(lock) => lock,
        None => {
            if allow_in_use {
                eprintln!(
                    "warning: {} is open by a running instance; taking an INCONSISTENT copy \
                     because the in-use override was given",
                    data.display()
                );
                return backup_data_dir_opts(data, output, force, version, true);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                format!(
                    "{} is open by a running Nucleus instance. A snapshot taken from outside \
                     that instance would be TORN. To back up a serving database, have the \
                     server snapshot itself: connect and run `BACKUP DATABASE TO '<path>'` \
                     (superuser; destination outside the data directory). Otherwise stop the \
                     server and re-run, or pass --allow-in-use to accept an inconsistent copy \
                     (it is recorded as inconsistent in the manifest).",
                    data.display()
                ),
            ));
        }
    };

    let db_path = data.join("nucleus.db");
    if !db_path.is_file() {
        // No SQL data file (fresh or non-SQL directory) — nothing to
        // coordinate with; a plain copy is exactly right and is honest about
        // having no LSN consistency point.
        return backup_data_dir_opts(data, output, force, version, false);
    }

    let catalog = Arc::new(Catalog::new());
    let engine = DiskEngine::open_segmented(
        &db_path,
        catalog,
        nucleus::storage::buffer::DEFAULT_POOL_SIZE,
        64,
    )
    .map_err(|e| {
        std::io::Error::other(format!(
            "could not open {} for an online backup: {e}. If this database is encrypted or \
                 compressed, take the backup without --online (the plain copy needs no key).",
            db_path.display()
        ))
    })?;
    backup_online(data, output, force, version, &engine)
}

#[allow(clippy::too_many_arguments)]
fn cmd_restore_pitr(
    base: PathBuf,
    archive: PathBuf,
    data: PathBuf,
    db_file: String,
    lsn: Option<u64>,
    time: Option<u64>,
    force: bool,
) {
    if lsn.is_some() && time.is_some() {
        eprintln!("PITR restore: pass at most one of --lsn / --time");
        std::process::exit(1);
    }
    let target = match (lsn, time) {
        (Some(n), _) => nucleus::pitr::PitrTarget::Lsn(n),
        (_, Some(t)) => nucleus::pitr::PitrTarget::UnixSeconds(t),
        (None, None) => nucleus::pitr::PitrTarget::Latest,
    };
    match nucleus::pitr::restore_pitr(
        &base,
        &archive,
        target,
        &data,
        &db_file,
        env!("CARGO_PKG_VERSION"),
        force,
    ) {
        Ok(report) => {
            println!(
                "PITR restore complete: base {} + archive {} -> {}",
                base.display(),
                archive.display(),
                data.display()
            );
            println!(
                "  Replayed to LSN {} ({} WAL segment(s) reconstructed)",
                report.restored_lsn, report.segments_written
            );
            if report.target_lsn != u64::MAX && report.restored_lsn < report.target_lsn {
                println!(
                    "  Note: archive ended at LSN {} — target {} was beyond the archived history",
                    report.restored_lsn, report.target_lsn
                );
            }
            // State the recovery point in wall-clock terms, and say plainly
            // what is not in it. An LSN alone reads as success: the operator
            // has no way to tell that commits made after the last segment was
            // archived are simply absent, and this is being read during an
            // incident, when a silent gap is most expensive.
            match report.recovery_point_unix {
                Some(unix) => {
                    let when = chrono::DateTime::from_timestamp(unix as i64, 0)
                        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| format!("unix {unix}"));
                    println!(
                        "  Recovery point: {when} — commits after this are NOT in the archive"
                    );
                }
                None => {
                    println!(
                        "  Recovery point: unknown (archive index missing or unreadable). \
                         Commits made after the last segment was archived are NOT included."
                    );
                }
            }
            println!(
                "  A segment reaches the archive when it fills, on the archive timeout \
                 (NUCLEUS_WAL_ARCHIVE_TIMEOUT_SECS), or at a clean shutdown. Anything \
                 written after the last such point was never archived and cannot be replayed."
            );
            // The same discipline as the recovery point above, for the other
            // thing this restore does not do. Replay reconstructs the SQL
            // substrate's page WAL; the specialty-model logs come from the base
            // snapshot as a byte copy and are not advanced. Restoring to a
            // target after the base therefore leaves SQL at the target and
            // these models at the base, and until now the command said nothing,
            // so a partial restore printed exactly like a complete one.
            if !report.specialty_logs_at_base.is_empty() {
                println!(
                    "  NOT replayed: {} specialty-model log(s) restored at the BASE snapshot's \
                     point, not the target — {}",
                    report.specialty_logs_at_base.len(),
                    report.specialty_logs_at_base.join(", ")
                );
                println!(
                    "  SQL is at LSN {}; those models are at the base. If the target is after \
                     the base, they are stale relative to the relational data. Cross-model PITR \
                     is DATABASE_COMPLETION.md M4 (NU-030) and is not implemented.",
                    report.restored_lsn
                );
            }
            println!("  Start with: nucleus start --data {}", data.display());
        }
        Err(e) => {
            eprintln!("PITR restore failed: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_prune_archive(archive: PathBuf, keep_from_lsn: u64, dry_run: bool) {
    if dry_run {
        // Read-only: report the same decision the real run would make, without
        // touching anything. Worth having as the default habit -- every other
        // destructive path here refuses a dirty target or stages through a temp
        // file, and this one deletes recovery data outright.
        match nucleus::storage::wal::plan_prune_archive(&archive, keep_from_lsn) {
            Ok(plan) => {
                println!(
                    "Dry run: {} segment(s) would be removed, {} kept (horizon LSN {keep_from_lsn})",
                    plan.removed.len(),
                    plan.kept
                );
                if !plan.removed.is_empty() {
                    println!("  Would remove: {:?}", plan.removed);
                }
                if !plan.skipped_unreadable.is_empty() {
                    println!(
                        "  Kept because their LSN range could not be read: {:?}",
                        plan.skipped_unreadable
                    );
                }
                match plan.oldest_retained_lsn {
                    Some(lsn) => println!(
                        "  After this the archive could serve a restore back to LSN {lsn}, no further."
                    ),
                    None => println!(
                        "  WARNING: this would empty the archive. Nothing could be replayed \
                         onto a base snapshot afterwards."
                    ),
                }
            }
            Err(e) => {
                eprintln!("Prune archive failed: {e}");
                std::process::exit(1);
            }
        }
        return;
    }
    match nucleus::storage::wal::prune_archive(&archive, keep_from_lsn) {
        Ok(report) => {
            println!(
                "Pruned {}: removed {} segment(s), freed {} bytes, kept {}",
                archive.display(),
                report.removed.len(),
                report.bytes_freed,
                report.kept
            );
            if !report.skipped_unreadable.is_empty() {
                println!(
                    "  Kept because their LSN range could not be read: {:?}",
                    report.skipped_unreadable
                );
            }
            match report.oldest_retained_lsn {
                Some(lsn) => {
                    println!("  The archive can now serve a restore back to LSN {lsn}, no further.")
                }
                None => println!(
                    "  WARNING: the archive is now empty. Nothing can be replayed onto a base \
                     snapshot."
                ),
            }
        }
        Err(e) => {
            eprintln!("Prune archive failed: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_restore(input: PathBuf, data: PathBuf, force: bool) {
    match nucleus::backup::restore_data_dir(&input, &data, force, env!("CARGO_PKG_VERSION")) {
        Ok(_) => {
            println!(
                "Restore complete: {} -> {}",
                input.display(),
                data.display()
            );
            println!("  Start with: nucleus start --data {}", data.display());
        }
        Err(e) => {
            eprintln!("Restore failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_dump(data: PathBuf, output: Option<PathBuf>) {
    let executor = match nucleus::executor::open_persistent_executor(&data).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Dump failed to open '{}': {e}", data.display());
            std::process::exit(1);
        }
    };
    let script = match executor.dump_logical().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Dump failed: {e}");
            std::process::exit(1);
        }
    };
    match output {
        Some(path) => match std::fs::write(&path, &script) {
            Ok(()) => println!("Logical dump written to {}", path.display()),
            Err(e) => {
                eprintln!("Dump failed to write '{}': {e}", path.display());
                std::process::exit(1);
            }
        },
        None => print!("{script}"),
    }
}

async fn cmd_load(input: PathBuf, data: PathBuf) {
    let script = match std::fs::read_to_string(&input) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Load failed to read '{}': {e}", input.display());
            std::process::exit(1);
        }
    };
    let executor = match nucleus::executor::open_persistent_executor(&data).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Load failed to open '{}': {e}", data.display());
            std::process::exit(1);
        }
    };
    match executor.restore_logical(&script).await {
        Ok(()) => {
            println!("Logical dump restored into {}", data.display());
            println!("  Start with: nucleus start --data {}", data.display());
        }
        Err(e) => {
            eprintln!("Load failed: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_version() {
    println!("Nucleus {}", env!("CARGO_PKG_VERSION"));
    println!("The Definitive Database");
    println!();
    println!("Features: OLTP, columnar, document, graph, KV, cache, pub/sub,");
    println!("          full-text search, geospatial, vectors, sparse vectors,");
    println!("          time-series, tensor, blob storage, CDC, reactive subs,");
    println!("          stored procedures, database branching, data versioning,");
    println!("          Raft consensus, sharding, replication, TLS, encryption.");
    println!();
    println!("Protocol: PostgreSQL wire protocol (psql, any Postgres client/ORM)");
}

fn env_var_truthy(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn parse_auth_method_env(value: &str) -> Option<AuthMethod> {
    match value.trim().to_ascii_lowercase().as_str() {
        "scram-sha-256" | "scram_sha_256" | "scram" => Some(AuthMethod::ScramSha256),
        "cleartext" | "password" | "plain" => Some(AuthMethod::Cleartext),
        _ => None,
    }
}

fn load_internal_tls_from_env() -> Result<Option<nucleus::tls::InternalTlsConfig>, String> {
    if !env_var_truthy("NUCLEUS_INTERNAL_TLS") {
        return Ok(None);
    }

    let cert = std::env::var("NUCLEUS_INTERNAL_TLS_CERT")
        .map_err(|_| "NUCLEUS_INTERNAL_TLS=1 requires NUCLEUS_INTERNAL_TLS_CERT".to_string())?;
    let key = std::env::var("NUCLEUS_INTERNAL_TLS_KEY")
        .map_err(|_| "NUCLEUS_INTERNAL_TLS=1 requires NUCLEUS_INTERNAL_TLS_KEY".to_string())?;
    let ca = std::env::var("NUCLEUS_INTERNAL_TLS_CA")
        .map_err(|_| "NUCLEUS_INTERNAL_TLS=1 requires NUCLEUS_INTERNAL_TLS_CA".to_string())?;
    let server_name = std::env::var("NUCLEUS_INTERNAL_TLS_SERVER_NAME")
        .unwrap_or_else(|_| "localhost".to_string());
    let cert_path = PathBuf::from(cert);
    let key_path = PathBuf::from(key);
    let ca_path = PathBuf::from(ca);

    nucleus::tls::load_internal_tls_config(
        cert_path.as_path(),
        key_path.as_path(),
        ca_path.as_path(),
        server_name,
    )
    .map(Some)
    .map_err(|e| e.to_string())
}

/// Read anonymous RSS (heap + stack) from /proc/self/status.
/// Excludes file-backed pages (WAL, segment files, shared libs) which the
/// kernel reclaims automatically. Returns 0 on non-Linux platforms.
fn read_rss_bytes() -> u64 {
    let Ok(contents) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("RssAnon:") {
            let trimmed = rest.trim().trim_end_matches(" kB").trim();
            if let Ok(kb) = trimmed.parse::<u64>() {
                return kb * 1024;
            }
        }
    }
    0
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn normalize_metrics_endpoint(endpoint: &str) -> String {
    if endpoint.is_empty() {
        "/metrics".to_string()
    } else if endpoint.starts_with('/') {
        endpoint.to_string()
    } else {
        format!("/{endpoint}")
    }
}

/// Minimal HTTP server for Prometheus metrics endpoint.
async fn serve_metrics_http(metrics: Arc<MetricsRegistry>, port: u16, endpoint: String) {
    let addr = format!("127.0.0.1:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!("Failed to bind metrics HTTP on {addr}: {e}");
            return;
        }
    };
    tracing::info!("Metrics HTTP endpoint on http://{addr}{endpoint}");

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(_) => continue,
        };
        let metrics_ref = metrics.clone();
        let endpoint_ref = endpoint.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buf[..n]);
            let request_prefix = format!("GET {endpoint_ref} ");

            // Only respond to GET configured metrics endpoint.
            let body = if request.starts_with(&request_prefix) {
                metrics_ref.render_prometheus()
            } else {
                "404 Not Found\r\n".to_string()
            };

            let status = if body.starts_with("404") {
                "404 Not Found"
            } else {
                "200 OK"
            };
            let content_type = if status == "200 OK" {
                "text/plain; version=0.0.4; charset=utf-8"
            } else {
                "text/plain"
            };

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

async fn cmd_status(host: &str) {
    println!("Checking Nucleus at {host}...");

    // Try to connect via TCP to check if the server is running
    match tokio::net::TcpStream::connect(host).await {
        Ok(_) => {
            println!("Nucleus is running at {host}");
        }
        Err(e) => {
            eprintln!("Cannot connect to Nucleus at {host}: {e}");
            std::process::exit(1);
        }
    }
}

/// One-shot shell: execute a single SQL statement and exit (`shell -c`).
/// Prints SELECT results as a table (or JSON with --json), command tags as-is.
/// Exits 1 on connection failure or statement error so scripts can gate on it.
async fn cmd_shell_exec(host: &str, port: u16, sql: &str, json: bool) {
    use nucleus::cli::{PgClient, QueryResult, TableDisplay};

    let mut client = match PgClient::connect(host, port).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect: {e}");
            std::process::exit(1);
        }
    };

    let sql = sql.trim().trim_end_matches(';');
    let mut failed = false;
    match client.simple_query(sql).await {
        Ok(QueryResult::Select { columns, rows }) => {
            if json {
                // JSON array of objects keyed by column name; all values are
                // the wire's text rendering (KV/FTS results are text anyway).
                let objs: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|row| {
                        let mut obj = serde_json::Map::new();
                        for (i, col) in columns.iter().enumerate() {
                            let v = row
                                .get(i)
                                .map(|s| serde_json::Value::String(s.clone()))
                                .unwrap_or(serde_json::Value::Null);
                            obj.insert(col.clone(), v);
                        }
                        serde_json::Value::Object(obj)
                    })
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string(&objs).unwrap_or_else(|_| "[]".into())
                );
            } else {
                let display = TableDisplay::new(columns, rows);
                println!("{}", display.format());
            }
        }
        Ok(QueryResult::Command { tag }) => {
            if json {
                println!("{}", serde_json::json!({ "tag": tag }));
            } else if tag.is_empty() {
                println!("OK");
            } else {
                println!("{tag}");
            }
        }
        Ok(QueryResult::Error { message }) => {
            eprintln!("ERROR: {message}");
            failed = true;
        }
        Err(e) => {
            eprintln!("Error: {e}");
            failed = true;
        }
    }

    if let Err(e) = client.close().await {
        eprintln!("Warning: disconnect error: {e}");
    }
    if failed {
        std::process::exit(1);
    }
}

/// Interactive SQL shell with rustyline line editing, history, and multi-line support.
///
/// Uses the PgClient from the cli module for clean wire protocol handling
/// and TableDisplay for psql-style aligned output.
async fn cmd_shell(host: &str, port: u16) {
    use nucleus::cli::{self, MetaCommand, PgClient};

    println!("Connecting to Nucleus at {host}:{port}...");

    let mut client = match PgClient::connect(host, port).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect: {e}");
            std::process::exit(1);
        }
    };

    println!("Connected to Nucleus.");
    println!("Type \\q to quit, \\? for help, \\timing to toggle timing.");
    println!();

    // Set up rustyline with history
    let mut rl = rustyline::DefaultEditor::new().expect("failed to create editor");

    // Load history (cross-platform home directory)
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    let history_path = home.join(".nucleus_history");
    let _ = rl.load_history(&history_path);

    let mut show_timing = false;
    let mut sql_buffer = String::new();

    loop {
        // Show a different prompt when accumulating multi-line SQL
        let prompt = if sql_buffer.is_empty() {
            "nucleus> "
        } else {
            "      -> "
        };

        let line = match rl.readline(prompt) {
            Ok(line) => line,
            Err(rustyline::error::ReadlineError::Interrupted) => {
                // Ctrl-C: clear current buffer, start fresh
                if !sql_buffer.is_empty() {
                    sql_buffer.clear();
                    println!("Query cleared.");
                }
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                // Ctrl-D: exit
                println!();
                break;
            }
            Err(_) => break,
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Meta-commands are only recognized at the start of a new statement
        if sql_buffer.is_empty() && trimmed.starts_with('\\') {
            let _ = rl.add_history_entry(&line);

            let cmd = cli::parse_meta_command(trimmed);
            match cmd {
                MetaCommand::Quit => break,
                MetaCommand::Help => {
                    println!("{}", cli::help_text());
                    println!();
                    continue;
                }
                MetaCommand::ToggleTiming => {
                    show_timing = !show_timing;
                    println!("Timing is {}.", if show_timing { "on" } else { "off" });
                    continue;
                }
                MetaCommand::Unknown(ref s) => {
                    eprintln!("Unknown command: {s}");
                    eprintln!("Type \\? for help.");
                    continue;
                }
                _ => {
                    // Commands that map to SQL (ListTables, DescribeTable, ShowStatus)
                    if let Some(sql) = cli::meta_command_to_sql(&cmd) {
                        shell_execute_and_display(&mut client, &sql, show_timing).await;
                    }
                    continue;
                }
            }
        }

        // Handle "exit" / "quit" keywords at statement start
        if sql_buffer.is_empty()
            && (trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit"))
        {
            break;
        }

        // Accumulate into the SQL buffer for multi-line support
        if !sql_buffer.is_empty() {
            sql_buffer.push(' ');
        }
        sql_buffer.push_str(trimmed);

        // Check if the statement is complete (ends with semicolon)
        if sql_buffer.ends_with(';') {
            let sql = sql_buffer.trim_end_matches(';').trim().to_string();
            let _ = rl.add_history_entry(&sql_buffer);
            sql_buffer.clear();

            if sql.is_empty() {
                continue;
            }

            shell_execute_and_display(&mut client, &sql, show_timing).await;
        }
    }

    // Save history
    let _ = rl.save_history(&history_path);

    // Graceful disconnect
    if let Err(e) = client.close().await {
        eprintln!("Warning: disconnect error: {e}");
    }

    println!("Bye.");
}

/// Execute a SQL query via the PgClient and display the result using TableDisplay.
async fn shell_execute_and_display(
    client: &mut nucleus::cli::PgClient,
    sql: &str,
    show_timing: bool,
) {
    use nucleus::cli::{QueryResult, TableDisplay};

    let start = std::time::Instant::now();

    match client.simple_query(sql).await {
        Ok(QueryResult::Select { columns, rows }) => {
            let display = TableDisplay::new(columns, rows);
            println!("{}", display.format());
        }
        Ok(QueryResult::Command { tag }) => {
            if tag.is_empty() {
                println!("OK");
            } else {
                println!("{tag}");
            }
        }
        Ok(QueryResult::Error { message }) => {
            eprintln!("ERROR: {message}");
        }
        Err(e) => {
            eprintln!("Error: {e}");
        }
    }

    if show_timing {
        println!("Time: {:.3} ms", start.elapsed().as_secs_f64() * 1000.0);
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_auth_method_env_scram_variants() {
        assert_eq!(
            parse_auth_method_env("scram"),
            Some(AuthMethod::ScramSha256)
        );
        assert_eq!(
            parse_auth_method_env("SCRAM-SHA-256"),
            Some(AuthMethod::ScramSha256)
        );
    }

    #[test]
    fn parse_auth_method_env_cleartext_variants() {
        assert_eq!(
            parse_auth_method_env("cleartext"),
            Some(AuthMethod::Cleartext)
        );
        assert_eq!(
            parse_auth_method_env("password"),
            Some(AuthMethod::Cleartext)
        );
    }

    #[test]
    fn parse_auth_method_env_unknown_returns_none() {
        assert_eq!(parse_auth_method_env("invalid"), None);
    }
}
