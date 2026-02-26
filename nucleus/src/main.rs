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
        }) => {
            cmd_start(
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
            )
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
        Some(Commands::Shell { host, port }) => {
            cmd_shell(&host, port).await;
        }
        None => {
            // Default: start in server mode (same as `nucleus start`)
            cmd_start(
                5432,
                "127.0.0.1".into(),
                PathBuf::from("nucleus_data"),
                false,
                None,
                None,
                None,
                5434,
                None,
                None,
                false,
                None,
                None,
                None,
                5433,
                false,
                false,
            )
            .await;
        }
    }
}

// ============================================================================
// Commands
// ============================================================================

async fn cmd_start(
    port: u16,
    host: String,
    data: PathBuf,
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
) {
    // Load config early so we can use logging.level for tracing
    let config_path = data.join("nucleus.toml");
    let mut config = match NucleusConfig::load(&config_path) {
        Ok(cfg) => {
            eprintln!("Loaded config from {}", config_path.display());
            cfg
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
    );

    // Configure tracing with config-driven log level
    let log_directive = format!("nucleus={}", config.logging.level);
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive(
                log_directive
                    .parse()
                    .unwrap_or_else(|_| "nucleus=info".parse().unwrap()),
            ),
        )
        .init();

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

    if let Some(ref region) = region {
        tracing::info!("Region: {region}");
    }

    tracing::info!("Log level: {}", config.logging.level);

    // Set up storage
    let catalog = Arc::new(Catalog::new());

    // Keep a separate Arc<DiskEngine> for shutdown flushing
    let disk_engine: Option<Arc<DiskEngine>>;

    let storage: Arc<dyn StorageEngine> = if memory {
        tracing::info!("Storage: in-memory with MVCC snapshot isolation");
        disk_engine = None;
        Arc::new(MvccStorageAdapter::new())
    } else {
        // Ensure data directory exists
        if !data.exists() {
            std::fs::create_dir_all(&data).expect("failed to create data directory");
            tracing::info!("Created data directory: {}", data.display());
        }

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
                    if let Err(e) = std::fs::write(&salt_path, &salt) {
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

        // Re-register tables restored from catalog so DiskEngine knows about them
        for table_name in catalog.table_names().await {
            if let Err(e) = engine.create_table(&table_name).await {
                tracing::warn!("Failed to re-register table {table_name}: {e}");
            }
        }

        disk_engine = Some(engine.clone());

        // Wrap DiskEngine in BufferedDiskEngine for transaction atomicity + rollback
        let buffered = Arc::new(BufferedDiskEngine::new(engine));
        tracing::info!("Transaction support: buffered write-ahead (atomicity + rollback)");
        buffered as Arc<dyn StorageEngine>
    };

    // Set up shared metrics registry
    let metrics = Arc::new(MetricsRegistry::new());

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
    let executor = Arc::new(
        Executor::new_with_persistence(catalog, storage, catalog_path)
            .with_cache_size(cache_bytes)
            .with_metrics(metrics.clone())
            .with_replication(replication.clone())
            .with_conn_pool(conn_pool.clone())
            .with_cluster(cluster.clone()),
    );
    tracing::info!("Cache: {} MB", config.cache.max_memory_mb);

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
    }
    let handler = Arc::new(NucleusHandler::with_password_and_method(
        executor.clone(),
        resolved_password,
        resolved_auth_method,
    ));
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

    // Spawn cluster message receive loop (handles JoinCluster from new nodes, heartbeats, etc.)
    let transport_for_recv = transport.clone();
    let cluster_for_recv = cluster.clone();
    let executor_for_recv = executor.clone();
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

    // Spawn cluster heartbeat loop (every 2s, send heartbeats to all known peers)
    let transport_for_hb = transport.clone();
    let cluster_for_hb = cluster.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let (peer_ids, term) = {
                let coord = cluster_for_hb.read();
                (coord.peer_node_ids(), coord.epoch())
            };
            if peer_ids.is_empty() {
                continue; // No peers to heartbeat
            }
            // Heartbeat is best-effort; errors are ignored
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
                    nucleus::background::BackgroundTask::BufferFlush
                    | nucleus::background::BackgroundTask::WalCheckpoint => {
                        if let Some(ref engine) = disk_for_workers {
                            if let Err(e) = engine.flush() {
                                tracing::warn!("Background flush failed: {e}");
                            }
                        }
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

    // Graceful shutdown handler: flush dirty pages on Ctrl+C / SIGTERM
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let shutdown_for_handler = shutdown_notify.clone();
    let disk_for_shutdown = disk_engine.clone();
    let workers_for_shutdown = workers.clone();
    let runtime_for_shutdown = runtime.clone();
    let transport_for_shutdown = transport.clone();
    tokio::spawn(async move {
        // Wait for either Ctrl+C (SIGINT) or SIGTERM (on Unix).
        #[cfg(unix)]
        {
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            ).expect("failed to register SIGTERM handler");
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
        if let Some(ref engine) = disk_for_shutdown {
            match engine.flush() {
                Ok(()) => tracing::info!("Data flushed to disk successfully"),
                Err(e) => tracing::error!("Failed to flush data on shutdown: {e}"),
            }
        }
        tracing::info!("Nucleus stopped.");
        shutdown_for_handler.notify_one();
    });

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

        // Acquire a connection slot from the pool
        let pool_ref = conn_pool.clone();
        let conn_id = match pool_ref.acquire(&peer_addr.to_string()).await {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!("Rejected connection from {peer_addr}: {e}");
                drop(socket);
                continue;
            }
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
            if let Err(e) = pgwire::tokio::process_socket(socket, tls_ref, server_ref).await {
                tracing::error!("Connection error from {peer_addr}: {e}");
            }
            // Clean up the per-connection session state.
            handler_cleanup.cleanup_session(&peer_addr_str);
            metrics_ref.active_connections.dec();
            router_ref.connection_ended(core);
            pool_ref.release_with_metadata_cleanup(conn_id).await;
        });
    }

    // Drain in-flight connections with a timeout
    if !connection_tasks.is_empty() {
        let active = connection_tasks.len();
        tracing::info!("Waiting for {active} in-flight connection(s) to finish (5s timeout)...");
        let drain_deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
        tokio::pin!(drain_deadline);
        loop {
            tokio::select! {
                result = connection_tasks.join_next() => {
                    match result {
                        Some(_) => {
                            if connection_tasks.is_empty() {
                                tracing::info!("All connections drained");
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = &mut drain_deadline => {
                    let remaining = connection_tasks.len();
                    tracing::warn!("Shutdown timeout: aborting {remaining} remaining connection(s)");
                    connection_tasks.abort_all();
                    break;
                }
            }
        }
    }
}

/// Process an incoming cluster message (JoinCluster, Heartbeat, ForwardQuery, etc.).
async fn handle_cluster_message(
    cluster: &Arc<parking_lot::RwLock<nucleus::distributed::ClusterCoordinator>>,
    transport: &Arc<TcpTransport>,
    env: nucleus::transport::Envelope,
    local_address: &str,
    executor: &Arc<Executor>,
) {
    use nucleus::transport::Message;

    match env.message {
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
            match executor.execute(&query).await {
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
            match executor.execute(&sql).await {
                Ok(results) => {
                    let rows_affected: usize = results
                        .iter()
                        .filter_map(|r| {
                            if let nucleus::executor::ExecResult::Command { rows_affected, .. } = r {
                                Some(*rows_affected)
                            } else {
                                None
                            }
                        })
                        .sum();
                    let _ = transport
                        .send_message(
                            env.from,
                            Message::ForwardDmlResponse {
                                success: true,
                                rows_affected,
                                error: None,
                            },
                        )
                        .await;
                }
                Err(e) => {
                    let _ = transport
                        .send_message(
                            env.from,
                            Message::ForwardDmlResponse {
                                success: false,
                                rows_affected: 0,
                                error: Some(e.to_string()),
                            },
                        )
                        .await;
                }
            }
        }
        other => {
            tracing::debug!("Unhandled cluster message from {}: {:?}", env.from, other);
        }
    }
}

fn cmd_init(data: PathBuf) {
    if data.exists() {
        eprintln!("Data directory already exists: {}", data.display());
        std::process::exit(1);
    }

    std::fs::create_dir_all(&data).expect("failed to create data directory");
    println!("Initialized Nucleus data directory: {}", data.display());
    println!("Start with: nucleus start --data {}", data.display());
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
