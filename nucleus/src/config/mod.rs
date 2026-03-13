use serde::{Deserialize, Serialize};
use std::env;
use std::path::Path;

// ---------------------------------------------------------------------------
// ConfigError
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config file not found: {0}")]
    FileNotFound(String),
    #[error("TOML parse error: {0}")]
    ParseError(String),
    #[error("I/O error: {0}")]
    IoError(String),
}

// ---------------------------------------------------------------------------
// Sub-config structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    5432
}
fn default_max_connections() -> usize {
    100
}
fn default_idle_timeout_secs() -> u64 {
    300
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            max_connections: default_max_connections(),
            idle_timeout_secs: default_idle_timeout_secs(),
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_buffer_pool_size_mb")]
    pub buffer_pool_size_mb: usize,
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    #[serde(default)]
    pub use_direct_io: bool,
    #[serde(default)]
    pub memory_mode: bool,
}

fn default_data_dir() -> String {
    "nucleus_data".to_string()
}
fn default_buffer_pool_size_mb() -> usize {
    128
}
fn default_page_size() -> usize {
    16384
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            buffer_pool_size_mb: default_buffer_pool_size_mb(),
            page_size: default_page_size(),
            use_direct_io: false,
            memory_mode: false,
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_segment_size_mb")]
    pub segment_size_mb: usize,
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,
    #[serde(default = "default_group_commit_interval_us")]
    pub group_commit_interval_us: u64,
    #[serde(default = "default_sync_mode")]
    pub sync_mode: String,
}

fn default_true() -> bool {
    true
}
fn default_segment_size_mb() -> usize {
    64
}
fn default_checkpoint_interval_secs() -> u64 {
    300
}
fn default_group_commit_interval_us() -> u64 {
    1000
}
fn default_sync_mode() -> String {
    "fsync".to_string()
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            segment_size_mb: default_segment_size_mb(),
            checkpoint_interval_secs: default_checkpoint_interval_secs(),
            group_commit_interval_us: default_group_commit_interval_us(),
            sync_mode: default_sync_mode(),
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolConfig {
    #[serde(default = "default_min_idle")]
    pub min_idle: usize,
    #[serde(default = "default_max_idle_time_secs")]
    pub max_idle_time_secs: u64,
    #[serde(default = "default_max_lifetime_secs")]
    pub max_lifetime_secs: u64,
    #[serde(default = "default_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,
    #[serde(default = "default_validation_interval_secs")]
    pub validation_interval_secs: u64,
}

fn default_min_idle() -> usize {
    5
}
fn default_max_idle_time_secs() -> u64 {
    600
}
fn default_max_lifetime_secs() -> u64 {
    3600
}
fn default_acquire_timeout_secs() -> u64 {
    30
}
fn default_validation_interval_secs() -> u64 {
    60
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_idle: default_min_idle(),
            max_idle_time_secs: default_max_idle_time_secs(),
            max_lifetime_secs: default_max_lifetime_secs(),
            acquire_timeout_secs: default_acquire_timeout_secs(),
            validation_interval_secs: default_validation_interval_secs(),
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_cache_max_memory_mb")]
    pub max_memory_mb: usize,
    #[serde(default = "default_cache_ttl_secs")]
    pub default_ttl_secs: u64,
    #[serde(default = "default_eviction_policy")]
    pub eviction_policy: String,
}

fn default_cache_max_memory_mb() -> usize {
    64
}
fn default_cache_ttl_secs() -> u64 {
    300
}
fn default_eviction_policy() -> String {
    "lru".to_string()
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_memory_mb: default_cache_max_memory_mb(),
            default_ttl_secs: default_cache_ttl_secs(),
            eviction_policy: default_eviction_policy(),
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicationConfig {
    #[serde(default = "default_replication_mode")]
    pub mode: String,
    #[serde(default)]
    pub primary_host: Option<String>,
    #[serde(default)]
    pub primary_port: Option<u16>,
    #[serde(default = "default_replication_sync_mode")]
    pub sync_mode: String,
    #[serde(default = "default_failover_timeout_ms")]
    pub failover_timeout_ms: u64,
}

fn default_replication_mode() -> String {
    "standalone".to_string()
}
fn default_replication_sync_mode() -> String {
    "async".to_string()
}
fn default_failover_timeout_ms() -> u64 {
    5000
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: default_replication_mode(),
            primary_host: None,
            primary_port: None,
            sync_mode: default_replication_sync_mode(),
            failover_timeout_ms: default_failover_timeout_ms(),
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    #[serde(default = "default_metrics_endpoint")]
    pub endpoint: String,
}

fn default_metrics_port() -> u16 {
    9100
}
fn default_metrics_endpoint() -> String {
    "/metrics".to_string()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_metrics_port(),
            endpoint: default_metrics_endpoint(),
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default)]
    pub file: Option<String>,
}

fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "text".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            file: None,
        }
    }
}

// ---------------------------------------------------------------------------
// NucleusConfig (top-level)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[derive(Default)]
pub struct NucleusConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub wal: WalConfig,
    #[serde(default)]
    pub pool: PoolConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}


impl NucleusConfig {
    /// Load config from a TOML file, then overlay environment variables.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Err(ConfigError::FileNotFound(
                path.display().to_string(),
            ));
        }

        let contents = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::IoError(e.to_string()))?;

        let mut config = Self::from_toml(&contents)?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Load from a TOML string.
    pub fn from_toml(toml_str: &str) -> Result<Self, ConfigError> {
        toml::from_str(toml_str)
            .map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Apply environment variable overrides.
    ///
    /// Pattern: `NUCLEUS_SECTION_KEY` (e.g., `NUCLEUS_SERVER_PORT=5433`).
    pub fn apply_env_overrides(&mut self) {
        // server
        if let Ok(v) = env::var("NUCLEUS_SERVER_HOST") {
            self.server.host = v;
        }
        if let Ok(v) = env::var("NUCLEUS_SERVER_PORT")
            && let Ok(p) = v.parse::<u16>() {
                self.server.port = p;
            }
        if let Ok(v) = env::var("NUCLEUS_SERVER_MAX_CONNECTIONS")
            && let Ok(n) = v.parse::<usize>() {
                self.server.max_connections = n;
            }

        // storage
        if let Ok(v) = env::var("NUCLEUS_STORAGE_DATA_DIR") {
            self.storage.data_dir = v;
        }
        if let Ok(v) = env::var("NUCLEUS_STORAGE_MEMORY_MODE")
            && let Ok(b) = v.parse::<bool>() {
                self.storage.memory_mode = b;
            }

        // wal
        if let Ok(v) = env::var("NUCLEUS_WAL_ENABLED")
            && let Ok(b) = v.parse::<bool>() {
                self.wal.enabled = b;
            }

        // metrics
        if let Ok(v) = env::var("NUCLEUS_METRICS_ENABLED")
            && let Ok(b) = v.parse::<bool>() {
                self.metrics.enabled = b;
            }
        if let Ok(v) = env::var("NUCLEUS_METRICS_PORT")
            && let Ok(p) = v.parse::<u16>() {
                self.metrics.port = p;
            }

        // logging
        if let Ok(v) = env::var("NUCLEUS_LOGGING_LEVEL") {
            self.logging.level = v;
        }

        // cache
        if let Ok(v) = env::var("NUCLEUS_CACHE_ENABLED")
            && let Ok(b) = v.parse::<bool>() {
                self.cache.enabled = b;
            }
        if let Ok(v) = env::var("NUCLEUS_CACHE_MAX_MEMORY_MB")
            && let Ok(n) = v.parse::<usize>() {
                self.cache.max_memory_mb = n;
            }
        if let Ok(v) = env::var("NUCLEUS_CACHE_DEFAULT_TTL_SECS")
            && let Ok(n) = v.parse::<u64>() {
                self.cache.default_ttl_secs = n;
            }

        // pool
        if let Ok(v) = env::var("NUCLEUS_POOL_MIN_IDLE")
            && let Ok(n) = v.parse::<usize>() {
                self.pool.min_idle = n;
            }
        if let Ok(v) = env::var("NUCLEUS_POOL_MAX_IDLE_TIME_SECS")
            && let Ok(n) = v.parse::<u64>() {
                self.pool.max_idle_time_secs = n;
            }
        if let Ok(v) = env::var("NUCLEUS_POOL_MAX_LIFETIME_SECS")
            && let Ok(n) = v.parse::<u64>() {
                self.pool.max_lifetime_secs = n;
            }
        if let Ok(v) = env::var("NUCLEUS_POOL_ACQUIRE_TIMEOUT_SECS")
            && let Ok(n) = v.parse::<u64>() {
                self.pool.acquire_timeout_secs = n;
            }

        // replication
        if let Ok(v) = env::var("NUCLEUS_REPLICATION_MODE") {
            self.replication.mode = v;
        }
        if let Ok(v) = env::var("NUCLEUS_REPLICATION_PRIMARY_HOST") {
            self.replication.primary_host = Some(v);
        }
        if let Ok(v) = env::var("NUCLEUS_REPLICATION_PRIMARY_PORT")
            && let Ok(p) = v.parse::<u16>() {
                self.replication.primary_port = Some(p);
            }
        if let Ok(v) = env::var("NUCLEUS_REPLICATION_SYNC_MODE") {
            self.replication.sync_mode = v;
        }
        if let Ok(v) = env::var("NUCLEUS_REPLICATION_FAILOVER_TIMEOUT_MS")
            && let Ok(n) = v.parse::<u64>() {
                self.replication.failover_timeout_ms = n;
            }

        // storage (additional)
        if let Ok(v) = env::var("NUCLEUS_STORAGE_BUFFER_POOL_SIZE_MB")
            && let Ok(n) = v.parse::<usize>() {
                self.storage.buffer_pool_size_mb = n;
            }
        if let Ok(v) = env::var("NUCLEUS_STORAGE_USE_DIRECT_IO")
            && let Ok(b) = v.parse::<bool>() {
                self.storage.use_direct_io = b;
            }

        // wal (additional)
        if let Ok(v) = env::var("NUCLEUS_WAL_SEGMENT_SIZE_MB")
            && let Ok(n) = v.parse::<usize>() {
                self.wal.segment_size_mb = n;
            }
        if let Ok(v) = env::var("NUCLEUS_WAL_CHECKPOINT_INTERVAL_SECS")
            && let Ok(n) = v.parse::<u64>() {
                self.wal.checkpoint_interval_secs = n;
            }
        if let Ok(v) = env::var("NUCLEUS_WAL_GROUP_COMMIT_INTERVAL_US")
            && let Ok(n) = v.parse::<u64>() {
                self.wal.group_commit_interval_us = n;
            }
        if let Ok(v) = env::var("NUCLEUS_WAL_SYNC_MODE") {
            self.wal.sync_mode = v;
        }

        // server (additional)
        if let Ok(v) = env::var("NUCLEUS_SERVER_IDLE_TIMEOUT_SECS")
            && let Ok(n) = v.parse::<u64>() {
                self.server.idle_timeout_secs = n;
            }

        // logging (additional)
        if let Ok(v) = env::var("NUCLEUS_LOGGING_FORMAT") {
            self.logging.format = v;
        }
        if let Ok(v) = env::var("NUCLEUS_LOGGING_FILE") {
            self.logging.file = Some(v);
        }

        // metrics (additional)
        if let Ok(v) = env::var("NUCLEUS_METRICS_ENDPOINT") {
            self.metrics.endpoint = v;
        }
    }

    /// Merge CLI arguments into the config, overriding any TOML / env values.
    ///
    /// Only `Some` values are applied; `None` means "use the existing value".
    pub fn merge_cli_args(
        &mut self,
        host: Option<&str>,
        port: Option<u16>,
        data_dir: Option<&str>,
        memory_mode: Option<bool>,
    ) {
        if let Some(h) = host {
            self.server.host = h.to_string();
        }
        if let Some(p) = port {
            self.server.port = p;
        }
        if let Some(d) = data_dir {
            self.storage.data_dir = d.to_string();
        }
        if let Some(m) = memory_mode {
            self.storage.memory_mode = m;
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Default value tests ----

    #[test]
    fn test_default_config_has_expected_values() {
        let cfg = NucleusConfig::default();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 5432);
        assert_eq!(cfg.server.max_connections, 100);
        assert_eq!(cfg.server.idle_timeout_secs, 300);
        assert_eq!(cfg.storage.data_dir, "nucleus_data");
        assert_eq!(cfg.storage.buffer_pool_size_mb, 128);
        assert_eq!(cfg.storage.page_size, 16384);
        assert!(!cfg.storage.use_direct_io);
        assert!(!cfg.storage.memory_mode);
        assert!(cfg.wal.enabled);
        assert_eq!(cfg.wal.segment_size_mb, 64);
        assert_eq!(cfg.wal.sync_mode, "fsync");
        assert_eq!(cfg.pool.min_idle, 5);
        assert_eq!(cfg.pool.max_lifetime_secs, 3600);
        assert!(!cfg.cache.enabled);
        assert_eq!(cfg.cache.max_memory_mb, 64);
        assert_eq!(cfg.cache.eviction_policy, "lru");
        assert_eq!(cfg.replication.mode, "standalone");
        assert!(cfg.replication.primary_host.is_none());
        assert!(cfg.replication.primary_port.is_none());
        assert_eq!(cfg.replication.sync_mode, "async");
        assert!(!cfg.metrics.enabled);
        assert_eq!(cfg.metrics.port, 9100);
        assert_eq!(cfg.metrics.endpoint, "/metrics");
        assert_eq!(cfg.logging.level, "info");
        assert_eq!(cfg.logging.format, "text");
        assert!(cfg.logging.file.is_none());
    }

    #[test]
    fn test_server_config_default() {
        let sc = ServerConfig::default();
        assert_eq!(sc.host, "127.0.0.1");
        assert_eq!(sc.port, 5432);
        assert_eq!(sc.max_connections, 100);
        assert_eq!(sc.idle_timeout_secs, 300);
    }

    #[test]
    fn test_storage_config_default() {
        let sc = StorageConfig::default();
        assert_eq!(sc.data_dir, "nucleus_data");
        assert_eq!(sc.buffer_pool_size_mb, 128);
        assert_eq!(sc.page_size, 16384);
        assert!(!sc.use_direct_io);
        assert!(!sc.memory_mode);
    }

    #[test]
    fn test_wal_config_default() {
        let wc = WalConfig::default();
        assert!(wc.enabled);
        assert_eq!(wc.segment_size_mb, 64);
        assert_eq!(wc.checkpoint_interval_secs, 300);
        assert_eq!(wc.group_commit_interval_us, 1000);
        assert_eq!(wc.sync_mode, "fsync");
    }

    #[test]
    fn test_pool_config_default() {
        let pc = PoolConfig::default();
        assert_eq!(pc.min_idle, 5);
        assert_eq!(pc.max_idle_time_secs, 600);
        assert_eq!(pc.max_lifetime_secs, 3600);
        assert_eq!(pc.acquire_timeout_secs, 30);
        assert_eq!(pc.validation_interval_secs, 60);
    }

    #[test]
    fn test_cache_config_default() {
        let cc = CacheConfig::default();
        assert!(!cc.enabled);
        assert_eq!(cc.max_memory_mb, 64);
        assert_eq!(cc.default_ttl_secs, 300);
        assert_eq!(cc.eviction_policy, "lru");
    }

    #[test]
    fn test_replication_config_default() {
        let rc = ReplicationConfig::default();
        assert_eq!(rc.mode, "standalone");
        assert!(rc.primary_host.is_none());
        assert!(rc.primary_port.is_none());
        assert_eq!(rc.sync_mode, "async");
        assert_eq!(rc.failover_timeout_ms, 5000);
    }

    #[test]
    fn test_metrics_config_default() {
        let mc = MetricsConfig::default();
        assert!(!mc.enabled);
        assert_eq!(mc.port, 9100);
        assert_eq!(mc.endpoint, "/metrics");
    }

    #[test]
    fn test_logging_config_default() {
        let lc = LoggingConfig::default();
        assert_eq!(lc.level, "info");
        assert_eq!(lc.format, "text");
        assert!(lc.file.is_none());
    }

    // ---- TOML parsing tests ----

    #[test]
    fn test_toml_parse_all_fields() {
        let toml_str = r#"
[server]
host = "127.0.0.1"
port = 5433
max_connections = 200
idle_timeout_secs = 120

[storage]
data_dir = "/var/lib/nucleus"
buffer_pool_size_mb = 128
page_size = 8192
use_direct_io = true
memory_mode = true

[wal]
enabled = false
segment_size_mb = 128
checkpoint_interval_secs = 600
group_commit_interval_us = 500
sync_mode = "fdatasync"

[pool]
min_idle = 10
max_idle_time_secs = 300
max_lifetime_secs = 7200
acquire_timeout_secs = 15
validation_interval_secs = 30

[cache]
enabled = true
max_memory_mb = 256
default_ttl_secs = 600
eviction_policy = "lfu"

[replication]
mode = "primary"
primary_host = "10.0.0.1"
primary_port = 5432
sync_mode = "sync"
failover_timeout_ms = 10000

[metrics]
enabled = true
port = 9200
endpoint = "/prometheus"

[logging]
level = "debug"
format = "json"
file = "/var/log/nucleus.log"
"#;

        let cfg = NucleusConfig::from_toml(toml_str).unwrap();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 5433);
        assert_eq!(cfg.server.max_connections, 200);
        assert_eq!(cfg.server.idle_timeout_secs, 120);
        assert_eq!(cfg.storage.data_dir, "/var/lib/nucleus");
        assert_eq!(cfg.storage.buffer_pool_size_mb, 128);
        assert_eq!(cfg.storage.page_size, 8192);
        assert!(cfg.storage.use_direct_io);
        assert!(cfg.storage.memory_mode);
        assert!(!cfg.wal.enabled);
        assert_eq!(cfg.wal.segment_size_mb, 128);
        assert_eq!(cfg.wal.sync_mode, "fdatasync");
        assert_eq!(cfg.pool.min_idle, 10);
        assert_eq!(cfg.pool.max_lifetime_secs, 7200);
        assert!(cfg.cache.enabled);
        assert_eq!(cfg.cache.max_memory_mb, 256);
        assert_eq!(cfg.cache.eviction_policy, "lfu");
        assert_eq!(cfg.replication.mode, "primary");
        assert_eq!(cfg.replication.primary_host, Some("10.0.0.1".to_string()));
        assert_eq!(cfg.replication.primary_port, Some(5432));
        assert_eq!(cfg.replication.sync_mode, "sync");
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.metrics.port, 9200);
        assert_eq!(cfg.metrics.endpoint, "/prometheus");
        assert_eq!(cfg.logging.level, "debug");
        assert_eq!(cfg.logging.format, "json");
        assert_eq!(cfg.logging.file, Some("/var/log/nucleus.log".to_string()));
    }

    #[test]
    fn test_toml_parse_partial_fields_defaults_fill_in() {
        let toml_str = r#"
[server]
port = 5555
"#;

        let cfg = NucleusConfig::from_toml(toml_str).unwrap();
        // Explicit value
        assert_eq!(cfg.server.port, 5555);
        // Defaults fill in for the rest
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.max_connections, 100);
        // Other sections fully default
        assert_eq!(cfg.storage.data_dir, "nucleus_data");
        assert!(cfg.wal.enabled);
        assert_eq!(cfg.pool.min_idle, 5);
        assert!(!cfg.cache.enabled);
        assert_eq!(cfg.replication.mode, "standalone");
        assert!(!cfg.metrics.enabled);
        assert_eq!(cfg.logging.level, "info");
    }

    #[test]
    fn test_empty_toml_produces_defaults() {
        let cfg = NucleusConfig::from_toml("").unwrap();
        let def = NucleusConfig::default();
        assert_eq!(cfg, def);
    }

    #[test]
    fn test_invalid_toml_produces_parse_error() {
        let bad = "this is not [valid toml = = =";
        let err = NucleusConfig::from_toml(bad).unwrap_err();
        match err {
            ConfigError::ParseError(_) => {} // expected
            other => panic!("expected ParseError, got: {:?}", other),
        }
    }

    // ---- Environment variable override tests ----

    #[test]
    fn test_env_override_server() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_SERVER_HOST", "192.168.1.1");
            env::set_var("NUCLEUS_SERVER_PORT", "6543");
            env::set_var("NUCLEUS_SERVER_MAX_CONNECTIONS", "500");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.server.host, "192.168.1.1");
        assert_eq!(cfg.server.port, 6543);
        assert_eq!(cfg.server.max_connections, 500);
        // cleanup
        unsafe {
            env::remove_var("NUCLEUS_SERVER_HOST");
            env::remove_var("NUCLEUS_SERVER_PORT");
            env::remove_var("NUCLEUS_SERVER_MAX_CONNECTIONS");
        }
    }

    #[test]
    fn test_env_override_storage() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_STORAGE_DATA_DIR", "/mnt/ssd/nucleus");
            env::set_var("NUCLEUS_STORAGE_MEMORY_MODE", "true");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.storage.data_dir, "/mnt/ssd/nucleus");
        assert!(cfg.storage.memory_mode);
        unsafe {
            env::remove_var("NUCLEUS_STORAGE_DATA_DIR");
            env::remove_var("NUCLEUS_STORAGE_MEMORY_MODE");
        }
    }

    #[test]
    fn test_env_override_wal_and_logging() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_WAL_ENABLED", "false");
            env::set_var("NUCLEUS_LOGGING_LEVEL", "trace");
        }
        cfg.apply_env_overrides();
        assert!(!cfg.wal.enabled);
        assert_eq!(cfg.logging.level, "trace");
        unsafe {
            env::remove_var("NUCLEUS_WAL_ENABLED");
            env::remove_var("NUCLEUS_LOGGING_LEVEL");
        }
    }

    #[test]
    fn test_env_override_metrics_and_cache() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_METRICS_ENABLED", "true");
            env::set_var("NUCLEUS_METRICS_PORT", "9300");
            env::set_var("NUCLEUS_CACHE_ENABLED", "true");
            env::set_var("NUCLEUS_CACHE_MAX_MEMORY_MB", "512");
        }
        cfg.apply_env_overrides();
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.metrics.port, 9300);
        assert!(cfg.cache.enabled);
        assert_eq!(cfg.cache.max_memory_mb, 512);
        unsafe {
            env::remove_var("NUCLEUS_METRICS_ENABLED");
            env::remove_var("NUCLEUS_METRICS_PORT");
            env::remove_var("NUCLEUS_CACHE_ENABLED");
            env::remove_var("NUCLEUS_CACHE_MAX_MEMORY_MB");
        }
    }

    // ---- Roundtrip test ----

    #[test]
    fn test_roundtrip_serialize_deserialize() {
        let original = NucleusConfig {
            server: ServerConfig {
                host: "10.0.0.5".to_string(),
                port: 7777,
                max_connections: 42,
                idle_timeout_secs: 99,
            },
            storage: StorageConfig {
                data_dir: "/tmp/nucleus".to_string(),
                buffer_pool_size_mb: 32,
                page_size: 4096,
                use_direct_io: true,
                memory_mode: false,
            },
            wal: WalConfig::default(),
            pool: PoolConfig::default(),
            cache: CacheConfig {
                enabled: true,
                max_memory_mb: 128,
                default_ttl_secs: 60,
                eviction_policy: "lfu".to_string(),
            },
            replication: ReplicationConfig {
                mode: "replica".to_string(),
                primary_host: Some("primary.local".to_string()),
                primary_port: Some(5432),
                sync_mode: "sync".to_string(),
                failover_timeout_ms: 3000,
            },
            metrics: MetricsConfig::default(),
            logging: LoggingConfig {
                level: "warn".to_string(),
                format: "json".to_string(),
                file: Some("/var/log/nucleus.log".to_string()),
            },
        };

        let toml_str = toml::to_string(&original).expect("serialize failed");
        let restored = NucleusConfig::from_toml(&toml_str).expect("parse failed");
        assert_eq!(original, restored);
    }

    // ---- File loading tests ----

    #[test]
    fn test_load_nonexistent_file_returns_file_not_found() {
        let path = Path::new("/nonexistent/path/nucleus.toml");
        let err = NucleusConfig::load(path).unwrap_err();
        match err {
            ConfigError::FileNotFound(p) => {
                assert!(p.contains("nonexistent"));
            }
            other => panic!("expected FileNotFound, got: {:?}", other),
        }
    }

    // ---- Pool env override tests ----

    #[test]
    fn test_env_override_pool() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_POOL_MIN_IDLE", "10");
            env::set_var("NUCLEUS_POOL_MAX_IDLE_TIME_SECS", "300");
            env::set_var("NUCLEUS_POOL_MAX_LIFETIME_SECS", "7200");
            env::set_var("NUCLEUS_POOL_ACQUIRE_TIMEOUT_SECS", "15");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.pool.min_idle, 10);
        assert_eq!(cfg.pool.max_idle_time_secs, 300);
        assert_eq!(cfg.pool.max_lifetime_secs, 7200);
        assert_eq!(cfg.pool.acquire_timeout_secs, 15);
        unsafe {
            env::remove_var("NUCLEUS_POOL_MIN_IDLE");
            env::remove_var("NUCLEUS_POOL_MAX_IDLE_TIME_SECS");
            env::remove_var("NUCLEUS_POOL_MAX_LIFETIME_SECS");
            env::remove_var("NUCLEUS_POOL_ACQUIRE_TIMEOUT_SECS");
        }
    }

    // ---- Replication env override tests ----

    #[test]
    fn test_env_override_replication() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_REPLICATION_MODE", "primary");
            env::set_var("NUCLEUS_REPLICATION_PRIMARY_HOST", "10.0.0.1");
            env::set_var("NUCLEUS_REPLICATION_PRIMARY_PORT", "5432");
            env::set_var("NUCLEUS_REPLICATION_SYNC_MODE", "sync");
            env::set_var("NUCLEUS_REPLICATION_FAILOVER_TIMEOUT_MS", "10000");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.replication.mode, "primary");
        assert_eq!(cfg.replication.primary_host, Some("10.0.0.1".to_string()));
        assert_eq!(cfg.replication.primary_port, Some(5432));
        assert_eq!(cfg.replication.sync_mode, "sync");
        assert_eq!(cfg.replication.failover_timeout_ms, 10000);
        unsafe {
            env::remove_var("NUCLEUS_REPLICATION_MODE");
            env::remove_var("NUCLEUS_REPLICATION_PRIMARY_HOST");
            env::remove_var("NUCLEUS_REPLICATION_PRIMARY_PORT");
            env::remove_var("NUCLEUS_REPLICATION_SYNC_MODE");
            env::remove_var("NUCLEUS_REPLICATION_FAILOVER_TIMEOUT_MS");
        }
    }

    // ---- Additional env overrides tests ----

    #[test]
    fn test_env_override_storage_extended() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_STORAGE_BUFFER_POOL_SIZE_MB", "256");
            env::set_var("NUCLEUS_STORAGE_USE_DIRECT_IO", "true");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.storage.buffer_pool_size_mb, 256);
        assert!(cfg.storage.use_direct_io);
        unsafe {
            env::remove_var("NUCLEUS_STORAGE_BUFFER_POOL_SIZE_MB");
            env::remove_var("NUCLEUS_STORAGE_USE_DIRECT_IO");
        }
    }

    #[test]
    fn test_env_override_wal_extended() {
        let mut cfg = NucleusConfig::default();
        unsafe {
            env::set_var("NUCLEUS_WAL_SEGMENT_SIZE_MB", "128");
            env::set_var("NUCLEUS_WAL_CHECKPOINT_INTERVAL_SECS", "600");
            env::set_var("NUCLEUS_WAL_GROUP_COMMIT_INTERVAL_US", "500");
            env::set_var("NUCLEUS_WAL_SYNC_MODE", "fdatasync");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.wal.segment_size_mb, 128);
        assert_eq!(cfg.wal.checkpoint_interval_secs, 600);
        assert_eq!(cfg.wal.group_commit_interval_us, 500);
        assert_eq!(cfg.wal.sync_mode, "fdatasync");
        unsafe {
            env::remove_var("NUCLEUS_WAL_SEGMENT_SIZE_MB");
            env::remove_var("NUCLEUS_WAL_CHECKPOINT_INTERVAL_SECS");
            env::remove_var("NUCLEUS_WAL_GROUP_COMMIT_INTERVAL_US");
            env::remove_var("NUCLEUS_WAL_SYNC_MODE");
        }
    }

    // ---- CLI args merge tests ----

    #[test]
    fn test_merge_cli_args_overrides() {
        let mut cfg = NucleusConfig::default();
        cfg.merge_cli_args(Some("192.168.1.1"), Some(6543), Some("/data/db"), Some(true));
        assert_eq!(cfg.server.host, "192.168.1.1");
        assert_eq!(cfg.server.port, 6543);
        assert_eq!(cfg.storage.data_dir, "/data/db");
        assert!(cfg.storage.memory_mode);
    }

    #[test]
    fn test_merge_cli_args_none_preserves_defaults() {
        let mut cfg = NucleusConfig::default();
        let original = cfg.clone();
        cfg.merge_cli_args(None, None, None, None);
        assert_eq!(cfg, original);
    }

    #[test]
    fn test_merge_cli_args_partial() {
        let mut cfg = NucleusConfig::default();
        cfg.merge_cli_args(None, Some(9999), None, None);
        assert_eq!(cfg.server.host, "127.0.0.1"); // unchanged
        assert_eq!(cfg.server.port, 9999); // changed
    }
}
