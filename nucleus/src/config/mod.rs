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
    /// Global memory limit in MB. All subsystems (buffer pool, cache, KV, FTS,
    /// columnar) share this budget. 0 means no limit.
    #[serde(default = "default_max_memory_mb")]
    pub max_memory_mb: usize,
    /// Seconds a transaction may sit open with no activity before the server
    /// rolls it back, releasing its MVCC snapshot so GC can advance (T1.3).
    /// Mirrors Postgres `idle_in_transaction_session_timeout`. 0 disables it
    /// (the default) — an abandoned `BEGIN` otherwise pins the GC watermark
    /// forever and grows the database without bound.
    #[serde(default = "default_idle_in_transaction_timeout_secs")]
    pub idle_in_transaction_timeout_secs: u64,
    /// Percent of `max_memory_mb` a single query's working set may reserve.
    ///
    /// These were the same number, which made the query budget useless as a
    /// guard: one query could reserve the entire RSS cap, so the working-set
    /// limit could never fire BEFORE the RSS watchdog did. Keeping it below
    /// 100 means an oversized query gets a clean 53200 naming the query, while
    /// the rest of the server keeps serving.
    #[serde(default = "default_query_memory_percent")]
    pub query_memory_percent: usize,
    /// Reject ALL writes while the RSS watchdog reports critical pressure.
    ///
    /// Off by default, and it should stay off. RSS is not the server's working
    /// set — it includes the buffer pool and whatever the allocator has not
    /// returned to the OS — so the flag can be set while the server is
    /// perfectly able to serve a small INSERT. Worse, rejecting writes has no
    /// feedback path to RSS (the memory is held by caches and the pool, not by
    /// pending writes), so it does not clear the condition it reacts to; it
    /// just blocks the workload until something else frees memory. Bounding
    /// query working sets is the mechanism that actually limits allocation.
    ///
    /// Space-reclaiming statements (DELETE, TRUNCATE) are never rejected even
    /// when this is on: refusing the retention job that would free the memory
    /// is the exact opposite of the intent.
    #[serde(default = "default_reject_writes_on_memory_critical")]
    pub reject_writes_on_memory_critical: bool,
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
fn default_max_memory_mb() -> usize {
    512
}
fn default_idle_in_transaction_timeout_secs() -> u64 {
    0
}
fn default_query_memory_percent() -> usize {
    75
}
fn default_reject_writes_on_memory_critical() -> bool {
    false
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            max_connections: default_max_connections(),
            idle_timeout_secs: default_idle_timeout_secs(),
            max_memory_mb: default_max_memory_mb(),
            idle_in_transaction_timeout_secs: default_idle_in_transaction_timeout_secs(),
            query_memory_percent: default_query_memory_percent(),
            reject_writes_on_memory_critical: default_reject_writes_on_memory_critical(),
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
    /// How often to sample free space on the data directory's filesystem.
    /// 0 disables the disk watermark monitor entirely.
    #[serde(default = "default_disk_check_interval_secs")]
    pub disk_check_interval_secs: u64,
    /// Percentage of free space below which an operator alert is logged.
    #[serde(default = "default_disk_warn_free_pct")]
    pub disk_warn_free_pct: f64,
    /// Percentage of free space below which the server refuses writes
    /// (SQLSTATE 53100) instead of failing mid-write when the disk fills.
    #[serde(default = "default_disk_readonly_free_pct")]
    pub disk_readonly_free_pct: f64,
    /// Absolute free-space floor in MB. A percentage margin is meaningless on
    /// a small volume, so this triggers read-only independently.
    #[serde(default = "default_disk_min_free_mb")]
    pub disk_min_free_mb: u64,
    /// Free space must climb back above this percentage before writes resume
    /// (hysteresis), so the server cannot flap at the watermark.
    #[serde(default = "default_disk_resume_free_pct")]
    pub disk_resume_free_pct: f64,
}

fn default_disk_check_interval_secs() -> u64 {
    30
}
fn default_disk_warn_free_pct() -> f64 {
    10.0
}
fn default_disk_readonly_free_pct() -> f64 {
    3.0
}
fn default_disk_min_free_mb() -> u64 {
    256
}
fn default_disk_resume_free_pct() -> f64 {
    6.0
}

fn default_data_dir() -> String {
    "nucleus_data".to_string()
}
fn default_buffer_pool_size_mb() -> usize {
    32
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
            disk_check_interval_secs: default_disk_check_interval_secs(),
            disk_warn_free_pct: default_disk_warn_free_pct(),
            disk_readonly_free_pct: default_disk_readonly_free_pct(),
            disk_min_free_mb: default_disk_min_free_mb(),
            disk_resume_free_pct: default_disk_resume_free_pct(),
        }
    }
}

impl StorageConfig {
    /// The disk watermarks derived from this config.
    pub fn disk_watermarks(&self) -> crate::ops::DiskWatermarks {
        crate::ops::DiskWatermarks {
            warn_free_pct: self.disk_warn_free_pct,
            readonly_free_pct: self.disk_readonly_free_pct,
            min_free_bytes: self.disk_min_free_mb.saturating_mul(1024 * 1024),
            resume_free_pct: self.disk_resume_free_pct,
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
    /// Seal and archive the active WAL segment after this many seconds, even
    /// if it has not filled. `0` disables it. Only has an effect when
    /// `NUCLEUS_WAL_ARCHIVE_DIR` is set.
    ///
    /// Without this, a segment reaches the archive only when it fills, so the
    /// PITR recovery point is the last rollover rather than the last commit. At
    /// the default 64 MiB segment a low-write database can go days between
    /// rollovers, and every commit in the current segment is missing from the
    /// archive. This bounds that exposure in wall-clock terms: it is the
    /// recovery-point objective, and it defaults to a minute rather than to
    /// `off` because anyone who has configured an archive at all has said they
    /// want point-in-time recovery.
    #[serde(default = "default_archive_timeout_secs")]
    pub archive_timeout_secs: u64,
    #[serde(default = "default_group_commit_interval_us")]
    pub group_commit_interval_us: u64,
    /// How the WAL is forced to stable storage. One of:
    ///
    /// - `fsync` (default) — `sync_all`. On macOS this is `F_FULLFSYNC`, a true
    ///   drive-cache barrier: survives power loss, and costs ~4,253 µs here.
    /// - `fdatasync` — `sync_data`. Distinct on Linux; on macOS it is measurably
    ///   the same as `fsync` (3,849 vs 3,872 µs), so it is a knob that does
    ///   nothing there.
    /// - `flush_os` — plain `fsync(2)`, ~41 µs on this host. Survives process
    ///   crash, OS panic and `kill -9`; does NOT survive power loss, because the
    ///   drive may still hold the data in a volatile cache. This is the
    ///   guarantee PostgreSQL gives on macOS with its default
    ///   `wal_sync_method`, which is what makes an equal-footing write
    ///   comparison possible at all. On Linux `fsync(2)` normally does flush the
    ///   device, so the mode is not weaker there.
    /// - `none` / `off` — no sync. Loses committed data on any crash.
    ///
    /// The default stays `fsync`: durability is not something to trade away by
    /// accident, only deliberately.
    #[serde(default = "default_sync_mode")]
    pub sync_mode: String,
    /// Commit-time durability: "on" (default) forces the WAL (group commit)
    /// before a write statement or COMMIT is acked; "off" defers durability
    /// to the next flush/checkpoint (bounded loss window, higher throughput).
    /// Sessions can override with `SET synchronous_commit = on|off`.
    #[serde(default = "default_synchronous_commit")]
    pub synchronous_commit: String,
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
fn default_archive_timeout_secs() -> u64 {
    60
}
fn default_group_commit_interval_us() -> u64 {
    1000
}
fn default_sync_mode() -> String {
    "fsync".to_string()
}
fn default_synchronous_commit() -> String {
    "on".to_string()
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            segment_size_mb: default_segment_size_mb(),
            checkpoint_interval_secs: default_checkpoint_interval_secs(),
            archive_timeout_secs: default_archive_timeout_secs(),
            group_commit_interval_us: default_group_commit_interval_us(),
            sync_mode: default_sync_mode(),
            synchronous_commit: default_synchronous_commit(),
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
    16
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
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

/// Push an error when `value` is not one of `allowed` (case-insensitive).
fn check_enum(errors: &mut Vec<String>, setting: &str, value: &str, allowed: &[&str]) {
    if !allowed.iter().any(|a| value.eq_ignore_ascii_case(a)) {
        errors.push(format!(
            "{setting} must be one of {} (got {value:?})",
            allowed.join(", ")
        ));
    }
}

impl NucleusConfig {
    /// Load config from a TOML file, then overlay environment variables.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Err(ConfigError::FileNotFound(path.display().to_string()));
        }

        let contents =
            std::fs::read_to_string(path).map_err(|e| ConfigError::IoError(e.to_string()))?;

        let mut config = Self::from_toml(&contents)?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Load from a TOML string.
    pub fn from_toml(toml_str: &str) -> Result<Self, ConfigError> {
        toml::from_str(toml_str).map_err(|e| ConfigError::ParseError(e.to_string()))
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
            && let Ok(p) = v.parse::<u16>()
        {
            self.server.port = p;
        }
        if let Ok(v) = env::var("NUCLEUS_SERVER_MAX_CONNECTIONS")
            && let Ok(n) = v.parse::<usize>()
        {
            self.server.max_connections = n;
        }

        // storage
        if let Ok(v) = env::var("NUCLEUS_STORAGE_DATA_DIR") {
            self.storage.data_dir = v;
        }
        if let Ok(v) = env::var("NUCLEUS_STORAGE_MEMORY_MODE")
            && let Ok(b) = v.parse::<bool>()
        {
            self.storage.memory_mode = b;
        }

        // wal
        if let Ok(v) = env::var("NUCLEUS_WAL_ENABLED")
            && let Ok(b) = v.parse::<bool>()
        {
            self.wal.enabled = b;
        }
        if let Ok(v) = env::var("NUCLEUS_WAL_SYNCHRONOUS_COMMIT") {
            self.wal.synchronous_commit = v;
        }

        // metrics
        if let Ok(v) = env::var("NUCLEUS_METRICS_ENABLED")
            && let Ok(b) = v.parse::<bool>()
        {
            self.metrics.enabled = b;
        }
        if let Ok(v) = env::var("NUCLEUS_METRICS_PORT")
            && let Ok(p) = v.parse::<u16>()
        {
            self.metrics.port = p;
        }

        // logging
        if let Ok(v) = env::var("NUCLEUS_LOGGING_LEVEL") {
            self.logging.level = v;
        }

        // cache
        if let Ok(v) = env::var("NUCLEUS_CACHE_ENABLED")
            && let Ok(b) = v.parse::<bool>()
        {
            self.cache.enabled = b;
        }
        if let Ok(v) = env::var("NUCLEUS_CACHE_MAX_MEMORY_MB")
            && let Ok(n) = v.parse::<usize>()
        {
            self.cache.max_memory_mb = n;
        }
        if let Ok(v) = env::var("NUCLEUS_CACHE_DEFAULT_TTL_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.cache.default_ttl_secs = n;
        }

        // pool
        if let Ok(v) = env::var("NUCLEUS_POOL_MIN_IDLE")
            && let Ok(n) = v.parse::<usize>()
        {
            self.pool.min_idle = n;
        }
        if let Ok(v) = env::var("NUCLEUS_POOL_MAX_IDLE_TIME_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.pool.max_idle_time_secs = n;
        }
        if let Ok(v) = env::var("NUCLEUS_POOL_MAX_LIFETIME_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.pool.max_lifetime_secs = n;
        }
        if let Ok(v) = env::var("NUCLEUS_POOL_ACQUIRE_TIMEOUT_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
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
            && let Ok(p) = v.parse::<u16>()
        {
            self.replication.primary_port = Some(p);
        }
        if let Ok(v) = env::var("NUCLEUS_REPLICATION_SYNC_MODE") {
            self.replication.sync_mode = v;
        }
        if let Ok(v) = env::var("NUCLEUS_REPLICATION_FAILOVER_TIMEOUT_MS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.replication.failover_timeout_ms = n;
        }

        // storage (additional)
        if let Ok(v) = env::var("NUCLEUS_STORAGE_BUFFER_POOL_SIZE_MB")
            && let Ok(n) = v.parse::<usize>()
        {
            self.storage.buffer_pool_size_mb = n;
        }
        if let Ok(v) = env::var("NUCLEUS_STORAGE_USE_DIRECT_IO")
            && let Ok(b) = v.parse::<bool>()
        {
            self.storage.use_direct_io = b;
        }
        if let Ok(v) = env::var("NUCLEUS_DISK_CHECK_INTERVAL_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.storage.disk_check_interval_secs = n;
        }
        if let Ok(v) = env::var("NUCLEUS_DISK_WARN_FREE_PCT")
            && let Ok(n) = v.parse::<f64>()
        {
            self.storage.disk_warn_free_pct = n;
        }
        if let Ok(v) = env::var("NUCLEUS_DISK_READONLY_FREE_PCT")
            && let Ok(n) = v.parse::<f64>()
        {
            self.storage.disk_readonly_free_pct = n;
        }
        if let Ok(v) = env::var("NUCLEUS_DISK_MIN_FREE_MB")
            && let Ok(n) = v.parse::<u64>()
        {
            self.storage.disk_min_free_mb = n;
        }
        if let Ok(v) = env::var("NUCLEUS_DISK_RESUME_FREE_PCT")
            && let Ok(n) = v.parse::<f64>()
        {
            self.storage.disk_resume_free_pct = n;
        }

        // wal (additional)
        if let Ok(v) = env::var("NUCLEUS_WAL_SEGMENT_SIZE_MB")
            && let Ok(n) = v.parse::<usize>()
        {
            self.wal.segment_size_mb = n;
        }
        if let Ok(v) = env::var("NUCLEUS_WAL_CHECKPOINT_INTERVAL_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.wal.checkpoint_interval_secs = n;
        }
        if let Ok(v) = env::var("NUCLEUS_WAL_ARCHIVE_TIMEOUT_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.wal.archive_timeout_secs = n;
        }
        if let Ok(v) = env::var("NUCLEUS_WAL_GROUP_COMMIT_INTERVAL_US")
            && let Ok(n) = v.parse::<u64>()
        {
            self.wal.group_commit_interval_us = n;
        }
        if let Ok(v) = env::var("NUCLEUS_WAL_SYNC_MODE") {
            self.wal.sync_mode = v;
        }

        // server (additional)
        if let Ok(v) = env::var("NUCLEUS_SERVER_IDLE_TIMEOUT_SECS")
            && let Ok(n) = v.parse::<u64>()
        {
            self.server.idle_timeout_secs = n;
        }
        if let Ok(v) = env::var("NUCLEUS_MAX_MEMORY_MB")
            && let Ok(n) = v.parse::<usize>()
        {
            self.server.max_memory_mb = n;
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
    /// Reject a configuration that cannot do what it says, *before* the
    /// server starts serving.
    ///
    /// The failure mode this prevents is the quiet one: a typo'd or inverted
    /// setting that leaves a limit unenforced or a watermark unreachable, and
    /// is only discovered when the safety net was supposed to catch something.
    /// Every message names the setting and the value it got.
    ///
    /// Returns all problems at once so an operator fixes the file in one pass.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.server.max_connections == 0 {
            errors.push(
                "server.max_connections must be at least 1 (got 0): the server would refuse every connection"
                    .to_string(),
            );
        }
        if self.pool.min_idle > self.server.max_connections {
            errors.push(format!(
                "pool.min_idle ({}) must not exceed server.max_connections ({}): the pool could never reach its idle floor",
                self.pool.min_idle, self.server.max_connections
            ));
        }
        if self.storage.page_size < 512 || !self.storage.page_size.is_power_of_two() {
            errors.push(format!(
                "storage.page_size must be a power of two of at least 512 (got {})",
                self.storage.page_size
            ));
        }
        if self.storage.data_dir.trim().is_empty() {
            errors.push("storage.data_dir must not be empty".to_string());
        }
        // A memory ceiling smaller than the buffers carved out of it is not a
        // ceiling; the subsystems would be over-committed from the start.
        if self.server.max_memory_mb > 0 {
            let reserved = self.storage.buffer_pool_size_mb + self.cache.max_memory_mb;
            if reserved > self.server.max_memory_mb {
                errors.push(format!(
                    "storage.buffer_pool_size_mb ({}) + cache.max_memory_mb ({}) = {reserved} MB exceeds server.max_memory_mb ({}): the global memory limit would be blown before a single query runs",
                    self.storage.buffer_pool_size_mb,
                    self.cache.max_memory_mb,
                    self.server.max_memory_mb
                ));
            }
        }
        if self.metrics.enabled && self.metrics.port == self.server.port {
            errors.push(format!(
                "metrics.port ({}) must differ from server.port ({})",
                self.metrics.port, self.server.port
            ));
        }
        if self.metrics.enabled && !self.metrics.endpoint.starts_with('/') {
            errors.push(format!(
                "metrics.endpoint must start with '/' (got {:?})",
                self.metrics.endpoint
            ));
        }

        // Enumerated strings: a typo here silently selects the default
        // behaviour, so fail instead of guessing.
        check_enum(
            &mut errors,
            "wal.sync_mode",
            &self.wal.sync_mode,
            &["fsync", "fdatasync", "async", "none", "off"],
        );
        check_enum(
            &mut errors,
            "cache.eviction_policy",
            &self.cache.eviction_policy,
            &["lru", "lfu", "fifo", "random"],
        );
        check_enum(
            &mut errors,
            "replication.mode",
            &self.replication.mode,
            &["standalone", "primary", "replica"],
        );
        check_enum(
            &mut errors,
            "replication.sync_mode",
            &self.replication.sync_mode,
            &["async", "sync"],
        );
        check_enum(
            &mut errors,
            "logging.level",
            &self.logging.level,
            &["trace", "debug", "info", "warn", "error", "off"],
        );
        check_enum(
            &mut errors,
            "logging.format",
            &self.logging.format,
            &["text", "json"],
        );

        if self.replication.mode.eq_ignore_ascii_case("replica")
            && self.replication.primary_host.is_none()
        {
            errors.push(
                "replication.mode = \"replica\" requires replication.primary_host".to_string(),
            );
        }

        if let Err(e) = self.storage.disk_watermarks().validate() {
            errors.push(e);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn merge_cli_args(
        &mut self,
        host: Option<&str>,
        port: Option<u16>,
        data_dir: Option<&str>,
        memory_mode: Option<bool>,
        max_memory_mb: Option<usize>,
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
        if let Some(m) = max_memory_mb {
            self.server.max_memory_mb = m;
        }
    }

    /// Derive subsystem memory budgets from the global max_memory_mb setting.
    /// Adjusts buffer_pool_size_mb and cache.max_memory_mb to fit within budget.
    /// Derive the memory budget from the container's cgroup limit when nothing
    /// set it explicitly (CLI/env/TOML all left the 512 MB default in place).
    ///
    /// A 512 MB default inside an 8 GB container is a foot-gun: the watchdog
    /// starts silently rejecting writes at ~460 MB RSS while the host has
    /// gigabytes free (teploy-observe dogfood finding #33). Uses 80% of the
    /// cgroup limit, leaving headroom for allocator slack and page cache.
    /// Returns the derived budget in MB when applied.
    pub fn apply_cgroup_memory_default(&mut self) -> Option<usize> {
        const DEFAULT_MB: usize = 512;
        if self.server.max_memory_mb != DEFAULT_MB {
            return None; // explicitly configured — leave it alone
        }
        let limit = Self::cgroup_memory_limit_bytes()?;
        let derived_mb = ((limit as f64 * 0.80) as usize / (1024 * 1024)).max(DEFAULT_MB);
        if derived_mb == DEFAULT_MB {
            return None; // container is <= ~640 MB — the default already fits
        }
        self.server.max_memory_mb = derived_mb;
        Some(derived_mb)
    }

    /// Read the effective cgroup memory limit, if the process runs under one.
    /// cgroup v2 (`/sys/fs/cgroup/memory.max`) first, then v1. "max" or
    /// absurdly large values mean "no limit" and read as None.
    fn cgroup_memory_limit_bytes() -> Option<u64> {
        const NO_LIMIT_FLOOR: u64 = 1 << 60;
        for path in [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ] {
            if let Ok(s) = std::fs::read_to_string(path) {
                let s = s.trim();
                if s == "max" {
                    return None;
                }
                if let Ok(v) = s.parse::<u64>() {
                    if v >= NO_LIMIT_FLOOR {
                        return None;
                    }
                    return Some(v);
                }
            }
        }
        None
    }

    pub fn apply_memory_budget(&mut self) {
        let max = self.server.max_memory_mb;
        if max == 0 {
            return;
        }
        // Budget: tight proportional allocation to leave room for runtime data.
        //   Buffer pool: 10% of max_memory (was 25%)
        //   Cache:        5% of max_memory (was 12%)
        //   Remaining 85%: FTS, KV, columnar, query execution, OS overhead
        let bp = (max / 32).clamp(4, 256); // ~3% of budget, 4 MB min, 256 MB max
        let cache = (max / 64).clamp(2, 128); // ~1.5% of budget, 2 MB min, 128 MB max
        // Always enforce proportional sizing relative to max_memory
        self.storage.buffer_pool_size_mb = self.storage.buffer_pool_size_mb.min(bp);
        self.cache.max_memory_mb = self.cache.max_memory_mb.min(cache);
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
        assert_eq!(cfg.storage.buffer_pool_size_mb, 32);
        assert_eq!(cfg.storage.page_size, 16384);
        assert!(!cfg.storage.use_direct_io);
        assert!(!cfg.storage.memory_mode);
        assert!(cfg.wal.enabled);
        assert_eq!(cfg.wal.segment_size_mb, 64);
        assert_eq!(cfg.wal.sync_mode, "fsync");
        assert_eq!(cfg.pool.min_idle, 5);
        assert_eq!(cfg.pool.max_lifetime_secs, 3600);
        assert!(!cfg.cache.enabled);
        assert_eq!(cfg.cache.max_memory_mb, 16);
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
        assert_eq!(sc.buffer_pool_size_mb, 32);
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
        assert_eq!(cc.max_memory_mb, 16);
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::remove_var("NUCLEUS_SERVER_HOST");
            env::remove_var("NUCLEUS_SERVER_PORT");
            env::remove_var("NUCLEUS_SERVER_MAX_CONNECTIONS");
        }
    }

    #[test]
    fn test_env_override_storage() {
        let mut cfg = NucleusConfig::default();
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::set_var("NUCLEUS_STORAGE_DATA_DIR", "/mnt/ssd/nucleus");
            env::set_var("NUCLEUS_STORAGE_MEMORY_MODE", "true");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.storage.data_dir, "/mnt/ssd/nucleus");
        assert!(cfg.storage.memory_mode);
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::remove_var("NUCLEUS_STORAGE_DATA_DIR");
            env::remove_var("NUCLEUS_STORAGE_MEMORY_MODE");
        }
    }

    #[test]
    fn test_env_override_wal_and_logging() {
        let mut cfg = NucleusConfig::default();
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::set_var("NUCLEUS_WAL_ENABLED", "false");
            env::set_var("NUCLEUS_LOGGING_LEVEL", "trace");
        }
        cfg.apply_env_overrides();
        assert!(!cfg.wal.enabled);
        assert_eq!(cfg.logging.level, "trace");
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::remove_var("NUCLEUS_WAL_ENABLED");
            env::remove_var("NUCLEUS_LOGGING_LEVEL");
        }
    }

    #[test]
    fn test_env_override_metrics_and_cache() {
        let mut cfg = NucleusConfig::default();
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
                max_memory_mb: 512,
                idle_in_transaction_timeout_secs: 0,
                query_memory_percent: default_query_memory_percent(),
                reject_writes_on_memory_critical: false,
            },
            storage: StorageConfig {
                data_dir: "/tmp/nucleus".to_string(),
                buffer_pool_size_mb: 32,
                page_size: 4096,
                use_direct_io: true,
                memory_mode: false,
                ..StorageConfig::default()
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::set_var("NUCLEUS_STORAGE_BUFFER_POOL_SIZE_MB", "256");
            env::set_var("NUCLEUS_STORAGE_USE_DIRECT_IO", "true");
        }
        cfg.apply_env_overrides();
        assert_eq!(cfg.storage.buffer_pool_size_mb, 256);
        assert!(cfg.storage.use_direct_io);
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::remove_var("NUCLEUS_STORAGE_BUFFER_POOL_SIZE_MB");
            env::remove_var("NUCLEUS_STORAGE_USE_DIRECT_IO");
        }
    }

    #[test]
    fn test_env_max_memory_survives_default_cli_merge() {
        // Regression: `nucleus start` used to pass its clap default
        // (Some(512)) into merge_cli_args unconditionally, stomping
        // NUCLEUS_MAX_MEMORY_MB / nucleus.toml. main.rs now passes None
        // when --max-memory wasn't explicitly given; merge_cli_args must
        // preserve the env-driven value in that case.
        let mut cfg = NucleusConfig::default();
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::set_var("NUCLEUS_MAX_MEMORY_MB", "8192");
        }
        cfg.apply_env_overrides();
        cfg.merge_cli_args(None, None, None, None, None);
        assert_eq!(cfg.server.max_memory_mb, 8192);
        // An explicit CLI value still wins.
        cfg.merge_cli_args(None, None, None, None, Some(1024));
        assert_eq!(cfg.server.max_memory_mb, 1024);
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
        unsafe {
            env::remove_var("NUCLEUS_MAX_MEMORY_MB");
        }
    }

    #[test]
    fn test_env_override_wal_extended() {
        let mut cfg = NucleusConfig::default();
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        // SAFETY: test-only. edition-2024 marks process-env mutation unsafe (not thread-safe); each test sets and removes its own NUCLEUS_* keys.
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
        cfg.merge_cli_args(
            Some("192.168.1.1"),
            Some(6543),
            Some("/data/db"),
            Some(true),
            Some(256),
        );
        assert_eq!(cfg.server.host, "192.168.1.1");
        assert_eq!(cfg.server.port, 6543);
        assert_eq!(cfg.storage.data_dir, "/data/db");
        assert!(cfg.storage.memory_mode);
        assert_eq!(cfg.server.max_memory_mb, 256);
    }

    #[test]
    fn test_merge_cli_args_none_preserves_defaults() {
        let mut cfg = NucleusConfig::default();
        let original = cfg.clone();
        cfg.merge_cli_args(None, None, None, None, None);
        assert_eq!(cfg, original);
    }

    #[test]
    fn test_merge_cli_args_partial() {
        let mut cfg = NucleusConfig::default();
        cfg.merge_cli_args(None, Some(9999), None, None, None);
        assert_eq!(cfg.server.host, "127.0.0.1"); // unchanged
        assert_eq!(cfg.server.port, 9999); // changed
    }

    // -----------------------------------------------------------------
    // validate() — startup config gates
    // -----------------------------------------------------------------

    /// The shipped defaults must pass their own validator, or every fresh
    /// install fails to start.
    #[test]
    fn default_config_is_valid() {
        assert_eq!(NucleusConfig::default().validate(), Ok(()));
    }

    /// A realistic hand-written config must also pass.
    #[test]
    fn representative_config_is_valid() {
        let mut cfg = NucleusConfig::default();
        cfg.server.max_connections = 200;
        cfg.server.max_memory_mb = 4096;
        cfg.storage.buffer_pool_size_mb = 1024;
        cfg.cache.enabled = true;
        cfg.cache.max_memory_mb = 512;
        cfg.metrics.enabled = true;
        cfg.metrics.port = 9100;
        cfg.logging.level = "debug".to_string();
        cfg.logging.format = "json".to_string();
        assert_eq!(cfg.validate(), Ok(()));
    }

    fn errors_of(cfg: &NucleusConfig) -> Vec<String> {
        cfg.validate().unwrap_err()
    }

    fn assert_flags(cfg: &NucleusConfig, needle: &str) {
        let errs = errors_of(cfg);
        assert!(
            errs.iter().any(|e| e.contains(needle)),
            "expected an error mentioning {needle:?}, got {errs:?}"
        );
    }

    #[test]
    fn zero_max_connections_is_rejected() {
        let mut cfg = NucleusConfig::default();
        cfg.server.max_connections = 0;
        assert_flags(&cfg, "server.max_connections");
    }

    #[test]
    fn pool_min_idle_above_max_connections_is_rejected() {
        let mut cfg = NucleusConfig::default();
        cfg.server.max_connections = 10;
        cfg.pool.min_idle = 25;
        assert_flags(&cfg, "pool.min_idle");
        // Just under: fine.
        cfg.pool.min_idle = 10;
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn overcommitted_memory_budget_is_rejected() {
        let mut cfg = NucleusConfig::default();
        cfg.server.max_memory_mb = 512;
        cfg.storage.buffer_pool_size_mb = 400;
        cfg.cache.max_memory_mb = 200; // 600 > 512
        assert_flags(&cfg, "exceeds server.max_memory_mb");
        // Just under the ceiling: accepted.
        cfg.cache.max_memory_mb = 112;
        assert_eq!(cfg.validate(), Ok(()));
        // 0 means "no global limit", so over-commit is not checkable.
        cfg.server.max_memory_mb = 0;
        cfg.cache.max_memory_mb = 4096;
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn bad_page_size_is_rejected() {
        let mut cfg = NucleusConfig::default();
        cfg.storage.page_size = 5000; // not a power of two
        assert_flags(&cfg, "storage.page_size");
        cfg.storage.page_size = 256; // too small
        assert_flags(&cfg, "storage.page_size");
        cfg.storage.page_size = 8192;
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn typoed_enums_fail_instead_of_silently_defaulting() {
        let mut cfg = NucleusConfig::default();
        cfg.wal.sync_mode = "fsyncc".to_string();
        assert_flags(&cfg, "wal.sync_mode");

        let mut cfg = NucleusConfig::default();
        cfg.logging.level = "verbose".to_string();
        assert_flags(&cfg, "logging.level");

        let mut cfg = NucleusConfig::default();
        cfg.cache.eviction_policy = "mru".to_string();
        assert_flags(&cfg, "cache.eviction_policy");

        // Values the codebase actually uses must all pass.
        let mut cfg = NucleusConfig::default();
        cfg.wal.sync_mode = "fdatasync".to_string();
        cfg.cache.eviction_policy = "lfu".to_string();
        cfg.replication.mode = "primary".to_string();
        cfg.replication.sync_mode = "sync".to_string();
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn replica_without_a_primary_host_is_rejected() {
        let mut cfg = NucleusConfig::default();
        cfg.replication.mode = "replica".to_string();
        assert_flags(&cfg, "primary_host");
        cfg.replication.primary_host = Some("10.0.0.2".to_string());
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn metrics_port_colliding_with_the_sql_port_is_rejected() {
        let mut cfg = NucleusConfig::default();
        cfg.metrics.enabled = true;
        cfg.metrics.port = cfg.server.port;
        assert_flags(&cfg, "metrics.port");
        // Disabled metrics cannot collide.
        cfg.metrics.enabled = false;
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn inverted_disk_watermarks_are_rejected_at_startup() {
        // read-only above warn: the server would degrade without ever warning.
        let mut cfg = NucleusConfig::default();
        cfg.storage.disk_readonly_free_pct = 20.0;
        cfg.storage.disk_warn_free_pct = 10.0;
        assert_flags(&cfg, "without ever warning");

        // resume below read-only: the server would flap at the boundary.
        let mut cfg = NucleusConfig::default();
        cfg.storage.disk_resume_free_pct = 1.0;
        cfg.storage.disk_readonly_free_pct = 5.0;
        assert_flags(&cfg, "flap");
    }

    #[test]
    fn every_validation_problem_is_reported_at_once() {
        let mut cfg = NucleusConfig::default();
        cfg.server.max_connections = 0;
        cfg.storage.page_size = 3;
        cfg.logging.format = "xml".to_string();
        let errs = errors_of(&cfg);
        assert!(
            errs.len() >= 3,
            "expected all problems at once, got {errs:?}"
        );
    }

    #[test]
    fn disk_watermarks_come_from_config_including_the_mb_to_bytes_conversion() {
        let mut cfg = NucleusConfig::default();
        cfg.storage.disk_min_free_mb = 512;
        cfg.storage.disk_readonly_free_pct = 4.5;
        let marks = cfg.storage.disk_watermarks();
        assert_eq!(marks.min_free_bytes, 512 * 1024 * 1024);
        assert_eq!(marks.readonly_free_pct, 4.5);
    }
}
