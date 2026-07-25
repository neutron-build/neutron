//! Explicit storage-engine selection and resource accounting for scale/soak
//! harnesses.
//!
//! ## Why this exists
//!
//! The scale harnesses used to open whichever engine was convenient — in
//! practice `MvccStorageAdapter`, a `RwLock<Vec<MvccRow>>` that lives entirely
//! in RAM and whose secondary indexes clone rows. The server does not run that
//! engine: `main.rs` opens a paged [`DiskEngine`] and wraps it in
//! [`BufferedDiskEngine`] for transaction atomicity. Numbers produced against
//! the RAM engine describe a database nobody deploys, and nothing in the output
//! said which engine had been measured.
//!
//! So engine choice is now explicit and named. Every harness takes
//! `--engine <kind>`, defaults to [`EngineKind::BufferedDisk`] (what the server
//! runs), and prints the engine beside every number. The in-RAM engines remain
//! selectable so they can be measured *deliberately*, as a comparison point,
//! rather than by accident.
//!
//! ## What "server-default" means here
//!
//! [`EngineKind::BufferedDisk`] reproduces `main.rs`'s startup path: persisted
//! catalog, `DiskEngine::open_segmented_with_sync`, the configured buffer-pool
//! frame count, `BufferedDiskEngine` on top, and an executor built with
//! `new_with_persistence` + a metrics registry. The defaults in
//! [`EngineConfig`] are taken from `src/config/mod.rs`, so an unconfigured
//! harness run and an unconfigured server run see the same storage.

#![cfg(feature = "server")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::catalog::Catalog;
use crate::executor::{ExecError, ExecResult, Executor};
use crate::metrics::MetricsRegistry;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::{DiskEngine, MemoryEngine, MvccStorageAdapter, StorageEngine, wal};
use crate::types::{Row, Value};

/// Which storage engine a harness run measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineKind {
    /// `BufferedDiskEngine` over `DiskEngine` — **what `nucleus serve` runs**.
    BufferedDisk,
    /// Bare `DiskEngine`, no transaction buffering. Isolates the cost of the
    /// buffering layer from the cost of paged storage.
    Disk,
    /// `MvccStorageAdapter` with a WAL. Durable, but rows live in RAM.
    DurableMvcc,
    /// `MvccStorageAdapter`, no WAL. RAM-resident, snapshot isolation.
    Mvcc,
    /// `MemoryEngine`. RAM-resident, no isolation. Upper bound, not a product.
    Memory,
}

impl EngineKind {
    /// Every selectable engine, in the order harness `--help` should list them.
    pub const ALL: [EngineKind; 5] = [
        EngineKind::BufferedDisk,
        EngineKind::Disk,
        EngineKind::DurableMvcc,
        EngineKind::Mvcc,
        EngineKind::Memory,
    ];

    /// Parse a CLI selector. Accepts `-` or `_` separators.
    pub fn parse(s: &str) -> Option<EngineKind> {
        match s.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "buffered-disk" | "server" | "default" => Some(EngineKind::BufferedDisk),
            "disk" => Some(EngineKind::Disk),
            "durable-mvcc" => Some(EngineKind::DurableMvcc),
            "mvcc" => Some(EngineKind::Mvcc),
            "memory" | "mem" => Some(EngineKind::Memory),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            EngineKind::BufferedDisk => "buffered-disk",
            EngineKind::Disk => "disk",
            EngineKind::DurableMvcc => "durable-mvcc",
            EngineKind::Mvcc => "mvcc",
            EngineKind::Memory => "memory",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            EngineKind::BufferedDisk => {
                "BufferedDiskEngine over DiskEngine — the engine `nucleus serve` runs"
            }
            EngineKind::Disk => "DiskEngine, paged + WAL, without transaction buffering",
            EngineKind::DurableMvcc => "MvccStorageAdapter + WAL — rows resident in RAM",
            EngineKind::Mvcc => "MvccStorageAdapter, no WAL — rows resident in RAM",
            EngineKind::Memory => "MemoryEngine — RAM-resident, no isolation",
        }
    }

    /// True for the engine the shipped server actually uses.
    pub fn is_server_default(self) -> bool {
        self == EngineKind::BufferedDisk
    }

    /// True when the engine keeps its whole row set in RAM. Capacity numbers
    /// from these engines describe RAM, not storage.
    pub fn is_ram_resident(self) -> bool {
        matches!(
            self,
            EngineKind::DurableMvcc | EngineKind::Mvcc | EngineKind::Memory
        )
    }

    /// True when data survives process exit.
    pub fn is_durable(self) -> bool {
        matches!(
            self,
            EngineKind::BufferedDisk | EngineKind::Disk | EngineKind::DurableMvcc
        )
    }

    /// True when the engine has a paged buffer pool (so cache hit rate exists).
    pub fn has_buffer_pool(self) -> bool {
        matches!(self, EngineKind::BufferedDisk | EngineKind::Disk)
    }
}

impl std::fmt::Display for EngineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Storage knobs a harness may vary. Defaults mirror `src/config/mod.rs`, so an
/// unconfigured harness run matches an unconfigured `nucleus serve`.
#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
    /// Buffer-pool size in MB (server default 32).
    pub buffer_pool_mb: usize,
    /// WAL segment size in MB; 0 selects a single-file WAL (server default 64).
    pub wal_segment_mb: usize,
    /// WAL sync mode (server default `fsync`).
    pub sync_mode: wal::SyncMode,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            buffer_pool_mb: 32,
            wal_segment_mb: 64,
            sync_mode: wal::SyncMode::Fsync,
        }
    }
}

impl EngineConfig {
    pub fn describe(&self) -> String {
        format!(
            "buffer_pool={}MB wal_segment={} sync={:?}",
            self.buffer_pool_mb,
            if self.wal_segment_mb == 0 {
                "single-file".to_string()
            } else {
                format!("{}MB", self.wal_segment_mb)
            },
            self.sync_mode
        )
    }
}

/// Storage-side counters sampled at a point in time.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageSnapshot {
    /// Buffer-pool hits (paged engines only).
    pub cache_hits: u64,
    /// Buffer-pool misses (paged engines only).
    pub cache_misses: u64,
    /// Buffer-pool evictions.
    pub evictions: u64,
    /// Dirty pages currently resident.
    pub dirty_pages: u64,
    /// WAL bytes written since open.
    pub wal_bytes: u64,
    /// WAL sync calls since open.
    pub wal_syncs: u64,
    /// Total bytes on disk under the data directory.
    pub disk_bytes: u64,
    /// Process RSS at sample time.
    pub rss_bytes: u64,
}

impl StorageSnapshot {
    /// Buffer-pool hit rate in `[0, 1]`. Returns `None` when the engine has no
    /// buffer pool or nothing has been read yet — reporting 0.0 there would be
    /// a fabricated datum.
    pub fn cache_hit_rate(&self) -> Option<f64> {
        let total = self.cache_hits + self.cache_misses;
        (total > 0).then(|| self.cache_hits as f64 / total as f64)
    }

    /// Field-wise difference `self - earlier`, saturating at zero.
    pub fn delta(&self, earlier: &StorageSnapshot) -> StorageSnapshot {
        StorageSnapshot {
            cache_hits: self.cache_hits.saturating_sub(earlier.cache_hits),
            cache_misses: self.cache_misses.saturating_sub(earlier.cache_misses),
            evictions: self.evictions.saturating_sub(earlier.evictions),
            dirty_pages: self.dirty_pages,
            wal_bytes: self.wal_bytes.saturating_sub(earlier.wal_bytes),
            wal_syncs: self.wal_syncs.saturating_sub(earlier.wal_syncs),
            disk_bytes: self.disk_bytes.saturating_sub(earlier.disk_bytes),
            rss_bytes: self.rss_bytes,
        }
    }
}

/// A database opened on an explicitly named engine, with the storage handles a
/// harness needs to account for its own cost.
pub struct HarnessDb {
    kind: EngineKind,
    config: EngineConfig,
    executor: Arc<Executor>,
    /// Present only for paged engines — the source of buffer-pool/WAL counters
    /// and the target of an explicit checkpoint.
    disk: Option<Arc<DiskEngine>>,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<Catalog>,
    dir: PathBuf,
    /// Wall time the last `open` spent before the database accepted SQL. For a
    /// reopen of a populated directory this is recovery time.
    open_elapsed: Duration,
}

impl HarnessDb {
    /// Open (or reopen) `dir` on the named engine.
    ///
    /// Reopening the same directory on a durable engine exercises the real
    /// recovery path, and [`HarnessDb::open_elapsed`] reports how long it took.
    pub async fn open(
        kind: EngineKind,
        dir: &Path,
        config: EngineConfig,
    ) -> Result<Self, HarnessError> {
        let t0 = Instant::now();
        std::fs::create_dir_all(dir).map_err(|e| HarnessError(format!("create {dir:?}: {e}")))?;
        let catalog = Arc::new(Catalog::new());
        let catalog_path = dir.join("catalog.json");

        let mut disk: Option<Arc<DiskEngine>> = None;
        let storage: Arc<dyn StorageEngine> = match kind {
            EngineKind::Memory => Arc::new(MemoryEngine::new()),
            EngineKind::Mvcc => Arc::new(MvccStorageAdapter::new()),
            EngineKind::DurableMvcc => {
                let (adapter, schemas) = MvccStorageAdapter::with_wal(dir)
                    .map_err(|e| HarnessError(format!("open durable-mvcc: {e}")))?;
                register_recovered(&catalog, schemas, Default::default());
                Arc::new(adapter)
            }
            EngineKind::Disk | EngineKind::BufferedDisk => {
                // Mirror main.rs: restore the persisted catalog first so index
                // and constraint definitions survive a reopen, then open the
                // paged engine against that catalog.
                CatalogPersistence::new(&catalog_path)
                    .load_catalog(&catalog)
                    .await
                    .ok();
                let engine = Arc::new(open_disk_engine(dir, catalog.clone(), &config)?);
                let schemas = engine.recovered_schemas();
                let epochs = engine.recovered_table_epochs();
                register_recovered(&catalog, schemas, epochs);
                // Re-register catalog tables with the engine (main.rs does the
                // same) so a restored catalog and the on-disk directory agree.
                for table in catalog.table_names().await {
                    engine.create_table(&table).await.ok();
                }
                disk = Some(engine.clone());
                if kind == EngineKind::BufferedDisk {
                    Arc::new(BufferedDiskEngine::new(engine))
                } else {
                    engine as Arc<dyn StorageEngine>
                }
            }
        };

        let executor = Arc::new(
            Executor::new_with_persistence(
                catalog.clone(),
                storage.clone(),
                kind.is_durable().then(|| catalog_path.clone()),
                Some(dir),
            )
            .with_metrics(Arc::new(MetricsRegistry::new())),
        );
        executor.load_meta().await;

        Ok(Self {
            kind,
            config,
            executor,
            disk,
            storage,
            catalog,
            dir: dir.to_path_buf(),
            open_elapsed: t0.elapsed(),
        })
    }

    pub fn kind(&self) -> EngineKind {
        self.kind
    }

    pub fn config(&self) -> EngineConfig {
        self.config
    }

    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Wall time the last open took. On a populated directory this is recovery
    /// time: catalog load + WAL replay + engine bring-up.
    pub fn open_elapsed(&self) -> Duration {
        self.open_elapsed
    }

    /// One line naming the engine and configuration behind every number this
    /// run reports. Print it next to results — a latency without its engine is
    /// not a datum.
    pub fn provenance(&self) -> String {
        format!(
            "engine={} ({}) {}",
            self.kind.name(),
            self.kind.description(),
            self.config.describe()
        )
    }

    pub async fn execute(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        self.executor.execute(sql).await
    }

    /// Rows of the last SELECT in `sql`.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>, ExecError> {
        let results = self.executor.execute(sql).await?;
        for result in results.into_iter().rev() {
            if let ExecResult::Select { rows, .. } = result {
                return Ok(rows);
            }
        }
        Ok(Vec::new())
    }

    /// First column of the first row of the last SELECT.
    pub async fn query_one(&self, sql: &str) -> Result<Option<Value>, ExecError> {
        Ok(self
            .query(sql)
            .await?
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next()))
    }

    pub fn sync(&self) -> Result<(), ExecError> {
        self.storage.sync().map_err(ExecError::Storage)
    }

    /// Force a checkpoint and report what it cost. `None` when the engine has
    /// no checkpoint (the RAM engines).
    ///
    /// Checkpoint cost is one of the ledger's tracked quantities and had no
    /// counter anywhere in the tree; timing the operation directly is the
    /// honest way to get it without plumbing a registry through storage.
    pub fn checkpoint(&self) -> Option<Result<Duration, String>> {
        let disk = self.disk.as_ref()?;
        let t = Instant::now();
        Some(match disk.checkpoint() {
            Ok(()) => Ok(t.elapsed()),
            Err(e) => Err(e.to_string()),
        })
    }

    /// Sample every storage counter the ledger names.
    pub fn snapshot(&self) -> StorageSnapshot {
        let mut s = StorageSnapshot {
            disk_bytes: dir_size_bytes(&self.dir),
            rss_bytes: rss_bytes(),
            ..Default::default()
        };
        if let Some(disk) = &self.disk {
            let pool = disk.buffer_pool();
            let (hits, misses, evictions, dirty) = pool.stats().snapshot();
            let (wal_bytes, wal_syncs) = pool.wal_stats();
            s.cache_hits = hits;
            s.cache_misses = misses;
            s.evictions = evictions;
            s.dirty_pages = dirty;
            s.wal_bytes = wal_bytes;
            s.wal_syncs = wal_syncs;
        }
        s
    }

    /// Close the database, releasing engine handles and flushing durable state.
    /// Returns once nothing else holds the storage, so the directory can be
    /// reopened to measure recovery.
    pub fn close(self) {
        let _ = self.sync();
        drop(self);
    }
}

fn open_disk_engine(
    dir: &Path,
    catalog: Arc<Catalog>,
    config: &EngineConfig,
) -> Result<DiskEngine, HarnessError> {
    let db_path = dir.join("nucleus.db");
    let pool_frames = (config.buffer_pool_mb * 1024 * 1024) / 16384;
    let result = if config.wal_segment_mb > 0 {
        DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog,
            pool_frames,
            config.wal_segment_mb,
            config.sync_mode,
        )
    } else {
        DiskEngine::open_with_pool_size(&db_path, catalog, pool_frames)
    };
    result.map_err(|e| HarnessError(format!("open disk engine at {db_path:?}: {e}")))
}

/// Register WAL/directory-recovered schemas into a fresh catalog (the same
/// bridge `embedded.rs` performs). Tables the persisted catalog already knows
/// are left alone — that definition is richer (constraints, defaults).
fn register_recovered(
    catalog: &Arc<Catalog>,
    schemas: Vec<(String, Vec<(String, crate::types::DataType)>)>,
    epochs: std::collections::HashMap<String, u64>,
) {
    use crate::catalog::{ColumnDef, TableDef};
    for (name, columns) in schemas {
        // `create_table_sync` rejects a name the catalog already holds, so a
        // richer restored definition (constraints, defaults) always wins.
        let cols = columns
            .into_iter()
            .map(|(col_name, data_type)| ColumnDef {
                name: col_name,
                data_type,
                nullable: true,
                default_expr: None,
            })
            .collect();
        let epoch = epochs.get(&name).copied().unwrap_or(0);
        let _ = catalog.create_table_sync(TableDef {
            name,
            columns: cols,
            constraints: Vec::new(),
            append_only: false,
            epoch,
        });
    }
}

/// Harness setup failure.
#[derive(Debug)]
pub struct HarnessError(pub String);

impl std::fmt::Display for HarnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HarnessError {}

/// Total bytes of every regular file under `dir`, recursively. Symlinks are not
/// followed, so a link into another dataset cannot inflate the number.
pub fn dir_size_bytes(dir: &Path) -> u64 {
    fn walk(dir: &Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_file() {
                *total += meta.len();
            } else if meta.is_dir() {
                walk(&entry.path(), total);
            }
        }
    }
    let mut total = 0;
    walk(dir, &mut total);
    total
}

/// Current process resident set size in bytes, or 0 if unavailable.
///
/// Linux reads `/proc/self/status`. macOS has no `/proc`, so it shells out to
/// `ps` — slower, but a real number beats a zero that silently disables every
/// memory gate built on top of it.
pub fn rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:")
                    && let Some(kb) = rest
                        .split_whitespace()
                        .next()
                        .and_then(|n| n.parse::<u64>().ok())
                {
                    return kb * 1024;
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        let pid = std::process::id();
        let Ok(out) = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
        else {
            return 0;
        };
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<u64>()
            .map(|kb| kb * 1024)
            .unwrap_or(0)
    }
}

/// The machine a measurement came from. A latency without its hardware is not a
/// datum, so every harness that emits a number emits this beside it.
#[derive(Debug, Clone)]
pub struct MachineInfo {
    pub os: String,
    pub arch: String,
    pub cpu_model: String,
    pub logical_cpus: usize,
    pub total_memory_bytes: u64,
    pub build_profile: &'static str,
    pub nucleus_version: &'static str,
}

impl MachineInfo {
    pub fn detect() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_model: cpu_model(),
            logical_cpus: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(0),
            total_memory_bytes: total_memory_bytes(),
            build_profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
            nucleus_version: env!("CARGO_PKG_VERSION"),
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "{}/{} {} · {} logical CPUs · {:.1} GB RAM · nucleus {} ({})",
            self.os,
            self.arch,
            self.cpu_model,
            self.logical_cpus,
            self.total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            self.nucleus_version,
            self.build_profile
        )
    }
}

fn cpu_model() -> String {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/cpuinfo") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("model name") {
                    return rest.trim_start_matches([' ', ':']).trim().to_string();
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            && let Ok(s) = String::from_utf8(out.stdout)
            && !s.trim().is_empty()
        {
            return s.trim().to_string();
        }
    }
    "unknown-cpu".to_string()
}

fn total_memory_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:")
                    && let Some(kb) = rest
                        .split_whitespace()
                        .next()
                        .and_then(|n| n.parse::<u64>().ok())
                {
                    return kb * 1024;
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Regression-budget file parsing
// ---------------------------------------------------------------------------
//
// A budget file is the artifact most likely to rot silently: if the parser
// drops a bound, the run still prints "all bounds satisfied" and the tripwire
// is gone. (That is not hypothetical — the first version of this parser was
// swallowing the first bound in the file, so `mixed.ops_per_sec` was never
// checked.) The parsing therefore lives here, in the library, where
// `cargo test --lib` covers it.

/// One bound from a budget file. Exactly one side is normally set.
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetBound {
    pub metric: String,
    /// Fail if the measured value is above this.
    pub max: Option<f64>,
    /// Fail if the measured value is below this.
    pub min: Option<f64>,
}

/// A parsed budget file: the provenance that decides comparability, plus bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetFile {
    pub engine: String,
    pub machine: String,
    pub config: String,
    pub workload: String,
    pub runs_recorded: u64,
    pub bounds: Vec<BudgetBound>,
}

/// String field extraction. Deliberately minimal — a scale harness should not
/// drag a JSON dependency into the server binary's feature set to read six
/// fields.
pub fn json_str_field(src: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\"");
    let start = src.find(&needle)? + needle.len();
    let rest = &src[start..];
    let colon = rest.find(':')?;
    let after = rest[colon + 1..].trim_start();
    let after = after.strip_prefix('"')?;
    let end = after.find('"')?;
    Some(after[..end].to_string())
}

/// Numeric field extraction, companion to [`json_str_field`].
pub fn json_num_field(src: &str, key: &str) -> Option<f64> {
    let needle = format!("\"{key}\"");
    let start = src.find(&needle)? + needle.len();
    let rest = &src[start..];
    let colon = rest.find(':')?;
    rest[colon + 1..]
        .trim_start()
        .split(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-'))
        .find(|t| !t.is_empty())?
        .parse()
        .ok()
}

/// Parse a budget file written by `probe_soak --write-budget`.
pub fn parse_budget_file(src: &str) -> Result<BudgetFile, String> {
    let engine = json_str_field(src, "engine").ok_or("budget file has no \"engine\"")?;
    let machine = json_str_field(src, "machine").ok_or("budget file has no \"machine\"")?;

    // Scan strictly inside the "budgets" object, starting after its opening
    // brace, so the wrapper key can never be mistaken for the first entry.
    let budgets_at = src
        .find("\"budgets\"")
        .ok_or("budget file has no \"budgets\"")?;
    let open = src[budgets_at..]
        .find('{')
        .ok_or("\"budgets\" is not an object")?
        + budgets_at;
    let body = &src[open + 1..];

    let mut bounds = Vec::new();
    let mut rest = body;
    while let Some(q) = rest.find('"') {
        let after = &rest[q + 1..];
        let Some(close) = after.find('"') else { break };
        let metric = after[..close].to_string();
        let tail = &after[close + 1..];
        let Some(brace) = tail.find('{') else { break };
        // Between the metric name and its object there must be only a colon;
        // anything else means we have walked out of the budgets object.
        if tail[..brace].trim() != ":" {
            break;
        }
        let Some(end_rel) = tail[brace..].find('}') else {
            break;
        };
        let entry = &tail[brace..brace + end_rel];
        let num = |k: &str| -> Option<f64> { json_num_field(entry, k) };
        let (max, min) = (num("max"), num("min"));
        if max.is_none() && min.is_none() {
            return Err(format!(
                "budget metric '{metric}' declares neither max nor min"
            ));
        }
        bounds.push(BudgetBound { metric, max, min });
        rest = &tail[brace + end_rel..];
    }

    if bounds.is_empty() {
        return Err("budget file declares no bounds".into());
    }
    Ok(BudgetFile {
        engine,
        machine,
        config: json_str_field(src, "config").unwrap_or_default(),
        workload: json_str_field(src, "workload").unwrap_or_default(),
        runs_recorded: json_num_field(src, "runs_recorded").unwrap_or(1.0) as u64,
        bounds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_BUDGET: &str = r#"{
  "_comment": "an envelope over runs, with { braces } and \"budgets\" mentioned in prose",
  "engine": "buffered-disk",
  "machine": "macos/aarch64/Apple M4/10cpu/release",
  "config": "buffer_pool=32MB wal_segment=64MB sync=Fsync",
  "recorded_slack": 1.5,
  "runs_recorded": 3,
  "all_source_runs_passed_invariants": "false",
  "workload": "concurrency=8 duration_secs=90 rows_target=0",
  "budgets": {
    "mixed.ops_per_sec": {"min": 33.119},
    "insert.p50_us": {"max": 22809.000},
    "recovery_ms": {"max": 239.968}
  }
}
"#;

    #[test]
    fn budget_parse_keeps_the_first_bound() {
        // The original parser treated the "budgets" wrapper as an entry and
        // silently swallowed the first metric, so the throughput floor was
        // never enforced and every run still printed "all bounds satisfied".
        let b = parse_budget_file(SAMPLE_BUDGET).unwrap();
        let names: Vec<&str> = b.bounds.iter().map(|x| x.metric.as_str()).collect();
        assert_eq!(
            names,
            vec!["mixed.ops_per_sec", "insert.p50_us", "recovery_ms"],
            "every bound must survive parsing, including the first"
        );
    }

    #[test]
    fn budget_parse_reads_provenance_and_bounds() {
        let b = parse_budget_file(SAMPLE_BUDGET).unwrap();
        assert_eq!(b.engine, "buffered-disk");
        assert_eq!(b.machine, "macos/aarch64/Apple M4/10cpu/release");
        assert_eq!(b.config, "buffer_pool=32MB wal_segment=64MB sync=Fsync");
        assert_eq!(b.workload, "concurrency=8 duration_secs=90 rows_target=0");
        assert_eq!(b.runs_recorded, 3);
        assert_eq!(b.bounds[0].min, Some(33.119));
        assert_eq!(b.bounds[0].max, None);
        assert_eq!(b.bounds[1].max, Some(22809.0));
        assert_eq!(b.bounds[2].max, Some(239.968));
    }

    #[test]
    fn budget_parse_rejects_files_with_nothing_to_enforce() {
        assert!(parse_budget_file("{}").is_err());
        assert!(parse_budget_file(r#"{"engine":"disk","machine":"m"}"#).is_err());
        let empty = r#"{"engine":"disk","machine":"m","budgets": {}}"#;
        assert!(
            parse_budget_file(empty).is_err(),
            "an empty budget enforces nothing and must not parse as valid"
        );
        let bogus = r#"{"engine":"disk","machine":"m","budgets": {"a": {"typo": 1.0}}}"#;
        assert!(
            parse_budget_file(bogus).is_err(),
            "a bound with neither max nor min must be rejected, not skipped"
        );
    }

    #[test]
    fn json_num_field_reads_ints_and_floats() {
        let src = r#"{"a": 3, "b": 1.5, "c": -2.25, "d": "text"}"#;
        assert_eq!(json_num_field(src, "a"), Some(3.0));
        assert_eq!(json_num_field(src, "b"), Some(1.5));
        assert_eq!(json_num_field(src, "c"), Some(-2.25));
        assert_eq!(json_num_field(src, "missing"), None);
    }

    #[test]
    fn every_engine_name_round_trips() {
        for kind in EngineKind::ALL {
            assert_eq!(EngineKind::parse(kind.name()), Some(kind));
        }
        assert_eq!(EngineKind::parse("SERVER"), Some(EngineKind::BufferedDisk));
        assert_eq!(
            EngineKind::parse("durable_mvcc"),
            Some(EngineKind::DurableMvcc)
        );
        assert_eq!(EngineKind::parse("nope"), None);
    }

    #[test]
    fn only_buffered_disk_is_the_server_default() {
        let defaults: Vec<_> = EngineKind::ALL
            .iter()
            .filter(|k| k.is_server_default())
            .collect();
        assert_eq!(defaults, vec![&EngineKind::BufferedDisk]);
    }

    #[test]
    fn ram_resident_engines_are_labelled() {
        assert!(!EngineKind::BufferedDisk.is_ram_resident());
        assert!(!EngineKind::Disk.is_ram_resident());
        assert!(EngineKind::DurableMvcc.is_ram_resident());
        assert!(EngineKind::Mvcc.is_ram_resident());
        assert!(EngineKind::Memory.is_ram_resident());
    }

    #[test]
    fn cache_hit_rate_is_none_without_reads() {
        let empty = StorageSnapshot::default();
        assert!(empty.cache_hit_rate().is_none());
        let s = StorageSnapshot {
            cache_hits: 3,
            cache_misses: 1,
            ..Default::default()
        };
        assert_eq!(s.cache_hit_rate(), Some(0.75));
    }

    #[test]
    fn snapshot_delta_saturates() {
        let a = StorageSnapshot {
            cache_hits: 10,
            wal_bytes: 100,
            ..Default::default()
        };
        let b = StorageSnapshot {
            cache_hits: 4,
            wal_bytes: 250,
            ..Default::default()
        };
        let d = b.delta(&a);
        assert_eq!(d.cache_hits, 0);
        assert_eq!(d.wal_bytes, 150);
    }

    #[test]
    fn dir_size_counts_nested_files() {
        let dir = std::env::temp_dir().join(format!("nucleus_dirsize_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("a"), vec![0u8; 1000]).unwrap();
        std::fs::write(dir.join("nested/b"), vec![0u8; 24]).unwrap();
        assert_eq!(dir_size_bytes(&dir), 1024);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn machine_info_is_populated() {
        let m = MachineInfo::detect();
        assert!(!m.os.is_empty());
        assert!(!m.arch.is_empty());
        assert!(m.logical_cpus >= 1);
        assert!(m.describe().contains(&m.arch));
    }

    #[test]
    fn rss_is_nonzero_on_supported_platforms() {
        // The whole point of the macOS fallback: a zero here silently disables
        // every memory gate downstream.
        assert!(rss_bytes() > 0, "RSS probe returned 0 on this platform");
    }

    #[tokio::test]
    async fn buffered_disk_round_trips_and_reports_provenance() {
        let dir = std::env::temp_dir().join(format!(
            "nucleus_harness_bd_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let db = HarnessDb::open(EngineKind::BufferedDisk, &dir, EngineConfig::default())
            .await
            .unwrap();
        assert!(db.provenance().contains("buffered-disk"));
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute("INSERT INTO t (id, v) VALUES (1, 10), (2, 20)")
            .await
            .unwrap();
        let n = db.query_one("SELECT COUNT(*) FROM t").await.unwrap();
        assert!(matches!(n, Some(Value::Int64(2)) | Some(Value::Int32(2))));

        let cp = db.checkpoint().expect("paged engine checkpoints");
        cp.expect("checkpoint succeeds");
        let snap = db.snapshot();
        assert!(snap.disk_bytes > 0, "paged engine must occupy disk");

        db.close();
        // Reopen exercises the real recovery path.
        let db2 = HarnessDb::open(EngineKind::BufferedDisk, &dir, EngineConfig::default())
            .await
            .unwrap();
        let n = db2.query_one("SELECT COUNT(*) FROM t").await.unwrap();
        assert!(
            matches!(n, Some(Value::Int64(2)) | Some(Value::Int32(2))),
            "rows must survive reopen, got {n:?}"
        );
        db2.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ram_engines_still_open_for_deliberate_comparison() {
        for kind in [EngineKind::Mvcc, EngineKind::Memory] {
            let dir = std::env::temp_dir().join(format!(
                "nucleus_harness_{}_{}",
                kind.name(),
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let db = HarnessDb::open(kind, &dir, EngineConfig::default())
                .await
                .unwrap();
            db.execute("CREATE TABLE t (id BIGINT, v INT)")
                .await
                .unwrap();
            db.execute("INSERT INTO t (id, v) VALUES (1, 10)")
                .await
                .unwrap();
            assert!(db.checkpoint().is_none(), "{kind} has no checkpoint");
            assert!(
                db.snapshot().cache_hit_rate().is_none(),
                "{kind} has no buffer pool"
            );
            db.close();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
