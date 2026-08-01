//! Disk watermarks and safe degradation to read-only.
//!
//! Running the data directory's filesystem to zero free bytes is one of the
//! few faults a database cannot recover from cleanly: WAL appends, page
//! writes, and checkpoint renames all fail at arbitrary points. The guard
//! here samples free space on an interval and crosses two watermarks:
//!
//! * **warning** — log an operator alert, keep serving writes;
//! * **critical** — put [`ServiceState`] into read-only so every write is
//!   refused with SQLSTATE `53100` and an actionable message, *before* the
//!   filesystem actually fills.
//!
//! Recovery uses hysteresis: free space must climb back above a resume
//! watermark strictly higher than the critical one, so a database hovering
//! at the boundary does not flap between read-only and read-write.
//!
//! The filesystem probe is injectable ([`SpaceProbe`]), so the disk-full
//! path is tested deterministically without needing a real full disk.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

use parking_lot::RwLock;

use super::mode::{DegradeReason, ServiceState};

/// Free/total space for one filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpaceInfo {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

impl SpaceInfo {
    /// Percentage of the filesystem that is still available (0.0–100.0).
    /// A zero-sized filesystem reports 0% free (treated as critical).
    pub fn free_pct(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.available_bytes as f64 / self.total_bytes as f64) * 100.0
    }
}

/// Source of filesystem free-space readings.
///
/// Injectable so tests can drive the watermark state machine deterministically
/// instead of trying to actually fill a disk.
pub trait SpaceProbe: Send + Sync + std::fmt::Debug {
    fn probe(&self, path: &Path) -> std::io::Result<SpaceInfo>;
}

/// Real filesystem probe (`statvfs` on Unix).
#[derive(Debug, Clone, Copy, Default)]
pub struct FsSpaceProbe;

impl SpaceProbe for FsSpaceProbe {
    #[cfg(unix)]
    fn probe(&self, path: &Path) -> std::io::Result<SpaceInfo> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        // SAFETY: `stat` is a valid, fully-initialised-by-the-callee output
        // buffer and `c_path` is a NUL-terminated path that outlives the call.
        let stat = unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            stat
        };
        // `f_frsize` is the fragment size that block counts are expressed in;
        // fall back to `f_bsize` when a platform reports 0.
        let unit = if stat.f_frsize > 0 {
            stat.f_frsize as u64
        } else {
            stat.f_bsize as u64
        };
        Ok(SpaceInfo {
            total_bytes: (stat.f_blocks as u64).saturating_mul(unit),
            // `f_bavail` is space available to unprivileged processes, which
            // is what actually bounds us — not `f_bfree`, which includes the
            // root-reserved pool.
            available_bytes: (stat.f_bavail as u64).saturating_mul(unit),
        })
    }

    #[cfg(not(unix))]
    fn probe(&self, _path: &Path) -> std::io::Result<SpaceInfo> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "filesystem free-space probing is only implemented for Unix targets",
        ))
    }
}

/// Watermark configuration. All thresholds are "free space remaining".
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskWatermarks {
    /// Below this percentage free, log an operator alert.
    pub warn_free_pct: f64,
    /// Below this percentage free, refuse writes.
    pub readonly_free_pct: f64,
    /// Absolute floor: below this many free bytes, refuse writes regardless
    /// of percentage (a 1% margin on a 10 TB volume is 100 GB; a 1% margin on
    /// a 2 GB volume is 20 MB, which is not a usable margin).
    pub min_free_bytes: u64,
    /// Free space must climb back above this percentage before writes resume
    /// (hysteresis). Must be >= `readonly_free_pct`.
    pub resume_free_pct: f64,
}

impl Default for DiskWatermarks {
    fn default() -> Self {
        Self {
            warn_free_pct: 10.0,
            readonly_free_pct: 3.0,
            min_free_bytes: 256 * 1024 * 1024,
            resume_free_pct: 6.0,
        }
    }
}

impl DiskWatermarks {
    /// Reject nonsensical watermark configurations eagerly, at startup,
    /// rather than discovering them when the disk is already nearly full.
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=100.0).contains(&self.warn_free_pct) {
            return Err(format!(
                "storage.disk_warn_free_pct must be between 0 and 100 (got {})",
                self.warn_free_pct
            ));
        }
        if !(0.0..=100.0).contains(&self.readonly_free_pct) {
            return Err(format!(
                "storage.disk_readonly_free_pct must be between 0 and 100 (got {})",
                self.readonly_free_pct
            ));
        }
        if !(0.0..=100.0).contains(&self.resume_free_pct) {
            return Err(format!(
                "storage.disk_resume_free_pct must be between 0 and 100 (got {})",
                self.resume_free_pct
            ));
        }
        if self.readonly_free_pct > self.warn_free_pct {
            return Err(format!(
                "storage.disk_readonly_free_pct ({}) must not exceed storage.disk_warn_free_pct ({}): the server would enter read-only without ever warning",
                self.readonly_free_pct, self.warn_free_pct
            ));
        }
        if self.resume_free_pct < self.readonly_free_pct {
            return Err(format!(
                "storage.disk_resume_free_pct ({}) must be at least storage.disk_readonly_free_pct ({}): a lower resume watermark makes the server flap between read-only and read-write",
                self.resume_free_pct, self.readonly_free_pct
            ));
        }
        Ok(())
    }
}

/// Severity of the most recent free-space reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskLevel {
    /// Free space is above every watermark.
    Normal,
    /// Below the warning watermark; writes still admitted.
    Warning,
    /// Below the critical watermark; writes refused.
    Critical,
    /// The probe itself failed; the previous admission state is retained.
    Unknown,
}

impl DiskLevel {
    fn as_u8(self) -> u8 {
        match self {
            DiskLevel::Normal => 0,
            DiskLevel::Warning => 1,
            DiskLevel::Critical => 2,
            DiskLevel::Unknown => 3,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => DiskLevel::Warning,
            2 => DiskLevel::Critical,
            3 => DiskLevel::Unknown,
            _ => DiskLevel::Normal,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            DiskLevel::Normal => "normal",
            DiskLevel::Warning => "warning",
            DiskLevel::Critical => "critical",
            DiskLevel::Unknown => "unknown",
        }
    }
}

/// One evaluation result.
#[derive(Debug, Clone, PartialEq)]
pub struct DiskObservation {
    pub level: DiskLevel,
    pub space: Option<SpaceInfo>,
    /// Human-readable explanation, suitable for logs and status output.
    pub detail: String,
    /// Whether this evaluation changed the level (edge trigger for alerts).
    pub changed: bool,
}

/// Samples free space and drives [`ServiceState`] across the watermarks.
#[derive(Debug)]
pub struct DiskGuard {
    data_dir: PathBuf,
    probe: Arc<dyn SpaceProbe>,
    marks: DiskWatermarks,
    service: Arc<ServiceState>,
    level: AtomicU8,
    last: RwLock<Option<DiskObservation>>,
    probe_failures: AtomicU64,
    checks: AtomicU64,
    monitor_panics: AtomicU64,
}

impl DiskGuard {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        probe: Arc<dyn SpaceProbe>,
        marks: DiskWatermarks,
        service: Arc<ServiceState>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            probe,
            marks,
            service,
            level: AtomicU8::new(DiskLevel::Normal.as_u8()),
            last: RwLock::new(None),
            probe_failures: AtomicU64::new(0),
            checks: AtomicU64::new(0),
            monitor_panics: AtomicU64::new(0),
        }
    }

    /// Convenience constructor using the real filesystem probe.
    pub fn with_fs_probe(
        data_dir: impl Into<PathBuf>,
        marks: DiskWatermarks,
        service: Arc<ServiceState>,
    ) -> Self {
        Self::new(data_dir, Arc::new(FsSpaceProbe), marks, service)
    }

    pub fn level(&self) -> DiskLevel {
        DiskLevel::from_u8(self.level.load(Ordering::Relaxed))
    }

    pub fn last_observation(&self) -> Option<DiskObservation> {
        self.last.read().clone()
    }

    pub fn probe_failures(&self) -> u64 {
        self.probe_failures.load(Ordering::Relaxed)
    }

    /// Readings taken since start. The monitor's liveness signal: read-only is
    /// only ever cleared by a later reading, so a counter that stops advancing
    /// means writes can never resume without a restart — which is precisely
    /// what "writes resume automatically" promises will not happen.
    pub fn checks_completed(&self) -> u64 {
        self.checks.load(Ordering::Relaxed)
    }

    /// Times a reading panicked and the monitor loop caught it.
    pub fn monitor_panics(&self) -> u64 {
        self.monitor_panics.load(Ordering::Relaxed)
    }

    /// Run the watermark monitor until the process exits.
    ///
    /// This lives here rather than as a bare `tokio::spawn` in `main.rs` for
    /// two reasons. It was untestable there, and the promise the operator sees
    /// — "writes resume automatically" — is only true while this loop keeps
    /// running: read-only is latched and nothing else clears it. A panicking
    /// reading used to kill the task silently, leaving a server that refused
    /// writes forever with no indication why and no fix but a restart. Now a
    /// panic is caught, counted and logged, and the next tick still happens.
    pub fn spawn_monitor(
        self: Arc<Self>,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // the first tick completes immediately
            loop {
                ticker.tick().await;
                let guard = Arc::clone(&self);
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || guard.evaluate()));
                if result.is_err() {
                    let n = self.monitor_panics.fetch_add(1, Ordering::Relaxed) + 1;
                    tracing::error!(
                        "disk watermark check panicked ({n} so far); the monitor survived and \
                         will take the next reading. Investigate anyway: read-only mode is \
                         cleared only by a reading, so a check that never succeeds leaves the \
                         server refusing writes until it is restarted"
                    );
                }
            }
        })
    }

    pub fn watermarks(&self) -> DiskWatermarks {
        self.marks
    }

    /// Take a reading and apply it to the admission state.
    ///
    /// Probe failure is deliberately *not* treated as a critical reading: a
    /// transient `stat` error must not take a healthy database offline. It is
    /// counted and logged at error level, and the previous admission state is
    /// left untouched.
    pub fn evaluate(&self) -> DiskObservation {
        let space = match self.probe.probe(&self.data_dir) {
            Ok(space) => space,
            Err(e) => {
                self.probe_failures.fetch_add(1, Ordering::Relaxed);
                let previous = self.level();
                let changed = previous != DiskLevel::Unknown;
                let detail = format!(
                    "free-space probe of {} failed: {e}; disk watermarks are not being enforced",
                    self.data_dir.display()
                );
                if changed {
                    tracing::error!("{detail}");
                }
                self.level.store(DiskLevel::Unknown.as_u8(), Ordering::SeqCst);
                let obs = DiskObservation {
                    level: DiskLevel::Unknown,
                    space: None,
                    detail,
                    changed,
                };
                *self.last.write() = Some(obs.clone());
                self.checks.fetch_add(1, Ordering::Relaxed);
                return obs;
            }
        };

        let free_pct = space.free_pct();
        let critical =
            free_pct < self.marks.readonly_free_pct || space.available_bytes < self.marks.min_free_bytes;
        let level = if critical {
            DiskLevel::Critical
        } else if free_pct < self.marks.warn_free_pct {
            DiskLevel::Warning
        } else {
            DiskLevel::Normal
        };

        let detail = format!(
            "{} has {:.2}% free ({} of {}); watermarks warn<{:.2}% readonly<{:.2}% min-free={}",
            self.data_dir.display(),
            free_pct,
            human_bytes(space.available_bytes),
            human_bytes(space.total_bytes),
            self.marks.warn_free_pct,
            self.marks.readonly_free_pct,
            human_bytes(self.marks.min_free_bytes),
        );

        let previous = self.level();
        let changed = previous != level;
        self.level.store(level.as_u8(), Ordering::SeqCst);

        match level {
            DiskLevel::Critical => {
                if self
                    .service
                    .enter_read_only(DegradeReason::DiskWatermark, detail.clone())
                {
                    tracing::error!("ALERT disk critical: entering read-only mode — {detail}");
                }
            }
            DiskLevel::Warning => {
                if changed {
                    tracing::warn!("ALERT disk low: {detail}");
                }
                // A warning-level reading is above the critical watermark but
                // may still be below the resume watermark, so only resume when
                // hysteresis is actually satisfied.
                self.maybe_resume(free_pct, &space, &detail);
            }
            DiskLevel::Normal => {
                if changed && previous != DiskLevel::Normal {
                    tracing::info!("disk recovered: {detail}");
                }
                self.maybe_resume(free_pct, &space, &detail);
            }
            DiskLevel::Unknown => {}
        }

        let obs = DiskObservation {
            level,
            space: Some(space),
            detail,
            changed,
        };
        *self.last.write() = Some(obs.clone());
        self.checks.fetch_add(1, Ordering::Relaxed);
        obs
    }

    /// Resume writes only when free space has climbed back above the resume
    /// watermark *and* the absolute floor, and only if the disk monitor is
    /// what caused the degradation in the first place.
    fn maybe_resume(&self, free_pct: f64, space: &SpaceInfo, detail: &str) {
        if free_pct < self.marks.resume_free_pct || space.available_bytes < self.marks.min_free_bytes
        {
            return;
        }
        if self.service.resume_if(DegradeReason::DiskWatermark) {
            tracing::warn!("disk pressure cleared: resuming writes — {detail}");
        }
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecError;
    use crate::ops::mode::ServiceMode;

    /// Probe whose readings the test drives directly.
    #[derive(Debug)]
    struct FakeProbe {
        total: u64,
        available: parking_lot::Mutex<u64>,
        fail: parking_lot::Mutex<bool>,
        panic_on_probe: parking_lot::Mutex<bool>,
    }

    impl FakeProbe {
        fn new(total: u64, available: u64) -> Arc<Self> {
            Arc::new(Self {
                total,
                available: parking_lot::Mutex::new(available),
                fail: parking_lot::Mutex::new(false),
                panic_on_probe: parking_lot::Mutex::new(false),
            })
        }
        fn set_available(&self, v: u64) {
            *self.available.lock() = v;
        }
        fn set_fail(&self, v: bool) {
            *self.fail.lock() = v;
        }
        fn set_panic(&self, v: bool) {
            *self.panic_on_probe.lock() = v;
        }
    }

    impl SpaceProbe for FakeProbe {
        fn probe(&self, _path: &Path) -> std::io::Result<SpaceInfo> {
            if *self.panic_on_probe.lock() {
                panic!("injected probe panic");
            }
            if *self.fail.lock() {
                return Err(std::io::Error::other("injected probe failure"));
            }
            Ok(SpaceInfo {
                total_bytes: self.total,
                available_bytes: *self.available.lock(),
            })
        }
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    fn marks() -> DiskWatermarks {
        DiskWatermarks {
            warn_free_pct: 10.0,
            readonly_free_pct: 3.0,
            min_free_bytes: 0, // percentage-only for these cases
            resume_free_pct: 6.0,
        }
    }

    fn guard(probe: Arc<FakeProbe>, marks: DiskWatermarks) -> (DiskGuard, Arc<ServiceState>) {
        let service = Arc::new(ServiceState::new());
        (
            DiskGuard::new("/tmp/nucleus-test", probe, marks, service.clone()),
            service,
        )
    }

    #[test]
    fn plenty_of_space_admits_writes() {
        let probe = FakeProbe::new(100 * GIB, 50 * GIB);
        let (g, service) = guard(probe, marks());
        let obs = g.evaluate();
        assert_eq!(obs.level, DiskLevel::Normal);
        assert!(!service.is_read_only());
        assert!(service.admit_write("INSERT").is_ok());
    }

    /// Just *above* the critical watermark: writes must still succeed.
    #[test]
    fn just_above_critical_watermark_still_admits_writes() {
        // 3.5% free, critical is <3%.
        let probe = FakeProbe::new(100 * GIB, 3 * GIB + GIB / 2);
        let (g, service) = guard(probe, marks());
        let obs = g.evaluate();
        assert_eq!(obs.level, DiskLevel::Warning, "{}", obs.detail);
        assert!(!service.is_read_only());
        assert!(service.admit_write("INSERT").is_ok());
    }

    /// Just *below* the critical watermark: writes must be refused with the
    /// actionable disk-full error, while reads stay available.
    #[test]
    fn just_below_critical_watermark_refuses_writes() {
        let probe = FakeProbe::new(100 * GIB, 2 * GIB); // 2% free
        let (g, service) = guard(probe, marks());
        let obs = g.evaluate();
        assert_eq!(obs.level, DiskLevel::Critical, "{}", obs.detail);
        assert!(service.is_read_only());
        let err = service.admit_write("INSERT").unwrap_err();
        assert!(matches!(err, ExecError::DiskFull(_)), "got {err:?}");
        assert!(err.to_string().contains("read-only"), "{err}");
        assert!(
            matches!(service.mode(), ServiceMode::ReadOnly { reason, .. } if reason == DegradeReason::DiskWatermark)
        );
    }

    #[test]
    fn absolute_min_free_bytes_floor_triggers_independently_of_percentage() {
        // 20% free by percentage — well above every percentage watermark —
        // but only 200 MiB in absolute terms.
        let marks = DiskWatermarks {
            warn_free_pct: 10.0,
            readonly_free_pct: 3.0,
            min_free_bytes: 256 * 1024 * 1024,
            resume_free_pct: 6.0,
        };
        let probe = FakeProbe::new(1024 * 1024 * 1024, 200 * 1024 * 1024);
        let (g, service) = guard(probe, marks);
        assert_eq!(g.evaluate().level, DiskLevel::Critical);
        assert!(service.is_read_only());
    }

    #[test]
    fn recovery_requires_hysteresis_above_the_resume_watermark() {
        let probe = FakeProbe::new(100 * GIB, 2 * GIB);
        let (g, service) = guard(probe.clone(), marks());
        assert_eq!(g.evaluate().level, DiskLevel::Critical);
        assert!(service.is_read_only());

        // Back above the critical watermark (4%) but below resume (6%):
        // still read-only, so the server cannot flap at the boundary.
        probe.set_available(4 * GIB);
        assert_eq!(g.evaluate().level, DiskLevel::Warning);
        assert!(
            service.is_read_only(),
            "resumed below the hysteresis watermark"
        );

        // Above the resume watermark: writes come back automatically.
        probe.set_available(7 * GIB);
        assert_eq!(g.evaluate().level, DiskLevel::Warning);
        assert!(!service.is_read_only());
        assert!(service.admit_write("INSERT").is_ok());
    }

    #[test]
    fn min_free_bytes_floor_also_gates_recovery() {
        let marks = DiskWatermarks {
            warn_free_pct: 10.0,
            readonly_free_pct: 3.0,
            min_free_bytes: 256 * 1024 * 1024,
            resume_free_pct: 6.0,
        };
        let probe = FakeProbe::new(1024 * 1024 * 1024, 10 * 1024 * 1024);
        let (g, service) = guard(probe.clone(), marks);
        assert!(g.evaluate().level == DiskLevel::Critical);
        // 25% free by percentage, but still under the absolute floor.
        probe.set_available(255 * 1024 * 1024);
        g.evaluate();
        assert!(service.is_read_only(), "resumed below the absolute floor");
        probe.set_available(300 * 1024 * 1024);
        g.evaluate();
        assert!(!service.is_read_only());
    }

    #[test]
    fn probe_failure_does_not_take_a_healthy_database_offline() {
        let probe = FakeProbe::new(100 * GIB, 50 * GIB);
        let (g, service) = guard(probe.clone(), marks());
        g.evaluate();
        probe.set_fail(true);
        let obs = g.evaluate();
        assert_eq!(obs.level, DiskLevel::Unknown);
        assert_eq!(g.probe_failures(), 1);
        assert!(!service.is_read_only());
        assert!(service.admit_write("INSERT").is_ok());
    }

    #[test]
    fn probe_failure_does_not_clear_an_existing_degraded_state() {
        let probe = FakeProbe::new(100 * GIB, GIB);
        let (g, service) = guard(probe.clone(), marks());
        assert!(g.evaluate().level == DiskLevel::Critical);
        probe.set_fail(true);
        g.evaluate();
        assert!(
            service.is_read_only(),
            "an unreadable filesystem must not silently re-admit writes"
        );
    }

    #[test]
    fn zero_sized_filesystem_reads_as_critical_not_nan() {
        let probe = FakeProbe::new(0, 0);
        let (g, service) = guard(probe, marks());
        assert_eq!(g.evaluate().level, DiskLevel::Critical);
        assert!(service.is_read_only());
    }

    #[test]
    fn watermark_validation_rejects_inverted_thresholds() {
        let bad = DiskWatermarks {
            warn_free_pct: 2.0,
            readonly_free_pct: 5.0,
            min_free_bytes: 0,
            resume_free_pct: 10.0,
        };
        let err = bad.validate().unwrap_err();
        assert!(err.contains("without ever warning"), "{err}");

        let bad = DiskWatermarks {
            warn_free_pct: 10.0,
            readonly_free_pct: 5.0,
            min_free_bytes: 0,
            resume_free_pct: 4.0,
        };
        let err = bad.validate().unwrap_err();
        assert!(err.contains("flap"), "{err}");

        let bad = DiskWatermarks {
            warn_free_pct: 120.0,
            ..DiskWatermarks::default()
        };
        assert!(bad.validate().is_err());

        assert!(DiskWatermarks::default().validate().is_ok());
    }

    #[test]
    fn real_fs_probe_reports_a_plausible_reading_for_the_temp_dir() {
        let probe = FsSpaceProbe;
        let info = probe
            .probe(&std::env::temp_dir())
            .expect("statvfs on the temp dir should succeed on a supported target");
        assert!(info.total_bytes > 0, "total bytes must be positive");
        assert!(info.available_bytes <= info.total_bytes);
        assert!((0.0..=100.0).contains(&info.free_pct()));
    }

    #[test]
    fn human_bytes_scales() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GiB");
    }

    /// Let the paused clock run until `cond` holds, bounded so a broken monitor
    /// fails the test instead of hanging it.
    async fn wait_for(cond: impl Fn() -> bool) {
        for _ in 0..30 {
            if cond() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }

    /// The promise in the operator-facing error is "writes resume
    /// automatically". That is only true while the monitor keeps taking
    /// readings: read-only is latched and nothing else clears it. A panicking
    /// reading used to kill the spawned task silently, and the only recovery
    /// was a restart — the reported symptom this guards against.
    #[tokio::test(start_paused = true)]
    async fn a_panicking_reading_does_not_stop_the_monitor() {
        let probe = FakeProbe::new(100 * GIB, 2 * GIB);
        let (g, service) = guard(probe.clone(), marks());
        let g = Arc::new(g);
        assert_eq!(g.evaluate().level, DiskLevel::Critical);
        assert!(service.is_read_only());

        // A reading panics. Before supervision this killed the task outright.
        probe.set_panic(true);
        let handle = Arc::clone(&g).spawn_monitor(std::time::Duration::from_secs(30));

        // Paused time auto-advances while every task is idle, so sleeping here
        // lets the monitor's ticker fire without any real delay.
        wait_for(|| g.monitor_panics() >= 1).await;
        assert!(g.monitor_panics() >= 1, "the panic was not caught");
        assert!(!handle.is_finished(), "the monitor task died on a panic");

        // Space is freed. The monitor must still be alive to notice.
        probe.set_panic(false);
        probe.set_available(7 * GIB);
        wait_for(|| !service.is_read_only()).await;
        assert!(
            !service.is_read_only(),
            "writes did not resume automatically — a restart would be the only fix"
        );
        handle.abort();
    }

    /// Liveness is observable at all. Without a counter, "the monitor stopped"
    /// and "the disk is still full" look identical from outside.
    #[tokio::test(start_paused = true)]
    async fn the_monitor_reports_that_it_is_still_taking_readings() {
        let probe = FakeProbe::new(100 * GIB, 50 * GIB);
        let (g, _service) = guard(probe, marks());
        let g = Arc::new(g);
        let handle = Arc::clone(&g).spawn_monitor(std::time::Duration::from_secs(30));

        wait_for(|| g.checks_completed() >= 3).await;
        assert!(
            g.checks_completed() >= 3,
            "the monitor is not taking readings: {} in 30 ticks",
            g.checks_completed()
        );
        handle.abort();
    }
}
