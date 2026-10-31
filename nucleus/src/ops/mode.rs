//! Process-wide write-admission mode.
//!
//! A database that is running out of a resource it cannot recover from
//! (primarily disk) must stop accepting writes *before* it corrupts or
//! crashes. `ServiceState` is the single gate that decides that: it is read
//! on every write statement, so the hot path is one relaxed atomic load, and
//! the degraded reason is recorded so the rejection message tells the
//! operator what to actually do.
//!
//! Degradation is *reason-scoped*: the disk monitor may only clear a
//! disk-triggered read-only state, and an operator-requested read-only is
//! never silently cleared by a background task.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::executor::ExecError;

/// Why the server is refusing writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradeReason {
    /// A disk watermark was crossed; writes are refused to protect the data
    /// directory from filling completely.
    DiskWatermark,
    /// An operator explicitly requested read-only mode.
    Operator,
}

impl DegradeReason {
    fn as_u8(self) -> u8 {
        match self {
            DegradeReason::DiskWatermark => 1,
            DegradeReason::Operator => 2,
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(DegradeReason::DiskWatermark),
            2 => Some(DegradeReason::Operator),
            _ => None,
        }
    }

    /// Short stable token for status output and metrics labels.
    pub fn as_str(self) -> &'static str {
        match self {
            DegradeReason::DiskWatermark => "disk_watermark",
            DegradeReason::Operator => "operator",
        }
    }
}

/// The current admission mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceMode {
    /// Reads and writes are both admitted.
    ReadWrite,
    /// Reads are admitted; writes are refused with an actionable error.
    ReadOnly {
        reason: DegradeReason,
        detail: String,
    },
}

impl ServiceMode {
    pub fn is_read_only(&self) -> bool {
        matches!(self, ServiceMode::ReadOnly { .. })
    }

    /// Stable token for status output: `read-write` or `read-only`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceMode::ReadWrite => "read-write",
            ServiceMode::ReadOnly { .. } => "read-only",
        }
    }
}

/// Shared, cheaply-readable write-admission state.
#[derive(Debug)]
pub struct ServiceState {
    /// 0 = read-write, otherwise [`DegradeReason::as_u8`].
    degraded: AtomicU8,
    /// Human-readable explanation of the current degraded state.
    detail: RwLock<String>,
    /// Count of write statements refused since process start.
    rejected_writes: AtomicU64,
    /// Count of transitions into read-only since process start.
    degrade_events: AtomicU64,
}

impl Default for ServiceState {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceState {
    pub fn new() -> Self {
        Self {
            degraded: AtomicU8::new(0),
            detail: RwLock::new(String::new()),
            rejected_writes: AtomicU64::new(0),
            degrade_events: AtomicU64::new(0),
        }
    }

    /// Hot-path check: `true` when writes are currently refused.
    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.degraded.load(Ordering::Relaxed) != 0
    }

    /// The reason writes are refused, if they are.
    pub fn reason(&self) -> Option<DegradeReason> {
        DegradeReason::from_u8(self.degraded.load(Ordering::Relaxed))
    }

    /// A full snapshot of the current mode.
    pub fn mode(&self) -> ServiceMode {
        match self.reason() {
            None => ServiceMode::ReadWrite,
            Some(reason) => ServiceMode::ReadOnly {
                reason,
                detail: self.detail.read().clone(),
            },
        }
    }

    /// Enter read-only mode. Returns `true` if this call changed the state
    /// (so callers can log an alert exactly once per transition).
    ///
    /// An operator request overrides a disk-triggered state; a disk trigger
    /// never overrides an operator request (the operator's intent outlives
    /// a transient free-space recovery).
    ///
    /// NE-27: the decision and the store are ONE compare-and-swap. The old
    /// load-decide-store sequence let a disk-monitor enter that had read the
    /// pre-operator state store its DiskWatermark OVER a just-installed
    /// operator hold — downgrading the reason so a later disk recovery
    /// cleared it.
    pub fn enter_read_only(&self, reason: DegradeReason, detail: impl Into<String>) -> bool {
        let detail = detail.into();
        let mut current = self.degraded.load(Ordering::Relaxed);
        loop {
            if current == DegradeReason::Operator.as_u8() && reason != DegradeReason::Operator {
                return false;
            }
            match self.degraded.compare_exchange(
                current,
                reason.as_u8(),
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        *self.detail.write() = detail;
        let changed = current != reason.as_u8();
        if changed {
            self.degrade_events.fetch_add(1, Ordering::Relaxed);
        }
        changed
    }

    /// Unconditionally return to read-write. Returns `true` if the state
    /// changed. Use [`Self::resume_if`] from background monitors so they
    /// cannot clear an unrelated degraded state.
    pub fn resume(&self) -> bool {
        let previous = self.degraded.swap(0, Ordering::SeqCst);
        if previous != 0 {
            self.detail.write().clear();
            true
        } else {
            false
        }
    }

    /// Return to read-write only if the current degraded state was caused by
    /// `reason`. Returns `true` if the state changed.
    ///
    /// NE-27: the check and the clear are ONE compare-and-swap. The old
    /// load-check-resume sequence let a disk monitor that had read
    /// DiskWatermark clear a just-installed operator hold in the gap between
    /// its read and its unconditional `resume`.
    pub fn resume_if(&self, reason: DegradeReason) -> bool {
        let mut current = self.degraded.load(Ordering::Relaxed);
        loop {
            if current != reason.as_u8() {
                return false;
            }
            match self
                .degraded
                .compare_exchange(current, 0, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.detail.write().clear();
                    return true;
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Admission check for a write statement. Increments the rejection
    /// counter and returns the actionable error when degraded.
    pub fn admit_write(&self, what: &str) -> Result<(), ExecError> {
        if !self.is_read_only() {
            return Ok(());
        }
        self.rejected_writes.fetch_add(1, Ordering::Relaxed);
        let detail = self.detail.read().clone();
        match self.reason() {
            Some(DegradeReason::DiskWatermark) => Err(ExecError::DiskFull(format!(
                "cannot execute {what}: the server is in read-only mode because {detail}. Free space in the data directory (or raise storage.disk_readonly_free_pct / storage.disk_min_free_mb), then writes resume automatically"
            ))),
            _ => Err(ExecError::ReadOnly(format!(
                "cannot execute {what}: the server is in read-only mode ({detail})"
            ))),
        }
    }

    /// Number of write statements refused since process start.
    pub fn rejected_writes(&self) -> u64 {
        self.rejected_writes.load(Ordering::Relaxed)
    }

    /// Number of transitions into read-only since process start.
    pub fn degrade_events(&self) -> u64 {
        self.degrade_events.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_admits_writes() {
        let s = ServiceState::new();
        assert!(!s.is_read_only());
        assert_eq!(s.mode(), ServiceMode::ReadWrite);
        assert!(s.admit_write("INSERT").is_ok());
        assert_eq!(s.rejected_writes(), 0);
    }

    #[test]
    fn disk_degrade_rejects_with_disk_full_error() {
        let s = ServiceState::new();
        assert!(s.enter_read_only(DegradeReason::DiskWatermark, "only 1% free on /data"));
        let err = s.admit_write("INSERT").unwrap_err();
        match err {
            ExecError::DiskFull(msg) => {
                assert!(msg.contains("read-only"), "{msg}");
                assert!(msg.contains("1% free on /data"), "{msg}");
                assert!(msg.contains("Free space"), "{msg}");
            }
            other => panic!("expected DiskFull, got {other:?}"),
        }
        assert_eq!(s.rejected_writes(), 1);
    }

    #[test]
    fn operator_degrade_rejects_with_read_only_error() {
        let s = ServiceState::new();
        s.enter_read_only(DegradeReason::Operator, "maintenance window");
        assert!(matches!(
            s.admit_write("UPDATE"),
            Err(ExecError::ReadOnly(_))
        ));
    }

    #[test]
    fn transition_is_edge_triggered() {
        let s = ServiceState::new();
        assert!(s.enter_read_only(DegradeReason::DiskWatermark, "a"));
        // Same reason again is not a new transition (no duplicate alerts).
        assert!(!s.enter_read_only(DegradeReason::DiskWatermark, "b"));
        assert_eq!(s.degrade_events(), 1);
        // ... but the detail is refreshed.
        assert!(matches!(s.mode(), ServiceMode::ReadOnly { detail, .. } if detail == "b"));
        assert!(s.resume());
        assert!(!s.resume());
    }

    #[test]
    fn disk_monitor_cannot_clear_operator_read_only() {
        let s = ServiceState::new();
        s.enter_read_only(DegradeReason::Operator, "maintenance window");
        // A disk monitor observing plenty of free space must not undo the
        // operator's explicit request.
        assert!(!s.resume_if(DegradeReason::DiskWatermark));
        assert!(s.is_read_only());
        assert_eq!(s.reason(), Some(DegradeReason::Operator));
        // ... nor may it downgrade the reason.
        assert!(!s.enter_read_only(DegradeReason::DiskWatermark, "low disk"));
        assert_eq!(s.reason(), Some(DegradeReason::Operator));
        // The operator can always clear their own state.
        assert!(s.resume_if(DegradeReason::Operator));
        assert!(!s.is_read_only());
    }

    #[test]
    fn operator_overrides_disk_degrade() {
        let s = ServiceState::new();
        s.enter_read_only(DegradeReason::DiskWatermark, "low disk");
        assert!(s.enter_read_only(DegradeReason::Operator, "manual"));
        assert_eq!(s.reason(), Some(DegradeReason::Operator));
        // Now the disk monitor recovering must not resume.
        assert!(!s.resume_if(DegradeReason::DiskWatermark));
        assert!(s.is_read_only());
    }

    /// NE-27: a concurrent disk-monitor enter/resume must never clear or
    /// downgrade an operator hold. The old load-decide-store transitions
    /// could: a monitor that read the pre-operator state stored
    /// DiskWatermark over the hold (downgrading it for a later clear), and a
    /// resume_if that read DiskWatermark cleared the hold unconditionally in
    /// its check→resume gap. CAS transitions close both windows; this stress
    /// runs them continuously against an installed hold.
    #[test]
    fn concurrent_disk_monitor_transitions_never_clear_an_operator_hold() {
        let s = std::sync::Arc::new(ServiceState::new());
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // A churning "disk monitor": enter and exit disk degradation forever.
        let monitor = {
            let s = s.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    s.enter_read_only(DegradeReason::DiskWatermark, "churn");
                    s.resume_if(DegradeReason::DiskWatermark);
                }
            })
        };

        // Install and verify operator holds repeatedly: while held, the mode
        // must be Operator and writes refused — never cleared and never
        // downgraded to DiskWatermark (which a monitor could then clear).
        for _ in 0..2_000 {
            s.enter_read_only(DegradeReason::Operator, "hold");
            for _ in 0..50 {
                assert!(
                    s.is_read_only(),
                    "the operator hold was cleared by a concurrent monitor (NE-27)"
                );
                assert_eq!(
                    s.reason(),
                    Some(DegradeReason::Operator),
                    "the operator hold was downgraded by a concurrent monitor (NE-27)"
                );
            }
            assert!(s.resume_if(DegradeReason::Operator));
        }
        stop.store(true, Ordering::Relaxed);
        monitor.join().unwrap();
    }
}
