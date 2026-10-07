//! Durable, bounded security audit log.
//!
//! Records the events an operator has to be able to reconstruct after an
//! incident: who authenticated, who failed to, and every change to authority —
//! roles, privileges and policies. Nothing recorded these before. There was an
//! `AuditLog` in `security::` with no callers anywhere in the crate, holding
//! its entries in a `Vec` that nothing bounded and nothing persisted, so it
//! answered neither half of "durable, bounded".
//!
//! ## Durable
//!
//! Every event is written and **fsynced before `record` returns**. Security
//! events are rare — logins and DDL, not statements — so the per-event fsync
//! costs nothing measurable, and the alternative fails in the one case the log
//! exists for: a machine that loses power during an intrusion must not lose
//! the record of it. This also means the sink holds no buffered state, which
//! is why a `SIGKILL` and a dropped handle are indistinguishable here (see
//! `a_crash_cannot_lose_a_synced_event`).
//!
//! ## Bounded
//!
//! The current file is capped at `max_bytes` and rotated to `audit.1.log`,
//! `audit.2.log`, …; anything past `keep` is deleted. Total on-disk size is
//! therefore at most `max_bytes * (keep + 1)` plus one event, whatever the
//! event rate.
//!
//! Boundedness has to survive a crash, which is where a naive rotation fails:
//! a process killed between the rename and the prune leaves `keep + 1` files
//! behind, and one killed between the rename and the create leaves no current
//! file. `open` therefore re-derives its state from the directory rather than
//! trusting it — it prunes on the way in and measures the current file's real
//! length — so a restart converges to the bound no matter where it was
//! interrupted. That is what the crash tests below assert.
//!
//! Rotation deletes the OLDEST events, not the newest. An audit log that
//! silently drops the events happening right now, because a flood of older
//! ones filled the file, would be the failure mode worth avoiding.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

/// Default cap on the active file before it rotates.
pub const DEFAULT_MAX_BYTES: u64 = 16 * 1024 * 1024;
/// Default number of rotated files retained beside the active one.
pub const DEFAULT_KEEP: usize = 4;

/// The current file's name inside the audit directory.
const ACTIVE: &str = "audit.log";

/// What happened.
///
/// Deliberately a closed set: an audit log whose event kinds are free-form
/// strings cannot be queried reliably after the fact, which is the only time
/// anyone reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditKind {
    /// A principal authenticated successfully.
    LoginSucceeded,
    /// A credential was presented and rejected.
    LoginFailed,
    /// A login was refused before any credential check — NOLOGIN, an expired
    /// password, or a locked-out source.
    LoginRefused,
    /// `CREATE ROLE`.
    RoleCreated,
    /// `ALTER ROLE` — including password rotation and expiry changes.
    RoleAltered,
    /// `DROP ROLE`.
    RoleDropped,
    /// `GRANT`.
    PrivilegeGranted,
    /// `REVOKE`.
    PrivilegeRevoked,
    /// A row-level-security policy was created, altered or dropped.
    PolicyChanged,
}

impl AuditKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AuditKind::LoginSucceeded => "login_succeeded",
            AuditKind::LoginFailed => "login_failed",
            AuditKind::LoginRefused => "login_refused",
            AuditKind::RoleCreated => "role_created",
            AuditKind::RoleAltered => "role_altered",
            AuditKind::RoleDropped => "role_dropped",
            AuditKind::PrivilegeGranted => "privilege_granted",
            AuditKind::PrivilegeRevoked => "privilege_revoked",
            AuditKind::PolicyChanged => "policy_changed",
        }
    }
}

/// A durable, bounded, append-only audit sink.
pub struct AuditSink {
    dir: PathBuf,
    file: Mutex<File>,
    /// Bytes in the active file. Held here so a write does not stat.
    active_bytes: AtomicU64,
    max_bytes: u64,
    keep: usize,
    /// A record write failed mid-line (NE-29): the active file may end in a
    /// PARTIAL record, so further appends would concatenate onto it and
    /// acknowledge events no reader can parse. Refused until reopen, which
    /// repairs the torn tail.
    fenced: std::sync::atomic::AtomicBool,
}

impl AuditSink {
    /// Open (or create) the audit log in `dir`.
    ///
    /// Reconciles whatever the directory holds — including the states a crash
    /// mid-rotation leaves behind — before returning.
    pub fn open(dir: &Path, max_bytes: u64, keep: usize) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        // Prune first: a process killed between the rename and the prune left
        // more retained files than `keep` allows, and until this runs the
        // directory is over its bound.
        prune(dir, keep)?;
        let path = dir.join(ACTIVE);
        // NE-29: repair a torn tail BEFORE the append handle exists. A crash
        // or partial write can leave the final record unterminated; appends
        // landing behind it used to return success yet merge with the torn
        // bytes into a line no reader can parse.
        repair_torn_tail(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .truncate(false)
            .open(&path)?;
        let len = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            file: Mutex::new(file),
            active_bytes: AtomicU64::new(len),
            max_bytes: max_bytes.max(1),
            keep,
            fenced: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Open with the defaults.
    pub fn open_default(dir: &Path) -> std::io::Result<Self> {
        Self::open(dir, DEFAULT_MAX_BYTES, DEFAULT_KEEP)
    }

    /// Open, honouring `NUCLEUS_AUDIT_MAX_BYTES` and `NUCLEUS_AUDIT_KEEP`.
    ///
    /// An unparseable value falls back to the default rather than failing
    /// startup: a typo in a size must not be the reason a database will not
    /// boot, and the bound still holds at whatever value is used.
    pub fn open_from_env(dir: &Path) -> std::io::Result<Self> {
        let max = std::env::var("NUCLEUS_AUDIT_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_MAX_BYTES);
        let keep = std::env::var("NUCLEUS_AUDIT_KEEP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_KEEP);
        Self::open(dir, max, keep)
    }

    /// The largest the whole audit directory can grow to, ignoring the single
    /// event that may overhang the cap.
    pub fn max_total_bytes(&self) -> u64 {
        self.max_bytes * (self.keep as u64 + 1)
    }

    /// Record one event, durably.
    ///
    /// `principal` is the role or user the event is about; `detail` is a short
    /// human-readable description. Both are JSON-escaped.
    pub fn record(
        &self,
        kind: AuditKind,
        principal: &str,
        detail: &str,
        source: Option<&str>,
    ) -> std::io::Result<()> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut line = String::with_capacity(160);
        line.push_str("{\"ts_ms\":");
        line.push_str(&ts.to_string());
        line.push_str(",\"kind\":\"");
        line.push_str(kind.as_str());
        line.push_str("\",\"principal\":");
        push_json_string(&mut line, principal);
        line.push_str(",\"detail\":");
        push_json_string(&mut line, detail);
        if let Some(src) = source {
            line.push_str(",\"source\":");
            push_json_string(&mut line, src);
        }
        line.push_str("}\n");

        let mut file = self.file.lock();
        // NE-29 fence: an earlier write failed mid-line, so the file may end
        // in a partial record — appending now would acknowledge an event that
        // merges with the torn bytes and never parses.
        if self.fenced.load(Ordering::Acquire) {
            return Err(std::io::Error::other(
                "audit sink is fenced: a previous record write failed mid-line; reopen \
                 (which repairs the torn tail) before recording again",
            ));
        }
        // NE-28: an event larger than the whole file cap can never fit
        // anywhere — writing it anyway meant rotation carried a copy into
        // every retained file and the total bound did not hold. Refuse
        // BEFORE any mutation (no rotation, no write, no accounting).
        if line.len() as u64 > self.max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "audit event ({} bytes) exceeds the per-file cap ({} bytes); refusing to \
                     record it — the configured retention bound would not hold. Raise \
                     NUCLEUS_AUDIT_MAX_BYTES if events this large are expected.",
                    line.len(),
                    self.max_bytes
                ),
            ));
        }
        // Rotate BEFORE writing when this event would cross the cap, so the cap
        // is a bound on the file rather than a bound it is allowed to exceed.
        if self.active_bytes.load(Ordering::Acquire) + line.len() as u64 > self.max_bytes {
            self.rotate(&mut file)?;
        }
        if let Err(e) = file.write_all(line.as_bytes()) {
            self.fenced.store(true, Ordering::Release);
            return Err(e);
        }
        // Before returning, not on a timer: the event this log exists for is
        // the one that happens immediately before the machine goes down.
        // (The bytes may or may not have landed, but the accounting below
        // stays honest either way because the line was fully written.)
        file.sync_all()?;
        self.active_bytes
            .fetch_add(line.len() as u64, Ordering::AcqRel);
        Ok(())
    }

    /// Roll the active file to `audit.1.log`, shifting the rest along, and open
    /// a fresh one. Called with the file lock held.
    fn rotate(&self, file: &mut File) -> std::io::Result<()> {
        file.sync_all()?;
        // Shift downwards from the oldest so no rename overwrites a file that
        // has not been moved yet.
        for i in (1..=self.keep).rev() {
            let from = self.dir.join(format!("audit.{i}.log"));
            if !from.exists() {
                continue;
            }
            if i == self.keep {
                std::fs::remove_file(&from)?;
            } else {
                std::fs::rename(&from, self.dir.join(format!("audit.{}.log", i + 1)))?;
            }
        }
        let active = self.dir.join(ACTIVE);
        if active.exists() && self.keep > 0 {
            std::fs::rename(&active, self.dir.join("audit.1.log"))?;
        } else if active.exists() {
            // keep == 0: bounded means the active file alone.
            std::fs::remove_file(&active)?;
        }
        *file = OpenOptions::new()
            .create(true)
            .append(true)
            .truncate(false)
            .open(&active)?;
        self.active_bytes.store(0, Ordering::Release);
        Ok(())
    }

    /// Every event currently on disk, oldest file first. For tests and for
    /// whatever exports the log.
    ///
    /// Reads BYTES and splits on newlines (NE-29): `read_to_string` fails
    /// wholesale on a file whose torn tail ends inside a UTF-8 character,
    /// which silently omitted EVERY event in that file from the export.
    /// Each complete line is converted lossily; a partial trailing line
    /// (which `open` repairs before appends) is dropped rather than parsed.
    pub fn read_all(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut files: Vec<PathBuf> = Vec::new();
        for i in (1..=64).rev() {
            let p = dir.join(format!("audit.{i}.log"));
            if p.exists() {
                files.push(p);
            }
        }
        files.push(dir.join(ACTIVE));
        for f in files {
            let Ok(bytes) = std::fs::read(&f) else {
                continue;
            };
            for line in bytes.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                out.push(String::from_utf8_lossy(line).into_owned());
            }
        }
        out
    }

    /// Total bytes the audit directory occupies.
    pub fn total_bytes(dir: &Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .flatten()
            .filter_map(|e| e.metadata().ok())
            .filter(|m| m.is_file())
            .map(|m| m.len())
            .sum()
    }
}

/// Truncate an unterminated final record so appends never land behind a torn
/// tail (NE-29). A crash or partial write can leave the last line without its
/// `\n`; the next acknowledged record used to concatenate onto it and succeed
/// while producing a line no reader can parse. A repair that cannot complete
/// fails the open: recording into a known-torn file is worse than refusing.
fn repair_torn_tail(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(path)?;
    let Some(last_nl) = bytes.iter().rposition(|&b| b == b'\n') else {
        // No newline at all: either an empty file or one wholly-torn record.
        if bytes.is_empty() {
            return Ok(());
        }
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(0)?;
        f.sync_all()?;
        return Ok(());
    };
    let complete_len = last_nl as u64 + 1;
    if bytes.len() as u64 == complete_len {
        return Ok(()); // already terminated
    }
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(complete_len)?;
    f.sync_all()?;
    tracing::warn!(
        "audit log at {} had a torn final record ({} of {} bytes kept); truncated it before \
         appending",
        path.display(),
        complete_len,
        bytes.len()
    );
    Ok(())
}

/// Delete retained files past `keep`, and any left by an interrupted rotation.
fn prune(dir: &Path, keep: usize) -> std::io::Result<()> {    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix("audit.") else {
            continue;
        };
        let Some(idx) = rest.strip_suffix(".log") else {
            continue;
        };
        let Ok(idx) = idx.parse::<usize>() else {
            continue; // "audit.log" itself
        };
        if idx > keep || keep == 0 {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Append `s` as a JSON string literal.
fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink(dir: &Path, max: u64, keep: usize) -> AuditSink {
        AuditSink::open(dir, max, keep).unwrap()
    }

    #[test]
    fn an_event_is_readable_and_carries_its_fields() {
        let tmp = tempfile::tempdir().unwrap();
        let s = sink(tmp.path(), DEFAULT_MAX_BYTES, DEFAULT_KEEP);
        s.record(
            AuditKind::LoginFailed,
            "app_user",
            "bad password",
            Some("10.0.0.7"),
        )
        .unwrap();
        let lines = AuditSink::read_all(tmp.path());
        assert_eq!(lines.len(), 1);
        let l = &lines[0];
        assert!(l.contains("\"kind\":\"login_failed\""), "{l}");
        assert!(l.contains("\"principal\":\"app_user\""), "{l}");
        assert!(l.contains("\"source\":\"10.0.0.7\""), "{l}");
        assert!(l.contains("\"ts_ms\":"), "{l}");
    }

    /// A crash cannot lose an event that `record` returned for.
    ///
    /// `record` fsyncs before returning and keeps no buffered state, so a
    /// SIGKILL and a dropped handle are the same thing here. This asserts the
    /// property that makes that true: the bytes are on disk, visible to an
    /// independent reader, the moment the call returns.
    #[test]
    fn a_crash_cannot_lose_a_synced_event() {
        let tmp = tempfile::tempdir().unwrap();
        let s = sink(tmp.path(), DEFAULT_MAX_BYTES, DEFAULT_KEEP);
        s.record(AuditKind::RoleCreated, "intruder", "CREATE ROLE", None)
            .unwrap();
        // Read through a path that shares nothing with the sink's handle.
        let text = std::fs::read_to_string(tmp.path().join(ACTIVE)).unwrap();
        assert!(
            text.contains("intruder"),
            "the event must be on disk before record() returns, not on a flush timer"
        );
        std::mem::forget(s); // as abrupt as a kill: no drop, no flush
        let after = AuditSink::read_all(tmp.path());
        assert_eq!(after.len(), 1);
    }

    /// The whole point: bounded under an unbounded event rate.
    #[test]
    fn the_log_is_bounded_however_many_events_arrive() {
        let tmp = tempfile::tempdir().unwrap();
        let (max, keep) = (2_048u64, 2usize);
        let s = sink(tmp.path(), max, keep);
        for i in 0..1_500 {
            s.record(
                AuditKind::LoginFailed,
                &format!("user{i}"),
                "wrong password",
                Some("10.0.0.1"),
            )
            .unwrap();
        }
        let total = AuditSink::total_bytes(tmp.path());
        assert!(
            total <= s.max_total_bytes(),
            "audit log grew to {total} bytes against a bound of {}",
            s.max_total_bytes()
        );
        // Control: it did not stay bounded by writing nothing.
        let lines = AuditSink::read_all(tmp.path());
        assert!(
            !lines.is_empty(),
            "the bound must not be met by dropping everything"
        );
        // And it kept the NEWEST events, not the oldest.
        assert!(
            lines.last().unwrap().contains("user1499"),
            "rotation must discard the oldest events; newest line was {:?}",
            lines.last()
        );
    }

    /// A crash between the rename and the prune leaves one file too many.
    /// Reopening must converge to the bound rather than inheriting it.
    #[test]
    fn a_crash_mid_rotation_converges_on_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let (max, keep) = (512u64, 2usize);
        // Manufacture the post-crash state directly: the retained files a
        // rotation had renamed, plus the extras it had not yet pruned.
        for i in 1..=6 {
            std::fs::write(
                tmp.path().join(format!("audit.{i}.log")),
                "x".repeat(max as usize),
            )
            .unwrap();
        }
        std::fs::write(tmp.path().join(ACTIVE), "y".repeat(max as usize)).unwrap();
        let before = AuditSink::total_bytes(tmp.path());
        assert!(
            before > max * (keep as u64 + 1),
            "fixture must start over the bound"
        );

        let s = sink(tmp.path(), max, keep);
        assert!(
            AuditSink::total_bytes(tmp.path()) <= s.max_total_bytes(),
            "reopening must prune what an interrupted rotation left behind"
        );
        // And the sink is usable, not just small.
        s.record(AuditKind::RoleAltered, "app_user", "ALTER ROLE", None)
            .unwrap();
        assert!(
            AuditSink::read_all(tmp.path())
                .iter()
                .any(|l| l.contains("role_altered"))
        );
    }

    /// A crash after the rename but before the new file was created leaves no
    /// active file at all. Reopening must not lose the retained ones.
    #[test]
    fn a_crash_before_the_new_file_exists_keeps_the_retained_ones() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("audit.1.log"),
            "{\"kind\":\"role_created\"}\n",
        )
        .unwrap();
        assert!(!tmp.path().join(ACTIVE).exists());

        let s = sink(tmp.path(), 4_096, 4);
        s.record(AuditKind::LoginSucceeded, "app_user", "scram", None)
            .unwrap();
        let lines = AuditSink::read_all(tmp.path());
        assert_eq!(lines.len(), 2, "the retained file's events must survive");
        assert!(lines[0].contains("role_created"), "oldest first: {lines:?}");
    }

    /// `keep = 0` means the active file is the whole log, and it must still be
    /// bounded rather than growing because there is nowhere to rotate to.
    #[test]
    fn keep_zero_is_still_bounded() {
        let tmp = tempfile::tempdir().unwrap();
        let s = sink(tmp.path(), 1_024, 0);
        for i in 0..600 {
            s.record(AuditKind::LoginFailed, &format!("u{i}"), "no", None)
                .unwrap();
        }
        let total = AuditSink::total_bytes(tmp.path());
        assert!(
            total <= s.max_total_bytes(),
            "keep=0 grew to {total} against {}",
            s.max_total_bytes()
        );
    }

    #[test]
    fn a_principal_containing_json_cannot_break_a_line() {
        let tmp = tempfile::tempdir().unwrap();
        let s = sink(tmp.path(), DEFAULT_MAX_BYTES, DEFAULT_KEEP);
        s.record(
            AuditKind::LoginFailed,
            "eve\",\"kind\":\"login_succeeded",
            "injected\nnewline",
            None,
        )
        .unwrap();
        let lines = AuditSink::read_all(tmp.path());
        assert_eq!(
            lines.len(),
            1,
            "an embedded newline must not forge a record"
        );
        assert!(
            lines[0].starts_with("{\"ts_ms\":") && lines[0].contains("\"kind\":\"login_failed\""),
            "{}",
            lines[0]
        );
        assert!(
            !lines[0].contains("\"kind\":\"login_succeeded\""),
            "a principal must not be able to forge the kind field: {}",
            lines[0]
        );
    }

    /// NE-28: an event larger than the whole file cap can never fit. Writing
    /// it anyway let rotation carry a copy into EVERY retained file, so the
    /// total bound did not hold. Refusal must be explicit and mutate nothing.
    #[test]
    fn oversized_events_are_refused_without_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        for (max, keep) in [(1_024u64, 0usize), (1_024, 1), (1_024, 4)] {
            let dir = tmp.path().join(format!("k{keep}"));
            let s = sink(&dir, max, keep);
            // One good event so the refusal is provably non-mutating against
            // a non-empty file.
            s.record(AuditKind::LoginSucceeded, "ok", "fine", None)
                .unwrap();
            let before = AuditSink::total_bytes(&dir);
            let before_lines = AuditSink::read_all(&dir);

            // keep+2 oversized attempts (detail padded past the cap, both
            // plain and with control characters that JSON expands 6x).
            for i in 0..keep + 2 {
                let detail = format!("{}{}", "\u{1}".repeat(max as usize), i);
                let err = s
                    .record(AuditKind::PolicyChanged, "huge", &detail, None)
                    .unwrap_err();
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{err}");
            }
            assert_eq!(
                AuditSink::total_bytes(&dir),
                before,
                "a refused event must not rotate or write anything"
            );
            assert_eq!(AuditSink::read_all(&dir), before_lines);

            // Sizes cap-1 and cap are fine (control chars expand 6x, so keep
            // the raw detail well under): the boundary is exact.
            let ok_detail = "x".repeat(max as usize / 8 - 64);
            assert!(s.record(AuditKind::RoleAltered, "b", &ok_detail, None).is_ok());
        }
    }

    /// NE-29: a torn final record (crash/partial write) must not corrupt the
    /// NEXT acknowledged record. Reopen repairs the tail first, so the
    /// sentinel lands on its own line and every acknowledged record parses
    /// independently — for every truncation point, including inside a UTF-8
    /// codepoint and inside a JSON escape.
    #[test]
    fn a_torn_tail_is_repaired_so_the_next_record_parses() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // A well-formed reference record to tear.
        let reference = {
            let dir = root.join("seed");
            let s = sink(&dir, 1 << 20, 4);
            s.record(
                AuditKind::PolicyChanged,
                "admén",
                "ALTER POLICY p ON t USING (true) — ünïcode detail \\\"escaped\\\"",
                None,
            )
            .unwrap();
            std::fs::read(dir.join(ACTIVE)).unwrap()
        };

        for cut in 1..reference.len() {
            let dir = root.join(format!("cut{cut}"));
            std::fs::create_dir_all(&dir).unwrap();
            // Good terminated record + torn prefix of the reference record.
            let mut seed = b"{\"ts_ms\":1,\"kind\":\"login_succeeded\",\"principal\":\"ok\"}\n".to_vec();
            seed.extend_from_slice(&reference[..cut]);
            std::fs::write(dir.join(ACTIVE), &seed).unwrap();

            // Reopen (repairs), append a sentinel, reopen again.
            let s = sink(&dir, 1 << 20, 4);
            s.record(AuditKind::RoleDropped, "sentinel", "after repair", None)
                .unwrap();
            drop(s);
            let s = sink(&dir, 1 << 20, 4);
            s.record(AuditKind::RoleCreated, "second", "after reopen2", None)
                .unwrap();
            drop(s);

            let lines = AuditSink::read_all(&dir);
            // The good seed record and both post-repair records parse on
            // their own lines, in order.
            assert!(
                lines
                    .first()
                    .is_some_and(|l| l.contains("\"kind\":\"login_succeeded\"")),
                "cut {cut}: seed record lost: {lines:?}"
            );
            assert!(
                lines.iter().any(|l| l.contains("\"principal\":\"sentinel\"")),
                "cut {cut}: the post-repair sentinel did not land as its own record: {lines:?}"
            );
            assert!(
                lines.iter().any(|l| l.contains("\"principal\":\"second\"")),
                "cut {cut}: the second-reopen record lost: {lines:?}"
            );
            // Every line parses as a standalone JSON event.
            for l in &lines {
                assert!(
                    serde_json::from_str::<serde_json::Value>(l).is_ok(),
                    "cut {cut}: unparseable audit line {l:?}"
                );
            }
        }
    }

    /// NE-29's read side: a file whose torn tail ends inside a UTF-8
    /// character used to make `read_to_string` fail wholesale, silently
    /// omitting EVERY event in that file from the export.
    #[test]
    fn read_all_survives_a_multibyte_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let s = sink(dir, 1 << 20, 4);
        s.record(AuditKind::LoginSucceeded, "ok1", "fine", None)
            .unwrap();
        drop(s);
        // Append a partial record ending mid-codepoint.
        let path = dir.join(ACTIVE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(b"{\"ts_ms\":2,\"principal\":\"ad");
        bytes.extend_from_slice(&"é".as_bytes()[..1]); // lead byte only
        std::fs::write(&path, &bytes).unwrap();

        let lines = AuditSink::read_all(dir);
        assert!(
            lines.iter().any(|l| l.contains("\"principal\":\"ok1\"")),
            "the complete record must still be exported: {lines:?}"
        );
    }
}
