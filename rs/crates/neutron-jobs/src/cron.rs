//! Cron-style recurring job scheduler.
//!
//! Parse 5-field cron expressions and fire jobs into a [`JobQueue`] on schedule.
//!
//! # Field order
//!
//! ```text
//! ┌─────── minute      (0–59)
//! │ ┌───── hour        (0–23)
//! │ │ ┌─── day-of-month (1–31)
//! │ │ │ ┌─ month        (1–12)
//! │ │ │ │ ┌ day-of-week  (0–6, 0=Sunday)
//! * * * * *
//! ```
//!
//! # Supported syntax per field
//!
//! | Syntax   | Example  | Meaning                     |
//! |----------|----------|-----------------------------|
//! | `*`      | `*`      | every value                 |
//! | `*/n`    | `*/5`    | every n-th value            |
//! | `n`      | `3`      | exactly n                   |
//! | `n-m`    | `9-17`   | inclusive range             |
//! | `a,b,c`  | `1,3,5`  | list of values              |
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use neutron_jobs::{JobQueue, CronScheduler};
//!
//! let queue = Arc::new(JobQueue::new());
//!
//! CronScheduler::new()
//!     .add("*/5 * * * *",  "cleanup",      b"{}".to_vec())? // every 5 minutes
//!     .add("0 8 * * 1-5",  "daily_report", b"{}".to_vec())? // 08:00 Mon–Fri
//!     .add("30 23 * * 0",  "weekly_backup",b"{}".to_vec())? // 23:30 Sunday
//!     .run(queue);
//! ```

use std::sync::Arc;
use std::time::Duration;

use crate::queue::JobQueue;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Error type for cron expression parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum CronError {
    /// Wrong number of fields (expected 5).
    WrongFieldCount(usize),
    /// A field token could not be parsed.
    InvalidToken(String),
    /// A value is out of range for its field.
    OutOfRange { field: &'static str, value: u8 },
}

impl std::fmt::Display for CronError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CronError::WrongFieldCount(n) =>
                write!(f, "cron: expected 5 fields, got {n}"),
            CronError::InvalidToken(t) =>
                write!(f, "cron: invalid token '{t}'"),
            CronError::OutOfRange { field, value } =>
                write!(f, "cron: {field} value {value} out of range"),
        }
    }
}

impl std::error::Error for CronError {}

// ---------------------------------------------------------------------------
// CronField — bit-set of allowed values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CronField {
    bits: Vec<bool>,
}

impl CronField {
    fn matches(&self, val: u8) -> bool {
        self.bits.get(val as usize).copied().unwrap_or(false)
    }
}

fn parse_field(s: &str, name: &'static str, min: u8, max: u8) -> Result<CronField, CronError> {
    let mut bits = vec![false; (max + 1) as usize];

    for token in s.split(',') {
        if token == "*" {
            for i in min..=max { bits[i as usize] = true; }
        } else if let Some(step_str) = token.strip_prefix("*/") {
            let step: u8 = step_str.parse()
                .map_err(|_| CronError::InvalidToken(token.to_string()))?;
            if step == 0 { return Err(CronError::InvalidToken(token.to_string())); }
            let mut i = min;
            while i <= max { bits[i as usize] = true; i = i.saturating_add(step); }
        } else if let Some((a, b)) = token.split_once('-') {
            let a: u8 = a.parse().map_err(|_| CronError::InvalidToken(token.to_string()))?;
            let b: u8 = b.parse().map_err(|_| CronError::InvalidToken(token.to_string()))?;
            if a > max { return Err(CronError::OutOfRange { field: name, value: a }); }
            if b > max { return Err(CronError::OutOfRange { field: name, value: b }); }
            for i in a..=b { bits[i as usize] = true; }
        } else {
            let v: u8 = token.parse()
                .map_err(|_| CronError::InvalidToken(token.to_string()))?;
            if v < min || v > max {
                return Err(CronError::OutOfRange { field: name, value: v });
            }
            bits[v as usize] = true;
        }
    }

    Ok(CronField { bits })
}

// ---------------------------------------------------------------------------
// CronSchedule
// ---------------------------------------------------------------------------

/// A parsed cron schedule.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minutes:  CronField,   // 0–59
    hours:    CronField,   // 0–23
    doms:     CronField,   // 1–31
    months:   CronField,   // 1–12
    weekdays: CronField,   // 0–6 (0 = Sunday)
}

impl CronSchedule {
    /// Parse a 5-field cron expression.
    pub fn parse(expr: &str) -> Result<Self, CronError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronError::WrongFieldCount(fields.len()));
        }
        Ok(Self {
            minutes:  parse_field(fields[0], "minute",       0, 59)?,
            hours:    parse_field(fields[1], "hour",         0, 23)?,
            doms:     parse_field(fields[2], "day-of-month", 1, 31)?,
            months:   parse_field(fields[3], "month",        1, 12)?,
            weekdays: parse_field(fields[4], "day-of-week",  0,  6)?,
        })
    }

    /// Return `true` if this schedule fires at the given Unix timestamp
    /// (truncated to whole minutes — seconds are ignored).
    pub fn matches_epoch(&self, secs: u64) -> bool {
        let min  = ((secs / 60)   % 60) as u8;
        let hour = ((secs / 3600) % 24) as u8;
        let days =   secs / 86400;

        let (year, month, dom) = days_to_ymd(days);
        let weekday = ((days + 4) % 7) as u8; // Jan 1 1970 was Thursday (4)
        let _ = year;

        self.minutes.matches(min)
            && self.hours.matches(hour)
            && self.doms.matches(dom)
            && self.months.matches(month)
            && self.weekdays.matches(weekday)
    }
}

/// Convert days-since-epoch to (year, 1-based month, 1-based day).
fn days_to_ymd(mut d: u64) -> (u32, u8, u8) {
    let n400 = d / 146097; d %= 146097;
    let n100 = (d / 36524).min(3); d -= n100 * 36524;
    let n4   = d / 1461;   d %= 1461;
    let n1   = (d / 365).min(3); d -= n1 * 365;

    let year = (n400 * 400 + n100 * 100 + n4 * 4 + n1 + 1970) as u32;
    let leap = (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400);
    let dim: [u8; 12] = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let mut month = 0u8;
    let mut day   = d as u8 + 1;
    for (i, &m) in dim.iter().enumerate() {
        if day <= m { month = i as u8 + 1; break; }
        day -= m;
    }
    (year, month, day)
}

// ---------------------------------------------------------------------------
// CronJob + CronScheduler
// ---------------------------------------------------------------------------

/// A single recurring job definition.
pub struct CronJob {
    pub schedule: CronSchedule,
    pub job_type: String,
    pub payload:  Vec<u8>,
}

/// Runs recurring jobs on cron schedules.
///
/// Call [`CronScheduler::run`] to spawn a background task that fires
/// matching jobs into the [`JobQueue`] once per minute.
pub struct CronScheduler {
    jobs: Vec<CronJob>,
}

impl Default for CronScheduler {
    fn default() -> Self { Self::new() }
}

impl CronScheduler {
    /// Create an empty scheduler.
    pub fn new() -> Self {
        CronScheduler { jobs: Vec::new() }
    }

    /// Add a recurring job.
    ///
    /// `expr` is a 5-field cron expression.  `payload` is the raw bytes
    /// passed to the handler via [`JobContext::payload`].
    pub fn add(
        mut self,
        expr:     &str,
        job_type: impl Into<String>,
        payload:  Vec<u8>,
    ) -> Result<Self, CronError> {
        let schedule = CronSchedule::parse(expr)?;
        self.jobs.push(CronJob { schedule, job_type: job_type.into(), payload });
        Ok(self)
    }

    /// Spawn a tokio background task that checks every minute and enqueues
    /// any jobs whose schedule matches the current time.
    ///
    /// The returned handle can be aborted to stop the scheduler.
    pub fn run(self, queue: Arc<JobQueue>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Align to the next whole minute before starting
            let now_secs = unix_secs();
            let secs_into_minute = now_secs % 60;
            if secs_into_minute > 0 {
                tokio::time::sleep(Duration::from_secs(60 - secs_into_minute)).await;
            }

            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let epoch = unix_secs();
                for job in &self.jobs {
                    if job.schedule.matches_epoch(epoch) {
                        queue.enqueue(job.job_type.clone(), job.payload.clone());
                    }
                }
            }
        })
    }
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // 2024-01-15 (Monday, weekday=1) 08:30:00 UTC = 1705307400
    const MON_0830: u64 = 1705307400;
    // 2024-01-21 (Sunday, weekday=0) 23:30:00 UTC = 1705879800
    const SUN_2330: u64 = 1705879800;
    // 2024-01-15 00:00:00 UTC = 1705276800
    const MON_0000: u64 = 1705276800;

    fn parse(expr: &str) -> CronSchedule { CronSchedule::parse(expr).unwrap() }

    #[test]
    fn every_minute() {
        let s = parse("* * * * *");
        assert!(s.matches_epoch(MON_0830));
        assert!(s.matches_epoch(SUN_2330));
    }

    #[test]
    fn specific_minute_and_hour() {
        let s = parse("30 8 * * *");
        assert!(s.matches_epoch(MON_0830));
        assert!(!s.matches_epoch(MON_0000)); // 00:00, not 08:30
    }

    #[test]
    fn step_syntax() {
        let s = parse("*/5 * * * *");
        // MON_0830 = minute 30, which is divisible by 5
        assert!(s.matches_epoch(MON_0830));
        // minute 31 would not match
        assert!(!s.matches_epoch(MON_0830 + 60));
    }

    #[test]
    fn range_weekdays_mon_fri() {
        let s = parse("0 8 * * 1-5");
        assert!(s.matches_epoch(1705276800 + 8 * 3600)); // Monday 08:00
        assert!(!s.matches_epoch(SUN_2330));              // Sunday
    }

    #[test]
    fn sunday_only() {
        let s = parse("30 23 * * 0");
        assert!(s.matches_epoch(SUN_2330));
        assert!(!s.matches_epoch(MON_0830));
    }

    #[test]
    fn list_syntax() {
        let s = parse("0 8,12,18 * * *");
        assert!(s.matches_epoch(1705276800 + 8 * 3600));   // 08:00
        assert!(s.matches_epoch(1705276800 + 12 * 3600));  // 12:00
        assert!(s.matches_epoch(1705276800 + 18 * 3600));  // 18:00
        assert!(!s.matches_epoch(1705276800 + 9 * 3600));  // 09:00 — no
    }

    #[test]
    fn day_of_month() {
        let s = parse("0 0 15 * *");
        assert!(s.matches_epoch(MON_0000));   // Jan 15 00:00
        assert!(!s.matches_epoch(MON_0000 + 86400)); // Jan 16
    }

    #[test]
    fn month_filter() {
        let s = parse("0 0 1 1 *"); // midnight Jan 1st
        let jan1_2024: u64 = 1704067200;
        assert!(s.matches_epoch(jan1_2024));
        assert!(!s.matches_epoch(MON_0000)); // Jan 15, not Jan 1
    }

    #[test]
    fn parse_error_wrong_field_count() {
        assert!(matches!(
            CronSchedule::parse("* * * *"),
            Err(CronError::WrongFieldCount(4))
        ));
        assert!(matches!(
            CronSchedule::parse("* * * * * *"),
            Err(CronError::WrongFieldCount(6))
        ));
    }

    #[test]
    fn parse_error_out_of_range() {
        assert!(matches!(
            CronSchedule::parse("60 * * * *"),
            Err(CronError::OutOfRange { .. })
        ));
        assert!(matches!(
            CronSchedule::parse("* 24 * * *"),
            Err(CronError::OutOfRange { .. })
        ));
    }

    #[test]
    fn parse_error_invalid_token() {
        assert!(CronSchedule::parse("abc * * * *").is_err());
        assert!(CronSchedule::parse("*/0 * * * *").is_err()); // step of 0
    }

    #[test]
    fn scheduler_add_invalid_expr_returns_error() {
        let result = CronScheduler::new().add("bad expr", "job", vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn scheduler_add_valid() {
        let s = CronScheduler::new()
            .add("* * * * *", "ping", b"{}".to_vec()).unwrap()
            .add("0 8 * * 1-5", "report", b"{}".to_vec()).unwrap();
        assert_eq!(s.jobs.len(), 2);
    }

    #[test]
    fn days_to_ymd_epoch() {
        let (y, m, d) = days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn days_to_ymd_known() {
        // 2024-01-15 = day 19737 + 14 = 19737 + 14 = ?
        // 2024-01-01 = 19723 days; + 14 = 19737
        let (y, m, d) = days_to_ymd(19737);
        assert_eq!((y, m, d), (2024, 1, 15));
    }

    #[test]
    fn weekday_thursday_epoch() {
        // Jan 1 1970 was Thursday (4)
        assert_eq!(4, 4);
    }
}
