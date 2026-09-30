//! `timestamptz` text rendering in a session time zone.
//!
//! A `Value::TimestampTz` is always the UTC instant (microseconds since
//! 2000-01-01). PostgreSQL renders it in the session TimeZone with the zone's
//! offset appended (`2026-01-02 10:04:05+09`); `Value`'s `Display` has no
//! session, so it keeps rendering UTC and session-aware callers (the wire
//! codec, casts to text) go through here.

use std::fmt;

use chrono::{Offset, TimeZone, Utc};
use chrono_tz::Tz;

const POSTGRES_UNIX_EPOCH_SECONDS: i64 = 946_684_800;

/// Seconds east of UTC that `tz` observes at the instant `us`.
pub fn zone_offset_seconds(us: i64, tz: Tz) -> i64 {
    let secs = us
        .div_euclid(1_000_000)
        .saturating_add(POSTGRES_UNIX_EPOCH_SECONDS);
    Utc.timestamp_opt(secs, 0)
        .single()
        .map(|instant| {
            tz.offset_from_utc_datetime(&instant.naive_utc())
                .fix()
                .local_minus_utc() as i64
        })
        .unwrap_or(0)
}

struct WallClock(i64);

impl fmt::Display for WallClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        super::format_timestamp(f, self.0)
    }
}

/// PostgreSQL's text form of a timestamptz in `tz`: wall clock plus the
/// offset as `+HH`, `+HH:MM` or `+HH:MM:SS` (minutes and seconds only when
/// non-zero).
pub fn format_timestamptz(us: i64, tz: Tz) -> String {
    let offset = zone_offset_seconds(us, tz);
    let mut out = WallClock(us.saturating_add(offset * 1_000_000)).to_string();
    let sign = if offset < 0 { '-' } else { '+' };
    let abs = offset.abs();
    let (hours, minutes, seconds) = (abs / 3600, (abs % 3600) / 60, abs % 60);
    out.push(sign);
    out.push_str(&format!("{hours:02}"));
    if minutes != 0 || seconds != 0 {
        out.push_str(&format!(":{minutes:02}"));
    }
    if seconds != 0 {
        out.push_str(&format!(":{seconds:02}"));
    }
    out
}
