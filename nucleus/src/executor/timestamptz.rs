//! Session-TimeZone handling for `timestamptz` input and output.
//!
//! A timestamptz is stored as a UTC instant. An explicit offset or zone name
//! on input decides the instant; only a literal with no zone is read as wall
//! time in the session TimeZone. Output renders in the session TimeZone.

use chrono_tz::Tz;

use super::ExecError;
use super::helpers::{local_timestamp_at_time_zone, parse_time_zone};
use crate::types::TimestampZone;

/// The TimeZone of the session running on this task, UTC when there is none
/// (or the stored name no longer parses). For code that has no `&Executor`
/// (value comparison, formatting).
pub(super) fn ambient_time_zone() -> Tz {
    super::session::CURRENT_SESSION
        .try_with(|session| session.settings.read().get("timezone").cloned())
        .ok()
        .flatten()
        .and_then(|name| parse_time_zone(&name).ok())
        .unwrap_or(Tz::UTC)
}

/// Text to a timestamptz instant: an explicit offset or zone name wins, a bare
/// literal is wall time in `session_tz`.
pub(super) fn parse_timestamptz_text(text: &str, session_tz: Tz) -> Result<i64, ExecError> {
    let (local, zone) = crate::types::parse_timestamptz_zoned(text).map_err(ExecError::Runtime)?;
    match zone {
        Some(TimestampZone::Offset(offset)) => local
            .checked_sub(offset * 1_000_000)
            .ok_or_else(|| ExecError::Runtime("timestamp value out of range".into())),
        Some(TimestampZone::Named(name)) => {
            local_timestamp_at_time_zone(local, parse_time_zone(&name)?)
        }
        None => local_timestamp_at_time_zone(local, session_tz),
    }
}

/// Non-executor twin of [`parse_timestamptz_text`] for comparisons, which have
/// no `Result<_, ExecError>` channel: `None` when the text is not a timestamp.
pub(super) fn timestamptz_text_to_instant(text: &str) -> Option<i64> {
    parse_timestamptz_text(text, ambient_time_zone()).ok()
}

const DAY_US: i64 = 86_400_000_000;

/// `date_trunc(field, ts)` on wall-clock microseconds (floor division, so
/// pre-2000 values truncate downward). `None` for an unsupported field.
pub(super) fn truncate_wall_clock(field: &str, ts: i64) -> Option<i64> {
    let days = ts.div_euclid(DAY_US);
    let in_day = ts.rem_euclid(DAY_US);
    let (y, m, _) = crate::types::days_to_ymd(i32::try_from(days).ok()?);
    let day_start = |d: i32| i64::from(d) * DAY_US;
    Some(match field {
        "year" => day_start(crate::types::ymd_to_days(y, 1, 1)),
        "quarter" => day_start(crate::types::ymd_to_days(y, (m - 1) / 3 * 3 + 1, 1)),
        "month" => day_start(crate::types::ymd_to_days(y, m, 1)),
        // 2000-01-01 is a Saturday, so Monday-based weekday = (d + 5) mod 7.
        "week" => (days - (days + 5).rem_euclid(7)) * DAY_US,
        "day" => days * DAY_US,
        "hour" => days * DAY_US + in_day / 3_600_000_000 * 3_600_000_000,
        "minute" => days * DAY_US + in_day / 60_000_000 * 60_000_000,
        "second" => days * DAY_US + in_day / 1_000_000 * 1_000_000,
        _ => return None,
    })
}

/// One calendar/clock field of wall-clock microseconds as `date_part` returns
/// it; `instant_us` is the UTC instant, which alone determines `epoch`.
pub(super) fn wall_clock_field(field: &str, ts: i64, instant_us: i64) -> Option<i64> {
    let total_secs = ts.div_euclid(1_000_000);
    let days = i32::try_from(total_secs.div_euclid(86_400)).ok()?;
    let time_secs = total_secs.rem_euclid(86_400);
    let (y, m, d) = crate::types::days_to_ymd(days);
    Some(match field {
        "year" => i64::from(y),
        "month" => i64::from(m),
        "day" => i64::from(d),
        "hour" => time_secs / 3600,
        "minute" => time_secs % 3600 / 60,
        "second" => time_secs % 60,
        "dow" | "dayofweek" => i64::from((days + 6).rem_euclid(7)),
        "doy" | "dayofyear" => i64::from(days - crate::types::ymd_to_days(y, 1, 1) + 1),
        "epoch" => instant_us.div_euclid(1_000_000) + 946_684_800,
        _ => return None,
    })
}
