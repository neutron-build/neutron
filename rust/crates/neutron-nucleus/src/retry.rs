//! Serialization-failure classification and a managed retry helper.
//!
//! `SERIALIZABLE` is real on the shipping engine, which makes SQLSTATE 40001
//! something applications actually receive. A serializable transaction that is
//! never retried is a transaction that randomly fails under concurrency, and
//! no PostgreSQL driver retries for you — drivers surface the code, the
//! application decides. This is the SDK's answer to that obligation, per
//! `FRAMEWORK_CONTRACT.md` §3.14.
//!
//! ```rust,ignore
//! let moved = db
//!     .with_transaction(&RetryOptions::serializable(), |tx| async move {
//!         tx.execute("UPDATE accounts SET balance = balance - 10 WHERE id = $1", &[&id])
//!             .await?;
//!         Ok(())
//!     })
//!     .await?;
//! ```

use std::future::Future;
use std::time::Duration;

use crate::db::{Db, NucleusTransaction};
use crate::error::NucleusError;

/// The transaction lost a conflict and MUST be retried from the beginning.
///
/// Raised by two mechanisms: strict 2PL wait-die on the disk engine (the
/// younger transaction is killed to break a potential deadlock) and SSI on the
/// MVCC engine (a dangerous structure detected at commit).
pub const SQLSTATE_SERIALIZATION_FAILURE: &str = "40001";

/// `lock_timeout` elapsed waiting for a table lock. Deliberately NOT retryable:
/// the holder is still there, so retrying spins against a lock that is not
/// moving. Raise `lock_timeout` or find the transaction holding it.
pub const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";

/// A statement was issued after the transaction had already been aborted. The
/// transaction is dead and only ROLLBACK is accepted, so the whole transaction
/// must re-run — which is exactly what the retry helper does.
pub const SQLSTATE_IN_FAILED_TRANSACTION: &str = "25P02";

/// The SQLSTATE of a driver error, or `None` if it carries no code.
///
/// Classification is by SQLSTATE, never by message text, and it looks through
/// the error wrapping so a code stays visible after the SDK boxes it.
pub fn sqlstate(err: &NucleusError) -> Option<&str> {
    let pg = match err {
        NucleusError::Query(e) | NucleusError::Connect(e) => e,
        NucleusError::Migration { source, .. } => return sqlstate(source),
        _ => return None,
    };
    pg.as_db_error().map(|db| db.code().code())
}

/// Whether `err` is a conflict the caller should retry (40001, or 25P02 from a
/// transaction already killed by one).
pub fn is_serialization_failure(err: &NucleusError) -> bool {
    matches!(
        sqlstate(err),
        Some(SQLSTATE_SERIALIZATION_FAILURE) | Some(SQLSTATE_IN_FAILED_TRANSACTION)
    )
}

/// Whether `err` is a `lock_timeout` expiry (55P03).
///
/// Kept distinct from [`is_serialization_failure`] on purpose: the two call for
/// opposite responses. A serialization failure means "someone else won, try
/// again"; a lock timeout means "the lock is still held, retrying will not
/// help".
pub fn is_lock_not_available(err: &NucleusError) -> bool {
    sqlstate(err) == Some(SQLSTATE_LOCK_NOT_AVAILABLE)
}

/// Retry policy for [`Db::with_transaction`].
#[derive(Debug, Clone)]
pub struct RetryOptions {
    /// Attempts including the first. Values below 1 are treated as 1.
    pub max_attempts: u32,
    /// Delay before the second attempt; doubled each subsequent attempt.
    pub base_delay: Duration,
    /// Ceiling on the backoff.
    pub max_delay: Duration,
    /// Isolation level for the transaction, e.g. `"SERIALIZABLE"`. `None`
    /// leaves the server default.
    pub isolation_level: Option<String>,
}

impl Default for RetryOptions {
    /// Backoff is randomised (full jitter). Without it two conflicting
    /// transactions retry in lockstep and collide again on the same schedule —
    /// and under wait-die the younger one loses every round, so a fixed
    /// backoff can starve it indefinitely.
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(2),
            max_delay: Duration::from_millis(250),
            isolation_level: None,
        }
    }
}

impl RetryOptions {
    /// Defaults, with the transaction run at `SERIALIZABLE`.
    pub fn serializable() -> Self {
        Self {
            isolation_level: Some("SERIALIZABLE".into()),
            ..Self::default()
        }
    }
}

/// Deterministic full-jitter backoff without pulling in a RNG dependency:
/// hashes the attempt number and the clock into `[0, delay]`.
fn jittered(delay: Duration, attempt: u32) -> Duration {
    let nanos = delay.as_nanos().max(1) as u64;
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
        ^ (attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    // xorshift64*, enough for spreading retries.
    let mut x = seed | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    Duration::from_nanos(x % (nanos + 1))
}

impl Db {
    /// Run `f` inside a transaction, retrying it on serialization failure.
    ///
    /// `f` MUST be idempotent with respect to anything outside the database: it
    /// can run more than once. Everything it does through the transaction is
    /// rolled back between attempts; anything it does elsewhere (sending mail,
    /// charging a card, mutating shared state) is not.
    ///
    /// On success the transaction commits. On a serialization failure it is
    /// rolled back and retried with jittered exponential backoff. On any other
    /// error it is rolled back and the error returned unchanged — in
    /// particular a `lock_timeout` (55P03) is NOT retried.
    pub async fn with_transaction<F, Fut, T>(
        &self,
        opts: &RetryOptions,
        mut f: F,
    ) -> Result<T, NucleusError>
    where
        F: FnMut(NucleusTransaction) -> Fut,
        Fut: Future<Output = Result<(NucleusTransaction, T), NucleusError>>,
    {
        let attempts = opts.max_attempts.max(1);
        let mut delay = if opts.base_delay.is_zero() {
            RetryOptions::default().base_delay
        } else {
            opts.base_delay
        };
        let mut last_err: Option<NucleusError> = None;

        for attempt in 1..=attempts {
            let tx = self.transaction().await?;
            if let Some(level) = &opts.isolation_level {
                // Rejected rather than silently downgraded by an engine that
                // cannot honour the level, so this surfaces the mismatch.
                if let Err(e) = tx
                    .execute(&format!("SET TRANSACTION ISOLATION LEVEL {level}"), &[])
                    .await
                {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }

            match f(tx).await {
                Ok((tx, value)) => match tx.commit().await {
                    Ok(()) => return Ok(value),
                    Err(e) if is_serialization_failure(&e) => last_err = Some(e),
                    Err(e) => return Err(e),
                },
                Err(e) => {
                    // The closure consumed the transaction; it is dropped on
                    // its error path, and `Drop` issues a best-effort
                    // ROLLBACK so an abandoned exclusive lock cannot block
                    // every other serializable transaction on that table.
                    if !is_serialization_failure(&e) {
                        return Err(e);
                    }
                    last_err = Some(e);
                }
            }

            if attempt < attempts {
                let sleep = jittered(delay, attempt);
                tokio::time::sleep(sleep).await;
                delay = (delay * 2).min(opts.max_delay.max(delay));
            }
        }

        Err(last_err.unwrap_or(NucleusError::PoolExhausted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_timeout_is_not_treated_as_retryable() {
        // The distinction that matters: retrying 55P03 spins against a lock
        // that is not moving, so it must never be folded into 40001.
        assert_ne!(
            SQLSTATE_LOCK_NOT_AVAILABLE,
            SQLSTATE_SERIALIZATION_FAILURE,
            "lock timeout and serialization failure are different outcomes"
        );
    }

    #[test]
    fn serializable_options_request_the_level() {
        let opts = RetryOptions::serializable();
        assert_eq!(opts.isolation_level.as_deref(), Some("SERIALIZABLE"));
        assert!(opts.max_attempts >= 1);
    }

    #[test]
    fn jitter_stays_within_the_delay() {
        let d = Duration::from_millis(50);
        for attempt in 1..64 {
            assert!(jittered(d, attempt) <= d, "jitter must not exceed the delay");
        }
    }

    #[test]
    fn zero_delay_falls_back_to_the_default() {
        let opts = RetryOptions {
            base_delay: Duration::ZERO,
            ..RetryOptions::default()
        };
        assert!(opts.base_delay.is_zero());
        assert!(!RetryOptions::default().base_delay.is_zero());
    }
}
