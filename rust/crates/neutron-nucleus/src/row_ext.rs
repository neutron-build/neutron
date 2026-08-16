//! Non-panicking column access.
//!
//! `tokio_postgres::Row::get` panics when the requested Rust type does not match
//! the column's declared type. Every model method in this crate used it, so a
//! server that described a column differently than the client expected aborted
//! the caller's process instead of returning an error.
//!
//! That is not hypothetical: Nucleus declared `TS_LAST` as `varchar` while
//! returning a float, and the Rust client panicked partway through a run. The
//! engine bug is fixed, but a client should not be one Describe disagreement
//! away from a crash, so column reads go through `get_ck` and surface
//! [`NucleusError::Decode`].

use tokio_postgres::types::FromSql;
use tokio_postgres::Row;

use crate::error::NucleusError;

pub(crate) trait RowExt {
    /// Read column `idx` as `T`, returning an error rather than panicking when
    /// the declared type does not match.
    fn get_ck<'a, T: FromSql<'a>>(&'a self, idx: usize) -> Result<T, NucleusError>;
}

impl RowExt for Row {
    fn get_ck<'a, T: FromSql<'a>>(&'a self, idx: usize) -> Result<T, NucleusError> {
        self.try_get::<'a, _, T>(idx)
            .map_err(|source| NucleusError::Decode {
                column: idx,
                source,
            })
    }
}
