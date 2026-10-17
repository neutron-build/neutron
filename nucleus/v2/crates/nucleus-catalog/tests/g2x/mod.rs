//! g2x: the G2 checker over the typed table layer (card C-T6g). The
//! checker itself is **not here**: `mod g2` includes `nucleus-txn`'s own
//! `tests/g2/{history,check}.rs` verbatim through `#[path]` — no copy of
//! the checker exists in this crate and those files are never edited (the
//! card's "No copy of the checker").
//!
//! **Why the paths look wrong.** For a `mod` item declared *inside* an
//! inline module, rustc resolves a `#[path]` attribute relative to the
//! inline module's own directory — this file's directory joined with the
//! module's name — not relative to the file itself. `mod g2 { … }` in
//! `tests/g2x/mod.rs` therefore resolves its paths against
//! `tests/g2x/g2/` (a directory that does not need to exist, since every
//! nested module carries an explicit `#[path]`). From there four `..`
//! climb to `crates/`, which is what makes
//! `../../../../nucleus-txn/tests/g2/history.rs` name the checker's
//! history module. `check.rs` opens with
//! `use super::history::{…}`, and `super` of a file included as
//! `g2::check` is exactly this inline `g2` — so the two files compile
//! against each other unmodified, exactly as they do inside
//! `nucleus-txn`'s own `g2` test crate.
//!
//! `runner` is this card's own: G2-style seeded histories whose slots are
//! rows of an access-method table (`nucleus-catalog::Db`), with a DDL
//! session issuing ADD COLUMN, TRUNCATE and one online
//! `CREATE INDEX CONCURRENTLY` on a mirror column `m = pk` (an index
//! range scan observes the same slots as a pk scan, so the checker's
//! semantics are unchanged).
#![allow(dead_code)]
// Three separate test binaries share this tree and each uses a different
// slice of it (the checker's hand-history builders included); the lint
// would fire on the union of what any one binary leaves out.

pub mod g2 {
    #[path = "../../../../nucleus-txn/tests/g2/check.rs"]
    pub mod check;
    #[path = "../../../../nucleus-txn/tests/g2/history.rs"]
    pub mod history;
}

pub mod runner;
