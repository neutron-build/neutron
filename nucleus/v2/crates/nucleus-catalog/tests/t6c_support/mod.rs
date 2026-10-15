//! C-T6c1afix2 FU-2: the write half of the TRUNCATE-window rule (c1b
//! FU-B) and the per-level read split (c1a FU-5 / c1b FU-C) live in
//! `diff/check.rs` proper now — folded back exactly as C-T6c1afix's
//! first cut had them (the `Stats` level fields and the window check's
//! write leg), so this module no longer restates any of the merged
//! checker's internals (`committed_truncates`, `truncate_window`,
//! `where_op`, the write pass, the level split): one source of truth,
//! no drift against C-T6d2's `diff/` changes.
//!
//! What remains is the entry point C-T6c1afix's write-window pin calls
//! (`t6c_regress_flags_an_in_window_rr_write`, a file outside that
//! card's and this card's scope): a plain delegation to the merged
//! checker, which since the fold includes both passes. New callers
//! should call `diff::check` directly.
#![allow(dead_code)]

use crate::diff::check;
use crate::diff::{Run, Verdict};

/// The full verdict the c1afix binaries assert on: the merged
/// `diff::check` — every check it makes, the write-side TRUNCATE-window
/// pass (c1b FU-B) and the per-level read split (c1a FU-5 / c1b FU-C)
/// included since C-T6c1afix2 FU-2 folded them in. Before the fold this
/// composed the write pass over the checker's verdict; now the checker
/// owns it, and this is the compatibility name the out-of-scope
/// write-window pin still calls.
pub fn check_with_write_windows(run: &Run) -> Verdict {
    check(run)
}
