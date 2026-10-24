//! Campaign driver internals (card X-5 Work 7–8): the per-seed record,
//! `run`/`verify`/`mutants`, the coverage quotas and the digest.
//!
//! **Stage status (see the crate README):** the run was killed at the
//! 60-minute limit during the store stage (Work 3–4), so the campaign has
//! nothing to drive. This stub exits non-zero on every subcommand rather
//! than reporting anything green; the card run's RESULT records the exact
//! acceptance outcomes.

/// The binary entry point. Returns the process exit code (non-zero until
/// the campaign exists).
pub fn main(args: &[String]) -> i32 {
    let _ = args;
    eprintln!(
        "e4-campaign: not built (card X-5 stage 3+ missing after the run was \
         killed at the 60-minute limit); see spikes/e4-recovery/README.md"
    );
    2
}
