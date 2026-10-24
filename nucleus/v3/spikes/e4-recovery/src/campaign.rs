//! Campaign driver internals (card X-5 Work 7–8): the per-seed record,
//! `run`/`verify`/`mutants`, the coverage quotas and the digest.
//!
//! **Stage status (see the crate README):** two runner attempts were each
//! killed at the 60-minute limit. The sim and the P-written oracle are
//! done and green; the store (Work 3–4) is committed and compile-checked
//! but cannot be exercised until its call flow is converted to
//! internal-lock primitives (its methods park the OS thread at yield
//! points, which deadlocks under a held world mutex), and the workload
//! driver is an uncompiled draft. This stub exits non-zero on every
//! subcommand rather than reporting anything green; the card run's RESULT
//! records the exact acceptance outcomes.

/// The binary entry point. Returns the process exit code (non-zero until
/// the campaign exists).
pub fn main(args: &[String]) -> i32 {
    let _ = args;
    eprintln!(
        "e4-campaign: not built (card X-5 stages 3+ unwired after two 60-minute \
         runner kills; see spikes/e4-recovery/README.md)"
    );
    2
}
