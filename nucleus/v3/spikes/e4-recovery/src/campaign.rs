//! Campaign driver internals (card X-5 Work 7–8): the per-seed record,
//! `run`/`verify`/`mutants`, the coverage quotas and the digest.
//!
//! Placeholder while the store lands (staged build); filled by stage 3 of
//! the card.

/// The binary entry point. Returns the process exit code.
pub fn main(args: &[String]) -> i32 {
    let _ = args;
    eprintln!("e4-campaign: not yet built (staged card); see the crate README");
    2
}
