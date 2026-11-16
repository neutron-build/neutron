//! The campaign driver (card X-5 Work 7): `run`, `verify`, `mutants`.
//!
//! Stage note — see `src/campaign.rs` and the crate README: the X5-R6
//! guard stops the first judged campaign run; `verify`/`mutants
//! --check` exit non-zero on the missing results files, by design.
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(e4_recovery::campaign::main(&args));
}
