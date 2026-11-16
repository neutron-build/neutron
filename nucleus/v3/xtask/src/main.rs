// Provenance (card A-1a): nucleus/v2/xtask/src/main.rs at 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b.
//! Repository tooling for nucleus v3: `cargo run -q -p xtask -- <command>`.

mod api_surface;

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.split_first() {
        Some((cmd, rest)) if cmd == "api-surface" => api_surface::run(rest),
        _ => {
            eprintln!("usage: xtask api-surface ALLOWLIST FILE...");
            ExitCode::from(2)
        }
    }
}
