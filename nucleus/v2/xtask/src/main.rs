//! Repository tooling for nucleus v2: `cargo run -q -p xtask -- <command>`.

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
