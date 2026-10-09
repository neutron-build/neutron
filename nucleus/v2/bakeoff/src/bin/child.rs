//! Child writer for the kill -9 probe (C-S1 work item 3): opens a backend at
//! the given directory and writes cross-prefix atomic batches with
//! `Durability::No` until killed or `n` batches are done. Progress lines on
//! stdout let the parent pick a moment to kill it mid-stream.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use nucleus_kv::{Batch, Durability, OrderedKv};

#[derive(Debug)]
struct Args {
    backend: String,
    dir: PathBuf,
    batches: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args();
    let _prog = it.next();
    let backend = it.next().ok_or("backend (rocks|fjall)")?;
    let dir = it.next().ok_or("dir")?;
    let batches = it.next().ok_or("batches")?;
    let batches: u64 = batches.parse().map_err(|_| "batches must be a u64")?;
    if !matches!(backend.as_str(), "rocks" | "fjall") {
        return Err(format!("unknown backend {backend}"));
    }
    Ok(Args {
        backend,
        dir: PathBuf::from(dir),
        batches,
    })
}

fn write_loop<Kv: OrderedKv>(kv: &Kv, batches: u64) -> Result<(), nucleus_kv::KvError> {
    let mut out = std::io::stdout().lock();
    for i in 0..batches {
        // One atomic batch across two far-apart prefixes plus a counter:
        // all-or-nothing after a crash is the one-WAL requirement.
        let mut b = Batch::default();
        b.put(format!("a/{i:012}").into_bytes(), i.to_be_bytes().to_vec());
        b.put(format!("z/{i:012}").into_bytes(), i.to_be_bytes().to_vec());
        b.put(b"ctr".to_vec(), i.to_be_bytes().to_vec());
        kv.write(b, Durability::No)?;
        if i % 1000 == 0 {
            let _ = out.write_all(format!("batch {i}\n").as_bytes());
            let _ = out.flush();
        }
    }
    let _ = out.write_all(b"done\n");
    let _ = out.flush();
    Ok(())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()
        .map_err(|e| format!("usage: bakeoff-child <rocks|fjall> <dir> <batches>: {e}"))?;
    match args.backend.as_str() {
        #[cfg(feature = "rocks")]
        "rocks" => write_loop(
            &nucleus_bakeoff::rocks::RocksKv::open(&args.dir)?,
            args.batches,
        )?,
        #[cfg(not(feature = "rocks"))]
        "rocks" => return Err("built without the rocks feature".into()),
        #[cfg(feature = "fjall")]
        "fjall" => write_loop(
            &nucleus_bakeoff::fjall_kv::FjallKv::open(&args.dir)?,
            args.batches,
        )?,
        #[cfg(not(feature = "fjall"))]
        "fjall" => return Err("built without the fjall feature".into()),
        _ => return Err("unknown backend".into()),
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bakeoff-child: {e}");
            ExitCode::FAILURE
        }
    }
}
