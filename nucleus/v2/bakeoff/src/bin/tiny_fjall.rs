//! Tiny program linking fjall, for the release-binary-size score. Built with
//! `--no-default-features --features fjall`.

use nucleus_kv::OrderedKv;
use std::process::ExitCode;

fn main() -> ExitCode {
    let dir = nucleus_bakeoff::scratch::root().join("tiny-fjall");
    let kv = match nucleus_bakeoff::fjall_kv::FjallKv::open(&dir) {
        Ok(kv) => kv,
        Err(e) => {
            eprintln!("tiny-fjall: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ok = kv
        .write(
            nucleus_kv::Batch {
                ops: vec![nucleus_kv::Op::Put(b"k".to_vec(), b"v".to_vec())],
            },
            nucleus_kv::Durability::Yes,
        )
        .and_then(|()| kv.sync_wal())
        .and_then(|()| kv.get_latest(b"k"))
        .map(|v| v == Some(b"v".to_vec()))
        .unwrap_or(false);
    let _ = std::fs::remove_dir_all(&dir);
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
