//! StdEnv: every backend-generic conformance check of `common`, plus
//! the StdEnv-only tests (card A-1a Work 6). Unix only: the crate
//! compiles no `StdEnv` elsewhere, and this binary has nothing to run.
//! The trait's contract is the crate doc of `nucleus-env`; PLAN-v3
//! §3.2 and S-STORE §2, §12.5 and §14.1 are the normative sources.
#![cfg(unix)]
#![allow(missing_docs)]

mod common;

use nucleus_env::{StdEnv, StdFile};
use std::path::{Path, PathBuf};

/// A scratch directory under `CARGO_TARGET_TMPDIR`, one per test,
/// created and removed by the test (card A-1a Work 6). Removing it in
/// `Drop` keeps the tree clean even when a check fails.
struct Scratch {
    dir: PathBuf,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Best effort: a panic in `drop` during unwinding would abort.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A `StdEnv` over a fresh empty scratch directory, and the scratch
/// itself. Fails as a `String` because this helper runs outside
/// `#[test]` fns, where the test lint exemptions do not reach.
fn std_scratch(name: &str) -> Result<(StdEnv, Scratch), String> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("nucleus-env-std-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir); // a previous aborted run's leftovers
    std::fs::create_dir_all(&dir).map_err(|e| format!("create scratch {dir:?}: {e}"))?;
    let env = StdEnv::new(&dir).map_err(|e| format!("StdEnv::new {dir:?}: {e:?}"))?;
    Ok((env, Scratch { dir }))
}

macro_rules! std_test {
    ($name:ident, $check:ident) => {
        #[test]
        fn $name() {
            let (env, _scratch) = match std_scratch(stringify!($check)) {
                Ok(pair) => pair,
                Err(e) => panic!("{e}"),
            };
            assert_eq!(common::$check(&env), Ok(()));
        }
    };
}

std_test!(std_open_modes, open_modes);
std_test!(std_read_write_roundtrip, read_write_roundtrip);
std_test!(std_read_handle_refuses_writes, read_handle_refuses_writes);
std_test!(std_namespace_ops, namespace_ops);
std_test!(std_invalid_paths_rejected, invalid_paths_rejected);
std_test!(std_sync_dir_root_and_subdir, sync_dir_root_and_subdir);
std_test!(std_sync_on_writable_handles, sync_on_writable_handles);
std_test!(
    std_locks_exclusive_excludes_all,
    locks_exclusive_excludes_all
);
std_test!(
    std_locks_shared_readers_coexist,
    locks_shared_readers_coexist
);
std_test!(
    std_locks_shared_excludes_writer,
    locks_shared_excludes_writer
);
std_test!(
    std_lock_released_on_unlock_and_drop,
    lock_released_on_unlock_and_drop
);
std_test!(std_lock_creates_no_file, lock_creates_no_file);
std_test!(std_clock_and_rand, clock_and_rand);

/// StdEnv-only: the root must exist and be a directory. A missing path
/// gives `NotFound`; a file gives `InvalidPath` ("root is not a
/// directory") — recorded here, as card A-1a's RESULT asks.
#[test]
fn std_root_must_exist() {
    let tmp = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("nucleus-env-std-{}-root", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();

    let missing = tmp.join("no-such-dir");
    match StdEnv::new(&missing) {
        Err(nucleus_env::EnvError::NotFound { path }) => {
            assert!(path.ends_with("no-such-dir"), "path {path:?}")
        }
        other => panic!("StdEnv::new of a missing path gave {other:?}, wanted NotFound"),
    }

    let file = tmp.join("i-am-a-file");
    std::fs::write(&file, b"x").unwrap();
    match StdEnv::new(&file) {
        Err(nucleus_env::EnvError::InvalidPath { why, .. }) => {
            assert_eq!(why, "root is not a directory")
        }
        other => panic!("StdEnv::new of a file gave {other:?}, wanted InvalidPath"),
    }

    std::fs::remove_dir_all(&tmp).unwrap();
}

/// All backends are `Send + Sync`, so engine code can share one across
/// threads (card A-1a Work 2; test `backends_are_send_sync`).
#[test]
fn backends_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StdEnv>();
    assert_send_sync::<StdFile>();
}
