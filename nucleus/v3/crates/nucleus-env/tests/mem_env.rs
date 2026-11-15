//! MemEnv: every backend-generic conformance check of `common`, plus
//! the MemEnv-only tests (card A-1a Work 6). The trait's contract is
//! the crate doc of `nucleus-env`; PLAN-v3 §3.2 and S-STORE §2, §12.5
//! and §14.1 are the normative sources.

#![allow(missing_docs)]

mod common;

use nucleus_env::{Env, MemEnv, MemFile};
use std::time::Duration;

#[test]
fn mem_open_modes() {
    let env = MemEnv::new(1);
    assert_eq!(common::open_modes(&env), Ok(()));
}

#[test]
fn mem_read_write_roundtrip() {
    let env = MemEnv::new(1);
    assert_eq!(common::read_write_roundtrip(&env), Ok(()));
}

#[test]
fn mem_read_handle_refuses_writes() {
    let env = MemEnv::new(1);
    assert_eq!(common::read_handle_refuses_writes(&env), Ok(()));
}

#[test]
fn mem_namespace_ops() {
    let env = MemEnv::new(1);
    assert_eq!(common::namespace_ops(&env), Ok(()));
}

#[test]
fn mem_invalid_paths_rejected() {
    let env = MemEnv::new(1);
    assert_eq!(common::invalid_paths_rejected(&env), Ok(()));
}

#[test]
fn mem_sync_dir_root_and_subdir() {
    let env = MemEnv::new(1);
    assert_eq!(common::sync_dir_root_and_subdir(&env), Ok(()));
}

#[test]
fn mem_locks_exclusive_excludes_all() {
    let env = MemEnv::new(1);
    assert_eq!(common::locks_exclusive_excludes_all(&env), Ok(()));
}

#[test]
fn mem_locks_shared_readers_coexist() {
    let env = MemEnv::new(1);
    assert_eq!(common::locks_shared_readers_coexist(&env), Ok(()));
}

#[test]
fn mem_locks_shared_excludes_writer() {
    let env = MemEnv::new(1);
    assert_eq!(common::locks_shared_excludes_writer(&env), Ok(()));
}

#[test]
fn mem_lock_released_on_unlock_and_drop() {
    let env = MemEnv::new(1);
    assert_eq!(common::lock_released_on_unlock_and_drop(&env), Ok(()));
}

#[test]
fn mem_lock_creates_no_file() {
    let env = MemEnv::new(1);
    assert_eq!(common::lock_creates_no_file(&env), Ok(()));
}

#[test]
fn mem_clock_and_rand() {
    let env = MemEnv::new(1);
    assert_eq!(common::clock_and_rand(&env), Ok(()));
}

/// MemEnv-only: the same seed gives the same first 8 `rand` values,
/// and seeds 1 and 2 differ (E-CLOCK/RAND; card A-1a Work 6).
#[test]
fn mem_rand_is_seeded() {
    let draw = |seed: u64| {
        (0..8)
            .map(|_| MemEnv::new(seed).rand())
            .collect::<Vec<u64>>()
    };
    let a = draw(1);
    let b = draw(1);
    let c = draw(2);
    assert_eq!(a, b, "the same seed must give the same sequence");
    assert_ne!(a, c, "seeds 1 and 2 must differ");
}

/// MemEnv-only: the clock is virtual. `sleep(3600 s)` returns at once
/// and `now` grows by exactly 3600 s (card A-1a Work 6).
#[test]
fn mem_clock_is_virtual() {
    let env = MemEnv::new(7);
    let t0 = env.now();
    env.sleep(Duration::from_secs(3600));
    let t1 = env.now();
    assert_eq!(t1.checked_sub(t0), Some(Duration::from_secs(3600)));
}

/// All backends are `Send + Sync`, so engine code can share one across
/// threads (card A-1a Work 2; test `backends_are_send_sync`).
#[test]
fn backends_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MemEnv>();
    assert_send_sync::<MemFile>();
}
