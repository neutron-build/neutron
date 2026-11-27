//! The frame latch's cost (card A-3b, Work 10; S-STORE §5.3: "A-3b pins `K`,
//! and measures the latch's cost against PLAN-v3 §3.12's buffer-pool
//! trigger").
//!
//! Not engine code (row A-1a-B-F3): the one file of this card that reads a
//! clock. Build and run once:
//!
//! ```text
//! cargo run --release -q -p nucleus-store --example latch_cost
//! ```
//!
//! One thread; a pool from `Pool::new` at its minimum for 1 caller at
//! 8,192-byte pages, with one resident page. Each measure is the median of 3
//! runs of 1,000,000 iterations, in nanoseconds per iteration:
//!
//! - `pool_shared`: `pin_resident` + `latch_shared` + drop + drop;
//! - `pool_exclusive`: the same with `latch_exclusive`;
//! - `latch_shared`: `Latch::<StdPrimitives>` shared + release;
//! - `std_rwlock_read`: `std::sync::RwLock::read` + drop, the latch card
//!   A-3a used.
//!
//! Then `estimate h=3 <x>% h=4 <y>%`: `h × pool_shared / 1182.42` as a
//! percentage, where 1182.42 ns is E0's measured point get
//! (`spikes/e0-btree/README.md`, W-point (i) mem), and a point get visits
//! `h` pages, each one lookup and one latch. The trigger is "buffer-pool
//! latch and lookup > 20% of point-op CPU" (PLAN-v3 §3.12); whether it is
//! met is E0's and E7's decision, not this example's.

use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use nucleus_env::{Env, MemEnv, OpenMode};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, H_MIN, OWN_PINNING_THREADS, PageKey, Pool, PoolConfig, STEP2_RESERVE_FRAMES,
};
use nucleus_store::latch::{Latch, Mode};
use nucleus_store::page::Kind;
use nucleus_store::sync::StdPrimitives;

/// Iterations in one run of a measure.
const ITERATIONS: u64 = 1_000_000;

/// Runs of a measure; the median is printed.
const RUNS: usize = 3;

/// E0's measured point get, in nanoseconds (W-point (i) mem).
const E0_POINT_GET_NS: f64 = 1182.42;

/// The tree heights the estimate is printed for.
const HEIGHTS: [u64; 2] = [3, 4];

/// S-STORE §5.4's minimum frame count for `callers` callers with R-1's
/// values (the tests' formula).
fn minimum_frames(callers: u32) -> u32 {
    let n = u64::from(callers) + u64::from(OWN_PINNING_THREADS);
    let per_thread = u64::from(H_MIN) + 2;
    let r_epoch = n + u64::from(STEP2_RESERVE_FRAMES) + 1;
    let r_map = n * per_thread + r_epoch;
    u32::try_from(n * 3 * per_thread + r_epoch + r_map).unwrap_or(u32::MAX)
}

/// One run of a measure: nanoseconds per iteration of `body`.
fn measure(mut body: impl FnMut() -> Result<(), StoreError>) -> Result<f64, StoreError> {
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        body()?;
    }
    Ok(start.elapsed().as_nanos() as f64 / ITERATIONS as f64)
}

/// The median of the runs.
fn median(mut runs: [f64; RUNS]) -> f64 {
    runs.sort_by(|a, b| a.total_cmp(b));
    runs[RUNS / 2]
}

/// A measure: the median ns per iteration over [`RUNS`] runs.
fn measured(
    name: &str,
    mut body: impl FnMut() -> Result<(), StoreError>,
) -> Result<f64, Box<dyn Error>> {
    let mut runs = [0.0; RUNS];
    for run in &mut runs {
        *run = measure(&mut body)?;
    }
    let ns = median(runs);
    println!("{name} {ns:.2} ns/iteration");
    Ok(ns)
}

fn main() -> Result<(), Box<dyn Error>> {
    let env = MemEnv::new(1);
    let file = env.open("data", OpenMode::CreateNew)?;
    let pool = Pool::new(
        Arc::new(file),
        PoolConfig {
            frames: minimum_frames(1),
            page_size: 8192,
            callers: 1,
        },
    )?;
    let x = PageKey::Lpid(1);
    // Tree X's root, resident: reserve, create, release.
    let res = pool.reserve()?;
    let created = match pool.create(res, x, Kind::Leaf, 1)? {
        CreateOutcome::Created(page) => page,
        CreateOutcome::Exists(..) | CreateOutcome::Loading(..) => {
            return Err("X's root already has a frame in a fresh pool".into());
        }
    };
    let _root = created.release();

    let pool_shared = measured("pool_shared", || {
        let pin = pool
            .pin_resident(x)?
            .ok_or_else(|| StoreError::Invalid("X's root is not resident".into()))?;
        let guard = pin.latch_shared()?;
        black_box(&*guard);
        drop(guard);
        drop(pin);
        Ok(())
    })?;
    let _pool_exclusive = measured("pool_exclusive", || {
        let pin = pool
            .pin_resident(x)?
            .ok_or_else(|| StoreError::Invalid("X's root is not resident".into()))?;
        let guard = pin.latch_exclusive()?;
        black_box(&*guard);
        drop(guard);
        drop(pin);
        Ok(())
    })?;
    let latch: Latch<StdPrimitives> = Latch::new(1, 1)?;
    let _latch_shared = measured("latch_shared", || {
        let guard = latch.latch(Mode::Shared)?;
        black_box(guard.stamps());
        guard.release()?;
        Ok(())
    })?;
    let rwlock: std::sync::RwLock<u64> = std::sync::RwLock::new(0);
    let _std_rwlock_read = measured("std_rwlock_read", || {
        let guard = rwlock
            .read()
            .map_err(|_| StoreError::Poisoned("the example's RwLock".into()))?;
        black_box(&*guard);
        drop(guard);
        Ok(())
    })?;

    let estimates: Vec<String> = HEIGHTS
        .iter()
        .map(|h| {
            format!(
                "h={h} {:.1}%",
                (*h as f64) * pool_shared / E0_POINT_GET_NS * 100.0
            )
        })
        .collect();
    println!("estimate {}", estimates.join(" "));
    Ok(())
}
