#![allow(dead_code)]
// Why: each of the five test binaries (wb_unit, wb_slots, wb_race, wb_floor
// and wb_harness) declares `mod wb_support;` and uses a subset of it, so the
// rest would warn as dead code in each (the precedent is
// tests/buf_support/mod.rs).

//! Shared fixtures for card A-3c's tests of `buf`'s write-back (Work 13):
//! constants, the pool fixture, S-STORE §15's stand-ins S1 and S3 (the flip
//! and the callers A-7c, A-4a and A-4b will replace), an independent oracle
//! for slots and homes, a pauser for the interleaving harness, and the lock
//! order tracker.
//!
//! Every wait here is on an event a hook or a thread sends; every bound is a
//! count. Nothing reads a clock. Outside `#[test]` functions the helpers
//! return `Result` and never unwrap.

pub mod drive;
pub mod file;
pub mod model;
pub mod oracle;
pub mod track;

use std::sync::Arc;

use nucleus_env::File;
use nucleus_store::StoreError;
use nucleus_store::buf::{Hold, Pool, PoolConfig, WriteBackConfig};
use nucleus_store::sync::Primitives;

pub use file::WbFile;

/// The page size of every fixture.
pub const PS: u32 = 4096;

/// The byte a test writes as a page's marker; the pool never decodes a body.
pub const MARK: usize = 100;

/// The reserved slots of the replica region at `PS` (S-STORE §2.4: at 4 KiB
/// pages, slots 256 and 257).
pub const RESERVED: [u64; 2] = [256, 257];

/// The height floor the fixtures state, `H_min` lowered to 1 (R-1-memory
/// §4.2: "A test build may lower `H_min`"), which gives `F` = 18, 32 and 46
/// for `n` = 1, 2 and 3.
pub const H_MIN_TEST: u32 = 1;

/// `F_min` as the test computes it from S-STORE §5.4 with R-1-memory §4.2's
/// values, not asked of the build: `F_min = 4n(H_min + 2) + 2(n + s + 1)`
/// with `n_s = 0` and `s = 1`, so `2(n + s + 1) = 2(n + 2)`.
pub fn f_min(n: u32, h_min: u32) -> u32 {
    let per_thread = h_min + 2;
    4 * n * per_thread + 2 * (n + 2)
}

/// `F_min` for `n` = 1, 2 and 3 at the lowered floor: 18, 32 and 46.
pub fn min_f(n: u32) -> u32 {
    f_min(n, H_MIN_TEST)
}

/// The callers' count a fixture of `frames` at height floor `h_min` is for:
/// the `n` its `F_min` names, or `None` if no `n` does.
pub fn callers_for(frames: u32, h_min: u32) -> Option<u32> {
    let per_n = 4 * (h_min + 2) + 2;
    frames
        .checked_sub(4)
        .and_then(|rest| rest.checked_div(per_n))
        .filter(|n| f_min(*n, h_min) == frames)
}

/// An error as text.
pub fn said<E: std::fmt::Debug>(e: E) -> String {
    format!("{e:?}")
}

/// The pool fixture (card A-3c, Work 13.2): `frames` frames at `PS`, fanout
/// `fanout`, a file of `slot_count` slots, epoch `epoch`, on the backend
/// `P`, with `minimum` the height floor the test states. The callers' count
/// is the `n` whose `F_min` (R-1-memory §4.2, with `n_s = 0` and `s = 1`)
/// equals `frames` at that floor. A pool at least the build's own minimum
/// (at `H_MIN` = 5) opens through `Pool::with_writeback`, with its minimum
/// check; one below it, as the fixtures at the lowered floor are, opens
/// through the `test-hooks` twin that waives the check, since the build's
/// check takes no other floor.
pub fn wb_pool<P: Primitives>(
    frames: u32,
    fanout: u64,
    slot_count: u64,
    epoch: u32,
    minimum: u32,
) -> Result<(Arc<WbFile>, Pool<WbFile, P>), String> {
    let file = Arc::new(WbFile::new().map_err(said)?);
    file.set_len(slot_count * u64::from(PS)).map_err(said)?;
    wb_pool_on(
        Arc::clone(&file),
        frames,
        fanout,
        slot_count,
        epoch,
        minimum,
    )
    .map(|pool| (file, pool))
}

/// [`wb_pool`] over a file the caller already made: one file shared by the
/// fixtures of an exhaustive table, whose length is the only thing the pool
/// checks about it, since every page a pool reads was written by that pool,
/// to its lowest free slot, over whatever an earlier fixture left there.
pub fn wb_pool_on<P: Primitives>(
    file: Arc<WbFile>,
    frames: u32,
    fanout: u64,
    slot_count: u64,
    epoch: u32,
    minimum: u32,
) -> Result<Pool<WbFile, P>, String> {
    let n = callers_for(frames, minimum)
        .ok_or_else(|| format!("{frames} frames are no F_min at height floor {minimum}"))?;
    if f_min(n, minimum) != frames {
        return Err(format!(
            "{frames} is not F_min for {n} callers at floor {minimum}"
        ));
    }
    let cfg = PoolConfig {
        frames,
        page_size: PS,
        callers: n,
    };
    let wb = WriteBackConfig {
        epoch,
        ..WriteBackConfig::fresh(PS, fanout, slot_count)
    };
    let build_minimum = f_min(n, nucleus_store::buf::H_MIN);
    let pool = if frames >= build_minimum {
        Pool::<WbFile, P>::with_writeback(Arc::clone(&file), cfg, wb)
    } else {
        Pool::<WbFile, P>::with_writeback_below_minimum_for_tests(Arc::clone(&file), cfg, wb)
    }
    .map_err(said)?;
    Ok(pool)
}

/// Pin `key` through the pool's own map under `hold`, which must be mapped:
/// the `Pin`, or an error naming an unmapped key.
pub fn pin<'p, P: Primitives>(
    pool: &'p Pool<WbFile, P>,
    key: nucleus_store::buf::PageKey,
    hold: &Hold<'_, '_, WbFile, P>,
) -> Result<nucleus_store::buf::Pin<'p, WbFile, P>, StoreError> {
    match pool.pin_page(key, hold)? {
        nucleus_store::buf::PinOutcome::Pinned(p) => Ok(p),
        nucleus_store::buf::PinOutcome::Unmapped => Err(StoreError::Invalid(format!(
            "{key:?} is unmapped where the test expected a page"
        ))),
    }
}

/// Pin `key` holding nothing, read its marker under the shared latch and
/// drop the pin.
pub fn pin_read<P: Primitives>(
    pool: &Pool<WbFile, P>,
    key: nucleus_store::buf::PageKey,
) -> Result<u8, StoreError> {
    let p = pin(pool, key, &Hold::nothing())?;
    let g = p.latch_shared()?;
    let m = g
        .get(MARK)
        .copied()
        .ok_or_else(|| StoreError::Invalid("a page shorter than the marker's byte".to_string()))?;
    drop(g);
    drop(p);
    Ok(m)
}

/// The frame holding `key`, or an error when none does.
pub fn frame_of<P: Primitives>(
    pool: &Pool<WbFile, P>,
    key: nucleus_store::buf::PageKey,
) -> Result<nucleus_store::buf::FrameId, StoreError> {
    pool.frame_of(key)?.ok_or_else(|| {
        StoreError::Invalid(format!(
            "no frame holds {key:?} where the test expected one"
        ))
    })
}
