//! Pool sizing (S-STORE §5.4 "Minimum size" and "Below the minimum";
//! R-1-memory §4.1–§4.5 and §8 "The report"; card A-3b-0, Work 1): the least
//! frame count a pool may have, and the numbers an open derives from its frame
//! count, as pure functions of the configuration. Nothing here allocates,
//! locks or reads a file.
//!
//! Every number is a count of frames inside `F` (R-1-memory §2.2 item 5):
//! `R_EPOCH`, `R_MAP` and the per-thread terms are not memory of their own,
//! and the pool neither grows nor gives memory back.
//!
//! Invariants owned here:
//! - **The minimum** (S-STORE §5.4; R-1-memory §4.2): `F_min = threads × 3 ×
//!   (H_min + 2) + R_EPOCH_min + R_MAP`, in R-1-memory §4.2's closed form
//!   `4 × n × (H_min + 2) + 2 × (n + s + 1)`, with `n = n_c + n_s`. It counts
//!   the three per-thread terms (the path, the reservation's pages, the
//!   reservation's MapLeafs), the epoch-copy reserve and the MapLeafs §4.1's
//!   pins hold. Tests `t_pool_minimum`, `pool_sizing_table`,
//!   `pool_sizing_worked_rows`; mutants POOL-MIN-PATH-ONLY, MIN-RMAP-DENSE,
//!   MIN-RESERVE-NO-STEP2, NS-IGNORED, MIN-BOUNDARY, CALLERS-ZERO.
//! - **Below it, refused** (S-STORE §5.4: "the open is refused with 53200"):
//!   [`PoolSizing::compute`] answers [`StoreError::BelowMinimum`] for every
//!   `frames < minimum`, and [`super::Pool::new`] allocates nothing before it
//!   is asked. Tests `t_pool_minimum`, `pool_sizing_table`; mutants
//!   POOL-MIN-PATH-ONLY, MIN-RMAP-DENSE, MIN-RESERVE-NO-STEP2, MIN-BOUNDARY.
//! - **Above it, the height bound holds** (R-1-memory §4.5): with
//!   `R_EPOCH = R_EPOCH_min + ⌊(F − F_min) / 32⌋`, `H = ⌊(F − 2 × R_EPOCH) /
//!   (4 × n)⌋ − 2` is at least `H_min` for every `F ≥ F_min`, and
//!   `3n(H + 2) + R_EPOCH + R_MAP ≤ F` with `R_MAP = n(H + 2) + R_EPOCH`.
//!   Tests `pool_sizing_table`, `pool_sizing_worked_rows`; mutants
//!   H-NO-MINUS-TWO, EPOCH-SLOPE-16.
//! - **`o_frame` bounds a frame's cost besides its page** (R-1-memory §4.1,
//!   §12): for every `F`, `F × o_frame` is at least the frame headers plus the
//!   page table, and `o_frame` depends on neither `F` nor the page size.
//!   Test `o_frame_bound`; mutant O-FRAME-NO-TABLE.
//! - **No panic on any input**: every sum is checked, and an overflow is
//!   [`StoreError::Invalid`]. Test `pool_sizing_refusals`.

use core::mem::size_of;

use super::frame::{FrameId, FrameLatch};
use super::table::FrameMeta;
use crate::store::StoreError;

/// The height floor `H_min` (R-1-memory §4.5): an open whose height bound
/// would be below it is below the minimum (S-STORE §5.4).
pub const H_MIN: u32 = 5;

/// `n_s`: the store's own threads that pin frames (its checkpointer, evictors
/// and job threads; R-1-memory §1, §4.2, §8 "The report"). None exists at this
/// card, and there are none in N10's cooperative mode; the card that adds one
/// raises this. [`PoolSizing::compute`] takes it as an argument all the same,
/// so a test can add threads as S-STORE §5.4's `t_pool_minimum` asks.
pub const OWN_PINNING_THREADS: u32 = 0;

/// `s`: the reserve frames S-STORE §13 step 2 holds at once (R-1-memory §4.3,
/// §12), as the build reports it. Provisionally 1, `max(w, 1)` for a data
/// phase that writes one page at a time; card A-3c, which builds step 2's data
/// phase, sets it.
pub const STEP2_RESERVE_FRAMES: u32 = 1;

/// The image-record list's record, in bytes (R-1-memory §4.1, §12): a frame's
/// share of the list in `o_frame`. Provisionally 24 bytes; card A-3c replaces
/// it with its record's size.
pub const IMAGE_RECORD_BYTES: u32 = 24;

/// How many frames above the minimum add one frame to the epoch-copy reserve:
/// `R_EPOCH = R_EPOCH_min + ⌊(F − F_min) / 32⌋` (R-1-memory §4.3; the slope
/// is provisional, A-7e measures it).
const R_EPOCH_SLOPE: u64 = 32;

/// A pool's numbers (R-1-memory §4.1–§4.5, §8 "The report"; card A-3b-0,
/// Work 1), each a count of frames inside `F`. [`super::Pool::sizing`] reports
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSizing {
    /// `F`: the frame count (S-STORE §5.1).
    pub frames: u32,
    /// `n_c`: the callers' count the open takes (R-1-memory §1, §11).
    pub callers: u32,
    /// `n_s`: the store's own pinning threads ([`OWN_PINNING_THREADS`]).
    pub own_threads: u32,
    /// `n = n_c + n_s`: S-STORE §5.4's `threads` (R-1-memory §1).
    pub threads: u32,
    /// `s`: the reserve frames step 2 holds at once
    /// ([`STEP2_RESERVE_FRAMES`]).
    pub step2_frames: u32,
    /// `H_min`: the height floor ([`H_MIN`]).
    pub height_floor: u32,
    /// `F_min`: the least frame count a pool may have (R-1-memory §4.2).
    pub minimum: u64,
    /// `R_EPOCH`: the epoch-copy reserve (R-1-memory §4.3).
    pub r_epoch: u64,
    /// `H`: the open's height bound (R-1-memory §4.5).
    pub height: u32,
    /// `R_MAP`: the MapLeafs §4.1's pins hold and no frame acquisition
    /// releases (R-1-memory §4.4).
    pub r_map: u64,
    /// `o_frame`: the bytes each frame costs besides its page (R-1-memory
    /// §4.1, §12).
    pub o_frame: u32,
}

/// The sums S-STORE §5.4's minimum rests on (R-1-memory §4.2): the thread
/// count, the least reserve, the minimum and `o_frame`.
struct Base {
    threads: u64,
    r_epoch_min: u64,
    minimum: u64,
    o_frame: u32,
}

/// A sum past `u64`: no configuration reaches it, so it is an engine
/// invariant violation.
fn past_u64() -> StoreError {
    StoreError::Invalid("pool sizing: a sum passed u64".into())
}

/// A `usize` size as a `u32` term of `o_frame`.
fn term(n: usize) -> Result<u32, StoreError> {
    u32::try_from(n).map_err(|_| past_u64())
}

/// S1, S2 and S7 (card A-3b-0, Work 1): the refusals, then `n`, `R_EPOCH_min`,
/// `F_min` and `o_frame`, each in checked arithmetic.
fn base(
    callers: u32,
    own_threads: u32,
    step2_frames: u32,
    height_floor: u32,
) -> Result<Base, StoreError> {
    // S1.
    if callers == 0 || step2_frames == 0 || height_floor == 0 {
        return Err(StoreError::Invalid(format!(
            "pool sizing: callers {callers}, step-2 reserve frames {step2_frames} and the height \
             floor {height_floor} must each be at least 1"
        )));
    }
    let past = past_u64;
    // S2.
    let threads = u64::from(callers)
        .checked_add(u64::from(own_threads))
        .ok_or_else(past)?;
    let r_epoch_min = threads
        .checked_add(u64::from(step2_frames))
        .and_then(|r| r.checked_add(1))
        .ok_or_else(past)?;
    let minimum = u64::from(height_floor)
        .checked_add(2)
        .and_then(|h| threads.checked_mul(h))
        .and_then(|t| t.checked_mul(4))
        .and_then(|m| r_epoch_min.checked_mul(2).and_then(|r| m.checked_add(r)))
        .ok_or_else(past)?;
    // S7: a frame's header (its latch with its content, and its metadata), its
    // page-table share (a bucket count below `2F`, since the buckets are
    // `F.next_power_of_two()`, and its free-stack slot) and its image-record
    // list record.
    let bucket = term(size_of::<Option<FrameId>>())?;
    let id = term(size_of::<FrameId>())?;
    let o_frame = term(size_of::<FrameLatch>())?
        .checked_add(term(size_of::<FrameMeta>())?)
        .and_then(|o| o.checked_add(bucket))
        .and_then(|o| o.checked_add(bucket))
        .and_then(|o| o.checked_add(id))
        .and_then(|o| o.checked_add(IMAGE_RECORD_BYTES))
        .ok_or_else(past)?;
    Ok(Base {
        threads,
        r_epoch_min,
        minimum,
        o_frame,
    })
}

/// S4–S6 (card A-3b-0, Work 1): the numbers of a pool at or above its
/// minimum, from the configuration and its [`Base`].
fn sized(
    frames: u32,
    callers: u32,
    own_threads: u32,
    step2_frames: u32,
    height_floor: u32,
    b: &Base,
) -> Result<PoolSizing, StoreError> {
    let past = past_u64;
    let f = u64::from(frames);
    // S4.
    let r_epoch = f
        .checked_sub(b.minimum)
        .and_then(|d| d.checked_div(R_EPOCH_SLOPE))
        .and_then(|d| d.checked_add(b.r_epoch_min))
        .ok_or_else(past)?;
    // S5: `H = ⌊(F − 2 × R_EPOCH) / (4 × n)⌋ − 2`, which is at least the
    // height floor for every `F ≥ F_min` (R-1-memory §4.5). Anything else is
    // an engine invariant violation (XX000 once card A-3c maps errors).
    let height = r_epoch
        .checked_mul(2)
        .and_then(|r| f.checked_sub(r))
        .and_then(|n| b.threads.checked_mul(4).and_then(|d| n.checked_div(d)))
        .and_then(|q| q.checked_sub(2))
        .ok_or_else(past)?;
    let height = u32::try_from(height).map_err(|_| past())?;
    if height < height_floor {
        return Err(StoreError::Invalid(format!(
            "pool sizing: {frames} frames give a height bound of {height}, below the floor \
             {height_floor}"
        )));
    }
    // S6.
    let r_map = u64::from(height)
        .checked_add(2)
        .and_then(|h| h.checked_mul(b.threads))
        .and_then(|m| m.checked_add(r_epoch))
        .ok_or_else(past)?;
    Ok(PoolSizing {
        frames,
        callers,
        own_threads,
        threads: u32::try_from(b.threads).map_err(|_| past())?,
        step2_frames,
        height_floor,
        minimum: b.minimum,
        r_epoch,
        height,
        r_map,
        o_frame: b.o_frame,
    })
}

impl PoolSizing {
    /// The numbers of a pool of `frames` frames for `callers` calling threads
    /// (`n_c`), `own_threads` of the store's own (`n_s`), a step 2 that holds
    /// `step2_frames` reserve frames at once (`s`) and the height floor
    /// `height_floor` (`H_min`): S-STORE §5.4's minimum with R-1-memory §4.2's
    /// values, then `R_EPOCH`, `H` and `R_MAP` as R-1-memory §4.3–§4.5 derive
    /// them (card A-3b-0, Work 1, S1–S7). Allocates nothing.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if a count is 0 or a sum passes `u64`;
    /// [`StoreError::BelowMinimum`] (53200) if `frames` is below the minimum.
    pub fn compute(
        frames: u32,
        callers: u32,
        own_threads: u32,
        step2_frames: u32,
        height_floor: u32,
    ) -> Result<PoolSizing, StoreError> {
        let b = base(callers, own_threads, step2_frames, height_floor)?;
        // S3.
        if u64::from(frames) < b.minimum {
            return Err(StoreError::BelowMinimum {
                frames,
                minimum: b.minimum,
            });
        }
        sized(frames, callers, own_threads, step2_frames, height_floor, &b)
    }

    /// [`PoolSizing::compute`] for a pool below its minimum (card A-3b-0,
    /// Work 1): the refusals, the minimum and `o_frame` as there, and, when
    /// `frames` is at or above the minimum, the same numbers. Below it,
    /// `r_epoch` is `R_EPOCH_min`, `height` is 0 and `r_map` is `2n + R_EPOCH_min`.
    ///
    /// Those three mean nothing below the minimum. Only
    /// [`super::Pool::new_below_minimum_for_tests`] uses this, so the crate's
    /// own tests can run the pin protocol and the clock on pools of a few
    /// frames; no store path calls it, so without `test-hooks` nothing here
    /// uses it.
    #[cfg(feature = "test-hooks")]
    pub(crate) fn below_minimum(
        frames: u32,
        callers: u32,
        own_threads: u32,
        step2_frames: u32,
        height_floor: u32,
    ) -> Result<PoolSizing, StoreError> {
        let b = base(callers, own_threads, step2_frames, height_floor)?;
        if u64::from(frames) >= b.minimum {
            return sized(frames, callers, own_threads, step2_frames, height_floor, &b);
        }
        let r_map = b
            .threads
            .checked_mul(2)
            .and_then(|t| t.checked_add(b.r_epoch_min))
            .ok_or_else(past_u64)?;
        Ok(PoolSizing {
            frames,
            callers,
            own_threads,
            threads: u32::try_from(b.threads).map_err(|_| past_u64())?,
            step2_frames,
            height_floor,
            minimum: b.minimum,
            r_epoch: b.r_epoch_min,
            height: 0,
            r_map,
            o_frame: b.o_frame,
        })
    }
}
