//! Card A-3b-0's tests of pool sizing and the minimum at open (Work 12;
//! S-STORE §5.4 "Minimum size" and "Below the minimum", §15.1's
//! `t_pool_minimum` row; R-1-memory §4.1–§4.6, §8 and §12): the minimum
//! computed here from the spec and never asked of the build, the open one
//! frame below it refused with 53200's error before any write or read,
//! R-1-memory's worked rows, the sizing over a table of thread counts, the
//! refusals, and the bound `o_frame` puts on a frame's cost besides its page.
//!
//! No thread and no hook.

mod buf_support;

use std::sync::Arc;

use buf_support::{FaultFile, minimum_frames};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    H_MIN, IMAGE_RECORD_BYTES, OWN_PINNING_THREADS, Pool, PoolConfig, PoolSizing,
    STEP2_RESERVE_FRAMES,
};

/// S-STORE §5.4's minimum with R-1-memory's values, as a test computes it:
/// `(F_min, R_EPOCH_min, R_MAP)` for `n` threads and a step 2 that holds `s`
/// reserve frames. Written from the spec (R-1-memory §4.2–§4.4), not from the
/// build's sizing.
fn spec_minimum(n: u64, s: u64) -> (u64, u64, u64) {
    let per_thread = u64::from(H_MIN) + 2;
    let r_epoch = n + s + 1;
    let r_map = n * per_thread + r_epoch;
    (n * 3 * per_thread + r_epoch + r_map, r_epoch, r_map)
}

/// The test's own page file, which must see no write and no read.
fn file() -> Result<Arc<FaultFile>, nucleus_env::EnvError> {
    Ok(Arc::new(FaultFile::new()?))
}

/// S-STORE §15.1's `t_pool_minimum` row at A-3b-0 (I-POOL-BOUND, full; §5.4
/// "Below the minimum"): for one, two and eight callers' threads, with the
/// build's own pinning threads added, at every allowed page size and the
/// height floor, a pool one frame below the minimum is refused with 53200's
/// error and writes nothing, and a pool at it is accepted. The minimum is
/// computed from the formula, not asked of the build. Mutants
/// POOL-MIN-PATH-ONLY, MIN-RMAP-DENSE and MIN-RESERVE-NO-STEP2 accept the
/// pool one frame below.
#[test]
fn t_pool_minimum() {
    // The build's own values, as R-1-memory §12 reports them provisionally.
    assert_eq!(
        (OWN_PINNING_THREADS, STEP2_RESERVE_FRAMES, H_MIN),
        (0, 1, 5)
    );
    for (callers, f_min) in [(1u32, 34u32), (2, 64), (8, 244)] {
        let n = u64::from(callers) + u64::from(OWN_PINNING_THREADS);
        let s = u64::from(STEP2_RESERVE_FRAMES);
        // `F_min`, `R_EPOCH` and `R_MAP`, from the formula.
        let (spec_min, spec_epoch, spec_map) = spec_minimum(n, s);
        assert_eq!(spec_min, u64::from(f_min), "{callers} callers");
        assert_eq!(u64::from(minimum_frames(callers)), spec_min);
        for page_size in [4096u32, 8192, 16384, 32768, 65536] {
            let what = format!("{callers} callers, {page_size}-byte pages");

            // One frame below the minimum: refused, and the file is untouched.
            let below = f_min - 1;
            let file = file().unwrap();
            let opened = Pool::new(
                Arc::clone(&file),
                PoolConfig {
                    frames: below,
                    page_size,
                    callers,
                },
            );
            match opened {
                Err(e @ StoreError::BelowMinimum { frames, minimum })
                    if frames == below && minimum == spec_min =>
                {
                    assert!(e.to_string().contains("below its minimum"), "{what}: {e}");
                }
                other => panic!("{what}: a pool of {below} frames opened as {other:?}"),
            }
            assert_eq!((file.writes(), file.reads()), (0, 0), "{what}");

            // At the minimum: accepted, with nothing read or written, and
            // the report (R-1-memory §8) says the spec's numbers: the height
            // is at its floor.
            let pool = Pool::new(
                Arc::clone(&file),
                PoolConfig {
                    frames: f_min,
                    page_size,
                    callers,
                },
            )
            .unwrap_or_else(|e| panic!("{what}: a pool of {f_min} frames: {e:?}"));
            let PoolSizing {
                frames,
                callers: reported_callers,
                own_threads,
                threads,
                step2_frames,
                height_floor,
                minimum,
                r_epoch,
                height,
                r_map,
                // Named, not compared here: the test cannot see the private
                // types it sums; `o_frame_bound` checks it.
                o_frame: _,
            } = pool.sizing();
            let got = (
                u64::from(frames),
                u64::from(reported_callers),
                u64::from(own_threads),
                u64::from(threads),
                u64::from(step2_frames),
                u64::from(height_floor),
                minimum,
                r_epoch,
                u64::from(height),
                r_map,
            );
            let want = (
                u64::from(f_min),
                u64::from(callers),
                u64::from(OWN_PINNING_THREADS),
                n,
                s,
                u64::from(H_MIN),
                spec_min,
                spec_epoch,
                u64::from(H_MIN),
                spec_map,
            );
            assert_eq!(got, want, "{what}: the report");
            assert_eq!((pool.frames(), pool.page_size()), (f_min, page_size));
            assert_eq!((file.writes(), file.reads()), (0, 0), "{what}");
        }
    }
}

/// R-1-memory §4.6's three rows through `PoolSizing::compute`, against
/// numbers written from that section, an oracle the code did not produce.
/// `compute` allocates nothing, so the server row costs nothing. Mutants
/// H-NO-MINUS-TWO and EPOCH-SLOPE-16 (the embedded row) fail here.
#[test]
fn pool_sizing_worked_rows() {
    for (frames, callers, minimum, r_epoch, height, r_map) in [
        (64u32, 2u32, 64u64, 4u64, 5u32, 18u64),
        (5878, 8, 244, 186, 170, 1562),
        (916_452, 20, 604, 28_642, 10_737, 243_422),
    ] {
        let s = PoolSizing::compute(frames, callers, 0, 1, 5)
            .unwrap_or_else(|e| panic!("{frames} frames, {callers} callers: {e:?}"));
        let got = (s.frames, s.minimum, s.r_epoch, s.height, s.r_map);
        assert_eq!(
            got,
            (frames, minimum, r_epoch, height, r_map),
            "{frames} frames, {callers} callers"
        );
        // Each row satisfies the minimum's terms (§4.6).
        let n = u64::from(callers);
        let fits = n * 3 * (u64::from(height) + 2) + r_epoch + r_map;
        assert!(fits <= u64::from(frames), "{frames} frames: {fits}");
    }
}

/// The sizing over a table of thread counts: for one to four callers, none
/// to two of the store's own threads and a step 2 holding one to three
/// reserve frames, from two frames below the minimum to seventy above it
/// (two of `R_EPOCH`'s `1/32` steps for the smallest thread count), `compute`
/// agrees with a computation written here from R-1-memory §4.3–§4.5. Below
/// the minimum it gives `BelowMinimum`; at and above, the height is at least
/// its floor and the minimum's terms fit. Mutant NS-IGNORED fails the rows
/// with own threads.
#[test]
fn pool_sizing_table() {
    for callers in 1u32..=4 {
        for own in [0u32, 1, 2] {
            for s in [1u32, 2, 3] {
                let n = u64::from(callers + own);
                let (f_min, r_epoch_min, _) = spec_minimum(n, u64::from(s));
                for f in (f_min - 2)..=(f_min + 70) {
                    let what = format!("{callers} callers, {own} own threads, s {s}, {f} frames");
                    let frames = u32::try_from(f).unwrap();
                    let got = PoolSizing::compute(frames, callers, own, s, H_MIN);
                    if f < f_min {
                        match got {
                            Err(StoreError::BelowMinimum { frames, minimum }) => {
                                assert_eq!((u64::from(frames), minimum), (f, f_min), "{what}");
                            }
                            other => panic!("{what}: {other:?}"),
                        }
                        continue;
                    }
                    let got = got.unwrap_or_else(|e| panic!("{what}: {e:?}"));
                    // R-1-memory §4.3–§4.5.
                    let r_epoch = r_epoch_min + (f - f_min) / 32;
                    let height = (f - 2 * r_epoch) / (4 * n) - 2;
                    let r_map = n * (height + 2) + r_epoch;
                    let had = (
                        u64::from(got.frames),
                        u64::from(got.callers),
                        u64::from(got.own_threads),
                        u64::from(got.threads),
                        u64::from(got.step2_frames),
                        u64::from(got.height_floor),
                        got.minimum,
                        got.r_epoch,
                        u64::from(got.height),
                        got.r_map,
                    );
                    let want = (
                        f,
                        u64::from(callers),
                        u64::from(own),
                        n,
                        u64::from(s),
                        u64::from(H_MIN),
                        f_min,
                        r_epoch,
                        height,
                        r_map,
                    );
                    assert_eq!(had, want, "{what}");
                    assert!(height >= u64::from(H_MIN), "{what}: height {height}");
                    let fits = n * 3 * (height + 2) + r_epoch + r_map;
                    assert!(fits <= f, "{what}: the minimum's terms do not fit");
                }
            }
        }
    }
}

/// The refusals (Work 1's S1 and S2): no callers, no reserve frame for step
/// 2 and no height floor are each `Invalid`, and so is a sum that passes
/// `u64`; none panics. Mutant CALLERS-ZERO accepts no callers.
#[test]
fn pool_sizing_refusals() {
    let invalid = |r: Result<PoolSizing, StoreError>| match r {
        Err(e @ StoreError::Invalid(_)) => e.to_string(),
        other => panic!("expected Invalid, got {other:?}"),
    };
    invalid(PoolSizing::compute(34, 0, 0, 1, 5));
    invalid(PoolSizing::compute(34, 1, 0, 0, 5));
    invalid(PoolSizing::compute(34, 1, 0, 1, 0));
    invalid(PoolSizing::compute(
        u32::MAX,
        u32::MAX,
        u32::MAX,
        u32::MAX,
        u32::MAX,
    ));
}

/// `o_frame` bounds what a frame costs besides its page (R-1-memory §4.1,
/// §12; Work 1's S7): for every frame count, `F × o_frame` is at least the
/// frames' headers and the page table and its list record, and `o_frame`
/// depends on neither the frame count nor the page size. Mutant
/// O-FRAME-NO-TABLE leaves out the page table's share and fails at 1025
/// frames, where the bucket count doubles.
#[test]
fn o_frame_bound() {
    let o = PoolSizing::compute(34, 1, 0, 1, 5)
        .unwrap_or_else(|e| panic!("{e:?}"))
        .o_frame;
    assert!(o > IMAGE_RECORD_BYTES, "o_frame {o}");
    // What a frame costs besides its page and its list record.
    let per_frame = u64::from(o - IMAGE_RECORD_BYTES);
    for page_size in [4096u32, 65536] {
        let cfg = |frames: u32| PoolConfig {
            frames,
            page_size,
            callers: 1,
        };
        // How many bytes `F × o_frame` has to spare over what the pool
        // holds besides its pages and their list records.
        let spare = |frames: u32| -> i128 {
            // No allocation: `bytes_for` is arithmetic.
            let b = Pool::<FaultFile>::bytes_for(cfg(frames)).unwrap();
            assert_eq!(b.page_bytes, u64::from(frames) * u64::from(page_size));
            i128::from(u64::from(frames) * per_frame) - i128::from(b.frame_bytes + b.table_bytes)
        };
        assert!(spare(1) >= 0);
        assert!(spare(5) >= 0);
        assert!(spare(64) >= 0);
        assert!(spare(1000) >= 0);
        assert!(spare(1025) >= 0, "1025 frames");

        // Independent of `F` and of the page size.
        for frames in [1u32, 5, 64] {
            let pool = Pool::new_below_minimum_for_tests(file().unwrap(), cfg(frames)).unwrap();
            assert_eq!(pool.sizing().o_frame, o, "{frames} frames, {page_size}");
        }
        for frames in [1000u32, 1025] {
            assert_eq!(
                PoolSizing::compute(frames, 1, 0, 1, 5).unwrap().o_frame,
                o,
                "{frames} frames, {page_size}"
            );
        }
    }
}
