//! Card A-3c's slot rows of S-STORE §15.1 (Work 14.2), one thread, each with
//! `WbOracle` predicting every allocation: I-NO-INPLACE (`row_i_no_inplace`,
//! with a write hook on evictions), I-FREE-SAFE (`row_i_free_safe`, the
//! pending frees), I-TWO-GEN (`t_two_gen`; S3: step 6's release as A-7c's
//! checkpoint will call it, with an allocation hook) and I-META-COPIES's
//! `t_replica_region` (in memory: allocation and eviction through the region
//! at 4 KiB pages, a hook on every page write).
//!
//! Fixture: `wb_pool::<StdPrimitives>` at `PS`, `n` 1, `F` 18 (computed from
//! R-1-memory §4.2's formula at the lowered floor), `slot_count` 600 or 258,
//! epoch 1. The oracle is the I-NO-INPLACE judge and is told every free,
//! flip and release the pool is.

mod wb_support;

use std::sync::{Arc, Mutex, PoisonError};

use nucleus_env::File;
use nucleus_store::StoreError;
use nucleus_store::buf::{EvictOutcome, FrameId, Hold, PageKey, Pool};
use nucleus_store::sync::StdPrimitives;
use wb_support::drive::{
    evict, evict_if_resident, evict_out, s1_flip, s3_free, s3_new_page, s3_release, s3_touch,
};
use wb_support::file::WbFile;
use wb_support::oracle::WbOracle;
use wb_support::{H_MIN_TEST, PS, min_f, pin, pin_read, wb_pool};

/// The key of an LPID.
fn lpid(x: u64) -> PageKey {
    PageKey::Lpid(x)
}

/// The key of a MapLeaf.
fn mapleaf(i: u64) -> PageKey {
    PageKey::MapLeaf(i)
}

/// A small count as a `usize`, for a helper.
fn idx(x: u64) -> Result<usize, StoreError> {
    usize::try_from(x).map_err(|_| StoreError::Invalid("an index past usize".to_string()))
}

/// A small count as a `u8`, for a helper's marker.
fn byte(x: usize) -> Result<u8, StoreError> {
    u8::try_from(x).map_err(|_| StoreError::Invalid("a marker past u8".to_string()))
}

/// Lock a test mutex, recovering from poison.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The little-endian `u64` at `at` of `b`, 0 past the end.
fn le_u64(b: &[u8], at: usize) -> u64 {
    let mut bytes = [0u8; 8];
    for (k, slot) in bytes.iter_mut().enumerate() {
        *slot = b.get(at + k).copied().unwrap_or(0);
    }
    u64::from_le_bytes(bytes)
}

/// The little-endian `u32` at `at` of `b`, 0 past the end.
fn le_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([
        b.get(at).copied().unwrap_or(0),
        b.get(at + 1).copied().unwrap_or(0),
        b.get(at + 2).copied().unwrap_or(0),
        b.get(at + 3).copied().unwrap_or(0),
    ])
}

/// Install I-NO-INPLACE's write hook (card A-3c, Work 14.2): for every page
/// write, the slot is not 0, 1, 2 or reserved (§4.4: "Slots 0–2 and the
/// reserved slots (§2.4) are never page homes"), it is not the home of any
/// page or map page the oracle holds live, and it equals the oracle's
/// prediction; then the oracle is told the write, with the written header's
/// kind, id and epoch.
fn write_hook(file: &WbFile, oracle: &Arc<Mutex<WbOracle>>) {
    let oracle = Arc::clone(oracle);
    file.set_on_write(Some(Arc::new(move |slot, bytes| {
        let mut o = lock(&oracle);
        if slot < 3 {
            return Err(format!("a page write at the fixed slot {slot}"));
        }
        if slot == 256 || slot == 257 {
            return Err(format!("a page write at the reserved slot {slot}"));
        }
        if o.is_live_home(slot) {
            return Err(format!("a page write at slot {slot}, a live page's home"));
        }
        let want = o.predict_alloc();
        if slot != want {
            return Err(format!(
                "a page write at slot {slot} where the oracle predicts {want}"
            ));
        }
        o.wrote(
            bytes.get(4).copied().unwrap_or(0),
            le_u64(bytes, 8),
            slot,
            le_u32(bytes, 16),
        );
        Ok(())
    })));
}

/// Install the allocation hook (S-STORE §4.4's `t_two_gen` form): every slot
/// `alloc` hands out is the oracle's prediction; anything else is recorded.
fn slot_hook(
    pool: &Pool<WbFile, StdPrimitives>,
    oracle: &Arc<Mutex<WbOracle>>,
) -> Arc<Mutex<Vec<String>>> {
    let oracle = Arc::clone(oracle);
    let violations = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&violations);
    pool.set_slot_hook(Some(Arc::new(move |slot| {
        let o = lock(&oracle);
        let want = o.predict_alloc();
        if slot != want {
            lock(&sink).push(format!(
                "an allocation of slot {slot} where the oracle predicts {want}"
            ));
        }
    })));
    violations
}

/// `row_i_no_inplace` (I-NO-INPLACE, full: "a write hook on evictions"; card
/// A-3c, Work 14.2). `n` 1, `F` 18, fanout 4, `slot_count` 600, epoch 1.
/// Mutant INPLACE-EVICT fails it at the first re-eviction (round 1).
#[test]
fn row_i_no_inplace() {
    assert_eq!(min_f(1), 18);
    let (file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let oracle = Arc::new(Mutex::new(WbOracle::new(PS, 600, 1)));
    write_hook(&file, &oracle);
    // 24 pages, markers 1..24.
    let mut pages = Vec::new();
    for m in 1..=24u8 {
        let (x, p) = s3_new_page(&pool, m).unwrap();
        assert_eq!(x, u64::from(m));
        pages.push(x);
        drop(p);
    }
    assert_eq!(pages, (1..=24).collect::<Vec<u64>>());
    let mut last = [0u8; 25];
    for (x, m) in pages.iter().zip(1..=24u8) {
        last[usize::try_from(*x).unwrap()] = m;
    }
    for round in 1..=6u8 {
        for (k, &x) in pages.iter().enumerate() {
            // A new marker per page and round.
            let m = 0x40 + (round - 1) * 24 + u8::try_from(k + 1).unwrap();
            let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
            s3_touch(&pool, &p, m).unwrap();
            drop(p);
            last[usize::try_from(x).unwrap()] = m;
        }
        for &x in &pages {
            evict_if_resident(&pool, lpid(x)).unwrap();
        }
        if round == 2 || round == 4 {
            for i in 0..=6u64 {
                evict_if_resident(&pool, mapleaf(i)).unwrap();
            }
        }
        if round == 3 {
            for x in [6u64, 12, 18] {
                s3_free(&pool, x, 4).unwrap();
                lock(&oracle).freed(3, x);
            }
            let mut got = Vec::new();
            for m in [0xE1u8, 0xE2, 0xE3] {
                let (x, p) = s3_new_page(&pool, m).unwrap();
                got.push(x);
                last[usize::try_from(x).unwrap()] = m;
                drop(p);
            }
            assert_eq!(got, vec![6, 12, 18], "the freed LPIDs, lowest first");
        }
    }
    assert!(file.violations().is_empty(), "{:?}", file.violations());
    assert_eq!(
        lock(&oracle).slot_count(),
        pool.slot_stats().unwrap().slot_count
    );
    assert!(file.writes().len() > 24, "pages were written");
    for x in 1..=24u64 {
        assert_eq!(
            pin_read(&pool, lpid(x)).unwrap(),
            last[usize::try_from(x).unwrap()],
            "LPID {x}'s last marker"
        );
    }
}

/// What [`free_safe_fixture`] hands back.
struct FreeSafe {
    file: Arc<WbFile>,
    pool: Pool<WbFile, StdPrimitives>,
    oracle: Arc<Mutex<WbOracle>>,
    /// The allocation hook's violations.
    violations: Arc<Mutex<Vec<String>>>,
    /// The eight pages' homes of epoch 1.
    h: [u64; 8],
    /// The three MapLeafs' homes of epoch 1.
    m: [u64; 3],
}

/// The fixture of `row_i_free_safe` and `t_two_gen`: the state at the end of
/// `row_i_free_safe`. Phase 1: pages P1..P8 (LPIDs 1..8, MapLeafs 0..2),
/// each evicted (homes `h1..h8`, entry epoch 1), then MapLeafs 0..2 evicted
/// (homes `m0..m2`). Then `s1_flip`. Phase 2: MapLeafs 0..2 pinned to the
/// end, so that no MapLeaf is evicted after the flip and no MapInner is
/// mutated after it (Work 6.4); three rounds of touch and evict for P1..P8,
/// every allocation checked against the oracle.
fn free_safe_fixture() -> Result<FreeSafe, StoreError> {
    let (file, pool) =
        wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).map_err(StoreError::Invalid)?;
    let oracle = Arc::new(Mutex::new(WbOracle::new(PS, 600, 1)));
    write_hook(&file, &oracle);
    let violations = slot_hook(&pool, &oracle);
    // Phase 1.
    let mut pages = Vec::new();
    for m in 0x11..=0x18u8 {
        let (x, p) = s3_new_page(&pool, m)?;
        pages.push(x);
        drop(p);
    }
    assert_eq!(pages, (1..=8).collect::<Vec<u64>>());
    let mut h = [0u64; 8];
    for (k, &x) in pages.iter().enumerate() {
        evict_out(&pool, lpid(x))?;
        h[k] = pool.entry_of(x)?.slot;
        assert_ne!(h[k], 0);
        assert_eq!(pool.entry_of(x)?.epoch, 1);
    }
    let mut m = [0u64; 3];
    for i in 0..3u64 {
        evict_out(&pool, mapleaf(i))?;
        m[idx(i)?] = pool.latch_leaf_parent(i)?.leaf_ref().slot;
        assert_ne!(m[idx(i)?], 0);
    }
    assert_eq!(s1_flip(&pool)?, 2);
    lock(&oracle).flip();
    // Phase 2.
    let keeps: Vec<_> = (0..3u64)
        .map(|i| pin(&pool, mapleaf(i), &Hold::nothing()))
        .collect::<Result<_, _>>()?;
    for _round in 0..3 {
        for (k, &x) in pages.iter().enumerate() {
            let marker = 0x20 + byte(k)?;
            let p = pin(&pool, lpid(x), &Hold::nothing())?;
            s3_touch(&pool, &p, marker)?;
            drop(p);
            evict_out(&pool, lpid(x))?;
        }
    }
    drop(keeps);
    Ok(FreeSafe {
        file,
        pool,
        oracle,
        violations,
        h,
        m,
    })
}

/// `row_i_free_safe` (I-FREE-SAFE, full, pending frees; card A-3c, Work
/// 14.2; S1 for the flip). Pass: every allocation predicted; afterwards the
/// homes of epoch 1 are still `Used`. FREE-EARLY fails it at round 1's
/// second eviction, which takes `h1`.
#[test]
fn row_i_free_safe() {
    assert_eq!(min_f(1), 18);
    let FreeSafe {
        file,
        pool,
        oracle: _oracle,
        violations,
        h,
        m,
    } = free_safe_fixture().unwrap();
    assert!(lock(&violations).is_empty(), "{:?}", lock(&violations));
    assert!(file.violations().is_empty(), "{:?}", file.violations());
    for &home in &h {
        assert_eq!(
            pool.slot_use(home).unwrap(),
            nucleus_store::buf::SlotUse::Used
        );
    }
    for &home in &m {
        assert_eq!(
            pool.slot_use(home).unwrap(),
            nucleus_store::buf::SlotUse::Used
        );
    }
    for x in 1..=8u64 {
        assert_eq!(pool.entry_of(x).unwrap().epoch, 2);
    }
}

/// `t_two_gen` (I-TWO-GEN; card A-3c, Work 14.2; S3: step 6's release as
/// A-7c's checkpoint will call it, with an allocation hook). FREE-ONE-GEN
/// fails it at the first eviction after the first release, which takes `h1`.
#[test]
fn t_two_gen() {
    assert_eq!(min_f(1), 18);
    let FreeSafe {
        file,
        pool,
        oracle,
        violations,
        h,
        m: _m,
    } = free_safe_fixture().unwrap();
    let newly: Vec<u64> = h.to_vec();
    // Step 6 of checkpoint 2: the slots image 1 references and image 2 does
    // not.
    s3_release(&pool, &newly).unwrap();
    lock(&oracle).release(&newly);
    for round in 0..2 {
        for (k, x) in (1..=8u64).enumerate() {
            let marker = 0x60 + round * 8 + u8::try_from(k).unwrap();
            let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
            s3_touch(&pool, &p, marker).unwrap();
            drop(p);
            evict_out(&pool, lpid(x)).unwrap();
        }
        for &home in &h {
            assert_eq!(
                pool.slot_use(home).unwrap(),
                nucleus_store::buf::SlotUse::Pending,
                "home {home} is pending"
            );
        }
    }
    assert!(lock(&violations).is_empty(), "{:?}", lock(&violations));
    assert!(file.violations().is_empty(), "{:?}", file.violations());
    // Checkpoint 3's step 6: the pending generation becomes free, and the
    // next eviction takes the lowest of it.
    s3_release(&pool, &[]).unwrap();
    lock(&oracle).release(&[]);
    for &home in &h {
        assert_eq!(
            pool.slot_use(home).unwrap(),
            nucleus_store::buf::SlotUse::Free
        );
    }
    {
        let p = pin(&pool, lpid(1), &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x71).unwrap();
    }
    assert!(matches!(
        evict(&pool, lpid(1)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
    assert_eq!(
        pool.entry_of(1).unwrap().slot,
        h[0],
        "the lowest freed home"
    );
    assert!(lock(&violations).is_empty(), "{:?}", lock(&violations));
}

/// `t_replica_region` (I-META-COPIES, in memory; card A-3c, Work 14.2). `n`
/// 1, `F` 18, fanout 254, `slot_count` 258, epoch 1. Mutant
/// REPLICA-SLOT-FREE fails it at the 254th allocation, which takes slot 256.
#[test]
fn t_replica_region() {
    assert_eq!(min_f(1), 18);
    let (file, pool) = wb_pool::<StdPrimitives>(18, 254, 258, 1, H_MIN_TEST).unwrap();
    assert_eq!(nucleus_store::map::max_fanout(4096), 254);
    let oracle = Arc::new(Mutex::new(WbOracle::new(PS, 258, 1)));
    write_hook(&file, &oracle);
    for i in 1..=300u64 {
        let marker = u8::try_from(i % 251).unwrap();
        let (x, p) = s3_new_page(&pool, marker).unwrap();
        assert_eq!(x, i);
        drop(p);
    }
    let stats = pool.stats().unwrap();
    assert_eq!(stats.frames, 18);
    for f in 0..18u32 {
        let out = pool.evict_frame(FrameId(f), &Hold::nothing()).unwrap();
        drop(out);
    }
    assert!(file.violations().is_empty(), "{:?}", file.violations());
    assert_eq!(lock(&oracle).slot_count(), 514);
    let stats = pool.slot_stats().unwrap();
    assert_eq!(
        (stats.slot_count, stats.growths, stats.pending),
        (514, 1, 0)
    );
    assert_eq!(stats.free, lock(&oracle).predict_free());
    assert_eq!(
        pool.slot_use(256).unwrap(),
        nucleus_store::buf::SlotUse::Reserved
    );
    assert_eq!(
        pool.slot_use(257).unwrap(),
        nucleus_store::buf::SlotUse::Reserved
    );
    assert_eq!(file.len().unwrap(), 514 * u64::from(PS));
    for i in 1..=300u64 {
        assert_eq!(
            pin_read(&pool, lpid(i)).unwrap(),
            u8::try_from(i % 251).unwrap(),
            "LPID {i}'s marker"
        );
    }
}
