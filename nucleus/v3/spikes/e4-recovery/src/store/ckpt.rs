#![deny(clippy::indexing_slicing)]
//! Checkpoint (S-STORE §13; card Work 4.6, hooks 4.8), recovery (§14;
//! card 4.7, fills G2–G4), and file open/create (§2; card 3.1, fills
//! G1/G6) — converted to the S-BATON step discipline (card X-5b Work 11).
//!
//! The checkpoint steps run in order, each step boundary a named hook
//! (`CkptHook`); the scheduler can crash at any hook. Recovery follows
//! §14's two passes with the `commit_ts > T` skip, truncates
//! **physically** (G2: zero from the valid end, `sync_data`, `sync_dir`)
//! and fails XX001 on a missing start segment (G3).
//!
//! Step 2's shape (with the Work 12 flat-map amendment): the data phase
//! writes every dirty frame with content epoch ≤ k and every epoch copy
//! out to fresh slots, resolving all `CopyPending` overrides; after it,
//! the effective image is snapshot-stable (a later first-touch freeze
//! records the same ref the image already follows — see
//! [`crate::store::prims`]), so the map radix (CoW: only leaves whose
//! content changed since image k−1) and the free set serialise exactly
//! the same image, with the free set last (§13 step 2).

use std::collections::{BTreeMap, BTreeSet};

use super::page::{self, Ref};
use super::prims;
use super::tree;
use super::wal::{self, K_CHUNK, K_COMMIT, K_MARKER, K_SEGEND};
use super::{Cx, Fail, Img, Mutant, StoreCfg, StoreHost, StoreInner, WAL_SEG};
use crate::sim::{E4Env, PAGE_FILE, ROOT_DIR, WAL_DIR};

/// The bit index of LPID `l` in the persisted free-set bitmap: slot bits
/// `[0, slot_count-3)` (slots `bit+3`), then one bit per LPID starting at
/// `slot_count-3` for LPID 1 (§4.3's two bitmaps, serialised as one run).
pub fn lpid_bit(slot_count: u64, l: u64) -> u64 {
    (slot_count - 3) + l - 1
}

/// The checkpoint hooks (card 4.8), in `CKPT_HOOKS` order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CkptHook {
    /// Before step 0.
    Before0,
    /// After step 0.
    After0,
    /// After step 1.
    After1,
    /// Inside step 2: fired after **every** step-2 write; the harness
    /// crashes at its seeded fraction of them.
    Mid2,
    /// After step 2.
    After2,
    /// After step 3.
    After3,
    /// After step 4.
    After4,
    /// After step 5.
    After5,
    /// Inside step 6, between the frees and the recycling.
    Mid6,
    /// After step 6.
    After6,
}

/// The hooks in card order.
pub const CKPT_HOOKS: [CkptHook; 10] = [
    CkptHook::Before0,
    CkptHook::After0,
    CkptHook::After1,
    CkptHook::Mid2,
    CkptHook::After2,
    CkptHook::After3,
    CkptHook::After4,
    CkptHook::After5,
    CkptHook::Mid6,
    CkptHook::After6,
];

/// A checkpoint hook callback: `Err` aborts the checkpoint (a harness
/// crash arms the scheduler's crash flag and returns `Crashed`).
pub type Hooks<'a> = dyn FnMut(CkptHook) -> Result<(), Fail> + 'a;

// ---- open / create (§2) ----

/// Create the store (§2.3 + G6): page 0 + `sync_all`, image 0 (an empty
/// tree directory and an empty page map), meta 0 + `sync_all`, with G1's
/// directory syncs, all completing before the first crash point (the
/// harness runs creation in its own pre-pass). Returns the store; the
/// caller installs it in the host.
///
/// Image 0 is built with direct env writes (the store is not yet serving
/// traffic): the directory root at slot 3, the map radix and the free
/// set above it, exactly the byte layout step 2 produces later.
pub fn create<H: StoreHost>(cx: &Cx<'_, H>, cfg: StoreCfg, uuid: u128) -> Result<StoreInner, Fail> {
    let page_file = cx.env().create(PAGE_FILE).map_err(Fail::Sim)?;
    // G1/G6: `sync_dir("/")` after creating the page file and `wal/`.
    cx.env().create("/wal/.keep").map_err(Fail::Sim)?;
    cx.env().sync_dir(ROOT_DIR).map_err(Fail::Sim)?;
    cx.env().sync_dir(WAL_DIR).map_err(Fail::Sim)?;
    let p0 = page::build_page0(uuid, WAL_SEG as u32);
    cx.env().write_at(page_file, 0, &p0).map_err(Fail::Sim)?;
    cx.env().sync_all(page_file).map_err(Fail::Sim)?;

    let mut probe = StoreInner::empty(cfg, page_file);
    // The directory root: LPID 1 (a leaf; epoch 1), written to slot 3.
    let dir_root = 1u64;
    probe.meta.next_lpid = dir_root + 1;
    let leaf = page::build_leaf(dir_root, 1, &[]);
    cx.env()
        .write_at(page_file, 3 * page::PAGE as u64, &leaf)
        .map_err(Fail::Sim)?;
    let dir_ref = Ref {
        slot: 3,
        epoch: page::header_epoch(&leaf),
        crc: page::header_crc(&leaf),
    };
    probe.map.cur.insert(dir_root, dir_ref);
    probe.slots.used_cur.insert(3);

    // The map radix: one MapLeaf at slot 4 (LPID 1 mapped, the rest
    // unmapped) — a single leaf covers `next_lpid = 2` at every cap.
    let cap = probe.cfg.map_leaf_cap;
    let entries: Vec<Option<Ref>> = {
        let mut v = vec![None; cap as usize];
        if let Some(e) = v.get_mut(1) {
            *e = Some(dir_ref);
        }
        v
    };
    let map_bytes = page::build_map_leaf(0, cap, &entries);
    cx.env()
        .write_at(page_file, 4 * page::PAGE as u64, &map_bytes)
        .map_err(Fail::Sim)?;
    let map_root = Ref {
        slot: 4,
        epoch: page::header_epoch(&map_bytes),
        crc: page::header_crc(&map_bytes),
    };
    probe.slots.used_cur.insert(4);

    // The free set: both bitmaps over the fresh file, one leaf at
    // slot 5 (bits: slots 3–15 minus the used 3/4/5; LPID 1 mapped).
    let bits = probe.cfg.freeset_leaf_bits;
    let slot_count = 16u64;
    let next_lpid = probe.meta.next_lpid;
    let used: BTreeSet<u64> = BTreeSet::from([3, 4, 5]);
    let fs_bytes = page::build_free_leaf(0, bits, &|bit: u64| {
        if bit < slot_count - 3 {
            !used.contains(&(bit + 3))
        } else {
            let lpid = bit - (slot_count - 3) + 1;
            lpid >= next_lpid
        }
    });
    cx.env()
        .write_at(page_file, 5 * page::PAGE as u64, &fs_bytes)
        .map_err(Fail::Sim)?;
    let free_root = Ref {
        slot: 5,
        epoch: page::header_epoch(&fs_bytes),
        crc: page::header_crc(&fs_bytes),
    };
    probe.slots.used_cur.insert(5);

    let meta = page::Meta {
        ckpt_id: 0,
        t: 0,
        gc_w: 0,
        epoch: 0,
        map_root,
        free_root,
        dir_root,
        next_lpid,
        slot_count,
        wal_start_lsn: 0,
    };
    let bytes = page::build_meta(&meta);
    // §2.2: meta 0 goes to slot 1 + (0 mod 2) = 1.
    cx.env()
        .write_at(page_file, page::PAGE as u64, &bytes)
        .map_err(Fail::Sim)?;
    cx.env().sync_all(page_file).map_err(Fail::Sim)?;

    probe.k = 0;
    probe.e = 1; // epoch 1 begins (§2.3)
    probe.seq.next_ts = 1; // ts 0 is reserved (§7.1)
    probe.meta = meta;
    probe.slots.slot_count = slot_count;
    let mut durable = BTreeSet::from([0, 1, 3, 4, 5]);
    durable.extend(probe.map.cur.values().map(|r| r.slot));
    probe.slots.used_durable = durable;
    probe
        .map
        .prev_leaves
        .insert(0, (hash_bytes(&map_bytes), map_root));
    probe.map.prev_root = Some(map_root);
    Ok(probe)
}

/// Open the store: page 0, both metas, the highest valid `ckpt_id` wins —
/// **no fallback** (§2.2): the newest valid meta's image is the durable
/// image; if its walk fails, open fails XX001. G3: a missing start
/// segment is XX001, never an empty WAL. Returns the store (not yet
/// installed in the host) and the opened meta.
pub fn open<H: StoreHost>(cx: &Cx<'_, H>, cfg: StoreCfg) -> Result<(StoreInner, page::Meta), Fail> {
    let page_file = cx.env().open(PAGE_FILE).map_err(Fail::Sim)?;
    let p0 = cx
        .env()
        .read_at(page_file, 0, page::PAGE as u64)
        .map_err(Fail::Sim)?;
    page::parse_page0(&p0).map_err(|()| Fail::Corrupt("page 0 invalid".into()))?;
    cx.env().accepted(page_file, 0, page::PAGE as u64);
    let m1 = cx
        .env()
        .read_at(page_file, page::PAGE as u64, page::PAGE as u64)
        .map_err(Fail::Sim)?;
    let m2 = cx
        .env()
        .read_at(page_file, 2 * page::PAGE as u64, page::PAGE as u64)
        .map_err(Fail::Sim)?;
    let c1 = page::parse_meta(&m1).ok();
    let c2 = page::parse_meta(&m2).ok();
    let best = match (c1, c2) {
        (Some(a), Some(b)) => Some(if a.ckpt_id >= b.ckpt_id { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    let Some(meta) = best else {
        return Err(Fail::Corrupt("no valid meta".into()));
    };
    // The accept hook: the meta chosen at open (card 1.4), not the loser.
    let chosen_slot = 1 + (meta.ckpt_id % 2);
    cx.env().accepted(
        page_file,
        chosen_slot * page::PAGE as u64,
        page::PAGE as u64,
    );
    let mut st = StoreInner::empty(cfg.clone(), page_file);
    st.k = meta.ckpt_id;
    st.e = meta.epoch.max(1);
    st.meta = meta;
    st.slots.slot_count = st.slots.slot_count.max(meta.slot_count);
    // Load the map and the free set from the durable bytes (§14 step 2).
    load_image(cx, &mut st, &meta)?;
    // Install the opened store, so the directory load (and recovery's
    // replay) reads through the loaded map.
    cx.with("open:install", |c| {
        *c.store() = st;
        Ok(())
    })?;
    let dir = tree::dir_load(cx, meta.dir_root)?;
    cx.with("open:dir", |c| {
        c.store().dir = dir;
        Ok(())
    })?;
    // G3: the segment holding `wal_start_lsn` must exist.
    let start_seg = meta.wal_start_lsn - (meta.wal_start_lsn % WAL_SEG);
    let name = format!("/wal/{start_seg:016x}.log");
    if meta.wal_start_lsn > 0 && cx.env().open(&name).is_err() {
        if matches!(meta_is_mutant(cx)?, Some(Mutant::StartSegmentMissingOk)) {
            // The mutant treats the WAL as empty.
            cx.with("open:mutwal", |c| {
                let st = c.store();
                st.wal.append = 0;
                st.wal.durable = 0;
                st.wal.written = 0;
                Ok(())
            })?;
        } else {
            return Err(Fail::Corrupt(format!("start segment {name} missing (G3)")));
        }
    }
    let out = cx.with("open:take", |c| Ok(c.store().clone()))?;
    Ok((out, meta))
}

/// The host store's mutant (open's G3 check reads it after install).
fn meta_is_mutant<H: StoreHost>(cx: &Cx<'_, H>) -> Result<Option<Mutant>, Fail> {
    cx.with("open:mut", |c| Ok(c.store().cfg.mutant))
}

/// Load the flat map and the durable slot set from the persisted radix
/// (walking refs with §3.3 checks; malformed input is XX001, never a
/// panic).
pub fn load_image<H: StoreHost>(
    cx: &Cx<'_, H>,
    st: &mut StoreInner,
    meta: &page::Meta,
) -> Result<(), Fail> {
    let cap = st.cfg.map_leaf_cap;
    let mutant = st.cfg.mutant;
    let pf = st.page_file;
    let mut flat: BTreeMap<u64, Ref> = BTreeMap::new();
    let mut used: BTreeSet<u64> = BTreeSet::new();
    if meta.map_root.slot != 0 {
        walk_radix(cx, pf, meta.map_root, cap, 0, &mut flat, &mut used, mutant)?;
    }
    // The free set (both bitmaps; §4.3).
    let mut free_bits: BTreeSet<u64> = BTreeSet::new();
    let mut fs_used: BTreeSet<u64> = BTreeSet::new();
    if meta.free_root.slot != 0 {
        let total_bits = (meta.slot_count.saturating_sub(3)) + meta.next_lpid;
        walk_freeset(
            cx,
            pf,
            meta.free_root,
            st.cfg.freeset_leaf_bits,
            total_bits,
            &mut free_bits,
            &mut fs_used,
            mutant,
        )?;
    }
    used.extend(fs_used.iter().copied());
    // The current free-LPID set: the LPID bitmap marks unmapped ones.
    let mut free_lpids = BTreeSet::new();
    for l in 1..meta.next_lpid {
        if free_bits.contains(&lpid_bit(meta.slot_count, l)) {
            free_lpids.insert(l);
        }
    }
    st.map.cur = flat;
    st.slots.used_cur = used.clone();
    st.slots.used_durable = used;
    st.slots.slot_count = st.slots.slot_count.max(meta.slot_count);
    st.slots.free_lpids = free_lpids;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn walk_radix<H: StoreHost>(
    cx: &Cx<'_, H>,
    pf: crate::sim::FileId,
    root: Ref,
    cap: u64,
    depth: u8,
    flat: &mut BTreeMap<u64, Ref>,
    used: &mut BTreeSet<u64>,
    mutant: Option<Mutant>,
) -> Result<(), Fail> {
    if depth > 8 || root.slot == 0 {
        return Ok(());
    }
    let bytes = cx
        .env()
        .read_at(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
        .map_err(Fail::Sim)?;
    let expect_id = page::header_id(&bytes);
    page::check(&bytes, &root, expect_id, mutant)
        .map_err(|()| Fail::Corrupt(format!("map page slot {}", root.slot)))?;
    cx.env()
        .accepted(pf, root.slot * page::PAGE as u64, page::PAGE as u64);
    used.insert(root.slot);
    let kind = page::header_kind(&bytes);
    if kind == page::KIND_MAP_LEAF {
        let base = expect_id;
        let entries = page::parse_map_leaf(&bytes, base, cap, &root, mutant)
            .map_err(|()| Fail::Corrupt("bad map leaf".into()))?;
        for (i, e) in entries.iter().enumerate() {
            if let Some(r) = e {
                flat.insert(base + i as u64, *r);
                used.insert(r.slot);
            }
        }
        Ok(())
    } else {
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        let refs = page::parse_radix_inner(&bytes, expect_id, n as usize, &root, mutant)
            .map_err(|()| Fail::Corrupt("bad map inner".into()))?;
        for r in refs.into_iter().flatten() {
            walk_radix(cx, pf, r, cap, depth + 1, flat, used, mutant)?;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn walk_freeset<H: StoreHost>(
    cx: &Cx<'_, H>,
    pf: crate::sim::FileId,
    root: Ref,
    leaf_bits: u64,
    _total_bits: u64,
    set: &mut BTreeSet<u64>,
    used: &mut BTreeSet<u64>,
    mutant: Option<Mutant>,
) -> Result<(), Fail> {
    if root.slot == 0 {
        return Ok(());
    }
    let bytes = cx
        .env()
        .read_at(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
        .map_err(Fail::Sim)?;
    let expect_id = page::header_id(&bytes);
    page::check(&bytes, &root, expect_id, mutant)
        .map_err(|()| Fail::Corrupt(format!("freeset page slot {}", root.slot)))?;
    cx.env()
        .accepted(pf, root.slot * page::PAGE as u64, page::PAGE as u64);
    used.insert(root.slot);
    let kind = page::header_kind(&bytes);
    if kind == page::KIND_FREE_LEAF {
        let bits = page::parse_free_leaf(&bytes, expect_id, leaf_bits, &root, mutant)
            .map_err(|()| Fail::Corrupt("bad freeset leaf".into()))?;
        for (i, b) in bits.iter().enumerate() {
            if *b {
                set.insert(expect_id + i as u64);
            }
        }
        Ok(())
    } else {
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        let refs = page::parse_radix_inner(&bytes, expect_id, n as usize, &root, mutant)
            .map_err(|()| Fail::Corrupt("bad freeset inner".into()))?;
        for r in refs.into_iter().flatten() {
            walk_freeset(cx, pf, r, leaf_bits, _total_bits, set, used, mutant)?;
        }
        Ok(())
    }
}

// ---- checkpoint (§13) ----

/// The flip's outcome (one step's result).
enum FlipOut {
    /// The flip succeeded; the fields step 0 captured (§13 step 0.2).
    Ok {
        t: u64,
        gc_w: u64,
        enqueue_end: u64,
        wal_start: u64,
        k: u64,
    },
    /// The fail-stop abort (gc_w > T; the gate was taken).
    Abort(Fail),
}

/// Run checkpoint `k+1` (steps 0–6, in order) with the hooks. The hooks
/// fire at every step boundary and after every step-2 write (`Mid2`);
/// `Err` from a hook aborts (the harness's crash hook arms the crash and
/// returns `Crashed`).
pub fn checkpoint<H: StoreHost>(cx: &Cx<'_, H>, hooks: &mut Hooks<'_>) -> Result<(), Fail> {
    cx.with("ckpt:begin", |c| {
        let st = c.store();
        st.alive()?;
        st.ckpt_open = true;
        st.ckpt_splits = 0;
        st.ckpt_merges = 0;
        st.fs_building.clear();
        Ok(())
    })?;
    hooks(CkptHook::Before0)?;
    // Step 0: the flip (§13 step 0): the gate exclusive, take `T`,
    // `gc_w`, `wal_start_lsn`, set `e := k+1`, then release.
    prims::gate_exclusive(cx)?;
    let flip = cx.with("ckpt:flip", |c| {
        let st = c.store();
        let k = st.k + 1;
        let t = st.seq.visible_ts;
        // I-GCW: `gc_w ≤ T` always; the meta write is fail-stop otherwise
        // (GCW-BOUND-SKIP removes the check; GCW-NOT-RECORDED zeroes it).
        let mut gc_w = st.gc_w_used;
        let over = gc_w > t;
        let skip_bound = matches!(st.cfg.mutant, Some(Mutant::GcwBoundSkip));
        if over && !skip_bound {
            st.poisoned = Some("gc_w > T at meta write".into());
            return Ok(FlipOut::Abort(Fail::Poisoned("gc_w > T".into())));
        }
        if matches!(st.cfg.mutant, Some(Mutant::GcwNotRecorded)) {
            gc_w = 0; // the previous value or 0
        }
        let enqueue_end = st.wal.append;
        st.wal.flip_enqueue_pos.push(enqueue_end);
        let open_chunks = st.wal.open_chunks.clone();
        let wal_start = wal::wal_start_lsn(st, t, &open_chunks);
        // §12.6 retention: drop exactly the entries whose markers this
        // flip's `T` covers (wholly in the image, chunks disposable at
        // the next recycle); every other entry survives so the next
        // flip can still cover its chunks.
        st.wal.open_chunks = open_chunks
            .iter()
            .filter(|(txn, first)| match wal::chunk_txn_marker(st, txn, first) {
                Some(ts) => ts > t,
                None => true,
            })
            .map(|(k, v)| (*k, *v))
            .collect();
        st.e = k as u32 + 1;
        st.flip_window = true;
        Ok(FlipOut::Ok {
            t,
            gc_w,
            enqueue_end,
            wal_start,
            k,
        })
    })?;
    let (t, gc_w, enqueue_end, wal_start, k) = match flip {
        FlipOut::Ok {
            t,
            gc_w,
            enqueue_end,
            wal_start,
            k,
        } => (t, gc_w, enqueue_end, wal_start, k),
        FlipOut::Abort(e) => {
            prims::gate_flip_release(cx);
            return Err(e);
        }
    };
    prims::gate_flip_release(cx);
    hooks(CkptHook::After0)?;
    // Step 1: WAL through every record enqueued before the flip (G5).
    wal::wal_step1(cx, t, enqueue_end)?;
    hooks(CkptHook::After1)?;
    // Step 2: the image. Data phase first (dirty frames with content
    // epoch ≤ k, and epoch copies), then the map, then the free set last.
    ckpt_step2_data(cx, hooks, k)?;
    // Free the epoch-copy frames (written or adopted by the data phase).
    cx.with("ckpt:freecopies", |c| {
        let st = c.store();
        for f in st.reserve_frames_mut() {
            if f.is_epoch_copy {
                f.key = 0;
                f.bytes = Vec::new();
                f.dirty = false;
                f.is_epoch_copy = false;
                f.pins = 0;
                f.evicting = false;
            }
        }
        Ok(())
    })?;
    let (map_root, free_root, img_slot_count, img_next_lpid) = {
        // The frozen image table: one atomic step (B5). The radix, the
        // free set and the step-5 durable set read only it, and its
        // slots are pinned, so evictions in flight across the freeze
        // cannot unpin or redirect an embedded ref. The table stays set
        // through steps 3–5 (the durable set reads it) and is cleared
        // by the post-5 resolution.
        cx.with("ckpt:freeze", |c| {
            prims::freeze_image(c.store());
            Ok(())
        })?;
        match write_map_radix(cx) {
            Ok(root) => {
                let (free_root, img_slot_count, img_next_lpid) = write_free_set(cx)?;
                (root, free_root, img_slot_count, img_next_lpid)
            }
            Err(e) => {
                cx.with("ckpt:unfreeze", |c| {
                    c.store().image_frozen = None;
                    Ok(())
                })
                .ok();
                cx.w.wake(crate::sim::WaitKey::Pin);
                return Err(e);
            }
        }
    };
    hooks(CkptHook::After2)?;
    // Step 3.
    let skip3 = cx.with("ckpt:mut3", |c| {
        Ok(matches!(c.store().cfg.mutant, Some(Mutant::MetaBeforeSync)))
    })?;
    if !skip3 {
        let pf = cx.with("ckpt:pf", |c| Ok(c.store().page_file))?;
        cx.env().sync_all(pf).map_err(Fail::Sim)?;
    }
    hooks(CkptHook::After3)?;
    // Steps 4–5: the meta, then sync. The image's sizes are the frozen
    // ones the free set actually covered (§13 step 2's convergence);
    // `next_lpid` never shrinks below the live counter's floor for the
    // *current* epoch (allocations after the freeze stay above it).
    let dir_root = cx.with("ckpt:dirroot", |c| Ok(c.store().meta.dir_root))?;
    let meta = page::Meta {
        ckpt_id: k,
        t,
        gc_w,
        epoch: k as u32,
        map_root,
        free_root,
        dir_root,
        next_lpid: img_next_lpid,
        slot_count: img_slot_count,
        wal_start_lsn: wal_start,
    };
    // The full referenced set of image k (data + map + free-set pages).
    let image_slots = full_image_slots(cx)?;
    // FREE-EARLY: the step-6 frees happen before step 5 (I-FREE-SAFE).
    let free_early = cx.with("ckpt:freeearly", |c| {
        Ok(matches!(c.store().cfg.mutant, Some(Mutant::FreeEarly)))
    })?;
    if free_early {
        cx.with("ckpt:freeearly:apply", |c| {
            let st = c.store();
            let old = std::mem::take(&mut st.slots.used_durable);
            st.slots.pending_free = old.difference(&image_slots).copied().collect();
            st.slots.used_durable = image_slots.clone();
            Ok(())
        })?;
    }
    let pf = cx.with("ckpt:pf2", |c| Ok(c.store().page_file))?;
    let bytes = page::build_meta(&meta);
    cx.env()
        .write_at(pf, (1 + (k % 2)) * page::PAGE as u64, &bytes)
        .map_err(Fail::Sim)?;
    hooks(CkptHook::After4)?;
    cx.env().sync_all(pf).map_err(Fail::Sim)?;
    // Step 5 done: meta k is durable. Resolve the image map into the new
    // durable base; the step-6 frees become visible (§4.4's I-FREE-SAFE:
    // they were protected by the old `used_durable` until here). The
    // live `next_lpid`/`slot_count` counters keep any post-freeze growth
    // (the next image's free set covers the then-current values).
    cx.with("ckpt:post5", |c| {
        let st = c.store();
        let live_next = st.meta.next_lpid;
        let live_sc = st.slots.slot_count;
        st.k = k;
        st.meta = meta;
        if live_next > st.meta.next_lpid {
            st.meta.next_lpid = live_next;
        }
        if live_sc > st.meta.slot_count {
            st.meta.slot_count = live_sc;
        }
        st.meta_durable_k = k;
        st.flip_window = false;
        st.image_frozen = None;
        let old_durable = std::mem::take(&mut st.slots.used_durable);
        st.slots.used_durable = image_slots;
        st.slots.pending_free = old_durable
            .difference(&st.slots.used_durable)
            .copied()
            .collect();
        st.map.img.clear();
        st.slots.image_refs.clear();
        st.map.prev_root = Some(map_root);
        // B5: the previous image's free-set pages leave `used_cur` (the
        // new image allocated its own); they stay pinned only if
        // something still references them (the three-reference rule).
        let old_fs = std::mem::take(&mut st.fs_prev);
        for s in old_fs {
            prims::unpin_home(st, s);
        }
        st.fs_prev = std::mem::take(&mut st.fs_building);
        Ok(())
    })?;
    cx.w.wake(crate::sim::WaitKey::MetaDurable);
    hooks(CkptHook::After5)?;
    // Step 6: frees then recycling (Mid6 between).
    cx.with("ckpt:step6", |c| {
        c.store().slots.pending_free.clear();
        Ok(())
    })?;
    hooks(CkptHook::Mid6)?;
    wal::wal_recycle(cx, wal_start)?;
    hooks(CkptHook::After6)?;
    cx.with("ckpt:end", |c| {
        let st = c.store();
        st.ckpt_done += 1;
        st.ckpt_open = false;
        Ok(())
    })?;
    // `wait_ckpt` (Work 13) polls `force_ckpt || ckpt_open`; the step-5
    // wake fired while `ckpt_open` was still true, so a waiter re-parked
    // and nothing woke it after the close. Wake here too — a checkpoint
    // that completed is a checkpoint a forced waiter can pass (B5
    // prototype-deviation fix; the directed D9 shape pinned it).
    cx.w.wake(crate::sim::WaitKey::MetaDurable);
    Ok(())
}

/// Step 2's data phase: write every dirty frame with content epoch ≤ k
/// and every epoch copy out, resolving `CopyPending` overrides; fire
/// `Mid2` after every write. A frame that was mutated in epoch k+1 after
/// the snapshot (its epoch-k content was frozen at the mutation's first
/// touch) is written for the image only — the current home does not adopt
/// it (§13 step 2).
fn ckpt_step2_data<H: StoreHost>(
    cx: &Cx<'_, H>,
    hooks: &mut Hooks<'_>,
    k: u64,
) -> Result<(), Fail> {
    let mut idx = 0usize;
    loop {
        let target = cx.with("ckpt2:scan", |c| {
            let st = c.store();
            let n = st.pool.frames.len();
            while idx < n {
                let i = idx;
                idx += 1;
                if let Some(f) = st.pool.frames.get(i)
                    && f.key != 0
                    && f.dirty
                    && !f.evicting
                    && (f.is_epoch_copy || f.epoch <= k as u32)
                {
                    return Ok(Some((i, f.key, f.bytes.clone())));
                }
            }
            Ok(None)
        })?;
        let Some((i, key, bytes)) = target else {
            return Ok(());
        };
        let r = prims::write_fresh(cx, &bytes)?;
        cx.with("ckpt2:record", |c| {
            let st = c.store();
            let is_copy = st
                .pool
                .frames
                .get(i)
                .map(|f| f.is_epoch_copy)
                .unwrap_or(false);
            // Any image state for this LPID (a CopyPending freeze or an
            // `Img::At` a previous-epoch eviction recorded during the
            // window, §5.2) means the image's content is pinned
            // elsewhere: this write is **for the image only** — record
            // it as the image ref and do not adopt it as the current
            // home. Adopting anyway would unpin the eviction's slot
            // while the radix still references it (a double allocation
            // that overwrote a live page — prototype deviation fixed
            // in B5).
            let pinned = st.map.img.contains_key(&key);
            if is_copy || pinned {
                st.map.img.insert(key, Img::At(r));
                st.slots.image_refs.insert(r.slot);
            } else if st.pool.frames.get(i).map(|f| f.key == key && f.dirty && f.epoch <= k as u32).unwrap_or(false) {
                let old = st.map.cur.insert(key, r);
                if let Some(old) = old {
                    prims::unpin_home(st, old.slot);
                }
                st.slots.used_cur.insert(r.slot);
                if let Some(f) = st.pool.frames.get_mut(i) {
                    f.dirty = false;
                }
            }
            Ok(())
        })?;
        hooks(CkptHook::Mid2)?;
    }
}

/// The map radix for the image (changed leaves CoW, up to the root;
/// unchanged leaves keep their image k−1 slots and are shared between the
/// two images — Work 12's condition 2). Reads **only the frozen table**
/// (B5): the entries and the leaf span are the freeze step's snapshot,
/// so concurrent evictions cannot change what a leaf already built
/// embeds. Returns the root ref.
fn write_map_radix<H: StoreHost>(cx: &Cx<'_, H>) -> Result<Ref, Fail> {
    let (cap, frozen) = cx.with("mapradix:cap", |c| {
        let st = c.store();
        Ok((
            st.cfg.map_leaf_cap,
            st.image_frozen.clone().ok_or_else(|| {
                Fail::Invalid("map radix without a frozen image".to_owned())
            })?,
        ))
    })?;
    let n_leaves = frozen.next_lpid.div_ceil(cap);
    // Leaf level.
    let mut leaf_refs: Vec<Option<Ref>> = Vec::new();
    for j in 0..n_leaves {
        let base = j * cap;
        let bytes = cx.with("mapradix:leaf", |c| {
            let st = c.store();
            let frz = st
                .image_frozen
                .as_ref()
                .ok_or_else(|| Fail::Invalid("frozen image gone".to_owned()))?;
            let mut entries: Vec<Option<Ref>> = Vec::with_capacity(cap as usize);
            for i in 0..cap {
                entries.push(frz.entries.get(&(base + i)).copied().flatten());
            }
            Ok(page::build_map_leaf(base, cap, &entries))
        })?;
        let h = hash_bytes(&bytes);
        let unchanged = cx.with("mapradix:prev", |c| {
            let st = c.store();
            Ok(st
                .map
                .prev_leaves
                .get(&j)
                .map(|(ph, _)| *ph == h)
                .unwrap_or(false))
        })?;
        if unchanged {
            let r = cx.with("mapradix:keep", |c| {
                Ok(c.store().map.prev_leaves.get(&j).map(|(_, r)| *r))
            })?;
            leaf_refs.push(r);
        } else {
            let r = prims::write_fresh(cx, &bytes)?;
            cx.with("mapradix:upd", |c| {
                let st = c.store();
                if let Some((_, or)) = st.map.prev_leaves.insert(j, (h, r)) {
                    prims::unpin_home(st, or.slot);
                }
                st.slots.used_cur.insert(r.slot);
                Ok(())
            })?;
            leaf_refs.push(Some(r));
        }
    }
    // Inner levels up to a single root.
    let mut level = leaf_refs;
    let mut depth: u8 = 0;
    while level.len() > 1 {
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        let mut up: Vec<Option<Ref>> = Vec::new();
        for (bi, chunk) in level.chunks(n as usize).enumerate() {
            let base = bi as u64 * n * cap.pow(depth as u32 + 1);
            let bytes = page::build_radix_inner(base, page::KIND_MAP_INNER, chunk);
            let key = (depth, bi as u64);
            let h = hash_bytes(&bytes);
            let unchanged = cx.with("mapradix:previnner", |c| {
                let st = c.store();
                Ok(st
                    .map
                    .prev_inners
                    .get(&key)
                    .map(|(ph, _)| *ph == h)
                    .unwrap_or(false))
            })?;
            if unchanged {
                let r = cx.with("mapradix:keepinner", |c| {
                    Ok(c.store().map.prev_inners.get(&key).map(|(_, r)| *r))
                })?;
                up.push(r);
            } else {
                let r = prims::write_fresh(cx, &bytes)?;
                cx.with("mapradix:updinner", |c| {
                    let st = c.store();
                    if let Some((_, or)) = st.map.prev_inners.insert(key, (h, r)) {
                        prims::unpin_home(st, or.slot);
                    }
                    st.slots.used_cur.insert(r.slot);
                    Ok(())
                })?;
                up.push(Some(r));
            }
        }
        level = up;
        depth += 1;
    }
    let root = level.first().copied().flatten().unwrap_or_default();
    Ok(root)
}

/// Serialise both bitmaps as a fresh FreeSet radix, its own slots marked
/// used (I-FREE-EXACT; BITMAP-OMITS-FREESET-PAGES omits them), with the
/// §13 step-2 sizing and growth convergence. Reads **only the frozen
/// table** (B5): the referenced/mapped sets and both size spans are the
/// freeze step's snapshot. Returns `(root, slot_count, next_lpid)` —
/// the frozen image sizes the bitmap actually covers.
fn write_free_set<H: StoreHost>(cx: &Cx<'_, H>) -> Result<(Ref, u64, u64), Fail> {
    let leaf_bits = cx.with("fs:bits", |c| Ok(c.store().cfg.freeset_leaf_bits))?;
    // The frozen image sizes and sets (one step, with the map pages the
    // radix just wrote).
    let (frozen_used, img_lpids_mapped, img_next_lpid, frozen_sc) = cx.with("fs:used", |c| {
        let st = c.store();
        let frz = st
            .image_frozen
            .as_ref()
            .ok_or_else(|| Fail::Invalid("free set without a frozen image".to_owned()))?
            .clone();
        let mut used = prims::frozen_slot_set(&frz);
        for (_, r) in st.map.prev_leaves.values() {
            used.insert(r.slot);
        }
        for (_, r) in st.map.prev_inners.values() {
            used.insert(r.slot);
        }
        let mapped: BTreeSet<u64> = frz
            .entries
            .iter()
            .filter_map(|(l, r)| r.map(|_| *l))
            .collect();
        Ok((used, mapped, frz.next_lpid, frz.slot_count))
    })?;
    let mut used = frozen_used;
    // Allocate the free-set pages with the convergence loop (§13 step
    // 2's sizing note): after growth, recompute and allocate the
    // difference.
    let mut fs_slots: Vec<u64> = Vec::new();
    let mut rounds = 0u32;
    let fan = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
    let slot_count = frozen_sc;
    loop {
        rounds += 1;
        if rounds > 8 {
            return Err(Fail::Invalid("freeset sizing did not converge".into()));
        }
        let total_bits = (slot_count - 3) + img_next_lpid;
        let mut pages = total_bits.div_ceil(leaf_bits);
        let mut lvl = total_bits.div_ceil(leaf_bits);
        while lvl > 1 {
            lvl = lvl.div_ceil(fan);
            pages += lvl;
        }
        while (fs_slots.len() as u64) < pages {
            let s = prims::alloc_slot(cx)?;
            fs_slots.push(s);
        }
        cx.with("fs:building", |c| {
            let st = c.store();
            for &s in fs_slots.iter() {
                st.fs_building.insert(s);
            }
            Ok(())
        })?;
        let sc2 = cx.with("fs:sc2", |c| Ok(c.store().slots.slot_count))?;
        if sc2 <= slot_count {
            break;
        }
        // The file grew (an fs allocation above the frozen span): mark
        // it (Q17); the frozen bitmap span is unchanged, so one pass
        // suffices — the growth's own accounting belongs to the next
        // image (post 5 keeps the live counter).
        cx.with("fs:grew", |c| {
            c.store().freeset_grew = true;
            Ok(())
        })?;
        break;
    }
    // Mark the free-set pages used (the mutant omits them: the bitmap
    // then marks the free set's own slots free — I-FREE-EXACT).
    let omit = cx.with("fs:mut", |c| {
        Ok(matches!(
            c.store().cfg.mutant,
            Some(Mutant::BitmapOmitsFreesetPages)
        ))
    })?;
    let fs_slots_set: BTreeSet<u64> = fs_slots.iter().copied().collect();
    if !omit {
        used.extend(fs_slots_set.iter().copied());
    }
    // Serialise the leaves (bit index → free). The `used` and mapped sets
    // are the frozen snapshot; no step inside reads the live store.
    let pf = cx.with("fs:pf", |c| Ok(c.store().page_file))?;
    let mut leaf_refs: Vec<Option<Ref>> = Vec::new();
    for (j, &slot) in fs_slots.iter().enumerate() {
        let base = j as u64 * leaf_bits;
        let used_ref = &used;
        let mapped_ref = &img_lpids_mapped;
        let bytes = page::build_free_leaf(base, leaf_bits, &|bit: u64| {
            if bit < slot_count - 3 {
                !used_ref.contains(&(bit + 3))
            } else {
                let lpid = bit - (slot_count - 3) + 1;
                !mapped_ref.contains(&lpid)
            }
        });
        cx.env()
            .write_at(pf, slot * page::PAGE as u64, &bytes)
            .map_err(Fail::Sim)?;
        cx.with("fs:mark", |c| {
            c.store().slots.used_cur.insert(slot);
            Ok(())
        })?;
        leaf_refs.push(Some(Ref {
            slot,
            epoch: page::header_epoch(&bytes),
            crc: page::header_crc(&bytes),
        }));
    }
    // Inner levels.
    let mut level = leaf_refs;
    let mut depth: u8 = 0;
    while level.len() > 1 {
        let mut up: Vec<Option<Ref>> = Vec::new();
        for (bi, chunk) in level.chunks(fan as usize).enumerate() {
            let base = bi as u64 * fan * leaf_bits * (1u64 << depth.min(30));
            let bytes = page::build_radix_inner(base, page::KIND_FREE_INNER, chunk);
            let r = prims::write_fresh(cx, &bytes)?;
            cx.with("fs:markinner", |c| {
                c.store().slots.used_cur.insert(r.slot);
                Ok(())
            })?;
            up.push(Some(r));
        }
        level = up;
        depth += 1;
    }
    let root = level.first().copied().flatten().unwrap_or_default();
    Ok((root, slot_count, img_next_lpid))
}

/// The full set of slots image k references (data + map + free-set pages
/// plus the fixed slots), for the step-5 durable set. Reads the frozen
/// table (B5): exactly the slots the radix and the free set embedded.
fn full_image_slots<H: StoreHost>(cx: &Cx<'_, H>) -> Result<BTreeSet<u64>, Fail> {
    cx.with("full_slots", |c| {
        let st = c.store();
        let frz = st
            .image_frozen
            .as_ref()
            .ok_or_else(|| Fail::Invalid("full image slots without a frozen image".to_owned()))?;
        let mut out = prims::frozen_slot_set(frz);
        for (_, r) in st.map.prev_leaves.values() {
            out.insert(r.slot);
        }
        for (_, r) in st.map.prev_inners.values() {
            out.insert(r.slot);
        }
        // The image's own free-set pages (B5: the durable set must cover
        // everything meta k references, its free set included).
        out.extend(st.fs_building.iter().copied());
        out.insert(0);
        out.insert(1);
        out.insert(2);
        Ok(out)
    })
}

// ---- recovery (§14) ----

/// Recover: two passes, skip `ts ≤ T`, chunks only of transactions with a
/// marker > T, physical truncation (G2), resume `dropping` walks.
/// Returns the store (installed back out of the host) and the frontier
/// (the largest replayed ts, or `T`).
pub fn recover<H: StoreHost>(cx: &Cx<'_, H>, cfg: StoreCfg) -> Result<(StoreInner, u64), Fail> {
    let (mut st, meta) = open(cx, cfg)?;
    st.seq.next_ts = meta.t + 1;
    st.seq.visible_ts = meta.t;
    // Passes 1+2 (§14 steps 3–4): the walk collects records and markers
    // (pass 1), then applies in LSN order (pass 2). §12.5's stop rules:
    // bad magic/CRC, `lsn` ≠ expected, `len` past the segment end, and
    // SegmentEnd hops.
    let mut replay: Vec<wal::Rec> = Vec::new();
    let mut valid_end = meta.wal_start_lsn;
    let mut lsn = meta.wal_start_lsn;
    let mut markers: BTreeMap<(u32, u64), u64> = BTreeMap::new();
    let mutant = st.cfg.mutant;
    'outer: loop {
        let seg = wal::seg_of(lsn);
        let name = format!("/wal/{seg:016x}.log");
        let fid = match cx.env().open(&name) {
            Ok(f) => f,
            Err(_) => break 'outer,
        };
        let mut off = lsn - seg;
        while off + 16 <= WAL_SEG {
            let want = (WAL_SEG - off).min(16 * 1024 + 16);
            let bytes = cx.env().read_at(fid, off, want).map_err(Fail::Sim)?;
            match wal::dec_record_with_lsn_check(&bytes, lsn, seg + WAL_SEG, mutant) {
                Ok(Some(rec)) => {
                    let len = rec.len();
                    // Accept the record the reader yields (card 1.4).
                    cx.env().accepted(fid, off, len);
                    if rec.kind == K_SEGEND {
                        lsn = seg + WAL_SEG;
                        continue 'outer;
                    }
                    if rec.kind == K_MARKER {
                        markers.insert(rec.txn, rec.ts);
                    }
                    replay.push(rec);
                    valid_end = lsn + len;
                    lsn += len;
                    off += len;
                }
                Ok(None) | Err(()) => break 'outer,
            }
        }
        break 'outer;
    }
    // Pass 2: install the opened store, then apply in LSN order through
    // the gated insert path (§9.1's identical-entry no-op makes it
    // idempotent).
    cx.with("recover:install", |c| {
        *c.store() = st;
        Ok(())
    })?;
    let mut frontier = meta.t;
    for rec in replay.iter() {
        match rec.kind {
            K_COMMIT if rec.ts > meta.t => {
                for op in wal::rec_ops(rec) {
                    apply_op(cx, &op, rec.ts, false)?;
                }
                cx.with("recover:applied", |c| {
                    c.store().seq.applied.insert(rec.ts);
                    Ok(())
                })?;
                frontier = frontier.max(rec.ts);
            }
            K_CHUNK => {
                if let Some(ts) = markers.get(&rec.txn).copied()
                    && ts > meta.t
                {
                    for op in wal::rec_ops(rec) {
                        apply_op(cx, &op, ts, true)?;
                    }
                    cx.with("recover:applied2", |c| {
                        c.store().seq.applied.insert(ts);
                        Ok(())
                    })?;
                    frontier = frontier.max(ts);
                }
            }
            _ => {}
        }
    }
    // Advance visible_ts through contiguous applied commits.
    cx.with("recover:visible", |c| {
        let st = c.store();
        while st.seq.applied.remove(&(st.seq.visible_ts + 1)) {
            st.seq.visible_ts += 1;
        }
        st.seq.next_ts = st.seq.visible_ts + 1;
        Ok(())
    })?;
    // G2: physical truncation before returning (and before any new
    // append): zero from the valid end to the end of its segment and
    // every later segment's content, `sync_data` each, then `sync_dir`.
    // TRUNCATE-LOGICAL-ONLY moves the append position only (the mutant).
    let logical_only = cx.with("recover:mut", |c| {
        Ok(matches!(
            c.store().cfg.mutant,
            Some(Mutant::TruncateLogicalOnly)
        ))
    })?;
    if !logical_only {
        truncate_wal(cx, valid_end)?;
    }
    // The WAL writer state restarts at the valid end. The replayed
    // records' tags and the chunk bookkeeping survive the restart
    // (§12.6): a post-recovery flip must still cover the chunks of a
    // transaction whose marker is above its `T`, or the next recycle
    // could eat them before a later crash's replay (I-REPLAY-IDEM).
    cx.with("recover:walreset", |c| {
        let st = c.store();
        st.wal.append = valid_end;
        st.wal.durable = valid_end;
        st.wal.written = valid_end;
        st.wal.buf.clear();
        st.wal.buf_tags.clear();
        st.wal.open_chunks.clear();
        for rec in replay.iter() {
            if rec.kind == K_COMMIT || rec.kind == K_MARKER || rec.kind == K_CHUNK {
                st.wal
                    .buf_tags
                    .insert(rec.lsn, (rec.ts, rec.kind == K_MARKER, rec.txn));
                if rec.kind == K_CHUNK {
                    st.wal
                        .open_chunks
                        .entry(rec.txn)
                        .or_insert(rec.lsn);
                }
            }
        }
        // Entries whose markers the opened image covers are droppable
        // right away; the rest must stay until a flip covers them.
        let t = meta.t;
        st.wal.open_chunks.retain(|txn, _| {
            markers
                .get(txn)
                .map(|ts| *ts > t)
                .unwrap_or(true)
        });
        Ok(())
    })?;
    // §14 step 6: resume `dropping` walks.
    tree::resume_drops(cx)?;
    let out = cx.with("recover:take", |c| Ok(c.store().clone()))?;
    Ok((out, frontier))
}

/// Apply one op at `ts` (the gated insert path; §9). `chunked` marks a
/// chunked transaction's superseding replay ops (§14: the last op per key
/// wins — a replace, not a conflict).
pub fn apply_op<H: StoreHost>(
    cx: &Cx<'_, H>,
    op: &wal::WOp,
    ts: u64,
    chunked: bool,
) -> Result<(), Fail> {
    match op {
        wal::WOp::CreateTree { tree } => {
            tree::dir_create_tree(cx, *tree)?;
            Ok(())
        }
        wal::WOp::DropTree { tree } => {
            tree::dir_drop_tree(cx, *tree)?;
            Ok(())
        }
        wal::WOp::Put { tree, key, value } => {
            if key.len() > page::K_MAX || value.len() > page::V_INLINE {
                return Err(Fail::Invalid("oversize key or value".into()));
            }
            let root = tree::dir_root_of(cx, *tree)?;
            let ekey = tree::enc_ekey(key, ts);
            let mut v = vec![0u8];
            v.extend_from_slice(value);
            tree::tree_insert(cx, *tree, root, &ekey, &v, chunked)?;
            Ok(())
        }
        wal::WOp::Delete { tree, key } => {
            let root = tree::dir_root_of(cx, *tree)?;
            let ekey = tree::enc_ekey(key, ts);
            tree::tree_insert(cx, *tree, root, &ekey, &[1u8], chunked)?;
            Ok(())
        }
    }
}

/// G2: zero from the valid end through the rest of its segment and every
/// later segment; `sync_data` each; `sync_dir`.
pub fn truncate_wal<H: StoreHost>(cx: &Cx<'_, H>, valid_end: u64) -> Result<(), Fail> {
    let first = wal::seg_of(valid_end);
    let mut seg = first;
    loop {
        let name = format!("/wal/{seg:016x}.log");
        let Ok(f) = cx.env().open(&name) else {
            break;
        };
        let off = if seg == first { valid_end - seg } else { 0 };
        if off < WAL_SEG {
            let zeros = vec![0u8; (WAL_SEG - off) as usize];
            cx.env().write_at(f, off, &zeros).map_err(Fail::Sim)?;
        }
        cx.env().sync_data(f).map_err(Fail::Sim)?;
        seg += WAL_SEG;
    }
    cx.env().sync_dir(WAL_DIR).map_err(Fail::Sim)?;
    Ok(())
}

fn hash_bytes(b: &[u8]) -> u64 {
    crate::crc::crc32c(0, b) as u64
}
