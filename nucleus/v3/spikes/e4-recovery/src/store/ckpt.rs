#![deny(clippy::indexing_slicing)]
//! Checkpoint (S-STORE §13; card Work 4.6, hooks 4.8), recovery (§14;
//! card 4.7, fills G2–G4), amcheck (card 3.8), and file open/create
//! (§2; card 3.1, fills G1/G6).
//!
//! The checkpoint steps run in order, each step boundary a named hook
//! (`CkptHook`); the scheduler can crash at any hook. Recovery follows
//! §14's two passes with the `commit_ts > T` skip, truncates **physically**
//! (G2: zero from the valid end, sync, `sync_dir`) and fails XX001 on a
//! missing start segment (G3). Amcheck checks a quiescent image through
//! SimDisk reads.

use std::collections::{BTreeMap, BTreeSet};

use super::{page, Fail, Img, Mutant, StoreInner, Cx, WAL_SEG};
use crate::sim::{E4Env, FileId, PAGE_FILE, ROOT_DIR, WAL_DIR};

/// The checkpoint hooks (card 4.8), in `CKPT_HOOKS` order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CkptHook {
    /// Before step 0.
    Before0,
    /// After step 0.
    After0,
    /// After step 1.
    After1,
    /// Inside step 2, after a seeded fraction of its writes.
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

/// The hooks in card order (5.5 uses `(seed / 20) % 10` over this).
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

impl StoreInner {
    // ---- open / create (§2) ----

    /// Create the store (§2.3 + G6): page 0 + `sync_all`, image 0 (an
    /// empty tree directory and an empty page map), meta 0 + `sync_all`,
    /// with G1's directory syncs, all completing before the first crash
    /// point.
    pub fn create(cx: &Cx, cfg: super::StoreCfg, uuid: u128) -> Result<Self, Fail> {
        let page_file = cx.env().create(PAGE_FILE).map_err(Fail::Sim)?;
        // G1: sync_dir("/") after creating the page file and wal/.
        cx.env().create("/wal/.keep").map_err(Fail::Sim)?;
        cx.env().sync_dir(ROOT_DIR).map_err(Fail::Sim)?;
        cx.env().sync_dir(WAL_DIR).map_err(Fail::Sim)?;
        let p0 = page::build_page0(uuid);
        cx.env().write_at(page_file, 0, &p0).map_err(Fail::Sim)?;
        cx.env().sync_all(page_file).map_err(Fail::Sim)?;
        // Build image 0 through the normal checkpoint path (§2.3): an
        // empty tree directory (a root leaf) and an empty page map.
        let mut st = StoreInner::empty(cfg, page_file);
        st.alloc_slot_hinted(3);
        let dir_root = st.alloc_lpid();
        let leaf = page::build_leaf(dir_root, 1, &[]);
        let slot = st.alloc_slot(cx)?;
        cx.env()
            .write_at(page_file, slot * page::PAGE as u64, &leaf)
            .map_err(Fail::Sim)?;
        st.map.cur.insert(
            dir_root,
            page::Ref {
                slot,
                epoch: 1,
                crc: page::header_crc(&leaf),
            },
        );
        st.slots.used_cur.insert(slot);
        let meta = page::Meta {
            ckpt_id: 0,
            t: 0,
            gc_w: 0,
            epoch: 0,
            map_root: page::Ref::default(),
            free_root: page::Ref::default(),
            dir_root,
            next_lpid: st.meta.next_lpid,
            slot_count: st.slots.slot_count,
            wal_start_lsn: 0,
        };
        st.meta = meta;
        st.write_meta(cx, &meta)?;
        st.k = 0;
        st.e = 1; // epoch 1 begins (§2.3)
        st.slots.used_durable = st.image_slot_set();
        st.slots.used_durable.insert(slot);
        Ok(st)
    }

    /// An empty store shell (no file content).
    pub fn empty(cfg: super::StoreCfg, page_file: FileId) -> Self {
        let mut pool = super::Pool {
            frames: Vec::new(),
            hand: 0,
        };
        for _ in 0..cfg.pool_frames {
            pool.frames.push(super::Frame {
                key: 0,
                bytes: Vec::new(),
                epoch: 0,
                dirty: false,
                pins: 0,
                is_epoch_copy: false,
                refed: false,
            });
        }
        let mut slots = super::SlotState::default();
        slots.slot_count = 16; // grows on demand (card 3.3: max(16, n/16))
        Self {
            cfg,
            page_file,
            k: 0,
            e: 1,
            meta: page::Meta::default(),
            dir: BTreeMap::new(),
            map: super::MapState::default(),
            slots,
            pool,
            gate: super::Gate::default(),
            tree_mu: BTreeMap::new(),
            seq: super::SeqState::default(),
            gc_w_used: 0,
            ckpt_done: 0,
            flip_window: false,
            ckpt_open: false,
            ckpt_splits: 0,
            ckpt_merges: 0,
            epoch_exhausted: 0,
            poisoned: None,
            wal: super::wal::WalState::default(),
            meta_durable_k: 0,
            freeset_grew: false,
            stale_seg_reads: 0,
            chunk_open_crash: false,
            chunk_marked_crash: false,
            chunk_partial_crash: false,
            drop_walk_crash: false,
            load_unpublished_crash: false,
            load_published_crash: false,
            gc_advanced_flag: false,
        }
    }

    fn alloc_slot_hinted(&mut self, _floor: u64) {}

    /// Open the store: page 0, both metas, the highest valid `ckpt_id`
    /// wins — **no fallback** (§2.2): the newest valid meta's image is the
    /// durable image; if its walk fails, open fails XX001. G3: a missing
    /// start segment is XX001, never an empty WAL.
    pub fn open(cx: &Cx, cfg: super::StoreCfg) -> Result<(Self, page::Meta), Fail> {
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
        // The accept hook: the meta chosen at open (not the losing one).
        let chosen_slot = 1 + (meta.ckpt_id % 2);
        cx.env().accepted(
            page_file,
            chosen_slot * page::PAGE as u64,
            page::PAGE as u64,
        );
        let mut st = StoreInner::empty(cfg, page_file);
        st.k = meta.ckpt_id;
        st.e = meta.epoch.max(1);
        st.meta = meta;
        // Load the map and the free set from the durable bytes (§14 step 2).
        st.load_image(cx, &meta)?;
        // The directory.
        st.dir = st.dir_load(cx)?;
        // G3: the start segment must exist.
        let start_seg = meta.wal_start_lsn - (meta.wal_start_lsn % WAL_SEG);
        let name = format!("/wal/{start_seg:016x}.log");
        if meta.wal_start_lsn > 0 && cx.env().open(&name).is_err() {
            if matches!(st.cfg.mutant, Some(Mutant::StartSegmentMissingOk)) {
                // The mutant treats the WAL as empty.
                st.wal.append = 0;
                st.wal.durable = 0;
                st.wal.written = 0;
            } else {
                return Err(Fail::Corrupt(format!(
                    "start segment {name} missing (G3)"
                )));
            }
        }
        Ok((st, meta))
    }

    /// Load the flat map and the durable slot set from the persisted radix
    /// (walking refs with §3.3 checks).
    pub fn load_image(&mut self, cx: &Cx, meta: &page::Meta) -> Result<(), Fail> {
        let cap = self.cfg.map_leaf_cap;
        let mut flat: BTreeMap<u64, page::Ref> = BTreeMap::new();
        let mut used: BTreeSet<u64> = BTreeSet::new();
        let mut map_pages: Vec<(u64, page::Ref)> = Vec::new();
        if meta.map_root.slot != 0 {
            Self::walk_radix(
                cx,
                self.page_file,
                meta.map_root,
                cap,
                meta.next_lpid,
                0,
                &mut flat,
                &mut used,
                &mut map_pages,
            )?;
        }
        // The free set (both bitmaps; §4.3).
        let mut free_bits: BTreeSet<u64> = BTreeSet::new();
        let mut free_pages: Vec<(u64, page::Ref)> = Vec::new();
        if meta.free_root.slot != 0 {
            let total_bits = (meta.slot_count.saturating_sub(3)) + meta.next_lpid;
            Self::walk_freeset(
                cx,
                self.page_file,
                meta.free_root,
                self.cfg.freeset_leaf_bits,
                total_bits,
                &mut free_bits,
                &mut free_pages,
            )?;
        }
        // The current free-LPID set: the LPID bitmap marks unmapped ones.
        let mut free_lpids = BTreeSet::new();
        for l in 1..meta.next_lpid {
            if free_bits.contains(&(meta.slot_count - 3 + l)) {
                free_lpids.insert(l);
            }
        }
        self.map.cur = flat;
        self.slots.used_cur = used.clone();
        self.slots.used_durable = used.clone();
        self.slots.slot_count = self.slots.slot_count.max(meta.slot_count);
        self.slots.free_lpids = free_lpids;
        Ok(())
    }

    fn walk_radix(
        cx: &Cx,
        pf: FileId,
        root: page::Ref,
        cap: u64,
        next_lpid: u64,
        depth: u8,
        flat: &mut BTreeMap<u64, page::Ref>,
        used: &mut BTreeSet<u64>,
        pages: &mut Vec<(u64, page::Ref)>,
    ) -> Result<(), Fail> {
        if depth > 8 || root.slot == 0 {
            return Ok(());
        }
        let bytes = cx
            .env()
            .read_at(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
            .map_err(Fail::Sim)?;
        let id = if next_lpid > cap.pow(depth as u32 + 1) {
            0
        } else {
            0
        };
        let _ = id;
        // The expected id is the node's radix position; leaves carry their
        // base, inners theirs.
        let expect_id = page::header_id(&bytes);
        page::check(&bytes, &root, expect_id, None)
            .map_err(|()| Fail::Corrupt(format!("map page slot {}", root.slot)))?;
        cx.env().accepted(pf, root.slot * page::PAGE as u64, page::PAGE as u64);
        used.insert(root.slot);
        pages.push((expect_id, root));
        let kind = bytes.get(1).copied().unwrap_or(0);
        if kind == page::KIND_MAP_LEAF {
            let base = expect_id;
            let entries = page::parse_map_leaf(&bytes, base, cap, &root)
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
            let refs = page::parse_radix_inner(&bytes, expect_id, n as usize, &root)
                .map_err(|()| Fail::Corrupt("bad map inner".into()))?;
            let span = cap.pow(depth as u32 + 1);
            for (_i, r) in refs.iter().enumerate() {
                if let Some(r) = r {
                    Self::walk_radix(
                        cx,
                        pf,
                        *r,
                        cap,
                        next_lpid,
                        depth + 1,
                        flat,
                        used,
                        pages,
                    )?;
                    let _ = span;
                }
            }
            Ok(())
        }
    }

    fn walk_freeset(
        cx: &Cx,
        pf: FileId,
        root: page::Ref,
        leaf_bits: u64,
        total_bits: u64,
        set: &mut BTreeSet<u64>,
        pages: &mut Vec<(u64, page::Ref)>,
    ) -> Result<(), Fail> {
        if root.slot == 0 {
            return Ok(());
        }
        let bytes = cx
            .env()
            .read_at(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
            .map_err(Fail::Sim)?;
        let expect_id = page::header_id(&bytes);
        page::check(&bytes, &root, expect_id, None)
            .map_err(|()| Fail::Corrupt(format!("freeset page slot {}", root.slot)))?;
        cx.env().accepted(pf, root.slot * page::PAGE as u64, page::PAGE as u64);
        pages.push((expect_id, root));
        let kind = bytes.get(1).copied().unwrap_or(0);
        if kind == page::KIND_FREE_LEAF {
            let bits = page::parse_free_leaf(&bytes, expect_id, leaf_bits, &root)
                .map_err(|()| Fail::Corrupt("bad freeset leaf".into()))?;
            for (i, b) in bits.iter().enumerate() {
                if *b {
                    set.insert(expect_id + i as u64);
                }
            }
            Ok(())
        } else {
            let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
            let refs = page::parse_radix_inner(&bytes, expect_id, n as usize, &root)
                .map_err(|()| Fail::Corrupt("bad freeset inner".into()))?;
            for (_i, r) in refs.iter().enumerate() {
                if let Some(r) = r {
                    Self::walk_freeset(cx, pf, *r, leaf_bits, total_bits, set, pages)?;
                    let _ = total_bits;
                }
            }
            Ok(())
        }
    }

    // ---- checkpoint (§13) ----

    /// Run checkpoint `k+1` (steps 0–6) with the hooks. `crash_hook`, when
    /// it matches, crashes the round at that hook (targeted crashes).
    /// `mid2_frac` seeds the `Mid2` position.
    pub fn checkpoint(
        &mut self,
        cx: &Cx,
        hook: &mut dyn FnMut(&mut StoreInner, CkptHook) -> Result<(), Fail>,
    ) -> Result<(), Fail> {
        self.alive()?;
        let k = self.k + 1;
        self.ckpt_open = true;
        self.ckpt_splits = 0;
        self.ckpt_merges = 0;
        hook(self, CkptHook::Before0)?;
        // Step 0: the flip (§13 step 0).
        self.gate_exclusive(cx)?;
        let t = self.seq.visible_ts;
        // I-GCW: `gc_w ≤ T` always; the meta write is fail-stop otherwise.
        let mut gc_w = self.gc_w_used;
        if gc_w > t {
            if matches!(self.cfg.mutant, Some(Mutant::GcwBoundSkip)) {
                // GCW-BOUND-SKIP: the check is removed.
            } else {
                self.gate_flip_release(cx);
                self.poisoned = Some("gc_w > T at meta write".into());
                return Err(Fail::Poisoned("gc_w > T".into()));
            }
        }
        if matches!(self.cfg.mutant, Some(Mutant::GcwNotRecorded)) {
            gc_w = 0; // the previous value or 0
        }
        let enqueue_end = self.wal.append;
        let open_chunks = self.wal.open_chunks.clone();
        let wal_start = self.wal_start_lsn(t, &open_chunks);
        self.e = k as u32 + 1;
        self.flip_window = true;
        self.gate_flip_release(cx);
        hook(self, CkptHook::After0)?;
        // Step 1: WAL through every record enqueued before the flip (G5).
        self.wal_step1(cx, t, enqueue_end)?;
        hook(self, CkptHook::After1)?;
        // Step 2: the image. Dirty frames with content epoch ≤ k, epoch
        // copies, then the map, then the free set last (§13 step 2).
        {
            let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();
            for i in 0..self.pool.frames.len() {
                let (key, bytes, dirty, epoch, is_copy) = {
                    let f = self
                        .pool
                        .frames
                        .get(i)
                        .ok_or_else(|| Fail::Invalid("frame".to_owned()))?;
                    (f.key, f.bytes.clone(), f.dirty, f.epoch, f.is_epoch_copy)
                };
                if key != 0 && dirty && (is_copy || epoch <= k as u32) {
                    writes.push((key, bytes));
                }
            }
            let mut mid_fired = false;
            for (key, bytes) in writes.iter() {
                let r = self.write_page_out(cx, *key, bytes)?;
                if self.map.img.get(key).copied() == Some(Img::CopyPending) {
                    self.map.img.insert(*key, Img::At(r));
                }
                if !mid_fired {
                    hook(self, CkptHook::Mid2)?;
                    mid_fired = true;
                }
            }
        }
        // Free the epoch-copy frames.
        for f in self.pool.frames.iter_mut() {
            if f.is_epoch_copy {
                f.key = 0;
                f.bytes = Vec::new();
                f.dirty = false;
                f.is_epoch_copy = false;
                f.pins = 0;
            }
        }
        // The map radix (CoW diff of changed leaves, up to the root).
        let map_root = self.write_map_radix(cx)?;
        // The free set last, with its own slots marked used
        // (BITMAP-OMITS-FREESET-PAGES omits them).
        let free_root = self.write_free_set(cx, k)?;
        hook(self, CkptHook::After2)?;
        // Step 3.
        if !matches!(self.cfg.mutant, Some(Mutant::MetaBeforeSync)) {
            cx.env().sync_all(self.page_file).map_err(Fail::Sim)?;
        }
        hook(self, CkptHook::After3)?;
        // Steps 4–5: the meta, then sync.
        let meta = page::Meta {
            ckpt_id: k,
            t,
            gc_w,
            epoch: k as u32,
            map_root,
            free_root,
            dir_root: self.meta.dir_root,
            next_lpid: self.meta.next_lpid,
            slot_count: self.slots.slot_count,
            wal_start_lsn: wal_start,
        };
        // FREE-EARLY: the frees happen before step 5.
        let mut image_slots = self.image_slot_set();
        image_slots.insert(map_root.slot);
        image_slots.insert(free_root.slot);
        if matches!(self.cfg.mutant, Some(Mutant::FreeEarly)) {
            let old = std::mem::take(&mut self.slots.used_durable);
            self.slots.pending_free = old.difference(&image_slots).copied().collect();
            self.slots.used_durable = image_slots.clone();
        }
        self.write_meta(cx, &meta)?;
        hook(self, CkptHook::After4)?;
        cx.env().sync_all(self.page_file).map_err(Fail::Sim)?;
        // Step 5 done: meta k is durable.
        self.k = k;
        self.meta = meta;
        self.meta_durable_k = k;
        self.flip_window = false;
        let old_durable = std::mem::take(&mut self.slots.used_durable);
        self.slots.used_durable = image_slots.clone();
        self.slots.pending_free = old_durable.difference(&image_slots).copied().collect();
        self.map.img.clear();
        cx.w.wake(crate::sim::WaitKey::MetaDurable);
        hook(self, CkptHook::After5)?;
        // Step 6: frees then recycling (Mid6 between).
        self.slots.pending_free.clear();
        hook(self, CkptHook::Mid6)?;
        let _ = self.wal_recycle(cx, wal_start)?;
        hook(self, CkptHook::After6)?;
        self.ckpt_done += 1;
        self.ckpt_open = false;
        Ok(())
    }

    /// Write the map radix for the image (changed leaves CoW, up to the
    /// root). Returns the root ref.
    fn write_map_radix(&mut self, cx: &Cx) -> Result<page::Ref, Fail> {
        let cap = self.cfg.map_leaf_cap;
        // The effective image entries.
        let next_lpid = self.meta.next_lpid;
        let n_leaves = (next_lpid + cap - 1) / cap;
        let get_entry = |st: &StoreInner, l: u64| -> Option<page::Ref> {
            let eff = st.map.img.get(&l).copied().unwrap_or(Img::Same);
            match eff {
                Img::Same => st.map.cur.get(&l).copied(),
                Img::At(r) => Some(r),
                Img::Free | Img::CopyPending | Img::Lost => None,
            }
        };
        // Leaf level.
        let mut leaf_refs: Vec<Option<page::Ref>> = Vec::new();
        for j in 0..n_leaves {
            let base = j * cap;
            let mut entries: Vec<Option<page::Ref>> = Vec::with_capacity(cap as usize);
            for i in 0..cap {
                entries.push(get_entry(self, base + i));
            }
            let bytes = page::build_map_leaf(base, cap, &entries);
            // CoW: write only if changed vs the previous image's leaf.
            let changed = match self.map.prev_leaves.get(&j) {
                Some((h, _)) => *h != hash_bytes(&bytes),
                None => true,
            };
            if changed {
                let r = self.write_page_out_map(cx, base, &bytes)?;
                leaf_refs.push(Some(r));
                self.map.prev_leaves.insert(j, (hash_bytes(&bytes), r));
            } else {
                let r = self
                    .map
                    .prev_leaves
                    .get(&j)
                    .map(|(_, r)| *r)
                    .unwrap_or_default();
                leaf_refs.push(if r.slot == 0 { None } else { Some(r) });
            }
        }
        // Inner levels up to a single root.
        let mut level = leaf_refs;
        let mut depth: u8 = 0;
        while level.len() > 1 {
            let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
            let mut up: Vec<Option<page::Ref>> = Vec::new();
            for (bi, chunk) in level.chunks(n as usize).enumerate() {
                let base = bi as u64 * n * cap.pow(depth as u32 + 1);
                let bytes = page::build_radix_inner(base, page::KIND_MAP_INNER, chunk);
                let key = (depth, bi as u64);
                let changed = match self.map.prev_inners.get(&key) {
                    Some((h, _)) => *h != hash_bytes(&bytes),
                    None => true,
                };
                if changed {
                    let r = self.write_page_out_map(cx, base, &bytes)?;
                    up.push(Some(r));
                    self.map.prev_inners.insert(key, (hash_bytes(&bytes), r));
                } else {
                    let r = self
                        .map
                        .prev_inners
                        .get(&key)
                        .map(|(_, r)| *r)
                        .unwrap_or_default();
                    up.push(if r.slot == 0 { None } else { Some(r) });
                }
            }
            level = up;
            depth += 1;
        }
        let root = level.first().copied().flatten().unwrap_or_default();
        self.map.prev_root = Some(root);
        Ok(root)
    }

    /// Write a map/freeset page to a fresh slot (physically addressed; the
    /// LPID space does not apply — track in `used_cur` only).
    fn write_page_out_map(&mut self, cx: &Cx, _base: u64, bytes: &[u8]) -> Result<page::Ref, Fail> {
        let slot = self.alloc_slot(cx)?;
        cx.env()
            .write_at(self.page_file, slot * page::PAGE as u64, bytes)
            .map_err(Fail::Sim)?;
        let r = page::Ref {
            slot,
            epoch: page::header_epoch(bytes),
            crc: page::header_crc(bytes),
        };
        self.slots.used_cur.insert(slot);
        Ok(r)
    }

    /// Serialise both bitmaps as a fresh FreeSet radix, its own slots
    /// marked used (I-FREE-EXACT); sizing after growth converges in one
    /// round (§13 step 2).
    fn write_free_set(&mut self, cx: &Cx, _k: u64) -> Result<page::Ref, Fail> {
        let leaf_bits = self.cfg.freeset_leaf_bits;
        let before = self.slots.slot_count;
        // The image's free slots (both bitmaps; I-FREE-EXACT).
        let img_slots = self.image_slot_set();
        let next_lpid = self.meta.next_lpid;
        let omit = matches!(self.cfg.mutant, Some(Mutant::BitmapOmitsFreesetPages));
        // Allocate the FreeSet leaves (growing the file first if needed).
        let total_bits = |slot_count: u64| (slot_count - 3) + next_lpid;
        let n_leaves = (total_bits(self.slots.slot_count) + leaf_bits - 1) / leaf_bits;
        let mut fs_slots: Vec<u64> = Vec::new();
        for _ in 0..n_leaves {
            let s = self.alloc_slot(cx)?;
            fs_slots.push(s);
        }
        // Growth may have changed slot_count: recompute (one convergence
        // round, §13 step 2).
        if self.slots.slot_count != before {
            if self.slots.slot_count > before {
                self.freeset_grew = true;
            }
            let n2 = (total_bits(self.slots.slot_count) + leaf_bits - 1) / leaf_bits;
            while fs_slots.len() < n2 as usize {
                if let Some(s) = self.alloc_slot(cx).ok() {
                    fs_slots.push(s);
                } else {
                    break;
                }
            }
        }
        let sc = self.slots.slot_count;
        let is_free = |bit: u64| -> bool {
            if bit < sc - 3 {
                let slot = bit + 3;
                !img_slots.contains(&slot)
                    && !fs_slots.contains(&slot)
                    && !(omit && fs_slots.contains(&slot))
                    && !fs_slots.contains(&slot)
            } else {
                let lpid = bit - (sc - 3) + 1;
                lpid >= 1 && lpid < next_lpid && {
                    let eff = self.map.img.get(&lpid).copied().unwrap_or(Img::Same);
                    match eff {
                        Img::Free => true,
                        Img::Same => !self.map.cur.contains_key(&lpid),
                        _ => false,
                    }
                }
            }
        };
        let mut refs: Vec<Option<page::Ref>> = Vec::new();
        for (j, &slot) in fs_slots.iter().enumerate() {
            let base = j as u64 * leaf_bits;
            let bytes = page::build_free_leaf(base, leaf_bits, &is_free);
            cx.env()
                .write_at(self.page_file, slot * page::PAGE as u64, &bytes)
                .map_err(Fail::Sim)?;
            let r = page::Ref {
                slot,
                epoch: page::header_epoch(&bytes),
                crc: page::header_crc(&bytes),
            };
            self.slots.used_cur.insert(slot);
            refs.push(Some(r));
        }
        // Inner levels.
        let mut level = refs;
        let mut depth: u8 = 0;
        while level.len() > 1 {
            let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
            let mut up: Vec<Option<page::Ref>> = Vec::new();
            for (bi, chunk) in level.chunks(n as usize).enumerate() {
                let base = bi as u64 * n * leaf_bits * (2u64).pow(depth as u32);
                let bytes = page::build_radix_inner(base, page::KIND_FREE_INNER, chunk);
                let r = self.write_page_out_map(cx, base, &bytes)?;
                up.push(Some(r));
            }
            level = up;
            depth += 1;
        }
        Ok(level.first().copied().flatten().unwrap_or_default())
    }

    /// Write a meta to its slot (§2.2: slot `1 + (k mod 2)`).
    fn write_meta(&mut self, cx: &Cx, m: &page::Meta) -> Result<(), Fail> {
        let bytes = page::build_meta(m);
        let slot = 1 + (m.ckpt_id % 2);
        cx.env()
            .write_at(self.page_file, slot * page::PAGE as u64, &bytes)
            .map_err(Fail::Sim)?;
        Ok(())
    }

    // ---- recovery (§14) ----

    /// Recover: two passes, skip `ts ≤ T`, chunks only of transactions
    /// with a marker > T, physical truncation (G2), resume `dropping`
    /// walks. Returns the frontier (the largest replayed ts, or `T`).
    pub fn recover(cx: &Cx, cfg: super::StoreCfg) -> Result<(Self, u64), Fail> {
        let (mut st, meta) = Self::open(cx, cfg)?;
        st.seq.next_ts = meta.t + 1;
        st.seq.visible_ts = meta.t;
        // Pass 1 + pass 2 walk the WAL from wal_start_lsn (§14 steps 3–4;
        // §12.5's stop rules: bad magic/CRC, `lsn` ≠ expected, `len` past
        // the segment end, and SegmentEnd hops).
        let mut replay: Vec<super::wal::Rec> = Vec::new();
        let mut valid_end = meta.wal_start_lsn;
        let mut lsn = meta.wal_start_lsn;
        let mut markers: BTreeMap<(u32, u64), u64> = BTreeMap::new();
        'outer: loop {
            let seg = lsn - (lsn % WAL_SEG);
            let name = format!("/wal/{seg:016x}.log");
            let fid = match cx.env().open(&name) {
                Ok(f) => f,
                Err(_) => break 'outer,
            };
            let mut off = lsn - seg;
            while off + 16 <= WAL_SEG {
                let want = (WAL_SEG - off).min(16 * 1024 + 16);
                let bytes = cx
                    .env()
                    .read_at(fid, off, want)
                    .map_err(Fail::Sim)?;
                match super::wal::dec_record_with_lsn_check(
                    &bytes,
                    lsn,
                    seg + WAL_SEG,
                    st.cfg.mutant,
                ) {
                    Ok(Some(rec)) => {
                        let len = rec_len(&rec);
                        // Accept the record the reader yields (card 1.4).
                        cx.env().accepted(fid, off, len);
                        if rec.kind == super::wal::K_SEGEND {
                            lsn = seg + WAL_SEG;
                            continue 'outer;
                        }
                        if rec.kind == super::wal::K_MARKER {
                            markers.insert(rec.txn, rec.ts);
                        }
                        replay.push(rec);
                        valid_end = lsn + len;
                        lsn += len;
                        off += len;
                    }
                    Ok(None) => {
                        break 'outer;
                    }
                    Err(()) => break 'outer,
                }
            }
            break 'outer;
        }
        // Pass 2 semantics: apply in LSN order.
        let mut frontier = meta.t;
        for rec in replay.iter() {
            match rec.kind {
                super::wal::K_COMMIT if rec.ts > meta.t => {
                    for op in super::wal::rec_ops(rec) {
                        st.apply_op(cx, &op, rec.ts)?;
                    }
                    st.seq.applied.insert(rec.ts);
                    frontier = frontier.max(rec.ts);
                }
                super::wal::K_CHUNK => {
                    if let Some(ts) = markers.get(&rec.txn).copied()
                        && ts > meta.t
                    {
                        for op in super::wal::rec_ops(rec) {
                            st.apply_op(cx, &op, ts)?;
                        }
                        st.seq.applied.insert(ts);
                        frontier = frontier.max(ts);
                    }
                }
                _ => {}
            }
        }
        // Advance visible_ts through contiguous applied commits.
        while st.seq.applied.remove(&(st.seq.visible_ts + 1)) {
            st.seq.visible_ts += 1;
        }
        st.seq.next_ts = st.seq.visible_ts + 1;
        // G2: physical truncation before returning (and before any new
        // append): zero from the valid end to the segment end and every
        // later segment's content, sync each, then sync_dir.
        if !matches!(st.cfg.mutant, Some(Mutant::TruncateLogicalOnly)) {
            st.truncate_wal(cx, valid_end)?;
        } else {
            st.wal.append = valid_end; // logical only
            st.wal.buf.clear();
        }
        // The WAL writer state restarts at the valid end.
        st.wal.append = valid_end;
        st.wal.durable = valid_end;
        st.wal.written = valid_end;
        st.wal.buf.clear();
        st.wal.buf_tags.clear();
        // Step 6: resume dropping walks.
        st.resume_drops(cx)?;
        Ok((st, frontier))
    }

    fn stale_stop(&mut self, _b: &[u8], _lsn: u64) -> bool {
        // Counted where a stale first record stopped the walk; the
        // reader's lsn check above reports it via dec_record's error.
        false
    }

    /// Apply one op at `ts` (the gated insert path; §9).
    pub fn apply_op(&mut self, cx: &Cx, op: &super::wal::WOp, ts: u64) -> Result<(), Fail> {
        match op {
            super::wal::WOp::CreateTree { tree } => self.dir_create_tree(cx, *tree).map(|_| ()),
            super::wal::WOp::DropTree { tree } => self.dir_drop_tree(cx, *tree).map(|_| ()),
            super::wal::WOp::Put { tree, key, value } => {
                let root = self.dir_root_of(*tree)?;
                let ekey = super::tree::enc_ekey(key, ts);
                let mut v = vec![0u8];
                v.extend_from_slice(value);
                self.tree_insert(cx, *tree, root, &ekey, &v).map(|_| ())
            }
            super::wal::WOp::Delete { tree, key } => {
                let root = self.dir_root_of(*tree)?;
                let ekey = super::tree::enc_ekey(key, ts);
                self.tree_insert(cx, *tree, root, &ekey, &[1u8]).map(|_| ())
            }
        }
    }

    /// A tree's root (XX001 if the directory lacks it and the op needs it).
    pub fn dir_root_of(&self, tree: u64) -> Result<u64, Fail> {
        self.dir
            .get(&tree)
            .map(|e| e.root)
            .ok_or_else(|| Fail::Corrupt(format!("tree {tree} missing in directory")))
    }

    /// G2: zero from the valid end through the rest of its segment and
    /// every later segment; `sync_data` each; `sync_dir`.
    pub fn truncate_wal(&mut self, cx: &Cx, valid_end: u64) -> Result<(), Fail> {
        let mut seg = valid_end - (valid_end % WAL_SEG);
        loop {
            let name = format!("/wal/{seg:016x}.log");
            let Ok(f) = cx.env().open(&name) else {
                break;
            };
            let off = if seg == valid_end - (valid_end % WAL_SEG) {
                valid_end - seg
            } else {
                0
            };
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

    // ---- amcheck (card 3.8) ----

    /// Amcheck a quiescent image read through SimDisk: §3.3 on every
    /// referenced page, tree order and bounds, entry decodes and tags,
    /// LPID exactness, free-set exactness, meta bounds. Returns findings.
    pub fn amcheck(cx: &Cx, cfg: &super::StoreCfg) -> Vec<crate::oracle::Finding> {
        let mut out = Vec::new();
        let pf = match cx.env().open(PAGE_FILE) {
            Ok(f) => f,
            Err(_) => {
                out.push(crate::oracle::Finding {
                    slot: None,
                    what: "open failed".into(),
                });
                return out;
            }
        };
        let m1 = cx.env().read_at(pf, page::PAGE as u64, page::PAGE as u64);
        let m2 = cx
            .env()
            .read_at(pf, 2 * page::PAGE as u64, page::PAGE as u64);
        let (meta, meta_slot) = match (m1, m2) {
            (Ok(a), Ok(b)) => {
                let pa = page::parse_meta(&a).ok();
                let pb = page::parse_meta(&b).ok();
                match (pa, pb) {
                    (Some(x), Some(y)) => {
                        if x.ckpt_id >= y.ckpt_id {
                            (x, 1)
                        } else {
                            (y, 2)
                        }
                    }
                    (Some(x), None) => (x, 1),
                    (None, Some(y)) => (y, 2),
                    (None, None) => {
                        out.push(crate::oracle::Finding {
                            slot: Some(1),
                            what: "no valid meta".into(),
                        });
                        return out;
                    }
                }
            }
            _ => {
                out.push(crate::oracle::Finding {
                    slot: None,
                    what: "meta read failed".into(),
                });
                return out;
            }
        };
        let _ = meta_slot;
        // Meta bounds.
        if meta.gc_w > meta.t {
            out.push(crate::oracle::Finding {
                slot: Some(meta_slot as u64),
                what: "gc_w > T".into(),
            });
        }
        // Load the map (findings on integrity failures).
        let mut probe = StoreInner::empty(cfg.clone(), pf);
        if let Err(e) = probe.load_image(cx, &meta) {
            out.push(crate::oracle::Finding {
                slot: None,
                what: format!("image walk: {e:?}"),
            });
            return out;
        }
        // LPID exactness: the mapped LPIDs equal the reachable pages from
        // the directory roots plus the directory's own pages.
        let mut reachable: BTreeSet<u64> = BTreeSet::new();
        let dir_root = meta.dir_root;
        if dir_root != 0 {
            Self::amcheck_tree(cx, &probe, dir_root, &mut reachable, &mut out, "dir");
        }
        for (id, entry) in probe.dir.iter() {
            if id != &super::DIR_TREE {
                Self::amcheck_tree(cx, &probe, entry.root, &mut reachable, &mut out, "tree");
            }
        }
        for lpid in probe.map.cur.keys() {
            if !reachable.contains(lpid) && *lpid != dir_root {
                out.push(crate::oracle::Finding {
                    slot: probe.map.cur.get(lpid).map(|r| r.slot),
                    what: format!("mapped lpid {lpid} unreachable"),
                });
            }
        }
        for lpid in reachable.iter() {
            if !probe.map.cur.contains_key(lpid) {
                out.push(crate::oracle::Finding {
                    slot: None,
                    what: format!("reachable lpid {lpid} unmapped"),
                });
            }
        }
        // Free-set exactness.
        let mut free_bits: BTreeSet<u64> = BTreeSet::new();
        if meta.free_root.slot != 0 {
            let total_bits = (meta.slot_count.saturating_sub(3)) + meta.next_lpid;
            let mut pages = Vec::new();
            if let Err(e) = Self::walk_freeset(
                cx,
                pf,
                meta.free_root,
                cfg.freeset_leaf_bits,
                total_bits,
                &mut free_bits,
                &mut pages,
            ) {
                out.push(crate::oracle::Finding {
                    slot: Some(meta.free_root.slot),
                    what: format!("free set walk: {e:?}"),
                });
            }
        }
        let referenced: BTreeSet<u64> = probe.slots.used_cur;
        for bit in free_bits.iter() {
            if *bit < meta.slot_count - 3 {
                let slot = bit + 3;
                if referenced.contains(&slot) {
                    out.push(crate::oracle::Finding {
                        slot: Some(slot),
                        what: "free bitmap marks a referenced slot".into(),
                    });
                }
            }
        }
        for slot in referenced.iter() {
            if *slot >= 3 && *slot < meta.slot_count && !free_bits.contains(&(*slot - 3)) {
                out.push(crate::oracle::Finding {
                    slot: Some(*slot),
                    what: "referenced slot not marked used".into(),
                });
            }
        }
        if meta.next_lpid <= *probe.map.cur.keys().max().unwrap_or(&0) {
            out.push(crate::oracle::Finding {
                slot: Some(meta_slot as u64),
                what: "next_lpid not above every mapped lpid".into(),
            });
        }
        out
    }

    fn amcheck_tree(
        cx: &Cx,
        probe: &StoreInner,
        root: u64,
        reachable: &mut BTreeSet<u64>,
        out: &mut Vec<crate::oracle::Finding>,
        what: &str,
    ) {
        // Walk through probe's refs (durable reads, §3.3 via load).
        let mut stack: Vec<u64> = vec![root];
        while let Some(l) = stack.pop() {
            if !reachable.insert(l) {
                out.push(crate::oracle::Finding {
                    slot: probe.map.cur.get(&l).map(|r| r.slot),
                    what: format!("{what}: lpid {l} reachable more than once"),
                });
                continue;
            }
            let Some(r) = probe.map.cur.get(&l) else { continue };
            let bytes = match cx.env().read_at(probe.page_file, r.slot * page::PAGE as u64, page::PAGE as u64) {
                Ok(b) => b,
                Err(_) => {
                    out.push(crate::oracle::Finding {
                        slot: Some(r.slot),
                        what: format!("{what}: read failed"),
                    });
                    continue;
                }
            };
            if page::check(&bytes, r, l, None).is_err() {
                out.push(crate::oracle::Finding {
                    slot: Some(r.slot),
                    what: format!("{what}: §3.3 fail on lpid {l}"),
                });
                continue;
            }
            cx.env().accepted(probe.page_file, r.slot * page::PAGE as u64, page::PAGE as u64);
            let kind = bytes.get(1).copied().unwrap_or(0);
            if kind == page::KIND_LEAF {
                if let Ok(cells) = super::tree::leaf_cells_pub(&bytes) {
                    let mut prev: Option<Vec<u8>> = None;
                    for (k, v) in cells {
                        if let Some(p) = &prev
                            && p >= &k
                        {
                            out.push(crate::oracle::Finding {
                                slot: Some(r.slot),
                                what: format!("{what}: leaf keys out of order"),
                            });
                        }
                        prev = Some(k.clone());
                        let tag = v.first().copied().unwrap_or(255);
                        if what != "dir" {
                            match super::tree::dec_ekey(&k) {
                                Some((_, ts)) if ts != 0 && ts != u64::MAX && tag <= 1 => {}
                                _ => out.push(crate::oracle::Finding {
                                    slot: Some(r.slot),
                                    what: format!("{what}: entry decode/tag fail"),
                                }),
                            }
                        }
                    }
                }
            } else if let Ok((seps, right)) = super::tree::inner_seps_pub(&bytes) {
                let mut prev: Option<Vec<u8>> = None;
                for (c, s) in seps {
                    if let Some(p) = &prev
                        && p >= &s
                    {
                        out.push(crate::oracle::Finding {
                            slot: Some(r.slot),
                            what: format!("{what}: separators out of order"),
                        });
                    }
                    prev = Some(s.clone());
                    stack.push(c);
                }
                stack.push(right);
            }
        }
    }
}

fn cx_open_chunks(_cx: &Cx) -> BTreeMap<(u32, u64), u64> {
    BTreeMap::new()
}

#[allow(dead_code)]
fn unused_open_chunks() {}

fn rec_len(rec: &super::wal::Rec) -> u64 {
    // The record's own length: header + body (derived from the payload).
    (rec.payload.len() + 45) as u64
}

fn hash_bytes(b: &[u8]) -> u64 {
    crate::crc::crc32c(0, b) as u64
}

impl StoreInner {
    /// Used by the harness's bulk-load publish wait (§12.8).
    pub fn wait_meta_durable(&mut self, cx: &Cx, k: u64) -> Result<(), Fail> {
        loop {
            let ok = self.meta_durable_k >= k;
            cx.yield_now()?;
            if ok {
                return Ok(());
            }
            if matches!(self.cfg.mutant, Some(Mutant::PublishBeforeMeta)) {
                return Ok(());
            }
            cx.wait_for(crate::sim::WaitKey::MetaDurable)?;
        }
    }
}

/// Extra: a version of `dec_record` with the LSN-CHECK-SKIP mutant hook.
pub fn dec_record_with_lsn_check_unused() {}
