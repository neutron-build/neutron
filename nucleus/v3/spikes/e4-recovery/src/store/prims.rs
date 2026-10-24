#![deny(clippy::indexing_slicing)]
//! The store primitives: epoch gate, per-tree mutex, LPID and slot
//! allocation, pool fetch/store (the `write_guard`), and clock eviction
//! with dirty write-out (S-STORE §4, §5; card 3.3–3.5).
//!
//! Every primitive is a short lock-work-unlock step; page bytes travel as
//! owned copies between them (S-BATON). Yields: gate attempts (§5.6),
//! every `store_page` (the `write_guard` yield point), pin waits, and the
//! env calls inside the write-out paths.
//!
//! The image map (the CoW delta of §4.1/§13 step 2): while a checkpoint's
//! flip window is open (step 0 → step 5), the **first** post-flip touch of
//! each LPID freezes its image-k state — `At(home)` for a clean page (its
//! bytes are safe at the home slot), `CopyPending` for a dirty one (the
//! bytes exist only in the frame; §5.6's epoch copy preserves them and
//! step 2 or a reserve eviction writes them to a fresh slot), `Free` for a
//! page that did not exist at the flip. After step 5 the overrides are
//! resolved into the new durable base and cleared. This is the in-memory
//! stand-in for the spec's copy-on-write radix leaves (declared
//! simplification, README): the persisted format is the spec's.

use super::{page, Fail, Img, StoreInner, Cx};
use crate::sim::{E4Env, WaitKey};
use std::collections::BTreeSet;

impl StoreInner {
    // ---- epoch gate (§5.6) ----

    /// Acquire the gate shared. Blocks behind a pending flip (the flip
    /// waits for holders; new acquisitions queue).
    pub fn gate_shared(&mut self, cx: &Cx) -> Result<(), Fail> {
        loop {
            let wait = {
                let g = &mut self.gate;
                if g.exclusive || g.flip_pending {
                    true
                } else {
                    g.holders += 1;
                    false
                }
            };
            // The gate-acquire attempt is a yield point (card 1.2).
            cx.yield_now()?;
            if !wait {
                return Ok(());
            }
            cx.wait_for(WaitKey::GateShared)?;
        }
    }

    /// Release one shared hold.
    pub fn gate_release(&mut self, cx: &Cx) {
        let wake_flip = {
            let g = &mut self.gate;
            g.holders = g.holders.saturating_sub(1);
            g.flip_pending && g.holders == 0
        };
        if wake_flip {
            cx.w.wake(WaitKey::GateFlip);
        }
    }

    /// Take the gate exclusively (step 0's flip): waits for the shared
    /// holders to drain while blocking new acquisitions. With the
    /// FLIP-NO-GATE mutant this does not wait (a gated operation can then
    /// straddle the flip).
    pub fn gate_exclusive(&mut self, cx: &Cx) -> Result<(), Fail> {
        let no_gate = matches!(self.cfg.mutant, Some(super::Mutant::FlipNoGate));
        {
            let g = &mut self.gate;
            if g.exclusive {
                return Err(Fail::Invalid("nested flip".to_owned()));
            }
            g.flip_pending = !no_gate;
        }
        if no_gate {
            self.gate.exclusive = true;
            return Ok(());
        }
        cx.w.wake(WaitKey::GateShared);
        loop {
            let ready = self.gate.holders == 0;
            cx.yield_now()?;
            if ready {
                self.gate.exclusive = true;
                return Ok(());
            }
            cx.wait_for(WaitKey::GateFlip)?;
        }
    }

    /// Release the exclusive hold (end of the flip's critical part).
    pub fn gate_flip_release(&mut self, cx: &Cx) {
        let g = &mut self.gate;
        g.exclusive = false;
        g.flip_pending = false;
        cx.w.wake(WaitKey::GateShared);
    }

    // ---- per-tree sim mutex (card 3.6) ----

    /// Acquire tree `t`'s sim mutex, held for one gated operation.
    pub fn tree_lock(&mut self, cx: &Cx, t: u64) -> Result<(), Fail> {
        loop {
            let busy = self.tree_mu.get(&t).copied().unwrap_or(usize::MAX) != usize::MAX;
            cx.yield_now()?;
            if !busy {
                self.tree_mu.insert(t, cx.t);
                return Ok(());
            }
            cx.wait_for(WaitKey::Tree(t))?;
        }
    }

    /// Release tree `t`'s mutex.
    pub fn tree_unlock(&mut self, cx: &Cx, t: u64) {
        self.tree_mu.remove(&t);
        cx.w.wake(WaitKey::Tree(t));
    }

    // ---- LPIDs (§4.2) and slots (§4.4–§4.5) ----

    /// Allocate the lowest free LPID, or `next_lpid++` (never 0).
    pub fn alloc_lpid(&mut self) -> u64 {
        if let Some(&l) = self.slots.free_lpids.iter().next() {
            self.slots.free_lpids.remove(&l);
            l
        } else {
            let l = self.meta.next_lpid.max(1);
            self.meta.next_lpid = l + 1;
            l
        }
    }

    /// Free an LPID (immediate reuse in memory, §4.2). During a flip
    /// window the first touch freezes the image state instead of dropping
    /// it.
    pub fn free_lpid(&mut self, l: u64) {
        if self.flip_window && !self.map.img.contains_key(&l) {
            let frozen = match self.map.cur.get(&l) {
                Some(r) => Img::At(*r),
                None => Img::Free,
            };
            self.map.img.insert(l, frozen);
        }
        if !self.flip_window {
            self.map.img.remove(&l);
        }
        self.map.cur.remove(&l);
        if let Some(r) = self.slots.used_cur.iter().find(|s| **s < 3).copied() {
            let _ = r; // slots 0-2 are never homes; nothing to do
        }
        if l + 1 == self.meta.next_lpid && l > 0 {
            self.meta.next_lpid -= 1;
        } else {
            self.slots.free_lpids.insert(l);
        }
    }

    /// Allocate the lowest free slot, growing the file by
    /// `max(16, slot_count/16)` when none is free (card 3.3 / §4.5).
    pub fn alloc_slot(&mut self, cx: &Cx) -> Result<u64, Fail> {
        let mut s = 3u64;
        loop {
            if s >= self.slots.slot_count {
                let grow = (self.slots.slot_count / 16).max(16);
                self.slots.slot_count += grow;
                let newlen = self.slots.slot_count * page::PAGE as u64;
                let f = self.page_file;
                cx.env().set_len(f, newlen).map_err(Fail::Sim)?;
                s = 3;
                continue;
            }
            if !self.slots.used_cur.contains(&s)
                && !self.slots.used_durable.contains(&s)
                && !self.slots.image_refs.contains(&s)
            {
                return Ok(s);
            }
            s += 1;
        }
    }

    // ---- pool (§5) ----

    /// Find or load the frame of `lpid`, pinning it. Loads go through the
    /// map reference with §3.3's four checks; accepted bytes are reported
    /// to the accept hook (card 1.4). Returns the owned page bytes and
    /// the frame's content epoch.
    pub fn fetch_page(&mut self, cx: &Cx, lpid: u64) -> Result<(Vec<u8>, u32), Fail> {
        self.alive()?;
        loop {
            let hit = {
                let f = self
                    .pool
                    .frames
                    .iter_mut()
                    .find(|f| f.key == lpid && !f.is_epoch_copy);
                match f {
                    Some(f) => {
                        f.pins += 1;
                        f.refed = true;
                        Some((f.bytes.clone(), f.epoch))
                    }
                    None => None,
                }
            };
            if let Some(hit) = hit {
                return Ok(hit);
            }
            let full = self
                .pool
                .frames
                .iter()
                .all(|f| f.key != 0 && (f.pins > 0 || f.is_epoch_copy));
            if full {
                // A pin wait is a yield point (card 1.2); try to make a
                // frame first (this fetch runs inside the caller's holds,
                // so the eviction may use the gate).
                let evicted = self.evict_one(cx, true)?;
                if !evicted {
                    cx.wait_for(WaitKey::Pin)?;
                }
                continue;
            }
            let r = match self.map.cur.get(&lpid) {
                Some(r) => *r,
                None => return Err(Fail::Corrupt(format!("lpid {lpid} unmapped"))),
            };
            let f = self.page_file;
            let off = r.slot * page::PAGE as u64;
            let bytes = cx.env().read_at(f, off, page::PAGE as u64).map_err(Fail::Sim)?;
            page::check(&bytes, &r, lpid, self.cfg.mutant)
                .map_err(|()| Fail::Corrupt(format!("integrity fail lpid {lpid} slot {}", r.slot)))?;
            // Accept hook: a page passing §3.3 (card 1.4).
            cx.env().accepted(f, off, page::PAGE as u64);
            if let Some(fr) = self
                .pool
                .frames
                .iter_mut()
                .find(|f| f.key == 0 && !f.is_epoch_copy)
            {
                fr.key = lpid;
                fr.bytes = bytes.clone();
                fr.epoch = r.epoch;
                fr.dirty = false;
                fr.pins = 1;
                fr.refed = true;
            }
            return Ok((bytes, r.epoch));
        }
    }

    /// Unpin a page (op end).
    pub fn unpin(&mut self, cx: &Cx, lpid: u64) {
        for f in self.pool.frames.iter_mut() {
            if f.key == lpid && !f.is_epoch_copy && f.pins > 0 {
                f.pins -= 1;
            }
        }
        cx.w.wake(WaitKey::Pin);
    }

    /// `write_guard(frame)` + install: the only way pages mutate (§5.6).
    /// A yield point first; then the first post-flip touch of the LPID
    /// freezes its image state (an epoch copy when the frame was dirty),
    /// and the frame receives the new bytes with content epoch `e`.
    pub fn store_page(&mut self, cx: &Cx, lpid: u64, bytes: Vec<u8>) -> Result<(), Fail> {
        self.alive()?;
        cx.yield_now()?;
        let no_copy = matches!(self.cfg.mutant, Some(super::Mutant::WriteGuardNoCopy));
        if self.flip_window && !self.map.img.contains_key(&lpid) {
            let frame = self
                .pool
                .frames
                .iter()
                .find(|f| f.key == lpid && !f.is_epoch_copy)
                .map(|f| (f.bytes.clone(), f.dirty));
            match frame {
                Some((old, true)) => {
                    // The bytes exist only in the frame: §5.6's epoch copy.
                    if !no_copy {
                        self.record_image_copy(cx, lpid, old)?;
                        self.map.img.insert(lpid, Img::CopyPending);
                    } else {
                        // WRITE-GUARD-NO-COPY: no copy and no freeze — the
                        // mutant's damage.
                        self.map.img.insert(lpid, Img::Lost);
                    }
                }
                Some((_, false)) => {
                    let home = self.map.cur.get(&lpid).copied();
                    match home {
                        Some(r) => {
                            self.map.img.insert(lpid, Img::At(r));
                        }
                        None => {
                            self.map.img.insert(lpid, Img::Free);
                        }
                    }
                }
                None => match self.map.cur.get(&lpid) {
                    Some(r) => {
                        self.map.img.insert(lpid, Img::At(*r));
                    }
                    None => {
                        self.map.img.insert(lpid, Img::Free);
                    }
                },
            }
        }
        let want_epoch = self.e;
        loop {
            let placed = {
                if let Some(f) = self
                    .pool
                    .frames
                    .iter_mut()
                    .find(|f| f.key == lpid && !f.is_epoch_copy)
                {
                    f.bytes = bytes.clone();
                    f.epoch = want_epoch;
                    f.dirty = true;
                    f.refed = true;
                    true
                } else if let Some(f) = self
                    .pool
                    .frames
                    .iter_mut()
                    .find(|f| f.key == 0 && !f.is_epoch_copy)
                {
                    f.key = lpid;
                    f.bytes = bytes.clone();
                    f.epoch = want_epoch;
                    f.dirty = true;
                    f.pins = 1;
                    f.refed = true;
                    true
                } else {
                    false
                }
            };
            if placed {
                return Ok(());
            }
            let evicted = self.evict_one(cx, true)?;
            if !evicted {
                cx.wait_for(WaitKey::Pin)?;
            }
        }
    }

    /// Record an epoch copy of `bytes` for the image being built: into a
    /// reserve frame, or — when the `R_EPOCH` reserve is exhausted — by
    /// writing the oldest reserve copy out to a fresh slot recorded in the
    /// image map and reusing its frame (§5.6).
    fn record_image_copy(&mut self, cx: &Cx, lpid: u64, bytes: Vec<u8>) -> Result<(), Fail> {
        let copies = self.pool.frames.iter().filter(|f| f.is_epoch_copy).count();
        if copies >= self.cfg.r_epoch {
            self.epoch_exhausted += 1;
            let oldest = self
                .pool
                .frames
                .iter()
                .enumerate()
                .filter(|(_, f)| f.is_epoch_copy)
                .map(|(i, _)| i)
                .min();
            if let Some(i) = oldest {
                let (k, bytes_out) = {
                    let f = self
                        .pool
                        .frames
                        .get(i)
                        .ok_or_else(|| Fail::Invalid("frame vanished".to_owned()))?;
                    (f.key, f.bytes.clone())
                };
                let r = self.write_page_out(cx, k, &bytes_out)?;
                self.map.img.insert(k, Img::At(r));
                if let Some(f) = self.pool.frames.get_mut(i) {
                    f.key = 0;
                    f.bytes = Vec::new();
                    f.dirty = false;
                    f.is_epoch_copy = false;
                    f.pins = 0;
                }
            }
        }
        if let Some(f) = self
            .pool
            .frames
            .iter_mut()
            .find(|f| f.key == 0 && !f.is_epoch_copy)
        {
            f.key = lpid;
            f.bytes = bytes;
            f.epoch = self.e.saturating_sub(1);
            f.dirty = true;
            f.is_epoch_copy = true;
            f.pins = 1;
            f.refed = true;
        }
        Ok(())
    }

    /// Write a page image out to a **fresh** slot (I-NO-INPLACE), moving
    /// the current home (§4.4: the old home leaves `used_cur`; the durable
    /// and image sets protect it until its image stops referencing it).
    /// With the INPLACE-EVICT mutant the caller writes to the current home
    /// instead (see `evict_one`).
    pub fn write_page_out(
        &mut self,
        cx: &Cx,
        lpid: u64,
        bytes: &[u8],
    ) -> Result<page::Ref, Fail> {
        let slot = self.alloc_slot(cx)?;
        let off = slot * page::PAGE as u64;
        let f = self.page_file;
        cx.env().write_at(f, off, bytes).map_err(Fail::Sim)?;
        let r = page::Ref {
            slot,
            epoch: page::header_epoch(bytes),
            crc: page::header_crc(bytes),
        };
        if let Some(old) = self.map.cur.insert(lpid, r) {
            self.slots.used_cur.remove(&old.slot);
        }
        self.slots.used_cur.insert(slot);
        Ok(r)
    }

    /// One clock-eviction step (§5.2): an unpinned, non-epoch-copy victim
    /// (the pin check stands in for try-latch, card 3.6). Dirty victims
    /// are written to a fresh slot; with the INPLACE-EVICT mutant to the
    /// current home instead. An eviction outside a gated operation only
    /// *tries* the gate: with a flip pending it takes a clean victim, and
    /// if there is none returns `false` (the caller restarts later).
    pub fn evict_one(&mut self, cx: &Cx, inside_op: bool) -> Result<bool, Fail> {
        let n = self.pool.frames.len();
        if n == 0 {
            return Ok(false);
        }
        for _ in 0..2 * n {
            let i = self.pool.hand % n;
            self.pool.hand += 1;
            let take = match self.pool.frames.get(i) {
                Some(f) => f.key != 0 && !f.is_epoch_copy && f.pins == 0 && !f.refed,
                None => false,
            };
            if take {
                let (key, dirty, bytes) = {
                    let f = self
                        .pool
                        .frames
                        .get(i)
                        .ok_or_else(|| Fail::Invalid("frame vanished".to_owned()))?;
                    (f.key, f.dirty, f.bytes.clone())
                };
                if !dirty {
                    if let Some(f) = self.pool.frames.get_mut(i) {
                        f.key = 0;
                        f.bytes = Vec::new();
                    }
                    return Ok(true);
                }
                if !inside_op && self.gate.flip_pending {
                    // §5.2: no clean victim under a pending flip — restart.
                    return Ok(false);
                }
                if matches!(self.cfg.mutant, Some(super::Mutant::InplaceEvict)) {
                    let r = self.map.cur.get(&key).copied().unwrap_or_default();
                    let off = r.slot * page::PAGE as u64;
                    cx.env()
                        .write_at(self.page_file, off, &bytes)
                        .map_err(Fail::Sim)?;
                } else {
                    self.write_page_out(cx, key, &bytes)?;
                }
                if let Some(f) = self.pool.frames.get_mut(i) {
                    f.key = 0;
                    f.bytes = Vec::new();
                    f.dirty = false;
                }
                return Ok(true);
            }
            if let Some(f) = self.pool.frames.get_mut(i) {
                f.refed = false;
            }
        }
        Ok(false)
    }

    /// The set of slots referenced by the image being built (the effective
    /// image map's slots; `CopyPending` entries are not yet placed).
    pub fn image_slot_set(&self) -> BTreeSet<u64> {
        let mut out = BTreeSet::new();
        for (l, cur) in self.map.cur.iter() {
            let eff = self.map.img.get(l).copied().unwrap_or(Img::Same);
            if let Img::Free | Img::CopyPending | Img::Lost = eff {
                if matches!(eff, Img::Free | Img::Lost) {
                    continue;
                }
                continue;
            }
            match eff {
                Img::Same => {
                    out.insert(cur.slot);
                }
                Img::At(r) => {
                    out.insert(r.slot);
                }
                _ => {}
            }
        }
        out
    }
}
