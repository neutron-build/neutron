//! The harness-side monitors (card X-5 Work 3.10; S-STORE's diagnostic
//! invariants). They read only SimDisk state — the round's disk events
//! and the durable-write history — plus the flip's reported enqueue
//! positions (the card's named M-CKPT-T hook); never store memory that
//! the disk does not show.
//!
//! - **M-NOINPLACE** (I-NO-INPLACE, I-FREE-SAFE): at each `sync_all` of
//!   the page file that makes a new meta durable, walk the newly durable
//!   image from the shadow bytes to get its slot set; any later page
//!   write to one of those slots while that meta is still the durable
//!   one is a violation.
//! - **M-META-ATOMIC** (I-META-ATOMIC): when a meta write is issued,
//!   every slot it transitively references must already be durable.
//! - **M-WAL-ORDER** (I-WAL-TS-ORDER): across `Commit` and
//!   `CommitMarker` records, `commit_ts` strictly increases with lsn.
//! - **M-CKPT-T** (I-CKPT-AT-T): when meta k's write is issued, the WAL
//!   must be durable through the end of every record enqueued before
//!   flip k (the flip's reported position).
//!
//! Hits are reported per round, apart from O-1/O-2/O-3.

#![allow(clippy::collapsible_if, clippy::collapsible_match)]
use std::collections::{BTreeMap, BTreeSet};

use crate::sim::{DiskEvent, PAGE_FILE, SimDisk, WAL_DIR};
use crate::store::page::{self};
use crate::store::wal::{self, K_COMMIT, K_MARKER, K_SEGEND};

/// A shadow of one file's bytes, replayed from the disk events (the
/// monitor's own view; the write-issued shadow is what a completed sync
/// will make durable).
#[derive(Default)]
struct Shadow {
    /// Issued (current view) bytes.
    issued: Vec<u8>,
    /// Durable bytes (advanced at sync events).
    durable: Vec<u8>,
}

impl Shadow {
    fn write(&mut self, off: u64, bytes: &[u8]) {
        let end = off as usize + bytes.len();
        if end > self.issued.len() {
            self.issued.resize(end, 0);
        }
        if let Some(dst) = self.issued.get_mut(off as usize..end) {
            dst.copy_from_slice(bytes);
        }
    }
}

/// The monitor configuration: the persisted radix's shape (the store
/// configuration's test-scale overrides).
#[derive(Debug, Clone, Copy)]
pub struct MonitorCfg {
    /// `map_leaf_cap`.
    pub map_leaf_cap: u64,
    /// `freeset_leaf_bits`.
    pub freeset_leaf_bits: u64,
}

/// Run the monitors over a round's disk events; returns the hit names.
pub fn run(events: &[DiskEvent], flip_enqueue_pos: &[u64], cfg: &MonitorCfg) -> Vec<&'static str> {
    let mut hits: Vec<&'static str> = Vec::new();
    m_wal_order(events, &mut hits);
    m_meta_atomic_and_noinplace(events, cfg, &mut hits, &mut BTreeSet::new());
    m_ckpt_t(events, flip_enqueue_pos, &mut hits);
    hits.sort_unstable();
    hits.dedup();
    hits
}

/// X5-R5 condition 3 (card Work 3.10's "per-write verdicts"): the slots
/// whose page writes drew an M-NOINPLACE hit this round. The harness
/// excludes these from the exception set `R` (a slot written in
/// violation of I-NO-INPLACE is a bug the monitor names, not a
/// rollback consequence the exception may excuse).
pub fn noinplace_slot_hits(events: &[DiskEvent], cfg: &MonitorCfg) -> BTreeSet<u64> {
    let mut hits = Vec::new();
    let mut slots = BTreeSet::new();
    m_meta_atomic_and_noinplace(events, cfg, &mut hits, &mut slots);
    slots
}

/// Public batch parser (the harness's written/acked derivation shares
/// it).
pub fn parse_batch_pub(bytes: &[u8]) -> Vec<(u64, u8, u64)> {
    parse_batch(bytes)
}

/// Parse every WAL record in a write batch (its bytes) sequentially:
/// `(lsn, kind, ts)` triples, skipping `SegmentEnd` fillers.
fn parse_batch(bytes: &[u8]) -> Vec<(u64, u8, u64)> {
    // Each record is decoded at its own lsn (§12.2 stores it at body
    // offset 0); the §12.5 stale check needs the exact position.
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 16 <= bytes.len() {
        let len = {
            let s = bytes.get(off + 4..off + 8).unwrap_or(&[]);
            let mut b = [0u8; 4];
            if s.len() == 4 {
                b.copy_from_slice(s);
            }
            u32::from_le_bytes(b) as usize
        };
        if len < 16 || off + len > bytes.len() {
            break;
        }
        let lsn = bytes
            .get(off + 16..off + 24)
            .map(|s| {
                let mut a = [0u8; 8];
                a.copy_from_slice(s);
                u64::from_le_bytes(a)
            })
            .unwrap_or(u64::MAX);
        match wal::dec_record(bytes.get(off..off + len).unwrap_or(&[]), lsn, u64::MAX) {
            Ok(Some(rec)) => {
                if rec.kind == K_SEGEND {
                    off += len;
                    continue;
                }
                out.push((lsn, rec.kind, rec.ts));
                off += len;
            }
            _ => break,
        }
    }
    out
}

/// M-WAL-ORDER: commit_ts strictly increases with lsn across
/// Commit/CommitMarker records.
fn m_wal_order(events: &[DiskEvent], hits: &mut Vec<&'static str>) {
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    for e in events {
        if let DiskEvent::WriteIssued {
            inode,
            off: _,
            bytes,
        } = e
        {
            // Only WAL files (created under /wal/): identified by writes
            // that parse as NWAL batches with record lsns.
            for (lsn, kind, ts) in parse_batch(bytes) {
                if kind == K_COMMIT || kind == K_MARKER {
                    pairs.push((lsn, ts));
                    let _ = inode;
                }
            }
        }
    }
    pairs.sort_unstable();
    for w in pairs.windows(2) {
        let (l0, t0) = w[0];
        let (l1, t1) = w[1];
        if t0 >= t1 {
            hits.push("M-WAL-ORDER");
            return;
        }
        let _ = (l0, l1);
    }
}

/// M-META-ATOMIC + M-NOINPLACE: replay the page-file events through a
/// shadow; at each meta-making `sync_all` compute the new durable
/// image's slot set; check that a meta write's references were durable
/// at issue, and that no later write touched the durable image's slots
/// before the next meta became durable.
fn m_meta_atomic_and_noinplace(
    events: &[DiskEvent],
    cfg: &MonitorCfg,
    hits: &mut Vec<&'static str>,
    noinplace_slots: &mut BTreeSet<u64>,
) {
    let mut shadow = Shadow::default();
    let mut durable_meta: Option<page::Meta> = None;
    let mut durable_slots: Vec<u64> = Vec::new();
    for e in events {
        match e {
            DiskEvent::WriteIssued { inode, off, bytes } => {
                if !is_page_file(*inode, events) {
                    continue;
                }
                // M-META-ATOMIC: a **checkpoint** meta write's references
                // must already be durable (§13's step 3 → step 4 order).
                // Image 0's creation meta (§2.3) is written before its
                // covering sync by design, so k = 0 is out of scope.
                if (*off == 4096 || *off == 8192)
                    && let Ok(m) = page::parse_meta(bytes)
                    && m.ckpt_id >= 1
                {
                    let refs = shadow_refs(&shadow.durable, &m, cfg);
                    for r in refs.iter() {
                        if !slot_durable(&shadow.durable, *r) {
                            hits.push("M-META-ATOMIC");
                        }
                    }
                }
                // M-NOINPLACE: writing a page home the durable meta
                // still references. Slots 0–2 are fixed (never page
                // homes, §4.4). Per-write verdicts: every hit also names
                // its slot (X5-R5's condition 3, via
                // [`noinplace_slot_hits`]).
                if let Some(m) = &durable_meta {
                    let slot = off / 4096;
                    if slot >= 3 && durable_slots.contains(&slot) && m.map_root.slot != 0 {
                        hits.push("M-NOINPLACE");
                        noinplace_slots.insert(slot);
                    }
                }
                shadow.write(*off, bytes);
            }
            DiskEvent::SyncReturned { inode, all } => {
                if *all
                    && is_page_file(*inode, events)
                    && let Some(m) = {
                        // The issued shadow becomes durable.
                        shadow.durable = shadow.issued.clone();
                        durable_meta_now(&shadow.durable)
                    }
                {
                    let newer = match &durable_meta {
                        Some(prev) => m.ckpt_id > prev.ckpt_id,
                        None => true,
                    };
                    if newer {
                        durable_meta = Some(m);
                        durable_slots = shadow_refs(&shadow.durable, &m, cfg);
                    }
                }
            }
            _ => {}
        }
    }
}

/// M-CKPT-T: at each meta write issue, the WAL must be durable through
/// the flip's reported enqueue position (the flips and the metas appear
/// in order).
fn m_ckpt_t(events: &[DiskEvent], flips: &[u64], hits: &mut Vec<&'static str>) {
    // The durable WAL extent at each point in the event stream.
    let mut wal_durable: BTreeMap<usize, u64> = BTreeMap::new();
    let mut written: BTreeMap<usize, u64> = BTreeMap::new();
    let mut flip_idx = 0usize;
    for e in events {
        match e {
            DiskEvent::WriteReturned { inode, off, len } => {
                if is_wal_file(*inode, events) {
                    let w = written.entry(*inode).or_default();
                    *w = (*w).max(off + len);
                }
            }
            DiskEvent::SyncReturned { inode, .. } => {
                if is_wal_file(*inode, events) {
                    let d = wal_durable.entry(*inode).or_default();
                    let w = written.get(inode).copied().unwrap_or(0);
                    *d = (*d).max(w);
                }
            }
            DiskEvent::WriteIssued { inode, off, bytes } => {
                if *inode != 0 && is_page_file(*inode, events) && (*off == 4096 || *off == 8192) {
                    if let Ok(m) = page::parse_meta(bytes) {
                        // Meta k = checkpoint k: its flip is the k-th
                        // reported position (image 0's creation is not a
                        // flip).
                        let k = m.ckpt_id;
                        if k >= 1 {
                            let idx = (k - 1) as usize;
                            if let Some(&want) = flips.get(idx) {
                                let durable: u64 = wal_durable.values().copied().sum();
                                if durable < want {
                                    hits.push("M-CKPT-T");
                                }
                            }
                        }
                        let _ = flip_idx;
                        flip_idx += 1;
                    }
                }
            }
            _ => {}
        }
    }
}

fn durable_meta_now(durable: &[u8]) -> Option<page::Meta> {
    let read = |slot: usize| -> Vec<u8> {
        let off = slot * 4096;
        let mut v = vec![0u8; 4096];
        if off < durable.len() {
            let end = (off + 4096).min(durable.len());
            if let Some(src) = durable.get(off..end) {
                v[..end - off].copy_from_slice(src);
            }
        }
        v
    };
    match (page::parse_meta(&read(1)), page::parse_meta(&read(2))) {
        (Ok(a), Ok(b)) => Some(if a.ckpt_id >= b.ckpt_id { a } else { b }),
        (Ok(a), Err(())) => Some(a),
        (Err(()), Ok(b)) => Some(b),
        (Err(()), Err(())) => None,
    }
}

/// The slots a meta transitively references, from shadow bytes (map
/// radix, free set, tree pages).
fn shadow_refs(durable: &[u8], m: &page::Meta, cfg: &MonitorCfg) -> Vec<u64> {
    let mut out = Vec::new();
    let read = |slot: u64| -> Vec<u8> {
        let off = slot as usize * 4096;
        let mut v = vec![0u8; 4096];
        if off < durable.len() {
            let end = (off + 4096).min(durable.len());
            if let Some(src) = durable.get(off..end) {
                v[..end - off].copy_from_slice(src);
            }
        }
        v
    };
    // Map radix + free set.
    let mut flat: BTreeMap<u64, page::Ref> = BTreeMap::new();
    let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
    let span = crate::store::ckpt::radix_child_span(cfg.map_leaf_cap, n, m.next_lpid);
    walk_shadow_map(
        durable,
        m.map_root,
        0,
        span,
        cfg.map_leaf_cap,
        0,
        &mut flat,
        &mut out,
    );
    if m.free_root.slot != 0 {
        let total_bits = (m.slot_count.saturating_sub(3)) + m.next_lpid;
        let span = crate::store::ckpt::radix_child_span(cfg.freeset_leaf_bits, n, total_bits);
        walk_shadow_free(durable, m.free_root, 0, span, &mut out);
    }
    // The trees from the directory: the directory's own pages first
    // (its root may be an inner page), then every tree root its leaf
    // cells name (B5 fix: the draft stopped at the directory page, so
    // the durable slot set missed every tree page — M-NOINPLACE was
    // blind to writes the durable image still referenced).
    if let Some(r) = flat.get(&m.dir_root).copied() {
        let mut stack = vec![(m.dir_root, r)];
        let mut roots: Vec<u64> = Vec::new();
        while let Some((l, r)) = stack.pop() {
            if out.contains(&r.slot) {
                continue;
            }
            out.push(r.slot);
            let b = read(r.slot);
            if page::check(&b, &r, l, None).is_err() {
                continue;
            }
            if page::header_kind(&b) == page::KIND_INNER {
                if let Ok((seps, right)) = crate::store::tree::inner_seps(&b) {
                    for (c, _) in seps {
                        if let Some(cr) = flat.get(&c).copied() {
                            stack.push((c, cr));
                        }
                    }
                    if let Some(rr) = flat.get(&right).copied() {
                        stack.push((right, rr));
                    }
                }
            } else if let Ok(cells) = crate::store::tree::leaf_cells(&b) {
                for (k, v) in cells {
                    if k.len() != 8 || v.len() != 9 {
                        continue;
                    }
                    let mut rb = [0u8; 8];
                    if let Some(s) = v.get(0..8) {
                        rb.copy_from_slice(s);
                    }
                    roots.push(u64::from_le_bytes(rb));
                }
            }
        }
        for root in roots {
            if let Some(rr) = flat.get(&root).copied() {
                stack.push((root, rr));
                while let Some((l, r)) = stack.pop() {
                    if out.contains(&r.slot) {
                        continue;
                    }
                    out.push(r.slot);
                    let b = read(r.slot);
                    if page::check(&b, &r, l, None).is_err() {
                        continue;
                    }
                    if page::header_kind(&b) == page::KIND_INNER {
                        if let Ok((seps, right)) = crate::store::tree::inner_seps(&b) {
                            for (c, _) in seps {
                                if let Some(cr) = flat.get(&c).copied() {
                                    stack.push((c, cr));
                                }
                            }
                            if let Some(rr) = flat.get(&right).copied() {
                                stack.push((right, rr));
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn walk_shadow_map(
    durable: &[u8],
    r: page::Ref,
    base: u64,
    span: u64,
    cap: u64,
    depth: u8,
    flat: &mut BTreeMap<u64, page::Ref>,
    out: &mut Vec<u64>,
) {
    if depth > 8 || r.slot == 0 {
        return;
    }
    // Each page is read from the shadow's durable bytes **by its own
    // ref** — the draft recursed with the parent's `b`, so with any
    // multi-level radix `flat` never filled (the tree descent below died
    // at the directory) and the durable slot set undercounted by every
    // tree page (the gate's R1: FREE-EARLY's reuse was real but
    // M-NOINPLACE stayed blind to it).
    let b = {
        let off = r.slot as usize * 4096;
        let mut v = vec![0u8; 4096];
        if off < durable.len() {
            let end = (off + 4096).min(durable.len());
            if let Some(src) = durable.get(off..end) {
                v[..end - off].copy_from_slice(src);
            }
        }
        v
    };
    out.push(r.slot);
    if page::header_kind(&b) == page::KIND_MAP_LEAF {
        if let Ok(entries) = page::parse_map_leaf(&b, base, cap, &r, None) {
            for (i, e) in entries.iter().enumerate() {
                if let Some(rr) = e {
                    flat.insert(base + i as u64, *rr);
                    out.push(rr.slot);
                }
            }
        }
    } else if let Ok(refs) = page::parse_radix_inner(
        &b,
        base,
        (page::PAGE - page::HDR) / page::MAP_ENTRY,
        &r,
        None,
    ) {
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        for (i, rr) in refs.into_iter().enumerate() {
            if let Some(rr) = rr {
                let child_span = (span / n).max(1);
                let child_base = base + i as u64 * span;
                walk_shadow_map(
                    durable,
                    rr,
                    child_base,
                    child_span,
                    cap,
                    depth + 1,
                    flat,
                    out,
                );
            }
        }
    }
}

fn walk_shadow_free(durable: &[u8], r: page::Ref, base: u64, span: u64, out: &mut Vec<u64>) {
    if r.slot == 0 {
        return;
    }
    let b = {
        let off = r.slot as usize * 4096;
        let mut v = vec![0u8; 4096];
        if off < durable.len() {
            let end = (off + 4096).min(durable.len());
            if let Some(src) = durable.get(off..end) {
                v[..end - off].copy_from_slice(src);
            }
        }
        v
    };
    out.push(r.slot);
    if page::header_kind(&b) == page::KIND_FREE_INNER {
        if let Ok(refs) = page::parse_radix_inner(
            &b,
            base,
            (page::PAGE - page::HDR) / page::MAP_ENTRY,
            &r,
            None,
        ) {
            let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
            for (i, rr) in refs.into_iter().enumerate() {
                if let Some(rr) = rr {
                    let child_span = (span / n).max(1);
                    let child_base = base + i as u64 * span;
                    walk_shadow_free(durable, rr, child_base, child_span, out);
                }
            }
        }
    }
}

fn slot_durable(durable: &[u8], slot: u64) -> bool {
    (slot as usize + 1) * 4096 <= durable.len()
}

/// Whether an inode is the page file: it received the page-0 write.
fn is_page_file(inode: usize, events: &[DiskEvent]) -> bool {
    if inode == usize::MAX {
        return false;
    }
    events.iter().any(|e| match e {
        DiskEvent::WriteIssued {
            inode: i,
            off: 0,
            bytes,
        } if *i == inode => bytes.len() == 4096 && page::parse_page0(bytes).is_ok(),
        _ => false,
    })
}

/// Whether an inode is a WAL segment: a file created under /wal/.
fn is_wal_file(inode: usize, events: &[DiskEvent]) -> bool {
    let mut name: Option<&str> = None;
    for e in events {
        if let DiskEvent::DirOpIssued { what, .. } = e {
            if what.starts_with("create /wal/") {
                name = Some(what.strip_prefix("create ").unwrap_or(""));
                continue;
            }
            if what.starts_with("rename /wal/") {
                let to = what.split("-> ").nth(1).unwrap_or("").trim();
                name = Some(to);
                continue;
            }
            let _ = name;
        }
        let _ = e;
    }
    // The page file is never a WAL file; everything else that receives
    // NWAL-parsing writes is.
    !is_page_file(inode, events)
}

/// Kept for the module's contract documentation: the monitors never read
/// store memory (the flip hook aside); this anchor keeps `SimDisk`
/// referenced for future durable-history queries.
pub fn _disk_anchor(_d: &SimDisk) {
    let _ = PAGE_FILE;
    let _ = WAL_DIR;
}
