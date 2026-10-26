#![deny(clippy::indexing_slicing)]
//! amcheck (card X-5 Work 3.8; S-STORE §15's A-8 subset): a read-only
//! structural check of a quiescent image, entirely through SimDisk reads
//! — never store memory (the harness may run it on a crashed disk before
//! recovery). Malformed input is a finding, never a panic.
//!
//! Checks (card 3.8):
//! - **pages:** §3.3 on every referenced page (map, free set, tree,
//!   directory) — each failure names its slot;
//! - **tree:** I-TREE order and subtree bounds;
//! - **entries:** every entry key decodes and every tag is legal
//!   (I-ENC-ROUNDTRIP, I-TAG-RESERVED);
//! - **LPIDs:** mapped LPIDs equal the reachable pages, from the
//!   directory roots plus the directory's own pages (I-LPID-EXACT);
//! - **free set:** both bitmaps exact against the references
//!   (I-FREE-EXACT);
//! - **meta:** `gc_w ≤ T`, `next_lpid` above every mapped LPID,
//!   `slot_count` at least every referenced slot.
//!
//! Clean means an empty findings list.

use std::collections::{BTreeMap, BTreeSet};

use super::ckpt::lpid_bit;
use super::page::{self, Ref};
use super::tree;
use super::{Cx, Fail, StoreCfg, StoreHost};
use crate::oracle::Finding;
use crate::sim::{E4Env, PAGE_FILE};

/// amcheck the durable image of the page file. The harness runs this on
/// quiescent disks (before replay as (i), after the harness checkpoint as
/// (ii)).
pub fn amcheck<H: StoreHost>(cx: &Cx<'_, H>, cfg: &StoreCfg) -> Vec<Finding> {
    let mut out = Vec::new();
    let pf = match cx.env().open(PAGE_FILE) {
        Ok(f) => f,
        Err(_) => {
            out.push(Finding {
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
    // G7's shared selection, positional here unconditionally (the
    // mutant's single site is the store's open).
    let (meta, meta_slot) = match (m1, m2) {
        (Ok(a), Ok(b)) => match page::select_meta(&a, &b, false) {
            page::MetaPick::Win { meta, slot } => (meta, slot),
            page::MetaPick::MisplacedNewer { slot, .. } => {
                out.push(Finding {
                    slot: Some(slot),
                    what: "no valid meta (G7: a misplaced valid meta outranks every placed one)"
                        .into(),
                });
                return out;
            }
            page::MetaPick::NoValid => {
                out.push(Finding {
                    slot: Some(1),
                    what: "no valid meta".into(),
                });
                return out;
            }
        },
        _ => {
            out.push(Finding {
                slot: None,
                what: "meta read failed".into(),
            });
            return out;
        }
    };
    // Meta bounds.
    if meta.gc_w > meta.t {
        out.push(Finding {
            slot: Some(meta_slot),
            what: "gc_w > T".into(),
        });
    }
    // Load the flat map with §3.3 (integrity failures are findings, each
    // naming the failing slot).
    let cap = cfg.map_leaf_cap;
    let mutant = cfg.mutant;
    let mut flat: BTreeMap<u64, Ref> = BTreeMap::new();
    let mut used: BTreeSet<u64> = BTreeSet::new();
    if meta.map_root.slot != 0 {
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        let span = super::ckpt::radix_child_span(cap, n, meta.next_lpid);
        if let Err(e) = walk_map(
            cx,
            pf,
            meta.map_root,
            0,
            span,
            cap,
            0,
            &mut flat,
            &mut used,
            mutant,
            &mut out,
        ) {
            out.push(Finding {
                slot: Some(meta.map_root.slot),
                what: format!("map walk: {e:?}"),
            });
            return out;
        }
    }
    // The free set.
    let mut free_bits: BTreeSet<u64> = BTreeSet::new();
    let mut fs_used: BTreeSet<u64> = BTreeSet::new();
    if meta.free_root.slot != 0 {
        let total_bits = (meta.slot_count.saturating_sub(3)) + meta.next_lpid;
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        let span = super::ckpt::radix_child_span(cfg.freeset_leaf_bits, n, total_bits);
        if let Err(e) = walk_free(
            cx,
            pf,
            meta.free_root,
            0,
            span,
            cfg.freeset_leaf_bits,
            total_bits,
            &mut free_bits,
            &mut fs_used,
            mutant,
            &mut out,
        ) {
            out.push(Finding {
                slot: Some(meta.free_root.slot),
                what: format!("free set walk: {e:?}"),
            });
        }
    }
    used.extend(fs_used.iter().copied());
    // The directory tree and every tree under it: order, bounds, tags and
    // reachability. `reachable` collects the LPIDs the trees use.
    let mut reachable: BTreeSet<u64> = BTreeSet::new();
    let dir_root = meta.dir_root;
    if dir_root != 0 {
        check_tree(
            cx,
            pf,
            dir_root,
            &flat,
            &mut reachable,
            &mut out,
            "dir",
            true,
        );
    }
    // The directory entries (from the dir tree's own pages) name the
    // trees.
    let dir_entries = read_dir_entries(cx, pf, dir_root, &flat, &mut out);
    for (_id, root) in dir_entries.iter() {
        check_tree(
            cx,
            pf,
            *root,
            &flat,
            &mut reachable,
            &mut out,
            "tree",
            false,
        );
    }
    // I-LPID-EXACT: mapped LPIDs equal the reachable set (plus the
    // directory's own root).
    for lpid in flat.keys() {
        if !reachable.contains(lpid) && *lpid != dir_root {
            out.push(Finding {
                slot: flat.get(lpid).map(|r| r.slot),
                what: format!("mapped lpid {lpid} unreachable"),
            });
        }
    }
    for lpid in reachable.iter() {
        if !flat.contains_key(lpid) && *lpid != dir_root {
            out.push(Finding {
                slot: None,
                what: format!("reachable lpid {lpid} unmapped"),
            });
        }
    }
    // I-FREE-EXACT: both bitmaps exact. Slot bits: [3, slot_count).
    for bit in free_bits.iter() {
        if *bit < meta.slot_count - 3 {
            let slot = bit + 3;
            if used.contains(&slot) {
                out.push(Finding {
                    slot: Some(slot),
                    what: "free bitmap marks a referenced slot".into(),
                });
            }
        }
    }
    // Unreferenced slots in [3, slot_count) must be marked free.
    for slot in 3..meta.slot_count {
        if !used.contains(&slot) && !free_bits.contains(&(slot - 3)) {
            out.push(Finding {
                slot: Some(slot),
                what: "unreferenced slot not marked free".into(),
            });
        }
    }
    // LPID bits: [1, next_lpid) — free iff unmapped in the image.
    for l in 1..meta.next_lpid {
        let bit = lpid_bit(meta.slot_count, l);
        let marked_free = free_bits.contains(&bit);
        let mapped = flat.contains_key(&l) && (reachable.contains(&l) || l == dir_root);
        if marked_free && mapped {
            out.push(Finding {
                slot: flat.get(&l).map(|r| r.slot),
                what: format!("lpid {l} marked free but reachable"),
            });
        }
        if !marked_free && !flat.contains_key(&l) {
            out.push(Finding {
                slot: None,
                what: format!("lpid {l} unmapped but marked used"),
            });
        }
    }
    if meta.next_lpid <= flat.keys().copied().max().unwrap_or(0) {
        out.push(Finding {
            slot: Some(meta_slot),
            what: "next_lpid not above every mapped lpid".into(),
        });
    }
    if let Some(m) = used.iter().copied().max()
        && m >= meta.slot_count
    {
        out.push(Finding {
            slot: Some(meta_slot),
            what: "slot_count below a referenced slot".into(),
        });
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn walk_map<H: StoreHost>(
    cx: &Cx<'_, H>,
    pf: crate::sim::FileId,
    root: Ref,
    base: u64,
    span: u64,
    cap: u64,
    depth: u8,
    flat: &mut BTreeMap<u64, Ref>,
    used: &mut BTreeSet<u64>,
    mutant: Option<super::Mutant>,
    out: &mut Vec<Finding>,
) -> Result<(), Fail> {
    if depth > 8 || root.slot == 0 {
        return Ok(());
    }
    let bytes = cx
        .env()
        .read_at(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
        .map_err(Fail::Sim)?;
    // §3.3 with the positional expected id (the gate's R4).
    let expect_id = base;
    if page::check(&bytes, &root, expect_id, mutant).is_err() {
        out.push(Finding {
            slot: Some(root.slot),
            what: format!("§3.3 fail on map page (id {expect_id})"),
        });
    }
    if let Err(e) = cx
        .env()
        .accepted(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
    {
        // The accept hook could not record (a baton violation): a
        // finding, never a silent drop of an oracle input.
        out.push(Finding {
            slot: Some(root.slot),
            what: format!("accept hook failed: {e:?}"),
        });
    }
    used.insert(root.slot);
    let kind = page::header_kind(&bytes);
    if kind == page::KIND_MAP_LEAF {
        if let Ok(entries) = page::parse_map_leaf(&bytes, base, cap, &root, mutant) {
            for (i, e) in entries.iter().enumerate() {
                if let Some(r) = e {
                    flat.insert(base + i as u64, *r);
                    used.insert(r.slot);
                }
            }
        }
        Ok(())
    } else {
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        if let Ok(refs) = page::parse_radix_inner(&bytes, base, n as usize, &root, mutant) {
            for (i, r) in refs.into_iter().enumerate() {
                if let Some(r) = r {
                    let child_span = (span / n).max(1);
                    let child_base = base + i as u64 * span;
                    walk_map(
                        cx,
                        pf,
                        r,
                        child_base,
                        child_span,
                        cap,
                        depth + 1,
                        flat,
                        used,
                        mutant,
                        out,
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn walk_free<H: StoreHost>(
    cx: &Cx<'_, H>,
    pf: crate::sim::FileId,
    root: Ref,
    base: u64,
    span: u64,
    leaf_bits: u64,
    _total_bits: u64,
    set: &mut BTreeSet<u64>,
    used: &mut BTreeSet<u64>,
    mutant: Option<super::Mutant>,
    out: &mut Vec<Finding>,
) -> Result<(), Fail> {
    if root.slot == 0 {
        return Ok(());
    }
    let bytes = cx
        .env()
        .read_at(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
        .map_err(Fail::Sim)?;
    // §3.3 with the positional expected id (the gate's R4).
    let expect_id = base;
    if page::check(&bytes, &root, expect_id, mutant).is_err() {
        out.push(Finding {
            slot: Some(root.slot),
            what: format!("§3.3 fail on free-set page (id {expect_id})"),
        });
    }
    if let Err(e) = cx
        .env()
        .accepted(pf, root.slot * page::PAGE as u64, page::PAGE as u64)
    {
        out.push(Finding {
            slot: Some(root.slot),
            what: format!("accept hook failed: {e:?}"),
        });
    }
    used.insert(root.slot);
    let kind = page::header_kind(&bytes);
    if kind == page::KIND_FREE_LEAF {
        if let Ok(bits) = page::parse_free_leaf(&bytes, base, leaf_bits, &root, mutant) {
            for (i, b) in bits.iter().enumerate() {
                if *b {
                    set.insert(base + i as u64);
                }
            }
        }
        Ok(())
    } else {
        let n = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        if let Ok(refs) = page::parse_radix_inner(&bytes, base, n as usize, &root, mutant) {
            for (i, r) in refs.into_iter().enumerate() {
                if let Some(r) = r {
                    let child_span = (span / n).max(1);
                    let child_base = base + i as u64 * span;
                    walk_free(
                        cx,
                        pf,
                        r,
                        child_base,
                        child_span,
                        leaf_bits,
                        _total_bits,
                        set,
                        used,
                        mutant,
                        out,
                    )?;
                }
            }
        }
        Ok(())
    }
}

/// Check one tree: §3.3 per page, I-TREE order and bounds, entry decodes
/// and tags. Collects reachable LPIDs.
#[allow(clippy::too_many_arguments)]
fn check_tree<H: StoreHost>(
    cx: &Cx<'_, H>,
    pf: crate::sim::FileId,
    root_lpid: u64,
    flat: &BTreeMap<u64, Ref>,
    reachable: &mut BTreeSet<u64>,
    out: &mut Vec<Finding>,
    what: &str,
    is_dir: bool,
) {
    // (lpid, subtree lower bound, subtree upper bound)
    #[allow(clippy::type_complexity)]
    let mut stack: Vec<(u64, Option<Vec<u8>>, Option<Vec<u8>>)> = vec![(root_lpid, None, None)];
    while let Some((lpid, lo, hi)) = stack.pop() {
        if !reachable.insert(lpid) {
            out.push(Finding {
                slot: flat.get(&lpid).map(|r| r.slot),
                what: format!("{what}: lpid {lpid} reachable more than once"),
            });
            continue;
        }
        let Some(r) = flat.get(&lpid) else {
            out.push(Finding {
                slot: None,
                what: format!("{what}: lpid {lpid} unmapped"),
            });
            continue;
        };
        let bytes = match cx
            .env()
            .read_at(pf, r.slot * page::PAGE as u64, page::PAGE as u64)
        {
            Ok(b) => b,
            Err(_) => {
                out.push(Finding {
                    slot: Some(r.slot),
                    what: format!("{what}: read failed"),
                });
                continue;
            }
        };
        if page::check(&bytes, r, lpid, None).is_err() {
            out.push(Finding {
                slot: Some(r.slot),
                what: format!("{what}: §3.3 fail on lpid {lpid}"),
            });
            continue;
        }
        if let Err(e) = cx
            .env()
            .accepted(pf, r.slot * page::PAGE as u64, page::PAGE as u64)
        {
            out.push(Finding {
                slot: Some(r.slot),
                what: format!("{what}: accept hook failed: {e:?}"),
            });
            continue;
        }
        let kind = page::header_kind(&bytes);
        if kind == page::KIND_LEAF {
            if let Ok(cells) = tree::leaf_cells(&bytes) {
                let mut prev: Option<Vec<u8>> = None;
                for (k, v) in cells {
                    if let Some(p) = &prev
                        && p >= &k
                    {
                        out.push(Finding {
                            slot: Some(r.slot),
                            what: format!("{what}: leaf keys out of order"),
                        });
                    }
                    prev = Some(k.clone());
                    if let (Some(l), false) = (&lo, is_dir)
                        && k.as_slice() < l.as_slice()
                    {
                        out.push(Finding {
                            slot: Some(r.slot),
                            what: format!("{what}: entry below subtree bound"),
                        });
                    }
                    if let Some(h) = &hi
                        && k.as_slice() >= h.as_slice()
                    {
                        out.push(Finding {
                            slot: Some(r.slot),
                            what: format!("{what}: entry above subtree bound"),
                        });
                    }
                    let tag = v.first().copied().unwrap_or(255);
                    if is_dir {
                        if k.len() != 8 || v.len() != 9 {
                            out.push(Finding {
                                slot: Some(r.slot),
                                what: format!("{what}: bad directory entry shape"),
                            });
                        }
                    } else {
                        match tree::dec_ekey(&k) {
                            Some((_, ts)) if ts != 0 && ts != u64::MAX && tag <= 1 => {}
                            _ => out.push(Finding {
                                slot: Some(r.slot),
                                what: format!("{what}: entry decode/tag fail"),
                            }),
                        }
                    }
                }
            }
        } else if let Ok((seps, right)) = tree::inner_seps(&bytes) {
            // Convention: cell i `(c, s)` holds keys in [s_{i-1}, s_i);
            // `right` holds keys >= s_last.
            let mut prev: Option<Vec<u8>> = None;
            for (i, (c, s)) in seps.iter().enumerate() {
                if let Some(p) = &prev
                    && p >= s
                {
                    out.push(Finding {
                        slot: Some(r.slot),
                        what: format!("{what}: separators out of order"),
                    });
                }
                let child_lo = if i == 0 { lo.clone() } else { prev.clone() };
                let child_hi = Some(s.clone());
                prev = Some(s.clone());
                if let Some(cr) = flat.get(c) {
                    let _ = cr;
                    stack.push((*c, child_lo, child_hi));
                } else {
                    out.push(Finding {
                        slot: None,
                        what: format!("{what}: child lpid {c} unmapped"),
                    });
                }
            }
            stack.push((
                right,
                seps.last().map(|(_, s)| s.clone()).or_else(|| lo.clone()),
                hi,
            ));
        }
    }
}

/// Read the directory entries from the dir tree's pages (amcheck's own
/// walk; a corrupt entry is a finding).
fn read_dir_entries<H: StoreHost>(
    cx: &Cx<'_, H>,
    pf: crate::sim::FileId,
    dir_root: u64,
    flat: &BTreeMap<u64, Ref>,
    out: &mut Vec<Finding>,
) -> Vec<(u64, u64)> {
    let mut entries = Vec::new();
    let mut stack = vec![dir_root];
    let mut seen = BTreeSet::new();
    while let Some(lpid) = stack.pop() {
        if !seen.insert(lpid) {
            continue;
        }
        let Some(r) = flat.get(&lpid) else { continue };
        let bytes = match cx
            .env()
            .read_at(pf, r.slot * page::PAGE as u64, page::PAGE as u64)
        {
            Ok(b) => b,
            Err(_) => continue,
        };
        if page::check(&bytes, r, lpid, None).is_err() {
            continue;
        }
        let kind = page::header_kind(&bytes);
        if kind == page::KIND_LEAF {
            if let Ok(cells) = tree::leaf_cells(&bytes) {
                for (k, v) in cells {
                    if k.len() != 8 || v.len() != 9 {
                        out.push(Finding {
                            slot: Some(r.slot),
                            what: "dir: bad entry shape".into(),
                        });
                        continue;
                    }
                    let mut rb = [0u8; 8];
                    let mut ok = true;
                    if let Some(s) = v.get(0..8) {
                        rb.copy_from_slice(s);
                    } else {
                        ok = false;
                    }
                    if ok && let Ok(id) = k.try_into() {
                        entries.push((u64::from_be_bytes(id), u64::from_le_bytes(rb)));
                    }
                }
            }
        } else if let Ok((seps, right)) = tree::inner_seps(&bytes) {
            stack.push(right);
            for (c, _) in seps {
                stack.push(c);
            }
        }
    }
    entries
}
