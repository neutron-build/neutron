//! Differential test: the G0-gc model's abstract LSM against `MemKv` in LSM
//! mode (C-K3b), driven in lockstep over **every** op sequence up to length 6
//! over two logical keys (exhaustive, not random).
//!
//! Each root-to-node path is one op sequence: the model side is carried as a
//! clone chain, and the MemKv side is rebuilt per node by replaying the path
//! into a fresh store (MemKv has no undo). Exploration is deduplicated on the
//! model side's content signature, which preserves the argument: identical
//! model states behave identically, so pruning their subtrees compares
//! nothing that could differ.
//!
//! Ops mirror the model's workload alphabet: §2.2 version puts (live values
//! and tombstones), raw deletes, range DeleteRanges, flushes, compactions of
//! chosen L0 files (every non-empty subset, with MemKv's expansion rules on
//! both sides), whole-keyspace compactions through the reference §9.2 filter
//! (`SpecGcFilter`, at either of two watermarks), and opening/closing a
//! snapshot that stays open across later ops -- which is what checks the
//! adversarial drop-registry semantics (a filter drop visible to an
//! already-open snapshot) on both sides.
//!
//! At every explored state, both stores must agree on:
//! - the merged rows each fresh snapshot returns over each logical key's
//!   whole §2.2 range, and therefore the §4 read at every snapshot ts;
//! - the same reads through an open snapshot (copy + live drop registry);
//! - latest-state point reads.

use std::collections::BTreeSet;
use std::ops::Bound;

use nucleus_g0::gc::{GcSpec, Lsm, LsmOp};
use nucleus_kv::conformance::layout;
use nucleus_kv::conformance::SpecGcFilter;
use nucleus_kv::mem::MemSnap;
use nucleus_kv::{Batch, Durability, MemKv, Op, OrderedKv, Snapshot};

type Row = (Vec<u8>, Vec<u8>);

/// Op sequences up to this length are enumerated.
const MAX_DEPTH: usize = 6;
/// The snapshot ts values reads are compared at.
const READ_TS: [u64; 8] = [0, 5, 10, 15, 20, 25, 30, 35];

fn ok<T>(r: Result<T, nucleus_kv::KvError>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("{what}: {e}"),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum VKind {
    LiveA,
    LiveB,
    Tomb,
}

impl VKind {
    fn value(self) -> Vec<u8> {
        match self {
            VKind::LiveA => layout::live_value(b"a"),
            VKind::LiveB => layout::live_value(b"b"),
            VKind::Tomb => layout::tombstone_value(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op1 {
    /// Put a §2.2 version of logical key `k` at `ts`.
    Put(u8, u64, VKind),
    /// Delete one raw version key.
    Del(u8, u64),
    /// One of two fixed DeleteRanges.
    Dr(u8),
    Flush,
    /// Compact the L0 files whose position's bit is set.
    Compact(u8),
    SetW(u64),
    FullCompact,
    SnapOpen,
    SnapClose,
}

fn key_name(k: u8) -> &'static [u8] {
    if k == 0 {
        b"k0"
    } else {
        b"k1"
    }
}

/// The §4 read at ts `s` over one side's merged rows (the comparator is
/// shared, so only the rows can differ).
fn read_at_rows(rows: &[(Vec<u8>, Vec<u8>)], s: u64) -> Option<Vec<u8>> {
    for (key, value) in rows {
        if let Some((_, layout::Entry::Version(ts))) = layout::parse(key) {
            if ts <= s {
                return if layout::is_tombstone(value) {
                    None
                } else {
                    Some(value.clone())
                };
            }
        }
    }
    None
}

fn rows_both_sides(k: u8, mine: &Lsm, mem: &MemKv) -> (Vec<Row>, Vec<Row>) {
    let l = key_name(k);
    let (lo, hi) = (layout::intent_key(l), layout::end_key(l));
    let mine_rows = mine.merged_scan(&lo, &hi);
    let mem_rows = snap_rows(&mem.snapshot(), &lo, &hi);
    (mine_rows, mem_rows)
}

fn snap_rows<S: Snapshot>(snap: &S, lo: &Vec<u8>, hi: &Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let it = snap.scan(
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        false,
    );
    it.map(|r| match r {
        Ok(kv) => kv,
        Err(e) => panic!("scan: {e}"),
    })
    .collect()
}

fn my_snap_rows(mine_snap: &Lsm, live: &Lsm, k: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
    let l = key_name(k);
    Lsm::snap_scan(mine_snap, live, &layout::intent_key(l), &layout::end_key(l))
}

fn compare(mine: &Lsm, kv: &MemKv, my_snap: Option<&Lsm>, mem_snap: Option<&MemSnap>) {
    for k in 0..2u8 {
        let (mine_rows, mem_rows) = rows_both_sides(k, mine, kv);
        assert_eq!(
            mine_rows.len(),
            mem_rows.len(),
            "k{k}: fresh row counts differ"
        );
        for s in READ_TS {
            assert_eq!(
                read_at_rows(&mine_rows, s),
                read_at_rows(&mem_rows, s),
                "k{k} at S={s}: fresh read differs (mine {mine_rows:?}, MemKv {mem_rows:?})"
            );
        }
        if let (Some(ms), Some(mem)) = (my_snap, mem_snap) {
            let l = key_name(k);
            let mine_rows = my_snap_rows(ms, mine, k);
            let mem_rows = snap_rows(mem, &layout::intent_key(l), &layout::end_key(l));
            assert_eq!(
                mine_rows.len(),
                mem_rows.len(),
                "k{k}: open-snapshot row counts differ"
            );
            for s in READ_TS {
                assert_eq!(
                    read_at_rows(&mine_rows, s),
                    read_at_rows(&mem_rows, s),
                    "k{k} at S={s}: open-snapshot read differs (mine {mine_rows:?}, MemKv {mem_rows:?})"
                );
            }
        }
    }
    // Latest-state point reads (through both raw delete markers and range
    // tombstones).
    for (k, ts) in [(0u8, 20u64), (1, 10)] {
        let key = layout::version_key(key_name(k), ts);
        let mine_v = mine.merged_get(&key);
        let mem_v = ok(kv.get_latest(&key), "get_latest");
        assert_eq!(mine_v, mem_v, "latest read of {key:?} differs");
    }
}

fn mem_l0(kv: &MemKv) -> Vec<u64> {
    ok(kv.files(), "files")
        .into_iter()
        .filter(|(level, _, _)| *level == 0)
        .map(|(_, id, _)| id)
        .collect()
}

/// One step of the op language, applied to both sides in lockstep. The
/// snapshot arguments are part of the state being driven.
struct Drive {
    mine: Lsm,
    kv: MemKv,
    my_snap: Option<Lsm>,
    mem_snap: Option<MemSnap>,
    w: Option<u64>,
}

impl Drive {
    fn new() -> Self {
        Self {
            mine: Lsm::new(),
            kv: MemKv::lsm(),
            my_snap: None,
            mem_snap: None,
            w: None,
        }
    }

    fn apply(&mut self, op: Op1) {
        match op {
            Op1::Put(k, ts, v) => {
                let key = layout::version_key(key_name(k), ts);
                let val = v.value();
                self.mine.apply(LsmOp::Put(key.clone(), val.clone()));
                ok(
                    self.kv.write(
                        Batch {
                            ops: vec![Op::Put(key, val)],
                        },
                        Durability::No,
                    ),
                    "write",
                );
            }
            Op1::Del(k, ts) => {
                let key = layout::version_key(key_name(k), ts);
                self.mine.apply(LsmOp::Delete(key.clone()));
                ok(
                    self.kv.write(
                        Batch {
                            ops: vec![Op::Delete(key)],
                        },
                        Durability::No,
                    ),
                    "write",
                );
            }
            Op1::Dr(which) => {
                let (start, end) = if which == 0 {
                    (layout::intent_key(b"k0"), layout::end_key(b"k0"))
                } else {
                    (layout::version_key(b"k0", 10), layout::end_key(b"k1"))
                };
                self.mine.apply(LsmOp::DeleteRange {
                    start: start.clone(),
                    end: end.clone(),
                });
                ok(
                    self.kv.write(
                        Batch {
                            ops: vec![Op::DeleteRange { start, end }],
                        },
                        Durability::No,
                    ),
                    "write",
                );
            }
            Op1::Flush => {
                self.mine.flush();
                ok(self.kv.flush(), "flush");
            }
            Op1::Compact(mask) => {
                let l0 = self.mine.level0_len();
                let all = mem_l0(&self.kv);
                assert_eq!(l0, all.len(), "L0 file counts diverged");
                let positions: Vec<usize> = (0..l0).filter(|i| mask & (1 << i) != 0).collect();
                let ids: Vec<u64> = positions.iter().map(|&i| all[i]).collect();
                self.mine
                    .compact_files(&positions, self.w.map(|w| GcSpec { w }));
                ok(self.kv.compact(0, &ids), "compact");
            }
            Op1::SetW(nw) => {
                self.w = Some(nw);
                self.kv.set_gc_filter(Box::new(SpecGcFilter { w: nw }));
            }
            Op1::FullCompact => {
                self.mine.compact_all(self.w.map(|w| GcSpec { w }));
                self.kv.compact_all();
            }
            Op1::SnapOpen => {
                self.my_snap = Some(self.mine.clone());
                self.mem_snap = Some(self.kv.snapshot());
            }
            Op1::SnapClose => {
                self.my_snap = None;
                self.mem_snap = None;
            }
        }
    }
}

fn sig(mine: &Lsm, my_snap: Option<&Lsm>, w: Option<u64>) -> Vec<u8> {
    let mut b = mine.content_signature();
    if let Some(ms) = my_snap {
        b.push(1);
        b.extend_from_slice(&ms.content_signature());
        b.extend_from_slice(&ms.open_era().to_be_bytes());
    } else {
        b.push(0);
    }
    b.extend_from_slice(&w.map(|x| x.to_be_bytes()).unwrap_or([0; 8]));
    b
}

fn ops_of(d: &Drive) -> Vec<Op1> {
    let mut ops: Vec<Op1> = Vec::new();
    for (k, ts, v) in [
        (0u8, 30u64, VKind::LiveA),
        (0, 20, VKind::Tomb),
        (0, 10, VKind::LiveB),
        (1, 30, VKind::LiveB),
        (1, 20, VKind::LiveA),
        (1, 10, VKind::Tomb),
    ] {
        ops.push(Op1::Put(k, ts, v));
    }
    ops.push(Op1::Del(0, 20));
    ops.push(Op1::Del(1, 10));
    ops.push(Op1::Dr(0));
    ops.push(Op1::Dr(1));
    ops.push(Op1::Flush);
    let l0 = d.mine.level0_len();
    assert_eq!(l0, mem_l0(&d.kv).len(), "L0 file counts diverged");
    for mask in 1..(1u32 << l0.min(7)) {
        ops.push(Op1::Compact(mask as u8));
    }
    ops.push(Op1::SetW(10));
    ops.push(Op1::SetW(25));
    ops.push(Op1::FullCompact);
    if d.my_snap.is_none() {
        ops.push(Op1::SnapOpen);
    } else {
        ops.push(Op1::SnapClose);
    }
    ops
}

fn dfs(path: &[Op1], visited: &mut BTreeSet<Vec<u8>>, stats: &mut (usize, usize)) {
    let mut d = Drive::new();
    for op in path {
        d.apply(*op);
    }
    compare(&d.mine, &d.kv, d.my_snap.as_ref(), d.mem_snap.as_ref());

    let signature = sig(&d.mine, d.my_snap.as_ref(), d.w);
    if !visited.insert(signature) {
        return;
    }
    stats.0 += 1;

    if path.len() >= MAX_DEPTH {
        return;
    }
    for op in ops_of(&d) {
        let mut next = path.to_vec();
        next.push(op);
        stats.1 += 1;
        dfs(&next, visited, stats);
    }
}

#[test]
fn gc_lsm_matches_memkv_exhaustively() {
    let mut visited = BTreeSet::new();
    let mut stats = (0usize, 0usize);
    dfs(&[], &mut visited, &mut stats);
    eprintln!(
        "gc_lsm_diff: {} states, {} sequences explored to depth {MAX_DEPTH}",
        stats.0, stats.1
    );
    assert!(stats.0 > 0, "nothing explored");
}
