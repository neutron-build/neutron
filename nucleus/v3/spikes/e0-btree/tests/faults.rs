//! `faults.rs` (card X-0-F2 Work 5): the fault-injecting store and the
//! atomicity oracle for the insert/split path.
//!
//! [`FaultStore`] wraps a [`MemStore`], counts calls per kind
//! (`allocate`, `read`, `write`) and fails the Nth call of the armed
//! kind exactly once with an I/O-style error. For each kind, and every
//! N from 1 to the number of calls that kind makes in a fixed seeded
//! workload, [`fault_sweep`] runs the workload with the fault armed,
//! asserts that a failed op left the tree exactly as the model had it
//! (T-ATOMIC: an insert that errors with anything but `Corrupt` has
//! changed nothing visible), finishes the workload with the fault
//! cleared, and then asserts `check()`, that every model key reads
//! back, that a full `range` equals the model — and all of that again
//! after `flush_all()` and a reopen through [`BTree::open`] over the
//! same store, which is where a lost `mark_dirty` shows up.
//!
//! The pool is 16 frames over a tree of ~60 pages, so eviction (and
//! with it the dirty write-back path) runs constantly. Keys are 1,200
//! bytes and values 300–2,048 bytes: an inner page fills at ~6
//! children, so leaf splits, inner splits and (at least two) root
//! splits all happen inside the workload, and the tree settles at
//! height 4 — within `2·h + 1 = 9 ≤ 16`, the minimum pool size card
//! X-0-F2's reservation strategy imposes.
//!
//! `faults_pool_starved_mid_split` covers the pool-size failure mode
//! the fault store cannot produce on its own: `PoolStarved` striking
//! while an insert holds its split reservation.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use e0_btree::page;
use e0_btree::{BTree, MemStore, PageId, PageStore, StoreError};

/// Pool size for the sweep (card X-0-F2 Work 5: small enough to force
/// constant eviction, and at least `2·h + 1` for the workload's height).
const CAPACITY: usize = 16;

/// Deterministic splitmix64 (same stream as the other suites).
struct Sm(u64);

impl Sm {
    /// New stream from a seed.
    fn new(seed: u64) -> Self {
        Sm(seed)
    }
    /// Next value in the stream.
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

// ---------------------------------------------------------------------------
// FaultStore
// ---------------------------------------------------------------------------

/// Which store-call kind a fault can be armed on (card X-0-F2 Work 5).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// `PageStore::allocate`.
    Allocate,
    /// `PageStore::read`.
    Read,
    /// `PageStore::write`.
    Write,
}

impl Kind {
    /// Index into the per-kind call counters.
    fn index(self) -> usize {
        match self {
            Kind::Allocate => 0,
            Kind::Read => 1,
            Kind::Write => 2,
        }
    }
    /// Human-readable name for messages.
    fn name(self) -> &'static str {
        match self {
            Kind::Allocate => "allocate",
            Kind::Read => "read",
            Kind::Write => "write",
        }
    }
}

/// The armed fault: fail the Nth call of `kind`, once.
#[derive(Clone, Copy)]
struct Arm {
    kind: Kind,
    n: u64,
}

/// Shared fault state, so the test can arm/disarm/inspect while the
/// tree owns the store.
#[derive(Default)]
struct FaultState {
    counts: [u64; 3],
    arm: Option<Arm>,
}

/// Lock the shared fault state, recovering from poison (the suite is
/// single-threaded; poison is impossible, and recovery beats panicking
/// inside a fault oracle).
fn lock(state: &Mutex<FaultState>) -> std::sync::MutexGuard<'_, FaultState> {
    state.lock().unwrap_or_else(|p| p.into_inner())
}

/// Test-side controller over a [`FaultStore`] (the store itself is
/// owned by the tree's pool).
struct FaultHandle {
    state: Arc<Mutex<FaultState>>,
}

impl FaultHandle {
    /// Zero the per-kind counters (the store's construction — the root
    /// page allocation and reads — is not part of the workload whose
    /// calls the sweep covers).
    fn reset(&self) {
        lock(&self.state).counts = [0, 0, 0];
    }
    /// Fail the Nth call of `kind`, once.
    fn arm(&self, kind: Kind, n: u64) {
        let mut st = lock(&self.state);
        st.counts = [0, 0, 0];
        st.arm = Some(Arm { kind, n });
    }
    /// Clear any armed fault (used before the end-of-run assertions, so
    /// a fault that never fired during the workload cannot strike
    /// during `check`/`flush_all`/reopen).
    fn disarm(&self) {
        lock(&self.state).arm = None;
    }
    /// Calls made so far, per kind.
    fn counts(&self) -> [u64; 3] {
        lock(&self.state).counts
    }
}

/// A [`PageStore`] wrapper that counts calls per kind and fails the Nth
/// call of the armed kind exactly once with an I/O-style error (card
/// X-0-F2 Work 5). Deterministic: the counter is bumped before the call
/// is made or refused, and the fault disarms itself as it fires.
struct FaultStore<S> {
    inner: S,
    state: Arc<Mutex<FaultState>>,
}

impl<S: PageStore> FaultStore<S> {
    /// Wrap `inner`, returning the store and a controller handle.
    fn new(inner: S) -> (Self, FaultHandle) {
        let state = Arc::new(Mutex::new(FaultState::default()));
        (
            Self {
                inner,
                state: Arc::clone(&state),
            },
            FaultHandle { state },
        )
    }

    /// Count the call and refuse it if the arm says so.
    fn bump(&self, kind: Kind) -> Result<(), StoreError> {
        let mut st = lock(&self.state);
        let i = kind.index();
        st.counts[i] += 1;
        if let Some(arm) = st.arm
            && arm.kind == kind
            && arm.n == st.counts[i]
        {
            st.arm = None; // one-shot: the workload continues cleared
            return Err(StoreError::Io(std::io::Error::other(format!(
                "injected {} fault #{} (card X-0-F2)",
                kind.name(),
                arm.n
            ))));
        }
        Ok(())
    }
}

impl<S: PageStore> PageStore for FaultStore<S> {
    fn page_size(&self) -> usize {
        self.inner.page_size()
    }
    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<(), StoreError> {
        self.bump(Kind::Read)?;
        self.inner.read(id, buf)
    }
    fn write(&self, id: PageId, buf: &[u8]) -> Result<(), StoreError> {
        self.bump(Kind::Write)?;
        self.inner.write(id, buf)
    }
    fn allocate(&self) -> Result<PageId, StoreError> {
        self.bump(Kind::Allocate)?;
        self.inner.allocate()
    }
    fn free(&self, id: PageId) -> Result<(), StoreError> {
        self.inner.free(id)
    }
}

// ---------------------------------------------------------------------------
// The fixed workload
// ---------------------------------------------------------------------------

/// One operation of the seeded workload.
#[derive(Clone, Debug)]
enum Op {
    /// Insert of a (so far unseen) key.
    Insert {
        /// Key.
        k: Vec<u8>,
        /// Value.
        v: Vec<u8>,
    },
    /// Upsert of an earlier key; every payload here is large, so a fair
    /// share of these grow the cell until the leaf splits (card X-0-F2
    /// Work 5: "upserts, some growing the value so they split").
    Upsert {
        /// Key.
        k: Vec<u8>,
        /// Value.
        v: Vec<u8>,
    },
    /// Delete.
    Delete {
        /// Key.
        k: Vec<u8>,
    },
    /// Point lookup.
    Get {
        /// Key.
        k: Vec<u8>,
    },
    /// Range scan `[lo, hi)`.
    Range {
        /// Low bound (inclusive).
        lo: Vec<u8>,
        /// High bound (exclusive).
        hi: Vec<u8>,
    },
}

/// A `len`-byte key: a big-endian u16 prefix (ascending u16 order is
/// ascending byte order) plus constant padding. 1,200 bytes of key make
/// inner pages fill at ~6 children, so inner splits and root splits
/// happen inside the workload.
fn key(n: u16, len: usize) -> Vec<u8> {
    let mut k = n.to_be_bytes().to_vec();
    k.extend(std::iter::repeat_n(0xA5, len - 2));
    k
}

/// A `len`-byte value of random bytes.
fn value(rng: &mut Sm, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.next_u64() as u8).collect()
}

/// The fixed seeded workload: inserts, splitting upserts, deletes, gets
/// and ranges (card X-0-F2 Work 5). Same op sequence in every run, so
/// each armed N lands on the same call.
fn workload() -> Vec<Op> {
    let mut rng = Sm::new(0xFA17_C0DE);
    let mut ops = Vec::new();
    let mut live: Vec<u16> = Vec::new();
    for i in 0u16..190 {
        let vlen = 300 + (rng.next_u64() % 600) as usize;
        ops.push(Op::Insert {
            k: key(i, 1200),
            v: value(&mut rng, vlen),
        });
        live.push(i);
        if i >= 8 && i % 3 == 0 {
            // growing upsert: 1,600–2,048 bytes against a 300–900-byte
            // cell — the replacement often does not fit, which is the
            // remove-before-split shape the UPSERT-REMOVE-FIRST mutation
            // targets.
            let j = live[(rng.next_u64() as usize) % live.len()];
            let vlen = 1600 + (rng.next_u64() % 449) as usize;
            ops.push(Op::Upsert {
                k: key(j, 1200),
                v: value(&mut rng, vlen),
            });
        }
        if i >= 12 && i % 4 == 0 {
            let j = live[(rng.next_u64() as usize) % live.len()];
            ops.push(Op::Delete { k: key(j, 1200) });
        }
        if i % 5 == 0 {
            let j = live[(rng.next_u64() as usize) % live.len()];
            ops.push(Op::Get { k: key(j, 1200) });
        }
        if i % 7 == 0 {
            let a = (rng.next_u64() % 190) as u16;
            let b = (rng.next_u64() % 190) as u16;
            ops.push(Op::Range {
                lo: key(a, 1200),
                hi: key(b, 1200),
            });
        }
    }
    ops
}

// ---------------------------------------------------------------------------
// The sweep
// ---------------------------------------------------------------------------

/// Run the workload once (fault armed or not) and assert the card's
/// five-step oracle. Returns the per-kind call counts as of the end of
/// the workload, and the tree height.
fn run_once(ops: &[Op], arm: Option<(Kind, u64)>) -> Result<([u64; 3], usize), String> {
    let store = MemStore::new(8192).map_err(|e| e.to_string())?;
    let (store, handle) = FaultStore::new(store);
    let tree = BTree::new(store, CAPACITY).map_err(|e| format!("fresh tree: {e}"))?;
    // Count and fault only the workload's calls, not the tree's
    // construction (the root page).
    handle.reset();
    if let Some((kind, n)) = arm {
        handle.arm(kind, n);
    }
    let mut model: Model = BTreeMap::new();

    for op in ops {
        match op {
            Op::Insert { k, v } | Op::Upsert { k, v } => match tree.insert(k, v) {
                Ok(old) => {
                    let want = model.insert(k.clone(), v.clone());
                    if old != want {
                        return Err(format!("insert: returned {old:?}, model says {want:?}"));
                    }
                }
                Err(StoreError::Corrupt(m)) => {
                    // Card X-0-F2 Work 6: a Corrupt from this suite is a
                    // finding, escalated verbatim by the failing test.
                    return Err(format!("Corrupt on insert (a card X-0-F2 finding): {m}"));
                }
                Err(_) => {
                    // T-ATOMIC: the failed upsert changed nothing
                    // visible — the pre-op value must still be there.
                    let got = tree.get(k).map_err(|e| format!("get: {e}"))?;
                    let want = model.get(k).cloned();
                    if got != want {
                        return Err(format!(
                            "failed insert was not atomic: key {:02x?}.. reads {got:?}, model has {want:?}",
                            k.get(..2).map(|s| s.to_vec()).unwrap_or_default(),
                        ));
                    }
                }
            },
            Op::Delete { k } => match tree.delete(k) {
                Ok(old) => {
                    let want = model.remove(k);
                    if old != want {
                        return Err(format!("delete: returned {old:?}, model says {want:?}"));
                    }
                }
                Err(StoreError::Corrupt(m)) => {
                    return Err(format!("Corrupt on delete (a card X-0-F2 finding): {m}"));
                }
                Err(_) => {
                    let got = tree.get(k).map_err(|e| format!("get: {e}"))?;
                    let want = model.get(k).cloned();
                    if got != want {
                        return Err("failed delete was not atomic".to_string());
                    }
                }
            },
            Op::Get { k } => match tree.get(k) {
                Ok(got) => {
                    let want = model.get(k).cloned();
                    if got != want {
                        return Err(format!("get: {got:?} vs model {want:?}"));
                    }
                }
                // A read fault surfacing on get/range is allowed (card
                // X-0-F2 Work 5); the end-of-run assertions prove it
                // damaged nothing.
                Err(StoreError::Corrupt(m)) => {
                    return Err(format!("Corrupt on get (a card X-0-F2 finding): {m}"));
                }
                Err(_) => {}
            },
            Op::Range { lo, hi } => {
                match tree.range(lo, hi) {
                    Ok(iter) => match iter.collect::<Result<Vec<(Vec<u8>, Vec<u8>)>, _>>() {
                        Ok(got) => {
                            if lo <= hi {
                                let want: Vec<(Vec<u8>, Vec<u8>)> = model
                                    .range(lo.clone()..hi.clone())
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                if got != want {
                                    return Err("range mismatch".to_string());
                                }
                            } else if !got.is_empty() {
                                return Err("inverted range not empty".to_string());
                            }
                        }
                        // Item errors may carry the injected fault; treat
                        // the whole scan as failed-but-harmless.
                        Err(StoreError::Corrupt(m)) => {
                            return Err(format!("Corrupt in range (a card X-0-F2 finding): {m}"));
                        }
                        Err(_) => {}
                    },
                    Err(StoreError::Corrupt(m)) => {
                        return Err(format!("Corrupt on range (a card X-0-F2 finding): {m}"));
                    }
                    Err(_) => {}
                }
            }
        }
    }
    // The workload is over: nothing from here on may fault (an arm that
    // never fired must not strike the assertions instead).
    handle.disarm();
    let counts = handle.counts();
    let stats = tree
        .check()
        .map_err(|m| format!("check() after workload: {m}"))?;
    assert_final_state(&tree, &model, "end of workload")?;
    tree.flush_all().map_err(|m| format!("flush_all: {m}"))?;
    let root = tree.root_page().map_err(|e| format!("root page: {e}"))?;
    let store = tree.into_store();
    let reopened = BTree::open(store, CAPACITY, root).map_err(|m| format!("reopen: {m}"))?;
    assert_final_state(&reopened, &model, "after reopen")?;
    Ok((counts, stats.height))
}

/// The end-of-run oracle (card X-0-F2 Work 5, steps 4–5): `check()` is
/// clean, every model key reads back with its value, and a full range
/// equals the model.
fn assert_final_state(
    tree: &BTree<FaultStore<MemStore>>,
    model: &Model,
    phase: &str,
) -> Result<(), String> {
    tree.check().map_err(|m| format!("check() {phase}: {m}"))?;
    for (k, v) in model {
        let got = tree.get(k).map_err(|m| format!("get {phase}: {m}"))?;
        if got.as_deref() != Some(v.as_slice()) {
            return Err(format!(
                "{phase}: key {:02x?}.. reads {got:?}, model has {} bytes",
                k.get(..2).map(|s| s.to_vec()).unwrap_or_default(),
                v.len()
            ));
        }
    }
    let scan: Vec<(Vec<u8>, Vec<u8>)> = tree
        .range(&[0x00], &[0xff; 1300])
        .map_err(|m| format!("range {phase}: {m}"))?
        .collect::<Result<_, _>>()
        .map_err(|m| format!("range item {phase}: {m}"))?;
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    if scan != want {
        return Err(format!(
            "{phase}: full range has {} entries, model has {}",
            scan.len(),
            want.len()
        ));
    }
    Ok(())
}

/// One `#[test]` per kind; each sweeps every call N of that kind.
fn fault_sweep(kind: Kind) -> Result<(), String> {
    let ops = workload();
    let (counts, height) = run_once(&ops, None)?;
    // The workload must exercise what this card's mutants target: root
    // splits (height grows only through one) and a pool that still
    // covers the reservation's 2·h + 1 frames.
    assert!(
        height >= 3,
        "workload must pass a root split, height {height}"
    );
    let needed = 2 * height + 1;
    assert!(
        needed <= CAPACITY,
        "height {height} needs {needed} frames, pool is {CAPACITY}"
    );
    let n = counts[kind.index()];
    assert!(n > 0, "workload makes no {} calls", kind.name());
    for i in 1..=n {
        run_once(&ops, Some((kind, i))).map_err(|m| format!("{} fault #{i}: {m}", kind.name()))?;
    }
    Ok(())
}

/// Sweep every `allocate` call of the workload (card X-0-F2 Work 5).
#[test]
fn fault_sweep_allocate() {
    fault_sweep(Kind::Allocate).unwrap();
}

/// Sweep every `read` call of the workload (card X-0-F2 Work 5; also
/// the LOCATE-NO-RESET catcher: a read failure that leaves the victim
/// frame claiming its old page shows up here as a model mismatch or a
/// `check()` failure after reopen).
#[test]
fn fault_sweep_read() {
    fault_sweep(Kind::Read).unwrap();
}

/// Sweep every `write` call of the workload (card X-0-F2 Work 5).
#[test]
fn fault_sweep_write() {
    fault_sweep(Kind::Write).unwrap();
}

// ---------------------------------------------------------------------------
// PoolStarved mid-split
// ---------------------------------------------------------------------------

/// The reference model shared by every scenario: an ordered map of the
/// live keys to their values.
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// A `MAX_KEY`-byte key: with `MAX_VALUE`-byte values every leaf cell
/// costs `page::MAX_LEAF_COST`, so a leaf holds at most two cells — and
/// a two-cell leaf is both unsafe and genuinely full: no cell fits, the
/// next insert must split. Inner cells cost `page::MAX_INNER_COST`, so
/// an inner page fills at four children.
fn max_key(n: u16) -> Vec<u8> {
    key(n, page::MAX_KEY)
}

/// A `MAX_VALUE`-byte value.
fn max_val() -> Vec<u8> {
    vec![0xC3; page::MAX_VALUE]
}

/// Build a tree of `n` sequential max-size cells at a comfortable pool,
/// then hand back the flushed store and its root for a small-pool
/// reopen (card X-0-F2 Work 5: pins are forced by pool size — the test
/// cannot hold pool guards through the pub API, and a held `RangeIter`
/// would pin the root's frame with a *read* latch the insert's descent
/// would deadlock on; a pool that fits the descent path but not the
/// reservation starves `new_page` at exactly the same points).
fn build_max_tree(n: u16) -> Result<(MemStore, PageId, Model), String> {
    let store = MemStore::new(8192).map_err(|e| e.to_string())?;
    let tree = BTree::new(store, 64).map_err(|e| format!("build tree: {e}"))?;
    let mut model = BTreeMap::new();
    for i in 0..n {
        let k = max_key(i);
        let v = max_val();
        let old = tree
            .insert(&k, &v)
            .map_err(|e| format!("build insert {i}: {e}"))?;
        let want = model.insert(k.clone(), v.clone());
        if old != want {
            return Err(format!("build insert {i}: {old:?} vs {want:?}"));
        }
    }
    tree.check().map_err(|m| format!("built tree: {m}"))?;
    tree.flush_all()
        .map_err(|m| format!("flush built tree: {m}"))?;
    let root = tree.root_page().map_err(|e| format!("root: {e}"))?;
    let store = tree.into_store();
    Ok((store, root, model))
}

/// The shared end-of-run oracle for the starve scenarios: the failed
/// insert changed nothing (its own key absent, every model key
/// readable), `check()` is clean at the unchanged height, the full
/// range equals the model, and the same holds after flush + reopen at
/// the same small pool.
fn assert_starved_untouched(
    tree: BTree<MemStore>,
    model: &Model,
    pending: &[u8],
    capacity: usize,
    height: usize,
    phase: &str,
) -> Result<(), String> {
    let got = tree.get(pending).map_err(|e| format!("get pending: {e}"))?;
    if got.is_some() {
        return Err(format!("{phase}: the starved insert left something behind"));
    }
    let stats = tree.check().map_err(|m| format!("check {phase}: {m}"))?;
    if stats.height != height {
        return Err(format!("{phase}: height changed to {}", stats.height));
    }
    let scan: Vec<(Vec<u8>, Vec<u8>)> = tree
        .range(&[0x00], &[0xff; page::MAX_KEY + 1])
        .map_err(|m| format!("range {phase}: {m}"))?
        .collect::<Result<_, _>>()
        .map_err(|m| format!("range item {phase}: {m}"))?;
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    if scan != want {
        return Err(format!("{phase}: full range differs from the model"));
    }
    for (k, v) in model {
        let got = tree.get(k).map_err(|e| format!("get {phase}: {e}"))?;
        if got.as_deref() != Some(v.as_slice()) {
            return Err(format!(
                "{phase}: model key {:02x?}.. does not read back",
                { k.get(..2).map(|s| s.to_vec()).unwrap_or_default() }
            ));
        }
    }
    tree.flush_all()
        .map_err(|m| format!("flush {phase}: {m}"))?;
    let root = tree.root_page().map_err(|e| format!("root {phase}: {e}"))?;
    let store = tree.into_store();
    let reopened =
        BTree::open(store, capacity, root).map_err(|m| format!("reopen {phase}: {m}"))?;
    let stats = reopened
        .check()
        .map_err(|m| format!("check reopen {phase}: {m}"))?;
    if stats.height != height {
        return Err(format!("{phase}: height after reopen: {}", stats.height));
    }
    for (k, v) in model {
        let got = reopened
            .get(k)
            .map_err(|e| format!("get reopen {phase}: {e}"))?;
        if got.as_deref() != Some(v.as_slice()) {
            return Err(format!("{phase}: model key lost over the reopen"));
        }
    }
    Ok(())
}

/// Card X-0-F2 Work 5, `faults_pool_starved_mid_split`: a pool too small
/// for the reservation returns `PoolStarved` while an insert is trying
/// to split, and the tree is exactly as it was afterwards.
///
/// Two deterministic scenarios (the reservation is attempted bottom-up:
/// the leaf's right page first, then the cascade's pages):
///
/// * **leaf split**: a height-2 tree reopened with 2 frames — the
///   descent pins root + leaf, so *the first* reserved page (the leaf's
///   new right half) cannot be gotten;
/// * **cascade**: a height-3 tree reopened with 4 frames, descending
///   through an unsafe (retained) inner page — the descent pins root +
///   inner + leaf, the leaf's own page takes the one free frame, and
///   the starvation strikes the *second* reserved page, the one the
///   cascade (the parent's split or the new root) would use. The key
///   count that produces a retained parent is found by scanning fresh
///   builds (deterministic; see the test body).
#[test]
fn faults_pool_starved_mid_split() {
    // --- Scenario 1: starve the leaf's own split page. ---------------
    // Four max-size cells grow a height-2 tree whose right-most leaf is
    // full (two cells). The fifth insert must split that leaf.
    let (store, root, model) = build_max_tree(4).expect("build scenario 1");
    let tree = BTree::open(store, 2, root).expect("reopen with 2 frames");
    let stats = tree.check().expect("check scenario 1");
    assert_eq!(stats.height, 2, "scenario 1 needs a height-2 tree");
    let pending = max_key(4);
    let err = tree
        .insert(&pending, &max_val())
        .expect_err("the leaf split must starve at 2 frames");
    assert!(
        matches!(err, StoreError::PoolStarved),
        "expected PoolStarved, got {err:?}"
    );
    assert_starved_untouched(tree, &model, &pending, 2, 2, "leaf-split starve")
        .expect("scenario 1 tree is untouched");

    // --- Scenario 2: starve the cascade's page. ----------------------
    // A height-3 tree reopened with 4 frames starves only when the
    // leaf's parent is itself retained (an unsafe inner), so the
    // descent pins root + inner + leaf and the leaf's reserved page
    // takes the last frame: the second reservation — the parent
    // split's or the new root's page — then finds every frame pinned.
    // Which key count produces that shape is a fact about the split
    // arithmetic, not something to hard-code: scan key counts on fresh
    // trees until the insert starves (deterministic — same build every
    // time — and bounded; every count whose insert succeeds is simply
    // not the shape, its tree is discarded).
    let mut starved = None;
    for n in 8u16..=40 {
        let (store, root, model) = build_max_tree(n).expect("build scenario 2");
        let tree = BTree::open(store, 4, root).expect("reopen with 4 frames");
        let stats = tree.check().expect("check the small-pool tree");
        assert_eq!(stats.height, 3, "scenario 2 needs a height-3 tree (n={n})");
        let pending = max_key(n);
        match tree.insert(&pending, &max_val()) {
            Err(StoreError::PoolStarved) => {
                assert_starved_untouched(tree, &model, &pending, 4, 3, "cascade starve")
                    .expect("scenario 2 tree is untouched");
                starved = Some(n);
                break;
            }
            Err(other) => panic!("n={n}: expected Ok or PoolStarved, got {other:?}"),
            Ok(_) => continue, // parent released: not the shape; try the next
        }
    }
    assert!(
        starved.is_some(),
        "no key count in 8..=40 starved the cascade page"
    );
}
