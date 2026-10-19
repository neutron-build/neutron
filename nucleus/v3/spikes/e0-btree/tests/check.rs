//! `check.rs` (cards X-0-F3 and X-0-F3-F1): hand-built trees that pin
//! the bounds rules of `BTree::check()`. X-0-F3 covered the two holes
//! named by the X-0 verdict's finding 3: before it, child 0 of an
//! inner page inherited `None` as its lower bound instead of the
//! parent's `lo`, and separators were never checked against the bounds
//! inherited from above — so `check()` was weaker than its own T-ORDER
//! doc promise, and X-1a…X-5 (plus the X-0-F2 fault oracle) could not
//! lean on it alone. X-0-F3-F1 adds the two follow-ups the X-0-F3 gate
//! verdict asked for (its findings 1–2, mutants G-SEP-LO-SKIPPED and
//! G-LO-PRECEDENCE): the `sep < lo` half of the separator rule and the
//! "later children take the previous separator" precedence each had no
//! e0-btree suite pinning them.
//!
//! The trees are built directly with the pub `page` builders over a
//! [`MemStore`] and opened with [`BTree::open`], so each test corrupts
//! exactly one bound and cannot be masked by insert-path repair. Both
//! X-0-F3 corrupted trees are accepted by the pre-X-0-F3 `check()`
//! (recorded in that card's RESULT block); each rejection below can
//! therefore fail only through the rule that card adds. The two
//! X-0-F3-F1 trees are likewise accepted under their respective mutant
//! (measured by the X-0-F3 gate and re-measured in this card's RESULT
//! block), so each pin is non-vacuous.

use e0_btree::page::{self, PageKind};
use e0_btree::{BTree, MemStore, PageId, PageStore, StoreError};

/// A u16 key in big-endian memcomparable form (the encoding `model.rs`
/// uses; ascending u16 order equals ascending byte order).
fn k(n: u16) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Hand-build one page: allocate it in the store, run the pub `page`
/// builders on a fresh buffer and write the bytes back.
fn build_page<F>(store: &MemStore, build: F) -> Result<PageId, StoreError>
where
    F: FnOnce(&mut [u8]) -> Result<(), StoreError>,
{
    let id = store.allocate()?;
    let mut buf = vec![0u8; store.page_size()];
    build(&mut buf)?;
    store.write(id, &buf)?;
    Ok(id)
}

/// A leaf holding `keys` (any order; inserted at their sorted position).
fn build_leaf(store: &MemStore, keys: &[u16]) -> Result<PageId, StoreError> {
    build_page(store, |buf| {
        page::init(buf, PageKind::Leaf)?;
        for key in keys {
            let pos = match page::leaf_find(buf, &k(*key))? {
                Ok(_) => return Err(StoreError::Corrupt("duplicate key in builder".into())),
                Err(p) => p,
            };
            if !page::leaf_insert(buf, pos, &k(*key), b"v")? {
                return Err(StoreError::Corrupt("builder leaf overflow".into()));
            }
        }
        Ok(())
    })
}

/// An inner page with `(children[i], seps[i])` cells and `right` as its
/// right-most child (cells given in ascending separator order).
fn build_inner(
    store: &MemStore,
    children: &[PageId],
    seps: &[u16],
    right: PageId,
) -> Result<PageId, StoreError> {
    build_page(store, |buf| {
        page::init(buf, PageKind::Inner)?;
        for (i, (child, sep)) in children.iter().zip(seps).enumerate() {
            if !page::inner_insert(buf, i, *child, &k(*sep))? {
                return Err(StoreError::Corrupt("builder inner overflow".into()));
            }
        }
        page::set_right_child(buf, right)?;
        Ok(())
    })
}

/// The three-level tree of card X-0-F3 (root → 2 inner pages → 5
/// leaves), with one knob per corruption:
///
/// ```text
/// root (bounds open):        cell (A, 100), right = B
/// A (bounds (-inf, 100)):    cell (leaf0, 50), cell (leaf1, a_last), right = leaf2
/// B (bounds [100, +inf)):    cell (leaf3, 200), right = leaf4
/// leaf0: 10, 20, 30, 40      window (-inf, 50)
/// leaf1: 55, 60, 70          window [50, a_last)
/// leaf2: (empty leaf; T-NOMERGE leaves empty pages behind) window [a_last, 100)
/// leaf3: 100, 120, 150, 180  (+ extra_leftmost when corrupting) window [100, 200)
/// leaf4: 200, 250, 300, 400  window [200, +inf)
/// ```
///
/// `a_last = 75` with `extra_leftmost = None` is the clean tree. The two
/// corruptions (each accepted by the pre-card `check()`):
/// - `extra_leftmost = Some(90)` puts a key below the root separator 100
///   into leaf3, the leftmost leaf of the right inner page B — the exact
///   child-0 case of finding 3. Nothing else is disturbed: leaf3's own
///   upper bound (200) still covers the key, so only the inherited
///   lower bound can reject it.
/// - `a_last = 100` or `150` raises A's second separator onto/above A's
///   inherited upper bound (the root separator 100). Every leaf key
///   stays inside its own immediate bounds — leaf2 is empty precisely so
///   the narrowed window `[a_last, 100)` holds no key — so only the new
///   separator-vs-inherited-bounds rule can reject the page.
fn three_level_tree(
    a_last: u16,
    extra_leftmost: Option<u16>,
) -> Result<BTree<MemStore>, StoreError> {
    let store = MemStore::new(8192)?;
    let leaf0 = build_leaf(&store, &[10, 20, 30, 40])?;
    let leaf1 = build_leaf(&store, &[55, 60, 70])?;
    let leaf2 = build_leaf(&store, &[])?;
    let mut leaf3_keys = vec![100u16, 120, 150, 180];
    if let Some(extra) = extra_leftmost {
        leaf3_keys.push(extra);
        leaf3_keys.sort_unstable();
    }
    let leaf3 = build_leaf(&store, &leaf3_keys)?;
    let leaf4 = build_leaf(&store, &[200, 250, 300, 400])?;
    let a = build_inner(&store, &[leaf0, leaf1], &[50, a_last], leaf2)?;
    let b = build_inner(&store, &[leaf3], &[200], leaf4)?;
    let root = build_inner(&store, &[a], &[100], b)?;
    BTree::open(store, 16, root)
}

/// The clean tree must pass the tightened `check()`: the new rules have
/// no false positives on a well-formed three-level tree (card X-0-F3
/// Work 4).
#[test]
fn check_accepts_a_valid_three_level_tree() {
    let tree = three_level_tree(75, None).expect("clean hand-built tree");
    let stats = tree.check().expect("clean tree passes the tightened check");
    assert_eq!(stats.height, 3, "root -> 2 inner pages -> leaves");
    assert_eq!(stats.pages, 8, "root + 2 inner pages + 5 leaves");
}

/// Pin 1 (finding 3, child 0): a key below the separator above it must
/// be rejected even when it sits in the leftmost leaf of an inner page —
/// that leaf's own parent supplies no separator below it, so the bound
/// has to arrive as the inner page's inherited `lo` (card X-0-F3 Work 1).
/// The pre-card `check()` accepted this tree because child 0 started
/// unbounded below.
#[test]
fn check_rejects_key_below_leftmost_subtree_bound() {
    let tree = three_level_tree(75, Some(90)).expect("corrupted tree still builds");
    let err = tree
        .check()
        .expect_err("key 90 in leaf3 is below B's inherited bound 100");
    assert!(
        err.contains("key below the subtree bound"),
        "unexpected message: {err}"
    );
}

/// Pin 2 (finding 3, separators): a separator that sits on or above its
/// page's inherited upper bound — here the root separator 100 above
/// inner page A — must be rejected, even though every leaf key still
/// fits its own immediate bounds (card X-0-F3 Work 2). `a_last = 150`
/// covers `s > hi`; `a_last = 100` covers `s == hi`, the boundary the
/// SEP-HI-INCLUSIVE mutant (`s <= hi`) would wave through. The pre-card
/// `check()` accepted both trees because separators were never compared
/// with the inherited bounds.
#[test]
fn check_rejects_separator_outside_inherited_bounds() {
    for a_last in [150u16, 100] {
        let tree = three_level_tree(a_last, None).expect("corrupted tree still builds");
        let err = tree
            .check()
            .expect_err("a separator must stay inside the inherited bounds");
        assert!(
            err.contains("separator at cell 1 above the subtree bound"),
            "a_last={a_last}: unexpected message: {err}"
        );
    }
}

/// The X-0-F3 gate's probe tree for its finding 1 (mutant
/// G-SEP-LO-SKIPPED, which drops only the `sep < lo` half of the
/// separator rule): inner page B whose **cell-0** separator sits below
/// B's inherited `lo`:
///
/// ```text
/// root (bounds open):     cell (A, 100), right = B
/// A (bounds (-inf, 100)): cell (leaf0, 50), right = leaf1
/// B (bounds [100, +inf)): cell (leaf_e, 90), cell (leaf95, 200), right = leaf_r
/// leaf0: 10, 20           window (-inf, 50)
/// leaf1: 60, 70           window [50, 100)
/// leaf_e: (empty)         window [100, 90) — inverted, but keyless
/// leaf95: 95, 120         window [90, 200)
/// leaf_r: 200, 300        window [200, +inf)
/// ```
///
/// Every key fits the window the separators immediately around it
/// describe, so no child-level rule can reject anything: with the
/// `sep < lo` rule gone, child 0 is an *empty* leaf (nothing to compare
/// against its inherited `lo` of 100) and key 95 in child 1 lies inside
/// `[sep, hi) = [90, 200)` — a key routed below the subtree bound of
/// 100. Only the separator rule itself catches the page, which is why
/// it needs its own pin (card X-0-F3-F1 Work 1). The gate measured this
/// exact tree as `Err("page PageId(6): separator at cell 0 below the
/// subtree bound")` on the real code and `Ok(height 3, 8 pages)` under
/// the mutant (allocation order below makes B `PageId(6)`, as in the
/// verdict).
fn separator_below_lo_tree() -> Result<BTree<MemStore>, StoreError> {
    let store = MemStore::new(8192)?;
    let leaf0 = build_leaf(&store, &[10, 20])?;
    let leaf1 = build_leaf(&store, &[60, 70])?;
    let leaf_e = build_leaf(&store, &[])?;
    let leaf95 = build_leaf(&store, &[95, 120])?;
    let leaf_r = build_leaf(&store, &[200, 300])?;
    let a = build_inner(&store, &[leaf0], &[50], leaf1)?;
    let b = build_inner(&store, &[leaf_e, leaf95], &[90, 200], leaf_r)?;
    let root = build_inner(&store, &[a], &[100], b)?;
    BTree::open(store, 16, root)
}

/// Pin 3 (X-0-F3 verdict finding 1, mutant G-SEP-LO-SKIPPED): a
/// separator below its page's inherited lower bound must be rejected on
/// its own merits. The damage of such a separator is *widening* — the
/// child after it inherits a window reaching below `lo` — so the
/// child-level key checks are exactly what it subverts and cannot be
/// counted on to catch it (card X-0-F3-F1 Work 1; the empty child 0 and
/// the key in `[sep, lo)` in child 1 leave the separator rule as the
/// only possible rejection).
#[test]
fn check_rejects_separator_below_inherited_bound() {
    let tree = separator_below_lo_tree().expect("corrupted tree still builds");
    let err = tree
        .check()
        .expect_err("B's cell-0 separator 90 is below B's inherited bound 100");
    assert!(
        err.contains("separator at cell 0 below the subtree bound"),
        "unexpected message: {err}"
    );
}

/// The probe tree for the X-0-F3 gate's finding 2 (mutant
/// G-LO-PRECEDENCE, `lo.or(prev_sep)`, which hands cells 1.. of a
/// non-root inner page the page's own `lo` instead of the previous
/// separator): a key in a non-first child of a **non-root** inner page,
/// below that child's previous separator but at or above the page's own
/// `lo`:
///
/// ```text
/// root (bounds open):     cell (A, 100), right = B
/// A (bounds (-inf, 100)): cell (leaf0, 50), right = leaf1
/// B (bounds [100, +inf)): cell (leaf3, 200), cell (leaf3b, 300), right = leaf4
/// leaf0: 10, 20           window (-inf, 50)
/// leaf1: 60, 70           window [50, 100)
/// leaf3: 100, 120         window [100, 200)   (child 0: B's `lo`)
/// leaf3b: 150             window [200, 300)   (child 1: previous separator)
/// leaf4: 300, 400         window [300, +inf)
/// ```
///
/// Key 150 in leaf3b is below the previous separator 200 — rejected
/// only because later children take `prev_sep` — yet at or above B's
/// own `lo` of 100, so `lo.or(prev_sep)` accepts the whole tree. B has
/// to be non-root for the two operands to differ (the root's `lo` is
/// open, which is also why the `A` side of the tree can never catch
/// this mutant); A exists to give B a set `lo` via the root separator
/// (card X-0-F3-F1 Work 2).
fn key_below_previous_separator_tree() -> Result<BTree<MemStore>, StoreError> {
    let store = MemStore::new(8192)?;
    let leaf0 = build_leaf(&store, &[10, 20])?;
    let leaf1 = build_leaf(&store, &[60, 70])?;
    let leaf3 = build_leaf(&store, &[100, 120])?;
    let leaf3b = build_leaf(&store, &[150])?;
    let leaf4 = build_leaf(&store, &[300, 400])?;
    let a = build_inner(&store, &[leaf0], &[50], leaf1)?;
    let b = build_inner(&store, &[leaf3, leaf3b], &[200, 300], leaf4)?;
    let root = build_inner(&store, &[a], &[100], b)?;
    BTree::open(store, 16, root)
}

/// Pin 4 (X-0-F3 verdict finding 2, mutant G-LO-PRECEDENCE): cells 1..
/// of an inner page take the *previous separator* as their child's
/// lower bound, not the page's inherited `lo` — the two differ exactly
/// when the page is non-root, and this tree's key 150 sits in the gap
/// between them (card X-0-F3-F1 Work 2). This pins the operand order of
/// the `prev_sep.clone().or_else(|| lo.clone())` line against the
/// `.or()`-slip edits that working next to it invites.
#[test]
fn check_rejects_key_below_previous_separator_in_inner_page() {
    let tree = key_below_previous_separator_tree().expect("corrupted tree still builds");
    let err = tree
        .check()
        .expect_err("key 150 in leaf3b is below its previous separator 200");
    assert!(
        err.contains("key below the subtree bound"),
        "unexpected message: {err}"
    );
}
