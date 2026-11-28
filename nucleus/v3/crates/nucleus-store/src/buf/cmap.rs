//! The current page map (card A-3c, Work 3; S-STORE §4.1, §5.4 "What a pin
//! finds", §5.6 "Restart"): MapInners outside the pool and always resident,
//! MapLeafs through the pool under `PageKey::MapLeaf`, the real
//! [`CurrentRef`], MapLeaf entries and MapInner refs set in memory, and
//! growth by levels and MapInners, one map latch at a time, top-down.
//!
//! Invariants owned here, each rule quoting S-STORE:
//! - **MapInners are always resident, outside the pool** (§4.1 "Residency";
//!   §5.7): a MapInner is a node of this module in the pool's node list,
//!   which only grows, and "making one obtains no frame, evicts nothing and
//!   reads nothing" (§4.1 "Growth").
//! - **A MapInner's body mutex is its map latch**: exclusive only. It is a
//!   latch that hands its content to the holder ([`BodyCell`]): whoever
//!   holds the body holds the latch, and drops it back into the cell to
//!   release, which wakes the next waiter. So the latch has an owned guard
//!   with no lifetime ([`LeafParent`]), which §5.6's restart needs, since it
//!   holds the MapInner's latch across `create`. The code here holds it only
//!   for the body it reads or changes, never across I/O, a frame
//!   acquisition, a frame-latch wait, a hook or another MapInner's latch
//!   (§4.1: "it holds one map latch at a time, top-down"; §5.3 "Order": map
//!   pages rank after every tree page, and the page table after them, A-3a's
//!   BUF-TABLE-LEAF). The one nesting is the growth's: the root's latch,
//!   then the root pointer, then the node list, each released before
//!   anything below is latched.
//! - **Level 1 holds the MapLeafs' refs** (§4.1): `refs[j]` of the MapInner
//!   at `(1, index)` is the current ref of the MapLeaf at `index·f + j`: its
//!   home, or `Ref::NULL` "from its making (§5.6 "Restart") until a write of
//!   its frame gives it one". At level ≥ 2 `children[j]` names the child
//!   MapInner "from its making"; this card neither reads nor writes the
//!   `refs` of a level ≥ 2 (A-3c-C's map phase and A-7b/A-7d own them).
//! - **`content_epoch` and `dirty`** follow §1 and §5.6 "When it copies": a
//!   mutation sets `dirty = true` and `content_epoch = e`; nothing here
//!   clears `dirty` (the map phase writes MapInners, A-3c-C).
//! - **Growth** (§4.1 "Growth", I-MAP-GROW): "a new level under the current
//!   root's exclusive latch, made only if the root is unchanged and still
//!   lacks the index; a MapInner under its parent's exclusive latch, made
//!   only if the ref is still null; neither obtains a frame or reads a
//!   page". Positions count from the leaf level, so a new level changes no
//!   page's id (card A-2's `position_id`).
//! - **The touch check** (Work 6.4) stands in for `write_guard`'s copy
//!   decision until A-7a, on a MapLeaf's entries only: "It never copies a
//!   MapInner" (§5.6 "When it copies", card S-MU), so a MapInner's ref or
//!   child is set with no check. A MapLeaf mutation in epoch `e` of content
//!   from an earlier epoch needs the epoch copy A-7a builds and is `Invalid`
//!   here, except for a clean MapLeaf after S1's stand-in flip, which S1
//!   counts as the map phase having taken its content (see `touch_check`).
//! - **A load reads the map holding no map latch while it pins or waits**
//!   (§5.4: "No thread waits for a load while it holds a map latch"; §4.2).
//!
//! Tests `cmap_growth`, `entry_roundtrip`, `growth_race_level`,
//! `growth_race_inner`, `touch_check_table`; mutants MAPINNER-CREATE-
//! UNCONDITIONAL, LEVEL-GROW-UNCONDITIONAL, WB-GROW-INDEX-FROM-ROOT,
//! WB-TOUCH-SKIP.

use nucleus_env::File;

use super::gate::GateHold;
use super::key::PageKey;
use super::pool::{CurrentRef, HookPoint, PinOutcome, Pool};
use super::sync::{Arc, Condvar, Mutex, Primitives, StdPrimitives};
use super::wb::{Hold, NO_FRAME};
use crate::map::{self, MapGeometry};
use crate::meta::Ref;
use crate::store::StoreError;

/// An index into the map's node list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InnerId(pub(crate) u32);

/// A MapInner's content: what its map latch guards.
#[derive(Debug)]
pub(crate) struct InnerBody {
    /// The epoch of the content (§1), which a mutation sets to `e`.
    pub(crate) content_epoch: u32,
    /// Whether the body was mutated since the map phase last wrote it
    /// (§5.6 "When it copies"); set by every mutation, cleared by nothing
    /// at this card.
    pub(crate) dirty: bool,
    /// At level 1, the current refs of the MapLeafs it covers (§4.1); at
    /// level ≥ 2, what the open loaded, which this card does not touch.
    pub(crate) refs: Box<[Ref]>,
    /// The children MapInners, set from each one's making; at level 1, all
    /// `None`.
    pub(crate) children: Box<[Option<InnerId>]>,
}

impl InnerBody {
    /// The body a released [`LeafParent`] leaves in its place while it gives
    /// the real one back: no refs and no children, which allocates nothing.
    fn empty() -> InnerBody {
        InnerBody {
            content_epoch: 0,
            dirty: false,
            refs: Vec::new().into_boxed_slice(),
            children: Vec::new().into_boxed_slice(),
        }
    }
}

/// What a cell holds under its mutex: the body, unless a holder has it, and
/// how many latch requests wait for it.
struct Slot {
    body: Option<InnerBody>,
    waiters: u32,
}

/// A MapInner's map latch and what it guards (card A-3c, Work 3.1): the
/// holder takes the body out and holds it by value, and puts it back to
/// release; a request that finds the body taken waits on `wake`, counted
/// under the same mutex, and is woken when the body returns. No wake-up can
/// be lost: the count, the wait and the return all happen under one mutex.
pub(crate) struct BodyCell<P: Primitives> {
    slot: Mutex<P, Slot>,
    wake: Condvar<P>,
}

impl<P: Primitives> BodyCell<P> {
    /// A cell holding `body`, latched by nobody.
    fn new(body: InnerBody) -> BodyCell<P> {
        BodyCell {
            slot: Mutex::new(Slot {
                body: Some(body),
                waiters: 0,
            }),
            wake: Condvar::new(),
        }
    }

    /// Take the latch (blocking): the holder owns the body until it gives it
    /// back with [`BodyCell::give_back`].
    fn take(&self) -> Result<InnerBody, StoreError> {
        self.take_then(|| {})
    }

    /// [`BodyCell::take`], calling `before_wait` once if the body is taken:
    /// after the request is counted, with the cell's mutex released, so a
    /// hook it fires holds no mutex (card A-3c, Work 10). The wait then
    /// re-tests under the mutex, so a give-back in between is not lost: it
    /// finds the count raised and notifies, or the waiter finds the body.
    fn take_then(&self, before_wait: impl FnOnce()) -> Result<InnerBody, StoreError> {
        let mut s = self.slot.lock().map_err(|_| poisoned("map latch"))?;
        if s.body.is_none() {
            s.waiters = s
                .waiters
                .checked_add(1)
                .ok_or_else(|| StoreError::Invalid("a map latch's waiters passed u32".into()))?;
            drop(s);
            before_wait();
            s = self.slot.lock().map_err(|_| poisoned("map latch"))?;
            while s.body.is_none() {
                s = self.wake.wait(s).map_err(|_| poisoned("map latch"))?;
            }
            s.waiters = s.waiters.saturating_sub(1);
        }
        s.body
            .take()
            .ok_or_else(|| StoreError::Invalid("a map latch holder found no body to hold".into()))
    }

    /// Release the latch: the body returns, and a waiter is woken.
    fn give_back(&self, body: InnerBody) -> Result<(), StoreError> {
        let mut s = self.slot.lock().map_err(|_| poisoned("map latch"))?;
        if s.body.is_some() {
            return Err(StoreError::Invalid(
                "a MapInner's body was given back while another holder had it".into(),
            ));
        }
        s.body = Some(body);
        if s.waiters > 0 {
            self.wake.notify_all();
        }
        Ok(())
    }

    /// Latch, change or read the body through `f`, and release: the form for
    /// a hold that ends before the call returns. The body is given back
    /// whatever `f` answers.
    fn with<R>(
        &self,
        f: impl FnOnce(&mut InnerBody) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let mut body = self.take()?;
        let answer = f(&mut body);
        match (self.give_back(body), answer) {
            (Ok(()), answer) => answer,
            (Err(e), _) => Err(e),
        }
    }
}

/// A MapInner page, always resident (S-STORE §4.1 "Residency"): its position
/// and its body behind the map latch.
pub(crate) struct MapInner<P: Primitives> {
    /// The level, counted from the leaf level (0).
    pub(crate) level: u8,
    /// The index at that level.
    pub(crate) index: u64,
    /// The body and its latch.
    pub(crate) cell: Arc<BodyCell<P>>,
}

/// The current page map (S-STORE §4.1): its geometry, its root and every
/// MapInner it holds. The node list only grows, so an id read from the root
/// stays valid while its `Arc` is fetched.
pub(crate) struct CurrentMap<P: Primitives> {
    /// The store's page size and fanout.
    pub(crate) geom: MapGeometry,
    /// The root's id and level, under their own mutex.
    root: Mutex<P, (InnerId, u8)>,
    /// Every MapInner, by `InnerId`.
    inners: Mutex<P, Vec<Arc<MapInner<P>>>>,
}

/// The map's shape (the diagnostics of Work 3.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapShape {
    /// The root's level, counted from the leaf level (§4.1).
    pub root_level: u8,
    /// How many MapInners the map holds.
    pub inners: u32,
}

/// The level-1 MapInner that holds a MapLeaf's ref, latched (card A-3c,
/// Work 3.8): the form §5.6's restart latches before it creates a MapLeaf.
/// The holder owns the MapInner's body, so the latch needs no lifetime;
/// dropping the guard gives the body back, which releases the latch. Hold no
/// other map latch with it, and release it before any load or wait (§5.3,
/// §5.4).
pub struct LeafParent<P: Primitives = StdPrimitives> {
    /// The cell the body goes back to; declared first, so it drops last.
    cell: Arc<BodyCell<P>>,
    /// The MapInner's body, held for the latch's life.
    body: InnerBody,
    level: u8,
    index: u64,
    j: usize,
}

impl<P: Primitives> LeafParent<P> {
    /// The MapInner's level: 1.
    pub fn level(&self) -> u8 {
        self.level
    }

    /// The MapInner's index at its level.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// The MapLeaf's current ref: its home, or `Ref::NULL` while no write of
    /// its frame has given it one (§4.1).
    pub fn leaf_ref(&self) -> Ref {
        self.body.refs.get(self.j).copied().unwrap_or(Ref::NULL)
    }
}

impl<P: Primitives> Drop for LeafParent<P> {
    /// Release the latch: the body returns to its cell, waking a waiter. On
    /// a mutex a panic poisoned it cannot, and the body is lost with the
    /// store (S-STORE §9.2). Never panics.
    fn drop(&mut self) {
        let body = core::mem::replace(&mut self.body, InnerBody::empty());
        let _ = self.cell.give_back(body);
    }
}

impl<P: Primitives> core::fmt::Debug for LeafParent<P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LeafParent")
            .field("level", &self.level)
            .field("index", &self.index)
            .finish()
    }
}

/// Work 6.4's touch check, on a MapLeaf's frame: whether a mutation of its
/// entries may go on in epoch `e` without `write_guard`'s epoch copy (§5.6
/// "When it copies", A-7a's). A MapInner is never copied (§5.6: "It never
/// copies a MapInner"), so no MapInner mutation calls it.
///
/// - Content epoch `e`: no copy is needed.
/// - Above `e`: engine damage.
/// - Below `e` and dirty: the content of image `e − 1` is only in this
///   frame, so the mutation needs the copy, which A-7a builds: `Invalid`.
/// - Below `e` and clean: S-STORE 7.23 copies "whether or not its frame is
///   dirty ... unless the map phase has taken that content for image
///   `e − 1`". Before A-7c only S1 flips, and S1 runs no step 2. This card
///   reads S1 as counting its flip as the map phase having taken every clean
///   MapLeaf's content, the sentence its implementer's report proposes for
///   S-STORE §15 S1 (card A-3c, ESCALATE 3): so the mutation goes on. It
///   loses nothing at this card: no image is read before A-3c-C, and §4.4's
///   rule here frees no home whose epoch is below `e` before step 6.
pub(crate) fn touch_check(content_epoch: u32, dirty: bool, e: u32) -> Result<(), StoreError> {
    if content_epoch == e {
        return Ok(());
    }
    if content_epoch > e {
        return Err(StoreError::Invalid(format!(
            "a map page's content epoch {content_epoch} is above the current epoch {e}"
        )));
    }
    if !dirty {
        // S1's flip counts as the map phase having taken this content.
        return Ok(());
    }
    Err(StoreError::Invalid(format!(
        "this map-page mutation needs write_guard's epoch copy (S-STORE §5.6), A-7a's: \
         content epoch {content_epoch}, epoch {e}"
    )))
}

/// The error of a map mutex a panic poisoned.
fn poisoned(what: &str) -> StoreError {
    StoreError::Poisoned(format!("the page map's {what} was poisoned by a panic"))
}

/// The error of a failed reservation (N16: 53200).
fn oom(what: &str, e: std::collections::TryReserveError) -> StoreError {
    StoreError::OutOfMemory(format!("page map {what}: {e}"))
}

impl<P: Primitives> CurrentMap<P> {
    /// A fresh map (S-STORE §2.3 step 3: "a page map whose root is one empty
    /// MapInner at level 1"): one MapInner at level 1, index 0, content epoch
    /// `e`, dirty.
    pub(crate) fn fresh(geom: MapGeometry, e: u32) -> Result<CurrentMap<P>, StoreError> {
        let root = Arc::new(Self::make_inner(&geom, 1, 0, e, |_| {})?);
        let mut inners = Vec::new();
        inners.try_reserve_exact(1).map_err(|e| oom("nodes", e))?;
        inners.push(root);
        Ok(CurrentMap {
            geom,
            root: Mutex::new((InnerId(0), 1)),
            inners: Mutex::new(inners),
        })
    }

    /// The fanout `f`.
    pub(crate) fn fanout(&self) -> u64 {
        self.geom.fanout()
    }

    /// Make a MapInner at `(level, index)`, content epoch `e`, dirty, every
    /// ref null and no child, with `init` to set its children. Every
    /// allocation is fallible but the `Arc`s, which std makes abort on
    /// failure (it has no fallible `Arc::new` on this toolchain).
    fn make_inner(
        geom: &MapGeometry,
        level: u8,
        index: u64,
        e: u32,
        init: impl FnOnce(&mut [Option<InnerId>]),
    ) -> Result<MapInner<P>, StoreError> {
        let f = usize::try_from(geom.fanout())
            .map_err(|_| StoreError::Invalid("a map fanout past usize".into()))?;
        let mut refs = Vec::new();
        refs.try_reserve_exact(f).map_err(|e| oom("refs", e))?;
        refs.resize(f, Ref::NULL);
        let mut children = Vec::new();
        children
            .try_reserve_exact(f)
            .map_err(|e| oom("children", e))?;
        children.resize(f, None);
        let mut children = children.into_boxed_slice();
        init(&mut children);
        Ok(MapInner {
            level,
            index,
            cell: Arc::new(BodyCell::new(InnerBody {
                content_epoch: e,
                dirty: true,
                refs: refs.into_boxed_slice(),
                children,
            })),
        })
    }

    /// The root's id and level.
    fn root(&self) -> Result<(InnerId, u8), StoreError> {
        self.root.lock().map(|r| *r).map_err(|_| poisoned("root"))
    }

    /// Set the root.
    fn set_root(&self, id: InnerId, level: u8) -> Result<(), StoreError> {
        self.root
            .lock()
            .map(|mut r| *r = (id, level))
            .map_err(|_| poisoned("root"))
    }

    /// Node `id`: a clone of its `Arc`, which keeps it alive whatever the
    /// node list does.
    fn inner(&self, id: InnerId) -> Result<Arc<MapInner<P>>, StoreError> {
        let inners = self.inners.lock().map_err(|_| poisoned("node list"))?;
        inners
            .get(
                usize::try_from(id.0)
                    .map_err(|_| StoreError::Invalid("a map node id past usize".into()))?,
            )
            .cloned()
            .ok_or_else(|| StoreError::Invalid(format!("map node {} does not exist", id.0)))
    }

    /// Append `made` to the node list and return its id. Its place is
    /// reserved first, so a failed reservation changes nothing.
    fn push(&self, made: MapInner<P>) -> Result<InnerId, StoreError> {
        let mut inners = self.inners.lock().map_err(|_| poisoned("node list"))?;
        inners.try_reserve(1).map_err(|e| oom("nodes", e))?;
        let at = u32::try_from(inners.len()).map_err(|_| {
            StoreError::Invalid("the page map holds more nodes than a u32 names".into())
        })?;
        inners.push(Arc::new(made));
        Ok(InnerId(at))
    }

    /// `f^(level − 1)`: the MapLeaf indexes one child of a level-`level`
    /// node covers.
    fn span(&self, level: u8) -> Result<u64, StoreError> {
        self.geom
            .leaf_cover(
                level
                    .checked_sub(1)
                    .ok_or_else(|| StoreError::Invalid("a MapLeaf has no span".into()))?,
            )
            .ok_or_else(|| StoreError::Invalid("a map level past u64's cover".into()))
    }

    /// The slot of the MapInner at `level` on the path to `leaf`.
    fn slot(&self, leaf: u64, level: u8) -> Result<usize, StoreError> {
        let span = self.span(level)?;
        usize::try_from((leaf / span) % self.fanout())
            .map_err(|_| StoreError::Invalid("a map slot past usize".into()))
    }

    /// Work 3.4: the level-1 MapInner that holds the MapLeaf `i`'s ref, or
    /// `None` if the map does not cover `i` or the path to it lacks a
    /// MapInner. One map latch at a time, none held while another is taken.
    fn leaf_parent(&self, i: u64) -> Result<Option<Arc<MapInner<P>>>, StoreError> {
        let (mut id, level) = self.root()?;
        match self.geom.leaf_cover(level) {
            Some(cover) if i < cover => {}
            _ => return Ok(None),
        }
        let mut level = level;
        while level >= 2 {
            let node = self.inner(id)?;
            let slot = self.slot(i, level)?;
            let child = node
                .cell
                .with(|body| Ok(body.children.get(slot).copied().flatten()))?;
            match child {
                Some(c) => {
                    id = c;
                    level -= 1;
                }
                None => return Ok(None),
            }
        }
        self.inner(id).map(Some)
    }

    /// Work 3.5, the second step of §4.1's two-step write: set the MapLeaf
    /// `i`'s ref to `r` in its parent, and the ref it replaces.
    pub(crate) fn set_mapleaf_ref(&self, i: u64, r: Ref, e: u32) -> Result<Ref, StoreError> {
        let parent = self.leaf_parent(i)?.ok_or_else(|| {
            StoreError::Invalid(format!("MapLeaf {i} exists but its MapInner path does not"))
        })?;
        let j = usize::try_from(i % self.fanout())
            .map_err(|_| StoreError::Invalid("a map slot past usize".into()))?;
        // No touch check: a MapInner is never copied (§5.6 "When it
        // copies"). So nothing here can refuse once E6 has marked the
        // MapLeaf written, except engine damage (a missing path or ref).
        parent.cell.with(|body| {
            let old = body
                .refs
                .get(j)
                .copied()
                .ok_or_else(|| StoreError::Invalid("a map ref past the fanout".into()))?;
            let place = body
                .refs
                .get_mut(j)
                .ok_or_else(|| StoreError::Invalid("a map ref past the fanout".into()))?;
            *place = r;
            body.dirty = true;
            body.content_epoch = e;
            Ok(old)
        })
    }
}

impl<H: File, P: Primitives> CurrentRef<H, P> for CurrentMap<P> {
    fn current_ref(&self, pool: &Pool<H, P>, key: PageKey) -> Result<Ref, StoreError> {
        self.current_ref_with(pool, key, &Hold::nothing())
    }

    fn current_ref_with(
        &self,
        pool: &Pool<H, P>,
        key: PageKey,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<Ref, StoreError> {
        match key {
            // A MapLeaf's ref is in its parent, always resident: no frame is
            // pinned for it.
            PageKey::MapLeaf(i) => match self.leaf_parent(i)? {
                None => Ok(Ref::NULL),
                Some(parent) => {
                    let j = usize::try_from(i % self.fanout())
                        .map_err(|_| StoreError::Invalid("a map slot past usize".into()))?;
                    parent
                        .cell
                        .with(|body| Ok(body.refs.get(j).copied().unwrap_or(Ref::NULL)))
                }
            },
            // An LPID's entry is in its MapLeaf's frame, which the load pins
            // through the pool under the loader's hold, marked in a load; it
            // never pins `key` itself (A-3a OBL-3) and holds no map latch
            // while it pins or waits.
            PageKey::Lpid(x) => {
                let (i, j) = self.geom.lpid_entry(x);
                match pool.pin_with(PageKey::MapLeaf(i), self, &hold.in_load())? {
                    PinOutcome::Unmapped => Ok(Ref::NULL),
                    PinOutcome::Pinned(p) => {
                        let g = p.latch_shared()?;
                        let entry = u64::try_from(j)
                            .map_err(|_| StoreError::Invalid("a map entry past u64".into()))?;
                        let r = map::map_ref(&g, &self.geom, entry);
                        drop(g);
                        drop(p);
                        r
                    }
                }
            }
        }
    }
}

impl<H: File, P: Primitives> Pool<H, P> {
    /// The map's shape (the diagnostics of Work 3.7).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back;
    /// [`StoreError::Poisoned`] if a mutex was poisoned.
    pub fn map_shape(&self) -> Result<MapShape, StoreError> {
        let cmap = self.cmap()?;
        let (_, root_level) = cmap.root()?;
        let inners = cmap.inners.lock().map_err(|_| poisoned("node list"))?;
        Ok(MapShape {
            root_level,
            inners: u32::try_from(inners.len()).map_err(|_| {
                StoreError::Invalid("the page map holds more nodes than a u32 names".into())
            })?,
        })
    }

    /// The position of the level-1 MapInner that holds MapLeaf `leaf`'s ref,
    /// its index at level 1 (`leaf / f`), or `None` if the map does not cover
    /// `leaf` (the diagnostics of Work 3.7): a position no later growth
    /// changes, since positions count from the leaf level.
    ///
    /// # Errors
    /// As [`Pool::map_shape`].
    pub fn leaf_parent_id(&self, leaf: u64) -> Result<Option<u32>, StoreError> {
        let cmap = self.cmap()?;
        Ok(cmap
            .leaf_parent(leaf)?
            .map(|node| u32::try_from(node.index).unwrap_or(u32::MAX)))
    }

    /// Work 3.8: latch the level-1 MapInner that holds MapLeaf `leaf`'s ref
    /// (§5.6 "Restart": "does it latch exclusively the MapInner that holds
    /// the MapLeaf's ref"), for the caller that creates the MapLeaf in a
    /// frame it obtained first. `Invalid` if the map does not cover `leaf`
    /// or the path to it lacks a MapInner. Hold no other map latch, and
    /// release this one before any load or wait (§5.3, §5.4).
    ///
    /// If another thread holds that MapInner's latch,
    /// [`HookPoint::MapLatchWaiting`] fires before this one waits, with the
    /// MapLeaf's key and no frame, holding nothing.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back or for a missing
    /// path; [`StoreError::Poisoned`] if a mutex was poisoned.
    pub fn latch_leaf_parent(&self, leaf: u64) -> Result<LeafParent<P>, StoreError> {
        let cmap = self.cmap()?;
        let node = cmap.leaf_parent(leaf)?.ok_or_else(|| {
            StoreError::Invalid(format!("the page map has no MapInner above MapLeaf {leaf}"))
        })?;
        let j = usize::try_from(leaf % cmap.fanout())
            .map_err(|_| StoreError::Invalid("a map slot past usize".into()))?;
        let body = node.cell.take_then(|| {
            self.fire(HookPoint::MapLatchWaiting, PageKey::MapLeaf(leaf), NO_FRAME);
        })?;
        Ok(LeafParent {
            cell: Arc::clone(&node.cell),
            body,
            level: node.level,
            index: node.index,
            j,
        })
    }

    /// Work 3.7, growth (S-STORE §4.1 "Growth", I-MAP-GROW): grow the map
    /// until the path to MapLeaf `leaf` exists, in the caller's own gated
    /// operation `g` and before it makes the MapLeaf. A new level is made
    /// under the current root's latch only if the root is unchanged and
    /// still does not cover `leaf`; a MapInner is made under its parent's
    /// latch only if the parent's child is still null. One map latch at a
    /// time, top-down; no frame, no page read, no wait.
    ///
    /// [`HookPoint::GrowLocking`] fires before each MapInner's latch is
    /// taken, with the MapLeaf's key and no frame.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back;
    /// [`StoreError::OutOfMemory`] if a node cannot
    /// be allocated, nothing changed; [`StoreError::Poisoned`] if a mutex
    /// was poisoned.
    pub fn grow_map(&self, leaf: u64, g: &GateHold<'_, H, P>) -> Result<(), StoreError> {
        let cmap = self.cmap()?;
        let e = g.epoch();
        let key = PageKey::MapLeaf(leaf);

        // A new level, repeated until the root covers `leaf`.
        loop {
            let (rid, level) = cmap.root()?;
            match cmap.geom.leaf_cover(level) {
                Some(cover) if leaf < cover => break,
                Some(_) => {}
                None => {
                    return Err(StoreError::Invalid(
                        "the page map's root covers more than a u64 index".into(),
                    ));
                }
            }
            let r = cmap.inner(rid)?;
            // Holding no map latch: the thread has read the root and not yet
            // locked it.
            self.fire(HookPoint::GrowLocking, key, NO_FRAME);
            r.cell.with(|_body| {
                // The root again, under the root's latch: whoever changed it
                // held this same latch to.
                let now = cmap.root()?;
                if now != (rid, level) {
                    // Another thread grew the map first: go on from the new
                    // root.
                    return Ok(());
                }
                if level == u8::MAX {
                    return Err(StoreError::Invalid(
                        "the page map's root is at the top level".into(),
                    ));
                }
                let made = CurrentMap::make_inner(&cmap.geom, level + 1, 0, e, |children| {
                    if let Some(first) = children.first_mut() {
                        *first = Some(rid);
                    }
                })?;
                let new = cmap.push(made)?;
                cmap.set_root(new, level + 1)
            })?;
        }

        // A MapInner, from the root down to level 2.
        let (mut id, mut level) = cmap.root()?;
        while level >= 2 {
            let node = cmap.inner(id)?;
            let slot = cmap.slot(leaf, level)?;
            self.fire(HookPoint::GrowLocking, key, NO_FRAME);
            let made = node.cell.with(|body| {
                // The decision is made under the latch it changes, after the
                // hook.
                if let Some(child) = body.children.get(slot).copied().flatten() {
                    return Ok(child);
                }
                // No touch check: a MapInner is never copied (§5.6 "When
                // it copies"; §4.1 "Growth": "The parent a new MapInner
                // changes is changed through `write_guard`, which copies no
                // MapInner").
                let span = cmap.span(level)?;
                let made = CurrentMap::make_inner(&cmap.geom, level - 1, leaf / span, e, |_| {})?;
                let new = cmap.push(made)?;
                if let Some(place) = body.children.get_mut(slot) {
                    *place = Some(new);
                }
                body.dirty = true;
                body.content_epoch = e;
                Ok(new)
            })?;
            id = made;
            level -= 1;
        }
        Ok(())
    }

    /// Work 3.6: set LPID `lpid`'s entry to `r` in its MapLeaf, in epoch
    /// `e`, and the entry it replaces. The caller holds the page's exclusive
    /// latch (the eviction's victim guard, Work 7, or the free's guard,
    /// through [`Pool::set_entry`]), which is what keeps the reference
    /// moving "only under a frame" (S-STORE §5.4, I-PIN-CURRENT).
    ///
    /// The MapLeaf must be resident (its page is pinned or holds §4.1's pin
    /// for this page), and its latch is taken blocking: a map latch under at
    /// most one frame latch, the caller's.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the MapLeaf is not resident or the touch
    /// check refuses (Work 6.4); [`StoreError::Poisoned`] if a mutex was
    /// poisoned.
    pub(crate) fn set_entry_locked(&self, lpid: u64, r: Ref, e: u32) -> Result<Ref, StoreError> {
        let cmap = self.cmap()?;
        let (i, j) = cmap.geom.lpid_entry(lpid);
        let Some(p) = self.pin_resident_internal(PageKey::MapLeaf(i))? else {
            return Err(StoreError::Invalid(format!(
                "LPID {lpid}'s MapLeaf {i} is not resident: its entry is set only for a page \
                 whose MapLeaf a pin already holds (S-STORE §4.1, §5.6)"
            )));
        };
        let mut g = p.latch_exclusive()?;
        touch_check(g.content_epoch(), g.is_dirty(), e)?;
        // A MapLeaf holds no §4.1 pin: its parent is a MapInner, outside the
        // pool.
        g.mark_dirty(e)?;
        let entry =
            u64::try_from(j).map_err(|_| StoreError::Invalid("a map entry past u64".into()))?;
        let old = map::map_ref(&g, &cmap.geom, entry)?;
        map::set_map_ref(&mut g, &cmap.geom, entry, r)?;
        drop(g);
        drop(p);
        Ok(old)
    }

    /// [`Pool::set_entry_locked`] for A-4b's free (§4.2 "Freeing"; A-3a
    /// OBL-5) and for the tests' S3 drivers: under the caller's gate hold
    /// `g`, whose epoch the touch check takes. The caller holds the page's
    /// exclusive latch.
    ///
    /// # Errors
    /// As [`Pool::set_entry_locked`].
    pub fn set_entry(&self, lpid: u64, r: Ref, g: &GateHold<'_, H, P>) -> Result<Ref, StoreError> {
        self.set_entry_locked(lpid, r, g.epoch())
    }
}
