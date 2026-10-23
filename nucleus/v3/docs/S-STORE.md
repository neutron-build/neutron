# S-STORE: Storage layer (normative)

Status: draft 1 (card S-1, 2026-10-23). This is a P-authored spec edit and needs a P gate before it is normative. Every `nucleus-store` card (Phase A) and the storage side of every `nucleus-txn` card that touches trees (B-1c apply, B-3 GC, B-4d bulk load, B-6 drop cleanup) implements against this file. A change to an invariant (`I-*`, §15) is made here first, then in the tests and mutants that pin it, and only then in code.

**Scope.** This file covers:
- the file, the pages, the page map, the free set and the buffer manager;
- the B+tree, including merge;
- the **versioned entry layout of N3 = (b)** and the read, scan and GC algorithms over it;
- the memory and eviction model;
- the WAL format, the checkpoint order and recovery.

It covers **what must hold**; the reference implementations show one way to meet it.
- **Reference implementations, both at `nucleus/v3-scaffold` 7ccd1cff:**
  - `spikes/e0-btree` (card X-0, the E0 tree: page, pool and tree);
  - `spikes/e1-arm-b` (card X-2, the arm (b) entry layout and read, scan and GC algorithms).
- **Where the spec departs from a reference**, it says so. A reference's spike limit is never a requirement.

**Precedence.** This file refines PLAN-v3 §3.3 (N1, N4, N8, N14) and records the storage consequences of the E1′ decision (PLAN-v3 §4, "E1′ decision: card X-4"). If the plan disagrees with this file, this file wins. §18 lists every deliberate refinement of §3.3, so the plan can be fixed through an Opus card. The transaction semantics (snapshots, the watermark W, locks, the write set, commit visibility, SSI) belong to **C-T0 v8 (card S-2)**. This file states only the preconditions it needs from them (§17).

**Error classes.** Errors use PostgreSQL SQLSTATEs:

| SQLSTATE | Meaning here |
|---|---|
| XX001 | data corrupted (an integrity check failed) |
| 58030 | I/O error |
| 53100 | disk full |
| 53200 | out of memory (N16) |
| 54000 | program limit exceeded |

---

## 1. Vocabulary

| Term | Meaning |
|---|---|
| slot | A page-sized region of the page file at offset `slot × page_size`. Slots 0–2 are fixed (§2). Every other slot is either the **home** of exactly one page image or free. |
| LPID | Logical page id, `u64`, ≥ 1 (0 = null). B+tree pages refer to each other only by LPID, never by slot (N1). |
| page map | LPID → `(slot, epoch, crc)` (§4.1). |
| epoch `e` | The checkpoint epoch: a counter that starts at 1 when the file is created and is incremented by each checkpoint's flip (§13 step 0). Checkpoint `k` flips epoch `k` to `k+1`. **Image `k`** is the state of the page map and every page at the end of epoch `k`. |
| content epoch | The epoch in which a page's content was last mutated. It is stored in the page header and the map entry, and also kept per frame. |
| meta `k` | The meta record of image `k`, in slot `1 + (k mod 2)` (§2.2). |
| durable image | The image of the newest meta whose CRC is valid on disk. |
| `U` | Usable bytes of a B+tree page: `page_size` minus the page header (§3.1) and the B+tree header. `used(p)` = the bytes of p's live cells plus its slot array. |
| user key | The C-K1 memcomparable encoding (N7) of a row's primary key or of an index key, within one tree. Opaque bytes here. |
| entry | One B+tree cell `(entry key, entry value)`. Under N3 = (b), an entry is **one version** of one user key (§7). |
| group | The contiguous run of entries of one user key, ordered newest first (§7.2). |
| `ts` | A commit timestamp (C-T0 `Ts`, `u64`). `0` and `u64::MAX` are never commit timestamps (§7.1). |
| `S` | A snapshot ts. A read at `S` sees, per user key, the newest version with `ts ≤ S`. |
| `W` | The GC watermark (C-T0 v8): `W ≤` every registered snapshot, `W ≤ visible_ts`, and monotonic (§17). |
| apply frontier | The largest ts such that every commit with ts ≤ it has finished its apply to the trees. It equals C-T0's `visible_ts` while `visible_ts` advances eagerly (C-T0 §3). |
| gated operation | A mutation that holds the **epoch gate** shared from its first page mutation to its last (§5.6). |
| SMO | Structure modification: a split, a pack step (§6.5), a root split or a root collapse. |

---

## 2. File

One page file plus a `wal/` directory of segments (N1). All I/O goes through `nucleus-env` (`Env`/`File`, PLAN-v3 §3.2). No storage code calls `std::fs` or `std::time`.

### 2.1 Page 0: file header
Written once at creation and synced before the first meta is written. Never rewritten.
```
magic          8   "NUCLEUS\0"
format_version u32
page_size      u32   4096 | 8192 | 16384 | 32768 | 65536 (default 8192)
checksum_alg   u8    1 = CRC32C
wal_seg_size   u32   bytes per WAL segment (default 64 MiB)
creation_uuid  16
crc            u32   CRC32C of the bytes above
```
The rest of the page is zero.

### 2.2 Slots 1–2: meta records
Meta `k` is written to slot `1 + (k mod 2)`, so a torn meta write can only destroy the older of the two.
```
magic          8   "NUCMETA\0"
format_version u32
ckpt_id        u64   k
T              u64   apply frontier at the flip of checkpoint k (§13 step 0)
gc_w           u64   the largest W handed to any GC mutation in image k (§10.5)
epoch          u32   k (low 32 bits; compared for equality only)
map_root       ref   (slot u64, epoch u32, crc u32) of the page-map root (§4.1)
free_root      ref   the free-set root (§4.3)
dir_root       u64   LPID of the tree directory's root (§6.8)
next_lpid      u64   one past the highest LPID ever allocated
slot_count     u64   the file length in slots covered by image k's free set
wal_start_lsn  u64   replay start (§12.6)
crc            u32   CRC32C of the bytes above
```
**At open**, the meta with the highest `ckpt_id` among those whose magic and CRC are valid wins. There is **no fallback** to the older meta when the newer one is valid but its image fails an integrity check. After meta `k` is synced, the slots of image `k−1` may be reused (§4.4), so falling back is unsafe. The open fails with XX001, and amcheck reports the failure.

### 2.3 File creation
Creation runs as one sequence:
1. write page 0 and `sync_all`;
2. build image 0 through the normal checkpoint path (§13): an empty tree directory and an empty page map;
3. write meta 0 to slot 1 and `sync_all`.

Epoch 1 then begins.

---

## 3. Pages

### 3.1 Page header (every page in slots ≥ 3)
```
crc     u32   CRC32C of bytes 4..page_size
kind    u8    3 Leaf | 4 Inner | 5 Overflow | 6 MapLeaf | 7 MapInner | 8 FreeSetLeaf | 9 FreeSetInner
flags   u8    0 (reserved)
_       u16   0
id      u64   LPID for kinds 3–5; the radix position for kinds 6–9
epoch   u32   content epoch (low 32 bits)
_       u32   0
```
That is 24 bytes; the kind-specific body follows. The format version (§2.1) pins the byte layout of every body. `spikes/e0-btree/src/page.rs` is the reference for the slotted body:
- the slot array grows up and the cells grow down;
- leaf cells are `klen, vlen, key, value`; inner cells are `klen, child LPID, key`;
- every in-page offset field must address the whole page at every allowed page size.

Card A-2 pins the bytes and fuzzes the decoders. A decoder never panics: every malformed byte sequence is XX001.

### 3.2 Size limits
- `K_MAX(page_size)` bounds an encoded **entry key** (§7.2).
- `V_INLINE(page_size)` bounds an inline entry value.

Both are derived so that every leaf and inner page holds at least **4** cells of maximum size. E0 relied on 2 (`spikes/e0-btree/README.md`, "Known limits"). The doubling gives the pack step (§6.5) room to move a cell without splitting a parent. A-2 pins the formula and the values for each allowed page size.
- **Over-size keys.** A write whose encoded entry key exceeds `K_MAX` is refused with 54000 **when it enters the write set** (S-2's write path calls the store's size check). Apply never sees it (§9).
- **Large values.** A value above `V_INLINE` is stored in an overflow chain (§7.4), with C-T6 §5 TOAST semantics (card A-5; the row-level rules belong to S-3).

### 3.3 Integrity on every read (I-INTEGRITY)
Every page is read through a **reference**: a page-map entry for an LPID, or a parent reference for a map or free-set page. The reference supplies `(slot, epoch, crc)`, and the expected id is known. The read checks all four of:
- the header `crc` equals the CRC32C of the bytes;
- the header `crc` equals the reference's `crc`;
- the header `id` equals the expected id;
- the header `epoch` equals the reference's `epoch`.

Any mismatch returns XX001, and the bytes are never served. The check catches torn, lost (stale) and misdirected writes, not just torn ones (PLAN-v3 §3.3).

---

## 4. Page map, LPIDs and slots

### 4.1 Page map (N1)
- **Shape.** The map is a radix tree of **physically addressed** pages. MapLeaf pages hold `⌊(page_size − 24) / 16⌋` entries (510 at 8 KiB) of `slot u64, epoch u32, crc u32`. `slot = 0` means **unmapped**: the LPID is free. MapInner pages hold refs of the same shape to their children. The meta's `map_root` is the root ref.
- **Residency.** MapInner pages are **always resident**. They number about 1/510 of the MapLeaf pages, about 4 MiB per TiB of data at 8 KiB pages. MapLeaf pages are buffer-managed and evictable, under one rule: **a MapLeaf stays resident while any page it maps is dirty in the pool** (it holds one pin per such page; §5.4).
- **Writes are copy-on-write.** Map pages are mutated through `write_guard` like every other page (§5.6). A dirty MapLeaf is written to a fresh slot (I-NO-INPLACE), and its parent ref is updated in memory.

### 4.2 LPID allocation
- **Allocation.** Take the lowest free LPID; if none is free, take `next_lpid++`.
- **Freeing.** Freeing an LPID is a mutation of the page and of its map entry, both through `write_guard`, so image `k` keeps the page's epoch-`k` content if the free happens in epoch `k+1`.
- **Reuse is immediate, in memory.** A freed LPID may be reused at once because LPIDs never appear in the WAL (§12) and readers reach pages only through latched parents (§6.2). The image keeps its own epoch-`k` map version.

### 4.3 Free set
The free set is **two bitmaps**, persisted with each image as a radix of physically addressed FreeSet pages (the same shape as the map), with the root ref in the meta (`free_root`):
- **slot bitmap:** one bit per slot in `[3, slot_count)`; set = free in image `k`;
- **LPID bitmap:** one bit per LPID in `[1, next_lpid)`; set = unmapped in image `k`.

**Why bitmaps.** Their size depends only on the file length and `next_lpid`, never on fragmentation. That keeps the store's metadata memory a fixed fraction of the file: 1 bit per page, or 1/65,536 of the file at 8 KiB pages (§5.7). It also removes any fixed-point problem when the checkpointer writes them (§13 step 2).

**I-FREE-EXACT.** The persisted slot bitmap of image `k` marks free exactly the slots in `[3, slot_count)` that image `k` does not reference. "References" covers:
- the homes of the image's mapped LPIDs;
- the image's MapLeaf and MapInner pages;
- the image's FreeSet pages.

The LPID bitmap marks exactly the LPIDs that image `k`'s map leaves unmapped. amcheck verifies both bitmaps exactly: no leak, no double use.

As a result, the slots that eviction allocated after meta `k` are free in image `k`, so a crash reclaims them automatically (PLAN-v3 §3.3).

### 4.4 Slot reuse (I-NO-INPLACE, I-FREE-SAFE)
**In memory**, a slot may be written only if it is referenced by none of:
1. the durable image;
2. the image being written (the epoch-`k` map, during checkpoint `k`);
3. the current map (the homes of the current epoch's pages).

Slots 0–2 are never page homes. The two invariants follow:
- **I-NO-INPLACE:** a live page's home is never overwritten. Every page write (eviction or checkpoint) goes to a fresh slot.
- **I-FREE-SAFE:** a slot referenced by image `k−1` but not by image `k` becomes reusable only after meta `k` is synced (§13 step 6).

A slot that only the current map referenced (an earlier eviction home of a page in this epoch, never adopted by an image) is reusable as soon as the page's new home is written.

### 4.5 Allocation policy and file growth
- **Allocation.** Slots are allocated lowest-free-first, which keeps the file dense and reuses freed space before the file grows.
- **Growth.** When no slot is free, the file grows by `max(1 MiB, file_len / 16)` (`set_len`). The new slots are free.
- **Errors are fail-stop.** A failed extension or a failed page write is fail-stop (§9.2), with 53100 or 58030.
- **No shrinking in 2.0.** The file never shrinks in 2.0: GC returns space to the free set, not to the filesystem (§19 Q1).

---

## 5. Buffer manager and memory model (N8, N16)

### 5.1 Structure
- **Pool.** A fixed array of `F` frames, set at open from the N16 budget (card R-1). It **never grows**.
- **Frame.** Holds the page bytes, a `RwLock` latch, a dirty flag, a pin count and the content epoch.
- **Page table.** A hash map from page id to frame (`spikes/e0-btree/src/pool.rs`).
- **No `unsafe`** (N9).

Optimistic latches and pointer swizzling come only when an E0 or E7 trigger fires (PLAN-v3 §3.12).

### 5.2 Eviction
**Victim choice.** Clock. The evictor takes victims with `try_latch` and **never blocks on a victim**: if the latch is not free, it moves on. Pinned frames are skipped.
- **Clean victims** are dropped.
- **Dirty victims.** A dirty victim is written **to a fresh slot** (§4.4), and its map entry is updated in memory to `(new slot, content epoch, crc)`. This is a gated operation (§5.6), but it never blocks on the gate:
  - an eviction made inside a gated operation runs under that operation's hold;
  - any other eviction (a reader, a background evictor) only *tries* to take the gate shared. If the try fails because a flip is pending, the evictor takes a clean victim. If no victim is clean, the thread releases its latches and restarts (§5.3).

  A blocking acquire there could deadlock: the waiting flip blocks new shared holders, while the current holders may wait on that thread's latches.
- **Dirty victims from the previous epoch.** If the victim's content epoch is `k < e` during checkpoint `k` (after the flip, before step 2 has written that page), the write is also recorded in the image map (the epoch-`k` map), and the page counts as written for image `k`.

A dirty page may therefore be evicted **at any time**, independently of checkpoints. This is what lets write-heavy loads and long-reader version retention exceed RAM (PLAN-v3 §3.3).

### 5.3 Latch rules
- Latches are acquired **top-down**, and among siblings **left to right** (I-LATCH, §6.2). Nothing ever latches sideways or upward, so latching cannot deadlock.
- **No I/O while holding an exclusive latch on an inner node.** An operation that needs a non-resident page releases its latches, pins the page (loading it), and restarts from the root, re-validating what it saw (PLAN-v3 §3.3). Pins guarantee residency, not identity, so validation re-reads the parent under its latch.
- A frame's latch is never held across a wait on another transaction.

### 5.4 Pins and the minimum pool size
**Minimum size.** `F ≥ threads × (max tree height + 2) + R_EPOCH + R_MAP`:
- height + 2 covers the latch-coupled path plus the two siblings of a pack step;
- `R_EPOCH` is the epoch-copy reserve (§5.6);
- `R_MAP` covers the MapLeaf pins of §4.1, bounded by `⌈dirty frames / 510⌉`.

**Below the minimum,** the open is refused with 53200. A thread that cannot pin a frame because every frame is pinned waits for an unpin. The minimum makes that wait finite.

### 5.5 What lives in the pool
The pool is the **only** place tree content, overflow content and MapLeaf content live in memory. Versions retained for long readers are tree entries (§7), so under memory pressure they occupy **disk, not RAM**: they are evicted like any other page. This makes X-4's ruling R2 Assumption 1 true by construction: (b) adds no unspillable N16 consumer for long readers (PLAN-v3 §4, "Ruling R2").

### 5.6 The epoch gate and `write_guard`
**The epoch gate.** The gate is a `RwLock`. It is held **shared** by every gated operation:
- every tree mutation, including its splits;
- every pack step, root split and root collapse;
- every GC page mutation and every LPID free;
- every directory change;
- every dirty eviction;
- every apply insert.

Each of these holds the gate from its first page mutation to its last. The checkpoint's flip takes the gate **exclusively** (§13 step 0).
- **Acquisition.** An operation acquires the gate before its first latch, never twice, and releases it after its last latch. Nested work, such as an eviction or a page-local prune inside an insert, runs under the outer hold. An operation that restarts releases the gate and re-acquires it. A thread that holds latches and no gate never blocks on the gate (§5.2).
- **Waiting.** Holding the gate shared across a page read is permitted; the flip waits for it.
- **What this guarantees.** Every multi-page change is wholly in epoch `k` or wholly in epoch `k+1` (I-CKPT-AT-T).

**`write_guard(frame)`** is the only way to mutate a page.
- **When it copies.** It copies only if the frame's content epoch `< e`, **and** that content is not yet at a slot the image map references (the page is dirty with respect to image `k`). In that case it first copies the bytes into an **epoch-copy frame**, which belongs to the checkpointer.
- **After the call,** the frame's content epoch is `e`.
- **Reserve.** Epoch copies come from the `R_EPOCH` reserve. When the reserve is exhausted, `write_guard` dirty-evicts the oldest epoch copy (§5.2, recorded in the image map) and reuses its frame. Checkpoints therefore never need memory proportional to the dirty set.

### 5.7 Store memory outside the pool
These consumers do **not** grow with the version count or with retention. Each is accounted separately to R-1 (per-consumer accounting, X-4 R2 item 2):

| Consumer | Bound |
|---|---|
| page table | O(F) |
| MapInner pages | ≈ 1/510 of the MapLeaf count |
| the two free-set bitmaps | 1 bit per slot plus 1 bit per LPID |
| the pending-free lists (§13 step 6) | ≤ the pages written in one checkpoint interval |
| WAL buffers | configured |

Write sets and the lock table are S-2/R-1 consumers, not store consumers. **W6 measured 117 B/key of write set held in RAM** (`spikes/e1-bench/README.md` §W6), which is R-1's concern (X-4 R2 item 3).

---

## 6. B+tree

Reference: `spikes/e0-btree/src/tree.rs` for search, insert, split, latch coupling and the cursor stack. This section adds what E0 deliberately left out (README "Known limits"): merge, page freeing, a stable root and the tree directory.

### 6.1 Shape (I-TREE)
- **Keys** are memcmp-ordered byte strings: entry keys (§7).
- **Leaves** hold entries with strictly ascending keys.
- **Inner pages** hold separators with strictly ascending keys and `n+1` children. Every key below child `i` lies in `[sep(i−1), sep(i))` (E0's T-ORDER).
- **No leaf sibling links.** Range scans use a cursor stack, so copy-on-write never has to fix siblings (PLAN-v3 §3.3).
- One tree per relation and per index.

### 6.2 Latching (I-LATCH)
- **Writers** use latch coupling (crabbing) with exclusive latches: they release every ancestor once the latched child is *safe*, meaning it has room for one more maximum-size cell (E0's T-COUPLING).
- **Readers** use shared latches, top-down. They hold the parent while they latch the child.
- **Order.** Latches are taken in (depth ascending, then left to right among siblings).

The consequence that §4.2 and §6.6 rely on: no thread can reach a page except through a latched parent or a re-descent from the root, so an LPID unlinked under its parent's exclusive latch is unreachable afterwards.

### 6.3 Stable root (I-ROOT-STABLE)
A tree's root LPID is fixed when the tree is created and never changes. This departs from E0, which kept the root in a `RwLock<PageId>` and used a T-ROOT restart.
- **Root split:** move the root's cells into two new pages and rewrite the root as an inner page with two children.
- **Root collapse:** when the root is an inner page with one child, copy the child's content into the root and free the child.

**Why.** The tree directory (§6.8) never changes on a split or a collapse, readers need no root re-check, and the root never moves between images.

### 6.4 Split
A leaf that cannot take an insert first runs the **page-local prune** (§10.3). A split happens only if the prune did not free enough room.
- **Separator.** The split separator is the shortest byte string `s` with `last(left) < s ≤ first(right)`. Under (b), a split may fall inside a group, so `s` may include ts bytes.
- **Split-point preference.** A leaf split **should** prefer a group boundary within the middle third of the page. That makes more groups page-local, which is what page-local GC can prove things about (§10.3), and a latest-snapshot read of a key then touches one leaf.
- **Prefix truncation.** Inner-node prefix truncation is card A-4d.
- **Atomicity.** A split is all-or-nothing: E0's T-ATOMIC carries over. Every page it needs is reserved before the first mutation.

### 6.5 Pack step: merge and redistribution (I-PACK)
`pack(P, L, R)`. **Preconditions:**
- `P` is an inner page;
- `L` and `R` are adjacent children of `P`, with `R` immediately right of `L`;
- all three are pinned and resident (pin-then-validate, §5.3), and exclusively latched in the order `P`, `L`, `R`;
- the step is one gated operation.

With `F_PACK = 0.85`, the step does one of three things:
1. **Merge.** If `used(L) + used(R) ≤ F_PACK·U`, move all of `R`'s cells into `L`. For inner pages, first pull down the separator between them. Remove `R`'s separator and child pointer from `P`, then free `R`'s LPID (§4.2).
2. **Redistribute.** Otherwise, if `R`'s first cell fits in `L` with `used(L)` staying `≤ F_PACK·U`, move the **longest prefix** of `R`'s cells that keeps `used(L) ≤ F_PACK·U`. Replace the separator in `P` with a new shortest separator. For inner pages, rotate through `P`.
3. **Skip.** Otherwise, do nothing.

**When the step is skipped instead.** A step that would make `P` overflow (a longer separator) is skipped. A pack step never splits.

**`F_PACK`** is an engine constant, not a format field. It is below 1 so that a packed leaf can absorb new versions without splitting at once. Under (b), every update inserts a version into its key's leaf.

**Correctness.** The step preserves the multiset of entries, I-TREE and every key's subtree bound. It is atomic with respect to readers (latches) and to images (the gate).

**Who calls pack.**
- **The GC sweep** packs systematically, left to right (§10.4).
- **A delete that descends from the root under latch coupling** (a directory removal) may pack the affected leaf with its right neighbour, or its left neighbour if it is the rightmost child, when `used < U/4`, while it still holds the parent.
- **A delete made while holding only a leaf** (the page-local prune, §10.3, including at eviction) never packs. Taking the parent's latch from below would break I-LATCH. It leaves the page for the next sweep.
- **Inner pages** whose `used` falls below `U/4` after a pack removed a child are packed the same way under their parent.
- **The root** collapses when it is an inner page with one child (§6.3).

Under N3 = (b), **logical deletes are tombstone inserts** (§7). Only GC, DropTree and overflow freeing ever remove entries or pages.

### 6.6 Cursors (I-CURSOR)
- **Holding latches.** A cursor holds shared latches on its root-to-leaf stack while it is positioned (E0's design), so no SMO can change the path it holds.
- **Releasing and resuming.** A cursor that releases its latches resumes by **re-descending from the root with its resume key**, never by a remembered LPID. Cursors may release between batches, and card A-4e decides when. The resume key is the last key returned (exclusive), or a group's upper bound when skipping (§8.3).
- **Safety.** This is what keeps merges, frees and LPID reuse safe for scans that do not hold latches throughout.

Iterators are **streaming**: a range scan never materialises its range. The spike's whole-range materialisation is a spike limit (`verdicts/X-2.md` non-blocking finding 3), not a behaviour.

### 6.7 Overflow chains
An entry whose value exceeds `V_INLINE` stores a reference `(len u64, first LPID)` in its value, with tag 4 or 5 (§7.3). The chain is a list of Overflow pages.
- **Immutability.** Chains are immutable once written (versions are immutable).
- **Freeing.** A chain is freed **in the same gated operation** that deletes its entry (§10). A chain free therefore holds the flip for the chain's length. A-5 measures this at the maximum value size; if it is too long, A-5 proposes a resumable free, which is an edit of this file.
- **Reading without the leaf latch.** A reader that holds a registered snapshot `S ≥ W` may read the chain after releasing the leaf latch. GC never deletes a version that is the newest `≤ S` for any registered `S` (I-GC-READEQ), so a version that a registered snapshot reads cannot lose its chain while the reader holds that registration.

### 6.8 Tree directory
- **What it is.** A system B+tree, unversioned, with a stable root at `dir_root`. Its entries map `tree_id (u64 BE) → (root LPID u64, kind u8, state u8)`.
- **Tree ids** are C-T0 §10 storage ids, never reused. The WAL names trees only by tree id (§12).
- **States:** `live` or `dropping`.
- **DDL visibility** lives in the versioned catalog rows (C-T5), not here.

**CreateTree(id)** allocates a root leaf and inserts the directory entry. It is idempotent: if the id already exists, nothing happens.

**DropTree(id)**
1. Set the entry's state to `dropping`, in one directory mutation.
2. Free the tree's pages bottom-up in bounded gated steps: free the children first, unlink them from the parent, and free overflow chains with their entries. The root is freed last.
3. Remove the directory entry.

At open, every `dropping` entry's walk resumes (the B-6 boot job; C-B1 "drop cleanup = freeing pages"). DropTree is idempotent: dropping an absent id does nothing. Because the entry is removed only after its last page is freed, a crash mid-walk never leaks pages (I-LPID-EXACT).

**When the drop runs** (after `W` passes the DDL) is C-T0 v8's rule (v7.8 §9.2).

**I-LPID-EXACT** (amcheck, quiescent): the mapped LPIDs of an image are exactly:
- the pages reachable from the directory roots, including overflow chains (each referenced by exactly one entry);
- the directory's own pages.

---

## 7. Versioned entries (N3 = (b))

Decided by X-4 (PLAN-v3 §4, "E1′ decision") from the e1-bench evidence (§16). The layout is X-2's (`spikes/e1-arm-b/src/encode.rs`), pinned by its proptest. S-1 owns it (decision block, "Consequences").

### 7.1 Timestamps
- **Reserved values.** `ts = 0` and `ts = u64::MAX` are never commit timestamps. C-T0 already reserves `Ts::ZERO`. This file reserves `u64::MAX` for S-2: §7.3 and §19 Q4.
- **Uniqueness.** One commit writes at most one version per user key per tree, at its own unique ts, so **entry keys are unique** (I-ENTRY-UNIQUE).

### 7.2 Entry key
```
entry_key(k, ts) = escape(k) ‖ 0x00 0x00 ‖ be64(!ts)
escape(k)        = k with every 0x00 byte replaced by 0x00 0xFF
prefix(k)        = escape(k) ‖ 0x00 0x00                 # every entry of k starts with it; no other key's entry does
upper(k)         = prefix(k) ‖ 0xFF × 9                  # exclusive upper bound of k's group
seek(k, S)       = entry_key(k, S)                        # first entry ≥ it in k's group = newest version ≤ S
seek_newest(k)   = entry_key(k, u64::MAX) = prefix(k) ‖ 0x00 × 8
```
Reference: `encode.rs:67` `prefix_of`, `:84` `encode_key`, `:94` `encode_seek_newest`, `:102` `encode_key_upper`, `:120` `decode_key`.

**I-ENC-ORDER.** For any user keys and timestamps, the memcmp order of entry keys is (user key ascending, ts **descending**). Why:
- Escaping is a byte homomorphism, and it emits `0x00` only as the first byte of a `0x00 0xFF` pair, so `0x00 0x00` occurs in an entry key only as the terminator.
- For `k1 < k2`, the whole group of `k1` sorts below the whole group of `k2`. If `k1` is a proper prefix of `k2`, the terminator's second `0x00` loses against the `0xFF` follower of an escaped zero, or against any non-zero byte (`encode.rs` module docs).
- `!ts` big-endian inverts ts order within a group.
- `upper(k)` is above every entry of `k`, since `0xFF × 9` is longer than any 8-byte suffix, and below every entry of any `k' > k`, by the same argument.

**Decoding.** An entry key is split **from the right**: the last 8 bytes are `!ts`, the 2 before them must be `0x00 0x00`, and the rest must unescape cleanly (every `0x00` is followed by `0xFF`). Decoding never needs the schema. A key that does not decode is XX001 (I-ENC-ROUNDTRIP). `decode(entry_key(k, ts)) = (k, ts)` for every `k` and `ts`.

**Prefix ranges.** A user-key prefix `p` needs no extra rule. It is the user range `[p, succ(p))`, and `prefix()` maps it exactly. Every key that extends `p` encodes at or above `prefix(p)`: escaping preserves concatenation, and an escaped continuation begins with a non-zero byte or with `0x00 0xFF`, both of which exceed the terminator.

**Size.** `|entry_key| = |k| + zeros(k) + 10`. It must be `≤ K_MAX` (§3.2).

### 7.3 Entry value
One tag byte, then a tag-specific body:

| tag | meaning | body | C-T0 v7.8 §2.2 origin |
|---|---|---|---|
| 0 | live value | value bytes | header 0x00 |
| 1 | tombstone | none | header 0x02 |
| 2 | live value, `key_changed` | value bytes | header 0x01 |
| 3 | moved-tombstone (an UPDATE moved the row to a new primary key) | none | header 0x03 |
| 4 | live value in an overflow chain | `len u64, first LPID u64` | (overflow, §6.7) |
| 5 | live value in an overflow chain, `key_changed` | as tag 4 | (overflow) |
| 6–255 | **reserved** (S-2 provisional entries, §19 Q4) | — | — |

**Why these tags.**
- Tags 0 and 1 are the spike's (`encode.rs:50`, `:53`).
- Tags 2 and 3 carry the C-T0 behaviour that needs them (the KEY SHARE rule, and moved rows under RC EPQ), whose physical clauses C-T0 v8 replaces with references to this file (PLAN-v3 §3.4). C-T0 v8 owns what they mean.
- *Tombstone* below means tag 1 or 3; *live* means tags 0, 2, 4 and 5.

**I-TAG-RESERVED.** Until S-2 assigns them in an edit of this file, the following are XX001 on every read path and are never served:
- a reserved tag;
- an entry whose suffix encodes ts `0` or `u64::MAX`.

### 7.4 Size accounting
An inline version costs `|k| + zeros(k) + 10` key bytes, plus `1 +` value bytes, plus the cell and slot overhead. The escape's cost is measured: W1's 8-byte keys `i.to_be_bytes()` for `i < 2^20` (`spikes/e1-bench/src/util.rs:67`) always carry five `0x00` bytes, and W1 measured **124.07 B applied per txn** for a 100-byte value. That is 101 value bytes + 10 + 13.07 escaped key bytes (`spikes/e1-bench/README.md` §W1, "(b) B/txn"). The escape cost is accepted as part of X-2's pinned layout; leaf prefix compression is an open mitigation (§19 Q2).

---

## 8. Reads

Every read below runs on one tree. The write set merge (a transaction's own writes) is S-2's (C-T0 v8, I-HALLOWEEN); this section is the committed-data half.

### 8.1 Point read at S (I-SEEK)
`get(k, S)`:
1. descend once to `seek(k, S)`;
2. take the first entry `e ≥ seek(k, S)`. If the seek lands at the end of a leaf, the cursor stack moves to the next leaf (§6.6);
3. if `e` does not start with `prefix(k)`: no version of `k` is `≤ S`, so the result is **absent**;
4. otherwise decode `e`'s value: live → the value (reading the chain for tags 4 and 5); tombstone → **absent**; reserved → XX001.

There is one descent and no per-version stepping (`engine.rs:238` `version_at`). Mutants: TS-ASC, SEEK-NO-PREFIX-CHECK, TOMBSTONE-AS-VALUE.

### 8.2 Newest probe (I-NEWEST)
`newest(k)` = the first entry at or after `seek_newest(k)` that starts with `prefix(k)`, as `(ts, live value | tombstone)`, or none (`engine.rs:266`).
- **Precondition, owned by S-2.** The caller holds `k`'s lock, and locks are released only after the holder's commit is visible (C-COMMIT-ORDER). Then no apply to `k` is in flight, and the newest entry is the newest committed version.
- **Who uses it.** S-2's RR conflict rule (40001 iff `ts > S`, checked **after** the lock wait, `engine.rs:430–435`) and its liveness rule (a tombstone is not live, `engine.rs:452`).
- **GC cannot change the answer.** GC never deletes the newest entry of a group, except a tombstone with nothing older (§10.2). For both rules, that tombstone and no entry give the same answer (`verdicts/X-2.md`, "GC safety holds").

### 8.3 Range scan at S (I-SCAN)
`scan(lo, hi, S)` yields `(k, value)` for each user key `k` in the range, in key order: the newest version `≤ S`, omitted if it is a tombstone. Encoded bounds:

| user bound | encoded bound |
|---|---|
| `lo` included | `prefix(lo)` (inclusive) |
| `lo` excluded | `upper(lo)` (inclusive) |
| `lo` unbounded | the empty string |
| `hi` excluded | `prefix(hi)` (exclusive) |
| `hi` included | `upper(hi)` (exclusive) |
| `hi` unbounded | the tree end |

Using `upper(hi)` for an excluded `hi` is the SCAN-HI-INCL mutant (`verdicts/X-2.md`). Using `prefix(hi)` for an included one drops `hi`'s group.

**Per group:**
1. skip the entries with `ts > S`;
2. take the first entry with `ts ≤ S`;
3. continue at `upper(k)`.

Steps 1 and 3 **should be seeks** (a binary search within the leaf, or a re-descent) rather than entry-by-entry stepping, once the remaining group exceeds a small threshold that A-4e sets. This is the version-skipping mitigation named in the decision block for the latest-snapshot scan cost (§16, §19 trigger T1). Reference: `engine.rs:301` (stepping, and materialising; both are spike limits).

**Reverse scans** return exactly the forward result in reverse order. Within each group, they apply the forward rule (A-4e designs how).

**Concurrency.** A scan interleaved with GC steps sees, for each key, a state that I-GC-READEQ makes read-equivalent, so its results equal those of a scan with no GC.

---

## 9. Apply (the storage side of commit)

### 9.1 What apply does
- Apply turns each write of a commit at `ts = c` into **one entry insert**: `Put` → `entry_key(k, c)` with tag 0, 2, 4 or 5; `Delete` → tag 1 or 3. Each insert is a gated operation.
- **Order.** A commit inserts its entries in ascending user-key order, tree by tree (`engine.rs:495`).
- **When.** C-T0 v8 decides when apply runs, and how `visible_ts` and lock release follow it.

**I-ENTRY-UNIQUE / I-REPLAY-IDEM.** Inserting an entry whose key already exists:
- with **byte-identical** value: a no-op. This happens only in replay (§14);
- with a **different** value: XX001. Two commits cannot share a ts.

### 9.2 Failure (I-FAIL-STOP)
- **No logical failures.** Apply cannot fail logically: key size is checked when the write enters the write set (§3.2), and entries are unique.
- **Physical failures.** An I/O error, 53100 or 53200 inside apply, eviction or a checkpoint **poisons the store**. Every later mutation returns the poisoning error, and the process stops. Recovery replays the commit (PLAN-v3 §3.4; `engine.rs` B-FAIL-STOP).
- **No partial unwinding.** The store never tries to undo a partial apply: under (b), the replay of a partially applied commit is idempotent (§14).

---

## 10. GC (physical)

C-T0 v8 owns the watermark `W`: how it is computed, published and floored after a restart (§17). This section owns what may be removed given `W`, in what order, and how the space comes back. The rule is X-2's, as corrected by X-2-F1 and adopted in the decision block's "Consequences".

### 10.1 Precondition (A-GC-STABLE)
A GC mutation on a tree with watermark `w` requires two things:
1. `w ≤` the watermark C-T0 v8 published at the time of the mutation;
2. **every insert of an entry with `ts ≤ w` into that tree has finished.**

Condition 2 holds for apply, because `w ≤ visible_ts` and applies of later commits carry ts `> visible_ts`. It must also hold for bulk loads: S-3 registers the load's ts, so `W` cannot pass it while the load runs (§12.8). Under these conditions, **the set of entries with `ts ≤ w` changes only through GC**, which is what makes carrying state across leaves sound (§10.4).

### 10.2 The rule
For each user key, with `w` fixed:
- **keep** every version with `ts > w`;
- **keep** the newest version with `ts ≤ w`. Exception: when it is a tombstone, **elide** it, provided every older version of the key is deleted, and the tombstone is deleted **last** (I-GC-ORDER). This holds whether or not versions above `w` exist (X-2-F1, correcting the spike's `above_w == 0` condition at `engine.rs:585`);
- **delete** every older version, freeing its overflow chain in the same gated operation (§6.7).

**I-GC-READEQ.** Every GC mutation leaves unchanged the result of every read in §8.1–8.3 at every snapshot `S ≥` the watermark published at the time of the mutation. It also leaves unchanged the §8.2 answers for both S-2 rules.
- **Pruning.** A pruned version is shadowed for every such `S` by a newer version `≤ w ≤ S` that stays.
- **Tombstone elision.** Deleting the tombstone last means that at every intermediate state, either the tombstone shadows the older versions, or no older version exists. Both read as absent at every such `S`.

Mutants: GC-DROPS-VISIBLE, G-WATERMARK+1, G-RESURRECT (the tombstone is deleted while an older version remains, so that version becomes visible; `verdicts/X-2.md`).

**I-GC-COMPLETE.** After a complete sweep (§10.4) of a tree with `w` fixed and no concurrent inserts, every key has:
- its versions `> w`;
- at most one version `≤ w`, which is live.

Mutant: GC-KEEPS-TOMB, the spike's narrow elision. It is caught by a directed test:
1. put;
2. delete;
3. register a reader;
4. a newer put;
5. gc.

The `≤ w` tombstone must be gone, and the reader's reads must be unchanged. The reader is what makes the tombstone the newest `≤ w` while a version above `w` exists. X-2-F1's test as written ("with no readers") puts `w` at or above the newer put, which turns the tombstone into an ordinary older version and leaves the mutant uncaught.

### 10.3 Page-local prune
- **When it runs.** It **must** run before a leaf split (§6.4). It **may** run on any current-epoch leaf that a thread holds exclusively, including at eviction. It **must not** run on an epoch-copy frame, whose content belongs to image `k`, with `gc_w` already fixed.
- **What it may do,** using a `w` that satisfies §10.1, within one leaf, in one page mutation:
  - **delete** a version when a newer version of the same key with `ts ≤ w` precedes it **on the same leaf**;
  - **elide** a newest-`≤ w` tombstone only when an entry of a **different key follows its group on the same leaf**, which proves the group ends here, and the same mutation deletes the group's older versions.
- **What it must not do (I-GC-LOCAL).** It never deletes or elides on the strength of an entry it cannot see. In particular, the leaf's first entry is never pruned as "not newest" unless a newer `≤ w` entry precedes it on this leaf: its group may continue from the left. Mutant: PRUNE-FIRST-ENTRY.

The page-local prune is incomplete by design: a group that spans leaves keeps its garbage until the sweep reaches it.

### 10.4 The sweep
`gc_sweep(tree, w, token) → token | done` runs bounded steps of the §10.2 rule over a whole tree, in key order.
- **Concurrency.** At most one sweep per tree at a time.
- **Fixed `w`.** `w` is fixed for the whole sweep and is stored in the resumable `token`, together with the carried state: the current user key, whether its newest `≤ w` version has been seen, and a pending tombstone elision. Resuming with a later `w` would also be safe, but a fixed `w` keeps the token simple.

**Per leaf:**
1. Re-descend to the leaf by the resume key (I-CURSOR) and latch it exclusively.
2. Apply §10.2 to its entries in order, using the carried state across the leaf boundary. This is sound by §10.1.
3. Delete the entries marked for deletion in one page mutation.
   - **Tombstone group ends on this leaf:** a newest-`≤ w` tombstone t whose group ends on this leaf is deleted in that same mutation, together with the older versions of its group.
   - **Group continues onto later leaves:** t is recorded as **pending**. It is deleted in its own gated operation, by key, once the sweep has passed the group's end. If t is already gone (a page-local prune elided it), that operation does nothing.
4. **Pack:** if the previous leaf is a sibling under the same parent, run `pack(parent, previous, this)` (§6.5); then cascade inner-page packs and the root collapse (§6.5). This is how GC returns pages.
5. Release, and advance the resume key.

**Interruption.** A crash or stop between steps leaves a read-equivalent state (I-GC-READEQ) and leaks nothing (I-LPID-EXACT): every LPID free is in the same gated operation as its unlinking.

When sweeps run, how often, and with what I/O budget is S-2/B-3's decision (background jobs, C-B1 semantics).

### 10.5 The image's watermark (`gc_w`)
- **What it records.** The store keeps `gc_w_used`, the largest `w` it has applied in any GC mutation. At the flip of checkpoint `k`, it records that value as meta `k`'s `gc_w`. While the flip holds the gate, no GC mutation is in flight, so `gc_w` bounds every GC mutation contained in image `k`.
- **Bound.** `gc_w ≤ T` always: each `w` was `≤ visible_ts` at its use, and `visible_ts` at the flip is `T`. A violation at meta write is fail-stop.
- **After open,** C-T0 v8's watermark must be at least the recovered meta's `gc_w`, because the recovered pages were pruned with it. In v7.8 this was `/sys/gc_w`, synced before GC acted (C-T0 v7.8 §9.1). v3 has no unversioned system KV, so the image carries it instead. Pruning done after meta `k` is lost with a crash, together with its pages, so the meta's value is exact.

---

## 11. Space reclamation

The decision block requires that GC actually return space. "S-1/A-4b's merge and free set must give the space back." E0 never merged or freed pages, which is why (b) stayed at **20.11×** after GC cut its versions from 1.1M to 100k (`spikes/e1-bench/README.md` §W4; `spikes/e0-btree/src/tree.rs` T-NOMERGE). The chain from a dead version back to a reusable slot:
1. **Entry → page.** The sweep deletes the version (§10.4), and the pack step merges or redistributes leaves, freeing merged-away LPIDs (§6.5). Inner pages and the root follow.
2. **LPID → slot.** A freed LPID's home slot is no longer referenced by the current map. If the durable image or the image being written still references it, it stays pending (§4.4).
3. **Slot → free set.** After the first checkpoint `k` whose flip follows the sweep, image `k` no longer references the slot. Once meta `k` is synced, the slot is free in memory (§13 step 6) and in image `k`'s persisted bitmap (I-FREE-EXACT).
4. **Free set → reuse.** The slot is allocated lowest-first, before the file grows (§4.5).

**I-SPACE (quiescent).** After a complete sweep of a tree with `w` fixed and no concurrent inserts:
- **leaves:** every leaf except the rightmost child of each inner page has `used > F_PACK·U − C_max`, where `C_max` is the largest cell in the tree. A pack step stops redistributing only when `R`'s next cell no longer fits;
- **inner pages:** no non-root inner page has `used < U/4`;
- **root:** the root is a leaf, or has at least 2 children;
- **LPIDs:** every LPID removed from the tree is freed (I-LPID-EXACT).

**What this gives B-3's acceptance (derived, to be measured by B-3).** The acceptance re-runs the W3/W4 shape on the real tree and requires after-GC tree bytes `≤ 1.5×` the pre-update tree bytes (decision block). In that shape:
- before the updates and after GC, each key holds exactly one live version of equal size, so the leaf cell bytes are equal;
- no page holds more than `U`, so the pre-update tree has at least `cells/U` leaves;
- I-SPACE caps the after-GC leaves at `cells/(F_PACK·U − C_max)`, plus one per inner page.

At 8 KiB pages (`U ≈ 8.1 KiB`) with W3's ~135-byte cells, the bound is ≈ `1/(0.85 − 0.017) ≈ 1.2×`, plus under 1% for the partial last children and the inner pages. That is within 1.5× **whatever the pre-update fill**. A B-3 failure re-opens N3 (decision block).

---

## 12. WAL (N4, N14)

### 12.1 Segments
- **Files.** Segments are `wal/<first-lsn as 16 lowercase hex digits>.log`, each `wal_seg_size` bytes (§2.1), preallocated.
- **LSN.** An LSN is a byte position in the logical WAL stream; a segment covers `[first_lsn, first_lsn + wal_seg_size)`.
- **Recycling.** A segment is recycled by renaming it to a new first LSN. Its old content is then stale (§12.5).

### 12.2 Record framing
```
magic      u32  "NWAL"
len        u32  bytes of the whole record
crc        u32  CRC32C of the bytes after this field, to the end of the record
lsn        u64  this record's own start position
kind       u8   1 Commit | 2 Chunk | 3 CommitMarker | 4 SegmentEnd
txn        (u32 boot epoch, u64 n)   C-T0 TxnId
commit_ts  u64  0 in Chunk and SegmentEnd
payload
```
**Records never span segments.** A writer that cannot fit a record in the rest of a segment writes `SegmentEnd`, and the next record starts at the next segment's first LSN. A single record is at most `wal_seg_size / 4`; larger transactions are chunked.

### 12.3 Record kinds and ops
**Kinds:**
- `Commit{ops}`: one small transaction at `commit_ts`;
- `Chunk{seq u32, ops}`: repeated, written before the commit; `commit_ts = 0`;
- `CommitMarker{nchunks u32}` at `commit_ts`: commits the chunked transaction.

**Ops:**
```
CreateTree{tree_id, kind}   DropTree{tree_id}   Ddl{bytes}        # first within a record (opaque Ddl bytes are catalog-owned)
Put{tree_id, key, flags, value, before?}                          # flags: key_changed
Delete{tree_id, key, flags, before?}                              # flags: moved
```
- **Keys.** `key` is the **user key**, not the entry key. The version entry is formed at apply time with the commit's ts (§9). The WAL therefore carries no LPID, no slot and no N3 detail.
- **Index entries** are logged explicitly and never re-derived.
- **`before`** is N14's optional before-image, written when the catalog enables it for the tree.
- **Bytes.** Card A-6a pins the op encoding and fuzzes it.

### 12.4 Group commit and order (I-WAL-TS-ORDER)
- **Writer.** One writer drains the queue, writes one batch, does one `sync_data`, then acks the group. In embedded and wasm modes, the committing thread does this work cooperatively (N10).
- **`synchronous_commit=off`.** The ack comes after the write and before the sync (PLAN-v3 §3.3).
- **Apply waits for the append.** A commit's apply starts only after its records are appended.

**I-WAL-TS-ORDER.** Across `Commit` and `CommitMarker` records, `commit_ts` strictly increases with LSN. The commit path must assign the ts and the append position **in one critical section**. S-2's sequencer complies; this file imposes the requirement because recovery depends on it. Without it, a lost record could leave a durable later-ts commit behind a missing earlier one, and the recovered state would not be a ts-prefix, which breaks E4's oracle (PLAN-v3 §4 E4). Mutant: TS-AFTER-APPEND.

### 12.5 Stale and torn records
A reader walks records from a start LSN. **It stops at the first record that:**
- has a bad magic or CRC;
- has `lsn` ≠ the expected position, which catches a recycled segment's old content;
- has `len` running past the segment.

After `SegmentEnd`, the reader moves on to the next segment's first LSN. The stopping point is the **valid end**: recovery truncates there, and new appends overwrite from it.

### 12.6 Replay start and recycling
At the flip of checkpoint `k`, `wal_start_lsn` is the minimum of:
- the LSN of the first `Commit` or `CommitMarker` with `commit_ts > T`, or the append position if there is none;
- the first `Chunk` LSN of every transaction that has written chunks but no marker at the flip.

A transaction that aborted before the flip does not count. After a crash, an open transaction's chunks are ignored, because no marker follows them. **A segment wholly below the durable meta's `wal_start_lsn` may be recycled** (§13 step 6). This is PLAN-v3's chunk rule, stated as a replay-start position.

### 12.7 Large transactions
Chunks are written before commit. The commit is the marker, written in ts order (I-WAL-TS-ORDER). Apply follows the group sync. C-T0 v8 owns everything about pre-applying large transactions (§17).

### 12.8 Bulk loads (the rule PLAN-v3 §3.3 asks S-1 to pick)
**S-1 picks: force a checkpoint before the result is published. Page images are not WAL-logged.**
1. **Unlogged writes.** A bulk load (the sorted bulk load of an online index build, C-T6 §7) may write entries **without WAL records**, only into a tree that is **not yet published**: no reader may use it. The tree itself is created by a logged `CreateTree`. Concurrent logged writes into the tree, such as C-T6's write-only maintenance, stay logged as usual.
2. **The forced checkpoint.** The loaded entries become durable only through a checkpoint whose flip follows the load's last unlogged write.
3. **Publication.** The publishing commit (C-T6 write-only → public) may be issued only after that checkpoint's meta is durable (§13 step 5). Catch-up from the WAL (PLAN-v3 §3.5 step 3) is ordinary logged writing.
4. **Crash before publication.** The recovered tree holds only its logged content plus whatever the last image caught of the load. S-3 must treat such a tree as an unfinished build: restart or discard it.

Mutant: PUBLISH-BEFORE-META.

**Why not page images.** Under (b), a load writes versions at a registered snapshot ts `S`. Such versions may be `≤ T` of a later checkpoint, so a replay filtered by `commit_ts > T` (§14) could never place a logged copy of them. Page images would also carry LPIDs, which are meaningless to logical consumers (N14's replicas and CDC).

**Consequence, owned by D-6.** Unlogged loads are invisible to the WAL stream. Backup and PITR must therefore treat the forced checkpoint's image as a base-image boundary, and replicas must receive that image or rebuild the index (§19 Q5).

---

## 13. Checkpoint (the step order is binding)

At most one checkpoint runs at a time. Its triggers (WAL bytes, dirty volume, time, or a forced checkpoint for a bulk load) are configuration; correctness does not depend on them.

**Step 0 (flip).**
1. Take the epoch gate exclusively. No gated operation is now in flight (§5.6). This generalises PLAN-v3's "pause commit-apply" to every multi-page change.
2. Take `T` := the apply frontier, record `gc_w := gc_w_used` (§10.5) and the `wal_start_lsn` (§12.6), and set `e := k+1`.
3. Release the gate.

**Step 1 (WAL).** `sync_data` the WAL **through the end of every record appended before the flip**. This is stricter than PLAN-v3's "through the last record with commit_ts ≤ T", and the reason is out-of-order apply:
- applies run concurrently and finish out of ts order (`e1-mvcc/src/common/commit.rs:35–63`: apply, then `mark_applied` in ts order);
- so the image may hold some entries of a commit `c > T` that started applying before the flip;
- with `synchronous_commit=off`, `c`'s record may not yet be synced;
- syncing through every appended record guarantees `c`'s record is durable before the meta, so recovery replays `c` wholly (I-CKPT-AT-T, I-REPLAY-IDEM). Without it, a crash could expose part of `c` with no record to complete it (mutant WAL-SYNC-T-ONLY).

**Step 2 (image).** The checkpointer writes image `k`, each page to a fresh slot (§4.4), recording each write in the epoch-`k` map:
- every page whose epoch-`k` content is not already at a slot the image map references: the dirty frames with content epoch `≤ k`, and the epoch copies (§5.6). Pages that eviction already wrote during epoch `k`, or wrote after the flip as epoch-`k` content (§5.2), are adopted as they are;
- when a written frame was not mutated in epoch `k+1`, its new slot also becomes its current home;
- then the epoch-`k` MapLeaf and MapInner pages that changed since image `k−1`, written copy-on-write up to the root;
- then the free set **last**: allocate the slots for the FreeSet pages first (growing the file first if needed), then serialise both bitmaps of image `k`, with those slots marked used (I-FREE-EXACT). Bitmap size depends only on `slot_count` and `next_lpid`, so allocating the FreeSet pages cannot change how many are needed.

Pages created or mutated only after the flip are not part of the image.

**Step 3.** `sync_all` the page file.

**Step 4.** Write meta `k` (`ckpt_id = k`, `T`, `gc_w`, epoch, the roots, `next_lpid`, `slot_count`, `wal_start_lsn`, CRC) to slot `1 + (k mod 2)`.

**Step 5.** `sync_all`. Meta `k` is now the durable image.

**Step 6.** Only now:
- move the slots referenced by image `k−1` and not by image `k` from pending to free;
- recycle the WAL segments wholly below `wal_start_lsn`.

**I-META-ATOMIC.** A recovered meta never points at an unwritten or unsynced page: step 3 precedes step 4. Mutant: META-BEFORE-SYNC.

**I-CKPT-AT-T.** Image `k` is a consistent cut: every gated operation is wholly before or wholly after the flip. In image `k`:
- **(a)** every commit with ts `≤ T` is wholly present, up to GC `≤ gc_w`. For every snapshot `S` with `gc_w ≤ S ≤ T`, reads on the image equal reads on the committed state at `T`;
- **(b)** every entry with ts `> T` belongs to a commit `c > T` whose records are durable when meta `k` is written (step 1), so recovery replays `c` wholly;
- **(c)** no entry belongs to an uncommitted transaction (N2: trees hold only committed data; §19 Q4 for S-2's provisional entries).

Mutants: FLIP-NO-GATE (a split or pack straddles the flip), WAL-SYNC-T-ONLY.

**I-FREE-SAFE** (§4.4). Mutant: FREE-EARLY (step 6's frees moved before step 5).

---

## 14. Recovery

1. Read page 0 and both meta slots. The newest valid meta `k` wins (§2.2).
2. Load the map root and the free set of image `k`. Every slot not referenced by image `k` is free, including the slots eviction wrote after meta `k`.
3. **Pass 1:** walk the WAL from `wal_start_lsn` to the valid end (§12.5), collecting the set of `(txn, commit_ts)` of every `CommitMarker` with `commit_ts > T`.
4. **Pass 2:** walk the same span in LSN order, and apply:
   - every `Commit` with `commit_ts > T`;
   - every `Chunk` of a transaction in pass 1's set, at that set's `commit_ts`.

   A chunk whose transaction has no marker is ignored. Records with `commit_ts ≤ T` are skipped: they are in the image.
5. Truncate the WAL at the valid end.
6. Resume the walks of every `dropping` directory entry (§6.8), as a B-6 boot job.
7. Optionally run `amcheck` (A-8).

Afterwards, the apply frontier is the largest replayed `commit_ts`, or `T` if nothing was replayed. C-T0 v8 restarts its sequencer and watermark from that value and from `gc_w`.

**I-REPLAY-IDEM.** Replay is idempotent, so recovering twice, including a crash during recovery, gives the same state. Under (b), every op inserts a uniquely keyed entry (I-ENTRY-UNIQUE). Re-inserting an identical entry is a no-op, so partial applies already in the image (I-CKPT-AT-T (b)) are completed rather than duplicated. CreateTree and DropTree are idempotent (§6.8). The final state is the set union of the entries, independent of insertion order. Pass 2 still runs in LSN order, so tree creation and drop ops come before and after the writes that depend on them. Mutants: REPLAY-DUP-DIFFERENT (an identical re-insert treated as a conflict), a double-crash test.

**I-REDO-COMPLETE.** Every acked `synchronous_commit=on` commit is either `≤ T` of the recovered image, and in it, or has durable records at or after `wal_start_lsn` before the valid end, and is replayed. Mutant: START-LSN-PAST-CHUNK (the replay start ignores an open transaction's first chunk).

**E4's oracle.** The recovered state equals the model at some commit `c`, with max acked sync-on commit `≤ c ≤` last WAL-written commit (PLAN-v3 §4 E4). That follows from I-REDO-COMPLETE, I-CKPT-AT-T, I-REPLAY-IDEM and I-WAL-TS-ORDER: the durable commits form a ts-prefix.

---

## 15. Invariants

Each invariant needs a test and a named mutant that the test catches (PLAN-v3 §3.3, §8 phase A gate). The "Pinned by" column names the card that writes the test. These are P-written tests for the cards PLAN-v3 §7.1 lists (A-3, A-4, A-7, B-1, B-2). Mutant names taken from `verdicts/X-2.md` keep their meaning there.

| ID | Statement | Pinned by | Named mutants |
|---|---|---|---|
| I-ENC-ORDER | §7.2: entry-key order = (user key asc, ts desc); `upper(k)` separates groups | A-4a (port X-2's proptest + directed cases) | TS-ASC, ESCAPE-OFF |
| I-ENC-ROUNDTRIP | §7.2–7.3: decode∘encode = id; undecodable → XX001, never a panic | A-4a, fuzz | TOMBSTONE-AS-VALUE, DECODE-PANIC |
| I-TAG-RESERVED | §7.3: reserved tags and ts suffixes 0/MAX → XX001 on every read path | A-4e | RESERVED-AS-VALUE |
| I-SEEK | §8.1: one descent; newest version ≤ S; prefix-checked | A-4e | TS-ASC, SEEK-NO-PREFIX-CHECK |
| I-NEWEST | §8.2: newest entry of the group, ts and liveness | B-1c | NEWEST-SKIPS-TOMB, UNIQUE-TOMB-LIVE |
| I-SCAN | §8.3: bounds table; one version per key; tombstones omitted; streaming | A-4e | SCAN-NO-SKIP, SCAN-HI-INCL, SCAN-TOMB-EMIT |
| I-CURSOR | §6.6: resume by re-descent with the resume key, never a remembered LPID | A-4e (with A-4b packs racing) | CURSOR-STALE-LPID |
| I-TREE | §6.1: key order and subtree bounds after every SMO | A-4a/A-4b (proptest vs `BTreeMap`) | SPLIT-OFF-BY-ONE, SEP-NOT-UPDATED, PACK-SEP-STALE |
| I-LATCH | §5.3/§6.2: coupling and (depth, left-to-right) order | A-4c (loom/shuttle) | COUPLING-EARLY-RELEASE, PACK-RIGHT-FIRST |
| I-ROOT-STABLE | §6.3: a tree's root LPID never changes | A-4a/A-4b (reopen test) | ROOT-MOVES |
| I-PACK | §6.5: entries preserved; merge iff it fits in `F_PACK·U`; never splits | A-4b | PACK-DROP-LAST, PACK-OVERFILL |
| I-ENTRY-UNIQUE / I-REPLAY-IDEM | §9.1, §14: identical re-insert is a no-op, a different one XX001; recovering twice gives the same state | B-1c, A-7d | REPLAY-DUP-DIFFERENT, double crash |
| I-FAIL-STOP | §9.2: no mutation after a poisoning error | B-1c, A-3c | POISON-IGNORED |
| I-GC-READEQ | §10.2: every GC mutation is read-equivalent at every S ≥ the published W | B-3 (+ G0 re-point, B-7) | GC-DROPS-VISIBLE, G-WATERMARK+1 |
| I-GC-ORDER | §10.2/§10.4: an elided tombstone is deleted last | B-3 | G-RESURRECT |
| I-GC-COMPLETE | §10.2: after a quiescent sweep, at most one version ≤ w per key, and it is live | B-3 (the §10.2 directed test, with a reader) | GC-KEEPS-TOMB |
| I-GC-LOCAL | §10.3: page-local prune acts only on what the leaf proves | B-3 | PRUNE-FIRST-ENTRY |
| I-SPACE | §11: quiescent fill bound; B-3's W4-shape acceptance ≤ 1.5× | A-4b (bound), B-3 (acceptance) | PACK-OFF |
| I-LPID-EXACT | §6.8: mapped LPIDs = reachable pages; no leak on pack, drop or chain free | A-8 amcheck, A-4b, B-6 | LEAK-ON-PACK, DROP-ROOT-FIRST |
| I-POOL-BOUND | §5.1/§5.5: the pool never grows; a dirty set of 4× the pool completes | A-3b/A-3c (E4 shape) | POOL-GROW, EVICT-SKIP-DIRTY |
| I-NO-INPLACE | §4.4: a live home slot is never overwritten | A-3c, A-7b (SimEnv crash) | INPLACE-EVICT |
| I-INTEGRITY | §3.3: CRC, id and epoch checked on every read, with XX001 | A-2, A-7d (SimEnv lost/misdirected/torn) | CRC-SKIP, EPOCH-SKIP, ID-SKIP |
| I-FREE-SAFE | §4.4/§13 step 6: no reuse before the freeing meta is synced | A-3c, A-7c | FREE-EARLY |
| I-FREE-EXACT | §4.3: the persisted bitmaps are exact | A-7b, A-8 | BITMAP-OMITS-FREESET-PAGES |
| I-META-ATOMIC | §13: a meta never points at unwritten pages | A-7c (crash between every step) | META-BEFORE-SYNC |
| I-CKPT-AT-T | §13: the image is a consistent cut; (a)–(c) | A-7a/A-7c (splits and packs during a checkpoint; async mix) | FLIP-NO-GATE, WAL-SYNC-T-ONLY |
| I-REDO-COMPLETE | §14: every acked sync commit is in the image or replayed | A-6d, A-7d | START-LSN-PAST-CHUNK |
| I-WAL-TS-ORDER | §12.4: commit_ts strictly increases with LSN | A-6c, B-1c | TS-AFTER-APPEND |
| I-BULK-PUBLISH | §12.8: unlogged writes only into unpublished trees; publish after the forced meta | B-4d | PUBLISH-BEFORE-META |

amcheck (A-8) checks I-TREE, I-ENC-ROUNDTRIP, I-TAG-RESERVED, I-LPID-EXACT, I-FREE-EXACT and I-INTEGRITY on a quiescent file.

---

## 16. Measured evidence

All numbers are from n5000 (i5-13500H, 16 CPUs), rustc 1.99.0, in memory, medians of 3 runs, unless noted. Workload sources: `spikes/e1-bench/README.md` at 7ccd1cff.
- **Commits measured at:** W1 (b)/(c), W2, W3 and W4 at d28582e8; W1 (a), W5 and W6 at c39656fd.
- **Gate re-measurements:** `verdicts/X-1b.md` (rounds 1 and 2) and `verdicts/X-2.md`.
- **Decision:** PLAN-v3 §4, "E1′ decision".

| Design element here | Measured | Source |
|---|---|---|
| N3 = (b) (§7) | W1 CPU/txn (b) vs (c): 4.0/3.8 µs (t=1), 9.4/7.9 (t=4), 15.7/19.5 (t=8), 69.1/117.0 (t=16): never ≥ 2× for (c). W2 reader throughput (c)/(b): 1.18× (0 writers), 1.38× (10), 1.12× (100) | README §W1, §W2; decision block, rule clauses 2–3 and ruling R1 |
| One-seek point read (§8.1) | W2 reads at 0 writers: (b) 766,882 ops/s, p50 9,058 ns | README §W2 |
| Escape cost (§7.4) | 124.07 B applied per W1 txn (100 B values), of which 13.07 B is the escaped 8-byte key | README §W1; `e1-bench/src/util.rs:67` |
| Version skipping in scans (§8.3) | W3 100-row scans at 10 versions/key: latest (b) 88,019 ns vs (c) 10,715 ns (8.2×); old snapshot (b) 87,627 ns vs (c) 146,156 ns | README §W3; decision block "Known costs" |
| Merge and free set (§6.5, §11) | W4 (b) tree/logical 20.11 → 20.11 after GC, although versions fell 1.1M → 100k (1.13 MB freed): E0 never merges or frees | README §W4; `e0-btree/src/tree.rs` T-NOMERGE; decision block |
| Tombstone elision rule (§10.2) | The spike's `above_w == 0` elision leaves extra tombstones and inflates (b)'s space; X-2-F1 rule adopted | `verdicts/X-2.md` finding 1; decision block "Consequences" |
| Pool and dirty eviction govern retention (§5.5) | W5 long reader: (b) retained 3,000,000 versions, peak tree 536,436,736 B, inside a preallocated 1 GiB pool; VmHWM 1,874 MiB is pool-dominated, not per-consumer evidence | README §W5; `verdicts/X-1b.md`; decision block ruling R2, assumptions 1–2 |
| Eviction exercised only once so far | X-2 gate: S13 at 64 frames never exceeds 1 page; s14 at 16 frames reaches 854 pages / 100,000 versions | `verdicts/X-2.md` (experiment evidence, finding 2 → X-2-F2) |
| Eviction cost of the simple pool (§5) | E0 at a 1/4 pool: point 2,201 vs 1,182 ns in memory (1.9×); insert 2,745 vs 1,477 ns | `e0-btree/README.md` RESULT |
| Simple buffer manager is enough (N8) | E0 in memory vs SQLite: point 1,182 vs 1,317 ns (1.11×), range 7,003 vs 10,952 ns (1.56×): PASS; 8-thread W-mt only 1.09× the single-thread rate (the E7 input) | `e0-btree/README.md` RESULT |
| Large-transaction volume (§12.7) | W6 10M-row UPDATE: (b) post-commit tree ≈ 5.32 GB (≈ 649k pages), write set 117 B/key, commit 20.4 s, head-of-line p99 (= max) 17.94 s | README §W6; `verdicts/X-1b.md` |

---

## 17. Boundaries

**C-T0 v8 (card S-2) owns, and this file does not specify:**
- **Watermark.** The computation, publication and monotonicity of `W`, and its floor after a restart (`≥` meta `gc_w`, §10.5). That `W ≤` every registered snapshot and `W ≤ visible_ts` is the precondition this file relies on (§10.1).
- **The write set and lock table:** own-write merge (I-HALLOWEEN), escalation, NOWAIT/SKIP LOCKED.
- **The write-side rules:** the RR check after the lock wait, unique checks and ON CONFLICT, using §8.2.
- **Commit-visibility semantics.** The commit ack waits for `visible_ts ≥ c` or a session floor (decision block, "Commit visibility").
- **Pre-applying large transactions** as transaction-tagged provisional entries (decision block, "Large-txn head-of-line"). This file only **reserves** encoding space: tags 6–255 and ts `u64::MAX` (§7.1, §7.3, §19 Q4). Any use of that space, and how images and recovery treat provisional entries (I-CKPT-AT-T (c)), is an edit of this file made with S-2.
- **Lock release at commit cheaper than O(keys)** (325k–437k probes slipped through the release tail in W6).
- **Unrelated commits stalled** behind a big apply by the ts-ordered `visible_ts` advance.
- **SSI** without intents.
- **GC policy:** when sweeps run, their budgets, and DDL retirement timing (C-T0 v7.8 §9.2–9.3).
- **Bounds on disk growth under long readers.** Whether on-disk version growth under long readers needs a bound is S-2/B-3's call (decision block, ruling R2 "Not R-1's call"). This file only guarantees that the growth costs disk, not RAM (§5.5).

**S-3 owns:** the row format, TOAST semantics over §6.7's chains, and the online index build's steps under the §12.8 rule, including how an unfinished build is restarted.

**R-1 owns:** the numeric budgets per mode, `F`, `R_EPOCH`, and the per-consumer accounting of §5.7.

**D-6 and D-8 own:** backup, PITR and replica handling of the §12.8 boundary; format versioning and upgrades.

---

## 18. Refinements of PLAN-v3 §3.3

This file wins over the plan (precedence above). The plan should be updated through an Opus card to match these:

| # | Plan text | Here | Why |
|---|---|---|---|
| R1 | Meta fields: "checkpoint id, T, the page-map root, the free-set root, the catalog root, the WAL start LSN and a CRC" | Adds `gc_w`, `epoch`, `next_lpid`, `slot_count`; the root fields are refs with epoch and CRC; "catalog root" is the tree directory's root (§2.2) | `gc_w` replaces v7.8's synced `/sys/gc_w` (§10.5). The others are needed by I-FREE-EXACT and I-INTEGRITY on physically addressed pages |
| R2 | Step 1: "WAL `sync_data` through the last record with commit_ts ≤ T" | Through the end of every record appended before the flip (§13 step 1) | Applies finish out of ts order (`commit.rs`), so the image can hold part of a commit `> T` |
| R3 | Step 0: "Pause commit-apply briefly" | The epoch gate, held by every gated operation (§5.6) | A split, pack, GC or eviction that straddled the flip would tear the image (I-CKPT-AT-T) |
| R4 | Bulk loads: "S-1 picks one" | Force a checkpoint before publication (§12.8) | Versions at a registered ts `≤ T` cannot be placed by a replay filtered on `commit_ts > T`; page images carry LPIDs |
| R5 | B+tree (no root rule) | Stable root LPIDs (§6.3) | The directory and images never change on root splits |
| R6 | Free set (no representation) | Two bitmaps, slots and LPIDs (§4.3) | Memory is a fixed fraction of the file; no fixed-point problem at serialisation |
| R7 | WAL order (unstated) | I-WAL-TS-ORDER (§12.4) | Required for the ts-prefix that E4's oracle checks |
| R8 | "split and merge" (no merge rule) | The pack step with `F_PACK = 0.85`, driven by the GC sweep (§6.5, §10.4); I-SPACE (§11) | The decision block requires GC to return space; B-3's 1.5× acceptance |
| R9 | §3.4 GC "prunes versions below the watermark during page writes and splits" | Page-local prune (required at splits, optional at eviction) **plus** a whole-tree sweep (§10.3–10.4) | Page-local pruning cannot see groups that span leaves, so alone it cannot meet I-GC-COMPLETE or B-3's bound |

---

## 19. Open questions, known costs, revisit triggers

**Open:**
1. **File shrinking.** GC returns space to the free set, not to the filesystem (§4.5). Trigger for a tail-truncation step (at step 6, of free tail slots only) or an offline compaction: a browser quota (E6/D-5) or a user report of a file that does not shrink after a mass delete.
2. **Leaf prefix compression within groups.** Every version repeats its full escaped key, so a 10-version group of W1-sized keys spends about 117 B on repeated key bytes. A-4d covers inner nodes only. Trigger: NBench space rows, or I-SPACE met but B-3's bytes measured close to 1.5×.
3. **Optimistic latches and pointer swizzling** (N8/N9): only on the E0/E7 triggers (PLAN-v3 §3.12).
4. **Provisional entries.** Reserved space only (§7.1, §7.3, §17). S-2 must specify their key placement (ts `u64::MAX` sorts first in a group, like v7.8's intent), their value tag, how §8 reads treat them, and I-CKPT-AT-T (c) for images that contain them.
5. **PITR and replicas across a §12.8 bulk load** (D-6).
6. **Resumable overflow-chain free** if A-5 measures the flip hold as too long (§6.7).

**Known costs of (b), accepted, with revisit triggers (from the decision block, restated so the storage cards carry them):**
- **T1: latest-snapshot scans.** 8.2× slower than (c) at 10 versions/key (W3). Mitigations: GC at the watermark (B-3) and version skipping (§8.3, A-4e/B-3). **Trigger:** after both mitigations, the NBench OLTP range rows or E9 miss target, and a profile attributes more than 20% of scan CPU to stepping over invisible versions. Then an Opus card re-opens N3.
- **T2: space amplification after GC.** 20.11× in E0. **Trigger:** B-3's W3/W4-shape acceptance (after-GC tree bytes `≤ 1.5×` pre-update) fails on the real tree. Then N3 is re-opened.
- **T3: contended writes.** Both arms collapse past 8 threads. This is a shared commit-path or latching bottleneck owned by S-2 and E7, not by N3 or this file.

---

## Changelog

- **Draft 1** (2026-10-23, card S-1): first version. Pins the N3 = (b) entry layout from X-2 (and C-T0 v7.8 §2.2's value variants), the read, scan and GC algorithms, the pack step and the free set that make GC return space, the memory model behind X-4's ruling R2, the WAL format, the checkpoint order, recovery, the §15 invariant table with named mutants, the measured-evidence map, and the §18 refinements of PLAN-v3 §3.3.
