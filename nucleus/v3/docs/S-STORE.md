# S-STORE: Storage layer (normative)

Status: draft 6 (card S-1-F5, 2026-11-03: WAL back-pointers, `prev_lsn` and `prev_crc` with the meta's `wal_start_prev` and `wal_start_prev_crc`, and the splice check I-WAL-CHAIN, per the owner decision PLAN-v3 §10 item 9 and its 2026-11-03 addendum) on draft 5 (card S-1-F3, 2026-11-03: WAL loss after sync and the coverage check I-WAL-COVER with read-only salvage, I-ACK-PREFIX, the ordered boot-id stop and boot-id freshness, the exact segment end, XX001 and WAL-path poisoning in §9.2) on draft 4 (card S-1-F2, 2026-10-26: the meta own-slot rule in §2.2) on draft 3 (card S-1-F1, 2026-10-23) + the S-2 co-edit (card S-2, 2026-10-23: provisional entries, the apply frontier `M`, chunk supersede, the boot checkpoint) + the S-2-F1 fixes (2026-10-23: frontiers in §10.1, §10.5 and §17, the boot checkpoint's durability, chain free on replace) + the S-2-F2 fixes (2026-10-24: the boot id in §7.3 and §8, `Released(⊥)` chunks in §12.6, the read-only open in §14.1) + the S-2-F2 rework (2026-10-24: the stopped store and its remedy in §14.1, a short segment in §12.5). This is a P-authored spec edit and needs a P gate before it is normative. Every `nucleus-store` card (Phase A) and the storage side of every `nucleus-txn` card that touches trees (B-1c apply, B-1d provisional entries, B-3 GC, B-4d bulk load, B-6 drop cleanup) implements against this file. A change to an invariant (`I-*`, §15) is made here first, then in the tests and mutants that pin it, and only then in code.

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
| entry | One B+tree cell `(entry key, entry value)`. Under N3 = (b), an entry is **one version** of one user key (§7), or a **provisional entry** (below). |
| provisional entry | A large transaction's own data for one key, at the head of the key's group: ts suffix `u64::MAX`, tags 6–8 (§7.3). It is not a version until C-T0 v8 §2.4 says so. A **dead** provisional entry is one from a previous boot. |
| group | The contiguous run of entries of one user key, ordered newest first (§7.2). |
| `ts` | A commit timestamp (C-T0 `Ts`, `u64`). `0` and `u64::MAX` are never commit timestamps (§7.1). |
| `S` | A snapshot ts. A read at `S` sees, per user key, the newest version with `ts ≤ S`. |
| `W` | The GC watermark (C-T0 v8 §9.1): `W ≤` every registered snapshot, `W ≤` the apply frontier `≤ visible_ts`, and monotonic (§17). |
| apply frontier | The largest ts such that every commit with ts ≤ it has its versions in the trees as **ordinary entries** (tags 0–5). It is C-T0 v8's materialized frontier `M` (C-T0 v8 §3.5). It equals `visible_ts` except while a large transaction finalizes: its provisional entries are already readable as versions, so it is visible, but it is not yet applied in this sense. |
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
wal_start_prev u64   the start LSN of the record immediately before wal_start_lsn in the stream (§12.2 prev_lsn); all-ones if none
wal_start_prev_crc u32   the crc field of that record (§12.2 prev_crc); 0 if none
wal_ckpt_end   u64   the WAL append position read at the flip of checkpoint k (§13 step 0); step 1 made every byte below it durable
ts_ckpt_hi     u64   the commit_ts of the last Commit or CommitMarker appended before that flip, or the frontier the boot started from if none was; ≥ T
boot_id        u32   the boot id of the session that wrote meta k (C-T0 v8 §2.5, §14 step 8); a boot that opens meta k gets a newer id (§14 step 8); 0 in meta 0, written by the creating session (§2.3)
crc            u32   CRC32C of the bytes above
```
The creating session's boot id is 0, meta 0's `ckpt_id`. Meta 0 is built through the checkpoint path and synced before epoch 1 (§2.3), and every boot checkpoint has `ckpt_id ≥ 1` and a boot id no older than it (§14 step 8), so no later boot has id 0.

`wal_start_prev` and `wal_start_prev_crc` are the comparands for the first record a walk from meta `k` reads (§12.5, I-WAL-CHAIN). Step 0 records them with `wal_start_lsn`, and step 4 writes them (§13). The commit path keeps the pair for every position §12.6 can choose: for a record there, it is the `prev_lsn` and `prev_crc` written into that record; for the append position, it is the start and the `crc` field of the last record appended. That record ends at or below `wal_start_lsn ≤ wal_ckpt_end`, so step 1 has written it, and its `crc` is final, before step 4 writes the meta. They are compared, never read through: the record they name may lie in a recycled segment. Meta 0's are all-ones and 0.

**At open**, the meta with the highest `ckpt_id` among those whose magic and CRC are valid wins. There is **no fallback** to the older meta when the newer one is valid but its image fails an integrity check. After meta `k` is synced, the slots of image `k−1` may be reused (§4.4), so falling back is unsafe. The open fails with XX001, and amcheck reports the failure. A meta is valid at open only if it also sits in its own slot, `1 + (ckpt_id mod 2)`; a meta with valid magic and CRC in the other slot is a misdirected write and is never opened, and if its `ckpt_id` exceeds that of every valid meta, the open fails with XX001, because the meta before it was synced before it was written and falling back past a synced meta is unsafe.

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
- **Over-size keys (I-KEY-SIZE).** A write whose encoded entry key exceeds `K_MAX` is refused with 54000 **when it enters the write set** (S-2's write path calls the store's size check). Apply never sees it (§9). Mutant: SIZE-CHECK-OFF. Without the check, the over-size key reaches apply, fails there and poisons the store (§9.2), instead of returning 54000 to the writer with the store still usable.
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

**I-LINK-ATOMIC.** Every gated operation leaves the reference graph closed. An LPID becomes mapped in the same gated operation that inserts its first reference, and unmapped in the same gated operation that removes its last. The references are a parent's child pointer, an entry's chain reference, an Overflow page's next pointer, and a directory entry's root. So every image, at every cut, satisfies I-LPID-EXACT with no special case, and §4.2's immediate reuse never hands out an LPID something still names.

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
1. **Merge.** If `used(L) + used(R) ≤ F_PACK·U` (for inner pages, plus the cell of the separator between them, which is pulled down into `L` first), move all of `R`'s cells into `L`. Remove `R`'s separator and child pointer from `P`, then free `R`'s LPID (§4.2). A merge only shrinks `P`, so it can always run.
2. **Redistribute.** Otherwise, if the **first cell that would enter `L`** fits with `used(L)` staying `≤ F_PACK·U`, move the **longest prefix** that keeps `used(L) ≤ F_PACK·U`.
   - **Leaves.** The cell that enters `L` is `R`'s first cell, and the move is a prefix of `R`'s cells. Replace the separator in `P` with a new shortest separator.
   - **Inner pages (a rotation through `P`).** The cell that enters `L` is **`P`'s separator** between `L` and `R`, not `R`'s first cell. It comes down with `R`'s first child pointer. Moving `k` children brings down `P`'s separator and `R`'s first `k − 1` separators, together with `R`'s first `k` child pointers. `R`'s `k`-th separator rises into `P` in its place. The fits test is on `P`'s separator.
3. **Full.** Otherwise, do nothing. The cell that would enter `L` first (`R`'s first cell for a leaf, `P`'s separator for an inner page) is at most `C_max` and does not fit, so `L` already holds more than `F_PACK·U − C_max` (§11).

**Parent-overflow skip.** A redistribute whose new separator is longer than the old one by more than `P`'s free space is **skipped** instead, and nothing changes. A pack step never splits. The caller counts the skip (the sweep reports it, §10.4), and I-SPACE excludes the skipped `L` (§11). The next sweep retries it. A skip needs `P` within one separator's growth of full, so it is rare once merges have removed separators from `P`. Mutant: SKIP-UNCOUNTED, a skip left out of the count. It is caught by forcing a skip (a nearly full parent and long separators) and asserting I-SPACE with the reported count.

**`F_PACK`** is an engine constant, not a format field. It is below 1 so that a packed leaf can absorb new versions without splitting at once. Under (b), every update inserts a version into its key's leaf.

**Correctness.** The step preserves the multiset of entries, I-TREE and every key's subtree bound. It is atomic with respect to readers (latches) and to images (the gate).

**Who calls pack.**
- **The GC sweep** packs systematically, left to right (§10.4).
- **A delete that descends from the root under latch coupling** (a directory removal) may pack the affected leaf with its right neighbour, or its left neighbour if it is the rightmost child, when `used < U/4`, while it still holds the parent. The two leaves are latched left to right: to pack with the left neighbour, it releases the leaf and, still holding the parent exclusively, latches the neighbour and then the leaf.
- **A delete made while holding only a leaf** (the page-local prune, §10.3, including at eviction) never packs. Taking the parent's latch from below would break I-LATCH. It leaves the page for the next sweep.
- **Inner pages** whose `used` falls below `U/4` after a pack removed a child are packed under their parent, by a **re-descent** that latches the parent first. Nothing ever latches a parent after its child (I-LATCH). After a delete, they pack the same way as the leaf (the right neighbour, or the left one for a rightmost child). **The sweep uses its own rule instead** (§10.4 step 5): an inner page packs only with its right neighbour, and a junction walk follows. A left pack would make the left neighbour's former last child, which was exempt and may be underfull, a middle child beside a page the sweep has already passed, and nothing would pack the two again, so I-SPACE would fail (§11).
- **The root** collapses when it is an inner page with one child (§6.3).

Under N3 = (b), **logical deletes are tombstone inserts** (§7). Only GC, DropTree, overflow freeing and provisional-entry settling (C-T0 v8 §2.4) ever remove entries or pages.

### 6.6 Cursors (I-CURSOR)
- **Holding latches.** A cursor holds shared latches on its root-to-leaf stack while it is positioned (E0's design), so no SMO can change the path it holds.
- **Releasing and resuming.** A cursor that releases its latches resumes by **re-descending from the root with its resume key**, never by a remembered LPID. Cursors may release between batches, and card A-4e decides when. The resume key is the last key returned (exclusive), or a group's upper bound when skipping (§8.3).
- **Safety.** This is what keeps merges, frees and LPID reuse safe for scans that do not hold latches throughout.

Iterators are **streaming**: a range scan never materialises its range. The spike's whole-range materialisation is a spike limit (`verdicts/X-2.md` non-blocking finding 3), not a behaviour.

### 6.7 Overflow chains
An entry whose value exceeds `V_INLINE` stores a reference `(len u64, first LPID)` in its value, with tag 4 or 5 (§7.3). The chain is a list of Overflow pages.
- **Immutability.** Chains are immutable once written (versions are immutable).
- **Building.** A chain is mapped in the gated operation that inserts its entry, as it is freed with its entry (I-LINK-ATOMIC, §5.6).
- **Freeing.** A chain is freed **in the same gated operation** that deletes its entry (§10), or that replaces its entry's value: a provisional rewrite (C-T0 v8 §2.4) or a superseding replay op (§14), unless the new value is the same chain. **Exception:** a conversion of a tag-8 provisional entry (C-T0 v8 §2.4) moves the chain to the new tag-4 or tag-5 entry and deletes the provisional entry without freeing it. Both happen in one gated operation, so no image holds two references to one chain. A chain build or free therefore holds the flip for the chain's length. A-5 measures the flip hold for the build as well as the free, at the maximum value size; if either is too long, A-5 proposes a resumable build or free, which is an edit of this file (§19 item 6).
- **Reading without the leaf latch.** A reader that holds a registered snapshot `S ≥ W` may read the chain after releasing the leaf latch. GC never deletes a version that is the newest `≤ S` for any registered `S` (I-GC-READEQ), so a version that a registered snapshot reads cannot lose its chain while the reader holds that registration.

### 6.8 Tree directory
- **What it is.** A system B+tree, unversioned, with a stable root at `dir_root`. Its entries map `tree_id (u64 BE) → (root LPID u64, kind u8, state u8)`.
- **Tree ids** are C-T0 §10 storage ids, never reused. The WAL names trees only by tree id (§12).
- **States:** `live` or `dropping`.
- **DDL visibility** lives in the versioned catalog rows (C-T5), not here.

**CreateTree(id)** allocates a root leaf and inserts the directory entry, in one gated operation (I-LINK-ATOMIC, §5.6). It is idempotent: if the id already exists, nothing happens.

**DropTree(id)**
1. Set the entry's state to `dropping`, in one directory mutation.
2. Free the tree's pages bottom-up in bounded gated steps. Each step unlinks a child from its parent and frees it, with its entries' overflow chains, in one gated operation. A parent the step empties is freed in that same operation and unlinked from its own parent. A root's parent is its directory entry, so a step that empties the root is the last step (step 3).
3. **The last step frees the root and removes the directory entry in one gated operation**, and frees the root's entries' overflow chains if the root is a leaf. The root's latch is released before the directory descent; the gate is held across both (§5.6: never twice).
   - **A non-resident directory page** restarts the descent, not the operation: the thread releases its directory latches, keeps the gate, pins the page and re-descends. §5.6 permits a page read under the gate, and its release on a restart applies only before an operation's first mutation.
   - **Inside the step,** a concurrent allocation may take the freed LPID (§4.2) while the entry still names it. No flip can land there, and the tree's one walk, the only reader of a `dropping` entry's root, has finished with it.

At open, every `dropping` entry's walk resumes (the B-6 boot job; C-B1 "drop cleanup = freeing pages"). DropTree is idempotent: dropping an absent id does nothing. Every image therefore holds either the entry with its root mapped, or neither. A crash mid-walk leaks no page and names no freed LPID (I-LPID-EXACT, I-LINK-ATOMIC). At open, an unmapped root of a `dropping` entry is corruption like any other unmapped reference (XX001); §14 step 6 needs no other arm.

**When the drop runs** (after `W` passes the DDL) is C-T0 v8's rule (v7.8 §9.2).

**I-LPID-EXACT** holds of every image, crash images included; "quiescent" (§15) describes amcheck's reading, not an exemption for crash images. The mapped LPIDs of an image are exactly:
- the pages reachable from the directory roots, a `dropping` entry's root among them, including overflow chains (each referenced by exactly one entry);
- the directory's own pages.

Mutants (§15 I-LINK-ATOMIC): **DROP-ENTRY-SPLIT** (the root free and the entry removal in two gated operations), **DROP-UNLINK-SPLIT** (a child freed one gated step before its unlink), **CREATE-TREE-SPLIT** (the root mapped one gated step before the entry).

---

## 7. Versioned entries (N3 = (b))

Decided by X-4 (PLAN-v3 §4, "E1′ decision") from the e1-bench evidence (§16). The layout is X-2's (`spikes/e1-arm-b/src/encode.rs`), pinned by its proptest. S-1 owns it (decision block, "Consequences").

### 7.1 Timestamps
- **Reserved values.** `ts = 0` and `ts = u64::MAX` are never commit timestamps. C-T0 already reserves `Ts::ZERO`. The suffix `u64::MAX` marks a **provisional entry** (§7.3 tags 6–8; C-T0 v8 §2.4): `entry_key(k, u64::MAX) = seek_newest(k)`, the head of k's group.
- **Uniqueness.** One commit writes at most one version per user key per tree, at its own unique ts, so **entry keys are unique** (I-ENTRY-UNIQUE). A tree holds at most one provisional entry per user key.

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
| 6 | provisional live value | `txn (u32 boot id, u64 n), seq u32, data_seq u32, flags u8` (bit 0 = `key_changed`), then the value bytes | (C-T0 v8 §2.4) |
| 7 | provisional tombstone | `txn, seq, data_seq, flags` (bit 0 = `moved`) | (C-T0 v8 §2.4) |
| 8 | provisional live value in an overflow chain | `txn, seq, data_seq, flags, len u64, first LPID u64` | (C-T0 v8 §2.4) |
| 9–255 | **reserved** | — | — |

**Why these tags.**
- Tags 0 and 1 are the spike's (`encode.rs:50`, `:53`).
- Tags 2 and 3 carry the C-T0 behaviour that needs them (the KEY SHARE rule, and moved rows under RC EPQ), whose physical clauses C-T0 v8 replaces with references to this file (PLAN-v3 §3.4). C-T0 v8 owns what they mean.
- Tags 6–8 are C-T0 v8 §2.4's provisional entries, assigned by card S-2. They occur only with the ts suffix `u64::MAX`.
- *Tombstone* below means tag 1 or 3; *live* means tags 0, 2, 4 and 5. *Ordinary* means tags 0–5.

**I-TAG-RESERVED.** The following are XX001 on every read path and are never served:
- a reserved tag (9–255);
- an entry whose suffix encodes ts `0`;
- an ordinary tag (0–5) with the suffix `u64::MAX`, or a provisional tag (6–8) with any other suffix.

### 7.4 Size accounting
An inline version costs `|k| + zeros(k) + 10` key bytes, plus `1 +` value bytes, plus the cell and slot overhead. The escape's cost is measured: W1's 8-byte keys `i.to_be_bytes()` for `i < 2^20` (`spikes/e1-bench/src/util.rs:67`) always carry five `0x00` bytes, and W1 measured **124.07 B applied per txn** for a 100-byte value. That is 101 value bytes + 10 + 13.07 escaped key bytes (`spikes/e1-bench/README.md` §W1, "(b) B/txn"). The escape cost is accepted as part of X-2's pinned layout; leaf prefix compression is an open mitigation (§19 Q2).

---

## 8. Reads

Every read below runs on one tree. The write set merge (a transaction's own writes) is S-2's (C-T0 v8, I-HALLOWEEN); this section is the committed-data half.

**Provisional heads (C-T0 v8 §2.4, §4).** A group's first entry may be a provisional entry `e` (§7.3 tags 6–8). Every read in §8.1–§8.3 takes from C-T0 a **resolver** for `e`. The resolver returns *ignore* (dead, `Open`, `Aborted`, or the reader's own entry) or *a version at `c`*. It is called **while the leaf holding `e` is still latched** (C-T0 v8 I-PROV-LOOKUP). Then:
- **Head first.** The head is examined before the group's ordinary entries (I-PROV-READ). A conversion inserts `k@c` before it deletes `e` (C-T0 v8 I-PROV-CONVERT-ORDER), so a reader that finds no `e` finds `k@c`. Mutant ORDINARY-FIRST.
- **Candidate rule.** If the resolver yields a version at `c`, the read's answer is the newer of `(c, e's data)` and the ordinary answer below. Both may be at `c`, during a conversion; they are then the same version.
- **Fast path.** When the caller states that no current-boot provisional entry exists in the tree (C-T0 v8 §4's per-tree count is 0), §8.1 skips the head check and is exactly one descent. "Current-boot" means `txn.boot` equals C-T0 v8's boot id (§12.2), never the checkpoint epoch `e` (§1), which changes at every checkpoint. Dead entries never affect §8.1 at `S < u64::MAX`, because `seek(k, S)` lands after them.
- **Provisional data** is decoded like the matching ordinary tag (6 like 0 or 2, 7 like 1 or 3, 8 like 4 or 5, by the flags).

### 8.1 Point read at S (I-SEEK)
`get(k, S)`:
1. descend once to `seek(k, S)`;
2. take the first entry `e ≥ seek(k, S)`. If the seek lands at the end of a leaf, the cursor stack moves to the next leaf (§6.6);
3. if `e` does not start with `prefix(k)`: no version of `k` is `≤ S`, so the result is **absent**;
4. otherwise decode `e`'s value: live → the value (reading the chain for tags 4 and 5); tombstone → **absent**; reserved → XX001. A provisional head is handled by the rule above, before this step.

There is one descent and no per-version stepping (`engine.rs:238` `version_at`). Mutants: TS-ASC, SEEK-NO-PREFIX-CHECK, TOMBSTONE-AS-VALUE.

### 8.2 Newest probe (I-NEWEST)
`newest(k)` = the first entry at or after `seek_newest(k)` that starts with `prefix(k)`, as `(ts, live value | tombstone)`, or none (`engine.rs:266`). If that entry is provisional, the provisional-head rule applies: `newest(k)` is the newer of its resolved version (if any) and the next entry of the group.
- **Precondition, owned by S-2.** The caller holds `k`'s lock, and locks are released only after the holder's commit is visible (C-COMMIT-ORDER). Then no apply to `k` is in flight, and the newest entry is the newest committed version. The one exception is a conversion of an already-visible provisional entry, which the head-first rule makes unobservable (C-T0 v8 §5.1).
- **Who uses it.** S-2's RR conflict rule (40001 iff `ts > S`, checked **after** the lock wait, `engine.rs:430–435`) and its liveness rule (a tombstone is not live, `engine.rs:465`).
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
0. apply the provisional-head rule to the group's head;
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
- **When.** C-T0 v8 decides when apply runs, and how `visible_ts` and lock release follow it (C-T0 v8 §3.1). A large transaction does not apply at commit: its provisional entries become readable at its flip, and its finalize converts them (C-T0 v8 §3.2).

**I-ENTRY-UNIQUE / I-REPLAY-IDEM.** Inserting an ordinary entry whose key already exists:
- with an **identical** value: a no-op. This happens only in replay (§14) and in a provisional conversion that another settler already made (C-T0 v8 §2.4). *Identical* means the same tag and the same value bytes. For tags 4 and 5 it means the same `len` and the same chain bytes: the chain is compared, not its LPID. A chain built for the re-insert is then freed in the same gated operation;
- with a **different** value: XX001. Two commits cannot share a ts. **Exception:** the replay of a chunked transaction's superseded ops (§14).

**Provisional entries** are inserted, replaced and deleted only through C-T0 v8 §2.4: by their owner, or by a settler. Replacing a provisional entry's value is not an I-ENTRY-UNIQUE conflict.

### 9.2 Failure (I-FAIL-STOP)
- **No logical failures.** Apply cannot fail logically: key size is checked when the write enters the write set (§3.2), and entries are unique.
- **Physical failures poison the store.** An I/O error (58030), 53100, 53200 or XX001 poisons the store when it is raised inside any of these:
  - a gated operation (§5.6): an apply insert, a tree mutation or SMO, a pack step, a GC mutation, an LPID free, a directory change, a dirty eviction, a materialise or conversion step (C-T0 v8 §3.2);
  - a checkpoint step (§13);
  - a step of a background store job (the drop walk, §6.8; the GC sweep, §10.4);
  - a WAL write or `sync_data`, a segment's creation, preallocation or recycling rename and its `wal/` directory sync (§12.1, §12.4, §12.5), and recovery's step-5 truncation (§14).

  It does so whether or not the operation has mutated a page yet. An XX001 is a failed physical read, a read whose bytes fail §3.3, so it is a physical failure like 58030.
  - *Why even before a mutation.* An apply cannot be skipped: an XX001 on a page it must descend through leaves its commit unappliable, and nothing may skip a commit (C-T0 v8 §3.1). An SMO cannot be undone (no partial unwinding, below). Any other gated step that has mutated nothing yet (GC, pack, drop walk, materialise) would otherwise be retried forever by its job against the same unreadable page, holding back `W`, the drop or the finalize. That is the livelock this rule removes (below).
  - *Why the WAL path.* A failed WAL `sync_data` is never retried as if its bytes were safe (PostgreSQL's 2018 fsync-retry lesson). I-ACK-PREFIX's waiters (§12.4) cannot pass the failed batch, so without the poison they would wait forever. And §14.1 already treats a WAL append's 53100 as a stopped store.
- **Client reads never poison.** An XX001 on a read path that mutates nothing is returned to its caller, and the store stays usable. Such paths are §8's reads and the newest probe, cursors, amcheck, and every read of a read-only open (§14.1). A later gated operation that meets the same page poisons then.
- **How a poisoned store ends.**
  1. **The mark.** The poison mark is one atomic store that takes no lock and holds the error. The failing operation makes it before it releases its first latch, the gate or a pin.
  2. **Every acquisition checks it.** From then on, every call into the store returns the error, reads included. Every latch acquisition, every `try_latch` (the evictor, §5), every gate and pin acquisition checks the mark, so no reader or evictor walks a structure that the failed operation left half-changed.
  3. **Every wait includes it.** Every blocking wait, in the store and in C-T0 v8, has the mark in its wait predicate, evaluated under that wait's own mutex. The waits are:
     - commit-ack and visibility waiters (C-T0 v8 §3.4);
     - the group-commit queue and every wait on `written_end` or `durable_end` (§12.4), checkpoint step 1's included;
     - gate and pin waiters;
     - background jobs;
     - the lock-table waits (C-T0 v8 §6.3): `wait_for` on a holder, relation and advisory lock waits, deferrable-prefix waits and `wait_for_lockers`.

     A holder is released only once `visible_ts` reaches its commit (C-T0 v8 §3.3), and after a poisoned apply `visible_ts` never advances. So without the mark in its predicate, a lock waiter would park forever. Every wait releases its mutex and sleeps in one step (a condition variable or equivalent) and re-tests its predicate on every wake.
  4. **The broadcast.** After making the mark, the poisoner releases every latch, pin, the gate, the settle stripe and every internal mutex it holds, and **then** broadcasts to every wait queue, taking each queue's mutex in turn. "Every wait queue" includes each live transaction's waiter list. No waker holds a page latch while it takes a waiter's mutex (C-T0 v8 §2.6). A waiter that tested its predicate before the mark is either still holding its mutex, so the broadcast waits for it to sleep, or already asleep on it. So no waiter sleeps through the broadcast. Each woken waiter returns the error. A pending COMMIT returns it too, and its outcome is unknown to the client, as after a crash: it may be durable and recovered.
  5. **Nothing more is written:** no WAL write, no sync, no checkpoint step, no meta. On disk the store is a crashed store (§14.1).
  6. **The process stops.** A server exits non-zero after returning the error to its sessions. An embedded or browser handle returns it from every call until it is closed. The next open runs recovery (§14), which replays the commit (PLAN-v3 §3.4; `engine.rs` B-FAIL-STOP).
- **Why XX001 is named.** Without it, a gated operation that hits an unreadable referenced page fails alone while the store runs on. In E4, every other task then parked forever behind the service loops (X-5c, MP-LIVELOCK: 41 rounds; 0 with this rule as fill G8).
- **No partial unwinding.** The store never tries to undo a partial apply: under (b), the replay of a partially applied commit is idempotent (§14).

---

## 10. GC (physical)

C-T0 v8 owns the watermark `W`: how it is computed, published and floored after a restart (§17). This section owns what may be removed given `W`, in what order, and how the space comes back. The rule is X-2's, as corrected by X-2-F1 and adopted in the decision block's "Consequences".

### 10.1 Precondition (A-GC-STABLE)
A GC mutation on a tree with watermark `w` requires two things:
1. `w ≤` the watermark C-T0 v8 published at the time of the mutation;
2. **every insert of an entry with `ts ≤ w` into that tree has finished.**

Condition 2 holds for apply and for provisional conversions, because `w ≤ W ≤ M`, the apply frontier (C-T0 v8 §3.5, §9.1): applies of later commits carry ts `> visible_ts ≥ M`, and a conversion inserts at the commit ts `c` of a large transaction that is not finalized, so `c > M`. Provisional entries (suffix `u64::MAX`) are not versions and are not inserts in this sense. It must also hold for bulk loads, which write entries at a ts `S` (§12.8). Registering `S` itself is not enough, because it gives only `W ≤ S`, and a GC at `w = S` could then run beside the load's unfinished inserts at `S`. So for the whole load, S-3 keeps a snapshot registered below `S`, which gives `W < S`.

**The registration comes before `S` is chosen.** `W` is monotonic (§1), and it equals `visible_ts` when no older snapshot is registered. So `W` may already be `S` when `S` is picked, and a registration at `S − 1` made after that cannot hold `W` down. S-3 therefore:
1. registers a snapshot at some `R` (normally the current `visible_ts`), which holds `W ≤ R` from then on;
2. picks the load's `S > R`, for example `S = R + 1` once `visible_ts > R`;
3. keeps `R` registered until the load's last insert has finished.

That gives `W ≤ R ≤ S − 1 < S` for the whole load.

**No mutant for this condition (S-1 gate, ruling 1).** The load writes only live entries, all at `S`, into an unpublished tree. Suppose a GC with `w = S` races those inserts. Each loaded entry lands either behind the sweep's position, where it is newer than everything that GC kept, or ahead of it, where it is the newest `≤ w` and is kept. The worst outcome is under-pruning, and no read at any `S' ≥ W` changes. A mutant that registers `S` instead of `R` is therefore unobservable without a new hook. Condition 2 stays as a formal precondition, because it is what the proof of §10.4's carried state uses.

Under these conditions, **the set of entries with `ts ≤ w` changes only through GC**, which is what makes carrying state across leaves sound (§10.4).

### 10.2 The rule
For each user key, with `w` fixed:
- **keep** every version with `ts > w`;
- **keep** the newest version with `ts ≤ w`. Exception: when it is a tombstone, **elide** it, provided every older version of the key is deleted, and the tombstone is deleted **last** (I-GC-ORDER). This holds whether or not versions above `w` exist (X-2-F1, correcting the spike's `above_w == 0` condition at `engine.rs:585`). An elided tombstone still counts as the key's newest `≤ w` version, so every older version is deleted and none becomes the kept one;
- **delete** every older version, freeing its overflow chain in the same gated operation (§6.7).

**Provisional entries are not versions here.** No GC mutation counts, keeps or prunes a provisional entry as a version, or lets one change the carried state of §10.4. A GC mutation may delete a **dead** provisional entry (C-T0 v8 §2.4 (c)). Readers ignore dead entries, so that is read-equivalent. Because `W ≤` the apply frontier (C-T0 v8 §3.5), no conversion ever inserts an entry with `ts ≤ w`, and §10.1's condition 2 holds.

**Two departures from the spike's `gc`:**
- **The elision condition.** The spike elides only when `above_w == 0` (`engine.rs:585`); this file elides in every case (X-2-F1).
- **The delete order.** The spike pushes the tombstone candidate first (`engine.rs:585–588`) and then deletes the candidates in push order (`engine.rs:599`). It therefore deletes the tombstone **before** its older versions, and a read at `S ≥ W` between those deletes sees an older version. This transiently violates I-GC-ORDER. Here the tombstone goes last.

**I-GC-READEQ.** Every GC mutation leaves unchanged the result of every read in §8.1–8.3 at every snapshot `S ≥` the watermark published at the time of the mutation. It also leaves unchanged the §8.2 answers for both S-2 rules.
- **Pruning.** A pruned version is shadowed for every such `S` by a newer version `≤ w ≤ S` that stays.
- **Tombstone elision.** Deleting the tombstone last means that at every intermediate state, either the tombstone shadows the older versions, or no older version exists. Both read as absent at every such `S`.

Mutants (all three are `verdicts/X-2.md`'s, with X-2's meaning):
- GC-DROPS-VISIBLE;
- G-WATERMARK+1;
- G-RESURRECT: an elided tombstone at `≤ w` lets the next older version become the kept one.

I-GC-ORDER's mutant is new: **G-TOMB-FIRST**, the tombstone deleted before its older versions (the spike's order). It is caught by a group that spans leaves (so the sweep needs more than one mutation), with reads at `S ≥ W` between the sweep's steps: through an interleaving hook, or through a checkpoint flip between the steps followed by a crash. The image then holds the intermediate state, and recovery replays no GC.

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
- **Fixed `w`.** `w` is fixed for the whole sweep and is stored in the resumable `token`, together with:
  - the carried state: the current user key, whether its newest `≤ w` version has been seen, and a pending tombstone elision;
  - the **resume position**: "after entry key `x`" or "from key `f`";
  - the running count of parent-overflow skips (§6.5).

  Resuming with a later `w` would also be safe, but a fixed `w` keeps the token simple.

**Per step** (one gated operation; latches only top-down and left to right, I-LATCH):
1. **Descend.** Re-descend from the root by the resume position (I-CURSOR) with exclusive latch coupling. Keep latched both the leaf `L` that holds the position and its parent `P`: `P` first, then `L`.
   - On the way down, record the **fence** `f`: the lowest separator to the right of the path, or "tree end".
   - If `L` has a right sibling `R` under `P`, latch `R` after `L`.
   - Every page is pinned and resident before it is latched (pin-then-validate, §5.3). A non-resident page makes the step release everything, pin the page, and restart.
   - If the tree's root is a leaf, `L` is the root, and there is no `P` and no `R`.
2. **Mark.** Apply §10.2 in order to `L`'s entries past the resume position, then to all of `R`'s, using the carried state. This is sound by §10.1.
3. **Delete.** Delete the marked entries, in this step's mutation.
   - **The tombstone's group ends within `L` or `R`:** a newest-`≤ w` tombstone t is deleted in the same mutation as the older versions of its group. Within one gated operation under exclusive latches, the order is invisible.
   - **The group continues past this step:** t is recorded as **pending**. It is deleted in its own gated operation, by key, once the sweep has passed the group's end. If t is already gone (a page-local prune elided it), that operation does nothing.
4. **Pack.** If `R` exists, run `pack(P, L, R)` (§6.5), and add any parent-overflow skip to the token's count. This is how GC returns pages.
5. **Advance.**
   - **If `R` existed,** the resume position becomes "after `R`'s last entry key". The pack preserves entries, so the next descent lands on the page that now holds that key (the merged `L`, or `R`), and the sweep packs that page with its right sibling.
   - **If `L` was `P`'s rightmost child,** `P`'s children are done, and the sweep runs the **cascade** `finish(P)` before it moves on.
     - **Addressing.** Let `x` be `L`'s last entry key, and count levels from the leaves (a leaf is level 1). Each page of the cascade is addressed by `x` and its level: the page at that level on the path to `x`, or that page's right sibling.
     - **Latching.** Each pack is its own gated operation, reached by a fresh descent toward `x` that latches the parent first (§6.5).

     `finish(X)` runs on an inner page `X` whose children are all done:
     1. **Root.** If `X` is the root, collapse it while it is an inner page with one child (§6.3). Stop.
     2. **Rightmost child: go up.** If `X` is its parent `G`'s rightmost child, `X` is exempt (§11). `G`'s children are now done too, so run `finish(G)`.
     3. **Otherwise, pack right.** If `used(X) < U/4`, run `pack(G, X, X_r)`, where `X_r` is `X`'s **right** neighbour (never its left one, §6.5), and add any skip to the token's count. If the pack moved cells from `X_r` into `X` (a merge or a redistribute), run the **junction walk** from `X`. Then stop: `X_r`'s children, or what is left of them, are not done, so `G`'s children are not done either.
   - **The junction walk** from an inner page `X` that has just gained cells from its right neighbour.
     - **The problem.** `X`'s former rightmost child `c` is now a middle child, and its new right sibling `c_r` came from the neighbour and has not been swept. `c` was exempt as a rightmost child, so it may be underfull.
     - If `c` is the leaf `L`, stop. The leaf step that follows packs `L` with `c_r` (see below).
     - If `c` is an inner page with `used(c) < U/4`, run `pack(X, c, c_r)` and count any skip. If that pack moved cells into `c`, continue the walk from `c`, one level down.
     - Otherwise, stop. `c`'s own rightmost child has not moved, so it is still exempt.
   - **After the cascade,** the next step depends on `L`:
     - if `L` now has a right sibling under its parent (the junction walk reached `L`), the position stays "after `x`", so the next step packs `L` with that sibling;
     - otherwise the position becomes "from `f`".

     The token records the cascade's progress (`x`, the current level, and whether a junction walk is under way), so a resumed sweep continues the cascade instead of starting it again. Each check reads the current pages, so a repeated check would only cost work.

**Why the cascade reaches every page, at every height.**
- **Only unswept pages shrink.** In every pack, the right page has not yet been swept. Left pages and parents only grow, except a parent whose own `finish` is still to come. So a page the sweep has passed never drops below its clause again.
- **Exemptions are re-checked.** A passed page loses its rightmost-child exemption only when its parent gains cells from the right. That is exactly the junction walk, which re-checks the page.
- **Every inner page is finished after its last change.** Step 2 recurses through rightmost children, so `finish` reaches every ancestor whose subtree has just ended, not only `L`'s parent. A page that gains unswept children from a merge or redistribute runs `finish` again when those children are done.

At the end of a complete sweep, therefore, every non-root inner page has been checked against `U/4` after its last change, and every leaf has been packed with its final right sibling. Both I-SPACE clauses then hold at every height (§11).

Mutants:
- **SWEEP-PACK-LEFT:** the sweep packs a rightmost-child inner page with its left neighbour. The leaf clause then fails at the junction.
- **CASCADE-ONE-LEVEL:** the cascade checks only `L`'s parent, so step 2 does not recurse. The inner clause then fails at height ≥ 4.
- **JUNCTION-SKIP:** no junction walk after an inner pack. The inner clause then fails at height ≥ 4.

A-4b catches all three with small-page trees of height ≥ 4, asserting I-SPACE after a complete sweep.

**Sweep report.** A complete sweep returns `done` together with its count of parent-overflow skips, which I-SPACE and B-3 read (§11).

**Interruption.** A crash or stop between steps leaves a read-equivalent state (I-GC-READEQ) and leaks nothing (I-LPID-EXACT): every LPID free is in the same gated operation as its unlinking.

When sweeps run, how often, and with what I/O budget is S-2/B-3's decision (background jobs, C-B1 semantics).

### 10.5 The image's watermark (`gc_w`)
- **What it records.** The store keeps `gc_w_used`, the largest `w` it has applied in any GC mutation. At open it starts at the recovered meta's `gc_w`, so a checkpoint taken before the first GC mutation (including the boot checkpoint, §14 step 8) still records the image's bound (mutant GCW-NOT-RECORDED). At the flip of checkpoint `k`, it records that value as meta `k`'s `gc_w`. While the flip holds the gate, no GC mutation is in flight, so `gc_w` bounds every GC mutation contained in image `k`.
- **Bound.** `gc_w ≤ T` always. Each `w` was `≤ W` at its use (§10.1 condition 1), and `W ≤ M`, the apply frontier (C-T0 v8 §3.5, §9.1). `M` is monotonic, and the flip takes `T := M` (§13 step 0), so every `w` used before the flip is `≤ T`. At open, `gc_w_used` starts at the recovered `gc_w ≤` the recovered `T ≤` the recovered apply frontier. A violation at meta write is fail-stop.
- **I-GCW.** Meta `k`'s `gc_w` is `≥` every `w` of a GC mutation in image `k`, and `≤ T`. Mutants:
  - **GCW-NOT-RECORDED:** the meta carries the previous `gc_w` or 0. After a restart, `W` can then start below the image's pruning, and a snapshot in that gap reads a pruned state;
  - **GCW-BOUND-SKIP:** the `gc_w ≤ T` check at meta write is removed.
- **After open,** C-T0 v8's watermark must be at least the recovered meta's `gc_w`, because the recovered pages were pruned with it. In v7.8 this was `/sys/gc_w`, synced before GC acted (C-T0 v7.8 §9.1). v3 has no unversioned system KV, so the image carries it instead. Pruning done after meta `k` is lost with a crash, together with its pages, so the meta's value is exact.

---

## 11. Space reclamation

The decision block requires that GC actually return space. "S-1/A-4b's merge and free set must give the space back." E0 never merged or freed pages, which is why (b) stayed at **20.11×** after GC cut its versions from 1.1M to 100k (`spikes/e1-bench/README.md` §W4; `spikes/e0-btree/src/tree.rs` T-NOMERGE). The chain from a dead version back to a reusable slot:
1. **Entry → page.** The sweep deletes the version (§10.4), and the pack step merges or redistributes leaves, freeing merged-away LPIDs (§6.5). Inner pages and the root follow.
2. **LPID → slot.** A freed LPID's home slot is no longer referenced by the current map. If the durable image or the image being written still references it, it stays pending (§4.4).
3. **Slot → free set.** After the first checkpoint `k` whose flip follows the sweep, image `k` no longer references the slot. Once meta `k` is synced, the slot is free in memory (§13 step 6) and in image `k`'s persisted bitmap (I-FREE-EXACT).
4. **Free set → reuse.** The slot is allocated lowest-first, before the file grows (§4.5).

**I-SPACE (quiescent).** Take a complete sweep (§10.4) of a tree with `w` fixed and no concurrent inserts, which reports `skips` parent-overflow skips (§6.5). After it:
- **leaves:** every leaf has `used > F_PACK·U − C_max`, where `C_max` is the largest cell in the tree, with two exceptions: the rightmost child of each inner page, and the at most `skips` leaves whose pack step was skipped. A pack step leaves `L` short of that only by merging, and `L` then takes part in the next step;
- **inner pages:** no non-root inner page has `used < U/4`, except the rightmost child of each inner page and the at most `skips` pages whose pack was skipped. This holds at every tree height: the sweep's cascade recurses to every ancestor, and its junction walk re-checks a page that has stopped being a rightmost child (§10.4 step 5);
- **root:** the root is a leaf, or has at least 2 children;
- **LPIDs:** every LPID removed from the tree is freed (I-LPID-EXACT).

A test of I-SPACE asserts the clauses with the reported `skips`. B-3 reports `skips` with its measurement, and the derived bound below counts them. In that shape merges dominate and separators are short, so `skips` is expected to be 0.

**Tree bytes (the metric of B-3 and T2).**
- **Definition.** The tree bytes of a tree are `page_size ×` the number of its **reachable pages**: the mapped LPIDs reachable from its root, meaning inner pages, leaves and overflow chains. amcheck (A-8) counts them on a quiescent tree.
- **What it is not.** It is not the file length, `slot_count` or the count of pages ever allocated. The file never shrinks (§4.5), so those stay at the post-update peak, about 11 versions per key in W3/W4, and B-3 could never pass on them.
- **The e1-bench figure.** e1-bench's `tree_bytes` is `page_count × page_size`, which counts pages ever allocated (`spikes/e1-arm-b/src/engine.rs:617`). It equals the reachable count only because E0 never frees a page.
- **File length.** B-3 also reports the file length beside it, for information only (§19 Q1).

**What this gives B-3's acceptance (derived, to be measured by B-3).** The acceptance re-runs the W3/W4 shape on the real tree and requires after-GC tree bytes `≤ 1.5×` the pre-update tree bytes (decision block), both measured as defined above. In that shape:
- before the updates and after GC, each key holds exactly one live version of equal size, so the leaf cell bytes are equal;
- no page holds more than `U`, so the pre-update tree has at least `cells/U` leaves;
- I-SPACE caps the after-GC leaves at `cells/(F_PACK·U − C_max)`, plus one per inner page and one per skip.

At 8 KiB pages (`U ≈ 8.1 KiB`) with W3's ~135-byte cells, the bound is ≈ `1/(0.85 − 0.017) ≈ 1.2×`, plus under 1% for the partial last children and the inner pages, plus `skips` pages. That is within 1.5× **whatever the pre-update fill**. A B-3 failure re-opens N3 (decision block).

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
prev_lsn   u64  the start LSN of the record appended immediately before this one (SegmentEnd included); all-ones if none
prev_crc   u32  the crc field of that record; 0 if none
kind       u8   1 Commit | 2 Chunk | 3 CommitMarker | 4 SegmentEnd
txn        (u32 boot id, u64 n)      C-T0 TxnId, n ≥ 1; in SegmentEnd, (the writing boot's id, 0)
commit_ts  u64  0 in Chunk and SegmentEnd
payload
```
Every record carries the boot id of the boot that wrote it (§12.5). TxnId `n` starts at 1 in every boot (C-T0 v8 §2.5), so `SegmentEnd`'s `n = 0` names no transaction.

`prev_lsn` and `prev_crc` are the WAL back-pointer that §12.5 checks (I-WAL-CHAIN). They lie after `crc`, so the record CRC covers them. They name the record appended immediately before this one in the stream, by its start and by its `crc` field: across a segment hop, that is the `SegmentEnd`, or the record that ended exactly at the previous segment's end. **A boot's first record** names the last record its recovery walk accepted (§14), or, if that walk accepted none, the opened meta's `wal_start_prev` and `wal_start_prev_crc` (§2.2). The creating session's first record carries meta 0's, all-ones and 0. They are compared, never read through.
- *Why the CRC as well as the position* (PLAN-v3 §10 item 9, 2026-11-03 addendum). A position does not tell apart two histories that share a record boundary: two boots that append records of equal length from the same point put different records at the same starts. The predecessor's `crc` tells them apart unless the two predecessors are byte-identical (or their CRC32C values collide). SQLite chains its WAL checksums for the same reason.
- *Order of sealing.* A record's `crc` covers its `prev_crc`, so it is final only once its predecessor's is. The writer therefore finishes record CRCs in LSN order. It may checksum the rest of a record earlier and fold `prev_crc` in last (CRC32C is linear). Positions are still reserved at append, and I-ACK-PREFIX is unchanged (§12.4).

**Records never span segments.** A record is placed at the append position only if it ends exactly at its segment's end or leaves room for a `SegmentEnd` after it. Otherwise the writer writes `SegmentEnd` there, and the record starts at the next segment's first LSN. So a segment ends with `SegmentEnd` or with a record that ends exactly at its end, never with a shorter gap. A single record is at most `wal_seg_size / 4`; larger transactions are chunked.

### 12.3 Record kinds and ops
**Kinds:**
- `Commit{ops}`: one small transaction at `commit_ts`;
- `Chunk{seq u32, ops}`: repeated, written before the commit; `commit_ts = 0`;
- `CommitMarker{nchunks u32}` at `commit_ts`: commits the chunked transaction.

**Ops:**
```
CreateTree{tree_id, kind}   DropTree{tree_id}   Ddl{bytes}        # first within a record (opaque Ddl bytes are catalog-owned)
Put{tree_id, key, flags, value, before?}                          # flags: key_changed
Delete{tree_id, key, flags, before?}                              # flags: moved | revert (Chunk only)
```
- **Keys.** `key` is the **user key**, not the entry key. The version entry is formed at apply time with the commit's ts (§9). The WAL therefore carries no LPID, no slot and no N3 detail.
- **Index entries** are logged explicitly and never re-derived.
- **`before`** is N14's optional before-image, written when the catalog enables it for the tree.
- **`revert`** (only in `Chunk` records) means the transaction no longer writes `key`. It cancels the transaction's earlier ops on that key (§12.7, §14).
- **Bytes.** Card A-6a pins the op encoding and fuzzes it.

### 12.4 Group commit, the durable point, and order (I-ACK-PREFIX, I-WAL-TS-ORDER)
- **Append.** A record's LSN is fixed when it is appended: the commit path reserves its position (and, for `Commit` and `CommitMarker`, its ts, below) and moves the **append position** past it. Its bytes are written later, by a writer.
- **Writers.** Batches of appended records are written and synced by one writer thread, or cooperatively by committing threads in embedded and wasm modes (N10). More than one batch may be in flight. The rule below holds however batches are scheduled.
- **The two frontiers.** `written_end` is the largest LSN `L` such that every record byte below `L` has been written (its `write` returned). `durable_end` is the largest `L ≤ written_end` such that every record byte below `L` was written before the start of a `sync_data` on its segment that has completed. Both advance **only over the contiguous prefix**: writing or syncing a batch while an earlier one is unwritten advances neither past the earlier one.
- **I-ACK-PREFIX.** A `synchronous_commit=on` commit is acked only once `durable_end` is at or above the end of its `Commit` record, or of its `CommitMarker` for a chunked transaction, whose chunks lie below it. With `synchronous_commit=off`, it is acked once `written_end` is at or above that end, before the sync (PLAN-v3 §3.3). C-T0 v8's I-ACK (`visible_ts ≥ c`) is a further condition, not a substitute. Checkpoint step 1 waits on the same `durable_end` (§13).
  - *Why.* The reader stops at the first invalid record (§12.5), so a record behind an unwritten one is never replayed. An ack over that hole loses an acked commit in an ordinary crash, with no media fault, and I-REDO-COMPLETE depends on this rule. E4 found exactly this (X-5c-F3, seed 21 r0). Two tasks each wrote and synced their own batch and advanced the frontiers with `max`, so ts 15 was synced and acked while the batch holding ts 14 was unwritten, and the crash lost both.
  - Mutant **ACK-OVER-HOLE**: the frontiers advance by `max` over completed batches. Test `t_ack_prefix` (A-6c): batch 2 is written and synced before batch 1 is written, then a crash; batch 2's `on` commit must not have been acked.
- **Apply waits for the append.** A commit's apply starts only after its records are appended.

**I-WAL-TS-ORDER.** Across `Commit` and `CommitMarker` records, `commit_ts` strictly increases with LSN. The commit path must assign the ts and the append position **in one critical section**. S-2's sequencer must comply; this file imposes the requirement because recovery depends on it. Without it, a lost record could leave a durable later-ts commit behind a missing earlier one, and the recovered state would not be a ts-prefix, which breaks E4's oracle (PLAN-v3 §4 E4). Mutant: TS-AFTER-APPEND.

### 12.5 Stale and torn records (I-WAL-STALE)
A reader walks records from a start LSN. **It stops at the first record that:**
- has a bad magic or CRC;
- has `lsn` ≠ the expected position, which catches a recycled segment's old content;
- has `len` running past the segment;
- has a boot id (§12.2) **older** than the boot id of the last record the walk accepted, or, for the walk's first record, older than the opened meta's `boot_id` (§2.2). Boot ids are compared in serial-number order: `a` is older than `b` iff `(a − b) mod 2^32 ≥ 2^31`.

A record that passes every one of these checks is accepted only if its `prev_lsn` and `prev_crc` also name its predecessor (I-WAL-CHAIN, at the end of this section). A mismatch there is not a stop.

*Why the boot id, and why ordered.*
- **Stale records come from older boots.** Boot ids increase across the boots whose records one walk can reach: each boot's id is newer than the opened meta's `boot_id` and than the boot id of every record its recovery accepted (C-T0 v8 §2.5 with §14 step 8). A boot appends only from the valid end its own recovery found (step 5 truncates there, §14), which lies past every record that recovery accepted. So in the stream as it stands, boot ids never decrease in LSN order. A **stale** record is one that a later boot's truncation superseded: an older boot's record past an earlier valid end, uncovered when post-sync damage reverts the truncation that erased it (§14 step 5). Its magic, CRC and `lsn` may all be intact. It lies at or after the truncation point of a boot newer than its writer. If that newer boot appended up to the stale record's position, the records the walk accepts just before it come from that boot or a later one, so the stale record is older than the record before it, and the walk stops. If the newer boot appended nothing there, the bytes at its truncation point are the ones its own recovery found invalid (a reverted truncation restores exactly them), and the walk stops there anyway.
- **A walk may cross into newer boots.** The walk does not stop at a **newer** boot id. The opened meta need not be the newest meta written: a meta-slot fault can leave an older meta as the newest valid one. Then the walk legitimately runs from that meta's `wal_start_lsn` through its own boot's records, to the valid end at which the next boot's recovery truncated, and on into that boot's records, whose `lsn` values continue the stream. Stopping there would lose that boot's acked commits silently.
- **Bound.** Ordering u32 ids is sound while fewer than 2^31 checkpoints separate any two boots whose records the walk compares (68 years at one checkpoint per second).
- **Residual.** A boot none of whose records any walk reached (its first record torn) can still share its id with a later boot, if a meta is lost after its sync (C-T0 v8 §2.5). If that boot's records past the later boot's truncation point read back again, its stale record is not older than the later boot's records, and this check accepts it. That needs a lost meta together with post-sync WAL damage that reverts the truncation (classes MW and MP in one history). This assumes that §14 step 5's truncation is durable before the boot appends past it, as E4's prototype makes it; §14 does not yet require it (card S-1-F6, proposed). Back-pointers detect such a record as a splice unless its own predecessor is byte-identical, at the same position, to the record the walk accepted before it, or their CRCs collide (I-WAL-CHAIN, below; §19 T4).
- **Mutants.** **BOOT-ID-STOP-SKIP**: the boot-id bullet is removed. **BOOT-ID-EQ-STOP**: the walk stops at any boot id other than the opened meta's.
- **Tests.**
  - `t_stale_boot_record` (A-6b, A-7d): the step-5 truncation write is reverted after its sync, leaving an older boot's complete record at the next expected position after the newer boot's records. The walk must stop there. It fails under BOOT-ID-STOP-SKIP.
  - `t_meta_rollback_crosses_boot` (A-7d, class MP):
    - boot `b−1` writes meta `k` and crashes;
    - boot `b` recovers, truncates at its valid end `p_b`, writes its boot meta `k+1`, then appends and acks `synchronous_commit=on` commits from `p_b`;
    - a post-sync LOST write on meta `k+1`'s slot reverts it, and a crash follows before boot `b`'s next checkpoint;
    - the fixture keeps meta `k`'s WAL span out of recycling (`p_b` lies in the segment holding meta `k`'s `wal_start_lsn`), and keeps boot `b`'s page writes off image `k`'s slots;
    - the open selects meta `k` and must replay boot `b`'s acked commits; the recovered state is at or above the max acked commit.

    It fails under BOOT-ID-EQ-STOP, which stops at `p_b`, passes I-WAL-COVER and returns `Ok` without them.

After `SegmentEnd`, **or after an accepted record that ends exactly at its segment's end** (§12.2), the reader moves on to the next segment's first LSN, **and the valid end is then at least that LSN, even when that segment is short or missing**. The **valid end** is the LSN at which the walk stopped: the start of the first record it did not accept, or, after an accepted `SegmentEnd` or an accepted record that ends exactly at its segment's end, the next segment's first LSN, if the walk stops there. Recovery truncates there, and new appends overwrite from it. I-WAL-COVER compares meta `k`'s `wal_ckpt_end` against this valid end, never against an end-of-file (§14 step 3a).
- *Why the hop at an exact end.* A record that ends exactly at its segment's end leaves no room for a `SegmentEnd`. Without the hop the walk would stop there, step 5 would truncate the whole next segment, and the acked commits in it would be lost with no media fault, an I-REDO-COMPLETE break in class C (E4: 1064 r1, 4770 r2, 5054 r1 and 6263 r2). A flip after them would also give a false I-WAL-COVER XX001. The next segment's first record is still checked by `lsn` and boot-id order, as after `SegmentEnd`.
- Mutants: **SEG-EXACT-NO-HOP**: the reader does not hop after a record that ends exactly at its segment's end. **SEG-TAIL-SHORT**: the writer leaves a tail that is non-empty but shorter than a `SegmentEnd`.
- Test `t_wal_exact_segment_end` (A-6b, A-7d, class C): a record sized to end exactly at its segment's end, then acked `on` commits in the next segment, then a power cut. Recovery replays them; under SEG-EXACT-NO-HOP, step 5 zeroes the next segment and acked commits are lost. Variant: a flip after them; under the mutant it gives a false I-WAL-COVER XX001. Variant: a tail one byte shorter than `SegmentEnd`, under SEG-TAIL-SHORT.

A segment file shorter than the position the reader needs, or missing, because its creation or preallocation was cut short (for example by 53100), ends the walk at the start of the record that runs past its end-of-file, or, if it holds no further whole record, at the position the walk had reached in it. That position, never the end-of-file itself, is the valid end: for a read-write and a read-only open alike it is a stop, never 58030, since **a writer appends to a segment only after its creation or recycling rename (§12.1) and its preallocated length are durable**, not merely returned: the segment `sync_all`'d and its `wal/` directory entry synced through `Env` (or the platform's equivalent), so no record in it is acked before then and nothing acked lies past that point. Mutant **SEG-ACK-BEFORE-DURABLE**: a created or renamed segment is appended to, `sync_data`'d and acked before its directory entry and length are durable; a power cut loses the create, rename or length, and recovery stops cleanly short of an acked commit. *Why the record's start, not the end-of-file:* an end-of-file that lies past the start of a partial record would let the coverage check (§14 step 3a) pass while a record below `wal_ckpt_end` was never replayed. That is unreachable under the writer rule without media faults, but the rule must not depend on it. Mutant **COVER-AT-EOF**: the coverage check compares against the end-of-file of a short segment (test: `t_wal_loss_below_ckpt_end`, short-segment variant, §15 I-WAL-COVER). Mutant: LSN-CHECK-SKIP. With the `lsn` check removed, a recycled segment's old records, whose magic and CRC are intact, are replayed as if they were new.

**Splices (I-WAL-CHAIN).** A record that passes every stop check above (magic and CRC, `lsn`, `len`, and the ordered boot id) must also have a `prev_lsn` and a `prev_crc` (§12.2) equal to the start and the `crc` field of the record the walk accepted just before it. Across a hop, that is the `SegmentEnd`, or the record that ended exactly at its segment's end. For the walk's first record, they must equal the opened meta's `wal_start_prev` and `wal_start_prev_crc` (§2.2). Both are compared, and the chain is checked last, after every stop check. A record that passes the stop checks but fails this one, in either field, is a **splice**, never an end of the WAL. The walk ends there, the position is not a valid end, and recovery treats it as detected damage (§14 step 3a). Every stop check keeps its meaning: a record that fails one of them ends the walk cleanly at a valid end, as before, whatever its `prev_lsn`.
- *Why a mismatch is damage, not a crash.* Without post-sync damage, every record the walk accepts is one its writer appended at that position, right after the record the walk accepted before it:
  - an ordinary crash loses or tears only bytes that no completed sync covered, and the walk stops at the first of them (above), so it never compares a record behind a loss;
  - a boot's first record names the record its own recovery accepted last, or the opened meta's `wal_start_prev` and `wal_start_prev_crc` if it accepted none (§12.2). A later walk that reaches that record has accepted the same records before it, byte for byte, whether it runs from the same meta or from an older one (above): it passes no lost or torn byte (previous bullet), and any record a later boot wrote over them is stopped by the boot-id order (next bullet). At every meta, `wal_start_lsn` is a record start of that meta's own boot or its append position (§12.6), and the record there was written, or will be, with `prev_lsn = wal_start_prev` and `prev_crc = wal_start_prev_crc`;
  - a stale record of an older boot is stopped by the boot-id order before this check. That needs boot ids that increase across the boots one walk can reach (*Why the boot id*, above; §14 step 8). It holds outside the Residual, where a mismatch is a true detection.

  So a record that passes every stop check but names another predecessor (another start, or another `crc`) shows that the bytes before it are not the ones its writer appended after: a synced write was lost, reverted or misdirected, or a region came back from another history, and the WAL went on past the damage.
- *Unlike PostgreSQL,* which ends redo at a record whose `xl_prev` is wrong, the store fails closed. Here the stop checks alone define the end, so a record behind a splice is positive evidence that the WAL went on past it, and detected damage is never silent (PLAN-v3 §10 items 8 and 9).
- *What it does not catch:*
  - a lost tail: nothing follows to disagree;
  - damage the walk stops at: the records behind a stop are never read, so a stop stays a clean end. A forward scan past it would need a durable-end witness per batch, because after an ordinary crash, unsynced later batches can persist past a torn earlier one (§19 T4);
  - a spliced-in record whose own predecessor is byte-identical, at the same position, to the record the walk accepted before it: for example, byte-identical records from two boots that share an id and both appended from the same valid end (the Residual, above). Then the two histories agree up to the spliced-in record. A `crc` collision between differing predecessors at the same position also escapes, with probability about 2^-32 per splice. A position alone would also miss records of equal length that differ in content, which `prev_crc` catches (`t_wal_splice`, variant *same-start*).

  It adds no recovery.
- *Cost.* 12 bytes per record, 12 per meta, and two comparisons per record; and record CRCs are finished in LSN order (§12.2).
- *Status.* The E4 prototype (`spikes/e4-recovery`) omits back-pointers, so E4's oracle and its ruling X5-R33 stand as written. A prototype that adds them must widen X5-R33's explained-`Err` rule to accept a splice `Err` whose fault lies anywhere in the walked span.
- Mutants:
  - **PREV-CHECK-SKIP**: the check is removed;
  - **PREV-CRC-UNCHECKED**: the check compares `prev_lsn` only, a position-only chain;
  - **PREV-MISMATCH-AS-END**: a splice ends the walk cleanly, as a stop, and recovery truncates there;
  - **PREV-FIRST-WRONG**: a boot's first record carries its own LSN or all-ones instead of its recovery's last accepted record, or a `prev_crc` other than that record's `crc`;
  - **PREV-HOP-WRONG**: a segment's first record names something other than the `SegmentEnd` or exact-end record before it;
  - **WAL-START-PREV-WRONG**: `wal_start_prev` is the append position's predecessor even when an open chunked transaction holds `wal_start_lsn` back (§12.6);
  - **WAL-START-PREV-CRC-WRONG**: `wal_start_prev_crc` is not the `crc` of the record `wal_start_prev` names (for example, the `crc` of the record at `wal_start_lsn` itself, or of the append position's predecessor when `wal_start_lsn` is held back).
- Tests (A-6b, A-7d):
  - `t_wal_splice`: a synced region in the middle of the replay span is overwritten through `Env` with records that pass magic, CRC, `lsn` and boot-id order, and that end at the start of an intact later record whose `prev_lsn` names a different predecessor. The open must fail with XX001 and leave every file byte-identical. There are two placements: the splice below `wal_ckpt_end`, and at or above it. PREV-CHECK-SKIP replays the spliced records. PREV-MISMATCH-AS-END opens and drops the intact acked commits behind the splice. Variant *read-only*: the open serves salvage cut before the splice (§14.1), and SPLICE-SALVAGE-PAST-CUT fails it. Variant *same-id residual* (classes MW and MP): the fixture of the Residual, with the stale record's predecessor starting at a different LSN from the walk's. It must give XX001; PREV-CHECK-SKIP accepts it. Variant *same-start* (the case a position-only chain misses): the overwriting records have the same lengths as the records they replace but differ in content, so the intact record behind them names its predecessor's start correctly but not its `crc`; and the Residual's fixture with the two same-id boots appending records of equal length, differing in content, from the same valid end, so the stale record's predecessor starts where the walk's does. Both must give XX001; PREV-CRC-UNCHECKED accepts both.
  - `t_wal_chain_crash` (false-positive guard): ordinary power cuts, with no media fault, at every step around:
    - a `SegmentEnd` hop and an exact-end hop;
    - a boot's truncation and its first append, both when its walk accepted records and when it accepted none;
    - a flip while an open chunked transaction holds `wal_start_lsn` back;
    - a process crash, with no power cut, then a power cut after the next boot's first ack;
    - the walks of `t_meta_rollback_crosses_boot` and of both variants of `t_boot_id_after_rollback`.

    None may give a splice error. PREV-FIRST-WRONG, PREV-HOP-WRONG, WAL-START-PREV-WRONG and WAL-START-PREV-CRC-WRONG fail it.

### 12.6 Replay start and recycling
At the flip of checkpoint `k`, `wal_start_lsn` is the minimum of:
- the LSN of the first `Commit` or `CommitMarker` with `commit_ts > T`, or the append position if there is none;
- the first `Chunk` LSN of every chunked transaction whose marker **is not yet below `T`** at the flip. That is, it has written chunks, and either it has no `CommitMarker` yet, or its marker has `commit_ts > T`.

**Why the marked case counts.** A chunked transaction `c` can have its chunks at LSN 100–200 and its marker at LSN 400, with a small commit `≤ T` at LSN 300 between them. If `c`'s apply had not finished at the flip, then `T < c`. The first term alone gives 400, so pass 2 (§14) would never see the chunks, and step 6 could recycle them. `c` would be lost although acked. Counting `c`'s first chunk puts the start at 100.

A transaction that aborted before the flip does not count, and neither does one whose marker has `commit_ts ≤ T`, which is wholly in the image. **Nor does a transaction that committed with no ts** before the flip (C-T0 v8 `Released(⊥)`, §3.1 step 0: a large transaction whose `ROLLBACK TO` left it no data). It writes no marker, ever, so recovery ignores its chunks exactly as it ignores an aborted transaction's, and nothing durable depends on them. After a crash, an open transaction's chunks are ignored, because no marker follows them. **A segment wholly below the durable meta's `wal_start_lsn` may be recycled** (§13 step 6). This is PLAN-v3's chunk rule ("segments holding chunks of a txn whose marker isn't yet below T are never recycled"), stated as a replay-start position.

### 12.7 Large transactions
A transaction is chunked **iff** it is large (C-T0 v8 §2.1). Its `Chunk` records are written before commit, as it materialises its writes (C-T0 v8 §3.2), and the commit is the `CommitMarker`, written in ts order (I-WAL-TS-ORDER).
- **Supersede.** Within one transaction's chunks, a later op on the same `(tree_id, key)` **supersedes** the earlier one. That happens on a rewrite of the key and on `ROLLBACK TO`. A `Delete{revert}` cancels the key altogether. Replay applies the last op per key (§14). C-T0 v8 I-PROV-WAL keeps every provisional entry equal to its key's last op.
- **No apply at commit.** A large transaction's data becomes readable at its flip, through its provisional entries, and its finalize converts them into ordinary entries. Until then the apply frontier stays below its commit ts (C-T0 v8 §3.5), so a crash replays it from these chunks.

### 12.8 Bulk loads (the rule PLAN-v3 §3.3 asks S-1 to pick)
**S-1 picks: force a checkpoint before the result is published. Page images are not WAL-logged.**
1. **Unlogged writes.** A bulk load (the sorted bulk load of an online index build, C-T6 §7) may write entries **without WAL records**, only into a tree that is **not yet published**: no reader may use it. The tree itself is created by a logged `CreateTree`. Concurrent logged writes into the tree, such as C-T6's write-only maintenance, stay logged as usual.
2. **The forced checkpoint.** The loaded entries become durable only through a checkpoint whose flip follows the load's last unlogged write.
3. **Publication.** The publishing commit (C-T6 write-only → public) may be issued only after that checkpoint's meta is durable (§13 step 5). Catch-up from the WAL (PLAN-v3 §3.5 step 3) is ordinary logged writing.
4. **Crash before publication.** The recovered tree holds only its logged content plus whatever the last image caught of the load. S-3 must treat such a tree as an unfinished build: restart or discard it.

**Pages (I-LINK-ATOMIC).** §13's Exception exempts a load's *entries* from I-CKPT-AT-T, not its pages from I-LINK-ATOMIC (§5.6). Each load step links what it maps, so a discarded build (item 4) can be walked (§6.8) and leaks nothing.

Mutant: PUBLISH-BEFORE-META.

**Why not page images.** Under (b), a load writes versions at a registered snapshot ts `S`. Such versions may be `≤ T` of a later checkpoint, so a replay filtered by `commit_ts > T` (§14) could never place a logged copy of them. Page images would also carry LPIDs, which are meaningless to logical consumers (N14's replicas and CDC).

**TRUNCATE is not an unlogged load.** PLAN-v3 §3.3 lists TRUNCATE with bulk loads, but nothing in it bypasses the WAL, so it needs no forced checkpoint. It runs as three logged steps:
1. a `CreateTree` of a new, empty tree;
2. the catalog swap (`Ddl`) to the new tree, in the same commit;
3. a `DropTree` of the old tree, run when `W` passes the DDL (§6.8).

**Consequence, owned by D-6.** Unlogged loads are invisible to the WAL stream. Backup and PITR must therefore treat the forced checkpoint's image as a base-image boundary, and replicas must receive that image or rebuild the index (§19 Q5).

---

## 13. Checkpoint (the step order is binding)

At most one checkpoint runs at a time. Its triggers (WAL bytes, dirty volume, time, or a forced checkpoint for a bulk load) are configuration; correctness does not depend on them.

**Step 0 (flip).**
1. Take the epoch gate exclusively. No gated operation is now in flight (§5.6). This generalises PLAN-v3's "pause commit-apply" to every multi-page change.
2. Take `T` := the apply frontier, record `gc_w := gc_w_used` (§10.5) and the `wal_start_lsn` (§12.6) with its `wal_start_prev` and `wal_start_prev_crc` (§2.2). Inside the commit path's I-WAL-TS-ORDER critical section (§12.4), read `wal_ckpt_end` := the append position and `ts_ckpt_hi` := the commit_ts of the last `Commit` or `CommitMarker` appended, or the frontier this boot started from if none was. The two values are read together, in one entry to that critical section, so they describe the same point of the stream. `Chunk` reservations need not be made inside the critical section: a chunk appended concurrently with the read may fall either side of `wal_ckpt_end`, and its provisional entry is dead after recovery (C-T0 v8 §2.4). Every `Commit` and `CommitMarker` reservation is inside it, and that is all the bound below needs. Record the boot id and set `e := k+1`.

   Every ordinary entry in image `k` was inserted by an apply that started before the flip and after its record was appended, so its record ends at or below `wal_ckpt_end` and its ts is at most `ts_ckpt_hi`. A record appended after the read has no entry in image `k`: its apply inserts are gated operations, and the gate is held exclusively. The unlogged entries of a §12.8 bulk load are not apply inserts. Their ts is the load ts, which is the fence commit's ts `f` (C-T6-rehost §9.1 step 4.2), and `f`'s `Commit` record was appended before the load began, so before the flip: it too ends at or below `wal_ckpt_end`, and `f ≤ ts_ckpt_hi`.
3. Release the gate.

**Step 1 (WAL).** `sync_data` the WAL through `wal_ckpt_end`: the step ends only when `durable_end ≥ wal_ckpt_end` (I-ACK-PREFIX, §12.4), so every byte below it is written and synced, not just the last batch's. This is stricter than PLAN-v3's "through the last record with commit_ts ≤ T", and the reason is out-of-order apply:
- applies run concurrently and finish out of ts order (`e1-mvcc/src/common/commit.rs:35–63`: apply, then `mark_applied` in ts order);
- so the image may hold some entries of a commit `c > T` that started applying before the flip;
- with `synchronous_commit=off`, `c`'s record may not yet be synced;
- syncing through `wal_ckpt_end` guarantees `c`'s record is durable before the meta, so recovery replays `c` wholly (I-CKPT-AT-T, I-REPLAY-IDEM). Without it, a crash could expose part of `c` with no record to complete it (mutant WAL-SYNC-T-ONLY).

Mutant **CKPT-SYNC-OVER-HOLE**: step 1 ends when the latest batch's sync completes while an earlier batch is unwritten. Test `t_ckpt_sync_prefix` (A-7c): a flip with batch 1 unwritten and batch 2 synced, then a crash after step 5. The test needs no media fault: under the mutant, a class C open fails I-WAL-COVER with XX001.

**Step 2 (image).** The checkpointer writes image `k`, each page to a fresh slot (§4.4), recording each write in the epoch-`k` map:
- every page whose epoch-`k` content is not already at a slot the image map references: the dirty frames with content epoch `≤ k`, and the epoch copies (§5.6). Pages that eviction already wrote during epoch `k`, or wrote after the flip as epoch-`k` content (§5.2), are adopted as they are;
- when a written frame was not mutated in epoch `k+1`, its new slot also becomes its current home;
- then the epoch-`k` MapLeaf and MapInner pages that changed since image `k−1`, written copy-on-write up to the root;
- then the free set **last**: allocate the slots for the FreeSet pages first (growing the file first if needed), then serialise both bitmaps of image `k`, with those slots marked used (I-FREE-EXACT).
  - **Sizing.** The bitmap size depends only on `slot_count` and `next_lpid`. If allocating the FreeSet slots grows the file, `slot_count` changes, so the checkpointer recomputes the number of FreeSet pages after the growth and allocates the difference.
  - **Convergence: the condition.** Say the original allocation needs `n` FreeSet pages but finds only `f < n` free slots.
    - The growth adds `g ≥ 16` slots (§4.5: 1 MiB at the largest page size).
    - The rest of the original allocation, `r = n − f` pages, lands in those new slots.
    - Covering the `g` new slots needs `extra` more pages: at most `⌈g / (8·(page_size − 24))⌉` FreeSetLeaf pages plus one page per radix level. These must land in the new slots too.

    One growth is therefore enough exactly when **`r + extra ≤ g`**. `extra < g` alone is not enough.
  - **Convergence: why it holds.** `r ≤ n`, the whole FreeSet:
    - while the file is at most 16 MiB, `g` is at its 1 MiB floor and the FreeSet is a handful of pages;
    - above that, `g = file_len / 16` slots, while the FreeSet is about `(slot_count + next_lpid) / (8·(page_size − 24))` pages, under 1/30,000 of that sum even at 4 KiB, the smallest page size.

    So `n + extra ≤ g` unless `next_lpid` exceeds `slot_count` by orders of magnitude.
  - **Convergence: the check.** The checkpointer still checks `r + extra ≤ g` after the growth. If the check fails, it grows again and repeats the check. Each further growth places at least `g − extra > 0` of the outstanding pages, so the repetition ends.
  - Growth does not change `next_lpid`, because FreeSet pages are physically addressed.

Pages created or mutated only after the flip are not part of the image.

**Step 3.** `sync_all` the page file.

**Step 4.** Write meta `k` (`ckpt_id = k`, `T`, `gc_w`, epoch, the roots, `next_lpid`, `slot_count`, `wal_start_lsn`, `wal_start_prev`, `wal_start_prev_crc`, `wal_ckpt_end`, `ts_ckpt_hi`, `boot_id`, CRC) to slot `1 + (k mod 2)`. Checked at meta write: `T ≤ ts_ckpt_hi` and `wal_start_lsn ≤ wal_ckpt_end ≤ durable_end`. A violation is fail-stop (§9.2), as for `gc_w ≤ T` (§10.5).

**Step 5.** `sync_all`. Meta `k` is now the durable image.

**Step 6.** Only now:
- move the slots referenced by image `k−1` and not by image `k` from pending to free;
- recycle the WAL segments wholly below `wal_start_lsn`.

**I-META-ATOMIC.** A recovered meta never points at an unwritten or unsynced page: step 3 precedes step 4. Mutant: META-BEFORE-SYNC.

**I-CKPT-AT-T.** Image `k` is a consistent cut: every gated operation is wholly before or wholly after the flip. In image `k`:
- **(a)** every commit with ts `≤ T` is wholly present, up to GC `≤ gc_w`. For every snapshot `S` with `gc_w ≤ S ≤ T`, reads on the image equal reads on the committed state at `T`;
- **(b)** every ordinary entry with ts `> T` belongs to a commit `c` with `T < c ≤ ts_ckpt_hi` whose records end at or below `wal_ckpt_end` and are durable when meta `k` is written (step 1). Recovery replays `c` wholly **if those records still read back**; if they do not, I-WAL-COVER refuses to open the image as it is (§14 step 3a). An unlogged bulk-load entry (the Exception below) with ts `f > T` has its fence commit `f`'s record below `wal_ckpt_end` in the same way, and a recovered frontier below `f` fails the same check. No open therefore holds an entry above its recovered frontier;
- **(c)** every ordinary entry belongs to a committed transaction (N2). An image may hold **provisional entries** (§7.3 tags 6–8) of any transaction, committed or not. After recovery every one of them is dead (C-T0 v8 §2.4: its `txn.boot` is not the new boot id), so none is ever read as data. A committed transaction's data is in the image as ordinary entries if its commit ts is `≤ T` (`T` is the apply frontier, C-T0 v8's `M`), and otherwise it is replayed from its durable chunks (§12.6, §14). Mutant CKPT-T-VISIBLE (`T` taken as `visible_ts`).

**Exception.** The unlogged entries of a §12.8 bulk load in a tree that is **not yet published** are exempt from (a)–(c):
- they come from no commit;
- an image may hold any part of them;
- no reader can use the tree until after a forced checkpoint makes them whole (I-BULK-PUBLISH).

(a)'s read equality is over published trees.

Mutants: FLIP-NO-GATE (a split or pack straddles the flip), WAL-SYNC-T-ONLY.

**I-FREE-SAFE** (§4.4). Mutant: FREE-EARLY (step 6's frees moved before step 5).

---

## 14. Recovery

1. Read page 0 and both meta slots. The newest valid meta `k` wins (§2.2). A meta whose magic and CRC are valid but whose `T > ts_ckpt_hi` or `wal_start_lsn > wal_ckpt_end` is corrupt: the open fails with XX001, with no fallback (§2.2). Mutant **META-RANGE-SKIP**: this open-time check is removed (test `t_cover_ts_cross_check`, §15 I-WAL-COVER).
2. Load the map root and the free set of image `k`. Every slot not referenced by image `k` is free, including the slots eviction wrote after meta `k`.
3. **Pass 1:** walk the WAL from `wal_start_lsn` to the valid end (§12.5), collecting the set of `(txn, commit_ts)` of every `CommitMarker` with `commit_ts > T`. It checks each record for a splice as well (§12.5, I-WAL-CHAIN), and ends at the first one.

3a. **Coverage check (I-WAL-COVER).** Pass 1 also records the largest `commit_ts` of the `Commit` and `CommitMarker` records it reads. The WAL **covers** image `k` iff:
   - the valid end (§12.5: the LSN at which the walk stopped, never an end-of-file) is at or above meta `k`'s `wal_ckpt_end`; and
   - `max(T, that largest commit_ts) ≥ ts_ckpt_hi`.

   If it does not, there is **WAL loss**: bytes that step 1 of checkpoint `k` synced before meta `k` was written no longer read back, and image `k` may hold entries of the commits they recorded (I-CKPT-AT-T (b)). The open then fails with **XX001** (detail: WAL loss, with the valid end, `wal_ckpt_end`, `T` and `ts_ckpt_hi`) **before pass 2, and writes nothing**: no replay mutation, no eviction write and no truncation (step 5). The files stay exactly as found, including any intact records behind the damage. A read-only open serves the salvage state instead (§14.1).
   - *Why it is sound.* Suppose image `k` held an entry with ts `c` above the recovered frontier.
     - An ordinary entry's commit record lies below `wal_ckpt_end` (§13 step 0) and at or above `wal_start_lsn` (§12.6, I-WAL-TS-ORDER), and it was not replayed. So the walk stopped below it, and the first condition fails.
     - An unlogged bulk-load entry (§12.8) has ts `c = f`, its fence commit's ts. `f`'s `Commit` record was appended before the load, so before the flip, and so it ends at or below `wal_ckpt_end`. Since `f >` the frontier `≥ T`, the record lies at or above `wal_start_lsn` (§12.6), and it was not replayed. The first condition fails the same way.

     So no store opens holding an entry above its recovered frontier, and C-T0 v8's sequencer restart at frontier + 1 (C-T0 v8 §3.7) never reissues the ts of an entry in the trees (I-ENTRY-UNIQUE). The check has no false negatives for that hazard, but it is not exact (below). The second condition checks the same floor directly from the meta. In a correct store it never fails alone; it turns a breach of §12.4's "apply waits for the append" or of I-WAL-TS-ORDER into an XX001 here instead of a reissued ts.
   - *What it also refuses.* It fails on any loss of bytes below `wal_ckpt_end`, whether or not the image depends on them:
     - records of commits that were appended but not yet applied at the flip, which are synced and may be acked;
     - chunks of a chunked transaction that committed after the flip, whose chunks lie below `wal_ckpt_end` and whose marker lies above it;
     - bytes that held **no committed data at all**: chunks of a transaction that never committed, a `SegmentEnd`, or records `≤ T` with nothing above `T` behind them.

     This is deliberate. The check compares positions, so once the bytes are gone it cannot tell what they held, and detected damage below `wal_ckpt_end` is never silent.
   - *Why never open the image as it is.* The image would serve part of a commit whose record is gone, above the frontier, so the state would be neither atomic nor a ts-prefix, and the sequencer would then reissue that commit's ts. E4 found this in 451 class MW rounds (X-5c-F3, MW-ORPHAN).
   - Mutants:
     - **COVER-CHECK-SKIP**: the check is removed;
     - **COVER-END-AT-T**: `wal_ckpt_end` is taken as the end of the last record with commit_ts `≤ T`, WAL-SYNC-T-ONLY's position;
     - **COVER-AFTER-TRUNCATE**: the check runs after step 5 or pass 2;
     - **TS-HI-CHECK-SKIP**: the second condition is removed;
     - **COVER-AT-EOF**: the first condition compares against a short segment's end-of-file (§12.5).

     Tests: `t_wal_loss_below_ckpt_end` and `t_cover_ts_cross_check` (A-7d; §15 I-WAL-COVER).

   **A splice** (§12.5, I-WAL-CHAIN) found by pass 1 fails a read-write open in the same way, whether or not the coverage check passes. The open fails with **XX001** (detail: WAL splice, with the record's LSN, its `prev_lsn` and `prev_crc`, and the walk's predecessor's start and `crc`, plus the coverage result) before pass 2, and writes nothing. That holds wherever the splice lies, at or above `wal_ckpt_end` included. A read-only open serves salvage instead (§14.1), with the valid end cut back to the **splice cut**. The cut is the start of the first accepted record that ends after `m = min(the walk's predecessor's start, prev_lsn)`, or the walk's start if no accepted record ends after `m`. When `prev_lsn` equals the walk's predecessor's start and only `prev_crc` differs, `m` is that start, and the cut is the predecessor itself. Every record from that point on is suspect, because one of the two histories was overwritten there. If the splice record is genuine, the walk's history differs from the record covering `prev_lsn` on. If it is foreign, it differs from the splice on. The cut keeps neither.
4. **Pass 2:** walk the same span in LSN order, and apply:
   - every `Commit` with `commit_ts > T`;
   - every `Chunk` of a transaction in pass 1's set, at that set's `commit_ts`.

   A chunk whose transaction has no marker is ignored. Records with `commit_ts ≤ T` are skipped: they are in the image.

   **DropTree.** A replayed `DropTree` sets `dropping` and does nothing else (nothing if the id is absent). Its walk runs at step 6. Pass 2 runs no walk. Mutant **REPLAY-DROP-WALK**: pass 2 walks, so a read-only open frees a page (§14.1; test `t_ro_open` with a replayed `DropTree`, A-7d).

   **Chunk supersede (I-CHUNK-SUPERSEDE).** For a chunked transaction, each op inserts or replaces the entry `key@commit_ts`, and a `Delete{revert}` deletes it if present. A replace or delete frees the replaced value's overflow chain in the same gated operation (§6.7). So the last op per key wins, whatever the image already holds for that key at that ts. An op that inserts an entry identical to the present one is a no-op. Ops of a `Commit` record keep I-ENTRY-UNIQUE's XX001 for a different value. Mutant SUPERSEDE-AS-CONFLICT: a superseding op is treated as a conflict (XX001).
5. Truncate the WAL at the valid end.
6. Resume the walks of every `dropping` directory entry (§6.8), as a B-6 boot job.
7. Optionally run `amcheck` (A-8).
8. Run the **boot checkpoint** (§13). It runs every step 0–6 and **always writes a new meta** (`ckpt_id` one above the recovered meta's), even when no page is dirty. No transaction starts until its step 5 has made that meta durable. C-T0 v8's **boot id** (C-T0 v8 §2.5) comes from it, and is not the checkpoint epoch `e` (one higher after the flip). Its boot id is the newest, in serial-number order (§12.5), of the low 32 bits of its `ckpt_id`, one above the opened meta's `boot_id`, and one above the newest boot id of any record the recovery walk accepted. After a meta rollback (§2.2, X5-R5) the walk can cross into a boot whose meta was lost, and whose `ckpt_id` this boot checkpoint would otherwise repeat. A boot whose id was raised that way can be followed by one whose `ckpt_id` is below that id, which the opened meta's `boot_id` bounds. Meta `k+1`'s `boot_id` records it. Mutant **BOOT-ID-CKPT-ONLY**: the boot id is the low 32 bits of `ckpt_id` regardless. Test `t_boot_id_after_rollback` (A-7d, class MP): two successive rollbacks to meta `k`, across boots `b` and `b+1`; boot `b` leaves an unmarked chunked TxnId `(id, 1)`, and boot `b+1` commits a chunked transaction. Recovery from `k` must never attach `b`'s chunks to `b+1`'s marker; BOOT-ID-CKPT-ONLY does. Mutant **BOOT-ID-NO-META-FLOOR**: the opened meta's `boot_id` term is omitted. Variant of `t_boot_id_after_rollback` (A-7d, class MP), *raised id, then no records*, with `t_meta_rollback_crosses_boot`'s fixture constraints (§12.5): boot `b−1` writes meta `k` to one slot and crashes; boot `b` recovers from `k` (id `k+1`), writes meta `k+1` to the other slot, appends and acks, and that meta write is LOST before a crash; boot `b+1` recovers from `k`, its walk crosses `b`'s records (id `k+2`), and its meta `k+1` is LOST again before a crash; boot `b+2` recovers from `k`, its walk crosses `b`'s and `b+1`'s records (id `k+3`), its meta `k+1` (`boot_id` `k+3`, `wal_start_lsn` the append position `p`) is durable, and it crashes before any record of its own reads back; boot `b+3` recovers from meta `k+1`, its walk from `p` accepts nothing, it writes meta `k+2`, appends and acks `synchronous_commit=on` commits from `p`, and crashes before its next checkpoint; a LOST write on meta `k+2` leaves its slot holding meta `k`. The open selects meta `k+1` and must replay boot `b+3`'s acked commits. BOOT-ID-NO-META-FLOOR gives boot `b+3` the id `k+2`, older than meta `k+1`'s `boot_id`, so the walk stops at `p`, passes I-WAL-COVER and returns `Ok` without them. Chunks of the previous boot that have no marker belong to transactions that ended without one (aborted, cut off by the crash, or committed with no ts), all ended before this flip, so they do not hold back `wal_start_lsn` (§12.6). A **read-only open** (N11) skips this step and has no boot id; every provisional entry is dead to it (§14.1).

Afterwards, the apply frontier is the largest replayed `commit_ts`, or `T` if nothing was replayed. By I-WAL-COVER it is at least `ts_ckpt_hi`, so it is at least the ts of every entry in the trees, bulk-load entries included. C-T0 v8 restarts its sequencer and watermark from that value and from `gc_w` (C-T0 v8 §3.7). Provisional entries found in the image are dead and are ignored by every read (§8).

**Post-sync damage.** I-REDO-COMPLETE assumes the device keeps what a completed sync covered. When it does not, recovery's outcome depends on where the damaged bytes lie:
- **Below `wal_start_lsn`:** never read; the image holds what they recorded.
- **In `[wal_start_lsn, wal_ckpt_end)`:** detected by I-WAL-COVER (step 3a).
- **At or above `wal_ckpt_end`:** the valid end stops at the damage. The state is the ts-prefix up to it (I-WAL-TS-ORDER), and no image entry depends on what is lost. But synced, possibly acked, commits past it are lost **without detection**, unless the damaged bytes still pass every stop check and an intact record behind them names a different predecessor (a splice, below), and step 5 truncates any intact records behind it. A single-copy WAL keeps no durable record of how far it was synced past the last meta (§19 T4).
- **Bytes that still pass every stop check but are not what was appended** (a splice), anywhere in the walked span: detected by I-WAL-CHAIN when an intact later record names a different predecessor (§12.5). A read-write open fails with XX001, and a read-only open serves salvage cut before it (step 3a). Not detected when no record follows (a lost tail), or when the predecessor the intact record names is byte-identical, at the same position, to the one the walk accepted, or their CRCs collide (§12.5).
- **An older boot's record left at a valid position:** stopped by the ordered boot-id check (§12.5); a stale record that shares the later boot's id (the Residual, §12.5) is detected as a splice unless its predecessor is byte-identical at the same position too.

**I-REPLAY-IDEM.** Replay is idempotent, so recovering twice, including a crash during recovery, gives the same state. Under (b), every op inserts a uniquely keyed entry (I-ENTRY-UNIQUE). Re-inserting an identical entry is a no-op, so partial applies already in the image (I-CKPT-AT-T (b)) are completed rather than duplicated. A chunked transaction's superseded ops replace one another in LSN order, so each pass ends with the last op per key (I-CHUNK-SUPERSEDE). CreateTree and DropTree are idempotent (§6.8). The final state is the set union of the entries, independent of insertion order. Pass 2 still runs in LSN order, so tree creation and drop ops come before and after the writes that depend on them. Mutants (one per direction of I-ENTRY-UNIQUE):
- **REPLAY-DUP-CONFLICT:** an identical re-insert is treated as a conflict (XX001);
- **DUP-DIFFERENT-ACCEPTED:** a re-insert with a different value is accepted (overwritten or ignored) instead of XX001.

The double-crash test (A-7d) is the test that runs recovery twice. It is not a mutant.

**I-REDO-COMPLETE.** Every acked `synchronous_commit=on` commit is either `≤ T` of the recovered image, and in it, or has durable records at or after `wal_start_lsn` before the valid end, and is replayed, given I-ACK-PREFIX (§12.4) and a device that keeps what a completed sync covered (above). This holds for a walk from an older meta after a meta-slot fault too, because the walk crosses into newer boots' records (§12.5), when that meta's image and WAL span are intact; otherwise X5-R5's detection (XX001 on the older image's reused slots, §3.3), or I-WAL-COVER after recycling, applies. Mutants:
- **START-LSN-PAST-CHUNK:** the replay start ignores an open transaction's first chunk;
- **START-LSN-MARKED-CHUNK:** the replay start counts only transactions with no marker. Its test is §12.6's case: a chunked transaction whose marker (`commit_ts > T`) lies after a commit `≤ T`, with its apply unfinished at the flip, then a crash after step 6. The transaction must be recovered whole.

**E4's oracle.** The recovered state equals the model at some commit `c`, with max acked sync-on commit `≤ c ≤` last WAL-written commit (PLAN-v3 §4 E4). That follows from I-REDO-COMPLETE, I-CKPT-AT-T, I-REPLAY-IDEM and I-WAL-TS-ORDER: the durable commits form a ts-prefix.

### 14.1 Read-only open (N11)
**Every build offers a read-only open**, in server, embedded and browser modes (PLAN-v3 §10 item 8): it is the only path to the data after WAL loss (below), and the export path of a stopped store (C4 decision 5). N11's single-owner rule is unchanged. A read-only open recovers exactly the state a read-write open recovers, but keeps it in memory and writes nothing, except under WAL loss or a splice (below), where a read-write open recovers none.

**A stopped store opens read-only with zero free bytes (C4 decision 5).** A store that stopped on 53100 (a full volume or an OPFS quota) is, on disk, a crashed store, because poisoning lives only in memory (§9.2). Whatever the write point, a WAL append, a segment's creation or preallocation, a file extension (§4.5), a checkpoint page write or the meta write, the cut-short write lies past the valid end (§12.5), in a slot or extension that image `k` does not reference (§14 step 2), or in the meta slot that loses (§2.2). So a read-only open of it **succeeds on a volume with zero free bytes** and writes nothing, provided its pool meets the evictable minimum for the replay (below). The data stays readable and exportable without freeing any space. Test `t_ro_open_disk_full` (A-7d, §15 I-RO-OPEN).

**No writer.** While a read-only open is open, no read-write open of the file runs: it takes N11's file lock (through `Env`) in a mode that excludes a read-write open, and it fails rather than wait when a writer holds the lock. **The lock creates and writes no file**, in either mode: it is an advisory lock on the page file itself (opened read-only by a read-only open), or, on OPFS, the access-handle mode (`read-only` against a writer's exclusive `readwrite`). A lock file would be a write, and on a full volume its creation fails (mutant **RO-LOCK-FILE**). <!-- For A-1: an O_RDONLY fd cannot take a POSIX fcntl write lock, so the read-only open needs a shared (read) lock mode; PLAN-v3 §3.2's Env has only lock_exclusive (follow-up proposed in verdicts/S-2-F2.md). --> Why exclude a writer: a writer recycles WAL segments and reuses slots after every checkpoint (§13 step 6), so the reader's image and WAL span would change under it. Whether several read-only opens may share the file is the lock mode's choice; nothing here depends on it, because none of them writes.

**What it replays.** Steps 1–4, including the open-time meta check (step 1) and the coverage check (step 3a): the same winning meta (§2.2), the same image, the same valid end (§12.5), or the splice cut (§14 step 3a), the same two passes, and the same ops, with I-CHUNK-SUPERSEDE.
- So its state equals the state a read-write open of the same files has after step 4, before its boot checkpoint.
- The apply frontier is fixed as after step 4 (the largest replayed `commit_ts`, or `T`), and `W` starts at the meta's `gc_w` (C-T0 v8 §3.7 steps 2–3). Neither advances, because nothing commits.
- Skipping pass 2 would serve image `k` alone and silently lose every acked commit above `T` (mutant **RO-NO-REPLAY**).

**WAL loss: salvage (I-WAL-COVER).** When the coverage check fails, a read-write open fails with XX001 (§14 step 3a), but a read-only open succeeds in **salvage**:
- It runs pass 2 over the valid span as usual and fixes the apply frontier at the largest replayed `commit_ts`, or `T`. `visible_ts` is that frontier, and no snapshot above it is admitted.
- **It serves the ts-prefix state at that frontier.**
  - Every commit `≤ T` is in the image (I-CKPT-AT-T (a)), and every commit in `(T, frontier]` is replayed from records that read back.
  - Every lost commit has a ts above the frontier (I-WAL-TS-ORDER), including the commits whose entries sit in the image without a record, and so does every bulk-load entry whose fence commit was lost. A read at `S` sees only versions `≤ S` (§8), so no read sees a lost commit.
  - Dead provisional entries stay dead.
- **Dropped trees.** The tree directory is unversioned. Image `k` can hold a `dropping` mark, and page frees, from a drop-cleanup commit `c_d` above `T`, even one whose record is lost: `c_d`'s apply sets `dropping`, and the bounded walk then runs as a background job (C-T6-rehost §7.4), with nothing waiting for `M ≥ c_d`. Salvage still never reads a freed page:
  - the drop job issues `c_d` only once `W > S_job`, where `S_job ≥ S_ddl`, the retiring DDL's commit ts (C-T6-rehost §7.4, C-T0 v8 §9.3);
  - `c_d` was applied before the flip, so it was issued before it;
  - `W ≤ M` at all times and `M` is monotone (C-T0 v8 §3.5), so `S_ddl < W ≤ T ≤` the salvage frontier;
  - a read at a salvage snapshot `S ≥ S_ddl` reaches trees only through the catalog at `S` (C-T5), which no longer names the dropped trees;
  - a `CreateTree` of a lost commit leaves a tree that no catalog at `S ≤` the frontier names;
  - the drop walk does not resume in a read-only open (below).

  Snapshots below `S_ddl` are governed here exactly as in any open, by the watermark floor (C-T0 v8 §3.7 step 3).
- It reports the loss with the open: the valid end, `wal_ckpt_end`, `T`, `ts_ckpt_hi` and the frontier.
- **The remedy** is to export from it into a new file (D-7). The read-write open stays refused, and nothing in 2.0 repairs the file in place.
- Mutant **SALVAGE-ABOVE-FRONTIER**: the salvage open admits snapshots above the frontier (for example, `visible_ts := ts_ckpt_hi`) and reads part of a lost commit. Test `t_wal_loss_salvage` (A-7d).

**Splice: salvage (I-WAL-CHAIN).** A splice (§14 step 3a) is served in the same way, with the valid end cut back to the splice cut. Pass 2 replays only records below the cut. The apply frontier is fixed at the largest `commit_ts` replayed, or `T`, and no snapshot above it is admitted. So the open serves the ts-prefix at that frontier, by the argument above. The open reports the splice (the record's LSN, its `prev_lsn` and `prev_crc`, the walk's predecessor's start and `crc`, and the cut) and the coverage result against the cut. The remedy is the same export. Mutant **SPLICE-SALVAGE-PAST-CUT**: salvage replays records at or after the cut. Test `t_wal_splice`, read-only variant (A-7d).

**Where the replay lives: in the pool, unwritten.** Pass 2's mutations, including its splits and the LPIDs they allocate, exist only in memory.
- **Eviction.** A dirty frame is never evicted, because §5.2's dirty-victim write is a write. Clean frames are evicted as usual.
- **Pool budget.** The evictable part of the pool (`F` minus the dirty frames) must meet §5.4's minimum, both during pass 2 and after it. When it would not, the open fails with 53200, having written nothing.
- **The remedy for 53200** is either a read-only open with a larger pool budget (`F` is set per open from the N16 budget, §5.1), which still writes nothing, or a read-write open, whose boot checkpoint makes the replay durable, so that a read-only open of the files it leaves replays nothing.
- **On a volume with no free space, the larger read-only open is the only guaranteed remedy.** A read-write open may need space for its boot checkpoint (§14 step 8), can fail there with 53100 before its meta is durable (§13), and leaves a store that recovers the same state as before.
- **Replay is the only mutation.** No GC mutation (§10), page-local prune, LPID free, directory change or checkpoint runs. A replayed `DropTree` only sets its entry's state (§14 step 4), so the tree's pages stay mapped (mutant REPLAY-DROP-WALK).

**What it skips.**
- **Step 5:** the WAL is not truncated. The torn tail stays on disk, every walk stops at the same valid end (§12.5), and the next read-write open truncates it.
- **Step 6:** `dropping` walks do not resume. Each such tree is in the state a read-write open has before its B-6 job runs.
- **Step 7:** amcheck may run, because it only reads.
- **Step 8:** the boot checkpoint does not run, and there is no boot id. Every provisional entry, from any boot, is dead (C-T0 v8 §2.4), and every read ignores it from its bytes, with no lookup (§8). The live-txn registry stays empty.

**What it refuses.**
- **Every write through `Env`** on the page file or `wal/`: no write, sync, rename, truncate, preallocation, segment recycling or creation. A missing file fails the open, since §2.3's creation is a write. Mutant **RO-WRITES**: the open truncates the WAL (step 5), writes an evicted replayed frame, resumes a drop walk, or checkpoints.
- **Every write transaction.** Every transaction is READ ONLY, and a write gets 25006 (C-T0 v8 §2.5).
- **A corrupt image or meta** fails as it does for a read-write open (XX001, §2.2, §3.3).

**No trace.** Closing or crashing a read-only open leaves every file byte-identical. A later read-write open recovers the same state it would have recovered without it.

---

## 15. Invariants

Each invariant needs a test and a named mutant that the test catches (PLAN-v3 §3.3, §8 phase A gate). The "Pinned by" column names the card that writes the test. These are P-written tests for the cards PLAN-v3 §7.1 lists (A-3, A-4, A-7, B-1, B-2). Mutant names taken from `verdicts/X-2.md` keep their meaning there.

| ID | Statement | Pinned by | Named mutants |
|---|---|---|---|
| I-ENC-ORDER | §7.2: entry-key order = (user key asc, ts desc); `upper(k)` separates groups | A-4a (port X-2's proptest + directed cases) | TS-ASC, ESCAPE-OFF |
| I-ENC-ROUNDTRIP | §7.2–7.3: decode∘encode = id; undecodable → XX001, never a panic | A-4a, fuzz | TOMBSTONE-AS-VALUE, DECODE-PANIC |
| I-TAG-RESERVED | §7.3: reserved tags (9–255), suffix 0, an ordinary tag with suffix MAX and a provisional tag without it → XX001 on every read path | A-4e | RESERVED-AS-VALUE |
| I-PROV-READ | §8: provisional head first, resolver called under the leaf latch, candidate rule; dead entries never served | B-1d (C-T0 v8 §11.2 owns the txn side) | ORDINARY-FIRST, DEAD-AS-LIVE |
| I-SEEK | §8.1: one descent; newest version ≤ S; prefix-checked | A-4e | TS-ASC, SEEK-NO-PREFIX-CHECK |
| I-NEWEST | §8.2: newest entry of the group, ts and liveness | B-1c | NEWEST-SKIPS-TOMB, UNIQUE-TOMB-LIVE |
| I-SCAN | §8.3: bounds table; one version per key; tombstones omitted; streaming | A-4e | SCAN-NO-SKIP, SCAN-HI-INCL, SCAN-TOMB-EMIT |
| I-CURSOR | §6.6: resume by re-descent with the resume key, never a remembered LPID | A-4e (with A-4b packs racing) | CURSOR-STALE-LPID |
| I-TREE | §6.1: key order and subtree bounds after every SMO | A-4a/A-4b (proptest vs `BTreeMap`) | SPLIT-OFF-BY-ONE, SEP-NOT-UPDATED, PACK-SEP-STALE |
| I-LATCH | §5.3/§6.2: coupling and (depth, left-to-right) order | A-4c (loom/shuttle) | COUPLING-EARLY-RELEASE, PACK-RIGHT-FIRST |
| I-ROOT-STABLE | §6.3: a tree's root LPID never changes | A-4a/A-4b (reopen test) | ROOT-MOVES |
| I-PACK | §6.5: entries preserved; merge iff it fits in `F_PACK·U`; never splits | A-4b | PACK-DROP-LAST, PACK-OVERFILL |
| I-ENTRY-UNIQUE / I-REPLAY-IDEM | §9.1, §14: identical re-insert (chains compared by content) is a no-op, a different one XX001; recovering twice gives the same state | B-1c, A-7d (incl. the double-crash test; an overflow value in a partially applied commit) | REPLAY-DUP-CONFLICT, DUP-DIFFERENT-ACCEPTED, CHAIN-LPID-COMPARED |
| I-CHUNK-SUPERSEDE | §12.7, §14: within one chunked transaction the last op per key wins, `revert` deletes; replay twice gives the same state | A-7d, B-1d | SUPERSEDE-AS-CONFLICT |
| I-KEY-SIZE | §3.2: an entry key over `K_MAX` is refused with 54000 at write-set entry; apply never sees one | A-2 (formula and values), B-1a (the write path calls the check) | SIZE-CHECK-OFF |
| I-FAIL-STOP | §9.2: a 58030/53100/53200/XX001 inside a gated operation, a checkpoint step, a background store job or the WAL path (a WAL write or `sync_data`, a segment's creation, preallocation, recycling rename or `wal/` directory sync, recovery's step-5 truncation) poisons the store; a client read's XX001 never does. The mark is made before the first release, and every acquisition checks it. Every blocking wait, the lock-table waits included, has it in its predicate, under the wait's own mutex, and sleeps by releasing that mutex in one step, re-testing on every wake. The broadcast comes after every latch, pin, the gate, the settle stripe and every internal mutex are released, and reaches every wait queue, each live transaction's waiter list included. After poison, every call fails, every waiter returns, and nothing is written. Test `t_gated_xx001_poisons`: a damaged referenced leaf on an apply's path; every commit waiter returns the error within the test's step budget, and no task is left parked. Its **lock-waiter variant**: a txn waits in the lock table on a key held by a commit that sits behind the poisoned apply, so the holder is never released; the waiter must return the error (POISON-LOST-WAKEUP and POISON-WAITER-PARKED leave it parked; pinned in B-1c and B-1b). Its **WAL variant**: the group-commit writer's `sync_data` fails (58030); the store is poisoned and every waiter on `durable_end` returns the error. WAL-ERR-NOT-POISON (an error on the WAL path does not poison the store) fails it. Test `t_client_read_xx001_no_poison`: a client read of that leaf gets XX001, then an unrelated commit and a checkpoint both succeed. Test `t_poison_before_unlatch` (shuttle): a reader that races the failing SMO gets the error, never the half-changed page; so does an evictor's `try_latch`. Test `t_poison_wakeup_race` (shuttle; A-4c, B-1c): a waiter tests its predicate concurrently with the poisoner's mark and broadcast; under every schedule it returns the error. POISON-LOST-WAKEUP (a wait predicate omits the mark, so a woken waiter re-tests only its own condition and sleeps again) fails it, and so does a broadcast made before the mark | B-1c (commit waiters, COMMIT's error, the WAL variant); B-1b, the lock-table card, with B-2b for `wait_for` (the lock-waiter variant); A-3c, A-4c (the latch and `try_latch` check, shuttle) | POISON-IGNORED, XX001-NOT-POISON, CLIENT-READ-POISONS, POISON-WAITER-PARKED, POISON-AFTER-UNLATCH, POISON-LOST-WAKEUP, WAL-ERR-NOT-POISON |
| I-GC-READEQ | §10.2: every GC mutation is read-equivalent at every S ≥ the published W | B-3 (+ G0 re-point, B-7) | GC-DROPS-VISIBLE, G-WATERMARK+1, G-RESURRECT |
| I-GC-ORDER | §10.2/§10.4: an elided tombstone is deleted after every older version of its key | B-3 (a group spanning leaves, read between sweep steps or across a checkpoint and crash) | G-TOMB-FIRST |
| I-GC-COMPLETE | §10.2: after a quiescent sweep, at most one version ≤ w per key, and it is live | B-3 (the §10.2 directed test, with a reader) | GC-KEEPS-TOMB |
| I-GC-LOCAL | §10.3: page-local prune acts only on what the leaf proves | B-3 | PRUNE-FIRST-ENTRY |
| I-SPACE | §11: quiescent fill bound at every height, except rightmost children and the reported `skips`; B-3's W4-shape acceptance ≤ 1.5× in tree bytes (reachable pages × `page_size`) | A-4b (bound, incl. trees of height ≥ 4), B-3 (acceptance) | PACK-OFF, SKIP-UNCOUNTED, SWEEP-PACK-LEFT, CASCADE-ONE-LEVEL, JUNCTION-SKIP |
| I-GCW | §10.5: meta `gc_w` bounds the image's GC and is `≤ T` | A-7c, B-3 (restart floor) | GCW-NOT-RECORDED, GCW-BOUND-SKIP |
| I-LPID-EXACT | §6.8: mapped LPIDs = reachable pages; no leak on pack, drop or chain free | A-8 amcheck, A-4b, B-6 | LEAK-ON-PACK, DROP-ROOT-FIRST |
| I-POOL-BOUND | §5.1/§5.5: the pool never grows; a dirty set of 4× the pool completes | A-3b/A-3c (E4 shape) | POOL-GROW, EVICT-SKIP-DIRTY |
| I-NO-INPLACE | §4.4: a live home slot is never overwritten | A-3c, A-7b (SimEnv crash) | INPLACE-EVICT |
| I-INTEGRITY | §3.3: CRC, id and epoch checked on every read, with XX001 | A-2, A-7d (SimEnv lost/misdirected/torn) | CRC-SKIP, EPOCH-SKIP, ID-SKIP |
| I-META-SLOT | §2.2: the opened meta sits in slot `1 + (ckpt_id mod 2)`; a misplaced valid meta newer than every valid one is XX001, never opened and never fallen back past | A-7d (SimEnv misdirected meta) | META-WRONG-SLOT |
| I-FREE-SAFE | §4.4/§13 step 6: no reuse before the freeing meta is synced | A-3c, A-7c | FREE-EARLY |
| I-FREE-EXACT | §4.3: the persisted bitmaps are exact | A-7b, A-8 | BITMAP-OMITS-FREESET-PAGES |
| I-META-ATOMIC | §13: a meta never points at unwritten pages | A-7c (crash between every step) | META-BEFORE-SYNC |
| I-CKPT-AT-T | §13: the image is a consistent cut; (a)–(c), with `T` = the apply frontier `M`; (b) bounded by `wal_ckpt_end`/`ts_ckpt_hi`, bulk-load entries via their fence commit | A-7a/A-7c (splits and packs during a checkpoint; async mix), B-1d (a checkpoint during a finalize) | FLIP-NO-GATE, WAL-SYNC-T-ONLY, CKPT-T-VISIBLE |
| I-REDO-COMPLETE | §14: every acked sync commit is in the image or replayed (given I-ACK-PREFIX; §14 Post-sync damage; also from an older meta, `t_meta_rollback_crosses_boot`) | A-6d, A-7d | START-LSN-PAST-CHUNK, START-LSN-MARKED-CHUNK |
| I-WAL-COVER | §13 step 0, §14 step 3a: meta `k` records `wal_ckpt_end` and `ts_ckpt_hi`. An open fails with XX001 before pass 2 and writes nothing if its valid end (the walk's stop position, §12.5) is below `wal_ckpt_end`, or if its `Commit`/`CommitMarker` records do not reach `ts_ckpt_hi`. No open holds an entry above its recovered frontier. Test `t_wal_loss_below_ckpt_end`: a SimEnv fixture with post-sync WAL faults (LOST, TORN or MISDIRECTED on a synced WAL write) holds a commit `c > T` partly applied before the flip, whose record step 1 synced; then meta `k` is durable, then a fault on `c`'s record. The open gives XX001 and every file is byte-identical afterwards. Variants: the fault on a record that was appended but unapplied at the flip; on a `SegmentEnd` before `wal_ckpt_end`; on a chunk of a marked transaction; on a chunk of a transaction that never committed (no committed data lost; still XX001); on a bulk load's fence commit `f > T`, with loaded entries in image `k`; **short segment**: a segment cut short so that its end-of-file lies past the start of a record that runs past it, below `wal_ckpt_end`. Under COVER-CHECK-SKIP, a new commit and then a read at `S ≥ c` see part of `c`; under COVER-END-AT-T, the open succeeds; under COVER-AFTER-TRUNCATE, the files differ; under COVER-AT-EOF, the short-segment variant opens. Test `t_cover_ts_cross_check`: a meta whose `ts_ckpt_hi` exceeds every replayable commit (forged through `Env`) gives XX001; for §14 step 1's open-time check, a forged meta with `T > ts_ckpt_hi`, and one with `wal_start_lsn > wal_ckpt_end`, each with a valid CRC, give XX001 (META-RANGE-SKIP opens them) | A-7c (the fields), A-7d (SimEnv post-sync WAL damage) | COVER-CHECK-SKIP, COVER-END-AT-T, COVER-AFTER-TRUNCATE, TS-HI-CHECK-SKIP, COVER-AT-EOF, META-RANGE-SKIP |
| I-WAL-CHAIN | §2.2, §12.2, §12.5, §14 step 3a: every record carries `prev_lsn` and `prev_crc`, the start and the `crc` of the record appended before it (`SegmentEnd` included); a boot's first record names its recovery's last accepted record, or the opened meta's `wal_start_prev` and `wal_start_prev_crc`. The chain is checked last, after every stop check, and both fields are compared. A record that passes every stop check but names another predecessor (either field differs) is a splice, never an end: a read-write open fails with XX001 before pass 2 and writes nothing, and a read-only open serves salvage cut before it (§14.1). The stop checks keep their meaning. Undetected only when the spliced-in record's predecessor is byte-identical, at the same position, to the walk's (or their CRCs collide). Tests `t_wal_splice` (both placements, the read-only variant, the same-id residual variant, the same-start variant that a position-only chain misses), `t_wal_chain_crash` (no splice under ordinary crashes) | A-6a (the fields), A-6b (the check), A-7c (`wal_start_prev` and `wal_start_prev_crc`, each boot's first `prev_lsn` and `prev_crc`), A-7d (the outcomes) | PREV-CHECK-SKIP, PREV-CRC-UNCHECKED, PREV-MISMATCH-AS-END, PREV-FIRST-WRONG, PREV-HOP-WRONG, WAL-START-PREV-WRONG, WAL-START-PREV-CRC-WRONG, SPLICE-SALVAGE-PAST-CUT |
| I-RO-OPEN | §14.1: a read-only open writes nothing through `Env`, and its reads at every registered `S` equal those of a read-write open of the same files after step 4, except under WAL loss or a splice (§14 step 3a), where the read-write open fails and the read-only open serves the salvage state at the recovered frontier (§14.1). Every build offers it (§14.1). Test `t_ro_open`, with a SimEnv that fails every write, sync, rename and truncate. Its fixture is a crashed file with a torn WAL tail, small commits above `T`, a marked chunked txn whose marker lies after a commit `≤ T`, unmarked chunks, a `dropping` entry, provisional entries in the image, and replay splits. The test compares the reads with a read-write open of a copy, checks the files are byte-identical after close, and checks that a pool too small for the replay gives 53200 with nothing written. Test `t_ro_open_disk_full` (the stopped store, C4 decision 5): a SimEnv stops a writer with ENOSPC (53100) at each write point in turn, namely a WAL append, a segment's creation, its preallocation (a short segment, §12.5), a file extension (§4.5), a checkpoint page write and the meta write. A read-only open then runs on a SimEnv that reports zero free bytes and refuses every write, including a lock file's creation. It must succeed, its reads must equal those of a read-write open of a copy on a roomy Env, a full scan of every published tree must succeed, and every file must be byte-identical after close. With a pool below the replay's evictable minimum it must give 53200 with nothing written, and a read-only open of the same files with a larger pool budget must then succeed at zero free bytes (§14.1's remedy). SHORT-SEG-ERROR (a short or missing segment returns 58030 instead of ending the valid end) fails the open. Test `t_seg_ack_durable` (A-6b, §12.5's writer rule), with a SimEnv whose power cut also loses unsynced creates, renames and `set_len`: a segment is newly created (the lost-create case) or recycled by rename (the lost-rename case), records in it are acked, and power is cut at every step from the create or rename to the ack. Recovery must replay every acked record. SEG-ACK-BEFORE-DURABLE (ack before the segment's `sync_all` and its `wal/` directory sync) loses an acked commit as a clean stop and fails it. Test `t_wal_loss_salvage` (Q1 = A, PLAN-v3 §10 item 8): the `t_wal_loss_below_ckpt_end` fixture, plus a `dropping` mark from a drop-cleanup commit above `T` whose record is lost, opened read-only. At every admitted `S` it reads the model's state at `S ≤` the frontier, never an entry above it, and a full scan of every tree the catalog at `S` names succeeds. SALVAGE-ABOVE-FRONTIER fails it. Test `t_wal_splice`, read-only variant: the open serves the state at the frontier of the records below the splice cut; SPLICE-SALVAGE-PAST-CUT fails it | A-7d, A-6b | RO-NO-REPLAY, RO-WRITES, RO-LOCK-FILE, SHORT-SEG-ERROR, SEG-ACK-BEFORE-DURABLE, SALVAGE-ABOVE-FRONTIER, SPLICE-SALVAGE-PAST-CUT |
| I-WAL-STALE | §12.5: the reader stops at the first bad magic/CRC, wrong `lsn` or over-long `len`, or a boot id older, in serial-number order, than the last accepted record's (the opened meta's for the first record); a walk may cross into newer boots; recovery truncates there. It hops to the next segment after `SegmentEnd` or after a record that ends exactly at its segment's end, and a writer never leaves a shorter tail (§12.2). A boot's id is fresh after a meta rollback (§14 step 8). Tests `t_stale_boot_record`, `t_meta_rollback_crosses_boot`, `t_wal_exact_segment_end`, `t_boot_id_after_rollback` (both variants) | A-6b, A-7d | LSN-CHECK-SKIP, BOOT-ID-STOP-SKIP, BOOT-ID-EQ-STOP, SEG-EXACT-NO-HOP, SEG-TAIL-SHORT, BOOT-ID-CKPT-ONLY, BOOT-ID-NO-META-FLOOR |
| I-ACK-PREFIX | §12.4: `written_end` and `durable_end` advance only over the contiguous prefix; an `on` ack needs `durable_end ≥` the record's end; an `off` ack needs `written_end ≥` it; step 1 waits on `durable_end ≥ wal_ckpt_end` | A-6c (`t_ack_prefix`, porting X-5c-F3-F1 (a)'s two-task interleaving), A-7c (`t_ckpt_sync_prefix`), B-1c (C-T0 v8 §3.1 step 3's visibility over the same prefix) | ACK-OVER-HOLE, CKPT-SYNC-OVER-HOLE |
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
- **Watermark.** The computation, publication and monotonicity of `W`, and its floor after a restart (`≥` meta `gc_w`, §10.5). That `W ≤` every registered snapshot and `W ≤ M`, the apply frontier (hence `W ≤ visible_ts`), is the precondition this file relies on (§10.1, §10.5).
- **The write set and lock table:** own-write merge (I-HALLOWEEN), escalation, NOWAIT/SKIP LOCKED.
- **The write-side rules:** the RR check after the lock wait, unique checks and ON CONFLICT, using §8.2.
- **Commit-visibility semantics.** The commit ack waits for `visible_ts ≥ c` (decision block, "Commit visibility"). There is no session floor: C-T0 v8 §3.4 rejects it and says why.
- **Pre-applying large transactions** as transaction-tagged provisional entries (decision block, "Large-txn head-of-line"). Specified in C-T0 v8 §2.4 and §3.2. This file holds the encoding (§7.1, §7.3 tags 6–8), the read rule (§8), chunk supersede (§12.7, §14) and the image rule (I-CKPT-AT-T (c)), from the S-2 co-edit.
- **Lock release at commit cheaper than O(keys)** (325k–437k probes slipped through the release tail in W6): C-T0 v8 §3.3.
- **Unrelated commits stalled** behind a big apply by the ts-ordered `visible_ts` advance: C-T0 v8 §3.1–§3.2.
- **SSI** without intents.
- **GC policy:** when sweeps run, their budgets, and DDL retirement timing (C-T0 v7.8 §9.2–9.3).
- **Bounds on disk growth under long readers.** Whether on-disk version growth under long readers needs a bound is S-2/B-3's call (decision block, ruling R2 "Not R-1's call"). This file only guarantees that the growth costs disk, not RAM (§5.5).

**S-3 owns:** the row format, TOAST semantics over §6.7's chains, and the online index build's steps under the §12.8 rule. That includes registering a snapshot at `R` **before** picking the load's `S > R` and keeping it registered for the whole load (§10.1), and how an unfinished build is restarted. S-3 should also record §10.1's reasoning for why this condition has no mutant.

**R-1 owns:** the numeric budgets per mode, `F`, `R_EPOCH`, and the per-consumer accounting of §5.7.

**D-6 and D-8 own:** backup, PITR and replica handling of the §12.8 boundary; format versioning and upgrades.

---

## 18. Refinements of PLAN-v3 §3.3

This file wins over the plan (precedence above). The plan should be updated through an Opus card to match these. R10 refines the §4 decision block rather than §3.3. R11 comes from the S-2 co-edit. R12–R15 come from card S-1-F3, and R16 from card S-1-F5; R15 refines §3.4, as R9 does:

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
| R10 | §4 decision block, "Space amplification": "after-GC tree bytes" (undefined) | Tree bytes = reachable pages × `page_size` (§11) | The file never shrinks (§4.5), so a file-length or allocated-page reading stays at the post-update peak and B-3 could never pass |
| R11 | Step 0: "T = the apply frontier (every commit ≤ T is fully applied to the trees; under N2, trees hold only committed data)" | `T` = C-T0 v8's `M`; trees may also hold provisional entries, which are never data after recovery (§1, §13 (c)) | X-4 fired the pre-apply trigger; C-T0 v8 §3.2 |
| R12 | Meta fields (R1's list) | Adds `wal_ckpt_end`, `ts_ckpt_hi`, `boot_id` (§2.2) | I-WAL-COVER and the ordered boot-id stop |
| R13 | Recovery: "walk … to the valid end", with no rule for an early end | The coverage check against the walk's stop position: fail closed with XX001, write nothing, and a read-only salvage that every build offers (§14 step 3a, §14.1; PLAN-v3 §10 item 8) | E4 class MW (X-5c-F3) |
| R14 | Group commit: "one writer … then acks the group" | I-ACK-PREFIX (§12.4) | X-5c-F3 seed 21 |
| R15 | §3.4's fail-stop (I/O error, disk full) | I-FAIL-STOP includes XX001 inside gated operations, checkpoints and store jobs, and 58030/53100/53200/XX001 on the WAL path; client reads never poison; every wait, the lock-table waits included, observes the poison (§9.2) | G8, MP-LIVELOCK 41 → 0 |
| R16 | WAL record framing (unstated) and meta fields (R12) | Records carry `prev_lsn` and `prev_crc`, the meta carries `wal_start_prev` and `wal_start_prev_crc`, and the walk checks the chain (I-WAL-CHAIN, §12.5, §14 step 3a) | Owner decision, PLAN-v3 §10 item 9 (2026-11-03): back-pointers in the 2.0 format, because adding them later needs a format migration. Its addendum (owner, 2026-11-03): the chain also carries the predecessor's CRC, as SQLite's does, because a position alone misses two boots appending equal-length records from the same point |

---

## 19. Open questions, known costs, revisit triggers

**Open:**
1. **File shrinking.** GC returns space to the free set, not to the filesystem (§4.5). Trigger for a tail-truncation step (at step 6, of free tail slots only) or an offline compaction: a browser quota (E6/D-5) or a user report of a file that does not shrink after a mass delete.
2. **Leaf prefix compression within groups.** Every version repeats its full escaped key, so a 10-version group of W1-sized keys spends about 117 B on repeated key bytes. A-4d covers inner nodes only. Trigger: NBench space rows, or I-SPACE met but B-3's bytes measured close to 1.5×.
3. **Optimistic latches and pointer swizzling** (N8/N9): only on the E0/E7 triggers (PLAN-v3 §3.12).
4. ~~**Provisional entries.**~~ Closed by the S-2 co-edit: §7.1, §7.3 tags 6–8, §8, §12.7, I-CKPT-AT-T (c), §14 (C-T0 v8 §2.4).
5. **PITR and replicas across a §12.8 bulk load** (D-6).
6. **Resumable overflow-chain free or build** if A-5 measures the flip hold as too long (§6.7). A resumable chain free or build must keep I-LINK-ATOMIC (§5.6).

**Known costs of (b), accepted, with revisit triggers (from the decision block, restated so the storage cards carry them):**
- **T1: latest-snapshot scans.** 8.2× slower than (c) at 10 versions/key (W3). Mitigations: GC at the watermark (B-3) and version skipping (§8.3, A-4e/B-3). **Trigger:** after both mitigations, the NBench OLTP range rows or E9 miss target, and a profile attributes more than 20% of scan CPU to stepping over invisible versions. Then an Opus card re-opens N3.
- **T2: space amplification after GC.** 20.11× in E0. **Trigger:** B-3's W3/W4-shape acceptance (after-GC tree bytes `≤ 1.5×` pre-update, both as reachable pages × `page_size`, §11) fails on the real tree. Then N3 is re-opened.
- **T3: contended writes.** Both arms collapse past 8 threads. This is a shared commit-path or latching bottleneck owned by S-2 and E7, not by N3 or this file.

**Known cost of a single-copy WAL (cards S-1-F3, S-1-F5):**
- **T4: undetectable post-sync WAL loss** (§14, "Post-sync damage").
  - **The cost:** acked commits since the last checkpoint can be lost silently on a device that drops synced writes: a lost tail, or damage the walk stops at, above `wal_ckpt_end`.
  - **2.0 has WAL back-pointers** (I-WAL-CHAIN: `prev_lsn` and `prev_crc`; owner decision, PLAN-v3 §10 item 9, and its addendum adding `prev_crc`, both 2026-11-03). They catch a mid-log splice unless the spliced-in record's predecessor is byte-identical at the same position, and they fail it closed, but they catch neither a lost tail nor damage the walk stops at, and they add no recovery (§12.5).
  - **Hardware guidance (for D-9's release notes and the operator docs).** Durability of acked commits since the last checkpoint assumes a device that honours flush/FUA:
    - enterprise SSDs with **power-loss protection**, or consumer drives whose volatile write cache honours flush;
    - not virtual disks or network block devices configured to ignore flushes (for example, a hypervisor `cache=unsafe`).

    On browser OPFS, it is whatever the browser's flush guarantees (E6).
  - **Roadmap: opt-in synchronous replication for server mode** (PLAN-v3 §10 item 9). An `on` commit is acked only once a replica on another machine also holds its record. This is the real durability guarantee against a device that lies, because it gives both detection and repair. It builds on D-6's WAL-streaming replicas, which carry the chain (`prev_lsn`, `prev_crc`) through. A second WAL copy on the same device barely helps, because the same device can lie for both.
  - **Other mitigations, not in 2.0:** per-batch `durable_end` witnesses with a forward scan past the stop (detection only); more frequent checkpoints (a narrower window).
  - **Trigger:** a field report on a target that drops synced writes, a splice XX001 reported from the field, or server mode's synchronous-replication card.

---

## Changelog

- **Draft 6** (2026-11-03, card S-1-F5, from S-1-F3's optional item W-BP and its gate review, `verdicts/S-1-F3-draft2-gate.md` "W-BP" and required edit 6):
  - **Owner decision** (PLAN-v3 §10 item 9, decided 2026-11-03): WAL back-pointers go into the 2.0 format; fail-closed stays (item 8); the lost-tail blind spot is documented with hardware guidance and trigger T4; opt-in synchronous replication is on the server-mode roadmap. **Addendum** (owner, 2026-11-03): the chain also carries the predecessor record's CRC, `prev_crc`, as SQLite's chained WAL checksums do, because a position-only chain misses two boots appending equal-length records from the same point.
  - **Format.** `prev_lsn` and `prev_crc` in every record, after `lsn`, so under the record CRC; record CRCs are finished in LSN order (§12.2). `wal_start_prev` and `wal_start_prev_crc` in the meta (§2.2), recorded at step 0 with `wal_start_lsn` and written at step 4 (§13). R16.
  - **I-WAL-CHAIN** (§12.5): both fields checked after every stop check; a mismatch in either is a splice, never an end; the stop checks keep their meaning. Its premises, including increasing boot ids (§14 step 8), and its limits (a lost tail, damage the walk stops at, a predecessor byte-identical at the same position, a CRC collision). The E4 prototype omits it; X5-R33 stands.
  - **Outcomes** (§14 step 3a, §14.1): a read-write open fails with XX001 before pass 2 and writes nothing, wherever the splice lies; a read-only open serves salvage cut before it.
  - **Post-sync damage** (§14): the splice exception and row. **Residual** (§12.5): its truncation-durability premise (S-1-F6, proposed), and detection by back-pointers unless the stale record's predecessor is byte-identical at the same position.
  - **§15:** new row I-WAL-CHAIN; I-RO-OPEN amended. Mutants PREV-CHECK-SKIP, PREV-CRC-UNCHECKED, PREV-MISMATCH-AS-END, PREV-FIRST-WRONG, PREV-HOP-WRONG, WAL-START-PREV-WRONG, WAL-START-PREV-CRC-WRONG, SPLICE-SALVAGE-PAST-CUT; tests `t_wal_splice` (with the same-start variant), `t_wal_chain_crash`.
  - **§19 T4** rewritten: back-pointers in 2.0, hardware guidance, the replication roadmap.
- **Draft 5** (2026-11-03, card S-1-F3, per verdict X-5c-F3 038bb9cc and the draft gates `verdicts/S-1-F3-draft-gate.md` and `verdicts/S-1-F3-draft2-gate.md`):
  - **Owner Q1 = A** (PLAN-v3 §10 item 8, decided 2026-11-02): detected WAL loss refuses a read-write open with XX001, and a read-only open serves the ts-prefix salvage state, in every mode. Options B, C and C′ were declined.
  - **Meta fields (W1).** `wal_ckpt_end`, `ts_ckpt_hi` and `boot_id` (§2.2); the creating session's boot id is 0. R12.
  - **TxnId and `SegmentEnd` (W2).** Every record carries its writing boot's id; TxnId `n ≥ 1`; `SegmentEnd`'s `txn` is `(boot id, 0)` (§12.2).
  - **Exact segment end (draft-2 gate G2).** A record ends exactly at its segment's end or leaves room for a `SegmentEnd`; the reader hops after either (§12.2, §12.5). Mutants SEG-EXACT-NO-HOP and SEG-TAIL-SHORT, test `t_wal_exact_segment_end` (E4: 1064 r1, 4770 r2, 5054 r1, 6263 r2).
  - **I-ACK-PREFIX (W3).** §12.4 rewritten: `written_end` and `durable_end` advance only over the contiguous prefix; acks and checkpoint step 1 wait on them. Mutants ACK-OVER-HOLE and CKPT-SYNC-OVER-HOLE. R14.
  - **Ordered boot-id stop (W4).** §12.5 stops at a record whose boot id is older, in serial-number order, than the last accepted record's (the opened meta's for the first); a walk may cross into newer boots. The valid end is the walk's stop position; a short segment ends the walk at a record's start, never at its end-of-file. Mutants BOOT-ID-STOP-SKIP, BOOT-ID-EQ-STOP, COVER-AT-EOF.
  - **Boot-id freshness (draft-2 gate G1; post-apply gate R1).** A boot's id is the newest, in serial-number order, of its `ckpt_id`'s low 32 bits, one above the opened meta's `boot_id`, and one above the newest boot id its recovery walk accepted (§14 step 8). Mutants BOOT-ID-CKPT-ONLY and BOOT-ID-NO-META-FLOOR, test `t_boot_id_after_rollback` (two variants).
  - **Checkpoint (W5).** Step 0 reads `wal_ckpt_end` and `ts_ckpt_hi` in the sequencer's critical section; step 1 syncs through `wal_ckpt_end` on `durable_end`; step 4's range checks; I-CKPT-AT-T (b) restated, bulk loads included (§13).
  - **Recovery (W6).** The open-time meta check (step 1, META-RANGE-SKIP); the coverage check I-WAL-COVER (step 3a), failing closed with XX001 before pass 2 and writing nothing; the "Afterwards" bound; the "Post-sync damage" case table; I-REDO-COMPLETE's premises (§14). R13.
  - **Read-only open and salvage (W7).** Every build offers a read-only open; under WAL loss it serves the salvage state at the recovered frontier, dropped trees included (§14.1). Mutant SALVAGE-ABOVE-FRONTIER.
  - **I-FAIL-STOP (W8, G8).** §9.2 rewritten: 58030/53100/53200/XX001 inside a gated operation, a checkpoint step, a background store job or the WAL path poisons the store; client reads never do; the atomic mark, the mark in every wait predicate, the broadcast after every lock is released, and how the store ends. R15.
  - **§15 (W9).** New rows I-WAL-COVER and I-ACK-PREFIX; I-WAL-STALE, I-FAIL-STOP, I-RO-OPEN, I-REDO-COMPLETE and I-CKPT-AT-T amended.
  - **§18 and §19 (W10).** R12–R15; trigger T4 (undetectable post-sync WAL loss).
  - Not applied: the optional WAL back-pointer (owner Q2, PLAN-v3 §10 item 9, open).
- **Draft 4** (2026-10-26, card S-1-F2, per P's ruling on F3 of `verdicts/X-5b.md` at f8a6b6eb, branch card/v3-X-5b):
  - **Meta own slot (F3).** §2.2's open rule adds a positional check: a meta is valid at open only in its own slot, `1 + (ckpt_id mod 2)`. A valid meta in the other slot is a misdirected write. It is never opened, and if it is newer than every valid meta the open fails with XX001. Before this, magic and CRC alone made a misplaced copy openable, which breaks §2.2's torn-write guarantee and is the fallback past a synced meta that §2.2 already forbids. New row I-META-SLOT (§15), mutant META-WRONG-SLOT, pinned by A-7d.
- **S-2-F2 rework** (2026-10-24, card S-2-F2, per `verdicts/S-2-F2.md` REWORK; C4 decision 5's binding, PLAN-v3 §8.1):
  - **Stopped store (R1).** §14.1 states that a read-only open of a 53100-stopped store succeeds with zero free bytes and writes nothing. New test `t_ro_open_disk_full` (A-7d) in the I-RO-OPEN row.
  - **Remedy (R2).** The 53200 remedy includes a read-only open with a larger pool budget; on a full volume it is the only one.
  - **Lock (R3).** N11's lock creates and writes no file (advisory lock on the page file, or OPFS's access-handle mode). Mutant RO-LOCK-FILE.
  - **Short segment (R4, R6).** §12.5: a segment cut short by a failed creation or preallocation ends the valid end at its end-of-file, not 58030. Mutant SHORT-SEG-ERROR. This is sound because a writer appends to a segment only after its creation or recycling rename and its length are durable (`sync_all` plus a `wal/` directory sync through `Env`), per round 2's R6. Mutant SEG-ACK-BEFORE-DURABLE, test `t_seg_ack_durable` (A-6b, lost-create and lost-rename cases). Round 2's nits: "can fail" in §14.1's remedy bullet, and the A-1 shared-lock note as a comment in §14.1.
- **S-2-F2** (2026-10-24, card S-2-F2, with C-T0 v8 draft 8.2, per `verdicts/S-2-F1.md` MERGE-WITH-FOLLOWUPS):
  - **Boot id (F2-a).** §7.3 tag 6's `txn` is `(u32 boot id, u64 n)`, as in §12.2, and §8's fast path says "current-boot". Read as the checkpoint epoch `e`, both would have made live provisional entries dead at the next checkpoint.
  - **`Released(⊥)` (F2-c).** A transaction that committed with no ts writes no marker, so its chunks do not hold back `wal_start_lsn` (§12.6, §14 step 8).
  - **Read-only open (the verdict's N11 observation).** New §14.1: it replays steps 1–4 into the pool, never writes (no dirty eviction, no truncate, no drop walk, no checkpoint), fails with 53200 when the replay does not fit, has no boot id, and excludes a writer. New row I-RO-OPEN, mutants RO-NO-REPLAY and RO-WRITES, test `t_ro_open` (A-7d).
- **S-2-F1** (2026-10-23, card S-2-F1, with C-T0 v8 draft 8.1, per `verdicts/S-2.md`):
  - **Frontiers (item 3).** §10.5's Bound argues from `W ≤ M`, `M` monotonic and `T := M`. §17 "Watermark" names `W ≤ M`. §17 "Commit-visibility" drops the session floor. §10.1 condition 2 argues from `w ≤ W ≤ M` and covers conversions.
  - **Boot checkpoint (item 8).** Always a new meta, durable before the first transaction, skipped by read-only opens; its `ckpt_id` gives the **boot id**, renamed from "boot epoch" (§12.2, §13 (c), §14 step 8).
  - **Chain free on replace (item 10).** A provisional rewrite and a superseding replay op free the replaced chain (§6.7, §14 step 4).
- **S-2 co-edit** (2026-10-23, card S-2, with C-T0 v8 draft 8.0):
  - **Provisional entries.** Tags 6–8 at suffix `u64::MAX` (§7.1, §7.3); I-TAG-RESERVED restated; the provisional-head read rule and I-PROV-READ (§8); provisional entries are not versions to GC, and dead ones may be deleted (§10.2); the conversion's chain transfer (§6.7, §6.5).
  - **Apply frontier.** Now C-T0 v8's `M` (§1). I-CKPT-AT-T (c) covers images that hold provisional entries (§13).
  - **WAL.** Chunked ⇔ large; chunk supersede and `Delete{revert}` (§12.3, §12.7); I-CHUNK-SUPERSEDE in replay (§14).
  - **Boot.** The boot checkpoint (§14 step 8); `gc_w_used` starts at the recovered `gc_w` (§10.5).
  - **I-ENTRY-UNIQUE.** "Identical" compares overflow chains by content, not LPID (§9.1; mutant CHAIN-LPID-COMPARED). Before this, a replayed commit with a large value that was partly applied in the image would have hit XX001.
  - §17 and §19 Q4 updated.
- **Draft 3** (2026-10-23, card S-1-F1, per `verdicts/S-1.md` MERGE-WITH-FOLLOWUPS):
  - **Sweep cascade (F1 item 1).** In the sweep, an inner page packs only with its right neighbour, and a rightmost child is exempt. `finish` recurses to every ancestor whose subtree has ended, and a junction walk re-checks the pages that stop being rightmost children (§10.4 step 5, §6.5, §11). New mutants: SWEEP-PACK-LEFT, CASCADE-ONE-LEVEL and JUNCTION-SKIP (§15 I-SPACE).
  - **Bulk-load pin (F1 item 2).** S-3 registers `R` before it picks `S > R`. The section records ruling 1's reasoning for having no mutant (§10.1, §17).
  - **FreeSet convergence (F1 item 3).** The condition is `r + extra ≤ g`, with a re-check (§13 step 2).
  - **Inner redistribute (F1 item 4).** The cell that enters `L` is `P`'s separator. The fits test and the Full claim use it (§6.5).
- **Draft 2** (2026-10-23, card S-1 rework, per `verdicts/S-1.md` REWORK):
  - **Replay start (R-1).** §12.6 counts every chunked transaction whose marker is not yet below `T`; mutant START-LSN-MARKED-CHUNK.
  - **Tree bytes (R-2).** Defined as reachable pages × `page_size` (§11, §19 T2, §18 R10).
  - **Pack skips and sweep latching (R-3).** Pack skips are counted and excluded from I-SPACE (§6.5, §11). The sweep latches `P`, `L`, `R` top-down and cascades by re-descent (§10.4). The inner merge accounts for the pulled-down separator.
  - **G-RESURRECT (R-4).** Restored to X-2's meaning. Added G-TOMB-FIRST, and listed the spike's tombstone-first delete order as a departure (§10.2).
  - **Bulk-load watermark (R-5).** The load registers `S − 1` (§10.1).
  - **Nits.** TRUNCATE (§12.8). The I-CKPT-AT-T exception for unpublished bulk loads. FreeSet sizing after growth (§13 step 2). New rows I-KEY-SIZE, I-WAL-STALE and I-GCW. Both I-ENTRY-UNIQUE mutant directions. Cite fixes.
- **Draft 1** (2026-10-23, card S-1): first version. Pins the N3 = (b) entry layout from X-2 (and C-T0 v7.8 §2.2's value variants), the read, scan and GC algorithms, the pack step and the free set that make GC return space, the memory model behind X-4's ruling R2, the WAL format, the checkpoint order, recovery, the §15 invariant table with named mutants, the measured-evidence map, and the §18 refinements of PLAN-v3 §3.3.
