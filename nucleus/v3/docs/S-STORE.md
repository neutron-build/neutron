# S-STORE: Storage layer (normative)

Status: draft 7.7 (card S-1-F7-F2-F2-F4b, 2026-11-07: latch scope, one call and one tree per hold, with two exceptions that add no wait edge; parked cursors that hold no latch and no pin, and a parked position that carries a mid-group state; a per-call bound that also covers SER point reads and newest probes; task-fair latches as a bounded-bypass property, not FIFO; no recursive latch; no page read in a reader's hold; the deadlock argument, covering the checkpointer, the map radix order and the pool floor; the image map written only by step 2's map phase from an in-memory image-record list, so a claim takes no map latch and the reserve bound is one claim per thread; map-page writes that release the page before latching the parent; the map phase drains in-flight epoch-`k` writes, decides each source under its latch, never sets a map page's current home, and keeps a superseded slot while another reference holds it; map positions keyed apart from LPIDs; the list reserved at open; the data phase's adoption guard; the blocking root latch; batch buffers; reverse and scroll cursors; tests `t_latch_scope`, `t_root_latch_fair` and `t_cursor_park_equiv`, with new variants of `t_epoch_reserve_two_exhausters` and `t_step2_waits_for_claimed_copy`; twenty-seven new mutants) on draft 7.6 (card S-1-F7-F2-F2-F4a, 2026-11-07: root names come from an in-memory root table with no generation counter and a write-free read path; DropTree removes a tree's entry under the root's exclusive latch, before it frees the root; a root name is validated by a re-read under the root's latch, an unmapped pin on that path is a retry, and only the header `id` is read before validation; only a directory change latches the directory; the table is built at open and maintained in replay; `t_dir_root_no_stall` is kept as a regression with mutant DIR-LATCH-ON-READ; test `t_root_name_reuse` with mutants ROOT-NAME-UNVALIDATED, ROOT-UNPUBLISH-LATE, ROOT-PIN-UNMAPPED-FATAL, ROOT-DECODE-BEFORE-VALIDATE and ROOT-TABLE-GLOBAL-GEN; T5's cost is now a table read and a re-read; the root latch stays `try_latch` until S-1-F7-F2-F2-F4b; rework 2026-11-07: case B of `t_root_name_reuse` names the root in a mode that accepts `dropping` and reuses its LPID at the hook, a header `id` mismatch before validation is a retry unless the entry is unchanged, with mutant ROOT-HEADER-MISMATCH-FATAL, the exactness argument covers an LPID reused in another frame, and the churn variant is deterministic) on draft 7.5 (card S-1-F7-F2-F2, 2026-11-07: the free-set lock guards the LPID bitmap and `next_lpid` only, ranks after the gate and frame latches with the page table inside it, and runs no I/O, eviction or frame latch; a restarted allocation keeps its MapLeaf pins and retries on them first, an LPID with no MapLeaf yet restarts and gets its empty MapLeaf in a gated operation of its own, and the restart bound is stated; tests `t_freeset_lock_order` and `t_alloc_restart_progress`; mutants ALLOC-RETRY-LOWEST, ALLOC-NEW-MAPLEAF-IN-PLACE, FREESET-LOCK-IO, FREESET-LOCK-INVERT; rework 2026-11-07: the empty MapLeaf's frame is obtained before its MapInner is latched, and it is created only if the ref is still null; mutants MAPLEAF-CREATE-UNCONDITIONAL, MAPLEAF-FRAME-UNDER-INNER; root-latch liveness moved to S-1-F7-F2-F2-F4a and -F4b (ruling lockorder)) on draft 7.4 (card S-1-F7-F2-F1, 2026-11-07: every chain-freeing delete outside the sweep, settler (a)'s abort cleanup, `T`'s `ROLLBACK TO` delete and a replayed `Delete{revert}` of a tag-8 entry, frees its chain holding only the leaf; §6.7's settle clause precedes the replace's exception; `t_replace_free_shape` gains delete variants and a build variant; mutants DELETE-FREE-UNDER-P and CHAIN-BUILD-UNDER-P; rework 2026-11-07: the build variant's chain takes its LPIDs on evicted MapLeafs other than the split's, with an inline old value, and a chain-freeing delete does not pack because it releases `P` first and its leaf cannot fall below `U/4`) on draft 7.3 (card S-1-F7-F2, 2026-11-07: a restart after a removal or an unlink is tested; a split reserves its pages and allocations' MapLeafs before its prune, deciding from the prune's marks; an allocation pins a resident MapLeaf or restarts; a chain build and a replace's chain free hold only the leaf; DropTree's last step does not pack; the directory leaf is latched shared and a root is taken with `try_latch`; step 2's wait has a mutant; A-7b owns the walk and A-7d's sweep is a test-only stand-in; known cost T5) on draft 7.2 (card S-1-F7-F1-F1, 2026-11-07: the page-local prune frees no chain, at a split and at eviction, and elides a tombstone only when no older version of its group holds one; chain-bearing garbage is left to the sweep, and A-5's interim prune clauses are gone) on draft 7.1 (card S-1-F7-F1, 2026-11-07: A-5 measures the flip hold per gated operation, for a chain's build and free, a replace and cold chains, and a GC sweep deletes each chain-bearing entry with its chain in its own gated operation, holding only its leaf and conditional on the entry's identity; rework 2026-11-07: the identity check, the dead-entry reason the re-runs end, the prune in A-5's lists) on draft 7 (card S-1-F7, 2026-11-05: I-LINK-ATOMIC, an LPID mapped and unmapped in the same gated operation as its reference, so DropTree removes the directory entry and frees the root in one gated operation, an operation restarts only before its first mutation, and a chain-bearing entry goes with its chain in its own operation; E4's second design finding) on draft 6 (card S-1-F5, 2026-11-03: WAL back-pointers, `prev_lsn` and `prev_crc` with the meta's `wal_start_prev` and `wal_start_prev_crc`, and the splice check I-WAL-CHAIN, per the owner decision PLAN-v3 §10 item 9 and its 2026-11-03 addendum) on draft 5 (card S-1-F3, 2026-11-03: WAL loss after sync and the coverage check I-WAL-COVER with read-only salvage, I-ACK-PREFIX, the ordered boot-id stop and boot-id freshness, the exact segment end, XX001 and WAL-path poisoning in §9.2) on draft 4 (card S-1-F2, 2026-10-26: the meta own-slot rule in §2.2) on draft 3 (card S-1-F1, 2026-10-23) + the S-2 co-edit (card S-2, 2026-10-23: provisional entries, the apply frontier `M`, chunk supersede, the boot checkpoint) + the S-2-F1 fixes (2026-10-23: frontiers in §10.1, §10.5 and §17, the boot checkpoint's durability, chain free on replace) + the S-2-F2 fixes (2026-10-24: the boot id in §7.3 and §8, `Released(⊥)` chunks in §12.6, the read-only open in §14.1) + the S-2-F2 rework (2026-10-24: the stopped store and its remedy in §14.1, a short segment in §12.5). This is a P-authored spec edit and needs a P gate before it is normative. Every `nucleus-store` card (Phase A) and the storage side of every `nucleus-txn` card that touches trees (B-1c apply, B-1d provisional entries, B-3 GC, B-4d bulk load, B-6 drop cleanup) implements against this file. A change to an invariant (`I-*`, §15) is made here first, then in the tests and mutants that pin it, and only then in code.

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
- **Writes are copy-on-write.** Map pages are mutated through `write_guard` like every other page (§5.6). A dirty map page is written to a fresh slot (I-NO-INPLACE), and its parent's ref is then set in memory in two steps, so no thread latches a map page's parent while it holds that page's latch (§5.3). Under the page's latch the writer writes the bytes, keeping the frame pinned and in the page table; it then releases the page's latch, latches the parent exclusively and sets the ref. An eviction drops the frame only after that, and only if no one has dirtied or pinned it meanwhile: the pin keeps a second eviction off the frame, and the page-table entry keeps a lookup from reloading the old slot. A dirty eviction holds the gate across both steps (§5.2), so no flip lands between them. The writer holds no other map latch when it latches the parent, because no frame is obtained by eviction under a map latch: `write_guard`'s copies come from the reserve, and a MapLeaf is pinned or created before any map latch is taken (§5.6). Step 2 builds the image map's parents in buffers of its own (§13 step 2), so only current refs are set this way.

### 4.2 LPID allocation
- **Allocation.** Take the lowest free LPID; if none is free, take `next_lpid++`.
- **The free-set lock.** It guards the LPID bitmap's in-memory state and `next_lpid` only, despite its name: slot allocation (§4.4, §4.5) does not take it, and neither does eviction. It ranks after the gate and any frame latches: a thread may take it while holding those, and may take the page table inside it, never the reverse, so a thread holding the page table never takes the free-set lock. Under it a thread only tests and sets LPID bits, advances `next_lpid`, and tests and pins resident MapLeaf frames (§5.6): no I/O, eviction or frame latch runs under it, it creates no page, and it waits on nothing but the page table. So a dirty eviction, which allocates a slot and updates the page table while holding frame latches (§5.2), never meets it, and an allocation that finds a MapLeaf cold or missing releases it before doing anything about that (§5.6).
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
2. the image being written (the epoch-`k` map and its image-record list, §13 step 2, during checkpoint `k`);
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
- **Frame.** Holds the page bytes, a task-fair latch (§5.3), a dirty flag, a pin count and the content epoch.
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
- **Dirty victims from the previous epoch.** If the victim's content epoch is `k < e` during checkpoint `k` (after the flip, before step 2 has written that page), the write's record is also appended to the image-record list (§13 step 2) before the victim's latch is released, and the page counts as written for image `k`.

A dirty page may therefore be evicted **at any time**, independently of checkpoints. This is what lets write-heavy loads and long-reader version retention exceed RAM (PLAN-v3 §3.3).

### 5.3 Latch rules
- **Order.** Latches are acquired in one order: directory pages, which only an operation that changes the directory latches (§6.8 "Root names"); then the pages of one tree, top-down, and among siblings left to right (I-LATCH, §6.2); then map pages, in radix order (a MapInner before its children, siblings by ascending LPID range). Nothing ever latches sideways or upward: a thread blocks only on a latch ranked after every latch it holds, map pages ranking after every tree page. Among map pages two rules make that hold: a claim of an epoch copy takes no map latch, since only step 2 writes the image map (§5.6 "Reserve", §13 step 2); and a map page's write releases that page's latch before it latches the parent (§4.1).
- **Latch scope.** A thread holds frame latches only inside one storage call, and only on one tree's pages. Before the call returns, and before it starts an access to any other tree, it releases every frame latch and every pin it holds. Two exceptions add no wait edge: a `try_latch` acquirer (an eviction victim and its page-local prune, §5.2 and §10.3; an epoch-copy claim, §5.6), which never blocks on a tree latch and under a victim's latch takes only map latches; and a stale root name's first latch (§6.8 "Root names", step (2)), which may land on another tree's page while the thread holds no other latch. Directory pages count as no tree's here: a directory change latches its directory path and then at most one tree's pages, as the order says. No gated operation latches pages of two trees: apply, settle, convert, the GC sweep and index maintenance each work on one tree per gated operation (N2), and a cross-tree check such as an index scan's per-entry verification (C-T6 §8) is a separate call.
- **Fair latches.** A frame latch is task-fair: a waiting exclusive request blocks new shared grants; a waiter is overtaken by at most a bounded number of later requests, or for a bounded time; and `try_latch` never succeeds over a queued exclusive waiter. Strict FIFO handoff is not required, since it convoys on a write-hot root. `std::sync::RwLock` promises no policy and does not qualify. `parking_lot`'s RwLock blocks new readers behind a writer but unlocks unfairly by default, so A-3b checks the bypass bound against the latch type and version it pins.
- **No recursive latch.** No thread latches a page it already holds, in either mode. A task-fair latch deadlocks on a recursive shared acquisition behind a queued writer; the latch scope makes one impossible, since no access starts under a latch.
- **No I/O while holding an exclusive latch on an inner node.** An operation that needs a non-resident page releases its latches, pins the page (loading it), and restarts from the root, re-validating what it saw (PLAN-v3 §3.3). Pins guarantee residency, not identity, so validation re-reads the parent under its latch.
- **A reader's hold includes no page read.** A reader that needs a non-resident page releases its latches, pins the page and re-descends, as an inner exclusive holder does (above), so a reader's latch hold is CPU-bounded and a gated writer queued on it waits for no I/O. A writer's hold of its leaf may still include a read: on a leaf root, a chain build or free at `ROW_LIMIT` reads MapLeafs in place under the root's exclusive latch (§6.7), which A-5 measures.
- A frame's latch is never held across a wait on another transaction.
- **No latch escapes a call.** No store API returns a borrow into a frame, no latch guard outlives the call that took it, and a storage call never yields (an `await`, or an N10 cooperative step boundary) while it holds a latch. `!Send` guards do not enforce this on a single-threaded executor, so `t_latch_scope`'s tracker checks it at every yield point.
- **Why latching cannot deadlock.** Every holder waits only down the order. A latch-free thread holds nothing, or only the gate while it waits on a latch, and a latch holder never waits on the gate (§5.6), so it is on no cycle. A cursor is parked between calls and holds nothing (§6.6), so no thread reads under a cursor's latches. No thread holds latches of two trees except under the two exceptions above, and neither adds a wait edge. A gated writer latches within one tree, top-down, after the gate. A pin-only holder has no latch edge. A directory change holds the directory path and then at most one tree's pages; the directory ranks first and no ordinary operation latches it, so it waits only on that tree's brief holders. A `try_latch` acquirer never waits on a tree latch, and under a victim's latch it takes only map latches, which rank last. The checkpointer's step 2 waits on a latched frame holding no latch, map latches included (§13); it takes a current MapLeaf's latch only after a written frame's latch, and in the map phase it holds one latch at a time. Among map pages a thread blocks only toward a later radix rank: a claim takes no map latch, since only step 2 writes the image map (§5.6 "Reserve"), so two `write_guard`s that exhaust the reserve while holding MapLeafs never wait on each other's MapLeaf; and a map page's write releases the page's latch before it latches the parent (§4.1). At the pool floor, a thread that holds a latch never waits for an unpin: with no evictable frame it releases its latches and restarts (§5.2), and a reader pins with no latch held (above). A thread that waits for an unpin therefore holds no latch, and its wait is finite only because §5.4 counts every pin any thread can hold, a gate-waiter's restart pins included; that term is card S-1-F7-F2-F2-F2's, so this argument depends on it, and that card lands before A-3b. With one order, no recursive acquisition and no latch held across a call, fair blocking latches add no cycle.
- **The fairness bound.** A waiter of either mode is granted once the holders present at its arrival and the waiters queued ahead of it have released, plus the bounded bypass, and each hold is bounded by one call's work on one tree, which includes no transaction wait: a batch fill, bounded by A-4e's per-call bound (§6.6); a descent step, CPU-bounded for a reader (above); a writer's critical section, which on a leaf root includes a chain build or free bounded by the maximum value size (§6.7, measured by A-5); and a frame acquisition's victim write. This covers writers and readers alike, every holder shape and writers among themselves, and nothing is exempt. A gated writer blocked on a root delays a pending flip by at most the holds ahead of it on that latch, each bounded as above (at most one per thread), plus the bounded bypass, as §5.6 already permits for a page read.

### 5.4 Pins and the minimum pool size
**Minimum size.** `F ≥ threads × (max tree height + 2) + R_EPOCH + R_MAP`:
- height + 2 covers the latch-coupled path plus the two siblings of a pack step;
- `R_EPOCH` is the epoch-copy reserve (§5.6);
- `R_MAP` covers the MapLeaf pins of §4.1, bounded by `⌈dirty frames / 510⌉`.

**Below the minimum,** the open is refused with 53200. A thread that cannot pin a frame because every frame is pinned waits for an unpin. The minimum makes that wait finite. A parked cursor holds no latch and no pin (§6.6), so each thread counts one operation's path and open cursors add nothing to `F`. The wait is finite only if this formula counts every pin a thread can hold while it waits on the gate (§5.3 "Why latching cannot deadlock").

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
- **Acquisition.** An operation acquires the gate before its first latch, never twice, and releases it after its last latch. Nested work, such as an eviction or a page-local prune inside an insert, runs under the outer hold. A thread that holds latches and no gate never blocks on the gate (§5.2).
- **Restart.** An operation restarts only before its first mutation; an LPID allocation counts as one. That restart (§5.3) releases its latches and the gate, pins what it needs and re-acquires the gate. A flip may land in the gap, which is harmless because the operation has changed nothing. After its first mutation an operation never restarts and never releases the gate before its end. So whatever it would otherwise read under an inner page's exclusive latch (§5.3), such as the MapLeaf of an LPID it frees, it pins before that mutation. It may read a page in place only with no such latch held, such as a chain's next page under its leaf's latch (§6.7). A split's reservation precedes its prune: the prune is a mutation (§10.3), so the pages a split latches and the MapLeafs of the LPIDs it allocates are pinned before it (§6.4). The prune frees no LPID (§10.3), so the reservation covers only the split's own pages and allocations. An LPID allocation takes its LPID under the free-set lock only if that LPID's MapLeaf is resident, and pins it before the lock is released. Otherwise it restarts, which is legal only before the first mutation. An LPID with no MapLeaf yet, one that `next_lpid++` would take past the last MapLeaf, counts as not resident. The restart creates and pins its empty MapLeaf in a gated operation of its own, with no other latch held, before it re-acquires the gate for the retry. It obtains the MapLeaf's frame first, holding no latch, because obtaining a frame may evict (§5.2), so no eviction or I/O runs under the MapInner's exclusive latch (§5.3) or the free-set lock (§4.2). Only then does it latch exclusively the MapInner that holds the MapLeaf's ref, and it creates the MapLeaf in that frame only if the ref is still null. Otherwise another restart created it first, and that restart's retry may already have mapped an LPID there, so replacing the ref would lose the mapping (I-LPID-EXACT): this restart releases the unused frame and pins the MapLeaf the ref names, releasing the MapInner's latch first if pinning must load it (§5.3). An empty MapLeaf maps no LPID, so creating it changes no image's mapped set and is not an allocation. A restarted allocation keeps the MapLeaf pins its restart took and, on its retry, takes the lowest free LPIDs that lie under MapLeafs it holds pinned, before any other; under a pinned new MapLeaf, the LPIDs from `next_lpid` on count as free. That is the only exception to §4.2's lowest-first order. The arm makes progress because a pinned MapLeaf stays resident and a free only adds LPIDs: the retry restarts again only if other allocations took free LPIDs under its pins meanwhile, leaving fewer than it needs, and the rest lie under non-resident MapLeafs. Each such restart pins at least one more MapLeaf and drops the pin of any MapLeaf left with no free LPID, so an operation that takes `n` LPIDs holds at most `n` such pins, within §5.4's `R_MAP`. The bound in restarts: with no concurrent allocation and no change to its path, an allocation restarts at most once in all; each further restart is charged to LPIDs another operation took or to a change another operation made to its path, so some operation always progresses and the arm cannot livelock. An operation that allocates several LPIDs, such as a split that also splits its parent, takes them in one hold of the free-set lock, and only if every one's MapLeaf is resident, so its allocations are one mutation and its restart stays legal until they are taken. An allocation made after the operation's first mutation must have reserved its LPID and MapLeaf before that mutation, or must run with no inner exclusive latch held, reading a non-resident MapLeaf in place as above once it has released the free-set lock, as a chain build does (§6.7). Mutants: DROP-RESTART-MID-OP, DROP-RESTART-AFTER-REMOVE, UNLINK-RESTART-AFTER-UNLINK (§6.8), CHAIN-BUILD-SPLIT, REPLACE-FREE-UNDER-P (§6.7), SPLIT-RESERVE-AFTER-PRUNE, ALLOC-READ-UNDER-P, SPLIT-RESERVE-ALWAYS (§6.4), DELETE-FREE-UNDER-P, CHAIN-BUILD-UNDER-P (§6.7), ALLOC-RETRY-LOWEST, ALLOC-NEW-MAPLEAF-IN-PLACE (§5.6).
- **Waiting.** Holding the gate shared across a page read is permitted; the flip waits for it.
- **What this guarantees.** Every multi-page change is wholly in epoch `k` or wholly in epoch `k+1` (I-CKPT-AT-T).

**I-LINK-ATOMIC.** Every gated operation leaves the reference graph closed. An LPID becomes mapped in the same gated operation that inserts its first reference, and unmapped in the same gated operation that removes its last, after removing it. The references are a parent's child pointer, an entry's chain reference, an Overflow page's next pointer, and a directory entry's root. So every image, at every cut, satisfies I-LPID-EXACT with no special case, and §4.2's immediate reuse never hands out an LPID a mapped page or a directory entry still names.

A freed page's own references end with it, so an operation that frees a path frees it top-down. A thread's name for a page is not a reference: §6.2's coupling, §6.6's re-descent and §6.8's root-name rule keep it from outliving the latch that gave it.

**`write_guard(frame)`** is the only way to mutate a page.
- **When it copies.** It copies only if the frame's content epoch `< e`, **and** that content is not yet at a slot the image map references (the page is dirty with respect to image `k`). In that case it first copies the bytes into an **epoch-copy frame**, which belongs to the checkpointer.
- **After the call,** the frame's content epoch is `e`.
- **Reserve.** Epoch copies come from the `R_EPOCH` reserve. When the reserve is exhausted, `write_guard` dirty-evicts the oldest epoch copy it can claim (§5.2) and reuses its frame. It claims a copy with `try_latch` on the frame's latch, never a blocking acquire (§5.2), and moves to the next-oldest copy if that latch is not free, so two writers never evict the same copy. The claim writes the copy to a fresh slot and appends its record to the image-record list (§13 step 2) before it releases the copy's latch. It takes no map latch and calls no `write_guard`, because only step 2 writes the image map. So a claim waits on no latch and claims never nest: a writer that holds a MapLeaf's latch in `write_guard` can claim a copy whichever MapLeaf maps the copy's page. The copy's install is under the latch of the page it preserves. **Bound:** `R_EPOCH` exceeds the copy frames that can be latched at once, namely one claim per thread plus the copies step 2 is writing (§13). So an unclaimed copy always exists, a claim always succeeds, and a writer never waits holding a page latch. R-1 sets the number (§17). Checkpoints therefore never need pool frames proportional to the dirty set, and the image-record list is O(F) (§5.7). Mutant **EXHAUST-DOUBLE-EVICT**: two writers evict one copy, and the second clears the frame the first refilled. Mutant **EXHAUST-BLOCKING-CLAIM**: the claim blocks on the frame's latch. Mutant **CLAIM-LATCHES-MAP**: a claim records its copy by latching the MapLeaf that maps the copy's page. Mutant **IMAGE-RECORD-AFTER-RELEASE**: a claim appends its record after it releases the copy's latch. Test `t_epoch_reserve_two_exhausters` (A-3c, with A-4c): two first touches exhaust the reserve together; both copies reach the image. Its cross variant: two writers each hold a MapLeaf's latch in `write_guard`, A and B, and exhaust the reserve together, every claimable copy being of a page that the other's MapLeaf maps; both finish, both copies reach the image, and a hook asserts that no claim takes a map latch.

### 5.7 Store memory outside the pool
These consumers do **not** grow with the version count or with retention. Each is accounted separately to R-1 (per-consumer accounting, X-4 R2 item 2):

| Consumer | Bound |
|---|---|
| page table | O(F) |
| the root table (§6.8) | O(trees): one entry per directory entry |
| cursor batch buffers (§6.6) | A-4e's per-call bound per open cursor; R-1 accounts them per session and sets a cap or a spill policy |
| MapInner pages | ≈ 1/510 of the MapLeaf count |
| the two free-set bitmaps | 1 bit per slot plus 1 bit per LPID |
| the pending-free lists (§13 step 6) | ≤ the pages written in one checkpoint interval |
| the image-record list and the map phase's buffers (§13 step 2) | ≤ F records, one per page whose epoch-`k` content was dirty at the flip; one map page's buffer per radix level, for the deepest radix a `u64` LPID allows; reserved at open (R-1), so a checkpoint allocates none of it; emptied once the map phase has applied them |
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
- **Writers** use latch coupling (crabbing) with exclusive latches: they release every ancestor once the latched child is *safe*, meaning it has room for one more maximum-size cell (E0's T-COUPLING). Only an operation that changes the directory latches a directory page (§6.8 "Root names"), and it couples down the directory in the same way.
- **Readers** use shared latches, top-down. They hold the parent while they latch the child.
- **Order.** Within one tree, latches are taken in (depth ascending, then left to right among siblings); directory pages come before any tree page and map pages after, in §5.3's order.

The consequence that §4.2 and §6.6 rely on: no thread can reach a page except through a latched parent or a re-descent from the root, so an LPID unlinked under its parent's exclusive latch is unreachable afterwards.

### 6.3 Stable root (I-ROOT-STABLE)
A tree's root LPID is fixed when the tree is created and never changes. This departs from E0, which kept the root in a `RwLock<PageId>` and used a T-ROOT restart.
- **Root split:** move the root's cells into two new pages and rewrite the root as an inner page with two children.
- **Root collapse:** when the root is an inner page with one child, copy the child's content into the root and free the child.

**Why.** The tree directory (§6.8) never changes on a split or a collapse, readers need no root re-check, and the root never moves between images.

### 6.4 Split
A leaf that cannot take an insert first runs the **page-local prune** (§10.3). A split happens only if the prune did not free enough room. The prune frees no chain (§10.3), so it reads no page under the inner latches the insert holds.
- **Separator.** The split separator is the shortest byte string `s` with `last(left) < s ≤ first(right)`. Under (b), a split may fall inside a group, so `s` may include ts bytes.
- **Split-point preference.** A leaf split **should** prefer a group boundary within the middle third of the page. That makes more groups page-local, which is what page-local GC can prove things about (§10.3), and a latest-snapshot read of a key then touches one leaf.
- **Prefix truncation.** Inner-node prefix truncation is card A-4d.
- **Atomicity.** A split is all-or-nothing: E0's T-ATOMIC carries over. Every page it needs is reserved before the first mutation, including the MapLeafs of the LPIDs it allocates, and before the prune (§5.6). The insert decides whether it splits from the prune's marks, before the prune mutates: under the leaf's exclusive latch it marks what the prune would delete, with the `w` the prune will use, and splits only if the insert's cell does not fit with the marked entries, and any cell it replaces, counted as gone. A split's pages and allocations are then reserved, or the insert restarts, before the prune's mutation, which applies exactly those marks. An insert that does not split reserves nothing. Mutants SPLIT-RESERVE-AFTER-PRUNE, ALLOC-READ-UNDER-P and SPLIT-RESERVE-ALWAYS (§15 I-LINK-ATOMIC).
  - *The decision is exact.* The prune reads nothing outside its leaf and frees no chain or LPID (§10.3), so its marks are a function of the latched leaf and `w` alone. The leaf stays exclusively latched from the marking to the prune's mutation, so the marks cannot change in between. A restart releases the latch, so the marks are recomputed after the re-descent.
  - *Unconditional reservation is rejected.* An LPID allocation is a mutation (§5.6), so an insert that reserved before it knew it splits would allocate before the prune and, when the prune made room, would have to free an LPID it never linked, in the same gated operation. Every insert into an unsafe leaf would also pin the next free LPID's MapLeaf, and restart on a cold one, for a split that does not happen.
  - *Deciding after the prune is SPLIT-RESERVE-AFTER-PRUNE.* An insert that learns it must split only after its prune has mutated can no longer restart, so a non-resident page or MapLeaf would be read under `P`'s exclusive latch.

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
- **A delete that descends from the root under latch coupling** (a directory removal, but not DropTree's last step, §6.8) may pack the affected leaf with its right neighbour, or its left neighbour if it is the rightmost child, when `used < U/4`, while it still holds the parent. The two leaves are latched left to right: to pack with the left neighbour, it releases the leaf and, still holding the parent exclusively, latches the neighbour and then the leaf.
- **A delete made while holding only a leaf** (the page-local prune, §10.3, including at eviction) never packs. Taking the parent's latch from below would break I-LATCH. It leaves the page for the next sweep.
- **Inner pages** whose `used` falls below `U/4` after a pack removed a child are packed under their parent, by a **re-descent** that latches the parent first. Nothing ever latches a parent after its child (I-LATCH). After a delete, they pack the same way as the leaf (the right neighbour, or the left one for a rightmost child). **The sweep uses its own rule instead** (§10.4 step 5): an inner page packs only with its right neighbour, and a junction walk follows. A left pack would make the left neighbour's former last child, which was exempt and may be underfull, a middle child beside a page the sweep has already passed, and nothing would pack the two again, so I-SPACE would fail (§11).
- **The root** collapses when it is an inner page with one child (§6.3).

Under N3 = (b), **logical deletes are tombstone inserts** (§7). Only GC, DropTree, overflow freeing and provisional-entry settling (C-T0 v8 §2.4) ever remove entries or pages.

### 6.6 Cursors (I-CURSOR)
- **Within a call.** A cursor may hold its root-to-leaf stack shared, or less, while one storage call runs; card A-4e decides how much. Recommended: release the ancestors once the leaf is latched, and reach the next leaf by re-descent.
- **Parking.** Before a storage call returns, a cursor parks: it releases every latch and every pin, and keeps only its tree id, its registered snapshot `S` and its parked position. It resumes through a fresh, validated root name (§6.8) and a re-descent from the root at its parked position, never by a remembered LPID. A parked cursor holds nothing, so an open cursor adds nothing to §5.4's minimum and delays no writer. It keeps no pin, because a pin kept across user-paced calls would make §5.4's minimum depend on the number of open cursors, and with MVCC a pin buys no correctness.
- **Why parking is exact.** A cursor resumed at its registered `S` returns exactly what a latch-held scan would: entries at or below `S` are stable (C-T0 v8 §3.6, I-GC-READEQ); cursors and portals keep `S` registered until they close (C-T0 v8 §3.6); chains are read under the registration (§6.7); and a parked cursor's tree cannot be dropped, since the drop needs `W > D` while the cursor's `S < D` holds `W` (C-T6 §7.5). Under SER, C-T0 v8 §8.1 registers a scan's SIREAD over its full bound and checks the lock table before the tree read, so each key's tree read still follows `L`, and §8.3's case analysis is unchanged by parking.
- **The parked position.** Either *after group `k`*, which resumes at `upper(k)`, or *inside group `k`, after entry key `x`*, which carries the group's state: the head's candidate version, if any (§8, "Candidate rule"), and whether the newest version `≤ S` was already found. It mirrors the sweep token (§10.4). A resume inside a group continues after `x` with that state, so a finalize that converts the head meanwhile changes nothing it returns.
- **A bound per call.** Card A-4e bounds the entries a call examines, skipped versions included (§8.3's seeks). At the bound a cursor parks. A point read (§8.1) or a newest probe (§8.2), which also step over versions above `S` under SER (C-T0 v8 §8.1), releases its latches at the bound and re-descends at its parked position with the same carried state, inside the same call. So no latch hold exceeds the bound (§5.3, "The fairness bound").
- **Batches.** A batch is copied out under the leaf latch: at most A-4e's bound per open cursor, held outside the pool (§5.7). Chain values stay references and are read after the latch is released, under the registered `S` (§6.7, "Reading without the leaf latch"). A batch holds chain references only for a cursor that holds a registration, which every cursor does, and never holds own-transaction entries, whose layer comes from the write set (C-T0 v8 §4 step 1). A cursor's batch buffer is freed when it closes, a close by `ROLLBACK TO` included (C-T0 v8 §5.5).
- **Reverse and scroll.** No sibling links exist, so a reverse cursor reaches the previous leaf by releasing its latches and re-descending, never by latching leftward while it holds a leaf. A parked position holds in both directions: a resume in the other direction (a SCROLL cursor's FETCH PRIOR) starts at the same boundary and skips and repeats no key against the oracle in that direction; card A-4e maps SQL's cursor positions onto it.
- **Safety.** This is what keeps merges, frees and LPID reuse safe for scans that do not hold latches throughout.

Iterators are **streaming**: a range scan never materialises its range. The spike's whole-range materialisation is a spike limit (`verdicts/X-2.md` non-blocking finding 3), not a behaviour.

### 6.7 Overflow chains
An entry whose value exceeds `V_INLINE` stores a reference `(len u64, first LPID)` in its value, with tag 4 or 5 (§7.3). The chain is a list of Overflow pages.
- **Immutability.** Chains are immutable once written (versions are immutable).
- **Building.** An insert descends, latches its leaf and makes room for the reference, any split included, before it maps the chain's first page. So its restarts (§5.6) all lie before the build, and no restart strands a mapped chain. A chain is mapped in the gated operation that inserts its entry, as it is freed with its entry (I-LINK-ATOMIC, §5.6). The build holds only the leaf: once the insert has made room, any split's separators installed, it releases `P` and every other ancestor, and only then allocates the chain's LPIDs and maps its pages, reading a non-resident MapLeaf in place (§5.6). It does not reserve them before its first mutation instead, because a chain at the maximum value size may need more MapLeafs than §5.4's quota pins. An insert that both builds a chain and frees a replaced one (Freeing, below) releases `P` once, before both. Mutant CHAIN-BUILD-UNDER-P (§15).
- **Freeing.** A chain is freed **in the same gated operation** that deletes its entry (§10), or that replaces its entry's value: a provisional rewrite (C-T0 v8 §2.4) or a superseding replay op (§14), or that settles an aborted or dead tag-8 entry inside a writer's operation on `k` (settlers (b) and (c), C-T0 v8 §2.4), unless the new value is the same chain. **Exception:** a conversion of a tag-8 provisional entry (C-T0 v8 §2.4) moves the chain to the new tag-4 or tag-5 entry and deletes the provisional entry without freeing it. Both happen in one gated operation, so no image holds two references to one chain. The free holds only the entry's leaf in every gated operation outside the sweep that frees a chain: an insert that replaces or settles a chain-bearing entry, settler (a)'s abort cleanup (C-T0 v8 §7.1), `T`'s delete on `ROLLBACK TO` (C-T0 v8 §5.5), and a replayed `Delete{revert}` of a tag-8 entry (§14). An insert releases `P` and every other ancestor once any split has installed its separators, and only then replaces the entry and frees the old chain. Such a delete never packs: it releases `P` and every other ancestor before it deletes, so it is a delete made while holding only a leaf (§6.5). It loses no pack by that: coupling kept `P` only because the leaf had no room for a maximum-size cell (§6.2), so the leaf is more than three quarters full (§3.2), and one delete leaves it above `U/4`. Each reads the chain's pages and their MapLeafs in place (§5.6), as the sweep's chain deletes do (§10.4). It cannot pin them before its first mutation instead, because learning a chain's LPIDs reads its pages, and a chain may be longer than §5.4's quota. The free stays in the replacing operation: a separate operation would leave the old chain mapped with no reference at a cut (I-LINK-ATOMIC). Mutant REPLACE-FREE-UNDER-P (§15). Mutant DELETE-FREE-UNDER-P (§15). A chain build or free therefore holds the flip for the chain's length. A-5 measures the flip hold per gated operation, not only per chain: a chain's build and free at the maximum value size (`ROW_LIMIT` for the row layer, C-T6-rehost §4.3), at every allowed page size; a replace, which builds one chain and frees another in one gated operation; and, with cold (non-resident) chains, the gated operations of a GC sweep and of a DropTree walk over a toast-region leaf and over a leaf of maximum-size values (§6.8, §10.4). If any is too long, A-5 proposes a resumable build or free, which is an edit of this file (§19 item 6).
- **Reading without the leaf latch.** A reader that holds a registered snapshot `S ≥ W` may read the chain after releasing the leaf latch. GC never deletes a version that is the newest `≤ S` for any registered `S` (I-GC-READEQ), so a version that a registered snapshot reads cannot lose its chain while the reader holds that registration.

### 6.8 Tree directory
- **What it is.** A system B+tree, unversioned, with a stable root at `dir_root`. Its entries map `tree_id (u64 BE) → (root LPID u64, kind u8, state u8)`.
- **Tree ids** are C-T0 §10 storage ids, never reused. The WAL names trees only by tree id (§12).
- **States:** `live` or `dropping`.
- **DDL visibility** lives in the versioned catalog rows (C-T5), not here.
- **The root table.** An in-memory map `tree_id → (root LPID, kind, state)`, one entry per directory entry, never persisted. It is a name, not a reference: no image holds it, and I-LINK-ATOMIC's reference list is unchanged. It is built at open (§14 step 2) and changed only by a directory change, inside that change's gated operation, while it holds the directory leaf exclusively. It has no generation counter: tree ids are never reused and an entry only goes `live` → `dropping` → absent, so comparing (root LPID, kind, state) is exact, and no unrelated directory change makes a reader retry. A root-name read performs no store to memory common to all trees, so the table adds no shared cache line to an ordinary operation; A-7b chooses the mechanism. If it is lock-based, its lock is a leaf lock: nothing is acquired under it, and it ranks like the page table and the free-set lock (§4.2).

**CreateTree(id)** descends the directory, then allocates a root leaf and inserts the directory entry, in one gated operation (I-LINK-ATOMIC, §5.6). Its restarts (§5.6) all lie before the allocation. It allocates by the restart arm of §5.6's allocation rule: it takes the root's LPID, with those of any directory split (§6.4), in one hold of the free-set lock only if each one's MapLeaf is resident, pins them before the lock is released, and otherwise restarts; no mutation precedes the allocation, so that restart is legal. It is idempotent: if the id already exists, nothing happens. It installs the tree's root-table entry in the same gated operation, after the root page is initialised and while it holds the new root's latch exclusively.

**DropTree(id)**
1. Set the entry's state to `dropping`, in one directory mutation. The same mutation sets the root-table entry's state.
2. Free the tree's pages bottom-up in bounded gated steps, each reached by a fresh descent from the directory (root names, below):
   - **Chains first.** A chain-bearing entry is deleted with its chain (§6.7) in its own gated operation, holding only its leaf, before the step that unlinks that leaf. So one gated operation frees at most one chain and holds the flip for that chain's length (§6.7), no inner page is latched across the chain's reads (§5.3), and no later step frees a chain. A leaf this empties stays legal until it is unlinked (§6.1).
   - **Unlink.** A step unlinks a child that holds no chain reference from its parent and frees it, in one gated operation. A parent the step empties is freed in that same operation and unlinked from its own parent. Every page it latches, and the MapLeaf of every LPID it frees, is pinned before its first mutation (§5.6), so it reads no page after that mutation and frees at most one path of pages.

   A root's parent is its directory entry, so a step that empties the root is the last step (step 3).
3. **The last step frees the root and removes the directory entry in one gated operation.** A leaf root's chains went first (step 2), so it frees no chain.
   - **Before its first mutation,** it descends the directory and pins the directory leaf, the root and every page it empties (at most height + 1 pages, within §5.4's quota), and the MapLeaf of every LPID it frees (`R_MAP`). A non-resident page restarts it, and that restart releases the gate harmlessly (§5.6). So the flip waits for at most one directory descent over pinned pages, and no page read.
   - **In one hold,** it latches top-down from the directory leaf to the pages it frees, removes the entry first, and frees the root and any emptied pages after it, top-down (I-LINK-ATOMIC). It removes the tree's root-table entry after the directory entry, while it holds the root's exclusive latch, and before it frees the root; "Root names" below depends on this order. Once the entry is gone, the pages are unreachable, so a concurrent allocation may take a freed LPID at once (§4.2). The last step does not pack: its pins cover the directory leaf and the tree path only (§5.4), and latching the directory neighbour after the root would break I-LATCH. A later gated operation, or the directory's own sweep, may pack the leaf.

**One walk per tree.** At most one walk runs per tree: a DropTree of a tree whose walk is running does nothing. The claim on the walk is in memory and is not persisted, so a crash loses it and §14 step 6 starts each `dropping` entry's walk once.

**Root names.** A thread names a tree's root only through the root table, and validates the name under the root's latch. (1) It reads the tree's entry; if the entry is absent, or `dropping` where its operation needs `live`, it stops without naming a page. (2) It pins the entry's root LPID and latches the root frame, blocking on its fair latch (§5.3), holding no other latch except, for a directory change, its directory leaf. DropTree's last step may therefore block on the root while it holds the directory leaf exclusively: only directory changes can queue behind it, and the root's only possible holder then is a stale-name thread's brief latch, since sweeps skip `dropping` trees, cleanup deletes stop on `dropping`, and one walk runs per tree. A pin on this path that finds the LPID unmapped is a retry from (1), never XX001 or poison, and the latch may land on a page that is no longer the tree's root, even another tree's page: until (3) the thread reads nothing of the page but its header `id` (§3.1), which it checks against the LPID. A mismatch is not acted on before (3), since a stale name can latch a frame whose new owner has not yet initialised it: if the re-read finds the entry changed, the mismatch is a retry from (1), and only a mismatch under an unchanged entry is XX001. (3) Under that latch it reads the entry again: if the entry is unchanged, the name is valid for this latch hold; otherwise it releases the latch and the pin and starts again at (1). (4) It keeps no root LPID across gated operations, restarts or a cursor's release (§6.6). Validation is exact. DropTree's last step holds the root's exclusive latch from before it removes the root-table entry until after it frees the root, and the root's LPID can be reused only after that free (§4.2). If the thread latches the frame DropTree latched and the grant comes after DropTree releases it, the release is ordered after the removal and before the grant, so the re-read sees the removal; if the grant comes first, DropTree cannot latch the root until the thread releases it, so the LPID is still the tree's root throughout the hold. If the LPID has been reused, in that frame or another, the thread's pin finds the reuse's mapping and is ordered after the free: the free and the reusing allocation change the LPID's bit under the free-set lock (§4.2), and the pin acquires the mapping the reuse installed, so the re-read, after the pin, sees the removal. Splits and collapses never change an entry (I-ROOT-STABLE), so an entry changes only on CreateTree, DropTree and a state change. Only an operation that changes the directory (CreateTree, DropTree, a state change) latches a directory page; it couples down the directory as §6.2's writers do. So C-T6 §7.5's checks are not check-then-act: a sweep step or a cleanup delete reads the entry's state and the root's name in one table read and reads both again under the root's latch, a `dropping` or absent entry stops it before it names a page, and no root name survives into a later step, where §4.2's reuse could have changed what it names. Keeping a root name across operations would be an edit of this rule (§19 T5).

At open, every `dropping` entry's walk resumes (the B-6 boot job; C-B1 "drop cleanup = freeing pages"). DropTree is idempotent: dropping an absent id does nothing. Every image therefore holds either the entry with its root mapped, or neither. A crash mid-walk leaks no page and names no freed LPID (I-LPID-EXACT, I-LINK-ATOMIC). At open, an unmapped root of a `dropping` entry is corruption like any other unmapped reference (XX001); §14 step 6 needs no other arm.

**When the drop runs** (after `W` passes the DDL) is C-T0 v8's rule (v7.8 §9.2).

**I-LPID-EXACT** holds of every image, crash images included; "quiescent" (§15) describes amcheck's reading, not an exemption for crash images. The mapped LPIDs of an image are exactly:
- the pages reachable from the directory roots, a `dropping` entry's root among them, including overflow chains (each referenced by exactly one entry);
- the directory's own pages.

Mutants (§15 I-LINK-ATOMIC): **DROP-ENTRY-SPLIT** (the root free and the entry removal in two gated operations), **DROP-UNLINK-SPLIT** (a child freed one gated step before its unlink), **CREATE-TREE-SPLIT** (the root mapped one gated step before the entry), **DROP-RESTART-MID-OP** (the last step mutates, meets a non-resident directory page, and releases the gate on the restart), **DROP-RESTART-AFTER-REMOVE** (the last step removes the entry, meets the evicted root or MapLeaf, and restarts, releasing the gate), **UNLINK-RESTART-AFTER-UNLINK** (an unlink step unlinks a child, meets the evicted MapLeaf of its LPID, and restarts), **DROP-LAST-STEP-PACKS** (the last step packs the directory leaf with its neighbour), **DROP-FREE-BEFORE-REMOVE** (inside the hold, the root is freed before the entry is removed), **DROP-LEAF-CHAINS** (a step frees a leaf's chains with the leaf), **DROP-SECOND-WALK** (a DropTree starts a second walk beside a running one), **ROOT-NAME-KEPT** (a sweep keeps a root LPID across gated operations and latches it after its reuse), **ROOT-NAME-UNVALIDATED** (no re-read under the root's latch), **ROOT-UNPUBLISH-LATE** (DropTree removes the root-table entry after releasing the root's latch), **ROOT-PIN-UNMAPPED-FATAL** (a pin of an unmapped LPID on the root-name path raises XX001), **ROOT-DECODE-BEFORE-VALIDATE** (the thread reads the latched page as the tree's root before the re-read), **ROOT-TABLE-GLOBAL-GEN** (validation compares a table-wide counter that every directory change bumps), **ROOT-HEADER-MISMATCH-FATAL** (a header `id` mismatch before the re-read raises XX001). Mutants (§15 I-LATCH): **DIR-LATCH-ON-READ** (an operation that does not change the directory latches a directory page).

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
- **delete** every older version, freeing its overflow chain in the same gated operation (§6.7). The sweep deletes a chain-bearing version in its own gated operation, holding only its leaf, and only while it is still the same entry (§10.4, "Chains first").

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
  - **elide** a newest-`≤ w` tombstone only when an entry of a **different key follows its group on the same leaf**, which proves the group ends here, **and** none of the group's older versions holds a chain. The same mutation then deletes the group's older versions. Otherwise the tombstone is kept, and the prune may still delete the group's non-chain older versions, each shadowed by the tombstone (I-GC-READEQ). *On the leaf* covers the whole group: the tombstone is the newest `≤ w`, versions are in ts-descending order (§7.2), and the group ends here, so every older version of the group follows the tombstone on this leaf.
- **Chains.** The prune frees no chain. It deletes and elides only entries that hold no chain reference: tags 0–3 (by the two rules above) and dead tags 6–7 (§10.2). It never deletes a tag-4, tag-5 or tag-8 entry, dead or not, and so never maps, unmaps or reads an Overflow page. Chain-bearing garbage is left to the sweep's "Chains first" (§10.4), which deletes each such entry in its own leaf-only gated operation. The rule holds at every call site: before a split, on any exclusively held leaf, and at eviction, including nested under another operation's hold (§5.2).
- **What it must not do (I-GC-LOCAL).** It never deletes or elides on the strength of an entry it cannot see. In particular, the leaf's first entry is never pruned as "not newest" unless a newer `≤ w` entry precedes it on this leaf: its group may continue from the left. Mutant: PRUNE-FIRST-ENTRY.

The page-local prune is incomplete by design: a group that spans leaves keeps its garbage until the sweep reaches it, and so does every chain-bearing entry.

**Why the prune frees no chain.** Freeing a chain reads each of its pages for the next pointer (§10.4, "Why not under `P`"). Before a split the leaf is full, so it is not safe (§6.2), and the splitting insert still holds `P`, and possibly more ancestors, exclusively; at eviction the prune may run nested inside another operation's hold (§5.2). A chain free there would read pages under an inner exclusive latch (§5.3) after the operation's first mutation, when it can no longer restart (§5.6), and one insert into a toast-region leaf of cold chains would do thousands of such reads while the flip waits. A prune that frees no chain reads no chain page and frees no LPID, so it does no I/O under an inner latch, holds the flip for no chain, and needs no MapLeaf pins, at every call site. The insert stays live: §6.4 is unchanged, so if the prune frees too little room, the insert splits, and nothing waits on the sweep. The cost is space: chain-bearing garbage on a leaf stays until the sweep, and a toast-region leaf full of it splits where a chain-freeing prune would have made room. That is B-3's bound to hold (I-SPACE is read after a complete sweep, §11; trigger T2), not a correctness question. *Rejected:* a splitting insert that deletes each chain-bearing prunable entry in its own leaf-only gated operation, then restarts. It would put up to a leaf's worth of cold chain frees in front of a foreground insert, add a second chain-deleting path with its own identity check (C-T0 v8 §2.4 (c)) and liveness argument beside the sweep's, and could not serve the prune at eviction.

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
3. **Delete.** Delete the marked entries, in this step's mutation. None of them holds a chain, so the mutation frees no chain:
   - **Chains first.** If a marked entry of `L` or `R` holds a chain reference (tags 4, 5 and 8, §7.3), the step ends before its first mutation, so it has changed nothing and the flip may land after it (§5.6). The sweep then deletes each such entry with its chain (§6.7), in key order, each in its own gated operation, holding only its leaf: its descent releases the leaf's parent once it has latched the leaf, because a delete never grows the leaf, and it never packs (§6.5). Then the step runs again from the same token.
     - *Each delete is conditional on the entry's identity* (C-T0 v8 §2.4 (c)), checked under the leaf's exclusive latch, because the latches are released between the step's end and the delete. An older version is identified by its entry key `k@ts` (I-ENTRY-UNIQUE; no insert re-creates a `ts ≤ w` key, §10.1). A tag-8 entry sits at `entry_key(k, u64::MAX)`, which every provisional entry on `k` shares, so it is deleted only if its bytes still show it dead. In the gap a writer on `k` may settle the dead entry (settler (c)) and write its own current-boot provisional entry at that key: the delete finds a current-boot entry and leaves it, and its chain, alone. No entry becomes dead during a boot (below), so a dead entry found there is the one the step marked. If the entry is gone, or is no longer the same entry (a writer deleted it), the operation does nothing. Mutant CHAIN-DELETE-BY-KEY (§15): the delete matches the entry key alone, so it deletes the writer's live entry and frees its chain.
     - *Why not under `P`.* Freeing a chain reads each of its pages for the next pointer. Under `P`'s exclusive latch that is I/O §5.3 forbids, and after the step's first mutation it cannot restart (§5.6). One step could also free up to two leaves' worth of chains, and on a toast-region leaf nearly every entry holds one. So one gated operation frees at most one chain and holds the flip for that chain's length (§6.7), and no inner page is latched across a chain's reads (§5.3).
     - *Why it is read-equivalent.* Each such entry is an older version or a dead provisional entry (§10.2), never a kept version and never a tombstone, which has no chain. So its own delete is a GC mutation that I-GC-READEQ allows by itself: the newer version that shadows it, or the tombstone, stays. An elided tombstone is still deleted after every older version of its group (I-GC-ORDER), and each chain is freed after its entry, in the same gated operation (I-LINK-ATOMIC).
     - *Why the re-runs end.* The carried state is unchanged, because a deleted older version is neither a key's newest `≤ w` version nor a pending tombstone. Nothing creates an entry the step could mark as chain-bearing garbage, for two reasons. No insert creates an older version, since none creates an entry with `ts ≤ w` (§10.1). And no entry becomes dead: §10.1's reason excludes provisional entries, but dead means written in a previous boot (C-T0 v8 §2.4), so every provisional entry written while the sweep runs is current-boot, stays current-boot for the rest of the boot, and is never marked (§10.2). So the chain-bearing entries past the position that the step can mark form a finite set that only shrinks, each round removes from it every entry it marked (deleted, found gone or found replaced), and concurrent writers cannot keep the step from its mutation. A leaf the deletes empty is legal (§6.1), as one the step's own mutation empties is.
   - **The tombstone's group ends within `L` or `R`:** a newest-`≤ w` tombstone t is deleted in the same mutation as the remaining older versions of its group. The chain-bearing ones went first, each in its own gated operation, so the step's mutation, and t's delete in it, comes only after every one of them is gone (I-GC-ORDER; mutant STEP-MUTATE-BEFORE-CHAINS, §15). Within one gated operation under exclusive latches, the order is invisible.
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

**Pages (I-LINK-ATOMIC).** §13's Exception exempts a load's *entries* from I-CKPT-AT-T, not its pages from I-LINK-ATOMIC (§5.6). Each load step links what it maps, so a discarded build (item 4) can be walked (§6.8) and leaks nothing. Mutant **LOAD-PAGE-SPLIT**: a page mapped one gated step before its parent's pointer. Test `t_load_every_cut` (B-4d, §15 I-LINK-ATOMIC).

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

**Step 2 (image).** The checkpointer writes image `k`, each page to a fresh slot (§4.4). Only step 2 writes the image map, the epoch-`k` map pages. Every write of epoch-`k` content made after the flip outside the map phase, by step 2's data phase, by a claim (§5.6) or by an eviction (§5.2), appends `(page, slot, crc)` to the **image-record list** before it releases the written frame's latch; a page is an LPID, or a map page's position in image `k`'s radix, counted from the leaf level and keyed apart from LPIDs, so that no record for a map page can collide with one for an LPID and a radix level added in epoch `k+1` (S-1-F7-F2-F2-F1) changes no record's meaning. "The image map" in §5.2 and §5.6 means the epoch-`k` map pages with the list applied. The list lives in memory outside the pool, reserved at open (§5.7), under a leaf lock: under it a thread only appends or reads records, with no I/O and no other lock taken. A crash before meta `k` loses the list together with image `k`, which no open uses, and every slot it names was allocated after meta `k−1`, so it is free in image `k−1` (I-FREE-EXACT). Meta `k` is written only after the map phase has applied the list. Step 2 writes, in order:
- every page other than a map page whose epoch-`k` content is not already at a slot the image map references: the dirty frames with content epoch `≤ k`, and the epoch copies (§5.6). Pages that eviction already wrote during epoch `k`, or wrote after the flip as epoch-`k` content (§5.2), are adopted as they are;
- when a written frame was not mutated in epoch `k+1` and its page was not already written for image `k` (the image-record list holds no earlier record for it), its new slot also becomes its current home. The data phase decides both under the frame's latch, and it does not write a page whose epoch-`k` content the list already records: adopting such a write anyway would treat the earlier write's slot as current-only and reusable (§4.4) while image `k`'s map still names it;
- then the **map phase**, once the data phase has ended and no write of epoch-`k` content is in flight, whether a claim (§5.6), a §5.2 previous-epoch victim or a data-phase write: it waits on each such write's latched frame, holding no latch, so the list is complete when it starts, and an append for a page other than a map page after it has started is a debug assertion failure. It writes the epoch-`k` MapLeaf and MapInner pages that changed since image `k−1` or that the list's records name, in post-order (children before their parent). Each is built in a buffer of the checkpointer's own from the page's epoch-`k` content, with the list's records for its entries applied. Its source is decided under that source's latch and re-checked: its epoch copy, if under the copy's latch it is still that page's epoch copy; else its frame, if under the frame's latch its content epoch is `≤ k`; when it is `k+1`, the map phase checks for the epoch copy again, since one may have been installed meanwhile; else the slot the list records for it, which append-before-release guarantees exists once its copy or frame has been written; else its epoch-`k` home. A source is read into the buffer, never loaded into the page table, which names only current frames. The built page is written to a fresh slot, and that slot is recorded in its parent's buffer, up to the root. The map phase holds at most one latch at a time. A map page whose epoch-`k` content it has taken counts as written for image `k` (§5.6 "When it copies"). A slot the list records for a map page it rewrites leaves §4.4 item 2 once the new one is written; it is reusable only if no other reference of §4.4 holds it, since it may be the page's current home. The map phase never makes a slot a map page's current home: only a write of the page's live frame does, as §4.1 says. Image `k`'s slot bitmap is derived from its built map, not from the list (I-FREE-EXACT);
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

Step 2's data phase ends only when no epoch-`k` content is outstanding: it waits on a latched frame (§5.6's claim and install) (mutant STEP2-NO-WAIT, §15). It waits holding no latch, map latches included. It records a data write in the image-record list, not in a map page; it latches a current MapLeaf only after the written frame's latch, to set a current home; and in the map phase it holds at most one latch at a time (§5.3) (mutant CKPT-WAIT-HOLDING-MAP, §15).

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
2. Load the map root and the free set of image `k`. Every slot not referenced by image `k` is free, including the slots eviction wrote after meta `k`. Then build the root table from image `k`'s directory (§6.8), before replay.
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

   **DropTree.** A replayed `DropTree` sets `dropping` and does nothing else (nothing if the id is absent). Its walk runs at step 6. Pass 2 runs no walk. Mutant **REPLAY-DROP-WALK**: pass 2 walks, so a read-only open frees a page (§14.1; test `t_ro_open` with a replayed `DropTree`, A-7d). Its state change includes the root-table entry's, and a replayed `CreateTree` installs its entry, as in normal operation (§6.8).

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

**What it replays.** Steps 1–4, including the open-time meta check (step 1) and the coverage check (step 3a): the same winning meta (§2.2), the same image, the same valid end (§12.5), or the splice cut (§14 step 3a), the same two passes, and the same ops, with I-CHUNK-SUPERSEDE. It builds the root table as §14 step 2 does, and its replayed directory changes maintain it.
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
| I-CURSOR | §6.6: a cursor parks before each storage call returns, holding no latch and no pin, and resumes by re-descent through a validated root name (§6.8) at its parked position, never at a remembered LPID; a parked position is after a group, or inside one with the group's carried state. Test `t_cursor_park_equiv` (A-4e, with A-4b packs racing): a scan parks after every entry while writers add versions above `S`, a GC sweep packs and merges its leaves, and another tree is dropped and recreated on reused LPIDs; its output equals the oracle scan at `S`, and in the SER variant so do its recorded edges. A mid-group variant: a group spans several leaves, its head is a provisional entry whose owner is `Committed(c)` with `c ≤ S`, finalize is held back, and the per-call bound fires while the scan skips the versions above `S`; a point-read variant runs a SER point read over the same group, finalize still held back, with a bound smaller than the group. A scroll variant fetches forward, parks, fetches backward, parks and fetches forward again; each fetch equals the oracle's from the same boundary. A close variant closes a parked cursor through `ROLLBACK TO`; its batch buffer is freed and R-1's per-session account returns to its value before the open. CURSOR-STALE-LPID (resume by a remembered LPID) fails it; PARK-DROPS-CANDIDATE (a mid-group park keeps only the resume key) fails the mid-group and point-read variants, which return a version older than `c`; SCROLL-RESUME-SKIP (a direction change resumes past the boundary) fails the scroll variant; BATCH-KEPT-AFTER-CLOSE (a closed cursor's batch buffer is not freed) fails the close variant | A-4e (with A-4b packs racing) | CURSOR-STALE-LPID, PARK-DROPS-CANDIDATE, SCROLL-RESUME-SKIP, BATCH-KEPT-AFTER-CLOSE |
| I-TREE | §6.1: key order and subtree bounds after every SMO | A-4a/A-4b (proptest vs `BTreeMap`) | SPLIT-OFF-BY-ONE, SEP-NOT-UPDATED, PACK-SEP-STALE |
| I-LATCH | §5.3/§6.2: coupling; one order (directory pages only by a directory change, then one tree's pages by depth and left to right, then map pages by radix rank), in which a thread blocks only on a latch ranked after every latch it holds, a claim takes no map latch and a map page's write never holds the page while it latches the parent; one call and one tree per latch hold, with two stated exceptions (`try_latch` acquirers, a stale root name's first latch); task-fair latches with bounded bypass; no recursive latch; no page read in a reader's hold. §6.8 "Root names": only an operation that changes the directory latches a directory page, and a root name comes from the root table and is validated under the root's latch. Test `t_dir_root_no_stall` (A-7b, with A-4c's shuttle harness), a regression for the directory leaving the hot path: trees X, D and Y have their entries in one directory leaf. A chain operation holds X's leaf root; a sweep step of D that named D's root while D was `live` is paused under that root's latch, while DropTree(D)'s last step holds the directory leaf exclusively and is held up on D's root; a CreateTree queues on that leaf. A reader and a writer of Y finish within the step budget under every schedule, and a hook asserts that no operation that does not change the directory latches a directory page. DIR-LATCH-ON-READ (an operation that does not change the directory latches a directory page) fails it through the hook, deterministically. Test `t_freeset_lock_order` (A-3c, with A-4c's shuttle harness): allocations pin MapLeafs under the free-set lock while dirty evictions allocate slots and a pool at its minimum forces eviction; its fixture evicts the MapLeaf of the lowest free LPID, and a variant has no free LPID and `next_lpid` at a MapLeaf boundary. A hook asserts no page read, eviction, page creation or frame latch taken while the lock is held (the allocation already holds `P`'s), and no take of the free-set lock by a thread holding the page table, and every schedule finishes. FREESET-LOCK-IO (an allocation creates or reads a MapLeaf under the lock) and FREESET-LOCK-INVERT (a dirty eviction takes the free-set lock while holding the page table) fail it. Test `t_latch_scope` (A-4c): a per-thread latch tracker asserts, at every storage-call return and every yield point, that the thread holds no latch and no pin; at every blocking acquisition of a tree page, that the thread holds no latch of another tree; and at every blocking acquisition, that the thread holds no latch ranked at or after the one it requests (§5.3 "Order": map pages rank after every tree page, and pages of different trees are unranked). Fixtures: an index scan with table lookups, a self-join behind a queued writer, two cursors that each read the other's tree, and two threads that each hold a leaf exclusively and read the other's tree. A reverse variant runs a reverse scan across a left-to-right pack. A map variant runs a pool at its minimum through a checkpoint, so that dirty MapLeafs are evicted and step 2's map phase runs, while two writers that each hold a MapLeaf's latch in `write_guard` exhaust the reserve; a hook pauses one MapLeaf's eviction after it releases the MapLeaf's latch and before it sets the parent's ref, while another thread looks up an LPID whose entry in that MapLeaf changed since its old slot. In the same pause the evicted MapLeaf is a previous-epoch victim (§5.2) of checkpoint `k`, and step 2's map phase builds it from its frame; a hook asserts that the map phase sets no current ref. A checkpoint variant has step 2 wait on a frame whose holder records into a MapLeaf. A floor variant runs a pool at its minimum, a gated writer queued on a root with a flip pending, and a holder of that root that needs a frame. Every schedule finishes. CURSOR-LATCH-ACROSS-CALL and CURSOR-PIN-ACROSS-CALL (a cursor keeps a latch or a pin past the call's return) and LATCH-ACROSS-YIELD (a call yields holding a latch) fail it through the tracker; NESTED-READ-UNDER-LATCH (a thread reads another tree, or re-latches its own, while holding latches) fails it through the tracker and deadlocks the two-leaf and self-join fixtures; REVERSE-LATCH-LEFT (a reverse cursor latches the previous leaf while holding its leaf) deadlocks the reverse variant; MAP-PARENT-UNDER-CHILD (a map page's write latches its parent while holding the page) fails the map variant through the tracker; MAP-DROP-BEFORE-REF (an evicted map page leaves the page table before its parent's ref is set) fails the map variant: the paused lookup reloads the old slot and returns a stale entry; MAP-REF-SET-STALE (the map phase makes the slot it writes a map page's current home) fails the map variant through the hook, and the paused eviction's ref then names a slot already reusable; CKPT-WAIT-HOLDING-MAP (step 2 waits on a frame while holding a map latch) deadlocks the checkpoint variant; POOL-FLOOR-BLOCK (a thread holding a latch waits for an unpin instead of releasing and restarting) deadlocks the floor variant. Test `t_root_latch_fair` (A-3b, with A-4c's shuttle harness): a stream of overlapping readers holds tree X's root shared while a writer queues; the writer is granted within a bypass budget, a bound with slack on the later requests granted before it, not their arrival order, so a strict-FIFO latch passes too. A writer variant runs a stream of writers on X's root while a reader queues; the reader is granted within the same budget. A latch-level variant drives the latch type directly: with a writer queued on a frame that a reader holds shared, `try_latch` in either mode fails, including at the instant the reader releases. A cold variant has a reader under X's root meet a non-resident child while a gated writer queues on the root with a flip pending; a hook asserts that no reader reads a page while it holds a latch, and the flip completes within the step budget. LATCH-READER-PREF (a waiting writer does not block new shared grants) fails the reader stream; LATCH-WRITER-PREF (writers always overtake waiting readers) fails the writer variant; TRYLATCH-BYPASS (`try_latch` succeeds over a queued exclusive waiter) fails the latch-level variant; READER-IO-UNDER-LATCH (a reader loads a page while holding its parent's latch) fails the cold variant through the hook | A-4c (loom/shuttle, `t_latch_scope`), A-3b (`t_root_latch_fair`), A-3c (`t_freeset_lock_order`), A-7b (`t_dir_root_no_stall`) | COUPLING-EARLY-RELEASE, PACK-RIGHT-FIRST, DIR-LATCH-ON-READ, FREESET-LOCK-IO, FREESET-LOCK-INVERT, CURSOR-LATCH-ACROSS-CALL, CURSOR-PIN-ACROSS-CALL, NESTED-READ-UNDER-LATCH, LATCH-ACROSS-YIELD, REVERSE-LATCH-LEFT, MAP-PARENT-UNDER-CHILD, MAP-DROP-BEFORE-REF, MAP-REF-SET-STALE, CKPT-WAIT-HOLDING-MAP, POOL-FLOOR-BLOCK, LATCH-READER-PREF, LATCH-WRITER-PREF, TRYLATCH-BYPASS, READER-IO-UNDER-LATCH |
| I-ROOT-STABLE | §6.3: a tree's root LPID never changes | A-4a/A-4b (reopen test) | ROOT-MOVES |
| I-PACK | §6.5: entries preserved; merge iff it fits in `F_PACK·U`; never splits | A-4b | PACK-DROP-LAST, PACK-OVERFILL |
| I-ENTRY-UNIQUE / I-REPLAY-IDEM | §9.1, §14: identical re-insert (chains compared by content) is a no-op, a different one XX001; recovering twice gives the same state | B-1c, A-7d (incl. the double-crash test; an overflow value in a partially applied commit) | REPLAY-DUP-CONFLICT, DUP-DIFFERENT-ACCEPTED, CHAIN-LPID-COMPARED |
| I-CHUNK-SUPERSEDE | §12.7, §14: within one chunked transaction the last op per key wins, `revert` deletes; replay twice gives the same state | A-7d, B-1d | SUPERSEDE-AS-CONFLICT |
| I-KEY-SIZE | §3.2: an entry key over `K_MAX` is refused with 54000 at write-set entry; apply never sees one | A-2 (formula and values), B-1a (the write path calls the check) | SIZE-CHECK-OFF |
| I-FAIL-STOP | §9.2: a 58030/53100/53200/XX001 inside a gated operation, a checkpoint step, a background store job or the WAL path (a WAL write or `sync_data`, a segment's creation, preallocation, recycling rename or `wal/` directory sync, recovery's step-5 truncation) poisons the store; a client read's XX001 never does. The mark is made before the first release, and every acquisition checks it. Every blocking wait, the lock-table waits included, has it in its predicate, under the wait's own mutex, and sleeps by releasing that mutex in one step, re-testing on every wake. The broadcast comes after every latch, pin, the gate, the settle stripe and every internal mutex are released, and reaches every wait queue, each live transaction's waiter list included. After poison, every call fails, every waiter returns, and nothing is written. Test `t_gated_xx001_poisons`: a damaged referenced leaf on an apply's path; every commit waiter returns the error within the test's step budget, and no task is left parked. Its **lock-waiter variant**: a txn waits in the lock table on a key held by a commit that sits behind the poisoned apply, so the holder is never released; the waiter must return the error (POISON-LOST-WAKEUP and POISON-WAITER-PARKED leave it parked; pinned in B-1c and B-1b). Its **WAL variant**: the group-commit writer's `sync_data` fails (58030); the store is poisoned and every waiter on `durable_end` returns the error. WAL-ERR-NOT-POISON (an error on the WAL path does not poison the store) fails it. Test `t_client_read_xx001_no_poison`: a client read of that leaf gets XX001, then an unrelated commit and a checkpoint both succeed. Test `t_poison_before_unlatch` (shuttle): a reader that races the failing SMO gets the error, never the half-changed page; so does an evictor's `try_latch`. Test `t_poison_wakeup_race` (shuttle; A-4c, B-1c): a waiter tests its predicate concurrently with the poisoner's mark and broadcast; under every schedule it returns the error. POISON-LOST-WAKEUP (a wait predicate omits the mark, so a woken waiter re-tests only its own condition and sleeps again) fails it, and so does a broadcast made before the mark | B-1c (commit waiters, COMMIT's error, the WAL variant); B-1b, the lock-table card, with B-2b for `wait_for` (the lock-waiter variant); A-3c, A-4c (the latch and `try_latch` check, shuttle) | POISON-IGNORED, XX001-NOT-POISON, CLIENT-READ-POISONS, POISON-WAITER-PARKED, POISON-AFTER-UNLATCH, POISON-LOST-WAKEUP, WAL-ERR-NOT-POISON |
| I-GC-READEQ | §10.2: every GC mutation is read-equivalent at every S ≥ the published W, including a sweep's own delete of each chain-bearing entry (§10.4, "Chains first"). Test `t_sweep_step_shape` (B-3): a sweep over a tree of height ≥ 2 with a toast-region leaf and a leaf of maximum-size values, each holding superseded chain-bearing versions and dead tag-8 entries next to non-chain garbage, with every chain evicted, reads at `S ≥ W` between its gated operations, and a hook that counts each gated operation's chain frees and, at each chain page read, checks the latches the thread holds. The fixture also holds (a) an elided-tombstone group that ends within the step's `L` and `R` and whose older versions hold chains, with no version above `w`, read at `S ≥ W` between each two of the gated operations that delete its entries; and (b) a writer, run by a hook between the step's end and the delete of a dead tag-8 entry on key `k`, that settles that entry (settler (c)) and writes its own current-boot tag-8 entry on `k`, then stays `Open`. An operation frees at most one chain, no chain page is read under an inner page's latch, the step that packs frees no chain, every read is unchanged, the writer's entry and its chain survive the sweep (its owner reads its own value, and amcheck finds the chain mapped and referenced), and every read of the tombstone's key at `S ≥ W` finds no version. SWEEP-CHAINS-UNDER-P (a step deletes its chain-bearing entries and frees their chains in its own mutation, under `P`'s latch) fails it through the hook. CHAIN-DELETE-BY-KEY (the delete of a marked chain-bearing entry matches its entry key alone, with no identity check) fails it through (b): it deletes the writer's entry and frees its chain. STEP-MUTATE-BEFORE-CHAINS (the step deletes its non-chain marks, the ending group's tombstone among them, in its mutation, and only then deletes the chain-bearing ones) fails it through (a): a read between the tombstone's delete and an older version's sees that version | B-3 (+ G0 re-point, B-7) | GC-DROPS-VISIBLE, G-WATERMARK+1, G-RESURRECT, SWEEP-CHAINS-UNDER-P, CHAIN-DELETE-BY-KEY |
| I-GC-ORDER | §10.2/§10.4: an elided tombstone is deleted after every older version of its key, including the chain-bearing ones a sweep deletes in their own gated operations (§10.4, "Chains first") | B-3 (a group spanning leaves, read between sweep steps or across a checkpoint and crash; `t_sweep_step_shape`'s fixture (a), I-GC-READEQ row) | G-TOMB-FIRST, STEP-MUTATE-BEFORE-CHAINS |
| I-GC-COMPLETE | §10.2: after a quiescent sweep, at most one version ≤ w per key, and it is live | B-3 (the §10.2 directed test, with a reader) | GC-KEEPS-TOMB |
| I-GC-LOCAL | §10.3: page-local prune acts only on what the leaf proves; it frees no chain and elides a tombstone only when no older version of its group holds one. Test `t_prune_split_shape` (B-3): a tree of height ≥ 2 whose target leaf is a full toast-region leaf. Its superseded chunk versions (tag 4) each have a newer `≤ w` version on the leaf, and the leaf also holds dead tag-8 entries, an elided-tombstone candidate group that ends on the leaf with a tag-4 older version, non-chain garbage, and every chain evicted. An insert into that leaf prunes and splits. A hook counts each gated operation's chain frees and LPID frees and, at each page read, checks the latches the thread holds. The splitting insert frees no chain and no LPID and reads no Overflow page. Every chain of a pruned-around version stays mapped and referenced (amcheck). A read of the tombstone's key at `S ≥ W` after the prune still finds the tombstone. A sweep afterwards deletes each chain-bearing entry in its own gated operation, and I-GC-COMPLETE holds after it. Its eviction variant runs the prune at eviction of the same leaf, nested inside another tree's gated insert, with the same checks. PRUNE-FREES-CHAINS (the prune deletes a chain-bearing version or a dead tag-8 entry and frees its chain in the splitting insert's gated operation) fails it through the hook: it counts a chain free, and an Overflow page read under `P`'s exclusive latch. PRUNE-ELIDES-OVER-CHAIN (the prune skips a group's chain-bearing older version and still elides the group's tombstone) fails it through the read of the tombstone's key at `S ≥ W`, which finds the older version (I-GC-READEQ, I-GC-ORDER). PRUNE-EVICT-FREES-CHAIN (only the split-time prune skips chains; the prune at eviction still frees them, under the outer operation's hold) fails its eviction variant: the hook counts a chain free inside the outer insert | B-3 | PRUNE-FIRST-ENTRY, PRUNE-FREES-CHAINS, PRUNE-ELIDES-OVER-CHAIN, PRUNE-EVICT-FREES-CHAIN |
| I-SPACE | §11: quiescent fill bound at every height, except rightmost children and the reported `skips`; B-3's W4-shape acceptance ≤ 1.5× in tree bytes (reachable pages × `page_size`) | A-4b (bound, incl. trees of height ≥ 4), B-3 (acceptance) | PACK-OFF, SKIP-UNCOUNTED, SWEEP-PACK-LEFT, CASCADE-ONE-LEVEL, JUNCTION-SKIP |
| I-GCW | §10.5: meta `gc_w` bounds the image's GC and is `≤ T` | A-7c, B-3 (restart floor) | GCW-NOT-RECORDED, GCW-BOUND-SKIP |
| I-LPID-EXACT | §6.8: in every image, crash images included, mapped LPIDs = reachable pages; no leak on pack, drop or chain free. "Quiescent" describes amcheck's reading, not an exemption | A-8 amcheck, A-4b, B-6, A-7d | LEAK-ON-PACK, DROP-ROOT-FIRST |
| I-LINK-ATOMIC | §5.6, §6.7, §6.8, §12.8, §14 step 4: every gated operation leaves the reference graph closed. An LPID is mapped in the gated operation that inserts its first reference and unmapped in the one that removes its last, after removing it, so every image satisfies I-LPID-EXACT with no special case. An operation restarts only before its first mutation. DropTree deletes each chain-bearing entry with its chain in its own gated operation; its last step removes the entry, then frees the root, in one gated operation; one walk runs per tree; a root LPID is taken under its directory leaf's latch and not kept; pass 2 runs no walk. The walk's code is A-7b's; A-7d's tests call it directly, and B-6 runs it as the drop job and the boot resume. A split reserves its pages and its allocations' MapLeafs before its prune and decides from the prune's marks whether it splits (§6.4); an allocation pins a resident MapLeaf or restarts (§5.6); a replace, a settle or a chain-freeing delete outside the sweep frees its old chain holding only the leaf (§6.7). Test `t_drop_every_cut` (A-7d, B-6): a flip at every boundary between a DropTree's gated steps, with and without a concurrent split that takes the freed LPID. Its fixture holds chain-bearing entries in a non-root leaf and in a leaf root, an evicted directory page with a flip pending, an evicted root and an evicted MapLeaf of an LPID the walk frees, each with a flip pending, in the last step and in an unlink step, a running sweep of the tree paused between its steps until the walk ends, and a second DropTree during the walk. Every image passes amcheck and opens, no other tree loses a page, and one walk runs. DROP-ENTRY-SPLIT (directory root, unmap; E4's probe: 28 of 51 C-XX rounds), DROP-UNLINK-SPLIT (child pointer, unmap), DROP-RESTART-MID-OP (the flip pending lands in its restart), DROP-RESTART-AFTER-REMOVE and UNLINK-RESTART-AFTER-UNLINK, which are DROP-RESTART-MID-OP's realistic form (a forgotten pin: the flip lands in the restart, and the image fails I-LPID-EXACT, amcheck finding a mapped, unreachable page), ROOT-NAME-KEPT (the paused sweep then prunes another tree's page) and DROP-SECOND-WALK fail it. At A-7d the paused sweep is a test-only stand-in step, which follows §6.8's root names; `t_drop_step_shape` runs on the same fixture and shares it. ROOT-NAME-KEPT applied to the stand-in proves the fixture only. At B-6 both tests re-run with B-3's real sweep, and ROOT-NAME-KEPT is applied to B-3's sweep code and must fail there. A-7d's kill does not count toward the sweep. Test `t_drop_step_shape` (A-7d, B-6): on the same fixture, a hook counts each gated operation's frees and page reads, and at each LPID free reads the pages the operation holds. An operation frees at most one chain, an unlink step or a last step frees none and reads no page after its first mutation, and no held page or entry names the LPID being freed. DROP-LEAF-CHAINS and DROP-FREE-BEFORE-REMOVE fail it. Its fixture also holds a directory leaf that the removal leaves below `U/4`, and the hook asserts that the last step latches no directory page beside its own leaf; DROP-LAST-STEP-PACKS fails it. Test `t_split_reserve_order` (A-4a): a full leaf with prunable non-chain garbage, too little to fit the insert's cell, under a height-≥ 2 tree, the MapLeaf of the next free LPID evicted, and a flip pending; a variant whose split also splits `P` evicts the MapLeaf of its second LPID. A hook at each gate release and each page read asserts that no restart follows a mutation and no page is read under an inner exclusive latch. SPLIT-RESERVE-AFTER-PRUNE (the split prunes, then decides to split or reserves, meets the evicted MapLeaf and restarts, releasing the gate) and ALLOC-READ-UNDER-P (the allocation reads its evicted MapLeaf in place under `P`'s exclusive latch) fail it through the hook. Its second variant has a full leaf whose marks free just enough room for the insert's cell: the hook asserts that the insert allocates no LPID and pins no MapLeaf. SPLIT-RESERVE-ALWAYS fails it there: an insert into an unsafe leaf allocates and pins its split's reservation before it knows it splits. Test `t_alloc_restart_progress` (A-4a, with A-3c): a split whose allocation restarts; during each restart a hook frees a lower LPID under an evicted MapLeaf. The split finishes within the restart bound §5.6 states. A variant has no free LPID, `next_lpid` at a MapLeaf boundary and every MapLeaf evicted; the hook asserts no page is read or created under an inner exclusive latch. ALLOC-RETRY-LOWEST (the retry takes the lowest free LPIDs afresh) fails the first; ALLOC-NEW-MAPLEAF-IN-PLACE (the allocation creates the new MapLeaf under `P`'s exclusive latch) fails the variant. A race variant restarts two allocations on the same missing MapLeaf, with a pool at its minimum so that obtaining a frame evicts a dirty page: the first creates the MapLeaf, and its retry maps an LPID there before the second latches the MapInner; the second finds the ref set, pins that MapLeaf and maps the next LPID. Every image passes amcheck, both LPIDs stay mapped, and the hook asserts that no page is evicted or read under the MapInner's exclusive latch. MAPLEAF-CREATE-UNCONDITIONAL (the second creates a MapLeaf anyway and replaces the ref) fails it through amcheck (I-LPID-EXACT); MAPLEAF-FRAME-UNDER-INNER (the frame is obtained after the MapInner is latched) fails it through the hook. Test `t_replace_free_shape` (A-5, with A-4a's split): under a height-≥ 2 tree, a full leaf whose owner rewrites its provisional tag-8 entry with a larger value (so the leaf is unsafe and splits), and a second unsafe leaf where the rewrite fits without a split; the old chain's pages and the MapLeafs of its LPIDs evicted, and a flip pending. Variants: a writer on `k` settles a dead tag-8 entry with a chain (settler (c)), and a superseding replay op replaces a chain-bearing entry. Delete variants, each on a full leaf under the same height-≥ 2 tree with the tag-8 entry's chain pages and their MapLeafs evicted and a flip pending: settler (a)'s abort-cleanup delete, `T`'s `ROLLBACK TO` delete, and a replayed `Delete{revert}` of a tag-8 entry. A build variant, under the same height-≥ 2 tree: the rewritten entry's old value is inline, so nothing is freed; its new value needs a chain, and the rewrite splits the leaf but not `P`. The split takes the last LPID its MapLeaf covers, every lower LPID mapped, so the next free LPIDs the new chain takes lie on later MapLeafs, each evicted, unpinned and mapping no dirty page (§4.1), with a flip pending. Those MapLeafs are the only pages the variant reads cold after the split. A hook at each page read and each gate release asserts that no page is read under an inner exclusive latch and no restart follows a mutation, and every image passes amcheck. REPLACE-FREE-UNDER-P (the insert frees the old chain while still holding `P`) fails it through the hook: a chain page or MapLeaf is read under `P`'s exclusive latch. DELETE-FREE-UNDER-P (a chain-freeing delete outside the sweep frees its chain while still holding `P`) fails a delete variant through the hook: a chain page or MapLeaf is read under `P`'s exclusive latch. CHAIN-BUILD-UNDER-P (a chain build after a split keeps `P` and reads its allocations' MapLeafs under it) fails the build variant through the hook: an evicted MapLeaf is read under `P`'s exclusive latch. Test `t_create_every_cut` (B-6): the leak direction; under CREATE-TREE-SPLIT (directory root, map), I-LPID-EXACT fails after recovery. Test `t_chain_every_cut` (A-7d): a flip at every boundary between the gated operations that insert, replace and delete chain-bearing entries, with an evicted leaf and a flip pending; every image passes amcheck and opens. CHAIN-BUILD-SPLIT (chain reference, map: a chain mapped one gated step before its entry, such as a build before the descent and then a restart), CHAIN-FREE-SPLIT (chain reference, unmap: a chain freed one gated step after its entry's delete), CHAIN-NEXT-MAP-SPLIT (next pointer, map: an Overflow page mapped one gated step before its predecessor's pointer) and CHAIN-NEXT-FREE-SPLIT (next pointer, unmap: an Overflow page freed one gated step before its predecessor) fail it. Test `t_load_every_cut` (B-4d): a flip at every boundary between a load's gated steps, then a crash; every image passes amcheck, and walking the discarded build leaks nothing. LOAD-PAGE-SPLIT (child pointer, map) fails it. Test `t_ro_open` with a replayed `DropTree` (A-7d): under REPLAY-DROP-WALK, a read-only open frees a page. Test `t_root_name_reuse` (A-7b, with A-4c's harness): tree X has root `r`, and no LPID below `r` is free, so §4.2's lowest-first order hands `r` to the next allocation; a fixture check asserts that the reusing tree's root or page is at `r`, and an oracle records each LPID's owning tree at every allocation and free. Case A: thread A reads X's entry and a hook stops it before it pins `r`; DropTree(X) runs to completion, CreateTree(Z) takes `r` as Z's root, and A resumes. Case B: A names X's root in a test-only mode that accepts `dropping`, a stand-in as A-7d's paused sweep is; the hook is in DropTree(X)'s last step, after it releases the root's latch and before the operation ends, and there a split of tree Y, in its own gated operation, takes `r` before A names X's root. Case C: as case A, but `r` is freed and not reused, so A's pin finds `r` unmapped. Case D: as case A, but `r` is reused as a non-root page of tree Y, and in a variant as an Overflow page of Y. In a second variant a hook pauses Y's split after `r` is mapped, to a frame that last held another LPID's page, and before Y initialises that page; A latches the frame there if the allocation path allows it, and otherwise A's latch is granted only after Y initialises the page, so the variant is case D. A churn variant has a hook run one CreateTree of another tree to completion between A's read and its re-read, on each of A's attempts; A validates on its first attempt and finishes within the step budget. An exploration runs A, in both modes, with DropTree(X) and a split of Y that can take `r`, under shuttle with no hooks. The property is checked only when A's validation succeeds: the oracle says `r` is mapped and is X's root and X's entry is present, and A never reads or mutates a page the oracle assigns to another tree. Between its latch and its re-read A reads nothing of the page but its header `id`, and no case raises XX001 or poisons the store. ROOT-NAME-UNVALIDATED (no re-read under the root's latch) fails case A; ROOT-UNPUBLISH-LATE (DropTree removes the root-table entry after releasing the root's latch) fails case B, where A validates `r` as X's root after Y took it; ROOT-PIN-UNMAPPED-FATAL (a pin of an unmapped LPID on the root-name path raises XX001) fails case C; ROOT-DECODE-BEFORE-VALIDATE (the thread reads the latched page as X's root before the re-read) fails case D; ROOT-HEADER-MISMATCH-FATAL (a header `id` mismatch before the re-read raises XX001) fails case D's second variant where A latches the frame before Y initialises it; ROOT-TABLE-GLOBAL-GEN (validation compares a table-wide counter that every directory change bumps) fails the churn variant, where A retries without bound | A-4b, A-7b, B-4d, B-6, A-7d, A-8, A-8a | DROP-ENTRY-SPLIT, DROP-UNLINK-SPLIT, CREATE-TREE-SPLIT, DROP-RESTART-MID-OP, DROP-FREE-BEFORE-REMOVE, DROP-LEAF-CHAINS, DROP-SECOND-WALK, ROOT-NAME-KEPT, CHAIN-BUILD-SPLIT, CHAIN-FREE-SPLIT, CHAIN-NEXT-MAP-SPLIT, CHAIN-NEXT-FREE-SPLIT, LOAD-PAGE-SPLIT, REPLAY-DROP-WALK, DROP-RESTART-AFTER-REMOVE, UNLINK-RESTART-AFTER-UNLINK, SPLIT-RESERVE-AFTER-PRUNE, ALLOC-READ-UNDER-P, DROP-LAST-STEP-PACKS, SPLIT-RESERVE-ALWAYS, REPLACE-FREE-UNDER-P, DELETE-FREE-UNDER-P, CHAIN-BUILD-UNDER-P, ALLOC-RETRY-LOWEST, ALLOC-NEW-MAPLEAF-IN-PLACE, MAPLEAF-CREATE-UNCONDITIONAL, MAPLEAF-FRAME-UNDER-INNER, ROOT-NAME-UNVALIDATED, ROOT-UNPUBLISH-LATE, ROOT-PIN-UNMAPPED-FATAL, ROOT-DECODE-BEFORE-VALIDATE, ROOT-TABLE-GLOBAL-GEN, ROOT-HEADER-MISMATCH-FATAL |
| I-POOL-BOUND | §5.1/§5.5: the pool never grows; a dirty set of 4× the pool completes | A-3b/A-3c (E4 shape) | POOL-GROW, EVICT-SKIP-DIRTY |
| I-NO-INPLACE | §4.4: a live home slot is never overwritten | A-3c, A-7b (SimEnv crash) | INPLACE-EVICT |
| I-INTEGRITY | §3.3: CRC, id and epoch checked on every read, with XX001 | A-2, A-7d (SimEnv lost/misdirected/torn) | CRC-SKIP, EPOCH-SKIP, ID-SKIP |
| I-META-SLOT | §2.2: the opened meta sits in slot `1 + (ckpt_id mod 2)`; a misplaced valid meta newer than every valid one is XX001, never opened and never fallen back past | A-7d (SimEnv misdirected meta) | META-WRONG-SLOT |
| I-FREE-SAFE | §4.4/§13 step 6: no reuse before the freeing meta is synced | A-3c, A-7c | FREE-EARLY |
| I-FREE-EXACT | §4.3: the persisted bitmaps are exact | A-7b, A-8 | BITMAP-OMITS-FREESET-PAGES |
| I-META-ATOMIC | §13: a meta never points at unwritten pages | A-7c (crash between every step) | META-BEFORE-SYNC |
| I-CKPT-AT-T | §13: the image is a consistent cut; (a)–(c), with `T` = the apply frontier `M`; (b) bounded by `wal_ckpt_end`/`ts_ckpt_hi`, bulk-load entries via their fence commit; only step 2 writes the image map, from the image-record list. Test `t_epoch_reserve_two_exhausters` (A-3c with A-4c, §5.6): two first touches exhaust the reserve together; both copies reach the image. In its cross variant two writers each hold a MapLeaf's latch in `write_guard`, A and B, and exhaust the reserve together, every claimable copy being of a page that the other's MapLeaf maps; both finish, both copies reach the image, and a hook asserts that no claim takes a map latch. EXHAUST-DOUBLE-EVICT and EXHAUST-BLOCKING-CLAIM fail it; CLAIM-LATCHES-MAP (a claim records its copy by latching the MapLeaf that maps it) fails the cross variant through the hook, and deadlocks it. Test `t_step2_waits_for_claimed_copy` (A-3c, §13 step 2): a hook pauses a writer that has claimed an epoch-`k` copy (§5.6 "Reserve") after the copy's write and before its image-record append, just as step 2's data phase reaches its end. Step 2 must wait, and image `k` must hold the copy's content (amcheck, and a read at `T` after a crash at post-5). A slot variant forces slot allocations from a pool at its minimum after the claim's append and before the map phase runs. A map-copy variant evicts in epoch `k+1` a page that MapLeaf M maps, so that M is copied, and has a claim evict M's epoch copy before step 2's data phase writes another page that M maps. A current-home variant has a §5.2 previous-epoch eviction write MapLeaf M, dirty at the flip with no dirty page it maps, to slot `s1`, which becomes M's current home; after the map phase has rewritten M, slot allocations from a pool at its minimum reach `s1`, M is dropped and reloaded through the current map, and amcheck checks image `k`'s slot bitmap. A victim variant pauses a §5.2 previous-epoch victim after its write and before its image-record append, so that step 2's data phase finds the victim latched by its evictor and leaves it to that evictor (§5.2) as the data phase ends. A source variant pauses the map phase after it finds no epoch copy of MapLeaf M and before it latches M's frame, while a writer first-touches M, which installs M's epoch copy, and a claim evicts that copy. A namespace variant lists, in one checkpoint, a record for LPID `n` and one for the MapLeaf at leaf-level position `n` (a claimed copy of it), and, once map growth exists (S-1-F7-F2-F2-F1), adds a radix level in epoch `k+1` before the map phase runs. An adoption variant pauses step 2's data phase after it picks a dirty page P and before it latches P's frame, while a §5.2 previous-epoch eviction writes and lists P and P is reloaded; a hook asserts that the list holds at most one record per page and that no adoption replaces a home the list names. An allocation variant runs a checkpoint with a hook that fails any allocation the image-record list or a map-phase buffer makes after open; the checkpoint completes. STEP2-NO-WAIT (step 2's data phase ends with a claimed copy outstanding) and IMAGE-RECORD-AFTER-RELEASE (a claim appends its record after it releases the copy's latch) fail it: image `k` misses the page's epoch-`k` content. IMAGE-SLOT-REUSED (a slot that only the list names is reusable before the map phase applies it) fails the slot variant: a later write lands on the listed slot, and amcheck finds the page's CRC wrong. MAP-PHASE-DROPS-CLAIMED-COPY (the map phase builds M from its frame although the list records M's claimed copy) fails the map-copy variant: image `k`'s M maps the evicted page's epoch-`k+1` home, which a read at `T` after a crash at post-5 returns; MAP-HOME-SLOT-REUSED (a slot the list records for a map page becomes reusable once the map phase rewrites the page, although it is the page's current home) fails the current-home variant: the reload reads a reused slot and fails its CRC check with XX001; MAP-PHASE-SKIPS-VICTIM (the map phase starts while a §5.2 previous-epoch victim's write is in flight) fails the victim variant: image `k` maps the victim's earlier home, which a read at `T` after a crash at post-5 returns; MAP-SOURCE-UNCHECKED (the map phase builds a map page from its frame without checking the frame's content epoch under its latch) fails the source variant: image `k`'s M holds epoch-`k+1` entries, which a read at `T` after a crash at post-5 follows; MAP-POSITION-AS-LPID (a map page's record is keyed as an LPID) fails the namespace variant: one record replaces the other, and amcheck finds image `k`'s map wrong; ADOPT-LISTED-HOME (the data phase decides a page's write and its adoption before it latches the frame) fails the adoption variant through the hook; IMAGE-LIST-GROWS (the image-record list or a map-phase buffer allocates during a checkpoint) fails the allocation variant: the failed allocation is fail-stop with 53200 | A-3c (the reserve, the image-record list and the map phase; `t_step2_waits_for_claimed_copy`), A-7a/A-7c (splits and packs during a checkpoint; async mix), B-1d (a checkpoint during a finalize) | FLIP-NO-GATE, WAL-SYNC-T-ONLY, CKPT-T-VISIBLE, EXHAUST-DOUBLE-EVICT, EXHAUST-BLOCKING-CLAIM, STEP2-NO-WAIT, CLAIM-LATCHES-MAP, IMAGE-RECORD-AFTER-RELEASE, IMAGE-SLOT-REUSED, MAP-PHASE-DROPS-CLAIMED-COPY, MAP-HOME-SLOT-REUSED, MAP-PHASE-SKIPS-VICTIM, MAP-SOURCE-UNCHECKED, MAP-POSITION-AS-LPID, ADOPT-LISTED-HOME, IMAGE-LIST-GROWS |
| I-REDO-COMPLETE | §14: every acked sync commit is in the image or replayed (given I-ACK-PREFIX; §14 Post-sync damage; also from an older meta, `t_meta_rollback_crosses_boot`) | A-6d, A-7d | START-LSN-PAST-CHUNK, START-LSN-MARKED-CHUNK |
| I-WAL-COVER | §13 step 0, §14 step 3a: meta `k` records `wal_ckpt_end` and `ts_ckpt_hi`. An open fails with XX001 before pass 2 and writes nothing if its valid end (the walk's stop position, §12.5) is below `wal_ckpt_end`, or if its `Commit`/`CommitMarker` records do not reach `ts_ckpt_hi`. No open holds an entry above its recovered frontier. Test `t_wal_loss_below_ckpt_end`: a SimEnv fixture with post-sync WAL faults (LOST, TORN or MISDIRECTED on a synced WAL write) holds a commit `c > T` partly applied before the flip, whose record step 1 synced; then meta `k` is durable, then a fault on `c`'s record. The open gives XX001 and every file is byte-identical afterwards. Variants: the fault on a record that was appended but unapplied at the flip; on a `SegmentEnd` before `wal_ckpt_end`; on a chunk of a marked transaction; on a chunk of a transaction that never committed (no committed data lost; still XX001); on a bulk load's fence commit `f > T`, with loaded entries in image `k`; **short segment**: a segment cut short so that its end-of-file lies past the start of a record that runs past it, below `wal_ckpt_end`. Under COVER-CHECK-SKIP, a new commit and then a read at `S ≥ c` see part of `c`; under COVER-END-AT-T, the open succeeds; under COVER-AFTER-TRUNCATE, the files differ; under COVER-AT-EOF, the short-segment variant opens. Test `t_cover_ts_cross_check`: a meta whose `ts_ckpt_hi` exceeds every replayable commit (forged through `Env`) gives XX001; for §14 step 1's open-time check, a forged meta with `T > ts_ckpt_hi`, and one with `wal_start_lsn > wal_ckpt_end`, each with a valid CRC, give XX001 (META-RANGE-SKIP opens them) | A-7c (the fields), A-7d (SimEnv post-sync WAL damage) | COVER-CHECK-SKIP, COVER-END-AT-T, COVER-AFTER-TRUNCATE, TS-HI-CHECK-SKIP, COVER-AT-EOF, META-RANGE-SKIP |
| I-WAL-CHAIN | §2.2, §12.2, §12.5, §14 step 3a: every record carries `prev_lsn` and `prev_crc`, the start and the `crc` of the record appended before it (`SegmentEnd` included); a boot's first record names its recovery's last accepted record, or the opened meta's `wal_start_prev` and `wal_start_prev_crc`. The chain is checked last, after every stop check, and both fields are compared. A record that passes every stop check but names another predecessor (either field differs) is a splice, never an end: a read-write open fails with XX001 before pass 2 and writes nothing, and a read-only open serves salvage cut before it (§14.1). The stop checks keep their meaning. Undetected only when the spliced-in record's predecessor is byte-identical, at the same position, to the walk's (or their CRCs collide). Tests `t_wal_splice` (both placements, the read-only variant, the same-id residual variant, the same-start variant that a position-only chain misses), `t_wal_chain_crash` (no splice under ordinary crashes) | A-6a (the fields), A-6b (the check), A-7c (`wal_start_prev` and `wal_start_prev_crc`, each boot's first `prev_lsn` and `prev_crc`), A-7d (the outcomes) | PREV-CHECK-SKIP, PREV-CRC-UNCHECKED, PREV-MISMATCH-AS-END, PREV-FIRST-WRONG, PREV-HOP-WRONG, WAL-START-PREV-WRONG, WAL-START-PREV-CRC-WRONG, SPLICE-SALVAGE-PAST-CUT |
| I-RO-OPEN | §14.1: a read-only open writes nothing through `Env`, and its reads at every registered `S` equal those of a read-write open of the same files after step 4, except under WAL loss or a splice (§14 step 3a), where the read-write open fails and the read-only open serves the salvage state at the recovered frontier (§14.1). Every build offers it (§14.1). Test `t_ro_open`, with a SimEnv that fails every write, sync, rename and truncate. Its fixture is a crashed file with a torn WAL tail, small commits above `T`, a marked chunked txn whose marker lies after a commit `≤ T`, unmarked chunks, a `dropping` entry, a replayed `DropTree`, provisional entries in the image, and replay splits. The test compares the reads with a read-write open of a copy, checks the files are byte-identical after close, checks that a pool too small for the replay gives 53200 with nothing written, and checks that the replayed `DropTree`'s tree keeps every page mapped (REPLAY-DROP-WALK fails it). Test `t_ro_open_disk_full` (the stopped store, C4 decision 5): a SimEnv stops a writer with ENOSPC (53100) at each write point in turn, namely a WAL append, a segment's creation, its preallocation (a short segment, §12.5), a file extension (§4.5), a checkpoint page write and the meta write. A read-only open then runs on a SimEnv that reports zero free bytes and refuses every write, including a lock file's creation. It must succeed, its reads must equal those of a read-write open of a copy on a roomy Env, a full scan of every published tree must succeed, and every file must be byte-identical after close. With a pool below the replay's evictable minimum it must give 53200 with nothing written, and a read-only open of the same files with a larger pool budget must then succeed at zero free bytes (§14.1's remedy). SHORT-SEG-ERROR (a short or missing segment returns 58030 instead of ending the valid end) fails the open. Test `t_seg_ack_durable` (A-6b, §12.5's writer rule), with a SimEnv whose power cut also loses unsynced creates, renames and `set_len`: a segment is newly created (the lost-create case) or recycled by rename (the lost-rename case), records in it are acked, and power is cut at every step from the create or rename to the ack. Recovery must replay every acked record. SEG-ACK-BEFORE-DURABLE (ack before the segment's `sync_all` and its `wal/` directory sync) loses an acked commit as a clean stop and fails it. Test `t_wal_loss_salvage` (Q1 = A, PLAN-v3 §10 item 8): the `t_wal_loss_below_ckpt_end` fixture, plus a `dropping` mark from a drop-cleanup commit above `T` whose record is lost, opened read-only. At every admitted `S` it reads the model's state at `S ≤` the frontier, never an entry above it, and a full scan of every tree the catalog at `S` names succeeds. SALVAGE-ABOVE-FRONTIER fails it. Test `t_wal_splice`, read-only variant: the open serves the state at the frontier of the records below the splice cut; SPLICE-SALVAGE-PAST-CUT fails it | A-7d, A-6b | RO-NO-REPLAY, RO-WRITES, RO-LOCK-FILE, SHORT-SEG-ERROR, SEG-ACK-BEFORE-DURABLE, SALVAGE-ABOVE-FRONTIER, SPLICE-SALVAGE-PAST-CUT, REPLAY-DROP-WALK |
| I-WAL-STALE | §12.5: the reader stops at the first bad magic/CRC, wrong `lsn` or over-long `len`, or a boot id older, in serial-number order, than the last accepted record's (the opened meta's for the first record); a walk may cross into newer boots; recovery truncates there. It hops to the next segment after `SegmentEnd` or after a record that ends exactly at its segment's end, and a writer never leaves a shorter tail (§12.2). A boot's id is fresh after a meta rollback (§14 step 8). Tests `t_stale_boot_record`, `t_meta_rollback_crosses_boot`, `t_wal_exact_segment_end`, `t_boot_id_after_rollback` (both variants) | A-6b, A-7d | LSN-CHECK-SKIP, BOOT-ID-STOP-SKIP, BOOT-ID-EQ-STOP, SEG-EXACT-NO-HOP, SEG-TAIL-SHORT, BOOT-ID-CKPT-ONLY, BOOT-ID-NO-META-FLOOR |
| I-ACK-PREFIX | §12.4: `written_end` and `durable_end` advance only over the contiguous prefix; an `on` ack needs `durable_end ≥` the record's end; an `off` ack needs `written_end ≥` it; step 1 waits on `durable_end ≥ wal_ckpt_end` | A-6c (`t_ack_prefix`, porting X-5c-F3-F1 (a)'s two-task interleaving), A-7c (`t_ckpt_sync_prefix`), B-1c (C-T0 v8 §3.1 step 3's visibility over the same prefix) | ACK-OVER-HOLE, CKPT-SYNC-OVER-HOLE |
| I-WAL-TS-ORDER | §12.4: commit_ts strictly increases with LSN | A-6c, B-1c | TS-AFTER-APPEND |
| I-BULK-PUBLISH | §12.8: unlogged writes only into unpublished trees; publish after the forced meta | B-4d | PUBLISH-BEFORE-META |

amcheck (A-8) checks I-TREE, I-ENC-ROUNDTRIP, I-TAG-RESERVED, I-LPID-EXACT, I-FREE-EXACT and I-INTEGRITY on a quiescent file. At open (§14 step 7) it runs before the boot job's walks start, or holds the gate exclusively, because a running walk is not quiescent: either way it reads a closed graph. A crash image's `dropping` tree gets no exemption (I-LINK-ATOMIC).

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

This file wins over the plan (precedence above). The plan should be updated through an Opus card to match these. R10 refines the §4 decision block rather than §3.3. R11 comes from the S-2 co-edit. R12–R15 come from card S-1-F3, R16 from card S-1-F5, and R17 from card S-1-F7; R15 refines §3.4, as R9 does:

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
| R17 | "DropTree frees a tree's pages bottom-up, and an unfinished drop resumes at open" | The entry removal and the root free are one gated operation, entry first; a chain-bearing entry goes with its chain in its own gated operation; an operation restarts only before its first mutation; an LPID is mapped and unmapped in the same gated operation as its reference (I-LINK-ATOMIC, §5.6, §6.8); and pass 2 runs no walk (§14 step 4) | E4's second design finding: a flip between the root free and the entry removal gave a refused open, a replay that freed another tree's live page, or an image whose dropping entry named one (28 of 51 C-XX rounds). Page headers carry no tree id (§3.1), so a recovery arm cannot tell a reused root from the tree's own |

---

## 19. Open questions, known costs, revisit triggers

**Open:**
1. **File shrinking.** GC returns space to the free set, not to the filesystem (§4.5). Trigger for a tail-truncation step (at step 6, of free tail slots only) or an offline compaction: a browser quota (E6/D-5) or a user report of a file that does not shrink after a mass delete.
2. **Leaf prefix compression within groups.** Every version repeats its full escaped key, so a 10-version group of W1-sized keys spends about 117 B on repeated key bytes. A-4d covers inner nodes only. Trigger: NBench space rows, or I-SPACE met but B-3's bytes measured close to 1.5×.
3. **Optimistic latches and pointer swizzling** (N8/N9): only on the E0/E7 triggers (PLAN-v3 §3.12).
4. ~~**Provisional entries.**~~ Closed by the S-2 co-edit: §7.1, §7.3 tags 6–8, §8, §12.7, I-CKPT-AT-T (c), §14 (C-T0 v8 §2.4).
5. **PITR and replicas across a §12.8 bulk load** (D-6).
6. **Resumable overflow-chain free or build** if A-5 measures the flip hold of a gated operation as too long (§6.7): a chain's build or free, a replace, a GC sweep's or DropTree walk's chain delete over cold chains (§6.8, §10.4). A resumable chain free or build must keep I-LINK-ATOMIC (§5.6; mutants CHAIN-NEXT-MAP-SPLIT, CHAIN-NEXT-FREE-SPLIT).

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

**Known cost of root names (cards S-1-F7, S-1-F7-F2):**
- **T5: no root name kept across operations.** §6.8's root names let no thread keep a tree's root LPID across operations: every operation names its root through the root table and validates it under the root's latch.
  - **The cost.** One root-table read and one re-read per operation, in place of draft 7.5's two-latch directory descent (about +240 ns per point get, and 1.46× at 4 threads to 2.08× at 16, on E0's tree, verdict S-1-F7). No ordinary operation latches the directory, so that global cache line leaves the hot path, provided a root-name read stores to no memory common to all trees (§6.8 "The root table"). Each operation still takes its tree's root latch, a read-modify-write on a cache line per tree, so a workload on one hot table keeps E0's scaling cliff; the remedy is the E0/E7 optimistic-latch trigger (PLAN-v3 §3.12), not this rule.
  - **Measurement.** A-5, with E0, measures the table path: the point-op CPU spent naming roots, and multi-tree scaling at 4 to 16 threads. ROOT-TABLE-SHARED-WRITE (a root-name read takes a read guard or a reference count on memory common to all trees) shows there, not in a functional test.
  - **Later.** C-T6 §7.5 keeps a tree from being dropped while a registered snapshot can see it, so a foreground operation could keep a root name for its registered snapshot. That is a later spec edit, opened if A-5's measurement puts the table path above 5% of point-op CPU; background jobs keep the current rule.

---

## Changelog

- **Draft 7.7** (2026-11-07, card S-1-F7-F2-F2-F4b, from the design ruling on S-1-F7-F2-F2's lock order (ruling lockorder, parts (a) and (d)) and its independent review (AGREE-WITH-CHANGES, changes C1, C2's exception and C4–C8); from the map-order ruling (ruling maporder) after the implementer stopped on the map-page order, and from that ruling's independent review (AGREE-WITH-CHANGES, changes C1–C7); the second of the two cards, after S-1-F7-F2-F2-F4a):
  - **One order** (§5.3, §6.2). Directory pages, then one tree's pages top-down and left to right, then map pages in radix order; a thread blocks only on a latch ranked after every latch it holds. Map pages need two rules for this, both new below: a claim takes no map latch, and a map page's write releases the page before it latches the parent.
  - **Latch scope** (§5.3). A thread holds frame latches only inside one storage call and on one tree's pages, and releases every latch and pin before the call returns or another tree is accessed. Two exceptions add no wait edge: `try_latch` acquirers and a stale root name's first latch. Directory pages count as no tree's. No gated operation latches two trees' pages (checked against §9.1, §10.4 and C-T0 §2.4).
  - **Fair latches** (§5.1, §5.3; review C4). A frame latch is task-fair, a bounded-bypass property; strict FIFO is not required, and `std::sync::RwLock` does not qualify. No recursive latch; no latch escapes a call or a yield (review C6.4).
  - **No page read in a reader's hold** (§5.3; review C5). A reader that meets a non-resident page releases, pins and re-descends. A writer on a leaf root may still read MapLeafs in place for a chain, which A-5 measures.
  - **Why latching cannot deadlock** and **the fairness bound** (§5.3; review C6.1–C6.3). The argument covers the checkpointer, the map-page order, the pool floor (citing §5.2's release-and-restart) and the pin term that card S-1-F7-F2-F2-F2 owes §5.4. §5.4's "Below the minimum" paragraph states that dependency, and that open cursors add nothing to `F`.
  - **Cursors** (§6.6; review C1, C7, C8). A cursor parks before each call returns, holding no latch and no pin, and resumes by re-descent through a validated root name. Its parked position is after a group, or inside one with the group's carried state. A per-call bound covers scans, point reads and newest probes. Batches are copied out under the leaf latch and freed on close, `ROLLBACK TO` included; a new §5.7 row accounts them. Reverse movement is by re-descent, and a parked position holds in both directions.
  - **The root latch blocks** (§6.8 "Root names" step (2)). -F4a's interim retry is gone. S-1-F7-F2-F2's Do-not ("let a thread block holding a directory leaf") is deliberately replaced for DropTree's last step, whose only possible co-holder of the root is a stale-name thread's brief latch; `t_dir_root_no_stall` is its regression.
  - **The image map has one writer** (§4.4 item 2, §5.2, §5.6 "Reserve", §13 step 2; ruling maporder). Every write of epoch-`k` content after the flip, outside the map phase, appends `(page, slot, crc)` to an in-memory image-record list before it releases the written frame's latch. Step 2's map phase alone builds the epoch-`k` map pages, in buffers of the checkpointer's own, in post-order. A claim therefore takes no map latch and calls no `write_guard`, claims never nest, and §5.6's bound is one claim per thread. This closes a hole in draft 7.6, which gave no home to records for a map page whose epoch copy a claim had evicted. The list dies with image `k` on a crash before meta `k`, and its slots are free in image `k−1` (I-FREE-EXACT). New §5.7 row.
  - **The two-step map-page write** (§4.1; ruling maporder, item 2). Under the page's latch the writer writes the bytes and keeps the frame pinned and in the page table; it then releases that latch, latches the parent and sets the ref, and an eviction drops the frame only after that.
  - **The map phase, as the review fixed it** (§13 step 2, §5.7; review C1–C7):
    - it starts only once no write of epoch-`k` content is in flight, a §5.2 previous-epoch victim's included (C3), the drain E4's prototype already needed;
    - it decides each source under that source's latch, re-checks it, and never loads one into the page table (C4);
    - it never sets a map page's current home, which only a live-frame write does (C2);
    - a slot the list records for a map page it rewrites stays protected while another §4.4 reference, such as the page's current home, holds it, and image `k`'s slot bitmap comes from the built map (C1);
    - map positions are keyed apart from LPIDs (C5);
    - the list is reserved at open (C6);
    - the data phase adopts a write as a current home only if the page was not already written for image `k`, deciding under the frame's latch (C7, E4's B5 rule).
  - **§15.** I-LATCH: tests `t_latch_scope` (A-4c) and `t_root_latch_fair` (A-3b); the owner column names them. I-CURSOR: test `t_cursor_park_equiv` (A-4e). I-CKPT-AT-T: `t_epoch_reserve_two_exhausters`' cross variant, and `t_step2_waits_for_claimed_copy`'s slot, map-copy, current-home, victim, source, namespace, adoption and allocation variants. Twenty-seven new mutants: CURSOR-LATCH-ACROSS-CALL, CURSOR-PIN-ACROSS-CALL, NESTED-READ-UNDER-LATCH, LATCH-ACROSS-YIELD, REVERSE-LATCH-LEFT, CLAIM-LATCHES-MAP, IMAGE-RECORD-AFTER-RELEASE, IMAGE-SLOT-REUSED, MAP-PHASE-DROPS-CLAIMED-COPY, MAP-PARENT-UNDER-CHILD, MAP-DROP-BEFORE-REF, MAP-REF-SET-STALE, MAP-HOME-SLOT-REUSED, MAP-PHASE-SKIPS-VICTIM, MAP-SOURCE-UNCHECKED, MAP-POSITION-AS-LPID, IMAGE-LIST-GROWS, ADOPT-LISTED-HOME, CKPT-WAIT-HOLDING-MAP, POOL-FLOOR-BLOCK, LATCH-READER-PREF, LATCH-WRITER-PREF, TRYLATCH-BYPASS, READER-IO-UNDER-LATCH, PARK-DROPS-CANDIDATE, SCROLL-RESUME-SKIP, BATCH-KEPT-AFTER-CLOSE. Draft 7.6's deadlock variant of the reserve test is replaced by the cross variant.
  - No byte layout, persisted state, durability rule or SQL-visible behaviour changes.
- **Draft 7.6** (2026-11-07, card S-1-F7-F2-F2-F4a, from the design ruling on S-1-F7-F2-F2's lock order (ruling lockorder, part (c) and section 3) and its independent review (AGREE-WITH-CHANGES, changes C2 and C3); the first of two cards, S-1-F7-F2-F2-F4b adds latch scope, cursor parking and fair latches):
  - **The root table** (§5.7, §6.8). Root names come from an in-memory map `tree_id → (root LPID, kind, state)`, never persisted and a name only, so I-LINK-ATOMIC's reference list is unchanged. It carries no generation counter, since tree ids are never reused and an entry only goes `live` → `dropping` → absent; a table-wide counter would let unrelated DDL make readers retry without bound (review C3). A root-name read stores to no memory shared by all trees. CreateTree installs the entry while it holds the new root exclusively; DropTree step 1 sets its state; the last step removes it under the root's exclusive latch, after the directory entry and before the root is freed. New §5.7 row.
  - **Root names** (§6.2, §6.8). A thread reads the entry, pins and latches the root, and re-reads the entry under that latch; an unchanged entry validates the name for that hold. Validation is exact against DropTree followed by LPID reuse, by the order of the last step's removal, release and free. A pin that finds the LPID unmapped is a retry, and before validation the thread reads only the page header's `id` (review C2). Only CreateTree, DropTree and a state change latch the directory, so the global directory latch leaves every ordinary operation. The root latch is still taken with `try_latch` and a retry until S-1-F7-F2-F2-F4b. The old I-LATCH mutants for directory-leaf modes are retired; the new one is DIR-LATCH-ON-READ.
  - **Recovery** (§14 steps 2 and 4, §14.1). The table is built from image `k`'s directory before replay, in the normal and the read-only open; replayed CreateTree and DropTree maintain it.
  - **§15.** I-LATCH: `t_dir_root_no_stall` becomes a regression for the directory leaving the hot path, with a hook that no non-directory operation latches a directory page (DIR-LATCH-ON-READ). I-LINK-ATOMIC: test `t_root_name_reuse` (cases A–D, a churn variant and a shuttle exploration; the property is checked only when validation succeeds) and mutants ROOT-NAME-UNVALIDATED, ROOT-UNPUBLISH-LATE, ROOT-PIN-UNMAPPED-FATAL, ROOT-DECODE-BEFORE-VALIDATE and ROOT-TABLE-GLOBAL-GEN. It replaces the moved-root test of S-1-F7-F2-F2's earlier draft (R4). I-LINK-ATOMIC's statement still says a root LPID is "taken under its directory leaf's latch and not kept", and that clause is stale until S-1-F7-F2-F3 rewrites it.
  - **§19 T5.** Renamed to no root name kept across operations. The cost is one table read and one re-read per operation; a single hot table keeps E0's scaling cliff (review C3). A-5 measures the table path, and ROOT-TABLE-SHARED-WRITE shows only there; keeping a root name for a registered snapshot is a later edit if the table path exceeds 5% of point-op CPU.
  - **Rework** (2026-11-07, `verdicts/S-1-F7-F2-F2-F4a.md` R1–R6). `t_root_name_reuse` case B names X's root in a test-only mode that accepts `dropping`, and a split of Y takes `r` at the hook, so under ROOT-UNPUBLISH-LATE A validates a reused `r` on every schedule; the exploration gets the same reader and a reusing split. A header `id` mismatch before the re-read is not acted on: if the entry changed, it is a retry, and only under an unchanged entry is it XX001. Case D gains a variant with `r` mapped but not yet initialised, and mutant ROOT-HEADER-MISMATCH-FATAL. The exactness argument now covers an LPID reused in another frame, ordered through the free-set lock and the reuse's mapping. The churn variant runs one CreateTree per attempt from a hook, so ROOT-TABLE-GLOBAL-GEN fails on every schedule. §6.8's ROOT-DECODE-BEFORE-VALIDATE now reads "as the tree's root".
- **Draft 7.5** (2026-11-07, card S-1-F7-F2-F2, from the P gate of S-1-F7-F2, `verdicts/S-1-F7-F2.md` judgment calls 2 and 8, follow-up S-1-F7-F2-F2):
  - **The free-set lock** (§4.2). New in draft 7.3 with no scope or rank. It now guards the LPID bitmap's in-memory state and `next_lpid` only; slot allocation and eviction do not take it. Its rank is after the gate and any frame latches, with the page table inside it and never the reverse, so a dirty eviction that allocates a slot while holding the page table cannot invert the order against an allocation pinning a MapLeaf. Under it a thread only tests and sets LPID bits and tests and pins resident MapLeaf frames, with no I/O, eviction, page creation or frame latch. Test `t_freeset_lock_order` (A-3c); mutants FREESET-LOCK-IO and FREESET-LOCK-INVERT.
  - **Progress of the restart arm** (§5.6). The lowest free LPIDs could move across a restart, so a retry could meet another cold MapLeaf, and an LPID past the last MapLeaf had no MapLeaf to be resident. Such an LPID now counts as not resident, and the restart creates and pins its empty MapLeaf in a gated operation of its own, with no other latch held. A restarted allocation keeps its pins and retries on the lowest free LPIDs under them first, the one exception to §4.2's lowest-first order; it holds at most one pin per LPID it takes, and with no concurrent allocation or path change it restarts at most once, any further restart being charged to another operation's progress. Test `t_alloc_restart_progress` (A-4a); mutants ALLOC-RETRY-LOWEST and ALLOC-NEW-MAPLEAF-IN-PLACE.
  - **Root-latch liveness** (§6.8 "Root names", §15 `t_dir_root_no_stall`): moved to cards S-1-F7-F2-F2-F4a and S-1-F7-F2-F2-F4b (ruling lockorder, 2026-11-07). Both are draft 7.4's text.
  - **Rework** (2026-11-07, `verdicts/S-1-F7-F2-F2.md` R2 and judgment call 4).
    - R2: two restarts on one missing MapLeaf could each create it, and the second would replace a ref whose MapLeaf already mapped an LPID. Creation is now conditional on a null ref under the MapInner's exclusive latch, and the frame is obtained, possibly evicting, before that latch. `t_alloc_restart_progress` gains a race variant; mutants MAPLEAF-CREATE-UNCONDITIONAL and MAPLEAF-FRAME-UNDER-INNER.
    - Judgment call 4: `t_freeset_lock_order`'s hook now names a frame latch taken under the lock, since the allocation already holds `P`'s.
- **Draft 7.4** (2026-11-07, card S-1-F7-F2-F1, from the P gate of S-1-F7-F2, `verdicts/S-1-F7-F2.md` judgment calls 1 and 4, follow-up S-1-F7-F2-F1):
  - **The three deletes** (§6.7, §5.6, I-LINK-ATOMIC; the S-1-F7-F1-F1-F1 conflict, pre-existing). Settler (a)'s abort cleanup (C-T0 v8 §7.1), `T`'s delete on `ROLLBACK TO` (C-T0 v8 §5.5) and a replayed `Delete{revert}` of a tag-8 entry (§14) each free a chain outside the sweep. On a full leaf the delete found the leaf unsafe (§6.2) and kept `P`, so it read cold chain pages under `P`'s exclusive latch past its first mutation. It now releases `P` and every other ancestor before it deletes, holding only the leaf as the replace free does, and reads the chain in place. It is then a delete made while holding only a leaf, which never packs (§6.5), and it loses no pack: a leaf that coupling found unsafe (§6.2) is more than three quarters full (§3.2), so one delete leaves it above `U/4`. The free stays in the deleting operation. Mutant DELETE-FREE-UNDER-P; `t_replace_free_shape` gains one delete variant per delete, each on a full leaf with the chain and its MapLeafs evicted.
  - **The reorder** (§6.7 "Freeing"). The settle clause (settlers (b) and (c)) now precedes the "unless" clause, so the same-chain exception binds only to the replace.
  - **The build variant** (§6.7 "Building", I-LINK-ATOMIC). `t_replace_free_shape` evicted only the old chain's MapLeafs, so a build that kept `P` after a split went untested. Its build variant splits the leaf with an inline old value, so nothing is freed; the split takes the last LPID its MapLeaf covers, so the next free LPIDs the new chain takes lie on later MapLeafs, which are evicted, unpinned and map no dirty page; mutant CHAIN-BUILD-UNDER-P. The build's rule and §6.7's measurement sentence are unchanged.
  - **Rework** (2026-11-07, verdict 3cc5d766 R1–R3). R1: as first written, the build variant's chain took LPIDs on the split's own MapLeaf, which the split pins (§5.6, §6.4) and its dirty page keeps resident (§4.1), and its old chain's free read cold first, so CHAIN-BUILD-UNDER-P read nothing evicted under `P` of its own; the fixture above fixes both. R2: "not among §6.5's pack callers" was false, since §6.5's coupled-delete bullet names a directory removal only as an example; §6.7 now gives the reason above. R3 pins the rewritten sentences in the card's Acceptance and changes nothing here.
- **Draft 7.3** (2026-11-07, card S-1-F7-F2, from the P gate of S-1-F7, `verdicts/S-1-F7.md` N1–N5 and "Barring root caches: the cost"; verdict f8269110 (X-5c-F4-F1-F1-F2); `verdicts/PL-5-gate.md` ruling 1 and `PL-5-F1-gate.md` rulings 2–3; verdict 2353aeb1 (S-1-F7-F1-F1) "For S-1-F7-F2" and its follow-up S-1-F7-F1-F1-F1, folded in):
  - **N1, a restart after the removal or an unlink.** Under entry-first that restart leaves an image with no entry and a mapped root, a silent leak, and the fixture never ran the path. `t_drop_every_cut`'s fixture gains an evicted root and an evicted MapLeaf of a freed LPID, each with a flip pending, in the last step and an unlink step; mutants DROP-RESTART-AFTER-REMOVE and UNLINK-RESTART-AFTER-UNLINK, the realistic form of DROP-RESTART-MID-OP, which stays.
  - **N2, the split path under the restart rule** (§5.6, §6.4, §6.7, §6.8). A split's reservation comes before its prune. An allocation takes its LPID only with its MapLeaf resident and pins it under the free-set lock, or restarts; several allocations are taken in one hold of that lock; a later allocation reads in place with no inner exclusive latch held. CreateTree uses the restart arm. A chain build holds only the leaf (the pick Work 2 left to the author), so it releases `P` once the room is made. Test `t_split_reserve_order` (A-4a); mutants SPLIT-RESERVE-AFTER-PRUNE, ALLOC-READ-UNDER-P, SPLIT-RESERVE-ALWAYS.
  - **The split decision** (ruled by the card from verdict 2353aeb1). The insert marks what the prune would delete, under the leaf's exclusive latch, and splits only if its cell does not fit with those marks counted as gone; the reservation, or a restart, comes before the prune's mutation, and an insert that does not split reserves nothing. Reserving unconditionally is rejected, since it allocates before knowing a split follows; deciding after the prune is the mutant SPLIT-RESERVE-AFTER-PRUNE.
  - **The replace free** (S-1-F7-F1-F1-F1, §6.7). An insert that replaces or settles a chain-bearing entry (a provisional rewrite, settlers (b) and (c), a superseding replay op) released no ancestor before freeing the old chain, so after a split it read cold chain pages under `P`'s exclusive latch past its first mutation. It now releases `P` and every other ancestor first and reads the chain in place, as the sweep does; the free stays in the replacing operation. Test `t_replace_free_shape` (A-5); mutant REPLACE-FREE-UNDER-P. The measurement sentence and §19 item 6 are unchanged. C-T0 v8 §2.4 (c), which still lists the page-local prune among the tag-8 settlers, waits for the next C-T0 sync.
  - **N3** (§6.5, §6.8). DropTree's last step does not pack, since its pins and I-LATCH leave no room for the directory neighbour; §6.5's directory-removal bullet says so. `t_drop_step_shape` gains a directory leaf left below `U/4`; mutant DROP-LAST-STEP-PACKS.
  - **N4** (§6.2, §6.8, I-LATCH). The directory leaf is latched shared unless the operation changes the directory, and a thread holding it takes a root only by `try_latch`, else releases and re-descends. Test `t_dir_root_no_stall` (A-7b, with A-4c's shuttle harness); mutants DIR-LEAF-EXCLUSIVE, ROOT-WAIT-HOLDING-DIR.
  - **STEP2-NO-WAIT** (§13 step 2, I-CKPT-AT-T). The wait for claimed copies gets its mutant and test `t_step2_waits_for_claimed_copy` (A-3c).
  - **The walk's owner and the sweep stand-in** (I-LINK-ATOMIC). A-7b owns the walk's code, A-7d's tests call it, B-6 runs it; the owner column gains A-7b. At A-7d the paused sweep is a test-only stand-in shared by `t_drop_step_shape`; ROOT-NAME-KEPT counts only when applied to B-3's sweep at B-6.
  - **N5.** The card's Acceptance pins the G2–G8 sentences; none was reworded.
  - **§19 T5,** the known cost of root names, with its measurement and trigger; §6.8 points to it.
  - **N6** (`R_MAP` and `R_EPOCH` sizing, claim depth) went to R-1 through PL-5. No byte layout or persisted state changes.
- **Draft 7.2** (2026-11-07, card S-1-F7-F1-F1, from the P gate of S-1-F7-F1, `verdicts/S-1-F7-F1.md` at 1d15ae32 and 8f7b09c6, "Follow-up"):
  - **The defect.** §10.3's prune, which §6.4 runs before a split, deleted chain-bearing versions and dead tag-8 entries and freed their chains in the splitting insert's gated operation. The leaf is full, so the insert still holds `P`, and possibly more ancestors, exclusively, and each chain page read for its next pointer was I/O under an inner exclusive latch after the operation's first mutation, against §5.3 and §5.6. On a toast-region leaf of cold chains one insert could do thousands of such reads while the flip waited. The prune at eviction, nested under another operation's hold (§5.2), had the same flaw.
  - **§10.3, the shape chosen.** New bullet **Chains**: the prune frees no chain. It deletes and elides only entries that hold no chain reference (tags 0–3, dead tags 6–7) and never deletes a tag-4, tag-5 or tag-8 entry, at every call site. The elide bullet adds that no older version of the tombstone's group holds a chain; otherwise the tombstone stays and the group's non-chain older versions may still go. Chain-bearing garbage waits for the sweep's "Chains first" (§10.4). The insert stays live, since §6.4's prune-then-split order is unchanged; the cost is space, B-3's to bound (I-SPACE, T2).
  - **The shape rejected:** a splitting insert that deletes each chain-bearing prunable entry in its own leaf-only gated operation, then restarts. It would put a leaf's worth of cold chain frees in front of a foreground insert, add a second identity-checked chain-deleting path beside the sweep's, and not serve the prune at eviction.
  - **§6.4** says the prune frees no chain. **§10.4 "Chains first":** the parenthesis on an entry no longer the same names only a writer as its deleter, no longer the prune.
  - **Removed interim clauses** (draft 7.1, R2), each of which made A-5 also measure an insert whose split-time prune frees chains until this card: in §6.7's measurement sentence, which now ends at "(§6.8, §10.4)"; in §19 item 6's list; in C-T6-rehost §4.3, whose sentence on the prune now reads that it frees none; and in C-T6-rehost §13.1's N4 row.
  - **§15:** I-GC-LOCAL's statement gains the chain rule and test `t_prune_split_shape` (B-3) with an eviction variant, and mutants PRUNE-FREES-CHAINS, PRUNE-ELIDES-OVER-CHAIN and PRUNE-EVICT-FREES-CHAIN beside PRUNE-FIRST-ENTRY. No byte layout or persisted state changes; I-GC-READEQ, I-GC-ORDER and I-LINK-ATOMIC are unchanged.
- **Draft 7.1** (2026-11-07, card S-1-F7-F1, from the P gate of S-1-F7, `verdicts/S-1-F7.md` finding F3):
  - **§6.7.** A-5 measures the flip hold per gated operation, not only per chain: a chain's build and free at the maximum value size (`ROW_LIMIT`, C-T6-rehost §4.3) at every allowed page size, a replace, and, with cold chains, a sweep's and a DropTree walk's operations over a toast-region leaf and a leaf of maximum-size values. C-T6-rehost §4.3 and its §13.1 N4 row say the same.
  - **§10.4 step 3, "Chains first"; §10.2.** A sweep step frees no chain under `P`'s latch. A step whose marks include a chain-bearing entry ends before its first mutation; the sweep deletes each such entry with its chain in its own gated operation, holding only its leaf, and runs the step again from the same token. The defect: one step could free two leaves' worth of chains under `P`'s exclusive latch, reading each chain page, against §5.3 and §5.6. I-GC-READEQ, I-GC-ORDER and I-LINK-ATOMIC are kept; no byte layout or persisted state changes.
  - **§15:** I-GC-READEQ gains test `t_sweep_step_shape` and mutant SWEEP-CHAINS-UNDER-P. **§19 item 6** names the operations A-5 measures.
  - **Rework** (2026-11-07, `verdicts/S-1-F7-F1.md` R1, R2):
    - **R1, §10.4 "Chains first".** Each separate chain delete is conditional on the entry's identity (C-T0 v8 §2.4 (c)): an older version by its entry key (I-ENTRY-UNIQUE), a tag-8 entry only while its bytes still show it dead. The latches are released before the delete, so a writer on `k` may settle the dead entry and write its own at the same key; a delete by key alone would delete that live entry and free its chain. "Why the re-runs end" gains its second reason: dead entries come only from previous boots, and no current-boot entry becomes dead. **§15:** `t_sweep_step_shape`'s fixture gains that writer race and an elided-tombstone group whose older versions hold chains; I-GC-READEQ gains mutant CHAIN-DELETE-BY-KEY and I-GC-ORDER gains STEP-MUTATE-BEFORE-CHAINS, both caught by that test.
    - **R2, §6.7 and §19 item 6.** §10.3's split-time prune still frees a leaf's chains, possibly many in one gated operation, under the splitting insert's hold. Until card S-1-F7-F1-F1 changes that, A-5 also measures it on the same two leaves. C-T6-rehost §4.3 now claims at most one chain per operation for sweep and DropTree operations only, and lists the prune too, as does its N4 row.
- **Draft 7** (2026-11-05, card S-1-F7, from the P review of the C-XX diagnosis, E4's second design finding; evidence `~/.cache/nv3-review/pxx/`, base a4c1f869):
  - **The defect.** §6.8 freed a dropped tree's root in step 2 and removed the directory entry in step 3, and a flip could land between them, breaking I-LPID-EXACT. Three measured outcomes: a refused open (310 r0), a reuse during replay that freed another tree's live page (332 r1), and a reuse before the flip, so the opened image's dropping entry named another tree's live page (8010 r1). The architecture stands: CoW images, logical redo, the checkpoint order and the two passes are unchanged, no byte format or persisted state changes, and amcheck gets no exemption.
  - **I-LINK-ATOMIC** (§5.6): an LPID is mapped and unmapped in the same gated operation as its reference, and the reference goes before the free. It replaces recovery arms, which E4's probe `E4_PXX_ARM` refuted: 7 of the 28 rounds that need this fix still failed. R17.
  - **§5.6: the restart rule.** An operation restarts only before its first mutation, and a restart releases the gate; afterwards it reads in place or has pinned first. §5.3's restart stands under that rule.
  - **§6.8.** CreateTree is one gated operation. DropTree deletes each chain-bearing entry with its chain in its own gated operation, holding only its leaf, so a gated step stays bounded. Its walk unlinks and frees in one step, and its last step pins first, then in one hold removes the entry and frees the root. At most one walk runs per tree, and a root LPID is taken under its directory leaf's latch and not kept, which closes C-T6-rehost §7.5's check-then-act. I-LPID-EXACT holds of every image.
  - **§14 step 4, §14.1.** A replayed `DropTree` only sets `dropping`; its walk runs at step 6, and pass 2 runs no walk. C-T6-rehost §7.4's idempotence sentence says so.
  - **Siblings.** §6.7: an insert descends and makes room before it builds, a chain is mapped in the gated operation that inserts its entry, and A-5 measures the build's flip hold too. §12.8: a load's pages are not exempt, and each load step links what it maps. §19 item 6: a resumable chain free or build must keep the invariant.
  - **§5.6, §13 step 2: a clarification, not a defect.** The epoch-copy claim is a `try_latch` that moves to the next-oldest copy, `R_EPOCH` exceeds what can be latched at once, the install is latched, and step 2's data phase waits on a latched frame. The prototype broke these twice while the text was right.
  - **§15:** new row I-LINK-ATOMIC, with a mutant per reference kind and direction; I-LPID-EXACT, I-CKPT-AT-T, I-RO-OPEN and the amcheck line amended. Mutants DROP-ENTRY-SPLIT, DROP-UNLINK-SPLIT, CREATE-TREE-SPLIT, DROP-RESTART-MID-OP, DROP-FREE-BEFORE-REMOVE, DROP-LEAF-CHAINS, DROP-SECOND-WALK, ROOT-NAME-KEPT, CHAIN-BUILD-SPLIT, CHAIN-FREE-SPLIT, CHAIN-NEXT-MAP-SPLIT, CHAIN-NEXT-FREE-SPLIT, LOAD-PAGE-SPLIT, REPLAY-DROP-WALK, EXHAUST-DOUBLE-EVICT, EXHAUST-BLOCKING-CLAIM; tests `t_drop_every_cut`, `t_drop_step_shape`, `t_create_every_cut`, `t_chain_every_cut`, `t_load_every_cut`, `t_epoch_reserve_two_exhausters`, `t_ro_open` with a replayed `DropTree`.
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
