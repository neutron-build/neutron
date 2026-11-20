# R-1: Memory governance (normative)

Status: draft 1 (card R-1-SPEC, 2026-11-17: PLAN-v3 N16's one budget per store instance, every consumer's account and what it does at its limit; the numbers S-STORE §17 leaves to R-1 (`F`, `R_EPOCH`, `R_MAP`, the height bound `H` and its floor `H_min`) derived against S-STORE draft 7.16; C-T0 v8's thresholds per mode; per-mode budgets, proposed for the owner (§7.1); seven invariants with their tests and mutants, and the Phase A cards' share in S-STORE §15.1). This is a P-authored spec and needs a P gate before it is normative.

**Scope.** PLAN-v3 N16: "one budget across the buffer pool, write sets, undo, lock table, query operators and connection buffers. Every consumer spills or refuses with SQLSTATE 53200, and never OOMs." This file says what the budget is, who draws on it, how much each consumer may hold and what each does at its limit. It owns:
- the numbers S-STORE §17 gives R-1: the budgets per mode, `F`, `R_EPOCH`, `R_MAP`'s value, the height bound `H` and its floor `H_min`, and the per-consumer accounting of S-STORE §5.7;
- the numeric defaults C-T0 v8 leaves to R-1 (C-T0 §2.1, §2.2, §3.2, §6.2, §9.4, §12 O2 and O6);
- the accounts of the session layer, the executor and the front ends (§6).

Under N3 = (b) there is no undo (PLAN-v3 §4, X-4): versions kept for long readers are tree pages, which cost disk, not memory (S-STORE §5.5).

**Precedence.** A mechanism belongs to the spec that states it, and this file points to it: the pool, eviction, latches, the epoch gate, `write_guard`, the checkpoint and fail-stop are S-STORE's (§5, §9.2, §13); the write set, the lock table, escalation and SSI are C-T0 v8's (§2, §6, §8); rows and TOAST are C-T6-rehost's. A number or an account belongs to this file, which wins where another file states a different value. If PLAN-v3 disagrees with this file, this file wins.

**Error classes** (PostgreSQL SQLSTATEs):

| SQLSTATE | Here |
|---|---|
| 53200 | out of memory: a charge refused (§2.3); an open whose budget cannot hold its reservations (§4.2, §7.2); a tree past the height bound (§4.5) |
| 53300 | too many connections: a connection past the callers' count (§6.5) |
| 53100, 58030 | a temp-file write that fails while a consumer spills (§6.6): a statement error, never a poison |
| 22023 | invalid parameter value: open options that contradict each other (§7.3) |

## 1. Vocabulary

| Term | Meaning |
|---|---|
| budget `B` | The bytes one store instance may hold, fixed at open (§2.1). |
| store instance | One open of one file, read-write or read-only (PLAN-v3 N11). |
| account | A named holder of a share of `B`: its used bytes, its cap and its high-water mark (§2.3). |
| charge, credit | Adding bytes to an account before the allocation it covers; removing them after the free. |
| cap | The most an account may hold; a charge past it fails. |
| `R_fixed`, `R_file` | The reservations the open makes and only the store draws on: the fixed one (§5.1) and the file-proportional one with its growth allowance (§5.2). |
| shared pool `S` | The part of `B` that every session, transaction and cache account draws on (§2.2, §6). |
| poisoning context | A gated operation, a checkpoint step, a background store job's step or the WAL path: where S-STORE §9.2 makes a 53200 poison the store. |
| spill, shrink, evict, refuse | What a consumer does at its limit (§3, §6.6): move data to a temp file; do less per call; drop a cached entry; return 53200. |
| `n` | S-STORE §5.4's `threads`: `n = n_c + n_s`. |
| `n_c` | The callers' count the open takes: threads, or under N10 tasks, that can be inside storage calls at once (§6.5). |
| `n_s` | The store's own pinning threads as the build reports them (its checkpointer, evictors and job threads); 0 in N10's cooperative mode. |
| `s` | The reserve frames S-STORE §13 step 2 holds at once (§4.3), as the build reports it. |
| `F`, `F_min` | The pool's frame count, and its least value (§4.1, §4.2). |
| `H`, `H_min` | The open's height bound, and its floor (§4.5). |
| `o_frame` | The bytes each frame costs besides its page: its header, its page-table entry and its image-record list record (§4.1). |
| `M` | Pass 1's set at open: what recovery's pass 1 collects, with the recovery walk's record buffer (§5.3); credited when pass 2 ends. |
| base | The memory a store instance uses outside `B` (§2.5). |

## 2. The model

### 2.1 One budget per store instance
- Each store instance has one budget `B`, in bytes, fixed at open from configuration (§7.1 gives each mode's default). A read-only open is an instance of its own, with its own `B`. A process that opens several files holds one budget per file.
- Every allocation a store instance makes whose size depends on data, on workload, on configuration or on concurrency is charged to an account of `B` before it is made (I-MEM-ACCOUNTED). The table of §3 lists every consumer; nothing else allocates.
- The sum of every account's used bytes never exceeds `B`.

### 2.2 The partition at open
At open, `B` is divided in this order. Below, `R_fixed` is without its per-frame part, which `o_frame` carries.
1. `R_fixed`, the fixed reservation (§5.1): the checkpoint's buffers, the WAL writer's buffers, the job slots and, per frame, `o_frame`.
2. `R_file`, the file-proportional reservation (§5.2), with its growth allowance: first the terms the open computes from the meta it opens and the file's length, which it reads with no pool (S-STORE §14 step 1); then the root table, charged as S-STORE §14 step 2 builds it, and one entry for each CreateTree op that pass 2 will apply (§5.2).
3. `M`, pass 1's set (§5.3), charged as pass 1 builds it.
4. `S`, the shared pool, sized after step 3b (S-STORE §14), once the terms above are charged: the mode's session share `σ` of `B`, but at least the mode's session floor `S_min` (§7.1) and `M`, and at most what leaves the pool its minimum:

   `S = max(S_min, M, min(σ × B, B − R_fixed − R_file − F_min × (page_size + o_frame)))`

   `M` lies inside `S` until pass 2 ends, and is credited before any session exists.
5. The pool: `F` frames (§4.1) from what is left. `R_EPOCH`, `R_MAP` and the per-thread terms are counts of frames inside `F`, not memory of their own. Once the open is over, the pool neither grows (S-STORE §5.1) nor gives memory back, which is why `S` is sized before it; within the open, a fall-back drops every frame built from image `k` and sizes both again (§5.3).

**Before the sizing.** Steps 2–3b run before `S` and `F` are sized. Each charge above, and each frame the pool holds meanwhile, counts against `B` before its allocation (M1). Those frames hold only clean pages, since nothing before pass 2 dirties a page, so the pool drops unpinned frames before the open fails a charge; a charge that `B` still cannot hold fails the open with 53200. Meanwhile the height bound is §4.5's for the largest pool the budget allows, `⌊(B − R_fixed − R_file − S_min) / (page_size + o_frame)⌋` frames with `R_file` as charged so far, so step 2 refuses no tree that the sized pool would serve. The open sizes `S` and `F` after step 3b and before the retire write (S-STORE §14 step 4a) and pass 2, and then keeps at most `F` frames, dropping the rest. After a fall-back it charges and sizes again from meta `k−1` (§5.3).

**Why charged, not estimated.** The meta records no tree count (S-STORE §2.2), and step 2 reads the directory through the pool, so before the pool exists the open could size the root table only by a guess or by a bound. The one bound the meta gives, an entry for every LPID, reserves 64 bytes for every page of the file: 0.8% of it at 8 KiB pages, 800 MiB for a 100 GiB file whose 10,000 trees need 625 KiB. Charged as built, the table cannot be under-reserved: there is no estimate to fall short, and a table that does not fit fails the open with 53200 before its first write.

The open fails with 53200 when `F < F_min` (§4.2), that is when `B < B_min` (§7.2); its detail names `B_min`, the least budget that opens the file, from the terms charged so far. Such an open writes nothing, or after a fall-back nothing beyond what §5.3 lists.

### 2.3 The ledger
- Every account has a used amount, a cap and a high-water mark. A charge is the bytes requested from the allocator, capacity included, or an upper bound on them; a consumer whose memory a collection manages charges an upper bound per element.
- A charge to an account in `S` succeeds only if the account stays at or under its cap and `S`'s used bytes, with the growth allowance's debt paid first (§5.2), stay at or under `S`'s size. Otherwise it fails at once, and the consumer acts as §3 and §6.6 say.
- Accounts nest: a statement's charges count against its transaction's and its session's accounts, and a transaction's against its session's.

### 2.4 Three rules
- **M1, charge first** (I-MEM-ACCOUNTED). No allocation that §2.1 covers is made before its charge succeeds.
- **M2, nothing charged to `S` in a poisoning context** (I-MEM-STORE-PATHS). A gated operation, a checkpoint step, a background store job's step and the WAL path (S-STORE §9.2's list) charge nothing to `S`: each draws only on `R_fixed` and `R_file`, or on a charge its caller made before the context began. So no 53200 arises inside one but the two stated fail-stops of §5.4.
- **M3, no wait for memory** (I-MEM-NO-WAIT). A charge to `S` never waits. At its limit a consumer spills, shrinks, evicts or refuses with 53200 (§6.6). A 53200 from `S` fails the statement that asked, never poisons the store, and leaves every other account as it was. The store's own waits are S-STORE's: a frame acquisition waits only as S-STORE §5.3 and §5.4 bound it, and an appender waits on `written_end` for the WAL writer's buffers (S-STORE §9.2's wait list covers both).

Why M3: a session that waits for memory another session holds can sit in a cycle the lock table's deadlock detector does not see (a holder of a row lock waits for memory held by a session queued on that lock), and nothing would end it.

### 2.5 Outside the budget: the base
The base is what a store instance uses that does not depend on data, workload, configuration or concurrency: the binary's static memory, the stacks of the store's own threads (server mode), the allocator's metadata and fragmentation, and the standard library's lazy statics. It is measured (§8, §12), not budgeted. The embedding application's own memory is not the store's.

## 3. Consumers

Every consumer, its account, its bound, and what it does at its limit. "Card" is the PLAN-v3 §9 card that builds the consumer.

| Consumer | Account | Bound | At its limit | Mechanism | Card |
|---|---|---|---|---|---|
| buffer pool frames | `R_fixed` (the pool) | `F` | never allocates; a frame acquisition evicts (S-STORE §5.2) and a wait for an unpin is finite by S-STORE §5.4's count | S-STORE §5.1–§5.4 | A-3a, A-3b, A-3c |
| the epoch-copy reserve | frames inside `F` | `R_EPOCH` (§4.3) | a claim writes a copy out (S-STORE §5.6) | S-STORE §5.6 | A-3c, A-7a |
| MapLeaf pins of dirty pages | frames inside `F` | counted by `R_MAP` (§4.4) | never allocates | S-STORE §4.1, §5.4 | A-3c |
| the page table | `R_fixed` | `O(F)`, sized at open | never grows | S-STORE §5.1, §5.7 | A-3a |
| the image-record list, the map phase's buffers, the free set's serialisation | `R_fixed` | `F` records; `2 × L` page buffers (§5.1) | reserved at open; a checkpoint allocates none (mutant IMAGE-LIST-GROWS) | S-STORE §5.7, §13 step 2 | A-3c, A-7b |
| the free-set bitmaps | `R_file` | `⌈slot_count / 8⌉ + ⌈next_lpid / 8⌉` bytes | growth draws on the allowance (§5.2) | S-STORE §4.3, §5.7 | A-3c, A-7b |
| the pending-free sets | `R_file` | at most `⌈slot_count / 8⌉` bytes per generation, two generations | growth draws on the allowance | S-STORE §5.7, §13 step 6 | A-7c |
| MapInner pages | `R_file` | S-STORE §5.7's bound | map growth draws on the allowance | S-STORE §4.1 | A-3c, A-4a |
| the root table, with C-T0 §9.4's per-tree garbage estimate and entry count | `R_file` | `O(trees)`, about 64 bytes per tree | the open charges each entry as step 2 builds it, and one per CreateTree op that pass 2 will apply, before it sizes `S` and `F` (§2.2, §5.2); after the open, a CreateTree moves its entry's bytes from `S` into `R_file` before its gated operation, or its DDL statement fails with 53200; a DropTree's last step returns them, and so does the end of a transaction whose CreateTree is not applied | S-STORE §6.8, §14 step 2; C-T0 §9.4 | A-7b |
| the WAL writer's buffers | `R_fixed` | `W_buf` | an appender waits on `written_end` | S-STORE §12.4 | A-6c |
| a record's bytes until written | the writing transaction's write-set account | its records (§6.1) | charged with its layers; credited once `written_end` passes the record | S-STORE §12.2, §12.4; C-T0 §3.2 | B-1c, B-1d |
| background jobs: sweep tokens, drop-walk claims, finalize state | `R_fixed` (job slots) | `j` slots of one page | a job without a slot is not started until one frees | S-STORE §6.8, §10.4; C-T0 §9.4 | B-3, B-6, B-1d |
| recovery's pass 1 and its walk's record buffer | the open's charge, `M` | §5.3 | 53200 at open: nothing written, or after a fall-back nothing beyond the fence and image `k`'s pass-2 evictions (§5.3) | S-STORE §14 steps 3 and 4a | A-7d |
| a read-only open's replay | frames inside `F` | its dirty frames and the MapLeafs that S-STORE §4.1's pins hold for them | 53200 at open, nothing written | S-STORE §14.1 | A-7d |
| the write set and key log | per transaction, `ws_mem` | §6.1 | spills (C-T0 §2.1); 53200 when nothing can spill | C-T0 §2.1 | B-1a, B-1d |
| lock-table entries | per transaction, `lock_mem` | §6.2 | escalation, then 53200 (C-T0 §2.2, §6.2) | C-T0 §2.2, §6.2 | B-1b |
| SIREADs | per transaction, `ssi_mem` | §6.2 | v7.8's escalation (kept by C-T0 §8), then 53200; under pressure on `S`, the spill to disk of SSI state that C-T0 §8 keeps from v7.8 §8.5 | C-T0 §8 | B-2c |
| registries and an owner's shared state | the owning transaction or session | per entry | 53200 at registration | C-T0 §1, §2.2 | B-1b, B-1d |
| cursor batches and parked positions | per session, `cursor_mem` | §6.3 | a fill shrinks, to one entry; then 53200 | S-STORE §6.6 | A-4e; R-1 |
| sort, hash join, hash aggregate, window | per operator, `op_mem` | §6.4 | spill to temp files; 53200 for a row or group that does not fit | executor | C-4a–g |
| connection buffers (server) | per session, `conn_mem` | §6.4 | stream; 53200 for a message that cannot be streamed and does not fit | wire | C-1a |
| prepared statements, portals, the plan cache | per session, `plan_mem` | §6.4 | evict cached plans; 53200 for one plan over the cap | session | C-5, C-6 |
| the catalog cache | store-wide, `cat_mem`, in `S` | §6.4 | evict entries no statement pins; 53200 for one entry over the cap | catalog | B-4a |
| a WASM UDF call | per call, `udf_mem` | §6.4 | the call traps: 53200 | PLAN-v3 §3.10 | the extension cards |
| temp-file buffers | the spilling consumer's account | per spill | charged with the spill | — | B-1a, C-4a–g |
| a statement's working memory: its parse and plan, rows in flight, expression results and detoasted datums | the statement's account (§2.3), in `S` | what it holds at once | 53200 for a value or a row that does not fit; the wire streams what it sends (§6.4) | executor | C-4a–g |
| amcheck's sets (S-STORE §14 step 7) | at open, `S`, before any session; on a call, the calling session's | `⌈next_lpid / 8⌉ + ⌈slot_count / 8⌉` bytes, beside its walk's pins | 53200 to its caller, never a poison: it is a read path (S-STORE §9.2) | S-STORE §4.3 (I-FREE-EXACT), §6.8 (I-LPID-EXACT) | A-8 |
| a session's floor | per session, `sess_floor` | §6.5 | 53300 past the callers' count; 53200 when `S` cannot hold it | — | C-5, C-7 |

## 4. Pool numbers

### 4.1 F
- **Definition.** `F` is the number of frames in the buffer pool, fixed at open (S-STORE §5.1). Its uses: the pool (S-STORE §5.1); the minimum (S-STORE §5.4; §4.2 here); the image-record list's `F` records (S-STORE §5.6, §5.7, §13 step 2); the page table's `O(F)` (S-STORE §5.7); a read-only open's replay, which must fit beside the minimum (S-STORE §14.1); and E4's and A-9's pressure intervals of `4·F` distinct dirtied LPIDs, where `F` is "the pool frame count of the configuration under test" (PLAN-v3 §10 item 10).
- **From the budget.** `F = ⌊(B − S − R_fixed − R_file) / (page_size + o_frame)⌋`, with `S`, `R_fixed` and `R_file` as §2.2, §5.1 and §5.2 size them, `R_fixed` here without its per-frame part, which `o_frame` carries; the open sets it after S-STORE §14 step 3b (§2.2).
- **`o_frame`** is a frame's header (latch, pin count, dirty flag, content epoch and page id), its page-table entry, sized for `F` at open, and its image-record list record (§5.1). Provisionally 128 bytes; A-3b's build reports its value, and the open uses the reported one (§12).
- **Phase A** opens take `F` and `n_c` directly; dividing a budget is R-1's code (§11).

### 4.2 The minimum
S-STORE §5.4 states the minimum, `F ≥ threads × 3 × (max tree height + 2) + R_EPOCH + R_MAP`, and why every pin is counted. With this file's values (§4.3–§4.5), `threads = n` and the height at its floor:

`F_min = 4 × n × (H_min + 2) + 2 × (n + s + 1)`

that is, `3n(H_min + 2)` for the three per-thread terms, `R_EPOCH_min = n + s + 1`, and `R_MAP = n(H_min + 2) + R_EPOCH_min`. `n = n_c + n_s`: the callers' count the open takes and the build's own pinning threads as it reports them (S-STORE §5.4, `t_pool_minimum`). Below `F_min` the open fails with 53200 and writes nothing (S-STORE §5.4), or after a fall-back nothing beyond what §5.3 lists. A test build may lower `H_min`, as it may lower the map's fanout (S-STORE §4.1); nothing else changes.

### 4.3 R_EPOCH and the claim depth
**Who latches a reserve frame** (S-STORE §5.6 "Reserve", §13 step 2): a draw; a claim, and its re-check of a stale candidate; the install of a copy into the drawn or claimed frame, under the latch of the page it preserves; step 2's data phase, while it writes a copy; and step 2's map phase, which holds at most one detached frame out of the reserve, never during the data phase.

**The claim depth is 1.** A thread holds at most one reserve frame's latch at any instant:
1. While a claim holds its frame's latch, it writes the copy it claimed to a fresh slot and appends that copy's record to the image-record list; a draw or a claim then installs the new copy's bytes in the frame. Slot allocation takes no free-set lock and obtains no frame (S-STORE §4.2, §4.4, §4.5), and a file growth it causes draws on `R_file` (§5.2); the list is a leaf lock (S-STORE §13 step 2); the claim "takes no map latch and calls no `write_guard`" (S-STORE §5.6). So recording an evicted copy first-touches no MapLeaf, and nothing between the latch and its release draws or claims again.
2. `write_guard` draws or claims only in its copy step, which mutates no page but the copy it fills.
3. A dirty eviction sets its victim's map entry through `write_guard` on the victim's MapLeaf (S-STORE §4.1, §5.2), which may draw or claim. The evicting thread then holds no other reserve frame: an eviction is a frame acquisition, and none happens inside a draw or claim (item 1).
4. No latch is held across a yield (S-STORE §5.3), so N10's tasks add nothing.

The phrase "two claims per thread" (card S-1-F7's rework, c4397471) counted a claim that recorded its copy in the copy's MapLeaf through `write_guard`. Since card S-1-F7-F2-F2-F4b (33160e5d) a claim records into the image-record list, and S-STORE §5.6 says "one claim per thread". Mutant CLAIM-DEPTH-TWO (S-STORE §5.6) brings the second frame back.

**Step 2's share `s`.** The data phase holds a copy's latch while it writes it, `w` copies at a time (`w = 1` for a data phase that writes one page at a time); the map phase holds at most one detached frame (S-STORE §13 step 2). So `s = max(w, 1)`, which the build reports.

**The bound, confirmed.** A draw or claim by one thread finds an unlatched reserve frame when `R_EPOCH > (n − 1) + s`: the other threads hold at most one each, and step 2 at most `s`; an unlatched frame is free (a draw takes it) or a copy (a claim takes it). S-STORE §5.6 asks `R_EPOCH` to exceed `n + s`, "one claim per thread plus the copies step 2 is writing", which is sufficient with one frame of slack. So:

`R_EPOCH_min = n + s + 1`

**Above the minimum.** A larger reserve makes claims, which are synchronous writes on a writer's path during a checkpoint, rarer. The open sizes the reserve

`R_EPOCH = R_EPOCH_min + ⌊(F − F_min) / 32⌋`

which never lowers `H` below `H_min` (§4.5). The slope `1/32` is provisional; A-7e measures it (§12). A read-only open sizes it with its evictable part in place of `F` (§5.3): it runs no checkpoint (S-STORE §14.1), so it draws and claims nothing, and a reserve sized for `F` would only take frames from its replay.

### 4.4 R_MAP
**What S-STORE asks of it.** S-STORE §5.3's fairness bound needs, for every pin a thread may still take, a frame outside `R_EPOCH` that the thread can obtain without waiting for any other thread's call to end. S-STORE §5.4 counts each thread's pins in three terms and leaves `R_MAP` for the MapLeafs that §4.1 keeps pinned while a page they map is dirty.

**The classes.** Let `N = F − R_EPOCH`, the frames outside the reserve. At any instant each is in exactly one class:
- (T) pinned by a thread, in S-STORE §5.4's three per-thread terms; thread `t` holds `P_t` such pins and may still take `Q_t`, with `P_t + Q_t = 3(H + 2)`;
- (Mp) a MapLeaf that a thread needs for a page of its path term that it holds dirty, or is about to dirty, whichever pin holds the MapLeaf (§4.1's, or the thread's own);
- (Mc) a MapLeaf that only epoch copies' pins hold, if a copy holds one (below);
- (Mu) any other MapLeaf that §4.1's pins hold: every resident dirty page that holds it is unpinned;
- (U) a frame no pin holds.

**Obtainable frames.** A thread obtains a (U) frame by taking it, a dirty one by S-STORE §5.2's eviction. It obtains an (Mu) MapLeaf by evicting its unpinned dirty pages, each a (U) frame: the last eviction ends the MapLeaf's last pin, and the MapLeaf is then (U); a dirty MapLeaf's own eviction writes it and sets its parent's ref in a MapInner, outside the pool (S-STORE §4.1). Neither waits for another thread's call. So the frames a thread can obtain number

`O = |U| + |Mu| = N − |T| − |Mp| − |Mc|`

and `O ≥ 1` implies `|U| ≥ 1`, since an (Mu) MapLeaf has an unpinned dirty page: a thread that may still pin always finds a (U) frame, and waits at most for its brief latch holder (S-STORE §5.3). A read-only open evicts no dirty page (S-STORE §14.1), so there no (Mu) MapLeaf is obtainable, and its budget counts those MapLeafs with the replay's dirty frames instead (§5.3).

**The bounds.**
- `|T| ≤ Σ P_t = 3n(H + 2) − Σ Q_t` (S-STORE §5.4).
- `|Mp| ≤ n(H + 2)`. Of a thread's pins, only its path term's pages can be dirty under a MapLeaf the thread does not pin already: the reservation's pages lie under the reservation's MapLeafs, which are (T) (S-STORE §5.4's third term), and a chain build or free holds its chain pages' MapLeafs as (T) (S-STORE §5.4, §6.7). The path term is at most `H + 2` pages (S-STORE §5.4), so a thread needs at most `H + 2` MapLeafs of class (Mp), whichever pin holds them; that covers a MapLeaf a build pins before it dirties a page (§14, Q-MAPLEAF-DIRTY).
- `|Mc| ≤ R_EPOCH`: at most one MapLeaf per epoch-copy frame.

With `N = 3n(H + 2) + R_MAP`, `O ≥ Σ Q_t + R_MAP − n(H + 2) − R_EPOCH`, so

`R_MAP = n × (H + 2) + R_EPOCH`

gives `O ≥ Σ Q_t` at every instant, however the dirty pages' LPIDs lie: every pin a thread may still take has a frame it can obtain. At the minimum, `H = H_min`.

**Why no constant bounds §4.1's pins.** With scattered LPIDs each dirty page can hold a MapLeaf of its own, so the MapLeafs §4.1 pins number up to the resident dirty pages, and with both in the pool up to `⌊N / 2⌋`: they grow with `F`. `⌈dirty frames / 510⌉`, the figure S-STORE §5.4 gave at draft 7.15, is their number when dirty pages are packed onto MapLeafs (510 entries each at 8 KiB pages): the least, not the most. A constant can bound only the classes a frame acquisition cannot clear by itself, (Mp) and (Mc), and `R_MAP` counts those.

**The epoch copies' term.** S-STORE does not say whether an epoch copy holds a §4.1 pin on its page's MapLeaf (§4.1 says "dirty in the pool"; a claim sets no map entry, §5.6, so the pin would serve nothing). `R_MAP` counts one per copy, so it holds under either reading; if S-STORE rules that a copy holds none, the term is slack (§14, Q-COPY-PIN).

**The fairness bound holds with it.** S-STORE §5.3's premise, as draft 7.16 words it, is `O ≥ Σ Q_t`. Draft 7.15's wording, "a frame outside `R_EPOCH` that no pin holds" for every pin, is `|U| ≥ Σ Q_t`; a state whose (U) frames are all dirty pages, each alone under an (Mu) MapLeaf, has `|U| = |Mu| = (N − |T| − |Mp| − |Mc|) / 2`, so that wording would need `R_MAP ≥ Σ Q_t + |Mp| + |Mc|`, that is up to `3n(H + 2) + R_EPOCH`: two more tree paths per thread, for an unpinned frame the argument does not need before the thread arrives.

**A test may set `R_MAP` higher**, as S-STORE §5.4's pool-floor variant does: a larger value only adds frames.

### 4.5 The height bound
- **What it bounds.** S-STORE §5.4's `max tree height` is the open's height bound `H`: no descent pins a page deeper than `H`, and no root split makes a tree, the directory included, taller than `H`. A tree written under a larger pool may be taller; a read of it gets 53200, and a gated operation that descends it (a write, recovery's replay of one, a GC sweep step or a drop-walk step) poisons the store (S-STORE §19 T8). S-STORE §5.4 states the rule and its 53200s.
- **Its value.** The open sets `H` from its pool, as the largest height for which S-STORE §5.4's minimum holds with this file's `R_EPOCH` and `R_MAP`, since `3n(H + 2) + R_EPOCH + n(H + 2) + R_EPOCH ≤ F`:

  `H = ⌊(F − 2 × R_EPOCH) / (4 × n)⌋ − 2`

  A read-only open sets it with its evictable part in place of `F`, and with `R_EPOCH` sized the same way (§4.3, §5.3).

- **Its floor.** `H_min = 5`. An open whose `H` would be below it fails with 53200 and writes nothing; that is §4.2's minimum. With `R_EPOCH` as §4.3 sizes it, every `F ≥ F_min` gives `H ≥ H_min`, since `F − 2 × R_EPOCH ≥ F_min − 2 × R_EPOCH_min + (15/16)(F − F_min) ≥ 4n(H_min + 2)`.
- **Why 5** (owner point): the figure of the owner's point P-1 ("tree height 5"). At 8 KiB pages a tree of five levels with 100-byte entries holds about 6 × 10^9 of them; taller trees are served by pools above the minimum, where `H` grows with `F` (§4.6).
- **Why not a fixed limit.** A root split cannot fail logically (S-STORE §9.1), so a tree past a fixed limit could not be written again at any budget. With `H` from the pool a larger budget always serves the tree, and recovery replays the commit that failed (S-STORE §19 T8).

### 4.6 Worked numbers
8 KiB pages, `H_min = 5`, `s = 1`, `o_frame = 128` bytes, the §7.1 values; `R_fixed`, `R_file` and `B_min` as §5.1, §5.2 and §7.2 size them.

| Case | `B` | `n` | `F` | `F_min` | `R_EPOCH` | `H` | `R_MAP` |
|---|---|---|---|---|---|---|---|
| Browser, at its minimum (a 1 MiB file, 16 trees) | 2.97 MiB | 2 | 64 | 64 | 4 | 5 | 18 |
| Embedded, default (a 1 GiB file, 1,000 trees) | 64 MiB | 8 | 5,878 | 244 | 186 | 170 | 1,562 |
| Server, default on n5000 (38 GiB; a 100 GiB file, 10,000 trees) | 9.5 GiB | 20 | 916,452 | 604 | 28,642 | 10,737 | 243,422 |

Each row satisfies `3n(H + 2) + R_EPOCH + R_MAP ≤ F`. The arithmetic, in bytes:
- **Embedded.** `S = 16,777,216` (`σ × B`); `R_fixed = 1,212,416` (128 KiB of checkpoint buffers, 1 MiB of WAL buffers, four 8 KiB job slots); `R_file = 211,968` (§5.2: 16,384 + 16,384 for the free set, 32,768 for two pending-free generations, one 8,192-byte MapInner, 64,000 for 1,000 root-table entries, and an allowance of 8,704 + 65,536, its bitmaps sized by the 68 MiB step that follows a 64 MiB one); the pool is `67,108,864 − 16,777,216 − 1,212,416 − 211,968 = 48,907,264`, so `F = ⌊48,907,264 / 8,320⌋ = 5,878`; `R_EPOCH = 10 + ⌊(5,878 − 244) / 32⌋ = 186`; `H = ⌊(5,878 − 372) / 32⌋ − 2 = 170`; `R_MAP = 8 × 172 + 186 = 1,562`.
- **Browser at its minimum.** `F = F_min = 4 × 2 × 7 + 2 × 4 = 64`, `R_EPOCH = 4`, `H = ⌊56 / 8⌋ − 2 = 5`, `R_MAP = 2 × 7 + 4 = 18`, and `42 + 4 + 18 = 64`.
- **Server.** `S = 2,550,136,832` (2,432 MiB), `R_fixed = 16,973,824`, `R_file = 8,555,520`, pool `7,624,881,152`, `F = 916,452`, `R_EPOCH = 22 + ⌊(916,452 − 604) / 32⌋ = 28,642`, `H = ⌊(916,452 − 57,284) / 80⌋ − 2 = 10,737`, `R_MAP = 20 × 10,739 + 28,642 = 243,422`.

## 5. Store reservations

### 5.1 The fixed reservation

| Part | Size | Why it never grows |
|---|---|---|
| the frames | `F × page_size` | the pool never grows (S-STORE §5.1) |
| per frame | `F × o_frame`: frame headers, page-table entries (sized for `F` at open, never rehashed larger) and image-record list records (`F`, S-STORE §5.7) | fixed with `F` |
| the map phase's buffers and the free set's serialisation | `2 × L × page_size`, where `L` is the map's radix depth for a `u64` LPID: 9, 8, 7, 6 and 6 at 4, 8, 16, 32 and 64 KiB pages | reserved at open; a checkpoint allocates none (S-STORE §5.7, §13 step 2; IMAGE-LIST-GROWS) |
| the WAL writer's buffers | `W_buf` (§7.1) | appenders wait on `written_end` (S-STORE §12.4) |
| job slots | `j` slots (§7.1) of one page each, which hold a sweep's token (at most three keys of `K_MAX` bytes and its counts, S-STORE §10.4), a drop walk's claim or a finalize's position | a job without a slot is not started until one frees |

### 5.2 File-proportional memory and the growth allowance
- **What grows with the file:** the two free-set bitmaps, `⌈slot_count / 8⌉ + ⌈next_lpid / 8⌉` bytes (S-STORE §4.3, §5.7); the pending-free sets of two generations, each held in at most `⌈slot_count / 8⌉` bytes, as a bitmap where a list would be larger (S-STORE §5.7, §13 step 6); the MapInner pages, about one per `f²` LPIDs plus one per level on the path (S-STORE §4.1, §5.7); and the root table, `O(trees)`, about 64 bytes per tree (S-STORE §6.8; provisional, §12).
- **At open,** `R_file` is these for the file's length and the opened meta's `next_lpid`, plus the growth allowance and the root table, charged as §2.2 says.
- **The growth allowance** is what two growth steps would add. S-STORE §4.5's step `g = max(1 MiB, file_len / 16)` adds `g / page_size` slots, so a step adds `4 × ⌈g / (8 × page_size)⌉` bytes of bitmaps (the slot bitmap, the LPID bitmap for as many LPIDs, and two pending-free generations). The allowance is twice what the second of the next two steps adds (it is the larger: `g` taken at the length the first leaves), plus one MapInner path, `L × page_size`. A file growth, and the map growth (S-STORE §4.1 "Growth") and LPID allocation it allows, draw on the allowance; nothing they do charges `S` (M2).
- **Re-reserving.** After a draw on the allowance, the store moves free bytes of `S` into it, up to its size. The move never fails: it takes what `S` has free and records the rest as a debt, and while a debt stands every charge to `S` pays it first (§2.3), so no session is granted anything until the allowance is whole.
- **The root table.** The open charges each entry to `R_file` before step 2 inserts it (S-STORE §14 step 2), and, while step 3b reads the records pass 2 will apply (S-STORE §14 step 3b), one entry for each CreateTree op among them whose tree id the table does not hold, so that pass 2 installs no entry it has not charged; it sizes `S` and `F` only after both (§2.2). An entry's charge bounds the table's bytes per entry at any instant, its growth included (§2.3), so an install allocates nothing uncharged. After the open, a CreateTree moves its entry's bytes from `S` into `R_file` before its gated operation (S-STORE §6.8), or its DDL statement fails there with 53200; a DropTree's last step returns them to `S`, a credit, which M2 allows, and so does the end of a transaction whose CreateTree is not applied (an abort, or a `ROLLBACK TO` before it), so that no unapplied DDL leaves bytes in `R_file`. When step 2 meets an integrity failure or a read error on a directory page (S-STORE §14 step 4a), the entries charged before it go with everything else built from that image: a fall-back credits them and charges image `k−1`'s table (§5.3), and an open that fails frees them.
- **The residual.** The allowance covers two growth steps' bitmaps and one MapInner path. If sessions hold every free byte of `S` while the store draws more than that on it, the draw that finds it short raises 53200 inside a gated operation and poisons the store (S-STORE §9.2): at the latest the third growth, and sooner where LPID allocation outruns the file, which extends the LPID bitmap and the MapInner pages with no growth (a file whose free slots outnumber its free LPIDs, as after rewrites in two checkpoint intervals), or where one step's LPIDs need more MapInner pages than one path (files above about 127 GiB at 8 KiB pages). The remedy is a larger budget (§14, RM-T2).

### 5.3 Recovery and the read-only open
- **Pass 1's set `M`** (S-STORE §14 step 3) is charged as pass 1 builds it, before the open sizes `S` and `F`, and `S` is at least `M` (§2.2): about 24 bytes per `CommitMarker` above `T`, and at most one per `min(preapply_rows × 16 bytes, preapply_bytes)` of WAL span, since every chunked transaction wrote at least that much. `M` also holds the walk's record buffer, charged before each allocation it makes (M1): at most the longest record that pass 1, step 3b or pass 2 holds whole, itself at most `wal_seg_size / 4` (S-STORE §12.2), or the fixed buffer of a reader that takes a record in pieces. It is credited when pass 2 ends, before any session exists. If `B` cannot hold it beside the charges before it, the open fails with 53200; the detail names the bytes charged and the WAL position pass 1 had reached, so the caller can size a larger budget.
- **What a failing open has written.** Every charge §2.2 makes, and the sizing, come before pass 2, and nothing before pass 2 writes: the fence waits for the open's first write (S-STORE §14 step 1). So a 53200 from pass 1's set, the root table or the sizing writes nothing on the open's first pass through steps 2–3b.
- **After a fall-back** (S-STORE §14 step 4a). A read-write fall-back, an explicit rollback and a read-only open's salvage discard everything built from image `k` before they run steps 2–3b from meta `k−1`: its frames are dropped unwritten, and `R_file`'s terms from meta `k` (§2.2 item 2), its root table, its CreateTree entries and its `M` are credited. `R_file`'s terms from meta `k−1`, at the file's length then, and steps 2–3b from meta `k−1` then charge their own, and the open sizes `S` and `F` again (§2.2), so the two sets never stand together. A read-only open writes nothing at all (S-STORE §14.1). In a read-write open, when image `k` failed in step 2 nothing has been written, and when it failed in pass 2 the open has written at most the fence (S-STORE §14 step 1), if a meta place's read failed, and pass 2's evictions from image `k`, each into a slot that neither image references, and nothing else: S-STORE retires meta `k` only after steps 2–3b from meta `k−1` (item 2) and, unless the open is an explicit rollback, the proof (item 3), and the sizing precedes the retire write (§2.2). So a 53200 in the second pass 1, the second root table or the second sizing leaves "nothing written beyond the fence (step 1) and step 4a's pass-2 evictions", as S-STORE §14.2 words such outcomes: meta `k` stays, and image `k−1` and the WAL from meta `k−1`'s `wal_start_lsn` are as they were (S-STORE I-TWO-GEN). A later open selects meta `k` again and meets image `k`'s failure again, or opens meta `k` if the failure does not recur; with a budget that holds meta `k−1`'s charges it falls back as this open would have, by itself, as an explicit rollback or as a read-only salvage. So a 53200 in a fall-back's pass 1 or sizing leaves no state that an open with a larger budget cannot recover (`t_mem_open_sizes`, fall-back form).
- **Pass 2 runs in the pool** (S-STORE §14.1). A read-only open keeps the replay's dirty frames, and with them the MapLeafs that S-STORE §4.1's pins hold for them: it evicts no dirty page, so it releases neither, and §4.4's (Mu) frames are obtainable only through such evictions. What is left, its evictable part `F_e` (`F` minus both), stands for `F` in its sizing: it sets `R_EPOCH` (§4.3) and `H` (§4.5) from `F_e`, at open and again before each op that pass 2 replays, since `F_e` only falls as pass 2 dirties pages, and keeps the last values once pass 2 ends, when nothing dirties a page any more; a descent past the current `H` fails the open with 53200. So `F_e` must stay at least `F_min`, or the open fails with 53200 and writes nothing (S-STORE §14.1; `t_ro_open`); with scattered LPIDs the two together are up to twice the dirty frames. Sized from `F`, the open would be wrong either way: a reserve and a height bound that grow with frames the replay holds would leave a pool whose `F_e` is `F_min` with `F_min − R_EPOCH` frames outside the reserve, fewer than §4.4 counts once `F ≥ F_min + 32` and none at §4.6's server default, so the replay could wait for an unpin that never comes; and §5.4's minimum at `F`'s own `H` lies within `4n` frames of `F`, so a replay whose dirty frames and MapLeafs reach `4n` would fail at every budget. The detail names the dirty frames and those MapLeafs at the failure, so the caller can size a larger read-only budget.

### 5.4 No 53200 inside a poisoning context

| Path | Draws on | Card |
|---|---|---|
| an apply insert, a split, a pack step (S-STORE §6.4, §6.5, §9.1) | frames (§4); the LPID bitmap, MapInners and file growth from `R_file` | A-4a, A-4b, B-1c |
| a large transaction's materialisation, flip and finalize (C-T0 §3.2) | frames; chunk bytes already charged to its write set (§6.1) | B-1d |
| a dirty eviction (S-STORE §5.2) | frames; `R_file` for growth | A-3c |
| a checkpoint step (S-STORE §13) | `R_fixed`'s list and buffers; `R_file` | A-3c, A-7b, A-7c |
| a background job's step: the sweep, the drop walk, finalize (S-STORE §6.8, §10.4) | a job slot; frames | B-3, B-6, B-1d |
| the WAL path (S-STORE §12) | `R_fixed`'s writer buffers; record bytes already charged (§6.1) | A-6b, A-6c |
| recovery after its first write (S-STORE §14) | the charges §2.2 makes before pass 2; a fall-back makes its own again from meta `k−1` before the retire write, and a 53200 there writes nothing more (§5.3) | A-7d |

**The two stated fail-stops:** (1) a root split past the height bound, or a gated operation's descent past it (§4.5; S-STORE §5.4); (2) a file growth whose allowance is short (§5.2). Each raises 53200, which S-STORE §9.2 makes a poison. Neither is reachable at a budget well above its minimum with sessions that leave `S` room (§14, RM-T1 and RM-T2).

## 6. Session and transaction accounts

### 6.1 Write set and records
- C-T0 §2.1's write set, with its key log, is a per-transaction account with cap `ws_mem`. Its spill (sorted runs of non-top layers; a large transaction's materialisation of top layers as provisional entries) and its 53200 "when nothing can spill" are C-T0's.
- **A record's bytes.** A transaction's write-set charge covers the WAL record bytes its commit or a materialisation will serialise: a layer's charge includes its op's encoded size and its share of the framing. The bytes stay charged until `written_end` passes the record's end (S-STORE §12.4), then are credited. So `COMMIT` and a materialisation charge nothing new to `S`, and a commit never fails for memory once its statements have succeeded.
- **Thresholds per mode** (§7.1): `preapply_rows`, `preapply_bytes`, `ws_mem` and `chunk_batch`. C-T0 §2.1's two constraints hold in every mode: `ws_mem ≥ preapply_bytes`, and `preapply_bytes ≤ wal_seg_size / 4`; on a file whose segments are smaller than four times the mode's `preapply_bytes`, the open lowers it to `wal_seg_size / 4` (2 MiB at C-T6-rehost's least segment, 8 MiB).
- W6 measured 117 B per key for an in-memory write set (`spikes/e1-bench/README.md` §W6); B-1a re-measures it on the real write set (§12).

### 6.2 Lock table and SIREADs
- C-T0 §2.2's entries are a per-transaction account with cap `lock_mem`; escalation (C-T0 §6.2) keeps it bounded, and an acquire past it after escalation fails with 53200 (C-T0 §2.2). The entries a release leaves are credited when C-T0 §3.3's purge frees them.
- The defaults (§7.1) keep `lock_escalate_rows × e_lock ≤ lock_mem`, with `e_lock` the bytes of an entry with an average key (provisionally 128; B-1b measures), so in one tree escalation comes before 53200. A transaction that locks below the threshold in many trees can still meet 53200.
- SIREADs (C-T0 §8, which keeps v7.8's granularity and escalation) are a per-transaction account with cap `ssi_mem`. A committed transaction's retained SIREADs (C-T0 §8.6) stay charged to it until retention ends, or until C-T0 §8's spill moves them to disk: v7.8 §8.5, which C-T0 §8 keeps, says SSI state "is never summarised in 2.0; it spills to disk under memory pressure", and S-2b states that spill. Its temp-file buffers are charged as §6.4's are; this file does not replace the spill with a 53200.
- The live-txn registry's entry, the snapshot registry's entries and an owner's shared state (C-T0 §1, §2.2) are charged to their transaction or session when they are registered.

### 6.3 Cursors
- A session's cursors share one account, cap `cursor_mem` (§7.1). An open cursor's parked position (S-STORE §6.6: its tree id, its snapshot, and its resume key with the group's carried state, at most `K_MAX` bytes and a few words) is charged at OPEN; its batch buffer at each fill.
- **At the limit.** A fill copies out what the account allows: at least one entry, at most A-4e's per-call bound (S-STORE §6.6). An OPEN or a FETCH that cannot hold one entry or one parked position fails with 53200. A smaller batch only means more calls; parking makes every batch size exact (S-STORE §6.6, "Why parking is exact").
- **At close.** A cursor's buffers are credited when it closes, a close by `ROLLBACK TO` included (C-T0 §5.5), and the account returns to its value before the OPEN (I-MEM-RETURN; S-STORE §15.1's R-1 row of `t_cursor_park_equiv`, BATCH-KEPT-AFTER-CLOSE).

### 6.4 Operators, connections, plans, the catalog cache, UDFs and temp files

| Consumer | Account and cap | At the limit | Card |
|---|---|---|---|
| sort, hash join, hash aggregate, window | per operator, `op_mem` | spills to temp files through `Env`, as C-T0 §2.1's write set does; 53200 when one row or one group does not fit | C-4a–g |
| connection buffers (server) | per session, `conn_mem` | streams a message larger than its buffer (COPY, wide rows); 53200 for a message that cannot be streamed and does not fit | C-1a, C-1b |
| prepared statements, portals, the plan cache | per session, `plan_mem` | evicts cached plans, which re-plan on use; 53200 for one plan over the cap | C-5, C-6 |
| the catalog cache | store-wide, `cat_mem`, in `S` | evicts entries no statement pins; 53200 if one entry does not fit | B-4a |
| a WASM UDF call | per call, `udf_mem` (PLAN-v3 §3.10) | the call traps and fails with 53200 | the extension cards (2.1) |
| temp-file buffers | the spilling consumer's account | charged with the spill | B-1a, C-4a–g |

### 6.5 Admission
- **The callers' count `n_c`** (S-STORE §5.4's callers) is enforced where callers enter, so that no more than `n_c` threads, or tasks under N10, are inside storage calls at once:
  - server mode: the worker pool has `n_c` threads; a session waits for a worker and is not refused;
  - embedded and browser mode: at most `max_connections = n_c` connections are open, each used by one thread or task at a time; one more fails with 53300 (I-MEM-ADMIT).
- **The session floor.** A session charges `sess_floor` (§7.1) at connect: its connection buffers' least size, its registry entries and its transactions' commit bookkeeping (C-T0 §3.1's queue entry). If `S` cannot hold it, the connection fails with 53200.

### 6.6 At the limit
1. A charge fits within its account's cap, or the consumer acts at its cap: it spills, shrinks, evicts or refuses.
2. If `S` itself is full, the catalog and plan caches evict entries no statement pins, and the charge is tried once more.
3. Otherwise the charge fails with 53200: the statement fails, and its transaction is aborted as for any error (C-T0); nothing waits (M3) and nothing is poisoned.
4. A temp-file write that fails (53100, 58030) while a consumer spills fails its statement the same way: a spill is none of S-STORE §9.2's contexts.

## 7. Modes

### 7.1 Per-mode values
Proposed for the owner (PLAN-v3 §10 records the answer); the text above is written for them, and each is an open option.

| Value | Server | Embedded | Browser | Basis |
|---|---|---|---|---|
| `B`, default | 25% of physical memory, or of the cgroup limit when lower; at least 256 MiB | 64 MiB | 32 MiB | PostgreSQL's docs give 25% for `shared_buffers` (E3 ran PG17 at 6 GB of 38 GiB), and NBench's OLTP target is RSS ≤ PG17's; an embedded host keeps the rest; a browser tab on a phone |
| `σ`, the session share | 0.25 | 0.25 | 0.25 | the pool is the cache |
| `S_min` | `preapply_bytes` + 1 MiB | `preapply_bytes` + 1 MiB | `preapply_bytes` + 1 MiB | one session's small transaction at the large threshold, its locks, one operator, one cursor and its catalog entries |
| `n_c` | worker threads, one per CPU (16 on n5000) | `max_connections` = 8 | `max_connections` = 2 | PLAN-v3 N10 |
| `n_s` | 4: the checkpointer, one evictor, two job threads | 0 | 0 | N10's cooperative mode |
| page size | 8 KiB | 8 KiB | 8 KiB | S-STORE §2.1's default |
| `H_min` | 5 | 5 | 5 | owner point P-1's figure (§4.5) |
| `preapply_rows` | 4,096 | 4,096 | 4,096 | C-T0 §2.1's value; B-1d measures the stall |
| `preapply_bytes` | 4 MiB | 4 MiB | 1 MiB | C-T0 §2.1's value; a browser's records kept small; at most `wal_seg_size / 4` |
| `ws_mem` | 64 MiB | 16 MiB | 4 MiB | C-T0 §2.1's value for the server; at least `preapply_bytes` |
| `chunk_batch` | 1 MiB | 1 MiB | 256 KiB | C-T0 §3.2's value; a browser's sync costs more per byte (E6) |
| `lock_escalate_rows` | 1,000,000 | 100,000 | 10,000 | W6's working value for the server; scaled with `lock_mem` |
| `lock_mem` | 128 MiB | 16 MiB | 2 MiB | `lock_escalate_rows × e_lock`, rounded up |
| `ssi_mem` | 16 MiB | 4 MiB | 1 MiB | proposed; B-2c measures |
| `cursor_mem`, per session | 4 MiB | 2 MiB | 512 KiB | a few of A-4e's batches per open cursor |
| `op_mem`, per operator | 16 MiB | 8 MiB | 2 MiB | above PostgreSQL's `work_mem` default (4 MB); spilling makes any value correct |
| `conn_mem`, per session | 256 KiB | — | — | the wire's buffers; COPY streams |
| `plan_mem`, per session | 2 MiB | 1 MiB | 256 KiB | proposed |
| `cat_mem`, per store | 64 MiB | 8 MiB | 2 MiB | proposed |
| `sess_floor` | 64 KiB | 16 KiB | 16 KiB | connection buffers and bookkeeping |
| `W_buf` | 16 MiB | 1 MiB | 256 KiB | PostgreSQL's `wal_buffers` cap (16 MB) |
| `j`, job slots | 8 | 4 | 2 | sweeps, drop walks and finalizes in flight |
| `R_EPOCH`'s slope | 1/32 | 1/32 | 1/32 | provisional; A-7e |
| the AS OF retention window | 0 | 0 | 0 | C-T0 §12 O6; PostgreSQL has no AS OF |
| `gc_min_versions`, `gc_ratio` | 10,000; 0.1 | 10,000; 0.1 | 10,000; 0.1 | C-T0 §9.4's values, unchanged |

### 7.2 The minimum budget

`B_min = F_min × (page_size + o_frame) + R_fixed + R_file + max(S_min, M)`

with `F_min` from §4.2, `R_fixed` without its per-frame part, `R_file` for the file, its root table included, `S_min` the mode's and `M` pass 1's set with the walk's record buffer (§5.3), 0 when the walk reads no record, as in every row below. Worked with 8 KiB pages and `o_frame = 128` bytes:

| Mode | `n` | `F_min` | frames | `R_fixed` | `R_file` | `S_min` | `B_min` |
|---|---|---|---|---|---|---|---|
| Browser (a 1 MiB file, 16 trees) | 2 | 64 | 520 KiB | 400 KiB | 73 KiB | 2 MiB | ≈ 2.97 MiB |
| Embedded (a 1 GiB file, 1,000 trees) | 8 | 244 | 1,982.5 KiB | 1,184 KiB | 207 KiB | 5 MiB | ≈ 8.29 MiB |
| Server (n5000; a 100 GiB file, 10,000 trees) | 20 | 604 | 4,907.5 KiB | 16,576 KiB | 8,355 KiB | 5 MiB | ≈ 34.14 MiB |

The open fails with 53200 below `B_min` and writes nothing, or after a fall-back nothing beyond what §5.3 lists (I-MEM-OPEN). `B_min` grows with the file through `R_file`: about four bits per slot, and about 64 bytes per tree.

### 7.3 Configuration
Every §7.1 value is an open option. Options that contradict each other fail the open with 22023 and write nothing: `ws_mem < preapply_bytes`; `S_min < preapply_bytes`; `n_c = 0`; `σ` outside `(0, 1)`. A budget below `B_min` fails it with 53200 (§7.2). A per-account cap above `S` is not an error: once the open has sized `S` (§2.2), it lowers every such cap to `S`, and `lock_escalate_rows` to `⌊lock_mem / e_lock⌋` where the lowered `lock_mem` needs it, so that §6.2's order holds, and reports both (§8). At `B_min`, `S` is `S_min` or `M`, below several of §7.1's caps, so without the lowering no mode's defaults would open at their own `B_min`; `ws_mem ≥ preapply_bytes` still holds, since `S ≥ S_min ≥ preapply_bytes`.

## 8. Observability and the oracle
- **The report.** A store instance reports every account's used bytes, cap and high-water mark; `S`'s free bytes and debt; and `F`, `R_EPOCH`, `R_MAP`, `H`, `n_s`, `s` and `o_frame`. D-6c exports them as metrics (PLAN-v3 §3.9).
- **The oracle is the accounts, not VmHWM** (PLAN-v3 §4, X-4 ruling R2 item 2). Tests compare the accounts with a counting global allocator in the test binary: a dev-dependency allocator wrapper (dhat's testing mode, or one like it; the card that adds it justifies the dependency, PLAN-v3 §6.1); no `unsafe` in nucleus crates (PLAN-v3 N9). Samples are taken only at quiescent points (no statement and no job step in flight), with a fixed slack of 64 KiB for allocations outside the engine (the harness's own and the standard library's lazy statics).
- **RSS** is G9's check (PLAN-v3 §6.2): RSS at most `B` plus the base plus the allocator's fragmentation, the base and a fragmentation allowance measured per mode by D-13's soak (§12).

## 9. Invariants

Each invariant has a test and a named mutant that the test catches; "Pinned by" names the PLAN-v3 §9 card that runs it (§10 gives the forms).

| ID | Statement | Tests | Pinned by | Named mutants |
|---|---|---|---|---|
| I-MEM-ACCOUNTED | §2.1, §2.3, M1: every allocation that depends on data, workload, configuration or concurrency is charged first, and the accounts' sum never exceeds `B` | `t_mem_heap_accounted` | R-1; D-13 | UNCHARGED-CONSUMER |
| I-MEM-STORE-PATHS | §2.4 M2, §5.4, §6.1: nothing is charged to `S` from a gated operation, a checkpoint step, a background job's step or the WAL path, and `COMMIT` and a materialisation charge nothing new | `t_mem_store_paths_saturated` | R-1 | STORE-PATH-SHARED-CHARGE |
| I-MEM-NO-WAIT | §2.4 M3, §6.4, §6.6: a charge to `S` never waits; at its limit a consumer spills, shrinks, evicts or refuses with 53200, which never poisons | `t_mem_caps` | R-1; C-4a–g; C-1a; C-6 | CAP-WAITS, CAP-POISONS, OP-NO-SPILL, CONN-WHOLE-MESSAGE, PLAN-NO-EVICT |
| I-MEM-RETURN | §6.1–§6.3: when a consumer's owner ends (a statement, a cursor, a transaction once its purge has run, a session), its account returns to its value before the owner began, and its spill files are gone | `t_mem_account_returns`, `t_cursor_park_equiv` | R-1 | ACCOUNT-NOT-CREDITED, BATCH-KEPT-AFTER-CLOSE |
| I-MEM-OPEN | §2.2, §4, §5.3, §7.2: the open sizes `S`, `R_fixed`, `R_file`, `F`, `R_EPOCH`, `H` and `R_MAP` as this file says, `S` and `F` only once the root table and pass 1's set are charged, and again from meta `k−1` after a fall-back, with image `k`'s charges credited first; below `B_min` it fails with 53200, writing nothing beyond what §5.3 lists | `t_mem_open_sizes` | R-1 | OPEN-NO-FILE-TERM, POOL-SIZED-BEFORE-TABLE, FALLBACK-SET-KEPT |
| I-MEM-GROWTH | §5.2: a file growth draws on the allowance, which the store re-reserves before `S` grants a session anything | `t_mem_growth_allowance` | R-1 | ALLOWANCE-UNRESERVED |
| I-MEM-ADMIT | §6.5: no more than `n_c` callers are inside the store at once; an embedded or browser connection past `max_connections` fails with 53300 | `t_mem_admission` | C-7; D-5a/b | ADMIT-OVER-COUNT |

The pool's numbers are S-STORE invariants with this file's values (S-STORE §15, §15.1): `R_EPOCH`'s bound and the claim depth are I-CKPT-AT-T's floor variant of `t_epoch_reserve_two_exhausters` (A-3c, A-7a; RESERVE-PER-THREAD-ONLY, CLAIM-DEPTH-TWO); `F_min`, `R_MAP` and `H` are I-POOL-BOUND's `t_pool_minimum` (A-3b; MIN-RMAP-DENSE, MIN-RESERVE-NO-STEP2), `t_pool_floor_scattered` (A-3c, and A-4a in full; MAPPIN-KEPT) and `t_height_bound` (A-4a, and A-7f in full; HEIGHT-SPLIT-UNCHECKED, HEIGHT-DESCENT-UNCHECKED).

**The tests.** Each runs with no thread of the store's own (N10's cooperative mode) and no background job, checkpoint or GC except those it calls, unless it says otherwise.

- **`t_mem_heap_accounted`** (R-1; D-13 in G9's soak with every consumer). A counting global allocator (§8) and the embedded defaults. After open, with no session, the test samples `base = live heap − Σ used`. Then, each step ending at a quiescent point where it samples `live heap − base` against `Σ used`: (i) four sessions commit small transactions up to the large threshold; (ii) a large transaction with savepoints materialises and spills its non-top layers past `ws_mem`, then commits; (iii) a transaction takes `lock_escalate_rows − 1` row locks in one tree, a sample is taken, then one more lock (escalation) and 1,000 in a second tree, and another sample; (iv) eight cursors over one tree are opened, fetched in part and left parked; (v) a checkpoint; (vi) a GC sweep of a tree with garbage; (vii) a DropTree and its walk. At every sample `live heap − base ≤ Σ used + 64 KiB` and `Σ used ≤ B`. UNCHARGED-CONSUMER (the lock table allocates its entries without charging them) fails step (iii)'s first sample by about `(lock_escalate_rows − 1) × e_lock`, 12 MiB at the embedded defaults.
- **`t_mem_store_paths_saturated`** (R-1). Embedded mode with `B = B_min + 32 × (page_size + o_frame)`, so that `S` is `S_min` (`σ × B` is below it) and the pool 32 frames above its minimum, so that applies evict, and a file at the end of its last growth step so that evictions grow it. Session P1 writes a small transaction whose keys force leaf splits and an LPID past the last MapLeaf; session P2 writes a large transaction that materialises. Then sessions X1, X2, … charge cursor batches and write sets until `S` refuses one with 53200, and stay idle holding what they hold. Then P1 commits; P2 commits and the test runs its finalize; a checkpoint runs; a GC sweep runs over a tree with garbage; a DropTree and its walk run; and the commits' evictions grow the file twice. Each completes, none returns 53200, and a read afterwards succeeds (no poison). The store marks, thread-locally, entry to a gated operation, a checkpoint step, a job step and the WAL path; a hook in the ledger's charge records each charge's context, and no charge to `S` comes from one. STORE-PATH-SHARED-CHARGE (an LPID allocation charges the LPID bitmap's growth to `S`) fails it: P1's apply meets 53200 inside a gated operation and poisons the store, and the hook fires. Held still: X1, X2, … are idle (no statement in flight), and no other session runs.
- **`t_mem_caps`** (R-1 forms; C-4a–g, C-1a and C-6 forms). Caps set low by open options; `S` large unless a form says otherwise. Each form's error goes to its statement only, and afterwards a commit and a checkpoint succeed (no poison).
  - (w) A large transaction's non-top layers pass `ws_mem`: they spill to sorted runs, and it commits. Then a hook fails a temp-file write with 53100: the statement that spills fails with 53100, and the store is not poisoned.
  - (l) A transaction locks rows in three trees, each below `lock_escalate_rows`, until its entries pass `lock_mem`: that acquire fails with 53200.
  - (s) A SERIALIZABLE transaction's SIREADs pass `ssi_mem` after v7.8's escalation: 53200.
  - (c) Cursors over one tree with `cursor_mem` small: each fill returns fewer entries as the account fills, at least one, and each fetch equals the oracle's; an OPEN that cannot hold its parked position fails with 53200.
  - (x) Session A holds a row lock on key `k`. Session B charges until `S` is full, then waits on `k` in the lock table; a hook fires when B parks there (every build parks a lock-table waiter, C-T0 §6.3). Then A's next statement needs a charge: A gets 53200 at once, its transaction aborts, its lock is released, and B goes on. A holds its lock-table entry and no latch; B holds its accounts and waits with the poison mark in its predicate (S-STORE §9.2); A's charge takes nothing B holds. CAP-WAITS (a charge at its limit waits for memory) leaves A waiting for memory B holds while B waits for A's lock: neither finishes within the step budget. CAP-POISONS (a 53200 from `S` sets the poison mark) fails every form's later commit.
  - At C-4a–g: a sort, a hash join, a hash aggregate and a window, each past `op_mem`, return the oracle's rows, having spilled; a row larger than `op_mem` fails with 53200. OP-NO-SPILL (an operator at `op_mem` fails with 53200 where it could spill) fails them. At C-1a: a COPY whose messages exceed `conn_mem` is streamed and loads every row; CONN-WHOLE-MESSAGE (the wire reads a whole message into one buffer) fails it with 53200. At C-6: `PREPARE`s past `plan_mem` evict cached plans, and each `EXECUTE` returns the oracle's rows; PLAN-NO-EVICT fails the next `PREPARE` with 53200.
- **`t_mem_account_returns`** (R-1). For each consumer the test records its account, runs an owner and ends it, then compares: a statement that spills its write set's non-top layers and ends by `ROLLBACK TO` and then `ROLLBACK`; a transaction that escalates and commits, compared after C-T0 §3.3's purge, which a hook reports; a large transaction that materialises and aborts, compared after its cleanup, which a hook reports; a session that opens cursors and closes its connection. Each account equals its value before, and `Env` lists no spill file. ACCOUNT-NOT-CREDITED (a spilled run's index is freed without its credit) leaves the write-set account above its value before. `t_cursor_park_equiv`'s close variant (S-STORE §15.1, the R-1 row) checks the cursor account through `ROLLBACK TO` (BATCH-KEPT-AFTER-CLOSE). Each comparison waits for the hook that reports the owner's last free, never for a while.
- **`t_mem_open_sizes`** (R-1). For the browser and embedded defaults with a small file of 300 trees, so that its root table is larger than one frame's bytes, and server mode with `n_c = 4`, the test computes `S`, `R_fixed`, `R_file`, `F`, `R_EPOCH`, `H`, `R_MAP` and `B_min` from §2.2, §4, §5 and §7.2, from the build's reported `o_frame`, `n_s` and `s` and the file's length, its meta's `next_lpid` and its directory, and compares the open's report with them. An open at `B_min − 1` bytes fails with 53200, and a write hook sees nothing written; an open at `B_min` succeeds. OPEN-NO-FILE-TERM (the open leaves `R_file` out of `F`'s computation) accepts `B_min − 1`, since `R_file` is larger than one frame for every file (its growth allowance holds a MapInner path). POOL-SIZED-BEFORE-TABLE (the open sizes `S` and `F` before step 2, without the root table, and then charges the table to `S`) reports an `F` at least one frame above the computed one, at every budget.
  - *Fall-back form* (S-STORE §14 step 4a). The fixture of S-STORE's `t_fallback`, proven case, with its fault on a leaf that pass 2 replays into: a SimEnv read hook corrupts every read of that leaf's slot, which only image `k` references, until the slot is next written. `S_min`, `preapply_rows` and `preapply_bytes` are set low by open options, and chunked transactions commit between checkpoints `k−1` and `k`, and at least one after checkpoint `k`, so that pass 1's set from meta `k`, `M`, is not 0 and the set from meta `k−1`, `M′`, is larger than `M` and than `S_min`; and enough commits after meta `k`, each dirtying pages of its own, come before the leaf's in the WAL that pass 2 from image `k` evicts before it reaches the leaf, each into a free slot that neither image references, so that no open of the form grows the file (a SimEnv hook sees no extension): a growth would make `R_file` larger than the read-only reports give, and `B′` short by it on a correct build. The test takes `M` from the report of a read-only open of a copy of the files with no fault, and `M′` and image `k−1`'s root table from the report of a read-only open of a copy in which it has zeroed both copies of meta `k`; neither open falls back. `B′` is §7.2's `B_min` for meta `k−1` with `M′`, and the fixture holds enough of those transactions that §7.2's `B_min` for meta `k`, which the test computes from the first report, is below `B′`. (i) A read-write open of a copy at `B′` falls back with proof and recovers every acked commit. (ii) A read-write open of another copy at `B′ − 1` fails with 53200 when it sizes from meta `k−1`; a SimEnv write hook sees no write but pass 2's evictions from image `k`, each into a slot that neither image references (the fixture fails no meta place's read, so there is no fence), and meta `k`'s two places and every WAL file are byte-identical afterwards; an open of the same files at `B′` then falls back with proof and recovers every acked commit. Each open runs alone on the test's one thread, in N10's cooperative mode (no thread of the store's own), and holds N11's file lock from its start until it fails or closes, the read-only opens in the lock's read-only mode; the two hooks run on that thread and take no lock. FALLBACK-SET-KEPT (step 4a's discard leaves image `k`'s `M` charged, so that pass 1 from meta `k−1` charges beside it) fails (i) with 53200.
- **`t_mem_growth_allowance`** (R-1). A file at the end of its last growth step; sessions X1, X2, … hold every free byte of `S`, each charged until 53200. (1) A commit's evictions grow the file: no 53200, no poison, and the report shows the allowance one step short (a debt). (2) A session's charge fails with 53200. (3) X1 ends, freeing at least one step's bytes: the report shows the allowance whole again before any session is granted anything. (4) A charge of the freed bytes beyond the debt succeeds, and one byte more fails. (5) Two more growths complete with no 53200 and no poison. ALLOWANCE-UNRESERVED (no debt is kept: freed bytes go to sessions first) fails (3) and (4), and (5)'s second growth raises 53200 and poisons the store. Held still: no other session runs; the file grows only when the test's commits evict.
- **`t_mem_admission`** (C-7 embedded; D-5a/b browser). A store opened with `max_connections = 3` opens three connections, and a fourth fails with 53300; after one closes, another opens. ADMIT-OVER-COUNT (connections are not counted) opens the fourth.

## 10. Test ownership

One row per card that runs a test or a form of it. *Needs* names the cards whose code the form uses besides its own; the Phase A rows are S-STORE §15.1's, repeated here for the reader, and S-STORE wins where the two differ.

| Test | Invariant | Card | Needs | Form | Mutants judged here |
|---|---|---|---|---|---|
| `t_mem_heap_accounted` | I-MEM-ACCOUNTED | R-1 | B-1a, B-1b, B-1c, B-1d, A-4e, A-7c, B-3, B-6 | full for Phase B's consumers: write set, records, lock table, large transactions, cursors, checkpoint, GC, drop walk | UNCHARGED-CONSUMER |
| `t_mem_heap_accounted` | I-MEM-ACCOUNTED | D-13 | R-1, C-4a–g, C-1a, C-6 | full, every consumer of §3, inside G9's soak | UNCHARGED-CONSUMER |
| `t_mem_store_paths_saturated` | I-MEM-STORE-PATHS | R-1 | B-1c, B-1d, A-7c, B-3, B-6 | full | STORE-PATH-SHARED-CHARGE |
| `t_mem_caps` | I-MEM-NO-WAIT | R-1 | B-1a, B-1b, B-2c, A-4e | forms (w), (l), (s), (c) and (x) | CAP-WAITS, CAP-POISONS |
| `t_mem_caps` | I-MEM-NO-WAIT | C-4a–g | R-1 | the operator forms | OP-NO-SPILL |
| `t_mem_caps` | I-MEM-NO-WAIT | C-1a | R-1 | the connection form | CONN-WHOLE-MESSAGE |
| `t_mem_caps` | I-MEM-NO-WAIT | C-6 | R-1, C-5 | the plan form | PLAN-NO-EVICT |
| `t_mem_account_returns` | I-MEM-RETURN | R-1 | B-1a, B-1b, B-1d, B-2e, A-4e | full for Phase B's consumers | ACCOUNT-NOT-CREDITED |
| `t_cursor_park_equiv` | I-MEM-RETURN | R-1 | A-4e, B-2e | S-STORE §15.1's R-1 row: the close variant through `ROLLBACK TO` | BATCH-KEPT-AFTER-CLOSE |
| `t_mem_open_sizes` | I-MEM-OPEN | R-1 | A-3b, A-7b, A-7d | full, its fall-back form included | OPEN-NO-FILE-TERM, POOL-SIZED-BEFORE-TABLE, FALLBACK-SET-KEPT |
| `t_mem_growth_allowance` | I-MEM-GROWTH | R-1 | A-3c, B-1c | full | ALLOWANCE-UNRESERVED |
| `t_mem_admission` | I-MEM-ADMIT | C-7 | R-1 | embedded | ADMIT-OVER-COUNT |
| `t_mem_admission` | I-MEM-ADMIT | D-5a/b | R-1 | browser | ADMIT-OVER-COUNT |
| `t_pool_minimum` | S-STORE I-POOL-BOUND | A-3b | — | S-STORE §15.1 | POOL-MIN-PATH-ONLY, MIN-RMAP-DENSE, MIN-RESERVE-NO-STEP2 |
| `t_pool_floor_scattered` | S-STORE I-POOL-BOUND | A-3c | A-3b | S-STORE §15.1 | MAPPIN-KEPT |
| `t_pool_floor_scattered` | S-STORE I-POOL-BOUND | A-4a | A-3c | S-STORE §15.1, in full | MAPPIN-KEPT |
| `t_height_bound` | S-STORE I-POOL-BOUND | A-4a | A-3c | S-STORE §15.1 | HEIGHT-SPLIT-UNCHECKED, HEIGHT-DESCENT-UNCHECKED |
| `t_height_bound` | S-STORE I-POOL-BOUND | A-7f | A-7d, A-7c, A-7b, A-4a | S-STORE §15.1, (b) in full | HEIGHT-DESCENT-UNCHECKED |
| `t_epoch_reserve_two_exhausters` | S-STORE I-CKPT-AT-T | A-3c | A-4c0 | S-STORE §15.1, the floor variant | RESERVE-PER-THREAD-ONLY |
| `t_epoch_reserve_two_exhausters` | S-STORE I-CKPT-AT-T | A-7a | A-3c, A-4c0 | S-STORE §15.1, the floor variant through `write_guard` | RESERVE-PER-THREAD-ONLY, CLAIM-DEPTH-TWO |

R-1's rows are Phase B (PLAN-v3 §8: "N16 budget enforced"); the C and D rows are their cards', in Phase C and D.

## 11. Phase A cards and stand-ins
R-1's own code (the ledger, the division of `B`, the accounts) is Phase B (PLAN-v3 §9 row R-1). Phase A needs only numbers, which are arithmetic, and S-STORE's rules they enter.

| Card | Takes from this file | Pins (S-STORE §15.1) | Until R-1's code |
|---|---|---|---|
| A-3b | `F_min` (§4.2), with `n = n_c + n_s`, `s` and `H_min`; the open's 53200 below it, writing nothing; `H` at open (§4.5); the build reports `n_s`, `s` and `o_frame` | `t_pool_minimum` (POOL-MIN-PATH-ONLY, MIN-RMAP-DENSE, MIN-RESERVE-NO-STEP2) | the open takes `F` and `n_c`, not `B` |
| A-3c | the reserve's size (§4.3); §4.1's pins end when a MapLeaf's last dirty page leaves the pool (§4.4); the image-record list and buffers reserved at open (§5.1) | the floor variant of `t_epoch_reserve_two_exhausters` (RESERVE-PER-THREAD-ONLY); `t_pool_floor_scattered` (MAPPIN-KEPT); IMAGE-LIST-GROWS (already there) | S3's test-only callers; S3's growth call for `t_pool_floor_scattered`'s MapLeafs |
| A-4a | `H` (§4.5): the descent's depth check and the root split's check | `t_height_bound` (HEIGHT-SPLIT-UNCHECKED, HEIGHT-DESCENT-UNCHECKED); `t_pool_floor_scattered` in full (MAPPIN-KEPT) | S3's lowered bound and S5's second tree for `t_height_bound` (b), whose full form is A-7f's; a test build's `H_min` |
| A-7a | the claim depth (§4.3), through `write_guard` | the floor variant through `write_guard` (RESERVE-PER-THREAD-ONLY, CLAIM-DEPTH-TWO) | — |
| A-4e | A-4e's per-call bound and the close hook; the cap and the shrink are R-1's (§6.3) | `t_cursor_park_equiv`'s close form (BATCH-KEPT-AFTER-CLOSE, already there) | the store's close call |
| A-6c | `W_buf` as an open option; appenders wait on `written_end` | nothing new | — |
| A-7c | the pending-free sets in at most one bit per slot each (§5.2) | nothing new | — |
| A-7d | pass 1's charge and the read-only replay's 53200 (§5.3) | `t_ro_open`, `t_ro_open_disk_full` (already there) | the open takes `F` |
| A-7f | nothing new: it adds no code | `t_height_bound` (b) in full, its bound set by a reopen with the smaller pool (HEIGHT-DESCENT-UNCHECKED) | — |

## 12. Measurements owed

| Value | Provisional | Basis | Measured by |
|---|---|---|---|
| `o_frame` | 128 bytes | a 64-byte frame header, a 40-byte page-table entry, a 24-byte list record | A-3b (its build reports it) |
| `s` | 1 | a data phase that writes one page at a time | A-3c (its build reports it) |
| `R_EPOCH`'s slope | 1/32 | a start | A-7e: claims per first touch inside checkpoint windows, beside its commit p99 there |
| `e_lock` | 128 bytes | an entry with a key of about 40 bytes | B-1b |
| a root-table entry | 64 bytes | tree id, root LPID, kind and state, a map node's share, and C-T0 §9.4's per-tree garbage estimate and entry count, which live with it | A-7b |
| write-set bytes per key | 117 bytes | W6 (`spikes/e1-bench/README.md` §W6) | B-1a |
| `W_buf` | §7.1 | PostgreSQL's `wal_buffers` cap | A-6c, A-7e |
| `op_mem`, `cursor_mem`, `plan_mem`, `conn_mem` | §7.1 | PostgreSQL's `work_mem` order | C-4a–g, C-9, E9 |
| the base and the fragmentation allowance | — | — | D-13 (G9) |
| the per-mode budgets | §7.1 | §7.1's reasons | C-9 (NBench embedded and OLTP rows: RSS ≤ PG17), D-5b (NBench browser row) |

## 13. Boundaries
- **S-STORE owns** the pool, its eviction and latches, the epoch gate, `write_guard` and its reserve, the minimum's formula and why it is the minimum (§5.4), the fairness bound (§5.3), the height rule (§5.4), the checkpoint and recovery, and fail-stop (§9.2). This file gives them numbers.
- **C-T0 v8 owns** the write set and its spill, the lock table and escalation, SSI and its escalation, savepoints, and GC's scheduling (§9.4). This file sets their thresholds per mode.
- **C-T6-rehost owns** rows and TOAST, including the least WAL segment (8 MiB), which bounds `preapply_bytes` (§6.1).
- **The executor, session and wire cards** (C-1a, C-1b, C-4a–g, C-5, C-6, C-7) build their consumers within §6.4's accounts; **D-6c** exports §8's report; **D-13** runs G9.
- **The owner** decides the per-mode budgets and thresholds (§7.1).
- **Not R-1's** (PLAN-v3 §4, X-4 ruling R2): on-disk growth under long readers.

## 14. Known costs, open questions, revisit triggers
- **RM-T1: the height bound's fail-stop** (S-STORE §19 T8). A pool serves trees of up to `H` levels; past it a write fails stop and a read gets 53200. At the default budgets `H` is in the hundreds or more (§4.6); near the floor it is `H_min`. **Trigger:** S-STORE §19 T8's.
- **RM-T2: the growth residual** (§5.2). Draws on the growth allowance past two growth steps' bitmaps and one MapInner path, while sessions hold every free byte of `S`, poison the store with 53200: at the latest at the third growth, sooner where LPID allocation outruns the file or one step needs more MapInner pages than one path (§5.2). **Trigger:** a report of it at a budget at least twice `B_min`. Then a card moves the free-set and pending-free bitmaps into the pool, as MapLeafs are, so that they cost frames, not growth.
- **RM-T3: caps are not reservations.** Many transactions at their caps can fill `S`, and the last to charge gets 53200, as PostgreSQL's `work_mem` allows. **Trigger:** a 53200 at the default budgets in NBench or G9. Then a card adds admission per statement.
- **Escalation by bytes.** C-T0 §6.2 escalates by a count per tree; a transaction below the count in many trees meets `lock_mem` first (§6.2). **Trigger:** a 53200 from `lock_mem` in NBench or G9 below `lock_escalate_rows`. Then a C-T0 card considers a byte trigger.
- **Q-COPY-PIN** (an S-STORE question): does an epoch copy hold a §4.1 pin on its page's MapLeaf? `R_MAP` counts one either way (§4.4); a "no" from S-STORE makes `R_EPOCH` of `R_MAP`'s frames slack.
- **Q-MAPLEAF-DIRTY** (an S-STORE question): who makes a clean page's MapLeaf resident when the page is first dirtied, and when, given no I/O under an inner page's exclusive latch (S-STORE §5.3) and no restart after an operation's first mutation (S-STORE §5.6)? `R_MAP` counts the frame either way (§4.4: class (Mp), whichever pin holds it).

## Changelog
- **Draft 1** (2026-11-17, card R-1-SPEC): the file. PLAN-v3 §9 row R-1; verdict S-1-F7's N6; verdict S-1-F7-F2-F2-F4b's follow-up F2 (a); card S-DELTA's owner point P-1; X-4 ruling R2. With S-STORE draft 7.16 and C-T0 draft 8.8.
