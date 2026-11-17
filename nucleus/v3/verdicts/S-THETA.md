Card: S-THETA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 9e03f3bb-fc0b-44d3-9f64-30a72223b690 (an independent P gate: this agent wrote neither the card, its implementation 718333fb, nor any earlier verdict, and is not the orchestrator)
Date: 2026-11-17
Gate-exit: GATE EXIT=0
Commit: 718333fb6e2f3f8afce2044244c84eea777f126b
Mutants: spec text, none in code. The card's 13, walked as text on their tests at 718333fb, each caught deterministically by the case the card names: LOAD-REF-BEFORE-PUBLISH -> t_pin_load_race (a) (A-3a, A-3c); LOADING-SERVED -> (b); FAILED-LOAD-KEPT -> (c); MISS-TWO-FRAMES -> (e); CREATE-OVER-LOADING -> (f); CREATE-BESIDE-FREED -> (g); PAGE-KEY-MERGED -> (i) (all A-3a); EVICT-UNLINK-BEFORE-ENTRY -> (h) (A-3c); FREED-FRAME-WRITTEN -> I-LPID-EXACT's row test after an eviction of every unpinned frame (A-4b); MAPLEAF-EXISTS-BY-REF -> t_alloc_restart_progress race run (1) (A-4a, A-7f); MAPLEAF-WAIT-UNDER-INNER -> run (3), a deadlock the harness reports (A-4a); INVARIANT-NOT-POISON -> t_poison_before_unlatch invariant and read variants, PANIC-UNPOISONED -> its panic variant (A-3c); ROOT-PIN-MAP-FIRST -> (j) (A-3a). Gate's own: G1 LOAD-KEPT-ACROSS-RESTART (a loader that holds no gate keeps its frame in the page table while it restarts for a pending flip) -> UNCAUGHT at 718333fb, no case has a flip (finding F1), caught after the rework by t_pin_load_race (k); G2 a reader that waits for a load under its parent's latch instead of restarting -> UNCAUGHT (READER-IO-UNDER-LATCH's hook watches reads, not waits; S-THETA-F2); G3 an LPID creation that waits for a load while holding that LPID's MapLeaf latch -> UNCAUGHT, on no row (S-THETA-F2); G4 step 2 writes a freed frame that eviction drops -> not caught at A-4b, which runs no checkpoint (S-THETA-F2). Text mutants on the tip, judged by the card's 34 non-gate Acceptance lines: each of the 30 blocks left out -> its own line(s) plus 33 (all 30 in the default locale, 8 also under LC_ALL=C); 9 one-word plants (in P3, P6, P7, P18, L1, L2, L4, P23, and one outside the blocks) -> each its own line plus 33 (32 and 33 for the outside one). The correction list below was tried the same way: each of C1-C14 left out fails its own line plus 33. All in scratch clones; the worktree was never written
Followups: S-THETA-F1, S-THETA-F2, S-THETA-F3, S-THETA-F4

## Verdict in brief

**REWORK**, on one finding (F1). The rest of the card is right.

**The bytes are right.** My own fence-aware parser applied the card's 30 blocks to the base's draft 7.15 (sha256 `a1376c94…700f`). With `<date>` = 2026-11-16 the result equals the committed file byte for byte (sha256 `1d99e0af…4434`). With 2026-11-17 it is the card's recorded `38428ea9…7f90`. Only `nucleus/v3/docs/S-STORE.md` changed (+36/−18). All 34 non-gate Acceptance lines pass in both locales, the checker prints `OK`, and the fresh gate exits `GATE EXIT=0`.

**The P0 facts, in short** (point 2, in full below):
- **(a) is a defect.** Every step of the card's six-step interleaving is allowed by a 7.15 sentence, and no sentence forbids any of them. The stale bytes pass §3.3, so an acked commit is lost silently.
- **(a′) is an under-specification.** "Page id" is undefined. §3.1 and §13 step 2 already tell a careful reader that position ids and LPIDs collide.
- **(c) is a defect only in the second sense: the text contradicts itself.** Read literally, the mechanism writes a freed frame into an image that I-LPID-EXACT and I-FREE-EXACT forbid. No committed data is lost; amcheck sees it.
- **(d) is a defect.** §5.6's decision by the ref alone, and its own test's "finds the ref set", contradict §4.1, where a made-but-unwritten MapLeaf's ref is null. A literal build loses an LPID's mapping: XX001 on a healthy store, and acked data missing from later images.

**F1 is a new deadlock that the new waits allow.** P6 lets a gated operation wait for another thread's load: a pin by an apply that holds the gate, or a creation that also holds its tree's latches. The loader may hold no gate. When it needs a second frame, for the page's MapLeaf, while a flip is pending and every candidate frame is dirty, §5.2 makes it restart. Nothing in 7.16 ends its load first. So the flip waits for the gated operation, that operation waits for the load, and the loader waits for the flip. Under 7.15 none of these waits existed. The card's own derivation checked the MapLeaf latch the load needs, but not the frame its MapLeaf pin needs, which a thread holding no gate can take only when no flip is pending.

**The fix is local.** One rule sentence in P6: a load ends, frame out of the page table and nothing served, before its thread restarts or waits for the flip, and that thread loads the page again only once the flip has run. Then one mutant, LOAD-KEPT-ACROSS-RESTART, and one case, `t_pin_load_race` (k) at A-3c under the interleaving harness. Below is the one list: C1 to C15 against the committed file, and 24 exact Acceptance-line edits. I tried them all in scratch:
- the corrected file passes the corrected 34 lines in both locales;
- the checker prints `160 rows; 240 mutants; …` and `71 new since 7.10 (…, 14 here)` with `OK`;
- each correction left out fails its own line;
- the corrections apply once each on top of every composition with R-1-SPEC, S-ZETA and S-ETA.

**Merge timing, for the record.** By P0 re-check 1, this card may be built and gated on a branch, but may not merge before P0 PASS unless the owner says so in their own words.

## Gate run

- **Extract.** A shallow `--no-local` clone of `nucleus/v3-scaffold` gave scaffold id `71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`. `fresh-gate.sh` and `git-env.sh` were extracted, with no `EXTRACT FAILED`.
- **Run.** From `~/Workspace/neutron-v3`, `bash "$fg/fresh-gate.sh" 718333fb6e2f3f8afce2044244c84eea777f126b`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-THETA` and stdout and stderr captured separately:
  - `fresh-gate: commit 718333fb6e2f3f8afce2044244c84eea777f126b`
  - `fresh-gate: base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `fresh-gate: checks passed (4874 files hashed in 4.93 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - every step `EXIT=0`: test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests and api-surface;
  - stderr empty, exit 0, and the literal last line `GATE EXIT=0`.
- `$fg` was removed afterwards.
- **The worktree.** Before this verdict's commit, `~/.cache/nv3-cards/S-THETA` was at `718333fb` on `card/v3-S-THETA` and clean. That commit is one commit on `71ecf3e6`.

## Mechanics (point 1)

- **Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` lists only `nucleus/v3/docs/S-STORE.md` (+36/−18), the card's Touch only. No protected path is touched.
- **The blocks.** My parser (fence-aware, since E2's Old holds a `## Changelog` line) took E1, P1 to P24, L1 to L4 and E2 from the card in its listed order. Each Old occurs exactly once at its turn. The result equals the committed file byte for byte, with `<date>` = 2026-11-16.
- **Nothing else changed.** The committed file and my application are identical, so no byte outside the blocks moved.
- **Acceptance.** I ran the card's 35 lines as `tools/run_card.py` parses them: `/bin/sh -c` from the root of a clean `--no-local` clone at 718333fb, with GNU grep 3.11. All 34 lines before `gate.sh` pass under the default locale (`en_CA.UTF-8`) and under `LC_ALL=C`. On the base, all 34 fail in both locales.
- **The checker** (line 31) prints:
  - `160 rows; 239 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`
  - `70 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 13 here), 70 of them judged on a Phase A row; 48 invariants, 49 tests`
  - `OK`
- **Omit-one.** All 30 blocks were left out one at a time and committed in scratch. Each fails its own content line or lines, plus 33, and usually 29, 30, 31 or 32 too. E1 fails 1, 2, 32 and 33; E2 fails 3 to 6, 32 and 33. A sample of 8 (P3, P6, P18, P23, L1, L2, L4, E2) under `LC_ALL=C` gives the same lines.
- **Planted errors.** Nine plants each fail their own line plus 33:
  - P3 "freed and not dirty" made "freed and dirty": lines 9 and 33.
  - P6 "throughout" made "briefly": 14 and 33.
  - P7 with the page-table condition dropped: 16 and 33.
  - P18 case (i) on LPID 6: 23 and 33.
  - L1 "never a time" made "or a time": 11 and 33.
  - L2 the same: 18 and 33.
  - L4 "time" made "clock": 19 and 33.
  - P23 "(a) to (g), (i) and (j)" made "(a) to (j)": 27 and 33.
  - A stray edit to §5.2's "Clean victims are dropped": 32 and 33.
- **The split** (Notes). The P blocks alone and the L blocks alone each apply to the base. P then L equals L then P, and equals all blocks without E1 and E2. Checked by script.
- **The date (the known deviation).** The commit is dated `2026-11-17T00:53:33-07:00`, but both `<date>` positions read 2026-11-16 (line 3, and the Changelog's 7.16 head). Work 2 says "Replace `<date>` (E1, E2) with the commit date". The two strings are the only difference from the card's recorded `38428ea9…7f90`, which I reproduced with 2026-11-17. No Acceptance line sees dates (lines 1 and 3 match any date, and line 33 normalises them), and no rule depends on them.
  - It still needs correcting: it is the spec's own record, and later status lines chain it.
  - The exact correction is C15 below. Both positions carry the rework commit's own date: `(card S-THETA, 2026-11-17: ` and `- **Draft 7.16** (2026-11-17, card S-THETA` if it is committed on 2026-11-17.
  - Had the verdict been MERGE, the same two-string fix would be the whole correction.
- **Stale restatements.** I grepped the whole committed file, and C-T0 draft 8.7, with GNU grep, for every rule the card changes:
  - "page id"; "bounded time"; `parking_lot`; "bounded bypass";
  - "never poison"; "ref is still null"; "finds the ref set"; "only when no frame holds";
  - "drops the frame"; "every schedule"; "step budget"; "within a bound"; "a damaged store"; XX000.

  No stale normative restatement remains. The hits are:
  - the edited texts themselves;
  - §4.1 "Growth"'s MapInner race, where the ref is set from making, as P2 now says;
  - §5.3's "plus the bounded bypass", which is generic and consistent with `K`;
  - §9.2's "another check that only a damaged store can fail", which P8's XX001 class reads as memory damage, consistently;
  - `t_append_section_leaf`'s A-6c "within a bound", which the card leaves for A-6c's card (S-THETA-F3);
  - and status-line and Changelog history.

  C-T0 has no "never poison". Its §2.4 owner-lookup and settle-table failures, and its reader-path hit (line 404), already say "the engine poisons the store", which P8 now classes.

## Point 2: the P0 question, my own interleavings against draft 7.15

This section states the facts the next P0 review needs. It does not rule on what that review should conclude. Quotations are 7.15's text at `71ecf3e6`.

### (a) A stale load installs superseded bytes: **DEFECT** (an acked commit lost silently)

Each step, and the sentence that permits it:
1. Page `L` lives at slot `s1`, the durable image's home. No frame holds it.
2. T1 pins `L`, finds no frame and reads `r1 = (s1, e, c1)`. §5.4: "A pin looks for the page in the page table first, and reads the LPID's current map entry only when no frame holds the page." No sentence says when T1's frame enters the page table relative to that read.
3. T2 does the same, reads `s1` into its own frame `f2`, which passes §3.3, and puts `f2` in the table.
4. T2's apply inserts a committed entry into `L`, so `f2` is dirty.
5. An eviction writes `f2` to `s2`, sets `L`'s entry to `r2`, then takes `f2` out of the table. This is §5.2's order exactly: "its map entry is updated in memory … before its frame leaves the page table, so that a pin finds the page in its frame or through that entry". That sentence protects pins that look after the eviction, not a load that straddles it.
6. T1 reads `s1`. §4.4 item 1 keeps it intact, because the durable image references it. The bytes pass all four of §3.3's checks against `r1`, and T1 installs them, since `L` is absent.

What stops a careful implementer? The §5.1 reference, E0's `locate`, holds the table mutex across victim choice, write-back and read, "all under the page-table mutex so two loaders cannot install the same page twice (or lose an eviction)". But in v3 the reference read latches a MapLeaf, and may load it. 7.15 ranks the page table as a leaf lock: §4.2 says "a thread holding the page table never takes the free-set lock", and §6.8 says the root table's lock "ranks like the page table and the free-set lock". So a v3 build cannot copy E0's serialisation, and the text names no replacement.

Only the end-to-end requirements forbid the outcome (I-SEEK, and acked commits surviving recovery), and those forbid every bug. No sentence forbids a step, and I-INTEGRITY cannot see stale-but-consistent bytes.

**Consequence.** T2's entry is gone from the pool. The next write of `L` starts from the old bytes, and its eviction replaces `r2`. Once a checkpoint's `T` passes T2's commit, recovery never replays it: an acked commit is lost, and amcheck sees a well-formed tree. If `s1` had been a current-map-only home, which §4.4 lets the next write reuse, step 6 could read reused bytes instead and fail §3.3. Under an apply, that poisons a healthy store.

### (a′) A MapLeaf position colliding with an LPID: **UNDER-SPECIFICATION**

§5.1 keys the table by "page id", which no section defines. §1 defines "LPID | Logical page id". §3.1 gives kinds 6–9 a position id "in an encoding A-2 pins that gives no two positions one id", which is unique among positions only. A-2's card (not merged; card text, not built code) pins `position_id(Map, 0, i) = i`. I checked its Work 1 and its test `position_ids`.

So a build that keys the table by the header `id` puts MapLeaf `i` and LPID `i` under one key.

A careful reading already forbids that:
- §13 step 2 keys image records "keyed apart from LPIDs, so that no record for a map page can collide with one for an LPID";
- §3.1 promises uniqueness only among positions.

**Consequence of the literal reading.** A pin of LPID `i` is served MapLeaf `i`'s frame, or the reverse. That is a false XX001, which poisons inside a gated operation, if the decoders check the kind byte. If one does not, it mutates the wrong kind of page. A-2's decoders are card text, so this is unverified here.

### (c) A freed page's dirty frame written into the next image: **DEFECT, by self-contradiction only** (no committed data lost)

**The sentences that lead to it.**
- §4.2: "Freeing an LPID is a mutation of the page and of its map entry, both through `write_guard`".
- §5.6: "`write_guard(frame)` is the only way to mutate a page".
- §5.2: "Clean victims are dropped. Dirty victims. A dirty victim is written to a fresh slot (§4.4), and its map entry is updated in memory to `(new slot, content epoch, crc)`".
- §13 step 2: it writes "the dirty frames with content epoch `≤ k`"; "its new slot also becomes its current home"; and the map phase builds each map page "with the list's records for its entries applied".
- 7.15 never says that a mutation sets the dirty flag. But nothing else keeps a mutation from being dropped as clean, so the literal reading is a dirty freed frame.

**The sentences that forbid the result.**
- I-LPID-EXACT (§6.8): it "holds of every image, crash images included", and "The mapped LPIDs of an image are exactly … the pages reachable from the directory roots".
- I-FREE-EXACT (§4.3).
- I-LINK-ATOMIC (§5.6): an LPID becomes "unmapped in the same gated operation that removes its last" reference.

§15's case C ("if the free leaves `r`'s frame in the page table, a test seam drops the frame without writing it") shows the gap was seen and left to a seam.

**My interleaving.** It is the card's: a merge frees `R` in epoch `k`; flip `k`; step 2 writes the dirty freed frame and records `(R, s, crc)`; image `k` maps `R`, which nothing references. Through an eviction, the same happens in the current map with no checkpoint.

**Consequence.**
- No committed data is lost, because `R` was unlinked first.
- The durable image fails amcheck: I-LPID-EXACT, and I-FREE-EXACT if the image's LPID bitmap follows the in-memory one.
- If the bitmap follows the built map instead, `R` and `s` leak for good after an open from that image.
- 7.15's I-LPID-EXACT row test at A-4b ("after each pack") would not see it, since no eviction intervenes. The A-7f amcheck form would see it only if a freed frame is resident at a checkpoint.

A careful implementer who ranks the invariants above the mechanism avoids it, as A-3a's `mark_freed` does, but must invent the frame's fate.

### (d) A second restart replacing an unwritten MapLeaf: **DEFECT** (the rule and its test contradict §4.1; a literal build loses an LPID's mapping)

**The contradiction.**
- §5.6: "it creates the MapLeaf in that frame only if the ref is still null. Otherwise another restart created it first, and that restart's retry may already have mapped an LPID there, so replacing the ref would lose the mapping". That rule assumes a made MapLeaf has a non-null ref.
- §4.1 sets a map page's parent ref only by its write: "A dirty map page is written to a fresh slot (I-NO-INPLACE), and its parent's ref is then set in memory in two steps". The ref's shape is "`slot u64, epoch u32, crc u32` … `slot = 0` means unmapped". A MapLeaf with no home has no slot to name, and no sentence sets its ref when it is made.
- 7.15's own test (I-LINK-ATOMIC, `t_alloc_restart_progress` race variant): "the first creates the MapLeaf, and its retry maps an LPID there before the second latches the MapInner; the second finds the ref set". That state is reachable only if the MapLeaf was written in between. The fixture does not arrange it, and a MapLeaf that holds a pin for its new dirty page cannot be evicted until that page is written.

**My interleaving.** It is the card's six steps: A makes MapLeaf `i`; A's retry maps `x`; `x`'s page is evicted, setting `x`'s entry in A's unwritten MapLeaf; B finds the ref null and makes MapLeaf `i` again. Two outcomes:
- **B's frame replaces A's in the page table.** Then a pin of `x` reads slot 0: XX001 on a healthy store, which poisons under an apply. Image `k` omits `x`'s mapping, so `x`'s acked content, below `T`, is unreachable afterwards. The loss is detected, not silent.
- **The insertion is refused.** No sentence says what follows, so a build must invent a failure, and that poisons a healthy store.

A careful implementer could lean on §5.1's map holding one frame per page id, and on §5.4's page-table-first rule for pins. But §5.6 decides by the ref, explicitly. A-3a's `create`, which returns `Exists`, is the careful reading.

## Point 3: the new rules as design

### F1 (blocking): a waited-for load whose loader waits for the flip

**The interleaving at 718333fb.**

Setup: tree X's root `r` and its MapLeaf `m` are not resident. The pool is at its floor, and every unpinned frame is dirty.

1. R, a client read of X, holds no gate and no latch. It names `r` through the root table and pins it. It takes a frame: its eviction's try of the gate succeeds, since no flip is pending. It puts the frame in the page table as `r` being loaded (P6). To read `r`'s reference it must pin `m`. The card's own derivation calls that pin "one of the loading thread's own pins".
2. A, an apply insert into X, takes the gate shared and names `r`. §5.6: "An operation acquires the gate before its first latch". A's pin finds `r` being loaded and waits. P6: "A pin that finds the page being loaded waits for that load to end". A holds no latch, and "Holding the gate shared across a page read is permitted; the flip waits for it" (§5.6), so nothing makes A restart instead.
3. The checkpoint's flip requests the gate exclusively. §5.2: "the waiting flip blocks new shared holders".
4. R needs a frame for `m`, and every candidate is dirty. Holding no gate, its eviction "only *tries* to take the gate shared. If the try fails because a flip is pending, the evictor takes a clean victim. If no victim is clean, the thread releases its latches and restarts" (§5.2).
   - Nothing in 7.16 ends R's load here. A load ends only "if the reference has slot 0, or the read or a check fails" (P6). A restart keeps "the page it found cold" pinned (§5.6).
   - So `r` stays in the page table as being loaded, while R waits for the flip or retries.
   - "A thread that holds latches and no gate never blocks on the gate" bars only latch holders from blocking, so R may block.
5. The flip waits for A, A waits for R's load, and R waits for the flip. That is a deadlock, or, if R spins, a livelock until some clean frame appears. Every apply behind the pending flip stalls with it.

At 7.15, A would have loaded `r` itself: its eviction "made inside a gated operation runs under that operation's hold". The cycle exists only because of P6's new wait.

**The creation's wait has the same cycle.** P6 allows a creation to wait under its tree's latches "since that load reads no page". That is true, but the load can still need a frame:
- A stale root name's load of LPID `K` began while `K`'s MapLeaf was cold.
- Its nested pin is stuck on the pending flip, as in step 4.
- A third thread then loads the MapLeaf, an allocation takes `K`, and the creation waits for the stale load, holding the gate and its tree's latches.

**Why the derivation missed it.** "The load's own needs" checked the MapLeaf latch, which is why no waiter may hold a map latch. It did not check the frame the MapLeaf pin needs. For a thread that holds no gate, that frame depends on the flip (§5.2).

**The correction (C1).** A load ends, frame out of the page table and nothing served, before its thread restarts or waits for the flip, and that thread loads the page again only once the flip has run. A then looks again, finds no frame, loads `r` under its own hold and finishes. The flip runs, and R's retry is served `r`. The clause "loads the page again only once the flip has run" stops R from republishing ahead of A forever.

**The rest of the correction** (C2 to C14) adds:
- the mutant LOAD-KEPT-ACROSS-RESTART;
- the hook *restarting*;
- case (k) at A-3c, under the interleaving harness with S1 for the flip, where every schedule finishes on a correct build;
- under the mutant, the check at *restarting* fails, and T2, T1 and the flip deadlock, which the harness reports;
- A-4c0 in the A-3c row's Needs, which is in A-3c's Depends closure (the checker passes).

### Each rule against every path (no other defect found)

- **I-PIN-CURRENT (b).** I enumerated every changer of a current reference, and each holds a frame for the page that is in the page table:
  - a dirty eviction (§5.2) and step 2's data-phase write (§13 step 2), each under the frame's latch;
  - a map page's two-step write, under a pin (§4.1);
  - a free, in `write_guard` (§4.2).

  Neither an allocation, a creation nor a MapLeaf's making changes a reference: the entry stays at slot 0, and a MapLeaf's ref stays null. MapInners and new levels are outside the pool, a claim changes no current reference, and the map phase "never makes a slot a map page's current home". The only exceptions run with no frame in the table: the open's build, and a fall-back's or explicit rollback's rebuild after "Discards everything it built from image `k`" (§14 step 4a item 2).

  So a load's reference stays current, and its slot stays referenced (§4.4 item 3), until the load ends. One frame per page holds through pin, load, create, write-back, eviction, free and re-allocation (reuse in place), checkpoint data and map phases, recovery pass 2, a read-only open's clean evictions, drop-tree, and a crash, which leaves no page table.

  I found no case of two frames for one page, a page written to a freed slot, a leaked or doubly freed slot, or a stale read once the load protocol holds.
- **Waits and wake-ups.**
  - A pin's waiter is woken when the load ends, whether it succeeds, fails or finds slot 0, and then looks again.
  - A poisoned or panicking loader reaches every waiter through §9.2's mark and broadcast: "gate and pin waiters" are in item 3.
  - Load waits form no cycle among themselves. An LPID load waits at most for its MapLeaf's load, and a MapLeaf load needs only the always-resident MapInner's latch and its own slot read.
  - No load waits on a tree latch.
  - After C1, no load waits on the gate. Before it, F1.
- **The creation under tree latches.** Deadlock-free after C1. The load it waits for:
  - reads no slot, because the entry is slot 0 until the creation's page is written;
  - needs no frame, because the creation holds the LPID's MapLeaf pinned;
  - takes only that MapLeaf's latch shared. Its holders never wait on tree latches, frames or loads: map pages rank last (§5.3), and "no frame is obtained by eviction under a map latch" (§4.1).

  R-1-SPEC's pool-floor premise (its E3, R-1-memory §4.4) is about frames a thread can obtain. The creation's wait obtains none, so nothing there is contradicted.
- **The keying rule (a′)** fits A-2's encoding exactly. Every pool page is an LPID or a MapLeaf: MapInners and FreeSet pages are outside the pool.
- **The freed frame (c).** Reuse in place keeps stale pins valid. Per §6.8, a stale name latches, reads the header `id`, which may match, re-reads its entry and retries. The creation latches the freed frame exclusively, and its other latch holders are brief and wait on nothing.
- **(d).** The restart's look in the page table under the MapInner's latch is a leaf-lock read. Its wait releases the MapInner first, and the MapLeaf load it waits on needs only that latch.
- **(e)** is sound against §9.2. Within a gated operation a panic needs the mark, by the same half-done-SMO argument, and wasm32 traps. Two non-blocking gaps go to S-THETA-F4:
  - a wasm trap does not by itself stop a JS caller from re-entering the instance;
  - the panic clause names latches, pins and the gate but not internal mutexes. Item 4 does, and a std `MutexGuard` dropped by unwinding releases before the mark.
- **D-4 (cosmetic).** It calls a live frame for a just-allocated LPID "damage". A device-damaged FreeSet page would fail §3.3 first, so by P8's own definition it is closer to an engine invariant violation. It is raised only inside a gated operation, where both classes poison, so nothing changes.

### (f) The count-bounded latch on the sync layer: sound

- `K` is stated as a per-latch count fixed at construction, which A-3b pins. The spec leaves the number to A-3b, as it leaves `R_MAP` to R-1.
- `t_root_latch_fair` becomes decidable: under the harness its budget counts grants per execution. A-4c0's monitor M4 is the count.
- Excluding time-bounded and self-parking latches is right. The first cannot be judged by a checker with no clock, and the second blocks its single OS thread. `std::sync::RwLock` was already excluded.
- **Cost.** The sync layer's production backend is the platform's primitives, re-exported, so only the latch's own design costs anything: a mutex round trip per grant, unless it has an atomic fast path. This is unmeasured. A-3b measures it against PLAN-v3 §3.12's 20% trigger, whose remedy, optimistic latches, is already planned. That is acceptable.
- **No `parking_lot` assumption is left in the spec.** Its only mentions are L1's exclusion and E2. PLAN's A-3b row still says "a `parking_lot` choice is checked against the pinned version's unlock fairness", which the card's plan item 4 rewrites. A-4c0's Limit 4 quotes §5.3 and changes with L1, as the card notes.

### (g) and (h): sound; every changed test stays decidable, and none weakens

The tests whose meaning changes are exactly those the card lists. My grep of every "schedule", "shuttle", "loom" and "step budget" use found no other:
- `t_dir_root_no_stall`, `t_freeset_lock_order`, `t_latch_scope`, `t_append_section_leaf` (A-7c) and `t_poison_wakeup_race`;
- `t_root_name_reuse`'s shuttle exploration and its churn, case E and walk-poison budgets;
- the I-LATCH row test "(loom/shuttle)";
- `t_poison_before_unlatch`, `t_epoch_reserve_two_exhausters`, `t_root_latch_fair` and its cold variant;
- `t_gated_xx001_poisons`.

Each now has a decidable condition: a stated exploration, and a step budget that is a count. Each mutant must still fail within it.

**What 7.15 claimed.** It claimed "every schedule" of fixtures that no checker the plan has can enumerate. So no kill was ever exhaustive in practice. The six exploration-dependent kills are:
- REVERSE-LATCH-LEFT, CKPT-WAIT-HOLDING-MAP and POOL-FLOOR-BLOCK;
- COUPLING-EARLY-RELEASE and PACK-RIGHT-FIRST;
- POISON-LOST-WAKEUP's broadcast-before-mark form.

These are as certain as a seeded, replayable run on the binary under the gate. That is enough to judge a card.

**What a gate must be shown** for each such kill:
- the stated exploration: scheduler, depth, seed and executions, never below A-4c0's floor;
- the step budget;
- the clean build passing every execution;
- the mutant's failing execution, as seed and index, replayed;
- how many executions of the total fail.

A kill seen in only a sliver of executions is fragile across code changes, and should get a hook-steered form (S-THETA-F3).

**Two loose ends** (S-THETA-F3):
- "A test whose fixture's search ends within the cap" does not say whose cap: the test's stated one, or the harness card's floor.
- `t_append_section_leaf`'s A-6c "within a bound" on MemEnv is still undefined. The card leaves it, by name, to A-6c's card, which is not yet written.

## Point 4: tests and mutants

**`t_pin_load_race`'s ten cases**, walked on a correct conforming build. I used A-3a's protocol as the model: publish before resolve, waiters holding a count, `create` returning `Loading`, `Exists` or `Created`, and `mark_freed`. Each case passes as "For the gate" item 3 says.
- In (a) and (b), the controller records the order of two events, T2's *waiting* against its end or its being served, so nothing waits on time.
- In (c) and (d), each waiter loads afresh after the failed or unmapped load.
- In (e), T1's same-step look finds T2's frame.
- In (f), T3 waits holding only its reservation. T1's load reads slot 0 and removes its frame.
- In (g), the creation remakes the page in the freed frame.
- In (h), T2's pin raises the count without latching. It reads `m2` once the paused evictor, which holds the victim's latch and the gate and no map latch, is released and keeps the now-pinned frame.
- (i) and (j) need only page-table-first and separate keys.

**Each of the 13 mutants** fails its named case deterministically (the `Mutants:` line). Each is judged on the card whose code it hits, as the card's "Which card's code each mutant hits" says:
- the load, the waits, the key and creation look-ups are A-3a's;
- the eviction order and the poison checks are A-3c's;
- the free is A-4b's;
- the restart's MapLeaf rules are A-4a's, where A-3c's growth call sits in the closure;
- MAPLEAF-EXISTS-BY-REF is also judged at A-7f.

**Hooks and holds.** Every hook and every party's latches and pins are named in the I-PIN-CURRENT row and in P10, P14 and P15, and no case uses a timing wait. Run (3) of `t_alloc_restart_progress`, and case (k) after the rework, run under the harness, because their mutants deadlock. That is consistent with L2.

**Gaps that do not block** (S-THETA-F2), my G2 to G4:
- a reader that waits for a load under its latches;
- an LPID creation that waits for a load holding the LPID's MapLeaf latch;
- step 2's half of FREED-FRAME-WRITTEN.

Each half-rule has no named judge. §5.3's "Why latching cannot deadlock" and "The fairness bound" also do not yet enumerate the new waits.

## Point 5: the split, the owner points and the Notes

**The split.** Not needed for judging.
- The halves apply independently and in either order (verified above).
- F1, and every finding that matters, sits in the P half. The L half, (f) to (h), is sound as written and needs no change.
- Both halves fall due before A-3b, and neither may merge before P0 PASS, so one draft number costs the cards waiting on 7.15 one rebase, not two.
- Split along the card's "Splitting" note (S-THETA keeps P, S-IOTA takes L) only if the P0 review rules the P half onto a different path from the L half.

**Owner points**, in plain words, not decided here:
- **OP-1.** When the database finds its own memory state broken, which means a bug or bad memory rather than damaged disk data, should it report PostgreSQL's "internal error" code, XX000, instead of XX001, "data corrupted"? It stops writing and recovers on reopen either way. The card is written on XX001 and recommends XX000.
- **OP-2.** Should each page's lock be the database's own, built so the test harness can check its fairness, instead of the widely used `parking_lot` lock, whose fairness is time-based and cannot be checked that way? It may cost some CPU per page access, which the plan already measures against a 20% trigger. The card is written on yes, and recommends yes.
- **OP-3.** If a read finds the database's internal memory state broken, should the whole database stop until reopened, instead of failing only that read and running on? Running on risks writing the broken state into the next checkpoint. Stopping costs availability. Damage read from disk still fails only that read. The card is written on stopping, and recommends it.

**The Notes, for each merge order.** I parsed each card's S-STORE blocks from its file. That includes S-ETA, which is now a card (`cards/S-ETA.local.md`, blocks E1, W1 to W33 and E2), not only the probe the Notes cite. I applied them to 7.15 in all 24 orders of {S-THETA, R-1-SPEC, S-ZETA, S-ETA}. Every Old of every card occurs once at its turn in every order, and all 24 results are byte-identical outside line 3 and the top Changelog entries above 7.15's. No non-head Old of one card lies inside another card's Old or New.

This card's checker, as written, on the compositions:
- **R-1-SPEC first.** It prints `164 rows; 246 mutants; …; 48 invariants, 51 tests`, and fails only on the mutant set (R-1-SPEC's seven) and `A-7f rows: 11, not 10`, exactly as the Notes say.
- **S-ZETA first.** `160 rows; 247 mutants; …; 48 invariants, 49 tests`, and fails only on the mutant set (S-ZETA's eight), as the Notes say.
- **S-ETA first.** `168 rows; 247 mutants; …; 48 invariants, 53 tests`, and fails only on the mutant set (S-ETA's eight). The Notes give no S-ETA numbers.
- **All four.** `172 rows; 262 mutants; …; 55 tests`.

So the Notes' procedure is right: the draft number moves, the checker gains the merged card's names and placements, and the counts and line-33 hash are recomputed. The orchestrator must add S-ETA's numbers: 8 mutants, 4 tests, 8 rows, and A-7f rows unchanged. After this rework adds one mutant, every mutant total above rises by one.

The corrections below apply once each on top of every composition.

## The rework: one list

Apply C1 to C14 to the committed `docs/S-STORE.md` (at 718333fb). Each Old occurs exactly once there. Then apply C15, the date. In the card, these land in blocks P6 (C1, C2), P18 (C3 to C8), P23 (C9), E1 (C10, C11) and E2 (C12 to C14). They add:
- one rule sentence;
- one mutant, LOAD-KEPT-ACROSS-RESTART, judged on A-3c;
- one case, (k), with one hook, *restarting*.

The counts become: §15 48 rows, **240** mutants, 49 tests; §15.1 160 rows; 24 judged only in Phase B; **71** new since 7.10 (**14** here). `git diff --numstat` stays `36 18`.

**What I ran in scratch.** I committed the result on 71ecf3e6 in a `--no-local` clone, with `<date>` = 2026-11-17. The file is sha256 `2c2351e58855a126a36f1d0eee7f9e2a5b1140eec787b7de2a22dfd7d86f3875`.
- All 34 corrected Acceptance lines pass under the default locale and under `LC_ALL=C`. The checker prints `160 rows; 240 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, then `71 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 14 here), 71 of them judged on a Phase A row; 48 invariants, 49 tests`, then `OK`.
- On 718333fb, exactly lines 1, 6, 14, 15, 23, 27, 30, 31 and 33 fail.
- Leaving out each of C1 to C14 fails its own line plus 33:
  - C1: 14
  - C2: 15 and 30
  - C3 to C6: 23
  - C7: 23 and 30
  - C8: 23, 30 and 31
  - C9: 27, 30 and 31
  - C10 and C11: 1
  - C12 to C14: 6

The scratch files are `~/.cache/nv3-review/gate-S-THETA/corr/` (`corrections.py`, `corrected.md`, and `accept_new.json`, the full corrected Acceptance list).

### Spec corrections (Old occurs once in the committed file)

**C1** (lands in block P6).

Old:
```text
the load takes its frame out of the page table, and nothing is served (§3.3). A pin that finds the page being loaded
```
New:
```text
the load takes its frame out of the page table, and nothing is served (§3.3). A load also ends so, serving nothing, before its thread restarts or waits for the flip, as a thread that holds no gate does when a flip is pending and every frame it could take for the page's MapLeaf is dirty (§5.2), and that thread loads the page again only once the flip has run: a gated operation may be waiting for the load, and the flip waits for that operation. A pin that finds the page being loaded
```

**C2** (lands in block P6).

Old:
```text
**EVICT-UNLINK-BEFORE-ENTRY** (a dirty eviction takes its frame out of the page table before it sets the map entry, so a pin between the two loads the old bytes).
```
New:
```text
**EVICT-UNLINK-BEFORE-ENTRY** (a dirty eviction takes its frame out of the page table before it sets the map entry, so a pin between the two loads the old bytes); **LOAD-KEPT-ACROSS-RESTART** (a load whose thread restarts, or waits for the flip, keeps its frame in the page table as a page being loaded, so a gated operation that waits for the load keeps the flip waiting while the loader waits for the flip).
```

**C3** (lands in block P18).

Old:
```text
reads nothing itself and never takes a frame whose load failed, and a page being loaded is not resident;
```
New:
```text
reads nothing itself and never takes a frame whose load failed, and a page being loaded is not resident; a load ends, its frame out of the page table, before its thread restarts or waits for the flip;
```

**C4** (lands in block P18).

Old:
```text
taking its frame out of the page table (*evicting*).
```
New:
```text
taking its frame out of the page table (*evicting*), and pause a thread where it restarts because it cannot take a frame (*restarting*).
```

**C5** (lands in block P18).

Old:
```text
a paused evictor holds the victim's exclusive latch and the gate shared, and no map latch.
```
New:
```text
a paused evictor holds the victim's exclusive latch and the gate shared, and no map latch; a thread paused at *restarting* holds no latch, no gate and no page being loaded.
```

**C6** (lands in block P18).

Old:
```text
is found by a pin, not as unmapped. LOAD-REF-BEFORE-PUBLISH fails (a)
```
New:
```text
is found by a pin, not as unmapped. (k) At A-3c, under the interleaving harness with these hooks and S1 for the flip: `L`'s MapLeaf is not resident and every frame a thread could take is dirty; T1, holding no gate, pins `L` and is paused at *published*; T2, a gated operation holding the gate shared, pins `L` and is *waiting*; S1 requests the gate exclusively and waits (a hook in the gate reports it); T1, released, cannot take a frame for `L`'s MapLeaf, since its eviction's try of the gate fails, and is paused at *restarting*, where `L` is not in the page table, until T2 has ended and S1 has flipped; T2 loads `L` under its hold, T1's retry is served `L`, and every schedule finishes within the step budget. LOAD-REF-BEFORE-PUBLISH fails (a)
```

**C7** (lands in block P18).

Old:
```text
EVICT-UNLINK-BEFORE-ENTRY fails (h) at A-3c, whose eviction it is: T2 reads `m1` |
```
New:
```text
EVICT-UNLINK-BEFORE-ENTRY fails (h) at A-3c, whose eviction it is: T2 reads `m1`; LOAD-KEPT-ACROSS-RESTART fails (k) at A-3c: at *restarting* `L` is still in the page table as a page being loaded, and T2, T1 and the flip wait for one another |
```

**C8** (lands in block P18).

Old:
```text
PAGE-KEY-MERGED, EVICT-UNLINK-BEFORE-ENTRY, ROOT-PIN-MAP-FIRST |
```
New:
```text
PAGE-KEY-MERGED, EVICT-UNLINK-BEFORE-ENTRY, LOAD-KEPT-ACROSS-RESTART, ROOT-PIN-MAP-FIRST |
```

**C9** (lands in block P23).

Old:
```text
| A-3c | A-3b | every case, with A-3c's map and dirty eviction; S3 for the writes, frees and creations, as `write_guard`'s, A-4b's and A-4a's will make them | LOAD-REF-BEFORE-PUBLISH, EVICT-UNLINK-BEFORE-ENTRY |
```
New:
```text
| A-3c | A-3b, A-4c0 | every case, with A-3c's map and dirty eviction; S3 for the writes, frees and creations, as `write_guard`'s, A-4b's and A-4a's will make them; case (k) under the interleaving harness, with S1 for the flip | LOAD-REF-BEFORE-PUBLISH, EVICT-UNLINK-BEFORE-ENTRY, LOAD-KEPT-ACROSS-RESTART |
```

**C10** (lands in block E1).

Old:
```text
never takes a frame whose load failed; a page's current reference changes only while
```
New:
```text
never takes a frame whose load failed, and a load ends before its thread restarts or waits for the flip; a page's current reference changes only while
```

**C11** (lands in block E1).

Old:
```text
one invariant, one test and thirteen mutants added
```
New:
```text
one invariant, one test and fourteen mutants added
```

**C12** (lands in block E2).

Old:
```text
never takes a frame whose load failed; and a page's current reference changes only while
```
New:
```text
never takes a frame whose load failed, and a load ends before its thread restarts or waits for the flip; and a page's current reference changes only while
```

**C13** (lands in block E2).

Old:
```text
EVICT-UNLINK-BEFORE-ENTRY (A-3c), and ROOT-PIN-MAP-FIRST
```
New:
```text
EVICT-UNLINK-BEFORE-ENTRY and LOAD-KEPT-ACROSS-RESTART (A-3c), and ROOT-PIN-MAP-FIRST
```

**C14** (lands in block E2).

Old:
```text
§15: 48 rows, 239 mutants, 49 tests. §15.1: 160 rows. One invariant, one test and thirteen mutants are added
```
New:
```text
§15: 48 rows, 240 mutants, 49 tests. §15.1: 160 rows. One invariant, one test and fourteen mutants are added
```

**C15** (E1, E2: the date). Both `<date>` positions carry the rework commit's own date, as Work 2 says. Committed on 2026-11-17, that is:

Old:
```text
(card S-THETA, 2026-11-16: 
```
New:
```text
(card S-THETA, 2026-11-17: 
```
Old:
```text
- **Draft 7.16** (2026-11-16, card S-THETA
```
New:
```text
- **Draft 7.16** (2026-11-17, card S-THETA
```

### Acceptance-line corrections

They are written in the card's escaping: a backtick or an apostrophe is `.`, and `.` and `*` are escaped. Each Old occurs once in its line. Line 27 holds the A-3c row twice, and both occurrences are replaced. Line 33 takes the new diff hash.

**A14/C1** (Acceptance line 14; the Old occurs once in that line).

Old:
```text
the load takes its frame out of the page table, and nothing is served (§3\.3)\. A pin that finds the page being loaded
```
New:
```text
the load takes its frame out of the page table, and nothing is served (§3\.3)\. A load also ends so, serving nothing, before its thread restarts or waits for the flip, as a thread that holds no gate does when a flip is pending and every frame it could take for the page.s MapLeaf is dirty (§5\.2), and that thread loads the page again only once the flip has run: a gated operation may be waiting for the load, and the flip waits for that operation\. A pin that finds the page being loaded
```

**A15/C2** (Acceptance line 15; the Old occurs once in that line).

Old:
```text
\*\*EVICT-UNLINK-BEFORE-ENTRY\*\* (a dirty eviction takes its frame out of the page table before it sets the map entry, so a pin between the two loads the old bytes)\.
```
New:
```text
\*\*EVICT-UNLINK-BEFORE-ENTRY\*\* (a dirty eviction takes its frame out of the page table before it sets the map entry, so a pin between the two loads the old bytes); \*\*LOAD-KEPT-ACROSS-RESTART\*\* (a load whose thread restarts, or waits for the flip, keeps its frame in the page table as a page being loaded, so a gated operation that waits for the load keeps the flip waiting while the loader waits for the flip)\.
```

**A23/C3** (Acceptance line 23; the Old occurs once in that line).

Old:
```text
reads nothing itself and never takes a frame whose load failed, and a page being loaded is not resident;
```
New:
```text
reads nothing itself and never takes a frame whose load failed, and a page being loaded is not resident; a load ends, its frame out of the page table, before its thread restarts or waits for the flip;
```

**A23/C4** (Acceptance line 23; the Old occurs once in that line).

Old:
```text
taking its frame out of the page table (\*evicting\*)\.
```
New:
```text
taking its frame out of the page table (\*evicting\*), and pause a thread where it restarts because it cannot take a frame (\*restarting\*)\.
```

**A23/C5** (Acceptance line 23; the Old occurs once in that line).

Old:
```text
a paused evictor holds the victim.s exclusive latch and the gate shared, and no map latch\.
```
New:
```text
a paused evictor holds the victim.s exclusive latch and the gate shared, and no map latch; a thread paused at \*restarting\* holds no latch, no gate and no page being loaded\.
```

**A23/C6** (Acceptance line 23; the Old occurs once in that line).

Old:
```text
is found by a pin, not as unmapped\. LOAD-REF-BEFORE-PUBLISH fails (a)
```
New:
```text
is found by a pin, not as unmapped\. (k) At A-3c, under the interleaving harness with these hooks and S1 for the flip: .L..s MapLeaf is not resident and every frame a thread could take is dirty; T1, holding no gate, pins .L. and is paused at \*published\*; T2, a gated operation holding the gate shared, pins .L. and is \*waiting\*; S1 requests the gate exclusively and waits (a hook in the gate reports it); T1, released, cannot take a frame for .L..s MapLeaf, since its eviction.s try of the gate fails, and is paused at \*restarting\*, where .L. is not in the page table, until T2 has ended and S1 has flipped; T2 loads .L. under its hold, T1.s retry is served .L., and every schedule finishes within the step budget\. LOAD-REF-BEFORE-PUBLISH fails (a)
```

**A23/C7** (Acceptance line 23; the Old occurs once in that line).

Old:
```text
EVICT-UNLINK-BEFORE-ENTRY fails (h) at A-3c, whose eviction it is: T2 reads .m1. |
```
New:
```text
EVICT-UNLINK-BEFORE-ENTRY fails (h) at A-3c, whose eviction it is: T2 reads .m1.; LOAD-KEPT-ACROSS-RESTART fails (k) at A-3c: at \*restarting\* .L. is still in the page table as a page being loaded, and T2, T1 and the flip wait for one another |
```

**A23/C8** (Acceptance line 23; the Old occurs once in that line).

Old:
```text
PAGE-KEY-MERGED, EVICT-UNLINK-BEFORE-ENTRY, ROOT-PIN-MAP-FIRST |
```
New:
```text
PAGE-KEY-MERGED, EVICT-UNLINK-BEFORE-ENTRY, LOAD-KEPT-ACROSS-RESTART, ROOT-PIN-MAP-FIRST |
```

**A27/C9** (Acceptance line 27; the Old occurs both occurrences in that line).

Old:
```text
| A-3c | A-3b | every case, with A-3c.s map and dirty eviction; S3 for the writes, frees and creations, as .write_guard..s, A-4b.s and A-4a.s will make them | LOAD-REF-BEFORE-PUBLISH, EVICT-UNLINK-BEFORE-ENTRY |
```
New:
```text
| A-3c | A-3b, A-4c0 | every case, with A-3c.s map and dirty eviction; S3 for the writes, frees and creations, as .write_guard..s, A-4b.s and A-4a.s will make them; case (k) under the interleaving harness, with S1 for the flip | LOAD-REF-BEFORE-PUBLISH, EVICT-UNLINK-BEFORE-ENTRY, LOAD-KEPT-ACROSS-RESTART |
```

**A1/C10** (Acceptance line 1; the Old occurs once in that line).

Old:
```text
never takes a frame whose load failed; a page.s current reference changes only while
```
New:
```text
never takes a frame whose load failed, and a load ends before its thread restarts or waits for the flip; a page.s current reference changes only while
```

**A1/C11** (Acceptance line 1; the Old occurs once in that line).

Old:
```text
one invariant, one test and thirteen mutants added
```
New:
```text
one invariant, one test and fourteen mutants added
```

**A6/C12** (Acceptance line 6; the Old occurs once in that line).

Old:
```text
never takes a frame whose load failed; and a page.s current reference changes only while
```
New:
```text
never takes a frame whose load failed, and a load ends before its thread restarts or waits for the flip; and a page.s current reference changes only while
```

**A6/C13** (Acceptance line 6; the Old occurs once in that line).

Old:
```text
EVICT-UNLINK-BEFORE-ENTRY (A-3c), and ROOT-PIN-MAP-FIRST
```
New:
```text
EVICT-UNLINK-BEFORE-ENTRY and LOAD-KEPT-ACROSS-RESTART (A-3c), and ROOT-PIN-MAP-FIRST
```

**A6/C14** (Acceptance line 6; the Old occurs once in that line).

Old:
```text
§15: 48 rows, 239 mutants, 49 tests\. §15\.1: 160 rows\. One invariant, one test and thirteen mutants are added
```
New:
```text
§15: 48 rows, 240 mutants, 49 tests\. §15\.1: 160 rows\. One invariant, one test and fourteen mutants are added
```

**A31/1** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
EVICT-UNLINK-BEFORE-ENTRY FREED-FRAME-WRITTEN
```
New:
```text
EVICT-UNLINK-BEFORE-ENTRY LOAD-KEPT-ACROSS-RESTART FREED-FRAME-WRITTEN
```

**A31/2** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
not 31+11+4+8+2+1+13
```
New:
```text
not 31+11+4+8+2+1+14
```

**A31/3** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
(31,11,4,8,2,1,13,70)
```
New:
```text
(31,11,4,8,2,1,14,71)
```

**A31/4** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
S-EPSILON.s 1 and this card.s 13:
```
New:
```text
S-EPSILON.s 1 and this card.s 14:
```

**A31/5** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
('EVICT-UNLINK-BEFORE-ENTRY','A-3c'),
```
New:
```text
('EVICT-UNLINK-BEFORE-ENTRY','A-3c'),('LOAD-KEPT-ACROSS-RESTART','A-3c'),
```

**A31/6** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
'A-3c':'LOAD-REF-BEFORE-PUBLISH EVICT-UNLINK-BEFORE-ENTRY'}
```
New:
```text
'A-3c':'LOAD-REF-BEFORE-PUBLISH EVICT-UNLINK-BEFORE-ENTRY LOAD-KEPT-ACROSS-RESTART'}
```

**A31/7** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
PAGE-KEY-MERGED EVICT-UNLINK-BEFORE-ENTRY'.split());E+=['%s judged outside
```
New:
```text
PAGE-KEY-MERGED EVICT-UNLINK-BEFORE-ENTRY LOAD-KEPT-ACROSS-RESTART'.split());E+=['%s judged outside
```

**A31/8** (Acceptance line 31; the Old occurs once in that line).

Old:
```text
1 at 7.15, 13 here)
```
New:
```text
1 at 7.15, 14 here)
```

**A30** (Acceptance line 30; the Old occurs once in that line).

Old:
```text
'EVICT-UNLINK-BEFORE-ENTRY:4' 
```
New:
```text
'EVICT-UNLINK-BEFORE-ENTRY:4' 'LOAD-KEPT-ACROSS-RESTART:4' 
```

**A33** (Acceptance line 33; the Old occurs once in that line).

Old:
```text
927f81ba9e3201986fd06fcdb96fd62d9c5f4dd652aeecac78a662484b57dc50
```
New:
```text
3a66bae362de968a579f998ee8fb0a425191faf8d67e33d7f2a5f25bcc35a92c
```

### Card text that follows

These are in the card, not the spec file. No Acceptance line reads them:
- **"The draft target, by number":** 240 distinct mutants (226 + 14), and 71 new since 7.10 (…, 14 here).
- **The Mutation evidence table** gains a row: LOAD-KEPT-ACROSS-RESTART | (a) | at *restarting* `L` is still being loaded, and T2, T1 and the flip wait for one another, which the harness reports | `t_pin_load_race` (k) | A-3c.
- **"What the checker does"** names 14 for this card, and LOAD-KEPT-ACROSS-RESTART on A-3c.
- **"For the gate"**, items 3 and 5, walk (k).
- **"For the plan card"**, item 5 (A-3c), adds (k) under the harness and the mutant. It also says that A-3b's and A-3c's frame acquisition ends a load before its thread restarts for a pending flip.
- **"If another card merges first"** adds one mutant to every total. It adds S-ETA's numbers, as point 5 above gives them.
- **The dry run's recorded hashes** become those above: file `2c2351e5…3875` at 2026-11-17, and line 33 `3a66bae3…a92c`.

## Follow-ups (non-blocking; they survive the rework)

- **S-THETA-F1. N10's cooperative mode: a load never yields.** State, in §5.4 "The load" or §5.3 "No latch escapes a call", that no storage call yields while it holds a page being loaded (A-3a's OBL-7).
  - Today no load can yield. Env calls are synchronous (`OpfsEnv` uses sync access handles), no latch is held across a yield, and §5.4's count leaves a frame for every pin.
  - But the card's derivation says a task that meets a yielding load "waits by yielding, holding no latch", which is false for two P6 waiters:
    - a creation, which waits holding its tree's latches;
    - a chain build or free, which waits under its leaf's latch.
  - Should a future Env yield, either waiter could only block the single thread, or yield holding latches against §5.3.
- **S-THETA-F2. Judge the new waits' untested halves, and complete §5.3's argument.**
  - (i) Extend READER-IO-UNDER-LATCH's hook (`t_root_latch_fair` cold variant, A-4a) from "reads a page" to "reads a page or waits for a load" while holding a latch (gate mutant G2).
  - (ii) Add a mutant for an LPID creation that waits for a load holding a map latch (G3), judged where creations run under splits (A-4a), under the harness. Today only the MapLeaf restart's form, MAPLEAF-WAIT-UNDER-INNER, is judged.
  - (iii) Place FREED-FRAME-WRITTEN on I-LPID-EXACT's A-7f row too, with a fixture that checkpoints while a freed frame is resident, so that step 2's half of (c) has a judge (G4).
  - (iv) Name, in §5.3's "Why latching cannot deadlock" and "The fairness bound", the waits P6 adds: a pin's wait for a load; a creation's under its tree's latches; a chain build's or free's under its leaf. Add the argument that a load waits on no tree latch, no gate (after C1) and no other thread's call for a frame.
- **S-THETA-F3. Exploration and budgets.**
  - (i) For each kill that depends on a sampled exploration, its card's gate is shown:
    - the exploration and step budget;
    - the clean build passing every execution;
    - the failing execution's seed and index, replayed;
    - the failing count out of all executions.

    A kill seen in a sliver of executions gets a hook-steered form. The six are REVERSE-LATCH-LEFT, CKPT-WAIT-HOLDING-MAP, POOL-FLOOR-BLOCK, COUPLING-EARLY-RELEASE, PACK-RIGHT-FIRST, and POISON-LOST-WAKEUP's broadcast-before-mark form.
  - (ii) Say whose cap "a test whose fixture's search ends within the cap" means.
  - (iii) Define `t_append_section_leaf`'s A-6c "within a bound" as a count, or as a hook at the wait, before A-6c's card is cut.
  - (iv) Point R-1-memory's `t_mem_caps` (x) at S-STORE §15's step budget, whichever card merges second (the card's own note).
- **S-THETA-F4. The panic clause on its targets.**
  - (i) On wasm32 a panic traps, and the JS side can call the instance again. The browser binding (D-5a) must treat a trap as item 6's stop, and refuse every later call.
  - (ii) The clause names "any latch, pin or the gate", while item 4 also releases internal mutexes after the mark. A std `MutexGuard` dropped by unwinding releases before the mark. So either:
    - the sync layer's mutex guard marks, or poisons, when its thread is panicking; or
    - a `PoisonError` met by another thread is an engine invariant violation (XX001, poisons).
  - The orchestrator's A-3a text ("`Poisoned` … cannot be met in a conforming build, since the panic poisons the store first") is wrong for std guards and should say the second.

## Not verified

- **shuttle.** Its version, its API, whether its depth-first search reduces partial orders, how it treats `yield_now` spins, and how it treats a panic a model thread catches. These are the card's own unverified items; A-4c0's Work 0 checks them.
- **The new case (k) is designed, not run.** I walked it as text against A-3a's protocol and the 7.16 rules. No code for A-3b's frame acquisition, A-3c's gate seams or the harness exists to run it.
- **The code side.**
  - A-2's `position_id` encoding is card text, not built code.
  - Whether A-2's decoders check the kind byte, which decides whether (a′)'s literal build gives a false XX001 or a wrong-kind mutation, is unverified.
  - E0's `locate` was read, and is quoted above.
- **`parking_lot`'s timer fairness and PostgreSQL's behaviour** (PANIC promotion, publish-before-read) were not re-read. The card takes them from memory. No ruling here depends on them.
- **What I did not run.** The card's mutants are spec text: no code mutant was applied, and no cargo test beyond the fresh gate's `gate.sh` was run. The `Mutants:` line is a walk on the text.
