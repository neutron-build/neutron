Card: A-3c
Class: impl
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5 (max)
Gate-session: claude-code session 7d783558-7832-4ea8-a3ce-b62c74ccc37d (T3 Code review sub-agent, P gate of record for A-3c, tier R, effort max; third run of this gate, resumed from the scratch of runs 1 and 2 after both were stopped by the Claude usage limit; requested by T3 thread 45584af2-e651-490e-862c-3402320cffa3)
Date: 2026-11-27
Gate-exit: GATE EXIT=0
Commit: a53cb1dea0e5c00d8f0beb2612485e32a21546fa
Mutants: card rows 1-38 (11 as a/b): 39/39 caught on two runs, every harness kill in 1000/1000 executions on both runs (3, 11a, 11b, 12, 17, 37; 19 also by t_pin_load_race_k); 15 ALLOC-RETRY-LOWEST -> lpid_held_scripts (a) and 34 WB-LPID-EMPTIED-TAKES -> lpid_held_scripts (b), both UNCAUGHT by the named lpid_table | sweep, 139: 89 caught (64 wb_unit, 16 wb_harness at 1000/1000 x2, 6 wb_race, 1 each wb_slots, buf_pin, wb_floor), 2 caught by a repeatable hang (SW-B01, SW-T01), 7 short of the every-execution bar (SW-G07, G14, G16, C03, C04, C05, T02: findings), 41 survivors, each equivalent or a finding | own: OWN-1 GATE-RELEASED-BEFORE-ENTRY UNCAUGHT, OWN-2 IN-LOAD-DROPS-GATE -> t_pin_load_race_k and t_pin_load_race_k_reload 1000/1000 x2, OWN-3 SUPERSEDE-BEFORE-ENTRY UNCAUGHT (findings, A-3c-F6) | the review's two LPID fixes reverted: LPID-TWICE -> lpid_held_scripts (d), NEXT-FROM-LAST -> lpid_held_scripts (f) | probes: Amendment 1's literal t_pin_load_race_k condition fails the correct build in 119/1000; R2's controller fix: correct build 0/1000, with SW-S14 0/1000, with row 37 1000/1000 (x2 each) | A-3b-F1-F1: NOTIFY-ONE 932/380/417 on the merged tree, unchanged
Followups: A-3c-F1, A-3c-F2, A-3c-F3, A-3c-F4, A-3c-F5, A-3c-F6

## Gate run

The scaffold's `fresh-gate.sh` and `git-env.sh`, extracted per GATE-PROMPT step 1 through a shallow `--no-local` clone (printed scaffold id `d155924534028dfc147aaad3d0991575fae4a2c3`, no EXTRACT FAILED), run once on the full commit id by run 1 of this gate (14:32 to about 14:46). Runs 2 and 3 each checked that `git rev-parse nucleus/v3-scaffold` is still `d155924534028dfc147aaad3d0991575fae4a2c3` (run 3 at 22:1x) and reused the saved stdout and stderr after reading them:

```
fresh-gate: commit a53cb1dea0e5c00d8f0beb2612485e32a21546fa
fresh-gate: base d155924534028dfc147aaad3d0991575fae4a2c3
fresh-gate: checks passed (4961 files hashed in 1.53 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests EXIT=0
STEP api-surface EXIT=0
GATE EXIT=0
```

stderr was empty. No `not ok`, `FAILED` or `panicked` line, and no known load or zombie row (FAST-TRACK §3) failed. The gate's scratch, logs and tools are in `~/.cache/nv3-review/gate-A-3c/` (`notes.md` is the running log; `results-*.jsonl`, `logs/`, `tools/`).

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: 25 files, every one under the card's Touch only list: `src/lib.rs`, `src/store.rs`, `src/buf/` (new `bitmap`, `cmap`, `evict`, `gate`, `lpids`, `slots`, `wb`; edits to `acquire`, `frame`, `mod`, `pool`, `table`), `tests/wb_{unit,slots,race,floor,harness}.rs` and `tests/wb_support/`. `tests/buf_support/mod.rs` is not edited. Nothing protected. The branch was cut from `98213932` (A-3b's merge); the scaffold has since gained A-3b-F1, and `git merge-tree` merges the two cleanly (tree `8fa51e1a5edf2c7d4b2dcb5faf61a903c35b8320`).

**Acceptance**, run by this gate from the card's text: lines 6, 7 and 8 exit 0; lines 1 to 5 are inside the fresh gate's test and wasm32 steps.

## The verdict in one paragraph

The write-back code is right against S-STORE 7.23 §4.1–§4.5, §5.2–§5.6 and §13 step 6, and against the card's Work 1–12 as Amendment 1 and this verdict's rulings read it. All 39 card mutants are caught on two runs, every harness kill in all 1,000 executions. The review-and-finish's fixes check out by reading and by mutants. **Two test defects block the merge.** `t_pin_load_race_a` fails a correct build in 7% to 16% of runs under load, because its own text lets T2 evict while T1 still holds its pin (R1). `t_pin_load_race_k_reload`'s controller misreads a correct schedule that a behaviour-neutral change exposes (R2). Neither is an engine defect. For R1, a probe shows the engine answering what S-STORE requires in every failing run; for R2, the controller fix passes the correct build in every execution and keeps row 37's kill. Separately, ESCALATE 3's S1 sentence is right in substance but needs two corrections, and the corrected sentence must land through spec card S-NU before A-3c merges (A-3c-F1). The re-gate after R1 and R2 is narrow (last section).

## REWORK

### R1. `t_pin_load_race_a` fails a correct build (a test defect; the engine is right)

**Measured.** Every failure is the same panic, at `wb_race.rs:182` (`r2.unwrap()`): `Invalid("evicting Lpid(1) gave NotVictim where Obtained was expected")`.
- Run 2, on binaries built from a pristine extract of the tip, with the host at a 1-minute load of 15 to 31: `--exact t_pin_load_race_a` failed 64 of 400 runs; the whole `wb_race` binary failed 45 of 300 runs, and no other test of it failed; on the merged tree `8fa51e1a` (what a promote tests) the test failed 21 of 300. `wb_floor` 0 of 100; `buf_race`, `buf_evict` and `buf_pin` 0 of 100 each.
- Run 3, on the pristine tip binary (its crate checked byte-identical to `a53cb1de` with `diff -r`), load about 22: 32 of 300 runs failed, all at `:182`.
- The sweep's logs show the same panic in the `wb_race` runs of SW-G04, G07, G09, G13, B01, E06, W07, A03 and T02. None of those mutants can reach this test, and no such catch was counted as a kill.

**Cause.** Once the controller releases T1, T1 ends its load and both threads are served T1's frame. T1's `pin_read` (`wb_race.rs:121`) releases its shared latch inside `marker_of` and only then drops its pin. T2's `t2_work` takes the exclusive latch as soon as T1's shared latch goes, touches the page, drops its own pin and calls `evict_out`. When T2 gets there before T1's `drop(p)`, `try_victim` finds T1's pin and rightly answers `NotVictim` (I-B12: a pinned frame is never a victim), and `evict_out` turns that into the error. Nothing orders T1's `drop(p)` before T2's eviction. The card's own text for (a) (Work 14.3) has the same race: "T2: `pin_page(L)`, `s3_touch(m2)`, drop the pin, `evict(L)`".

**Ruling: a test defect, not an engine defect.** A scratch probe (run 3; the tip's test with one change: on `NotVictim`, T2 records `frame_info` of its frame at once and the eviction is retried after both threads have joined) ran 300 times at load about 27: 0 failures. In 69 of the 300 runs T2's eviction met `NotVictim`, and in all 69 the frame was `FrameInfo { state: Resident, key: Some(Lpid(1)), pins: 1, freed: false, referenced: true }`. T2 had already dropped its own pin, so that one count was T1's. In all 69 the same eviction after the joins answered `Obtained`, and the rest of the test passed (the last pin reads `m2`; `s1` holds `m1`'s page). The engine never failed the test's intent.

**Required.**
- T2's `evict(L)` must come after T1's pin is dropped, ordered by an event a thread sends (Limit 5). The card's Do not bars "a timed wait, sleep or retry loop in a test", so a retried eviction is not a fix.
- One form that works: after `drop(p)`, T2 reports (for example on the `mpsc` channel `wb_race.rs` already imports) and waits for a message. The controller takes the first of `Seen(LoadWaiting)` and T2's report as its second event, releases T1, joins T1, then sends T2 its message and joins T2.
- LOAD-REF-BEFORE-PUBLISH must still fail at the second-event assertion (today `wb_race.rs:177`). Under it, T2's report comes before any `LoadWaiting`, so either form keeps the kill.
- The orchestrator amends the card's text for (a) to match. That is card text, not this branch.

### R2. `t_pin_load_race_k_reload`'s controller fails a correct schedule

**The defect.** In `race_k`'s reload branch (`wb_harness.rs:314–336`), the controller loops until T1's `Seen(Restarting)`, skipping T2's `Arrived(WaitWoken)` if it comes first ("T2's Arrived(WaitWoken) may come before or after it"). Then it takes the very next event as T1's (`out.first_after = Some(pz.next_event()?)`). When T2's `WaitWoken` comes after T1's `Restarting`, the test reports "T1's event after its first Restarting was Some(Arrived(WaitWoken, Lpid(1), FrameId(0))), not GateSharedBlocking with frame 1".

**A correct build allows that order.** `end_load` (`pool.rs:1252`) wakes the waiters with `notify_all` under the table mutex and returns. T1 then fires `Restarting` (`restart_or_wait`, `pool.rs:1133`), while T2 must re-take the table mutex, leave its wait and drop the mutex before it fires `WaitWoken` (`pool.rs:1033`). Either thread may fire first.

**Evidence.** SW-S14 removes only `SlotAlloc::alloc`'s `self.fire(slot)`. In `wb_harness` no slot hook is set, so that call is one lock of a facade mutex (`slots.rs:457`) and no change in store behaviour; under Shuttle it is one scheduling point. With it, the test fails on both runs at its fixed seed (`0xA3C0_000C`) with exactly that message. At the tip's own schedule layout all 1,000 executions happen to pass. A-3c-B adds a poison check at every wait predicate in `buf`, and a change like that can turn the test red on correct code.

**The card is right; the test departs from it.** Work 14.4's text has the controller wait until it has seen, after T1's first `Restarting`, "either T1's `GateSharedBlocking` … or a second `Restarting` or `LoadPublished`/`L` from T1". T2's `WaitWoken` is none of those.

**Required.** Skip T2's `Arrived(WaitWoken, L)` after T1's first `Restarting` too, before taking T1's next event. This gate planted that fix in scratch and measured it under the census wrapper (two runs each, PCT seed `0xA3C0_000C`, 1,000 executions):
- the correct build: 0 failing executions of 1,000, both runs;
- the fix with SW-S14: 0 of 1,000, both runs (SW-S14's "catch" was R2's false failure);
- the fix with card row 37, RELOAD-BEFORE-FLIP: 1,000 of 1,000 failing (999 Violation, 1 StepLimit), both runs, as on the unfixed test. The fix keeps the kill.

## Rulings

### (a) ESCALATE 3 and the S1 sentence

**Reading (a) is right.** Read literally, draft 7.23 contradicts itself at row A-3c:
- §5.6 "When it copies" copies a MapLeaf of content epoch `e − 1` at its first mutation in epoch `e`, "whether or not its frame is dirty", unless the map phase has taken that content.
- S1 runs no step 2, and no map phase exists before A-3c-C.
- So after an S1 flip, every eviction of a page whose entry sits in a pre-flip MapLeaf needs a copy that only A-7a builds.
- Yet §15.1 judges I-FREE-SAFE ("full (pending frees)") and `t_two_gen` at A-3c, and each needs exactly such evictions. A MapLeaf reaches content epoch `e` only by a copy or by being made in epoch `e`.

S1's own "Only these stand-ins are allowed" therefore needs the sentence. It is sound for three reasons:
- §5.6's copy guards against an eviction moving a MapLeaf's current home, which §4.4 then frees, before step 2 reads it. At A-3c no image is built or read.
- A-3c's S4 frees only homes whose epoch is `e`, so no pre-flip MapLeaf home is freed before step 6. 7.23's MapLeaf clause in §4.4 needs the image-record list and is not built here.
- Nothing flips in production before A-7c.

The code implements the reading in `touch_check` (`cmap.rs:293–310`): content epoch `e` goes on; above `e` is `Invalid`; below `e` and dirty is `Invalid`; below `e` and clean goes on. A MapInner is never checked: "It never copies a MapInner".

**Two corrections to the proposed wording.**
1. **"every MapLeaf whose frame is clean" leaves out a MapLeaf with no frame at the flip.** A-3c's own tests rest on that case. `free_safe_fixture` (`wb_slots.rs:240–245`; `row_i_free_safe` and `t_two_gen`) evicts MapLeafs 0..2 before `s1_flip`, then pins them again in phase 2 and evicts pages under them. `touch_check_table` (a) evicts MapLeaf 0 before the flip. Such a MapLeaf's content is at its current home exactly as a clean frame's is: a MapLeaf leaves the pool only after a write gives it a home, and a reload is clean. The code treats both alike at the mutation, which is the reading "not dirty in the pool at the flip". (A MapLeaf dirty at the flip stays dirty until A-3c-C, because E2 refuses a previous-epoch victim and no step 2 runs.)
2. **S1 also "opens image `k`'s image-record list", and none exists before A-3c-C.** `s1_flip` (`wb_support/drive.rs:28–35`) rightly opens none. Nothing would append to one either: §13 step 2's appends ("Every write of epoch-`k` content after the flip, outside the map phase") are a previous-epoch victim's write (§5.2), the claim (§5.6) and step 2's data phase, and all three come with A-3c-C. Unless the sentence says so, A-3c's S1 departs from S1 on a second, unstated point.

**Final wording.** S-STORE.md at the scaffold, draft 7.23. It replaces S-NU's N1 New, with the same Old:

Old:
```text
and releases the gate (item 3), without the rest of item 2. A test whose mutants all fail through a hook
```
New:
```text
and releases the gate (item 3), without the rest of item 2. At A-3c only, before step 2 exists (A-3c-C), the stand-in flip also counts as step 2's map phase having taken, for image `k`, the content of every MapLeaf that is not dirty in the pool at the flip, whether its frame is clean or it has no frame, since that content is then at its current home (§5.6 "When it copies"): such a MapLeaf is mutated in epoch `k+1` without an epoch copy, as it would be once the map phase had taken it. A MapLeaf dirty in the pool at the flip still needs the copy at its first mutation in epoch `k+1`, which A-7a builds, and A-3c refuses that mutation. Before A-3c-C the stand-in flip opens no image-record list: none exists, and nothing would append to one, since every write that appends to it (§13 step 2), a previous-epoch victim's (§5.2), a claim's (§5.6) and step 2's data phase, comes with A-3c-C. Nothing this leaves out is read: no image is written or read before A-3c-C, and until then no MapLeaf home whose epoch is below `e` is freed before step 6 (§4.4). A test whose mutants all fail through a hook
```

- **It must land before the merge: follow-up A-3c-F1, card S-NU.** Without it, two §15.1 rows are judged at A-3c on a stand-in the spec does not state. A-3c merges after S-NU, or in the same pass with S-NU first.
- S-NU must re-derive the parts that quote the sentence, as its "The swap" note foresees: its title, its Goal's and "The rule, re-derived" summaries, its status clause, its Changelog entry and Acceptance lines 1, 3, 6, 7, 9 and 12. Line 9 counts "whose frame is clean", which is no longer in the sentence.
- **The scope is right.** The step-2 boundary keeps `t_step2_waits_for_claimed_copy`'s reload run, and so MAPLEAF-CLEAN-NOT-COPIED, outside the clause (S-NU's G-2). It also leaves A-3c's own flip tests resting on a clause that lapses at A-3c-C; A-3c-F2 hands that on.

### (b) The yield gap: acceptable to leave to A-4c (follow-up A-3c-F3)

- **The reachable yields are fixed here.** These are A6 and A6′ meeting a busy frame, now `yield_unless_in_load` (`acquire.rs:97–101`, called at `:253` and `:417`).
- **What remains is A7 inside a load.** That is the nested acquisition's wait for an unpin, which comes only when every frame is held. §5.3 says a load "obtains each frame it needs without waiting for another thread's call to end, as §5.4's count lets every frame acquisition do". So at or above `F_min` a correct build does not reach A7 inside a load; only the tests' pools below the minimum can.
- **It is not a yield on OS threads.** §5.3 forbids yields ("an `await`, or an N10 cooperative step boundary"), and a condition-variable wait on OS threads is neither.
- **No tracker exists before A-4c.** LOAD-ACROSS-YIELD is `t_latch_scope`'s at A-4c (§15.1). The yield revert (the review's r9; this gate's SW-A08) survives every A-3c binary by design, since no test here counts yields.

A-3c-F3 carries both points to A-4c.

### (c) Amendment 1's `t_pin_load_race_k` text: wrong as written; the tip's form is required

- **What fails.** The amendment's condition is "`frame_info(f_T1).pins ≤ 1`, and the frame is not `Loading`". It fails a correct build.
- **Why.** Once T1's load has ended, its frame is the only free one. T2, under its gate hold, can publish its own load of L there before T1 reaches `Restarting`.
- **Measured.** Run 2 planted the literal condition into the tip's test and ran the correct build under the census wrapper (PCT seed `0xA3C0_000B`, two runs). **119 of 1,000** executions failed on each run, the first at execution 17 with `T1's frame FrameId(0) is FrameInfo { state: Loading, key: Some(Lpid(1)), pins: 1, … }`.
- **Required card text: the tip's form** (`wb_harness.rs:392–410`): "…T1's loader count has left its frame: `frame_info(f_T1).pins ≤ 1`, and the frame is not `Loading` under a key other than `L`; either `frame_of(L)` is `None` or it is `f_T1`, T2 having loaded `L` there or begun to."
- **It keeps the kills.** It still kills LOAD-KEPT-ACROSS-RESTART and WB-GATE-BLOCKING in 1,000 of 1,000 executions on both runs. Under the first, `pins` is 2: T1's count and T2's waiter count.

### (d) Rows 15 and 34, and the two survivors

- **Rows 15 (ALLOC-RETRY-LOWEST) and 34 (WB-LPID-EMPTIED-TAKES): renaming accepted.**
  - Both survive their named test `lpid_table` on two runs. Its sequences of length 1 to 3 never allocate with a held MapLeaf: from the fixture, that needs a fourth op (`wb_unit.rs:974`).
  - Both are caught on two runs by `lpid_held_scripts` (a) at `wb_unit.rs:1004` and (b) at `:1014`, whose expectations are hand-derived from §5.6.
  - The card's Must fail for both should name `lpid_held_scripts` (NIT).
- **own2, the unheld event of a `Kept` after a write, is equivalent at A-3c.** This gate's SW-E12 (E5) and SW-E16 (E6) both survive every binary.
  - The bump exists for A6′'s exactness.
  - At A-3c, a dirty frame that an acquisition passes over without writing is either `PrevEpoch`, which no thread can clean before A-3c-C, or `FlipPending`, which ends that acquisition in a restart and never in `PoolStarved`.
  - A frame that a `Kept` leaves was writable. A waiter's own pass that met it unlatched would have taken it as a candidate. Met latched, it was busy, and A6′ looks again rather than waits. Met pinned, its unpin is U1's event.
  - So no wake-up is lost and no `PoolStarved` is wrong. It becomes load-bearing once previous-epoch victims are written (A-3c-F2).
- **r9, the yield revert, survives by design** (SW-A08 survives every binary); see (b).

### (e) Amendment 1's unapplied items and placements (QUEUE row A-3c, 11-27 14:0x)

- **Call 8, line 319 (unapplied).** The card should take the code's alphabet: `Cold(i)` and `Warm(i)` for `i` in `0..=3`, and `Make(3)`. That is 19 ops and 7,239 sequences, asserted at `wb_unit.rs:918`; the card says "about 2,200". MapLeaf 4 is never made, so `i = 4` adds nothing. NIT; no rework.
- **Call 10 (unapplied).** No card text is needed.
  - Work 6.5's "crate-private twins … fire nothing" is met by `Pin`'s `hooks` flag (`counted`, `quiet`).
  - L3's pins fire `Pinned`, as Work 6.5 says, through `pin_resident` (`lpids.rs`).
  - The half-sentence is optional (NIT).
- **Limit 4's own line (unapplied).** Work 3.2 as amended governs ("Limit 4 is void"), and `tests/buf_support/mod.rs` is untouched, so the work conflicts with nothing. Limit 4's line, Work 15's "(Limit 4 allows only the `TestMap` signature)" and the I-B4 cell should say so (NIT).
- **The header and the Acceptance lines (untouched).** Acceptance lines 1 to 8 hold as written. The header's "Spec base: S-STORE draft 7.20 and R-1-memory draft 1" is stale (7.23 and draft 3); the dated note records the facts (NIT).
- **Call 11's sentence placed twice: accepted.** Work 9 lists the test-hooks constructor and Work 13.2 uses it.
- **Call 3 split over 3.1 and 3.8: accepted.** Each sentence belongs where it stands. Work 3.8 now carries two signatures for `latch_leaf_parent`; the owned `LeafParent<P>` governs, and the first should go (NIT).
- **Call 6 keeping "→ release T1 → join." between its two sentences: accepted.** It is T1's release from its `Restarting` pause. The pass condition's text is replaced as (c) says.
- **Work 3.2's whole item replaced: accepted.** A provided `current_ref_with`, with `current_ref` as its default, keeps A-3b's `StaticMap` and `Failing` impls compiling unchanged; both are outside Touch only.
- **Calls 13 and 14 as applied (the review's items 2 and 3).**
  - Call 14's stray full stop before "; otherwise" in Work 8.3 is a NIT.
  - Work 5's amended "`release_lpid` may extend the LPID bitmap (`try_reserve_exact`) under the lock" is stale. Nothing allocates under the free-set lock now (`Bitmap::adopt`). The review's sentence should replace it: "`release_lpid` reserves a longer bitmap with the lock released and adopts it under the lock; nothing allocates under it" (NIT).
- **The review's new tests and hook point** (`lpid_held_scripts`, `release_step6_pending_refused`, `map_mutation_after_flip`, `flip_pending_rechecks_free`, `dirty_pin_end_wakes_waiter`, `map_latch_wait`; `HookPoint::MapLatchWaiting`) belong in the card's lists (NIT).

## Mutation evidence

**Method.**
- **One edit per mutant.** Every mutant was one exact edit set, each anchor checked to occur once, on its own fresh `tar -x -m` extract of the tip (`nucleus/v3` with `nucleus/v2` beside it).
- **Its own target.** Each had its own `CARGO_TARGET_DIR`: a copy of the pristine target with every `nucleus-store` and `nucleus-env` artifact, every test binary and all incremental state deleted, so each build recompiled both crates.
- **The worktree was never edited.**
- **Harness counts.** Wherever a harness share is quoted, `check` was replaced, in the scratch copy only, by a wrapper that counts every execution of the stated exploration through `harness::census` and fails as `check` does.
- **Pristine controls.** All eleven binaries twice passed under that wrapper before the sweep. After it: the same eleven binaries passed twice again under the wrapper (run 3, 22:44–22:57). Run 3 also ran the crate's whole suite on the pristine tip: 26 binaries, 251 passed, 0 failed, the review's count.

**The card's 38 rows (11 as a and b): 39 of 39 caught, each on two runs, none by a hang.**

| Row | Mutant | Caught by (both runs) |
|---|---|---|
| 1 | LOAD-REF-BEFORE-PUBLISH | `t_pin_load_race_a`, `wb_race.rs:177` (second event `Done("T2")`) |
| 2 | EVICT-UNLINK-BEFORE-ENTRY | `t_pin_load_race_h`, `wb_race.rs:493` |
| 3 | LOAD-KEPT-ACROSS-RESTART | `t_pin_load_race_k`: Deadlock in 1000/1000 |
| 4 | POOL-GROW | `row_i_pool_bound_e4` (exhaustive, 1 execution): "the pool's frame count is not F" |
| 5 | EVICT-SKIP-DIRTY | `row_i_pool_bound_e4`: "PoolStarved" |
| 6 | MAPPIN-KEPT | `t_pool_floor_scattered`, `wb_floor.rs:155` |
| 7 | INPLACE-EVICT | `row_i_no_inplace`, `wb_slots.rs:155` |
| 8 | FREE-EARLY | `row_i_free_safe`, `wb_slots.rs:286` |
| 9 | FREE-ONE-GEN | `t_two_gen`, `wb_slots.rs:333` |
| 10 | REPLICA-SLOT-FREE | `t_replica_region`, `wb_slots.rs:380` |
| 11a | FREESET-LOCK-IO (a) | `t_freeset_lock_order`: Violation in 1000/1000 |
| 11b | FREESET-LOCK-IO (b) | `t_freeset_lock_order_boundary`: Violation in 1000/1000 |
| 12 | FREESET-LOCK-INVERT | `t_freeset_lock_order`: 1000/1000 (996 Violation, 4 Deadlock) |
| 13 | MAPINNER-CREATE-UNCONDITIONAL | `growth_race_inner`, `wb_race.rs:910` |
| 14 | LEVEL-GROW-UNCONDITIONAL | `growth_race_level`, `wb_race.rs:847` |
| 15 | ALLOC-RETRY-LOWEST | `lpid_held_scripts` (a), `wb_unit.rs:1004`; `lpid_table` UNCAUGHT |
| 16 | MAP-DROP-BEFORE-REF | `mapleaf_two_step`, `wb_unit.rs:1518` |
| 17 | WB-GATE-BLOCKING | `t_pin_load_race_k`: Deadlock in 1000/1000 |
| 18 | WB-NO-GATE | `evict_holds_gate`, `wb_race.rs:706` |
| 19 | WB-GATE-WRITER-STARVE | `gate_rules`, `wb_race.rs:612`; also `t_pin_load_race_k`, Deadlock in 1000/1000 |
| 20, 21 | WB-EPOCH-NOT-ADVANCED, WB-ROOT-NOT-RECORDED | `s1_records_root`, `wb_unit.rs:1925`, `:1926` |
| 22 | WB-MAPPIN-NOT-TAKEN | `mappin_residency`, `wb_unit.rs:1582` |
| 23 | WB-DIRTY-COLD-IGNORED | `dirty_needs_resident_mapleaf`, `wb_unit.rs:1638` |
| 24 | WB-DIRTYPIN-HOOKED | `pin_hook_kinds`, `wb_unit.rs:1838` |
| 25 | WB-SUPERSEDE-NEVER | `evict_steps`, `wb_unit.rs:1413` |
| 26 | WB-SLOT-NOT-LOWEST | `slot_table`, `wb_unit.rs:379` |
| 27 | WB-GROW-ONE-SLOT | `slot_growth`, `wb_unit.rs:435` |
| 28 | WB-PENDING-NEVER-FREED | `t_two_gen`, `wb_slots.rs:347` |
| 29 | WB-PREV-EPOCH-EVICTED | `evict_prev_epoch`, `wb_unit.rs:1795` |
| 30 | WB-TOUCH-SKIP | `touch_check_table` (b), `wb_unit.rs:1692` |
| 31 | WB-MAPLEAF-UNPINNED-GAP | `mapleaf_two_step`, `wb_unit.rs:1518` |
| 32 | WB-ENTRY-BEFORE-WRITE | `evict_steps` (the error case), `wb_unit.rs:1423` |
| 33 | WB-LPID-SPLIT-HOLD | `lpid_table`, `wb_unit.rs:795` |
| 34 | WB-LPID-EMPTIED-TAKES | `lpid_held_scripts` (b), `wb_unit.rs:1014`; `lpid_table` UNCAUGHT |
| 35 | WB-GROW-INDEX-FROM-ROOT | `cmap_growth`, `wb_unit.rs:1222` |
| 36 | WB-EVICT-FREED-WRITTEN | `freed_frame_not_written`, `wb_unit.rs:2030` |
| 37 | RELOAD-BEFORE-FLIP | `t_pin_load_race_k_reload`: 1000/1000 (999 Violation, 1 StepLimit) |
| 38 | WB-EVICT-PIN-HOOKED | `pin_hook_kinds`, `wb_unit.rs:1867` |

The forms were checked against the card's sentences. The gate chose the form for four rows:
- row 1: A-3a's mutant-3 shape, before P4;
- row 6: the eviction path's `mark_written` with the dirty flag cleared first, so Work 6.3's end is skipped there only;
- row 24: the three `DirtyPinned`/`DirtyUnpinned` sites outside `create`;
- row 35: the card's own formula, `leaf / f^(root_level − L)`.

**The systematic sweep: 139 mutants.**
- **Coverage.** 137 cover every function of `gate.rs`, `slots.rs`, `bitmap.rs`, `lpids.rs`, `cmap.rs`, `evict.rs` and `wb.rs`, and the Work 6, 8 and 10 sites in `pool.rs`, `acquire.rs` and `frame.rs`. By category: (1) 69, (2) 15, (3) 27, (4) 10, (5) 3 (every `notify_all` in the new code), (7) 13. Two more cover (6), try ↔ blocking, beside card row 17: SW-T01 and SW-T02.
- **Run 2's pass.** Each mutant ran on its own extract and target, through the binaries in order until one caught it on both runs: `wb_unit`, `wb_slots`, `buf_pin`, `buf_model`, `buf_evict`, `buf_minimum`, `wb_harness`, `buf_latch`, `wb_race`, `wb_floor`, `buf_race`. The OS-thread hook tests come last, since a mutant that removes the hook such a test waits on hangs it. Survivors ran every binary.
- **Run 3: the card's rule.** "A harness kill counts only if it fails in every execution of the stated exploration, on both runs." Every catch made only by `wb_harness` was re-run under the census on its catching test, twice. Each that fell short then ran its other catching harness test and the binaries the sweep had not reached for it.
- **Other run-3 checks.**
  - A catch made only by R1's flake (`wb_race.rs:182`) is no kill. Where a run's first panic was the flake, the run's log was read for another failure.
  - Two records were 300 s bounds that host load hit (SW-A06's `buf_latch`, SW-T02's `wb_harness`). Both were re-run with longer bounds and completed, so neither was a hang.
  - SW-A06 to SW-A08 and the two category-(6) mutants, which run 2 never reached, ran in run 3.

**Results.**
- **89 caught on both runs.** By binary: `wb_unit` 64, `wb_harness` 16, `wb_race` 6, and one each in `wb_slots`, `buf_pin` and `wb_floor`. Every `wb_harness` kill fails 1,000 of 1,000 executions on both runs, or 300 of 300 in the first exploration of `dirty_pin_end_wakes_waiter`.
- **2 caught by a hang** that repeats (SW-B01, SW-T01).
- **7 short of the card's every-execution bar** (SW-G07, G14, G16, C03, C04, C05, T02). Each fails at a fixed seed on every run, but in fewer than all executions, and no other binary catches it: findings.
- **41 survivors**, each shown equivalent or a finding.

Every mutant that is not a kill:
- **SW-G03** (survived; G2 drops the exclusive term): equivalent: a second exclusive request never exists (§13: one checkpoint at a time), and `grant_exclusive` refuses a second grant itself (`Invalid`, `gate.rs:156`), so nothing is granted twice.
- **SW-G07** (partial; gate_shared's wait does not re-test (if for while)): finding, A-3c-F4. Measured: `t_pin_load_race_k_reload` 3 of 1,000 on two runs; passing on two runs: `buf_latch`, `wb_race without t_pin_load_race_a`, `wb_floor`, `buf_race`.
- **SW-G12** (survived; gate_exclusive's wait does not re-test): finding, A-3c-F4.
- **SW-G14** (partial; GateHold's drop notifies nobody): finding, A-3c-F4. Measured: `t_pin_load_race_k` and `t_pin_load_race_k_reload` 998 of 1,000 each, on two runs. On OS threads, `gate_rules`, `evict_holds_gate` and `flip_pending_rechecks_free` hang on two runs (150 s bound). Passing on two runs: `buf_latch` (logs `logs/SW-G14-p2b/`), the other eleven `wb_race` tests (`t_pin_load_race_a` aside, R1), `wb_floor`, `buf_race`.
- **SW-G16** (partial; FlipGuard's drop notifies nobody): finding, A-3c-F4. Measured: `t_pin_load_race_k_reload` 12 of 1,000 on two runs. On OS threads, `gate_rules` hangs on two runs (150 s bound). Passing on two runs: `buf_latch` (logs `logs/SW-G16-p2/`), the other thirteen `wb_race` tests (`t_pin_load_race_a` aside, R1), `wb_floor`, `buf_race`.
- **SW-S02** (survived; Given accepts a slot at slot_count): equivalent: a `Given` slot at `slot_count` is refused anyway, by the scratch bitmap's own bound.
- **SW-S14** (survived; alloc fires no slot hook): equivalent: no store behaviour changes, and `t_two_gen`'s other judges (the file's write hook, `slot_use`) remain; its recorded catch by `t_pin_load_race_k_reload` is R2's false failure (0 of 1,000 under the fixed controller, both runs).
- **SW-S17** (survived; supersede accepts slot_count): equivalent: a home at `slot_count` is refused anyway, by `pending.set` past the bitmap's length, before anything changes.
- **SW-S18** (survived; supersede accepts a free slot): untested refusal of an impossible input (NIT): a double supersede is engine damage.
- **SW-S22** (survived; release_step6 accepts slot_count): untested refusal of an impossible input (NIT): no caller passes a slot at `slot_count`.
- **SW-B01** (caught-by-hang; first_set_in drops its low mask): caught by hang, deterministic: `first_set_in(x + 1, …)` returns `x` again, so L1's scan loops forever in store code with no scheduling point. `wb_unit` and `wb_harness` hang; the single-threaded `lpid_held_scripts` hangs again on two more runs under a 60 s bound. No test can fail rather than hang there without a clock (Limit 5).
- **SW-B02** (survived; first_set_in drops its high mask): finding, A-3c-F6.
- **SW-B03** (survived; set accepts bit len): equivalent: every caller of `Bitmap::set` bound-checks below `len` first.
- **SW-L02** (survived; fresh_start counts only LPIDs past next): finding, A-3c-F6.
- **SW-L07** (survived; L1 needs more than n candidates): finding, A-3c-F6.
- **SW-L17** (survived; release_lpid accepts next_lpid): untested refusal of an impossible input (NIT): the model skips `Release(x)` at or past `next_lpid`.
- **SW-L18** (survived; release_lpid accepts a free LPID): untested refusal of an impossible input (NIT): the model skips the release of a free LPID.
- **SW-C02** (survived; the map latch's wait does not re-test): finding, A-3c-F4.
- **SW-C03** (partial; the map latch's request is not counted): finding, A-3c-F4. Measured: `map_latch_wait` 325 of 1,000 on two runs; passing on two runs: `buf_latch`, `wb_race without t_pin_load_race_a`, `wb_floor`, `buf_race`.
- **SW-C04** (partial; the map latch's give-back notifies only past one waiter): finding, A-3c-F4. Measured: `map_latch_wait` 325 of 1,000 on two runs; passing on two runs: `buf_latch`, `wb_race without t_pin_load_race_a`, `wb_floor`, `buf_race`.
- **SW-C05** (partial; the map latch's give-back notifies nobody (r8, measured)): finding, A-3c-F4. Measured: `map_latch_wait` 325 of 1,000 on two runs; passing on two runs: `buf_latch`, `wb_race without t_pin_load_race_a`, `wb_floor`, `buf_race`.
- **SW-C06** (survived; leaf_parent takes i == cover as covered): finding, A-3c-F6.
- **SW-C08** (survived; set_mapleaf_ref leaves the MapInner clean): equivalent at A-3c: a MapInner's `dirty` is written and never read (NIT below).
- **SW-C09** (survived; set_mapleaf_ref leaves the MapInner's content epoch): equivalent at A-3c: a MapInner's `content_epoch` is written and never read (NIT below).
- **SW-C12** (survived; growth fires no GrowLocking before a new level): finding, A-3c-F6.
- **SW-C16** (survived; growth leaves a parent clean): equivalent at A-3c, as SW-C08.
- **SW-C17** (survived; growth decides on a child before its hook and lock (TOCTOU)): finding, A-3c-F6.
- **SW-C18** (survived; touch_check accepts a content epoch above e): equivalent: a content epoch above `e` cannot arise (a mutation sets it to `e` under the gate, a load to its ref's epoch, and `e` only grows).
- **SW-E03** (survived; E3 does not check the victim's header): equivalent on every correct path: a dirty frame's kind and id are the engine's own, written at creation, and no path changes them (an engine-invariant check, which A-3c-B makes fail-stop).
- **SW-E06** (survived; E4 fires EvictWrite before it allocates): equivalent: the hook's place relative to `alloc` changes no store behaviour, and no test reads the slot at `EvictWrite` (its one `wb_race` failure is R1's flake).
- **SW-E12** (survived; E5's Kept counts no unheld event (own2, re-run)): equivalent at A-3c (own2 at E5): ruling (d); load-bearing at A-3c-C (A-3c-F2).
- **SW-E13** (survived; E6 releases the latch before it pins): finding, A-3c-F6 (recommended for the rework).
- **SW-E16** (survived; E6's Kept counts no unheld event): equivalent at A-3c (own2 at E6), as SW-E12.
- **SW-W01** (survived; build accepts epoch 0): untested refusal (NIT): `with_writeback` accepting epoch 0.
- **SW-W02** (survived; build accepts a file of the wrong length): untested refusal (NIT): a file whose length is not `slot_count × page_size`.
- **SW-W03** (survived; build accepts next_lpid 0): untested refusal (NIT): `next_lpid` 0.
- **SW-W04** (survived; build accepts a free LPID at next_lpid): untested refusal (NIT): a free LPID at `next_lpid`.
- **SW-W05** (survived; build accepts a repeated free LPID): untested refusal (NIT): a free LPID listed twice.
- **SW-W07** (survived; is_nothing ignores latched): finding, A-3c-F6 (its one `wb_race` failure is R1's flake).
- **SW-W08** (survived; is_nothing ignores the gate): equivalent: a gated hold never meets `FlipPending` (E1 uses the caller's hold; `FlipPending` comes only from a failed try, `evict.rs:160–166`), and `restart_or_wait` is `is_nothing`'s only caller.
- **SW-P15** (survived; try_victim fires no LatchTaking): equivalent: `LatchTaking` at `try_victim` is a seam of the free-set tracker, and a correct build never takes a latch under the free-set lock, so its absence changes no store behaviour and no test result.
- **SW-A01** (survived; the acquisition ignores a pending flip): equivalent, and evidence for A-3c-F5: with the early `FlipPending` return disabled, A5 and A6′ give the same answer after the acquisition's own look, and every binary passes.
- **SW-A02** (survived; a pending flip's re-check ignores the generation): finding, A-3c-F5.
- **SW-A03** (survived; a pending flip's re-check ignores the free list): equivalent: A1 found the free list empty, and every later push onto it is an unheld event (U2–U4) that moves the generation, so the generation term alone decides (its one `wb_race` failure is R1's flake).
- **SW-A04** (survived; a Kept eviction is passed, not busy): equivalent: every `Kept` bumps the generation (`cleaned_victim_left`), so A5 looks again whether the sweep counts it busy or passed.
- **SW-A06** (survived; a PrevEpoch eviction is busy): test gap, transient (NIT below): no acquisition meets only `PrevEpoch` frames; its one `buf_latch` record was the 300 s bound under load, re-run with 1,200 s: 12 passed, both runs (`buf_latch` builds only `Pool::new` pools, where `take_candidate` returns before the edited line).
- **SW-A07** (survived; the own pass answers Starved for a pending flip): finding, A-3c-F5.
- **SW-A08** (survived; yield inside a load put back (r9, re-run)): by design (the review's r9): LOAD-ACROSS-YIELD is `t_latch_scope`'s, A-3c-F3.
- **SW-T01** (caught-by-hang; try_victim takes its victim's latch blocking): caught by hang, deterministic: `try_victim` blocks on a latch where it must refuse. A-3a's and A-3b's suites hang: `buf_pin::remove_rules`, `buf_model::transition_table`, and `buf_evict`'s `acquire_shared_hand`, `acquire_shared_hand_mirrored`, `acquire_vs_acquire` and `clock_skips`. The single-threaded `remove_rules` and `transition_table` hang again on two more runs under a 60 s bound: a thread waiting on a latch it holds can only hang.
- **SW-T02** (partial; the entry set takes its MapLeaf's latch by try (refuses when busy)): finding, A-3c-F6. Measured: `t_freeset_lock_order` 8 of 1,000 on two runs; `t_freeset_lock_order_boundary` 5 of 1,000 on two runs; passing on two runs: `wb_race without t_pin_load_race_a`, `wb_floor`, `buf_race`.

**The gate's own mutants** (GATE-PROMPT step 4). They aim at the write-back path's riskiest logic and at nothing the card's rows or the sweep generate. Each ran on its own extract and target, through every binary in the sweep's order (with `t_pin_load_race_a` split off and run last), twice, harness tests under the census.

| Own | Mutant | Result |
|---|---|---|
| OWN-1 | GATE-RELEASED-BEFORE-ENTRY: E1's gate try hold ends right after E4's write, before E5's entry set and supersede (E6's ref set) | **UNCAUGHT**: every binary passes on both runs. Finding, A-3c-F6 |
| OWN-2 | IN-LOAD-DROPS-GATE: `Hold::in_load()` also drops the caller's gate, so a gated caller's nested MapLeaf acquisition evicts under a try | caught: `t_pin_load_race_k` and `t_pin_load_race_k_reload`, each 1,000 of 1,000 executions on both runs (`PoolStarved`) |
| OWN-3 | SUPERSEDE-BEFORE-ENTRY: E5 supersedes the old home (freed at once in its epoch) before the entry names the new one | **UNCAUGHT**: every binary passes on both runs. Finding, A-3c-F6 |

**The review's two LPID fixes, reverted** (the brief's check).
- REV-1 (L3 always starts at `next_lpid`; F-B reverted) is caught by `lpid_held_scripts` (d) at `wb_unit.rs:1043` on both runs. It shows up first in L2's restart leaf, which `fresh_start` also computes: `Restart { leaf: 3 }` where 4 is right.
- REV-2 (`next_lpid` from the last LPID taken, not the highest) is caught by (f) at `:1079` on both runs.

## A-3b-F1-F1 (the gate obligation on the second merge): done, unchanged

- **The merged tree** (scaffold `d1559245` with the tip, `git merge-tree`, tree `8fa51e1a`). `fixture` still builds its pool through `Pool::new_with_below_minimum_for_tests` → `Pool::build(…, 1, None)`: no write-back. All 13 tests of the merged `buf_latch` pass, among them `harness_unheld_two_waiters_starved` (PCT and RANDOM, 1,000 each). The run had 16 tests: those 13 and the gate's three evidence tests, added in scratch only.
- **A-3b-F3.** `harness_unheld_pin_drop: EXHAUSTIVE, 95737 executions (cap 100000)`, as before this card. `kill_evidence` on the correct build answers "no execution failed" at all three bounds.
- **With NOTIFY-ONE planted at `became_unheld`,** verbatim:

```
pct seed 2764046338 depth 3 executions 1000 max_steps 10000: first failing execution 0 (deadlock) replayed; failing 932 of 1000 (93.2%), V/D/S/P 0/932/0/0; share 5%: met
random seed 2764046337 depth - executions 1000 max_steps 10000: first failing execution 2 (deadlock) replayed; failing 380 of 1000 (38.0%), V/D/S/P 0/380/0/0; share 5%: met
random seed 2764046339 depth - executions 1000 max_steps 10000: first failing execution 4 (deadlock) replayed; failing 417 of 1000 (41.7%), V/D/S/P 0/417/0/0; share 5%: met
```

These are A-3b-F1's 932/380/417 exactly, and the committed test fails under the mutant (deadlock at PCT execution 0). No other form is owed; A-3b-F1-F1 can close.

## A-3b-0-F1 (folded into Work 8.3; "stays open until A-3c's gate confirms")

- **Confirmed: the cleaning write's unheld event.** `cleaned_victim_left` (`evict.rs`) runs on E5's and E6's `Kept`, under the table mutex, as U1–U4 do.
- **Confirmed: the own pass's dirty frames.** A dirty frame the own pass latches is handled as the clock's is; both go through `take_candidate`.
- **Confirmed: the order of A-3b's answers.** `PoolStarved` and A-3b's own restart are answered only after A6′.
- **Not confirmed in full: A-3c's own `FlipPending` restart.**
  - It follows Work 8.3's first sentence as amended (call 14): the clock's sweep, then A5's free-list and generation re-check. So the code does what the card says.
  - §5.2 restarts only "if no victim is clean". Read for every restart, the folded sentence says "no acquisition answers `PoolStarved` or a restart on the strength of the shared hand alone".
  - The sweep ends once the shared hand has moved `2F` positions, counting every thread's moves, so under concurrency it may not have visited every frame. An acquirer can therefore restart while a clean, unpinned frame exists, and a caller holding nothing then waits for the flip.
  - The cost is liveness only: a restart before a mutation is always safe, and the wait ends with the flip.
  - SW-A01 drops the early answer, so that A5 and A6′ give `Pass::FlipPending` after the acquisition's own look, and it survives every binary.
  - Follow-up A-3c-F5 settles the card text and pins it.

## Correctness review (what this gate checked, runs 2 and 3)

**Every new file of `src/buf/` against Work 1–9, in order** (run 2):

| Rules | Where | Checked |
|---|---|---|
| G1–G6 | `gate.rs` | registration before the hook; the re-test loops; `notify_all` after each state change; `checked_add` at `u32::MAX` |
| S1–S5 | `slots.rs` | the growth's bitmap reserved before the one `set_len`; a failed `set_len` changes no visible state; duplicates found with no allocation |
| L1–L3 | `lpids.rs` | `fresh_start` and `commit`'s maximum: the run from `next_lpid` is always contiguous, so no LPID below `next_lpid` is left neither free nor taken |
| E0–E9 | `evict.rs` | E1's gate (the caller's hold, or a try that never blocks; `FlipPending` only from a failed try); E2's `PrevEpoch`; E3's seal; E5's and E6's orders |
| Work 3.3–3.8 | `cmap.rs` | one map latch at a time, top-down; the `BodyCell` latch counts its waiter under its mutex, so no give-back's wake-up is lost; `touch_check` as in (a) |
| Work 9 | `wb.rs` | the configuration validated before the pool is built; a pool from `Pool::new` answers `Invalid` to every write-back call (`writeback_absent`) |

- **The review's fixes, re-read in run 3.**
  - §4.1's pin end counts the unheld event at all three sites under the table mutex: `create`'s failed revival (`pool.rs:1542`), `mark_freed` (`:1741`) and `end_dirty_pin_of` for `mark_written` (`:1829`). `end_dirty_pin` is `#[must_use]`.
  - `yield_unless_in_load` is at A6 and A6′.
  - `is_nothing` is used only by `restart_or_wait`, whose one restart kind (`FlipPending`) a gated hold never meets.
- **E6's window.** Between `drop(v)` and the second `try_victim`, another thread may dirty the MapLeaf. `remove_victim` keeps a dirty frame, so no write is lost.
- **Every edit to A-3a's and A-3b's files** matches the review's RESULT item 6.
- **Code rules** (PLAN §6.1): no `unsafe`, `unwrap`, `expect` or `panic!`, no truncating `as` and no TODO in the new code. `indexing_slicing` is denied crate-wide, and `missing_docs`, `unwrap_used`, `expect_used` and `panic` are enforced by the clippy step.
- **Oracles.**
  - `WbOracle` derives the reserved slots from §2.4 and calls no store code.
  - `lpid_table`'s model restates L1–L3 in its own code.
  - `lpid_held_scripts`' expectations are hand-derived.
  - Comparisons destructure with no `..`.
- **The races** record, release, join, then assert. At each pause the controller makes only calls that cannot block on the paused party's holds; `t_pin_load_race_h` was read line by line for this. R1 and R2 are the two exceptions found.
- **`t_pool_floor_scattered`'s numbers** are R-1-memory's (`F` 94, `R_EPOCH` 5, `R_MAP` 26), computed in the test.

## NITs

- NIT: `dirty_pin_target` and `mark_dirty_frame` (`pool.rs`) take §4.1's pin from a MapLeaf frame found in the table in any state, where Work 6.2 says "it must be Resident". A `Loading` MapLeaf passes. Only a caller that breaks SF-1's rule (it did not pin the MapLeaf first) can meet this, and the error then surfaces as `Invalid` at the page's eviction, never silently.
- NIT: `create`'s revival takes the §4.1 pin before `t.raise(g)`. A raise that overflows (a `u64` count) leaves the MapLeaf's pin taken but unrecorded. Unreachable.
- NIT: E6's `Kept` after the frame was dirtied again in the window calls `cleaned_victim_left`: an extra unheld event, harmless.
- NIT: `lock_freeset` fires `FreesetLocking` with `fire` and its guard fires `FreesetUnlocked` with `fire_wb`. Harmless, since the free-set lock exists only in write-back pools.
- NIT: `lib.rs` block (d)'s narrowed line names A-3c-C for "a pool built with `Pool::new`" too; A-3c-C changes write-back pools only.
- NIT: `Pool::with_writeback` builds its write-back state before `Pool::build` refuses more than 65,535 callers: a transient allocation before an `Invalid`.
- NIT: `touch_check`'s doc (`cmap.rs:285–292`) cites the report's sentence; once S-NU lands it should cite S1's clause.
- NIT: the card's text for rows 15 and 34, call 8's alphabet, Limit 4, Work 3.8's first signature, Work 5's `release_lpid` sentence, call 14's full stop and the new tests' names, as in (c), (d) and (e).
- NIT: some refusals of impossible inputs are driven by no test, so SW-S18, SW-S22, SW-L17 and SW-L18 survive. They are `supersede` of a free slot, `release_step6` of a slot at `slot_count`, and `release_lpid` at or past `next_lpid` or of a free LPID. The models skip those ops as precondition failures, and no correct caller reaches them. A-3c-B, which turns each such `Invalid` into a poisoning XX000, should test each.
- NIT: a MapInner's `dirty` and `content_epoch` (`cmap.rs`) are written and never read. 7.23 never copies a MapInner, and step 2 is A-3c-C's, so SW-C08, SW-C09 and SW-C16 are equivalent here. A-3c-C should decide whether its map phase needs them, or remove them.
- NIT: `with_writeback`'s refusals (Work 9) are driven by no test, so SW-W01 to SW-W05 survive. They are epoch 0, a file of the wrong length, `next_lpid` 0, and a free LPID that is 0, at or past `next_lpid`, or listed twice. A-7d, which builds the configuration from an image, should test them.
- NIT: no acquisition in any test meets a `PrevEpoch` frame with no other victim, so SW-A06 (`PrevEpoch` counted busy) survives every binary. Under it, a write-back pool whose unpinned frames are all dirty from before the flip would spin rather than answer `PoolStarved`, the departure `lib.rs` documents. This is transient: A-3c-C writes previous-epoch victims and removes `PrevEpoch`.

## Follow-ups

**A-3c-F1 (spec, before the merge).** Card S-NU carries S1's clause for A-3c, with N1's New replaced by this verdict's wording in (a). Its title, summaries, status line, Changelog entry and the Acceptance lines that quote the sentence are re-derived for it. A-3c merges only after S-NU merges, or in the same pass with S-NU first.

**A-3c-F2 (for A-3c-C).** Four obligations this card leaves to the card that lands step 2 and the image-record list:
1. **Convert the flip tests.** S1's clause lapses where step 2 exists. The A-3c tests that mutate a MapLeaf of content epoch below `e` after an S1 flip must then run step 2's map phase after the flip, or make `write_guard`'s copy as S3 does. They are `row_i_free_safe` phase 2, `t_two_gen`, `touch_check_table` (a) and (c), `evict_steps` round 3, `map_mutation_after_flip`, and any other a grep for `s1_flip` finds.
2. **The touch check follows §5.6 itself.** A-3c-C's drafted Work 6.2 already does this: `written_for == e − 1` replaces `!dirty`. The tests in item 1 fail under it unless they are converted, and that card names only `evict_prev_epoch`'s rewrite.
3. **§4.4's MapLeaf clause in S4 and the oracle.** S4 (`SlotAlloc::supersede`) and `WbOracle::wrote` both gain the clause ("an eviction home of a MapLeaf that no image-record list names, whatever epoch its ref carries"). A-3c-C's card plans the `SlotAlloc` half; the oracle must follow. Until then such a home leaks until step 6 frees it, which is safe.
4. **Test the unheld event of a `Kept` after a write.** It becomes load-bearing for A6′ once previous-epoch victims are written. Add a test that fails without `cleaned_victim_left`; at this card SW-E12 and SW-E16 survive.

**A-3c-F3 (for A-4c, `t_latch_scope`).**
- LOAD-ACROSS-YIELD must be judged on the nested MapLeaf pin of a load (Work 3.3, 8.6), the first acquisition that runs inside a load. The revert of `yield_unless_in_load` (SW-A08, the review's r9) survives every A-3c binary and must fail there.
- Under N10's cooperative mode, the nested acquisition's A7 wait must be shown unreachable at `F_min` (§5.3, §5.4's count) or counted by the tracker as a yield point.

**A-3c-F4 (for A-3c-B, which adds the poison mark to every wait predicate in `buf`).** The waits' re-tests and wake-ups are pinned by chance or not at all. The card's rule asks a harness kill to fail in every execution.
- **G4's re-test** ("every blocking wait re-tests its grant condition … on every wake") is pinned on neither gate wait, nor on the map latch's wait:
  - SW-G12 (the flip's wait does not re-test) survives every binary.
  - SW-G07 (`gate_shared`'s wait does not re-test) fails 3 of 1,000 executions of `t_pin_load_race_k_reload` on both runs (first at execution 0, "T1 read epoch 1 after its pin"). `t_pin_load_race_k` passes it, and `gate_rules` failed it in one of two OS-thread runs (`wb_race.rs:618`, "S is granted at the new epoch"). The binaries the sweep never reached for it (`buf_latch`, `wb_race` without `t_pin_load_race_a`, `wb_floor`, `buf_race`) pass it on two runs.
  - SW-C02 (the map latch's wait in `BodyCell::take_then` does not re-test) survives every binary. With two waiters, the second, woken after the first took the body, would take none (`Invalid`, not silent); `map_latch_wait` has one waiter.
- **Lost wake-ups:**
  - SW-G14 (`GateHold`'s drop notifies nobody): `t_pin_load_race_k` and `t_pin_load_race_k_reload`, 998 of 1,000 each, both runs. On OS threads, `gate_rules`, `evict_holds_gate` and `flip_pending_rechecks_free` hang under it on both runs (150 s bound) rather than fail. `buf_latch`, `wb_floor`, `buf_race` and the other eleven `wb_race` tests pass it.
  - SW-G16 (`FlipGuard`'s drop notifies nobody): `t_pin_load_race_k_reload` 12 of 1,000, both runs. On OS threads, `gate_rules` hangs under it on both runs rather than fail; every other binary passes it.
  - SW-C03 (the map latch's request is not counted), SW-C04 (the give-back notifies only past one waiter) and SW-C05 (the give-back notifies nobody, the review's r8): `map_latch_wait` 325 of 1,000 each, both runs. Every other binary passes all three on both runs. The test's own doc says why: whether W is already asleep at the give-back is the schedule's.
- **The hangs are test defects the card asks the gate to report.** The card counts a hang on OS threads, rather than a failure, as a test defect. Without a clock (Limit 5), a lost wake-up can only hang an OS-thread test, so the kill for these mutants belongs to the harness.
- **What A-3c-B adds, per wait:**
  - a hook-steered test that wakes the waiter while its condition is still false, so that each re-test mutant fails in every execution. Examples: two shared holders under a waiting flip, where the first release must not grant it; a shared waiter woken while a flip is still registered; two map-latch waiters.
  - for each dropped or narrowed notify, a test that releases only once the waiter has registered and fired its waiting hook. No hook can fire after a thread is asleep, so a lost wake-up fails only the executions in which the waiter slept first. The card that adds these tests states the share it requires, in A-3b-F1's form ("share 5%: met"), in place of every execution.

**A-3c-F5 (for A-3c-B, the next card in `acquire.rs`).** Settle and pin whether Work 8.3's `FlipPending` restart needs the acquisition's own pass first.
- The card's first sentence, as amended, answers it after the clock's sweep and A5's re-check.
- §5.2 ("If no victim is clean") and the folded A-3b-0-F1 sentence, read for every restart, call for A6′ first.
- Recommended: drop the early return, so that A5 and A6′ give the same answer (`Pass::FlipPending`) after the acquisition's own look. This gate's SW-A01 is that change, and every binary passes with it.
- The test is a hook-placed race. Another acquirer moves the shared hand past the only clean frame while T's sweep meets a dirty frame it cannot write for the pending flip. T must take the clean frame, and `Restarting` must not fire.
- Two survivors belong to the same test:
  - SW-A02: the re-check without its generation term misses an unpin that leaves a frame unheld but frees none. `flip_pending_rechecks_free` frees a reservation, which moves both terms.
  - SW-A07: the own pass answering `Starved` for a pending flip is reached by no test while the early return answers first.

**A-3c-F6 (tests; for the next card that touches `tests/wb_race.rs`, `tests/wb_unit.rs` and `tests/wb_harness.rs`, A-4a at the latest; the R1/R2 rework may fold any of them in).** These mutants of correct code survive every binary, or fall short of the card's every-execution bar. Each has a one-case fix.
- **OWN-1 (E1's gate try hold ends after E4's write, before E5's entry set and supersede) and SW-E13 (E6 releases the victim's latch before it pins the frame) matter most; both are recommended for the rework.**
  - OWN-1: `evict_holds_gate` pauses the evictor only at `EvictWrite`, so nothing checks that the gate still covers E5 and E6. Once step 2 exists, a flip in that window would hand E5 a stale `e`. Its `supersede` would then free at once an epoch-`e` home that image `e` may reference (I-FREE-SAFE), and its touch check would pass a MapLeaf that was dirty at the flip. Fix: run `evict_holds_gate`'s check a second time with the evictor paused at `EvictEntrySet`, spawning the flip there. The flip must register and wait (`GateExclusiveWaiting`), not end. Under OWN-1 it ends (`Done("X")`) in every run.
  - SW-E13: in the window, another evictor can take the clean, unpinned, unlatched MapLeaf frame as a clean victim while its parent's ref still names the old home. E6 then pins a frame that is no longer the MapLeaf's (`Table::raise` does not check the state). A pin of the MapLeaf inside the window reads the old home through the stale ref and installs content missing every entry set since. Nothing fails, so the loss is silent. `mapleaf_two_step` checks the pin only at `EvictEntrySet`. Fix: a recording hook at the first `TableLocking` after `EvictWrite` for `MapLeaf(0)`, the one `pin_frame_internal` takes, where `try_victim(f_M)` must be `None` because the latch is still held.
- **SW-T02 (the entry set takes its MapLeaf's latch by try and refuses when busy).** It fails 8 of 1,000 executions of `t_freeset_lock_order` and 5 of 1,000 of its boundary form, the same on both runs, and survives every other binary. Under A-3c-B such a refusal would poison the store. Fix: a hook-placed case in which another thread holds the MapLeaf's latch shared at the evictor's entry set; the evictor must wait, then finish.
- **SW-L02** (`fresh_start` counts only LPIDs past `next_lpid`). With `next_lpid` at a held MapLeaf's last LPID and more LPIDs asked than the held candidates, L3 would hand `next_lpid` out twice. This is the double hand-out the review fixed; it is pinned only away from that boundary (`lpid_held_scripts` (e) has `next_lpid` at the MapLeaf's first LPID).
- **SW-L07** (L1 needs more than `n` candidates). Exactly `n` held candidates beside an emptied held MapLeaf must be taken by L1; the mutant restarts.
- **SW-B02** (`first_set_in` loses its high mask). A free LPID under a later MapLeaf in the same bitmap word must not count as a candidate of an emptied held MapLeaf. Case (b) has the free LPID below the held MapLeaf, which the low mask covers.
- **SW-C06** (`leaf_parent` takes `i == cover` as covered). A pin just past the root's cover would descend through slot 0 and load MapLeaf 0's home: `Corrupt` from the id check where `Unmapped` is right. Fix: one case in `cmap_growth` or `entry_roundtrip`.
- **SW-C12** (growth fires no `GrowLocking` before a new level). `growth_race_level` still passes, because T2 then makes the levels itself and pauses at the MapInner hook. The test should assert its stated precondition at T2's pause (the root still `(R, 1)`), or LEVEL-GROW-UNCONDITIONAL's kill rests on an unasserted hook placement.
- **SW-C17** (growth reads a child before its hook and latch, then decides on that read). `growth_race_inner` pauses T2 at the root's `GrowLocking`, one level above the parent that lacks the MapInner. Pausing T2 at its `GrowLocking` for `(2,1)` (its second firing) places the race where Work 3.7's "each decision is made under the mutex it changes, after the hook" applies.
- **SW-W07** (`is_nothing` ignores `latched`). No test hands a pin a `Hold::latched()` or `Hold::gated_latched()` hold. Under a pending flip such a pin must answer `Restart`, not wait for the flip while it holds latches. Fix: one OS-thread case, before A-4a, the first real caller.
- **OWN-3 (E5 supersedes the old home before the entry names the new one).** Nothing is placed between E5's steps, so the card's order, entry then supersede, is pinned by no test.
  - In the window the old slot is free while the current map still references it, against §4.4's item 3.
  - Another evictor's lowest-free allocation can take it and overwrite it. A pin of the page through the stale entry then reads a foreign page: `Corrupt` from the id or crc check, so not silent.
  - Fix: a pause at the MapLeaf latch's `LatchTaking` inside E5's entry set, where `slot_use(old)` must still be `Used`.

## The re-gate after R1 and R2

Scope: the fixes' diff, which should touch `tests/wb_race.rs` and `tests/wb_harness.rs`, plus the test file of any A-3c-F6 case folded in, and nothing under `src/`.
- The scaffold's fresh gate on the new tip.
- Row 1 (LOAD-REF-BEFORE-PUBLISH) still caught on two runs at the second-event assertion.
- 400 runs of `t_pin_load_race_a` and 300 runs of the whole `wb_race` binary under load (1-minute load at least 15), with no failure.
- Row 37 (RELOAD-BEFORE-FLIP) failing 1,000 of 1,000 executions of `t_pin_load_race_k_reload` on two runs under the census; SW-S14 surviving it; SW-G06 still 1,000 of 1,000.
- Any A-3c-F6 case folded in kills its mutant on two runs (OWN-1, SW-E13, …).

Everything else in this verdict, rulings (a) to (e) included, carries over unchanged if the code under `src/` is byte-identical to `a53cb1de`.
