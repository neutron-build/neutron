Card: A-3b
Class: impl
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session de9acebb-035a-4284-b33f-867b0566534a (T3 Code review sub-agent, P gate of record for A-3b, requested by T3 thread 45584af2-e651-490e-862c-3402320cffa3)
Date: 2026-11-27
Gate-exit: GATE EXIT=0
Commit: ef27ff2d1abdb180d64245540c2ba04a32dc30ee
Mutants: 1 LATCH-READER-PREF -> t_root_latch_fair_reader_stream (Violation at execution 0, RANDOM: M2 and direct count 6 > B = 2) | 2 LATCH-WRITER-PREF -> t_root_latch_fair_writer (Deadlock at execution 0, RANDOM; not M4) | 3 TRYLATCH-BYPASS -> t_root_latch_fair_latch_level (Violation at execution 87, EXHAUSTIVE: M3 and the direct assertion) | 5 FRAME-CAPACITY-ONE -> frame_latch_pinned (assertion), t_root_latch_fair_reader_stream (StepLimit at execution 0, RANDOM; not Invalid) | 12 LOAD-NO-NOTIFY -> harness_load_waiter_woken (Deadlock at execution 1, the parked order under PCT) | 13 WAIT-NO-RECHECK -> harness_load_waiter_woken (Deadlock at execution 0, the look order, EXHAUSTIVE) | 19 NOTIFY-ONE -> UNCAUGHT by every committed test (harness_unheld_pin_drop 95,737, _reservation_drop 188, _failed_load 4,637 all pass); NOT equivalent: the gate's dirty-page probe deadlocks 380/1000 RANDOM, 932/1000 PCT, 417/1000 RANDOM seed 0xA4C0_0003, correct build 0 failing in each (A-3b-F1) | 20 TRYLATCH-BYPASS-HELD -> t_root_latch_fair_latch_level (Violation at execution 60273, EXHAUSTIVE: M2 and the direct assertion) | TL1 std WouldBlock->Err(Poisoned) -> sync_mutex_try_lock_std (:1684) | TL2 std Poisoned->Ok(None) -> sync_mutex_try_lock_std (:1697) | TL3 backend WouldBlock->Err(Poisoned) -> sync_mutex_try_lock_model (Violation at execution 4) | O1 BYTES_FOR-NO-QUEUES (gate's) -> footprint_at_production_sizes, pool_fixed | O2 CAPACITY-FIELD-FROM-BOUND (gate's) -> footprint_at_production_sizes, latch_api | O4 UNPINNED-HOOK-UNDER-TABLE (gate's) -> hooks_fire_unlocked, acquire_hooks_unlocked | O8 SHARED-AS-EXCLUSIVE (gate's) -> UNCAUGHT by every binary of the crate (A-3b-F2) | rows 4, 6-11, 14-18 and ORPHAN-FREE-NO-NOTIFY -> as the second GLM pre-gate records them on ef27ff2d (its report section c.1), not re-run by this gate; code and tests byte-identical
Followups: A-3b-F1, A-3b-F2, A-3b-F3, A-3b-F4, A-3b-F5, A-3b-F6

## Gate run

The scaffold's `fresh-gate.sh` and `git-env.sh`, extracted per GATE-PROMPT step 1 through a shallow `--no-local` clone (printed scaffold id `84d661e05181698149168e6f27e450e64189d8b1`; no EXTRACT FAILED), run once on the full commit id, first run:

```
fresh-gate: commit ef27ff2d1abdb180d64245540c2ba04a32dc30ee
fresh-gate: base 84d661e05181698149168e6f27e450e64189d8b1
fresh-gate: checks passed (4942 files hashed in 1.48 s)
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

Scope: `git diff nucleus/v3-scaffold...HEAD --stat` lists 14 files, every one under the card's Touch only (`src/{sync,latch,lib}.rs`, `src/buf/*`, `tests/{buf_latch,latch}.rs`, `tests/harness/mod.rs`, `examples/latch_cost.rs`); nothing protected. Acceptance lines 1 to 9, run by the gate in the worktree from the card's text: every line exits 0 (line 8 with a clean status). Times (debug): `buf_latch` 12 tests in 22.22 s, `latch` 20 tests in 15.81 s, `harness_selftest` 22 tests in 2.67 s. The branch's base is the scaffold's tip `84d661e0`, so it merges as it stands.

Mutants were planted in a `git archive` extract of the tip, one at a time, with their own target directory, each under `timeout 600`, each file restored from a pristine extract and `diff -rq` clean after every entry. No catch rested on a hang.

## Findings

The production code is right against S-STORE §5.3, §9.2 and §15.1 and the card's Work 1 to 8. Nothing blocks the merge. The gaps are in the tests and in the card's text.

1. **The frame latch, the cell and the pool on the sync layer (judgement (a)).** `FrameLatch<P>` is `Latch<P>` with `K = FRAME_LATCH_BOUND = 1` and capacity `PoolSizing::threads`, beside a `RwCell<P, FrameContent>`. All four methods take the latch first and the cell second, by a try, and a failed try is `Invalid` (F7, XX000). The guard is dropped on every error path through `?`. `LatchShared`/`LatchExclusive` declare the cell first, and `release` drops it first (CELL-RELEASE-ORDER caught at 720, its explicit form at 8). The queue is fixed at `new_at`. A blocking waiter holds its pin and nothing else: the latch waits on its own internal mutex and condvar, never under the table mutex. The acquisition never blocks on a frame latch (`try_victim` → `try_exclusive`). Every lock, wait, notify and yield in `buf` goes through `crate::sync`, `Arc` aside. Hooks still fire with no mutex held: the hook slot is cloned under its own mutex and called after it is released (O4 caught twice through the `table_free` seam).
   - **Poison paths.** `Pin::drop`, `FrameReservation::drop` and `wait_unheld` return on a poisoned table mutex and leave the count, the `Reserved` state or the waiter count as they are. This is what §9.2 asks: "a thread that then takes it acts on nothing it guards: it returns the poison's error". It is also forced: the sync layer has no `into_inner` (F5). Removing A-3b-0's `unregister_waiter` follows from that, and leaves a stale count only in a store that is already poisoned.
   - **Earlier cards' pins.** Every earlier card's test binary passes unchanged (Acceptance line 1).
   - **What the tests do not pin that later cards rely on:** that two shared holds of one frame coexist (O8 survives the whole crate; A-3b-F2); a two-waiter lost wake-up (row 19; A-3b-F1); and the poison broadcast (A-3b-F5).
2. **The Opus implementer's four commits (b)** are right and inside the card as amended:
   - `d631a428` puts the latch-level variant in the card's order (W spawned right after main's shared grant). The search ends at 60,522, and row 20 fails it at 60273, which the gate reproduced.
   - `0ae3e08c` takes `footprint()`'s latch size at `StdPrimitives`, as Work 6 says, with the table lengths recorded at build. The new pin catches the old form and O1/O2. The allocation-free `Debug` is correct and pins the stale bytes, not allocation; that cannot be pinned under `unsafe_code = "forbid"`.
   - `4bb8c379` and `ef27ff2d` are docs only.
3. **Row 19 NOTIFY-ONE is not equivalent (e).** The gate's own probe, independent of the second review's:
   - Fixture: 1 frame for 3 callers; main creates `Lpid(1)`, `mark_dirty(1)`, and keeps its pin; two threads `reserve()`; main spins through `yield_now` until `stats().unpin_waiters >= 2`, then unpins.
   - Under the mutant the woken waiter leaves by `PoolStarved`, which notifies nobody, and the other sleeps for ever: `Deadlock` in 380/1000 (RANDOM, first failure at 2), 932/1000 (PCT, at 0) and 417/1000 (RANDOM seed `0xA4C0_0003`, at 4), each replayed.
   - The correct build fails no execution in any of the three.
   - The implementer's reading holds only for a clean frame.
4. **O8 SHARED-AS-EXCLUSIVE survives (a).** With `FrameLatch::shared` taking `latch(Mode::Exclusive)`, every binary of the crate passes: `buf_pin`, `buf_race`, `buf_evict`, `buf_model`, `buf_minimum`, `buf_latch`, `latch` and `harness_selftest`. The monitor's traces record the test's intended mode, not the latch's, so no test sees shared holds serialise. A-4a's tree relies on concurrent shared holds of the root (OP-3's reason). This is a test gap, not a code defect: A-3b-F2.
5. **The cost example (f).** It was run once as committed: `pool_shared 116.18`, `pool_exclusive 118.68`, `latch_shared 32.37`, `std_rwlock_read 16.68` ns/iteration; `estimate h=3 29.5% h=4 39.3%`. The crate's self dev-dependency (`Cargo.toml:55`, `features = ["test-hooks"]`) unifies `test-hooks` into the library the example links, and nothing inside Touch only turns it off. With the feature off in the gate's scratch copy only: `pool_shared 94.68`, `pool_exclusive 95.35`, `estimate h=3 24.0% h=4 32.0%`. Both builds exceed PLAN-v3 §3.12's 20%. The example meets Work 10 as written and needs no change in this card. The record for E0 and E7 is A-3b-F4.
6. **ORCHESTRATOR AMENDMENT 1 (c) is sound, with two texts corrected (Card corrections 4 and 5).**
   - The try-lock is the smallest addition that keeps BUF-TABLE-LEAF's nine oracles. It never waits, it answers poison per F5, it is a scheduling point under the harness, and no store path calls it. That last point is a convention, not a check: a later card that wants a mutex try in store code must justify it against F4 (a latch, not a mutex, excludes).
   - Work 9's "and the test prints them (or a companion test does)" cannot be met: on a correct build `kill_evidence` answers "no execution failed", as the gate saw.
   - Row 20 points outside the repository.
7. **Limits (i).**
   - Limits 1 and 4 are met as amended. Acceptance line 7 is clean, and the try-lock is the only facade addition beyond Work 1 and 2.
   - Limit 6 is met: the sampled parked order uses counts, not time, and S-STORE §15's sampled road with its evidence.
   - Limit 7 is met: `buf_latch` 22.22 s (the reviews: 33.97 s, longest test 21.12 s); A-4c0's binaries 18.5 s together.
   - S9 was not re-counted by this gate. `src/latch.rs` changed only `Debug` and two constructor-fixed fields, which add no scheduling point, and `latch_model_exhaustive` passes.
8. **How this branch was built (k).**
   - A GLM 5.4 worker built a first pass through the runner (cc542603, b3f3e591, a2b691d5, df87d895).
   - A GLM pre-gate reviewed it, and the orchestrator amended the card on that report (ORCHESTRATOR AMENDMENT 1).
   - A separate Opus 5.5 implementer session fixed what it found (d631a428, 0ae3e08c, 4bb8c379, ef27ff2d).
   - A second GLM review checked those commits.
   - This P gate judged all eight commits and the amendment. It relied on no report without its own run, except for rows 4, 6 to 11 and 14 to 18 and ORPHAN-FREE-NO-NOTIFY, which are the second review's records on the same tip.
   - The worker's three escalations are ruled here: the try-lock (accepted), the sampled parked order (accepted), and the 4.3% margin (A-3b-F3).

## Card corrections (the orchestrator applies them to the card text; no code change)

1. Row 1, **Must fail:** "`t_root_latch_fair_reader_stream`, as a `Violation` at execution 0 under `RANDOM`: M2 (shared request 16 granted while blocking exclusive request 8 waits) and the direct count (6 later requests, more than `B` = 2)."
2. Row 2, **Must fail:** "`t_root_latch_fair_writer`, as a `Deadlock` at execution 0 under `RANDOM`, not by M4: Work 2's queue names a request by its position, so an R3 that grants a non-head Exclusive entry shifts the entries behind it, the waiters' seqs no longer name their entries, and every thread blocks. No R3 mutant reaches M4 under Work 2's queue."
3. Row 5, **Must fail:** "`frame_latch_pinned` (assertion: capacity 1 where `threads` is expected); `t_root_latch_fair_reader_stream`, as a `StepLimit` at execution 0 under `RANDOM`, not `Invalid`: the waiter past capacity gets `Invalid`, its reader ends, `arrivals` never reaches `a0 + 1 + m`, and main's bounded spin ends at the step limit."
4. Row 20 in full: "20. **TRYLATCH-BYPASS-HELD** (a Try is not held back by a queued exclusive while a holder holds the latch). Site: `LatchCore::admits` (`src/latch.rs`), whose `blocked_by_writer` becomes `(mode == Mode::Shared || kind == Kind::Try) && self.exclusive_queued() && !(kind == Kind::Try && self.holders != Holders::Free)`. **Must fail:** `t_root_latch_fair_latch_level`, as a `Violation` at execution 60273 under `EXHAUSTIVE` (M2 and the direct assertion). It passes that test in the order the first pass committed (W spawned after the first round of tries), and it fails it in the card's order (W spawned right after main takes the latch shared), whose search ends at 60,522."
5. Work 9, `harness_load_waiter_woken`: replace "…each replayed and at or above the share, and the test prints them (or a companion test does)." with "…each replayed and at or above the share. No committed test prints them: on a correct build `kill_evidence` answers "no execution failed", so the lines exist only with the mutant planted; the gate reproduces them on a fresh extract."
6. Row 19, **Must fail:** "`harness_unheld_two_waiters_starved` (follow-up A-3b-F1; `Deadlock`, sampled). No test of this card fails it."
7. Notes, ORPHAN-FREE-NO-NOTIFY (OP-6): the classification is accepted; it stays a survivor with the card's reason.

## Follow-ups

- **A-3b-F1 (tests, `tests/buf_latch.rs`; folds into A-3c, the next card in `buf`).** Add `harness_unheld_two_waiters_starved` under `PCT` and `RANDOM` (1,000 executions each).
  - Fixture: a pool of 1 frame for 3 callers over `NoIoFile`. Main creates `Lpid(1)` through the created page's guard, calls `mark_dirty(1)`, releases the latch and keeps the pin.
  - Two spawned threads each call `reserve()`, accept `Err(PoolStarved)` or a reservation (which they drop), and fail on any other error.
  - Main waits through `yield_now`, under the step budget, until `stats().unpin_waiters >= 2` (or behind a `Gate` on `UnpinWait`), drops its pin, joins both, and requires `unpin_waiters == 0`.
  - On `ef27ff2d` the correct build fails no execution under RANDOM, PCT or RANDOM seed `0xA4C0_0003`. With NOTIFY-ONE planted at `became_unheld`: `Deadlock` in 380, 932 and 417 of 1,000, first failures at 2, 0 and 4, each replayed. The second review's gated form: 802, 207 and 809 of 1,000.
  - The RESULT carries the three `kill_evidence` lines with the mutant planted, and row 19's **Must fail** names this test.
  - A-3c, which adds dirty write-back, keeps a starved second waiter in the scenario (for example a dirty page whose write-back the test's file refuses) or re-measures it.
- **A-3b-F2 (tests; folds into A-3c).** Pin that two shared holds of one frame coexist.
  - In `frame_latch_pinned`, while `reader` (a `latch_shared` hold) is held, `pin.try_latch_shared()` must answer `Some`. Release both.
  - In `t_root_latch_fair_reader_stream`, at least two readers' holds must overlap in stamps in some execution. A batch grant gives them overlapping `[grant, release]` intervals.
  - Mutant SHARED-AS-EXCLUSIVE (`FrameLatch::shared` takes `latch(Mode::Exclusive)`) must fail. On `ef27ff2d` every binary of the crate passes it.
- **A-3b-F3 (tests; folds into A-3c).** `harness_unheld_pin_drop` ends at 95,737 of 100,000 executions on `ef27ff2d`, 4.3% under the cap. It judges rows 14 and 15 and A-3b-0's UNHELD-NO-BUMP-UNPIN.
  - Before A-3c adds a scheduling point to the frame acquisition or `Pin::drop`, it re-measures the count.
  - If the search no longer ends within the cap, A-3c gives the scenario another form and measures that.
  - A registration-only gate (main unpins only once T fired `UnpinWait`) is not that form: measured on `ef27ff2d`, its search is `NotExhausted` at 99,999.
  - Never raise the cap or sample this test: three kills rest on its complete search.
- **A-3b-F4 (record for E0 and E7, the owners of PLAN-v3 §3.12's trigger; no code).**
  - `examples/latch_cost.rs` builds with `test-hooks` on, through the crate's self dev-dependency. `pool_shared` and `pool_exclusive` therefore include two hook-mutex lock/clone/unlock pairs per iteration (`Pinned`, `Unpinned`) that production does not pay.
  - On `ef27ff2d`, one run each: as committed, `pool_shared` 116.18 ns, estimate 29.5% (h = 3) and 39.3% (h = 4); with the feature off, `pool_shared` 94.68 ns, **24.0% and 32.0%**. The second review measured 115.88 against 94.57 ns.
  - Both builds exceed the 20% trigger. The production pair is the estimate to read. `latch_shared` (32.4 ns) against `std_rwlock_read` (16.7 ns) is the latch's own share.
  - The trigger is E0's and E7's to decide. They measure with `test-hooks` off, in a workspace crate that depends on `nucleus-store` without the feature.
- **A-3b-F5 (for A-3c, which builds the poison mark in every acquisition and wait).**
  - At this card a waiter already asleep on `unheld` or `settled` when the table mutex is poisoned is not woken by the poisoning. A `Drop` that meets the poison returns without notifying. The waiter ends only at the next notify, with `Poisoned`.
  - S-STORE §9.2 item 4's broadcast must reach both condition variables. §9.2 also says only the broadcast "still takes" a poisoned mutex, while the sync layer's F5 gives no way into one.
  - A-3c states how its broadcast wakes these queues: a notify without the mutex, argued free of a lost wake-up against the waiters' predicate, or a sync-layer primitive for the broadcast. It pins this with `t_poison_wakeup_race`'s pool-wait variant.
- **A-3b-F6 (for A-3c's card and R-1-memory §4.1; no code at this card).** `PoolConfig::callers` is now bounded by `LatchCore::new_at`'s capacity domain: `callers + OWN_PINNING_THREADS ≤ 65,535`. Above it `Pool::new` answers the latch's `Invalid`, a refusal that A-3b-0's sizing does not state and no test pins. A-3c states it in `PoolConfig::callers`'s doc and pins the refusal at 65,536. R-1-memory §4.1's `o_frame = c + n` (the card's SF-4) gains the same bound.

Notes for other cards, not follow-ups of this one:
- **A-4c, which edits `latch.rs` next:** `Latch::bound()` and `Latch::capacity()` now return `u32` from fields fixed at construction (A-4c0-F4 (d), first option). They take no lock and return no `Result`.
- **For the record:** the core's `Debug` is allocation-free (the second review measured 0 allocations). The test pins only the stale bytes, and allocation cannot be pinned without `unsafe`.
