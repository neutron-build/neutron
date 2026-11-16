Card: X-5c-F4-F1-F1-F3
Class: experiment
Card-gate: P
Verdict: ESCALATE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-gate-X-5c-F4-F1-F1-F3-1 (Claude Code session 1467fdb0-5c34-470f-8a46-aa14bca897a6)
Date: 2026-11-07
Gate-exit: GATE EXIT=0
Commit: d1d9faaeb69b7455e4c957bba1cbbeb49f05ca33
Mutants: M-R41-NOHOLD -> x5_r41_aligned_round_has_q4_and_reuse, M-R41-WRONGWINDOW -> x5_r41_aligned_round_has_q4_and_reuse, M-R41-NOPREP -> x5_r41_aligned_round_has_q4_and_reuse, M-R41-DRAW -> x5_r37_base_tuning_reproduces_1b0ce54e, M-R41-AFTERCRASHARM -> x5_r41_aligned_round_has_q4_and_reuse, G-Q4MG-FROM-SPLITS -> x5_r41_aligned_round_has_q4_and_reuse, M-REUSE-DROPS-IMG -> x5_r41_completed_window_arm, M-FREE-HOME-ALLOC -> UNCAUGHT (inert: digest-identical to clean on 400/400 seeds), M-Q4I-LASTCKPT -> x5_r42_q4i_keys_on_the_opened_image, M-Q4I-REFUSED -> x5_r42_refused_round_is_not_q4i, M-Q4I-NOFLOOR -> x5_r42_row_at_999_and_1000, M-Q4I-NOREPRO -> x5_r42_flipped_q4i_fails_reproduction, M-SEEDED-NOBASELINE -> x5_r43_known_failures_are_not_new, M-SEEDED-NOARMRULE -> x5_r43_caught_needs_the_arm_predicate, G-SEL-NODROPCLAUSE -> x5_r41_completed_window_arm, G-SEL-NOTARGET -> UNCAUGHT, G-REUSE-ANY -> UNCAUGHT, G-LEDGER-NOREMOVE -> UNCAUGHT, G-SEEDED-FOLD -> UNCAUGHT
Followups: X-5c-F4-F1-F1-F3-F1, X-5c-F4-F1-F1-F3-F2

## Verdict in one paragraph

ESCALATE, because the card requires it: the decision rule is NOT MET by all three shares, and the seeded Slot fault is not caught. I reproduce every number in the worker's shares table exactly. **Two of the worker's explanations are wrong, though, and the corrections change what the owner is choosing between:**

1. The Q4 shortfall is **sampling, not the `!knobs.drop_tree` clause**. The 1,760 prototype reading (`probes/r_tnd1000`) already ran with that clause; it is the verdict X-5c-F4-F1-F1 column "R41 1000, targeted, no drop rounds". Its seeds 0..200 read Q4 = 28, which is exactly this tip's 28.
2. The Slot fault is **neither uncatchable nor hidden by the harness. The card's mutant is inert.** It never makes a slot allocatable, and its arm is digest-identical to clean.

The code is sound enough to stay on the chain, inert at share 0, as the base for -F1 (see "Merge or not").

## Gate run

- Scaffold `504f581c2899af8e588d6bdb69fbf0cb2c316393`, extracted by the shallow `--no-local` clone, with no EXTRACT FAILED line.
- Run 1, while the machine was loaded (my own builds, load 10–14): `GATE EXIT=1`. The only red is `ack_prefix_two_batches_in_flight` ("driver timeout: B never completed batch 2"). This is the known 30-second wall-clock flake from the X-5c-F4-F1-F1-F2 gate. `runner e2e: K` is **ok**.
- Run 2, with nothing of mine running: `fresh-gate: checks passed (4512 files hashed)`, every STEP EXIT=0, `GATE EXIT=0`, and K ok again.
- So the worker's three K reds (claim 5) are their own session's container issue, which the card's Notes predicted. No `scripts/` file is touched.

## Scope and correctness

- Every changed file is under `spikes/e4-recovery/`.
- The added code has no `unsafe`, non-test `unwrap`/`expect`/`panic`, `std::env`, wall clock or `HashMap`.
- The selection, the block, the hold (before the crash arm) and the BASE draw contract match step 1, and the 54-seed BASE pin passes.
- The ledger keys on the opened meta's `ckpt_id`, so a rollback reads the older image's window.
- Defects:
  - **D1: `Mutant::FreeHomeAlloc` is inert, and its doc comment is false.** It says "a later `unpin_home` … releases it from `used_cur`". It never does: in 2,935 firings (share 1000, seeds 0..200) every freed home was still in `used_cur`, with 0 releases and 0 hand-outs. The README repeats the false cause, "its damage lands only in rounds that already carry oracle texts". In fact no round record differs at all.
  - **D2: Q4-I counts reuses after the freeze.** The owner's row says "reused **before its step 2 froze the table**". `ckpt_reuse` counts until step 5; the worker flags this as a judgment call. With a strict pre-freeze count, `q4i` reads 28 (×50 = 1,400) at 0..200, not 30 (1,500), and 135 instead of 140 at 0..1000. So the claim "image arm = 1,500, exactly on the bar" holds only under the looser reading.
  - **D3: two doc comments were joined mid-line.** `campaign.rs:2521` (`empty,    /// which`) and `workload.rs:1075` (`(5.5/// step 2`).
  - **D4: four unpinned behaviours** (my own mutants, all UNCAUGHT):
    - the targeted-crash clause of the selection;
    - `ckpt_reuse`'s `map.img` test;
    - the ledger's removal of the harness checkpoint;
    - the c3d1b4b5 correction itself. Its pin's "known failure" has `pass = false`, so the fold reading and the oracle reading agree on it.

## Measurements

All runs used my own `git archive` export with `ulimit -v 25165824`, 16 threads unless stated, and output outside the worktree.

**Shares table, seeds 0..200.** I reproduce every count exactly: rounds 304/308/307/304; Q4 0/28/23/18; Q5a 3/4/2/2; Q7 6/6/7/7; Q18 19/94/84/63; q4i 4/30/27/18; failing rounds 2/1/1/2.

**CPU is not a stable measure here.** Five back-to-back BASE/1000 pairs, user+sys CPU seconds:

| pair | BASE | share 1000 | increase |
|---|---|---|---|
| 1 | 158 | 325 | +106% |
| 2 | 240 | 337 | +40% |
| 3 | 193 | 339 | +75% |
| 4 | 239 | 340 | +42% |
| 5 | 242 | 340 | +41% |

- The worker's +103% reproduces only in pair 1, where BASE read low.
- At 4 threads: BASE 111, share 1000 157 (+41%), share 600 134 (+21%).
- The share-1000 runs barely vary, but BASE does (158–242). The rule's CPU term therefore reads anywhere from +40% to +106% depending on load. That does not decide the outcome: Q4 misses on its own.

**Seeds 0..1000 at share 1000** (beyond the card's range; gate data, not a probe the rule names):

| quantity | per 1,000 seeds | ×10 (per 10k) |
|---|---|---|
| Q4 | 177 | 1,770 |
| Q4 by 200-seed block | 28 / 37 / 33 / 39 / 40 | |
| Q5a | 25 | 250 |
| Q7 | 19 | 190 (BASE also 19, so the floor is 190) |
| Q18 | 500 | 5,000 |
| q4i | 140 | 1,400 |
| q4i, pre-freeze | 135 | 1,350 |
| imgreuse ≥ 1 | 159 | 1,590 |
| imgsp ∧ imgmg | 153 | 1,530 |

CPU was 1,774 against BASE's 1,314 (+35%). Seeds 0..200 are the lowest of the five blocks, both here and in the prototype (28/36/33/39/40).

**The image arm's reading decides the wider-range result.** The card's 1,500 bar came from the earlier verdict's "reuse 1,590", which is imgreuse. The worker read the image arm as q4i. Under q4i the arm misses 1,500 at every range I ran. Under imgreuse it passes. This choice is the owner's.

**Seeded faults over 0..200, share 1000.**

- Joint: 19 new failing rounds, 6 of them with `q4i_raw`; CAUGHT. This reproduces the worker's result exactly. BASE gives 1 (63r1). On seeds 200..400: 20 new failing, 5 with `q4i_raw`; CAUGHT.
- Slot, the card's form (`used_durable` + `image_refs` removed): 0 new failing, and **0/200 digests differ from clean**. The same holds at BASE and on 200..400.
- Slot, the earlier gate's prototype form (`r41-prototype.patch` also removes `used_cur`; the card's text and `patch.py` dropped that line): 37 new failing, **6** with `q4_split_merge`, CAUGHT. On 200..400: 37 new failing, 11 with `q4_split_merge`, CAUGHT. At BASE: 23 new failing, 0 with the predicate.

**The underlying cause is a store deviation.** In the clean store, a freed LPID's home slot never leaves `used_cur` until a restart: in 2,935/2,935 frees it stayed, and step 6's `pending_free.clear()` frees nothing. This contradicts S-STORE §6.5 item 3 and §13 step 6, which make the slot free in memory once meta k is synced. It is a leak, not a loss. But it means the clean spike never reuses a freed page's slot within a session.

I also tried a scratch store fix that unpins the slot at `free_lpid`. With it:
- the clean arm shows 0 new oracle texts and 0 fold flips over 0..200, while 62/200 digests move;
- the card's literal mutant wakes up (21–22 new failing rounds), but with 0 `q4_split_merge` hits, because frees inside a window still pin the slot through `Img::At`. It remains NOT CAUGHT.

## The X5-R43 correction and the pin removal

Both are **legitimate, and they strengthen the test rather than weaken it**.

- Under the fold reading, the inert Slot mutant, with byte-identical digests, would have read 45 new failing rounds, 2 of them with `q4_split_merge`: a false **CAUGHT**. The Joint fault's 63 "catches" under that reading include 44 with identical texts.
- The removed pin `x5_r43_free_home_alloc_is_caught` (seed 17 r0) was vacuous for the same reason.
- The blind spot you asked about is real but small. About 15% of clean rounds (46/308, all MP rounds that carry an expected text) cannot count as catches. At share 1000 the prototype-form Slot fault changed the text in 8 of those rounds. That is not why Slot reads 0.

## Merge or not

Keep the code on the chain as the stacked base for -F1. It is inert at BASE (no digest moves, and the 54-seed pin passes) and gated green. Its machinery reproduces exactly. -F1 fixes its defects D1–D4. Do not hand off a share; FINAL stays BASE.

## FOR THE OWNER

Your decision rule is unmet as written. The two reasons the worker gave were wrong; the real reasons are below.

1. **(Recommended) Fix the Slot mutant and re-run the rule over a wider seed range.** This is card -F1: add the missing `used_cur` release; count Q4-I before the freeze; you choose, before it runs, whether "the image arm" means q4i or imgreuse; then run share 1000 over 0..1000 ×10. It costs about an hour of work and a few minutes of runs, and it amends pre-registered text after seeing data, which only you can authorize.
   - On my data, Q4, CPU, the floors and both faults pass.
   - The image arm passes only under the imgreuse reading.
   - Left unproven: the clean store's own slot reuse (option 2).
2. **Also fix the store** (card -F2: free a freed home at step 6, per spec). This makes I-FREE-SAFE testable in the clean store. It moves digests, so every literal and the 10k baseline must be re-derived. On its own it does not catch Slot with the Q4 predicate, so it still needs option 1.
3. **Accept the gaps.** Hand share 1000 to F4 as is and state Slot as a known gap. This is the cheapest. It leaves seeded-fault detection for I-FREE-SAFE unproven, and F4's `seeded-faults` Acceptance would always exit 2.

**My recommendation: 1 now, with 2 before Phase A.** Neither failure comes from the mechanism: one is a 200-seed sample, the other a dropped line in the mutant's text. If you keep q4i at 1,500, no share can reach it, because 1000 is the maximum. That would need a design change (for example selecting uniform-crash rounds too) or your acceptance.

## Follow-ups

**X-5c-F4-F1-F1-F3-F1** (experiment, Gate P; base this tip; after the owner's answer).
- Make `M-FREE-HOME-ALLOC` also remove the slot from `used_cur`, as `r41-prototype.patch` does, and correct its doc comment and README text.
- Add the pin `x5_r43_free_home_alloc_is_caught` on a literal seed whose digest differs from clean.
- Pin the oracle-pass reading with a clean round that has `pass = true` and an oracle text.
- Pin the selection's targeted-crash clause, the reuse counter's `map.img` test, and the ledger's harness-checkpoint removal.
- Split `ckpt_reuse` into a pre-freeze count, which Q4-I reads, and the whole-window count, which `q4reuse` keeps.
- Repair the two joined doc comments.
- Then apply the rule as the owner re-states it (seed range, and the image-arm quantity fixed in advance), and run `seeded-faults` at the share it chooses.

**X-5c-F4-F1-F1-F3-F2** (Class spec/core, P ruling first).
- The spike never releases a freed LPID's home slot from `used_cur` within a session. This contradicts S-STORE §6.5 item 3 and §13 step 6.
- Rule whether it is a prototype deviation, and if so fix it so that step 6 actually frees the slot. A gate scratch that unpins at `free_lpid` moved 62/200 digests and added no oracle failures.
- Re-derive the X5-R37 literals and the baseline afterwards.
- Evidence: `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F3/m/` (`t1000.err`, `s*_fix*`).
