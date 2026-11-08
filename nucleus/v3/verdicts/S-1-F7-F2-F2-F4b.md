Card: S-1-F7-F2-F2-F4b
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-regate-S-1-F7-F2-F2-F4b (T3 review sub-agent, Claude Code session 2d11159a-bb6a-42b8-9655-fe9d42b569b8; it wrote neither the card, its amendments, the rulings, the reviews, the REWORK verdict c1f42387, nor any S-1-F7-F2-F2-F4b commit)
Date: 2026-11-07
Gate-exit: GATE EXIT=0
Commit: 7be2bf1593924aa7e7afc9815277113d70b11788
Mutants: gate text mutants on a fresh clone at 7be2bf15 (scaffold ref at acc81c77), all 59 G run per mutant: GM1 -> G58; GM2 -> G58; GM3 -> G58, G59; GM4 -> G58; GM5 -> G14; GM6 -> G15; GM7 -> G48; GM8 -> G7; GM9 -> G10; GM10 -> G21; B71 -> G14; B72 -> G14; B77 (both forms) -> G13; B78 (each of the three holders) -> G27; B79 -> G59; own: X1 swap R4c's order (unlatch, then release to the reserve) -> G59; X2 drop "(at most one per thread)" -> G14; X3 drop "which includes no transaction wait" -> UNCAUGHT; X4 drop "a descent step, CPU-bounded for a reader (above); " -> UNCAUGHT; X5 replace "This covers ... nothing is exempt." with "Readers are exempt." -> G14; X6 drop the flip clause's "plus the bounded bypass" -> G14; X7 "Every wait for an unpin, that one included," -> "A wait for an unpin" -> G13; X8 evictor latch "taken blocking" -> G27; X9 drop "the holders present at its arrival and" -> G14
Followups: none

## Gate run

- **Script.** The scaffold's `fresh-gate.sh` and `git-env.sh` were extracted through the `--no-local` shallow clone (GATE-PROMPT step 1), with no `EXTRACT FAILED` line. Scaffold id acc81c776183bd9853f7a69e2cec4262d300d89f. The script ran from `~/Workspace/neutron-v3` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-1-F7-F2-F2-F4b`, on commit 7be2bf1593924aa7e7afc9815277113d70b11788.
- **Lines.** One run, as a background task of the foreground harness, waited for. `fresh-gate: commit 7be2bf1593924aa7e7afc9815277113d70b11788`, `fresh-gate: base acc81c776183bd9853f7a69e2cec4262d300d89f`, `fresh-gate: checks passed (4779 files hashed in 3.12 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`; STEP test EXIT=0; STEP clippy EXIT=0; STEP fmt EXIT=0; STEP deny EXIT=0; STEP wasm32 EXIT=0; STEP core-paths-selftest EXIT=0; STEP enforcement-tests EXIT=0; literal last line `GATE EXIT=0`. stderr empty, no refusal. `$fg` removed afterwards.
- **Scope.** `git diff acc81c77...7be2bf15 --stat` lists `nucleus/v3/docs/S-STORE.md` and the earlier REWORK verdict `nucleus/v3/verdicts/S-1-F7-F2-F2-F4b.md` (the gate's own file). The rework's six commits change only S-STORE §5.3 (two bullets), §6.8 "Root names" (the one inserted sentence), §13 step 2's map-phase bullet (one inserted sentence) and the changelog. All of it is within Touch only and Limits 2, 7 and 16, as the amendment says.
- **Acceptance.** All 59 greps (G1–G59) were run on a fresh `--no-local` clone checked out at 7be2bf15, with `nucleus/v3-scaffold` at acc81c77: every one passes. On the scaffold, every G fails except G36 and G38.

## Is the amendment faithful to R1–R4?

I compared the card against the pre-amendment copy `~/.cache/nv3-review/S-1-F7-F2-F2-F4b.pre-rework-amend.local.md`, line by line.

- **R1.** The ruling's term "plus the waiters queued ahead of it" is restored. The flip clause now counts "the holds ahead of it on that latch, each bounded as above (at most one per thread), plus the bounded bypass", which is what R1 asked for. G14 pins both, and a negative grep pins out "by at most one such hold".
- **R2.** G58 pins GM1–GM4 in place, including the clause that G43 lost. G59 pins GM3 a second time, plus the new R4c sentence.
- **R3.** The A-3c quote now names all eight variants inside the quote. The A-4c "Replace" target string occurs verbatim in PLAN-v3 row A-4c (line 875), so the replacement applies. "After A-3c" is stated.
- **R4a, R4b and R4d.** These are carried as asked. B78's three holders are each pinned, and so are GM5, GM6 and GM8.
- **Re-anchors.** None is loosened. The new G7, G14, G15 and G27 each contain the old pattern's literal text, plus more text and, for G14 and G27, a negative grep. G13's old literal "a thread that holds a latch never waits for an unpin" changed only because R4a corrects it. The new pattern pins the whole corrected passage, and its negative grep pins out "therefore holds no latch". The `.*` spans in G14 cover the same text as before. X3 and X4 show that two clauses inside them were never pinned, which was true before the rework too (finding R3 below).

The amendment answers R1–R4 faithfully. The two blocking findings below come from what the rework's new text says, not from how faithfully it was carried.

## Substance

- **R1, the arrival-time bound.** I hold this to be true for every waiter, under §5.3's policy as defined, that is, per waiter: "a waiter is overtaken by at most a bounded number of later requests, or for a bounded time".
  - A waiter `w` waits for the holders present at its arrival and for the waiters already queued then. Every request granted ahead of it that arrived later is, by definition, a bypass of `w`, so a stream of new arrivals is covered by `w`'s own bypass bound, including arrivals that bypass the waiters ahead of `w`.
  - Under `try_latch` and the evictor, nothing changes: `try_latch` cannot succeed over a queued exclusive waiter.
  - "At most one per thread" holds, because there is no recursive latch.
  - The policy's "bounded time" arm is satisfied by `parking_lot`'s eventual fairness only if the per-waiter time bound covers the bypass periods of the waiters ahead too. That is A-3b's existing check against the pinned latch type, and it changes no text.
- **What is still false in the bound.** The bound is false for a waiter behind a pool-floor holder. That is R1 below.
- **R4b.** All three listed holders hold the root briefly and take only a MapLeaf latch or map latches under it. A directory change waits on none of them, so DropTree's blocking wait stays acyclic. "Taken without blocking" keeps `try_latch` out of the paragraph, as G26 requires.
- **R4c, crash effects.** The epoch copy is memory only. Meta `k` follows the map phase, so a crash anywhere in the window loses only image `k`, which no open uses. After the map phase takes `M`, the frame source cannot produce a new epoch-`k` copy of `M`, because `M` counts as written. No crash effect follows.
- **R4c, concurrency.** That part is R2 below.

## Rework (precise)

**R1 (blocking): §5.3's fairness bound no longer covers every hold, now that §5.3 says a latch holder may wait for an unpin.**
- **The two bullets now disagree.**
  - R4a's new text in "Why latching cannot deadlock" says: "A gated operation past its first mutation cannot restart (§5.6 "Restart"), so it may wait for an unpin while it holds its one tree's latches."
  - The next bullet, "The fairness bound", still says "each hold is bounded by one call's work on one tree, which includes no transaction wait", and lists a batch fill, a descent step, a writer's critical section and "a frame acquisition's victim write".
- **How the holder's wait escapes the bound.** A wait for an unpin is a wait on other threads' calls, and those threads may be blocked on latches of another tree. It is finite only through §5.4's count, and it is not "one call's work on one tree". Example: a gated insert on a leaf root that has done its split, and so is past its first mutation, then builds a chain (§6.7). Each new overflow page needs a frame. At the pool floor, the insert waits for an unpin while it holds the root exclusively.
- **Consequence.** A waiter queued behind that hold, and through "each bounded as above" a pending flip, is delayed by a term the bound does not state. The pre-rework text hid this by falsely saying that a latch holder never waits for an unpin, and R4a fixed only one of the two bullets. A-3b sizes `t_root_latch_fair`'s budget from this bound, so the user's question has an answer: the bound is not yet true for every waiter.
- **Fix (P amendment).** Add the pool-floor unpin wait of a gated operation past its first mutation as a hold term. Say that it is finite through §5.4 (S-1-F7-F2-F2-F2) and bounded by the pinning threads' own calls, not by one call on one tree. Alternatively, state explicitly that A-3b's budget excludes the pool floor and why. Re-anchor G14 on the new term and add a text mutant that drops it.

**R2 (blocking): R4c puts a still-latched frame into the `R_EPOCH` reserve.**
- **The text.** "It takes a map page's epoch copy under the copy's latch and releases the copy to the `R_EPOCH` reserve before it releases that latch". Between those two steps the frame is in the reserve while the map phase holds its latch.
- **Who can see it.** Before this sentence, a frame in the reserve was never latched. A claim reuses its victim's frame directly and does not return it to the reserve. Now a `write_guard` that draws an epoch-copy frame from the reserve can draw this one.
- **What happens.** The writer must latch the frame to write the copy's bytes (no `unsafe`, N9). It blocks on the map phase's latch while it holds the page's exclusive latch, and in the cross variant also a MapLeaf's latch. That breaks two rules:
  - §5.6 "Reserve": "a writer never waits holding a page latch".
  - §5.3's order: no blocking acquisition after a map latch, which ranks last. Epoch-copy frames have no rank at all.
  
  `t_latch_scope`'s tracker, which "asserts rank at every blocking acquisition", would flag it. There is no cycle, because the map phase holds that one latch and waits for nothing. But the deadlock argument and the reserve bound no longer cover this edge as written.
- **The reverse direction is not affected.** No claim can evict the copy after the map phase has taken it, in either order, provided the copy is detached under the latch.
- **Fix (P amendment).**
  - Under the copy's latch, the map phase detaches the copy, so that it is no longer that page's epoch copy and no longer claimable.
  - It then releases the latch, and only then returns the frame to the reserve.
  - State that a claim's `try_latch`, once granted, re-checks that the frame is still an epoch copy before it evicts it. This was already implicit in EXHAUST-DOUBLE-EVICT, and it is needed in either order, because a claimant may hold a stale candidate.
  - Re-anchor G59 and replace B79's companion with a mutant that returns the frame before unlatching.

**R3 (minor, same amendment).** In "The fairness bound", "which includes no transaction wait" (X3) and "a descent step, CPU-bounded for a reader (above)" (X4) sit inside G14's `.*` and are unpinned. This is not a regression, because the old G14 had the same span. Pin them while G14 is being re-anchored for R1.

## Revert

Mutants were applied only to a scratch clone, which was restored after each run (`git status` clean). The worker's worktree is untouched apart from this verdict.
