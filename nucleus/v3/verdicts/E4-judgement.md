# E4 judgement

**From:** P (the independent gate role). **Session:** T3 review sub-agent, Claude Code `claude-opus-5-5` (b446fa14-5471-4946-8ccd-310af50a6487). **Date:** 2026-11-14.
**For:** the owner. This page judges E4 itself. The card gate that came first is in `~/.cache/nv3-review/X-5c-F4-F7.verdict.md`.

## The answer

**E4 is passed.** That holds under your two rulings of 2026-11-14, and not without them.

Each of the three seed sets now reads "every failing round is a ruled class" (exit 0):

| Set | Its file of record | Reading | Classes 1 / 2 / 3 |
|---|---|---|---|
| main (seeds 0–9999) | the final run's file, `cb947c19…ae45` | **exit 0, 0 unmatched** | 2 / 4 / 5 |
| held-out (seeds 10000–19999) | the re-check run under the fixed test harness, `f572e1da…6a` | **exit 0, 0 unmatched** | 5 / 3 / 6 |
| MP-pressure (2,000 seeds) | the final run's file, `75d719b8…3ce7` | **exit 0, 0 unmatched** | 4 / 8 / 4 |

Here is what the three classes mean in plain words. Each is a round with disk damage in which **the database either refused to open, or opened correctly and reported the damage.**
- **Classes 1 and 2:** the store refused to open and handed no data to anyone. This is your MP-A ruling, "a documented, detected refusal".
- **Class 3:** the integrity checker over-reported a whole subtree, and the per-round diagnostic proved it. This is your 2026-11-11 ruling.

**No round anywhere served wrong data silently.** E4's rule is that such a round is never a pass, and the table has no class for one.

## What the pass rests on, item by item

1. **The final run.** It ran at commit T `cb2aca0d`, on 11-13/14: one attempt, one boot, an idle host, every set inside its time box.
   - Every set and every `verify` exited 0.
   - Main met every row and both of your bars (Q4 1,713 and Q4-I 1,921, each against 1,500).
   - The planted Joint and Slot faults were caught. CPU cost was +55.44%, against a limit of +60%.
   - MP-pressure met its row: SZC-MP 148, against 100.
   - Main's and MP-pressure's files are used exactly as that run wrote them. Each hash matches its provenance record.
2. **X-5c-F4-F6's pin.** Held-out has three seeds that exercise a rare test-harness path, and all three read clean. The rule asked for one test to lock that in (seed 10669) before E4 is judged.
   - That test was gated MERGE, and it still passes at the card's commit.
   - The census line is unchanged in the re-check run: 3 live arms, 0 stale reads.
3. **The test-harness fix and its re-check, for held-out 17948.** You ruled the failing "twin" at seed 17948 a test-harness defect.
   - The twin is a replay of a damaged round with the damage removed. In this round it had inherited earlier damage, so it was not damage-free.
   - Card X-5c-F4-F7 fixed this by healing the inherited damage on the twin's copy of the disk, and it re-ran both judged sets.
   - **Main came out byte-identical. Held-out changed in seed 17948 only:** its twin now passes. Nothing else changed in any of the 10,000 records, including every real round of 17948.
   - My gate (MERGE-WITH-FOLLOWUPS) checked this independently, by re-running 38 seeds and instrumenting 18 of them.
4. **The table amendment for MP-pressure 26018 and 26958. It is a post-data amendment, and is named as one here.** These two rounds refused to open and gave no caller any data, which are the two facts MP-A rests on. They missed the frozen table only on a bookkeeping count, `rlen`, which P's diagnostics showed has nothing to do with the refusal.
   - You widened the narrow form to any `rlen`.
   - The table changed in exactly those two values: frozen `55ae4734…1158`, amended `a05534c7…0fdea`.
   - Re-matched, exactly those two rounds moved, into classes 1 and 2. Nothing else moved in any set.
5. **Two re-check runs whose load guards read VOID.** Another session's builds loaded the host during the re-check runs (`foreign_pct` 19.42 and 26.79). **That does not affect this judgement.**
   - The runs are used only for their results, compared byte for byte. Every seed's result is a pure function of the seed, and the main re-run is byte-identical to the original under that same load.
   - Every timing claim still rests on the final run, whose guards were clean.

## Caveats, stated plainly

- **Two of the readings changed after the data was seen, by your rulings.** Under the definitions frozen before launch, held-out read exit 1 (17948) and MP-pressure read exit 1 (26018, 26958). Both cases were root-caused before you ruled: one is a test-harness defect, and the other is a count that is not causal. The safety facts behind every class are unchanged: a refused open, or a correct open with the damage reported, and no caller data on any refusal. Still, the pass is "pass with two post-data rulings". It is not "pass on the frozen definition".
- **Held-out is no longer unseen.**
  - The final run's held-out seeds were untriaged at launch, which the final gate checked.
  - Since then, the final gate replayed 28 of them, P's diagnostics replayed all 3,000 held-out MP and MW seeds, and this gate replayed 26 more.
  - The held-out file now used is a re-run of the same seeds under the fixed test harness, made after those replays. It is not a fresh draw. Per your ruling, no fresh held-out range was used.
- **The 10669 pin relies on a diagnostic counter.** That test sees the fault it guards against only through a stale-read counter. A version of the bug that also zeroed the counter would go unseen (X-5c-F4-F6 gate, finding 1). That gate inspected the counter, and it is honest.
- **The harness fix has limits, found by my gate. None of them touched E4's data.**
  - **The fix's safety check can raise a false alarm.** The check after the heal can flag a disk region whose bytes are actually correct, when a later unsaved write there was lost in a simulated crash. That would be a loud seed failure, never a false pass.
  - **The heal only knows bytes the database itself once wrote.** A stray write that lands somewhere the database never wrote is left in place. In this test harness, nothing ever reads such a spot.
  - **Two safeguards have no test that pins them.**

  Measured: all 11 damaged regions in the data (on the 6 affected seeds) healed to exactly the expected bytes, and no seed reported a defect. These go to follow-up X-5c-F4-F7-F1, before the harness is reused.
- **The held-out file has no provenance record of its own.**
  - Its hash `f572e1da…6a` is recorded in my verdict, and the file is byte-identical to the test runner's own output.
  - My gate's binaries, built from the card's commit, reproduce every seed I sampled from it.
- **What E4 tests.** E4 is a prototype on a simulated disk. It shows that the recovery design (N4) holds under these faults. It does not test the production store; that is A-8a and A-9.

## Still open (none of it re-opens E4)

- **The record.** Card X-5c-F4-F7 (MERGE-WITH-FOLLOWUPS) has to merge, so that the fixed harness and the amended table live where the evidence points. Every verdict of this chain was written to `~/.cache/nv3-review/` because the branches were read-only for the gates: the final run's ESCALATE, F6 MERGE, and F7 MERGE-WITH-FOLLOWUPS. The orchestrator commits them.
- **A hash record for the held-out file.** Record `f572e1da…6a` beside it in `runlogs/`, as the provenance records do for the other two.
- **The decision block.** PLAN §4's "E4 status" decision block still has to be written, a plan-text task for Opus. It should record the outcome above, both post-data rulings, and the caveats.
- **Follow-up X-5c-F4-F7-F1**, the hardening of the harness fix. It should land before the harness is reused for A-8a and A-9.
- **P0 as a whole is not passed by this.** Safari C2 is still yours to run, and Phase A waits for all of P0 (§10 item 12). The exceptions are A-0, A-1a and A-1b, which may start now (item 13 (e)).

## Carry forward into Phase A

1. **AMCHECK-1** (your 2026-11-11 ruling): the production integrity checker must report class 3's subtree fallout as one structural finding, not as many orphans.
2. **The header design** (2026-11-10): two copies, two generations, and refuse plus explicit rollback without proof that the log is complete. Classes 1 and 2 are the "needs a backup" outage. A second header copy shrinks it but does not remove it: in 26018 and 26958 the newest snapshot itself was damaged too.
3. **The availability cost of fail-closed.** The store refused to open in 79% of main's MP rounds (the rounds with simulated page damage), 78% of held-out's and 70% of MP-pressure's (`MP_REFUSAL`, which counts recoveries that produced no store; most of these rounds pass as correct detections). That is safe and detected, but it is an outage.
   - After a detected loss that still opens (X5-R6), the store keeps running with known-damaged pages, because S-STORE has no repair path.
   - Phase A should decide on repair, scrub or salvage.
4. **The harness port (A-8a).**
   - Carry Work 2.5's twin with the heal, plus X-5c-F4-F7-F1's hardening.
   - Re-argue the "never-written bytes are never read" point for the real store, whose allocator may read the disk.
   - Pin stale supply through an outcome, not only through a counter.
5. **Fresh seeds for any future judged run.** Seeds 10000–29999 have now been seen. A future "held-out" claim needs a new range.
6. **Test time.** The crate's workload test binary takes 167–260 s in debug, against X-5c's 60 s budget. Size A-8a's tests with that in mind.

## What I could not check

- I did not re-run main's rows, bars, seeded faults or CPU. Main's file is byte-identical to the final run's, whose `verify` checked them.
- I did not replay every held-out seed under the fix. The runner's full re-check run did that, and I sampled it.
- I did not replay every MP-pressure seed under the fix (forbidden). "No re-seed in MP-pressure" rests on P's census at T: 2,342 twin rounds, all `reseed=none`. The re-seed code is unchanged at the card's commit, and my 8-seed sample plus its 4 diagnostic runs reproduce.
- I did not check the 86 damage-free re-seeds one by one under the fix. 12 were instrumented, and the heal touched nothing on any of them.
- Seeds run outside the scanned directories before launch: as the final gate recorded.

## Files

- **This gate's work:** `~/.cache/nv3-review/gate-X-5c-F4-F7/`.
  - Class match: `match/` (the `*.nodiag.out`, `*.match.out`, `*.diag/` and `*.diag-seeds` files), `class-match.log` and `scripts/class-match.sh` (the X-5c-F4 card's lines and `read_match`, verbatim).
  - Held-out rows: `rows.{final,fresh}-heldout.txt`.
  - The 10669 pin: `f6-pin.out`.
  - The sample, mutants and probes: as listed in the verdict.
- **The match ran from** a `git archive` export of the card's commit `48ac99203f97d9dd5a53b1370ed141a294c4df0c` (`match-head/`). Its `self-test` read 132 cases, 0 failed.
- **Every diagnostic run reproduced its seed's digest** (the script checks it). All 15 are byte-identical to the final gate's runs at T.
