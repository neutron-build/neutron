Card: A-3b-F1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session 3200b239-4c0f-40b1-a4b0-43b00b3c335f (T3 Code review sub-agent, P gate of record for A-3b-F1, requested by T3 thread 45584af2-e651-490e-862c-3402320cffa3)
Date: 2026-11-27
Gate-exit: GATE EXIT=0
Commit: 61a0073059260f5400115437d5dd74127370c825
Mutants: 1 NOTIFY-ONE (A-3b row 19) -> harness_unheld_two_waiters_starved (Deadlock at execution 0, PCT; kill_evidence PCT 932/1000 first 0, RANDOM 380/1000 first 2, RANDOM seed 0xA4C0_0003 417/1000 first 4, each replayed, share 5% met) | 2 SHARED-AS-EXCLUSIVE (A-3b O8) -> frame_latch_pinned (assertion :1253), t_root_latch_fair_reader_stream (assertion :856, overlapped in 0 of 1000) | G1 TRY-SHARED-AS-EXCLUSIVE (gate's, the card's suggestion) -> frame_latch_pinned (assertion :1253); reader stream passes, as the card predicted | G2 WAIT-NO-DECREMENT (gate's) -> harness_unheld_two_waiters_starved (Violation at execution 0, PCT: unpin_waiters 2 at the end) | G3 BATCH-OF-ONE (gate's) -> UNCAUGHT by both F2 tests (overlapped in 995 of 1000) and by latch; buf_latch fails it only as harness_unheld_pin_drop NotExhausted at 99999, not a pin (A-3b-F1-F2) | G4 NOTIFY-TWO (gate's) -> UNCAUGHT, equivalent in this fixture (two waiters at most); kill_evidence answers no execution failed at all three bounds
Followups: A-3b-F1-F1, A-3b-F1-F2

## Gate run

The scaffold's `fresh-gate.sh` and `git-env.sh`, extracted per GATE-PROMPT step 1 through a shallow `--no-local` clone (printed scaffold id `9821393286856bbe69e9354514e10a7a3841122f`; no EXTRACT FAILED), run once on the full commit id, first run, at 1-minute load 1.15:

```
fresh-gate: commit 61a0073059260f5400115437d5dd74127370c825
fresh-gate: base 9821393286856bbe69e9354514e10a7a3841122f
fresh-gate: checks passed (4943 files hashed in 1.50 s)
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

72 `test result:` lines, every one `ok`; `buf_latch` 13 passed in 22.23 s. stderr was empty.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: one file, `nucleus/v3/crates/nucleus-store/tests/buf_latch.rs` (+173/−15), the card's only Touch only entry. Nothing under `src/`, `docs/`, `scripts/`, `tools/` or `verdicts/`. The branch is one commit on the scaffold's tip `98213932`, so it merges as it stands.

**Acceptance lines 1 to 12**, run by the gate in the worktree from the card's text (cargo with the gate's own target directory; the status stayed clean): every line exits 0. Line 2: `13 passed … finished in 22.63s`; line 3: `1 passed … 0.89s`; line 4: `0.00s`; line 5: `0.49s`. Line 13 is the fresh gate above.

**Mutants.** Each mutant was planted in its own fresh copy of a `git archive` extract of the tip (`nucleus/v3` and `nucleus/v2`, new mtimes), with its own target directory, by exact-text replacement checked to occur once, each test under `timeout 600`. No catch rested on a hang, and no mutant hung. The worktree was never edited. The pristine extract, with three throwaway `kill_evidence` tests appended in the scratch copy only, passed all 16 tests (21.93 s). On the correct build, `kill_evidence` on `two_waiters_starved::<Shuttle>` answers `no execution failed` at PCT (seed 2764046338), RANDOM (seed 2764046337) and RANDOM seed `0xA4C0_0003` (2764046339): the clean build fails none of 1,000 executions at each bound. With NOTIFY-ONE planted, the same three tests print, verbatim:

```
pct seed 2764046338 depth 3 executions 1000 max_steps 10000: first failing execution 0 (deadlock) replayed; failing 932 of 1000 (93.2%), V/D/S/P 0/932/0/0; share 5%: met
random seed 2764046337 depth - executions 1000 max_steps 10000: first failing execution 2 (deadlock) replayed; failing 380 of 1000 (38.0%), V/D/S/P 0/380/0/0; share 5%: met
random seed 2764046339 depth - executions 1000 max_steps 10000: first failing execution 4 (deadlock) replayed; failing 417 of 1000 (41.7%), V/D/S/P 0/417/0/0; share 5%: met
```

These are the verdict's numbers and the worker's lines exactly, and each run took under 0.5 s. The committed test fails under the mutant at `buf_latch.rs:1803`: `harness_unheld_two_waiters_starved, under PCT: deadlock at execution 0 … blocked tasks: [main-thread (task main-thread(0)), <unknown> (task main-thread(2))]`.

**The worker's log** (`logs/A-3b-F1.attempt1.jsonl`) was opened and checked: each claimed output (the three evidence lines, both row-2 failures, the 164.20 s run and the printed counts) appears in the tool output of the command the RESULT names.

## Findings

Nothing blocks the merge. The work is F1 and F2 exactly as the card words them, and both A-3b survivors now have a killer.

1. **The work matches the card (a).** The diff is the card's six one forms and three doc New blocks, byte for byte, plus one blank line. The fixture is the verdict's:
   - `fixture::<H>(1, 3)` over `NoIoFile`;
   - `reserve` and `create(…, X, Leaf, 1)` give `Created(page)`, then `mark_dirty(1)` and `release()`, keeping the pin;
   - two `H::spawn`s of `starved_or_reserved`, which accepts `Ok(res)` (dropped) or `Err(PoolStarved)` and fails anything else;
   - `spin_until` on `unpin_waiters >= 2`, then `drop(held)` and two `H::join(..)??`;
   - the end condition is `unpin_waiters != 0` → `Err`, and nothing more.

   The F2 pins are as the verdict words them: the try between `reader`'s stamps and its release, released first (OP-4), and the overlap count read before `judged` consumes `all`. `readers_overlap` skips `all[0]` (main) and `all[1]` (W), which is how `reader_stream` builds `all`. The imports are untouched: `AtomicUsize` and `Ordering` were already imported at `:63`. There is no `unsafe`, no non-test `unwrap` or `panic!`, no clock and no `allow`. The doc edits are true: "for the rest" no longer counts the sampled test among the exhaustive ones, and the index names the new test and both mutants.
2. **The sampled catch is sound under S-STORE §15 (c).** The gate was shown everything §15 requires:
   - the exploration (PCT and RANDOM presets) and the step budget (10,000);
   - the clean build passing every execution, at all three bounds;
   - the mutant's first failing execution by seed and index, replayed, with the failing share: 93.2%, 38.0% and 41.7%, all far above the 5% share.

   So the kill is not fragile, and no hook-steered form is owed now. It is sampled, so it is evidence and not proof, and it can drift when A-3c adds scheduling points or write-back. Hence A-3b-F1-F1.
3. **G3 BATCH-OF-ONE survives (`head_batch` returns at most 1).** R3's batch grant has no direct pin. Under it the readers are still granted beside each other one at a time, so two shared holds still coexist and overlap (995 of 1,000). `tests/latch.rs` (20 tests) passes. `buf_latch` fails it only because `harness_unheld_pin_drop`'s state space grows past its cap (`NotExhausted at execution 99999`), which is not a pin. This is outside F2's ask ("two shared holds coexist"), so it does not count against this card: A-3b-F1-F2.
4. **G4 NOTIFY-TWO (two `notify_one`s) is equivalent here.** The pool has 3 callers and main holds the pin, so at most two acquisitions wait. The test pins "wakes at least two", not "wakes all". Recorded as the test's scope. No follow-up: the mutant is contrived, and a third waiter would need a larger fixture that nothing requires.

## Rulings on the card

- **OP-1 (the spin, not the gate): accepted.** It is the verdict gate's own probe, needs no new `Gate`, and has the stronger PCT share (93.2% against 20.7%).
- **OP-2 (names): accepted.** They are bound by Acceptance lines 9 to 11 and follow the file's `harness_unheld_pin_drop`/`unheld_pin_drop` pattern.
- **OP-3 (counter across executions): accepted** as the letter of "in some execution". The correct build overlaps in 1,000 of 1,000, so the every-execution form would also hold. It would kill G3 in only 5 of 1,000, below the share, so it adds no sound kill and is not owed.
- **OP-4 (try released first): accepted.** The Base's stamp checks stand unchanged.
- **OP-5 (module-doc edits): accepted and required.** Without them `:8-9` would be false. The New text is true.
- **OP-6 (throwaway evidence tests): accepted.** On a correct build `kill_evidence` answers `no execution failed`, which the gate confirmed at all three bounds. The "time unmeasured" concern is answered: under 0.5 s per bound.
- **OP-7 (A-3c merging first): moot for this tip.** The Base is pre-A-3c (no write-back under `src/buf/`). The obligation moves to A-3b-F1-F1.
- **OP-8 (kills not run when drafted): now run.** Both rows are caught as predicted, and the card's suggested TRY-SHARED-AS-EXCLUSIVE behaves as it predicted.
- **The drafter's three readings: all accepted.**
  - "In some execution" needs a counter outside the model, read after `check` returns.
  - The presets cannot be picked from the command line, so the second seed exists only in throwaway tests.
  - A committed test cannot print the `kill_evidence` lines on a correct build.

## Limit 9 (d)

Met. In the gate's debug profile on an idle host:
- `buf_latch` takes 22.23 s in the fresh gate, 22.63 s in the gate's Acceptance run and 21.93 s with the evidence tests.
- The new test takes 0.89 s for 2,000 executions, and the reader stream 0.49 s.
- The longest test is still `harness_unheld_pin_drop` (about 22 s), which this card does not touch.

The worker's 164.20 s run was at a load of 23.45 on 16 cores (user time 4m34s against 2m47s real). That is host contention, not the card. The budget is judged at gate conditions, and the binary is about 2.7× under its 60 s there. Its sensitivity to load sits in `harness_unheld_pin_drop`, whose margin A-3b-F3 already owns. No new follow-up.

## The worker's judgment calls (e)

All four are accepted:
1. The blank line: rustfmt-neutral, and inside a range Acceptance line 8 excludes.
2. The throwaway `use` line: removed, as Acceptance lines 7 and 8 confirm.
3. The reader stream run twice under the mutant: to capture a truncated printed line, not a flake hunt, so Limit 10 is not touched.
4. No other binary run under the mutants: the card names only these tests.

The worker's anomaly 4 (the three Read-list cards are absent from the worktree) is expected: cards are `.local.md` and never committed (PLAN-v3 §7.3 rule 5). Anomaly 1 (the `packed-refs.lock` message) left the commit and the tree intact.

## For the record (f)

- A-3b's row 19 NOTIFY-ONE now has its killer: `harness_unheld_two_waiters_starved`, a `Deadlock` at PCT execution 0, with the three `kill_evidence` lines above. A-3b's card correction 6 is now true of the code.
- A-3b's O8 SHARED-AS-EXCLUSIVE is caught by `frame_latch_pinned` (the refused try, `:1253`) and by `t_root_latch_fair_reader_stream` (overlapped in 0 of 1,000, `:856`).
- The QUEUE rows A-3b-F1 and A-3b-F2 are both discharged by this card.
- The correct build fails no execution of the new scenario at PCT, RANDOM or RANDOM seed `0xA4C0_0003`.

## Follow-ups

- **A-3b-F1-F1 (gate obligation on A-3c, or on whichever of A-3c and this card merges second; no code unless the numbers move).**
  - A-3c adds dirty write-back, a notify on a cleaning write (its Work 8.3), and possibly scheduling points in the acquisition. Any of these can change why the second waiter stays starved, and the shares of a sampled kill.
  - On the tip that holds both, the gate of the second merge reads that `fixture` still builds a pool without write-back (A-3c Work 8.2: "A pool without write-back keeps A-3b's behaviour").
  - With NOTIFY-ONE planted, it re-runs the three `kill_evidence` lines on `two_waiters_starved::<Shuttle>` at PCT, RANDOM and RANDOM seed `0xA4C0_0003`, and records them beside this verdict's 932/380/417.
  - If the fixture gains write-back, the woken waiter no longer ends starved, or any line falls below the 5% share or is not replayed, the scenario keeps a starved second waiter some other way (for example, a page file that refuses the write-back) or gains a hook-steered form. Never lower a bound to fit.
- **A-3b-F1-F2 (tests, for the next card that edits `src/latch.rs`, A-4c, or `tests/latch.rs`).**
  - R3's batch grant (the head's whole run of shared requests granted at one instant, with W2's one tick per member) has no direct pin.
  - Mutant BATCH-OF-ONE (`LatchCore::head_batch` returns `len.min(1)` in place of `len`) passes `tests/latch.rs` and both F2 tests (overlap in 995 of 1,000). `buf_latch` fails it only as `harness_unheld_pin_drop` `NotExhausted` at execution 99999, a state-space accident and not a check.
  - Pin it directly: for example, in a latch-level scenario where k shared requests queue behind an exclusive holder, the holder's release grants all k with consecutive grant stamps before any of them collects. Then show BATCH-OF-ONE fails it by a check, not by a cap.
