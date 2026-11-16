Card: X-5c-PINS-2
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (289123f3-9375-40dd-b074-9c0eff6b54c8, 2026-11-11)
Date: 2026-11-11
Gate-exit: GATE EXIT=0
Commit: a95e50f3fe8937e2805820999e2fd4e1cd03b347
Mutants: OWN-W7-END-LATE -> x5r56_w7_window_edges_hold, OWN-W7-START-EARLY -> UNCAUGHT (no observable effect, as the card allows), M-CWIN-W7-NONE -> cwin_round_trips_every_window_label, OWN-SZI-NOFOLD -> UNCAUGHT (no pin; NOT equivalent at FINAL, see finding 3 and F1), OWN-TUNING-ARM -> x5_r44_mp_pressure_runs_its_own_tuning, M-SEEDS-SKIPPED -> x5_r44_malformed_seeds_refuse_in_every_run_form, OWN-GUARD-WAIT-IGNORED -> x5_guard.sh --self-test wait-injectable-timeout, OWN-GUARD-NO-STEAL -> x5_guard.sh --self-test busy-in-nice-irq-softirq-steal, M-GUARD-PREWAIT-LOAD -> x5_guard.sh --self-test prewait-load-read-after-wait, M-BAND-SUBSHELL -> x5_cpu_term.sh --self-test band-logged-term, OWN-FIRSTONLY -> es1_two_publications_each_checked, G-SEEDS-LOOSE-LO -> x5_r44_malformed_seeds_refuse_in_every_run_form, G-REQ-OFF-BY-ONE -> es1_two_publications_each_checked, G-GUARD-FIELD-ORDER -> x5_guard.sh --self-test line-shape-loads-in-order, G-GUARD-NO-REREAD -> x5_guard.sh --self-test prewait-load-read-after-wait, G-GUARD-CAP-IGNORED -> x5_guard.sh --self-test (hangs: the step-0 fixture never times out), G-BAND-SITE-REVERT -> UNCAUGHT (measure() call site, pinned by reading)
Followups: X-5c-PINS-2-F1, X-5c-PINS-2-F2

## Plain statement

Six of the seven items are done as their source verdicts asked. The `src/campaign.rs` change is the two `cwin` read-back arms and the malformed-`--seeds` refusal, nothing else. Neither can move a digest or a judgement. The runner's main 10k at the tip is **byte-identical** to the chain head's baseline: both files have sha256 `cb947c19…ae45`. My own comparator run reads `DIGESTS_MOVED 0`. A 207-seed sample re-run at the tip reproduces every seed record exactly.

The worker's ESCALATE is **overruled**: OWN-SZI-NOFOLD is not equivalent. It is unobservable at BASE, which the card specified because FINAL equalled BASE when the follow-up was written. At FINAL, which the main set runs, it flips real rounds: seed 249 round 1 reads `szi` false on the real code and true under the mutant. The pin moves to F1. Item 3 of X-5c-F4-F2-F2 is carried as F2. Neither follow-up moves a digest, and neither blocks E4-CLASS-1.

## How the gate was run

- **Fresh gate.** I extracted the scaffold's `fresh-gate.sh` through the `--no-local` clone, at scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`. Its lines were:
  - `fresh-gate: commit a95e50f3fe8937e2805820999e2fd4e1cd03b347`
  - `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`
  - `fresh-gate: checks passed (4535 files hashed in 4.71 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`

  Every STEP was `EXIT=0`, and the last line was `GATE EXIT=0`. stderr was empty.
- **Scope.** Against the Base `ec9f84a6`, six files change, all on the Touch-only list. Tests and tools add lines only, apart from the self-test expectation strings that move to the new GUARD shape and the band cases that move to the new contract. No test or assertion was removed or weakened. The `nucleus/v3-scaffold...HEAD` stat is the whole stacked chain, as expected.
- **Scratch.** Every build used a `git archive` export of `a95e50f3` (v2 included for the path deps). Each Rust mutant had its own copy and its own `CARGO_TARGET_DIR`, and each tool mutant had its own script copy. The worktree held no mutant and was clean before this file was written.
- **Hygiene.** Per the owner's 2026-11-11 process change, I did not re-run the full 10k. I ran only probe ranges below 10000 and five single-seed probes at FINAL (three on the mutant build). No `verify`, no `--set`, no `--probe`, and no seed of 10000 or above.

## Findings

**1. The seven items against their source verdicts.**
- (1) **F5-F1, done.** At share 1000, the w7 edge pin counts 78 `w7` rounds and 0 `w7-` rounds over the uniform seeds of `0..400`, matching the verdict's 78.
  - OWN-W7-END-LATE is caught at seed 239 (`uniform@y:182`).
  - OWN-W7-START-EARLY still survives, which the card allows.
- (2) **F5-F2, done.** The two arms are in, and the round trip covers all six labels and `None`.
- (3) **F4-F4-F1 (a)–(d), done.**
  - `load_before_wait` is read before the wait and `load_at_start` after it.
  - The load source and the step and cap are injectable, and the decision rule and its defaults are unchanged.
  - The nice/irq/softirq/steal fixture is in.
  - `band_passes` now returns its answer in globals.
  - The stale promise is replaced by a pointer.
  - The arithmetic and the exit rules are unchanged.
- (4) **F4-F2-F1, done.** REFRESH 5's and REFRESH 9's text replaces the transcriptions, anomaly (b) gains its closing sentence, and the `szi` sentence is narrowed.
- (5) **F4-F2-F2:**
  - The OWN-TUNING-ARM pin is done. Attempt 3's switch from `ptr::eq` to a value comparison is sound: the `assert_ne!(MP_PRESSURE, FINAL)` precondition keeps the pin able to tell them apart, and the mutant is caught.
  - The OWN-SZI-NOFOLD pin is **not done**. See finding 3.
- (6) **F4-F2-F3, done.** `cmd_run` refuses before `run_form`, and every form refuses.
- (7) **E4-SILENT-1-F1, done.** At kmax 3, `n = 6` refuses at open at both seeds for all three kinds, and no cell reads `O-3(d)`. OWN-FIRSTONLY fails with exactly the silent shape: seed 4242, n=6, tree 6.

**2. No behaviour change: confirmed.**
- **The `cwin` arms** are in the parser only. `verify` compares digests, `q4i`, `szq`, `szi` and `rfs`, never `cwin`. The writer and `x5r52_compare.py` (unchanged since `ec9f84a6`) never read the parsed label.
- **The `--seeds` refusal** returns exit 2 before `run_form`. It changes nothing for any well-formed value. The only invocations that newly refuse are malformed ones, which the card asked to refuse: for example `--seeds 5`, and `--seeds abc..10000`, which used to parse as `0..10000`.
- `Tuning::BASE`, `Tuning::FINAL` and `Tuning::MP_PRESSURE` are untouched, and no `src/` file other than `campaign.rs` changes.
- **The runner's run.** The run is attempt 3's, at `a95e50f3`. It ran 2888 s and exited 0, and its census is `arm_seeds=[1069,3669]`.
  - `pins2.main.json`, both under `$CARGO_TARGET_DIR/x5pins2` and in the runlogs copy, hashes to `cb947c1950ee0f5a35fff45f572630328a0d34e2eb2b5fcbbecc1d2cc1b6ae45`. That equals the FINAL baseline's recorded sha.
  - The runlogs `.out` and `.guard` are byte-equal to the runner's originals.
  - My comparator run (`--no-twin-exemption --max-twin-failing 0`) exited 0 with `DIGESTS_MOVED 0 FAILING_ROUNDS 11 MONITOR 0`. Every list is empty, and `MPA_CLASS` reads base 6, tip 6.
  - The GUARD line reads `wait=timeout … foreign_pct=21.18 verdict=VOID`. The host was shared, so the run's wall time is not evidence. Its digests are deterministic and byte-identical, so the VOID does not touch this card's claim.
- **Sample.** I re-ran 207 seeds at the tip as probe ranges: 14 blocks of 13 spread over `0..9300`, `9987..9999`, and singly 1069, 3669 and the 11 failing seeds (1257, 2927, 3988, 5168, 5808, 7177, 7528, 7727, 8657, 8728 and 9328). All 207 full seed records, not only the digests, are equal to the runner's file.

**3. The ESCALATE rulings.**
- **OWN-SZI-NOFOLD: not equivalent. The worker's ESCALATE is overruled and the pin is carried as F1.**
  - The worker's BASE sweep is credible: 141 `szq` rounds and 58 `szi` rounds on both builds.
  - But the card fixed BASE because FINAL equalled BASE at F2's tip. Since X-5c-F4-F3, FINAL differs.
  - In the runner's FINAL main file, 271 rounds read `szq` true and `szi` false with a hook crash at After0 through After4.
  - I built the mutant on a scratch export and ran seeds 249, 286 and 301 at FINAL. The digests were unchanged, and `szi` flipped false→true on seed 249 round 1, seed 286 round 1 and seed 301 round 2.
  - So the conjunct governs the judged SZI row: 777 at the chain head, and the mutant would add up to about 271. The real code is correct by reading, so this is a pin gap, not a defect. The worker correctly obeyed Work 3's STOP rule.
- **X-5c-F4-F2-F2 item 3: carried as F2.** This is the literal-flag versus hook-carrier comparison over `0..9999` at FINAL. It needs a scratch-instrumented full set, which is the orchestrator's work, not a worker's. Gap rounds could only move `szi` counts, not digests. With SZI at 777 against a floor of 300, it cannot flip the row, but the README's narrowed sentence stays a BASE-only measurement until F2 runs.

**4. Mutants.**
- All eleven card rows were re-applied. OWN-W7-START-EARLY survives, which the card allows. OWN-SZI-NOFOLD survives, ruled under finding 3.
- I added seven of my own:
  - G-SEEDS-LOOSE-LO: a loose lower half, so `..7` reads as `0..7`.
  - G-REQ-OFF-BY-ONE: `id > ckpt_id + 1`. It is caught at seed 4242 n=5, a cell outside kmax 3.
  - G-GUARD-FIELD-ORDER.
  - G-GUARD-NO-REREAD.
  - G-GUARD-CAP-IGNORED: the self-test hangs instead of failing, so it is caught but not cleanly.
  - G-BAND-SITE-REVERT: the old `np=$(band_passes)` plus the echo, restored at `measure()`'s site. **It survives**, because the self-test drives `band_decide` and never `measure()`. The site now calls `band_decide` in the parent shell and the only BAND echo lives there. I re-applied M-BAND-SUBSHELL inside `band_decide`, and it is caught. So the measuring site is pinned by reading. Low severity; no card.
- **Observation, no card.** The `--seeds` test's command-level half calls `campaign::main`. A regression at `cmd_run` (the pure reader right but the refusal dropped) would start a full `main` run inside `cargo test` instead of failing fast. Today the pure half aborts first. A future edit should keep the pure assertions ahead of the loop.

**5. Fresh gate.** Green, as above.

## Readiness

- **E4-CLASS-1:** ready now.
- **The final run:** after F1 and F2. Both are cheap and neither moves a digest. F1 is a test-only pin. F2 is one orchestrator-run instrumented scratch pass over `0..9999`, the one long run it needs. The baseline of record for the final run's compare stays `X-5c-F4-F5.head.main.json` (`cb947c19…ae45`). This card's run reproduces it byte for byte.

## Follow-ups

**X-5c-PINS-2-F1 (the OWN-SZI-NOFOLD pin at FINAL; test-only; before the final run).** Add a literal pin in `tests/workload.rs`'s `x5_r39_*` family: run seed 249 at `Tuning::FINAL` and assert on round 1. The round must read `szq` true, have its hook crash inside the `ckpt_unflipped` window (After1), and read `szi` false. Assert its digest against a new literal. The doc comment should name the conjunct and say why BASE could not hold the pin (equivalent over `0..10000` at BASE, this card's sweep). The must-fail row is OWN-SZI-NOFOLD (`rr.szi = rr.szq && ckpt_unflipped(..)`), which reads `szi` true there. Seeds 286 round 1 and 301 round 2 are alternates. If the workload exposes `ckpt_closed_dirty` and `four_f` to the test, also assert `ckpt_closed_dirty < 4·F`. No `src/` change.

**X-5c-PINS-2-F2 (X-5c-F4-F2-F2 item 3, carried; orchestrator measurement; before the final run).** Make a scratch build at the chain tip with the card's literal flag added: cleared at `ckpt:post5`, where step 5 sets `meta_durable_k`, and on a non-crash abort. Compare every round of `0..9999` at FINAL against the hook-position carrier. The gap is after `meta_durable_k` and before `After5`. Record the number of differing rounds and of rounds in that gap as probe evidence, with the output file's sha. Zero differences confirms the README's narrowed `szi` sentence at FINAL. Any difference goes to P before the final run. It cannot move a digest, but it would move the SZI count.
