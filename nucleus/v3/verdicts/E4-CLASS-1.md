Card: E4-CLASS-1
Class: test
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (05479320-72f5-45d8-bcd9-d310d5d947fc, 2026-11-11)
Date: 2026-11-11
Gate-exit: GATE EXIT=0
Commit: 93f2109756466b1cd365e7aeadefb6386f767314
Mutants: M-EXIT0-UNMATCHED -> unmatched-round, M-MISSING-READS-FALSE -> missing-each-field, M-MISSING-ONLY-MATCHED-CLASS -> missing-each-field, M-NO-TWIN-CHECK -> unmatched-twin-fail, M-NO-SEED-CHECK -> unmatched-deadlock/-panic/-baton/-defect, M-EQ-LOOSE -> unmatched-false-is-not-zero, M-RE-SEARCH -> unmatched-o1-text-after, M-NO-TABLE-SHA -> table-sha-mismatch, M-NO-RESULTS-SHA -> results-sha-mismatch, M-FIRST-CLASS-WINS -> ambiguous-class, M-NO-QCHECK -> unmatched-q15-zero, M-NO-MEDIA-CHECK -> unmatched-no-media-fault, M-NO-TWIN-INDEX-COND -> unmatched-twin-text, M-DIAG-SKIPPED -> fallout-no-diag-dir, M-DIAG-EQUAL-IGNORED -> fallout-equal-false, M-DIAG-CUTOFF-IGNORED -> fallout-cutoff-false, M-DIAG-DIGEST-IGNORED -> fallout-digest-differs, M-DIAG-SLOT-IGNORED -> fallout-slot-not-unreachable, M-DIAG-FIRST-LINE -> fallout-two-r29, M-TABLE-WIDEN -> Acceptance sha line + table-is-frozen + match exit 2, M-TABLE-OK-RECOVERED -> Acceptance sha line + table-is-frozen + match exit 2, G-DEFECT-TRUTHY -> unmatched-defect-empty-text, G-BLOCK-NO-END -> fallout-r29-in-next-block, G-UNREACHABLE-ANY-LPID -> fallout-slot-not-unreachable, G-HEADER-ANY-ROUND -> fallout-block-of-another-round, G-DIAG-SEED-IGNORED -> fallout-diag-seed-differs, G-FAILED-EMPTY-OK -> fallout-failed-lpids-empty, G-INTERNAL-EXC-EXIT1 -> UNCAUGHT (no case reaches the catch-all), G-TWIN-INDEX-OPTIONAL -> UNCAUGHT (see finding 1), OWN-SZI-NOFOLD (F1) -> x5_r39_window_crash_without_a_4f_fold_is_not_szi
Followups: none

## Plain statement

The table, the matcher's matching logic, F1 and F2 are sound. The matcher never passed a hostile input I built, never failed a correct file (main, and synthesised held-out and MP-pressure shapes), and I reproduced the Acceptance's 2/4/5 `unmatched=0` exit 0 from my own diagnostic runs. **One defect blocks it:** a failing round on a real class-C seed exits **2** ("cannot check"), not **1** ("unmatched"). X-5c-F4 REFRESH 11 reads exit 2 as a STOP and exit 1 as "a new class, to the owner", and names "any failing C … round" under exit 1. The card's own case `unmatched-class-c-seed` expects exit 1, but its fixture keeps an MP twin array, so it never exercises the real C shape. After launch, the script may not be edited and no hand classification is allowed, so the mismatch has to be fixed now. The fix is small; see finding 1.

## How the gate was run

- **Fresh gate.** `fresh-gate.sh` and `git-env.sh` were extracted through the `--no-local --depth 1` clone of `nucleus/v3-scaffold` at `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, then run on `93f2109756466b1cd365e7aeadefb6386f767314` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-E4-CLASS-1`. Lines:
  - `fresh-gate: commit 93f2109756466b1cd365e7aeadefb6386f767314`
  - `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`
  - `fresh-gate: checks passed (4538 files hashed in 1.43 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`

  Every STEP was `EXIT=0`, stderr was empty, and the last line was `GATE EXIT=0`.
- **Scratch.** I worked on a `git archive` export of `93f21097` (target `gate-E4-CLASS-1-diag`), with scripts in a separate directory and each mutant on its own script copy. The worktree never held a mutant, and `git status` in it is clean.
- **Hygiene.** I ran no seed at or above 10000. My only runs were the five Acceptance single-seed diagnostics (3988, 5168, 7177, 7727, 8657) and one `cargo test` pin. I did not re-run F2's 10k.

## Findings

**1. REWORK: a failing class-C round exits 2, not 1.**
- **Cause.** In the FINAL main baseline, all 7,000 C seeds carry `"twin": []`; only MP and MW seeds have twins (one per round). `check_failing_round_shape` requires `twin[i]` on every failing round, before any class is tried.
- **Reproduction.** Take a real C seed (seed 0), set `rounds[0].pass=false` and set any `o3`. The result is `E4_CLASS_CANNOT_CHECK seed 0 round 0: no twin entry at index 0`, exit 2, and every other record of the set goes unprinted. The same round with a twin added exits 1. A failing real MW round exits 1.
- **Why it matters.** REFRESH 11 routes exit 1 to the owner as a new class and treats exit 2 as a STOP. A STOP with this line can be resolved only by hand-reading it as a C failure or by editing the script after launch. Both are forbidden. It cannot pass a bad run and cannot fail a good one, because no class takes a C round and no correct run has one. It mis-routes the most likely real new failure and hides the rest of that set's classification.
- **Source.** The card's contract bullet ("On every failing round: … the twin entry at the same index") conflicts with its own case table (`unmatched-class-c-seed` → exit 1) and with REFRESH 11. The worker followed the bullet literally, so this is not a worker error. P must amend the bullet.
- **Fix, fail-closed.**
  - Require `twin[i]` and its `twin_same_index` fields only on a failing round whose seed meets every `seed` condition of at least one class. Such a round is one a class could take.
  - Any other failing round is unmatched with its `no-class` reason. Its `round`, `q` and media fields are still required.
  - `first_mismatch` already checks seed conditions before it reads the twin, so no round can match without its twin being read.
- **Tests to add.**
  - `unmatched-real-c-seed`: a literal C record with `"twin": []`. Expect exit 1.
  - The same record with an MP class and no twin. Expect exit 2.
  - A row for G-TWIN-INDEX-OPTIONAL. That mutant currently survives the self-test, and its surviving behaviour is in fact the fix's behaviour for C.

**2. Non-blocking, fold into the rework: the catch-all has no case.** G-INTERNAL-EXC-EXIT1 makes the `except Exception` branch return 1 instead of 2, and it survives. Add one self-test case that forces an unforeseen exception through `run_match`, for example a `seeds` entry whose `rounds[i]` is a list while `pass` is read. That case must exit 2.

**3. Can it pass something it shouldn't? No, apart from the trust limits below.**
- **Hostile inputs.** Each of these read as unmatched, exit 1:
  - class-1 and class-2 rounds that opened (`c=13`, `frontier=13`, or `frontier` alone);
  - class-3 rounds with `o1` set, `o1f=true`, `served=true`, `mrb=true`, `o2` set or `c=null`;
  - `terr`, `lls` and `cxx` set;
  - a failing twin beside a class round;
  - deadlock, panic, baton, `defect=false` and `defect=""`;
  - a panic seed with no rounds;
  - Arabic-Indic digits in `o3`, and a trailing newline on `o1` or `o3`;
  - a failing MW round.
- **Exit 2.** Each of these exits 2:
  - a missing `o3`;
  - a missing `q` on a C round;
  - `NaN` as the digest;
  - no `seeds` list;
  - the wrong results file for a named sha;
  - a table widened (`pass_na` dropped) or recovered (`c` dropped), whether through `--table` or in place.
- **Trust limits, not defects.** None blocks the run.
  - A duplicated JSON key (`"pass": false, "pass": true`) reads as its last value and passes.
  - `lost=-1, mis=1, torn=1` meets the media floor.
  - `{"seeds": []}` exits 0.
  - The producer (serde, `u32` counts) cannot emit the first two. The results sha is bound to the `.prov`. `verify --set` and the census line `seeds=10000` check coverage, and the matcher does not. An empty seeds list exiting 0 is consistent with the contract ("every failing record matched"). P may want the matcher to print the seed count, but nothing requires it.

**4. Does it fail a correct run? No.**
- **Main baseline.** My own diagnostic runs, from the export, are byte-identical to the worker's five `.json` and `.err` files. Results:
  - with `--diag-dir`: `refused_old_page=2 refused_old_header=4 amcheck_subtree_fallout=5 unmatched=0`, exit 0, and `match.out` is byte-identical to the worker's;
  - without it: `amcheck_subtree_fallout=0 unmatched=5`, exit 1;
  - self-test: 113 cases, 0 failed.
- **Synthesised shapes.** I renumbered the baseline into held-out shape (`10000..20000`) and into MP-pressure shape (the 2,000 MP seeds placed on `s%10 ∈ {7,8}` in `20000..30000`). I rewrote the diagnostic files' seed numbers to match and ran no seeds. Both sets read 2/4/5 `unmatched=0` with exit 0, and `unmatched=5` with exit 1 without the diagnostics.
- **Exit 2 on a correct file.** A passing round is read for `pass` only, so no correct file reaches exit 2.

**5. The table.**
- `tools/e4-class-table.json` hashes to `55ae47347b77f610d51a51287259513d735e2aa92a20fec47889517481c61158`. That equals the card's block, extracted byte for byte, and the script's constant.
- **MP-A narrow form.** Classes 1 and 2 require `c=null` and `frontier=0`, and the opened variants are unmatched (tested).
- **Class 3.** It requires `served=false`, `o1=null`, `o1f=false` and `needs_diag=true`. The owner-delegated ruling of 2026-11-11, appended after the block in `E4-class-table.md`, matches it: class 3 passes only with the per-round diagnostic.

**6. The worker's judgment calls.**
- **Stricter `R29 diag` parsing: accepted.** Any line starting `R29 diag:` counts toward "exactly one". `harness.rs:7457` prints a fixed format, and every real block has exactly one such line.
- **Extra exit-2 cases: accepted.** These are a digest that is not an integer, a duplicate seed, non-boolean flags, a non-list `rounds`, a media count that is not an integer, unforeseen exceptions and usage errors. All are fail-closed, and none trips on a real file.
- **Empty seeds exiting 0: accepted.** See finding 3.
- **C round exiting 2: rejected.** See finding 1.

**7. F1 (X-5c-PINS-2-F1): accepted.**
- **The pin.** `x5_r39_window_crash_without_a_4f_fold_is_not_szi` pins seed 249 round 1 at FINAL. The baseline records seed 249's digest as `867099511`, with r1 `After1@2`, hook `After1`, `szc=1`, `szq=1`, `szi=0` and `pass=true`. Position code 3 is inside `ckpt_unflipped` (2..=7).
- **Runs.** The pin passes on the clean export. Under OWN-SZI-NOFOLD on that export (`rr.szi = rr.szq && ckpt_unflipped(obs.ckpt_step)`), it fails at the `!r1.szi` assertion (`workload.rs:3687`, exit 101). The source was then reverted and diffs clean against the commit.
- **270 vs 271.**
  - Counted by the record's `hook` field over `szq ∧ ¬szi`, the window hooks give 271: After0 54, After1 55, Mid2 41, After2 47, After3 37, After4 37. The PINS-2 verdict's "After0 through After4" includes Mid2.
  - The one round that differs is seed 264 r1, which records hook `Mid2` with crash `After6@0`. Its crash-time position is outside the window, so 270 by position code is right.
  - The comment states both counts. The difference is immaterial to the pin.
- **The doc comment** states what it pins, why BASE could not hold it, and the alternates (286 r1 and 301 r2, both present in the baseline).

**8. F2 (X-5c-PINS-2-F2): accepted on spot-check.**
- **File sha.** `scratch/f2/f2.cmp.txt` has sha256 `7efdb4e03e0660f3ebf6a1f552813e6c467b81e5f9917e8a3832fee3ed07594d`, as claimed.
- **Rows.** It holds exactly 15,624 `F2CMP` rows, one per baseline round with none missing or repeated, and no other lines. Every row's `crash`, `szc`, `szq` and `szi` equals the baseline's.
- **Differences.** `carrier == lit` on all rows, `szi == szl` on all rows, and `gap=0` on all rows. So 0 differences and 0 rounds in the gap.
- **Byte identity.** The instrumented run's `f2.main.json` hashes to `cb947c19…ae45`, byte-identical to the baseline. So the scratch instrumentation (`instrument.py`, read) moved nothing.
- **Instrument sites.** The literal flag is set just before `hooks(After0)`, and cleared where step 5 sets `meta_durable_k`, on the non-crash abort and at `ckpt:end`. These are the sites the follow-up named.
- **Result.** This confirms the README's narrowed `szi` sentence at FINAL, as well as at BASE.

**9. Scope.**
- Three files change against the Base `fc275882`:
  - the two Touch-only tool files;
  - `tests/workload.rs` (+61 lines, one new test).
- The test is outside the card's Touch list and against its "Do not change `tests/`". The orchestrator's brief folded F1 into this card. It is test-only, adds lines only and moves no digest. **Accepted**, as authorised by the brief. The card text should say so when P amends it for finding 1.
- No protected path is touched, and `src/`, `Cargo.toml` and the README are unchanged.

## What remains before the final E4 run

1. **This rework, re-gated.** Finding 1, with finding 2 folded in. Only the script and its self-test change. The table stays frozen at `55ae4734…1158`.
2. **The idle-host CPU measurement and the rehearsal**, both already planned.

No README record has to change before launch. The README's PINS-2 record still says OWN-SZI-NOFOLD is "UNCAUGHT … ESCALATE, no pin". That stays true as that card's own history, since its verdict overruled it and F1 now pins it. A one-line pointer to F1 and F2 can go with X-5c's RESULT write-up.
