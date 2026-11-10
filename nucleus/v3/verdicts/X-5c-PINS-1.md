Card: X-5c-PINS-1
Class: test
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (15ebe81f-da81-4552-9e40-2dcde8e7cf4d, 2026-11-09)
Date: 2026-11-09
Gate-exit: GATE EXIT=0
Commit: 324a519cec23510a69156f5dae316d5173fb99bf
Mutants: GX-RUPF-NORESET -> x5_r42_second_window_reuse_pf_starts_at_zero, GX-R50-NOSELECT -> x5_r50_ineligible_rounds_draw_nothing, G5-R48-TREADS-KEPT -> UNCAUGHT, G-PRED-LEN-ONLY -> x5r52_twin_dirs_differ_compares_directory_keys, G-RESEED-TWIN-CUT -> UNCAUGHT, G-MON-FIRSTLINE -> monitor-later-line-counts, G-MON-FIRSTFILE -> monitor-last-file-counts, G-MON-EMPTY-OK -> monitor-empty-run-out, G-MON-NO-MONITOR-FIELD -> monitor-line-cut-before-monitor, G-MON-MAX -> monitor-sums-not-max, G-TIE-ONE-WAY -> monitor-more-run-outs-than-runs, M-MON-DUP-OUT -> monitor-run-out-repeated, G1-HEADER-WINDOW -> r47_record_completed_by_a_later_write, G-GUARD-EVICT-AT -> late_copy_after_previous_epoch_eviction_drops, M-LATECOPY-ATONLY -> late_copy_in_the_gap_drops_for_a_gated_touch + freed_lpids_late_copy_never_remaps, G-GUARD-ABSENT-OK -> late_copy_in_the_gap_drops_for_a_gated_touch + freed_lpids_late_copy_never_remaps, db1e5162 caught rows (46) -> their cases, own OWN-RU-NORESET -> x5_r42_second_window_reuse_pf_starts_at_zero, own OWN-DUP-RC1 -> monitor-run-out-repeated, own OWN-DUP-ADJACENT -> UNCAUGHT
Followups: none

## Plain statement

The card moves no digest and most of it is sound, but two of its required mutants survive and one of the two is the worker's mistake. The README says G-RESEED-TWIN-CUT is inert and that no literal can pin it. That is false: the mutant moves the digest of BASE seeds 6209 and 6849, so a one-row literal kills it. The other survivor, G5-R48-TREADS-KEPT, really is unreachable at this tip, and closing it needs a ruling from P.

**The chain is not ready for the F4 REFRESH or the final E4 run.** See "Before F4" below.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone, scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, on the full commit, as a background task. `fresh-gate: checks passed (4526 files hashed)`. STEP test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests all EXIT=0. Literal last line: `GATE EXIT=0`. stderr empty.
- **Scope.** `git diff 2e8d1b13 324a519c` touches six files, all on the Touch only list. `ckpt.rs` was not needed and did not change. The request names `2e8d1b13` as the base, so the diff is taken from there rather than from the scaffold.
- **Scratch.** Everything ran on `git archive` exports under `~/.cache/nv3-review/X-5c-PINS-1/`, each Rust mutant with its own export and target directory. The card's worktree was not touched and is clean.
- **Hygiene.** No `e4-campaign`, `verify` or `seeded-faults`, and no seed at or above 10000. The censuses below are in-process `run_seed_with` loops over 0..9999 on a scratch export.

## 1. No BASE digest moved

- **The re-read.** Run from the committed comparator: both sha256 values verified (`41c307a4…1cd2`, `158ce549…a69b`), rc 0, `DIGESTS_MOVED 0 FAILING_ROUNDS 19 MONITOR 0`, and `cmp` reads the output byte-identical to `runlogs/X-5c-F4-F1-F1-F3-F1.acc.compare.out`.
- **What that re-read cannot show.** Both of its inputs were recorded before this card, so it says nothing about this commit's code. I therefore ran this commit over BASE 0..9999 in-process (with stderr-only probes added) and compared each `SeedRec.digest` with `158ce549`: **0 of 10000 differ**, no defects.
- **The +45 lines.** Two `SeedRec` fields, two locals, two assignments after `treads.clear()` and two pushes. They read `treads.len()` and nothing else: no draw, no schedule change. `SeedRec` has no `Serialize` derive, `SeedRec::finish` does not read the fields, and neither the campaign binary nor the examples name them. The `twin_paired` manner holds.

## 2. The six items

| Item | State |
|---|---|
| 1. RUPF window reset, R50 selection | Done. Both mutants caught for the right reason. |
| 2a. Joint-arm supply pin at 7469 | **Not achievable as written** (finding 2). Diagnostic present and correct. |
| 2b. README step 6 base, step 4 scope | Done. |
| 3a. Equal-size predicate case | Done. |
| 3b. The cut pin | **Not done, and the stated reason is wrong** (finding 1). |
| 4. Six comparator cases, repeated `.out`, three README corrections | Done. `read_monitor`, the count tie and every classifier are unchanged: the comparator diff is two purely additive hunks. |
| 5. G1-HEADER-WINDOW, cost sentence | Done. The replacement quotes the O1C-F1 Cost section. |
| 6. Eviction arm, gap pin, doc correction | Done, with a deviation (finding 3). |

**Repeated `.out`, tested directly.** `[a, a]` and `[a, b, a]` each exit 2 with the `ERROR` line. Distinct paths with identical content exit 0, as Limit 6 requires.

**Assertions.** The test diff is 731 insertions and one deleted line, a doc-comment line re-expanded in the same place. No existing assertion was changed or weakened. README deletions were read: none touches X5-R51, X5-R52 or X5-R53.

## Findings

### 1. G-RESEED-TWIN-CUT is live, and the README says it is not (blocking)

The worker tried 14 event seeds and concluded the mutant is "inert on every measurable event seed" and that "a digest literal cannot catch it". The 10k has 36 event seeds, and I ran the mutant over all of them, toggled by an environment variable on one binary with a control pass on the same seeds.

| Tuning | Event seeds | Records that differ under the mutant |
|---|---|---|
| BASE | 36 | **6209** (digest 2438621228 -> 870698661), **6849** (3952923334 -> 1958644185) |
| S1 (1000, 500) | 31 | 809, 3009, 6849, 7409 |

Neither seed has a row in `x5_r37_base_tuning_reproduces_1b0ce54e`, so the mutant passes the workload suite (57 of 57 under the mutant). The card's search scope was narrow, which explains the miss, but the claim was written as a general one and it is wrong. `cut` feeds the twin's `c_prev` and `lo`, as verdict `af5c8d63` said.

### 2. G5-R48-TREADS-KEPT cannot be killed at this tip (needs a P ruling)

The worker's premise-drift report is correct, and my census is wider than the worker's 0..1000. `treads.clear()` is present and correct by reading.

- At 7469 the re-paired twin now fires at the first re-derived spec, so the joint block is not entered.
- **No seed of 0..9999 reaches the joint arm, at BASE or at S1.** Every `twin_joint_arm` reads false and every stale count reads 0.
- The workload suite passes 57 of 57 under the mutant.

So the card's "must fail it with the 17 stale reads showing" has no live seed. `r48_joint_supply_is_fresh_at_7469` can hold only a vacuous freshness assertion and a tripwire. Forcing the arm needs a test seam, which Limit 1 forbids. The worker cannot fix this inside the card.

**Recommendation to P:** accept the diagnostic and tripwire and strike the row, and have F4's REFRESH repeat this census under the real FINAL, since S1 is not all of F4's knobs. The mutant's direction is a false pass (a stale read can only suppress an oracle failure), so the census should not be skipped.

### 3. The eviction-arm pin departs from the card's verbatim shape (non-blocking)

The card asks for a gated insert. The pin parks a raw `free_lpid` and evicts inside the Mid2 hook, because an insert's fetch pins the frame. The card's rule for this situation was "stop for P", and the worker filed it as a judgment call instead. The doc comment states both compressions plainly.

I accept the pin. A probe build under G-GUARD-EVICT-AT printed the install facing `At(Ref { slot: 18 … })` with an equal `cur`, so it fails on the arm it names. The tree layer's free paths reach `free_lpid` with no pin held.

### 4. Stale text (fix with the rework)

- The inline comment at the diagnostic and the `twin_joint_stale_reads` doc give "17 at seed 7469" as if current. It was true at CXX-F1's tip.
- `r48_repaired_twin_fires_at_the_joint_spec`'s doc, and its assertion message, say round 1 runs the joint spec. At this tip `Mid2@3` is the first re-derived spec.

### 5. Comparator edges (non-blocking, not ruled defects)

- Own mutant OWN-DUP-ADJACENT (only the first two paths compared) passes `self-test`, because the case is `[a, a]` only. The shipped code handles `[a, b, a]`, by my direct run.
- `a.out` and `./a.out` pass as distinct paths (rc 0). That is literal path comparison, which the card asked for, but whoever assembles F4's `--run-out` list should not rely on it to catch an alias.

## Rework

1. **Pin the cut.** Add a digest literal for BASE seed 6849 (3952923334) or 6209 (2438621228) as a new row, no existing literal moved. G-RESEED-TWIN-CUT must fail it. No `src/` change is needed.
2. **Correct the record.** Withdraw ESCALATE item 2 and the "inert" and "no literal can pin it" text in step 3 and the RESULT table, and fill RESULT item 3 with the seed.
3. **Fix the stale text** in finding 4. Comment and doc text only.
4. Optional: add a `[a, b, a]` case beside `monitor-run-out-repeated`.
5. **G5 waits on P's ruling** (finding 2). The card's row must be amended before the re-gate, or this verdict's survivor stands.

## Before F4

- The rework above, a re-gate, and the fast-forward of the chain to it.
- P's ruling on G5-R48-TREADS-KEPT.
- F4's `Depends` still needs this card and the earlier chain cards, and `Tuning::FINAL` is still BASE (the S1 hand-off is F4's REFRESH).
- QUEUE's X-5c-F4 row still lists the owner's Q4 answer as a blocker. I did not establish whether it has since been given.
