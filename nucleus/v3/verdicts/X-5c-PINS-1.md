Card: X-5c-PINS-1
Class: test
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (0374f31c-811e-4bc7-ae11-e9a79c54be3c, 2026-11-09)
Date: 2026-11-09
Gate-exit: GATE EXIT=0
Commit: 4afeca0e0bac04f9f6f4d3bd4a4849b6ca6e2046
Mutants: G-RESEED-TWIN-CUT -> x5_r37_base_tuning_reproduces_1b0ce54e (seed 6849), G5-R48-TREADS-KEPT -> UNCAUGHT (no must-fail case at this tip, P ruling G5-CENSUS), OWN-DUP-ADJACENT -> monitor-run-out-repeated-non-adjacent, own OWN-DUP-TAIL -> monitor-run-out-repeated + monitor-run-out-repeated-non-adjacent, own OWN-DUP-ONLY-ADJ -> monitor-run-out-repeated-non-adjacent, own OWN-RESEED-NOFIRE -> x5_r37_base_tuning_reproduces_1b0ce54e + x5r52_twin_reseed_heals_2859_r1 + x5r52_twin_reseed_heals_609_r1, own OWN-RESEED-TWINMODEL -> the same three, verdict 74f411f0's caught rows -> carried, not re-applied (no code token in src/ changed)
Followups: none

## Plain statement

The rework does what verdict `74f411f0` and the amendment asked, and nothing else. The cut mutant now dies on the new literal, no existing literal moved, and no code changed in `src/`.

**The chain is ready for the F4 REFRESH, once this tip is fast-forwarded.** It is not ready for F4's final run until that REFRESH is written. See "Carried to the F4 REFRESH".

This re-gate judged the rework only, as its request directed. Findings 3 and 5 of `74f411f0` stand as written there.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone, scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, on the full commit, as a background task. `fresh-gate: checks passed (4527 files hashed in 1.34 s)`. STEP test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests all EXIT=0. Literal last line: `GATE EXIT=0`. stderr empty.
- **Scope.** `git diff 2e8d1b13 4afeca0e` touches six Touch-only files plus this verdict. The rework (`74f411f0..4afeca0e`) touches four: README, `src/workload.rs`, `tests/workload.rs`, the comparator.
- **Scratch.** All mutants ran on a `git archive` export under `~/.cache/nv3-review/`. The card's worktree was not touched and read clean before this verdict.
- **Hygiene.** No `e4-campaign`, `verify` or `seeded-faults` ran, and no seed at or above 10000.

## The five questions

1. **The cut pin holds.** The control is 57 of 57. Under G-RESEED-TWIN-CUT, the test fails at seed 6849 with `3952923334` against `1958644185`, the exact pair the amendment names. The rework's test diff adds one `WANT` row. Every other changed line is doc or assertion-message text, and no compared value moved.
2. **The re-read holds.** The comparator against baseline `41c307a4` and run `158ce549` exits 0, reads `DIGESTS_MOVED 0 FAILING_ROUNDS 19 MONITOR 0`, and is byte-identical to `X-5c-F4-F1-F1-F3-F1.acc.compare.out`. `src/` is not byte-identical from `74f411f0`: it has the amendment's two comment edits and no code token change, which is what R2 allowed. The BASE 0..9999 in-process comparison in `74f411f0` therefore still applies.
3. **The text is fixed.** Finding 4's four places are corrected. ESCALATE item 2 is withdrawn, item 1 is marked ruled, and neither banned phrase remains. The `[a, b, a]` case exists; self-test reads 86 cases, 0 failed, and OWN-DUP-ADJACENT fails exactly that case. The comparator's diff from `2e8d1b13` deletes no line.
4. **The G5 ruling is faithful and fails closed.** Every line of the card's block is in the README verbatim. No must-fail case: G5-R48-TREADS-KEPT leaves the suite 57 of 57, as (a) says. Per-set census on the judged runs: (c), and it is buildable, since `cmd_run` holds the `SeedRec`s in memory and F4's Acceptance runs each set as one process with stdout in its `.out`. Live seed: STOP and a required pin, (d) and (e) 2. Stale read: void, (e) 3. Missing, repeated, malformed or short line: STOP, never zero, (e) 4. The (d) pattern admits only one exact all-zero line.
5. **The fresh gate is green** (above).

## Findings (non-blocking)

1. **G5's step 2 table still says "UNCAUGHT at this tip — inert".** It is the first attempt's record, the statement about reachability is true, and Limit 9 confines the README edits to R4's seven items. The RESULT table carries the ruling. No action.
2. **The ruling binds F4 only once it is pasted.** `X-5c-F4.local.md` has no `JOINT_ARM_CENSUS` text today. Until (b)–(f) are in that card, nothing there enforces them. QUEUE's X-5c-F4 row records this, and (g) assigns it to P or the orchestrator.

## Carried to the F4 REFRESH

- Paste ruling (b)–(f), add the one Acceptance line per set, and make `Touch only` cover the file that prints the census (`src/campaign.rs` holds `cmd_run`).
- Sharding a set would make every census line short of `seeds`, which is a STOP. F4's Acceptance does not shard today; keep it that way.
- From `74f411f0` finding 5: `a.out` and `./a.out` pass as distinct paths, so whoever assembles F4's `--run-out` list should not rely on that check for aliases.
- F4's `Depends` and the `Tuning::FINAL` hand-off are still open. I did not re-check QUEUE's other relaunch orders.
