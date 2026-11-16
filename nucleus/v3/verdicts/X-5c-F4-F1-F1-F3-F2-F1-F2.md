Card: X-5c-F4-F1-F1-F3-F2-F1-F2
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: Claude Code session 362572a5-a441-428e-b1b6-718c6e0e7a81
Date: 2026-11-09
Gate-exit: GATE EXIT=0
Commit: 0f091aa49ef91cad9c666c8fdcd092d3ede2bc94
Mutants: M-TWIN-NO-DIR-RESEED -> x5r52_twin_reseed_heals_2859_r1 + x5r52_twin_reseed_heals_609_r1, M-TWIN-RESEED-ALWAYS -> x5r52_twin_chain_kept_when_directories_agree_639, M-TWIN-PRED-ALWAYS -> x5r52_twin_dirs_differ_compares_directory_keys + kept-chain 639, M-TWIN-PRED-ENTRIES -> x5r52_twin_dirs_differ_compares_directory_keys, M-TWIN-RESEED-FROM-TWIN -> 2859 + 609 pins, M-TWIN-RESEED-LATE -> 2859 + 609 + kept-chain 639 pins, G-PRED-ONEWAY-REAL -> x5r52_twin_dirs_differ_compares_directory_keys, G-PRED-ONEWAY-TWIN -> x5r52_twin_dirs_differ_compares_directory_keys + 2859 + 609 pins, G-RESEED-TWIN-MODEL -> 2859 + 609 pins, G-RESEED-TWIN-DISK -> 2859 + 609 pins, G-RESEED-MP-ONLY -> 2859 + 609 pins, G-PRED-LEN-ONLY -> UNCAUGHT, G-RESEED-TWIN-CUT -> UNCAUGHT, G-RESEED-MW-ONLY -> UNCAUGHT (no MP round fires in 10k)
Followups: X-5c-F4-F1-F1-F3-F2-F1-F2-F1, X-5c-F4-F1-F1-F3-F2-F1-F2-F2

## Verdict in one paragraph

MERGE-WITH-FOLLOWUPS. The worker's ESCALATE is correct: the runner cannot accept the two mutant lines as written. Both mutants are caught by the right pins for the right reason on my own run, so **the two lines count as met by this verdict's run and no P amendment is needed to merge this commit**. The re-seed does what the ruling says. It fired 37 times in 10000 seeds, all on MW rounds, each right after a real round that recorded a loss and passed. The comparator reading reproduces byte for byte. Twin failures fall from 7 to 0, with 0 new failing, MONITOR 0 and DEFECT_SEEDS 0, and the 19 failing real rounds are the same 19. My own 10k run, built with instrumentation, produced the same JSON sha256 as the runner's. Three of my mutants survive; two are test gaps for a follow-up and one makes no difference across the 10k.

## Gate run

- Scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, extracted through the `--no-local` shallow clone. No `EXTRACT FAILED`.
- `fresh-gate: commit 0f091aa49ef91cad9c666c8fdcd092d3ede2bc94`
- `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`
- `fresh-gate: checks passed (4523 files hashed in 1.33 s)`
- `fresh-gate: 2 file(s) matched through their eol conversion`
- All seven `STEP` lines `EXIT=0`, including `enforcement-tests`. Last line `GATE EXIT=0`. Stderr empty.
- The worker's `GATE EXIT=1` (the runner-e2e-K row in its own container) did not recur here or in the runner's container. That agrees with the earlier K ruling.

## Scope

`git diff --name-only db1e5162 0f091aa4` lists six files, all under `spikes/e4-recovery/`: README, `results/digest-stability.tsv`, `src/workload.rs`, `tests/workload.rs` and the two mutant patches. `x5r52_compare.py` is unchanged. In `src/`, the only changes are the `StoreInner` import, the predicate (a) and the call (b). No `unsafe` and no non-test `unwrap`, `expect` or `panic` were added. The worktree is clean, and every mutant ran on a scratch export of the commit under `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F3-F2-F1-F2/`.

## 1. The runner and the two mutant lines

**The impossibility argument is right.** `tools/run_card.py` line 2079, in place since `1b1b6bd3`, turns `rc == 0` into a failure when a line containing `cargo test` has no `test result: ok. N passed` in its captured output. Each mutant line's captured output is `tail -n 40 test.out`, and its exit code is the grep chain's. The chain can exit 0 only if cargo returned 101 and the named pins are in the failures block. With `--test workload` there is one libtest binary, so that binary prints `test result: FAILED`, which the regex cannot match. A line that passes its own check therefore always fails the runner's, and nothing inside the card's Touch only could change that. The worker was right to stop rather than print a line shaped like a summary.

**My run of the two lines**, on a fresh export of the commit, with the card's cargo command and grep logic:

| Line | rc | Summary | Failing pins | Reason |
|---|---|---|---|---|
| `m-twin-no-dir-reseed` | 101 | FAILED. 2 passed; 2 failed | 2859 r1 and 609 r1 pins | `2859 twin r1 must pass … O-1(a) at c=10: published {} != model {2, 3}`; `609 … c=12 … {2, 3, 4}` |
| `m-twin-reseed-always` | 101 | FAILED. 3 passed; 1 failed | kept-chain 639 | digest `1144734390`, literal `3366243401` |

The O-1 text is only the default that a recovery `Err` leaves behind, so I checked the error itself. I ran a no-re-seed build with the committed diag patch on all seven seeds. Every one logs `GATEDIAG recovery Err … class=C … Corrupt("tree N missing in directory")`, where N is the key the real directory holds and the twin lacks: 609 (5), 849 (3), 2599 r2 (3), 2859 (4), 3449 (5), 5049 (5), 7179 (4).

**Ruling.** The two lines are met by this run, recorded here. The no-tests-ran rule exists to stop a filter that matches nothing from passing; `rc=101` plus named failures already shows tests ran, so its purpose is served. `merge-card.sh` reads the verdict and never the runner's row. No P amendment is needed for this commit. Three cautions:
- Do not re-dispatch this card through the runner; it would fail the same way.
- The worker's card-side fix, appending an unrelated green test, is honest but proves nothing about the mutant. I would not adopt it.
- Its runner-side option of accepting `(ok|FAILED)` is too wide, because `cargo test … || true` would then pass. See -F2.

## 2. The fix

**The code matches the ruling.** The predicate compares `dir` key sets only. The call sits after the `is_none()` re-seed and before `take()`, and clones `disk`, `st`, `clone_model` and `cut` from the real chain.

**Measured over all 10000 seeds** (scratch build logging every firing, sha below):
- 37 events on 36 seeds, all MW; none on MP or C. Seed 639 has none.
- Every event follows a real round with `lost`, `torn` or `mis` above zero that passed.
- 9 are real-only (a lost DropTree): the 7 healed entries, plus 2089 r1 and 5739 r2, which do not move.
- 28 are twin-only, mostly a real chain back at `[]` after losing its catalog creation.
- 0 cases of equal keys with a different `dropping` flag (the residual path the predicate cannot see). 0 cases of equal-size but different key sets.

**Can it fire on a real divergence?** Yes. The predicate cannot tell why the directories differ, and none of the 37 was such a case. It cannot hide one, for two reasons. (b) runs at the next round's start and writes only the twin chain, so the real round's record is already pushed and the real chain is untouched. And a divergence caused by a store bug is judged in the round where it happens, against the model. As a check, I removed a key from the real chain's `dir` at r1 on five seeds, with and without (b). Real-round pass or fail was identical both ways. Only seed 9 exercised the path (r1 fails both ways, and with (b) its twin now fails too); on the other four the removal had no observable effect, so this is thin evidence and the argument above carries the weight.

**What it costs.** After a firing, the twin runs from the real start for that round, so the twin's own record of the diverged chain's next round is gone. That record was the false class this card removes.

**Effect beyond twins.** Class C never has a twin chain, and all nine moved digests are MW. A real round can move only through what the twin feeds it, as ruling (e) anticipates (see 2669 and 9059 below).

## 3. The numbers

The comparator, extracted from the committed blob, with `--no-twin-exemption --max-twin-failing 0` against the pinned baseline (`6b686266…739a`), exits 0 and its output is byte-identical to the runner's:

```
DIGESTS_MOVED 9 FAILING_ROUNDS 19 MONITOR 0
MOVED_GAINED 0 []    MOVED_MP_PENDING 0 []    META_ONLY_ELIGIBLE 0 []    MPA_PASS_FAIL 0 []
MPA_CLASS base 10 tip 10    TWIN_DDL_PENDING 0 []    MOVED_LOST 0 []    HEALED 0 []
TWIN_HEALED 7 [(609, 1), (849, 1), (2599, 2), (2859, 1), (3449, 1), (5049, 1), (7179, 1)]
TEXT_CHANGED 0 []    TWIN_FAILING 0 [] BASE 7    NEW_FAILING 0 [] BAD_SEEDS 0 []
```

`E4 FOLD FAILING_SEEDS=19 FAILING_ROUNDS=19 TWIN_FAILING_ROUNDS=0 DEFECT_SEEDS=0`, `MONITOR=0`. `verify --no-quotas` is OK. No `check-mp` is due, since `MOVED_MP_PENDING` is 0.

**Provenance.** The 10k JSON sha256 is `41c307a470ba51e1eae7911fa7b1b7e7146e810f191ccb348bd0713a19601cd2` from three separate runs: the worker's at `7a9d72e2` (README), the runner's at `1ebe9535` (the `.prov`, with `status_lines=0`), and mine, an instrumented build of `0f091aa4`, whose records are identical. `1ebe9535..0f091aa4` changes only the README. The acceptance files are copied to `runlogs/`, and the `.prov` sha matches.

**Every moved digest** (9 moved, all with a re-seed event, 0 unmoved seeds differ in any field):

| Seeds | What moved | Why |
|---|---|---|
| 609, 2599, 2859, 3449, 5049 | twin entry fail → pass | the seven target entries |
| 849, 7179 | the same, plus real r1 `nested` 2→0 and 1→0 | the twin's nested points feed the real round |
| 2669 | twin passes both ways; real r1 `Mid2@1`→`Mid2@0`, `c` 5→1, still passes | the real round follows the twin's re-derived hook spec; the twin now starts from the real `[]` start |
| 9059 | twin passes both ways; real `nested` r1 0→2, r2 0→1, still passes | twin-fed nested points |

The 27 other re-seeded seeds do not move. The 19 failing real rounds are the same set before and after.

**The twin failures that were the point of the card.** All gone. The 6 at Base were 609, 849, 3449, 5049, 6539 and 7179. Of those, 6539 had already healed at F2-F1 when its crash point moved; it passes here and has no re-seed event, so this card did not heal it. The other five, plus 2599 and 2859, are the 7 `TWIN_HEALED` entries.

**Quotas.** Six counters moved by at most 1, and no MET/MISSED state changed: Q2 −1, Q5a +1, Q5b −1, Q5c −1, Q7 +1, Q18 980→979. Q18 was already MISSED.

## 4. Literals and the README record

**Literals are honest.** One line is removed from `tests/workload.rs`: 2669's literal `357787865` → `1374321949`, at a seed with its re-seed event at r1. No assertion is weakened. 2669's new value equals the 10k's. The kept-chain literal `3366243401` equals the baseline's, and 639 has no event. I re-derived the selection from a 0..1000 run under `m-twin-reseed-always`: 639 is the first candidate that moves, and the thirteen lower ones are inert, as the README says. The `.tsv` holds exactly the 0..1000 movers, 609 and 849.

**The README's claims hold**, including the event list for 0..1000, `2599 at r1 and r2`, and "moved ⊆ re-seeded" (which it had measured only on 0..1000 plus singles, and which my run confirms over the whole 10k). Two points to read with care:
- Step 2's reason for the non-moving seeds, "the r1 script touches no tree the old twin chain lacked", is right for 259, 719 and 809 (twin-only, so the twin holds a superset). It is not general: 2089 and 5739 are real-only and also do not move.
- Its `Status` calls the family "root-caused and fixed". That holds for the round-start DDL divergence, which is every case seen. A recovery `Err` of another origin would still be new.

## 5. Mutants

All card-named mutants are caught, by the named pins or a superset. M-TWIN-RESEED-LATE adds the kept-chain pin, as the README says.

| Own mutant | Result |
|---|---|
| G-PRED-ONEWAY-REAL / -TWIN (one-direction key check) | caught |
| G-RESEED-TWIN-MODEL / -DISK (keeps the twin's model or disk) | caught |
| G-RESEED-MP-ONLY | caught |
| G-PRED-LEN-ONLY (compares key counts only) | **UNCAUGHT**, full workload suite green. No 10k event has equal-size key sets, so no reading depends on it. |
| G-RESEED-TWIN-CUT (keeps the twin's `cut`) | **UNCAUGHT**, full workload suite green. `cut` feeds the twin's `c_prev` and `lo` bound, so it is not equivalent. |
| G-RESEED-MW-ONLY | **UNCAUGHT**, suite green. Equivalent over the 10k, since all 37 events are MW. Left as is. |

Both test gaps are behaviour I confirmed correct at the tip by reading the code. My first full-suite run of the two survivors shared a build directory and ran another mutant's stale binary; I discarded it and re-ran each in an isolated target. All mutants are scratch only.

## 6. Before F4's final run

1. Merge this card first. X5-R20's campaign re-run applies, because digests moved.
2. Pin the new baseline: sha `41c307a4…1cd2`, files in `runlogs/X-5c-F4-F1-F1-F3-F2-F1-F2.acc.*`. F4's REFRESH and step 0 must read this, not `6b686266…`.
3. F3-F1 measures "moves no BASE digest" against this baseline, on a Base that includes this card.
4. Every comparison at a commit carrying the re-seed runs `--no-twin-exemption` (X5-R52 (g)). Any twin recovery `Err` of this family is new under E4's owner rule.
5. Q18 is 979 at this baseline, for F4's retune list.
6. Add both follow-ups to QUEUE, which `merge-card.sh` requires. Note on the row that the runner's FAIL on the two mutant lines is the rule conflict ruled on here.
7. Any new card with an expected-failure mutant Acceptance line will hit the same wall until -F2 lands.

-F1 is test-only and moves no digest, so it does not block F4.

## Follow-ups

- **X-5c-F4-F1-F1-F3-F2-F1-F2-F1 (tests only; moves no digest).** Add to `x5r52_twin_dirs_differ_compares_directory_keys` a case with equal-size, different key sets that must read true, which kills G-PRED-LEN-ONLY. Add a pin that the re-seeded twin chain takes the real chain's `cut`, which kills G-RESEED-TWIN-CUT, using a seed with an event where the two `cut`s differ, or a direct unit form if `run_seed_with` cannot expose it. No change to `src/`, to any literal or to the predicate.
- **X-5c-F4-F1-F1-F3-F2-F1-F2-F2 (tools, P; after the tooling freeze lifts).** `run_card.py`'s no-tests-ran rule cannot accept an Acceptance line that proves a test fails. Narrow the exemption: at `rc == 0`, accept a `test result: FAILED. N passed; M failed` summary with M ≥ 1 only for lines carrying the expected-failure proof (`rc=101` plus a failures-block grep), and add an enforcement row for each of those and for `cargo test … || true` staying refused. Do not accept `FAILED` summaries in general.
