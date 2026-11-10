Card: E4-SILENT-1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (30fb4e5e-05be-4bde-8980-5ca7fd6ad460, 2026-11-10)
Date: 2026-11-10
Gate-exit: GATE EXIT=0
Commit: f9d7ed56fa06aaf0e1800f4e94ad03f47698228f
Mutants: M-R1-SKIP -> es1_lost_forced_header_refused + es1_refused_open_writes_nothing, M-R1-GE -> es1_faultless_every_crash_point + es1_lost_forced_header_refused, M-R1-ZERO -> es1_lost_forced_header_refused, M-R1-LATE -> es1_refused_open_writes_nothing, M-OP-TAG5 -> wal_exact_segment_end_hops + wal_tail_short_never_placed, M-R2-GUARD -> es2_recycled_start_zero_refused, M-R2-ALWAYS -> es2_healthy_start_zero_every_crash_point, M-R2-ANYNAME -> es2_healthy_start_zero_every_crash_point, M-SEG0-ORDER -> es2_segment_zero_created_first, OWN-NEEDPRE -> es1_lost_forced_header_refused, OWN-TSINV -> es1_lost_forced_header_refused, OWN-FIRSTONLY -> UNCAUGHT
Followups: E4-SILENT-1-F1, E4-SILENT-1-F2

## Plain statement

Both silent-wrong-data bugs are closed. Across 3,974 directed cells written by the gate, every one of the base's 102 `O-3(d)` silent results becomes a refusal at `open`. No healthy round refuses, out of 12,800 class C crash rounds that include every scheduler yield of a fresh store, segment 0's creation and nested recoveries. The 10k reproduces byte for byte. The 18 moved digests all come from the publish record's 9 extra bytes. No new failing round appears, and the same 19 rounds fail. All 9 card mutants are caught. One gate mutant survives as a test gap, not a defect.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone; scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`. Lines: `fresh-gate: commit f9d7ed56fa06aaf0e1800f4e94ad03f47698228f`, `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, `fresh-gate: checks passed (4528 files hashed in 1.35 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. Last line `GATE EXIT=0`, stderr empty.
- **Scope.** Against the card's base `9f96f31e`, 8 files change, all on the Touch-only list. `src/`: 268 lines added, 45 removed. `tests/`: 762 lines added, **0 removed**. The `nucleus/v3-scaffold...HEAD` stat is the stacked chain. No protected path is touched, and the worktree was clean.
- **Scratch.** Every run and mutant used `git archive` exports in the gate's scratchpad. They added one Cargo `exclude` for `spikes/e1-bench`, whose v2 path dependency is absent from an export. The worktree never held a mutant.
- **Hygiene.** Seeds `0..9999` only: two `run --set main` (pristine and instrumented) and single-seed runs. No `verify` over a set, no `seeded-faults`, no probe. A parse of every shell command in both attempt logs (63 and 10) finds no seed at or above 10000. The `10000..`, `20000..` and `30000..` strings appear only as card text. The README and commit messages name no such seed.

## Findings by question

**1. Both bugs closed.** The gate's own matrix is in scratch `tests/g_es.rs` and was run on tip and base.
- The cells cover lost, torn and far-misdirected writes to every meta write, at 24 task-yield crash points, over two seeds, with and without a later checkpoint. They also cover every pair of lost metas, every hook of the forced and later checkpoints under three keep/tear mixes, a misdirect onto the other meta slot, and a two-publication case.
- Base: 102 cells `O-3(d): silent Ok` (O-1(b) "tree 5: scan mismatch", or the recycled start-0 empty open). Tip: 0.
- 84 of them now refuse and pass. 18 (hooks Mid2 and After2 of the checkpoint after the forced one, where step 2 already reused image-1 slots) refuse at open but keep `served=true`. That is the existing `refused_old_page` shape: the taint read happens in `load_image`, before the R1 check, and is not data returned to a client.
- No cell that opened and passed on base refuses on tip.
- R2's recycled matrix (After4, After5, Mid6, After6 over three seeds and four keep/tear mixes):
  - At After5, and at Mid6 when the rename is dropped, segment 0 is present and the open is correct.
  - At After6 it refuses (36 of 36). Base was silent in 27 of those.
- The whole-disk check (every file's names, lengths, bytes and dwrites) shows both refusals write nothing.
- **Remaining paths, by reading.**
  - The prototype's only unlogged work that depends on a forced checkpoint is `bulk_load`. GC is unlogged but safe on an older image. DDL and DropTree are logged.
  - The id is captured after `wait_ckpt`, and `force_ckpt=false` → `ckpt_open=true` has no yield between them, so the id is never below the forced id.
  - A silent path remains only through bytes lost past `wal_ckpt_end` (S-STORE §14, known) or a lost publish record (the tree stays unpublished).

**2. No false refusals.** The faultless crash rounds:
- 12,480 rounds at every scheduler yield 0..259, under four keep/tear mixes and three seeds, over four scripts: load plus later checkpoint, a recycled start 0, a nonzero start, and setup only (no segment).
- 320 rounds with nested recovery crashes.
- Every one opens and passes.

The 10k shows `E4_SILENT_REFUSE` R1 = 0 and R2 = 2. **Both R2 lines are the Base's G3 arm (start above 0), not the new start-0 arm:**
- seed 1787 r0 (MP, After6@2, opened 2, start 1903);
- seed 9477 r0 (MP, uniform, opened 1, start 168).

Both have `mrb=true`, pass, and digests equal to the baseline. The start-0 arm fired 0 times in the 10k. The instrumented run counts WR-1's branch entered 0 times and fired 0 times.

**3. Scope.**
- Behaviour changes only in E4-R1, E4-R2 and WR-1.
- `wal_create_segment` is a verbatim extraction.
- `wal_names` takes the world through `step` with no yield. At start above 0, `exists` is unchanged.
- A no-emission variant (tip with `Some(need_ckpt)` replaced by `None`) restores all 18 moved digests. Including `wal_names` and the `wal_writer:wr1` step, everything else in the diff moves no digest.
- FINAL equals BASE field for field, and no X5-R37 literal or test line changed.

**4. Baseline.**
- My pristine 10k (8 threads) is sha256 `fe598604e1918b9c9a15366b6b393e3711b1efd4bcddc6b6c5d0f44d2a97ede8`, byte-identical to the worker's. The instrumented run gives the same sha.
- Comparator: rc 0, `DIGESTS_MOVED 18 FAILING_ROUNDS 19 MONITOR 0`, `NEW_FAILING 0`, `HEALED 0`, `MPA_CLASS base 10 tip 10`, `DEFECT_SEEDS=0`.
- Moved 18 (14 C, 3 MW, 1 MP):
  - In 14 seeds' moved rounds, a uniform crash point was redrawn because the round's yield total changed.
  - Three hook-crash seeds (1609 r1, 2224 r1, 3829) saw a different WAL layout under the same hook.
  - All 18 return to the base digest without the 9-byte op.
- `check-mp` 2098 r0: `EXPLAINED (b)5`, slots [21, 55] tainted.
- `MOVED_GAINED` is 1609 r1 (MW, now a cover detection) and 2098 r0. `TEXT_CHANGED` is 199 r1 (the O-1(a) text names a later c).

**5. Mutants.** All 9 card rows fail their named tests. Some fail more, which is fine. M-R2-ALWAYS and M-R2-ANYNAME also fail `es1_faultless_every_crash_point`. Gate mutants:
- OWN-NEEDPRE, the id read before `wait_ckpt`: caught.
- OWN-TSINV, the T filter inverted: caught.
- **OWN-FIRSTONLY**, the check stopping after the first `RequireCkpt` op: **UNCAUGHT** by the crate. The gate's two-publication case catches it (12 silent cells). The code is correct (that case gives 18 refused, 0 silent on tip). See F1.

**6. Pins.** The four seed pins 1926, 1005, 344 and 9149 hold unchanged. Their literals (1_452_311_068, 3_142_266_577, 3_836_486_934, 3_826_847_088) equal the new file's digests, and none is in the 18. None was redone and none needs redoing. No X5 literal was re-derived and no assertion was weakened.

**Notes for later readers.**
- The R2 stderr line is printed by both G3 arms, so "R2=2" counts Base refusals.
- The card's third deliberate over-refusal is stated as "pinned" but has no test. That is the fall-back to the forced image when a later non-forced checkpoint was durable at capture. See F2.
- The step-3 README edit joined two lines of the X-5c-F4-F2 record ("folded cards.- **(b)"). It is cosmetic.

## Next

- **The new baseline of record** is sha256 `fe598604e1918b9c9a15366b6b393e3711b1efd4bcddc6b6c5d0f44d2a97ede8`.
- **Must be redone.** E4-CLASS-1's frozen table (`baseline_sha256`, its own sha, the script constant, `--results`/`--results-sha256`) must be re-run against that file. Its Base moves to this card. Expect 3, 7 and 9 with the same 19 rounds.
- **Not needed.** F4-F2's follow-up pins need no change, since no pin seed moved.
- **F4-F3** can start once this card merges, stacked on `card/v3-E4-SILENT-1`, with `--ref` set to the new file.

## Follow-ups

**E4-SILENT-1-F1: pin that every publication is checked.** Add a directed test with two bulk loads in one round: publication A depends on k, publication B on k+1. A lost meta k+1 opens k, and the open must refuse on B. The check that stops at the first dependency op (gate mutant OWN-FIRSTONLY) passes the whole crate today. The gate's scratch case `g8_two_publications_each_checked` is a working shape: seeds 4242 and 91, `TaskYield(1, 0..16)`, lost/torn/misdirected newest meta, no `O-3(d)`, and refusals above 0. It is test-only and moves no digest. It must land before the final run.

**E4-SILENT-1-F2: the third over-refusal and record hygiene.** Either pin the card's third deliberate over-refusal (a fall-back to the forced image when `id` came from a later non-forced checkpoint durable at capture, which needs a two-task schedule), or record that it is reasoned but unpinned. Also:
- Split the R2 stderr line so the start-0 arm is distinguishable from the Base G3 arm (stderr only, never a digest input).
- Restore the line break the step-3 README edit removed in the X-5c-F4-F2 record.

Non-blocking.
