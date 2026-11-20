Card: S-ETA
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session ecdca529-1b7a-462a-800b-23873858d62d (gate 2, a narrow re-gate after rework 1: this agent wrote neither the card S-ETA, its implementation becd0afc, the rework 901e85dd nor gate 1's verdict, and is not the orchestrator)
Date: 2026-11-20
Gate-exit: GATE EXIT=0
Commit: 901e85ddf94dd4d68dc8652795c880b5a8451582
Mutants: spec text, none in code. Omit-one per rework block, each committed off 71ecf3e6 and judged by the card's 44 non-gate Acceptance lines in both locales: RW-1 -> lines 20, 22, 43, caught; RW-2 -> 32, 42, 43, caught; RW-3 -> 22, 32, 43, caught; RW-4 -> 22, 33, 43, caught; RW-5 -> 22, 34, 43, caught; RW-6 -> 6, 43, caught; RW-7 -> 6, 43, caught. Gate 2's own planted errors, same method: G2-1 W22's lost record back to c's Commit record (D1's unkillable fixture) -> 33, 43, caught; G2-2 the retire write dropped from §14.2's read-error row -> 32, 43, caught; G2-3 the retire write dropped from §12.5 "Read errors" -> 20, 43, caught; G2-4 SHORT-SEG-ERROR's gloss back to "short" -> 34, 43, caught; G2-5 I-WAL-STALE's read-error clause reworded -> 35, 43, caught; G2-6 the wal_seg_size row moved below its neighbour -> 32, caught. Named mutants re-walked as text on the reworked fixtures: COVER-AT-EOF -> t_wal_loss_below_ckpt_end short-segment variant (RW-4), caught; SHORT-SEG-ERROR -> t_ro_open_disk_full under the corrected gloss, caught; the rest unchanged from gate 1 (History). Gate 2's own design mutant: G2-7 a pass 2 that takes a WAL read error as the end of its walk and opens -> UNCAUGHT (no named test makes a read error that only pass 2 meets; S-ETA-F6 (c)). The worktree was never written before this commit
Followups: S-ETA-F1, S-ETA-F2, S-ETA-F3, S-ETA-F4, S-ETA-F5, S-ETA-F6

## Verdict in brief

**MERGE-WITH-FOLLOWUPS.** The rework is exactly RW-1 to RW-7 and nothing else, the card now matches gate 1's lists, and D1 to D4 are closed. Nothing found here meets the blocking bar: no text leads a builder to a wrong build, no test is made unpassable, and no named mutant is left unkillable.

One new non-blocking finding, S-ETA-F6. RW-1 fixed the bound on what a read error leaves written in §12.5, §14.2 and the Changelog. Two summaries this card wrote still say "writing nothing" without the bound: §15 I-WAL-STALE's row and the head status line. §14 step 4 names no read error, and no test makes a read error that only pass 2 meets. The normative text (§12.5 "Read errors", §14.2's row, §14 step 4a "Retiring meta `k`") is right and agrees with itself, so a builder who follows it builds the right thing. F6 gives exact riders for the next S-STORE card.

**Merge timing.** This card may not merge before P0 PASS unless the owner says so in their own words.

## Gate run
- **Scaffold extract** (GATE-PROMPT step 1): a shallow `--no-local` clone of `nucleus/v3-scaffold` printed **71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b**. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **The run:** `bash "$fg/fresh-gate.sh" 901e85ddf94dd4d68dc8652795c880b5a8451582`, from `~/Workspace/neutron-v3`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate2-S-ETA` and both streams captured to files. Its lines:
  - `fresh-gate: commit 901e85ddf94dd4d68dc8652795c880b5a8451582`
  - `fresh-gate: base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `fresh-gate: checks passed (4875 files hashed in 6.04 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`
  - The literal last line was `GATE EXIT=0`, the exit status 0, and stderr empty. The known `spikes/e1-arm-c` `undo_order.rs` flake did not fire, so no re-run was needed. `$fg` was removed afterwards.
- **The worktree** `~/.cache/nv3-cards/S-ETA`, branch `card/v3-S-ETA`, was clean at tip 901e85dd before this commit. `git status` was empty, and `git ls-files -v` showed no assume-unchanged or skip-worktree entry. Every Acceptance run and mutant used a `--no-local` scratch clone under `~/.cache/nv3-review/gate2-S-ETA/`.
- **The branch:** 71ecf3e6 → becd0afc (the card) → 3e6e999c (gate 1's REWORK verdict) → 901e85dd (rework 1, `docs/S-STORE.md` only, 7 lines changed) → this verdict.

## Scope 1: the rework is RW-1 to RW-7 and nothing else
- **The diff.** `git diff --stat becd0afc 901e85dd` shows `docs/S-STORE.md` (+7/−6) and gate 1's verdict file, which 3e6e999c added. 901e85dd itself touches only `nucleus/v3/docs/S-STORE.md`. Against the scaffold, `docs/S-STORE.md` is +63/−28.
- **The bytes.** I wrote my own parser for gate 1's "Corrections RW-1 to RW-7" section. I applied the seven blocks to becd0afc's committed `docs/S-STORE.md` (sha256 `91b40716…11e4b8`). Each Old occurred exactly once, in file order. The result is byte-identical to 901e85dd's file: sha256 `3a75fb626065ff4b8081ecbc6eba3105c6e0fff06d92809512026ed47ecb9b79`. That matches gate 1's dry run `~/.cache/nv3-review/gate-S-ETA/corrected.md` and the implementer's report.
- **The card's Acceptance.** I parsed the list with `tools/run_card.py`'s `parse_card` regex from the reworked card and from the pre-rework backup (`~/.cache/nv3-review/S-ETA.card-backup-pre-rework1.local.md`). Both have 45 commands. Replacing lines 6, 20, 22, 32, 33, 34, 42 and 43 of the backup with gate 1's eight lines gives the reworked list exactly: 0 mismatches. The last command is `cd nucleus/v3 && bash scripts/gate.sh`, which is `GATE_CMD`.
- **The card's own blocks.** My fence-aware parser of the card's `## Edits` section read 37 blocks: E1, W1 to W20, W20a, W21 to W23, W23a, W24 to W33, E2. Applied in order to the base 7.15 (sha256 `a1376c94…700f`), with `<date>` = 2026-11-17 (the commit date of both becd0afc and 901e85dd), they give sha256 `3a75fb62…9b79`, 901e85dd's file.
  - Every Old occurs once in the base.
  - Every New occurs once in the result.
  - The reverse (E2 down to E1, New to Old) gives the base byte for byte.
  - O1 to O12 (OP-1's riders) each occur once, at their turn, in the result.
- **The card's prose** carries every point of gate 1's "Card text the rework must update":
  - The Edits order line, with W20a and W23a.
  - R-1's "What has been written by then", now with pass 2's walk and the retire write.
  - R-7's fix, "For the gate" item 4 and the Mutation evidence, now naming the `Chunk` after the last commit.
  - Limits 2, now with W20a and W23a.
  - The draft target, +63/−28.
  - The reverse, the dry run, the omit-one table and the merge-order note, each marked as re-checked after rework 1.
  - The card's "What changes, in plain words" and the cases list did not need to change: gate 1 kept R-1's cases 1 to 4 for pass 1.

## Scope 2: Acceptance, checker and omit-one
- **Acceptance.** All 44 commands before `gate.sh` were run as the runner runs them: `/bin/sh -c` from the root of a clean `--no-local` clone at 901e85dd, with `env -i`, `PATH=/usr/bin:/bin`, and a local `nucleus/v3-scaffold` at 71ecf3e6. They pass **44 of 44 under `LANG=en_CA.UTF-8` and 44 of 44 under `LC_ALL=C`**. Line 45, `gate.sh`, is the fresh gate above.
- **The checker** (line 41) prints:
  - `166 rows; 234 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`
  - `65 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 8 here), 65 of them judged on a Phase A row; 47 invariants, 52 tests`
  - `OK`
- **The new list rejects becd0afc.** On becd0afc as it stands, lines 6, 20, 22, 32, 33, 34, 42 and 43 fail, in both locales.
- **Omit-one, for each RW.** Each variant was built with my parser from becd0afc's file, leaving one block out, and committed on its own branch off 71ecf3e6. It was run in both locales, and the locales agreed every time:

  | Left out | Lines that fail |
  |---|---|
  | RW-1 | 20, 22, 43 |
  | RW-2 | 32, 42, 43 |
  | RW-3 | 22, 32, 43 |
  | RW-4 | 22, 33, 43 |
  | RW-5 | 22, 34, 43 |
  | RW-6 | 6, 43 |
  | RW-7 | 6, 43 |

  This is gate 1's table exactly.
- **Planted errors** (gate 2's own), on the reworked file, each committed off 71ecf3e6 and run in both locales:

  | # | Fault | Lines that failed |
  |---|---|---|
  | G2-1 | RW-4's lost record back to "`c`'s `Commit` record", the fixture D1 showed cannot fail COVER-AT-EOF | 33, 43 |
  | G2-2 | §14.2's read-error row without "once meta `k` is retired (step 4a), the retire write" | 32, 43 |
  | G2-3 | §12.5 "Read errors" without the retire write | 20, 43 |
  | G2-4 | SHORT-SEG-ERROR's gloss back to "missing, or short" | 34, 43 |
  | G2-5 | I-WAL-STALE's read-error clause reworded | 35, 43 |
  | G2-6 | the `wal_seg_size` row swapped with the row below it | 32 |

  G2-6 escapes line 43, because swapping an added line with an unchanged neighbour leaves the `-U0` diff body the same. Line 32's adjacency check catches it, which is what that check is for.

## Scope 3: the reworked text as recovery design
I re-read §12.5 "Short segments", "Read errors" and "Malformed records", §14 steps 1 to 5 with the fence, step 4a, "Retiring meta `k`" and the explicit rollback, §14.1, §14.2, and the §15 rows I-WAL-COVER, I-RO-OPEN and I-WAL-STALE. Then I searched the whole file for each changed rule: "writing nothing" and "writes nothing", 58030, "nothing written beyond", SHORT-SEG-ERROR, "short or missing", "short-segment variant", `**short segment**`, 212 and `wal_seg_size`, and the retire write.

- **D1, closed.** The variant's lost record is now a `Chunk` of a transaction that never commits, appended after the last `Commit` or `CommitMarker` before the flip. I walked it through both coverage conditions:
  - The walk reads every commit up to the one that carries `ts_ckpt_hi`, so `max(T, largest commit_ts) ≥ ts_ckpt_hi`, and the second condition holds.
  - It stops on the lost record's zeros by the magic check, at a position below `wal_ckpt_end`. It never reads past the end-of-file, so no §12.5 short-read damage fires.
  - So the build gives XX001 from the first condition. COVER-AT-EOF compares the end-of-file, which the test placed at or above `wal_ckpt_end`, so the check passes and the open succeeds. The mutant is caught.
  - The fixture is buildable. A never-committing transaction's chunk is below `wal_ckpt_end` and synced by checkpoint `k`'s step 1, and an open chunked transaction only lowers `wal_start_lsn` (§12.6), so the walk still starts at or before the lost record.
  - I-WAL-COVER's row ("under COVER-AT-EOF, the short-segment variant opens"), §12.5's mutant line and the Changelog bullet agree with it.
- **D2, closed.** SHORT-SEG-ERROR's only gloss, I-RO-OPEN's, now says a hopped segment that is missing or too short for a header at its first LSN. That matches §12.5 "Short segments" and line 932 ("even when that segment is short or missing", which is about a hop).
  - `t_ro_open_disk_full` stops a writer with 53100 at a segment's creation and at its preallocation, which leaves exactly such a hopped segment, so the mutant stays killed.
  - The only other "short or missing" texts are the 7.x Changelog history and R21's quoted plan wording. Both are history.
- **D3, closed in the normative text.**
  - **§12.5 "Read errors"** (RW-1) now bounds the writes for every open:
    - Pass 1 from meta `k` writes nothing. Step 1's fence is lazy ("an open refused before its first write fence[s] nothing"), and pass 1 evicts nothing.
    - A fall-back's or a rollback's pass 1 from meta `k−1` can follow the fence and image `k`'s pass-2 evictions. The retire write comes after pass 1, because item 4 retires after the proof, and a rollback "falls back as a proven fall-back does".
    - Pass 2 from meta `k` can follow the fence and its own evictions.
    - Pass 2 from meta `k−1` can follow both passes' evictions and the retire write.
  - Each case fits "at most the fence, pass 2's evictions and, once … retired, the retire write". This agrees with step 4a's "Only the fence (step 1) and the pass-2 evictions made from image `k` before it failed can precede the retire write".
  - "An open that later reads those bytes recovers as if the error had not happened" holds after a retire too. The next open selects meta `k−1` as the newest, and its walk is the one the proof, or the rollback, had already accepted.
  - §14.2's row (RW-3) and the Changelog (RW-6) say the same.
  - §14 step 3's own sentence ("writes nothing, or, in a fall-back from an image `k` that failed in pass 2, nothing more than the fence and that pass 2's evictions") is about pass 1, and it is true for pass 1.
  - Two summaries still say "writing nothing" without the bound: §15 I-WAL-STALE's row and the head status line. Step 4 names no read error. No test makes a read error that only pass 2 meets. Each is non-blocking (below), and S-ETA-F6 covers them.
- **D4, closed.** §14.2 now has the `wal_seg_size < 212` row right after step 1's range-check row: XX001 with nothing written, read-write and read-only. That matches W5's open check. The scaffold is still at 71ecf3e6, so S-ZETA has not merged. If it merges first, its well-formed row and this row agree, as gate 1 said.
- **No new defect in R-1 to R-7.** The rework changed only sentences about write bounds, one fixture and one gloss. None of R-1 to R-6's rules moved. Gate 1's walk of them (History) stands.

### Why F6 is not blocking
- **"Writing nothing" in I-WAL-STALE's row and the status line.**
  - Both are summaries that point to §12.5. §12.5, §14.2, step 4a and "Retiring meta `k`" all state the bound explicitly and agree with each other.
  - No build can honour the literal summary after pass 2 has evicted. Pass 2 must evict at a minimum pool (§5.4), and the retire write must precede pass 2 from meta `k−1`. So the summary cannot steer a builder toward a different correct-looking build.
  - No named test asserts byte-identity after a pass-2-only read error. `t_wal_read_error` (c) asserts "nothing more than the fence and the pass-2 evictions", which agrees with RW-1.
  - The same phrase was in becd0afc's I-WAL-STALE row (W26), where it was also loose for a fall-back's pass 1. Gate 1's search did not list it.
- **A read error that only pass 2 meets** (G2-7).
  - A build whose pass 2 takes the walk's error for an end and opens would lose acked commits behind it. Its boot checkpoint would move `wal_start_lsn` past them.
  - RW-1's sentence forbids this. But every named case of `t_wal_read_error` fails the read in pass 1 first, so that form is uncaught.
  - It is not a named mutant: WAL-READ-ERROR-AS-END is killed by its pass-1 cases.
  - A-6b's walk is the one walk both passes use, and its hook test pins that the walk returns the error. Only an A-7d pass-2 driver that swallows the error escapes.
  - The test belongs to A-7d's card or the next S-STORE card (F6 (c)).

## The P0 question
Gate 1's P0 facts (History) are carried unchanged. Rework 1 changed no rule they rest on:
- (i) F-3 is not a defect under the given definition, but an under-specification with an inconsistency.
- (ii) The recycling target is a defect at the level of the spec's claims, reachable only under a reading no planned build takes.
- (iii) F-1 is an under-specification only.

I did not re-derive the two-copy-fault lowest-free-name path; gate 1 marked it as reasoned, not modelled. This gate does not rule on what P0 should conclude.

## Owner point OP-1 (not decided here)
Carried from gate 1 unchanged: should a read-only open serve salvage before a WAL read error? The card and this verdict keep 58030 for every open.
- O1 to O12 still apply once each, at their turn, to 901e85dd's file.
- If the owner says yes, S-ETA-F6 (a)'s I-WAL-STALE rider and O11 rewrite the same clause. Whichever lands second must carry the other's text: O11's "writing nothing" then also takes RW-1's bound.

## Follow-ups
S-ETA-F1 to S-ETA-F5 are gate 1's and are carried unchanged (History, "Follow-ups"). I re-checked each against 901e85dd:
- F1 (a)'s "at the first LSN of" wording is still in step 3, W17's exception and R21.
- F1 (b) still holds: (c)'s hooked record lies past meta `k`'s `wal_ckpt_end`.
- F2 to F5 are untouched by the rework.

- **S-ETA-F6, riders for the next S-STORE card (none blocks).** Each Old below occurs exactly once in 901e85dd's `docs/S-STORE.md`. Applied in this order in a scratch copy, they apply once each, in file order.
  - (a) §15 I-WAL-STALE: the read error's write bound.
    Old: `a read error fails the open with 58030 in both modes, writing nothing, and`
    New: ``a read error fails the open with 58030 in both modes, writing nothing more than the open had written when the read failed (nothing in pass 1 from meta `k`), and``
  - (b) The head status line, if the next card keeps 7.16's entry in it.
    Old: `fails the open with 58030 in both modes, writing nothing;`
    New: `fails the open with 58030 in both modes, writing nothing more than it had written when the read failed;`
  - (c) §14 step 4 and `t_wal_read_error`: pass 2's read error.
    - Text. Old: ``A chunk whose transaction has no marker is ignored. Records with `commit_ts ≤ T` are skipped: they are in the image.`` New: the same, then `A read error in this walk fails the open with **58030** as in step 3, after no more than the fence, pass 2's own evictions and any retire write before it (§12.5, "Read errors").`
    - Test. Add to `t_wal_read_error` at A-7d a case (d): a hook that lets pass 1 read one synced record past `wal_ckpt_end` and fails pass 2's read of it, with a pool small enough that pass 2 evicts first. The read-write open fails with 58030, every meta place and WAL file is byte-identical, every slot written is one that neither image references, and the next open, without the hook, recovers every acked commit. In a fall-back form, the same is done after the retire write, and the next open selects meta `k−1` and recovers every acked commit.
    - The case kills G2-7, a pass 2 that takes the read error as its walk's end and opens. The test addition is A-7d's, or the next S-STORE card's if it adds a mutant name.
  - (d) §14 step 3's sentence covers an explicit rollback that a pass-2 read error let proceed only through "falls back as a proven fall-back does". Optional wording: "a fall-back from an image `k` that failed in pass 2, or a rollback that a read error in pass 2 let proceed". If OP-1 lands, O4 rewrites that sentence and must carry this too.
  - Before the merge, S-ETA-F6 needs a QUEUE entry (`- S-ETA-F6 | …`), as F1 to F5 do; `merge-card.sh` checks each named follow-up against QUEUE.

## Not verified
- No A-6b, A-7c, A-7d or A-7f code exists. Every "caught" for a named test was walked as text, as in gate 1.
- **Merge orders.** I did not re-run the S-ZETA or R-1-SPEC orders. The card says its author re-checked all six after rework 1; gate 1 checked the S-ZETA-first and R-1-SPEC-first paths with RW-1 to RW-7. RW-2's row and S-ZETA's well-formed row were not checked together on a composed file here.
- **OP-1's riders' semantics** against RW-1's new pass-2 sentence. I checked only that they apply.
- Gate 1's P0 point (ii) two-copy-fault path, OPFS length durability (F4), E4's `truncate_wal` and A-6b-0's `DirLoss` remain as gate 1 left them.

## History

- **Gate 1: REWORK, verdict commit 3e6e999cddef9d052f99d11b49f40f36815f8f38, on becd0afcd40d6d18252606a989c48f62c5eb0d67.**
  - **The gate.** A P gate in Claude Code session 40cbaf78-508d-49e9-b95d-3edb21da8347, with GATE EXIT=0 on becd0afc.
  - **Findings.** D1, COVER-AT-EOF unkillable under the variant's natural fixture; D2, a stale SHORT-SEG-ERROR gloss; D3, "writes nothing" false for pass 2 and after a retire write; D4, no §14.2 row for the `wal_seg_size` floor.
  - **The fix.** RW-1 to RW-7 and eight replaced Acceptance lines (6, 20, 22, 32, 33, 34, 42, 43), with dry-run sha256 `3a75fb62…9b79`. Follow-ups S-ETA-F1 to S-ETA-F5, owner point OP-1, and the P0 facts (i) to (iii).
- **The rework: commit 901e85ddf94dd4d68dc8652795c880b5a8451582** (a GLM 5.3 implementer). It applied RW-1 to RW-7 to `docs/S-STORE.md` only, giving sha256 `3a75fb62…9b79`. The card was updated in the main checkout (cards are local); its pre-rework copy is `~/.cache/nv3-review/S-ETA.card-backup-pre-rework1.local.md`. Reported: 45 of 45 Acceptance in both locales, `GATE EXIT=0`.
- **Gate 2 (this verdict): MERGE-WITH-FOLLOWUPS on 901e85dd.**

**Gate 1's verdict, as committed at 3e6e999c, verbatim.** Its header lines carry a "- " prefix, so that no tool reads them as this verdict's fields. Everything after them is unchanged, headings included. Where its sections ("Merge timing", "Follow-ups", "Owner point OP-1", "The P0 question") differ from the sections above, the sections above are current. The same text is at `git show 3e6e999c:nucleus/v3/verdicts/S-ETA.md`.

<!-- gate 1's verdict (3e6e999c), verbatim, begins -->

- Card: S-ETA
- Class: spec
- Card-gate: P
- Verdict: REWORK
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: T3 review sub-agent, Claude Code session 40cbaf78-508d-49e9-b95d-3edb21da8347 (an independent P gate; this agent wrote neither the card S-ETA, its implementation becd0afc, card A-6b nor its findings, and is not the orchestrator)
- Date: 2026-11-17
- Gate-exit: GATE EXIT=0
- Commit: becd0afcd40d6d18252606a989c48f62c5eb0d67
- Mutants: spec text, none in code; each named mutant walked as text against its test: WAL-READ-ERROR-AS-END -> t_wal_read_error (A-6b hook; A-7d (a)-(c)), caught; WAL-MALFORMED-AS-END -> t_wal_malformed (A-6b streams; A-7d first run), caught; START-SEG-SHORT-AS-END -> t_wal_start (A-6b short start; A-7d (e)), caught; SHORT-READ-AS-END -> t_wal_short_read (A-6b damage cases; A-7d (a)), caught; SHORT-HOP-AS-DAMAGE -> t_wal_short_read (A-6b stop cases; A-7d (b)), caught; SEG-LEN-NOT-DURABLE -> t_seg_ack_durable (A-6b P3 rounds), caught; SEG-SIZE-UNCHECKED -> t_wal_seg_size (A-7c creation form caught; A-7d open form caught only if the walk's own Invalid is not mapped to XX001, S-ETA-F2); RECYCLE-TARGET-LOW -> t_wal_start (A-6b two runs; A-7d (f)), caught; COVER-AT-EOF -> t_wal_loss_below_ckpt_end short-segment variant as committed: UNCAUGHT when the lost record is the main fixture's c (the coverage check's second condition gives XX001 for build and mutant alike), caught after RW-4. Gate's own: G1 a read-only open that serves salvage at a WAL read error -> t_wal_read_error (b), caught; G2 an explicit rollback that answers 55000 when pass 1 from meta k meets a read error -> (b), caught; G3 a hopped segment's first record whose body runs past the end-of-file taken as a stop -> t_wal_short_read A-6b, caught; G4 a step 5 that shortens the valid end's segment and grows it back -> UNCAUGHT (no named test crashes inside step 5; S-ETA-F1 (e)). Text mutants, judged by the card's 44 non-gate Acceptance lines in both locales in scratch clones: 35 of 35 omit-one-block variants fail their own lines and line 43; 12 of 12 planted errors fail; on the reworked file each of RW-1 to RW-7 left out fails its own lines and line 43. The worktree was never written
- Followups: S-ETA-F1, S-ETA-F2, S-ETA-F3, S-ETA-F4, S-ETA-F5

## Verdict in brief

**REWORK**, with seven exact blocks, RW-1 to RW-7, and eight replaced Acceptance lines. The gate tried all of them in a scratch clone. The rules themselves stand: R-1 to R-6 lose no acknowledged commit silently, and no rule refuses a state that a correct build leaves after an ordinary crash or a power cut with honoured syncs. The defects are in a test fixture and in what the text claims around the rules:

1. **COVER-AT-EOF can still escape (W22; RW-4, RW-7).** R-7 exists to make this mutant killable, but the reworked variant names no lost record. The main fixture's record is `c`, a commit above `T`. If that record is the one lost, the walk stops below `ts_ckpt_hi`, the coverage check's second condition fails, and the build and the mutant both give XX001. That is a named mutant unkillable under the variant's most natural fixture.
2. **A stale restatement of the old rule (RW-5).** I-RO-OPEN's gloss of SHORT-SEG-ERROR still says that "a short or missing segment" ends the valid end. Under 7.16, that holds only for a segment entered by a hop, at its first LSN.
3. **"Writes nothing" is false for pass 2 (RW-1, RW-3, RW-6).** Pass 2 reads the WAL again (§14 step 4). A read error that only pass 2 meets comes after the fence and pass 2's evictions, and in a fall-back or an explicit rollback after the retire write. §12.5's new "Read errors" paragraph and §14.2's new row say less than that.
4. **§14.2 is missing the new open refusal (RW-2).** It has no row for an identity whose `wal_seg_size` is below 212, so its "Otherwise: opens meta `k`" row covers a file that W5 refuses.

Mechanically, the commit is exactly the card:
- My own parser reproduces it byte for byte from the base.
- It touches one file.
- 44 of 44 Acceptance lines pass in both locales, and the checker ends `OK`.
- The fresh gate gives `GATE EXIT=0`.

On the P0 question (below):
- F-3 is not a defect at 7.15 under the given definition. The loss needs a device that loses a length after its sync, which is outside I-REDO-COMPLETE's assumption and inside §19 T4's documented loss. It is an under-specification and an inconsistency: the same file's lost directory entry is detected.
- The recycling target is a defect at the level of the spec's claims. 7.15's text admits a lowest-free-name build, and §2.2 and §14 say the resulting state is never silent. No planned build takes that reading.
- F-1 is an under-specification only.

## Gate run
- **Scaffold extract** (GATE-PROMPT step 1): a shallow `--no-local` clone of `nucleus/v3-scaffold` at **71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b**. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **The run:** `bash "$fg/fresh-gate.sh" becd0afcd40d6d18252606a989c48f62c5eb0d67`, from `~/Workspace/neutron-v3`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-ETA` and both streams captured. Its lines:
  - `fresh-gate: commit becd0afcd40d6d18252606a989c48f62c5eb0d67`
  - `fresh-gate: base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `fresh-gate: checks passed (4874 files hashed in 4.89 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`
  - The literal last line was `GATE EXIT=0`, the exit status 0, and stderr empty. `$fg` was removed afterwards.
- **The worktree** `~/.cache/nv3-cards/S-ETA`, branch `card/v3-S-ETA`, was clean at tip becd0afc before this commit.
  - `git status` was empty.
  - `git ls-files -v` showed no assume-unchanged or skip-worktree entry.
  - Its `docs/S-STORE.md` hashed to the committed blob.
  - Every Acceptance run, mutant and rework trial used scratch clones under `~/.cache/nv3-review/gate-S-ETA/`.
- **Worker logs:** none exist under `cards/logs/` for S-ETA. The implementer's report (45/45 Acceptance, `GATE EXIT=0`) came from the brief. I re-ran everything it claims.

## Scope and bytes
- **The diff.** `git diff --stat nucleus/v3-scaffold...HEAD` lists one file, `nucleus/v3/docs/S-STORE.md`, +62/−28. That is the card's Touch only list, and no protected path is touched.
- **My parser** is fence-aware and reads only the `## Edits` section, so W34 and O1 to O12 are excluded.
  - It read 35 blocks, E1, W1 to W33 and E2, and applied them in order to the base 7.15 (sha256 `a1376c94…700f`), with `<date>` set to 2026-11-17.
  - Every Old occurred exactly once at its turn, and once in the base.
  - The result is byte-identical to becd0afc's file: sha256 `91b407161c03eb77a83b5b857c78ecb10db7e8c63dd8b9117e4dac94a211e4b8`.
- **Acceptance.** I took the 45 commands with the runner's own `parse_card` regexes and ran the 44 before `gate.sh` with `/bin/sh -c`, `PATH=/usr/bin:/bin`, from a clean `--no-local` clone at becd0afc whose `nucleus/v3-scaffold` is 71ecf3e6.
  - They pass 44 of 44 under `LANG=en_CA.UTF-8` and 44 of 44 under `LC_ALL=C`.
  - The checker printed `166 rows; 234 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, then `65 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 8 here), 65 of them judged on a Phase A row; 47 invariants, 52 tests`, then `OK`.
- **Omit-one-block**, all 35 blocks, each committed on its own branch off 71ecf3e6 and run in both locales. The locales agreed every time, and every omission failed its own line plus line 43, and line 42 where the line count moves:
  - E1: 1, 2, 42, 43. W1: 7, 42, 43. W3: 9, 22, 40, 42, 43. W5: 11, 40, 42, 43. W6: 12, 22, 40, 42, 43.
  - W7: 13, 22, 42, 43. W8: 14, 40, 42, 43. W10: 16, 40, 42, 43. W11: 17 to 22, 40, 42, 43.
  - W13: 24, 42, 43. W16: 22, 27, 42, 43. W20: 22, 31, 42, 43. W21: 32, 42, 43. W22: 22, 33, 42, 43.
  - W23: 22, 34, 43. W25: 22, 34, 40, 41, 43. W26: 22, 35, 40, 41, 42, 43. W27: 22, 36, 40, 41, 42, 43.
  - W28: 22, 37, 40, 41, 42, 43. W30: 37, 38, 40, 41, 42, 43. W33: 22, 39, 42, 43. E2: 3, 4, 5, 6, 42, 43.
  - The rest of W2 to W32 were likewise caught by their own line.
- **Planted errors**, each one fault in the committed file, committed off 71ecf3e6 and run in both locales (the lines that failed):

  | # | Fault | Lines that failed |
  |---|---|---|
  | PE1 | §12.5 "Read errors": 58030 → XX001 | 20, 43 |
  | PE2 | §14.2 read-error row, the read-only cell → "salvage at the read" | 32, 43 |
  | PE3 | "Short segments": "and never damage" dropped | 17, 43 |
  | PE4 | §12.1's floor 212 → 211 | 11, 43 |
  | PE5 | SHORT-HOP-AS-DAMAGE dropped from `t_wal_short_read`'s A-7d row | 37, 40, 41, 43 |
  | PE6 | `t_wal_seg_size`'s creation row moved A-7c → A-7b | 37, 38, 41, 43 |
  | PE7 | 7.15's short-segment stop restated in §14.1's "What it skips" | 42, 43 |
  | PE8 | step 3: "at the first LSN of" a hopped segment → "in" one | 24, 43 |
  | PE9 | the Changelog's count 234 → 233 | 5, 6, 43 |
  | PE10 | RECYCLE-TARGET-LOW dropped from I-WAL-START's §15 row | 36, 40, 41, 43 |
  | PE11 | W22's variant back to 7.15's fixture | 22, 33, 42, 43 |
  | PE12 | recycling target "above the append position" dropped | 12, 43 |

- **Merge orders.** I applied the 35 blocks to S-ZETA's committed file (card/v3-S-ZETA, 5c62470b) and to R-1-SPEC's (card/v3-R-1-SPEC, 32f68472). Every Old occurred once at its turn on both, W34's Old occurs once in S-ZETA's file, and RW-1 to RW-7 then apply once on both results.
  - I did not check all six three-card orders.
  - **Not verified:** the card's "checked by script" claim for the orders I did not run.

## Findings (REWORK)

### D1. COVER-AT-EOF is not killable under the variant's natural fixture (W22; RW-4, RW-7)
R-7's argument about 7.15's fixture is right, and I re-derived it:
- `wal_ckpt_end` is a record boundary, so a record that runs past an end-of-file lies wholly below it or wholly at or above it.
- The valid end and the end-of-file therefore fall on the same side of it, and the mutant changes no outcome.

The reworked fixture fails for another reason. Coverage has two conditions, and the variant controls only the first:
- The variant reads "a synced record below `wal_ckpt_end` lost after its sync". The test's main fixture is "a commit `c > T` partly applied before the flip ... then a fault on `c`'s record".
- If the lost record is `c`'s, or any record before the last `Commit` or `CommitMarker` that precedes the flip, the walk stops before reading the commit that carries `ts_ckpt_hi`. Every commit it does read has a smaller ts, by I-WAL-TS-ORDER, and `T < c ≤ ts_ckpt_hi`.
- So `max(T, largest commit_ts read) < ts_ckpt_hi`, and the second condition gives XX001 whatever the first compares. COVER-AT-EOF changes only the first condition, so it gives XX001 too, and the variant cannot fail it.
- The variant fails COVER-AT-EOF only when the lost record lies after the last commit before the flip. Then the walk has read up to `ts_ckpt_hi` and only the first condition decides.
- A `SegmentEnd` cannot be that record. After a `SegmentEnd`, `wal_ckpt_end` lies in a later segment, so the cut-short file's end-of-file cannot reach it.

RW-4 names the record: a `Chunk` of a transaction that never commits, appended after the last commit before the flip. Then:
- The build's valid end, at that chunk, lies below `wal_ckpt_end`, so the build gives XX001.
- The mutant's end-of-file lies at or above it, so the mutant opens.

RW-7 brings the Changelog bullet in line.

### D2. A stale restatement of the 7.15 rule (RW-5)
§15 I-RO-OPEN glosses SHORT-SEG-ERROR as "(a short or missing segment returns 58030 instead of ending the valid end)". Under 7.16:
- A short or missing **start** segment is XX001.
- A short read after an accepted record, or in a body, is XX001.

Only a segment entered by a hop that is missing, or too short for a header at its first LSN, ends the walk. The fixture of `t_ro_open_disk_full` stops a writer with 53100 at a segment's creation and at its preallocation. Both leave such a hopped segment, so the mutant stays killed under the corrected gloss.

Acceptance line 22's stale-phrase list missed this sentence. The new line 22 adds it and three more.

### D3. A WAL read error in pass 2 (RW-1, RW-3, RW-6)
R-1's "What has been written by then" assumes the walk is pass 1. Pass 2 walks the same span again (§14 step 4). A transient error, or a sector that goes bad between the passes, can meet pass 2 alone. By then the open may have written several things:
- the fence;
- pass 2's evictions;
- in a proven fall-back or an explicit rollback, the retire write, which precedes pass 2 from meta `k−1`.

§12.5's "It writes nothing, or, in a fall-back from an image `k` that failed in pass 2, nothing more than the fence and that pass 2's evictions" is then false. §14.2's row covers step 3 only. Nothing is lost:
- The evictions go only to slots neither image references.
- A retired meta `k` leaves meta `k−1`, whose walk the proof or the rollback had accepted, to the next open.

RW-1 states the bound for every case. RW-3 and RW-6 carry it into §14.2 and the Changelog.

### D4. §14.2 has no row for the new open refusal (RW-2)
W5 makes an open of a file whose identity carries `wal_seg_size < 212` fail with XX001 and write nothing, in both modes. §14.2 has no row for it, so the "Otherwise | opens meta `k`" row covers it. RW-2 adds the row next to step 1's range check.

If S-ZETA merges first, its row for an identity that is not well-formed covers the same case once W34 applies. The two rows then agree (XX001), and the rework may keep RW-2 or drop it with its clause in line 32.

## The design, rule by rule
The rules were walked through every open:
- read-write and read-only;
- an explicit rollback;
- the fall-back to meta `k−1` with its proof.

The faults were:
- a crash at each point of creation, preallocation, recycling, step 1 and step 5;
- a power cut with honoured syncs, and a dropped sync;
- lost, torn and misdirected writes;
- transient and persistent read errors;
- a length or a directory entry lost after its sync.

The base is E-DURABLE and SimEnv's model (`crates/nucleus-env/src/sim/disk.rs`):
- `set_len` is volatile until `sync_all`.
- `sync_data` makes durable the length its data needs.
- A power cut keeps an unsynced length only as far as surviving data, rounded up to its 512-byte sector.
- A process crash keeps the page cache.

- **R-1, read errors: sound, with D3.**
  - No open can take a read error for an end, so step 5 never truncates behind it.
  - 58030 fits the catalogue: a read error is not evidence of damage, S-EPSILON's principle.
  - An explicit rollback gets 58030 when pass 1 from meta `k` meets the error, because the open stops at step 3 before its precondition can be settled.
  - Through image `k`'s failure, a rollback reaches pass 1 from meta `k−1`, which crosses the same span. A fall-back's pass 1 that meets the error retires nothing, because the proof needs the walk.
  - The read-only open's 58030 is OP-1.
  - An open of a segment path that is a directory (`IsADirectory`, E-KIND) also falls under the 58030 rule. It is not silent.
- **R-2, malformed records: sound.**
  - A-6a's `decode_body` refuses every case of `t_wal_malformed`'s A-6b form (A-6a's Work 3 and its `wal_malformed` list): kind 0, op tags 0, 7 and 255, a byte string past the payload, a byte left over, a `SegmentEnd` with `n ≠ 0`, and a `Chunk` with `commit_ts ≠ 0`. So the test is passable on a correct build.
  - The stop, chain, body order gives the lower cut for a splice that also does not decode.
  - The cost is about 2^-32 per torn record whose header survived. That is the chain's own order.
  - The wording "under §12.2 and §12.3" is narrower than A-6a's decoder, which also applies §7.1's `commit_ts` rule. See S-ETA-F1 (d).
- **R-3, short segments: sound. The enumeration holds.** I re-derived every state:
  - **Creation of a segment by the writer.**
    - Before the create: absent.
    - After the create: present and empty, or absent.
    - After `set_len`: full under a process crash; empty or absent after a power cut.
    - After `sync_all`: full, or absent.
    - After `sync_dir`: full.
  - **Recycling.** It renames a full file, and its target lies above the append position.
  - **The writer's first use of a segment present at open.** It is full.
  - **Segment 0.** It is durable before meta 0. A crash inside creation leaves no valid meta.
  - **Step 1.** It ensures the start segment, with its length.
  - **Step 5.** It zeroes in place (W16).

  Three states the card's table does not list. Each settles without a short read:
  - (a) A crashed process's page-cache segment at the valid end. Step 5's in-place zeroing and its `sync_data` make the length durable, but the directory entry is still unsynced. The writer, or the boot checkpoint's step 1, then makes it durable. A power cut first leaves it absent, or full of zeros.
  - (b) A crashed process's step-5 unlink without its `sync_dir`. The next open has nothing to remove and syncs no directory. The writer's later `CreateNew` and `sync_dir` settle it.
  - (c) A 53100 on the first write into a sparse preallocation's hole. That is a partial write inside a full-length file, so it is a stop.

  The edge cases the request named all come out the same way. Each gives a stop at a hopped first LSN, a magic, CRC or `len` stop inside a full file, or no read at all:
  - a crash between `set_len` and `sync_all`;
  - a crash between `sync_all` and `sync_dir`;
  - a recycling rename cut in the middle;
  - a segment partly zero-filled;
  - the first header torn;
  - the start segment just after creation;
  - step 5's zeroing cut short.

  **No legitimate crash state was missed.** The availability cost falls only on an Env that breaks E-DURABLE's `set_len` rule (StdEnv's `sync_all` is `fsync`; OPFS is S-ETA-F4). It also falls on a writer that skips `sync_all` (SEG-LEN-NOT-DURABLE), which is refused, not silent.

  Residuals stay a stop, as §19 T4 already documents:
  - a hopped segment whose length a device lost to under a header at its first LSN;
  - a hopped segment whose directory entry a device lost after its sync.

  W16's pin has no test. See S-ETA-F1 (e).
- **R-4, segment size: sound.**
  - 212 is the structural minimum: `place`, the walk and A-6b's `check_seg_size` all use it.
  - Creation's 22023 fits: PostgreSQL's invalid parameter value, now in the error table.
  - The open's XX001 fits, as an intact identity outside the domain, matching S-ZETA's treatment.
  - SEG-SIZE-UNCHECKED's open form is masked at A-7d by A-6b's own check. See S-ETA-F2.
- **R-5, the writer rule: sound.** SEG-ACK-BEFORE-DURABLE's new form is killed at A-6b by the crashed process's rename.
  - At A-7f, the boot checkpoint's step 1 makes the valid end's segment durable.
  - The form shows there only for a segment above the valid end that step 5 zeroed in place, so A-7f's fixture must place it there.
  - SEG-LEN-NOT-DURABLE is killed by a P3 round: the file ends at the last synced record, and the next header read is short after an accepted record.
- **R-6, recycling targets: sound.**
  - Every name from the oldest selectable start up to the append position's segment holds a live segment.
  - A meta's start never lies above the append position, even after a rollback lowers it.
  - So no rename above the append position can re-populate a recycled start segment.
  - The race with the writer's `ensure_segment` fails loudly (E-NAMESPACE `AlreadyExists`, poison). Keeping the two apart is A-6c's and A-7c's.
- **R-7: D1.**
- **Error codes and §14.2.**
  - 58030 is used for I/O.
  - XX001 is used for detected damage and the identity floor.
  - 22023 is used for creation.
  - §14.2 is complete after RW-2 and RW-3. A malformed record, a short segment or a read error met in a fall-back's pass 1 falls under the existing row "The same, and a check against meta `k−1` refuses", which applies "as if meta `k−1` were the newest", or under the read-error row.
- **Tests.** A correct build passes every new and changed form, and every named mutant fails it, with these exceptions:
  - COVER-AT-EOF (D1);
  - SEG-SIZE-UNCHECKED's open form (S-ETA-F2);
  - the precision points in S-ETA-F1 (b) and (c).

  Each test is judged on a Phase A row, as the checker confirms. Every hook is named: `Env` forgeries, SimEnv's post-sync LOST, `HookEnv` read failures and P3 rounds. No case waits on time.
- **Stale restatements.** I searched the whole of S-STORE outside the Changelog and C-T0. The search terms were: short or missing segments, end-of-file, read errors and 58030, decoding, `wal_seg_size`, recycling and renames, the writer rule, SEG-ACK-BEFORE-DURABLE, "exists durably" and COVER-AT-EOF.
  - D2 is the only stale restatement in S-STORE.
  - C-T0 restates none of the changed rules. It cites only `wal_seg_size / 4`, which is unchanged.
  - C-T6-rehost §4.3 still says "A-2/A-6a enforce it". See S-ETA-F3.

## The P0 question: the facts it needs
**(i) F-3, a start segment whose length a device loses after its sync. Not a DEFECT under the given definition. An under-specification with an inconsistency.**
- **Can 7.15 lose commits silently here?** Yes. A build that applies 7.15's "Any other segment file shorter than the position the reader needs ... ends the walk ... a stop, never 58030" to the start segment returns `Ok` after losing acknowledged commits.
  - The literal reading of "any other", after the missing-start-segment paragraph, does exactly that.
  - So does A-6b's walk as carded (Work 7 steps 3b and 3e).
  - I walked the card's six steps. At a quiescent flip, coverage passes (`y ≥ p = wal_ckpt_end`, `T = ts_ckpt_hi`), and step 5 zeroes the rest of `s` and zeroes or removes `s+1` and the intact segments after it.
- **Inside or outside the assumption?** Outside. `sync_all` had made the length durable before meta `k` (E-DURABLE, §13 step 1), and I-REDO-COMPLETE assumes "a device that keeps what a completed sync covered". The loss is inside the silent loss 7.15 already documents:
  - §14 "Post-sync damage" says of damage at or above `wal_ckpt_end`: "lost without detection, and step 5 truncates any intact records behind it".
  - §19 T4 lists "damage the walk stops at, above `wal_ckpt_end`".
- **Is 7.15 consistent?** No. I-WAL-START treats the same file's directory entry, lost after its sync ("or it was lost itself"), as detected damage. It does so on the evidence that the segment was durable before the meta, and that evidence covers the length as much as the entry.
- **Can an ordinary crash cause it?** No. No ordinary crash or power cut with honoured syncs leaves a short start segment.

**(ii) The recycling target. A DEFECT at 7.15 at the level of the spec's claims, under the given definition, reachable only under a reading that no planned build takes.**
- **The text admits the build.** 7.15's §12.1 says only "renaming it to a new first LSN". A build that takes the lowest free name keeps every stated rule: lowest first, each rename durable before the next, and E-NAMESPACE's no-replace.
- **The scenario.** I walked W10 (f):
  - Checkpoint 2's step 6 renames segment 0 above.
  - Checkpoint 3's step 6 renames segment `s` to the name 0 and segment `2s` to `s`.
  - Both copies of metas 2 and 3 are lost after their syncs.
  - The open selects meta 1, whose start segment is present with segment `2s`'s bytes. The walk stops at `wal_start_lsn`, a magic, CRC or `lsn` stop depending on the offset.
  - At meta 1's quiescent flip, coverage passes, and the open returns `Ok` with every commit after that flip gone.
- **The spec claims this state is never silent.** §2.2 ("a recycled start segment fails I-WAL-START ... Such an open is never silent") and §14 "Post-sync damage" ("Both copies of the two newest metas: ... None is silent") both say so. §12.1 also derives "while a segment is present, no higher segment has been recycled", which this build breaks.
- **Fault count.** In S-ALPHA's model each copy is one fault (verdict b8f51964, "Each fault counts as one"), so W10 (f) is four copy faults, or two dropped meta syncs.
  - S-ALPHA's own two-fault state also opens outside the two generations: both copies of `L` lost, and a crash tearing `L+1`'s in-flight copies over `L−1`'s places, so the open selects `L−2`.
  - S-ALPHA called that open "detected per read (I-INTEGRITY, I-WAL-START)".
  - A lowest-free-name build re-populates `L−2`'s start segment within `L`'s step 6 whenever that run has two or more sources: the second takes the first's freed name. Only I-INTEGRITY on image `L−2`'s freed slots is then left, and it holds only if checkpoint `L+1`'s step 2 overwrote them.
  - So S-ALPHA's "no silent loss at 0, 1 or 2 faults" holds for builds that rename above the append position: its own model, E4, and A-6b's Work 6. It does not hold for every build 7.15's text allows.
  - **Not verified:** this two-fault path is reasoned, not modelled.

**(iii) F-1, a read error during the walk. An under-specification, not a defect.**
- **The text says nothing about it.** §14 step 4a defers the case to "those steps and §9.2", and none of them names a WAL read.
- **No conforming reading makes the error a stop.**
  - The stop list is exhaustive.
  - The short-or-missing sentence speaks of a file "shorter than the position the reader needs, or missing".
  - In the Env contract, "missing" is `NotFound` (E-OPEN), and `read_at` is short only at end-of-file (E-IO).
  - So a build that ends the walk at an I/O error invents a stop that the text does not give.
- **Where the hazard lies.** The card's hazard, a build that takes an open's I/O error after a hop for a missing segment, is real for such a build, and it lies inside the device assumption. It is not a reading the text licenses.
- **What is unsaid:** the outcome in each mode, for the rollback and for the fall-back's pass 1, and what has been written.

The same reading gives F-2 as an under-specification with no silent loss. F-4 and F-5 are not losses. This gate does not rule on what the P0 review should conclude.

## Point 5: P-1, S-ZETA, and R-5's "durable"
- **P-1: R-3 for every segment, as written.**
  - The evidence is the same for every segment a record was written in.
  - The enumeration shows that the broad rule refuses no state a correct build leaves.
  - The narrow rule would leave a lost length in a later segment as a clean stop, so step 5 could still destroy intact later segments.
- **S-ZETA's "`wal_seg_size` not 0" against 212.** They mean the same thing once both say 212, and the card's Notes reach that in either order:
  - S-ZETA first: W34 changes its domain to "at least 212 (§12.1)". Its Old occurs once in 5c62470b's file.
  - S-ETA first: S-ZETA's Z2 New says the same, and its table's `wal_seg_size` 0 stays out of the domain.
  - Both checks then give XX001 for the same files.
  - S-ZETA's "a change to a domain is a later format version" governs released files. No file exists, so W34 changes the draft domain of `format_version` 1, not the format.
  - In the S-ZETA-first order, RW-2's row duplicates S-ZETA's well-formed row (D4).
- **R-5's "durable": sound as written.**
  - **Why an accepted record is proof.** A record the walk accepts at `p` in segment `s` carries `lsn = p`, a valid CRC and the chain, so a writer wrote it for `p`. A correct writer writes into `s` only after its create or rename and its length are durable (`sync_all`, then `sync_dir`, completed). After that, nothing a correct build does takes the name away while `s` holds records a walk can accept:
    - recycling runs only below an older meta's start;
    - step 5 removes only above a valid end, and makes the removal durable before the step ends.
  - **Why a segment without one is not.** It may be a crashed process's create or rename that exists only in the page cache. So the boot must make it durable itself. A-6b's re-sync of every segment it writes in a boot conforms.

## Owner point OP-1 (not decided here)
In plain words: when part of the transaction log cannot be read from the disk, should a **read-only** open still let you read and export everything before that point, clearly reported as incomplete? The card, and this verdict, keep the answer that needs no owner decision: it refuses with an I/O error (58030), like every other open, until the disk reads again or the files are copied off the device.
- **What it is about.** A bad sector that never reads again then leaves the data unreachable, even read-only.
- **The card's recommendation.** The card recommends "yes", by analogy with S-ALPHA-F4. A read-write open would stay refused either way, so that a passing error never costs acknowledged commits.
- **If the owner says yes.** O1 to O12 in the card apply. I parsed them from the card and applied them, in order, to becd0afc's file and to the reworked file. Each Old occurs exactly once at its turn on both.

## Merge timing
By P0 re-check 1, this card may be built and gated on its branch. It may not merge before P0 PASS unless the owner says so in their own words.

## Corrections RW-1 to RW-7

Apply in this order to `docs/S-STORE.md` at becd0afcd40d6d18252606a989c48f62c5eb0d67 (sha256 `91b407161c03eb77a83b5b857c78ecb10db7e8c63dd8b9117e4dac94a211e4b8`). Each Old occurs exactly once in that file, in file order. Nothing else in `docs/` changes. The result is sha256 `3a75fb626065ff4b8081ecbc6eba3105c6e0fff06d92809512026ed47ecb9b79`, +63/−28 against the scaffold. The gate built it in a scratch clone and ran the Acceptance below on it.

**RW-1. §12.5 "Read errors": what an open has written by the read error, pass 2 included.**

Old:
```text
It writes nothing, or, in a fall-back from an image `k` that failed in pass 2, nothing more than the fence and that pass 2's evictions. An open that later reads those bytes recovers as if the error had not happened.
```
New:
```text
It writes nothing more than the open had written when the read failed: nothing, when pass 1 from meta `k` meets it; otherwise at most the fence (§14 step 1), pass 2's evictions and, once a fall-back or an explicit rollback has retired meta `k`, the retire write (§14 step 4a). Pass 2 reads again the span that pass 1 read, and a read error that only pass 2 meets fails the open in the same way. An open that later reads those bytes recovers as if the error had not happened.
```

**RW-2. §14.2: the segment-size row.**

Old:
```text
| The newest meta fails step 1's range check | XX001, with no fall-back | XX001 |
```
New:
```text
| The newest meta fails step 1's range check | XX001, with no fall-back | XX001 |
| The file's identity carries a `wal_seg_size` below 212 (§12.1) | XX001, nothing written | XX001 |
```

**RW-3. §14.2: the read-error row, pass 2 and the retire write included.**

Old:
```text
| A read error in the WAL walk (step 3), from meta `k`, or from meta `k−1` in a fall-back | 58030, nothing written beyond the fence and step 4a's pass-2 evictions; an explicit rollback fails with 58030 too | 58030 |
```
New:
```text
| A read error in the WAL walk (step 3, or pass 2's walk in step 4), from meta `k`, or from meta `k−1` in a fall-back | 58030, nothing written beyond the fence, pass-2 evictions and, once meta `k` is retired (step 4a), the retire write; an explicit rollback fails with 58030 too | 58030 |
```

**RW-4. §15 I-WAL-COVER: the short-segment variant's lost record.**

Old:
```text
**short segment**: a synced record below `wal_ckpt_end` lost after its sync, so that the walk stops on its zeros, in a segment whose file the test then cuts short past that record, at or above `wal_ckpt_end` (forged through `Env`).
```
New:
```text
**short segment**: a synced record below `wal_ckpt_end` lost after its sync, so that the walk stops on its zeros, in a segment whose file the test then cuts short past that record, at or above `wal_ckpt_end` (forged through `Env`); the lost record is a `Chunk` of a transaction that never commits, appended after the last `Commit` or `CommitMarker` before the flip, so that the walk reads every commit up to `ts_ckpt_hi`, the second condition holds, and only the first decides.
```

**RW-5. §15 I-RO-OPEN: SHORT-SEG-ERROR's gloss.**

Old:
```text
SHORT-SEG-ERROR (a short or missing segment returns 58030 instead of ending the valid end) fails the open.
```
New:
```text
SHORT-SEG-ERROR (a segment entered by a hop that is missing, or too short for a record header at its first LSN, returns 58030 instead of ending the walk there, §12.5) fails the open.
```

**RW-6. Changelog 7.16: the read-error bullet.**

Old:
```text
and writes nothing, or, in a fall-back, nothing more than the fence and its pass-2 evictions (§12.5 "Read errors", §14 step 3).
```
New:
```text
and writes nothing more than the open had written when the read failed (nothing, in pass 1 from meta `k`), pass 2's own walk included (§12.5 "Read errors", §14 step 3).
```

**RW-7. Changelog 7.16: the COVER-AT-EOF bullet.**

Old:
```text
The variant now loses a synced record below `wal_ckpt_end` in a segment that the test then cuts short at or above it (§15 I-WAL-COVER).
```
New:
```text
The variant now loses a synced record below `wal_ckpt_end`, a `Chunk` appended after the last commit before the flip so that the check's second condition holds, in a segment that the test then cuts short at or above it (§15 I-WAL-COVER).
```

## Acceptance lines that change

Number the card's Acceptance list from 1. Replace lines 6, 20, 22, 32, 33, 34, 42 and 43 with these lines; the other 37 stay as they are, the list stays at 45 commands, and the last one stays `cd nucleus/v3 && bash scripts/gate.sh`.

Line 6:
```text
- `cd nucleus/v3 && test "$(awk '/^- \*\*Draft 7\.16\*\* /{f=1;next} /^- \*\*Draft 7\.15\*\* /{f=0} f' docs/S-STORE.md | grep -c -x -e '  - \*\*A read error during the walk\*\* (F-1; I-WAL-STALE)\. Neither §14 step 3 nor §9\.2 said what a failing read of a WAL segment does\. It is neither a stop nor damage: the open fails with 58030, read-write and read-only alike, an explicit rollback and a fall-back.s pass 1 included, and writes nothing more than the open had written when the read failed (nothing, in pass 1 from meta .k.), pass 2.s own walk included (§12\.5 "Read errors", §14 step 3)\. Test .t_wal_read_error.; mutant WAL-READ-ERROR-AS-END (A-6b, A-7d)\.' -e '  - \*\*A record that does not decode\*\* (F-2; I-WAL-STALE)\. A record that passes every stop check and the chain check but whose body does not decode is detected damage, as a splice is: XX001 before pass 2, writing nothing, and a read-only open.s salvage cut at it (§12\.5 "Malformed records", §14 step 3a)\. Pass 1 decodes every record it accepts\. Test .t_wal_malformed.; mutant WAL-MALFORMED-AS-END (A-6b, A-7d)\.' -e '  - \*\*Short segments\*\* (F-3; I-WAL-START, I-WAL-STALE)\. Draft 7\.15.s short-segment sentence made any read past a segment file.s end-of-file a clean stop, the start segment.s included, so a start segment whose length a device lost after its sync ended the walk where its file ended: at a quiescent flip the coverage check passed and the open recovered none of the commits past that point, while step 5 zeroed or removed the intact segments after it\. A correct build leaves a short segment only where the walk enters it by a hop, too short for a record header at its first LSN: the writer rule makes a segment.s length durable before its first record, and the start segment.s before its meta, and nothing shortens a segment file\. That is now the only short read that ends the walk cleanly; every other is detected damage (§12\.5 "Short segments", §13 "The start segment", §14 steps 3 and 3a)\. Test .t_wal_short_read.; mutants START-SEG-SHORT-AS-END (on .t_wal_start.), SHORT-READ-AS-END and SHORT-HOP-AS-DAMAGE (A-6b, A-7d)\.' -e '  - \*\*The segment size\*\* (F-4; I-WAL-STALE)\. .wal_seg_size. is at least 212, so that a segment holds a record header; creation refuses less than 8 MiB outside the store.s tests, C-T6-rehost §4\.3.s floor for its row-layer ops (its §13\.1 N3), with 22023 and before any write; an open of a file whose identity carries less than 212 fails with XX001 (§2\.1, §2\.3, §12\.1)\. Test .t_wal_seg_size.; mutant SEG-SIZE-UNCHECKED (A-7c, A-7d)\.' -e '  - \*\*The writer rule\*\* (F-5, and card A-6b.s open point 5)\. SEG-ACK-BEFORE-DURABLE no longer claims that a lost preallocated length loses a synced record: a .sync_data. makes durable the length its data needs\. A writer that makes a segment.s directory entry durable but not its length now has its store.s read-write open refused after a power cut, where it lost nothing before (SEG-LEN-NOT-DURABLE, judged by .t_seg_ack_durable. at A-6b and A-7f)\. "Durable" in the writer rule means made durable by the writer.s own boot, or shown so by a record that its recovery walk accepted in the segment, so a boot.s writer makes a segment present at open durable itself (§12\.5)\.' -e '  - \*\*Recycling targets\*\* (card A-6b.s open point 4; I-WAL-START)\. A recycled segment.s new first LSN lies above the append position.s segment and is held by no file, so no name below the append position is taken again and a recycled start segment stays missing (§12\.1)\. Draft 7\.15 did not say where: a build that took the lowest free name would, after both copies of the two newest metas were lost, open a meta whose start segment held another segment.s records as a clean stop at a quiescent flip\. Mutant RECYCLE-TARGET-LOW, on .t_wal_start. (A-6b, A-7d)\.' -e '  - \*\*COVER-AT-EOF.s test\*\* (found while deriving F-3)\. .t_wal_loss_below_ckpt_end..s short-segment variant could not fail under COVER-AT-EOF: a record that runs past an end-of-file lies wholly below or wholly at or above .wal_ckpt_end., a record boundary, so the end-of-file and the record.s start were always on the same side of it\. The variant now loses a synced record below .wal_ckpt_end., a .Chunk. appended after the last commit before the flip so that the check.s second condition holds, in a segment that the test then cuts short at or above it (§15 I-WAL-COVER)\.' -e '  - \*\*Counts\.\*\* §15: 47 rows, 234 mutants, 52 tests\. §15\.1: 166 rows\. Four tests and eight mutants are added, each mutant judged on a Phase A row, and no invariant\. 24 mutants are judged only on Phase B cards, as at draft 7\.15\. No invariant is weakened, and no mutant or test is removed or renamed\.')" = 8 && test "$(awk '/^- \*\*Draft 7\.16\*\* /{f=1;next} /^- \*\*Draft 7\.15\*\* /{f=0} f' docs/S-STORE.md | wc -l | tr -d ' ')" = 8`
```

Line 20:
```text
- `cd nucleus/v3 && grep -qx -e '\*\*Read errors\.\*\* A read of a segment that fails with an I/O error, or an open of one that fails other than by the file.s absence, is neither a stop nor damage: the bytes it could not read may hold acked records, and a later open may read them\. The walk ends with that error and no valid end\. The open fails with \*\*58030\*\*, read-write and read-only alike, and so does an explicit rollback, and a fall-back whose pass 1 from meta .k−1. meets one (§14 steps 3 and 4a)\. It writes nothing more than the open had written when the read failed: nothing, when pass 1 from meta .k. meets it; otherwise at most the fence (§14 step 1), pass 2.s evictions and, once a fall-back or an explicit rollback has retired meta .k., the retire write (§14 step 4a)\. Pass 2 reads again the span that pass 1 read, and a read error that only pass 2 meets fails the open in the same way\. An open that later reads those bytes recovers as if the error had not happened\. Mutant \*\*WAL-READ-ERROR-AS-END\*\*: a read error ends the walk as a stop where it was, so a read-write open truncates there (§14 step 5), and a read error that a later read would not meet costs every acked commit behind it, without detection\.' docs/S-STORE.md && grep -qx -e '- Test .t_wal_read_error. (A-6b, A-7d; §15\.1)\. At A-6b, through an .Env. whose reads of one segment fail (a hook): a failing read of a record.s header, a failing read of its body, and a failing open of the segment that a hop enters each end the walk with that error, after the records before it, never with a stop or damage, and the walk writes nothing\. At A-7d, acked commits after meta .k., and a hook that fails every read of one synced record past meta .k..s .wal_ckpt_end.: (a) transient, at the first open only: the read-write open fails with 58030 and leaves every file byte-identical, and the next open recovers every acked commit; (b) persistent: the read-write open, the read-only open and an explicit rollback naming meta .k. each fail with 58030 and leave every file byte-identical; (c) in a fall-back: image .k. fails at open (.t_fallback..s proven fixture) and the read fails in pass 1 from meta .k−1.: the open fails with 58030, writes nothing more than the fence and the pass-2 evictions made before the fall-back, and retires nothing, and the next open, without the hook, falls back with proof and recovers every acked commit\. WAL-READ-ERROR-AS-END fails (a) to (c): the read-write open returns .Ok., and the next open lacks the acked commits behind the read\.' docs/S-STORE.md`
```

Line 22:
```text
- `cd nucleus/v3 && ! sed -n '4,/^## Changelog/p' docs/S-STORE.md | grep -n -e 'Any other segment file shorter than the position the reader needs' -e 'acked before its directory entry and length are durable' -e 'a power cut loses the create, rename or length' -e 'Why the record.s start, not the end-of-file' -e 'The exception is a splice (below): when' -e 'It does not apply to WAL loss or a splice under' -e '(a short segment is not extended), and zero or remove every' -e 'a segment cut short so that its end-of-file lies past the start of a record that runs past it' -e 'its open finds its start segment missing and fails with XX001' -e 'except under WAL loss or a splice (§14 step 3a)' -e '(ack before the segment.s .sync_all. and its .wal/. directory sync)' -e 'exists durably: step 1 has made a record in it durable, or,' -e 'renaming it to a new first LSN\. Its old content' -e 'at the end of this section)\. A mismatch there is not a stop\.' -e 'never a valid end (START-SEG-AS-END fails it)\. At A-7c' -e 'WAL loss, a splice, a missing start segment,' -e 'step 4a item 1)\. A newest image that fails, or that a read error stops' -e 'SEG-ACK-BEFORE-DURABLE, SALVAGE-ABOVE-FRONTIER' -e '| I-WAL-STALE | §12\.5: the reader stops' -e 'exists durably; segments are recycled lowest first, each rename durable before the next; a missing start segment' -e 'every meta.s start segment exists before the meta is written; a missing start segment is XX001, never an empty WAL (§2\.3, §12\.5, §13 step 1, §14 step 3; I-WAL-START) |' -e 'the walk reaches every acked record | SEG-ACK-BEFORE-DURABLE |' -e 'full: acks and replay | SEG-ACK-BEFORE-DURABLE |' -e 'and the walk.s start | START-SEG-AS-END, SEG0-LAZY, RECYCLE-UNORDERED |' -e 'full: (a)–(d) |' -e '^Creation runs as one sequence:$' -e 'It writes nothing, or, in a fall-back from an image .k. that failed in pass 2' -e 'a short or missing segment returns 58030 instead of ending the valid end' -e 'A read error in the WAL walk (step 3), from meta' -e '(forged through .Env.)\. Under COVER-CHECK-SKIP'`
```

Line 32:
```text
- `cd nucleus/v3 && test "$(grep -A2 -x -e '| A splice (step 3a, I-WAL-CHAIN) | XX001, nothing written | salvage at the splice cut |' docs/S-STORE.md | tail -n 2 | grep -c -x -e '| A malformed record, or a short segment that §12\.5 counts as damage, the start segment.s included (step 3a; I-WAL-STALE, I-WAL-START) | XX001, nothing written | salvage at the damage cut |' -e '| A read error in the WAL walk (step 3, or pass 2.s walk in step 4), from meta .k., or from meta .k−1. in a fall-back | 58030, nothing written beyond the fence, pass-2 evictions and, once meta .k. is retired (step 4a), the retire write; an explicit rollback fails with 58030 too | 58030 |')" = 2 && grep -A1 -x -e '| The newest meta fails step 1.s range check | XX001, with no fall-back | XX001 |' docs/S-STORE.md | tail -n 1 | grep -qx -e '| The file.s identity carries a .wal_seg_size. below 212 (§12\.1) | XX001, nothing written | XX001 |'`
```

Line 33:
```text
- `cd nucleus/v3 && sed -n '/^## 15\. /,/^### 15\.1 /p' docs/S-STORE.md | grep '^| I-WAL-COVER | ' | grep -q -e '\*\*short segment\*\*: a synced record below .wal_ckpt_end. lost after its sync, so that the walk stops on its zeros, in a segment whose file the test then cuts short past that record, at or above .wal_ckpt_end. (forged through .Env.); the lost record is a .Chunk. of a transaction that never commits, appended after the last .Commit. or .CommitMarker. before the flip, so that the walk reads every commit up to .ts_ckpt_hi., the second condition holds, and only the first decides\. Under COVER-CHECK-SKIP, '`
```

Line 34:
```text
- `cd nucleus/v3 && sed -n '/^## 15\. /,/^### 15\.1 /p' docs/S-STORE.md | grep '^| I-RO-OPEN | ' | grep -e 'except under WAL loss, a splice, a malformed record or a short segment (§14 step 3a), a missing start segment (step 3),' | grep -e 'a segment is newly created (the lost-create case) or recycled by rename (the lost-rename case, the rename made by a process that crashes before its directory sync, so that the next boot.s writer finds the segment present), records in it are acked, and power is cut at every step from the create or rename to the ack\. Recovery must replay every acked record, and the walk must end in a stop\. SEG-ACK-BEFORE-DURABLE (ack before the segment.s .wal/. directory sync, or, for a segment present at open, before this boot.s own) loses an acked commit as a clean stop and fails it; SEG-LEN-NOT-DURABLE (the directory entry synced but not the length) leaves the segment short behind its last synced record, so that the walk reports damage and a healthy store.s read-write open fails with XX001, and fails it\.' | grep -q -e '| RO-NO-REPLAY, RO-WRITES, RO-LOCK-FILE, SHORT-SEG-ERROR, SEG-ACK-BEFORE-DURABLE, SEG-LEN-NOT-DURABLE, SALVAGE-ABOVE-FRONTIER, SPLICE-SALVAGE-PAST-CUT, ' && sed -n '/^## 15\. /,/^### 15\.1 /p' docs/S-STORE.md | grep '^| I-RO-OPEN | ' | grep -q -e 'SHORT-SEG-ERROR (a segment entered by a hop that is missing, or too short for a record header at its first LSN, returns 58030 instead of ending the walk there, §12\.5) fails the open\.'`
```

Line 42:
```text
- `cd nucleus/v3 && test "$(git diff --numstat nucleus/v3-scaffold...HEAD -- docs/S-STORE.md | cut -f1,2 | tr '\t' ' ')" = '63 28'`
```

Line 43:
```text
- `cd nucleus/v3 && test "$(git diff --no-color --diff-algorithm=myers -U0 nucleus/v3-scaffold...HEAD -- docs/S-STORE.md | tail -n +5 | grep -v '^@@' | sed 's/20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/<date>/g' | sha256sum | cut -c1-64)" = 'e84aeaf312f081be9bc430c8faeee0516efc2a9600153b65c107671da0d8fb25'`
```

## How the rework is checked
I checked the rework three ways, in a scratch clone with a commit on top of becd0afc:
- **The new list passes.** With RW-1 to RW-7 applied, the 44 non-gate commands (lines 6, 20, 22, 32, 33, 34, 42 and 43 replaced) pass 44 of 44 under the default locale and under `LC_ALL=C`. The checker's three lines are unchanged and end `OK`. `git diff --numstat` gives 63 added and 28 removed.
- **The new list rejects the current commit.** On becd0afc as it stands, the same list fails lines 6, 20, 22, 32, 33, 34, 42 and 43, in both locales.
- **Each block is needed.** Leaving one RW block out fails these lines, in both locales:

  | Left out | Lines that fail |
  |---|---|
  | RW-1 | 20, 22, 43 |
  | RW-2 | 32, 42, 43 |
  | RW-3 | 22, 32, 43 |
  | RW-4 | 22, 33, 43 |
  | RW-5 | 22, 34, 43 |
  | RW-6 | 6, 43 |
  | RW-7 | 6, 43 |

## Card text the rework must update
The card is not this gate's to edit; these points are for the author or orchestrator who applies the rework.
- **The Edits.**
  - W11's, W21's, W22's and E2's New blocks take the RW-1, RW-3, RW-4, RW-6 and RW-7 text.
  - Two blocks are added: one after W20 (Old and New as RW-2) and one after W23 (Old and New as RW-5).
  - Applied to the base, the card's 37 blocks then give sha256 `3a75fb62…9b79`. The gate checked this by script, and each new block's Old occurs once in the base.
  - Limits 2 gains §14.2's range-check neighbour and I-RO-OPEN's SHORT-SEG-ERROR gloss.
  - "The draft target" diff becomes +63/−28.
  - The reverse (E2, the RW-amended blocks and E1, inverted) is to be re-checked by the reworker.
- **R-1.** "What has been written by then" gains pass 2's own walk: the fence, pass 2's evictions and, after a retire, the retire write. Cases 1 to 4 of "The cases" stay right for pass 1.
- **R-7, "For the gate" item 4 and the Mutation evidence.** The variant's lost record lies after the last commit before the flip, so the second condition holds.
- **The author's dry run and the runner's parse.** Both are to be redone on the reworked list. The commands keep their 45 count and their order.

## Follow-ups
- **S-ETA-F1, riders for the next S-STORE card (or the rework, at its author's choice; none blocks).**
  - (a) W13's step-3 sentence, W17's exception and W33's R21 say "anywhere but at the first LSN of a segment it entered by a hop". §12.5 makes a body read past the end-of-file at that first LSN damage. Precise form: "anywhere but in the record header at the first LSN of a segment it entered by a hop". `t_wal_short_read`'s A-6b form already pins the body case.
  - (b) `t_wal_read_error` (c): the hooked record lies past meta `k`'s `wal_ckpt_end`, so pass 1 from meta `k` meets it before image `k` can fail in pass 2. (c) reaches a fall-back's pass 1 only when image `k` fails in step 2, so name `t_fallback`'s step-2 pages (a MapLeaf, a FreeSet page or a directory page).
  - (c) `t_wal_start` (f):
    - The crash should follow checkpoint 3's step 6 with no write between, so that image 1's slots, freed there, are not reused. Otherwise the build and RECYCLE-TARGET-LOW can both give XX001 from I-INTEGRITY at step 2.
    - "on a stale record's `lsn`" should read "on the stale bytes at that offset (a magic, CRC or `lsn` stop)".
  - (d) "Malformed records", "under §12.2 and §12.3": A-6a's decoder also refuses a `Commit` or `CommitMarker` whose `commit_ts` is 0 or all-ones (§7.1). Say "under §7.1, §12.2 and §12.3".
  - (e) W16 ("no segment file is shortened") has no test. A step 5 that shortens the valid end's segment and grows it back refuses a healthy store after a process crash between the two (gate mutant G4). Add to `t_wal_short_read` (b) at A-7d a crash at every step of §14 step 5.
- **S-ETA-F2, A-7d card text: SEG-SIZE-UNCHECKED's open form.**
  - Under the mutant, the open hands 211 to A-6b's walk, whose own `check_seg_size` refuses it with `Invalid` before any I/O. So the mutant's open fails too.
  - At A-7d the mutant is caught only if that `Invalid` maps to a code other than XX001, or if the test asserts a detail that only the open's own check produces. W5 gives that check no detail.
  - A-7d's card must pick one. The A-7c creation form is caught regardless.
- **S-ETA-F3, C-T6-rehost.**
  - §4.3's "A-2/A-6a enforce it at file creation (§13.1 N3)" names cards that do not enforce it. Apply the card's own exact rider ("For the plan card", item 6) at that document's next edit.
  - Its §13.1 N3 row's "§2.1 / §12.2" then reads "§2.3".
- **S-ETA-F4, E6 and a future OPFS Env.**
  - R-3 makes a lost preallocated length a refusal, where it used to be a clean stop.
  - So an Env whose flush does not make a `set_len` durable would turn every power cut after a record into XX001 on a healthy browser store.
  - E6, or the OPFS Env card, must verify OPFS's `truncate` then `flush` against E-DURABLE, or preallocate by writing zeros, before browser mode ships.
- **S-ETA-F5, pre-existing at 7.15 and not this card's: COVER-END-AT-T in `t_wal_loss_below_ckpt_end`.**
  - The same second-condition masking as D1 applies. In the main fixture, the fault on `c`'s record (`c > T`) leaves the walk below `ts_ckpt_hi`, so the open gives XX001 under COVER-END-AT-T as well.
  - "under COVER-END-AT-T, the open succeeds" holds only in a variant whose lost record lies after the last commit before the flip. The next card that touches I-WAL-COVER's row should name that variant.

## Not verified
- No A-6b, A-7c, A-7d or A-7f code exists. So no named test has run, and "passes on a correct build and fails under the mutant" was walked as text.
- **OPFS's length durability** (S-ETA-F4).
- **The lowest-free-name path at two copy-faults** (P0 point (ii)) is reasoned, not modelled.
- **Merge orders.** I checked S-ETA's blocks, and then RW-1 to RW-7, on S-ZETA's and on R-1-SPEC's committed files, not all six three-card orders. I did not run the S-ZETA or R-1-SPEC checkers.
- **E4's `truncate_wal` zeroing** (the card's D-9 source) and **A-6b-0's `DirLoss`** were not read. The `DirLoss` code is not on the scaffold yet, and SimEnv's current directory loss is prefix-only.

<!-- gate 1's verdict (3e6e999c), verbatim, ends -->
