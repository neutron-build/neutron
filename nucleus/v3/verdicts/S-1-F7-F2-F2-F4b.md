Card: S-1-F7-F2-F2-F4b
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-gate-S-1-F7-F2-F2-F4b-3 (T3 review sub-agent, Claude Code session 7e039623-d24d-4edb-b879-f7d4b6297e54; it wrote neither the card, its amendments, the rulings, the reviews, the REWORK verdicts c1f42387 and 27d658c0, nor any S-1-F7-F2-F2-F4b commit)
Date: 2026-11-08
Gate-exit: GATE EXIT=0
Commit: 40a8e64460806c5676e3cc5701d18e6e1d5d3373
Mutants: gate text mutants on a fresh clone at 40a8e644 (scaffold ref at acc81c77), all 60 G run per mutant: GM1 -> G58; GM2 -> G58; GM3 -> G58, G59; GM4 -> G58; GM5 -> G14; GM6 -> G15; GM7 -> G48; GM8 -> G7; GM9 -> G10; GM10 -> G21; B13a -> G12; B15 -> G14; B16 -> G14; B38 -> G39; B40 -> G40; B41 -> G41; B45a -> G43, G58; B54 -> G51; B55 -> G51; B56 -> G52; B58 -> G43; B60 -> G50; B71 -> G14; B72 -> G14; B77 (three forms) -> G13; B78 (each of the three holders) -> G27; B79 -> G59; B83 -> G14; B84 (both forms) -> G14; B85 -> G59; B86 -> G59; B87 -> G59; B88 -> G60; B89 (both forms) -> G60; B90 -> G60; B91/X3 -> G14; B92/X4 -> G14; verdict 27d658c0's X1 -> G59; X2 -> G14; X5 -> G14; X6 -> G14; X7 -> G13; X8 -> G27; X9 -> G14; own: Y4 re-check accepts any copy -> G60; Y5 drop "all latched counts as exhausted" -> G60; Y6 drop "which counts every pin" -> G14; Y7 drop "whose holders never wait for a frame" -> G14; Y8 evictor takes tree latches -> G14; Y9 detach after unlatch -> G59; Y10 late record silently ignored -> G59; Y1 claim blocks on the copy's latch -> UNCAUGHT; Y2 drop the claim's try_latch clause -> UNCAUGHT; Y3 drop the install-under-latch sentence -> UNCAUGHT; Y11 drop the EXHAUST-BLOCKING-CLAIM sentence -> UNCAUGHT; Y12 drop the Rework 2 changelog sub-bullet -> UNCAUGHT; Y13 drop R_EPOCH from §5.4's formula -> UNCAUGHT; Y14 drop R_MAP from §5.4's formula -> UNCAUGHT. Every UNCAUGHT clause is present at the tip, or unchanged from the scaffold (see "Grep coverage"). The 27 spec mutants: §15 is unchanged since verdict 27d658c0, so that paper check stands.
Followups: S-1-F7-F2-F2-F4b-F1, S-1-F7-F2-F2-F4b-F2

## Gate run

- **The implementer's Acceptance and mutant results are lost (host reboot), so everything below was run by this gate on fresh copies.** None of it relies on the implementer.
- **Script.** The scaffold's `fresh-gate.sh` and `git-env.sh` were extracted through the `--no-local` shallow clone (GATE-PROMPT step 1), with no `EXTRACT FAILED` line. Scaffold id acc81c776183bd9853f7a69e2cec4262d300d89f. They ran from `~/Workspace/neutron-v3` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-1-F7-F2-F2-F4b`, on commit 40a8e64460806c5676e3cc5701d18e6e1d5d3373.
- **Runs.** A first run was killed part-way through its test step by a T3 server restart and gave no result; it is not counted. The extract was lost with the scratch directory, so the extract and the run were repeated. The second run went to completion.
- **Lines.** `fresh-gate: commit 40a8e64460806c5676e3cc5701d18e6e1d5d3373`, `fresh-gate: base acc81c776183bd9853f7a69e2cec4262d300d89f`, `fresh-gate: checks passed (4779 files hashed in 1.40 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`; STEP test EXIT=0; STEP clippy EXIT=0; STEP fmt EXIT=0; STEP deny EXIT=0; STEP wasm32 EXIT=0; STEP core-paths-selftest EXIT=0; STEP enforcement-tests EXIT=0; literal last line `GATE EXIT=0`. stderr empty, no refusal. `$fg` removed afterwards.
- **Scope.** `git diff acc81c77...40a8e644 --stat` lists `nucleus/v3/docs/S-STORE.md` and this verdict file only. The three rework commits change four S-STORE lines: the fairness bullet, the Reserve bullet, the map-phase bullet and one changelog sub-bullet. All are inside Limits 2, 15, 16 and 11.
- **Acceptance.** All 60 greps (G1–G60) were run on a fresh `--no-local` clone checked out at 40a8e644, with `nucleus/v3-scaffold` at acc81c77: every one passes. On the scaffold every G fails except G36 and G38, as in the previous verdict.
- **The rework is complete, not cut off.** All 24 blockquoted verbatim Work blocks in the card, including Work 1a, Work 1b's three map-phase lines and Work 8, occur byte-for-byte in S-STORE at the tip. Each item of amendment 2 (the hold term and its "not circular" sentence, detach then return, the assertion, the re-check, the no-return-under-latch rule, the `try_latch` draw, the changelog sub-bullet) is present.

## Is amendment 2 faithful, and is any grep loosened?

Compared against `~/.cache/nv3-review/S-1-F7-F2-F2-F4b.pre-rework2-amend.local.md`. Only G14 and G59 changed, G60 is new, and Touch only and the Limits are byte-identical.

- **R1.** Answered, by the option the verdict offered first. The verdict's suggested wording ("bounded by the pinning threads' own calls") is rightly not used: that bound is circular, as the amendment says, and the count is the sound replacement.
- **R2.** Answered as asked: detach under the latch, unlatch, return, and a claim re-check. The amendment goes further and rules on draws, because the verdict's rule alone does not cover a stale claimant.
- **R3.** X3 and X4 are pinned (G14 is now the whole bullet with no `.*`).
- **G14** is not loosened. One wording in the amendment is imprecise: the old literal "; and a frame acquisition's victim write." is not a substring of the new pattern, because R1 inserts a term after it. It changed only for that requested term, the clause is still pinned (GM5 fails G14), and the new pattern has no `.*`.
- **G59** is not loosened. Its old literal is the sentence R2 corrects. The new pattern pins the corrected sentences in place, and a negative grep pins out the old order.

## Substance

**R1, the fairness bound. It holds, on one stated premise.**
- The pigeonhole is right. With `F − R_EPOCH = T(h+2) + R_MAP`, the frames outside `R_EPOCH` that no pin holds are at least the sum over threads of the pins each may still take. A thread obtaining a frame has quota left, so does each evictor, and each latched unpinned frame is charged to one of them. So an unpinned, unlatched frame exists, and the wait does not depend on any pin holder's call, which is what broke the circle in verdict 27d658c0.
- A gated operation past its first mutation can evict dirty victims under its own gate hold (§5.2), so a pending flip does not stop it.
- For a pending flip: the only other latch holders on such a frame hold the gate or got it by a successful try, or took a clean victim. Step 2 is not running then, since there is one checkpointer. Map-latch holders never wait for a frame (§4.1; MapInners are outside the pool, §5.7).
- The list of holders is complete: every other latch holder has a pin. The prune under a victim's latch is CPU-only and reads no chain (§10.3).
- **Can the wait be unbounded?** Only if the premise fails. The sentence says §5.4's count "counts every pin any thread can hold". That is not yet true, in two ways: S-1-F7-F2-F2-F2's term, which §5.3 already names as a dependency, and `R_MAP`, which it does not name. `⌈dirty frames / 510⌉` understates MapLeaf pins for scattered LPIDs, which is the normal layout of one tree's path. At the floor about half the non-reserve frames can be dirty pinned path pages and the other half their MapLeafs, every frame then pinned. PLAN's R-1 row already carries this ("reads as a lower bound"), and R-1's spec is written before A-3b, so it is routed and not new. What is new is that the fairness bound now rests on it too. See follow-up F2.
- "At most one write per other thread" is exact only per instant: a schedule can keep moving the frame the clock reaches. A frame acquisition already had that property, so it is not a regression. A-3b's budget carries slack.

**R2, the map phase and the reserve. Sound.**
- **No thread blocks on a reserve frame's latch.** The amendment says plainly that "no latched frame in the reserve" cannot hold literally, since a stale claimant may hold a reserve frame's latch for its re-check. The guarantee actually needed is that nobody waits on it, and the text gives that: claims and draws use `try_latch`, and the map phase and data phase never return a frame under its latch (the rule is general).
- **Late record: unreachable.** A claim of M's copy either won the latch first (its append precedes the build, and the re-check sends the map phase to the record) or wins after the detach and finds no copy. Once M is taken it counts as written, so `write_guard` makes no new copy and §5.2 appends no record. Post-order means a child's record cannot follow its parent's build. A debug assertion is the right treatment of an unreachable path.
- **The list stays exact.** One record per page: a copy exists only when the content is not at a slot the image map references, and a record makes it so.
- **Crash cuts around the detach.** Memory only, and the content is already in the buffer. Meta `k` follows the map phase, so a crash anywhere in the window loses only image `k`, whose slots are free in `k−1`. A failed page write is fail-stop (§9.2), so no retry needs the detached copy.
- **The reserve's Bound.** With `R_EPOCH > T + s`, a draw that finds every free frame latched has at least one unlatched copy to claim, with at most `T − 1 + s` frames latched. The window between the map phase's unlatch and its return is one frame, inside step 2's share `s`, as long as it returns the frame before latching the next (F2).

**New problems from these fixes (the last two reworks each introduced one).** None found that blocks.
- **§5.6.** "A writer never waits holding a page latch" now really holds for the draw. The re-check adds no wait.
- **Latch order and I-LATCH.** No latch holder blocks on an epoch-copy frame. Only step 2 does, holding no latch.
- **I-CKPT-AT-T.** Unchanged, as above. The source order is unchanged, and the detach happens only after the source is read.
- **Literal gap.** Latch scope's exception list names "an epoch-copy claim", not a reserve draw, which now also try-latches a frame that is no tree's page. It adds no wait edge (F2).
- **Wording.** §5.3 says a gated operation "may wait for an unpin", and the next bullet says that wait "waits for no pin". Read with the count these agree, but a reader will stumble (F2).

## Grep coverage (not blocking)

- Y1, Y2, Y3 and Y11 leave G passing, though Y1 and Y2 remove the claim's own `try_latch` rule. G60's anchor starts one sentence too late. The verbatim check above shows that the text is present at the tip, so nothing is unpinned where it matters: the greps' job is finished once this card merges.
- Y12 (changelog) is present at the tip. Y13 and Y14 (§5.4's formula) are outside this card's Limits and unchanged from the scaffold.

## Follow-ups

**S-1-F7-F2-F2-F4b-F1 (P spec card, before A-3c is cut).** Rework 2's detach and draw rules have text mutants but no spec mutant or §15 test. A map phase that leaves a taken copy claimable, or a draw that blocks on a reserve frame's latch, passes every test the row names, because none schedules a claim during the map phase or a stale claimant landing on a returned frame. Severity is low: the first appends a harmless record, and the second blocks briefly on a latch whose holder waits on nothing. Add both as spec mutants to `t_step2_waits_for_claimed_copy` or `t_epoch_reserve_two_exhausters`, with a variant that pauses a stale claimant across the detach and return, in I-CKPT-AT-T and the A-3c PLAN row.

**S-1-F7-F2-F2-F4b-F2 (P spec card; part (a) before A-3b).** (a) State that §5.3's fairness bound rests on `R_MAP` as well as on S-1-F7-F2-F2-F2's term, and add that to PLAN's R-1 row: `R_MAP` must bound MapLeaf pins for scattered LPIDs, not `⌈dirty frames / 510⌉`. A counting sketch suggests a value near `threads × (height + 2)`, plus -F2-F2-F2's terms and any pin an epoch copy holds, is enough, since each pinned MapLeaf implies a dirty frame; R-1 should derive it. (b) Reconcile "may wait for an unpin" with "waits for no pin". (c) Name a reserve draw in Latch scope's `try_latch` exception. (d) Say the map phase returns a detached frame before it latches the next source, so the Bound's step-2 share covers it. (e) Say "at most one per other thread" holds per instant, so A-3b sizes its budget with slack for moving-target contention.

## Revert

Mutants were applied only to a scratch clone, which was restored after each run (`git status` clean). The worker's worktree is untouched apart from this verdict.
