Card: X-5c-F4-F1-F1
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F4-F1-F1 P gate, 2026-11-05)
Date: 2026-11-05
Gate-exit: GATE EXIT=0
Commit: e3460d24d89253650cfffb362f7527cb3df3c737
Mutants: M-SMO-NOWAIT -> x5_r40_aligned_round_has_split_and_merge_in_window, M-SMO-NOMERGE -> x5_r40_aligned_round_has_split_and_merge_in_window, M-SMO-DRAW -> x5_r40_smo_not_drawn_at_base, M-RENAME-SWALLOW -> szc_rename_crash_keeps_the_open_fold, M-RENAME-ANYERR -> x5_r37_base_tuning_reproduces_1b0ce54e, G-W11-NORETIRE -> x5_r38_w11_needs_no_completed_sync_since, G-SZC-ABORTNORESET -> szc_non_crash_abort_resets_the_fold, own G-SMO-GEN-AT-ZERO -> x5_r37_base_tuning_reproduces_1b0ce54e, own G-SMO-SAMETREE -> x5_r40_aligned_round_has_split_and_merge_in_window, own G-SMO-NOBACKSTOP -> x5_r40_aligned_round_has_split_and_merge_in_window, own G-Q4MG-FROM-SPLITS -> UNCAUGHT (finding 3)
Followups: X-5c-F4-F1-F1-F1, X-5c-F4-F1-F1-F2, X-5c-F4-F1-F1-F3

## Plain statement

The card did what it was asked. Its decision rule was applied verbatim and is NOT MET by all three settings, and I reproduce that. **But the card's answer to "is Q4 ≥ 1,000 reachable?" holds only for X5-R40, not for Q4.** X5-R40 misses for three design reasons. A scratch alignment that fixes them, with no store, oracle or quota change, reads Q4 about 1,760 per 10k with the floors held. No threshold moved, and none is proposed lower. Running that scratch also exposed a real store-level defect (finding 1), which must land before F4's final file. Every number from scratch below is a probe, not evidence.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone, scaffold 3fb03646a7e3739c7d2897578aeae67b42f01833, on the full commit, from `~/Workspace/neutron-v3`, as a background task, uncontained (that card is held). Lines: `fresh-gate: commit e3460d24…`, `fresh-gate: base 3fb03646…`, `fresh-gate: checks passed (4507 files hashed in 1.41 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. Every STEP EXIT=0. Literal last line `GATE EXIT=0`. stderr empty. `$fg` removed.
- **Scope.** `git diff 11aea6a7..HEAD` touches only `nucleus/v3/spikes/e4-recovery/`. The store diff is step 0's one change in `wal_recycle` and its docs. No new non-test `unwrap`/`expect`/`panic`/`unsafe`/`std::env`/wall clock. The worktree is clean and the tip did not move.
- **Scratch.** Mutants and probes ran on `git archive` exports under `~/.cache/nv3-review/gate-X-5c-F4-F1-F1/`, kept there: `probes/`, `k/`, `r41-prototype.patch`. Run-hygiene note: the gate's wide runs are the gate's own.
- **The resumed attempt.** The card's Limit-breaking risk was a tree left with M-SMO-NOMERGE applied. By reading, the sweeps are present and FINAL is BASE. By running, my BASE and setting 3 match the worker's table to the digit (below). The claim stands.

## Re-measured

| Probe | Rounds | Q4 | Q5a | Q7 | Q18 | failing | worker |
|---|---|---|---|---|---|---|---|
| BASE, 0..200 | 305 | 0 | 3 | 5 | 20 | 8 r0, 178 r1 | same |
| 1000/900/8, 0..200 | 309 | 0 | 3 | 3 | 180 | 151 r1, 190 r1 | same |
| 1000/900/8, 200..1000 | 1,239 | 1 (886 r2) | | | | | same (1,239, one hit) |

Halves: my in-window counts read 10 and 4 at 1000/900/8 (worker 8 and 4), and 7 and 0 at BASE (same), because I count any crash at position codes 1–6, uniform included. No effect on any conclusion.

Wall is not a stable input on this host: BASE read 18–30 s here against the worker's 72 s. Setting 2's "94 s > 90 s" carries no weight. It changes nothing, since Q4 is false at all three.

**`x5_r40_smo_not_drawn_at_base`:** passes, and M-SMO-DRAW fails it. It pins an API shape: `smo_pick` builds a fresh `Rng`, so a draw there cannot affect anything. The contract that matters, no `S_GEN` state consumed at share 0, is held by `x5_r37_base_tuning_reproduces_1b0ce54e` (my G-SMO-GEN-AT-ZERO fails it). Acceptable as written. The doc comment's "share 1‰" over a share of 100‰ is a nit.

**Seed 886 r2:** reproduced: digest 2522830599, `After2@2`, `q4sp` 2, `q4mg` 1. Note what it shows: **Q4 true with no LPID reuse in the window** (reuse count 0, read from my counter). It pins the machinery and proves no interaction. See "Q4: what the data says".

**151 r1 and 190 r1: not a new class.** Both are class C `O-3(b)` XX001 (151 at `[Open]` with O-1(a), 190 at `[AmcheckIi]` with O-1(b), nested 1). That is **C-XX**, 51 rounds in F4's BASE file (`Open` 16, `AmcheckIi` 35), about 1 per 300 rounds, so BASE reading none on 0..200 is chance. Neither reproduces at 900/8 or 900/16 without the alignment, so the schedule moved them into view. **Two corrections.** The README's "ordinary oracle failures" undersells a corruption class that E4 will judge (finding 4). And my own "new-class notice" for seeds 221, 423 and 370 (finding 8 of c1db3197) was wrong in the same way: they are C-XX shapes.

## Findings

1. **Blocking F4, not this card; found by this gate: a late epoch copy leaks into the next image (I-CKPT-AT-T, silent loss in class C).** Trace at seed 845 r0 under the scratch alignment. `freeze_first_touch` set `CopyPending` for LPID 4. Step 2 of checkpoint 1 wrote the normal frame and recorded `Img::At(slot 15)`. Then the copy installed, with `is_epoch_copy`, epoch 1 (`epoch_copy lpid=4 e=2`). Checkpoint 2's scan took it (`ckpt2 key=4 k=2 r=slot 56, epoch 1 ... frame=(4,true,false,true,1) img=None`) and recorded it, so image 2 maps LPID 4 to its epoch-1 content while `T` = 6. The key's ts 6 is lost: `O-1(b) at c=37, tree 3: scan mismatch`, with no XX001. By reading, the gap is the reserve-exhausted path's `write_fresh`; the card must confirm it.
   - A scratch guard (install only while `Img::CopyPending` holds) clears seed 845 and moves 13 of 1,000 BASE digests.
   - At BASE over 0..1000: 34 late installs and 7 stale copies taken by a later scan. So it is live at BASE. F4's 10k holds no silent class C O-1(b), so I find no BASE failure I can attribute to it.
   - **Not C-XX:** 310 r0, 332 r1 and 881 r0 are unchanged by the guard. Nor the three O-1(c) r0 rounds (1066, 6555, 7590: 0 late copies).
   - **My ruling:** a prototype deviation against S-STORE §5.6's copy condition, not an architecture failure. The owner should be told, since it is a silent loss. Seed 845 appears only on the scratch workload. See follow-up -F2.
2. **Non-blocking: the pre-registration moved.** X5-R40 was "fixed before this card's probes", yet the op's exit set (`left <= 1`, the 1,024-turn budget), the task it lives in, the half-and-half sweep order and the tree choice were each changed after probes `s1000a`, `s1000b`, `s1000d`. These are in code comments and the README, and absent from the RESULT's judgment calls. The livelock fix was forced and is sound. The outcome is still NOT MET, so nothing was tuned into a pass. A future card that iterates like this should stop or list it.
3. **Non-blocking: `q4sp`/`q4mg` have no value pin.** G-Q4MG-FROM-SPLITS (`q4mg` from `obs.splits`) passes all tests, because the pin asserts `≥ 1` and the round-trip only tests the JSON. They are diagnostics and no quota input. -F3 pins exact values.
4. **Non-blocking: README wording** on 151 r1 and 190 r1, as above. Corrected by REFRESH 8; the next README touch carries it.
5. **Noted: the worker's cause is right, and incomplete.** The burst's deletes are drawn from the chunked txn's own keys, so they cannot be below `w` until its marker applies. That explains the merge half. The joint miss has two further causes in X5-R40's own design: the gate opens on the first window, while targeted crashes pick checkpoint 0–2 and each `ckpt:begin` zeroes the counters; and nothing holds the window, so the crash hook beats the block.

Judgment calls accepted: the resume (above), the bounded pin hunt (four 200-seed chunks, declared), reading hygiene as governing probes, and the one subreaper demonstration (no repo change).

## K ruling

**Measured** (full `test-enforcement.sh`, one suite at a time, `unshare --user --map-current-user --pid --fork --kill-child --mount-proc`, the held card's `CONTAIN`):

| Tree | init | Result |
|---|---|---|
| card tip e3460d24 | `/bin/sh -c 'cd nucleus/v3 && bash …'` | all passed; K ok |
| held tip 6664ca75 | same | all passed; K ok |
| card tip | non-reaping stand-in (waits for its one child only) | **K FAIL**, alone |
| held tip | same stand-in | **K and BG FAIL**, nothing else |

Race check: group gone 20 ms after the kill: 30 of 30 under `/bin/sh`, 0 of 30 under the stand-in. The stand-in is mine; the real-opencode result is the worker's 4 of 4 (`/proc/1/comm` = opencode), and it matches. Earlier "the full gate passes inside a container" (verdicts X-6-F1-F1-F2-F1, -F1) stays true, but it was measured with a shell as init, so it never covered opencode as init. That is the gap, and my earlier verdict did not name it.

**Rulings.**
1. **The diagnosis is right.** `kill_child` is correct. `dead()` asks the wrong question: `killpg` succeeds on a zombie-held group.
2. **It does not block X-6-F1-F1-F2-F1-F1's merge.** Acceptance and fresh-gate run with `/bin/sh` as init, dash stays as PID 1 waiting, and dash reaps. Both trees pass.
3. **Only a worker's own-session `gate.sh` reads red.** The runner's Acceptance and every gate stay truthful (§7.6.4: exit truth is read from those).
4. **Home: new card -F1, a zombie-aware probe in the test file,** not -F3 and not a wrapper init. A zombie cannot commit, so the fix is correct on any host. -F3's zombie-count question stays separate.
5. **Order: not ahead of F4.** F4's branch carries its own older `test-enforcement.sh` (merge base ee8298c3), so -F1 could not reach F4's worktree anyway. REFRESH 8 and the -F2/-F3 cards tell the workers. Queue -F1 right after X-6-F1-F1-F2-F1-F2.

## Q4: what the data says

**Why merges miss the window, from the store.** A merge needs a pair under `F_PACK·U`, which needs versions removed, which needs tombstones at or below `w = min(snapshots, visible)`. A burst of the chunked txn's keys cannot satisfy that before the marker applies. Later, merges land with no window open. A window is short in this spike, because its dirty set is small.

**X5-R41, the scratch prototype** (`r41-prototype.patch`). In a selected round, at the GC task's front: a dense run of 40 Puts and their Deletes in their own commits, then wait for the target checkpoint's flip, sweep, commit a split run in another tree, release. The checkpointer holds at `After0` of the crash's own checkpoint until the block ends, before the crash arm. Store untouched. `run --seeds 0..1000 --threads 16`, one session, load about 23, raw counts (× 10 for per-10k):

| | BASE | R41 600, all rounds | R41 600, no drop rounds | **R41 1000, targeted, no drop rounds** |
|---|---|---|---|---|
| Rounds | 1,540 | 1,534 | 1,532 | 1,543 |
| **Q4** | 0 | 204 | 140 | **176** |
| LPID freed after the flip and reused, crash's window | 0 | 204 | 137 | 176 |
| Recovered image's own window held both | 3 | 117 | 83 | **152** |
| Q5a (row 200) | 24 | 20 | 20 | 25 |
| Q7 (row 200) | 21 | 15 | 20 | 22 |
| Q18 (row 1,000) | 97 | 536 | 425 | 495 |
| CPU s / wall s | 1,359 / 118 | 2,041 / 161 | 1,832 / 145 | 1,879 / 147 |

The right-hand column would meet this card's rule on these seeds: Q4 × 10 = 1,760, floors held, wall +25%, CPU +38%. Its failing rounds are known classes (MP O-3(e), MP-A, C-XX 310, 332, 881) plus 224 r0 (C-XX shape) and 845 r0 (finding 1). No store, oracle or quota text changes.

**So this is within P's authority: a 5.2 amendment, X5-R41, after -F2.** I say so plainly. Q4 needs no owner action to become reachable. The standing rule still sends this to the owner, and there is a real question left, below.

**What Q4 asks, against the plan row.** The row says "splits and merges during a checkpoint", beside a separate "a crash between every step 0–6". §5.6 joined them: interrupted, and both halves in one round. So Q4 asks for more than the row in one way (the crash must interrupt that window), and for less in another (no order, no interaction: seed 886 r2 counts with reuse 0).

**What the conjunction really covers.** §4.2 makes LPID reuse immediate. A merge freeing an LPID after the flip and a later allocation handing it out while image `k` still maps it is the store's one true SMO-against-SMO interaction, and it is where X-5c-F3's C-XX `next_lpid` bug lived. Seeded faults, over 0..200, R41 at share 1000 on all rounds, failing rounds (including known ones):

| Seeded fault | BASE | R41 | Split commit removed (workload splits remain) | Merge sweep removed |
|---|---|---|---|---|
| **Joint:** reuse drops `map.img[l]` | 3 | **24; 0 of 61 Q4-window reuse rounds; all at After4–After6** | 22 | 3 |
| **Slot:** free makes a durable home allocatable (I-FREE-SAFE) | 27, 8 in window | 55, 24 in window | 52, 20 in window | 35, 12 in window |

1. **Separate-round quotas would lose the joint case.** With the merge removed, it almost disappears (3 against 24). A merge in another round proves nothing about this. The same-round conjunction is worth keeping, and I would not trade it.
2. **Q4's crash position cannot see that case.** An interrupted image is discarded by construction; recovery reads image k−1 and replays. All 24 catches come from crashes after meta k is written. Q4's interrupted rounds prove something else, that k−1 survives SMOs in the window, and that case needs the merge half alone (20 against 24 in window).
3. **So E4 has a hole Q4 cannot fill, whatever its count.** At BASE the arm that sees the joint case has 30 rounds per 10k with both halves, 90 with a reuse. Under R41 it comes free at about 1,520 and 1,590, from the hooks After4–After6 of the same selected rounds, but nothing requires it.

Limits of this: one seeded joint fault of my choosing, 200 seeds, a scratch harness. It shows Q4 is blind to that fault, not that no fault exists which only Q4 could catch.

**MP ≥ 100: my read does not change it.** R41 does not raise MP (`szc` 0 in MP over 1,000 seeds at share 600, raw). It does change the wall: about +25–38% on BASE's 17 min. It also removes reasons for other cost: Q18 is met with room (4,950 per 10k), so k1's gc 12 and every 4–8 and the burst knobs lose their Q4 and Q18 purpose. F4 tries first, as before. If it comes up short, the budget is the owner's, and X5-R17 calls it a time budget, not a correctness rule.

## OWNER QUESTION (nothing is lowered or redefined; P's recommendation is A)

Q4 ≥ 1,000, "crash interrupts a checkpoint with ≥ 1 split and ≥ 1 merge in its window", read 0 and is now reachable by a harness change at about 1,760 per 10k (probe). The data shows Q4 cannot see the interaction its conjunction exists for, since the interrupted image is thrown away. Which should E4 require?

- **A (recommended): Q4 exactly as written, plus a new row Q4-I:** ≥ 1,000 crash rounds whose final recovery opens an image whose own window held an LPID freed after its flip and reused before its table froze. Cost: no new mechanism; probe about +25% wall and +38% CPU; 1,520 per 10k counted as "both", with reuse at 1,590, so the 1,500 margin is thin and -F3 may need a little more. It adds a condition before E4's decision, a strengthening. **Why:** E4's verdict decides whether Phase A opens, and A-7a and A-7b build this reuse and image rule. Covering it here costs minutes per campaign; a hole found after Phase A opens costs far more. If adopted, A-9 should carry the same row at ×10, as SIZING does.
- **B: Q4 exactly as written, nothing added.** It needs the same mechanism at the same cost, so A's only extra cost over B is the row. E4 passes by the letter, with the image arm present but unguarded: a later retune can silently remove it.
- **C: separate quotas in place of the conjunction.** Not recommended, and the data argues against it: it removes the one thing the conjunction does (merge removed: 3 failing rounds against 24). It saves nothing, since R41 reaches the conjunction anyway.

## X-5c-F4 card changes (paste as REFRESH 8, after REFRESH 7, overriding it where they differ; **they apply only after the owner answers**)

> **REFRESH 8 (P gate X-5c-F4-F1-F1, MERGE-WITH-FOLLOWUPS, 2026-11-05).**
> - **Status.** Blocked on X-5c-F4-F1-F1-F2 and X-5c-F4-F1-F1-F3, in that order. Fast-forward `card/v3-X-5c-F4` to -F3's gated tip, then relaunch `--keep`.
> - **Q4.** Definition and threshold unchanged. X5-R40 is gone; its 0..200 result is in `verdicts/X-5c-F4-F1-F1.md`. Use -F3's handed-off `r41_share` (X5-R41), and hold it beside FINAL's other knobs. Do not use burst knobs for Q4 or Q18: the probe reads Q18 about 4,950 per 10k under X5-R41. Keep a burst only if another row needs it, with its number.
> - **Q4-I (X5-R42).** [Owner option A only.] The row exists from -F3. Report it beside Q4, and judge it in `verify` like every row. [Option B: `INFO` only.]
> - **Digests.** -F2 moves BASE digests, and -F3 takes its list. This card's baseline is the new file, not `bec7056c…`; the F4-F1 re-gate's counts in REFRESH 7 are close but no longer exact.
> - **Step 4 notice, corrected.** Seeds 221, 423, 370, 151 and 190 on retuned knobs are **C-XX**, the class of 51 rounds in the baseline file. They are not new and not "ordinary"; X5-R16 applies. A class C O-1 **with no XX001** is new: stop and ESCALATE with the seed.
> - **MP.** Unchanged: if any row, MP above all, cannot be met at ≤ 40 min with every other quota held, ESCALATE with the numbers. X5-R41 costs about +25–38% on BASE by probe, so measure it, and do not assume REFRESH 6's table holds.
> - **Your own `gate.sh`.** Inside your container, `runner e2e: K` reads red: opencode as PID 1 reaps no orphans, and the test's probe reads the zombie as alive (X-5c-F4-F1-F1-F1). It is the only row. The runner's Acceptance is the truth. Do not chase it, and do not run a subreaper to make it pass.

## Follow-ups

**X-5c-F4-F1-F1-F1** (`cards/X-5c-F4-F1-F1-F1.local.md`; Class test, Gate P, delegated). `res()`'s `dead()` in `scripts/test-enforcement.sh` becomes zombie-aware, with rows under a non-reaping init. Queue right after X-6-F1-F1-F2-F1-F2. It does not block the held card or F4.

**X-5c-F4-F1-F1-F2** (`cards/X-5c-F4-F1-F1-F2.local.md`; Class core, Gate P). Finding 1: a literal schedule first, then the install guard, then the X5-R37 literal list. Critical path. It must land before F4's final file. It may run before the owner answers.

**X-5c-F4-F1-F1-F3** (`cards/X-5c-F4-F1-F1-F3.local.md`; Class experiment, Gate P). X5-R41, pre-registered, replacing X5-R40, with a decision rule and the image-arm fields. X5-R42 if the owner picks A. Waits for -F2 and the owner's answer.
