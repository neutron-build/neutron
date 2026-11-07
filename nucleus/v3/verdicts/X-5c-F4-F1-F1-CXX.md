Card: X-5c-F4-F1-F1-CXX
Class: core
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-X-5c-F4-F1-F1-CXX-gate-3 (T3 review sub-agent, Claude Code session 38efbbc2-b2a5-4152-b0b8-2e419db10695)
Date: 2026-11-06
Gate-exit: GATE EXIT=0
Commit: 56758f38871ad39e9a8f5a2743e08c38f718ed63
Mutants: M-DROP-UNPIN-EARLY -> drop_last_step_keeps_its_pages_pinned, G3-FREE-IN-SECOND-HOLD -> drop_one_hold_the_flip_never_finds_the_straddle, G2-ORPHAN-KEEPS-SLOT -> exhausted_reserve_keeps_both_copies, G-W11-NORETIRE -> x5_r38_w11_needs_no_completed_sync_since, M-DROP-ENTRY-SPLIT -> drop_cut_never_names_a_freed_root and drop_cut_never_names_a_reused_root, M-EXHAUST-NO-REVALIDATE -> exhausted_reserve_keeps_both_copies and exhausted_record_stays_out_of_the_next_window, M-EXHAUST-RECORD-FIRST -> exhausted_record_stays_out_of_the_next_window, M-REPLAY-DROP-WALK -> drop_replay_only_marks, M-DROP-FREE-FIRST -> drop_entry_gone_before_root_free, G7-R21C-NOREPAIR -> r21c_the_real_rounds_retry_re_pairs_its_twin, G6-UNPIN-BEFORE-FREES -> UNCAUGHT at the literal seed 38 (fails at 467; finding 3, closed by -F1)
Followups: X-5c-F4-F1-F1-CXX-F1

## Summary

All eight rework items are closed, and the fix holds: C-XX is 0 over 0..10000. Failing rounds fall from 76 to 24, none in a new class. One seed-level defect remains, 7469. It is a harness gap in X5-R21 (c)'s re-pair, not a store or oracle fault, and this card's limits freeze that code. So it goes to -F1, which FINAL needs.

## Gate run

Scaffold `fresh-gate.sh` and `git-env.sh` through the `--no-local` clone, scaffold id ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0, `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-5c-F4-F1-F1-CXX`, with no other job of mine running (another session's `cargo check` was on the host):
- `fresh-gate: commit 56758f38871ad39e9a8f5a2743e08c38f718ed63`
- `fresh-gate: base ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0`
- `fresh-gate: checks passed (4510 files hashed in 1.28 s)`
- `fresh-gate: 2 file(s) matched through their eol conversion`
- STEP test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests all `EXIT=0`
- `GATE EXIT=0`, stderr empty

`runner e2e: K`, which the worker saw red in its nested session, is green here. That matches the card's Notes.

Scope: `10567da6..56758f38` touches only README.md, `src/store/tree.rs`, `tests/store.rs` and `tests/workload.rs` under `nucleus/v3/spikes/e4-recovery/`. No protected path.

Mutants and probes ran on scratch exports, and the worktree stayed clean. Evidence: `~/.cache/nv3-review/X-5c-F4-F1-F1-CXX/gate3/`.

## Rework items

| # | Item | Result |
|---|---|---|
| 1 | PinGuard kept across the last step | Closed. Live through the frees, released on each error arm, other arms unchanged, `evict:finish` untouched. My scan at the tip: 0 of 1..=1000 end mapped and free |
| 2 | `drop_last_step_keeps_its_pages_pinned`, M-DROP-UNPIN-EARLY | Closed. Fails at seed 38 under the mutant. See finding 3 |
| 3 | W11 literal | Closed. Seed 30 reads `w11-` clean and `w11` under G-W11-NORETIRE |
| 4 | X5-R37 literals and late-copy digests, one commit, attribution | Closed. 112, 1769 and 7469 moved, as the window confirms. README text slip in finding 4 |
| 5 | One-hold pin under G3 | Closed. Fails at seed 5 under G3, and 0 of 1..=200 fail at the tip |
| 6 | Pin 3 under G2 | Closed. `slots [13] entered used_cur referenced by nothing`. See finding 5 |
| 7 | Pin 2's assertion | Closed. Eight cells, comment and code agree |
| 8 | README | Closed. The finding, 444 and 3005 as the census's blind spot |

## The 10k window

`e4-campaign run` at BASE over 0..10000, lists of 200 seeds, none in the held-out set. No deadlock, panic or baton flag. The classifier reproduces 26 on the f703c08d file.

| Class | File | f703c08d | Tip | Note |
|---|---|---|---|---|
| C O-3(b) XX001 (C-XX) | 51 | 2 | **0** | 444 and 3005 clear; 190, 6195, 6990 pass |
| C O-1(c) (O1C) | 3 | 4 | 3 | 1066, 6555, 7590; 1376 is gone |
| MP O-3(a) and O-3(e) | 21 | 20 | 21 | 19 from the file, plus 457 and 8777 in existing shapes |
| MW O-1(b) | 1 | 0 | 0 | |
| **Failing rounds** | **76** | **26** | **24** | No new class |
| Defect seeds | 0 | 0 | 1 | 7469, finding 1 |

- **The tip equals my pin-held trial:** all 10000 digests are identical to `trial-10k.json`. So the worker's unrun 54-seed loop is covered, and I accept it.
- **Twins:** failing twin rounds go 20 -> 6, all `O-1(a) ... published {}` at round 1 of MW seeds. 6539 and 7179 are new in that shape; 609, 849, 3449 and 5049 are from the file.
- **50000..50200:** only 50028 and 50137 fail, as the card predicted. 50025 clears.
- **O1C-F1:** it still owns 1066, 6555, 7590. 1376 passes at the tip with `(lo, hi, c, frontier)` = (5, 7, 7, 7), so it drops out. Its `r47_three_rounds` values still fit: at the tip `lo` is 20, 22, 19 and the frontier 21, 24, 21, as that card lists. `hi` with today's derivation reads 20, 23, 20.

## Findings

### 1. 7469: a re-paired twin that never fires (X5-R16 treatment; owner -F1)

Round 1 of seed 7469, traced on a scratch copy with an unchanged digest:

| Attempt | Spec | Fired | Masks |
|---|---|---|---|
| Twin, first | `Mid2@0` | yes | `[f]` |
| Real, first | `Mid2@0` | no | `[3f7, 3ff, 3f7, 3ff, ...]` |
| Twin, re-paired | `Mid2@1` | **no** | `[3ff, 3f7, 3ff, 3f7, ...]` |
| Real, final | `Mid2@1` | yes, passes | |

- **Reproduces:** the same defect and digest 1670717970 in my trial, the window and a single-seed run.
- **Cause:** `src/workload.rs:1940`. The R21 (c) re-pair runs the twin at a spec derived only from the real round's masks, and the chains have diverged. Unlike the other two retry arms, this arm has no re-derive.
- **Classification:** instrumentation, NEVER-FIRED on the re-pair arm. It is not the store's: on the twin's chain, checkpoint 1 never reaches `Mid2` (bit 8 of its mask is clear), the shape X5-R16 describes. It is not an oracle miss either, because the guard failed loudly as designed.
- **Cost:** a failing seed under the fired-guard. The seed also stops at round 1 because recovery returned no store, so it loses round 2.
- **Fix:** X5-R48 in -F1, a joint spec from both complete mask lists. My scratch trial (`trial-joint-repair.diff`): `Mid2@4`, both fire, three rounds pass, no defect, digest 1393729414. The literal pin goes in -F1.
- **Why not REWORK:** Limit 1 and Limit 2 freeze this path, and the card's Do not forbids it.

### 2. r21c's literal 7469 -> 9469: accepted, with a condition (worker's judgment call)

9469 bites: G7-R21C-NOREPAIR fails the pin there. It is also the only one of the five seeds that reaches the re-pair with a firing twin. By trace, 2669 re-derives in the twin's first attempt, and 4049, 4309 and 9809 re-derive nothing, so the pin's doc overstates them. The move hid no store fault, but it did remove the one literal that would have shown the defect. So -F1 restores 7469 to the list.

### 3. The pin hold's narrower form is uncaught at the literal seed (minor; -F1)

G6-UNPIN-BEFORE-FREES releases the guard after the directory descent and before the tree mutex is re-taken. It passes the pin at seed 38, and fails at 7, 467, 488, 542, 589, 595, 693, 817 of 1..=1000. The campaign sees it too: 444 fails one round. -F1 adds 467, which also fails under the f703c08d code.

### 4. README count (minor; -F1)

"Of the nine, four first moved at the replay change" disagrees with the README's own table: ten seeds, three at replay, one at B1.

### 5. Pin 3's slot 5 (observation, no card)

The pin expects one extra unreferenced slot, as measured. By reading: `unpin_home` keeps a slot pinned by `image_refs`, and post-5 clears `image_refs` without releasing it from `used_cur`. So the slot stays used until the next open. I did not trace slot 5 itself. §4.4 would let it be reused, so this is a space leak in the spike and not a safety fault. The store is frozen here, and the spec is right for Phase B. If it is ever fixed, the pin's literal fails loudly. Accepted.

### 6. What I checked and found right

- **Unpin after free:** `unpin` works by LPID, so a dropper could in principle steal a pin from a task that reused the freed LPID. Probe over 0..10000: 0 hits, and the probe is live on 310, 8010 and 444. No finding.
- **W11:** seed 30 accepted, the pre-CXX literal.
- **Pass/fail on the three movers:** unchanged.
- **Two rework-6 commits:** harmless, no history rewritten.

## Rulings

- **r21c literal:** accepted under the Limit-3 class, condition in finding 2.
- **The 54-seed loop not re-run:** accepted, covered by the window.
- **Pin 3's literal slot:** accepted, finding 5.

## Mutants

| Mutant | Pin | Result |
|---|---|---|
| M-DROP-UNPIN-EARLY | `drop_last_step_keeps_its_pages_pinned` | caught, seed 38 |
| G3-FREE-IN-SECOND-HOLD | `drop_one_hold_the_flip_never_finds_the_straddle` | caught, seed 5 |
| G2-ORPHAN-KEEPS-SLOT | `exhausted_reserve_keeps_both_copies` | caught, slot 13 |
| G-W11-NORETIRE | `x5_r38_w11_needs_no_completed_sync_since` | caught, `w11` |
| M-DROP-ENTRY-SPLIT | `drop_cut_never_names_a_freed_root`, `drop_cut_never_names_a_reused_root` | caught, both |
| M-EXHAUST-NO-REVALIDATE | `exhausted_reserve_keeps_both_copies`, `exhausted_record_stays_out_of_the_next_window` | caught, both |
| M-EXHAUST-RECORD-FIRST | `exhausted_record_stays_out_of_the_next_window` | caught |
| M-REPLAY-DROP-WALK | `drop_replay_only_marks` | caught |
| M-DROP-FREE-FIRST | `drop_entry_gone_before_root_free` | caught |
| G7-R21C-NOREPAIR (mine) | `r21c_the_real_rounds_retry_re_pairs_its_twin` | caught, seed 9469 |
| G6-UNPIN-BEFORE-FREES (mine) | `drop_last_step_keeps_its_pages_pinned` | UNCAUGHT at 38; fails at 467, finding 3 |

## Follow-ups

**X-5c-F4-F1-F1-CXX-F1** (`cards/X-5c-F4-F1-F1-CXX-F1.local.md`, Status: waiting, Gate P, Base this card). X5-R48: when a re-paired twin never fires, twin and real re-run at a spec derived from both complete mask lists, so 7469 stops being a defect seed. It restores 7469 to the r21c pin, adds seed 467 to the last-step pin, and corrects the README count. Expected digest movement: 7469 only. FINAL needs it; it is independent of O1C-F1.
