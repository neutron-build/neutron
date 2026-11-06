Card: X-5c-F4-F1-F1-CXX
Class: core
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-X-5c-F4-F1-F1-CXX-gate-1 (T3 review sub-agent, Claude Code session 6fd4d159-37b7-4ac0-a43f-3dfe333ca550)
Date: 2026-11-06
Gate-exit: GATE EXIT=0
Commit: f703c08d69c91105e84527d6d1752b8baf5becb0
Mutants: M-DROP-ENTRY-SPLIT -> drop_cut_never_names_a_freed_root and drop_cut_never_names_a_reused_root, M-EXHAUST-NO-REVALIDATE -> exhausted_reserve_keeps_both_copies and exhausted_record_stays_out_of_the_next_window, M-EXHAUST-RECORD-FIRST -> exhausted_record_stays_out_of_the_next_window, M-REPLAY-DROP-WALK -> drop_replay_only_marks, M-DROP-FREE-FIRST -> drop_entry_gone_before_root_free, G1-REVALIDATE-ANY-COPY -> exhausted_reserve_keeps_both_copies, G4-STEP-LEAKS-CHILD -> r4_positional_id_discriminates_the_empty_leaf_plant, G2-ORPHAN-KEEPS-SLOT -> UNCAUGHT, G3-FREE-IN-SECOND-HOLD -> UNCAUGHT by the directed suites (campaign: 25 rounds of the 54 seeds), G5-MEM-DIR-AFTER-FREE -> UNCAUGHT (equivalent on the pinned schedule), G-W11-NORETIRE -> UNCAUGHT at the moved literal (finding 2)
Followups: none

## Summary

The entry-first order, the B1 re-validation and pass 2's mark-only drop are right, and all 51 C-XX rounds of the file clear. But the last step drops its pins before the directory descent, which §6.8 forbids, and that introduces a new faultless class C XX001 at BASE: 444 r1 and 3005 r1. C-XX did not fall as the census predicted. The moved W11 literal has also lost its bite. Both are fixable inside the card's limits, and holding the pins clears C-XX to 0 over 0..10000 in a scratch trial.

## Gate run

Scaffold `fresh-gate.sh` and `git-env.sh` through the `--no-local` clone, scaffold id ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0, `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-5c-F4-F1-F1-CXX`. Three runs:

1. Killed by a T3 server restart during `enforcement-tests`, no exit line. Not a result.
2. `GATE EXIT=1`, `STEP test EXIT=101`, on `ack_prefix_two_batches_in_flight` alone (`driver timeout: B never completed batch 2`), every other step 0. My own window was running at load 26-28. This is the known flake, card X-5c-F4-F1-F1-F2-F2.
3. Alone on the host. Recorded result:
   - `fresh-gate: commit f703c08d69c91105e84527d6d1752b8baf5becb0`
   - `fresh-gate: base ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0`
   - `fresh-gate: checks passed (4509 files hashed in 1.28 s)`
   - `fresh-gate: 2 file(s) matched through their eol conversion`
   - every STEP EXIT=0
   - `GATE EXIT=0`

Scope: `card/v3-X-5c-F4-F1-F1-F2...f703c08d` touches only `nucleus/v3/spikes/e4-recovery/` (README.md, src/campaign.rs, src/harness.rs, src/store/ckpt.rs, mod.rs, prims.rs, tree.rs, tests/store.rs, tests/workload.rs). No protected path.

Mutants and my probes ran on scratch exports, never in the worktree, which stayed clean. Evidence: `~/.cache/nv3-review/X-5c-F4-F1-F1-CXX/`.

## Findings

### 1. The last step frees pages it no longer has pinned (blocking; I-LPID-EXACT, I-LINK-ATOMIC, §6.8 step 3)

`dir_drop_tree` runs `drop(pins)` right after `drop_walk_step`, before `dir_remove_entry_held`. §6.8 step 3 says the last step pins the root and every page it empties before its first mutation and holds them through the one hold. Unpinned, the root's dirty frame is a legal victim during the directory descent's I/O:

1. The evictor latches the root and yields in `write_fresh`.
2. `free_lpid` clears the frame, removes `map.cur[root]` and adds the LPID to the free set.
3. `evict:finish` runs `map.cur.insert(key, r)` unconditionally and puts the freed root back.

The LPID is then both mapped and free, with no reference to it. The base did not reach this, because its walk freed under its pins.

Measured, with a trace on a scratch copy, digests unchanged:
- 444 r1: `LAST tree 3 path [4, 63]`, `FREE 4`, then `EVICTFINISH-RESURRECT lpid 4 ... slot 109 ... free_has=true`. Amcheck (ii): `mapped lpid 4 unreachable`.
- 3005 r1: `LAST tree 4 path [5, 75]`, the same resurrection, slot 132. Amcheck (ii): `mapped lpid 5 unreachable`.

Both pass at the base digests. Neither is in the census or the 54 seeds, which is why Work 5 read 0. S-1-F7's trial range and 50000..50200 miss them too: over that window the tip fails only 50028 and 50137, and 50025 clears, as the card predicted.

Directed reproduction, clean tip: a dirty leaf root, a dropper and an evictor looping `evict_one(&cx, false)`. Seeds 38, 85, 444 and 467 of 1..=500 end with the root mapped and in the free set. With the pins held, 0 of 500. Source below and in `gate_evict_scan.rs`.

Trial on a scratch copy, the pins moved past the frees (`trial-hold-pins.diff`, 4 hunks in `tree.rs`, a diagnostic, not code to copy): the 54 seeds plus 444, 3005 and 1376 give 0 failing rounds, and the drop pins pass. Over 0..10000, C-XX is 0. One seed carries a defect, 7469: `twin round 1: the crash spec never fired (Mid2@1)`, the NEVER-FIRED shape, on a moved schedule. The re-gate must read that seed again; it is not at the tip.

### 2. The moved W11 literal is vacuous (blocking; Limit-3 ruling below)

`x5_r38_w11_needs_no_completed_sync_since`'s doc says G-W11-NORETIRE fails it. At seed 11 it does not: with the retain line removed the pin passes and the pick still reads `w11-`, so that round has no boundary to retire. Seed 11 is a seed that reads `w11-`, not one that tests the rule. Scan of 0..199, clean against the mutant, at this tip: 50 seeds read `w11-`, and only 37, 98, 111, 138, 156, 170 and 192 change to `w11` under the mutant. The worker's RESULT calls the invariant unchanged but shows no mutant run.

### 3. Two rules have no directed pin (rework adds them)

- **The gate is never taken twice** (Work 2). G3 releases and re-takes the gate between the entry removal and the frees. Every directed test passes; only the campaign sees the leak, at 25 rounds of the 54 seeds.
- **The orphan releases its slot** (Work 1). G2 keeps the slot in `used_cur`. The suites pass and the 54 seeds read 0 failing rounds, so the slot just leaks until the next open.

### 4. Pin 2's assertion disagrees with its comment (minor)

The comment says only the eight phase-A cells are guaranteed. The code demands nine (`n < 9`, `0..9`). It holds at the pinned seed 77, but seeds 7, 15, 19, 26, 42 and 53 read `8 of 9` with no loss: the ninth cell is an unlogged insert above the flip. With the assertion at eight, all 60 seeds pass.

### 5. What I checked and found right

- **Entry-first order (§6.8 as merged).** In one gate hold: entry, in-memory record, then the path top-down, root first. The mutexes never nest. The pins are the only departure. `dir_remove_entry_held`'s error paths release the gate at the caller.
- **B1.** `record_image_at` is inside the re-validation. G1 and M-EXHAUST-RECORD-FIRST both fail their pins. An ABA on `(key, is_epoch_copy)` needs a re-freeze of the same LPID in one window, and `map.img` stops that.
- **`epoch_copy:pick` then `:install`.** I suspected the same race on the free-reserve path. 400 seeds of two first touches over a free frame show no overwrite: `cx.with` holds the baton, so the pair has no yield. No finding.
- **§14 step 4.** Both replay passes mark only, and `resume_drops` walks at step 6. `resume_drops` itself is untouched.
- **Mutant-table rows and the census.** On the 54 seeds, M-DROP-ENTRY-SPLIT fails 28 rounds (§15's probe figure, 28) and M-EXHAUST-NO-REVALIDATE 22 (the card's A-fix-alone figure, 21, plus moved seeds).

### 6. Digest movement: explained, no hidden behaviour change

I re-ran every X5-R37 and late-copy seed at each commit. The moved set equals the worker's list, and each move has one owner:

| First moves at | Seeds |
|---|---|
| Step 1 (B1) | 190 |
| Step 2 (A) | 66, 907, 1769, 6149, 7469, 7528 |
| Step 3 (replay) | 112, 198, 303 |

Fields that differ are the crash draw and what follows from it. Pass/fail does not change except 190 r1, which clears. 7528 fails at MP in both. The README's "every drop-bearing schedule re-derived" is loose: three moves are the replay change, one is B1.

### 7. The 10k by class

`e4-campaign run` at BASE over 0..10000, lists of at most 200 seeds, none in the held-out set. No defect, deadlock or panic at the tip.

| Class | File | Of those, still failing at tip | New at tip | Trial with pins |
|---|---|---|---|---|
| C O-3(b) XX001 (C-XX) | 51 | 0 | 2 (444, 3005) | 0 |
| C O-1(c) (O1C) | 3 | 3 | 1 (1376) | 3 |
| MP O-3(a) and O-3(e) | 21 | 18 | 2 (457, 8777) | 21 |
| MW O-1(b) | 1 | 0 | 0 | 0 |
| Total | 76 | 21 | 5 | 24 |

No new class: the new rounds sit in existing shapes, and the two XX001 are finding 1. O1C: 1066, 6555 and 7590 fail as before (frontier above `hi`, no XX001), and they need O1C-F1. `E4_O1C_TRACE` on a scratch tip puts 1376 r0 in the same shape: a recycled inode 2, `ts=24` above a stitched walk that jumps at an earlier life's bytes. It is gone in the trial, so it is a schedule move. O1C-F1's "3 rounds" should be read against whatever the rework's tip shows.

## Rulings

- **Limit 1, `harness.rs:670`.** Accepted. A one-token signature update with no behaviour change, and `tests/store.rs`'s matching token at `wal_loss_below_ckpt_end_fails_closed` (not in the RESULT) likewise.
- **Limit 1, "their helper".** Accepted. There are three new functions, not two: `dir_mark_dropping`, `dir_drop_tree_replay`, `dir_remove_entry_held`. All are the drop's own machinery.
- **Limit 3, W11.** The class is accepted: this pin is a schedule literal of the X5-R37 kind and had to move. The chosen seed is rejected (finding 2).
- **Pin 2, seed 7 -> 77.** Accepted. The cause was the worker's fixture, and the mutant shows the reuse shape at 77.
- **Watcher pin.** Accepted, and the RESULT undersells it. The committed pin checks both the persisted cells and `dir`, as its doc says. Clean over seeds 1..=60. G5 passes because this schedule has no yield between the frees and the record's removal, and no image can cut inside the hold, so it needs no pin.
- **The worker's unconfirmed "stale 6-cell page": refuted, a fixture bug, no store defect and no card.** The pin's prefill patch did not apply: the file still held `for i in 0..6u64` (attempt 2, entry 197) while the worker's diagnostic inserted nine and passed, which is why the two disagreed. Rebuilt with 122 trees: the prefill as committed keeps every durable cell at all 8 seeds tried, and `0..6` reproduces the report (`6 of 9` at seed 7). X-5c-F4-F1-F1-F2-F1 needs no change.

## Rework

1. **`tree.rs`, `dir_drop_tree`:** keep the walk's `PinGuard` across the last step until after the frees (§6.8 step 3). Every other arm unchanged. Do not touch `evict:finish`: Limit 1 does not name it, and §6.8 is the rule.
2. **New pin `drop_last_step_keeps_its_pages_pinned`,** from the source below at a literal seed, failing at f703c08d (38 does). Hand mutant M-DROP-UNPIN-EARLY, shown failing and reverted.
3. **W11 literal:** a seed that reads `w11-` clean and `w11` under G-W11-NORETIRE, shown failing. Re-measure after step 1, since these seeds may move. Correct the pin's doc and the README.
4. **Re-derive once, in one commit,** the X5-R37 literals and the late-copy digests. Attribute each move in the README by the table in finding 6, as re-measured.
5. **New pin for the one hold:** a flip that waits for "entry absent, root mapped" must never find it. It must fail under G3, shown.
6. **Pin 3:** assert the orphan's slot left `used_cur`. It must fail under G2, shown.
7. **Pin 2:** the assertion matches its comment, eight cells.
8. **README:** this finding, and 444 and 3005 as the census's blind spot.

Run hygiene stands. The re-gate runs the window and reads 7469.

## Gate's pin source (clean tip: fails at seeds 38, 85, 444, 467 of 1..=500)

```rust
fn gate_evict_body(seed: u64) -> Result<(), String> {
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    let (disk, st, root2) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx { w: &sched, t: HARNESS };
        for id in [2u64, 3, 4, 5] {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
        }
        let root2 = tree::dir_root_of(&cx, 2).expect("root 2");
        ins(&cx, 2, root2, &1u64.to_be_bytes(), 1, &val(1, 1));
        cx_checkpoint(&sched, |_| Ok(()));
        ins(&cx, 2, root2, &2u64.to_be_bytes(), 2, &val(2, 2));
        let g = sched.lock();
        (g.disk.clone(), g.st.clone(), root2)
    };
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                tree::dir_drop_tree(&cx, 2).map_err(fail_of)?;
                set_flag(sched, "d_done");
                Ok(())
            }
            2 => {
                while !flag(sched, "d_done") {
                    let _ = prims::evict_one(&cx, false).map_err(fail_of)?;
                    cx.yield_now("gate:evict").map_err(fail_of)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(3, seed, disk, st, body);
    report.assert_clean("the gate evict schedule");
    let g = sched.lock();
    let mapped = g.st.map.cur.contains_key(&root2);
    let free = g.st.slots.free_lpids.contains(&root2);
    if mapped && free {
        return Err(format!("seed {seed}: root {root2} is mapped and in the free set"));
    }
    Ok(())
}
```

## Follow-ups

None. The rework carries everything above.
