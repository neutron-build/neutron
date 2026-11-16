Card: X-5c-F4-F1-F1-F2
Class: core
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (nv3-X-5c-F4-F1-F1-F2-gate-1, 2026-11-05)
Date: 2026-11-05
Gate-exit: GATE EXIT=0
Commit: 5b038acec4bd908d46e2e9ff3d62de2e18c32751
Mutants: M-LATECOPY-INSTALL -> late_epoch_copy_must_not_enter_the_next_image, M-LATECOPY-DROPALWAYS -> late_epoch_copy_must_not_enter_the_next_image, M-LATECOPY-ATONLY -> freed_lpids_late_copy_never_remaps, own G-GUARD-ABSENT-OK -> freed_lpids_late_copy_never_remaps, own G-GUARD-WINDOW -> late_epoch_copy_must_not_enter_the_next_image, own G-GUARD-ONLY-ABSENT -> late_epoch_copy_must_not_enter_the_next_image, own G-GUARD-EVICT-AT -> UNCAUGHT (finding 3)
Followups: X-5c-F4-F1-F1-F2-F1, X-5c-F4-F1-F1-F2-F2

## Plain statement

The fix is right and it holds at 10k. A late copy reached a later scan 7 times over BASE 0..1000 before the fix, and 0 times over 0..10000 after it. It also clears the F4 file's one silent O-1(b), 4419 r1, which nobody had attributed. The worker's sibling is real and was right to escalate, but it is not a new cause and not a spec gap. It is B1's own missing re-validation with a second, silent outcome. CXX's Work 1 already closes it, and CXX needs one more pin to hold it there. No spec changes. The README's "no seed fails before or after" is wrong: 3 rounds now fail that passed, all B1, and all clear under CXX.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone, scaffold b13a0971b1ce9c966433b660f55c21870359fd74, on the full commit, from `~/Workspace/neutron-v3`, as a background task. **Run three times, and all three are reported.**

| Run | Host load | Result |
|---|---|---|
| 1 | 25-30, with my mutants and probes running | `STEP test EXIT=101`, every other step 0, `GATE EXIT=1` |
| 2 | 11-25, after my 10k | same, same test, `GATE EXIT=1` |
| 3 | 3-15, nothing of mine running | every STEP EXIT=0, literal last line `GATE EXIT=0`, stderr empty |

- The lines of run 3: `fresh-gate: commit 5b038ace…`, `fresh-gate: base b13a0971…`, `fresh-gate: checks passed (4508 files hashed in 1.29 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. 447 tests passed. `$fg` removed after each run. Runs 1 and 2 stopped at the `store` suite, so only run 3 exercised `tests/workload.rs` in the gate. See finding 2.
- **Why a re-run is sound here.** Both red runs have one cause, `ack_prefix_two_batches_in_flight`'s driver timeout, on a path this card does not touch (`writer:prewrite`, the WAL). The same test fails the same way at a4c1f869, before this card's change (1 of 4 solo runs). I did not change the test, the tree or the gate between runs.
- **Scope.** `git diff a4c1f869..HEAD` touches only `README.md`, `src/store/prims.rs`, `tests/store.rs` and `tests/workload.rs` under `nucleus/v3/spikes/e4-recovery/`. The store diff is the guard and the `epochcopy:prewrite` park, both inside `record_image_copy`, plus a header comment. No new non-test `unwrap`/`expect`/`panic`/`unsafe`/`std::env`/wall clock. The worktree is clean and the tip did not move.
- **Scratch.** Mutants and probes ran on `git archive` exports under `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F2/`, kept there. Every number below from a probe binary is a probe. The probe binary with no toggle reproduces the tip's digests; with `E4P_NOFIX` it reproduces the base's over 0..1000.

## Mutants

Each ran over the whole `e4-recovery` suite, in release, `--no-fail-fast`, and was reverted (`CLEAN` against the commit at the end).

| Mutant | Result | Also failing |
|---|---|---|
| M-LATECOPY-INSTALL | caught: `late_epoch_copy_must_not_enter_the_next_image` | `freed_lpids_late_copy_never_remaps`, `late_copy_count_is_zero_at_base` |
| M-LATECOPY-DROPALWAYS | caught: same pin, `step2_fixpoint_writes_late_epoch_copies` | `x5_r37_base_tuning_reproduces_1b0ce54e` and others |
| M-LATECOPY-ATONLY | caught: `freed_lpids_late_copy_never_remaps` only | |
| own G-GUARD-ABSENT-OK (an absent entry installs) | caught: `freed_lpids_late_copy_never_remaps` only | |
| own G-GUARD-WINDOW (re-test `flip_window` instead of `CopyPending`) | caught: `late_epoch_copy_must_not_enter_the_next_image` | `freed_lpids_late_copy_never_remaps`, `late_copy_count_is_zero_at_base` |
| own G-GUARD-ONLY-ABSENT (drop only when the entry is absent) | caught: `late_epoch_copy_must_not_enter_the_next_image` | `late_copy_count_is_zero_at_base` |
| own G-GUARD-EVICT-AT (`At(r)` with `cur == r` installs: the eviction arm) | **UNCAUGHT** | finding 3 |

`ack_prefix_two_batches_in_flight` also failed in the M-LATECOPY-INSTALL run. That is finding 2, not a catch.

## Re-measured

| Range | Tree | LATECOPY | STALECOPY | Failing rounds |
|---|---|---|---|---|
| 0..200 | pre-fix | 7 | 0 | |
| 0..1000 | pre-fix | 34 | 7 | 7 |
| 0..1000 | tip | 33 (all drops) | **0** | 8 |
| 0..10000 | tip | 234 (all drops) | **0** | 75 (F4's file: 76) |

- **The worker's numbers hold.** 0..200: 7 and 0. Per seed: 96 ×2, 104, 161, 190 ×3. Digests: 96 → 4266526636, 190 → 4013865743; 104 and 161 unmoved. 0..1000: 34 and 7, and 13 moved digests, equal to my earlier probe.
- **Decision rule, step 2.** Applied verbatim. Note that 0 before and 0 after over 0..200 has no power on its own. The conclusion stands on the wider ranges above: **the `ckpt2:scan` guard is not needed.**
- **Step 4.** All 54 X5-R37 literals equal my 10k digests, so no literal moves. 103 other seeds moved against F4's file (seed 4 also differs, by its known F4-F1 literal change, which the base reproduces).
- **Failing rounds against F4's file, 10k.** No deadlock, panic or defect record. No new shape.
  - **Out (4):** 5706 r1, 8725 r2, 8956 r0 (B2, as CXX's table says), and **4419 r1**.
  - **In (3):** 190 r1, 6195 r0, 6990 r2. All are `O-1(b)` with XX001 at `[AmcheckIi]`. Each trace shows B1's signature (`exhaustion frame 0 no longer holds …`, then `FREEZE unresolved CopyPending`). Each passes with CXX's Work 1 alone, and with all of the C-XX toggles (probe).
- **4419 r1 (class MW, faults injected) corrects my earlier verdict.** F4's file has it as `O-1(b) at c=41, tree 2: scan mismatch` with no O-3, in no queue row. At the base, a late copy reaches a scan in that seed (STALECOPY ×3, key 3, copy epoch 6, k=7). At the tip it passes, with the schedule moved. My verdict X-5c-F4-F1-F1 said F4's 10k held no silent class C O-1(b). That is literally true, because this round is MW, and it is the wrong conclusion: the mechanism did show up as a silent failure in the file. The owner should know.

## Does the fix hold against the spec?

Yes. §5.6's copy condition says a copy is made only while the content "is not yet at a slot the image map references". S-1-F7's draft goes further and rules the state out: the writer holds the page's latch across the install and step 2 waits on it. The prototype has no such latch, so step 2 writes the frame instead. The result is the same, because the parked task is the frame's mutator, so the frame still holds the epoch-`k` bytes the copy would have preserved. The guard is P's reading, in one atomic step.

Reuse of the `CopyPending` test across windows is not reachable. Every `store_page` in `src/store/tree.rs` runs under the gate (`insert_locked`, `link_split`, `gc_locked`, `root_collapse`, `dir_put_locked`, `dir_create_tree`, `drop_walk_step`, `dir_remove_entry`), and a held gate stops the next flip, so a parked copy crosses post-5 at most, never a second flip. The same holds for CXX's Work 1 re-validation.

The guard's drops are live at BASE over 10k: 231 found `At` (step 2's write or an eviction; the probe does not tell them apart) and 3 found the entry absent with the window closed (4419).

**The card's Note: does §5.6 need "re-test the copy condition after any wait on the reserve"? No, and no spec card.** S-1-F7's latch and wait make the state impossible, so a re-test sentence would describe something that cannot happen.

## The sibling ruling

**Same as B1?** Same defect, second outcome. It sits at the same step (`epoch_copy:record`), lacks the same re-validation and takes the same fix. CXX's Work 1 already says it "also keeps a late `record_image_at` out of the next window". It differs in what the failure looks like:
- B1 destroys a copy: `FREEZE unresolved CopyPending`, XX001, loud.
- The sibling inserts `Img::At(flip-k bytes)` into the map post-5 cleared. Nothing clears it, the next freeze takes it over `cur`, and the next image silently loses the mutations between the flips.

So it is not a new cause, but it is a new silent path that CXX's three pins do not hold. It needs its own pin.

**Measured.**
- The worker's repro fails at the tip: image 2 maps P to slot 32, epoch 1, crc 4182374088, with key 2 lost. It passes with Work 1 literally applied.
- The repro uses a raw `store_page`, so I wrote the pin below with a real gated `tree_insert`. It fails the same way at the tip, so **the path is reachable under the gate**. It passes with Work 1, and fails under the reading that guards the frame clear but records anyway.
- `late_epoch_copy_must_not_enter_the_next_image`, `freed_lpids_late_copy_never_remaps` and `step2_fixpoint_writes_late_epoch_copies` still pass with Work 1 applied. `late_copy_count_is_zero_at_base` will not (finding 5).
- **At BASE, 10k:** the record crossed post-5 only in seed 4419 (3 identical lines: key 6, slot 114). It reached a freeze 0 times, so no failing round is attributable to it.

**Prototype deviation or spec gap?** Prototype deviation. §5.2 records a previous-epoch victim only "during checkpoint `k` (after the flip, before step 2 has written that page)", and §5.6's exhaustion path cites §5.2. `evict:finish` re-tests `flip_window`; `epoch_copy:record` does not.

**Does S-1-F7's draft cover the rule?** Yes (draft 7, 3e937e59). §5.6's claim is under the frame's latch, and §13 step 2's data phase "waits on a latched frame", so the write-out and its record finish before step 2 ends, hence before post-5. **One gap, for nv3-S-1-F7-gate-1, not blocking this card:** that wait is the rule preventing this silent loss, and §15 names no mutant or test for it, only EXHAUST-DOUBLE-EVICT, the claim's half. Proposed: mutant **STEP2-NO-WAIT** (step 2's data phase ends with a claimed copy outstanding) → `t_step2_waits_for_claimed_copy` (A-3c).

**Which card?** CXX, with the change below. No separate card.

## Findings

1. **Non-blocking: the README and RESULT misstate the failures.** "No seed fails before or after" is false: at this tip, 190 r1 fails and passed before. The RESULT's "the per-seed records pass" read the seed-level fields, not `rounds[].pass`. The fix did not cause this: B1 was already there, and the schedule moved onto it. CXX's README corrects it (below).
2. **Non-blocking for this card, and it will hit other gates: `ack_prefix_two_batches_in_flight` flakes.** The driver busy-polls against a 30 s wall-clock deadline. Solo release wall time read 12-44 s at load 25-45; it failed in 2 of 3 fresh-gate runs (debug builds), 1 of 7 mutant suite runs, 0 of 10 solo tip runs and 1 of 4 solo base runs. A red `cargo test` also stops before later suites run, so it hides `tests/workload.rs`. Follow-up -F2.
3. **Non-blocking: the eviction arm has no pin.** G-GUARD-EVICT-AT passes everything. The doc comment names this arm, and nothing holds it. Follow-up -F1.
4. **Non-blocking: `freed_lpids_late_copy_never_remaps` overclaims.** Its doc calls the two-flip crossing "the shape a bounded copy wait reaches in the real system". No gated caller reaches it (see above). It is still a valid compression that catches the mutant. Follow-up -F1 adds the reachable form and corrects the comment.
5. **Non-blocking: `late_copy_count_is_zero_at_base` will go stale.** Its seed 190 literal includes a failing round, and CXX moves it (probe: 679383372 under Work 1 alone). CXX's Limit 3 names only the X5-R37 list, so it must also name this pin.

## Judgment calls

- **Digests instead of a count:** accepted. Limit 1 does forbid a counter, and M-LATECOPY-INSTALL, G-GUARD-WINDOW and G-GUARD-ONLY-ABSENT all fail the pin. The name promises more than it checks (finding 5).
- **Raw `store_page` and a direct free in the freed-LPID pin:** accepted as a stated compression, with the doc claim rejected (finding 4).
- **Step 0's failing commit, `crash_clean` as "crash after the meta", the empty literal update:** accepted. The last is confirmed by the 54-literal check.
- **Escalating rather than widening the fix:** correct, as Limit 1 required.

## The CXX card change (verbatim, for the orchestrator to apply to `cards/X-5c-F4-F1-F1-CXX.local.md`)

1. **Goal, after the paragraph beginning "The 3 with both are":** add:
   > **B1 has a second, silent outcome (verdict X-5c-F4-F1-F1-F2).** When `epoch_copy:record` lands after its checkpoint's post-5, instead of after another writer's clear, it inserts `Img::At(flip-k bytes)` into the cleared map. Nothing clears it, the next freeze takes it over `cur`, and the next image loses the mutations between the flips with no XX001. It is reachable under a gated tree insert. Work 1 closes it, because `ckpt:freecopies` has already cleared the frame. At BASE over 0..10000 it landed in one seed (4419) and reached no freeze. Separately, -F2's guard moved three rounds onto B1: 190 r1, 6195 r0, 6990 r2. Each shows B1's signature and passes under the probes.
2. **Read:** add `docs/S-STORE.md` §5.2 ("Dirty victims from the previous epoch") and `nucleus/v3/verdicts/X-5c-F4-F1-F1-F2.md`.
3. **Limit 3:** replace with: "**Tests:** the new pins; the X5-R37 literal list and `late_copy_count_is_zero_at_base`'s four digests are re-derived once, in step 5."
4. **Work 0:** change "Three literal pins" to "Four literal pins", and add:
   > - `exhausted_record_stays_out_of_the_next_window`: the source in verdict X-5c-F4-F1-F1-F2, verbatim. At this tip it fails with image 2 mapping P to its flip-1 leaf: left `{k1}`, right `{k1, k2}`.
5. **Work 1:** change "Pin 3 passes." to "`record_image_at` must be inside the re-validation: guarding only the frame clear fails pin 4. Pins 3 and 4 pass."
6. **Work 5:** replace "Run the 51 seeds in one single-seed loop." with "Run the 51 seeds plus 190, 6195 and 6990 (54 seeds) in one single-seed loop." Replace "update the literals in one commit naming this card" with "update the literals and `late_copy_count_is_zero_at_base`'s digests in one commit naming this card".
7. **Work 6:** add "and the correction to -F2's README: 'no seed fails before or after' is false (190 r1)".
8. **Mutation evidence:** change the M-EXHAUST-NO-REVALIDATE row's "Must fail" to "`exhausted_reserve_keeps_both_copies`, `exhausted_record_stays_out_of_the_next_window`", and add this row, applied by hand and not added to the `mutants` table:
   > | M-EXHAUST-RECORD-FIRST: `epoch_copy:record` guards the frame clear but calls `record_image_at` unconditionally | `exhausted_record_stays_out_of_the_next_window` |

The new pin, for the end of `tests/store.rs`. It builds on this tip's helpers, passes `rustfmt`, and is also at `~/.cache/nv3-review/x-5c-F4-F1-F1-F2/exhausted_record_pin.rs`:

```rust
/// **X-5c-F4-F1-F1-CXX step 0, pin 4 (verdict X-5c-F4-F1-F1-F2): the
/// exhausted path's record must not outlive its checkpoint.** A gated
/// tree insert (the real caller, gate and tree mutex held) parks writing
/// the oldest copy out of a one-frame reserve while its checkpoint runs
/// to the end: step 2 writes that copy, `ckpt:freecopies` clears its
/// frame, post-5 clears the map. It resumes in the gap before the next
/// flip. At the tip before CXX its `epoch_copy:record` inserts
/// `Img::At(flip-1 bytes)` into the cleared map; nothing clears it, the
/// next freeze lets it override `cur`, and image 2 maps P to its flip-1
/// leaf with key 2 lost and no XX001. Evicting P's frame between the
/// checkpoints stops the next scan rewriting the entry.
#[test]
fn exhausted_record_stays_out_of_the_next_window() {
    let seed = 3u64;
    let cfg = tcfg(8, 18, 1);
    let (disk, st) = create_store(cfg.clone(), seed, None);
    let (disk, st, roots) = {
        let sched = pass_world(1, seed, disk, st);
        let cx = Cx {
            w: &sched,
            t: HARNESS,
        };
        let mut roots = Vec::new();
        for id in 1u64..=12 {
            tree::dir_create_tree(&cx, id).unwrap_or_else(|e| panic!("CreateTree {id}: {e:?}"));
            let root = tree::dir_root_of(&cx, id).unwrap_or_else(|e| panic!("root {id}: {e:?}"));
            if id == 11 || id == 12 {
                ins(&cx, id, root, &1u64.to_be_bytes(), 1, &val(1, 1));
            }
            roots.push(root);
        }
        cx.with("rec:budget", |c| {
            c.store().parks.insert("epochcopy:prewrite", 900_000);
            Ok(())
        })
        .expect("park budget");
        let (d, s) = {
            let g = sched.lock();
            (g.disk.clone(), g.st.clone())
        };
        (d, s, roots)
    };
    let root_p = roots[10];
    let root_b = roots[11];
    let pads: Vec<u64> = roots.iter().take(10).copied().collect();
    let body = move |sched: &Sched<TestWorld>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        match t {
            1 => {
                while !(flag(sched, "flipped1") && flag(sched, "filled")) {
                    cx.yield_now("rec:wait-fill").map_err(fail_of)?;
                }
                // The gated touch: tree_insert holds the gate and tree
                // 12's mutex across the park.
                ins(&cx, 12, root_b, &2u64.to_be_bytes(), 2, &val(2, 2));
                set_flag(sched, "m_done");
                Ok(())
            }
            2 => {
                let r = ckpt::checkpoint(&cx, &mut |hook| {
                    if hook == CkptHook::After0 {
                        set_flag(sched, "flipped1");
                    }
                    if hook == CkptHook::Mid2 && !flag(sched, "filled") {
                        let cxh = Cx { w: sched, t };
                        ins(&cxh, 11, root_p, &2u64.to_be_bytes(), 2, &val(2, 2));
                        set_flag(sched, "filled");
                    }
                    Ok(())
                });
                r.map_err(fail_of)?;
                for (id, root) in pads.iter().enumerate() {
                    ins(
                        &cx,
                        id as u64 + 1,
                        *root,
                        &2u64.to_be_bytes(),
                        2,
                        &val(2, 2),
                    );
                }
                loop {
                    let gone = cx
                        .with("rec:chk", |c| {
                            Ok(!c
                                .store()
                                .pool
                                .frames
                                .iter()
                                .any(|f| f.key == root_p && !f.is_epoch_copy))
                        })
                        .map_err(fail_of)?;
                    if gone {
                        break;
                    }
                    if !prims::evict_one(&cx, false).map_err(fail_of)? {
                        cx.wait_for(e4_recovery::sim::WaitKey::Pin)
                            .map_err(fail_of)?;
                    }
                    cx.yield_now("rec:evict").map_err(fail_of)?;
                }
                // Past post-5, before the next flip: release the park.
                {
                    let mut g = sched.lock();
                    g.st.parks.insert("epochcopy:prewrite", 0);
                }
                while !flag(sched, "m_done") {
                    cx.yield_now("rec:wait-m").map_err(fail_of)?;
                }
                ckpt::checkpoint(&cx, &mut |_| Ok(())).map_err(fail_of)?;
                set_flag(sched, "ckpt2_done");
                cx.yield_now("rec:end").map_err(fail_of)?;
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let (sched, report) = run_world(3, seed, disk, st, body);
    report.assert_clean("the late-record schedule");
    assert!(flag(&sched, "ckpt2_done"), "both checkpoints ran");
    let g = sched.lock();
    let mut disk_after = g.disk.clone();
    drop(g);
    crash_clean(&mut disk_after, seed + 50);
    let sched2 = pass_world(1, seed + 100, disk_after, StoreInner::empty(cfg.clone(), 0));
    let cx2 = Cx {
        w: &sched2,
        t: HARNESS,
    };
    let (opened, meta) = ckpt::open(&cx2, cfg.clone()).expect("reopen meta 2");
    assert_eq!(meta.ckpt_id, 2, "meta 2 is the image");
    cx2.with("rec:install", |c| {
        *c.store() = opened;
        Ok(())
    })
    .expect("install");
    let want: BTreeMap<Vec<u8>, Option<Vec<u8>>> = [
        (1u64.to_be_bytes().to_vec(), Some(val(1, 1)[1..].to_vec())),
        (2u64.to_be_bytes().to_vec(), Some(val(2, 2)[1..].to_vec())),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        tree::tree_scan(&cx2, root_p, 10).expect("scan P"),
        want,
        "image 2 must hold P's flip-2 cut: a late record for the oldest copy maps its flip-1 leaf"
    );
    assert_eq!(
        tree::tree_scan(&cx2, root_b, 10).expect("scan B"),
        want,
        "image 2 must hold B's flip-2 cut"
    );
}
```

## Follow-ups

- **X-5c-F4-F1-F1-F2-F1 (test, Status: waiting on CXX):** pin the install guard's eviction arm (G-GUARD-EVICT-AT) and a gated touch's late copy in the gap (M-LATECOPY-ATONLY, G-GUARD-ABSENT-OK), and correct the freed-LPID pin's doc. It waits because the late record fails these schedules first until CXX lands.
- **X-5c-F4-F1-F1-F2-F2 (test, Status: waiting):** make `ack_prefix_two_batches_in_flight`'s driver sleep between polls and raise its 30 s deadlines, with no assertion changed. Worth running soon: it can redden any gate on a busy host.
