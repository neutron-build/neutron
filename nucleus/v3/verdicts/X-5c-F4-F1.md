Card: X-5c-F4-F1
Class: experiment
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F4-F1 P gate, 2026-11-04)
Date: 2026-11-04
Gate-exit: GATE EXIT=0
Commit: 46443a6d1fe58ce83e0470b78798c46a4db309fd
Mutants: M-BASEDRIFT -> x5_r37_base_tuning_reproduces_1b0ce54e, M-Q11UNWIRED -> q11_stale_record_at_recycled_segment_start_counts, M-Q11ANY -> q11_mid_segment_stale_does_not_count, M-W12ALL -> x5_r38_w12_crash_drops_the_rename, M-SZC-ANY (at szc_of) -> szc_a_completed_4f_interval_and_a_small_cut, M-SZC-ANY (at round_rec, the card's literal `szc := sizing_ok`) -> UNCAUGHT, M-CKPOISONb -> g8_poison_mid_checkpoint_ends_the_workload_quietly, M-CKPOISONa -> arm deleted (step 5 option 2; accepted), G-SZC-NOCK (disjunct 2 removed) -> UNCAUGHT, G-SZC-NOFOLD (ckpt_closed_dirty never set) -> UNCAUGHT, G-W11ALL -> x5_r38_w11_crash_reads_stale_at_start, G-W11ANYB -> x5_r38_w11_crash_reads_stale_at_start, G-CWIN-NODIGEST -> UNCAUGHT, G-STALE-NOLSN -> UNCAUGHT (equivalent at the only call site)
Followups: X-5c-F4-F1-F1

The card's Author line is P, and the request says this role wrote it (ruling nv3-X-5c-F4-esc-1). This gate is a separate session with no context from that ruling, and finding 3 below reverses part of it. The orchestrator requested the gate knowing this.

## How the gate was run

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the `--no-local` clone, scaffold 1ca11c89e54560535d33c220b877550716f34f6a, on the full commit, from `~/Workspace/neutron-v3`, as a background task. Lines: `fresh-gate: commit 46443a6d…`, `fresh-gate: base 1ca11c89…`, `fresh-gate: checks passed (4506 files hashed in 1.23 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. Every STEP EXIT=0. Literal last line `GATE EXIT=0`. stderr empty. `$fg` removed.
- **Stacked-card friction: none in the script.** No refusal, about 6 min, no `--base` needed. The only friction is GATE-PROMPT step 2's `git diff nucleus/v3-scaffold...HEAD`, which lists the whole X-5c stack (other cards' verdicts included) on a card this far out. Scope was read against `card/v3-X-5c-F4...HEAD`, as the request says: all changes are in `nucleus/v3/spikes/e4-recovery/`. No new non-test `unwrap`/`expect`/`panic`/`unsafe` in `src/`.
- **Mutants and probes** ran in scratch exports of the commit under `~/.cache/nv3-review/gate-X-5c-F4-F1/`, never in the worker's worktree, which stayed clean. Scratch fixes and diagnostics are `probe-variant.patch` there; they are not in this commit.

## Measurements re-run by the gate

**Full 10k at the tip** (`run --seeds 0..10000 --threads 16`; a build differing only by extra JSON diagnostic fields, digest code untouched; output `full.json`, 1,180 s on a shared host). Against F4's file (sha `bec7056c…`):
- `E4 SEEDS=10000 O1_FAIL=2365 O2_FAIL=35 O3_FAIL=72 DEADLOCK=0 PANIC=0 O1F_FAIL=2365 MONITOR=0` and `FAILING_SEEDS=94 FAILING_ROUNDS=76 TWIN_FAILING_ROUNDS=20 DEFECT_SEEDS=0`: both lines identical to F4's.
- **Exactly 4 digests moved: 4, 8422, 9226, 9606.** Each differs only in the q11 flag (6 rounds). The card's "every digest equals 1b0ce54e's except Q11 hits" holds over the whole file.
- **All 67 poison rounds' seeds are unmoved**, which settles step 5 across every schedule the campaign has.
- **`ckpt_closed_dirty` is 0 in all 15,386 rounds** (finding 1).
- By class, rounds / recovered state / `sizing_ok` / `szc`: C 11,861 / 11,845 / 378 / 58; MP 2,190 / 409 / 2 / 0; MW 1,335 / 802 / 19 / 0. Round 0's `sizing_ok` is 0 of 10,000. The worker's numbers reproduce.

**Stopped seeds.** MP 1,792: **1,771 passed with no recovered state**, 21 failed. MW 534: 533 and 1. C 54: all failed.

**Probes (0..N, labelled, never evidence).** Abort fix and diagnostics applied; rows marked `+W11` also have finding 2's fix. `szq`/`szi` are computed from the diagnostics per REFRESH 5's definitions. Wall × (10000/N) on a host with other load: BASE is 15.6 min/10k.

| Set | N | Knobs (over BASE) | Min/10k | SZC | MP | MW | SZI | Q4 | Q5a | Q7 | Q8 | Q11 | Q12 | Q18 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base | 400 | none | 15.6 | 150 | 0 | 0 | 50 | 0 | 200 | 325 | 325 | 25 | 0 | 1150 |
| k4 +W11 | 800 | chunked 700–900, value_lo 280, gc 6–8, every 8–12, drop 1/2, burst 600/4, shares .10/.10 | 39 | 1525 | **12** | 125 | 512 | 62 | 112 | 50 | 550 | 312 | 188 | 662 |
| k5 +W11 | 800 | chunked 900–1200, budget 20–31, value_lo 280, gc 8, every 8–12, drop 1/2, shares .10/.10 | 50 | 3200 | **88** | 225 | 888 | 12 | 288 | 62 | 525 | 112 | 212 | 488 |
| k1 | 400 | k5's chunked and budget, gc 12, every 4–8, burst 600/8, shares .20/.20 | 61 | 2475 | **175** | 175 | 450 | 25 | 625 | 225 | 275 | 0 | 325 | 3475 |
| k2 | 400 | k1 with every 12–19 | 82 | 3400 | **100** | 225 | 1050 | 25 | 175 | 25 | 500 | 25 | 450 | 250 |
| k3 | 400 | k1 with chunked 1200–1500, budget 30–45, every 16–24 | killed: over 54 min for 400 seeds | | | | | | | | | | | |

All counts × (10000/N). MP rests on 1–7 raw hits per row, so read it as ±40% at best.

**Window yields, per pick.** W11 as built, `w11_share` 1000, BASE, 0..200: **0 of 150 picks hit Q11**. W11 with finding 2's fix, same seeds: **45 of 79 (57%)**; 72 empty fallbacks. On k4/k5, 18/44 and 6/24. W12: 46 of 151 at BASE; 15/67 and 17/65 on k4/k5, consistent with the worker's prefix explanation.

## Findings

1. **REWORK: `szc`'s second disjunct is dead.** `checkpoint()`'s `ckpt:abort` runs on every `Err`, including a sim crash, because the unwinding task gets its turn; it sets `ckpt_closed_dirty = 0` before `run_round` snapshots `obs`. Result: 0 in 15,386 rounds, including 120 of 305 in 0..200 crashing at a mid-checkpoint hook. No pin covers the disjunct (G-SZC-NOCK and G-SZC-NOFOLD pass the whole suite), and no pin reads `RoundRec.szc` (the card's literal M-SZC-ANY survives; the worker mutated `szc_of`). REFRESH 5's `szi` needs this field and would be always 0, and its `ckpt_unflipped` ("cleared wherever `ckpt_open` is cleared") has the same bug. SZI ≥ 300 would have gone to the owner as a false unreachable.
2. **REWORK: W11 opens at the wrong sync.** `w11_window_note` fires on any completed sync of the segment below `b`, not only one whose extent reaches `b`. Traces: seed 11 opens with the walk stopping at offset 22,477 of a 65,536-byte segment; seed 10 at 59,723. The crash lands there, the walk stops on zeros below `b`, and Q11 cannot hit. Because the window then stays open, the true opening is never recorded. The worker's 1-in-14 was a small sample, and the pin's seed 276 is one of the rare hits (k1 and k2 together: 1 of 95). Q11 at share 0.20 would have been about 10 per 10k, and F4 would have escalated it as unreachable. Adding `st.wal.written >= b` fixes it (57%).
3. **P's error, not the worker's: ESCALATE 4's refutation is withdrawn.** The "Q4 132 of 309" in ruling nv3-X-5c-F4-esc-1 is q[3], **Q5-partial**. In P's saved file (`X-5c-F4-esc1-q4probe-0..200.json`) q[0] is 0 and q[3] is 132; its 309 rounds and q[19] = 49 (Q18) match the ruling's other figures. P's patch re-run here: Q4 0 of 309. The worker's burst knobs reproduce that: 600/8 gives 0 of 309, 900/16 gives 0 of 308. In-window hook rounds at 900/16: 88, of which 5 have a split and 0 a merge. Probes top out at 62 per 10k against 1,000. REFRESH 4's Q4 bullet is wrong and F4 cannot meet Q4 with it. See follow-up X-5c-F4-F1-F1.
4. **REWORK: README record.** Step 2's movement list says seed 4 only; that was the pinned subset, and the file has four. The MP root cause says "a failed r0 or a recovery without a store"; it is 1,771 of 1,792 recoveries legitimately refusing (detection), with only 21 failures.
5. **REWORK: `cwin` is unpinned as a digest input** (G-CWIN-NODIGEST). The card requires it.
6. **Minor.** `E4_W11_DIAG` eprintlns remain in `wal.rs` and `recover`; remove them in the rework. G-STALE-NOLSN is equivalent at the only call site, since the checked decode has already failed there.
7. **Pre-existing, no card.** `ack_prefix_two_batches_in_flight` has a 30 s wall-clock deadline. It failed 3 of 3 at both this tip and base 1b0ce54e while a 16-thread campaign ran, and passes in about 4 s on a quieter host. X5-R31 already forbids that co-scheduling. It ended three of my first suite runs, so those three mutants were re-run with `--no-fail-fast`.
8. **New-class notice for F4 step 4 (probes only).** Class C `O-3(b): XX001` with O-1 failures appear on retuned knobs: k4 seed 221 r1 (After0, AmcheckIi), k4 seed 370 r1 (Open), k5 seed 423 r0 (After0, AmcheckIi). Seeds 221 and 423 reproduce with shares at 0; 370 does not. These are not part of this card, and X5-R16 applies. They may be real.

Accepted as they stand: X5-R37 and its 51 literals (P's 7359 is simply unpinned at this tip); the generic property tests on FINAL; `current + 1` list entries; the Q11 walk and `is_stale_record`; no test assertion changed.

## Rulings

**(i) W11.** The worker's reading is accepted in substance: `durable == b` never occurs, because `durable` advances only after the batch's sync loop and the batch already holds `b`'s record (seed 4: durable 261147, written 263952, `b` 262144). The implementation is not accepted (finding 2). This text replaces step 3's W11 bullet and X5-R38's W11 clause:

> **W11.** Let `b` be a segment boundary whose segment a `wal_recycle` rename bound and which has had no completed `sync_data` since. W11 opens when a `sync_data` of the segment below `b` completes with `wal.written ≥ b`: that segment's whole extent, filler or boundary-ending record included, is then durable, so the walk reaches `b`. It closes when a `sync_data` of a segment at or above `b` completes. "Untouched" means the durable image: a write at `≥ b` may be issued and still volatile, and F-CRASH decides whether it survives. Q11 then requires that `b`'s first sector does not survive with new content.

**(ii) `szc`.** Not as shipped (finding 1). With the fix, it measures the owner's quota arm exactly, from the fold to the checkpoint's end. Nested recoveries and the harness checkpoint are excluded because `obs` is taken when the tasks end; a round's `dirty_now` starts clear because the harness checkpoint folded it. Poison ends count as the round's crash, and F4 reports that count as `INFO` (0 of 67 at the tip). REFRESH 5 otherwise suffices, once corrected as in (v).

**(iii) MP.** F4's retune raises it, since r0 can now reach `szq` (k5: 4 hits). The worker's "workload property" is half the cause; the larger half is that `szq` excludes refused rounds and 81% of MP rounds refuse (409 of 2,190 hold a state). Measured MP per 10k: 12 at 39 min, 88 at 50 min, 175 at 61 min, 100 at 82 min. **MP ≥ 100 is shown only at or past the 40-minute budget, on 1–7 raw hits; it is neither proven reachable nor proven unreachable inside it.** No threshold moves. **Owner item 1**, below.

**(iv) M-CKPOISONa's deletion: accepted.** The reasoning holds (`cx.with` takes no yield; `ckpt:begin` refuses before any hook; `checkpoint_poison_end` is the same end), and the full-file check is stronger than the worker's single schedule: none of the 67 poison rounds' seeds moved. M-CKPOISONb was re-run and is caught.

**(v) F4 cannot relaunch.** It waits for this card's rework gate and X-5c-F4-F1-F1. Paste the text under "F4 card changes".

## REWORK list (one commit each, same run hygiene; re-gate by P)

1. **`ckpt.rs`, the abort path:** a sim-crash unwind must not reset the observable. Keep `st.ckpt_closed_dirty = 0` only when `!matches!(e, Fail::Sim(_))`. `ckpt_open` and the rest are unchanged. Pins: `run_seed_with(41, None, &Tuning::BASE)` r2 (After1) and seed 303 r2 (After3) have `RoundRec.szc` true with the open interval below 4·F (the record shows `szc` itself; add `ckpt_closed_dirty` to the assertion through `run_round` if needed). No digest moves: `szc` is not an input. Mutants: **M-SZC-ABORTRESET** (reset on every `Err`), **M-SZC-NOCK**, and the card's M-SZC-ANY at `round_rec`, each failing a pin.
2. **`wal.rs`:** add `st.wal.written >= b` to the W11 open condition, and correct the doc comment. Re-derive `x5_r38_w11_crash_reads_stale_at_start`'s seed by lookup if 276 no longer hits. Add a pin that a W11 crash's walk reaches `b`. Mutant **M-W11-ANYSYNC** fails it. Remove `E4_W11_DIAG`.
3. **`cwin` digest pin:** two records differing only in `cwin` have different digests; both `None` leaves it unchanged. Mutant: G-CWIN-NODIGEST.
4. **README:** the (i) text verbatim; the four-seed movement list with rounds and flags (literal list stays as is: 8422, 9226 and 9606 are unpinned); the MP root cause with finding 4's numbers; the re-measured probe with W11 yield; and this verdict's finding 3 as a correction beside ESCALATE 4.

Allowed by this list beyond the card's Limits: only the above. Then fmt, clippy, `cargo test -q --release -p e4-recovery`, end of attempt.

## F4 card changes (paste as REFRESH 6, overriding older text where they differ)

**REFRESH 6 (P gate X-5c-F4-F1, REWORK, 2026-11-04).**
- **Status.** Blocked until X-5c-F4-F1's rework passes its P gate and `card/v3-X-5c-F4` is fast-forwarded to it, **and** X-5c-F4-F1-F1 is done. Then relaunch `--keep`.
- **REFRESH 5 corrections.** "F4-F1 needs no change" is withdrawn. `ckpt_unflipped`: replace "Also cleared wherever `ckpt_open` is cleared" with "cleared on `ckpt:end` and on the abort path of a non-crash failure; **never on a sim-crash unwind**, because `obs` is read after the unwind". Add mutant **M-SZI-ABORTRESET** (cleared on every abort), which the `After4` pin must fail. Report poison-end `szq` as `INFO`.
- **REFRESH 4's Q4 bullet is withdrawn.** P's "132 of 309" was Q5-partial; Q4 was 0. Use X-5c-F4-F1-F1's result. Q4's threshold is unchanged.
- **W11.** X5-R38's W11 clause is the text in this verdict's ruling (i).
- **Probes (not evidence): start here.** See this verdict's table. k5 is nearest on SIZING, MP short at 88; Q7 62 and Q18 488 there are short, and k1 shows gc 12 and every 4–8 restore them at more wall. Do not use k3's region: over 54 min per 400 seeds. Shares of 0.10 gave Q11 112–312 and Q12 188–212 with the fixed window; 0.05 is untested and may fall short on k5's knobs.
- **Unreachable, restated.** If any row, MP above all, cannot be met at ≤ 40 min with every other quota held, ESCALATE with the probe numbers and raw counts. Never lower a threshold, narrow a definition or exceed the budget unasked.
- **Step 4 notice.** Class C XX001 on retuned knobs (seeds 221, 423, 370 in this verdict) gets X5-R16's treatment if it recurs on FINAL.
- **Pin.** Add `x5_r37_final_stays_in_card_ranges`: FINAL stays inside 5.1 and 5.2's ranges, since the existing knob-range tests derive under BASE.

## Owner items

1. **MP ≥ 100, the refused-round exclusion and the 40-minute budget may not all fit.** Measured: 88 at 50 min, 12 at 39 min, 175 at 61 min. Nothing is changed. F4 tries first and escalates with numbers if it falls short. The thresholds are the owner's, and X5-R17 calls the budget a time budget, not a correctness rule. The owner needs to choose only if F4 escalates.
2. **Q4 ≥ 1,000: the evidence given before was wrong.** The ruling that told the worker Q4 was reachable misread a column. Measured, with REFRESH 4's knobs: at most 62 per 10k. X-5c-F4-F1-F1 tries a pre-registered alignment first. If it cannot reach 1,000, this returns to the owner under the standing rule, and nothing is lowered.

## Follow-ups

**X-5c-F4-F1-F1** (Status: waiting; `cards/X-5c-F4-F1-F1.local.md`). Q4's reachability: separate per-round split and merge counts in the record, a pre-registered X5-R40 script alignment that runs burst GC inside an open checkpoint, three settings over 0..200 at most, and a fixed decision rule, with an unmet rule going to the owner.
