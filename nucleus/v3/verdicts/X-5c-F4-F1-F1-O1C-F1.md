Card: X-5c-F4-F1-F1-O1C-F1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-X-5c-F4-F1-F1-O1C-F1-gate-1 (Claude Code session 30a6272d-fa00-4c84-b753-3cff3d14b4f4)
Date: 2026-11-06
Gate-exit: GATE EXIT=0
Commit: 2ec189e682b847c333d6bf74799a70d4412dc219
Mutants: M-R47-KEY-COVER -> r47_new_life_record_behind_old_write (also r47_f1_on_recycled_file, r47_three_rounds), M-R47-FINAL-IMAGE -> r47_earlier_life_record_stays_written, M-R47-NO-TS -> r47_wrong_record_at_position_stays_unwritten, G1-HEADER-WINDOW -> UNCAUGHT, G2-NO-MARKER -> x5_r37_base_tuning_reproduces_1b0ce54e (also cxx_highest_lpid_free_never_shrinks_next_lpid_rounds), G3-GUARD-UNWIRED -> UNCAUGHT
Followups: X-5c-F4-F1-F1-O1C-F1-F1

X5-R47 is implemented as ruled, and the accounting fix hides no loss over the 10k: exactly the three named rounds go fail to pass, and nothing goes pass to fail. The worker's +45% CPU is host state, not this code: the replay is cheaper than the walk it replaced. Two mutants are uncaught. One is a pin gap with no live shape (finding 2). The other is a guard with nothing to trip it (finding 4).

## Gate run
Scaffold ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0, through the `--no-local` clone. Two runs of the scaffold's `fresh-gate.sh` at 2ec189e6, both with `checks passed` (4510 files).

- **Run 1: `GATE EXIT=1`.** `STEP test EXIT=101`, on `ack_prefix_two_batches_in_flight` only: `driver timeout: B never completed batch 2`, at its 30 s wall-clock deadline. Every other step was 0. This is the known load flake of card X-5c-F4-F1-F1-F2-F2. The host was at load 30+, from another session's 16-thread job plus my own builds, probes and the first 10k. This card does not touch `tests/store.rs`.
- **Run 2, with no campaign of mine running: every STEP EXIT=0, `GATE EXIT=0`, stderr empty.** This is the run the header records. `runner e2e: K` is green with no wrapper, because this host's init reaps.

Scope: `e3d1bfc3..2ec189e6` touches README.md, src/harness.rs, src/store/ckpt.rs and tests/harness.rs of the crate only. The ckpt.rs lines are the cherry-picked O1C diagnostic. `b3e10e53..2ec189e6` does not touch the store.

## The 10k comparison (verdict a4cabc44's check, at this tip)
Three full `run --seeds 0..10000 --threads 16` at BASE, on scratch exports. No seed of `10000..30000` ran.

- **pre:** b3e10e53, the old derivation.
- **post:** the card's tip, unmodified.
- **shadow:** the tip with the old derivation restored beside the new one, computing both per round and judging with the new. All 10,000 of its digests equal post's.

Every seed was judged under both, which is a superset of the seeds whose walk differs.

| Rounds, pre vs post | Result |
|---|---|
| fail → pass | **exactly 1066, 6555 and 7590 r0** |
| pass → fail | **0** |
| `(lo, hi, c, frontier)` of the three | (20,20,—,21) → (20,21,21,21); (22,23,—,24) → (22,24,24,24); (19,20,—,21) → (19,21,21,21) |
| `lo`, `frontier`, `ackl`, `ackls`, `o2`, `o3`, every other field | 0 changes |
| `hi`, `c`, `o1`, `o1f`, `pass` | changed on those three rounds only |
| 6555 | continues to r1 and r2, both passing ((38,38,38,38), (46,46,46,46)) |
| digests moved | 3, those seeds; 8852 and every X5-R37 literal unmoved |
| failing rounds | 24 → 21: the MP class only, O1C gone |
| twin failures; defect seeds | 6 → 6; 1 → 1 (7469, CXX-F1's) |
| O3 failures, deadlocks, panics | 21 → 21; 0; 0 |

| Derivation, shadow | Count |
|---|---|
| derivations | 19,138 |
| commit written only by the old walk (`hi` too high) | **0** |
| rounds where the replay finds records the walk missed | 1,039 (789 seeds) |
| acked only under the old | 0 |
| acked only under the new | 312 rounds, SyncOff only |
| SyncOn acked differences | **0** |
| `unparsed_written` on either side | 0 |
| max written moved | 9 lines, 8 rounds |

Of those 8, five (300 r1, 4343, 5441 r1, 8239, 8485) leave the judged `hi` unmoved, because `lo` already stands at the new value. 8239 is a faulted MW round (`mis` 1, `torn` 1) and is identical in both arms.

Why this cannot hide a loss, on data and code: `lo` is unchanged in all 10k. It reads SyncOn `acked`, and those are identical in all 19,138 derivations. The replay only adds written commits, so the only permissive change it can make is a higher `hi`, and that changed a verdict in the three rounds alone. Each of those passes at `c = frontier` with O-1, O-2 and O-3 clean.

8852 does not move at this tip: (8,8,8,8) in both arms, digest 564931007. The worker's report is right. CXX moved its schedule, and the card's expected movement no longer exists. Accepted.

## Cost and the final-run budget
**The +45% does not reproduce. The replay costs less than the walk it replaced.**

| Measure | pre / old | post / new |
|---|---|---|
| derivation time inside the run, 0..10000 (shadow's per-call timers) | 96.7 s | **63.9 s** |
| 0..200, alternating, CPU s (user + sys) | 174, 251, 250 | 243, 253, 252 |
| 0..10000, CPU s | 13,171 | 13,831 (shadow, computing both: 13,073) |
| 0..10000, wall | 22.8 min | 22.1 min (shadow 20.0) |

- **Where the +45% came from.** The worker's 157 s resembles my first pre probe of the alternating series, 174 s, taken at a quiet moment. The next pre, the same binary, read 251 s. Contention inflates CPU seconds on this host. Whole-run CPU spreads about ±5% between arms, in no consistent direction.
- **Direct bound.** The derivation is 0.5% of a run's CPU either way, and the change is −33 s CPU over 10k, about −2 s of wall at 16 threads. The pairing guard is one linear scan.
- **Scaling.** Writes × commits sums to 1.3 × 10⁸ comparisons over the 10k, under a second of CPU. Even at three times BASE's work per seed, the derivation is about 190 s of CPU, about 12 s of wall, against a 40-minute budget.
- **Budget effect: none (under 15 s at any plausible FINAL).** The anchor stays 17.1 min/10k × F3's +25–38% = 21–24 min, as before this card, leaving F4's other knobs their 16–19 min. Today's 20–23 min readings here are at load 15–48, so they are not an anchor.
- **Cheaper form.** An index by `pos` over the commits fits X5-R47's text, but it would save a fraction of 0.5%. Not needed, and not required.

## Findings
1. **Correctness: holds.** Overlap window, image application, monotone sets, F1 gated on not-written, SyncOff acked iff written, SyncOn untouched, `returned_end` gone, guard through `task_error`, no non-test `unwrap`/`expect`/`panic`, diagnostics stderr-only and off by default. X5-R47 is carried verbatim in the README. The trace now calls the derivation, so it cannot drift again.
2. **G1-HEADER-WINDOW is uncaught (minor; -F1).** Testing only writes that overlap the first 24 bytes misses a record completed by a later write. It passes the whole crate and moves no digest over 0..200, because the WAL writer splits a batch only at segment boundaries (`wal.rs:592`) and no record straddles one. The code is right and the shape is not live, but a future writer change would silently under-count `written`. Hence one pin.
3. **The README's cost sentence is wrong (minor; -F1).** "The replay costs about +45% CPU … the price of the ruling's per-write record tests" is labelled a probe, but it attributes host state to the ruling, and F4's worker reads this README for its budget.
4. **G3-GUARD-UNWIRED is uncaught (accepted).** `SimDisk::write_at` cannot produce an unpaired write, so no real stream can reach the wiring at `harness.rs:2667`. It would need a sim fault hook the card forbids. The function has its own pin, and the wiring is confirmed by reading. It never fired over the 10k: `terr` is unchanged, and post's digests equal shadow's.
5. **Worker's judgment calls: accepted.** The seventh pin; 8852 as measured; the trace's inode source; the subreaper wrapper. The wrapper changes no step, and K is green here without it. K's false red in nested sessions is already card X-5c-F4-F1-F1-F1, so no new card.
6. **G2-NO-MARKER is caught** by the digest pin, so `CommitMarker` is live in real streams.

Artifacts: `~/.cache/nv3-review/X-5c-F4-F1-F1-O1C-F1/gate/` (`mkshadow.py`, `mutants.py`, `cmp.py`, `agg.py`, `mutlog/`, `out/{pre,post,shadow}10k.*`; json sha256 prefixes pre 59a1abb7676f05d2, post e6f44888fb205e6f). Mutants ran on a scratch export; the worktree stayed clean.

## Place in the chain
O1C-F1 → CXX-F1 → F3 → F4. The orchestrator's edits, verbatim:

`X-5c-F4-F1-F1-CXX-F1`: `Base: card/v3-X-5c-F4-F1-F1-O1C-F1        Depends: X-5c-F4-F1-F1-CXX, X-5c-F4-F1-F1-O1C-F1        Gate: P`

`X-5c-F4-F1-F1-F3`: `Base: card/v3-X-5c-F4-F1-F1-CXX-F1        Depends: X-5c-F4-F1-F1-CXX-F1, X-5c-F4-F1-F1-O1C-F1 (the owner answered the Q4 question on 2026-11-05: option A; PLAN §10 item 11)        Gate: P`

This supersedes a4cabc44's F3 line, which predates CXX-F1. CXX-F1's expected movement (7469 only) is unaffected: 7469 did not move here. In F4's REFRESH, a class C O-1 with no XX001 after this merge is new and escalates under REFRESH 8.

## Follow-ups
- **X-5c-F4-F1-F1-O1C-F1-F1** (`cards/X-5c-F4-F1-F1-O1C-F1-F1.local.md`, Status: waiting, Class test, Gate S, runner + GLM): the pin `r47_record_completed_by_a_later_write`, which G1-HEADER-WINDOW must fail, and the README's cost sentence corrected to this verdict's numbers. It moves no digest, is non-blocking, and can stack anywhere after this card and before F4's relaunch.
