Card: A-3c
Class: impl
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5 (medium)
Gate-session: claude-code session 3707e387-8f71-43d2-a707-7b73d7291322 (T3 Code review sub-agent, the confirming P gate after REWORK, tier R, requested by T3 thread 45584af2-e651-490e-862c-3402320cffa3; an independent gate: this agent wrote neither the card, the code, the rework (7c71b66b, d6565caf, b6376c4f, 2bfb7255) nor the first verdict (380b0b3e), and is not the orchestrator)
Date: 2026-11-28
Gate-exit: GATE EXIT=0
Commit: 2bfb72550c38d12e1154303e782e5a187b22bc99
Mutants: re-gate scope of verdict 380b0b3e, each on two runs on a fresh extract of 2bfb7255: 1 LOAD-REF-BEFORE-PUBLISH -> t_pin_load_race_a (second-event assertion, wb_race.rs:205) | 37 RELOAD-BEFORE-FLIP -> t_pin_load_race_k_reload 1000/1000 (999 Violation, 1 StepLimit) | SW-G06 -> t_pin_load_race_k_reload 1000/1000 (Deadlock) | SW-S14 survives t_pin_load_race_k_reload 0/1000 (as R2 requires: its earlier catch was R2's false failure) | OWN-1 GATE-RELEASED-BEFORE-ENTRY -> evict_holds_gate (wb_race.rs:743, at EvictEntrySet) | SW-E13 -> mapleaf_pinned_under_latch (wb_unit.rs:1632) | control (no edit): all four tests pass, k_reload 0/1000 | flake: t_pin_load_race_a 0/400, wb_race whole 0/300 under load 16.9-45.0 | every other row of 380b0b3e carries over (src/ byte-identical to a53cb1de)
Followups: A-3c-F1, A-3c-F2, A-3c-F3, A-3c-F4, A-3c-F5, A-3c-F6

## Verdict in brief

**MERGE-WITH-FOLLOWUPS** on `2bfb72550c38d12e1154303e782e5a187b22bc99`, **after spec card S-NU (A-3c-F1), or in the same pass with S-NU first.**

- R1 and R2 of the REWORK verdict (380b0b3e) are fixed as their "Required" text and the card's Amendment 2 state. The fixes use no sleep, no timed wait and no retry loop.
- The rework also folds in two A-3c-F6 cases, OWN-1 and SW-E13. Each now kills its mutant on two runs.
- `src/` is byte-identical to `a53cb1de`, so every other finding, ruling (a) to (e), mutant row and NIT of verdict 380b0b3e carries over unchanged, as that verdict's last section provides.
- The fresh gate is green on its first run.

## Gate run

The scaffold's `fresh-gate.sh` and `git-env.sh` were extracted per GATE-PROMPT step 1 through the shallow `--no-local` clone. The printed scaffold id was `8a2df81d1bd7c8e65192cf0d463044c006e6bfcb`, and there was no `EXTRACT FAILED`. The gate ran once with `systemd-run --user --scope --quiet -p CPUWeight=10000 --`, which worked, and `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-A-3c`, from 00:47 to about 01:10. Verbatim:

```
fresh-gate: commit 2bfb72550c38d12e1154303e782e5a187b22bc99
fresh-gate: base 8a2df81d1bd7c8e65192cf0d463044c006e6bfcb
fresh-gate: checks passed (4962 files hashed in 1.53 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests EXIT=0
STEP api-surface EXIT=0
GATE EXIT=0
```

- stderr was empty.
- No known load or zombie row (FAST-TRACK §3) failed, so no re-run was needed.
- The extract was removed afterwards.

The scratch, logs and tools are in `~/.cache/nv3-review/gate2-A-3c/`:
- `notes.md`;
- `fg1.out` and `fg1.err`;
- `results-g2.jsonl` and `logs/`;
- `flake/`;
- `tools/`: `run.py` is the first gate's runner with its paths moved, and `spec_g2.py` loads the first gate's own mutant edits from `gate-A-3c/tools`.

## The re-gate list (verdict 380b0b3e, last section)

1. **Fresh gate:** `GATE EXIT=0`, above.
2. **Scope:**
   - `git diff a53cb1de 2bfb7255 -- nucleus/v3/crates/nucleus-store/src` is empty.
   - `diff -r` of the two extracted `src/` trees is clean.
   - `git diff --stat 380b0b3e 2bfb7255` touches only `tests/wb_harness.rs` (+15 −4), `tests/wb_race.rs` (+101 −61) and `tests/wb_unit.rs` (+70). All three are in the card's Touch only list.
3. **Row 1 (LOAD-REF-BEFORE-PUBLISH):** caught on two runs at `wb_race.rs:205`, the second-event assertion. The message is `Some(Done("T2 unpinned"))`: T2's report came before any `LoadWaiting`, as R1 predicted.
4. **Flake, under load.**
   - Binaries: `wb_race` and `wb_harness` built from a pristine `git archive` extract of the tip.
   - Load source: six loops of that `wb_harness` binary, the implementer's method, since the host had gone quiet.
   - Runs: from 00:49:29, when the 1-minute load was 16.92.
   - `--exact t_pin_load_race_a`: **0 failures in 400 runs**, at a 1-minute load of 16.92 to 33.01 (mean 26.13).
   - The whole `wb_race` binary: **0 failures in 300 runs**, at 33.01 to 44.99 (mean 41.53).
5. **Census on `t_pin_load_race_k_reload`** (PCT seed `0xA3C0_000C`, 1,000 executions, two runs each):
   - row 37: 1000/1000 failing (999 Violation, 1 StepLimit) on both runs;
   - SW-S14: 0/1000 on both runs, so it survives;
   - SW-G06: 1000/1000 (Deadlock) on both runs;
   - the control: 0/1000 on both runs.
6. **The folded A-3c-F6 cases:**
   - OWN-1 is caught on two runs at `wb_race.rs:743`, in `evict_holds_gate`'s second check ("EvictEntrySet: the flip registered before it could end"). The flip ends rather than registers, as the first verdict predicted.
   - SW-E13 is caught on two runs at `wb_unit.rs:1632` ("try_victim at E6's pin: the latch is still held"; the hook recorded `Some(FrameId(..))`). This also shows the hook sits after the latch's release under the mutant.
7. **The test diff against R1, R2 and Amendment 2:**
   - **R1.** After `drop(p)`, T2 sends `Done("T2 unpinned")` through `Pauser::done` and blocks on an `mpsc` receive. The controller works in this order:
     1. it takes the first of `Seen(LoadWaiting)` and that report as its second event;
     2. it releases T1 and joins it;
     3. then it sends T2's message, and only after that receive does T2 call `evict_out`.

     So T1's pin is dropped before the eviction, ordered by events threads send. This is the form R1 offers and Amendment 2's text for (a).
   - **R2.** `race_k`'s reload branch skips T2's `Arrived(WaitWoken, L)` once, whether it comes before T1's first `Restarting` (the `woken` flag) or as the next event after it. Only then does it take T1's next event, which is R2's required fix and Amendment 2's text for 14.4.
   - **OWN-1.** `evict_holds_gate` now runs its check twice, each on a fresh fixture: once paused at `EvictWrite`, once at `EvictEntrySet`. This is the fix A-3c-F6 named.
   - **SW-E13.** `mapleaf_pinned_under_latch` uses a recording hook at the first `TableLocking` after MapLeaf 0's `EvictWrite`, where `try_victim(f_M)` must be `None`. This is A-3c-F6's fix. The hook disarms itself before its own `try_victim`, and the test asserts that exactly one record was made.
   - **No sleep, timed wait or retry loop.** A grep of the three files for `sleep`, `Duration`, `timeout` and new loops finds none. The only waits added are the channel receive (R1) and the existing event reads.

## Carried forward by reference (verdict 380b0b3e)

- The gate run of record at `a53cb1de`, scope, Acceptance, and the correctness review of `src/`.
- Rulings (a) to (e):
  - (a) ESCALATE 3 and the S1 sentence;
  - (b) the yield gap left to A-4c;
  - (c) `t_pin_load_race_k`'s tip form, now in Amendment 2;
  - (d) rows 15 and 34 and their two survivors;
  - (e) Amendment 1's unapplied items and placements.
- The full mutation evidence: card rows 1 to 38, the 139-mutant sweep, OWN-2, OWN-3, the LPID reverts, the probes, and A-3b-F1-F1 and A-3b-0-F1.
- Every NIT.

## Follow-ups

A-3c-F1 to A-3c-F6 stand exactly as described in verdict 380b0b3e's "Follow-ups" section, with one narrowing and one order:

- **A-3c-F1 (spec, before the merge).** Spec card S-NU carries S1's clause for A-3c. **A-3c merges only after S-NU merges, or in the same pass with S-NU first.**
- **A-3c-F2** (for A-3c-C), **A-3c-F3** (for A-4c), **A-3c-F4** (for A-3c-B) and **A-3c-F5** (for A-3c-B) are unchanged.
- **A-3c-F6** (tests): OWN-1 and SW-E13 are closed by this rework (items 6 and 7 above). The rest of A-3c-F6 stays open for the next card that touches `tests/wb_race.rs`, `tests/wb_unit.rs` or `tests/wb_harness.rs`, A-4a at the latest:
  - SW-T02, SW-L02, SW-L07, SW-B02, SW-C06, SW-C12, SW-C17, SW-W07;
  - OWN-3.

## NITs

- NIT: R1's report goes through `Pauser::done`, so it reads as `Done("T2 unpinned")` beside T2's own `Done("T2")`. The doc on `T2_UNPINNED` says why. It is clear enough, though a distinct event kind would read better.
