Card: X-5c-F4-F1-F1-O1C
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-X-5c-F4-F1-F1-O1C-gate-1 (Claude Code session 64c49d33-c18d-460a-a2a1-1243805f9c18)
Date: 2026-11-05
Gate-exit: GATE EXIT=0
Commit: 82062a02bf0fb888ef056968e3eaf2c1add5ca8a
Mutants: M-O1C-FIRE-AT-HI (trace fires at frontier == hi) -> UNCAUGHT by x5_r37 pins, M-O1C-WALK-NO-MARKER (recover's trace line drops markers) -> UNCAUGHT by x5_r37 pins
Followups: X-5c-F4-F1-F1-O1C-F1

The worker's conclusion holds: this is a harness accounting defect, with no store fault in the three rounds. One part of its proposed fix is wrong (finding 3). Over the 10k, no verdict moves in the permissive direction except those three rounds, and each is justified by the record and by the oracle at the higher cut.

## Gate run
Scaffold ed321a0b9b96f2ebfc0661e1dd2df93c92a106b0, through the `--no-local` clone. `fresh-gate: commit 82062a02…`, `checks passed (4509 files hashed in 1.26 s)`, every STEP EXIT=0, last line `GATE EXIT=0`. The extract directory was removed while the run was in progress, after its steps had started; the script was already open and the run completed. Scope: base..tip touches README.md, src/harness.rs and src/store/ckpt.rs of the crate only.

## Findings
1. **Disproof attempt on the raw events, all three seeds: failed, so the finding stands.** A scratch replay, independent of `stitched`, applies each `WriteIssued` in order to a per-inode image and calls `dec_record` at `lsn mod WAL_SEG`. 1066: ts 21 decodes with its own lsn 199061 in inode 2 at event 1806 of 1819. Inode 2 was renamed to segment `…30000` at E[1685], whose first lsn is 196608, and 199061 − 196608 = 2453, the record's position. `D` = 201654 = its end, covered by the sync at E[1812..1813]. Meta `wal_start_lsn` 179837 lies above the recycled segments. 6555 and 7590 show the same shape. **With corrected flags, each of the three passes at `c = frontier` (21, 24, 21) with O-1, O-2 and O-3 clean**, so the replayed content equals the model at that cut. This was the check that could have exposed a wrong replay. 6555 continues through r1 and r2, both passing. A correct store must replay these records. Digests match the worker's: 1383263257, 3687676066, 3340396508.

2. **Other routes: measured over 0..10000 (19,118 derivations, twin rounds included; base and corrected at the same tip).**

   | Measure | Count |
   |---|---|
   | commit written only by the current walk (`hi` too high) | **0** |
   | rounds where the replay finds records the walk missed | 1,051 (801 seeds), all with a rename in `E` |
   | in-round max written moved | 12 lines (10 seeds) |
   | judged `hi` moved | 4 (1066, 6555, 7590, 8852) |
   | `unparsed_written` on either side | 0 |

   All 801 seeds were re-judged with the corrected flags (the other 9,199 have an identical derivation and are deterministic):

   | Round comparison | Result |
   |---|---|
   | fail → pass | **exactly 1066, 6555 and 7590 r0** |
   | pass → fail | 0 |
   | `lo`, `frontier`, `ackl`, `ackls`, `o2`, `o3` | 0 changes |
   | twin rounds (264) | identical |
   | the other failing rounds of the 801 seeds | unchanged |
   | digests moved | 4; **no X5-R37 literal among them** |

   8852 r0 moves `hi` 32 → 33 and stays passing: ts 33 is a SyncOn record issued 2 events before the crash and not durable (`D` is below its end), and the frontier stays 32. That is correct behaviour.

   Why the current defect cannot hide a loss, on the code as well as the data:
   - `lower_bound` reads only SyncOn `acked`, and that comes from `D`, not the walk. MW's `h` and `a` do not read the walk either. So `lo` cannot be wrong through this route.
   - The walk's `written=true` requires a decodable record built from issued bytes, and issue and return are one step in `write_at`, so `hi` too high is unreachable.
   - The defect only lowers `hi`, which only removes candidates. That is stricter, never permissive.
   - The fix raises `hi` only to records truly issued, so the only permissive change it can make is the one measured here.

   The worker's sub-claim that the same shadowing can silence F1 is true by code reading, and silently so, but the count is 0 under the replay at this tip. X5-R47 closes it anyway.

3. **The worker understates the scope, and two of its suggested fixes are wrong.** The missing commit is not only the round's last: earlier-life records (ts 1, 2, 3, 4, and mid-round ts 16) are missed too, in 1,051 rounds. Only the top one moves `hi`. A final-image reading (the β view, or "latest issued wins" over the whole history) repairs the three rounds, but it loses earlier-life records whose bytes were overwritten: **1,028 of 19,118 rounds**, with SyncOff `acked` following them. `hi` was not wrong under it in this window, but `written` would be false for commits that were written. Hence X5-R47's replay form, which tests each record as of each write and is monotone. `derive_written_acked`'s doc comment, "the walk sees exactly the latest life", is false and goes too.

4. **Diagnostics.** Default off, stderr only; digests with and without the env var match across the worker's traces and this gate's runs. No test pins stderr, so both of my mutants are UNCAUGHT. This is acceptable for a diagnosis card, and F1 deletes the mirror walk.

5. **Limits of the evidence.** BASE tuning at the -F2 tip. CXX and X5-R41 change the rounds, and X5-R41 opens more checkpoint windows, so O1C-F1's gate repeats the comparison at its own tip. A `mutants --seeds 0..40` sweep was byte-identical in both arms, but it reads a uniform rate (0.25, first seed 7) for the mutants inspected, so it is **not** counted as evidence of detection power.

Artifacts: `~/.cache/nv3-review/gate-O1C/` (`harness.r47.rs`, `out/shadow10k.{json,err}`, `out/use/`, `scan.py`, `cmp.py`).

## Ruling X5-R47
The text is in `cards/X-5c-F4-F1-F1-O1C-F1.local.md`, verbatim, fixed before that card's code: a commit is written iff, immediately after some `WriteIssued` is applied in `E`'s order to a per-inode image, an image holds at its position a record `dec_record` accepts with its own lsn and ts; monotone; no key resolver; `returned_end` removed, with a guard that issue and return stay paired; F1 by the same replay; SyncOn acked untouched.

## Place in the chain
-F2 → CXX → **O1C-F1** → F3 → F4. **It must land before F3.** F3's clean arm of `seeded-faults`, its exact-value pins and its "failing rounds by class" would otherwise carry false class C O-1(c) rounds, and X5-R41's longer windows give more recycling, each tripping REFRESH 8's escalation. F4's final run cannot pass without it. This card's own branch is not fast-forwarded into the chain: O1C-F1 cherry-picks its two commits onto CXX. F2-F1 and F2-F2 are unaffected.

Orchestrator edit to F3, verbatim: `Base: card/v3-X-5c-F4-F1-F1-O1C-F1        Depends: X-5c-F4-F1-F1-O1C-F1 (the owner answered the Q4 question on 2026-11-05: option A; PLAN §10 item 11)        Gate: P`

In F4's REFRESH, 1066, 6555 and 7590 r0 are no longer a class C O-1 with no XX001. Any such round after O1C-F1 is new and escalates under REFRESH 8 as before.

## Follow-ups
- **X-5c-F4-F1-F1-O1C-F1** (card written; Status waiting; runner + GLM; Gate P because it changes an oracle input): implement X5-R47, its pins and five mutants. Any verdict change beyond the three rounds is an ESCALATE.
