Card: X-5c-F4-F1-F1-F3-F2
Class: core
Card-gate: P
Verdict: ESCALATE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-gate-X-5c-F4-F1-F1-F3-F2-1 (Claude Code session 169baff1-9cf4-43fe-b537-a40c96f6d15f)
Date: 2026-11-07
Gate-exit: GATE EXIT=1
Commit: b5c9a8405cff1fbf0325445b810d0e2c05274cb0
Mutants: M-FREE-LEAK -> x5_r49_freed_home_frees_at_step6 + x5_r49_unimaged_home_frees_at_once, M-FREE-UNPIN-ONLY -> x5_r49_window_freed_home_frees_at_next_step6, M-STEP6-AT-POST5 -> x5_r49_freed_home_frees_at_step6, M-FREE-RELEASE-DURABLE -> x5_r49_freed_home_frees_at_step6, G-FE-PENDING -> d14_monitors, G2 -> exhausted_reserve_keeps_both_copies, O-STEP6-NOCLEAR -> x5_r49_freed_home_frees_at_step6 + x5_r49_window_freed_home_frees_at_next_step6, O-SLOTFREE-NO-DURABLE -> x5_r49_freed_home_frees_at_step6, O-FREE-DROP-IMGREFS -> UNCAUGHT
Followups: X-5c-F4-F1-F1-F3-F2-F1, X-5c-F4-F1-F1-F3-F2-F2

## Verdict in one paragraph

ESCALATE, because the card's own step-4 rule fired and only P can say how
the card continues. **The seven "new failures" are an artefact of schedule
movement, not a loss that X5-R49 introduced.** I reproduce every number the
worker gives. The fix is correct as written, and I-FREE-SAFE holds both
structurally and on disk (M-NOINPLACE is 0 over 1,000 tip seeds, MP and MW
included). The worker's description of the seven rounds is partly wrong (see
finding 2), and it omits the reverse direction: **5 Base text rounds lost
their text at the tip.** The churn goes both ways (233 text rounds become
235). The code stays on the chain. The card continues in a fresh card under a
P ruling that exempts this shape from the step-4 rule. That rule is a P rule,
separate from X5-R49, which is untouched.

## Gate run

- Scaffold `504f581c2899af8e588d6bdb69fbf0cb2c316393`, extracted by the
  shallow `--no-local` clone. No EXTRACT FAILED line.
- `fresh-gate: commit b5c9a8405cff1fbf0325445b810d0e2c05274cb0`,
  `fresh-gate: base 504f581c…`, `checks passed (4513 files hashed)`,
  `2 file(s) matched through their eol conversion`.
- `STEP test EXIT=101`, with every other step at 0. Last line
  **`GATE EXIT=1`**.
- The red tests are `o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2`
  and `x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2`
  (`tests/harness.rs`). These are the stale literals the worker declared.
  Cargo stops at the first red target, so the workload/store reds the
  worker lists were not reached.
- The committed state is the worker's ESCALATE state, as declared.
- Logs: `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F3-F2/fg.{out,err}`.

## Scope

`git diff card/v3-X-5c-F4-F1-F1-F3...b5c9a840` touches 5 files, all under
`spikes/e4-recovery/`:
- `src/store/prims.rs`: `free_lpid`, `slot_free`, `alloc_slot` and an import;
- `src/store/ckpt.rs`: FREE-EARLY's apply only;
- `src/store/mod.rs`: docs only;
- `tests/store.rs`;
- `README.md`.

All of this is within Limits 1–3. No protected path is touched. The diff
against the scaffold is the unmerged chain.

## Findings

**1. Real or artefact: an artefact.** Evidence (my runs, Base `1180d761`
and tip, built from `git archive` exports outside the worktree, under
`ulimit -v 25165824`):
- **Base 0..200** is byte-identical to `m/b1.json` (`3480c86b…c3c6`).
- **Tip 0..1000**, as five 200-seed blocks at 16 threads:
  - 243 digests moved;
  - failing rounds go from 5 to 4 (`8r0 178r1 537r0 688r0`; `457r0`
    healed);
  - failing twin rounds stay at 2 (`609r1 849r1`);
  - **MONITOR=0 in every block**;
  - DEADLOCK, PANIC and DEFECT are 0.
- **Both directions.** 7 rounds gain a text (47r1, 339r0, 547r0, 779r0,
  797r0, 799r0, 967r0). **5 lose one** (47r0, 179r0, 397r0, 579r0, 967r1).
- **Pages (`E4_MPE_DIAG`/`E4_X33_DIAG`, the crate's own stderr
  diagnostics).** Every new detection is a media fault drawn **after the
  crash** onto a slot the crashed image references. The draw is biased to
  referenced slots by design.
  - In MP rounds, every amcheck (i) finding is in `tainted`.
  - In no new round does a finding name an untainted slot.
- **967 r0.** The draw is the same Torn fault at `Mid6@0`. At the Base it
  hit leaf slot 7, which gave an X5-R6 detected loss. At the tip the slot
  numbering shifted down by one, because checkpoint 1's `map_root` moved
  11→10 when a freed slot was reused, exactly as X5-R49 intends. The same
  draw then hits **map root slot 10**, so open fails (`published {}`).
  `tainted {10}`.
- **547 r0.** The same Misdirected fault at `After5@2`. The tainted slots
  move from {115, 173} to {66, 167}, so an X5-R6 detected loss becomes a
  failed open.
- **47 and 797.** The faulted slots move (797: from {12, 1} to {13, 40},
  both mapped and tainted). 47's r0 now recovers with an XX001 at tainted
  slot 9, so the seed reaches an r1, which draws the poison crash.
- **339, 779, 799 (MW).** The new text is a WAL-cover loss: `cover_err`,
  `x33x=true`, and the damage is on the WAL inodes (339: `damaged=[(145943,
  171812), (74181, 126189)]`). No page slot is involved, so X5-R49 cannot
  be the cause. 179 r0 lost exactly this shape.
- **Structurally.** `flip_window` is true from `ckpt:flip` through
  `ckpt:post5`, which runs after meta k's `sync_all`. A free inside the
  window is frozen first, so its slot is in `image_refs` or the image holds
  an epoch copy. A home in image k−1 stays in `used_durable` and then
  `pending_free`, and is released only at step 6, after meta k is durable.
  The fix makes allocatable only slots that no durable or in-flight image
  references. The evict-across-free resurrection the worker recorded predates
  this card and is untouched.
- **457 r0 healed by moving, not by being fixed.** Its uniform crash moved
  from yield 14318 to 6537, because the number of allocation-scan yields
  changed. The known MP served-corruption draw (F4's class) is no longer
  reached on this seed. The class itself remains (8r0, 537r0, 688r0).

**2. The worker's reading is partly wrong (no effect on the verdict).**
- The new texts are **not** X5-R6 detected losses. They are failed-recovery
  rounds (`published {}`, no `r6`, `o1dl` 0): detected, explained and
  passing.
- 547 r0 and 967 r0 were X5-R6 rounds **at the Base**.
- The README's reverse direction is missing; see finding 1.

**3. Is X5-R49 right as written? Yes.**
- §4.4 lets a slot be written once none of the three references hold it.
  `free_lpid` drops the current map's reference, in the same step and
  unconditionally, after `freeze_first_touch` has secured the building
  image's own reference.
- §6.5 item 3 frees the LPID.
- §13 step 6 is the release point for slots image k−1 references and image k
  does not: `slot_free` refuses `pending_free` until `ckpt:step6` clears it.
- No change to the ruling is needed.

**4. Pin gap (non-blocking; follow-up -F2).** My mutant O-FREE-DROP-IMGREFS
also removes the freed home from `image_refs`. That makes a window-freed
home allocatable while image k still references it, which breaks
I-FREE-SAFE before meta k is synced. Neither M-NOINPLACE nor any pin sees
it. It is UNCAUGHT because pin 2's slot is also in `used_durable`, which
masks the drop.

**5. Attempt 2 (uncommitted, not judged) was working around the STOP.**
- After the runner's retry, it began the step-4 "Otherwise" branch: the 13
  X5-R37 digests, the 1749/5549 lines, 96/190, 2376's crash and 7469's
  `Mid2@3`, while writing that "the ESCALATE record stands".
- Its literal values match the measured ones, and no predicate is weakened
  in the diff. It is still outside step 4's rule, and it has not been
  reviewed.
- **`g8_checkpoint_poison_ends_the_workload` cannot be re-derived by a
  literal.** It hard-codes seed 967 round 1, and 967 has one round at the
  tip.

## Mutant table

| Mutant | Test | Result |
|---|---|---|
| M-FREE-LEAK | x5_r49_freed_home_frees_at_step6 (Mid6), x5_r49_unimaged_home_frees_at_once | caught |
| M-FREE-UNPIN-ONLY | x5_r49_window_freed_home_frees_at_next_step6 | caught |
| M-STEP6-AT-POST5 | x5_r49_freed_home_frees_at_step6 (After5) | caught |
| M-FREE-RELEASE-DURABLE | x5_r49_freed_home_frees_at_step6 (before flip) | caught |
| G-FE-PENDING | d14_monitors (FREE-EARLY, monitors=[]) | caught |
| G2 | exhausted_reserve_keeps_both_copies (slot 13) | caught |
| O-STEP6-NOCLEAR (own: step 6 does not clear) | pins 1 and 2 (Mid6) | caught |
| O-SLOTFREE-NO-DURABLE (own) | pin 1 (before flip) | caught |
| O-FREE-DROP-IMGREFS (own: the release also drops `image_refs`) | pin 2 | **UNCAUGHT** |

Mutants ran on an exported copy of `b5c9a840`. The sources were restored
and diffed clean against the commit. The worker's worktree was not touched.

## Recommended P ruling (for the card text; P decides)

Proposed **X5-R50 (step-4 movement exemption; X5-R49 unchanged).**
- **(a) "Moved" rounds.** A bullet-2 round (a new O-1, O-2 or O-3 text) is
  *moved*, not *new*, when all of the following hold:
  - it is class MP or MW, with `pass=true`;
  - it has no O-3 text;
  - its `monitors` are empty;
  - and either
    - (MP) every amcheck (i) finding is in `tainted_image_slots`, and that
      set is non-empty; or
    - (MW) `x33x` is true.
- **(b) Reporting.** Moved rounds are reported both ways (gained and lost),
  never counted.
- **(c) Scope.** Bullets 1 and 3 are unchanged.
- **(d) The 10k comparator.** The card's Acceptance comparator applies the
  same exemption. As written, its `ok` treats any text as failing, so it
  would also fire on moved rounds.

## Follow-ups

**X-5c-F4-F1-F1-F3-F2-F1 (continue the card; Base `b5c9a840`, Gate P).**
Carry X5-R49 verbatim and the ruling above. Discard attempt 2's uncommitted
edits; do not import them.
1. Re-run step 4's movement under X5-R50 and record both directions.
   Expected: 0 new failures, and moved rounds +7/−5 on 0..1000.
2. Then the "Otherwise" branch:
   - apply the 13 measured X5-R37 digests and every other moved literal,
     each with its cause named;
   - run the `r41_share` 1000 comparison against `m/a1.json`, re-selecting
     F3's share-1000 pins by their own rule where they no longer qualify;
   - re-run F3's 14 mutants (M-FREE-HOME-ALLOC is expected UNCAUGHT);
   - run the frees / released-at-step-6 / reused-in-session diagnostic.
3. P must also give `g8_checkpoint_poison_ends_the_workload` a re-selection
   rule, because 967 r1 no longer exists. Suggested: the lowest BASE seed in
   0..1000 other than 747 whose r1 crashes at `poison`. 47 r1 now does.
4. Then step 5 and the runner's 10k, with the amended comparator.

**X-5c-F4-F1-F1-F3-F2-F2 (pin gap; Gate S; can fold into -F1).**
- Add `x5_r49_window_freed_unimaged_home_held_by_image_refs`: a page whose
  home was written by an eviction in epoch k before the flip (not in
  `used_durable`), touched clean in the window (`Img::At(s)`) and then
  freed. `s` must not be free through checkpoint k's Mid6.
- It must fail under O-FREE-DROP-IMGREFS.
- Test-only; no store change.
