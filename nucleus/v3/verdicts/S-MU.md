Card: S-MU
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session b1ce2eab-7c6b-4ddc-9b92-6a12c32c38d7, dispatched from T3 thread 45584af2-e651-490e-862c-3402320cffa3 (an independent P re-gate: this agent wrote neither the card S-MU, its build b1ecf549, nor its rework c7d446e9, and is not the orchestrator)
Date: 2026-11-24
Gate-exit: GATE EXIT=0
Commit: c7d446e9404c267cdf95c614381ca27645315584
Mutants: spec text, none in code; each text form committed one at a time in a throwaway clone and judged by Acceptance lines 1 to 67 in both locales. MAPLEAF-CLEAN-NOT-COPIED form A (R-2: "whether or not its frame is dirty and whatever slot it was loaded from, unless" -> "if its frame is dirty, unless") -> lines 19, 60; MAPLEAF-CLEAN-NOT-COPIED form B (R-1: "So it references no MapLeaf's eviction home … until the map phase has taken that content" -> "So it references each MapLeaf's current home, as each MapLeaf's epoch-`k` content is there") -> 28, 57, 60; R-1 left out -> 28, 56, 57, 59, 60; R-3's reload-run sentence left out -> 44, 57, 60; the first gate's eleven mutants stand (verdict fadeebfa)
Followups: S-MU-F1, S-MU-F2

## Verdict in brief

**MERGE-WITH-FOLLOWUPS** on `c7d446e9404c267cdf95c614381ca27645315584` (S-STORE draft 7.21, R-1-memory draft 2), the rework of the REWORK verdict at `fadeebfa` (build `b1ecf549`).

This re-gate judges the rework only; the first gate's design judgment, its findings and its mutant table stand. The rework is exactly R-1, R-2 (with F-5 folded in), R-3 (i) to (iii) and the two document recomputes of "What else moves" (E1, E2), word for word, and nothing else. F-1 and F-3 are closed: the definition no longer admits the reading that left a clean MapLeaf of content epoch `k` uncopied, §5.6 now states the copy rule outright, and a reload run with its mutant pins it.

## What I verified myself

- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through GATE-PROMPT step 1's shallow `--no-local` clone (scaffold `adcfab94227b6b9361e7d9db510835abbd130f53`, no `EXTRACT FAILED`), run on the full commit id with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-MU`:
  - `fresh-gate: commit c7d446e9404c267cdf95c614381ca27645315584`
  - `fresh-gate: base adcfab94227b6b9361e7d9db510835abbd130f53`
  - `fresh-gate: checks passed (4897 files hashed in 7.19 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - Steps: `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`. No `not ok` row, no `test result: FAILED`, and stderr is empty.
  - The literal last line is `GATE EXIT=0`, on the first run (1-minute load 33.9 at launch, 4.75 at the end); no re-run was needed.
- **Scope.** `git diff fadeebfa..c7d446e9`: one file, `nucleus/v3/docs/S-STORE.md`, 7 insertions and 7 deletions (lines 3, 371, 1087, 1409, 1538, 1539, 1752). Against the base (merge-base with the scaffold is still `c81e84a1`; the scaffold's two documents equal `c81e84a1`'s blobs `60999acf…`, `34ff172a…`): S-STORE 47+/40−, R-1-memory 17+/16− (unchanged), plus this verdict. Both documents are on the Touch only list.
- **The diff is the rework text, verbatim.** I applied the verdict's exact strings by script to `fadeebfa`'s S-STORE (each anchor once): R-1's Old → New; "which only step 2 sets" → "which only step 2 and the open set" (F-5); R-2's sentence, with its leading space, after U13's "(card S-MU)."; R-3's reload run, with its leading space, after "image `k+1` holds X's epoch-`k` content.". The remaining character diff against the tip is only: R-3's mutant clause, inserted after the MAPLEAF-HOME-LEAKED clause ("…takes another slot") before the cell's " |"; ", MAPLEAF-CLEAN-NOT-COPIED" appended to U41's list and to the A-3c and A-7f rows' Mutants cells; "; the reload run" appended to those two rows' Form cells; "six" → "seven" in the status line (E1); "280" → "281" and "Six" → "Seven" in the 7.21 Counts bullet (E2). Nothing else.
- **Acceptance lines 1 to 67** (the card as now installed, sha256 `0c57d2d2…`), at the tip, in a `--no-local` throwaway clone with `nucleus/v3-scaffold` at `adcfab94`: all pass under `LANG=en_CA.UTF-8` and `LC_ALL=C`. Line 58 prints `174 rows; 281 mutants; 24 judged only on Phase B cards; 48 invariants; 55 tests` and `OK` in both. Line 59's `47 40` is `git diff --numstat c81e84a1 c7d446e9`; line 60's hash `fdbe1725…` passes on the committed diff.
- **Recomputes the verdict named.** E1, E2, "The draft target" (281 = 274 + 7; 47/40), the §15 rows section ("New mutants (7)", the seventh on I-CKPT-AT-T, A-3c and A-7f), D-7 ("a growth run and a reload run … Seven mutants") and Acceptance lines 1, 6, 28, 44, 46, 47, 52, 53, 57 to 60 all hold: each passes, and each fails when the text it pins is taken out (below).
- **Mutants** (table below): each committed on a throwaway branch, judged, then dropped; the clone ended at the tip with a clean status. The card worktree was not touched.
- **The reworker's negative checks.** I re-ran two, committed: R-1 left out fails 28, 56, 57 (`card S-MU:37: found 36`), 59 and 60; the reload-run sentence left out fails 44, 57 (`card S-MU:37: found 36`) and 60. These match the report's lines (its 59/60 were "if committed" forms; committed, they fail as stated).

## Mutants (text forms; Acceptance 1 to 67, both locales, identical)

| Mutant | Change | Failing lines |
|---|---|---|
| MAPLEAF-CLEAN-NOT-COPIED, form A | R-2: copied "if its frame is dirty, unless …" | 19, 60 |
| MAPLEAF-CLEAN-NOT-COPIED, form B | R-1: the image map "references each MapLeaf's current home" | 28, 57, 60 |
| R-1 left out | §13 step 2's definition back to 7.20's | 28, 56, 57, 59, 60 |
| R-3 reload run left out | the I-CKPT-AT-T row without "Its reload run …" | 44, 57, 60 |

The new mutant is caught in both of its text forms; the first gate's eleven stand.

## Rulings on the reworker's choices and what it left alone

- **(a) F-2's text under A-3c-C in the card: accepted.** The verdict's "Card text for the orchestrator (F-2), added under A-3c-C" asked for exactly that; the four bullets are the verdict's, verbatim.
- **(b) Acceptance lines 19, 55 and 56 recomputed: accepted, and required.** 19 pins U13's New and 55 U46's multi-line New, both of which R-2 and R-3 change, so without the recompute they fail on the correct text; the verdict's list omitted them. 56's added stretch (R-1's Old, "s the epoch-.k. map pages with the list applied") follows the card's own rule for 56 and is what catches R-1 left out. Each recomputed line was checked against the text by the runs above.
- **(c) The joins for R-3 (iii): accepted.** The name at the end of each list and "; " before "the reload run" are the card's convention (every block it adds appends; the Form cells use "; "). Line 58 confirms where the mutant is listed and judged.
- **The Changelog's first bullet: a follow-up, not this draft.** The normative §15 row and the Counts bullet are right; the first bullet's mutant list and its prose omit only the copy rule and its mutant, which no rule depends on. Re-cutting the entry now would move lines 6 and 60 again for no change in meaning. It goes to S-MU-F1 (text below), recorded in the next draft's own entry.
- **The card's "Mutation evidence", "The author's dry run", "For the gate" and Notes: a follow-up (S-MU-F2).** Card-local and git-ignored; nothing ships or is judged by them, but Notes' merge-order numbers are what the orchestrator uses to re-anchor S-KAPPA and S-LAMBDA, and they are now one low.

## Findings

No blocking finding. F-1 and F-3 of the first gate are closed; F-2 is carried in the card; F-5 and F-8 rode in the rework (F-8 in the card's prose: "seven rows", D-2's "which is new"); F-4, F-6 and F-7 remain S-MU-F1.

## Follow-ups

**S-MU-F1 (extended; spec, wording and a test gap; next S-STORE card).** As the first verdict named: F-4 (U22's lead-in to say what its elaboration says), F-6 (U17's exception in §9.2 item 6), F-7 (a mutant for U19's check, judged on A-3c-B's rows). Add F-12: draft 7.21's Changelog entry, first bullet, names MAP-INNER-FROM-CURRENT, IMAGE-REF-NOT-SET, LEVEL-IMAGE-REF-STALE and MAPLEAF-HOME-LEAKED but not MAPLEAF-CLEAN-NOT-COPIED, and does not mention §5.6's clean-MapLeaf copy rule that rework 1 added. The next draft's own Changelog entry carries, as one bullet: "- **Draft 7.21's rework, recorded** (§5.6 \"When it copies\", §13 step 2; I-CKPT-AT-T; verdict S-MU). Draft 7.21 also defined \"the image map\" as image `k`'s map as the map phase builds it, and made a MapLeaf whose content epoch is `e − 1` copied at its first mutation in epoch `e` whether or not its frame is dirty; mutant MAPLEAF-CLEAN-NOT-COPIED (A-3c, A-7f: `t_step2_waits_for_claimed_copy`'s reload run) judges it. Draft 7.21's entry did not name them." Draft 7.21's entry itself stays as merged.

**S-MU-F2 (card text, orchestrator; before S-KAPPA or S-LAMBDA is re-anchored on 7.21).** The local card `S-MU.local.md` still describes the pre-rework build in "Mutation evidence" (six mutants; no reload run), "The author's dry run", "For the gate" and Notes. In Notes, the merge-order counts are built on 280 mutants: "176 rows, 286 mutants" (S-KAPPA first) is now 287, "checker 174 rows and 288 mutants" (S-LAMBDA first) is now 289, and "All three merged: 176 rows, 294 mutants" is now 295; and S-MU now has 51 S-STORE blocks (U20a added). The orchestrator corrects those counts (re-derived by script on the composed files, as the Notes' own method says) before cutting either re-anchor card, and adds MAPLEAF-CLEAN-NOT-COPIED and the reload run to "Mutation evidence". No spec text moves.
