Card: R-1-SPEC
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent for thread 45584af2-e651-490e-862c-3402320cffa3 (an independent P gate 5, a narrow confirming gate after rework 3: this agent wrote neither the card, its implementation 32f68472, rework 1 98c85aca, rework 2 a8486ede, the move ecdae5d9, rework 3 c2e9dcff, nor gate 1's, gate 2's, gate 3's or gate 4's verdict, and is not the orchestrator)
Date: 2026-11-21
Gate-exit: GATE EXIT=0
Commit: c2e9dcff2142fc43f6ede525f055d5fc106b119d
Mutants: spec text, none in code; rework 3 changes no test, mutant or judging row, so gates 1–4's walks stand (History). Omit-one on every K12 entry, each reverted alone on c2e9dcff in a scratch clone and judged by the card's 53 non-gate Acceptance lines in both locales: K12a -> 53; K12b -> 53; K12c -> 53; K12d -> 48, 53; 4/4 caught, the two locales agreeing. At ecdae5d9 lines 48 and 53 fail and the other 51 pass
Followups: R-1-SPEC-F1, R-1-SPEC-F2, R-1-SPEC-F3, R-1-SPEC-F4, R-1-SPEC-F5, R-1-SPEC-F6, R-1-SPEC-F7, R-1-SPEC-F8

## Verdict in brief

**MERGE-WITH-FOLLOWUPS** on rework 3, `c2e9dcff2142fc43f6ede525f055d5fc106b119d`. Rework 3 is exactly gate 4's K12 and nothing else, and D11 is closed. Gate 4's verdict stands, and so does its merge-timing ruling: on the owner's own words of 2026-11-20, R-1-SPEC may merge before P0 PASS. The follow-ups R-1-SPEC-F1 to F8 carry, and so does gate 4's record of the owner points (P-R4 still open). This gate judged only the four things it was asked to check. Each one passes:
1. **The diff is exactly K12.** `git diff 0ea08b61 c2e9dcff` touches two files: `docs/C-T0-txn-protocol.md` (+1/−1) and `docs/R-1-memory.md` (+3/−3). Its four hunks are K12a–K12d. Each replaces "S-STORE draft 7.16" with "7.19" and changes nothing else on the line.
2. **The card holds K12, and its blocks reproduce all four files.** The card changes and stale-text updates are the ones gate 4 listed. Its blocks, applied by this gate's own parser, give all four of c2e9dcff's spec files byte for byte.
3. **Acceptance passes.** 53 of 53 non-gate lines pass in both locales, both checkers print `OK`, omit-one catches all four K12 entries, and the fresh gate ends GATE EXIT=0.
4. **No stale "7.16" is left.** No "7.16" remains that should be 7.19.

## Gate run

- **Load check.** At 23:57:34 (2026-11-20) the load average was 15.27 / 14.74 / 17.69 on the 16-CPU host. When the gate started it was 5.16 / 11.43 / 16.19, so I did not need to wait.
- **Scaffold extract** (GATE-PROMPT step 1): the shallow `--no-local` clone of `nucleus/v3-scaffold` printed **1cd6d48271566110e5af19d89f2435f26512885e**. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **The run:** `bash "$fg/fresh-gate.sh" c2e9dcff2142fc43f6ede525f055d5fc106b119d`, from `~/Workspace/neutron-v3`, with `PATH=$HOME/.cargo/bin:$PATH` and `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate5-R-1-SPEC`.
  - Stdout and stderr went to separate files, `~/.cache/nv3-review/gate5-R-1-SPEC/gate1.out` and `gate1.err`.
  - It ran from 23:59:23 (load 5.16 / 11.43 / 16.19) to 00:13:06 on 2026-11-21 (load 1.32 / 9.25 / 14.86). The 1-minute load, sampled each minute (`load1.log`), peaked at 31.46 (00:06:23).
  - Its lines:
    - `fresh-gate: commit c2e9dcff2142fc43f6ede525f055d5fc106b119d`
    - `fresh-gate: base 1cd6d48271566110e5af19d89f2435f26512885e`
    - `fresh-gate: checks passed (4892 files hashed in 1.52 s)`
    - `fresh-gate: 2 file(s) matched through their eol conversion`
    - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`
  - The literal last line was **`GATE EXIT=0`**, the exit status 0, and stderr was empty (0 bytes).
- **Flakes.** None of the known flakes occurred:
  - The test step exited 0, so neither `undo_order.rs` `readers_never_see_past_their_snapshot_during_apply` nor `sim_env.rs` `sim_halted_off_task` failed.
  - The enforcement self-tests printed `ok   premise: GUC: a -c core.untrackedCache=true beats the environment` and `ok   runner refs: RV cost ten honest moved refs verify in under 2 s`.
  - There was no SIGTERM, so no re-run was needed.

  `$fg` was removed afterwards.
- **The worktree** `~/.cache/nv3-cards/R-1-SPEC`, branch `card/v3-R-1-SPEC`, was clean at tip c2e9dcff before this commit (`git status --short` empty). Nothing but this verdict was written to it. Acceptance, omit-one and the base check ran in a `--shared` scratch clone, `~/.cache/nv3-review/gate5-R-1-SPEC/sc`.
- **The branch:** … → e7703ea2 (gate 3) → ecdae5d9 (the move) → 0ea08b61 (gate 4's REWORK) → c2e9dcff (rework 3) → this verdict.

## Check 1: the diff is K12 and nothing else

- **The commits.** c2e9dcff has one parent, 0ea08b61 (gate 4's verdict commit, whose only change on ecdae5d9 is `verdicts/R-1-SPEC.md`). Its subject is "nucleus v3: R-1-SPEC rework 3 (K12 of verdicts/R-1-SPEC.md)".
- **The diff.** `git diff --stat 0ea08b61 c2e9dcff` lists `nucleus/v3/docs/C-T0-txn-protocol.md | 2 +-` and `nucleus/v3/docs/R-1-memory.md | 6 +++---`, and nothing else. The four hunks are, word for word:
  - R-1-memory line 3: `derived against S-STORE draft 7.16;` → `7.19;` (K12a);
  - line 190: `as draft 7.16 words it,` → `7.19` (K12b);
  - line 466: `With S-STORE draft 7.16 and C-T0 draft 8.8.` → `7.19` (K12c);
  - C-T0 line 861: `card R-1-SPEC, with S-STORE draft 7.16 and the new` → `7.19` (K12d).
- **The sha256s.** At c2e9dcff: R-1-memory `c59273029ee150367a542aed711ca0b57cbd8eb3f604b13ee7e7b7d2978958bb` and C-T0 `58924b86b56ef246146de83353d59a70ba429c63d7bad7426553e14f39d9efb7`, both gate 4's values. S-STORE `a9978ac5…24b8` and README `191f528f…ab82` are unchanged from ecdae5d9 (`git diff --quiet` on the two files).

## Check 2: the card's changes, and its blocks

- **The changes to the card.** I compared the card, now sha256 `18a7dfc0…0170`, with its pre-rework backup (`~/.cache/nv3-review/R-1-SPEC.card-backup-pre-rework3.local.md`, `67ab3ef2…eb1f`), using a word diff:
  - **The blocks:** Doc block 0's status line, §4.4 and the Changelog take K12a–K12c, and T9's New takes K12d. No other block changes.
  - **Acceptance:** line 48's hash `cb803f38…2bbc` → `14e769ca75104f3af6b25fd92856ffd0ce5aac686bf3402a7d677f307190bb12`, and the new line 53 is gate 4's, verbatim. The gate line becomes 54.
  - **The card text gate 4 required** ("Card text the rework must update"). All of it is present:
    - "Mutation evidence": "7.18 … at 7.19", a record of the move (ecdae5d9: the blocks on 7.18, the 52/52 run in both locales, the checkers' lines), and rework 3's evidence;
    - "The runner": 54 commands, rework 3 adding line 53;
    - Notes: "Dry-run results" with the move's and K12's sha256s (S-STORE `a9978ac5…`, R-1-memory `c5927302…`, C-T0 `58924b86…`, README `191f528f…`);
    - Notes: "If S-ZETA, S-ETA or S-THETA merges first" records that all three merged (in the order S-THETA, S-ETA, S-ZETA, the move being ecdae5d9) and adds the four 7.16 references;
    - Notes: "Not carried here" says F2-b's Old no longer occurs since S-ETA's 7.17, and that the S-EPSILON-F2 card re-anchors it;
    - "For the gate": item 13 (D11, K12).
- **It matches gate 4's trial card.** Against gate 4's trial card (`~/.cache/nv3-review/gate4-R-1-SPEC/card-k12.md`, `c0f2819b…d221`), the only differences fall in "Mutation evidence", "For the gate" and Notes (card lines 1063–1222). No block differs, and no line of the Acceptance section (1100–1157).
- **The blocks reproduce the files.** I used my own parser (`~/.cache/nv3-review/gate5-R-1-SPEC/g5rep.py`), written without reading the implementer's tools. It reads 15 Doc blocks and 36 edits, and applies them to 1cd6d482's files, every Old exactly once at its turn. The date is 2026-11-20 for S-STORE and 2026-11-17 for the other three files. The four results equal c2e9dcff's `docs/S-STORE.md`, `docs/C-T0-txn-protocol.md`, `docs/README.md` and `docs/R-1-memory.md` byte for byte (`cmp`).

## Check 3: Acceptance, checkers, omit-one

- **The runner.** The branch's `tools/run_card.py` `parse_card` reads `R-1-SPEC`, four Touch only paths, 54 commands (the last `cd nucleus/v3 && bash scripts/gate.sh`) and no container, and `validate_card` returns `None`. My own extraction of the Acceptance section gives the same 54 lines.
- **Acceptance.** I ran the 53 lines before `gate.sh` with `/bin/sh -c` in a `--shared` scratch clone (`~/.cache/nv3-review/gate5-R-1-SPEC/sc`), detached at c2e9dcff, clean, with a local `nucleus/v3-scaffold` at 1cd6d482 (the shared repository's ref has the same value). **53 of 53 pass under `LANG=en_CA.UTF-8` and 53 of 53 under `LC_ALL=C`.** Line 54 is the fresh gate above.
- **The checkers** print, in both locales:
  - line 17: `7 invariants; 20 rows` and `OK`;
  - line 37: `172 rows; 263 mutants; 24 judged only on Phase B cards; 48 invariants; 55 tests` and `OK`.
- **On ecdae5d9** (pre-K12): lines 48 and 53 fail and the other 51 pass, in both locales.
- **Omit-one per K12 entry.** I reverted each entry alone on c2e9dcff (its New back to its Old, which occurs exactly once) and committed it in the scratch clone:

  | Entry left out | Failing lines (both locales) |
  |---|---|
  | K12a | 53 |
  | K12b | 53 |
  | K12c | 53 |
  | K12d | 48, 53 |

## Check 4: no "7.16" left that should be 7.19

- **R-1-memory, C-T0 and README** (c2e9dcff): `grep -E '7\.16'` finds nothing in any of the three.
  - `grep -c 'S-STORE draft 7\.19' docs/R-1-memory.md` gives 2 (the status line and the Changelog), and §4.4 reads "as draft 7.19 words it".
  - C-T0's 8.8 entry reads "with S-STORE draft 7.19".
  - README names no S-STORE draft.
  - R-1-memory's two remaining "draft 7.15"s ("the figure S-STORE §5.4 gave at draft 7.15", "Draft 7.15's wording") are true of drafts 7.15 to 7.18, as gate 4 ruled.
- **S-STORE 7.19.** "7.16" occurs four times, and every one means S-THETA's draft:
  - line 3, the status chain's "on draft 7.16 (card S-THETA, …";
  - line 1743, S-ZETA's 7.18 Counts, "as at drafts 7.16 and 7.17";
  - line 1752, S-ETA's 7.17 Counts, "as at draft 7.16";
  - line 1753, the heading `**Draft 7.16** (2026-11-17, card S-THETA, …`.

  The 7.19 entry (line 1732) pairs itself with R-1-memory draft 1 and C-T0 8.8, so all three version pairings now agree.
- **The card.** Its remaining "7.16"s are history or D11's own description: the S-EPSILON-F1 source, the S-STORE Edits' preamble, revision 1's evidence record, For the gate item 13, and the Notes. None is a statement about the current draft.

## Rulings on the owner points (the gate's part)

Gate 4's record carries unchanged. In the owner's own words of 2026-11-20 (QUEUE head, `# OWNER DECISION 2026-11-20 17:00 MST`):
- P-R1 (~224 KiB per connection), P-R2 (the height bound and its stop), P-R3 (the provisional per-mode budgets), P-R5 (the growth residual) and P-R6 (the caps 8 and 2) are **accepted**.
- **P-R4** (C-T0's thresholds per mode, and the earlier switch to range locks in embedded and browser mode) is **still open**. The reply does not name it. The no-decision default is the text as written: R-1-memory §7.1 and C-T0 8.8 carry the recommended values.

K12 changes no owner point.

## Merge timing

The owner's own words of 2026-11-20 (QUEUE head) say: "The spec cards S-ETA, S-THETA, S-ZETA and R-1-SPEC may merge before P0 passes, and P0 is then judged on the merged spec." R-1-SPEC may therefore merge on this verdict, before P0 PASS.

## Follow-ups (F1–F8 carried from gates 3 and 4, unchanged)

The full descriptions are gate 3's and gate 1's, and gate 4 re-checked them against ecdae5d9 (History). K12 touches none of them.
- **R-1-SPEC-F1** (an S-STORE spec card, before A-7d is cut): the read-only budget's two test forms on A-7d, RO-BUDGET-DIRTY-ONLY and RO-SIZED-FROM-F.
- **R-1-SPEC-F2** (the R-1 spec, before R-1's Phase B card): an exact "third growth" allowance.
- **R-1-SPEC-F3** (R-1 and A-7c, before A-7c is cut): step 2's copy of image `k`'s two bitmaps.
- **R-1-SPEC-F4** (the R-1 spec, before R-1's Phase B card): the open refuses with 53200 a directory taller than the sized pool's `H`, plus gate 3's read-only item.
- **R-1-SPEC-F5** (the next S-STORE card; pointers only):
  - S-STORE §14 step 3b and §6.8's CreateTree point to R-1-memory §5.2.
  - Now due, since S-THETA merged: R-1-memory's "step budget" should point to S-STORE §15's definition.
  - The optional read-only parenthetical in §5.4 and §19 T8.
- **R-1-SPEC-F6** (the R-1 spec, with or before R-1's Phase B card): tests for K1, K7 and K10.
- **R-1-SPEC-F7** (an S-STORE question; P-R2 is accepted): whether a GC sweep or drop walk that descends past `H` may skip the tree with a report rather than poison.
- **R-1-SPEC-F8** (R-1 and S-STORE, with F1, before A-7d is cut): the read-only floor checked continuously, the reserve allowed to shrink mid-op, a fall-back re-sizing afresh, and a third test form (RO-FLOOR-PER-OP and RO-H-SET-ONCE on A-7d).

No new follow-up. F2-b's re-anchor belongs to the S-EPSILON-F2 card, as gate 4 said.

## Not verified

- **No code exists.** No test or mutant was re-walked, since rework 3 changes none of them (gates 1–4 walked them as text).
- **Gate 4's own checks of the move** were not repeated, as this request directed: the S-STORE resolution, the reverse application, the counts table, and Scope 4's rule comparison against S-THETA, S-ETA and S-ZETA. My block reproduction (Check 2) does re-confirm the S-STORE result byte for byte from 1cd6d482's 7.18.
- **Omit-one on the 36 edit blocks and the 15 Doc blocks** was not repeated. Gates 1–4 ran their own.
- **The implementer's scratch** (`~/.cache/nv3-review/rework3-R-1-SPEC/`) was not read, and none of its outputs was used.
- **Gate 4's carried items** stay unverified: the provisional constants, and S-THETA's, S-ETA's and S-ZETA's own checkers on 7.19.

## History

- **Gate 1: REWORK, verdict commit 7fb189020418b1700928486fd8d84d3aba4a5358, judging 32f68472b4155671f5418f0fb633b0859958c89a.** Defects D1–D9, corrections K1–K10, follow-ups F1–F7.
- **Rework 1: commit 98c85acadefe95d62badb88856eaff989727099e.** Applied K1–K10.
- **Gate 2: REWORK, verdict commit 22722c9dc481a41246bf5bad1bc7c6faad3b8943, judging 98c85aca.** Defect D10, corrections K11, F1 extended.
- **Rework 2: commit a8486ede087555faa2c55f86c6417c28063419d0.** Applied K11.
- **Gate 3: MERGE-WITH-FOLLOWUPS, verdict commit e7703ea255e9a7fbbddb3974296a8e37ba251eef, judging a8486ede.** Against S-STORE 7.15. F8 new.
- **S-THETA, S-ETA and S-ZETA merged first** (6e3423f6, adbe6a16, 1cd6d482), taking S-STORE to 7.16, 7.17 and 7.18.
- **The move: commit ecdae5d93c33dcde53d59ad98a282a1044b6a2fb.** Merged 1cd6d482. R-1-SPEC's S-STORE becomes draft 7.19.
- **Gate 4: REWORK, verdict commit 0ea08b610299512eeae6b6db127967d4281f879e, judging ecdae5d9.** P gate, Claude Code session c0435d3e-bb96-47e9-a5d9-ab45ffb866ab, GATE EXIT=0. Defect D11, corrections K12, line 48's new hash and the new line 53. Merge timing updated from the owner's words of 2026-11-20.
- **Rework 3: commit c2e9dcff2142fc43f6ede525f055d5fc106b119d,** on 0ea08b61. Applied K12, and updated the card's stale text.
- **Gate 5 (this verdict): MERGE-WITH-FOLLOWUPS on c2e9dcff.**

**Gate 4's verdict, as committed at 0ea08b61, verbatim.** Its header lines are prefixed with "- ", so that no tool reads them as this verdict's fields. Everything after them is unchanged, headings included. That includes gate 3's verdict, which gate 4's History holds verbatim, and gates 2's and 1's verdicts inside it. Where its sections differ from the ones above, the sections above are current. The same text is at `git show 0ea08b61:nucleus/v3/verdicts/R-1-SPEC.md`.

<!-- gate 4's verdict (0ea08b61), verbatim but for its header lines, begins -->

- Card: R-1-SPEC
- Class: spec
- Card-gate: P
- Verdict: REWORK
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: T3 review sub-agent for thread 45584af2-e651-490e-862c-3402320cffa3, Claude Code session c0435d3e-bb96-47e9-a5d9-ab45ffb866ab (an independent P gate 4, a narrow re-gate after the card was moved onto S-ZETA's draft 7.18: this agent wrote neither the card, its implementation 32f68472, rework 1 98c85aca, rework 2 a8486ede, the move ecdae5d9, nor gate 1's, gate 2's or gate 3's verdict, and is not the orchestrator)
- Date: 2026-11-20
- Gate-exit: GATE EXIT=0
- Commit: ecdae5d93c33dcde53d59ad98a282a1044b6a2fb
- Mutants: spec text, none in code; the move changes no test, mutant or judging row, so gates 1–3's walks stand (History). Omit-one on 12 of the 36 edit blocks, each built by my own parser on 1cd6d482's files with one block left out, committed in a scratch clone and judged by the card's 52 non-gate Acceptance lines in both locales: E3 -> 26, 36, 39; E5 -> 27, 38, 39; E8 -> 30, 38, 39, 51; E9 -> 31, 36, 39; E12a -> 38, 39, 51, 52; E14 -> 37, 38, 39; E18 -> 37, 38, 39; E21 -> 34, 38, 39; E24 -> 35, 38, 39, 51; E25 -> 24, 38, 39, 52; T5 -> 42, 43, 48; X1 -> 44, 45, 49; 12/12 caught, the two locales agreeing. K12 (this verdict's corrections) in scratch: at ecdae5d9 the new line 53 and line 48 with its new hash fail; omitting any one K12 entry fails line 53 (and K12d line 48 too), in both locales
- Followups: R-1-SPEC-F1, R-1-SPEC-F2, R-1-SPEC-F3, R-1-SPEC-F4, R-1-SPEC-F5, R-1-SPEC-F6, R-1-SPEC-F7, R-1-SPEC-F8

## Verdict in brief

**REWORK**, narrow, on the merge commit `ecdae5d93c33dcde53d59ad98a282a1044b6a2fb`, which moves R-1-SPEC onto S-ZETA's draft 7.18 and makes S-STORE draft 7.19. The S-STORE resolution is exact, and the checks pass. But the move left four cross-references in the two other edited spec files naming "S-STORE draft 7.16". On the merged scaffold, draft 7.16 is **S-THETA's** draft, not R-1-SPEC's, so those references are now wrong. One of them makes a false statement. The fix is four one-number replacements (K12), the C-T0 diff hash and one new Acceptance line, all tried in scratch.

- **The S-STORE resolution is exact.**
  - My own parser applies the card's 26 S-STORE blocks to 1cd6d482's 7.18 (`79cd8027…bcb9`), with `<date>` 2026-11-20. The result is the merge commit's file byte for byte (`a9978ac5…24b8`), and the reverse gives 7.18 back.
  - The blocks are gate 3's, unchanged: all 36 Olds and 34 of the 36 News are byte-identical to the pre-move card's. E1 changes only its draft number. E25 changes only its number and its Counts bullet, and the counts are right (below).
  - No S-THETA, S-ETA or S-ZETA text is altered.
  - `docs/R-1-memory.md`, `docs/C-T0-txn-protocol.md`, `docs/README.md` and `verdicts/R-1-SPEC.md` are byte-identical to gate 3's commit e7703ea2.
- **The checks pass.** Fresh gate: `GATE EXIT=0` (below). All 52 non-gate Acceptance lines pass at ecdae5d9 under `en_CA.UTF-8` and under `LC_ALL=C`, and all 52 fail at 1cd6d482. Both checkers print `OK`. Omit-one catches 12 of the 12 blocks I sampled.
- **The defect (D11, blocking): the R-1-memory and C-T0 cross-references now name the wrong S-STORE draft.** Four places say "S-STORE draft 7.16":
  - R-1-memory's status line ("derived against S-STORE draft 7.16");
  - R-1-memory §4.4 ("S-STORE §5.3's premise, as draft 7.16 words it, is `O ≥ Σ Q_t`");
  - R-1-memory's Changelog ("With S-STORE draft 7.16 and C-T0 draft 8.8");
  - C-T0's 8.8 Changelog entry ("with S-STORE draft 7.16 and the new `docs/R-1-memory.md` draft 1").

  Gate 3 judged these when draft 7.16 *was* R-1-SPEC's S-STORE. On the merged scaffold, draft 7.16 is S-THETA's (6e3423f6). Its §5.3 still has draft 7.15's premise, "a frame outside `R_EPOCH` that no pin holds" (E3's Old occurs once in it, E3's New not at all), and it never mentions R-1-memory.
  - So §4.4's sentence is now false of the draft it names.
  - The three version pairings contradict S-STORE's own 7.19 status line and Changelog, which pair *7.19* with R-1-memory draft 1 and C-T0 8.8.
  - This happens just as P0 is to be judged on the merged spec (the owner's decision of 2026-11-20). A reader who opens S-STORE 7.16 to check R-1's numbers finds none of them.

  The move changed these references' meaning without touching their bytes. Neither the card's Notes ("If S-ZETA, S-ETA or S-THETA merges first") nor the move updated them. K12 fixes them.
- **Nothing else blocks.** I compared R-1's rules in 7.19 and R-1-memory against S-THETA's pool rules (I-PIN-CURRENT, pages being loaded, freed frames kept for reuse, waits under tree latches, the flip wait), S-ETA's WAL-walk rules and S-ZETA's open refusal. They neither restate nor contradict any of them in a way that matters (Scope 4).

**Merge timing.** The owner's own words of 2026-11-20 (QUEUE head, `# OWNER DECISION 2026-11-20 17:00 MST`) say: "The spec cards S-ETA, S-THETA, S-ZETA and R-1-SPEC may merge before P0 passes, and P0 is then judged on the merged spec." So once K12 is applied and re-gated, R-1-SPEC may merge before P0 PASS. That replaces gate 3's "may not merge before P0 PASS unless the owner says so in their own words".

## Gate run

- **Load check.** At 23:35:21 the load average was 21.09 / 21.99 / 24.43, on a 16-CPU host with other projects' jobs running. I waited, polling every 30 s (`loadwait.log`), until the 1-minute average fell below 16: 14.56 / 20.03 / 23.53 at 23:36:51.
- **Scaffold extract** (GATE-PROMPT step 1): the shallow `--no-local` clone of `nucleus/v3-scaffold` printed **1cd6d48271566110e5af19d89f2435f26512885e**. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **The run:** `bash "$fg/fresh-gate.sh" ecdae5d93c33dcde53d59ad98a282a1044b6a2fb`, from `~/Workspace/neutron-v3`, with `PATH=$HOME/.cargo/bin:$PATH` and `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate4-R-1-SPEC`. Stdout and stderr went to separate files under `~/.cache/nv3-review/gate4-R-1-SPEC/` (`gate1.out`, `gate1.err`). It ran from 23:37:15 (load 12.19 / 19.03 / 23.11) to 23:52:39 (load 11.81 / 13.20 / 18.45). The 1-minute load, sampled each minute (`load1.log`), peaked at 29.89 during the run. Its lines:
  - `fresh-gate: commit ecdae5d93c33dcde53d59ad98a282a1044b6a2fb`
  - `fresh-gate: base 1cd6d48271566110e5af19d89f2435f26512885e`
  - `fresh-gate: checks passed (4892 files hashed in 2.82 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`
  - The literal last line was **`GATE EXIT=0`**, the exit status 0, and stderr was empty (0 bytes).
- **Flakes.** None of the known flakes occurred. The test step exited 0, so neither `undo_order.rs` `readers_never_see_past_their_snapshot_during_apply` nor `sim_env.rs` `sim_halted_off_task` failed. The enforcement self-tests printed `ok   premise: GUC: a -c core.untrackedCache=true beats the environment` and `ok   runner refs: RV cost ten honest moved refs verify in under 2 s`. There was no SIGTERM and no re-run. `$fg` was removed afterwards.
- **The worktree** `~/.cache/nv3-cards/R-1-SPEC`, branch `card/v3-R-1-SPEC`, was clean at tip ecdae5d9 before this commit (`git status --short` empty), and nothing but this verdict was written to it. The omit-one variants and the K12 trial used a `--shared` scratch clone, `~/.cache/nv3-review/gate4-R-1-SPEC/sc`.
- **The branch:** 71ecf3e6 → 32f68472 (the card) → 7fb18902 (gate 1's REWORK) → 98c85aca (rework 1) → 22722c9d (gate 2's REWORK) → a8486ede (rework 2) → e7703ea2 (gate 3's MERGE-WITH-FOLLOWUPS) → ecdae5d9 (the merge of 1cd6d482, the move) → this verdict.

## Scope 1: the merge commit

- **Its parents and tree.** The parents of ecdae5d9 are e7703ea2 (gate 3's verdict commit, on rework 2 a8486ede) and 1cd6d482 (the scaffold tip after S-ZETA's merge, pass 172). `git diff --stat 1cd6d482 ecdae5d9` lists exactly five files:
  - `docs/S-STORE.md` (+37/−18, as against 7.15 at gate 3);
  - `docs/R-1-memory.md` (+466, new);
  - `docs/C-T0-txn-protocol.md` (+9/−8);
  - `docs/README.md` (+2/−1);
  - `verdicts/R-1-SPEC.md`.

  The four docs are Touch only, and the verdict file is gate 3's, unchanged (blob `99f2b8de` at both commits). No protected path is touched.
- **C-T0 and README did not move.** `git diff 71ecf3e6 1cd6d482` on `docs/C-T0-txn-protocol.md`, `docs/README.md` and `docs/R-1-memory.md` is empty, and 1cd6d482's C-T0 is still the card's Base hash `96caa6cb…a0c0`. So T1–T9 and X1 apply unchanged.
  - The merge commit's R-1-memory (`e3956d03…bc5d`), C-T0 (`8a826331…9149`) and README (`191f528f…ab82`) have the same blob ids as at e7703ea2: `41e7a967`, `6bb2754a` and `273949d4`.
- **The S-STORE resolution, by my own parser.** I wrote a line-based parser (`~/.cache/nv3-review/gate4-R-1-SPEC/g4parse.py`) without reading the implementer's `apply719.py` or gate 3's. It reads 15 Doc blocks and 36 edits from the card: E1–E25 with E12a, T1–T9 and X1. That is 87 fenced blocks, matching the card's 87 `text` fences.
  - **7.18 to 7.19.** Every E-block Old occurs exactly once in 1cd6d482's untouched 7.18 and exactly once at its turn. Applied in order with `<date>` 2026-11-20, the date in 7.19's status clause and Changelog head, the result equals the merge commit's `docs/S-STORE.md` byte for byte (sha256 `a9978ac53a81d499764cb162302da723d1e1cd1cc55e934b4afe39239d2824b8`).
  - **The reverse.** E25 down to E1, New to Old, each New found exactly once, gives 1cd6d482's 7.18 byte for byte.
  - **The other three files.** T1–T9 on 1cd6d482's C-T0, X1 on its README, and the Doc blocks joined, all with `<date>` 2026-11-17, give the merge commit's three files byte for byte. With 2026-11-20 they differ only in their dates.
- **Meaning unchanged from what gate 3 judged.** I parsed the pre-move backup (`~/.cache/nv3-review/R-1-SPEC.card-backup-pre-718.local.md`, sha256 `602470e9…ac58`) with the same parser and compared it block by block with the moved card (`cards/R-1-SPEC.local.md`, sha256 `67ab3ef2…eb1f`).
  - The 15 Doc blocks are identical, and so are all 36 Olds.
  - 34 of the 36 News are identical. The two that differ, word by word:
    - **E1:** `Status: draft 7.16 (card R-1-SPEC,` → `7.19`.
    - **E25:** the heading `**Draft 7.16**` → `**Draft 7.19**`. In its Counts bullet: `47` → `48` rows, `233` → `263` mutants, `50` → `55` tests and `162` → `172` §15.1 rows; "as at drafts 7.14 and 7.15" → "7.17 and 7.18"; and "every mutant of draft 7.15" → "7.18".
  - **The counts are right.** My own counter on each file gives:

    | File | §15.1 rows | mutants | judged only on Phase B | invariants | tests |
    |---|---|---|---|---|---|
    | 7.15 (71ecf3e6) | 158 | 226 | 24 | 47 | 48 |
    | 7.16, R-1's (e7703ea2) | 162 | 233 | 24 | 47 | 50 |
    | 7.17 (adbe6a16) | 168 | 248 | 24 | 48 | 53 |
    | 7.18 (1cd6d482) | 168 | 256 | 24 | 48 | 53 |
    | 7.19 (ecdae5d9) | 172 | 263 | 24 | 48 | 55 |

    R-1's delta is +4 rows, +7 mutants and +2 tests, the same on 7.18 as on 7.15. "24 … as at drafts 7.17 and 7.18" is true.
- **No S-THETA, S-ETA or S-ZETA text is altered.** Every R-1 Old occurs once in 7.15 and once in 7.18, unchanged, so no Old spans a line those cards changed.
  - Nine of R-1's 22 hunks sit within three lines of a line that S-THETA, S-ETA or S-ZETA changed: line 3, the status line; lines 288, 328/329, 1277, 1386, 1394 and 1665, in §5 and the §15/§15.1 tables; and 1718, the Changelog head.
  - In each case R-1's hunk replaces only its own Old's text. E1 replaces only the prefix `Status: draft 7.1`, and the tail after `) on draft 7.18 (card S-ZETA, ` is the scaffold's. E25 inserts above S-ZETA's `- **Draft 7.18**`, which it keeps.
  - The reverse application above shows this byte for byte.

## Scope 2: the card's updates

`diff` of the pre-move backup against the moved card shows 15 changed hunks, and every change is one that the base change needs:
- **Draft and base.** The title; the Base line (1cd6d482, pass 172; 7.18's sha256 `79cd8027…bcb9`); Edits' preamble (a sentence on the three cards merging first); Touch only item 2; Read; and Work 1, 3 and 4 (Work 4 reports the drafts used as S-STORE 7.19, C-T0 8.8 and R-1-memory 1).
- **E1 and E25**, as in Scope 1.
- **Acceptance.**
  - Lines 22 to 24: the draft number and the chain, `) on draft 7\.18 (card S-ZETA, ` and `Draft 7\.19.*card R-1-SPEC.*Draft 7\.18.*card S-ZETA`.
  - Line 37's checker: base `48e52d3b` → `1cd6d482`, and counts 162/47/50 → 172/48/55. Its mutant set `W` (R-1's seven) and every placement are unchanged. That is right, because it tests `M − Mo` and moves against the base, so S-THETA's, S-ETA's and S-ZETA's mutants enter through `Mo` and need no names.
  - Line 39's hash → `9b305768…4228`.
  - Line 38's numstat stays `'37 18'`, which is right: the diff on 7.18 is +37/−18 (`git diff --numstat`).
- **Left out by the implementer**, who hit its usage limit before running `gate.sh` or reporting. These are card text only, and the card does not merge. The rework updates them (below, "Card text the rework must update"):
  - "Mutation evidence" still says "every mutant that S-STORE 7.15 judges on a Phase A row is judged on the same rows at 7.16".
  - There is no record of the move's dry run.
  - The Notes' "Dry-run results" are still rework 2's, on 7.15.
  - The Notes' "If S-ZETA, S-ETA or S-THETA merges first" does not say it has happened. It also lists only E1, the status chain, the counts, the checker and lines 38–39, which misses the four 7.16 references (D11).
  - The Notes' "Not carried here" says each Old of S-EPSILON-F2's five riders "still occurs once on this card's 7.16 result". That is no longer true: F2-b's Old ("**A corrupt meta**, and a newest image that fails at open with no previous image, fail as they do for a read-write open …") occurs 0 times in 7.18 and 7.19, because S-ETA's 7.17 (adbe6a16) rewrote that sentence. F2-a, F2-c, F2-d and F2-e still occur once. That is not R-1-SPEC's doing, but the S-EPSILON-F2 card must re-anchor F2-b.
- **The runner.** The branch's `tools/run_card.py` `parse_card` and `validate_card` read `R-1-SPEC`, four Touch only paths, 53 commands (the last `cd nucleus/v3 && bash scripts/gate.sh`), no container, and `validate_card` returns `None`. My own extraction of the Acceptance section gives the same 53 lines.

## Scope 3: gate, Acceptance, checkers, omit-one

- **Acceptance.** I ran the 52 commands before `gate.sh` with `/bin/sh -c` from the worktree root at ecdae5d9 (clean, `git status --short` empty). They pass **52 of 52 under `LANG=en_CA.UTF-8` and 52 of 52 under `LC_ALL=C`**. Line 53 is the fresh gate above.
- **On the base.** In a `--shared` scratch clone (`~/.cache/nv3-review/gate4-R-1-SPEC/sc`, local `nucleus/v3-scaffold` = 1cd6d482), all 52 fail at 1cd6d482 in both locales.
- **The checkers** print, in both locales:
  - line 17: `7 invariants; 20 rows` and `OK`;
  - line 37: `172 rows; 263 mutants; 24 judged only on Phase B cards; 48 invariants; 55 tests` and `OK`.
- **Omit-one** (12 of the 36 edit blocks, all four files' kinds). I built each variant with my parser on 1cd6d482's files with one block left out, committed it on ecdae5d9 in the scratch clone, and ran the 52 lines in both locales. All 12 are caught, and the locales agree:

  | Block left out | Failing lines |
  |---|---|
  | E3 | 26, 36, 39 |
  | E5 | 27, 38, 39 |
  | E8 | 30, 38, 39, 51 |
  | E9 | 31, 36, 39 |
  | E12a | 38, 39, 51, 52 |
  | E14 | 37, 38, 39 |
  | E18 | 37, 38, 39 |
  | E21 | 34, 38, 39 |
  | E24 | 35, 38, 39, 51 |
  | E25 | 24, 38, 39, 52 |
  | T5 | 42, 43, 48 |
  | X1 | 44, 45, 49 |
- **Tests and mutants.** The move changes no test, mutant, judging row or Needs cell of R-1-SPEC's: the blocks are gate 3's. S-THETA's, S-ETA's and S-ZETA's tests and rows are untouched (line 37's "moved" and "lost" checks pass against 7.18). So no test becomes unpassable and no mutant unkillable by the move. Gates 1–3's walks stand.

## Scope 4: R-1's rules against S-THETA's, S-ETA's and S-ZETA's in 7.19

I grepped 7.19, R-1-memory and R-1's S-STORE blocks for: I-PIN-CURRENT; "being loaded"/load; "for reuse"/"freed"; "page table"; tree latches and waits under latches; the flip wait; `wal_seg_size`; 58030; XX001; the fence; refusal; the walk; salvage; "well-formed"/"intact"; segments; and "writes nothing". I read each hit in context.
- **S-THETA's pool rules.** Gate 1's analysis (History, "Merge order with S-ZETA, S-ETA and S-THETA") still holds on the merged text. I re-checked it against R-1-memory §4.4's five classes and E3's §5.3 sentence in 7.19:
  - A frame being loaded is the loader's own pin, so it is (T) in the loader's path term. A pin that finds the page being loaded holds no frame for it and waits for no frame acquisition. Its unused `Q_t` is slack.
  - A load ends, its frame out of the page table, before its thread restarts or waits for the flip. So no loading frame stays pinned across the flip wait.
  - A freed page's frame is no longer dirty (7.16, §4.2). It is unpinned and clean, so it is (U), and its MapLeaf no longer holds a §4.1 pin for it, which only shrinks (Mu). A creation that makes its page in a freed frame obtains no frame.
  - A creation that waits for a load under its tree latches (§5.4) waits for no frame and no page read. That is S-THETA's own wait, outside §5.3's frame-acquisition bound, which E3's sentence ("so the operation waits only for such a frame's brief latch holder") concerns. The 7.15 sentence already had that scope, and E3 changes only what it counts.
  - I-PIN-CURRENT's "a pin looks in the page table first" and its "at most one frame per page" leave `|T|`, `|Mp|` and `|Mc|` as counted.
  - §5.2's existing rule (a non-gated evictor under a pending flip takes a clean victim or restarts) predates S-THETA. R-1's fairness premise is for a gated operation past its first mutation, which holds the gate and can evict dirty pages.

  No restatement, no contradiction.
- **S-ETA's WAL-walk rules.**
  - R-1-memory §5.3 charges "the walk's record buffer … at most the longest record that pass 1, step 3b or pass 2 holds whole, itself at most `wal_seg_size / 4`". S-ETA makes pass 1 decode every record it accepts, with no decode state beyond the record that S-STORE §12.5 names, so the bound stands.
  - R-1-memory §6.1's lowering of `preapply_bytes` to `wal_seg_size / 4`, "2 MiB at C-T6-rehost's least segment, 8 MiB", agrees with S-ETA's 8 MiB creation floor, and its 212-byte floor binds only the store's tests.
  - S-ETA's 58030 for a WAL read error, and its XX001 for malformed records and short segments, are outcomes R-1 does not restate. R-1-memory's error table lists 58030 only for its own temp-file case.
  - R-1-memory §5.3's "nothing written beyond the fence (step 1) and step 4a's pass-2 evictions" for a 53200 before the retire write agrees with S-ETA's new §14.2 row. That row adds "the retire write" only once meta `k` is retired, which R-1's sizing precedes.
- **S-ZETA's open refusal.**
  - S-ZETA's refusals are at §14 step 1, before any charge or sizing of R-1's (steps 2–3b), and "an open that the open rule refuses writes nothing, the fence included". R-1-memory's open 53200s come at its charges and sizing in steps 2–3b, after step 1, and write nothing, so the two do not meet.
  - R-1's texts name no XX001 (the owner's XX000 for engine bugs, S-IOTA, does not touch them).

Nothing here meets the blocking bar. D11 is the only blocking defect, and it is found by the draft references, not by these rules.

## D11 (REWORK): four cross-references name the wrong S-STORE draft

R-1-memory and C-T0 8.8 were written when R-1-SPEC's S-STORE was draft 7.16. The move makes R-1-SPEC's S-STORE draft 7.19 and leaves draft 7.16 to S-THETA. The two other spec files still pair themselves with "S-STORE draft 7.16" (four places; `grep -c 'draft 7\.16'` gives 3 in R-1-memory and 1 in C-T0):
- **R-1-memory §4.4** says "S-STORE §5.3's premise, as draft 7.16 words it, is `O ≥ Σ Q_t`". S-THETA's 7.16 words it as draft 7.15 did, `|U| ≥ Σ Q_t`, so the sentence is false of the draft it names.
- **R-1-memory's status line and Changelog, and C-T0's 8.8 Changelog entry,** pair R-1-memory draft 1 and C-T0 8.8 with S-STORE 7.16. S-STORE 7.19's status clause and Changelog pair them with 7.19.

Blocking because the move changed these sentences' meaning (their referent), because one of them is now false, and because the version pairings contradict S-STORE's own. P0 is to be judged on this merged spec.

"Draft 7.15's wording" (R-1-memory §4.4), "the figure S-STORE §5.4 gave at draft 7.15" (§4.4) and S-STORE 7.19's E25 "draft 7.15's wording" stay true: drafts 7.15 to 7.18 all have that wording. They need no change.

## Corrections (one complete list; each Old occurs exactly once in ecdae5d9's file named)

In the card, K12a–K12c are Doc block text (K12a in the status line, K12b in §4.4, K12c in the Changelog), and K12d is T9's New. Each Old also occurs exactly once in the card. Applied in this order to ecdae5d9's files in scratch, they give `docs/R-1-memory.md` sha256 `c59273029ee150367a542aed711ca0b57cbd8eb3f604b13ee7e7b7d2978958bb` and `docs/C-T0-txn-protocol.md` sha256 `58924b86b56ef246146de83353d59a70ba429c63d7bad7426553e14f39d9efb7`, at the dates the files carry (2026-11-17). `docs/S-STORE.md` and `docs/README.md` do not change.

**K12a.** `docs/R-1-memory.md` — the status line (Doc block 0).

Old:
```text
derived against S-STORE draft 7.16;
```
New:
```text
derived against S-STORE draft 7.19;
```

**K12b.** `docs/R-1-memory.md` — §4.4, "The fairness bound holds with it."

Old:
```text
S-STORE §5.3's premise, as draft 7.16 words it,
```
New:
```text
S-STORE §5.3's premise, as draft 7.19 words it,
```

**K12c.** `docs/R-1-memory.md` — the Changelog, draft 1.

Old:
```text
With S-STORE draft 7.16 and C-T0 draft 8.8.
```
New:
```text
With S-STORE draft 7.19 and C-T0 draft 8.8.
```

**K12d.** `docs/C-T0-txn-protocol.md` — the Changelog, draft 8.8 (T9's New).

Old:
```text
card R-1-SPEC, with S-STORE draft 7.16 and the new
```
New:
```text
card R-1-SPEC, with S-STORE draft 7.19 and the new
```

## Acceptance-line changes (card numbering; tried in scratch)

- **Line 48** keeps its command and its numstat `'9 8'`, and its hash becomes `14e769ca75104f3af6b25fd92856ffd0ce5aac686bf3402a7d677f307190bb12`.
- **Lines 38 and 39** do not change: K12 touches no S-STORE line.
- **One new line**, inserted after line 52, so the gate line becomes line 54. It contains no backtick:
```text
cd nucleus/v3 && ! grep -n 'draft 7\.16' docs/R-1-memory.md docs/C-T0-txn-protocol.md && test "$(grep -c 'S-STORE draft 7\.19' docs/R-1-memory.md)" = 2 && grep -q 'premise, as draft 7\.19 words it, is .O ≥ Σ Q_t.\. Draft 7\.15.s wording' docs/R-1-memory.md && grep -q '^- \*\*Draft 8\.8\*\* (20[0-9][0-9]-[0-9][0-9]-[0-9][0-9], card R-1-SPEC, with S-STORE draft 7\.19 and the new ' docs/C-T0-txn-protocol.md
```
- **Tried.**
  - I committed K12's files on ecdae5d9 in the scratch clone (a throwaway commit). The card with K12, line 48's new hash and the new line applied (`~/.cache/nv3-review/gate4-R-1-SPEC/card-k12.md`, sha256 `c0f2819b…d221`) parses to 54 commands, and `validate_card` returns `None`. All 53 non-gate lines pass there, under `en_CA.UTF-8` and under `LC_ALL=C`.
  - That card's blocks, applied by my parser to 1cd6d482's files, reproduce all four K12 files byte for byte (dates 2026-11-20 for S-STORE, 2026-11-17 for the others).
  - At ecdae5d9, lines 48 and 53 fail and the other 51 pass, in both locales.
  - Omitting one K12 entry: K12a, K12b or K12c alone fails line 53, and K12d fails lines 48 and 53, in both locales.
  - Line 37's checker is unchanged, because K12 adds no row, test or mutant.

## Card text the rework must update (beyond the blocks and lines above)

- **"Mutation evidence".**
  - The sentence "every mutant that S-STORE 7.15 judges on a Phase A row is judged on the same rows at 7.16" becomes "… 7.18 … at 7.19".
  - A record of the move (2026-11-20, ecdae5d9): the blocks on 7.18, the run (52 of 52 in both locales), and the checkers' lines as Scope 3 gives them.
  - Rework 3's evidence: K12, the 53-line run, the new hash, and omit-one.
- **"The runner"** gives 54 commands, rework 3 adding line 53, with the gate line 54. Rework 2's record keeps its own numbers.
- **Notes.**
  - "Dry-run results" take the move's and K12's sha256s: S-STORE `a9978ac5…24b8` (date 2026-11-20); R-1-memory `c5927302…58bb` and C-T0 `58924b86…efb7` after K12 (2026-11-17); README `191f528f…ab82`.
  - "If S-ZETA, S-ETA or S-THETA merges first" records that all three did and that the move is ecdae5d9, and adds the four 7.16 references (K12) to its list.
  - "Not carried here" says that F2-b's Old no longer occurs, since S-ETA's 7.17, and that the S-EPSILON-F2 card re-anchors it.
- **"For the gate"** gains item 13: K12 (D11), with the check that no "draft 7.16" in R-1-memory or C-T0 is left meaning R-1-SPEC's S-STORE.

## Rulings on the owner points (the gate's part)

The owner answered most of them in their own words on 2026-11-20 (QUEUE head, `# OWNER DECISION 2026-11-20 17:00 MST`): "Accept the memory minimum (about 224 KiB per connection), the default connection caps (8 embedded, 2 browser) and the provisional per-mode budgets." and "Yes to: … the tree-height and file-growth stops." In the card's terms:
- **P-R1 (four tree paths, ~224 KiB per connection):** accepted. Gate 3 noted that P-1 had no answer in the owner's own words; the 224 KiB figure now has one.
- **P-R2 (the height bound and its stop):** accepted ("the tree-height … stops"). Gate 3's added consequence, a read-only open's lower bound, was stated in plain words before the answer and is part of what the stop means.
- **P-R3 (the per-mode budgets):** accepted as provisional ("the provisional per-mode budgets"). R-1-memory §12's re-measurements stand.
- **P-R5 (the growth residual):** accepted ("the … file-growth stops").
- **P-R6 (the caps 8 and 2):** accepted.
- **P-R4 (C-T0's thresholds per mode, and the earlier switch to range locks in embedded and browser mode):** **still open.** The reply does not name it. R-1-memory §7.1 and C-T0 8.8 carry the recommended values, and the no-decision default is the text as written.

K12 changes no owner point.

## Merge timing

The owner's own words of 2026-11-20 (QUEUE head) say: "The spec cards S-ETA, S-THETA, S-ZETA and R-1-SPEC may merge before P0 passes, and P0 is then judged on the merged spec." So after rework 3 and a passing re-gate, R-1-SPEC may merge before P0 PASS. This replaces gate 3's merge-timing line. K12 is worth landing before P0 reads the merged spec: it is exactly the cross-reference P0's reader would follow.

## Follow-ups (F1–F8 carried from gate 3, unchanged)

Gate 3's eight follow-ups carry as gate 3 stated them (History). I re-checked each against ecdae5d9 and the merged 7.19:
- **R-1-SPEC-F1** (S-STORE spec card, before A-7d is cut: the read-only budget's two test forms, RO-BUDGET-DIRTY-ONLY and RO-SIZED-FROM-F, on A-7d). This does not depend on the base: `t_ro_open`'s and `t_ro_open_disk_full`'s §15.1 rows are unchanged from 7.15 to 7.18.
- **R-1-SPEC-F2** (R-1 spec, before R-1's Phase B card: an exact "third growth" allowance). Unchanged.
- **R-1-SPEC-F3** (R-1 and A-7c, before A-7c is cut: step 2's copy of image `k`'s two bitmaps). Unchanged. S-THETA's freed-frame rule (no write of a freed frame by step 2) does not settle it.
- **R-1-SPEC-F4** (R-1 spec, before R-1's Phase B card: the open refuses with 53200 a directory taller than the sized pool's `H`; and gate 3's read-only item). Unchanged. S-ZETA's step-1 refusals precede it, and they do not cover it.
- **R-1-SPEC-F5** (the next S-STORE card; pointers only).
  - S-STORE §14 step 3b and §6.8's CreateTree point to R-1-memory §5.2.
  - **Now due:** S-THETA has merged, so R-1-memory's "step budget" (`t_mem_caps` (x)) should point to S-STORE §15's definition (its "the step budget is a count stated by each test, never a time").
  - The optional "(a read-only open: from its evictable part, R-1-memory §5.3)" in §5.4 and §19 T8.
- **R-1-SPEC-F6** (R-1 spec, with or before R-1's Phase B card: tests for K1, K7 and K10). Unchanged.
- **R-1-SPEC-F7** (S-STORE question; depends on P-R2): skip a tree rather than poison on a GC or drop-walk descent past `H`. The owner's "yes to … the tree-height … stops" accepts P-R2 as recommended, stop included. F7 stays a question for the next S-STORE card: whether a sweep or a walk may skip with a report, which the stop rule does not forbid.
- **R-1-SPEC-F8** (R-1 and S-STORE, with F1, before A-7d is cut: the read-only floor checked continuously, the reserve allowed to shrink mid-op, a fall-back re-sizing afresh, a third test form; RO-FLOOR-PER-OP and RO-H-SET-ONCE on A-7d). Unchanged.

Full descriptions are gate 3's and gate 1's (History). No new follow-up id: D11 is fixed by this rework, and the card-text items are listed above. F2-b's re-anchor belongs to the S-EPSILON-F2 card (its QUEUE row), not to R-1-SPEC.

## Not verified

- **No code exists.** Every test and mutant was walked as text by gates 1–3. I re-walked none: the move changes none of them.
- **Omit-one on the other 24 edit blocks and the 15 Doc blocks.** Gates 1–3 ran their own.
- **The implementer's scratch** (`~/.cache/nv3-review/R-1-SPEC-718/`: `apply719.py`, `checker37.*`, `run_acc.py`, `final/`). I read `apply719.py`'s head only, to see its date handling, and used none of its outputs. The implementer ran no `gate.sh` and wrote no report. No worker log exists for the move in `cards/logs/`.
- **Gate 1's 24-order composition** of the four cards' blocks on 7.15 was not repeated. The actual order (S-THETA, S-ETA, S-ZETA, R-1-SPEC) was checked directly on 7.18 (Scope 1).
- **The provisional constants** (`o_frame`, 64 bytes per root-table entry, `e_lock`, the 1/32 slope) are unmeasured, as gate 3 recorded. Gate 3's other unverified items carry.
- **S-ZETA's, S-ETA's and S-THETA's own checkers** were not run on 7.19. Line 37's moved and lost checks against 7.18 cover their mutants' placements.

## History

- **Gate 1: REWORK, verdict commit 7fb189020418b1700928486fd8d84d3aba4a5358, judging 32f68472b4155671f5418f0fb633b0859958c89a.** P gate, Claude Code session d36e5b00-a57c-4ef7-b43c-283e6f9e735c, GATE EXIT=0. It found defects D1–D9 and gave K1–K10 (25 entries), two new Acceptance lines and follow-ups F1–F7.
- **Rework 1: commit 98c85acadefe95d62badb88856eaff989727099e,** by a GLM 5.3 implementer. It applied K1–K10.
- **Gate 2: REWORK, verdict commit 22722c9dc481a41246bf5bad1bc7c6faad3b8943, judging 98c85aca.** P gate, Claude Code session c2c35c7c-b073-4464-9a1e-0c7e47f1942b, GATE EXIT=0. It found D10 and gave K11 (five entries), one new Acceptance line and line 39's new hash, and extended F1.
- **Rework 2: commit a8486ede087555faa2c55f86c6417c28063419d0,** by a GLM 5.3 implementer. It applied K11.
- **Gate 3: MERGE-WITH-FOLLOWUPS, verdict commit e7703ea255e9a7fbbddb3974296a8e37ba251eef, judging a8486ede.** P gate, Claude Code session df6338c5-4202-4216-a68d-447224f3dcc6, GATE EXIT=0, against S-STORE 7.15 at scaffold 71ecf3e6. Follow-up F8 new; F1–F7 carried.
- **S-THETA, S-ETA and S-ZETA merged first** (merges 6e3423f6, adbe6a16 and 1cd6d482; passes 169, 170 and 172), taking S-STORE to 7.16, 7.17 and 7.18. A-2-0 (0755a61e) and A-6a (9bbc77ab, pass 171) merged in between, without touching S-STORE, C-T0 or README.
- **The move: commit ecdae5d93c33dcde53d59ad98a282a1044b6a2fb,** by a GLM 5.3 implementer. It merges 1cd6d482 into the card branch, resolves `docs/S-STORE.md` as 7.18 with R-1-SPEC's blocks applied as draft 7.19, and updates the card in the main checkout; the card's pre-move copy is `~/.cache/nv3-review/R-1-SPEC.card-backup-pre-718.local.md`. It hit its usage limit before running `gate.sh` or reporting. The orchestrator then ran the 52 non-gate Acceptance lines at it (default locale): all pass.
- **Gate 4 (this verdict): REWORK on ecdae5d9,** for D11, with K12.

**Gate 3's verdict, as committed at e7703ea2, verbatim.** Its header lines are prefixed with "- ", so that no tool reads them as this verdict's fields. Everything after them is unchanged, headings included. That includes gate 2's verdict, which gate 3's History holds verbatim, and gate 1's verdict inside it. Where its sections differ from the ones above (merge timing, the owner points, F5's due pointer, Acceptance numbering), the sections above are current. The same text is at `git show e7703ea2:nucleus/v3/verdicts/R-1-SPEC.md`.

<!-- gate 3's verdict (e7703ea2), verbatim but for its header lines, begins -->

- Card: R-1-SPEC
- Class: spec
- Card-gate: P
- Verdict: MERGE-WITH-FOLLOWUPS
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: T3 review sub-agent for thread 45584af2-e651-490e-862c-3402320cffa3, Claude Code session df6338c5-4202-4216-a68d-447224f3dcc6 (an independent P gate 3, a narrow re-gate after rework 2 with a design re-read of K11: this agent wrote neither the card, its implementation 32f68472, rework 1 98c85aca, rework 2 a8486ede, nor gate 1's or gate 2's verdict, and is not the orchestrator)
- Date: 2026-11-20
- Gate-exit: GATE EXIT=0
- Commit: a8486ede087555faa2c55f86c6417c28063419d0
- Mutants: spec text, none in code; K11 adds no test or mutant, so gate 1's and gate 2's walks stand (History). G4 RO-SIZED-FROM-F (gate 2's) -> UNCAUGHT by t_ro_open and t_ro_open_disk_full as they stand, caught by F1's second form. Gate 3's own: G5 RO-FLOOR-PER-OP (a read-only open checks `F_e ≥ F_min` only before each replayed op, so a replayed insert whose overflow chain build takes `F_e` below `F_min` mid-op waits for an unpin) -> UNCAUGHT by t_ro_open, t_ro_open_disk_full and F1's two forms (F8); G6 RO-H-SET-ONCE (`R_EPOCH` re-set before each op but `H` kept at the open's value) -> UNCAUGHT by the same, since no form descends a tree taller than the current `H` and no taller than the open's (F8). Mechanical: K11 applied by this gate's own script to 98c85aca's files equals a8486ede's byte for byte; the card's blocks applied by this gate's own parser reproduce all four files at both dates; omitting any one K11 entry fails Acceptance in both locales (5/5 caught: K11a, K11b, K11c line 52; K11d, K11e lines 39 and 52)
- Followups: R-1-SPEC-F1, R-1-SPEC-F2, R-1-SPEC-F3, R-1-SPEC-F4, R-1-SPEC-F5, R-1-SPEC-F6, R-1-SPEC-F7, R-1-SPEC-F8

## Verdict in brief

**MERGE-WITH-FOLLOWUPS.** Rework 2 is exactly K11 and nothing else, and the card reproduces all four files byte for byte. D10 is closed. I re-derived K11's design independently, as gate 2 asked, and it holds.

- A read-only open sizes `R_EPOCH` and `H` from its evictable part `F_e`. It never draws on the reserve and never releases a dirty frame.
- R-1-memory §5.3's check and S-STORE §14.1's check now state one condition. At every instant of pass 2 and after it, `F_e ≥ F_min`, and no descent goes deeper than `H(F_e)`.
- That condition is monotone in the budget, so a larger read-only budget always serves the salvage. That is the property the full-volume path needs.
- Normal read-only serving after pass 2 cannot exhaust the pool.
- The read-write open's sizing is untouched.

**One gap, which is not blocking.** "Re-set before each op that pass 2 replays" leaves the reserve and the bound unchanged *within* an op. One replayed op can dirty pages that it releases before it ends: a split's pages, once its separators are installed, and an overflow chain's pages (S-STORE §6.7).
- K11's "`F_e` must stay at least `F_min`" and S-STORE's "both during pass 2" make the floor hold at every instant, so a conforming build fails such an op with 53200.
- With that reading, a clean frame outside the reserve always remains while `F_e ≥ F_min`. The stale reserve costs at most `⌈771 / 32⌉ = 25` frames, and the margin left is positive in every mode (below).
- A build that checks only between ops can hang, as D10 did. The text should say so outright, and a test should pin it: F8.

**Merge timing.** This card may not merge before P0 PASS unless the owner says so in their own words.

## Mechanics (all checked by this gate; nothing taken from the implementer's or gate 2's report)

- **Fresh gate.** I extracted the scaffold's `fresh-gate.sh` through the shallow `--no-local` clone. It printed the scaffold id `71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`, with no EXTRACT FAILED line. The run on `a8486ede087555faa2c55f86c6417c28063419d0` printed:
  - `fresh-gate: commit a8486ede087555faa2c55f86c6417c28063419d0`
  - `fresh-gate: base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `fresh-gate: checks passed (4876 files hashed in 4.49 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - steps test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests and api-surface, each `EXIT=0`
  - last line `GATE EXIT=0`, with exit status 0 and stderr empty. The known `undo_order.rs` flake did not occur, and nothing was re-run. The extract was removed afterwards.
- **Scope.** Over gate 2's verdict commit 22722c9d, rework 2 changes two files: `docs/R-1-memory.md` (+4/−2) and `docs/S-STORE.md` (+2/−2).
  - Over the scaffold, the branch touches the four Touch-only docs and this verdict file, and nothing in a protected path.
  - 22722c9d's docs equal 98c85aca's, and the worktree was clean.
- **Exactly K11.** My own script (`~/.cache/nv3-review/gate3-R-1-SPEC/tools/k11apply.py`) parses K11a–K11e from gate 2's verdict as committed at 22722c9d. It applies them in order to 98c85aca's files, and every Old occurs exactly once.
  - The results equal a8486ede's files byte for byte: R-1-memory `e3956d03…bc5d` and S-STORE `63c4b6e9…bc25`. These are gate 2's recorded hashes and also its scratch `k11out/` files.
- **The card's blocks.** A parser of my own (`tools/g3apply.py`), written without reading gate 1's, gate 2's or the implementer's, takes Doc blocks 0–14, E1–E25 with E12a, T1–T9 and X1. It applies them to 71ecf3e6's files (S-STORE `a1376c94…700f`, C-T0 `96caa6cb…a0c0`, the card's Base hashes), and every Old occurs exactly once at its turn.
  - With `<date>` 2026-11-17, all four results equal a8486ede's files byte for byte: R-1-memory `e3956d03…`, S-STORE `63c4b6e9…`, C-T0 `8a826331…9149` and README `191f528f…ab82`.
  - With 2026-11-16, they give the Notes' dry-run sha256s: `154b17f2…6981`, `1a0d9afd…c2ce`, `286c85d6…48a0` and `191f528f…ab82`.
- **E12a's extension: right, and needed.** K11d's Old spans the base bullet's trailing sentence ("When it would not, the open fails with 53200, having written nothing."), which the base file holds and K11d rewrites. With E12a's Old and New ending before that sentence, as they did, the card's New would leave the base sentence in place beside K11d's rewrite, and the card would not reproduce the file.
  - Extending both Old and New through it changes no meaning: the Old adds base text verbatim, and the New ends exactly in K11d's New.
  - The parser above confirms that the extended Old occurs once in 7.15 and that the result is a8486ede's file.
- **The card's prose.** I diffed the card against its backup from before rework 2 (`~/.cache/nv3-review/R-1-SPEC.card-backup-pre-rework2.local.md`). Every change is one that gate 2's "Card text the rework must update" asks for, and I found no other:
  - Finding 13 and D-13;
  - Doc blocks 4 and 5, E12a and E25;
  - the runner's count of 53;
  - rework 2's evidence;
  - Acceptance line 39's hash and the new line 52, each equal to gate 2's text;
  - "For the gate" item 12;
  - the plan card's F1, extended;
  - the Notes' dry-run hashes.
- **Acceptance.** I ran the 52 commands before the gate line, extracted with the runner's own `parse_card` pattern, in a fresh `--no-local` clone at a8486ede with `nucleus/v3-scaffold` at 71ecf3e6. Each ran as `/bin/sh -c` from the clone root.
  - All 52 pass under `en_CA.UTF-8` and all 52 under `LC_ALL=C`.
  - Line 17 prints `7 invariants; 20 rows` / `OK`, and line 37 prints `162 rows; 233 mutants; 24 judged only on Phase B cards; 47 invariants; 50 tests` / `OK`.
  - Line 53 is the fresh gate above.
  - At 22722c9d, lines 39 and 52 fail in both locales, as gate 2 recorded. The clone stayed clean.
- **Omit one K11 entry.** I built five scratch commits on a8486ede, each with K11 applied to 98c85aca's files minus one entry. Every one fails in both locales: K11a, K11b and K11c fail line 52, and K11d and K11e fail lines 39 and 52.

## K11's design, re-derived

Notation: `F_e = F − D − M`, where `D` is the resident dirty frames and `M` the MapLeafs that S-STORE §4.1's pins hold for them. `R(x) = R_EPOCH_min + ⌊(x − F_min)/32⌋` and `H(x) = ⌊(x − 2R(x))/(4n)⌋ − 2`. A read-only open evicts no dirty page and runs no checkpoint, GC, prune, drop walk or LPID free of its own (S-STORE §14.1), so `D` and `M` never fall during pass 2 and `F_e` never rises.

### What holds
- **No draw, no claim.** `write_guard` copies only content dirty against image `k` after a flip, and a read-only open has no flip. Pages loaded from image `k` sit at slots its map references, and pages pass 2 allocates carry the current epoch. So the class (Mc) of R-1-memory §4.4 is empty, and the reserve is idle frames. Sizing it from `F_e` only shrinks an idle set, which is always well-defined. Sizing it from `F` took up to `R(F) − R_EPOCH_min` frames from the replay (28,620 at the server default), which was D10.
- **Within one op, at its start's sizing.** Take the op's pins as §4.4 counts them: (T) at most `3(H + 2)` per thread, (Mp) at most `H + 2`, (Mc) zero. All of them come out of `F_e − R(F_e)`. Dirty frames the op still pins sit in (T) or (Mp), so they are not counted twice against it. `4n(H + 2) + 2R ≤ F_e` holds by the definition of `H`, so every pin the op takes before it releases anything finds a clean frame. This is §4.4's argument with (Mu) and the dirty (U) frames removed, which is exactly what `F_e` removes.
- **The two checks are one condition.** For `F_e ≥ F_min`, `F_e − 2R(F_e) = 4n(H_min + 2) + (F_e − F_min) − 2⌊(F_e − F_min)/32⌋ ≥ 4n(H_min + 2)`, so `H(F_e) ≥ H_min`. Below `F_min`, §4.5's floor fails the open. And `4n(H + 2) + 2R ≤ F_e` always holds at `H(F_e)`.
  - So "`F_e` meets §5.4's minimum at the `H` and `R_EPOCH` set from `F_e`" (S-STORE §14.1) is the same as "`F_e ≥ F_min`" (R-1-memory §5.3).
  - Both name a descent past that `H` as 53200. The S-STORE side gets its `H_min` floor through its pointer to R-1-memory, which is consistent, if terse.
- **A larger budget always serves.** `D` is the set of pages the replay mutates, which does not depend on `F`. `M ≤ D`, though which MapLeafs a fresh LPID lands under can depend on residency.
  - So `F_e(op i) ≥ F − 2D_i` grows without bound in `F`, and so does `H(F_e(op i))`.
  - Every refusal K11 adds is a 53200 that a larger read-only budget clears, with nothing written. That holds on a full volume too, where S-STORE §14.1 says the larger read-only open is the only remedy.
  - The fault D10 found in S-STORE's old check, failing at every budget once `D + M ≥ 4n`, is gone.
- **A tree taller than the new bound.** When `H` shrinks between ops, a later descent into a tree taller than it fails the open with 53200, writing nothing. That tree may be one pass 2 grew earlier, under a larger `H`.
  - No op is in flight at that point. Between replayed ops the replay thread holds no pin or latch (S-STORE §5.3, latch scope), so "re-set before each op" acts at a quiescent point.
  - After pass 2, `H` is fixed, and a client read of such a tree gets 53200 without poisoning anything, as for any tree written under a larger pool (R-1-memory §4.5; owner point P-R2). F4 should say whether the open reports such trees.
- **After pass 2, normal read-only serving cannot exhaust the pool.** Nothing dirties a page any more: no write transactions, prunes or GC. So (Mp) and (Mc) are empty, and every (U) frame is clean.
  - `O = F_e − R − |T| ≥ 4n(H + 2) + R − 3n(H + 2) > Σ Q_t`.
  - The final `H` and `R` are those of the last re-set, which is the final `F_e`.
- **The read-write open is untouched.** K11a and K11b only add read-only sentences. §4.3's and §4.5's formulas, `F_min` and §4.6's table are unchanged, and the Acceptance lines that pin them pass. A read-write open evicts dirty pages, so (Mu) is obtainable there and §4.4's proof stands.
- **Stale restatements.** I grepped R-1-memory, S-STORE, C-T0 and README for the old wordings and found none: "exceed `F_min` by both", "`F` minus the dirty frames)", and §14.1's base sentence.
  - The remaining "sets from its pool" pointers all point to R-1-memory §4.5, which now carries the read-only exception: S-STORE §5.4, R26, §19 T8, and the 7.16 changelog's height bullet. They read true, since `F_e` is part of the pool.
  - R-1-memory §3's read-only row and §4.1's "must fit beside the minimum" agree with K11. C-T0 and README say nothing about it.

### The gap: an op's own dirt, mid-op (F8)
S-STORE §6.7 has a replayed insert build its overflow chain after it releases `P` and every other ancestor, "once any split's separators [are] installed", holding only the leaf. A replayed replace (I-CHUNK-SUPERSEDE) also frees the old chain in the same op.
- In a read-write open, those released pages stay obtainable by eviction. In a read-only open they become frames of `D` and `M` mid-op.
- Per entry, `ROW_LIMIT` (1 MiB, C-T6-rehost §4.3) bounds the chain, at about 257 pages at 4 KiB. So one op adds at most `max(4h + 2 + 2 × 257, 3 × 257) = 771` frames at 4 KiB for any tree that can exist, `h ≤ 35`, and 400 at 8 KiB.

Two things follow:
1. **The floor must be checked at every instant, not per op.** An op that starts at `F_e = F_min + x` and adds more than `x` takes `F_e` below `F_min` mid-op.
   - K11c says `F_e` "must stay at least `F_min`, or the open fails with 53200", and S-STORE says "both during pass 2 and after it". So a conforming build fails with 53200 when a frame acquisition or a dirtying would take `F_e` below `F_min`.
   - A build that checks only "before each op", which a hurried reading of K11c's "at open and again before each op" invites, waits there for an unpin that never comes. That is D10's failure, and G5 is that build.
2. **With the floor checked continuously, nothing hangs.** Mid-op, `R_EPOCH` is the op's start value, stale by at most `⌈771/32⌉ = 25` frames at 4 KiB and 13 at 8 KiB.
   - While `F_e ≥ F_min`, the replay thread's clean frames outside the reserve number at least `F_min − R_EPOCH_min − 25 − 2`. That is 4 at `n = 1` and 33, 207 and 555 at the browser, embedded and server `n` (4 KiB). At 8 KiB it is 16, 45, 219 and 567.
   - The 2 frames subtracted are a frame just obtained and a MapLeaf just read in place, neither yet dirty. Other threads' pins in a read-only pass 2 are transient, since no session exists before pass 2 ends (R-1-memory §5.3), so a wait on them ends.
   - So the margin is positive, but it rests on `ROW_LIMIT` and on the tree heights that can exist. F8 removes the dependence: re-set `R_EPOCH` whenever `F_e` falls, which is safe because it only shrinks an idle set, or hold `R_EPOCH_min`.

A further nit for F8: "since `F_e` only falls" is true within one pass 2. A read-only salvage fall-back drops image `k`'s frames unwritten, so `F_e` rises, and R-1-memory §5.3's fall-back bullet sizes the open again. The re-set there is an assignment from the new `F_e`, not a minimum over the open, or the salvage from meta `k−1` would keep image `k`'s lower bound.

### Worked numbers (8 KiB pages, `s = 1`, `H_min = 5`)

| Case | `F` | `D + M` | At open: `R_EPOCH`, `H` | At pass 2's end: `F_e`, `R_EPOCH`, `H` | Sized from `F` (D10), at the end |
|---|---|---|---|---|---|
| Browser minimum (`n` 2, `F_min` 64) | 64 | 0 | 4, 5 | 64, 4, 5 | the same |
| Browser minimum | 64 | ≥ 1 | — | 53200, nothing written; needs `F = 64 + D + M` | the same |
| Browser, read-only budget for a 200-frame replay | 264 | 200 | 10, 28 | 64, 4, 5 | 54 frames outside the reserve; one thread at `H` 28 may need 120 |
| Embedded minimum (`n` 8, `F_min` 244) | 244 | 0 / ≥ 1 | 10, 5 | 244, 10, 5 / 53200 | the same |
| Embedded, read-only budget for a 1,000-frame replay | 1,244 | 1,000 | 41, 34 | 244, 10, 5 | 203 outside the reserve; one thread at `H` 34 may need 144 |
| Embedded default, full-volume replay at the limit | 5,878 | 5,634 | 186, 170 | 244, 10, 5 | 58 outside the reserve, against 60 for one write into a 13-level tree (gate 2) |
| Embedded default, 3,000 dirty pages packed (6 MapLeafs) | 5,878 | 3,006 | 186, 170 | 2,872, 92, 82 | — |
| Embedded default, 3,000 dirty pages scattered | 5,878 | 6,000 | 186, 170 | 53200 when `F_e` passes 244; needs `F` 6,244, about 2.9 MiB more | — |
| Server minimum (`n` 20, `F_min` 604) | 604 | 0 / ≥ 1 | 22, 5 | 604, 22, 5 / 53200 | the same |
| Server default, full-volume replay at the limit | 916,452 | 915,848 | 28,642, 10,737 | 604, 22, 5 | none outside the reserve (28,642 > 604) |

At each mode's minimum, K11 and D10's sizing agree: `F = F_min` gives `R(F) = R_EPOCH_min`, and a read-only open there serves only a replay that dirties nothing. That is the documented cost: a read-only open needs `F_min + D + M`, up to `F_min + 2D` with scattered LPIDs. K11 matters above the minimum, and most of all on the full-volume salvage path, where the open is sized for the replay.

## Rulings on the owner points (the gate's part)
Gate 1's and gate 2's rulings stand (History). K11 adds no cost: a read-only open still needs `F_min` plus its dirty frames plus their MapLeafs, as before. I add one plain-words consequence to P-R2 below, because it is owner-visible; it changes no recommendation.

## Owner points, in plain words (stated here, not decided)
- **P-1 and P-R1, memory per connection.** Each connection that can hold pages needs room for four tree paths of pages: about 224 KiB at 8 KiB pages and a height floor of 5.
  - P-1, the earlier figure (168 KiB, three paths), has no answer in the owner's own words. The plan records only a conditional reply whose condition is not shown to be met, and the 16:0x delegation did not cover it.
  - At 8 embedded connections, the floor is about 2 MiB of pages, and the total minimum is about 8.3 MiB for a 1 GiB file.
- **P-R2, the height bound.** The memory budget decides how tall an index tree may grow. A tree that would grow past it stops the database. The database is reopened with a larger budget, and nothing committed is lost. Near the minimum budget the bound is 5 levels, and three things follow:
  - long, prefix-sharing keys can reach it within a few hundred to a few thousand rows;
  - a database reopened with less memory than it was written under answers "out of memory" when a tall tree is read;
  - the first write, recovery replay, cleanup pass or drop of such a tree stops it.

  The alternative, letting fewer connections in as trees grow, never stops the database but costs a shared counter on every storage call.

  **Added by gate 3:** a read-only open, the salvage path, sets the bound from the memory left after its recovery replay, so the bound can be lower than a normal open's at the same budget. A tree that the replay grew can then answer "out of memory" when read, until the database is opened read-only with more memory.
- **P-R3 and P-R4, budgets and thresholds per mode.**
  - Budgets: server 25% of memory (at least 256 MiB), embedded 64 MiB, browser 32 MiB, a quarter of each for sessions.
  - Per-transaction caps as R-1-memory §7.1 lists them.
  - At small budgets the database lowers any cap above the session pool to fit it, and reports that. So a small database fails a very large transaction sooner, with "out of memory", instead of refusing to open.
  - Embedded and browser databases switch big transactions to coarse locks sooner.
- **P-R5, the growth residual.** The database stops if sessions hold all session memory while the file's bookkeeping outgrows its reserve. That happens at the third file growth in a row at the latest, and sooner after heavy rewrites or on files above about 127 GiB. Nothing committed is lost, and a reopen with more memory clears it.
- **P-R6, the connection cap.** An embedded app may open 8 connections at once by default, and a browser app 2. One more gets "too many connections". The cap is what keeps the memory floor true, and the app can raise it.

## Merge timing
This card may not merge before P0 PASS unless the owner says so in their own words.

## Follow-ups (F1–F7 carried from gate 2; F8 new)
- **R-1-SPEC-F1 (S-STORE spec card, before A-7d is cut).** The read-only budget gets a test and mutants.
  - `t_ro_open`, or `t_ro_open_disk_full`, gains a form whose replay dirties pages under MapLeafs of their own. A pool at `F_min` plus the dirty frames plus those MapLeafs opens, and one frame below fails with 53200 and writes nothing. Mutant **RO-BUDGET-DIRTY-ONLY** (the budget counts the dirty frames only): the replay waits for an unpin within the step budget, or the open succeeds at a budget the rule refuses.
  - Gate 2 adds a second form, run at a pool well above `F_min` (at least `F_min + 64`). Its replay drives `F_e` to `F_min` and then replays a write into a tree as tall as `F_e`'s `H` allows. With K11 it completes, and one more dirty page fails it with 53200. Mutant **RO-SIZED-FROM-F** (G4: `R_EPOCH` and `H` from `F`): the replay waits for an unpin within the step budget.
  - Judged on A-7d.
- **R-1-SPEC-F2 (R-1 spec, before R-1's Phase B card).** Size the growth allowance so that "the third growth" is exact:
  - size `R_file`'s LPID bitmap and MapInner terms for `max(next_lpid, slot_count)` LPIDs, plus a bound for the pool's never-written pages;
  - make the allowance's MapInner term follow each step's LPIDs;
  - recompute §4.6 and §7.2.
- **R-1-SPEC-F3 (R-1 and A-7c, before A-7c is cut).** Either reserve S-STORE §13 step 2's copy of image `k`'s two bitmaps in `R_file`, if step 2 makes one, or state how step 2 avoids it.
- **R-1-SPEC-F4 (R-1 spec, before R-1's Phase B card).** The open refuses with 53200, writing nothing, when the directory step 2 descended is taller than the sized pool's `H`. With K11, for a read-only open that is the `H` from `F_e`, the same as from `F` at open, since nothing is dirty yet. Gate 3 adds one item: state whether a read-only open whose pass 2 grew a tree taller than its final `H` reports that tree at open, or only answers 53200 when it is read.
- **R-1-SPEC-F5 (the next S-STORE card; pointers only).**
  - S-STORE §14 step 3b and §6.8's CreateTree point to R-1-memory §5.2.
  - After S-THETA merges, R-1-memory's "step budget" points to S-STORE §15.
  - Gate 3 adds, optionally: S-STORE §5.4's and §19 T8's "sets from its pool" may add "(a read-only open: from its evictable part, R-1-memory §5.3)".
- **R-1-SPEC-F6 (R-1 spec, with or before R-1's Phase B card).** Tests for the corrections that have none:
  - `t_mem_open_sizes`' report shows every cap at most `S` at `B_min` (K1);
  - `t_mem_heap_accounted` samples after an open whose replay reads a `wal_seg_size / 4` record, and after amcheck (K7);
  - `t_mem_account_returns` gains an aborted CREATE TABLE and a `ROLLBACK TO` past one (K10).
- **R-1-SPEC-F7 (S-STORE question; depends on P-R2).** A GC sweep or drop-walk step whose descent would pass `H` could skip the tree with a report rather than poison the store.
- **R-1-SPEC-F8 (R-1 and S-STORE, with F1, before A-7d is cut).** State outright what K11 implies for a read-only pass 2, and test it.
  - **(a) The floor is checked continuously.** A frame acquisition, or a dirtying or §4.1 pin, that would take `F_e` below `F_min` fails the open with 53200 at once, writing nothing. It never waits for an unpin, since in a read-only open nothing but the replay's own call ends would free a frame. "At open and again before each op" governs only the re-set of `R_EPOCH` and `H`.
  - **(b) The reserve may shrink mid-op.** `R_EPOCH` is re-set whenever `F_e` falls, or held at `R_EPOCH_min`, since a read-only open never draws or claims. That removes the margin's dependence on `ROW_LIMIT` and on the heights a tree can reach (see "The gap").
  - **(c) A fall-back re-sizes from scratch.** A read-only salvage fall-back sizes `R_EPOCH` and `H` afresh from the `F_e` left after it drops image `k`'s frames, as an assignment.
  - **(d) A third test form.** `t_ro_open` gains a form whose replay includes an insert that builds an overflow chain at `ROW_LIMIT`, starting at `F_e` just above `F_min`, so that the chain takes `F_e` below `F_min` mid-op: the open fails with 53200, writes nothing, and the pool's wait-for-unpin hook never fires. A form with a tree taller than the current `H` and no taller than the open's: the descent fails with 53200.
  - Mutants **RO-FLOOR-PER-OP** (G5): the replay waits for an unpin. **RO-H-SET-ONCE** (G6): the descent proceeds.
  - Judged on A-7d.

Full descriptions of F2–F7 are gate 1's (History, "Follow-ups"), unchanged except as stated above.

## Not verified
- No code exists. Every test and mutant was walked as text.
- The provisional constants are unmeasured: `o_frame`, 64 bytes per root-table entry, `e_lock`, and the 1/32 slope. The worked numbers use §4.6's values.
- The mid-op margin above rests on two assumptions from other specs, which I did not re-derive:
  - `ROW_LIMIT` = 1 MiB bounds every chain a replayed op builds or frees (C-T6-rehost §4.3; S-STORE §6.7);
  - a read-only pass 2 has no thread but the replay that holds pins past its own call. R-1-memory §5.3 says no session exists before pass 2 ends; S-STORE §14.1 runs no checkpoint, GC or drop walk.
- I did not settle whether S-STORE §14.1's "no LPID free" is meant to exclude the chain free of a replayed supersede (§14 step 4, §6.7). It counts the same way here, as dirtied MapLeafs, so the analysis covers it either way. The wording question predates this card.
- That S-2b will state SSI's spill as v7.8 §8.5 promises (carried from gate 1).
- The implementer's log: none exists for R-1-SPEC in `cards/logs/`. Rework 2's reported results were checked against the commit and the card, not a log.

## History

- **Gate 1: REWORK, verdict commit 7fb189020418b1700928486fd8d84d3aba4a5358, judging 32f68472b4155671f5418f0fb633b0859958c89a.** P gate, Claude Code session d36e5b00-a57c-4ef7-b43c-283e6f9e735c, GATE EXIT=0. It found defects D1–D9 and gave K1–K10 (25 entries), two new Acceptance lines and follow-ups F1–F7.
- **Rework 1: commit 98c85acadefe95d62badb88856eaff989727099e,** by a GLM 5.3 implementer. It applied K1–K10.
- **Gate 2: REWORK, verdict commit 22722c9dc481a41246bf5bad1bc7c6faad3b8943, judging 98c85aca.** P gate, Claude Code session c2c35c7c-b073-4464-9a1e-0c7e47f1942b, GATE EXIT=0. It found D10 and gave K11 (five entries), one new Acceptance line and line 39's new hash, and extended F1. It asked that a gate 3 re-read K11's design.
- **Rework 2: commit a8486ede087555faa2c55f86c6417c28063419d0,** by a GLM 5.3 implementer. It applied K11 to `docs/R-1-memory.md` and `docs/S-STORE.md` and updated the card in the main checkout (cards are local), with its pre-rework copy at `~/.cache/nv3-review/R-1-SPEC.card-backup-pre-rework2.local.md`.
- **Gate 3 (this verdict): MERGE-WITH-FOLLOWUPS on a8486ede.**

**Gate 2's verdict, as committed at 22722c9d, verbatim.** Its header lines are prefixed with "- ", so that no tool reads them as this verdict's fields. Everything after them is unchanged, headings included, and that includes gate 1's verdict, which gate 2's History holds verbatim with its own header lines prefixed. Where its sections differ from the ones above (owner points, follow-ups F4, F5 and F8, Acceptance numbering), the sections above are current. The same text is at `git show 22722c9d:nucleus/v3/verdicts/R-1-SPEC.md`.

<!-- gate 2's verdict (22722c9d), verbatim but for its header lines, begins -->

- Card: R-1-SPEC
- Class: spec
- Card-gate: P
- Verdict: REWORK
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: T3 review sub-agent, Claude Code session c2c35c7c-b073-4464-9a1e-0c7e47f1942b (an independent P gate 2, a narrow re-gate after rework 1: this agent wrote neither the card, its implementation 32f68472, rework 1 98c85aca, nor gate 1's verdict 7fb18902, and is not the orchestrator)
- Date: 2026-11-20
- Gate-exit: GATE EXIT=0
- Commit: 98c85acadefe95d62badb88856eaff989727099e
- Mutants: spec text, none in code; K1–K10 add no test or mutant, so gate 1's walk stands (see History), with HEIGHT-SPLIT-UNCHECKED now stated as K6 has it -> t_height_bound (a) caught by the root-split hook, the failing insert a later one at its descent. Gate 2's own: G4 RO-SIZED-FROM-F (a read-only open sizes `R_EPOCH` and `H` from `F`, as committed) -> UNCAUGHT by t_ro_open and t_ro_open_disk_full (D10; K11, F1). Mechanical: omitting any one of the ten K groups from the corrected files fails Acceptance in both locales (10/10 caught); omitting one K entry alone is caught for 18 of 25, the 7 uncaught (K2a, K2b, K7a, K7b, K7f, and K10a and K10b each alone; the pair is caught) all R-1-memory prose that no line pins, which Limit 1 allows; with K11 applied, omitting any one of its five entries fails line 52 in both locales (5/5 caught)
- Followups: R-1-SPEC-F1, R-1-SPEC-F2, R-1-SPEC-F3, R-1-SPEC-F4, R-1-SPEC-F5, R-1-SPEC-F6, R-1-SPEC-F7

## Verdict in brief

**REWORK (rework 2, one defect, five exact entries).** Rework 1 is exactly K1–K10, nothing else, and the card reproduces all four files byte for byte. D1, D3 and D4 are closed, as are D5–D9. The owner's memory decision is now worded accurately. **D2 is not closed.** K2 added the pinned MapLeafs to the read-only budget, but in a read-only open the epoch reserve `R_EPOCH` and the height bound `H` are still sized from the whole pool `F`, and both grow with frames the replay holds and never releases.

The two specs now state two different checks, and neither is safe:
- **R-1-memory §5.3** checks the evictable part `F_e` (`F` minus the dirty frames and those MapLeafs) against `F_min`. That bound was built with `R_EPOCH_min` and `H_min`, but the open sets aside `R_EPOCH(F)` frames and lets descents reach `H(F)`.
  - At §4.6's server default (`F` 916,452, `R_EPOCH` 28,642, `F_min` 604), the check admits `F_e` down to 604. Once `F_e` drops below about 28,670, no frame outside the reserve is left for the replay.
  - At the embedded default (`F` 5,878, `R_EPOCH` 186), `F_e = 244` leaves 58 frames outside the reserve. One replayed write into a 13-level tree, which `H(F) = 170` allows, can need 60.
  - A read-only open evicts no dirty page and runs no checkpoint, so nothing ever frees a frame. The replay waits for an unpin that never comes instead of returning 53200, which is D2's failure exactly, on the full-volume salvage path.
- **S-STORE §14.1** (E12a) checks `F_e` against "§5.4's minimum", which is computed at the open's `H` and `R_EPOCH`. §4.5 makes `H` the largest height whose minimum fits `F`, so that minimum lies within `4n` frames of `F`. A read-only replay whose dirty frames and MapLeafs reach `4n` (80 on the server) then fails with 53200 at every budget. "A read-only open with a larger pool budget" stops being a remedy, and the salvage of a full volume is stranded.

K11 fixes this by sizing a read-only open from its evictable part: `R_EPOCH` and `H` come from `F_e`, re-set before each replayed op (`F_e` only falls), and fixed when pass 2 ends. A descent past that `H`, or `F_e < F_min`, fails the open with 53200 and writes nothing. That makes R-1-memory §5.3's `F_min` check and S-STORE §14.1's "§5.4's minimum" the same condition. I tried K11 in scratch: with it, 52 of 52 non-gate Acceptance lines pass in both locales; without it, at 98c85aca, lines 39 and 52 fail.

## Mechanics (all checked by this gate; nothing taken from the implementer's or gate 1's report)

- **Fresh gate.** I extracted the scaffold's `fresh-gate.sh` through the shallow `--no-local` clone. The scaffold id printed was `71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`, with no EXTRACT FAILED line. The run, on `98c85acadefe95d62badb88856eaff989727099e`, printed:
  - `fresh-gate: commit 98c85acadefe95d62badb88856eaff989727099e`
  - `fresh-gate: base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `fresh-gate: checks passed (4876 files hashed in 3.64 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - steps test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests and api-surface, each `EXIT=0`
  - last line `GATE EXIT=0`, with exit status 0 and stderr empty. The known `undo_order.rs` flake did not occur, and nothing was re-run. The extract was removed afterwards.
- **Scope.** Over the verdict commit 7fb18902, rework 1 changes two files: `docs/R-1-memory.md` (+21/−19) and `docs/S-STORE.md` (+5/−5). C-T0 and README are 32f68472's. Over the scaffold the branch touches the four Touch-only docs and this verdict file, and nothing in a protected path. The worktree was clean.
- **Exactly K1–K10.** My own script (`~/.cache/nv3-review/gate2-R-1-SPEC/tools/kapply.py`) reads the 25 entries from gate 1's verdict and applies them, in its order, to 32f68472's files. Every Old occurs exactly once at its turn. The results equal 98c85aca's files byte for byte: R-1-memory `2eb26225…5d66` and S-STORE `07b9b040…7e34`. These are also the sha256s of gate 1's scratch `corrected/` files.
- **The card's blocks.** A second script of mine (`tools/cardapply.py`), written independently of gate 1's and the implementer's, parses the reworked card's Doc blocks 0–14, E1–E25 with E12a, T1–T9 and X1, and applies them to 71ecf3e6's files. Every Old occurs exactly once at its turn.
  - With `<date>` 2026-11-17, all four results equal 98c85aca's files byte for byte: R-1-memory `2eb26225…`, S-STORE `07b9b040…`, C-T0 `8a826331…9149` and README `191f528f…ab82`.
  - With 2026-11-16, they give the Notes' dry-run sha256s: `3d8e5b40…32b7`, `286c85d6…a0c0`, `191f528f…ab82` and `07a80689…286a`.
  - 48e52d3b's three base docs equal 71ecf3e6's. S-STORE is `a1376c94…700f` and C-T0 `96caa6cb…a0c0`, the card's Base hashes.
- **The card's prose against gate 1's lists.** I diffed the card against its pre-rework backup (`~/.cache/nv3-review/R-1-SPEC.card-backup-pre-rework1.local.md`). Every change is one that gate 1's "Card text the rework must update" names:
  - Sources, item 22: P-1 is no longer "accepted".
  - Finding 12 and D-12.
  - The blocks and E12a; Work 2 and 3.
  - HEIGHT-SPLIT-UNCHECKED's row.
  - The runner's count of 52, and rework 1's evidence.
  - Acceptance 38, 39 and the two new lines, each equal to gate 1's text.
  - "For the gate" item 11.
  - "For the plan card" item 11, carrying F1–F7.
  - P-R2's and P-R5's plain words.
  - The dry-run hashes.
  - (c)(4)'s "2.4–2.7 times".
  - The S-ZETA/S-ETA/S-THETA Note.

  I found no other change.
- **Acceptance.** I ran the 51 commands before the gate line in a fresh `--no-local` clone at 98c85aca, with `nucleus/v3-scaffold` at 71ecf3e6, as `/bin/sh -c` from the clone root. All 51 pass under `en_CA.UTF-8` and all 51 under `LC_ALL=C`. Line 17 prints `7 invariants; 20 rows` / `OK`, and line 37 prints `162 rows; 233 mutants; 24 judged only on Phase B cards; 47 invariants; 50 tests` / `OK`. Line 52 is the fresh gate above. The clone stayed clean.
- **Omit one K group.** I built ten scratch commits on 98c85aca, each with the corrected files minus one K group. Every one fails in both locales:

  | Group omitted | Lines that fail |
  |---|---|
  | K1, K3, K5, K7, K8 | 50, 51 |
  | K2 | 38, 39, 50, 51 |
  | K4, K10 | 50 |
  | K6 | 39, 51 |
  | K9 | 39, 50, 51 |

  The card's evidence says K6 fails line 51. It fails 39 too: incomplete, not wrong. Omitting single entries: see the Mutants line.
- **The owner's word.** The card's Sources now say that P-1 rests on "the conditional reply, whose condition 'is not shown to be met' for this point, and the 16:0x delegation did not cover it". PLAN-v3 says the same, at §9 A's card PL-14 pointer ("P-1 before A-3b … still rest on the owner's conditional reply, whose condition is not shown to be met") and at §10's "Not covered by the exchange" ("O-3, O-2 and P-1"). No line of the card or the docs now calls P-1 accepted.
  - The docs say "owner point P-1's figure".
  - P-R1 says "than owner point P-1 said".
  - P-R1 to P-R6 stay "Recommended".

## The corrected accounting, re-read

- **D1, closed.** At `B = B_min`, §2.2's `B − R_fixed − R_file − F_min(page_size + o_frame)` is `max(S_min, M)`. So `S = max(S_min, M)` and `F = F_min` exactly, as §7.3 now says.
  - With §7.1's defaults, `S_min = preapply_bytes + 1 MiB` in every mode, so `S_min < preapply_bytes` is never a default's 22023.
  - The lowering gives these values:
    - embedded: `ws_mem` 16 → 5 MiB, `lock_mem` 16 → 5 MiB with `lock_escalate_rows` 100,000 → 40,960, `op_mem` and `cat_mem` 8 → 5 MiB;
    - browser: `ws_mem` 4 → 2 MiB, with `lock_mem`, `op_mem` and `cat_mem` already at most 2 MiB;
    - server: `ws_mem` 64 → 5 MiB, `lock_mem` 128 → 5 MiB with 1,000,000 → 40,960 rows, `op_mem` 16 → 5 MiB and `cat_mem` 64 → 5 MiB.
  - Every lowered `ws_mem` stays ≥ `preapply_bytes` (4, 4 and 1 MiB), so C-T0 §2.1's constraint holds. `n_c × sess_floor` (8 × 16 KiB, 2 × 16 KiB, 16 × 64 KiB) fits every `S_min`. Every mode's defaults open at their own `B_min`, and `t_mem_open_sizes` passes there.
  - One nit, not a defect: the 22023 check compares the option values before the open lowers `preapply_bytes` to `wal_seg_size / 4` (§6.1), so it can refuse a user's `S_min` that the lowered value would allow. It refuses before any write, so nothing is lost.
- **D2, not closed: D10 below.** K2's MapLeaf term is right. In a read-only open no (Mu) MapLeaf is obtainable, and K2a–K2f state that in all six places without counting anything twice. A dirty MapLeaf that §4.1 also pins is counted in both terms, which only over-requires. The reserve and the height bound are not addressed.
- **D3, closed.** C-T0 v8 §8 keeps v7.8 §8 "with §0.2" and leaves the text to S-2b. R-1-memory §3's row and §6.2 now defer to that spill under pressure on `S` and keep only `ssi_mem`'s per-transaction cap, after v7.8's escalation. "This file does not replace the spill with a 53200" matches R-1-memory §0's precedence rule. Whether S-2b states the spill stays unverified, as gate 1 said.
- **D4, closed.** Each of the four accounts is bounded and charged where it allocates:
  - The walk's record buffer is inside `M`, charged per allocation (M1), at most `wal_seg_size / 4` or a reader's fixed buffer. `S ≥ M` and `B_min`'s `max(S_min, M)` count it.
  - A statement's working memory is in its statement account, nested in `S`. A value or row that does not fit gets 53200, and what goes to the wire streams.
  - amcheck's sets are `⌈next_lpid / 8⌉ + ⌈slot_count / 8⌉` bytes, charged to `S` at open, before any session, or to the caller's session. A shortfall is 53200 on a read path, never a poison.
  - C-T0 §9.4's per-tree estimate and count ride on the 64-byte root-table entry, which §12 measures (provisional).
- **D5–D9, closed.** K4a credits `R_file`'s meta-`k` terms and recharges from meta `k−1` at the file's length then. K4b's no-growth condition makes `B′` exact on a correct build. K5's `B_min + 32 × (page_size + o_frame)` gives `σB` ≈ 2.1 MiB < `S_min`, so `S = S_min` and `F = F_min + 32`. K6's failing insert is the next one, at its descent. K10's credit at a transaction's end bounds the unapplied CreateTree leak. K8 states the residual's real triggers, and K9 the poison.
- **Stale restatements.** I grepped all three docs for the old wordings (the 7.15 "Pool budget" text, "exceed `F_min` by them", "three file growths", "replays no chunked transaction", "`B_min + 4 MiB`" and "no insert fails"), and none remains. S-STORE §5.4's "which an open sets from its pool" still reads true after K11, since the evictable part is part of the pool. C-T0 is unchanged and restates nothing K changes.

## D10 (REWORK): a read-only open is sized from all of `F`

- **Where.** R-1-memory §4.3 sizes `R_EPOCH = R_EPOCH_min + ⌊(F − F_min)/32⌋` for "the open", and §4.5 sets `H` "from its pool". Neither excepts a read-only open.
  - S-STORE §5.3's premise and §5.6 keep the reserve for draws and claims, so a frame acquisition takes no reserve frame. A read-only open runs no checkpoint, so it never draws or claims.
  - R-1-memory §5.3 then checks `F_e ≥ F_min`, and S-STORE §14.1 checks `F_e ≥` "§5.4's minimum". That minimum is `4n(H + 2) + 2R_EPOCH`, at the open's `H` and `R_EPOCH`.
- **R-1's check is insufficient.** At `F_e = F_min` the frames outside the reserve number `F_min − R_EPOCH(F)`. That is fewer than §4.4's classes need once `F ≥ F_min + 32`, and negative at the server default. And `H(F)` lets the replay descend trees taller than `F_e` can serve.
  - Embedded default: a replayed write into a 13-level tree can need `4 × 15 = 60` frames, against 58.
  - Nothing frees a frame: no dirty eviction, no unpin, the replay alone. The open hangs and never returns 53200.
- **S-STORE's check is unmeetable.** `H` is the largest height whose minimum fits `F`, so the minimum is above `F − 4n`. At the server default it is 916,404 of 916,452.
  - `F_e ≥` that minimum needs fewer than `4n` dirty frames and MapLeafs, at any budget.
  - So `t_ro_open_disk_full` passes only on a replay that small, and §14.1's remedy, "a read-only open with a larger pool budget", never works.
- **Class.** The floor is insufficient (a wait for an unpin that never comes) under one text, and the store's salvage path is stranded under the other. This is D2's own failure mode, and the scope makes it blocking.
- **What it is not.**
  - It is not new in rework 1. The divergence is in 32f68472's text: "exceed `F_min` by them" against "§5.4's minimum", once E5 made `max tree height` the open's `H`. K2 restated both checks without reconciling them.
  - It does not touch a read-write open, which evicts dirty pages and draws on its reserve: §4.4's proof holds there.

## Corrections (one complete list; each Old occurs exactly once in 98c85aca's file named, at its turn)

In the card these become Doc block 4's (K11a, K11b) and Doc block 5's (K11c) text, E12a's New (K11d) and E25's New (K11e). Applied in this order to 98c85aca's files in scratch, they give `docs/R-1-memory.md` sha256 `e3956d031df63032e3e8a39b6bc573fb524a914aaab6d01f71e1b3c95dabbc5d` and `docs/S-STORE.md` sha256 `63c4b6e9d58d7e25f469348c9eee67a878f208479af2723856c64fc03824bc25`, at the date 2026-11-17, which K11 does not touch. Applied to the card's 2026-11-16 dry run, they give `154b17f2503929106ea62802f15e44415c8ad2b7e3d1388780f9affc2e6f6981` and `1a0d9afd5bb8d913333fd07c83d0ac5a3cd38da9b7cf84d380d9863e0bcec2ce`. C-T0 and README do not change. No block of S-ZETA, S-ETA or S-THETA contains "Pool budget", K11d's or K11e's Old, or §4.3's or §4.5's sentence that K11a and K11b anchor on (searched by grep in the three cards), so the 24 merge orders gate 1 composed still apply with K11 added.


**K11a.** `docs/R-1-memory.md` — R-1-memory §4.3, "Above the minimum" (Doc block 4).

Old:
```text
which never lowers `H` below `H_min` (§4.5). The slope `1/32` is provisional; A-7e measures it (§12).
```
New:
```text
which never lowers `H` below `H_min` (§4.5). The slope `1/32` is provisional; A-7e measures it (§12). A read-only open sizes it with its evictable part in place of `F` (§5.3): it runs no checkpoint (S-STORE §14.1), so it draws and claims nothing, and a reserve sized for `F` would only take frames from its replay.
```

**K11b.** `docs/R-1-memory.md` — R-1-memory §4.5, "Its value" (Doc block 4).

Old:
```text
  `H = ⌊(F − 2 × R_EPOCH) / (4 × n)⌋ − 2`

- **Its floor.**
```
New:
```text
  `H = ⌊(F − 2 × R_EPOCH) / (4 × n)⌋ − 2`

  A read-only open sets it with its evictable part in place of `F`, and with `R_EPOCH` sized the same way (§4.3, §5.3).

- **Its floor.**
```

**K11c.** `docs/R-1-memory.md` — R-1-memory §5.3, "Pass 2 runs in the pool" (Doc block 5; K2c's New).

Old:
```text
So its `F` must exceed `F_min` by both, or it fails with 53200 and writes nothing (S-STORE §14.1; `t_ro_open`); with scattered LPIDs that is up to twice the dirty frames.
```
New:
```text
What is left, its evictable part `F_e` (`F` minus both), stands for `F` in its sizing: it sets `R_EPOCH` (§4.3) and `H` (§4.5) from `F_e`, at open and again before each op that pass 2 replays, since `F_e` only falls as pass 2 dirties pages, and keeps the last values once pass 2 ends, when nothing dirties a page any more; a descent past the current `H` fails the open with 53200. So `F_e` must stay at least `F_min`, or the open fails with 53200 and writes nothing (S-STORE §14.1; `t_ro_open`); with scattered LPIDs the two together are up to twice the dirty frames. Sized from `F`, the open would be wrong either way: a reserve and a height bound that grow with frames the replay holds would leave a pool whose `F_e` is `F_min` with `F_min − R_EPOCH` frames outside the reserve, fewer than §4.4 counts once `F ≥ F_min + 32` and none at §4.6's server default, so the replay could wait for an unpin that never comes; and §5.4's minimum at `F`'s own `H` lies within `4n` frames of `F`, so a replay whose dirty frames and MapLeafs reach `4n` would fail at every budget.
```

**K11d.** `docs/S-STORE.md` — S-STORE §14.1, "Pool budget" (E12a's New).

Old:
```text
must meet §5.4's minimum, both during pass 2 and after it. When it would not, the open fails with 53200, having written nothing.
```
New:
```text
must meet §5.4's minimum, both during pass 2 and after it, with the height bound `H` and the reserve `R_EPOCH` that R-1-memory §5.3 sets from that evictable part rather than from `F`: sized from `F`, they would grow with frames the replay holds. When it would not, or when a descent would pass that `H`, the open fails with 53200, having written nothing.
```

**K11e.** `docs/S-STORE.md` — S-STORE Changelog, 7.16's R_MAP bullet (E25's New; K2f's text).

Old:
```text
but a read-only open, which evicts no dirty page, counts such MapLeafs with its dirty frames (§14.1);
```
New:
```text
but a read-only open, which evicts no dirty page, counts such MapLeafs with its dirty frames and sets its height bound and `R_EPOCH` from the evictable part that is left (§14.1);
```


## Acceptance-line changes (card numbering; tried in scratch)

- **Line 38** stays `'37 18'`: K11d and K11e rewrite lines that the diff already holds.
- **Line 39** keeps its command, and its hash becomes `25151f2bc932b13c320a174c24105c054ae7cb291f0a3243cd21225a43653e30`.
- **One new line**, inserted after line 51, so the gate line becomes line 53. It contains no backtick.
```text
cd nucleus/v3 && grep -q 'What is left, its evictable part .F_e. (.F. minus both), stands for .F. in its sizing' docs/R-1-memory.md && grep -q 'A read-only open sizes it with its evictable part in place of .F. (§5\.3)' docs/R-1-memory.md && grep -q 'A read-only open sets it with its evictable part in place of .F., and with .R_EPOCH. sized the same way' docs/R-1-memory.md && grep -q 'fewer than §4\.4 counts once .F ≥ F_min + 32. and none at §4\.6.s server default' docs/R-1-memory.md && ! grep -q 'So its .F. must exceed .F_min. by both' docs/R-1-memory.md && grep -q 'with the height bound .H. and the reserve .R_EPOCH. that R-1-memory §5\.3 sets from that evictable part rather than from .F.' docs/S-STORE.md && grep -q 'sets its height bound and .R_EPOCH. from the evictable part that is left (§14\.1);' docs/S-STORE.md
```
- **Tried.** I committed K11's files on 98c85aca in a scratch clone (scratch commit 945a291d, a throwaway). There, all 52 non-gate lines pass under both locales: the 51 current ones, with 39 changed as above, plus the new one. At 98c85aca, line 39 with the new hash and line 52 fail. Omitting any one K11 entry fails line 52, and K11d and K11e also fail 39, in both locales. Line 37's checker is unchanged, because K11 adds no row, test or mutant. The rework recomputes the Notes' hashes and the author's evidence, and its count of Acceptance commands becomes 53.

## Card text the rework must update (beyond the blocks)
- **Findings, Decisions and "For the gate"** gain D10's point. A read-only open sizes `R_EPOCH` and `H` from its evictable part `F_e`, re-set before each replayed op and fixed once pass 2 ends. A descent past that `H`, or `F_e < F_min`, is 53200 with nothing written. This makes R-1-memory §5.3's `F_min` check and S-STORE §14.1's "§5.4's minimum" one condition.
- **"For the plan card" item 11's F1** takes the extended F1 below.
- **The Notes** take the K11 hashes above, rework 2's evidence and the command count, 53.

## Tests and mutants
- K1–K10 change no test's pass condition except as gate 1 walked them (History). With K, `t_mem_open_sizes` passes at `B_min` (D1) and its fall-back form at `B′` (K4). `t_mem_store_paths_saturated` passes at K5's budget with K1's lowering. HEIGHT-SPLIT-UNCHECKED is caught as K6 states.
- **`t_ro_open` and `t_ro_open_disk_full`, as committed.**
  - Under S-STORE §14.1's literal check, a correct build passes them only while their replays stay below `4n` dirty frames and MapLeafs (D10).
  - Under R-1-memory §5.3's check, a correct build can hang on a pool whose `F_e` is near `F_min` with `F ≫ F_min`.
  - K11 makes both pass on any fixture whose `F_e` stays at least `F_min`. G4 RO-SIZED-FROM-F is caught only by F1's extended form.

## Rulings on P-R1 to P-R6 (the gate's part)
Gate 1's rulings stand (History). K11 changes no owner point: it is how a read-only open uses the budget the owner sets, not a new cost. A read-only open now needs `F_min` plus its dirty frames plus their MapLeafs, as R-1-memory §5.3 already said.

## Owner points, in plain words (stated here, not decided)
- **P-1 and P-R1, memory per connection.** Each connection that can hold pages needs room for four tree paths of pages: about 224 KiB at 8 KiB pages and a height floor of 5. P-1, the earlier figure (168 KiB, three paths), has no answer in the owner's own words. The plan records only a conditional reply whose condition is not shown to be met, and the 16:0x delegation did not cover it. At 8 embedded connections the floor is about 2 MiB of pages, and the total minimum is about 8.3 MiB for a 1 GiB file.
- **P-R2, the height bound.** The memory budget decides how tall an index tree may grow. A tree that would grow past it stops the database. The database is reopened with a larger budget, and nothing committed is lost. Near the minimum budget the bound is 5 levels, and three things follow:
  - long, prefix-sharing keys can reach it within a few hundred to a few thousand rows;
  - a database reopened with less memory than it was written under answers "out of memory" when a tall tree is read;
  - the first write, recovery replay, cleanup pass or drop of such a tree stops it.

  The alternative, letting fewer connections in as trees grow, never stops the database but costs a shared counter on every storage call.
- **P-R3 and P-R4, budgets and thresholds per mode.**
  - Budgets: server 25% of memory (at least 256 MiB), embedded 64 MiB, browser 32 MiB, a quarter of each for sessions.
  - Per-transaction caps as R-1-memory §7.1 lists them.
  - At small budgets the database lowers any cap above the session pool to fit it, and reports that. So a small database fails a very large transaction sooner, with "out of memory", instead of refusing to open.
  - Embedded and browser databases switch big transactions to coarse locks sooner.
- **P-R5, the growth residual.** The database stops if sessions hold all session memory while the file's bookkeeping outgrows its reserve: at the third file growth in a row at the latest, sooner after heavy rewrites or on files above about 127 GiB. Nothing committed is lost, and a reopen with more memory clears it.
- **P-R6, the connection cap.** An embedded app may open 8 connections at once by default, a browser 2. One more gets "too many connections". The cap is what keeps the memory floor true, and the app can raise it.

## Merge timing
This card may not merge before P0 PASS unless the owner says so in their own words.

## Follow-ups (gate 1's, carried; F1 extended)
- **R-1-SPEC-F1 (S-STORE spec card, before A-7d is cut).** The read-only budget gets a test and mutants.
  - `t_ro_open`, or `t_ro_open_disk_full`, gains a form whose replay dirties pages under MapLeafs of their own: a pool at `F_min` plus the dirty frames plus those MapLeafs opens, and one frame below fails with 53200 and writes nothing. Mutant **RO-BUDGET-DIRTY-ONLY** (the budget counts the dirty frames only): the replay waits for an unpin within the step budget, or the open succeeds at a budget the rule refuses.
  - Gate 2 adds a second form, run at a pool well above `F_min` (at least `F_min + 64`), whose replay drives `F_e` to `F_min` and then replays a write into a tree as tall as `F_e`'s `H` allows. With K11 it completes, and one more dirty page fails it with 53200. Mutant **RO-SIZED-FROM-F** (G4: `R_EPOCH` and `H` from `F`): the replay waits for an unpin within the step budget.
  - Judged on A-7d.
- **R-1-SPEC-F2 (R-1 spec, before R-1's Phase B card).** Size the growth allowance so that "the third growth" is exact:
  - size `R_file`'s LPID bitmap and MapInner terms for `max(next_lpid, slot_count)` LPIDs, plus a bound for the pool's never-written pages;
  - make the allowance's MapInner term follow each step's LPIDs;
  - recompute §4.6 and §7.2.
- **R-1-SPEC-F3 (R-1 and A-7c, before A-7c is cut).** Either reserve S-STORE §13 step 2's copy of image `k`'s two bitmaps in `R_file`, if step 2 makes one, or state how step 2 avoids it.
- **R-1-SPEC-F4 (R-1 spec, before R-1's Phase B card).** The open refuses with 53200, writing nothing, when the directory step 2 descended is taller than the sized pool's `H`. With K11 that includes a read-only open's `H` from `F_e`.
- **R-1-SPEC-F5 (the next S-STORE card; pointers only).**
  - S-STORE §14 step 3b and §6.8's CreateTree point to R-1-memory §5.2.
  - After S-THETA merges, R-1-memory's "step budget" points to S-STORE §15.
- **R-1-SPEC-F6 (R-1 spec, with or before R-1's Phase B card).** Tests for the corrections that have none:
  - `t_mem_open_sizes`' report shows every cap at most `S` at `B_min` (K1);
  - `t_mem_heap_accounted` samples after an open whose replay reads a `wal_seg_size / 4` record, and after amcheck (K7);
  - `t_mem_account_returns` gains an aborted CREATE TABLE and a `ROLLBACK TO` past one (K10).
- **R-1-SPEC-F7 (S-STORE question; depends on P-R2).** A GC sweep or drop-walk step whose descent would pass `H` could skip the tree with a report rather than poison the store.

Full descriptions of F2–F7 are gate 1's (History, "Follow-ups"), unchanged.

## Not verified
- No code exists. Every test and mutant was walked as text.
- The provisional constants are unmeasured: `o_frame`, 64 bytes per root-table entry, `e_lock`, and the 1/32 slope. D10's figures use §4.6's worked values.
- That S-2b will state SSI's spill as v7.8 §8.5 promises.
- The implementer's log: none exists for R-1-SPEC in `cards/logs/`. Rework 1's reported results were checked against the commit and the card, not a log.
- K11 is tried mechanically (bytes, Acceptance, omit-one, composition by grep against the three sibling cards' text). Its design rests on this gate's reading of S-STORE §5.3, §5.6 and §14.1 alone, so a gate 3 should re-read it.

## History

- **Gate 1: REWORK, verdict commit 7fb189020418b1700928486fd8d84d3aba4a5358, judging 32f68472b4155671f5418f0fb633b0859958c89a.** It was a P gate in Claude Code session d36e5b00-a57c-4ef7-b43c-283e6f9e735c, with GATE EXIT=0. It found defects D1–D9 and gave K1–K10 (25 exact entries), two new Acceptance lines and follow-ups F1–F7.
- **Rework 1: commit 98c85acadefe95d62badb88856eaff989727099e,** by a GLM 5.3 implementer. It applied K1–K10 to `docs/R-1-memory.md` and `docs/S-STORE.md`. The card was updated in the main checkout (cards are local), with its pre-rework copy at `~/.cache/nv3-review/R-1-SPEC.card-backup-pre-rework1.local.md`.
- **Gate 2 (this verdict): REWORK on 98c85aca,** for D10, with K11.

**Gate 1's verdict, as committed at 7fb18902, verbatim.** Its header lines are prefixed with "- ", so that no tool reads them as this verdict's fields. Everything after them is unchanged, headings included. Where its sections differ from the ones above (owner points, follow-up F1, Acceptance numbering), the sections above are current. The same text is at `git show 7fb18902:nucleus/v3/verdicts/R-1-SPEC.md`.

<!-- gate 1's verdict (7fb18902), verbatim but for its header lines, begins -->

- Card: R-1-SPEC
- Class: spec
- Card-gate: P
- Verdict: REWORK
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: T3 review sub-agent, Claude Code session d36e5b00-a57c-4ef7-b43c-283e6f9e735c (an independent P gate: this agent wrote neither the card, its implementation 32f68472, nor S-ZETA, S-ETA or S-THETA, and is not the orchestrator)
- Date: 2026-11-17
- Gate-exit: GATE EXIT=0
- Commit: 32f68472b4155671f5418f0fb633b0859958c89a
- Mutants: spec text, none in code; the card's 20 spec mutants walked as text on their tests: MIN-RMAP-DENSE -> t_pool_minimum caught; MIN-RESERVE-NO-STEP2 -> t_pool_minimum caught; RESERVE-PER-THREAD-ONLY -> t_epoch_reserve_two_exhausters floor variant caught (A-3c, A-7a); CLAIM-DEPTH-TWO -> the floor variant through write_guard caught; MAPPIN-KEPT -> t_pool_floor_scattered caught (correct build O = 33 >= 21; mutant leaves 3); HEIGHT-SPLIT-UNCHECKED -> t_height_bound (a) caught by the root-split hook (its stated failure is wrong, K6); HEIGHT-DESCENT-UNCHECKED -> t_height_bound (b) caught; UNCHARGED-CONSUMER -> t_mem_heap_accounted caught; STORE-PATH-SHARED-CHARGE -> t_mem_store_paths_saturated caught; CAP-WAITS, CAP-POISONS, OP-NO-SPILL, CONN-WHOLE-MESSAGE, PLAN-NO-EVICT -> t_mem_caps caught; ACCOUNT-NOT-CREDITED -> t_mem_account_returns caught; OPEN-NO-FILE-TERM, POOL-SIZED-BEFORE-TABLE -> t_mem_open_sizes caught, but that test fails a correct build at B_min as committed (D1); FALLBACK-SET-KEPT -> the fall-back form caught only when no open of the form grows the file (D5); ALLOWANCE-UNRESERVED -> t_mem_growth_allowance caught; ADMIT-OVER-COUNT -> t_mem_admission caught. Gate's own: G1 RO-BUDGET-DIRTY-ONLY (a read-only open's budget counts its dirty frames only, as committed) -> UNCAUGHT by t_ro_open and t_ro_open_disk_full (D2; K2, F1); G2 WALK-BUFFER-UNCHARGED (recovery reads a record into an uncharged buffer) -> UNCAUGHT, no test samples an open that replays (D4; K7, F6); G3 CREATE-ABORT-KEEPS-RFILE (an aborted CREATE leaves its entry's bytes in R_file) -> UNCAUGHT by t_mem_account_returns (D8; K10, F6). Mechanical: omitting any one of the 50 blocks fails Acceptance in both locales (50/50 caught); 13 of 16 planted errors caught, the 3 uncaught all in R-1-memory prose that no line pins, as Limit 1 intends
- Followups: R-1-SPEC-F1, R-1-SPEC-F2, R-1-SPEC-F3, R-1-SPEC-F4, R-1-SPEC-F5, R-1-SPEC-F6, R-1-SPEC-F7

## Verdict in brief

**REWORK.** The mechanics are clean and most of the design holds. My own derivations agree with the card's for the claim depth (1), `R_EPOCH_min = n + s + 1`, `R_MAP = n(H + 2) + R_EPOCH` with §5.3's reworded premise, `H` from the pool, `F_min`, and every worked number. "Charge as built, size last" is right, and so are points (a) and (b). Four defects still meet this gate's REWORK standard:

1. **D1. `B_min` cannot open with the default caps.** R-1-memory §7.3 fails the open with 22023 for "a per-account cap above `S`". At every mode's `B_min`, `S` is `S_min`, and several §7.1 caps are above it: embedded `ws_mem` is 16 MiB against 5 MiB, browser `ws_mem` is 4 MiB against 2 MiB, and server `lock_mem` is 128 MiB against 5 MiB. So the defaults do not open at their own `B_min`. That contradicts §7.2 ("the least budget that opens the file"), §4.6's browser-minimum row and §2.2, and it makes `t_mem_open_sizes` ("an open at `B_min` succeeds") fail on a build that follows the text.
2. **D2. A read-only open's budget is too small once `R_MAP` is settled.** Draft 7.16's `R_MAP` leaves out the MapLeafs that §4.1 pins for unpinned dirty pages, because a frame acquisition releases them by evicting those pages. A read-only open never evicts a dirty page, so it can never release them. S-STORE §14.1 ("`F` minus the dirty frames") and R-1-memory §3 and §5.3 still count only the dirty frames, so a read-only replay that passes the check can run out of frames it can obtain. It then waits for an unpin that never comes, instead of returning 53200, and this is the salvage path that must work on a full volume.
3. **D3. R-1-memory contradicts C-T0 on SSI memory.** C-T0 v8 §8.5 keeps v7.8 §8.5: SSI state "is never summarised in 2.0; it spills to disk under memory pressure". R-1-memory §3 and §6.2 give SIREADs "escalation, then 53200" and keep retained SIREADs charged until retention ends, with no spill. By R-1-memory's own precedence rule, the mechanism is C-T0's.
4. **D4. Some consumers have no account.** §2.1 says "The table of §3 lists every consumer; nothing else allocates." Four consumers are missing:
   - recovery's record buffer, for records of up to `wal_seg_size / 4` (16 MiB at the default segment size, against an embedded `B_min` of 8.29 MiB);
   - amcheck's file-proportional sets;
   - a statement's working memory (its parse and plan, rows in flight, detoasted datums);
   - C-T0 §9.4's per-tree garbage estimates.

Five smaller defects come with them. Each is listed below with its fix.
- **D5.** The fall-back form's exact budget fails a correct build if pass 2 grows the file, and §5.3 never says to credit `R_file`'s terms from meta `k`.
- **D6.** `t_mem_store_paths_saturated`'s budget does not give "a pool 32 frames above its minimum".
- **D7.** HEIGHT-SPLIT-UNCHECKED's stated failure is wrong, and nothing says that a gated operation descending a taller tree poisons the store.
- **D8.** A CreateTree that is never applied leaves its bytes in `R_file`.
- **D9.** The growth residual is wider than "three growths".

The corrections are one list of 25 exact Old/New pairs, K1 to K10, each tried in scratch. With them applied to the committed files, every Acceptance line passes except 38 and 39, whose new values are given below, and two new lines pin the corrected text.

## Mechanics (all checked by this gate; nothing taken from the implementer's report)

- **Fresh gate.** I extracted the scaffold's `fresh-gate.sh` through the shallow `--no-local` clone. The scaffold id printed was `71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`, with no EXTRACT FAILED line. The run's lines were:
  - `fresh-gate: commit 32f68472b4155671f5418f0fb633b0859958c89a`
  - `fresh-gate: base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `fresh-gate: checks passed (4875 files hashed in 2.22 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - steps test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests and api-surface, each `EXIT=0`
  - last line `GATE EXIT=0`, with stderr empty. The extract was removed afterwards.
- **Scope.** `git diff nucleus/v3-scaffold...HEAD` names four files, all in Touch only: R-1-memory.md +462, S-STORE.md +36/−17, C-T0 +9/−8 and README +2/−1. Nothing is in a protected path. The worktree is clean, and no worker log for R-1-SPEC exists in `cards/logs/`.
- **Bytes.** My own parser (scratch `~/.cache/nv3-review/gate-R-1-SPEC/tools/apply.py`; the other scripts named here sit beside it) takes doc blocks 0–14, E1–E25, T1–T9 and X1 from the card and applies them to 71ecf3e6's files. Every Old occurs exactly once at its turn. With the date 2026-11-17, the four results equal the committed files byte for byte. With 2026-11-16 they equal the card's recorded dry-run sha256s (`0c7c2313…`, `286c85d6…`, `191f528f…`, `be06c4f8…`). The base files are the card's (S-STORE `a1376c94…`, C-T0 `96caa6cb…`), and 48e52d3b's three docs equal 71ecf3e6's.
- **Acceptance.** I ran the 49 commands before the gate line in a fresh clone at 32f68472, with `nucleus/v3-scaffold` at 71ecf3e6, under `en_CA.UTF-8` and under `LC_ALL=C`: 49 of 49 pass in each. The checkers print `7 invariants; 20 rows` / `OK` and `162 rows; 233 mutants; 24 judged only on Phase B cards; 47 invariants; 50 tests` / `OK`. Line 50 is the fresh gate above.
- **Omit one block.** I built 50 variants, each the result with one block omitted (D0–D14, E1–E25, T1–T9, X1), committed each and ran it in both locales. Every one fails at least one line in both locales. Doc blocks 1–14 fail line 4 (the headings) and the lines for their own text, and doc block 0 fails lines 2 and 3. Every E block fails line 39, most also 38 and their own lines. The T blocks fail lines among 40–43, and 48. X1 fails 44, 45 and 49.
- **Planted errors.** I planted 16 errors, each a one-fault result run in both locales. 13 are caught:
  - `F_min` with `3 × n` (line 7);
  - §4.6's embedded `R_MAP` changed (13);
  - `R_MAP` with `2 × n` (10);
  - a stray word in E3's New (39);
  - the draft 7.17 (22, 39);
  - `t_pool_floor_scattered`'s A-4a row moved to B-3 (37, 39);
  - "the last form" added to that row (37, 39);
  - a stray edit in T4 (42, 48) and in T6 (48);
  - X1 shortened (49);
  - `H_min = 6` (12);
  - E5's "`H + 1`" made "`H + 2`" (39);
  - a stray edit of an untouched S-STORE line (38, 39).

  The other 3 are uncaught: FALLBACK-SET-KEPT dropped from I-MEM-OPEN's mutant cell (line 17 checks invariant → row, not row → invariant), "credited" made "kept" in §5.3's fall-back bullet, and the allowance halved. All three are R-1-memory prose that no line pins, which Limit 1 allows; the gate reads that prose.
- **The checker's "last form" rule** (point (b)). Line 37 refuses "last form" outside `t_epoch_reserve_two_exhausters` (planted above: caught), a full form that names a stand-in, a change to §15's parenthetical, and an A-7f row without A-7d.

## The design, re-derived

### What holds
- **The claim depth is 1** (doc §4.3). At 7.15 a claim takes no map latch, calls no `write_guard` and records into the image-record list, a leaf lock. Slot allocation takes no free-set lock and obtains no frame (S-STORE §4.2, §4.4, §4.5). So nothing done under a reserve frame's latch draws or claims again. The dirty eviction's `write_guard` on its victim's MapLeaf is a frame acquisition, which never runs inside a draw or a claim. With the others holding at most `(n − 1) + s` latches, `R_EPOCH > (n − 1) + s` is enough, and `n + s + 1` keeps one frame of slack. The history checks out: c4397471's "two claims" counted a claim that recorded into the copy's MapLeaf.
- **`R_MAP`** (doc §4.4). The five classes partition `N = F − R_EPOCH`. I checked every pin §5.4 counts:
  - the path, with the pack step's `L` and `R` and a restart's cold page;
  - the reservation's pages, which lie under the reservation's MapLeafs (T);
  - a chain build's or free's two pages, with both MapLeafs held (T);
  - a free's MapLeaf (T);
  - a load's MapLeaf pin under S-THETA, which falls in the path term;
  - the checkpointer and the evictors, which latch and do not pin.

  Only a path page can be dirty under a MapLeaf the thread does not pin, so `|Mp| ≤ n(H + 2)`. `O = N − |T| − |Mp| − |Mc| ≥ ΣQ_t` plus each class's own remaining room, so every pin, and every (Mp) or (Mc) MapLeaf a thread still needs, has a frame it can obtain. In a read-write store an (Mu) MapLeaf is obtainable: evicting its unpinned dirty pages, each a (U) frame, ends its pins. A dirty MapLeaf's own eviction sets a ref in a MapInner, outside the pool. E3's premise is what §5.3's argument uses. The 7.15 wording would need `3n(H + 2) + R_EPOCH`. P-1's three paths are short whenever path pages lie under MapLeafs of their own, and six paths buy nothing.
- **`H`** (doc §4.5). `H = ⌊(F − 2R_EPOCH)/(4n)⌋ − 2` is the largest height the minimum holds for. The 15/16 inequality gives `H ≥ H_min` for every `F ≥ F_min` at the 1/32 slope. Both places where the bound is enforced are sound: the descent checks before its pin, and the root split is decided in the reservation, before the first mutation (S-STORE §6.4). A fixed engine limit would be worse, since apply cannot fail logically (S-STORE §9.1).
- **Numbers.** I recomputed every figure:
  - `F_min`: 64, 244 and 604.
  - Embedded: `F` 5,878, `R_EPOCH` 186, `H` 170, `R_MAP` 1,562, with 2 frames of slack.
  - Server: `F` 916,452, `R_EPOCH` 28,642, `H` 10,737, `R_MAP` 243,422, with 48 frames of slack.
  - Browser: 64, 4, 5 and 18, exact.
  - `R_file`'s parts: the MapInners are 1 page and 52 pages, the allowances 74,240 and 935,936 bytes, and `L` is 9, 8, 7, 6 and 6.
  - `o_frame` = 64 + 40 + 24 bytes.
  - `B_min`: 3,114,176 B (2.97 MiB), 8,697,344 B (8.29 MiB) and 35,797,504 B (34.14 MiB).
  - Units are frames throughout §4 and bytes in §5 and §7.

  The per-thread terms and `R_MAP` are counts, not reservations; only `R_EPOCH`'s frames are set aside. So the floor is a worst-case pin count, not grossly more than needed: `S_min` dominates every `B_min`.
- **The WAL path.** Record bytes are charged with their layers until `written_end` passes them, so `COMMIT` charges nothing new to `S`. Appenders wait on `written_end`.
- **Pass 1's set.** About 24 bytes per marker, at most one marker per `min(preapply_rows × 16, preapply_bytes)` of span.
- **SSI.** A per-transaction cap, and the right account. Its behaviour at memory pressure is D3.
- **Hash tables and rehash.** §2.3's "capacity included" and §5.2's "at any instant, its growth included" make the rule govern rather than the provisional 64 or 128 bytes. The page table is sized for `F` and never rehashed.
- **Allocator overhead** is the base, measured by D-13, not budgeted.

### The floor's sufficiency
For a read-write store I looked for an operation that needs one frame more than the floor grants, and found none. The worst cases I checked:
- a root split of an `(H − 1)`-level tree: path ≤ `H + 2`, `H` new pages, ≤ `H` MapLeafs for its LPIDs, and its path pages' MapLeafs in (Mp);
- a restart that keeps its cold page and its reservation's pins while it waits on the gate;
- an eviction that must set its victim's entry: the MapLeaf is resident by §4.1, and its copy comes from `R_EPOCH`;
- a dirty MapLeaf's own eviction: its parent MapInner is outside the pool, and that MapInner's copy comes from `R_EPOCH`;
- a chain build or free;
- a checkpoint flip, which takes no frame (restarted allocations wait on it with counted pins);
- pass 2 of a read-write open, which may evict.

This depends on the Q-MAPLEAF-DIRTY mechanism (whoever makes a path page's MapLeaf resident must do it before the first mutation). The card counts that frame either way and routes the mechanism to S-STORE before A-3c. **The exception is a read-only open (D2).** There no dirty page is ever evicted, so (Mu) is not obtainable, and the stuck frames are the dirty frames plus up to as many clean MapLeafs.

Take one caller (`n = 1`, `s = 1`, `H = 5`), so `F_min = 34`, and give the open `F = 34 + D`, which the committed check accepts. The replay's thread can then obtain `31 − D_m` frames, where `D_m` is the number of clean MapLeafs pinned for the dirty pages. A root-split cascade needs up to 28 pins and (Mp) MapLeafs, so it stalls once `D_m ≥ 4`, and any operation stalls once `D_m` nears 30. At any `F`, a replay whose dirty pages lie under MapLeafs of their own runs out at about half the dirty frames the check allows. Nothing then ends a pin, so the open waits instead of returning 53200.

### Exhaustion, point by point
- **At open.** Every charge is made before its allocation, against `B`: `R_fixed`, `R_file`'s terms from the meta and the file's length, the root table and CreateTree entries, and `M`. A 53200 comes before the open's first write, or after a fall-back with nothing written beyond the fence and image `k`'s pass-2 evictions. D1 makes `B_min` itself fail, with 22023 (K1). D4's record buffer is uncharged (K7).
- **Pass 1** charges `M`. **Pass 2**, read-write, runs in the sized pool. Read-only is D2 (K2).
- **After a fall-back**, see point (a).
- **A client operation before its first mutation** charges `S`, never waits, and fails with 53200 without poisoning anything (M3). **After its first mutation**, inside apply, a gated operation, it charges nothing to `S` (M2). Its frames wait only as §5.3 and §5.4 bound them, and only the two stated fail-stops remain: the height bound and the growth residual. The residual is wider than stated (D9, K8).
- **A checkpoint** uses `R_fixed`'s list and buffers and `R_file`. Whether step 2 holds image `k`'s bitmaps beside the current ones is open (F3).
- **Background jobs** use a job slot, frames and `R_file`, and have the same two fail-stops. A gated operation that descends a tree taller than `H` poisons the store, which T8 and §4.5 did not say (D7, K9).

With the corrections, no path turns memory exhaustion into data loss or a wrong result. The fail-stops recover under a larger budget, since recovery replays the commit, and a read-only open fails with 53200 instead of waiting.

### Point (a): pass 1 after a fall-back (ruling: correct, with K4)
The sizing's place agrees with S-STORE §14 step 4a items 2–4, the retire paragraph and the explicit rollback: after step 3b, before the proof's retire write and before pass 2. "Nothing written" after a fall-back means the fence and image `k`'s pass-2 evictions, each into a slot neither image references. That is exactly §14.2's phrase. A 53200 in the second pass 1, the second root table or the second sizing leaves meta `k` in its places and image `k−1` with its WAL intact (I-TWO-GEN). A later open with a larger budget falls back as this one would have, by itself, by rollback or as a salvage, so no state is left that it cannot recover.

There are two gaps. First, §5.3 credits the root table, the CreateTree entries and `M`, but not `R_file`'s terms computed from meta `k` (K4a). Second, the fall-back form's `B′` is exact only if image `k`'s pass 2 grows no file. A growth makes `R_file` larger than the read-only reports give, and a correct build then fails (i) and the final open of (ii) with 53200 (K4b). With both fixed, FALLBACK-SET-KEPT's extra `M` leaves `F` exactly one frame short at `B′`.

### Point (b): no new "last form" (ruling: honest)
`t_pool_floor_scattered` uses a stand-in at A-3c only for the fill's MapLeafs (S3's growth call). Its threads use the pool's own calls in both forms, in a layout no operation's pins reach. I checked the correct-build arithmetic: `F` = 94, `R_EPOCH` = 5, `R_MAP` = 26, `|T|` = 42, `|Mp|` = 14, `O` = 33 ≥ 21. MAPPIN-KEPT leaves 3 obtainable frames, so G's fourth acquisition fires the wait hook.

`t_height_bound` (a) is full at A-4a, where fail-stop is A-3c's, in its closure. Its (b) runs at A-4a on S3 and S5, and in full at A-7f after a checkpoint, a close and a reopen with the smaller pool. That reopen descends no tree: there is nothing to replay and no amcheck, and step 2 reads only the directory. Every Needs cell is in its card's closure.

### Point (c): "charge as built, size last" (ruling: correct, and the best of the six)
The design cannot under-reserve, because there is no estimate. It never over-commits, because every allocation, the frames used before the sizing included, is charged against `B` first. The frames used before the sizing hold only clean pages and are dropped before a charge fails. The provisional `H` for step 2 is the one for the largest pool the budget allows, so step 2 refuses nothing the sized pool serves. The five rejected alternatives cost what the Notes say. One nit: (4)'s "about four times `B_min`" is about 2.4–2.7 times at §7.1's values.

- **More trees after the open.** A CreateTree moves its bytes from `S` to `R_file` at its DDL statement, which is not a poisoning context. But a CreateTree that is never applied (an abort, or `ROLLBACK TO`) never returns them (D8, K10).
- **A directory read error or integrity failure.** The entries charged so far go with image `k`, and a fall-back credits them (§5.2).

### The height bound's new wording, and the directory at open
"No descent passes `H` and no root split exceeds it" is right: "no tree is ever taller than `H`" is false for a tree written under a larger pool. It is applied consistently in E5, E14, R26, T8 and §4.5. Two things are missing: HEIGHT-SPLIT-UNCHECKED's failure text (K6), and the fact that writes, their replay, GC sweep steps and drop-walk steps into such a tree poison the store (K9).

The author's open note on the directory is an **acceptable residual, not a hole.** A directory taller than the sized pool's `H` would need billions of trees ever created at `H_min` = 5, since no directory page is ever packed or freed. Only a test build with a lower `H_min` reaches it. Its effect would be a fail-stop at the first directory change or replayed CreateTree, recovered with a larger budget, with nothing lost. F4 makes the open refuse it up front.

## Defects
- **D1** (REWORK): §7.3 against §7.2, §4.6, §2.2 and `t_mem_open_sizes`, as above. Fix: K1. A cap above `S` is lowered to `S` at open, and `lock_escalate_rows` with it where §6.2's order needs it; `S_min < preapply_bytes` becomes the 22023 that keeps C-T0 §2.1's `ws_mem ≥ preapply_bytes` true after the lowering.
- **D2** (REWORK): the read-only budget. Fix: K2a–K2f, in R-1-memory §3, §4.4 and §5.3 and S-STORE §5.4, §14.1 and the Changelog. A test and mutant follow as F1.
- **D3** (REWORK): SSI spill. Fix: K3a and K3b, which point to C-T0 §8's spill under pressure on `S` and keep the per-transaction cap's 53200.
- **D4** (REWORK): unaccounted consumers. Fix: K7a–K7f, plus K10a's row name.
  - The walk's record buffer joins `M`, so `S ≥ M` and `B_min` count it.
  - Two §3 rows are added: a statement's working memory, and amcheck's sets.
  - C-T0 §9.4's per-tree garbage estimate and entry count ride on the root-table entry.
- **D5**: the fall-back form (K4a, K4b).
- **D6**: `t_mem_store_paths_saturated`'s budget. `B_min + 4 MiB` gives `S = S_min`, since `σ × B` is below it, and a pool about 504 frames above the minimum, not 32 (K5).
- **D7**: K6 and K9a/K9b.
- **D8**: K10a and K10b. A ledger leak, unbounded over repeated aborted DDL until a reopen.
- **D9**: K8a and K8b. The allowance is drawn without any file growth when LPID allocation outruns the file: free slots outnumber free LPIDs after two checkpoint intervals of rewrites, since each page can hold three homes. One growth step also needs more MapInner pages than one path above about 127 GiB at 8 KiB pages. So the residual comes sooner than the third growth. K8 states it as it is, and F2 proposes the sizing that would make "the third growth" exact.

## Corrections (one complete list; each Old occurs exactly once in the committed file named, and at its turn)

In the card these become the named doc blocks' text and E6's, E8's, E24's and E25's News, plus one new S-STORE block for K2d. Its Old is §14.1's "Pool budget" sentence, which occurs once in 7.15 and once after E1–E25. It sits between E12 and E13 in file order (call it E12a, or renumber). Applied in order to 32f68472's files in scratch, the 25 entries give `docs/R-1-memory.md` sha256 `2eb26225667fd8575f9430c01382b3e3e68f76632073c5c9c0d0495178995d66` and `docs/S-STORE.md` sha256 `07b9b0406671d0f1eeea1b692608123f416e9317e1796491d199e813339c7e34`, both at the date 2026-11-17. C-T0 and README do not change. The S-STORE entries also compose with S-ZETA's, S-ETA's and S-THETA's blocks in all 24 orders of the four cards.

**K1.** `docs/R-1-memory.md` — R-1-memory §7.3 (Doc block 7).

Old:
```text
Every §7.1 value is an open option. Options that contradict each other fail the open with 22023 and write nothing: `ws_mem < preapply_bytes`; a per-account cap above `S`; `n_c = 0`; `σ` outside `(0, 1)`. A budget below `B_min` fails it with 53200 (§7.2).
```
New:
```text
Every §7.1 value is an open option. Options that contradict each other fail the open with 22023 and write nothing: `ws_mem < preapply_bytes`; `S_min < preapply_bytes`; `n_c = 0`; `σ` outside `(0, 1)`. A budget below `B_min` fails it with 53200 (§7.2). A per-account cap above `S` is not an error: once the open has sized `S` (§2.2), it lowers every such cap to `S`, and `lock_escalate_rows` to `⌊lock_mem / e_lock⌋` where the lowered `lock_mem` needs it, so that §6.2's order holds, and reports both (§8). At `B_min`, `S` is `S_min` or `M`, below several of §7.1's caps, so without the lowering no mode's defaults would open at their own `B_min`; `ws_mem ≥ preapply_bytes` still holds, since `S ≥ S_min ≥ preapply_bytes`.
```

**K2a.** `docs/R-1-memory.md` — R-1-memory §3, the read-only replay row (Doc block 3).

Old:
```text
| a read-only open's replay | frames inside `F` | its dirty frames | 53200 at open, nothing written | S-STORE §14.1 | A-7d |
```
New:
```text
| a read-only open's replay | frames inside `F` | its dirty frames and the MapLeafs that S-STORE §4.1's pins hold for them | 53200 at open, nothing written | S-STORE §14.1 | A-7d |
```

**K2b.** `docs/R-1-memory.md` — R-1-memory §4.4, "Obtainable frames" (Doc block 4).

Old:
```text
and `O ≥ 1` implies `|U| ≥ 1`, since an (Mu) MapLeaf has an unpinned dirty page: a thread that may still pin always finds a (U) frame, and waits at most for its brief latch holder (S-STORE §5.3).
```
New:
```text
and `O ≥ 1` implies `|U| ≥ 1`, since an (Mu) MapLeaf has an unpinned dirty page: a thread that may still pin always finds a (U) frame, and waits at most for its brief latch holder (S-STORE §5.3). A read-only open evicts no dirty page (S-STORE §14.1), so there no (Mu) MapLeaf is obtainable, and its budget counts those MapLeafs with the replay's dirty frames instead (§5.3).
```

**K2c.** `docs/R-1-memory.md` — R-1-memory §5.3, "Pass 2 runs in the pool" (Doc block 5).

Old:
```text
- **Pass 2 runs in the pool** (S-STORE §14.1). A read-only open keeps the replay's dirty frames, so its `F` must exceed `F_min` by them, or it fails with 53200 and writes nothing (S-STORE §14.1; `t_ro_open`). The detail names the dirty frames at the failure, so the caller can size a larger read-only budget.
```
New:
```text
- **Pass 2 runs in the pool** (S-STORE §14.1). A read-only open keeps the replay's dirty frames, and with them the MapLeafs that S-STORE §4.1's pins hold for them: it evicts no dirty page, so it releases neither, and §4.4's (Mu) frames are obtainable only through such evictions. So its `F` must exceed `F_min` by both, or it fails with 53200 and writes nothing (S-STORE §14.1; `t_ro_open`); with scattered LPIDs that is up to twice the dirty frames. The detail names the dirty frames and those MapLeafs at the failure, so the caller can size a larger read-only budget.
```

**K2d.** `docs/S-STORE.md` — S-STORE §14.1, "Pool budget" (a new block, E12a, between E12 and E13; file order).

Old:
```text
- **Pool budget.** The evictable part of the pool (`F` minus the dirty frames) must meet §5.4's minimum, both during pass 2 and after it.
```
New:
```text
- **Pool budget.** The evictable part of the pool (`F` minus the dirty frames and the MapLeafs that §4.1's pins hold for them, which a read-only open never releases, since it evicts no dirty page, and which §5.4's `R_MAP` does not count; R-1-memory §4.4, §5.3) must meet §5.4's minimum, both during pass 2 and after it.
```

**K2e.** `docs/S-STORE.md` — S-STORE §5.4, R_MAP's bullet (E6's New).

Old:
```text
A MapLeaf whose dirty pages no thread pins is not counted: evicting those pages, which a frame acquisition does (§5.2), ends its pins.
```
New:
```text
A MapLeaf whose dirty pages no thread pins is not counted: evicting those pages, which a frame acquisition does (§5.2), ends its pins; a read-only open, which evicts no dirty page, counts such MapLeafs with its dirty frames instead (§14.1).
```

**K2f.** `docs/S-STORE.md` — S-STORE Changelog, 7.16's R_MAP bullet (E25's New).

Old:
```text
A MapLeaf whose dirty pages no thread pins is not counted, since a frame acquisition releases it by evicting them;
```
New:
```text
A MapLeaf whose dirty pages no thread pins is not counted, since a frame acquisition releases it by evicting them, but a read-only open, which evicts no dirty page, counts such MapLeafs with its dirty frames (§14.1);
```

**K3a.** `docs/R-1-memory.md` — R-1-memory §3, the SIREADs row (Doc block 3).

Old:
```text
| SIREADs | per transaction, `ssi_mem` | §6.2 | v7.8's escalation (kept by C-T0 §8), then 53200 | C-T0 §8 | B-2c |
```
New:
```text
| SIREADs | per transaction, `ssi_mem` | §6.2 | v7.8's escalation (kept by C-T0 §8), then 53200; under pressure on `S`, the spill to disk of SSI state that C-T0 §8 keeps from v7.8 §8.5 | C-T0 §8 | B-2c |
```

**K3b.** `docs/R-1-memory.md` — R-1-memory §6.2 (Doc block 6).

Old:
```text
A committed transaction's retained SIREADs (C-T0 §8.6) stay charged to it until retention ends.
```
New:
```text
A committed transaction's retained SIREADs (C-T0 §8.6) stay charged to it until retention ends, or until C-T0 §8's spill moves them to disk: v7.8 §8.5, which C-T0 §8 keeps, says SSI state "is never summarised in 2.0; it spills to disk under memory pressure", and S-2b states that spill. Its temp-file buffers are charged as §6.4's are; this file does not replace the spill with a 53200.
```

**K4a.** `docs/R-1-memory.md` — R-1-memory §5.3, "After a fall-back" (Doc block 5).

Old:
```text
its frames are dropped unwritten, and its root table, its CreateTree entries and its `M` are credited. Steps 2–3b from meta `k−1` then charge their own,
```
New:
```text
its frames are dropped unwritten, and `R_file`'s terms from meta `k` (§2.2 item 2), its root table, its CreateTree entries and its `M` are credited. `R_file`'s terms from meta `k−1`, at the file's length then, and steps 2–3b from meta `k−1` then charge their own,
```

**K4b.** `docs/R-1-memory.md` — R-1-memory §9, t_mem_open_sizes' fall-back form (Doc block 9).

Old:
```text
come before the leaf's in the WAL that pass 2 from image `k` evicts before it reaches the leaf.
```
New:
```text
come before the leaf's in the WAL that pass 2 from image `k` evicts before it reaches the leaf, each into a free slot that neither image references, so that no open of the form grows the file (a SimEnv hook sees no extension): a growth would make `R_file` larger than the read-only reports give, and `B′` short by it on a correct build.
```

**K5.** `docs/R-1-memory.md` — R-1-memory §9, t_mem_store_paths_saturated (Doc block 9).

Old:
```text
Embedded mode with `B = B_min + 4 MiB`, a pool 32 frames above its minimum so that applies evict,
```
New:
```text
Embedded mode with `B = B_min + 32 × (page_size + o_frame)`, so that `S` is `S_min` (`σ × B` is below it) and the pool 32 frames above its minimum, so that applies evict,
```

**K6.** `docs/S-STORE.md` — S-STORE §5.4, t_height_bound (E8's New).

Old:
```text
HEIGHT-SPLIT-UNCHECKED fails (a): the tree reaches `H + 1` levels and no insert fails.
```
New:
```text
HEIGHT-SPLIT-UNCHECKED fails (a): the root-split hook sees the tree reach `H + 1` levels, and the insert that fails is a later one, at its descent, not the one whose reservation includes the root split.
```

**K7a.** `docs/R-1-memory.md` — R-1-memory §1, `M` (Doc block 1).

Old:
```text
| `M` | Pass 1's set at open: what recovery's pass 1 collects (§5.3); credited when pass 2 ends. |
```
New:
```text
| `M` | Pass 1's set at open: what recovery's pass 1 collects, with the recovery walk's record buffer (§5.3); credited when pass 2 ends. |
```

**K7b.** `docs/R-1-memory.md` — R-1-memory §3, the recovery row (Doc block 3).

Old:
```text
| recovery's pass 1 | the open's charge, `M` | §5.3 |
```
New:
```text
| recovery's pass 1 and its walk's record buffer | the open's charge, `M` | §5.3 |
```

**K7c.** `docs/R-1-memory.md` — R-1-memory §5.3, "Pass 1's set M" (Doc block 5).

Old:
```text
since every chunked transaction wrote at least that much. It is credited when pass 2 ends, before any session exists.
```
New:
```text
since every chunked transaction wrote at least that much. `M` also holds the walk's record buffer, charged before each allocation it makes (M1): at most the longest record that pass 1, step 3b or pass 2 holds whole, itself at most `wal_seg_size / 4` (S-STORE §12.2), or the fixed buffer of a reader that takes a record in pieces. It is credited when pass 2 ends, before any session exists.
```

**K7d.** `docs/R-1-memory.md` — R-1-memory §7.2 (Doc block 7).

Old:
```text
`M` pass 1's set (§5.3), 0 when the open replays no chunked transaction, as in every row below.
```
New:
```text
`M` pass 1's set with the walk's record buffer (§5.3), 0 when the walk reads no record, as in every row below.
```

**K7e.** `docs/R-1-memory.md` — R-1-memory §3, two rows after "temp-file buffers" (Doc block 3).

Old:
```text
| temp-file buffers | the spilling consumer's account | per spill | charged with the spill | — | B-1a, C-4a–g |
```
New:
```text
| temp-file buffers | the spilling consumer's account | per spill | charged with the spill | — | B-1a, C-4a–g |
| a statement's working memory: its parse and plan, rows in flight, expression results and detoasted datums | the statement's account (§2.3), in `S` | what it holds at once | 53200 for a value or a row that does not fit; the wire streams what it sends (§6.4) | executor | C-4a–g |
| amcheck's sets (S-STORE §14 step 7) | at open, `S`, before any session; on a call, the calling session's | `⌈next_lpid / 8⌉ + ⌈slot_count / 8⌉` bytes, beside its walk's pins | 53200 to its caller, never a poison: it is a read path (S-STORE §9.2) | S-STORE §4.3 (I-FREE-EXACT), §6.8 (I-LPID-EXACT) | A-8 |
```

**K7f.** `docs/R-1-memory.md` — R-1-memory §12, the root-table row (Doc block 12).

Old:
```text
| a root-table entry | 64 bytes | tree id, root LPID, kind and state, and a map node's share | A-7b |
```
New:
```text
| a root-table entry | 64 bytes | tree id, root LPID, kind and state, a map node's share, and C-T0 §9.4's per-tree garbage estimate and entry count, which live with it | A-7b |
```

**K10a.** `docs/R-1-memory.md` — R-1-memory §3, the root-table row (Doc block 3).

Old:
```text
| the root table | `R_file` | `O(trees)`, about 64 bytes per tree | the open charges each entry as step 2 builds it, and one per CreateTree op that pass 2 will apply, before it sizes `S` and `F` (§2.2, §5.2); after the open, a CreateTree moves its entry's bytes from `S` into `R_file` before its gated operation, or its DDL statement fails with 53200; a DropTree's last step returns them | S-STORE §6.8, §14 step 2 | A-7b |
```
New:
```text
| the root table, with C-T0 §9.4's per-tree garbage estimate and entry count | `R_file` | `O(trees)`, about 64 bytes per tree | the open charges each entry as step 2 builds it, and one per CreateTree op that pass 2 will apply, before it sizes `S` and `F` (§2.2, §5.2); after the open, a CreateTree moves its entry's bytes from `S` into `R_file` before its gated operation, or its DDL statement fails with 53200; a DropTree's last step returns them, and so does the end of a transaction whose CreateTree is not applied | S-STORE §6.8, §14 step 2; C-T0 §9.4 | A-7b |
```

**K10b.** `docs/R-1-memory.md` — R-1-memory §5.2, "The root table" (Doc block 5).

Old:
```text
a DropTree's last step returns them to `S`, a credit, which M2 allows.
```
New:
```text
a DropTree's last step returns them to `S`, a credit, which M2 allows, and so does the end of a transaction whose CreateTree is not applied (an abort, or a `ROLLBACK TO` before it), so that no unapplied DDL leaves bytes in `R_file`.
```

**K8a.** `docs/R-1-memory.md` — R-1-memory §5.2, "The residual" (Doc block 5).

Old:
```text
- **The residual.** The allowance covers two growths. If sessions hold every free byte of `S` while the file grows three times, the third growth finds the allowance short, raises 53200 inside a gated operation and poisons the store (S-STORE §9.2). The remedy is a larger budget (§14, RM-T2).
```
New:
```text
- **The residual.** The allowance covers two growth steps' bitmaps and one MapInner path. If sessions hold every free byte of `S` while the store draws more than that on it, the draw that finds it short raises 53200 inside a gated operation and poisons the store (S-STORE §9.2): at the latest the third growth, and sooner where LPID allocation outruns the file, which extends the LPID bitmap and the MapInner pages with no growth (a file whose free slots outnumber its free LPIDs, as after rewrites in two checkpoint intervals), or where one step's LPIDs need more MapInner pages than one path (files above about 127 GiB at 8 KiB pages). The remedy is a larger budget (§14, RM-T2).
```

**K8b.** `docs/R-1-memory.md` — R-1-memory §14, RM-T2 (Doc block 14).

Old:
```text
Three file growths while sessions hold every free byte of `S` poison the store with 53200 at the third.
```
New:
```text
Draws on the growth allowance past two growth steps' bitmaps and one MapInner path, while sessions hold every free byte of `S`, poison the store with 53200: at the latest at the third growth, sooner where LPID allocation outruns the file or one step needs more MapInner pages than one path (§5.2).
```

**K9a.** `docs/R-1-memory.md` — R-1-memory §4.5, "What it bounds" (Doc block 4).

Old:
```text
A tree written under a larger pool may be taller; a read of it gets 53200 (S-STORE §19 T8).
```
New:
```text
A tree written under a larger pool may be taller; a read of it gets 53200, and a gated operation that descends it (a write, recovery's replay of one, a GC sweep step or a drop-walk step) poisons the store (S-STORE §19 T8).
```

**K9b.** `docs/S-STORE.md` — S-STORE §19 T8 (E24's New).

Old:
```text
a read of a taller tree, written under a larger pool, gets 53200 and poisons nothing.
```
New:
```text
a read of a taller tree, written under a larger pool, gets 53200 and poisons nothing, while a gated operation that descends it (a write, recovery's replay of one, a GC sweep step or a drop-walk step) poisons the store (§5.4).
```

## Acceptance-line changes (card numbering; tried in scratch)

- **Line 38** becomes:
```text
cd nucleus/v3 && test "$(git diff --numstat nucleus/v3-scaffold...HEAD -- docs/S-STORE.md | cut -f1,2 | tr '\t' ' ')" = '37 18'
```
- **Line 39** keeps its command, and its hash becomes `ea04fa141a4d008cec75aafbaec46567119baa0c4960e371f9732cd9d384c363`.
- **Two new lines**, inserted after line 49, so the gate line becomes line 52. Neither contains a backtick.
```text
cd nucleus/v3 && grep -q 'A per-account cap above .S. is not an error: once the open has sized .S. (§2\.2), it lowers every such cap to .S.' docs/R-1-memory.md && grep -q 'A read-only open keeps the replay.s dirty frames, and with them the MapLeafs that S-STORE §4\.1.s pins hold for them' docs/R-1-memory.md && grep -q 'or until C-T0 §8.s spill moves them to disk' docs/R-1-memory.md && grep -q '^| SIREADs | per transaction, .ssi_mem. | §6\.2 | v7\.8.s escalation (kept by C-T0 §8), then 53200; under pressure on .S., the spill to disk' docs/R-1-memory.md && grep -q '.M. also holds the walk.s record buffer' docs/R-1-memory.md && grep -q '^| amcheck.s sets (S-STORE §14 step 7) |' docs/R-1-memory.md && grep -q '^| a statement.s working memory: ' docs/R-1-memory.md && grep -q 'and so does the end of a transaction whose CreateTree is not applied' docs/R-1-memory.md && grep -q 'R_file..s terms from meta .k. (§2\.2 item 2), its root table' docs/R-1-memory.md && grep -q 'so that no open of the form grows the file' docs/R-1-memory.md && grep -q 'so that .S. is .S_min. (.σ × B. is below it) and the pool 32 frames above its minimum' docs/R-1-memory.md && grep -q 'at the latest the third growth, and sooner where LPID allocation outruns the file' docs/R-1-memory.md && grep -q 'a gated operation that descends it (a write, recovery.s replay of one, a GC sweep step or a drop-walk step) poisons the store (S-STORE §19 T8)' docs/R-1-memory.md
cd nucleus/v3 && ! grep -n 'a per-account cap above .S.; .n_c = 0.\|so its .F. must exceed .F_min. by them, or\|(kept by C-T0 §8), then 53200 | C-T0 §8 |\|while the file grows three times, the third growth finds\|Three file growths while sessions hold\|0 when the open replays no chunked transaction\|B = B_min + 4 MiB' docs/R-1-memory.md && grep -q 'the MapLeafs that §4\.1.s pins hold for them, which a read-only open never releases' docs/S-STORE.md && grep -q 'a read-only open, which evicts no dirty page, counts such MapLeafs with its dirty frames instead (§14\.1)\.' docs/S-STORE.md && grep -q 'the root-split hook sees the tree reach .H + 1. levels, and the insert that fails is a later one, at its descent' docs/S-STORE.md && grep -q 'while a gated operation that descends it (a write, recovery.s replay of one, a GC sweep step or a drop-walk step) poisons the store (§5\.4)\.' docs/S-STORE.md && ! grep -q 'the tree reaches .H + 1. levels and no insert fails' docs/S-STORE.md
```
- **Tried.** I committed the corrected files in scratch on 71ecf3e6 (scratch commit 42bcf9c0, a throwaway). There, all 51 non-gate lines pass under both locales: the 49 old ones with 38 and 39 changed as above, plus the two new ones. At 32f68472 the two new lines fail, and so do 38 and 39 with the new values. Line 37's checker is unchanged: K adds no row, test or mutant. The rework recomputes the Notes' dry-run sha256s and the author's evidence, and its count of Acceptance commands becomes 52.

## Card text the rework must update (beyond the blocks)
- **Blocks.**
  - K1–K10's R-1-memory text goes into doc blocks 1, 3, 4, 5, 6, 7, 9, 12 and 14.
  - E6's, E8's, E24's and E25's News take K2e, K6, K9b and K2f.
  - The new block for K2d is added; Limit 2's and Work 3's "E1–E25" name it.
- **Findings, Decisions and "For the gate"** gain D1–D4's points:
  - the caps are lowered at open;
  - a read-only open counts the MapLeafs its dirty frames pin;
  - SSI spills, as C-T0 says;
  - four more consumers are listed.
- **Mutation evidence.** HEIGHT-SPLIT-UNCHECKED's row reads like K6. The author's evidence, the Notes' hashes and the command count are recomputed.
- **Sources.** "Item 22, P-1 (accepted 2026-11-16)" overstates the plan's record. The plan says P-1 was "accepted by the conditional reply" and that "the reply's condition is not shown to be met for this point", and that the 16:0x delegation did not cover it. Say that instead. The committed docs say only "owner point P-1's figure", which is accurate.
- **Open points.** P-R2's and P-R5's plain words take this verdict's facts (below).
- **Notes.**
  - Add "If S-ZETA, S-ETA or S-THETA merges first", as those cards' Notes describe for R-1-SPEC.
  - Correct the (c)(4) nit: about 2.4–2.7 times `B_min`, not four.
- **"For the plan card"** gains F1–F7.

## Tests and mutants, test by test
Every test below names its hooks and held locks and waits on a hook or a count, never on time. Each is judged on the card that builds its code: line 37 for the Phase A rows, and my own reading.
- **`t_pool_minimum`** (A-3b) is deterministic, and the test computes R-1's minimum itself. Under MIN-RMAP-DENSE the build's minimum is `n(H_min + 2) + R_EPOCH` frames smaller, and under MIN-RESERVE-NO-STEP2 `2s` frames smaller. Both accept the pool one frame below the minimum: caught.
- **`t_pool_floor_scattered`** (A-3c on S3; A-4a in full) passes on a correct build, as point (b) shows. Its wait hook sits where every build blocks for an unpin, and MAPPIN-KEPT fires it.
- **`t_height_bound`** (A-4a; A-7f in full) passes on a correct build. HEIGHT-SPLIT-UNCHECKED is caught by the root-split hook. Its stated failure, "no insert fails", is false: the next insert's descent check returns 53200 (K6). HEIGHT-DESCENT-UNCHECKED is caught by the read in (b), in both forms.
- **The floor variant of `t_epoch_reserve_two_exhausters`** (A-3c, A-7a). A correct build has two unlatched reserve frames. RESERVE-PER-THREAD-ONLY leaves none, and the run fails by the step budget. CLAIM-DEPTH-TWO fails through the reserve's latch hook. The hooks release no one before the last first touch completes.
- **R-1's own tests.**
  - `t_mem_heap_accounted`, `t_mem_caps` and `t_mem_admission` pass on a conforming build, and each named mutant fails them.
  - `t_mem_account_returns` passes and catches its mutant. It has no case for an aborted CREATE (G3, F6).
  - `t_mem_store_paths_saturated` passes, given K5 and, at `S = S_min`, K1's lowering.
  - `t_mem_growth_allowance` passes, reading the allowance's size as recomputed for the next two steps after each growth.
  - `t_mem_open_sizes` fails a correct build as committed (D1) and passes with K1. Its fall-back form passes with K4.
  - No test samples an open that replays records (G2), and none checks the read-only budget with scattered dirty pages (G1). F1 and F6 add them.

## Rulings on P-R1 to P-R6 (the gate's part)
- **P-R1.** Technically right. `R_MAP = n(H + 2) + R_EPOCH`, with E3's premise, is enough for a read-write store and is needed in the worst case the card names. Three paths per thread are too few, and six buy nothing. A read-only open needs K2. The memory cost is the owner's (below).
- **P-R2.** The rule is enforceable and consistent with S-STORE §9.2. Recovery replays the failed commit under a larger budget. A fixed limit would be worse. The text needs K6 and K9. The directory note is an acceptable residual, with F4.
- **P-R3 and P-R4** are the owner's. The gate's only technical point is D1: the default caps must be lowered at small budgets or `B_min` does not open (K1).
- **P-R5.** The residual is real, and recovery under a larger budget clears it, but its stated trigger was too narrow (D9). K8 states it as it is. Accepting it is the owner's choice.
- **P-R6.** Technically required: `n_c` is a term of `F_min`, so an uncapped count would make the floor false. The default numbers are the owner's.

## Owner points, in plain words (stated here, not decided)
- **P-1 and P-R1, memory per connection.** Each thread or connection that can hold pages needs room for four tree paths of pages: about 224 KiB per thread at 8 KiB pages and a height floor of 5. P-1, the earlier and smaller figure (168 KiB, three paths), has no answer in the owner's own words. The plan records it as accepted only by a conditional reply whose condition "is not shown to be met", and the 16:0x delegation did not cover it. At 8 embedded connections the floor is about 2 MiB of pages. The total minimum is about 8.3 MiB for a 1 GiB file.
- **P-R2, the height bound.** The memory budget decides how tall an index tree may grow. A tree that would grow past the bound stops the database. It is reopened with a larger budget and nothing committed is lost. At the default budgets the bound is far beyond any real table. Near the minimum budget it is 5 levels, and three things follow:
  - A table whose keys are near the maximum size and share long prefixes can reach it after a few hundred to a few thousand rows.
  - A database reopened with a smaller budget than it was written under answers "out of memory" when a tall tree is read.
  - It stops on the first write, recovery replay, garbage-collection pass or drop cleanup of that tree, even with no client write.

  The alternative, letting fewer connections into the store as trees grow, never stops it, but costs a shared counter on every storage call.
- **P-R3 and P-R4, budgets and thresholds per mode.**
  - Budgets: server 25% of memory (at least 256 MiB), embedded 64 MiB, browser 32 MiB, a quarter of each for sessions.
  - Per-transaction caps as listed in R-1-memory §7.1.
  - With K1, at small budgets the store lowers any per-session or per-transaction cap above the session pool to fit it, and reports that. So a small database fails a very large transaction sooner, with "out of memory", instead of refusing to open.
  - Embedded and browser databases switch big transactions to coarse locks sooner.
- **P-R5, the growth residual.** The database stops if sessions hold every byte of session memory while the file's bookkeeping grows past its reserve. That is the third file growth in a row at the latest. It can come sooner: when the file has more free space than free page ids, as after heavy rewrites, or on files above about 127 GiB at 8 KiB pages. Nothing committed is lost, and a reopen with a larger budget clears it. F2 would make it exactly "three growths".
- **P-R6, the connection cap.** An embedded app can open 8 connections by default, a browser 2. One more gets "too many connections". The cap is what keeps the memory floor true, and the app can raise it.

## Merge order with S-ZETA, S-ETA and S-THETA, and S-THETA's pool rules
- **Their Notes are right.** Only the status head and the Changelog head overlap with R-1-SPEC, by design, and the shared anchors work in either order.
  - I parsed all four cards' S-STORE blocks with my own code and composed them on 7.15 in all 24 orders. Every Old applies once at its turn in every order.
  - Each pair's two orders differ only at line 3 and the top Changelog entries.
  - With K2d, K2e, K2f, K6 and K9b added to R-1-SPEC's set, all 24 orders still apply. No block of the other three mentions "Pool budget", the evictable part, `R_MAP`, or the other sentences K changes.
  - Whichever card merges later takes the mechanical rebase that card's Notes describe: draft 7.17 or 7.18, the base hash, the status chain, its counts, its checker's mutant set and base, and its numstat and hash lines.
  - R-1-SPEC's own card has no such Note for S-ZETA, S-ETA or S-THETA (it predates them). The rework adds one, as listed above.
- **S-THETA's pool rules change nothing in R-1's accounting.**
  - A load's MapLeaf pin is in the path term, as S-THETA argues.
  - A frame being loaded is pinned, so it is (T).
  - A freed frame is clean and unpinned, so it is (U), and its MapLeaf pin ends, which only shrinks (Mu).
  - A creation that waits for a load under its tree latches waits for no page read and obtains no frame.
  - "One frame per page" removes duplicate frames.
  - R-1-memory §5.3's "drops every frame" at a fall-back is S-THETA's (b) exactly.
- **One pointer is due after both merge:** R-1-memory's "step budget" (`t_mem_caps` (x)) should point to S-THETA's definition in S-STORE §15 (F5).
- **S-ETA.** Its 22023 for invalid parameters matches R-1-memory's use, and its `wal_seg_size` floor needs nothing more from R-1. R-1-memory §6.1 already lowers `preapply_bytes` to `wal_seg_size / 4`.

## Merge timing (for the record)
By P0 re-check 1 this card may be built and gated on a branch but may NOT merge before P0 PASS unless the owner says so in their own words.

## Follow-ups
- **R-1-SPEC-F1 (S-STORE spec card, before A-7d is cut).** The read-only budget gets a test and a mutant.
  - `t_ro_open`, or `t_ro_open_disk_full`, gains a form whose replay dirties pages under MapLeafs of their own.
  - A pool at `F_min` plus the dirty frames plus those MapLeafs opens. One frame below fails with 53200 and writes nothing.
  - Mutant **RO-BUDGET-DIRTY-ONLY** (the budget counts the dirty frames only): the replay waits for an unpin within the step budget, or the open succeeds at a budget the rule refuses.
  - Judged on A-7d. The rework may instead carry it now, with line 37's counts updated.
- **R-1-SPEC-F2 (R-1 spec, before R-1's Phase B card).** Size the growth allowance so that "the third growth" is exact:
  - size `R_file`'s LPID bitmap and MapInner terms for `max(next_lpid, slot_count)` LPIDs, plus a bound for the pool's never-written pages (at most `F`, bounded before the sizing by the largest pool);
  - make the allowance's MapInner term follow each step's LPIDs (`⌈g / (page_size × f²)⌉` plus a path per step) rather than one path;
  - recompute §4.6 and §7.2.
- **R-1-SPEC-F3 (R-1 and A-7c, before A-7c is cut).** S-STORE §13 step 2 derives image `k`'s slot bitmap "from its built map" and then serialises "both bitmaps of image `k`". If that needs a copy of the two bitmaps beside the current ones, about `⌈slot_count / 8⌉ + ⌈next_lpid / 8⌉` bytes inside a checkpoint step (a poisoning context), reserve it in `R_file`. Otherwise state how step 2 avoids it, for example by building it in the pending-free generation that is idle between step 6s. Today neither is said.
- **R-1-SPEC-F4 (R-1 spec, before R-1's Phase B card).** The open refuses with 53200, writing nothing, when the directory, which step 2 has descended, is taller than the sized pool's `H`. Today it opens, and the store fails at the first directory change or replayed CreateTree.
- **R-1-SPEC-F5 (the next S-STORE card; pointers only).**
  - S-STORE §14 step 3b names only `RequireCkpt`. Point it to R-1-memory §5.2, which counts the CreateTree ops while step 3b reads the records, and point §6.8's CreateTree to §5.2's move of bytes from `S` to `R_file`.
  - After S-THETA merges, R-1-memory's "step budget" points to S-STORE §15.
- **R-1-SPEC-F6 (R-1 spec, with or before R-1's Phase B card).** Tests for the corrections that have none:
  - `t_mem_open_sizes`' report shows every cap at most `S` at `B_min` (K1);
  - a `t_mem_heap_accounted` form samples after an open whose replay reads a record of `wal_seg_size / 4` (K7, G2), and another after amcheck;
  - `t_mem_account_returns` gains an aborted CREATE TABLE and a `ROLLBACK TO` past one, after which `R_file` is back at its value (K10, G3).
- **R-1-SPEC-F7 (S-STORE question; depends on P-R2).** A GC sweep step or a drop-walk step whose descent would pass `H` could skip the tree with a report rather than poison the store. §9.2's reason for poisoning before a mutation, a job retrying forever, does not hold for a skip that waits for a reopen with a larger budget. Today a store reopened under a smaller budget can stop by itself (K9).

## Not verified
- No code exists. Every test and mutant above was walked as text, and the Phase B tests' passability rests on reading.
- The provisional constants are not measured: `o_frame` 128, 64 bytes per root-table entry, `e_lock` 128, the 1/32 slope. I checked that the rules, not these numbers, govern.
- Whether step 2 at a checkpoint holds image `k`'s bitmaps beside the current ones (F3) is undecided here.
- That S-2b will state SSI's spill as v7.8 §8.5 promises: S-2b is not written.
- The "billions of trees" estimate for a directory of height 6 is arithmetic on assumed cell sizes, not measured.
- Which two files fresh-gate matched "through their eol conversion" (the script prints only a count); the check passed.
- The implementer's own log: none exists for R-1-SPEC in `cards/logs/`. Its reported results were checked against the commit, not against a log.
- C-T6-rehost and C-T0 v7.8 were read only where cited.

<!-- gate 2's verdict (22722c9d) ends -->

<!-- gate 3's verdict (e7703ea2) ends -->

<!-- gate 4's verdict (0ea08b61) ends -->
