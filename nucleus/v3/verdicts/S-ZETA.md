Card: S-ZETA
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session e8fb90c5-c354-49fb-b11f-fe1e0ef95f4f (gate 3, a narrow re-gate after the card was moved onto S-ETA's draft 7.17: this agent wrote neither the card S-ZETA, its implementation 5c62470b, the rework b84d5c03, the move 278f7b64, nor gate 1's or gate 2's verdict, and is not the orchestrator)
Date: 2026-11-20
Gate-exit: GATE EXIT=0
Commit: 278f7b6457af9ec88aac568fdf65d6b4389fa599
Mutants: spec text, none in code. Omit-one on 7 of the 22 blocks, each built by my own parser on 7.17 (adbe6a16's file) with one block left out, committed in a scratch clone and judged by the card's 31 non-gate Acceptance lines in both locales: Z2 -> 8, 25, 27, 29, 30, caught; Z5 -> 11, 25, 29, 30, caught; Z9 -> 14, 26, 27, 30, caught; Z12 -> 17, 25, 29, 30, caught; Z14 -> 19, 25, 29, 30, caught; Z17 -> 21, 25, 27, 28, 29, 30, caught; E2 -> 3, 4, 5, 6, 29, 30, caught. One planted error: Z2's pre-move "`wal_seg_size` not 0;" put back -> 8, 30, caught. The named mutants' judgements are unchanged from gates 1 and 2 (History): Z1 and Z3 to Z20 are byte-identical to the blocks gate 2 judged, and Z2's floor touches no table-form symbol
Followups: S-ZETA-F1, S-ZETA-F2, S-ZETA-F3, S-ZETA-F4

## Verdict in brief

**MERGE-WITH-FOLLOWUPS** on the merge commit `278f7b6457af9ec88aac568fdf65d6b4389fa599`, which moves S-ZETA onto S-ETA's draft 7.17 as draft 7.18.
- **The move changes no meaning beyond Z2's floor.** All 22 Olds and 19 of the 22 News are byte-identical to the blocks gate 2 judged. E1 and E2 change only their draft number and counts. Z2 changes exactly as the card's Notes ("If S-ETA merges first") prescribe: `` `wal_seg_size` not 0; `` → `` `wal_seg_size` at least 212 (§12.1); ``.
- **The resolved file is 7.17 with S-ZETA's blocks applied**, byte for byte by my own parser (sha256 `79cd8027…bcb9`), and the reverse gives 7.17 back. No S-THETA or S-ETA text is altered.
- **Z2's change is right.** §2.2's well-formedness and S-ETA's §12.1 floor now agree, and every restatement of the domain in 7.18 says 212. The card's own "Terms." prose still says "not 0". That is card text, not spec, but it matters for card A-2-0-F1's code, so it is the new follow-up S-ZETA-F4.
- **The checks pass.** The fresh gate gives `GATE EXIT=0` on the first run. The 31 non-gate Acceptance lines pass in both locales, and the checker prints `OK`. S-ZETA's rules neither restate nor contradict S-THETA's or S-ETA's in a way that matters: XX001's two kinds, §14.2's rows and step 1's precedence over S-ETA's later 58030 all agree.
- **Carried and answered.** Gate 2's three follow-ups carry unchanged. OP-1 and OP-2 have been answered by the owner for newer-version files, and their rider goes to card S-IOTA.

**Merge timing.** The owner's own words of 2026-11-20 (QUEUE head, `# OWNER DECISION 2026-11-20`) say: "The spec cards S-ETA, S-THETA, S-ZETA and R-1-SPEC may merge before P0 passes, and P0 is then judged on the merged spec." So this card may merge now.

## Gate run

- **Load check.** At 21:39 the load average was 33.55 / 36.72 / 38.58, on a 16-CPU host with other projects' builds running. I waited, polling every 30 s, until the 1-minute average fell below 16. It read 15.59 at 21:57, and 14.26 / 24.10 / 29.74 at 21:57:59.
- **Scaffold extract** (GATE-PROMPT step 1): the shallow `--no-local` clone of `nucleus/v3-scaffold` printed **9bbc77abaf19b96925be6322226efde32eb3e042**. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **The run:** `bash "$fg/fresh-gate.sh" 278f7b6457af9ec88aac568fdf65d6b4389fa599`, from `~/Workspace/neutron-v3`, with `PATH=$HOME/.cargo/bin:$PATH` and `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate3-S-ZETA`. Stdout and stderr went to separate files under `~/.cache/nv3-review/gate3-S-ZETA/` (`gate1.out`, `gate1.err`). It ran 21:58 to about 22:21. The 1-minute load, sampled each minute (`load1.log`), was 13.60 at the start, rose to a peak of 62.94 at 22:07 when other projects' jobs started, and was 20.65 at 22:21. Its lines:
  - `fresh-gate: commit 278f7b6457af9ec88aac568fdf65d6b4389fa599`
  - `fresh-gate: base 9bbc77abaf19b96925be6322226efde32eb3e042`
  - `fresh-gate: checks passed (4883 files hashed in 4.54 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`
  - The literal last line was **`GATE EXIT=0`**, the exit status 0, and stderr was empty (0 bytes). No test failed.
- **Flakes.** None of the known flakes occurred:
  - The test step exited 0, so neither `undo_order.rs` `readers_never_see_past_their_snapshot_during_apply` nor `sim_env.rs` `sim_halted_off_task` failed.
  - ENF-TIMING-1's two self-tests printed `ok   premise: GUC: a -c core.untrackedCache=true beats the environment` and `ok   runner refs: RV cost ten honest moved refs verify in under 2 s`.

  No re-run was needed. `$fg` was removed afterwards.
- **The worktree** `~/.cache/nv3-cards/S-ZETA`, branch `card/v3-S-ZETA`, was clean at tip 278f7b64 before this commit (`git status --short` empty), and was never written except by this verdict. The omit-one variants used a `--shared`, sparse scratch clone, `~/.cache/nv3-review/gate3-S-ZETA/omit`, which was reset to 278f7b64 after each variant.
- **The branch:** 71ecf3e6 → 5c62470b (the card, 7.16) → 6d4465ca (gate 1's REWORK) → b84d5c03 (rework 1) → 512ea1f8 (gate 2's MERGE-WITH-FOLLOWUPS) → 278f7b64 (the merge of adbe6a16, the move, by a GLM 5.3 implementer) → this verdict.

## Scope 1: the merge commit

- **Its parents and tree.** The parents of 278f7b64 are 512ea1f8 (gate 2's verdict) and adbe6a16 (the scaffold tip after S-ETA's merge). `git diff --stat adbe6a16 278f7b64` lists exactly two files: `nucleus/v3/docs/S-STORE.md` (+36/−19 against 7.17, as against 7.15 at gate 2) and `nucleus/v3/verdicts/S-ZETA.md`. The verdict file is gate 2's, unchanged: `git diff 512ea1f8 278f7b64 -- nucleus/v3/verdicts/S-ZETA.md` is empty. Touch only is `nucleus/v3/docs/S-STORE.md`, and no protected path is touched. The scaffold tip has since moved to 9bbc77ab (A-6a, pass 171). A-6a touched only `crates/nucleus-store/src/wal/`, `src/lib.rs`, `tests/wal_codec.rs` and `verdicts/A-6a.md`. Its `docs/S-STORE.md` is still 7.17 (`2777e5d7…91dd`), and adbe6a16 is an ancestor of it.
- **The bytes, by my own parser.** I wrote a fence-aware parser for the card's `## Edits` section (`~/.cache/nv3-review/gate3-S-ZETA/bin/blocks.py`). It read 22 blocks: E1, Z1 to Z20, E2. I applied them in order to adbe6a16's `docs/S-STORE.md` (S-ETA's 7.17, sha256 `2777e5d739d4deacc0fb7f73e5abc3a9d927798baf236e51575e80cfb31391dd`), with `<date>` = 2026-11-20, the date the result carries in both its status clause and its Changelog head.
  - Every Old occurs exactly once at its turn, in file order.
  - The result is byte-identical to 278f7b64's file, sha256 `79cd8027141ef0bf3f4fb0640f5961afce38a4165bf76fdd96fbc725f28bbcb9`.
  - The reverse (E2 down to E1, New to Old) gives adbe6a16's file byte for byte.
  - The same parser on the pre-move card backup (`~/.cache/nv3-review/S-ZETA.card-backup-pre-717.local.md`, sha256 `cbf75339…2495`, the card gate 2 judged), applied to 7.15 (`a1376c94…700f`) with `<date>` = 2026-11-17, gives b84d5c03's file, sha256 `6c186f8975845278151c2f34465a38d5f13ec847f51609ed63a092b1d80025c4`, which is also 512ea1f8's. So the parser reads both cards the same way, and the blocks it compares are the ones gate 2 judged.
- **S-ZETA's meaning is unchanged apart from Z2's floor.** I compared the blocks of the pre-move backup and the moved card one by one. All 22 Olds and 19 of the 22 News are byte-identical. Three News differ, word by word, only as follows:
  - **E1:** `7.16` → `7.18` (the draft number in `Status: draft 7.18 (card S-ZETA,`).
  - **E2:** `7.16**` → `7.18**` (the heading); the Counts bullet `47` → `48` rows, `234` → `256` mutants, `48` → `53` tests, `158` → `168` §15.1 rows; and "as at drafts `7.14` and `7.15.`" → "`7.16` and `7.17.`" (Phase B only, 24).
  - **Z2:** `` `wal_seg_size` not 0; `` → `` `wal_seg_size` at least 212 (§12.1); ``. This is exactly the card's Notes' "If S-ETA merges first" text, which says "its New's "`.wal_seg_size.` not 0;" becomes "`.wal_seg_size.` at least 212 (§12.1);"". The dots in the Notes are regex wildcards for the backticks.
- **No S-THETA or S-ETA text is altered.** I diffed 7.15 → 7.17, which is S-THETA's and S-ETA's changes (98 lines of 7.17 touched), and 7.17 → 7.18, which is S-ZETA's (21 lines of 7.17 touched). The two sets meet only at line 3 and line 1708:
  - **Line 3** is the status line. E1 replaces only its prefix `Status: draft 7.1`, and Acceptance line 2 (passes) checks that the tail after `) on draft 7.17 (card S-ETA, ` equals the scaffold's.
  - **Line 1708** is S-ETA's Changelog head `- **Draft 7.17**`. E2 inserts above it and keeps it, and Acceptance line 5 (passes) checks that the Changelog from that head down equals the scaffold's.
  - No other changed line of S-ZETA's is on or next to a line S-THETA or S-ETA changed.

## Scope 2: Z2's change is right, and every restatement of the domain agrees

- **The floor agrees.** S-ETA's §12.1 ("Files") says that `wal_seg_size` is at least 212, four 53-byte record headers, and that "An open whose file's identity (§2.2) carries a smaller value fails with XX001 before it reads the WAL, and writes nothing". Under 7.18's §2.2, a page 0 or a set of shared identity fields with `wal_seg_size` below 212 is not well-formed, and the open fails with XX001 at §14 step 1, which is before the WAL, writing nothing. So the two rules give the same outcome on the same input, and S-ZETA's comes first. With "not 0" (the pre-move text), a page 0 carrying 1 to 211 would have been well-formed for §2.2 but refused by §12.1. That disagreement was harmless, since both refuse with XX001, but it would have made "well-formed" weaker than the spec's own domain. Z2 removes it.
- **Every restatement in 7.18.** I grepped for `wal_seg_size` and `212` and read each hit:
  - §2.1's record table: "at least 212 (§12.1)".
  - The error table's 22023 row: creation below its floor.
  - §2.3: creation's 8 MiB floor, or 212 in the store's tests.
  - §12.1: the 212 floor and the open's XX001.
  - §14.2: the row "The file's identity carries a `wal_seg_size` below 212 (§12.1) | XX001, nothing written | XX001".
  - §15.1: `t_wal_seg_size` on A-7c (8 MiB and 212) and A-7d (211, both modes).
  - S-ETA's Changelog entry.
  - Z2's new sentence.

  All agree with 212. No "not 0" remains anywhere in 7.18.
- **The §14.2 overlap.** S-ETA's `wal_seg_size` row and S-ZETA's new refusal row ("An intact page 0 that is not well-formed, or … an intact copy with the file's `creation_uuid` that is not well-formed …") both cover a page 0 with `wal_seg_size` 211. Both give XX001 in both columns and write nothing. This is a restatement, not a contradiction.
- **The card's "Terms." prose** still says "`wal_seg_size` not 0", and says "The first seven are card A-2-0's Work 3 list, exactly". G-2 says "It is A-2-0's list exactly", and "Card text for the orchestrator" says "(A-2-0 Work 3's list, with §14 step 1's …)". My ruling: **this is a card-text follow-up, and it does not matter to the spec.** The card's prose does not merge. The normative text is Z2's New, which the file carries and Acceptance line 8 pins: the moved Z2 passes line 8, and the pre-move "not 0" planted into the result fails lines 8 and 30. It matters for one later card, though. A-2-0's merged code checks `wal_seg_size == 0` (`crates/nucleus-store/src/meta.rs:344`), so card A-2-0-F1, which implements this rule, must use §2.2's 212, not A-2-0 Work 3's "not 0". The orchestrator's sentence for that card, taken verbatim from this card, would point it at the wrong list. This is S-ZETA-F4.
- **Nothing becomes unpassable or unkillable.** A-2-0's fixtures use `wal_seg_size` 64 MiB (`tests/meta.rs:64`) and 32 MiB (line 1111), both well-formed under 212. Their out-of-domain cases use 0, which is still out of domain. The table form (Z6) names no `wal_seg_size` symbol. S-ETA's SEG-SIZE-UNCHECKED ("an open accepts one below 212") stays killable on A-7d through `t_wal_seg_size`'s forged 211, whichever card's code holds the check.

## Scope 3: the card's other updates

I diffed `cards/S-ZETA.local.md` (sha256 `b21535c0fd5f4cb5a2f2b284b98b389dd34987ab4c9bc4e40aab41affc9fd54e`) against its pre-move backup, word by word. Every change is one that the base change needs:
- **Draft numbers and base.** The title, the Base line (adbe6a16; 7.17's sha256 `2777e5d7…91dd`), the Goal's "draft 7.18", the cases preamble, "The draft target", Edits' preamble, E1, E2, Limits 1 ("before S-ETA's", "7.17's line"), Read, Work 1 and 3, and the reverse (7.19; "draft 7.17 byte for byte"; 248 mutants).
- **The counts.** §15 48 rows, 256 mutants (248 + 8), 53 tests; §15.1 168; Phase B only 24; 87 new since 7.10, with "14 at 7.16, 8 at 7.17". These are S-ETA's 7.17 counts (48/248/53/168, its Changelog) plus this card's 0, 8, 0, 0. The checker prints them.
- **Acceptance.** Lines 1 to 6, 8, 28 and 30 changed, and no others:
  - 1 to 6: the draft numbers, the chain `card S-ETA,` and the Counts numbers.
  - 8: Z2's grep.
  - 28: the checker gains NT and NE and the count tuple `(…,1,14,8,8,87)`.
  - 30: the diff hash, `bce60c6c…5854`.

  The checker's NT (14 names) and NE (8 names) and all their placements, ROOT-PIN-MAP-FIRST's A-3a included, equal those in S-ETA's merged card checker, compared by script.
- **"Mutation evidence"** gained a "rebase dry run" record. It and "The mutant set" text name S-THETA's and S-ETA's placements. The 2026-11-16 dry-run record is kept as history.
- **Not updated, non-blocking (card text only).**
  - "§15 and §15.1: the rows" still ends "No row is added, removed or moved: 158 rows" (168 at 7.18).
  - Notes still say "It may not merge before P0 PASS", which the owner's words of 2026-11-20 supersede (below).
  - The Notes' merge-order paragraphs still give one-predecessor counts on 7.15. The rebase record says so.
  - The `wal_seg_size` prose of Scope 2.

  None of these is in a block. S-ZETA-F4 lists them.

## Scope 4: gate, Acceptance, checker, omit-one

- **Acceptance.** I parsed the card with the branch's `tools/run_card.py` `parse_card` and `validate_card`, extracted with `ast`. They read `S-ZETA`, Touch only `['nucleus/v3/docs/S-STORE.md']`, 32 commands, the last `cd nucleus/v3 && bash scripts/gate.sh`, no container, and `validate_card` returns `None`. I ran the 31 commands before `gate.sh` with `/bin/sh -c` from the worktree root at 278f7b64 (clean, `git status --short` empty). They pass **31 of 31 under `LANG=en_CA.UTF-8` and 31 of 31 under `LC_ALL=C`**. Line 32 is the fresh gate above.
- **The checker** (line 28) prints, in both locales:
  - `168 rows; 256 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`
  - `87 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 14 at 7.16, 8 at 7.17, 8 here), 87 of them judged on a Phase A row; 48 invariants, 53 tests`
  - `OK`
- **Omit-one** (7 of 22 blocks). I built each variant with my parser on 7.17 with one block left out, committed it on 278f7b64 in a `--shared`, sparse scratch clone (`~/.cache/nv3-review/gate3-S-ZETA/omit`, local `nucleus/v3-scaffold` = 9bbc77ab), and ran the 31 non-gate lines in both locales. Every omission is caught, and the two locales agree:

  | Block left out | Failing lines |
  |---|---|
  | Z2 | 8, 25, 27, 29, 30 |
  | Z5 | 11, 25, 29, 30 |
  | Z9 | 14, 26, 27, 30 |
  | Z12 | 17, 25, 29, 30 |
  | Z14 | 19, 25, 29, 30 |
  | Z17 | 21, 25, 27, 28, 29, 30 |
  | E2 | 3, 4, 5, 6, 29, 30 |

  One planted error, run in the default locale only: Z2's pre-move "not 0" put back into the result fails lines 8 and 30.
- **The named mutants.** Their judgements are unchanged from gates 1 and 2 (History). Z1 and Z3 to Z20 are byte-identical to the blocks gate 2 judged. Z2's change touches a field no table-form symbol uses, and the out-of-domain value its tests use (0) stays out of domain.

## Scope 5: S-ZETA's rules against S-THETA's and S-ETA's in 7.18

I grepped 7.18 for XX001, 58030, "well-formed", `creation_uuid`, "page 0", "§2.2", "engine invariant", "poison" and "writes nothing", and read each hit in the other cards' sections.
- **XX001's definitions.** S-THETA's error-table row reads "an integrity check failed, or an engine invariant violation (§9.2)". Its §9.2 item limits engine invariant violations to the engine's own bookkeeping "and … bytes the engine made itself before it writes them", and says "A check of what the device returned … is not one".
  - **S-ZETA's open refusal** (an intact record that is not well-formed) checks what the device returned. So it is an integrity check of the first kind, a refusal at open with no running store to poison.
  - **S-ZETA's writers' check** (Z2, Z11; WRITE-MALFORMED) checks bytes the engine made before writing them. Under S-THETA's §9.2 that is an engine invariant violation. S-ZETA makes it fail creation, and fail-stop at step 4 (§9.2). S-THETA's text gives it XX001 and poisoning, which a failed checkpoint step does anyway. The two agree, and S-ZETA names no other code.
  - **Note for S-IOTA:** when it moves engine invariant violations to XX000 (the owner's "yes" of 2026-11-20), that move covers step 4's well-formedness check, and must not cover the open's refusal.
- **§14.2.** S-ZETA's three rows (two changed, one added) sit in the open rule's block. S-ETA's rows (`wal_seg_size` below 212, malformed record or short segment, WAL read error 58030) are later steps, or the overlap ruled on in Scope 2. They give the same code wherever they meet. S-THETA added no §14.2 row.
- **58030 precedence.** S-ZETA's unlocated 58030 and its refusals are at §14 step 1. S-ETA's 58030 for a WAL read error is at step 3 and in pass 2, and its XX001s (start segment missing, malformed record, short segment) are at steps 3 and 3a. So step 1's outcome always comes first, and neither card's text says otherwise. S-THETA's §9.2 "client reads never poison … every read of a read-only open" concerns a running store, not an open's refusal.
- **Gate 2's S-ZETA-F3** (the torn page 0 with every place unreadable: 58030 by §2.2's first sentence, XX001 by the identity bullet) is unchanged by the move. Both of its Olds still occur exactly once in 7.18. Neither S-THETA nor S-ETA touches those lines.

Nothing here meets the blocking bar:
- the resolution changes no meaning beyond Z2's floor, which the card's Notes prescribe;
- it alters no other card's text;
- it leaves every test passable and every named mutant killable as gates 1 and 2 judged them;
- it introduces no contradiction.

So no Old/New list is needed.

## The P0 question

Gate 1's P0 fact is carried unchanged (History). At 7.15, under the events 7.15 covers, the text is not defective: no covered fault or crash makes an intact record of the file with an out-of-range field. But 7.15 is under-specified, because §2.2 and §14 step 1 are two-minded, and it lacks future-proofing: a later format version gets a silent rollback with loss after a downgrade. That is a due point before A-7d and before 2.0 ships, not a failure of 7.15 under its own model. This gate does not rule on what P0 concludes. Under the owner's decision, P0 is judged on the merged spec.

## Owner points OP-1 and OP-2: answered for newer-version files

The owner answered in their own words on 2026-11-20 (QUEUE head, `# OWNER DECISION 2026-11-20 17:00 MST`): "Yes to: … 55000 and full refusal for newer-version files". That is the card's recommended "yes" to OP-1 (55000 rather than XX001) and to OP-2 (refuse, read-only included, with no override in 2.0), for a file that a newer format version wrote.
- **Still open:** the faulty-build and deliberate-write cases of OP-2 (a record of this file that is not well-formed but carries `format_version` 1). The answer names only newer-version files. 7.18's text refuses those cases with XX001, the no-decision default.
- **Where the rider text goes:** to card S-IOTA (QUEUE row S-IOTA), not to this card. This card merges with the no-decision default (XX001 for every refused record), as gated. OP-1's rider is in the card's Open points: §2.2's two refusal sentences, the 55000 row of the error table, §14.2's new row split in two, and the mutant LATER-FORMAT-AS-CORRUPT on A-2 and A-7d. S-IOTA writes and applies it after S-ZETA merges. I did not apply it in scratch, because it is a description, not an Old/New list.

## Merge timing

The owner's own words of 2026-11-20 (QUEUE head) say: "The spec cards S-ETA, S-THETA, S-ZETA and R-1-SPEC may merge before P0 passes, and P0 is then judged on the merged spec." So S-ZETA may merge now. That replaces gate 2's "may not merge before P0 PASS unless the owner says so in their own words", and the card's Notes' "It may not merge before P0 PASS".

## Follow-ups

S-ZETA-F1 to S-ZETA-F3 carry unchanged from gate 2. Each has its QUEUE row. I re-checked them against 278f7b64:
- **S-ZETA-F1** (O-1's spec card, before A-7d): O-1's Old, §14 step 4a's "The newest image **fails at open** when …", occurs once in 7.18. Neither S-THETA nor S-ETA changed that sentence.
- **S-ZETA-F2** (the later-version rule's limits, for D-8 and §17): this does not depend on the base.
- **S-ZETA-F3** (§2.2's 58030 sentence and §14.2's 58030 row): both Olds occur exactly once in 7.18, so the two-line fix in History, gate 2, applies as written.

One new follow-up, non-blocking:
- **S-ZETA-F4. Card A-2-0-F1 uses §2.2's domain (`wal_seg_size` at least 212), not A-2-0 Work 3's list; S-ZETA's stale card text.**
  - **Card A-2-0-F1.** Since S-ETA merged first, §2.2's well-formedness list is A-2-0 Work 3's list with `wal_seg_size` raised from "not 0" to "at least 212 (§12.1)", plus §14 step 1's two range relations. A-2-0's merged code checks `== 0` (`crates/nucleus-store/src/meta.rs:344`). When the orchestrator writes card A-2-0-F1, the sentence it takes from S-ZETA's "Card text for the orchestrator" should read "S-STORE §2.2's domain list" without "(A-2-0 Work 3's list, …)", or name the floor. A-2-0-F1's table form should also carry a page 0 and a copy of the file's `creation_uuid` with `wal_seg_size` 211, so that a build which checks only "not 0" fails on A-2 and not first on A-7d's `t_wal_seg_size`. Placing S-ETA's SEG-SIZE-UNCHECKED on A-2 as well, if A-2-0-F1's shared check holds the open's floor, is the next S-STORE card's choice.
  - **Card text** (local, does not merge; for whoever next edits `cards/S-ZETA.local.md`, or for P0's reader):
    - "The rule, re-derived", Terms ("`wal_seg_size` not 0"; "the first seven are card A-2-0's Work 3 list, exactly");
    - G-2 ("A-2-0's list exactly");
    - "§15 and §15.1: the rows" ("158 rows", now 168);
    - Notes "It may not merge before P0 PASS" (superseded, above).

## Not verified

- **No code.** The 7.18 rule has no implementation yet; that is A-2-0-F1's work. I did not re-walk the named mutants or re-run gates 1 and 2's table-form models. The blocks they judged are byte-identical except Z2's floor, whose effect is argued in Scope 2, not modelled.
- **The implementer's throwaway-clone route** (merge-card accepting the merge plus a stub verdict; `audit: ok (134 commits)`). I did not re-run it. merge-card judges the real route when it runs on this verdict.
- **Omit-one on the other 15 blocks.** The planted error ran under the default locale only.
- **The card's Notes' merge-order counts** for R-1-SPEC with S-ZETA on 7.17 or 7.18, and whether R-1-SPEC's blocks still apply after 7.18. R-1-SPEC is not this card's scope.
- **The implementer's two red `gate.sh` runs** (ENF-TIMING-1). I did not reproduce them. Only this gate's fresh-gate runs are reported above.

## History

- **Gate 1: REWORK, verdict commit 6d4465ca4693451a1e496d6a5d5610ac0fe60b53, on 5c62470b7bf66431f2976b28836449cf60f7c32c.** Its findings, among them the test gap G-REFUSE-NEWER-ONLY (uncaught by every specified test), came with the fix RW-1 to RW-4. Follow-ups S-ZETA-F1 and F2, owner points OP-1 and OP-2, and the P0 fact.
- **The rework: commit b84d5c03ba3314c31626a93cb391f852390b7ef1**, RW-1 to RW-4 on `docs/S-STORE.md` only.
- **Gate 2: MERGE-WITH-FOLLOWUPS, verdict commit 512ea1f84e0c9be98810637d0cd4441cc31d631e, on b84d5c03.** Against S-STORE 7.15 at scaffold 71ecf3e6. New follow-up S-ZETA-F3.
- **S-THETA and S-ETA merged first** (passes 169 and 170; merges 6e3423f6 and adbe6a16), taking S-STORE to 7.16 and then 7.17. A-6a then merged (pass 171, scaffold 9bbc77ab), with S-STORE unchanged.
- **The move: commit 278f7b6457af9ec88aac568fdf65d6b4389fa599** (a GLM 5.3 implementer). It merges adbe6a16 into the card branch, with `docs/S-STORE.md` resolved to 7.17 plus S-ZETA's 22 blocks as draft 7.18 (sha256 `79cd8027…bcb9`) and Z2's floor as the card's Notes prescribe. The card was updated in the main checkout; its pre-move copy is `~/.cache/nv3-review/S-ZETA.card-backup-pre-717.local.md`. Its two `gate.sh` runs were red on ENF-TIMING-1's self-tests under load 33–46.
- **Gate 3 (this verdict): MERGE-WITH-FOLLOWUPS on 278f7b64.**

**Gate 2's verdict, as committed at 512ea1f8, verbatim.** Its header lines carry a "- " prefix, so that no tool reads them as this verdict's fields. Everything after them is unchanged, including gate 1's verbatim verdict in its own History. Where its sections ("Merge timing", "Follow-ups", the owner points) differ from the sections above, the sections above are current. The same text is at `git show 512ea1f8:nucleus/v3/verdicts/S-ZETA.md`.

<!-- gate 2's verdict (512ea1f8), verbatim, begins -->

- Card: S-ZETA
- Class: spec
- Card-gate: P
- Verdict: MERGE-WITH-FOLLOWUPS
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: T3 review sub-agent, Claude Code session fe4ac879-18e5-48b7-ac50-9730f0dfc01c (independent P gate 2 of S-ZETA, a narrow re-gate after rework 1; it wrote neither the card, its implementation 5c62470b, the rework b84d5c03, nor gate 1's verdict 6d4465ca, and is not the orchestrator)
- Date: 2026-11-20
- Gate-exit: GATE EXIT=0
- Commit: b84d5c03ba3314c31626a93cb391f852390b7ef1
- Mutants: spec text, none in code; the A-2 ones run in this gate's own model of the table form, written from the reworked text and independent of gate 1's (16 symbols, 4 page-0 states, 262,144 cases; ~/.cache/nv3-review/gate2-S-ZETA/tools/model2.py), numbers = cases whose outcome changes with RW-3a's alphabet: G-REFUSE-NEWER-ONLY -> table form (13,952; 0 with 5c62470b's alphabet, gate 1's finding reproduced), MALFORMED-AS-TORN -> table form (61,098), IDENTITY-NO-FV-ALG -> table form (15,222), STATICS-AS-FOREIGN -> table form (4,394), IDENTITY-DOMAIN-SKIP -> table form (1,120), FOREIGN-MALFORMED-REFUSES -> table form (3,446), UNLOCATED-AS-CORRUPT -> table form (256), META-RANGE-SKIP -> table form (14,838), TIE-NO-BOOT-ID -> table form (3,109), TIE-EQUAL-OPENS -> table form (2,092), MISPLACED-SHADOWS -> table form (3,124), IDENTITY-SKIP -> table form (24,302), PAGE0-REQUIRED -> table form (9,664), META-WRONG-SLOT -> table form (19,388), MISPLACED-NEXT-REFUSES -> table form (2,712), MISPLACED-IGNORED -> table form (12,996), G-58030-FIRST -> table form (2), FENCE-BEFORE-REFUSAL -> fault form unlocated variant (byte identity; text unchanged by the rework), WRITE-MALFORMED -> write form (text unchanged by the rework). Text mutants: each RW block left out (RW-1, RW-2, RW-3a, RW-3b, RW-4) fails its own Acceptance line and line 30 in both locales; 5c62470b's file fails lines 8, 9, 12, 14 and 30. All in a scratch clone; the worktree was never written before this verdict
- Followups: S-ZETA-F1, S-ZETA-F2, S-ZETA-F3

## Verdict in brief

**MERGE-WITH-FOLLOWUPS.** Rework 1 (b84d5c03) applies gate 1's RW-1 to RW-4 exactly and changes nothing else. The card now matches gate 1's lists and rebuilds the file byte for byte. The fresh gate gives `GATE EXIT=0`. All 31 non-gate Acceptance lines pass in both locales, and the checker prints `OK`. The test gap behind RW-3 is closed: G-REFUSE-NEWER-ONLY changes 0 of 262,144 table-form cases with 5c62470b's alphabet and 13,952 with the reworked one, in this gate's own model, and every named mutant is still caught.

One new non-blocking finding comes from re-reading the text, and it predates the rework (S-ZETA-F3). §2.2's first 58030 sentence, which RW-2 rewrote, still reads as 58030 for a **torn** page 0 with all four meta places unreadable. The identity bullet and the table form's explicit expectation give XX001 there. §14.2's 58030 row overlaps in the same way. In both cases the open refuses and writes nothing, and the table form's own sentence decides what the test expects. Only the error code is at stake, never a rollback or a refused covered single fault. So this does not block. An exact Old/New list, tried in scratch, is in S-ZETA-F3.

## Gate run

- **Extract.** GATE-PROMPT step 1's shallow `--no-local` clone was made under `~/.cache/nv3-review/gate2-S-ZETA/`. It printed scaffold id `71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`, with no `EXTRACT FAILED` line. The directory was removed afterwards.
- **Run.** From `~/Workspace/neutron-v3`: `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate2-S-ZETA bash "$fg/fresh-gate.sh" b84d5c03ba3314c31626a93cb391f852390b7ef1`, with stdout and stderr in separate files. Stderr was empty, and the script exited 0.
- **`fresh-gate:` lines.**
  - `commit b84d5c03ba3314c31626a93cb391f852390b7ef1`
  - `base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `checks passed (4875 files hashed in 2.40 s)`
  - `2 file(s) matched through their eol conversion`
- **STEP lines.** `test`, `clippy`, `fmt`, `deny`, `wasm32`, `core-paths-selftest`, `enforcement-tests` and `api-surface`, each `EXIT=0`. The literal last line was `GATE EXIT=0`.
- **The flake.** The known spike flake (`readers_never_see_past_their_snapshot_during_apply`) did not occur: one run, green.
- **Scope.**
  - b84d5c03 has one parent, 6d4465ca (gate 1's verdict), and changes only `nucleus/v3/docs/S-STORE.md`, +4/−4.
  - Against the scaffold, the diff is `docs/S-STORE.md` (numstat `36 19`) plus `verdicts/S-ZETA.md`.
  - Nothing touches a protected path. The worktree was clean before this verdict.

## 1. The rework is RW-1 to RW-4, exactly

- **The spec.** My own script (`apply_rw.py`) took the five Old/New pairs from gate 1's verdict (RW-1, RW-2, RW-3a, RW-3b, RW-4) and applied them to `docs/S-STORE.md` at 5c62470b. Each Old occurs once at its turn.
  - The result equals b84d5c03's file byte for byte: sha256 `6c186f8975845278151c2f34465a38d5f13ec847f51609ed63a092b1d80025c4`, the result gate 1 stated (`6c186f89…25c4`).
- **The card** (`cards/S-ZETA.local.md`, sha256 `cbf75339…2495` at gating) against its pre-rework backup:
  - **The blocks.** Lines 131, 142, 175 and 208 are Z2's, Z3's, Z6's and Z9's New blocks. E1, Z1–Z20 and E2, parsed fence-aware and applied in order to the base's 7.15 (`a1376c94…700f`) with the commit's date, rebuild b84d5c03's file byte for byte. With 2026-11-16 they give `70b49911…1bbb`, the dry-run hash the card now states (line 415). The backup's blocks rebuild 5c62470b's file.
  - **Acceptance.** Lines 8, 9, 12, 14 and 30 are character for character gate 1's replacements. No other Acceptance line changed, and line 29 stays `36 19`.
  - **The other edits match gate 1's "Card text the rework must update", item by item:**
    - case 6's cell ("XX001 at 7.15's and this card's reading; A-2-0's reading opens");
    - "For the plan card" item 3's alphabet (`format_version` 2 and `T` above `ts_ckpt_hi` higher, in their own parity; `map_level` 0 and `page_size` 8192 two below);
    - the Notes "S-ETA: a semantic overlap", "S-THETA: overlaps", "If S-ETA merges first" (with Z2's 212 change) and "If S-THETA merges first".
  - **No stale card prose.** No pre-rework form of RW-1 to RW-4 (the higher-only symbol list, "naming a place and", "is foreign (below).", the unconditional 58030 clause, the old hash `1d086ffd…`) is left in the card. The mutant table's claims (MALFORMED-AS-TORN past `map_level` 0, STATICS-AS-FOREIGN past `page_size` 8192, IDENTITY-NO-FV-ALG and META-RANGE-SKIP on the higher symbols) stay true with the stale symbols.

## 2. Acceptance, both locales; omit-one per RW

- **Acceptance.** The 32 commands were extracted from the card by my own scan of the "## Acceptance" section, with no backtick inside any command.
  - The 31 before `gate.sh` were run as `/bin/sh -c` in a `--shared` scratch clone at b84d5c03, with a local `nucleus/v3-scaffold` at 71ecf3e6. They ran once under `LANG=en_CA.UTF-8` and once under `LC_ALL=C`.
  - Result: 31/31 in each locale. Line 32 is the fresh gate above.
- **The checker (line 28).** It prints `158 rows; 234 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `65 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 8 here), 65 of them judged on a Phase A row; 47 invariants, 48 tests`, then `OK`, in both locales.
- **Omit one.** For each RW block, I committed on a scratch branch the 5c62470b file with every RW block but that one, and ran it in both locales. The locales agree every time.

  | Left out | Failing lines |
  |---|---|
  | RW-1 | 8, 30 |
  | RW-2 | 9, 30 |
  | RW-3a | 12, 30 |
  | RW-3b | 12, 30 |
  | RW-4 | 14, 30 |
  | all five (5c62470b's file) | 8, 9, 12, 14, 30 |

## 3. G-REFUSE-NEWER-ONLY and the named mutants, re-run

**The model.** I wrote it from the reworked §2.2, not from gate 1's model (`tools/model2.py`).
- **Places.** Slot 1 and replica 0 are parity 0, whose newest is `k` = 10. Slot 2 and replica 1 are parity 1, whose newest is 9.
- **The 16 symbols:**
  - valid newest, and the same copy written for the other place;
  - valid stale (two below);
  - torn, zeros and unreadable;
  - two misplaced copies: one `ckpt_id` above `k`, and one two or more above it, or in parity 1 a copy of `k` itself;
  - a foreign copy at a higher `ckpt_id`, and a foreign copy with `format_version` 2;
  - the newer-`boot_id` tie copy and the equal-`boot_id` tie copy;
  - the four refused copies.
- **The refused copies.** With RW-3a's form, `format_version` 2 and `T` above `ts_ckpt_hi` sit two above their parity's newest, in their own parity, and `map_level` 0 and `page_size` 8192 sit two below it. With 5c62470b's form, all four sit higher.
- **Page 0** is valid, torn, unreadable, or intact with `format_version` 2.
- **Cases.** 262,144 per alphabet.
- **Spot checks.** Healthy opens 10 with previous 9. Page 0 and both replicas unreadable gives 58030. Page 0 valid with every place unreadable gives 58030. Page 0 `format_version` 2 with every place unreadable gives XX001. Page 0 torn with every place unreadable gives XX001, by the identity bullet (see S-ZETA-F3).
- **Outcomes, the same with either alphabet:** open 20,483, XX001 241,404, 58030 257.

**G-REFUSE-NEWER-ONLY** is a build that defers each refusal and refuses only when the refused copy's `ckpt_id` is at least the newest valid meta's.
- With 5c62470b's alphabet it changes **0** cases, so gate 1's RW-3 finding reproduces.
- With RW-3a's alphabet it changes **13,952**; for example, page 0 valid with `map_level` 0 stale in slot 1 and valid copies elsewhere opens meta 10 where the rule gives XX001.
- So it is now caught by the table form. Gate 1 counted 10,124 with its own model. The two models differ in their misplaced and foreign symbol choices, so the counts differ while the conclusion is the same.

**Every named A-2 mutant is still caught**, with the same count under either alphabet except IDENTITY-SKIP (28,115 with the old one, 24,302 with the new):

| Mutant | Cases changed |
|---|---|
| MALFORMED-AS-TORN | 61,098 |
| IDENTITY-NO-FV-ALG | 15,222 |
| STATICS-AS-FOREIGN | 4,394 |
| IDENTITY-DOMAIN-SKIP | 1,120 |
| FOREIGN-MALFORMED-REFUSES | 3,446 |
| UNLOCATED-AS-CORRUPT | 256 |
| META-RANGE-SKIP | 14,838 |
| TIE-NO-BOOT-ID | 3,109 |
| TIE-EQUAL-OPENS | 2,092 |
| MISPLACED-SHADOWS | 3,124 |
| IDENTITY-SKIP | 24,302 |
| PAGE0-REQUIRED | 9,664 |
| META-WRONG-SLOT | 19,388 |
| MISPLACED-NEXT-REFUSES | 2,712 |
| MISPLACED-IGNORED | 12,996 |
| G-58030-FIRST (gate 1's) | 2 |

- **G-58030-FIRST.** Its two cases are page 0 `format_version` 2 and page 0 torn, each with every place unreadable.
- **FENCE-BEFORE-REFUSAL and WRITE-MALFORMED** are judged on A-7d and A-7c by text the rework did not touch. Gate 1's walk of them stands.

## 4. The reworked text, re-read

- **RW-1.** A misdirect from another file over page 0 now "fails the open, as another file's well-formed page 0 does". This is consistent:
  - an intact page 0 that is not well-formed is refused (XX001);
  - a well-formed foreign page 0 makes every copy of this file foreign, so no candidate remains (XX001);
  - §14.2's rows match.
- **RW-2.** The page-0 refusal now precedes the all-unreadable 58030, as the identity bullet and §14.2's refusal row say. But the new "unless" reads as a complete list of exceptions, and it is not: see S-ZETA-F3.
- **RW-3a and RW-3b.** The other places that list the table's symbols or restate the expectation (§15.1's A-2 row, the mutant entries at lines 152–157, the card's mutant table) name no `ckpt_id`, so none is stale. "whatever its `ckpt_id`" states what the rule's "in any place … its `ckpt_id` cannot be relied on" already requires.
- **RW-4.** "naming page 0 or a place" matches the identity bullet's detail ("detail: page 0, the field and its value") and the copy refusal's ("the place, the field and its value").
- **The restatement search.** Outside the Changelog, I searched the whole spec for "58030", "every place's read fails", "Every meta place unreadable", "unlocated", "higher `ckpt_id`", "`map_level` 0", "four symbols", "naming", "is foreign" and "over page 0", and read each hit in context.
  - None states a pre-rework form of RW-1, RW-3 or RW-4.
  - The one 58030 overlap is S-ZETA-F3.
  - §14 step 1's range-check sentence ("in any place, the open fails with XX001") agrees with RW-3b.
- **The S-ETA, S-THETA and R-1-SPEC cards** contain neither "every place's read fails" nor "Every meta place unreadable", so S-ZETA-F3's Olds are outside their blocks.

**Gate 1's rulings, carried unchanged:**
- **The P0 fact.** At 7.15, under the events 7.15 covers, the text is not defective: no covered fault or crash makes an intact record of the file with an out-of-range field. 7.15 is under-specified, because §2.2 and §14 step 1 are two-minded. It lacks future-proofing: a later format version gets a silent rollback with loss after a downgrade. That is a due point before A-7d and before 2.0 ships, not a 7.15 failure under its own model. The table is in History, gate 1 §4.
- **The owner points** OP-1 (55000 instead of XX001 for a newer-version file) and OP-2 (refuse, read-only included, with no override in 2.0) are in History, gate 1, "Owner points". They are not decided here.
- **"For the plan card"** is as gate 1 left it. In particular, A-2-0-F1's gate re-runs G-REFUSE-NEWER-ONLY and G-58030-FIRST and keeps the new symbols in their own parity.

## Merge timing (for the record)

This card may not merge before P0 PASS unless the owner says so in their own words.

## Not verified

- **No code.** The 7.16 rule has no implementation yet; that is A-2-0-F1's work. Every count above comes from this gate's model of the text, which abstracts each record to its fields and does not reproduce A-2-0's byte layouts or exact `ckpt_id`s.
- **The A-7c and A-7d forms.** They were not re-walked, beyond RW-4's detail wording; the rework did not touch them.
- **Gate 1's other claims.** I did not re-run them: the 22-block omission and the 14 planted errors on 5c62470b, the 200-case covered-fault enumeration, and the PostgreSQL citation.
- **Merge orders.** S-ZETA-F3's Olds were checked against the S-ETA, S-THETA and R-1-SPEC cards by grep only, not by composing the files.
- **The flake.** It was not met, so it was not reproduced.

## Follow-ups

- **S-ZETA-F1. O-1's spec card, before A-7d.** Carried from gate 1, unchanged; the full text is in History, gate 1, "Follow-ups". A page of image `k` that passes §3.3's four checks but that its decoder refuses counts as failing at open. The fall-back, the explicit rollback and the read-only salvage then reach image `k−1`. The card changes §14 step 4a, I-FALLBACK, §14.2's fall-back rows and R18 together, and adds a body-corrupting hook and the mutant FALLBACK-CHECKS-ONLY.
- **S-ZETA-F2. The later-version rule's limits, for D-8 and §17.** Carried from gate 1, unchanged; the full text is in History. It covers §17's D-8 rider (its Old still occurs once at b84d5c03) and D-8's plan row with the three upgrade facts.
- **S-ZETA-F3. §2.2's 58030 sentence and §14.2's 58030 row: a torn page 0 with every meta place unreadable is XX001.**
  - **The conflict.** §2.2 "At open" says: "if every place's read fails, the open fails with 58030 unless page 0 is intact but not well-formed (below)". Read literally, a torn page 0 with all four meta places unreadable gives 58030. The identity bullet gives XX001 there ("if neither holds an intact copy … XX001"), and so does the table form's explicit "page 0 torn with both replica blocks' reads failing gives XX001". That is also the stated rationale for 58030: "the open has seen no damage" is false when page 0 is torn. §14.2's row "Every meta place unreadable, or page 0 and both replica blocks unreadable | 58030" overlaps the row "page 0 and both replicas invalid, not all three unreadable | XX001" in the same case. The summary defers to §2.2 ("where it and they differ, they rule"), but §2.2 is itself two-minded here.
  - **Why it does not block.** The conflict is pre-existing: 7.15 and 5c62470b have it in their 58030 sentence and identity bullet, and gate 1's §2 table lists "page 0 torn with both replicas unreadable → XX001" as pre-existing (card O-3). Both outcomes refuse and write nothing. The table form's own sentence decides what the test expects. A build that follows the literal sentence (58030) is caught by the table form in exactly that case, as one of G-58030-FIRST's two.
  - **The fix,** with the next S-STORE spec card. Each Old occurs once at b84d5c03. The result, tried in scratch, changes exactly those two lines and nothing else. This card's Acceptance line 9 pins the current §2.2 sentence, so it is the fixing card's to update.
    - §2.2:

      Old: `if every place's read fails, the open fails with 58030 unless page 0 is intact but not well-formed (below), and so it does when page 0's read and both replica blocks' reads fail`

      New: `if every place's read fails and page 0 is intact and well-formed, the open fails with 58030, and so it does when page 0's read and both replica blocks' reads fail`
    - §14.2:

      Old: `| Every meta place unreadable, or page 0 and both replica blocks unreadable (§2.2) | 58030 | 58030 |`

      New: `| Every meta place unreadable with page 0 intact and well-formed, or page 0 and both replica blocks unreadable (§2.2) | 58030 | 58030 |`
  - **A-2-0-F1's table form** should assert XX001 for the case of page 0 torn with every place unreadable, by name.

## History

The verdicts of record before this one, newest first. Each keeps its header with every line prefixed by "- ", so `vfield` reads only the header at the top of this file.

### Gate 1 (verdict commit 6d4465ca4693451a1e496d6a5d5610ac0fe60b53, on 5c62470b7bf66431f2976b28836449cf60f7c32c)

Gate 1's verdict, verbatim except that each header line is prefixed with "- " and each "## " heading is demoted to "#### ":

- Card: S-ZETA
- Class: spec
- Card-gate: P
- Verdict: REWORK
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: T3 review sub-agent, Claude Code session 1fd0c91e-03e0-4018-a9b8-755bfca21238 (independent P gate 1 of S-ZETA; it wrote neither the card, its implementation 5c62470b, nor any verdict of A-2-0, and is not the orchestrator)
- Date: 2026-11-17
- Gate-exit: GATE EXIT=0
- Commit: 5c62470b7bf66431f2976b28836449cf60f7c32c
- Mutants: spec text, none in code; each walked on the committed text, and the A-2 ones also run in the gate's own executable model of the table form exactly as Z6 lists it (16 symbols, 4 page-0 states, 262,144 cases; ~/.cache/nv3-review/gate-S-ZETA/tools/mutants.py), numbers = cases whose outcome changes: MALFORMED-AS-TORN -> table form (65,876) and fault form later-format runs 1 and 2, IDENTITY-NO-FV-ALG -> table form (36,786), STATICS-AS-FOREIGN -> table form (4,860), IDENTITY-DOMAIN-SKIP -> table form (896) and later-format run 3, FOREIGN-MALFORMED-REFUSES -> table form (3,860) and the foreign copy's second run, UNLOCATED-AS-CORRUPT -> table form (256) and the unlocated variant, META-RANGE-SKIP -> table form (16,193) and t_cover_ts_cross_check, FENCE-BEFORE-REFUSAL -> fault form unlocated variant (byte identity), WRITE-MALFORMED -> write form (forged meta, forged page 0). Gate's own: G-REFUSE-NEWER-ONLY (a refused copy whose ckpt_id is below the newest valid meta's is ignored as stale) -> UNCAUGHT by every test the card specifies (0 of 262,144; 10,124 after RW-3), G-58030-FIRST (the all-unreadable 58030 check runs before page 0's refusal) -> table form (1 case: page 0 format_version 2, U U U U), G2-IDENTITY-NO-FV-ALG (gate 2's code edit, the domain checks kept) -> equivalent (0 cases), G-PAGE0-UUID-FOREIGN (an intact page 0 that is not well-formed is ignored when its uuid is not the replicas') -> UNCAUGHT (no foreign page-0 state; not blocking, RW-1 removes its textual support). Text mutants: each of the 22 blocks left out, and 14 planted errors, each fails its own Acceptance line(s) and line 30 in both locales. All in scratch clones; the worktree was never written
- Followups: S-ZETA-F1, S-ZETA-F2

#### Verdict in brief

**REWORK, four small text corrections.** The rule is right. Magic and CRC decide that a record exists, the domains decide whether the open may use it, and an intact record of this file that it may not use fails the open in any place. That is the safe long-term design, and nothing below changes it.

What blocks is narrow:

- **A test gap on the rule's central property (RW-3).** The card's rule refuses a record "in any place", because "its `ckpt_id` cannot be relied on". But every refused symbol in Z6's table form sits at a higher `ckpt_id`, and no other specified test has a refused copy below the newest valid meta. So a build that refuses such a record only when it looks newer, and passes over a "stale" one, passes every test. That build is wrong exactly where the rule matters: a later version's `ckpt_id` lies outside the pinned envelope, and a faulty writer's is unreliable. In the gate's model this mutant changes 0 of 262,144 cases. With two of the four refused symbols moved two below the newest, it changes 10,124, and every named mutant stays killable.
- **Two internal conflicts and one wording trap in the new §2.2 text (RW-1, RW-2, RW-4).**
  - RW-1: Z2 calls another file's record "foreign" even over page 0, where Z1, Z4 and §14.2 make it fail the open.
  - RW-2: Z3's 58030 sentence has no exception for a refused page 0.
  - RW-4: the later-format variant expects a detail "naming a place" where the correct detail names page 0. In §2.2's own vocabulary page 0 is not a "place", so a strict test author would write a test a correct build fails.

**Mechanics: all pass.**
- My own parser applies the 22 blocks to the base and rebuilds the committed file byte for byte.
- Only `docs/S-STORE.md` changes.
- The 31 non-gate Acceptance lines pass in both locales, and the checker prints `OK`.
- Every block left out fails its own lines, and so does each of 14 planted errors.
- The scaffold's fresh gate gives `GATE EXIT=0`.

**Point 4 (the P0 question).** At 7.15, under the events 7.15 covers, the text is **not defective**: no covered fault or crash makes the record in question. It is **under-specified**: §2.2 and §14 step 1 are two-minded about intact records with an out-of-range field. It **lacks future-proofing**: a later format version gets a silent rollback with loss after a downgrade. The section below gives the facts the P0 review needs.

#### Gate run

- **Extract.** The scaffold's `fresh-gate.sh` and `git-env.sh` came out through GATE-PROMPT step 1's shallow `--no-local` clone.
  - The printed scaffold id was `71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`, with no `EXTRACT FAILED` line.
  - The temporary directory was made under `~/.cache/nv3-review/gate-S-ZETA/` (the brief's scratch root) instead of `/tmp`, and removed afterwards.
- **Run.** From `~/Workspace/neutron-v3`:
  - The command was `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-ZETA bash "$fg/fresh-gate.sh" 5c62470b7bf66431f2976b28836449cf60f7c32c`.
  - Stdout and stderr went to separate files. Stderr was empty (0 bytes), and the script exited 0.
- **`fresh-gate:` lines:**
  - `commit 5c62470b7bf66431f2976b28836449cf60f7c32c`
  - `base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `checks passed (4874 files hashed in 1.44 s)`
  - `2 file(s) matched through their eol conversion`
- **STEP lines.** `test`, `clippy`, `fmt`, `deny`, `wasm32`, `core-paths-selftest`, `enforcement-tests` and `api-surface`, each `EXIT=0`. The literal last line was `GATE EXIT=0`.
- **The flake.** The implementer's first run failed on the spike test `readers_never_see_past_their_snapshot_during_apply` (QUEUE row E1C-FLAKE-1). This gate did not meet it: one run, green.
  - GATE-PROMPT has no rule for an unrelated flake. Had one occurred, this gate would have recorded both runs.
  - The card changes only a document, so no test outcome can be this card's.
- **Scope.** Against the scaffold, `git diff nucleus/v3-scaffold...HEAD --stat` lists one file: `nucleus/v3/docs/S-STORE.md`, +36/−19.
  - It is in Touch only, and nothing touches a protected path.
  - The commit has one parent, 71ecf3e6.
  - The worktree `~/.cache/nv3-cards/S-ZETA` was clean before and after. Every Acceptance run used a separate `--shared` scratch clone at 5c62470b.

#### 1. Mechanics

- **Bytes.**
  - My parser is fence-aware, since E2's Old holds a `## Changelog` line. It took E1, Z1–Z20 and E2 from the card (sha256 `17638fb0…741b` at gating) and applied them in order to the base's 7.15 (`a1376c94…a700f`).
  - Every Old occurs exactly once at its turn.
  - With `<date>` = 2026-11-17 the result is byte-identical to the committed file (`571b861d…030d`).
  - With 2026-11-16 it is the card's recorded `1d086ffd…3206`. The two files differ in exactly two characters, the day digit of the two `<date>` positions, as the implementer reported.
- **Acceptance, both locales.** The 32 commands were extracted with `tools/run_card.py`'s own regexes and with an independent line scan, and the two agree.
  - The 31 before `gate.sh` were run as `/bin/sh -c` from the clone's root, under `LANG=en_CA.UTF-8` and under `LC_ALL=C`, with `/usr/bin/grep` = GNU grep 3.11.
  - Result: 31/31 in each locale. Line 32 is the fresh gate's run.
  - The checker (line 28) printed:
    - `158 rows; 234 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`
    - `65 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 8 here), 65 of them judged on a Phase A row; 47 invariants, 48 tests`
    - `OK`
- **Omit one block (all 22, not a sample).** Each was committed on its own scratch branch off 71ecf3e6 and run in both locales. The two locales agree on every one. Failing lines:

  | Block left out | Failing lines |
  |---|---|
  | E1 | 1, 2, 29, 30 |
  | Z1 | 7, 25, 29, 30 |
  | Z2 | 8, 25, 27, 29, 30 |
  | Z3 | 9, 25, 29, 30 |
  | Z4 | 10, 25, 29, 30 |
  | Z5 | 11, 25, 29, 30 |
  | Z6 | 12, 25, 29, 30 |
  | Z7 | 13, 26, 29, 30 |
  | Z8 | 14, 25, 30 |
  | Z9 | 14, 26, 27, 30 |
  | Z10 | 15, 27, 29, 30 |
  | Z11 | 16, 25, 27, 29, 30 |
  | Z12 | 17, 25, 29, 30 |
  | Z13 | 18, 26, 27, 29, 30 |
  | Z14 | 19, 25, 29, 30 |
  | Z15 | 20, 25, 30 |
  | Z16 | 20, 26, 27, 28, 30 |
  | Z17 | 21, 25, 27, 28, 29, 30 |
  | Z18 | 22, 25, 27, 28, 29, 30 |
  | Z19 | 23, 25, 27, 28, 29, 30 |
  | Z20 | 24, 25, 27, 29, 30 |
  | E2 | 3, 4, 5, 6, 29, 30 |

- **Planted errors (14, the gate's own, not the card's P1–P31).** Each is one exact substitution on the committed file, committed and run in both locales. Each fails its own line and line 30.

  | Plant | Failing lines |
  |---|---|
  | §14.2's new row's read-only cell `58030` | 19 |
  | Z4 without "whatever the meta places hold" | 10 |
  | Z5's "in any place" → "in its own place" | 11 |
  | Z2's `map_level` "at least 0" | 8 |
  | Z9 "Every read-write open" | 14 |
  | FOREIGN-MALFORMED-REFUSES dropped from the A-7d row | 23, 27, 28 |
  | two mutant names swapped in I-META-COPIES' cell | 20 |
  | a word changed in §14.1 outside every block | 29 |
  | E2's "233 mutants" | 6 |
  | Z3 without "an open that this rule refuses writes nothing" | 9 |
  | Z12 without "with no fallback" | 17 |
  | Z6 without "or 58030" | 12 |
  | Z2's envelope without "at the same offset" | 8 |
  | §14.2's 58030 row read-only cell `XX001` | 19 |

- **Merge orders.** My parser composed this card with R-1-SPEC, S-ETA and S-THETA, each card's blocks parsed from its current file (sha256 `8842b103…`, `ac660c91…`, `f7d7851c…`).
  - Every two-card order applies, and so do the four-card orders tried, with every Old exactly once at its turn.
  - Each pair's two orders are byte-identical from line 4 to `## Changelog`. They differ only at line 3 and in the order of the two top Changelog entries.
  - The four-card result is the same body with this card first or last.
  - This card's checker on the compositions:
    - with R-1-SPEC: `162 rows; 241 mutants; …; 47 invariants, 50 tests`, failing only on R-1-SPEC's seven names and `A-7f rows: 11, not 10`. That is exactly the card's Notes.
    - with S-ETA: `166 rows; 242 mutants; …; 52 tests`, failing only on S-ETA's eight names, as S-ETA's Notes say.
    - with S-THETA: `160 rows; 247 mutants; …; 48 invariants, 49 tests`, failing only on S-THETA's names, as S-THETA's Notes say.
  - S-ETA's Notes were checked against an older S-ZETA card (`a63bc7bc…6539`). The rechecks here use the current one.

#### 2. The rule, judged independently

**Method.** I wrote my own executable model of §2.2 at 7.16 from the committed text (`tools/model.py`), beside a model of card A-2-0's "decodes" reading. That second model is A-2-0's code at 53484986 plus A-7d's later range check on the selected meta. I then judged two things:

- **Covered single faults.** Every one I could form from four healthy header states: steady state; a crash after step 4's primary, or after its replica, of the next checkpoint; that checkpoint complete. The faults:
  - on each meta place: a lost, torn, unreadable or misdirected write, the misdirect to every other meta place, to page 0 and to a page slot;
  - page 0 torn or unreadable;
  - both replica blocks torn or unreadable;
  - an 8 KiB slot write misdirected onto the replica region;
  - a foreign copy, format_version 1 or 2, at three `ckpt_id`s, in each replica block.

  That is **200 cases. The 7.16 rule and A-2-0's reading give the same outcome in all 200, and 7.16 refuses none.**
- **The case table below** (`tools/case_table.py`, fixture: page 0 valid, 4 KiB, meta 10 in parity 0, meta 9 in parity 1).

The two tests:
- **(i) No silent rollback:** no open selects an older checkpoint, or serves data as complete, past a newer record of this file.
- **(ii) Availability:** no covered single fault becomes a refusal where 7.15 opened.

**Why no fault within the file makes a refused record (checked against the card's claim and the gate's wider sentence).** Every write into the page file is one of these:
- a whole record of this file, which a correct writer writes well-formed (WRITE-MALFORMED);
- zeros (the fence, the retire write, the rest of a slot or block);
- a page image, whose bytes 4–7 are `kind`, a zero `flags` byte and a zero `u16` (§3.1, kinds 3–9). So it never carries `NUCLEUS\0` or `NUCMETA\0` (bytes 4–7 `EUS\0` / `ETA\0`).

WAL records live in other files and start `NWAL`.

So a lost, torn, misdirected or unreadable write within the file leaves either a record that is not intact or a whole well-formed record of this file. The one exception is a CRC collision, at about 2^-32 per damaged record. There, a torn mix of two of this file's copies could be intact with, for example, `epoch ≠ ckpt_id`: 7.16 refuses it, fail-closed, where A-2-0 passes over it.

- **The card's sentence** ("no fault within the file … with the file's `creation_uuid`") **holds.**
- **The A-2-0 gate's wider sentence** ("no lost, torn, misdirected or unreadable write leaves an intact record that is not well-formed") **is false.** A write misdirected from another file of another version leaves exactly such a record.
- **Two assumptions are worth recording.**
  - The foreign/refused split assumes distinct `creation_uuid`s. A byte copy of the file shares its uuid, the card's case 17: outside the model, and fail-closed here.
  - A lost page-0 write at creation can expose an earlier file's page 0 from reused blocks. It refuses at every reading, as at 7.15.

**The gate's case table** (model outcomes; the read-write and read-only opens are identical at step 1, and a refused open writes nothing in either):

| Case | 7.16 | A-2-0 reading | Right? |
|---|---|---|---|
| One copy of the newest meta, the file's uuid, any of the ten defects (`format_version` 2, `page_size` 12288, `checksum_alg` 2, `wal_seg_size` 0, `copy` 7, `map_level` 0, `epoch ≠ ckpt_id`, `T > ts_ckpt_hi`, `page_size` 8192, `wal_seg_size` 32 MiB) | XX001 | opens 10 (range failure: tie XX001 or the other copy wins) | yes: no covered fault makes it; refusing never rolls back |
| Both copies of the newest, `format_version` 2, `map_level` 0 or `page_size` 8192 | XX001 | **opens 9** | yes; A-2-0 is the card's case 8/9 rollback when the record is a later version's |
| A refused copy at a higher `ckpt_id`, own parity or the other parity's place | XX001 | opens 10 | yes |
| A refused copy at a stale `ckpt_id` (8), or in the previous meta's place (9) | XX001 | opens 10 | yes, but **untested** (RW-3) |
| All four places `format_version` 2, page 0 format 1 | XX001 | XX001 | yes |
| One later-version copy only (a partial upgrade, then a downgrade) | XX001 | opens 10 | yes |
| Both replicas torn or unreadable, a refused copy in a slot | XX001 | opens 9 or 10 | yes |
| A foreign copy (another uuid), well-formed or not, `format_version` 2, page 0 valid | opens 10 | opens 10 | yes: a covered fault, ignored |
| Page 0 intact, the file's uuid, any static field out of domain | XX001 | opens 10 | yes |
| Page 0 intact and foreign, format 1 | XX001 | XX001 | as at 7.15 (not a covered fault for page 0) |
| Page 0 intact and foreign, `format_version` 2 | XX001 | opens 10 | acceptable either way; 7.16's is the uuid-agnostic, safer one (RW-1 makes the text say so) |
| Page 0 intact but not well-formed, all four places unreadable | XX001 | XX001 | yes: no later open can succeed, so 58030's "a later open may not meet it" would be false |
| Page 0 valid, all four places unreadable | 58030 | 58030 | yes |
| Page 0 and both replica blocks unreadable | **58030** | XX001 | yes (S-EPSILON's D35 rule) |
| Page 0 torn with both replicas unreadable, or page 0 unreadable with one replica unreadable and the other torn | XX001 | XX001 | pre-existing (card O-3) |
| Page 0 torn, places sound | opens 10 | opens 10 | yes (covered) |
| Page 0 torn, every copy `format_version` 2 | XX001 | XX001 | yes |
| Page 0 torn, a replica `format_version` 2 or `map_level` 0 | XX001 | opens 10 | yes |
| Page 0 torn, the replicas' page sizes differ | XX001 | XX001 | yes |
| Page 0 torn, a foreign `format_version` 2 copy | XX001 | opens 10 | two faults; fail-closed, as 7.15's literal text |
| Page 0 unreadable, a replica with `page_size` 12288, the other unreadable | XX001 | XX001 | yes |
| Explicit rollback, any step-1 refusal | step 1's XX001 or 58030, never 55000 | (A-7d) | yes |
| Fall-back to meta k−1 from a refused newest record | none (§14 step 4a: "a refusal in step 1" never falls back) | — | yes: a refused record's `ckpt_id` gives no proof |

**No cell of this table is wrong under the card's rule** by (i) and (ii). The card's own table has one inaccurate cell, in card prose and not in the spec: its case 6 says "two identities, XX001 at every reading" for page 0 invalid plus a not-well-formed foreign copy, but A-2-0's reading opens there (the row "Page 0 torn, a foreign `format_version` 2 copy" above).

**The 58030 change and its precedence, against S-EPSILON's read-error rules.**
- S-EPSILON's rule (D35): a read error is not evidence of damage, and a later open may not meet it, so 58030 marks a refusal that a later open could escape.
- **Page 0 and both replica reads failing (58030, was XX001).** The open has seen only read errors, so 58030 is exactly that rule.
- **Nothing is written.**
  - §14 step 1's fence runs only before an open's first write, and "an open refused before its first write fence[s] nothing".
  - FENCE-BEFORE-REFUSAL pins it. A build that fences first would zero both replica blocks and turn a transient 58030 into a later XX001 when page 0 stays unreadable.
- **A refused record takes precedence over 58030.** It makes every later open fail, whatever its reads, so XX001 is the accurate code.
- **The remaining XX001s are pre-existing and outside this card**, where a read error alone stands between the open and success (page 0 torn with both replica reads failed; a valid page 0 with three places unreadable and one torn). The card's O-3 defers them; I agree.

**Outside this card, for the record.** At page sizes of 8 KiB and above, a slot write misdirected onto offset 1 MiB covers both replica blocks. Take parity 1's newest meta, its replica written first:
- its slot keeps the old meta;
- its record lands in parity 0's replica block, as a misplaced copy one above;
- the write's zero tail erases its own replica.

So one fault leaves it no copy in its own parity, and the open selects the meta before it, which recovers the same state (I-TWO-GEN). The model flags this one case of the 200.

That is an exception to I-META-COPIES' wording "never changes which meta opens", not to "what it recovers". Whether a build writes the whole slot is unspecified. It is the same at 7.15 and under A-2-0.

#### 3. Future-proofing

**The later-version rule is necessary, correctly stated, and sufficient for what it claims:**
- every build can tell whether a header record of any version is intact;
- every build refuses one with the file's uuid that it cannot use.

It forbids nothing a later version needs. A later version may redefine the meta's bytes 38–162, put additions after the record under their own check, and change domains with a higher `format_version`. A new build reads an old record by the same envelope and knows its layout from `format_version` 1. Neither build can mistake the other's record for a torn one.

**It is not, alone, sufficient for an old build to always refuse a newer file.** Three gaps, all D-8's (§17: "D-6 and D-8 own: … format versioning and upgrades"):
1. **Places.** The rule pins field offsets, not where records live. A later version that moved its metas, and left format-1 metas intact in slots 1–2 or §2.4's replica blocks, would have them opened by an old build.
2. **A single fault after a partial upgrade.** Say the later version wrote format-2 copies of its first checkpoint, and a dropped sync loses both of them (a covered single fault). If page 0 still says format 1, an old build sees only format-1 records. It opens meta k and walks into the later version's WAL. At the first record it cannot read, it stops and truncates there, or, if S-ETA has merged and that record passes every check, it refuses.
   - With page 0 rewritten to the later `format_version` before the later version's first other write, any single fault leaves a record that the old build refuses: page 0, or the later copies when page 0 is damaged.
   - 7.16 already gives that hook: "when page 0 is intact but not well-formed, the open fails with XX001, whatever the meta places hold".
3. **The WAL.** §12.2's framing has no version field, and the rule's list of layouts that `format_version` pins (§2.1, §2.2, §3.1) does not name it. Item 2's page-0 mark covers it.

None of these changes what a 2.0 build does. They are what D-8 must do with the hook 7.16 gives: follow-up S-ZETA-F2.

**A downgrade after an upgrade that wrote only some copies:**
- one later copy, or one of two lost, is refused (XX001);
- both lost with page 0 still format 1 is item 2 above.

**Writer checks.** Creation and §13 step 4 are the only writers of header records, since the fence and the retire write write zeros. Both now check the full domain list, and the write form tests both with a forged value: §15's fault injection, judged on A-7c, where the check's code is.
- The creation hook must inject after any configuration check, or WRITE-MALFORMED's creation half is masked. That is a note for A-7c.
- `gc_w ≤ T` stays a writer-only check (§10.5) and is not in the open's list. That is consistent with 7.15 and not this card's.

#### 4. The P0 question: is draft 7.15 defective here?

**Ruling.** Under the events 7.15 claims to cover, **no**:
- 7.15 followed either way has no silent rollback or lost acknowledged data because of an intact malformed record;
- neither has card A-2-0's code following its "decodes" rule.

7.15 is **under-specified** (two-minded), and it **lacks future-proofing** for a later format version. A-2-0's code has the same properties. It has no caller until A-7d.

Event by event:

| Event | Covered by 7.15? | What 7.15 (either reading) and A-2-0's code do | Class |
|---|---|---|---|
| A single lost, torn, misdirected or unreadable write within the file; a crash at any point of the flip | yes | No intact record with the file's uuid that is not well-formed results (the argument and 200-case enumeration in §2); outcomes equal 7.16's | no defect |
| A foreign copy written into a meta place (the fault form's) | yes | ignored at every reading when page 0 is valid; with page 0 also invalid (two faults), the literal reading refuses, and A-2-0 refuses too unless the foreign copy is not well-formed, when it opens | no defect |
| A write from another file over page 0 | no (a foreign format-1 page 0 refuses at every reading) | a later-version foreign page 0: literal reading refuses, A-2-0 opens from this file's replicas | under-specification; no loss |
| A later format version wrote into the file, then this build opens it (downgrade) | no; §17 assigns format versioning and upgrades to D-8; no second format exists | the later copies pass as foreign (literal reading) or as torn (A-2-0), older metas open, and a read-write open then overwrites the later version's places and truncates its WAL: **silent rollback with loss** | **future-proofing only** |
| A faulty format-1 writer | no | literal reading: a malformed copy with the file's identity can be selected; "decodes" reading: a malformed page 0 is masked for the file's life, and malformed metas are passed over as torn, which in every single-bug case I built recovers the same state through I-TWO-GEN | under-specification; defence in depth |
| A deliberate write | no | either | outside any model |

§14 step 1's "A meta whose magic and CRC are valid but whose `T > ts_ckpt_hi` …" is a third two-minded sentence: any copy, or the selected meta (A-2-0's reading). It has the same class: no covered event makes such a copy.

**Facts for the next P0 review (it rules, not this gate):**
- The readings of 7.15 differ only on records that no covered event produces. This gate's enumeration (200 covered single faults, four healthy states) found no outcome difference between 7.16 and A-2-0's reading, and no 7.16 refusal.
- The one silent loss found needs a second format version and a downgrade. 7.15 does not cover that, and assigns format versioning to D-8 (§17).
- The 2.0 build's reaction to a later file cannot be changed after 2.0 ships. So the rule must be in the 2.0 spec before 2.0's open is built (A-7d) and before any 2.0 build ships. That is a due point, not a 7.15 failure under its own model.
- Review 3 §4.3 ruled two comparable items (S-ALPHA-F4, O-3) not defects. The A-2-0 gates called the readings' agreement "on everything I-META-COPIES covers", and this gate confirms that by enumeration.
- The 58030 sub-case changes the code of a refusal, not a safety outcome.

#### 5. The card's differences from the A-2-0 gate's text

- **G-1, "another static field": the card is right.** "Another field" would also read as `ckpt_id`.
- **G-2, the domains in §2.2: the card is right.** A refusal now depends on the list, and A-7c's writers must use the same one. It cannot live in a code card.
- **G-3, §14 step 1's range relations in "well-formed": right.** 7.15's words ("A meta whose magic and CRC are valid but …") already read as any intact meta. No covered fault makes such a copy. Under A-2-0's narrower reading, a range-failing copy that loses a tie, or sits in a stale or misplaced place, is passed over silently: the skip this card closes.
  - META-RANGE-SKIP on A-2 is killable: 16,193 cases in the model.
  - §14 step 4a item 2's "runs step 1's range check on meta k−1" becomes redundant and stays true.
  - §14.2's row "The newest meta fails step 1's range check" is now a subset of the new row. That is not wrong.
- **G-4, §14.2's new row limited to the file's uuid: right.** The gate's row would refuse a foreign copy, which is a covered single fault.
- **G-5: right that the gate's sentence is false.** But the replacement's second sentence over-reaches for page 0. That is RW-1.
- **G-6, the unlocated 58030: right** (§2 above). Page 0 torn with both replica reads failed stays XX001, as gate 1 ruled.
- **G-7, the precedence: right in substance.** Z3 still states 58030 unconditionally. That is RW-2.
- **G-8, the detail in the text: right.**
- **G-9, the additions: right.** WRITE-MALFORMED is complete and testable, and FENCE-BEFORE-REFUSAL is killable through the unlocated variant's byte identity. The later-version rule has the limits in §3.
- **G-10, gate 1's "lenient decoder for salvage" dropped: right.** §14.1 salvages no step-1 refusal. A "lenient" reading of a later version's record cannot know its layout. The sentence would promise a path 2.0 does not have. What 2.0 does guarantee instead, that a refused open writes nothing, holds (§2).
- **G-11, "Card A-2" kept: right**, by PL-15's mapping. O-2: keep it.
- **G-12 and O-4, IDENTITY-NO-FV-ALG as a behaviour: right.**
  - With every intact copy's static fields domain-checked, gate 2's code edit (comparisons without `format_version` and `checksum_alg`) is equivalent. Each field has one allowed value, so "equals the identity's" equals "in domain". In the model it changes 0 of 262,144 cases.
  - The behaviour "neither compared nor domain-checked" is killable in every build: 36,786 cases.
  - `checksum_alg` has no table symbol. A skip of that field alone is left to the codec tests, which A-2-0-F1 must keep per field.
- **O-1, a page of image `k` that passes §3.3 but fails its decoder: adopt it, in its own small spec card before A-7d (S-ZETA-F1), not in this card.**
  - Such a page is an XX001 that no path recovers from: no fall-back, no explicit rollback, no read-only salvage. Yet image `k−1` and the WAL may be intact.
  - The proof-guarded fall-back is safe there, unlike case 21, because the selected meta is trusted.
  - The card's one-sentence text is not enough on its own. I-FALLBACK's statement ("on its own only when the newest image fails an integrity check at open"), §14.2's fall-back rows and R18 restate the integrity-only rule and must change with it.
- **The code's home, A-2-0-F1 before A-7c: right.** A-2's Touch only excludes `meta.rs` and `open.rs`, so a dedicated card is "the first card after that spec card that edits `open.rs` or `meta.rs`", which gate 1 allowed. A-7c's writer check needs the shared definition.
- **O-3, a wider 58030 rule: not in this card.** A diagnostics and availability matter (both outcomes refuse, and write nothing); its own card can weigh it.

#### 6. Tests, mutants, restatements, merge orders

**Each test passes on a correct build.** All three forms are deterministic, and no form waits on time:
- **The table form** is a pure function of bytes. On the model, a correct rule gives 24,311 opens, 237,576 XX001 and 257 58030 across 262,144 cases.
  - The 257 are page 0 and both replicas unreadable (256), plus page 0 valid with all four places unreadable (1).
  - The size is 16^4 × 4 = 262,144: A-2-0's 11 symbols and 3 page-0 states (43,923, checked against `tests/meta.rs`), plus 5 symbols and 1 state.
- **The write form** forges a value at step 4 and at creation: §15's fault injection.
- **The fault form**:
  - its later-format runs 1 to 3 refuse at step 1, before any write;
  - its foreign copy's second run is ignored;
  - its unlocated variant gives 58030 with nothing written, and the next open selects the faultless meta.

The hooks are named by what they do.

**Each named mutant fails a named test deterministically, on the right Phase A card.**
- The six A-2 mutants and META-RANGE-SKIP each fail the table form (counts in the header). The open rule's code is A-2-0-F1's, labelled "A-2" (PL-15).
- MALFORMED-AS-TORN, IDENTITY-DOMAIN-SKIP, FOREIGN-MALFORMED-REFUSES and UNLOCATED-AS-CORRUPT also fail A-7d's real open.
- FENCE-BEFORE-REFUSAL fails A-7d; the fence is A-7d's code.
- WRITE-MALFORMED fails A-7c; the writers are A-7c's code.

**Two exactness notes.**
- **IDENTITY-DOMAIN-SKIP depends on where the `format_version` 2 symbol sits.** If that symbol always sits in the other parity's place, the table form still kills the mutant, but only through one case (page 0 `format_version` 2, all places unreadable), not by "opening a file whose page 0 and every copy carry `format_version` 2". A-2-0-F1 should put the new symbols in their own parity, like the valid stale copy.
- **G-REFUSE-NEWER-ONLY survives the specified tests:** RW-3.

**Stale restatements.** Outside the Changelog, I read every line with "magic and CRC", "malformed", "decode", "foreign", "identity", "static field", "58030", "page 0", "replica block", "corrupt meta", "range check", "open-time" and "format version" in context.
- None states the old rule.
- Two places are incomplete, without contradiction:
  - §14.2's range-check row (G-3 above);
  - §17's "D-6 and D-8 own: … format versioning and upgrades", which does not point to §2.2's new rule (S-ZETA-F2).
- C-T0's §2.2 references are its own lock table; its only header-related line (248) is about boot ids. C-T6-rehost and `docs/README.md` restate nothing.

**The meaning conflict with S-ETA** (`wal_seg_size` "not 0" here, at least 212 there).
- Once S-ETA lands, "well-formed" must say "at least 212 (§12.1)". This card's own principle is that the domains decide whether the open may use a record, and a segment under 212 bytes is one S-ETA's walk cannot use.
- With both merged and neither change, the file would state two domains for one field: §2.1's and §2.2's.
- S-ETA's Notes carry the fix for both orders:
  - its W34 if this card merges first; its Old (`` `wal_seg_size` not 0; ``) occurs once in this card's result, reworked or not;
  - this card's Z2 New and Acceptance line 8 changing if S-ETA merges first.
- This card's own Notes cover only R-1-SPEC, and should mirror S-ETA's and S-THETA's (card text, below).
- Until S-ETA merges, "not 0" is right for 7.15's format.

#### Rework: the one list of corrections

Apply in this order to `nucleus/v3/docs/S-STORE.md` as committed at 5c62470b. Each Old occurs exactly once at its turn, and nothing else changes.

Tried in a scratch clone:
- **The result.** sha256 `6c186f89…25c4` with the commit's date (`70b49911…1bbb` with 2026-11-16). The diff against the scaffold stays `36 19`.
- **The new Acceptance list.** With lines 8, 9, 12, 14 and 30 replaced as given after the blocks, all 31 non-gate lines pass in both locales on that commit.
  - On 5c62470b, the five replaced lines fail.
  - Leaving out any one RW block fails its own line and line 30.
  - Every RW Old still occurs once after R-1-SPEC's, S-ETA's and S-THETA's blocks, in every order tried.
- **The model.** With RW-3's symbols:
  - G-REFUSE-NEWER-ONLY changes 10,124 cases;
  - every named A-2 mutant still changes outcomes: MALFORMED-AS-TORN 65,876, IDENTITY-NO-FV-ALG 36,786, STATICS-AS-FOREIGN 4,860, IDENTITY-DOMAIN-SKIP 896, FOREIGN-MALFORMED-REFUSES 3,860, UNLOCATED-AS-CORRUPT 256, META-RANGE-SKIP 16,193;
  - G2-IDENTITY-NO-FV-ALG stays equivalent.

**RW-1. §2.2, "Intact and well-formed": another file's record over page 0 (Z2; G-5's replacement over-reached) [Acceptance line 8].**

Old:
```text
A write misdirected from another file can leave an intact record that is not well-formed, but that record carries the other file's `creation_uuid` and is foreign (below).
```
New:
```text
A write misdirected from another file can leave an intact record that is not well-formed, but that record carries the other file's `creation_uuid`: in a meta place it is foreign (below), and over page 0 it fails the open, as another file's well-formed page 0 does.
```

**RW-2. §2.2, "At open": the 58030 sentence names the page-0 refusal that precedes it (Z3; G-7) [line 9].**

Old:
```text
if every place's read fails, the open fails with 58030, and so it does when page 0's read and both replica blocks' reads fail
```
New:
```text
if every place's read fails, the open fails with 58030 unless page 0 is intact but not well-formed (below), and so it does when page 0's read and both replica blocks' reads fail
```

**RW-3a. §2.2, `t_meta_copies`' table form: two of the four refused symbols below the newest valid meta (Z6; G-REFUSE-NEWER-ONLY) [line 12].**

Old:
```text
or an intact copy with the file's `creation_uuid` and a higher `ckpt_id` that has `format_version` 2, `map_level` 0, `T` above its `ts_ckpt_hi`, or another `page_size` (four symbols)
```
New:
```text
or an intact copy with the file's `creation_uuid` that has, at a higher `ckpt_id`, `format_version` 2 or `T` above its `ts_ckpt_hi`, or, at a stale `ckpt_id` two below its parity's newest, `map_level` 0 or another `page_size` (four symbols)
```

**RW-3b. §2.2, `t_meta_copies`' table form: the expectation says so (Z6) [line 12].**

Old:
```text
a copy that the rule refuses gives XX001 wherever the open reads it;
```
New:
```text
a copy that the rule refuses gives XX001 wherever the open reads it and whatever its `ckpt_id`;
```

**RW-4. §2.2, the fault form's later-format variant: page 0 is not a "place" in §2.2's vocabulary (Z9) [line 14].**

Old:
```text
fails with XX001, its detail naming a place and `format_version`, and leaves every file as the hook left it.
```
New:
```text
fails with XX001, its detail naming page 0 or a place, and `format_version`, and leaves every file as the hook left it.
```

**Acceptance: replace lines 8, 9, 12, 14 and 30 (the card's numbering from 1) with these.** Every other line is unchanged; line 29 stays `36 19`.

Line 8:

- `cd nucleus/v3 && grep -n '^The rest of the slot, or of the replica block, is zero\. Card A-2 pins the bytes and fuzzes the decoder, which never panics\.$' docs/S-STORE.md && grep -n '^\*\*Intact and well-formed\*\* (card S-ZETA)\. A record, page 0.s or a meta copy.s, is \*\*intact\*\* when its magic and its CRC are valid, and \*\*well-formed\*\* when it is intact and its fields lie in their domains: .format_version. 1, the version these layouts describe; .page_size. one of §2\.1.s; .checksum_alg. 1; .wal_seg_size. not 0; and, in a meta copy, .copy. 0 or 1, .map_level. at least 1, .epoch. the low 32 bits of .ckpt_id., .T ≤ ts_ckpt_hi. and .wal_start_lsn ≤ wal_ckpt_end. (§14 step 1)\. Magic and CRC decide whether a record exists; the domains decide whether the open may use it, and an intact record of this file that it may not use fails the open (below)\. Up to a CRC collision, no fault within the file (a lost, torn or misdirected write, or a read error) leaves an intact record with the file.s .creation_uuid. that is not well-formed or that differs from the file.s identity in another static field: such a record was written whole, by another format version, a faulty writer or on purpose, so the open never takes it for a torn one\. A write misdirected from another file can leave an intact record that is not well-formed, but that record carries the other file.s .creation_uuid.: in a meta place it is foreign (below), and over page 0 it fails the open, as another file.s well-formed page 0 does\. Creation (§2\.3) and §13 step 4 check that each record they write is well-formed before they write it; a record that is not fails creation, and is fail-stop at step 4 (§9\.2), so no correct build leaves one\. Every later format version keeps each record.s first bytes, the magic, .format_version. and, in a meta copy, .copy. and the static fields, at these offsets, and the record.s CRC32C at the same offset over the bytes before it, and puts anything it adds after the record under a check of its own; any change to a layout that .format_version. pins (§2\.1, §2\.2, §3\.1), or to a domain, is a later format version, with a higher .format_version.\. So every build can tell whether a record of any version is intact, and refuses one it cannot use\.$' docs/S-STORE.md`

Line 9:

- `cd nucleus/v3 && grep -n 'A place whose read fails holds an invalid copy; if every place.s read fails, the open fails with 58030 unless page 0 is intact but not well-formed (below), and so it does when page 0.s read and both replica blocks. reads fail, which leaves slots 1 and 2 unlocated (below): the open has seen no damage, only read errors, which a later open may not meet\. A read-write open zeroes every place whose read failed before its first write (§14 step 1, the fence), and an open that this rule refuses writes nothing\. Card A-2 builds the rule' docs/S-STORE.md`

Line 12:

- `cd nucleus/v3 && grep -n '^- \*Table form (A-2)\.\* The rule runs over every combination, for the newest meta and the previous one, of each place holding a valid copy of its parity.s newest meta, the same copy written for its parity.s other place, a valid stale copy, a torn copy, zeros, an unreadable place, a misplaced copy (one .ckpt_id. above the newest valid meta, and two above it), a foreign copy with a higher .ckpt_id., another with .format_version. 2, or an intact copy with the file.s .creation_uuid. that has, at a higher .ckpt_id., .format_version. 2 or .T. above its .ts_ckpt_hi., or, at a stale .ckpt_id. two below its parity.s newest, .map_level. 0 or another .page_size. (four symbols), with page 0 valid, torn, unreadable, and intact with .format_version. 2, and over both tie cases: the newer .boot_id. in either place, and equal .boot_id.s\. Each result is the rule.s: the meta selected, the previous meta, XX001 or 58030\. Among them, page 0.s read and both replica blocks. failing gives 58030, and page 0 torn with both replica blocks. reads failing gives XX001; a copy that the rule refuses gives XX001 wherever the open reads it and whatever its .ckpt_id.; and a file whose page 0 and every intact copy carry .format_version. 2 gives XX001\.$' docs/S-STORE.md`

Line 14:

- `cd nucleus/v3 && grep -n '^- \*Fault form (A-7d)\.\* .*separately, a fault on page 0, on both replica blocks at once, and a foreign copy with a higher .ckpt_id. written into a replica block, in a second run one with .format_version. 2\. Every open selects the meta the faultless run selects and recovers the same state, and every image passes amcheck\. .*\*In-flight variant\* (I-META-SLOT): step 4 of checkpoint .k+1. misdirects its first copy into one of meta .k..s two places, and a crash follows before the second copy.s write; the open selects meta .k. from its other copy and recovers every acked commit\. \*Later-format variant\* (card S-ZETA): a hook takes meta .k..s copy, sets its .ckpt_id. and .epoch. to .k+1. and its .format_version. to 2, re-seals it, and writes it through .Env. into both places of meta .k+1..s parity, as a later format version.s checkpoint would; a second run re-seals only page 0 with .format_version. 2, and a third re-seals page 0 and the copy in every meta place so\. Every open, read-write and read-only, fails with XX001, its detail naming page 0 or a place, and .format_version., and leaves every file as the hook left it\. \*Unlocated variant\* (card S-ZETA): a hook fails every read of page 0 and of both replica blocks at one read-write open, and again at one read-only open; each fails with 58030 and leaves every file byte-identical, and the next open, whose reads succeed, selects the meta the faultless run selects\.$' docs/S-STORE.md`

Line 30:

- `cd nucleus/v3 && test "$(git diff --no-color --diff-algorithm=myers -U0 nucleus/v3-scaffold...HEAD -- docs/S-STORE.md | tail -n +5 | grep -v '^@@' | sed 's/20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/<date>/g' | sha256sum | cut -c1-64)" = '9de849fd3f49b23e331c3a57ed09fc45206b668c9f706eee407048ff045d7666'`

**Card text the rework must update** (the orchestrator's; the implementer edits only `docs/S-STORE.md`):
- Z2's, Z3's, Z6's and Z9's New blocks as RW-1 to RW-4 leave them, so that E1, Z1–Z20 and E2 applied to the base rebuild the reworked file.
- The Acceptance lines above.
- "For the plan card" item 3's alphabet sentence: `format_version` 2 and `T` above `ts_ckpt_hi` at a higher `ckpt_id` in the place's own parity; `map_level` 0 and `page_size` 8192 two below their parity's newest. Still 16 symbols and 262,144 cases.
- Case 6's "XX001 at every reading" (A-2-0's reading opens).
- The dry-run hashes.
- Notes, "If S-ETA merges first" and "If S-THETA merges first", mirroring those cards' Notes, with S-ETA's Z2 change.

#### For the plan card

The card's "For the plan card" stands, with these changes:

- **A-2-0-F1.** Its table form follows RW-3: the stale `map_level` 0 and `page_size` 8192 symbols, and the new symbols in their own parity.
  - Its gate re-runs G-REFUSE-NEWER-ONLY and G-58030-FIRST.
  - It keeps per-field codec cases, so that a skip of one field (for example `checksum_alg` or `epoch`) stays killable. Gate 1's RW-2 of A-2-0 is the model.
- **A-7c.** The creation hook of the write form injects after any configuration check.
- **D-8's row** gains S-ZETA-F2.

#### Owner points, in plain words (not decided here)

- **OP-1. A file written by a newer version of the database.** When this version opens a file that a newer version has written to, should it refuse with its own error code, 55000 ("object not in prerequisite state"; the card reports that PostgreSQL uses it for incompatible data files), instead of XX001 ("data corrupted")?
  - Either way the open refuses and writes nothing. Only the code differs.
  - With XX001, an operator may restore a backup or run a repair on a file that is merely newer.
  - The card recommends yes, and gives the small spec card it needs.
- **OP-2. A file this version cannot read.** If a file's header holds a record this version cannot use, written by a newer version, a faulty build or on purpose, should the database refuse to open the file at all, read-only included, until a newer version or a repair tool (plan item 15, which does not exist in 2.0) deals with it?
  - The alternative is to open the newest part it can read. That can silently roll back past newer data.
  - Refusing writes nothing, so nothing is lost.
  - There is a cost case: a bug in this version's own writer that damages only bookkeeping fields. Opening the older checkpoint would usually have recovered everything there, and refusing locks the data away until a fix.
  - The writer check added here (WRITE-MALFORMED) is meant to stop this version ever writing such a record.
  - The card recommends yes, with no override in 2.0.

#### Merge timing (for the record)

By P0 re-check 1, this card may be built and gated on a branch, but may NOT merge before P0 PASS unless the owner says so in their own words.

#### Not verified

- **No code.** No implementation of the 7.16 rule exists yet; it is A-2-0-F1's. Every mutant count above comes from this gate's own model of the text, not from the crate.
  - The model abstracts each record to its fields. It does not reproduce A-2-0's byte layouts or its alphabet's exact `ckpt_id`s.
  - The A-7c and A-7d forms were walked by hand only.
- **The 262,144-case run time** in a debug build was not measured.
- **PostgreSQL.** The card's citation of `ReadControlFile` (card prose, not spec text) was not checked against PostgreSQL's source.
- **Four-card merge orders.** Only the orders named were composed, not all 24.
- **The flake.** The implementer's flake (E1C-FLAKE-1) was not reproduced; this gate's single run was green.

#### Follow-ups

- **S-ZETA-F1. O-1's spec card, before A-7d.** A page of image `k` that passes §3.3's four checks but that its decoder refuses (§3.1) counts as failing at open. The proof-guarded fall-back, the explicit rollback and the read-only salvage then reach image `k−1`, where 7.15 leaves no path at all.
  - The card must change, together:
    - §14 step 4a's definition (the card's O-1 block);
    - I-FALLBACK's "fails an integrity check at open";
    - §14.2's fall-back rows;
    - R18;
    - a full restatement search.
  - It adds a test hook that corrupts a page's body before checkpoint `k`'s step 2 seals it, and the mutant FALLBACK-CHECKS-ONLY on `t_fallback`'s A-7d row.
- **S-ZETA-F2. The later-version rule's limits, for D-8 and §17.**
  - **The §17 rider.** With the next S-STORE spec card, §17's D-8 line points to §2.2's rule:

    Old: `**D-6 and D-8 own:** backup, PITR and replica handling of the §12.8 boundary; format versioning and upgrades.`

    New: `**D-6 and D-8 own:** backup, PITR and replica handling of the §12.8 boundary; format versioning and upgrades, within §2.2's rule for later format versions.`

    The Old occurs once at 5c62470b and after the rework, and once in every composition tried.
  - **D-8's plan row**, before D-8 is cut, carries §3's three facts. A later version that writes into a format-1 file:
    - rewrites page 0 with its own `format_version`, durably, before its first other write;
    - keeps writing its header records, in the envelope, to the places format 1 reads, or invalidates them;
    - treats the WAL framing (no version field, and not in the rule's list) the same way, or relies on page 0's mark.

    Otherwise a single covered fault after a partial upgrade still lets an old build roll back.

<!-- gate 2's verdict (512ea1f8), verbatim, ends -->
