Card: S-NU
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5 (high)
Gate-session: T3 review sub-agent, Claude Code session 7cdd03f9-738a-452e-b3ef-a644a49bb624, dispatched from T3 thread 45584af2-e651-490e-862c-3402320cffa3 (an independent P gate for card S-NU: this agent wrote neither the card, nor its application (e35d5ab5), nor verdict A-3c (380b0b3e), and is not the orchestrator)
Date: 2026-11-28
Gate-exit: GATE EXIT=0
Commit: e35d5ab521127e59a3ab2a06480449ab8291b5ba
Mutants: spec card, so no code to mutate and no named mutant (the card adds none). Planted faults on the applied doc, each in its own commit in a separate shallow clone, Acceptance lines 1-13 in both locales: P1 `k+1` -> `k` inside the sentence -> lines 7, 12 | P2 "refuses" -> "allows" inside the sentence -> lines 7, 9, 12 | P3 I-FREE-SAFE's A-7c row "full (step 6)" -> "full (step 6, S1)" -> lines 10, 11, 12 | P4 a doubled space at the sentence's end -> lines 7, 12 | P5 `t_sim_timed_advance`'s "(S3)" -> "(S1)" -> lines 11, 12 only (line 10 passes it: see NIT 1) | P6 line 10 with the card's new exception removed, on the unchanged tip -> fails on row 27 only
Followups: S-NU-F1

## Verdict in brief
**MERGE-WITH-FOLLOWUPS** on `e35d5ab521127e59a3ab2a06480449ab8291b5ba` (S-STORE draft 7.25 on card S-XI's 7.24). The commit is the card's three blocks applied verbatim, and it changes one file. N1's New is verdict A-3c's "Final wording" byte for byte. Acceptance lines 1-13 pass in the default locale and under `LC_ALL=C`. The sentence is consistent with §5.6, §4.4, §13 step 2, I-FREE-SAFE and I-TWO-GEN, and S-XI's 7.24 additions do not touch it. The fresh gate is green on its first run (`GATE EXIT=0`).

The one follow-up, S-NU-F1, is a defect in S-XI's merged §15.1 text, not in this card. The card's new checker exception (`is a test-only caller`) is right for this card, but it does not make the row it admits correct (ruling below).

## What I verified myself
1. **Mechanics, with my own parser** (`~/.cache/nv3-review/gate-S-NU/tools/apply.py`, not the dry run's `blocks.py`). I split the card's Edits section into E1, N1 and E2 and read each block's Old and New from its two fences. Applied to the base blob (`git show 8a2df81d:…`, sha256 `642c27f6…`, the same blob as at 16915ce1), each Old occurs exactly once in the untouched base and once at its turn. With `<date>` = 2026-11-28 the result equals the tip's committed blob byte for byte (sha256 `246ecc38c126a1686e66dc5f9e40324699c22abca4e033c2c88936bcaf4b5195`, 2063 lines). The reverse (E2, N1, E1, New to Old) gives the base byte for byte. The worktree is clean, and its working file has the same sha256 as the committed blob.
2. **Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` shows one file, `nucleus/v3/docs/S-STORE.md` (5 lines added, 2 removed), which is the card's whole Touch only list. No protected path is touched.
3. **Acceptance lines 1-13**, extracted from the card and run with `/bin/sh -c` from the worktree root (`tools/acc.py`): **13 of 13 pass in the default locale (en_CA.UTF-8) and 13 of 13 under `LC_ALL=C`**. Line 10 prints `180 rows; 300 mutants; 24 judged only on Phase B cards; 49 invariants; 58 tests` and `OK` in both. Line 14 is the gate, below.
4. **Status line and Changelog.** Line 3 reads `Status: draft 7.25 (card S-NU, 2026-11-28: …) on draft 7.24 (card S-XI, …`, and the chain after it equals the scaffold's line 3 (Acceptance line 2). The Changelog's first two entries are `Draft 7.25` (line 1763) and then S-XI's `Draft 7.24` (line 1766). Every older entry is unchanged (Acceptance line 5).
5. **Planted faults:** see `Mutants:`. Each was caught by the lines written for it, except P5, which only the diff lines catch (NIT 1).
6. **Fresh gate:** below.

## The wording
**Byte-identical.** I extracted the two `text` fences after "**Final wording.**" in `nucleus/v3/verdicts/A-3c.md` at 380b0b3e (the verdict file has no later commit on its branch) and compared them with N1's two fences in the card: Old equal, New equal. The verdict's New occurs exactly once in the tip's S-STORE.md.

**Against draft 7.24, re-read:**
- **§5.6 "When it copies"** copies a MapLeaf at content epoch `e − 1` at its first mutation in epoch `e`, "whether or not its frame is dirty … unless the map phase has taken that content for image `e − 1`". S1 sets `e := k+1`, so the sentence's "for image `k` … mutated in epoch `k+1`" is exactly that exception, supplied by the stand-in. It narrows nothing for a MapLeaf dirty in the pool at the flip: that MapLeaf still needs the copy, and A-3c refuses the mutation. §5.6's rule text is unchanged.
- **§4.4.** The sentence's last clause ("no MapLeaf home whose epoch is below `e` is freed before step 6") is stricter than §4.4's slot-reuse paragraph. That paragraph would let an eviction home of a MapLeaf "that no image-record list names, whatever epoch its ref carries" be reused at once, and before A-3c-C no list names any. Freeing less than §4.4 permits cannot break I-NO-INPLACE or I-FREE-SAFE, so nothing is weakened. The "no image-record list" count of 2 in Acceptance line 9 is right: one occurrence was already in that §4.4 paragraph.
- **§13 step 2** names three appenders to the image-record list: "by step 2's data phase, by a claim (§5.6) or by an eviction (§5.2)". The sentence names the same three (the §5.2 one as "a previous-epoch victim's", the only §5.2 write that appends, per §5.2's "Dirty victims from the previous epoch"). So "nothing would append to one" before A-3c-C holds as long as A-3c refuses a previous-epoch victim, which verdict A-3c's (a) records (E2).
- **I-FREE-SAFE** (A-3c row "full (pending frees)") and **I-TWO-GEN** (`t_two_gen` A-3c row, "S3: step 6's release …") both need evictions, after a flip, of pages whose entries are in a pre-flip MapLeaf. Under reading (b), §5.6 demands a copy that only A-7a builds. I re-derived the contradiction: a MapLeaf reaches content epoch `e` only by a copy (§5.6) or by being made in epoch `e`. The sentence is what makes those two rows judgeable at A-3c, and it changes neither invariant's statement, mutant or row. `t_two_gen`'s S3 release frees at step 6, which the sentence's "before step 6" leaves open.
- **S-XI's 7.24 additions** (`git diff 16915ce1^1 16915ce1`): the timed-wait prose, I-TIMED-WAIT, I-WAL-TS-ORDER's pair read, the §15.1 preamble's added "B-2a", and four §15.1 rows. Outside the status line and Changelog, no added line names S1, a flip, an epoch copy, a MapLeaf or the image-record list, so there is no interaction.

## Ruling: the checker exception `is a test-only caller`
**The exception is right for this card. The row it admits is a spec defect in S-XI's merged text and needs its own follow-up (S-NU-F1).**
- *Why the exception is right here.* R-1-SPEC's "no full form names a stand-in" check was not in S-XI's own checker (S-XI.local.md has no such line), so S-XI merged a row the check refuses: `t_sim_timed_advance` | A-8a | "full, on the first SimEnv backend of the sync layer; the timed waiter is a test-only caller (S3)". S-NU must not touch that row (Limits 1), and its checker exists to pin that S-NU moves nothing. Without the exception, line 10 fails on text S-NU did not write. With the card's exact regex removed, I re-ran line 10 on the unchanged tip (P6), and it fails on row 27 alone, so the exception admits exactly that one row today. Lines 11 and 12 pin that no row changes.
- *Why the row is a defect.* It breaks two rules of the spec:
  1. §15.1's preamble says "*Form* is full, **or** the stand-ins it uses (S1–S6)". This row is both.
  2. §15 says "Only these stand-ins are allowed", and S3 then gives a closed list ("The cases are:"). That list has no timed-waiter case. The list names its cases down to single tests' variants, for example the height bound `H` for `t_height_bound` (b), and the end of a chunked transaction for `t_flip_chunk_race`'s variant *ended*.

  The same gap covers S-XI's `t_ckpt_pair_one_point` A-7c row ("S3: the `Commit` is appended by a test-only caller … as B-2a's sequencer will append it"). That row is not a full form, so the checker never sees it, but its case is not on S3's list either.
- *What is not ruled here.* The defect can be fixed in two ways, and choosing between them is a technical question for the follow-up's card, not a value trade-off:
  - (i) The timed waiter is the test's own driver, not a stand-in, as in `t_timed_wait`'s full form, which calls the same primitive and names no S3. Then the "(S3)" clause is dropped.
  - (ii) It stands in for C-T0 v8 §6.3's park, a later card's caller (S-XI's T-1 names B-7). Then S3's list gains the case, and the row reads "full except S3 for the timed waiter", in the existing `full except` pattern, or names the card that runs the full form.

## Fresh gate
GATE-PROMPT step 1 with the Fast-track priority. The scaffold's `fresh-gate.sh` (sha256 40a5b232…) and `git-env.sh` (8a784782…) were extracted through the shallow `--no-local` clone, which printed scaffold id `8a2df81d1bd7c8e65192cf0d463044c006e6bfcb` and no `EXTRACT FAILED`. The run was prefixed with `systemd-run --user --scope --quiet -p CPUWeight=10000 --` (the scope worked), used `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-NU`, and ran from about 00:52 to 01:07:34. stdout and stderr were captured separately: stderr is empty and the exit code is 0. The extract was removed afterwards. Lines, verbatim:
```
fresh-gate: commit e35d5ab521127e59a3ab2a06480449ab8291b5ba
fresh-gate: base 8a2df81d1bd7c8e65192cf0d463044c006e6bfcb
fresh-gate: checks passed (4994 files hashed in 4.65 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
advisories ok, bans ok, licenses ok, sources ok
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests SKIPPED (no scripts/ or tools/ change)
api-surface: no files in scope
STEP api-surface EXIT=0
GATE EXIT=0
```
`STEP enforcement-tests SKIPPED` is expected for this docs-only change on a branch cut from 8a2df81d. No FAST-TRACK §3 known load or zombie row appeared, and nothing else failed, so no re-run was needed and none was made.

## Notes
- NIT 1 (checker, non-blocking): the exception matches the phrase anywhere in the Form cell, not a test-only caller under S3 specifically. P5 shows that a row reading "… is a test-only caller (S1)" would pass line 10. On this card lines 11 and 12 catch any row edit, so nothing escapes. A later spec card that copies this checker should either drop the exception once S-NU-F1 lands or tie it to `S3`.
- NIT 2 (wording, the A-3c gate's own text, which this card must carry verbatim): the sentence's first clause is bounded by card ("At A-3c only, before step 2 exists (A-3c-C)"), but its image-record clause is bounded only by time ("Before A-3c-C the stand-in flip opens no image-record list"). A reader may ask whether the second also binds a card ordered before A-3c-C that uses S1. Either reading is harmless, because no list exists then. A-3c-F2, which takes on the clause's lapse at A-3c-C, is the place to make it explicit if wanted.
- The card's Merging line stands: S-NU before A-3c (A-3c-F1). Merging before P0 PASS remains the standing owner matter the header names; this verdict does not decide it.
- Scratch: `~/.cache/nv3-review/gate-S-NU/` (tools/, planted-fault clones under pl/, fg*.out/err).

## Follow-up
**S-NU-F1 (spec gap in S-STORE §15/§15.1, from card S-XI's draft 7.24; due before A-8a's or A-7c's card is cut).** Two §15.1 rows that S-XI added use S3 for cases that S3's closed list does not name. `t_sim_timed_advance` (A-8a) also writes "full" together with an S3 note, against §15.1's preamble ("*Form* is full, or the stand-ins it uses"). `t_ckpt_pair_one_point` (A-7c) uses S3 for a `Commit` appended as B-2a's sequencer will append it. A spec card fixes both rows. For each, it either removes the S3 claim (the caller is the test's own subject, not a stand-in) or adds the case to S3's list and states the form as "full except S3 for …" or names the card that runs the full form. Once the row is fixed, the next spec card's checker drops the `is a test-only caller` exception that S-NU's checker added.
