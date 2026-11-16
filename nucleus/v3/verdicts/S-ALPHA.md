Card: S-ALPHA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session fac269ed-6793-4f1f-9431-36c02803c99a (a fresh gate run; this agent wrote neither the card, the S-ALPHA commits, the first verdict nor the rework)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: 41795fba12ac558008bd3cdbbb1ba9ea405090d8
Mutants: the card names none (spec text). The gate's own text mutants, each on a scratch copy of the tip, judged by the card's Acceptance (line 10 is the checker): TM1 (the retire write zeroes meta `k`'s own places before the misplaced copies) -> UNCAUGHT; TM2 (I-FREE-SAFE's exception deleted) -> UNCAUGHT; TM3 (step 4a item 1 falls back without the previous free set) -> UNCAUGHT; TM4 (`t_fallback`'s A-7d row moved to B-6) -> checker; TM5 (RETIRE-ONE-COPY dropped from I-FALLBACK's row) -> checker. No Acceptance line reads prose rules such as TM1-TM3; the gate judged those by reading and with its models (TM1 strands an intact store: XX001 in 6 crash states of `model/retire.py`)
Followups: S-ALPHA-F1, S-ALPHA-F2, S-ALPHA-F4, S-ALPHA-F5, S-ALPHA-F6

The rework is exactly the first verdict's 41 blocks. The bytes match the gate's own re-application to 3ccc8b3d, and no name is lost. The first verdict's ten rulings now hold in the text, and the behaviour they fix is right as durability design, with one missed sibling of item 1. Of the implementer's five leftovers:
- N1 is real and blocking: `t_fallback`'s unread variant cannot pass on a correct build.
- N2 and N3 are real overclaims of I-META-COPIES and I-TWO-GEN. N3 is wider than reported: I-TWO-GEN's statement is also false after a newer synced meta is lost.
- N4 is consistent with §2.2's defined "newest meta", and N5 is cosmetic. Both ride.

The gate found two more defects of the same two kinds:
- the unproven case's WAL-loss placement lets a correct build fail its explicit-rollback assertion;
- an explicit rollback refused with 55000 after a completed pass 2 is said to write nothing.

Ten exact blocks, V1–V10, fix all of them. On a scratch copy of 41795fba each Old occurs once at its turn, every Acceptance line still passes (`141 rows; 200 mutants; 31 new, 31 of them judged on a Phase A row; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8` / `OK`), and no further S-BETA anchor moves. No block adds or removes a mutant, an invariant, a test or a §15.1 row, and every block lies inside E16, E38, E39, E46, E52 or E59.

The rebuilt open-rule model finds no silent loss at zero, one or two faults. Its only one-fault refusal is S-ALPHA-F5's, which is already in QUEUE and was already so at 7.10.

## History
- **3ccc8b3d**: the card's E1–E59, S-STORE draft 7.11.
- **d91802f6, first verdict: REWORK on 3ccc8b3d** (P, GATE EXIT=0).
  - Seven of the implementer's ten contradictions were real. Two of them changed behaviour: "writes nothing" became "writes nothing more", with the retire write before the first write from meta `k−1`; and the retire write zeroes misplaced copies first, under their own sync.
  - Eight mutants could not fail as specified.
  - The verdict gave 41 exact blocks, R1a–R18, and follow-ups S-ALPHA-F1 to F5. QUEUE carries F1, F2, F4 and F5.
- **41795fba, the rework**: the 41 blocks applied by script in the verdict's order, +37/−36 in one file, draft number unchanged. R18 adds the Changelog's gate-corrections line.
- This verdict replaces the first verdict's text.

## Gate run
- Scaffold extract (GATE-PROMPT step 1): shallow `--no-local` clone of `nucleus/v3-scaffold` at **108e180ef4f3e557b68f8bfd65b4a1df78ba3317**. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- The run: `bash "$fg/fresh-gate.sh" 41795fba12ac558008bd3cdbbb1ba9ea405090d8` from `~/Workspace/neutron-v3`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-ALPHA-2` and both streams captured. Its lines:
  - `fresh-gate: commit 41795fba12ac558008bd3cdbbb1ba9ea405090d8`
  - `fresh-gate: base 108e180ef4f3e557b68f8bfd65b4a1df78ba3317`
  - `fresh-gate: checks passed (4814 files hashed in 2.82 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`
  - literal last line `GATE EXIT=0`; exit status 0; stderr empty. `$fg` was removed afterwards.
- The worktree `~/.cache/nv3-cards/S-ALPHA`, branch `card/v3-S-ALPHA`, was at tip 41795fba and clean before and after.
  - No assume-unchanged or skip-worktree entry exists.
  - The worktree's `docs/S-STORE.md` hashes to the committed blob.
  - Every mutant and dry run used scratch copies under `~/.cache/nv3-review/gate-S-ALPHA-2/`.

## Scope and the rework's bytes
- `git diff --stat nucleus/v3-scaffold...HEAD` lists two files.
  - `nucleus/v3/docs/S-STORE.md`, +251/−56 against 7.10, is the card's Touch only list.
  - `nucleus/v3/verdicts/S-ALPHA.md` is the first verdict, which this file replaces.
  - No protected path is touched.
- 3ccc8b3d → 41795fba is one file, +37/−36.
  - The gate's own parser applied the first verdict's 41 blocks to 3ccc8b3d's S-STORE.md in the listed order, and every Old occurred exactly once at its turn.
  - The result is byte-identical to 41795fba: sha256 `ea3fca58f990746936311d54653025cb07f3015fb3afde9ec931ceb41d77da83`.
- A script compared names across 7.10, 3ccc8b3d and 41795fba: 46 invariant rows, 200 §15 mutants, 42 tests, 6 SQLSTATEs and 22 §18 rows. Every name and heading of 7.10 and 3ccc8b3d is present at 41795fba. The one exception is the heading `### 2.2 Slots 1–2: meta records`, which E7 renamed by design.
- Acceptance lines 1–10 pass in the worktree, and the checker prints the line quoted above, then `OK`.
- Line 11 fails, `cd nucleus/v3 && git diff --stat nucleus/v3-scaffold...HEAD -- . | tail -1 | grep -q '1 file changed'`.
  - Its `-- .` now also counts the first verdict's file; with `-- docs` it reports 1 file.
  - Any verdict commit on the branch makes it fail. The first gate ran it before committing, so it is not a rework defect.
- Line 12 is this gate run.

## Rulings on N1–N5
- **N1. REAL, blocking. Fixed by V5.** The unread variant sits under the Retire bullet, whose fixture corrupts "one read of a page of image `k` at the first open only, so that the page reads correctly afterwards".
  - At the next open image `k` reads cleanly and meta `k` opens with no failure. "The next open falls back with proof" therefore cannot happen on a correct build.
  - A persistent fault does not rescue the leaf case. With the previous free set unread, I-FREE-SAFE's exception lets the first open's pass-2 evictions reuse image `k−1`'s superseded slots before image `k` fails. "Writes nothing" is then false, and the later fall-back can meet a reused slot.
  - V5 moves the variant onto the proven fixture, whose post-sync LOST or TORN fault is persistent, with the fault on a page that step 2 reads. The first open then fails in step 2 before any write. The next open reads the free set and falls back with proof onto an untouched image `k−1`. This is the implementer's suggestion, adopted.
- **N2. REAL; the behaviour is safe, but two sentences overclaim it. Fixed by V6 and V8.**
  - In the gate's model, the open selects another meta than the faultless run in 130 of 4,743 one-fault states.
  - Every one is a crash between step 4's two writes, followed by a fault on the one copy of meta `L+1` that persisted: a read error, a misdirect, or a foreign overwrite. The open selects meta `L`, which recovers the same state.
  - §2.2 says so since R11. The I-META-COPIES row does not, and neither does §14's "A meta copy" sentence, whose "or unreadable" has no condition.
- **N3. REAL, and wider than reported. Fixed by V1, V2 and V9.** I-TWO-GEN's statement in §4.4 and in its §15 row ("while meta `k` is the durable image, image `k−1` and the WAL from meta `k−1`'s `wal_start_lsn` stay intact") is false in two cases:
  - (a) After an open whose step 2 could not read meta `k−1`'s free set: I-FREE-SAFE's exception reuses image `k−1`'s superseded slots. This is the reported case.
  - (b) After meta `k+1` was synced and then lost in both copies. The next open selects meta `k`, so meta `k` is "the durable image" (§1). But step 6 of checkpoint `k+1` already freed image `k−1`'s superseded slots and recycled below meta `k`'s start, so neither image `k−1` nor its WAL span is intact.
  - The same paragraph states both exceptions later. The first gate ruled that shape a normative contradiction for I-FREE-SAFE (its item 2).
  - The paragraph's conclusion, "So a fall-back … finds image `k−1` and its whole WAL span as they were", inherits (a).
  - V1, V2 and V9 state the invariant with step 4a item 1's condition, in R3's words.
  - Sentences elsewhere that cite I-TWO-GEN for an intact previous image inherit the condition through the citation and ride (S-ALPHA-F6 (d)).
- **N4. Not a defect.** I-FALLBACK's row says "only to the previous meta, never two generations". Both terms are relative to "the newest meta", which §2.2 defines as the candidate the open selects. §2.2's "three generations" is counted "from the lost newest one", a meta the open does not select. A clarifying phrase is optional and rides (F6 (a)).
- **N5. Cosmetic.** R12b's insertion left "…, so that its `boot_id` is the older, so that the stale meta `k` sits beside the new one". The meaning is recoverable, and the fix rides (F6 (b)).

### Found by this gate
- **G1. REAL, blocking. Fixed by V4.** `t_fallback`'s unproven case runs "as proven", and the proven case's four cells include "a leaf pass 2 replays into". The LOST record lies "between meta `k−1`'s and meta `k`'s `wal_ckpt_end`".
  - When the lost record lies at or above meta `k`'s `wal_start_lsn`, the open from meta `k` reads it. Its valid end falls below meta `k`'s `wal_ckpt_end`, and step 3a refuses with WAL loss before pass 2 reads the leaf.
  - Image `k` then never fails at open, so the explicit rollback's precondition does not hold. A correct build answers 55000 where the case expects the rollback to open.
  - FALLBACK-NO-PROOF is not caught in that cell either, because step 3a refuses under it too. It is caught in the step-2 cells.
  - When the loss lies below meta `k`'s start, only the walk from meta `k−1` reads it, and all four cells reach the fall-back.
- **G2. REAL. Fixed by V3 and V7.** An explicit rollback "proceeds only when … its image fails at open; otherwise it fails with **55000** and writes nothing".
  - When image `k` does not fail, the open learns that only after pass 2 has run from image `k`, and a read-write pass 2 may have evicted.
  - Those writes are harmless: they go to slots that image `k` does not reference. But "writes nothing" is false.
  - This is the sibling the first verdict's R1a–R1j missed when they fixed every other such sentence of step 4a. §14.2's 55000 row carries the same claim.

## The first verdict's ten rulings, re-checked against 41795fba
1. **"Writes nothing" (R1a–R1j).** Gone except for G2, and right as durability design. The setup: the open selects meta `k` with previous meta `k−1` (its free set read), and image `k` fails in step 2 or in pass 2. A crash at each point:
   - Before the retire's first sync. On disk are both copies of meta `k`, image `k` and image `k−1` untouched (pass-2 evictions went only to slots neither image references) and the WAL untouched. The next open selects meta `k` and decides again; after a transient fault it opens meta `k` with the same state.
   - During or between the retire's two syncs. `model/retire.py` covers every crash point, with and without a misplaced copy. The next open selects meta `k` (a copy survives in its own parity and image `k` is untouched) or meta `k−1` (retired). It never gives XX001, and never selects meta `k` once the second sync has completed.
   - After the retire, during pass 2 from meta `k−1`, whose evictions may go into image `k`'s slots. Meta `k` is gone, so the next open selects meta `k−1`, with no previous meta, and this open never wrote image `k−1`.
   - After step 5's truncation. The walk from meta `k−1` and a walk from meta `k` cross the same records from meta `k`'s start and end at the same valid end, which the proof puts at or above meta `k`'s `wal_ckpt_end`. The next open, from meta `k−1`, truncates at the same place.
   - Inside the boot checkpoint. A crash before step 4 leaves meta `k−1` the newest. A crash between or after step 4's writes leaves a copy of the new `ckpt_id` `k` (its image synced at step 3) or meta `k−1`. No tie can arise, because the retire left zeros in parity `k`'s places (D17).
   - One residual, not a defect. Between the retire and the boot checkpoint's step 5, the WAL between meta `k−1`'s and meta `k`'s `wal_ckpt_end` lies above the opened meta's `wal_ckpt_end`. A new post-sync loss there is undetected (§19 T4's class) until the new meta `k` records a higher end.
2. **I-FREE-SAFE's exception (R2).** Gone, and right: the exception is no weaker than 7.10. The residual is S-ALPHA-F2.
3. **No fall-back unless step 2 read the previous free set (R3).** Gone, and right, though conservative.
   - A failure in step 2 has written nothing, but the fall-back's own step 2 needs that free set anyway. In pass 2, the read is what kept the evictions off image `k−1`.
   - Its test was N1's variant (V5).
   - The text leaves open whether step 2 still reads the free set after a read of image `k` has failed in it. The proven case's FreeSet-page cell can pass only if it does, so the test pins the behaviour, and a clarifying sentence rides (F6 (c)).
4. **§11 (R4a–b).** Gone, and right.
5. **Fixtures that lost one copy (R5a–g).** Gone. The four mutants can fail (see "Mutants" below).
6. **The chained fall-back (R6a–c).** Gone; the text now states what the design does. The worst case:
   - Both copies of metas `K` and `K−1` are lost after step 6 of `K`, and the starts of metas `K−3` and `K−2` lie in one surviving segment.
   - The open selects `K−2`, since parity `K` holds `K−2` and parity `K−1` holds `K−3`.
   - A replayed page of image `K−2` lies in a slot that step 6 of `K` freed and a later write reused, so it fails I-INTEGRITY.
   - Step 4a reads `K−3`'s free set, proves against `K−2` only, retires `K−2` and opens `K−3`, whose superseded slots step 6 of `K−1` freed.
   - Nothing is served wrong without an error. Every page is read through a reference whose `crc`, `id` and `epoch` a later write into its slot cannot match, barring a CRC32C collision. The WAL from `K−3`'s start is contiguous (lowest-first recycling, each rename durable) or I-WAL-START refuses. Publications naming `K−2` or later fail step 3b. An older image only holds more versions.
   - So every commit acked under an honoured sync is either replayed or reported by an XX001 read. **No path is silent.** The cost is availability: such a store can open and then fail reads.
7. **The misplaced copy first (R7, R7b).** Gone, and right. Every crash point is safe in `model/retire.py`. A one-sync retire gives XX001 in 6 crash states (TM1).
8. **The identity rule's order (R8).** Gone, and right.
   - One case at three or more faults: page 0 invalid, every own copy invalid, and a foreign copy present. The rule then adopts the foreign identity and selects the foreign meta (33 of the 167,042 alphabet combinations).
   - Its first read fails I-INTEGRITY, so the open still fails. This is outside the card's fault budget and is noted only.
9. **"Steps 1–3b" (R9).** Gone.
10. **Wording (R10a–b).** Gone.

The first verdict's extra scenario, the equal-`boot_id` tie at three faults, stands as S-ALPHA-F1.

## The open rule, rebuilt (`~/.cache/nv3-review/gate-S-ALPHA-2/model/`)
`openrule.py` is §2.2's prose at 41795fba. `exhaustive.py` runs it over every combination of the four meta places (15 symbols each) × page 0, 167,042 in all. That covers:
- each place's content: a copy of meta `L+1`, `L`, `L−1`, `L−2` or `L−3`, with either `copy` byte;
- an older-boot or equal-boot version of `L`;
- zeros, an unreadable place, or a foreign copy.

The prose rule and §2.2's per-parity table never disagree.

`simulate.py` replays histories that end at newest synced meta `L` ∈ {0, 1, 2, 3, 6}.
- The crash cases: no crash, or a crash during checkpoint `L+1`'s step 4. Each of its two writes is then not done, done or torn, in either order.
- Each fault counts as one: LOST, TORN or MISDIRECTED on a synced copy of metas `L−5`…`L`, landing on another meta place or a page slot; a misdirect of an in-flight copy; a read error on a place; page 0 invalid; or a foreign copy.
- "Safe" means the open selects `L+1` (a copy persisted; its image was synced at step 3), `L` or `L−1`. Each of these recovers every commit acked under an honoured sync, by I-TWO-GEN and the walk's crossing into newer records.

The results:
- **0 faults (95 states):** every open is safe, with no refusal. A crash inside file creation, before meta 0 is durable, is XX001 by design (R14), and nothing was acked then.
- **1 fault (4,743 states):** 4,663 safe. 130 of them select `L` instead of `L+1`: the N2 case, which recovers the same state. **80 refuse**, all in one shape, S-ALPHA-F5's:
  - a write of an in-flight copy of meta `L+1` is misdirected into a place of meta `L`'s parity;
  - a crash follows before the other copy persists;
  - the misplaced `L+1` exceeds the newest valid meta `L`, so I-META-SLOT gives XX001.
  - It was already so at 7.10. It is the only one-fault refusal of a recoverable store, and it fails closed.
- **2 faults (127,132 states):** 121,384 safe. The refusals, named:
  - I-META-SLOT, misplaced copy newer than the newest (4,090). Either F5's shape plus any second fault; or a synced copy of `L` misdirected into the other parity with `L`'s other copy taken out (the META-WRONG-SLOT fixture at A-7d).
  - No valid copy (846). At `L` = 0, both copies of meta 0. At `L` = 1, meta 1's two copies with a crash that tore meta 2's writes over meta 0. At `L` ≥ 2, `L`'s two copies made invalid (torn, unreadable or foreign) with a crash that tore both in-flight copies over `L−1`'s places.
  - Page 0 invalid with a foreign copy (380).
  - Page 0 and both replica blocks invalid (162). At `L` = 0 parity 1's block is still zero; otherwise a crash tore the other replica.
  - 58030 needs four read errors, and a tie needs a rollback and at least three faults: none occur here.
- **Opens outside two generations (270 two-fault states):** both copies of `L` taken out, plus a crash that tore both in-flight copies of `L+1` over `L−1`'s places. The open selects `L−2`. Such an open is detected per read (I-INTEGRITY, I-WAL-START) and is never silent.
- With both copies of `L` out, the open selects `L−1` and names `L−2` as the previous meta. A third fault, image `L−1` failing at open, would chain to an image outside two generations, again detected per read.
- **Silent loss: none** at 0, 1 or 2 faults. No foreign meta opens at two faults or fewer.

`retire.py`: every crash point of the two-sync retire write opens meta `k` (image `k` untouched) or meta `k−1` (retired), with and without a misplaced copy.

## Mutants: fixture and owning Phase A card
| Mutant | Fixture that kills it | Phase A owner (§15.1) |
|---|---|---|
| REPLICA-SKIP | write form: the replica place does not hold meta `k` after a completed checkpoint; fault form: LOST on meta `k`'s primary opens `k−1` | A-7c, A-7d |
| COPY-INDEX-OFF | write form: the two `copy` bytes are equal | A-7c |
| TIE-NO-BOOT-ID | table form (the newer `boot_id` in the replica); tie variant (the primary's write lost) | A-2, A-7d |
| TIE-EQUAL-OPENS | table form | A-2 |
| MISPLACED-SHADOWS | table form; fault form's misdirect onto another meta place | A-2, A-7d |
| IDENTITY-SKIP | table form and fault form's foreign copy, page 0 valid | A-2, A-7d |
| PAGE0-REQUIRED | table form; fault form's page-0 fault | A-2, A-7d |
| REPLICA-SLOT-FREE | `t_replica_region`: the write hook at slot 256; the persisted bitmaps | A-3c, A-7b |
| FREE-ONE-GEN | `t_two_gen`: the allocation hook (S3); the write hook; (a) | A-3c, A-7c, A-7d |
| RECYCLE-ONE-GEN | `t_two_gen`: the rename hook; (a) | A-7c, A-7d |
| OPEN-PROTECTS-ONE-GEN | `t_two_gen` (b), with the overwritten slot on a page the fall-back reads | A-7d |
| FALLBACK-OFF | `t_fallback`, proven | A-7d |
| FALLBACK-NO-PROOF | `t_fallback`, unproven: its step-2 cells now, every cell after V4 | A-7d |
| FALLBACK-TWO-GEN | `t_fallback`, two generations: a read through meta `k−3`'s references, and the detail | A-7d |
| RETIRE-SKIP | proven's hook; retire: the next open selects meta `k` | A-7d |
| RETIRE-ONE-COPY | retire: the next open selects meta `k` through its replica | A-7d |
| ROLLBACK-SKIPS-PREV-CHECKS | unproven: a rollback over a second loss in meta `k−1`'s coverage span | A-7d |
| DEP-CHECK-SKIP | `t_ckpt_dep`, main case | A-7d |
| DEP-BY-POSITION | `t_ckpt_dep` (c) | A-7d |
| DEP-FIRST-ONLY | `t_ckpt_dep` (b) | A-7d |
| DEP-CHECK-LATE | main case: byte identity (an evicting pool) and the read-only cut | A-7d |
| DEP-ID-STALE | main case | A-7d |
| DEP-SALVAGE-PAST-CUT | read-only form | A-7d |
| START-SEG-AS-END | `t_wal_start`: the A-6b walk; (a) | A-6b, A-7d |
| SEG0-LAZY | the A-6b creation; (d) | A-6b, A-7d |
| START-SEG-NOT-ENSURED | the A-7c flip at a segment's first LSN; (d) | A-7c, A-7d |
| RECYCLE-UNORDERED | A-6b under lost renames; (c) | A-6b, A-7d |
| MAPINNER-CREATE-UNCONDITIONAL | MapInner race, amcheck (S2) | A-4a, A-7d |
| LEVEL-GROW-UNCONDITIONAL | level race, amcheck | A-4a, A-7d |
| MAP-ROOT-CURRENT | namespace variant on S1–S3 | A-3c, A-7d |
| MAP-POSITION-FROM-ROOT | namespace variant on S1–S3 | A-3c, A-7d |

The eight mutants the first verdict found unable to fail can all fail now.
- **BOOT-ID-EQ-STOP.** `t_meta_rollback_crosses_boot` now loses both copies of meta `k+1` (R5a), so the open selects meta `k`; parity `k+1` holds `k−1`. The mutant stops at `p_b`, and coverage passes because `p_b` ≥ meta `k`'s `wal_ckpt_end`. Its two fixture constraints now hold by I-TWO-GEN anyway.
- **BOOT-ID-NO-META-FLOOR.** Both copies are lost at each rollback (R5c–d). Boot `b+3` gets id `k+2`, older than meta `k+1`'s `k+3`, so the walk stops at `p`.
- **BOOT-ID-CKPT-ONLY.** Both boot metas lose both copies (R5b). Both boots get id `k+1`, and `b+1`'s TxnId `(k+1, 1)` meets `b`'s unmarked chunks.
- **META-WRONG-SLOT at A-7d.** A misdirected copy, with the other copy lost (R5e–f): the newest valid meta is `k−1` and the misplaced copy is `k`, so the rule gives XX001 and the mutant opens `k`.
- **REPLICA-SKIP at A-7c and COPY-INDEX-OFF.** Both fail through R13's completed-checkpoint condition.
- **FALLBACK-TWO-GEN.** It fails through R17a's hook and detail.
- **TIE-NO-BOOT-ID at A-7d.** R12b makes it fail: the old meta `k`'s `boot_id` is older because the walk accepted records of its boot.

**Mutants that cannot fail as specified: none.**
- FREE-ONE-GEN's A-7d form (a) can, in one cell, chain to meta `k−2` instead of giving XX001: `k−2`'s free set reads and its start segment survives. It still fails at A-3c and A-7c through hooks.
- OPEN-PROTECTS-ONE-GEN fails at A-7d only where the fixture puts the reused slot on a page the open reads, as (b) says it does.

## The owner's points, in plain words
- **O-1. Explicit rollback.**
  - What it means: when the newest checkpoint's data on disk is corrupt, and the log cannot prove that falling back to the previous checkpoint loses nothing, the store will not open for writing. A read-only open shows the previous checkpoint's state plus what the log still holds. An operator can then force a writable open from the previous checkpoint by naming the checkpoint to give up. The store reports what it gives up, still runs every other check, and loses the commits whose log records are gone. Asked for anywhere else, it fails with a new error code, 55000, and changes no data (after V3: at most harmless cache writes into unused space).
  - Recommendation: accept. It is the narrowest reading of the owner's "explicit rollback", and it keeps §10 items 8 and 9.
  - Coherence: the text is written for this answer and is consistent.
  - On "no, export only": these change: E2's 55000 row; step 4a item 5's "Only an explicit rollback opens it read-write."; the "Explicit rollback" paragraph; `t_fallback`'s unproven rollback sentences, with ROLLBACK-SKIPS-PREV-CHECKS and its §15 and §15.1 cells; §14.2's "an explicit rollback opens it" and its 55000 row; I-FALLBACK's "and only an explicit rollback naming meta `k` opens it read-write"; R18's "and an explicit rollback can open it"; and the Status and Changelog phrases. That answer also departs from the owner's own words, "else refuse + explicit rollback".
  - On "also for WAL loss": these change: that paragraph's last sentence ("It does not apply to WAL loss or a splice …"); §14.1's WAL-loss remedy ("The read-write open stays refused …"); §14.2's WAL-loss and splice rows; and PLAN §10 item 8, which is an owner decision.
- **O-2. Two generations everywhere before measuring.**
  - What it means: every mode, the browser included, keeps one more checkpoint's worth of replaced pages and log on disk, so that a damaged newest checkpoint can fall back. On E4's tiny prototype that cost about 14% more space per checkpoint on average, 20% at the peak, and 3% more log. It is not measured on the real store, and the file never shrinks.
  - Recommendation: accept; measure at A-7e on a browser-sized configuration, with §19 T7 as the revisit trigger.
  - Coherence: the text is written for this answer and is consistent.
  - On "no" (browser mode off until measured): a browser-mode condition (one generation, no fall-back) is added to I-TWO-GEN (§4.4 and its row), I-FREE-SAFE, §4.4 item 4, §12.6's recycling sentence, §13 step 6, §14 step 2's protection, §14 step 4a item 1, §5.7's pending-free row, §14.2's fall-back rows, I-FALLBACK, R19, §19 T4's new bullet and T7's trigger. The byte format does not change.
- **O-3. A disk that drops a sync now opens instead of refusing.**
  - What it means: if a disk claims to have saved the newest checkpoint header (both copies) and did not, the store now opens from the previous checkpoint and replays its log. Recent commits whose log bytes the disk also failed to save are missing, with no error. Before, the same disk usually made the store refuse to open at all, which needed a backup and still did not reveal the missing commits. No cheap way to detect them exists: any marker written in the same window can be lost the same way.
  - Recommendation: confirm. This follows from the owner's "two generations".
  - On "no": refusing instead means dropping two generations (the eval's "Drop B, keep A and Part 1"). That reopens decision (2) rather than changing a sentence. The sentences that change are §19 T4's "Two generations change one case" bullet, §14's "Both copies of the newest meta" sentence, and everything that rests on I-TWO-GEN and I-FALLBACK.
- **S-ALPHA-F4. A disk read error on the newest checkpoint.**
  - What it means: if one disk sector under the newest checkpoint's page-map root, free-space map or tree directory becomes permanently unreadable (an I/O error, not wrong bytes), nothing reaches the data. The writable open fails with a disk error. The explicit rollback refuses, because it covers corrupt pages, not unreadable ones. The read-only open fails too. All this happens although the previous checkpoint and the log may be intact.
  - Recommendation: let an explicit rollback, and a read-only open's salvage from meta `k−1`, accept a 58030 on a page read through image `k`'s references. Keep the automatic fall-back limited to integrity failures (D35).
  - Coherence: the text is written for "no", and nothing changes on that answer.
  - On "yes": step 4a's "Nothing else falls back: … a read error (58030) …" gains the exception; the explicit rollback's precondition gains "or a read through its references fails with 58030"; §14.1's corrupt-image bullet and §14.2 gain the case (read-write 58030; an explicit rollback opens; a read-only open serves salvage from meta `k−1`); I-FALLBACK's row changes; and `t_fallback` gains a case.

## What S-BETA must change after this card, checked against 41795fba
The gate's script applied S-BETA's 44 Old blocks in order to 41795fba. Every one occurs exactly once at its turn except **E25** and **E42**, which occur 0 times. The same holds after V1–V10.
1. **Confirmed.** Renumber S-BETA's §18 row R18 → **R23**. Re-anchor E42 on this card's lead-in ("…, R17 from card S-1-F7, and R18–R22 from card S-ALPHA; …", naming R23 from card S-BETA). Renumber E43's row, and change its Acceptance grep `^| R18 | ` to `^| R23 | `.
2. **Confirmed.** E25's Old ("after a meta-slot fault too, …") is gone. Insert its clause after "crosses into newer boots' records (§12.5)" in this card's I-REDO-COMPLETE sentence, before ", when that meta's image and WAL span are intact and no replayed commit depends on a newer checkpoint (§12.8)". This is S-BETA's own R-5.
3. **Confirmed.** E22's step 5 removes segment files above the valid end. §12.5's I-WAL-START sentence ("it leaves `wal/` only by recycling") must be amended to admit step 5's removal of segments whose first LSN lies above its valid end. Such a removal never takes the start segment of the meta that open selected.
4. **Confirmed.** E28's splice cut-back list gains step 3b's dependency check (S-BETA's own R-6). §14.1's "Where several cuts apply, the lowest one does" stands.
5. **Confirmed, and extended.** Each "lost after its sync" in E31's two tests and in rebase point R-4 becomes "both copies lost after their sync". E11's "(a meta rollback, §2.2)" cites "§2.2, §14 step 4a". E12's and E22's "a lost meta" may read "a meta lost in both copies".
6. **Confirmed.** §15.1 starts from 141 rows, 148 after S-BETA's seven. The checker's mutant set is 7.8's, plus this card's 31, plus S-BETA's own, with the 25 Phase-B-only mutants unchanged.
7. **Corrected: nothing to re-anchor.** S-BETA's §14.1 and I-RO-OPEN blocks (E26–E29, E34) still occur once at their turn.
8. **Optional precision.** E11's "`durable_end` at the walk's start, the opened meta's `wal_start_lsn`" means meta `k−1`'s after a fall-back.
9. If the orchestrator assigns them there, S-ALPHA-F6's riders, below.

## Rework: exact corrected text
The rework applies these ten blocks to 41795fba in the order listed. Each Old occurs exactly once at its turn. This was verified on a scratch copy, which then passes every Acceptance line and the checker unchanged; result sha256 `20283ae8fcde9ec3bcf78b05a5e517ecdfbe85b8025624d0e86b9de3089a28e4` with no other change.

They map to the findings as follows:
- V1, V2, V9: N3.
- V3, V7: G2.
- V4: G1.
- V5: N1.
- V6, V8: N2.
- V10: the Changelog.

Each block lies inside the card's own edits, E16, E38, E39, E46, E52 and E59, so the card's Limits need no change. The draft number stays 7.11.

**V1.**
Old:
~~~~text
- **I-TWO-GEN (two generations):** while meta `k` is the durable image, image `k−1` and the WAL from meta `k−1`'s `wal_start_lsn` stay intact.
~~~~
New:
~~~~text
- **I-TWO-GEN (two generations):** while meta `k` is the durable image and the newest meta that was synced, the WAL from meta `k−1`'s `wal_start_lsn` stays intact, and so does image `k−1` as long as every open since meta `k` was synced has read meta `k−1`'s free set (below).
~~~~

**V2.**
Old:
~~~~text
finds image `k−1` and its whole WAL span as they were.
~~~~
New:
~~~~text
finds image `k−1` and its whole WAL span as they were, when every open since that meta was synced read meta `k−1`'s free set (§14 step 4a).
~~~~

**V3.**
Old:
~~~~text
It proceeds only when the newest meta has that `ckpt_id` and its image fails at open; otherwise it fails with **55000** and writes nothing.
~~~~
New:
~~~~text
It proceeds only when the newest meta has that `ckpt_id` and its image fails at open; otherwise it fails with **55000** and writes nothing more than the evictions of a pass 2 that ran from image `k` without a failure, into slots that image `k` does not reference (above).
~~~~

**V4.**
Old:
~~~~text
plus a post-sync LOST on a synced WAL record, holding an acked commit, between meta `k−1`'s and meta `k`'s `wal_ckpt_end`.
~~~~
New:
~~~~text
plus a post-sync LOST on a synced WAL record, holding an acked commit, at or above meta `k−1`'s `wal_ckpt_end` and below meta `k`'s `wal_start_lsn`, so that only the walk from meta `k−1` reads it (in the leaf case, a loss at or above meta `k`'s start would fail step 3a's coverage check before pass 2, where neither a fall-back nor an explicit rollback applies).
~~~~

**V5.**
Old:
~~~~text
*Unread variant:* a hook fails one read of meta `k−1`'s free set in step 2 of the first open only: that open fails with XX001 (no previous image) and writes nothing, and the next open falls back with proof.
~~~~
New:
~~~~text
*Unread variant*, on the proven fixture rather than this one, with its fault on a page of image `k` that step 2 reads (a MapLeaf, a FreeSet page or a directory page): a hook fails one read of meta `k−1`'s free set in step 2 of the first open only: that open fails with XX001 (no previous image) and writes nothing, and the next open falls back with proof and recovers every acked commit.
~~~~

**V6.**
Old:
~~~~text
- **A meta copy:** one copy lost, torn or misdirected after its sync, or unreadable: the other copy opens the same meta (I-META-COPIES).
~~~~
New:
~~~~text
- **A meta copy:** where both copies of a meta were written, one copy lost, torn or misdirected after its sync, or unreadable: the other copy opens the same meta (I-META-COPIES). Where a crash between step 4's two writes left one copy of the newest meta, a fault on that copy opens the meta before it, which recovers the same state (§2.2).
~~~~

**V7.**
Old:
~~~~text
| An explicit rollback whose precondition does not hold | 55000, nothing written | (read-write only) |
~~~~
New:
~~~~text
| An explicit rollback whose precondition does not hold | 55000, nothing written beyond the evictions of a pass 2 that found no failure | (read-write only) |
~~~~

**V8.**
Old:
~~~~text
one fault on one copy never changes which meta opens or what it recovers; no page has a reserved slot as its home.
~~~~
New:
~~~~text
one fault on one copy of a meta whose two copies were both written never changes which meta opens or what it recovers; no page has a reserved slot as its home.
~~~~

**V9.**
Old:
~~~~text
| I-TWO-GEN | §4.4, §12.6, §13 step 6, §14 step 2: while meta `k` is the durable image, image `k−1` and the WAL from meta `k−1`'s `wal_start_lsn` stay intact;
~~~~
New:
~~~~text
| I-TWO-GEN | §4.4, §12.6, §13 step 6, §14 step 2: while meta `k` is the durable image and the newest meta that was synced, the WAL from meta `k−1`'s `wal_start_lsn` stays intact, and so does image `k−1` as long as every open since meta `k` was synced has read meta `k−1`'s free set;
~~~~

**V10.**
Old:
~~~~text
and the test texts that a correct build could not pass, or whose mutant could not fail.
~~~~
New:
~~~~text
and the test texts that a correct build could not pass, or whose mutant could not fail. A second gate (REWORK on 41795fba): I-META-COPIES and I-TWO-GEN stated with their conditions, the explicit rollback's 55000 after a pass 2 that evicted, and two more test texts that a correct build could not pass (`t_fallback`'s unread variant and the placement of its unproven case's WAL loss).
~~~~

## Follow-ups
- **S-ALPHA-F1 — unreadable meta places (the eval's dropped fence) and the equal-`boot_id` tie.** Kept, unchanged from the first verdict.
  - An open whose read of a meta place fails treats the place as invalid and may select an older meta, reuse the newer image's slots, and crash before its boot checkpoint's step 5. When the read later succeeds, the next open selects the newer meta over a partly overwritten image. That image fails I-INTEGRITY at open (and falls back safely) or at a later read.
  - The same mechanism gives the three-fault equal-`boot_id` tie.
  - Candidate: before its first write, a read-write open zeroes and syncs every meta place whose read failed, as the retire write does. Open-time policy, no format change; before A-7d.
- **S-ALPHA-F2 — protect the previous image when its free set cannot be read.** Kept.
  - After R3 and V1/V9, an open that cannot read meta `k−1`'s free set neither falls back nor protects image `k−1`. A later open that reads it can fall back onto reused slots, which fail I-INTEGRITY when read; this is detected, not silent.
  - Candidate: protect every slot below meta `k−1`'s `slot_count` that image `k` does not reference until the first step 6. I-FREE-SAFE's exception and V1's condition then shrink to "no previous meta". Before A-7d.
- **S-ALPHA-F4 — a persistent read error (58030) on the newest image (owner, with O-1).** Kept. See "The owner's points" above for the plain words, the recommendation, and the sentences each answer changes.
- **S-ALPHA-F5 — I-META-SLOT under two generations.** Kept.
  - The gate's model confirms this is the only one-fault refusal of a recoverable store: 80 of 4,743 one-fault states.
  - The candidate was re-derived and is sound. Refuse only when a misplaced `ckpt_id` exceeds `k+1`. A misplaced copy of meta `k+1` shows only that checkpoint `k+1`'s step 3 completed. If meta `k+1` was synced, its step 6 kept image `k` and the WAL from meta `k`'s start. So opening meta `k` recovers every acked commit.
  - It restates an invariant, so it needs a P card. Scope it with F1.
- **S-ALPHA-F6 (new) — riders for the next S-STORE card.** Wording only, none blocking: they are consistent with the text's defined terms, or the tests already pin them. Give them to S-BETA, which merges next; if S-BETA's Limits cannot take them, to the stand-ins tidy card (S-15-STANDINS-2-F1 with S-15-RELABEL and PL-11-F2).
  - (a) N4: I-FALLBACK's row "never two generations" → "never two generations from the meta the open selects".
  - (b) N5: in `t_meta_copies`' tie variant, ", so that the stale meta `k` sits beside the new one" → "; the stale meta `k` then sits beside the new one".
  - (c) Say that §14 step 2 reads meta `k−1`'s free set even after a read of image `k` has failed in it. `t_fallback`'s FreeSet-page cell and V5's variant already require this.
  - (d) Several sentences call image `k−1` intact by citing I-TWO-GEN: §1's "previous image"; §14's "whose image and WAL span are intact (I-TWO-GEN)"; I-REDO-COMPLETE's "The previous meta's image and WAL span are intact (I-TWO-GEN)"; and the Status line's and Changelog's "stay intact until meta `k+1` is durable". Each inherits V1's condition through the citation. A rider can say "as I-TWO-GEN states".
- The first verdict's S-ALPHA-F3 (fall-back hardening) is not in QUEUE. The gate does not re-raise it: its points are hardening, not defects. Its point (b) — keeping image `k` findable when image `k−1` fails after the retire — belongs to §10 item 15's card.
