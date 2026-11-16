Card: S-ALPHA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session daa11f52-d255-41a3-93b9-30dec11aadd9 (a fresh gate run; this agent wrote neither the card nor the S-ALPHA commit)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: 3ccc8b3d3712edae419fb3630b613ba47b1e2cfd
Mutants: the card names none (spec text). The gate's own text mutants, each on a scratch copy of the tip, judged by the card's Acceptance (line 10 is the checker): TM1 (the card's control: `t_fallback`'s row card A-7d -> B-6) -> checker; TM2 (the card's control: I-MAP-GROW's §15 row removed) -> lines 5 and 10; TM3 (the card's control: unpatched 7.10) -> lines 1-10; TM4 (COPY-INDEX-OFF's only row A-7c -> B-6) -> checker; TM5 (`t_two_gen`'s A-3c row needs B-3) -> checker; TM6 (META-WRONG-SLOT renamed in §15) -> checker; TM7 (I-FREE-SAFE back to "meta `k`") -> line 8; TM8 (the retire write zeroes the primary copy only) -> UNCAUGHT; TM9 (step 3b's "Every op is checked, not only the first." deleted) -> UNCAUGHT; TM10 (step 6 recycles below meta `k`'s own start) -> UNCAUGHT
Followups: S-ALPHA-F1, S-ALPHA-F2, S-ALPHA-F3, S-ALPHA-F4, S-ALPHA-F5

The shape is exact and the design is the owner's, but seven of the ten contradictions the implementer reported are real, five of them concern normative behaviour (items 1, 2, 3, 6, 7), one disables three 7.10 mutants (item 5), and the gate found nine more test or wording defects that a correct build could not pass or whose mutant could not fail. Every correction is below as an exact Old/New pair (R1a–R18, 41 blocks), checked on a scratch copy of 3ccc8b3d: each Old occurs once at its turn, and with all 41 applied every Acceptance line still passes (checker: `141 rows; 200 mutants; 31 new, 31 of them judged on a Phase A row; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8` / `OK`). No silent loss of a commit acked under a sync the device honoured was found, and no healthy store is refused at zero or one fault.

## Gate run
- Scaffold extract (GATE-PROMPT step 1): shallow `--no-local` clone of `nucleus/v3-scaffold` at **108e180ef4f3e557b68f8bfd65b4a1df78ba3317**; `fresh-gate.sh` and `git-env.sh` extracted, no `EXTRACT FAILED`.
- `bash "$fg/fresh-gate.sh" 3ccc8b3d3712edae419fb3630b613ba47b1e2cfd` from `~/Workspace/neutron-v3`, `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-ALPHA`, stdout and stderr captured:
  - `fresh-gate: commit 3ccc8b3d3712edae419fb3630b613ba47b1e2cfd`
  - `fresh-gate: base 108e180ef4f3e557b68f8bfd65b4a1df78ba3317`
  - `fresh-gate: checks passed (4813 files hashed in 1.42 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`
  - literal last line: `GATE EXIT=0`; stderr empty; `$fg` removed afterwards.
- Worktree `~/.cache/nv3-cards/S-ALPHA`, branch `card/v3-S-ALPHA`, tip 3ccc8b3d…, clean before and after (every mutant ran on scratch copies). No worker log for S-ALPHA exists under `cards/logs/`; the implementer's ten contradictions reached the gate through the dispatch.

## Scope and the diff
- `git diff --stat nucleus/v3-scaffold...HEAD`: one file, `nucleus/v3/docs/S-STORE.md`, +242/−48, 1,599 → 1,793 lines; it is the card's Touch only list; no protected path.
- 7.10's sha256 is `2e90020086b756b57d8741b25ed3cfbd98561a307f5dea443043fb7d45430825`, the card's base. My own parser of the card's 59 blocks (118 fences) applied them to 7.10 in order with `<date>` = 2026-11-15: every Old occurred once at its turn, and the result is byte-identical to the commit (sha256 `22a875abbd4de609213ab2699eb89aa32f65dfe22d12160b54c1e385f02e173e`). The diff is exactly E1–E59.
- Scripted against 7.10: invariant rows 40 → 46, none removed; mutants 169 → 200, none removed or renamed; tests 36 → 42, none removed; the 40 existing Named-mutants cells are byte-identical; three existing §15 rows change in statement or owners only (I-META-SLOT by E49, I-CKPT-AT-T by E50, I-RO-OPEN by E51); §15.1's 122 rows are an unchanged prefix of its 141. No statement is weakened as text, but item 5 below disables three 7.10 mutants' only fixtures.
- Status line and Changelog are right for 7.11 (card S-ALPHA, 2026-11-15, the 7.10 line kept byte for byte; Acceptance lines 1–3).
- Acceptance lines 1–11 pass in the worktree; line 12 is this gate run.
- Independent placement check, with a Depends map I read from PLAN §9's cells myself (not the card's dict; "dropped" notes excluded): no cycle; none of the 141 rows puts a Phase A card on Phase B code or on a card outside its Depends closure; 175 mutants are judged on a Phase A row and 25 only on Phase B cards, the same 25 as at 7.10, none of them new; every one of the 31 new mutants is judged on at least one Phase A row (placements as the card's map).

## Rulings on the implementer's ten contradictions
1. **"Writes nothing" vs pass-2 writes: REAL, normative.** Step 4a's trigger includes pass 2, and items 1–2 say pass 2 may have evicted, yet item 5, §14.2's row and I-FALLBACK say an unproven read-write open "writes nothing", the retire write must precede "anything else", `t_fallback`'s proven hook ("before the open's first other write") fires on a correct build, and its unproven byte-identity fails, whenever the damaged page is a leaf pass 2 reaches after evicting. The writes are harmless (slots neither image references, given step 2's protection), so the text must say so and order the retire write before the first write *from meta `k−1`*. R1a–R1j.
2. **I-FREE-SAFE vs I-TWO-GEN's exception: REAL, normative contradiction.** With no previous image, §14 step 2 frees image `k−1`'s superseded slots at once; I-FREE-SAFE allows no exception. That is no weaker than 7.10 (those slots were free once meta `k` was synced), so the exception is stated in I-FREE-SAFE. R2.
3. **Unreadable previous free set: REAL, normative.** A step-2 failure with a transient 58030 on the previous free set falls back under item 1 and is refused under I-TWO-GEN. Resolved to I-TWO-GEN's outcome, which is verify §2.4's ("no protection, no fall-back"): no fall-back unless step 2 read the previous free set, wherever image `k` failed; a retry falls back once the read succeeds. Both readings leave a residual across opens (S-ALPHA-F2). R3, with a test variant in R7b.
4. **§11 one checkpoint early: REAL (stale normative text);** §11 item 2 also omits the previous image. R4a, R4b.
5. **Fixtures that lose one copy: REAL, blocking.** With two copies one LOST write rolls nothing back. `t_meta_rollback_crosses_boot`, the only judge of BOOT-ID-EQ-STOP, now opens meta `k+1` (so its "selects meta `k`" fails a correct build and the mutant cannot fail); the variant of `t_boot_id_after_rollback`, the only judge of BOOT-ID-NO-META-FLOOR, the same; its main form leaves the mechanism unstated (BOOT-ID-CKPT-ONLY). The gate adds a fourth: I-META-SLOT's A-7d fixture "SimEnv misdirected meta" no longer reaches a misplaced-newer state while the other copy is valid (META-WRONG-SLOT still dies at A-2). §12.5's "a meta-slot fault can leave an older meta" is stale too. R5a–R5g.
6. **"Detected" can mean a further fall-back: REAL (text vs behaviour); no silent path.** Worst case: both copies of metas `K` and `K−1` lost after `K`'s step 6; metas `K−3`, `K−2` and `K−1` started in one segment, so the walk's start survives; the open selects `K−2`, a replayed page of image `K−2` sits in a slot reused after step 6 of `K` and fails I-INTEGRITY, step 4a falls back to `K−3` with proof against `K−2` only, retires `K−2`, and opens. Pages of image `K−3` that changed before `K−2` only by unlogged work (GC prunes, pack steps, drop walks) are not read at open; their slots, freed at step 6 of `K−1`, may hold later writes, so the first read of each fails I-INTEGRITY (XX001; a gated operation poisons). Never silent: every write into a reused slot carries a later page whose `crc`, `id` or `epoch` differs from the older image's reference; the WAL from the start is intact (lowest-first recycling); publications over the older image fail step 3b. But the store opens, reports a proven fall-back, and then fails reads. R6a–R6c and R3's last sentences state it; S-ALPHA-F3 proposes a narrowing.
7. **Retire write vs a misplaced copy: REAL, normative; strands an intact store.** Simulated: one of meta `k`'s copies misdirected into a place of parity `k−1` (meta `k−1` keeps its other copy, so the open still selects `k` with previous `k−1`); image `k` fails at open; proven fall-back; the retire zeroes meta `k`'s two own places; a crash before the boot checkpoint's step 5. The next open, read-write and read-only alike, is XX001 (a misplaced copy newer than every valid one), and no explicit rollback applies (its precondition, a newest image that fails at open, does not hold): image `k−1` and a proven-complete WAL, unreachable after two faults and an ordinary crash. "Both copies" in the text means meta `k`'s two places; it must mean every place holding a copy, the misplaced ones zeroed and synced first (zeroing all three under one sync can leave exactly this state). R7, with a test variant in R7b.
8. **Identity rule vs its table: wording.** The outcome is defined (§14.2's first row: XX001 with page 0 invalid and copies of two identities, two faults, fails closed), but the precedence is unstated and "if neither replica is valid" is circular. R8.
9. **"Steps 1–3b again": wording;** the item's own list shows only step 1's range check is meant. R9.
10. **Wording, both:** "the meta slot that loses" (R10a); S1's reference to item 2's last sentence, which also records the boot id (R10b).

**The extra scenario (equal `boot_id` tie): REAL, multi-fault, fails closed, strands the store.** A boot that appended no records writes meta `k` as its boot checkpoint; at the next open both copies of meta `k` return read errors, so it selects `k−1`, and its walk accepts the same records, so the three-way boot id comes out equal; its boot checkpoint rewrites `ckpt_id` `k`, one copy's write is lost, and the old meta `k` sits beside the new one with equal `boot_id`s: XX001, read-only too. Three faults; not a text contradiction. S-ALPHA-F1 closes it with the fence it proposes.

## The design, as a durability review

### The open rule (§2.2, §14.2): every combination
I modelled §2.2's rule exactly as written (`~/.cache/nv3-review/gate-S-ALPHA/model/`) and ran it over histories of six checkpoints: LOST, TORN or MISDIRECTED (to each other place) on each synced copy of the three newest metas, every crash state of the in-flight meta's two writes (none, whole or torn each), read errors on any place, page 0 valid or invalid, and foreign copies. With `L` the newest synced meta, an open is safe when it selects `L−1`, `L` or a persisted `L+1` (whose image step 3 synced).
- **0 faults:** always `L` or a persisted `L+1`.
- **1 fault** (any of the above, page 0 included): always safe, never a refusal. In four cases it selects another meta than the faultless run: a crash between step 4's writes leaves one copy of `L+1`, and a read error on that copy selects `L`; safe, but I-META-COPIES' sentence is too broad (R11), `t_meta_copies`' fault form would fail a correct build there (R12a), and the aftermath is S-ALPHA-F1.
- **2 faults:** 60 refusals (page 0 invalid with no replica block holding a valid copy; no valid copy; a misplaced copy newer than the newest) and 24 opens older than two generations (both copies of `L` gone and the in-flight meta torn over `L−1`'s places), all detected per read.

Per parity (either place order): valid `m` twice, equal but for `copy` and `crc` → `m`; valid `m` twice, other bytes differing → the newer `boot_id`, equal ones XX001; valid `m` with a stale valid `m′` → `m`; valid `m` with a torn, zeroed, unreadable, foreign or misplaced copy → `m`; a stale copy alone → that older meta; nothing valid → none. Across parities and page 0:

| Meta `L`'s parity | Meta `L−1`'s parity | Page 0 | Open | (a) silent loss under honoured syncs | (b) refuses a healthy store | (c) undefined |
|---|---|---|---|---|---|---|
| holds `L` | holds `L−1` | either | `L`, previous `L−1` | no | no | no |
| holds `L` | stale or none | either | `L`, no previous meta, no fall-back | no | no | no |
| stale or none | holds `L−1` | either | `L−1` (O-3) | no: needs a dropped sync | no | no |
| stale or none | stale or none | either | an older meta or XX001; outside two generations | no: I-INTEGRITY per read, I-WAL-START, I-CKPT-DEP | n/a (two faults) | text fixed by R6 |
| an in-flight `L+1` copy is valid | — | either | `L+1` (image synced at step 3) | no | no | no |
| a misplaced copy above the newest | — | either | XX001 | no | not healthy: one misdirect of an in-flight copy plus a crash refuses an intact store (pre-existing, S-ALPHA-F5); the retire write could create it (item 7, R7) | no |
| valid copies | valid copies | invalid, a foreign copy present | XX001 | no | no (two faults) | precedence unstated (R8) |
| — | — | invalid, both replica blocks without a valid copy | XX001 | no | no (two faults when the other block is still zero at checkpoint 0 or torn by a crash in step 4; three otherwise) | "valid" circular (R8) |
| every place unreadable | | either | 58030 | no | no | no |

(a) never: under syncs the device honoured, the newest synced meta keeps both copies, its image and its WAL span (I-META-ATOMIC, I-TWO-GEN). (b) never at zero or one fault. (c) only the two wording points of item 8, and `t_wal_start` (d)'s crash inside file creation (R14).

### The fall-back's proof
Given image `k−1` as it was, reaching meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi` from meta `k−1` loses nothing acked: commits `≤ T(k−1)` are in image `k−1`; every commit in `(T(k−1), T(k)]` has its record at or above meta `k−1`'s start (§12.6) and below meta `k`'s `wal_ckpt_end`, so it is replayed; later ones are replayed as from meta `k`. Unlogged loads: a publication naming `k` or later fails step 3b in item 2 (so neither a fall-back nor an explicit rollback proceeds), and an unpublished tree is an unfinished build (§12.8 item 4). GC: image `k−1` holds more versions, and W restarts from its `gc_w`. Evictions made before image `k` failed went to slots neither image references, given step 2's protection. DDL and drops are logged and replayed, the drop walk resumes at step 6, and LPIDs are re-derived (the WAL carries none). Boot ids: the new boot's id is newer than every record its walk accepted and than meta `k−1`'s, and meta `k`, whose id it may equal, is zeroed, so no tie arises (D17). The weak premise is "image `k−1` as it was": true when meta `k` is the newest synced meta and every open since protected image `k−1`; false after two lost metas (item 6) and after an earlier open that could not read the previous free set (S-ALPHA-F2). R3 now says so.

### Two generations
Step 6 after a reopen frees the open's pending list (image `k−1` minus image `k`), which image `k+1` cannot reference, and recycles below the opened meta's start, the same as "meta `k−1`'s" for checkpoint `k+1`. After a fall-back the opened meta is `k−1`, meta `k`'s places hold zeros, so there is no previous meta and nothing more to protect; the boot checkpoint writes `ckpt_id` `k`, and its step 6 frees nothing extra and recycles below meta `k−1`'s start. At creation and at checkpoints 1 and 2 the rules reduce correctly (no previous meta at 0; image 0 protected from meta 1 on). One availability note: checkpoint `k+1`'s step 4 overwrites meta `k−1`'s places, so a crash that tears both new copies also removes the fall-back target of meta `k` until the next checkpoint (safe; no fall-back is then possible).

### I-WAL-START
No healthy refusal found. Segment 0 exists before any meta; step 1 makes a boundary start segment durable (D27 is needed: an exact-end record leaves the next segment unmade); a start held back by a chunk lies in a segment that holds records. D26 carries E4-R2's premise better than the prototype did: each rename is made durable before the next, so "while a segment is present, no higher one was recycled" holds on a real filesystem, not only under the sim's prefix order. E4-R2's other premise, that nothing unlinks a segment, is carried as "it leaves `wal/` only by recycling", which S-BETA's E22 (step 5 may remove segments above the valid end) contradicts: S-BETA must amend that sentence (below). For the plan: OPFS has no directory sync, so `Env` must make each rename durable before the next on every target, or browser mode loses I-WAL-START's argument.

### The retire write
D17's argument holds: a replacement header with `ckpt_id` `k` ties with the abandoned meta if one of its copies is lost, and its `boot_id` can neither be raised (the walk from meta `k−1` would stop at its first record, §12.5) nor kept (the abandoned meta wins the tie). Zeros leave no tie and keep §14 step 8 and C-T0 v8 §2.5 unchanged. Two defects: item 7 (R7), and the retire's place before pass 2 from meta `k−1`, so that an integrity failure of image `k−1` there leaves meta `k` zeroed. That changes no outcome in §14.2 (both images then refuse), but it removes the only pointer to an image that was undamaged but for one page, which a future salvage (§10 item 15) could read (S-ALPHA-F3).

### Map growth
Sound. One map latch at a time, top-down; a new level is made only under the current root's exclusive latch after re-checking that the root is unchanged and still too short; a MapInner only under its parent's latch with the ref still null. No frame, eviction or read happens under a map latch (MapInners live outside the pool; the parent's epoch copy comes from the reserve's never-waiting draw or claim). Growth is gated, so it lies wholly on one side of a flip; step 0 records the root and `map_level`, and step 2 builds image `k` up to that root; positions counted from the leaf keep page ids and image-record keys stable; a crash loses only epoch-`k+1` pages, which no durable image maps. A `u8` `map_level` suffices for any fanout `f ≥ 2`. The races `t_alloc_restart_progress` names are the ones that lose a mapping, and its mutants fail through amcheck (I-LPID-EXACT). A test build's fanout is not recorded in the file; a mismatched build fails I-INTEGRITY's `id` check, which fails closed.

### Further test and wording defects found by the gate
- I-META-COPIES' single-fault sentence ignores a crash between step 4's writes (R11), and `t_meta_copies`' fault form puts its read error "on one copy of the newest meta", which fails a correct build there (R12a).
- The tie variant kills TIE-NO-BOOT-ID only if the primary's write is the lost one, and needs meta `k`'s `boot_id` strictly older (R12b).
- The write form's pass condition allows the parity's previous meta in either place after a completed checkpoint, so REPLICA-SKIP cannot fail at A-7c, and "differ only in `copy` and `crc`" lets byte-identical copies pass, so COPY-INDEX-OFF (judged only there) may not fail (R13).
- `t_wal_start` (d) requires every open to succeed after a crash at every step of file creation; before meta 0 is durable no valid meta copy exists, so a correct build gives XX001 (R14).
- §12.8 keeps E4-SILENT-1's over-refusal "an open from the forced image itself refuses when a later checkpoint was already durable when the id was read", but here the publication carries the forced checkpoint's own `ckpt_id`, so that open never refuses (R15).
- `RequireCkpt` is "before every other op of its record", while `CreateTree`, `DropTree` and `Ddl` are "first within a record"; a publishing commit holding a `Ddl` meets both, and a decoder that enforced either would reject the other's records (R16, R16b).
- FALLBACK-TWO-GEN's run ends in XX001 under the rule and under the mutant (image `k−3`'s reused slots or recycled start), so the test needs an observable (R17a). The A-2 table form omits an unreadable place (R17b).

## Mutants: the fixture that kills each, and its owner

| Mutant | Killing fixture as specified | Phase A owner (§15.1, PLAN §9) | Can it fail as specified? |
|---|---|---|---|
| REPLICA-SKIP | fault form: LOST on meta `k`'s primary opens `k−1`; bulk-load variant refused by 3b | A-7d (A-7c's write form) | at A-7d yes; at A-7c no until R13 |
| COPY-INDEX-OFF | write form: the two places' bytes equal | A-7c | only under a reading R13 makes explicit |
| TIE-NO-BOOT-ID | table form; tie variant | A-2, A-7d | A-2 yes; A-7d only with R12b |
| TIE-EQUAL-OPENS | table form | A-2 | yes |
| MISPLACED-SHADOWS | table form; fault form's misdirect onto another meta place | A-2, A-7d | yes |
| IDENTITY-SKIP | table form; fault form's foreign copy | A-2, A-7d | yes |
| PAGE0-REQUIRED | table form; fault form's page-0 fault | A-2, A-7d | yes |
| REPLICA-SLOT-FREE | `t_replica_region`'s write hook at slot 256; persisted bitmaps | A-3c, A-7b | yes |
| FREE-ONE-GEN | `t_two_gen`: allocation hook (S3), write hook, (a) | A-3c, A-7c, A-7d | yes |
| RECYCLE-ONE-GEN | `t_two_gen`: rename hook, (a) | A-7c, A-7d | yes |
| OPEN-PROTECTS-ONE-GEN | `t_two_gen` (b) | A-7d | yes, if the overwritten slot is one the fall-back reads (or amcheck runs) |
| FALLBACK-OFF | `t_fallback` proven | A-7d | yes |
| FALLBACK-NO-PROOF | `t_fallback` unproven | A-7d | yes |
| FALLBACK-TWO-GEN | `t_fallback` two generations | A-7d | only with R17a's observable |
| RETIRE-SKIP | `t_fallback` retire, and the proven hook | A-7d | yes (hook as R1e words it) |
| RETIRE-ONE-COPY | `t_fallback` retire, through the replica | A-7d | yes |
| ROLLBACK-SKIPS-PREV-CHECKS | `t_fallback` unproven: rollback over a lost record below meta `k−1`'s end | A-7d | yes |
| DEP-CHECK-SKIP | `t_ckpt_dep` main | A-7d | yes |
| DEP-BY-POSITION | `t_ckpt_dep` (c) | A-7d | yes |
| DEP-FIRST-ONLY | `t_ckpt_dep` (b) | A-7d | yes |
| DEP-CHECK-LATE | `t_ckpt_dep` main: byte-identity and the read-only cut | A-7d | yes (the byte form needs a pool at its minimum or a non-zero tail; the read-only form does not) |
| DEP-ID-STALE | `t_ckpt_dep` main | A-7d (A-7c's code) | yes |
| DEP-SALVAGE-PAST-CUT | `t_ckpt_dep` read-only | A-7d | yes |
| START-SEG-AS-END | `t_wal_start` A-6b walk; (a) | A-6b, A-7d | yes |
| SEG0-LAZY | `t_wal_start` A-6b creation; (d) | A-6b, A-7d | yes ((d) as R14 words it) |
| START-SEG-NOT-ENSURED | `t_wal_start` A-7c; (d) | A-7c, A-7d | yes |
| RECYCLE-UNORDERED | `t_wal_start` A-6b under lost renames; (c) | A-6b, A-7d | yes |
| MAPINNER-CREATE-UNCONDITIONAL | MapInner race, S2 | A-4a, A-7d | yes |
| LEVEL-GROW-UNCONDITIONAL | level race, S2 | A-4a, A-7d | yes |
| MAP-ROOT-CURRENT | namespace variant on S1–S3 | A-3c, A-7d | yes |
| MAP-POSITION-FROM-ROOT | namespace variant on S1–S3 | A-3c, A-7d | yes |

Cannot fail as specified at 3ccc8b3d: the 7.10 mutants BOOT-ID-EQ-STOP and BOOT-ID-NO-META-FLOOR (their only fixtures no longer roll a meta back), BOOT-ID-CKPT-ONLY's mechanism unstated, and META-WRONG-SLOT at A-7d (item 5, R5); of the new ones, REPLICA-SKIP at A-7c, COPY-INDEX-OFF and FALLBACK-TWO-GEN until R13 and R17a, and TIE-NO-BOOT-ID at A-7d until R12b. Each is a blocking defect, and each is fixed by the blocks below with no change in mutant counts.

## The owner's three points
- **O-1, explicit rollback.** Within the decision ("else refuse + explicit rollback"); the card's scope is its narrowest reading. In plain words: when the newest checkpoint's data is damaged and the log cannot prove that nothing would be lost, an operator may force the store open read-write from the previous checkpoint by naming the checkpoint to give up, and accepts losing the commits whose log records are gone (reported); it still runs every check on the previous checkpoint, and anywhere else it fails with 55000 and writes nothing. It also gives up any of those commits that only the damaged checkpoint still held, which a future salvage tool (§10 item 15) might recover. Recommendation: accept, and also decide S-ALPHA-F4: a disk read error (58030) on a page of the newest checkpoint is outside the rollback and outside the automatic fall-back, so neither a read-write open, a rollback nor a read-only open reaches the data, though the previous checkpoint and the log may be intact.
- **O-2, two generations everywhere before measuring.** Within the decision (the owner said "two generations" with no mode qualifier, after verify had suggested deferring it). In plain words: every mode, the browser included, keeps one more checkpoint's worth of replaced pages and log on disk; on E4's tiny prototype that was about 14% more space per checkpoint on average, 20% at the peak and 3% more log, not yet measured on the real store, and the file never shrinks. Recommendation: accept, measure at A-7e on a browser-sized configuration, and let §19 T7 decide browser mode; switching it off later is policy only, but browser mode would then lose the fall-back.
- **O-3, a dropped sync now opens instead of refusing.** Within the decision as the eval put it to the owner (§7, "Outcome under F2"), but the owner's words never mention it, so it should be confirmed in plain words: if a disk claims to have saved the newest checkpoint header and has not (both copies), the store now opens from the previous checkpoint and replays its log, and recent commits whose log bytes the disk also failed to save are missing without any error. Before, the same disk usually made the store refuse to open at all, needing a backup; that refusal never detected the missing commits (they were lost either way), it came from the older checkpoint having been overwritten. No cheap complete detection exists: under a device that drops syncs, any marker written in the same window can be lost with the data. The one partial measure is a durable-end witness per WAL batch (§19 T4, deferred), which catches the case where later batches survived past a dropped one. The design did not pass over a cheap fix.

## What S-BETA must change after this card (and its rework)
1. R18 becomes **R23**: re-anchor E42 on this card's lead-in ("…, R17 from card S-1-F7, R18–R22 from card S-ALPHA, and R23 from card S-BETA; …"), renumber E43's row, and change its Acceptance grep `^| R18 | ` to `^| R23 | `.
2. **E25**: its Old ("after a meta-slot fault too, …; ") no longer exists. Insert ", and each of those boots made every byte its own recovery walk accepted durable before its boot meta (I-BOOT-DURABLE)" after "crosses into newer boots' records (§12.5)" in this card's sentence, before ", when that meta's image and WAL span are intact and no replayed commit depends on a newer checkpoint (§12.8)".
3. **E22** lets step 5 remove segment files above the valid end, which contradicts §12.5's I-WAL-START sentence "it leaves `wal/` only by recycling". Amend that sentence: "or by a recovery's step 5, which removes only segments whose first LSN lies above its valid end, so never the start segment of the meta it opened". Such a removal can meet only a meta that a later open selects after that open had missed it, and then refuses (XX001), never silent.
4. **E28**: the splice cut-back list gains step 3b's dependency check ("…, the coverage check (§14 step 3a) and the dependency check (§14 step 3b) are taken over the records below the cut"); this card's "where several cuts apply, the lowest one does" (§14.1) stands.
5. Every "lost after its sync" (E11's *Why*, E31's `t_wal_trunc_durable`, rebase point R-4) becomes "both copies lost after their sync", as R5 makes `t_meta_rollback_crosses_boot`; E11's "(a meta rollback, §2.2)" cites "§2.2, §14 step 4a".
6. Counts: §15.1 starts from 141 rows (148 after S-BETA's 7); its checker's mutant set is 7.8's plus this card's 31 plus S-BETA's own, with the 25 Phase-B-only mutants unchanged.
7. Its I-RO-OPEN and §14.1 edits re-anchor on this card's E41 and E51 text.
8. Motivation: under two generations a dropped meta sync now opens the older meta instead of refusing, so I-BOOT-DURABLE's silent-loss path (a crashed boot's accepted, unsynced bytes lost under that older-meta walk) becomes more reachable. S-BETA should follow this card promptly.

## Rework: exact corrected text
The rework applies these 41 blocks to 3ccc8b3d in the order listed; each Old occurs exactly once at its turn (verified on a scratch copy, which then passes every Acceptance line and the checker unchanged). The rework card's Limits must admit the blocks outside E1–E59: R4a–b (§11), R5a and R5g (§12.5), R5b–d (§14 step 8), R5f (an existing §15.1 row's form cell; its Named-mutants cell is unchanged), R10a (§14.1) and R16b (§12.3). No block adds or removes a mutant, an invariant or a §15.1 row. R1a–R1j: item 1; R2: item 2; R3: items 3 and 6; R4: item 4; R5: item 5; R6: item 6; R7, R7b: items 7 and 3; R8–R10: items 8–10; R11–R17: the gate's own; R18: the Changelog.

**R1a.**
Old:
~~~~text
and a failure in steps 5–8 are handled as those steps and §9.2 say. The open then:
~~~~
New:
~~~~text
and a failure in steps 5–8 are handled as those steps and §9.2 say. Image `k` can fail in pass 2 after pass 2 has evicted: those evictions wrote only slots that image `k` does not reference, and none that image `k−1` references when step 2 read its free set (§4.4, I-TWO-GEN), and an outcome of this step that writes nothing writes nothing more than they did. The open then:
~~~~

**R3.**
Old:
~~~~text
   1. Falls back only to the previous meta (§2.2), meta `k−1`, and only while image `k−1` is as it was: when a read-write open's image `k` failed in pass 2, which may have evicted, only if step 2 protected image `k−1` (its free set read, I-TWO-GEN). Otherwise the open fails with **XX001** (detail: image `k` failed, with no previous image).
~~~~
New:
~~~~text
   1. Falls back only to the previous image (§1): meta `k−1`, and only when step 2 read meta `k−1`'s free set and so protected its slots (I-TWO-GEN). With no previous meta, or with its free set failing a read in step 2, there is nothing to fall back to, wherever image `k` failed, and the open fails with **XX001** (detail: image `k` failed, with no previous image). Image `k−1` is as it was when meta `k` is the newest meta that was synced and every open since meta `k` was synced read meta `k−1`'s free set too (I-TWO-GEN). Otherwise a slot of image `k−1` that a later write reused fails I-INTEGRITY when it is read (§3.3): at open, as item 2's integrity failure, or at a later read (§9.2); never silently (§2.2, "One fall-back, and only one").
~~~~

**R9.**
Old:
~~~~text
and runs steps 1–3b again from meta `k−1`: its range check, the load of image `k−1`,
~~~~
New:
~~~~text
and runs step 1's range check on meta `k−1` and steps 2–3b from meta `k−1`: the load of image `k−1`,
~~~~

**R1b.**
Old:
~~~~text
Pass-2 evictions made from image `k` before it failed wrote only slots that neither image `k` nor image `k−1` references (§4.4), so image `k−1` is as it was.
~~~~
New:
~~~~text
Pass-2 evictions made from image `k` before it failed wrote only slots that neither image `k` nor image `k−1` references (§4.4), so image `k−1` is as item 1 says. An integrity failure in image `k−1` in pass 2, after the retire write (below), leaves meta `k` retired: the next open selects meta `k−1` and refuses as this one did.
~~~~

**R1c.**
Old:
~~~~text
against meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi`), and writes nothing. A read-only open serves meta `k−1`'s recovered state as salvage,
~~~~
New:
~~~~text
against meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi`), and writes nothing more (above): no meta place, no WAL byte and no slot that image `k` or image `k−1` references changes. A read-only open serves meta `k−1`'s recovered state as salvage,
~~~~

**R7.**
Old:
~~~~text
   **Retiring meta `k`.** Before a read-write fall-back writes anything else (a pass-2 eviction, step 5's truncation, the boot checkpoint), it overwrites both copies of meta `k` with zeros and runs `sync_all`. Meta `k` is then invalid in both places, so no later open selects it and opens image `k` over slots this open reuses; the next open selects meta `k−1` as the newest, and the boot checkpoint later writes `ckpt_id` `k` to the same places, as any checkpoint does. A crash before the zeros are durable leaves meta `k` in place and image `k` as it was, and the next open decides again. A read-only open and a refused open retire nothing.
~~~~
New:
~~~~text
   **Retiring meta `k`.** Before a read-write fall-back writes anything from meta `k−1` (a pass-2 eviction, step 5's truncation, the boot checkpoint), it retires meta `k` in two writes: first it overwrites with zeros every place of the other parity that holds a copy of meta `k` (a misplaced copy, §2.2) and runs `sync_all`; then it overwrites both places of meta `k`'s own parity with zeros and runs `sync_all`. Only the pass-2 evictions made from image `k` before it failed can precede the retire write (above). No place then holds a copy of meta `k`, so no later open selects it and opens image `k` over slots this open reuses, and no later open fails on it as a misplaced copy newer than every valid one (I-META-SLOT); the next open selects meta `k−1` as the newest, and the boot checkpoint later writes `ckpt_id` `k` to its parity's places, as any checkpoint does. The misplaced copies go first: zeroing meta `k`'s own places while a misplaced copy survives would leave the next open XX001. A crash before the second `sync_all` completes leaves either a copy of meta `k` in a place of its own parity, with image `k` as it was, so that the next open decides again, or meta `k` retired. If a retire write fails, the open fails with its error and writes nothing more. A read-only open and a refused open retire nothing.
~~~~

**R1d.**
Old:
~~~~text
   - **RETIRE-SKIP**: a read-write fall-back writes before it retires meta `k`, or never retires it.
~~~~
New:
~~~~text
   - **RETIRE-SKIP**: a read-write fall-back writes from meta `k−1` before it retires meta `k`, or never retires it.
~~~~

**R1e.**
Old:
~~~~text
a hook asserts that both copies of meta `k` are zeros, and synced, before the open's first other write.
~~~~
New:
~~~~text
a hook asserts that every place that held a copy of meta `k` is zeros, and synced, before the open's first write from meta `k−1`, and that every write before the retire went to a slot that neither image `k` nor image `k−1` references.
~~~~

**R1f.**
Old:
~~~~text
The read-write open fails with XX001 and leaves every file byte-identical. The read-only open serves the state at its walk's frontier
~~~~
New:
~~~~text
The read-write open fails with XX001, and every meta place, every WAL file and every slot that image `k` or image `k−1` references is byte-identical afterwards; where image `k` fails before pass 2, every file is. The read-only open serves the state at its walk's frontier
~~~~

**R7b.**
Old:
~~~~text
A crash between the two zeroing writes: the next open decides again and recovers the same state.
~~~~
New:
~~~~text
A crash between the two zeroing writes: the next open decides again and recovers the same state. *Misplaced variant:* one copy of meta `k` was misdirected into a place of the other parity before the fault (meta `k−1` keeps its other copy), and a crash follows the retire write: the next open selects meta `k−1` and recovers every acked commit; a retire that zeroes only meta `k`'s own places gives XX001 (I-META-SLOT). *Unread variant:* a hook fails one read of meta `k−1`'s free set in step 2 of the first open only: that open fails with XX001 (no previous image) and writes nothing, and the next open falls back with proof.
~~~~

**R1g.**
Old:
~~~~text
and retires both copies of meta `k` before it writes anything else; unproven, it fails with XX001 and writes nothing,
~~~~
New:
~~~~text
and retires meta `k`, every place holding a copy of it, before it writes anything from meta `k−1`; unproven, it fails with XX001 and writes nothing more than the pass-2 evictions made from image `k` before it failed,
~~~~

**R1h.**
Old:
~~~~text
| The newest image fails at open, with a previous meta, and the proof holds (step 4a) |
~~~~
New:
~~~~text
| The newest image fails at open, with a previous image (its free set read at step 2), and the proof holds (step 4a) |
~~~~

**R1i.**
Old:
~~~~text
| The same, and the proof fails | XX001, nothing written; an explicit rollback opens it |
~~~~
New:
~~~~text
| The same, and the proof fails | XX001, nothing written beyond step 4a's pass-2 evictions; an explicit rollback opens it |
~~~~

**R1j.**
Old:
~~~~text
| The same, with no previous meta, or image `k−1` failing too | XX001 | XX001 |
~~~~
New:
~~~~text
| The same, with no previous image (no previous meta, or its free set unread at step 2), or image `k−1` failing too | XX001 | XX001 |
~~~~

**R2.**
Old:
~~~~text
- **I-FREE-SAFE:** a slot referenced by image `k−1` but not by image `k` becomes reusable only after meta `k+1` is synced (§13 step 6 of checkpoint `k+1`): one checkpoint after the meta that stopped referencing it.
~~~~
New:
~~~~text
- **I-FREE-SAFE:** a slot referenced by image `k−1` but not by image `k` becomes reusable only after meta `k+1` is synced (§13 step 6 of checkpoint `k+1`): one checkpoint after the meta that stopped referencing it. The one exception is an open from meta `k` that has no previous image (I-TWO-GEN): it cannot tell which slots image `k−1` references, so they are reusable once meta `k` is the durable image, as at draft 7.10, and that open has nothing to fall back to (§14 step 4a).
~~~~

**R4a.**
Old:
~~~~text
If the durable image or the image being written still references it, it stays pending (§4.4).
~~~~
New:
~~~~text
If the durable image, the image being written or the previous image still references it, it stays pending (§4.4).
~~~~

**R4b.**
Old:
~~~~text
image `k` no longer references the slot. Once meta `k` is synced, the slot is free in memory (§13 step 6) and in image `k`'s persisted bitmap (I-FREE-EXACT).
~~~~
New:
~~~~text
image `k` no longer references the slot, and image `k`'s persisted bitmap marks it free (I-FREE-EXACT). It is free in memory once meta `k+1` is synced (§13 step 6 of checkpoint `k+1`, I-TWO-GEN), one checkpoint later than at draft 7.10.
~~~~

**R5a.**
Old:
~~~~text
    - a post-sync LOST write on meta `k+1`'s slot reverts it,
~~~~
New:
~~~~text
    - a post-sync LOST write on both copies of meta `k+1` reverts it,
~~~~

**R5b.**
Old:
~~~~text
two successive rollbacks to meta `k`, across boots `b` and `b+1`;
~~~~
New:
~~~~text
two successive rollbacks to meta `k`, across boots `b` and `b+1`, each made by a post-sync LOST on both copies of the boot meta `k+1`;
~~~~

**R5c.**
Old:
~~~~text
boot `b−1` writes meta `k` to one slot and crashes; boot `b` recovers from `k` (id `k+1`), writes meta `k+1` to the other slot, appends and acks, and that meta write is LOST before a crash; boot `b+1` recovers from `k`, its walk crosses `b`'s records (id `k+2`), and its meta `k+1` is LOST again before a crash;
~~~~
New:
~~~~text
boot `b−1` writes meta `k` to its parity's places and crashes; boot `b` recovers from `k` (id `k+1`), writes meta `k+1` to the other parity's places, appends and acks, and both copies' writes are LOST before a crash; boot `b+1` recovers from `k`, its walk crosses `b`'s records (id `k+2`), and both copies of its meta `k+1` are LOST again before a crash;
~~~~

**R5d.**
Old:
~~~~text
a LOST write on meta `k+2` leaves its slot holding meta `k`.
~~~~
New:
~~~~text
a LOST write on both copies of meta `k+2` leaves its places holding meta `k`.
~~~~

**R5e.**
Old:
~~~~text
| A-2 (`t_meta_copies`' table form), A-7d (SimEnv misdirected meta) | META-WRONG-SLOT |
~~~~
New:
~~~~text
| A-2 (`t_meta_copies`' table form), A-7d (SimEnv misdirected meta copy, the meta's other copy lost) | META-WRONG-SLOT |
~~~~

**R5f.**
Old:
~~~~text
| — (row) | I-META-SLOT | A-7d | A-1b | full: SimEnv misdirected meta | META-WRONG-SLOT |
~~~~
New:
~~~~text
| — (row) | I-META-SLOT | A-7d | A-1b | full: SimEnv misdirected meta copy, with the meta's other copy lost | META-WRONG-SLOT |
~~~~

**R5g.**
Old:
~~~~text
The opened meta need not be the newest meta written: a meta-slot fault can leave an older meta as the newest valid one.
~~~~
New:
~~~~text
The opened meta need not be the newest meta written: faults on both copies of a meta can leave an older meta as the newest valid one.
~~~~

**R6a.**
Old:
~~~~text
So if both copies of the two newest metas are lost, the older meta the open selects meets reused slots, which fail I-INTEGRITY (§3.3), or a recycled start segment, which fails I-WAL-START, never silently.
~~~~
New:
~~~~text
So if both copies of the two newest metas are lost, the open selects a meta older than the two generations: a recycled start segment fails I-WAL-START, and a reused slot of its image fails I-INTEGRITY whenever it is read (§3.3), because it holds a later write whose `crc`, `id` or `epoch` differs from the reference. If that failure comes at open, §14 step 4a may fall back once from the selected meta, so three generations from the lost newest one; its proof then holds against the selected meta only, and a reused slot of the image it opens fails I-INTEGRITY when read too, at open (XX001) or at a later read (§9.2). Such an open is never silent, but it can succeed and then fail reads.
~~~~

**R6b.**
Old:
~~~~text
  So a fall-back of one generation finds image `k−1` and its whole WAL span as they were. If meta `k+1` was synced and then lost too, image `k−1`'s reused slots fail I-INTEGRITY (§3.3), and its recycled start segment fails I-WAL-START; neither is silent.
~~~~
New:
~~~~text
  So a fall-back of one generation from the newest synced meta finds image `k−1` and its whole WAL span as they were. If meta `k+1` was synced and then lost too, the open selects meta `k`, and a fall-back from it meets image `k−1` outside the two generations: its recycled start segment fails I-WAL-START, and its reused slots fail I-INTEGRITY when read (§3.3), at open or at a later read (§9.2); neither is silent (§2.2).
~~~~

**R6c.**
Old:
~~~~text
a recycled start segment fails I-WAL-START, and a publication over it fails I-CKPT-DEP, so none is silent.
~~~~
New:
~~~~text
a recycled start segment fails I-WAL-START, and a publication over it fails I-CKPT-DEP. An I-INTEGRITY failure at open there can make step 4a fall back once from the selected meta, to an image also outside the two generations, whose proof holds against the selected meta only: a reused slot of it fails I-INTEGRITY at open, or at a later read (§9.2). None is silent, but such an open can succeed and then fail reads.
~~~~

**R8.**
Old:
~~~~text
Otherwise it is the fields that the copies with valid magic and CRC share; if two of them differ in `format_version`, `page_size`, `checksum_alg`, `wal_seg_size` or `creation_uuid`, the open fails with XX001. With page 0 invalid, the replicas give the page size, and so the offsets of slots 1 and 2; if neither replica is valid, the open fails with XX001.
~~~~
New:
~~~~text
Otherwise the open reads the two replica blocks first: if neither holds a copy with valid magic and CRC, the open fails with XX001; else the replica copies give the page size, and so the offsets of slots 1 and 2. The identity is then the fields that every copy with valid magic and CRC, in any of the four places, shares; if two of them differ in `format_version`, `page_size`, `checksum_alg`, `wal_seg_size` or `creation_uuid`, the open fails with XX001. So with page 0 invalid a copy of another file's identity cannot be told apart and fails the open; the rest of this rule, and the table below, apply once the identity is known, and a copy is foreign only against it.
~~~~

**R10a.**
Old:
~~~~text
or in the meta slot that loses (§2.2).
~~~~
New:
~~~~text
or in a meta place, where a cut-short copy is invalid and the open rule selects the newest meta with a valid copy (§2.2).
~~~~

**R10b.**
Old:
~~~~text
records the map's root and level and sets `e := k+1`, as item 2's last sentence does, and releases the gate (item 3), without the rest of item 2.
~~~~
New:
~~~~text
records the map's root and its level and sets `e := k+1`, as item 2's last sentence does (S1 need not record the boot id, which only step 4 writes), and releases the gate (item 3), without the rest of item 2.
~~~~

**R11.**
Old:
~~~~text
A single fault on one copy (lost, torn, misdirected or unreadable) therefore never changes which meta opens or what it recovers (I-META-COPIES).
~~~~
New:
~~~~text
A single fault on one copy of a meta whose two copies were both written (lost, torn, misdirected or unreadable) therefore never changes which meta opens or what it recovers (I-META-COPIES). A crash between step 4's two writes can leave one copy of the new meta; a fault on that copy makes the open select the meta before it, whose image and WAL recover the same state (I-TWO-GEN).
~~~~

**R12a.**
Old:
~~~~text
on one copy of the newest meta, at every step of a checkpoint and of the next one;
~~~~
New:
~~~~text
on one copy of the newest synced meta, at every step of a checkpoint and of the next one;
~~~~

**R12b.**
Old:
~~~~text
its boot checkpoint rewrites `ckpt_id` `k` with one copy's write lost,
~~~~
New:
~~~~text
its boot checkpoint rewrites `ckpt_id` `k` with the primary copy's write lost (a second run loses the replica's), meta `k` having been written after acked commits of its own boot, so that its `boot_id` is the older,
~~~~

**R13.**
Old:
~~~~text
each holds meta `k` or its parity's previous meta, and the two copies of one meta differ only in `copy` and `crc`.
~~~~
New:
~~~~text
each holds meta `k` or its parity's previous meta; after every checkpoint that completes, both hold meta `k`, copy 0 in slot `1 + (k mod 2)` and copy 1 in the replica block, and the two differ in their `copy` byte and `crc` and in no other byte.
~~~~

**R14.**
Old:
~~~~text
  - (d) faultless: a crash at every step of file creation, of the first appends and of A-7c's flip. Every open succeeds;
~~~~
New:
~~~~text
  - (d) faultless: a crash at every step after file creation returns, of the first appends and of A-7c's flip. Every open succeeds (a crash inside creation, before meta 0 is durable, leaves no valid meta copy: XX001, §2.2);
~~~~

**R15.** (New is empty: the clause is deleted.)
Old:
~~~~text
, and an open from the forced image itself refuses when a later checkpoint was already durable when the id was read
~~~~
New:
~~~~text

~~~~

**R16.**
Old:
~~~~text
RequireCkpt{ckpt_id}                                              # store-level; before every other op of its record (§12.8, I-CKPT-DEP)
~~~~
New:
~~~~text
RequireCkpt{ckpt_id}                                              # store-level; first in its record, before CreateTree, DropTree and Ddl too (§12.8, I-CKPT-DEP)
~~~~

**R16b.**
Old:
~~~~text
CreateTree{tree_id, kind}   DropTree{tree_id}   Ddl{bytes}        # first within a record (opaque Ddl bytes are catalog-owned)
~~~~
New:
~~~~text
CreateTree{tree_id, kind}   DropTree{tree_id}   Ddl{bytes}        # first within a record but for a RequireCkpt (opaque Ddl bytes are catalog-owned)
~~~~

**R17a.**
Old:
~~~~text
FALLBACK-TWO-GEN fails it: it selects meta `k−3`.
~~~~
New:
~~~~text
FALLBACK-TWO-GEN fails it: it selects meta `k−3`, which a hook sees as a read through meta `k−3`'s references, and its XX001 detail is not "with no previous image".
~~~~

**R17b.**
Old:
~~~~text
a valid stale copy, a torn copy, zeros, a misplaced copy, or a foreign copy with a higher `ckpt_id`,
~~~~
New:
~~~~text
a valid stale copy, a torn copy, zeros, an unreadable place, a misplaced copy, or a foreign copy with a higher `ckpt_id`,
~~~~

**R18.**
Old:
~~~~text
and no existing mutant or test is removed or renamed.
- **Draft 7.10**
~~~~
New:
~~~~text
and no existing mutant or test is removed or renamed.
  - **Gate corrections** (`verdicts/S-ALPHA.md`, REWORK): pass-2 evictions made before image `k` fails, and the retire write after them; I-FREE-SAFE's exception for an open with no previous image; no fall-back unless step 2 read the previous free set; §11's reuse one checkpoint later; the fixtures that roll a meta back now lose both copies; the chained fall-back after two lost metas, stated; the retire write zeroes a misplaced copy first; the identity rule's order; and the test texts that a correct build could not pass, or whose mutant could not fail.
- **Draft 7.10**
~~~~

## Follow-ups
- **S-ALPHA-F1 — unreadable meta places (the eval's dropped fence) and the equal-`boot_id` tie.** An open whose read of a meta place fails treats it as invalid and may select an older meta; then it reuses the newer image's slots, and if it crashes before its boot checkpoint's step 5 and the read later succeeds, the next open selects the newer meta over a partly overwritten image. Usually that image fails I-INTEGRITY at open and falls back safely; where only pages the open does not read were reused, it opens and fails those reads later. One transient read error on the one copy written before a crash between step 4's writes is enough, and pre-existing at 7.10 with one copy. The eval's Part 3 fence was for this ("a header that was only briefly unreadable could be opened later over reused space"); verify §2.4 narrowed it on the ground that a missing header is indistinguishable from an unwritten one, which a read error is not. The same mechanism gives the extra scenario's tie. Candidate: before its first write, a read-write open zeroes every meta place whose read failed and syncs, as the retire write does, failing with that write's error if it fails; raising the boot id by one when a place was unreadable would close only the tie. Open-time policy, no format change; before A-7d.
- **S-ALPHA-F2 — protect the previous image when its free set cannot be read.** After R3, an open whose step 2 cannot read meta `k−1`'s free set does not fall back, but it still protects nothing. If it crashes before its boot checkpoint's step 5, a later open that reads the free set can fall back to an image `k−1` whose superseded slots the first open reused; a replayed page fails I-INTEGRITY (XX001), but a page changed only by unlogged work is not read at open, so the fall-back succeeds, retires meta `k`, and that page fails a later read. Candidate: when meta `k−1` is valid and its free set fails a read, protect every slot below its `slot_count` that image `k` does not reference until the first step 6 (allocation grows the file meanwhile); I-FREE-SAFE's exception then shrinks to "no previous meta". Before A-7d.
- **S-ALPHA-F3 — fall-back hardening.** (a) After both copies of the two newest metas are lost, step 4a can fall back from the selected meta into an image outside the two generations (item 6). A narrowing: an integrity failure on a page whose header has a valid CRC and a content epoch at least `k+3` in serial order, which only a write after meta `k+1` was synced can carry, is not a fall-back trigger (it does not cover a torn reuse write); or verify image `k−1`'s map, free set and directory before the retire write. (b) The retire write precedes pass 2 from meta `k−1`; if image `k−1` fails there, meta `k` is already zeros and image `k`, damaged in one page only, can no longer be found by a salvage tool. Protecting image `k`'s slots until pass 2 ends, and retiring then, would keep it. Bears on §10 item 15; before A-7d.
- **S-ALPHA-F4 — a persistent read error on the newest image (owner, with O-1).** D35 keeps 58030 out of the automatic fall-back, and the explicit rollback's precondition ("its image fails at open") excludes it too, so one unreadable sector under the newest image's map root, free set or directory leaves no path to the data: a read-write open fails with 58030, an explicit rollback is refused (its precondition does not cover a read error), and a read-only open fails with 58030, although image `k−1` and the WAL may be intact. That is the "needs a backup" outage (eval §4, F6) the two generations were meant to shrink. Recommendation: let an explicit rollback, and a read-only open's salvage from meta `k−1`, accept a 58030 on a page read through image `k`'s references; keep the automatic fall-back to integrity failures.
- **S-ALPHA-F5 — I-META-SLOT under two generations.** A misplaced copy whose `ckpt_id` exceeds the newest meta `k` still fails the open. At 7.10 that guarded a fall-back past a synced meta; with two copies and two generations a misplaced copy of meta `k+1` shows only that meta `k` was synced, and image `k` and its WAL stay intact until meta `k+2` is durable, which would have overwritten meta `k`'s places. So one misdirect of an in-flight copy of meta `k+1`, and an ordinary crash before its other copy is written, refuses a fully recoverable store (pre-existing at 7.10). Candidate: refuse only when the misplaced `ckpt_id` exceeds `k+1`, with that argument and a `t_meta_copies` case. It restates an invariant, so it is a P card; low priority.
