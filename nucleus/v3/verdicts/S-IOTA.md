Card: S-IOTA
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 30e37b44-0ab7-4a12-8953-f05a55a9a466, dispatched from T3 thread 45584af2-e651-490e-862c-3402320cffa3 (an independent P gate: this agent wrote neither the card S-IOTA nor its implementation 934e5dd1, and is not the orchestrator)
Date: 2026-11-21
Gate-exit: GATE EXIT=0
Commit: 934e5dd1137451a2add73a52e1e9bbda3ea2ccea
Mutants: spec text, none in code; every "caught" is walked on the 7.20 text. The card's eleven: INVARIANT-AS-CORRUPT -> t_poison_before_unlatch invariant, panic and read variants (A-3c); WRITE-CHECK-AS-CORRUPT -> t_meta_copies write form (A-7c); REFUSAL-AS-INVARIANT -> t_meta_copies table form (A-2); BOOT-GCW-AS-INVARIANT -> I-GCW's new A-7d row; LATER-FORMAT-AS-CORRUPT -> t_meta_copies table form (A-2) and the fault form's later-format variant (A-7d); REFUSAL-AS-LATER -> table form (A-2); LATER-BY-INEQUALITY -> table form, page 0 with format_version 0 (A-2); LATER-FORMAT-SALVAGED -> the later-format variant's runs 1 and 2, read-only (A-7d); WAL-READ-ERROR-RO-REFUSES -> t_wal_read_error (b) and (c) (A-7d); WAL-READ-ERROR-RO-AS-END -> t_wal_read_error (b), and (c) as well (A-7d); PASS2-READ-ERROR-AS-END -> t_wal_read_error (d) and its fall-back form (A-7d): 11 of 11 caught, deterministically, through named hooks. The gate's own six: G-A RO-SALVAGE-ON-PASS2 -> (d) read-only, caught; G-B READ-SALVAGE-ABOVE-FRONTIER (the read-error salvage path alone admits snapshots above its frontier) -> UNCAUGHT by t_wal_read_error, whose record lies past wal_ckpt_end where the two frontiers coincide; caught only through t_wal_loss_salvage's SALVAGE-ABOVE-FRONTIER if the build shares that path (S-IOTA-F3); G-C SHORTCUT-AFTER-PAGESIZE -> table form, caught; G-D RO-K1-READ-ERROR-AS-PROVEN -> (c) read-only, caught; G-E GCW-XX001-EVERYWHERE -> I-GCW's A-7c row, caught; G-F LATER-FROM-FOREIGN -> table form, caught. Text mutants on 934e5dd1's files, judged by the card's 45 non-gate Acceptance lines in both locales: omit-one on all 58 blocks, 58 of 58 caught, each failing its own content line; 12 planted errors, 12 of 12 caught; the two locales agree in every run
Followups: S-IOTA-F1, S-IOTA-F2, S-IOTA-F3, S-IOTA-F4

## Verdict in brief

**MERGE-WITH-FOLLOWUPS** on `934e5dd1137451a2add73a52e1e9bbda3ea2ccea` (S-STORE draft 7.19 → 7.20, C-T0 draft 8.8 → 8.9).
- **The bytes.** My own fence-aware parser read the card's 58 blocks (E1, I1 to I53, E2; T1 to T3) and reproduced both files byte for byte: S-STORE sha256 `20bfce7718555059f59926ad7c4c6302759d44d01ef1e131ef20248c3ff1269d`, C-T0 `e6c82f6b9d428cb11b0b42e7952e002e4cd9e9d34119de23769e15417be33722`. Nothing else changed. Each reverse gives the base file.
- **The checks.** The fresh gate gives `GATE EXIT=0` on its first run. The 45 non-gate Acceptance lines pass in both locales and fail on the base. The checker prints `OK`, and my own parse of §15 and §15.1 agrees with it.
- **Fidelity.** Each rule carries what the owner said and no more (Scope 2). XX000 covers checks that only an engine bug can fail; every check of device-returned data stays XX001. 55000 covers exactly records whose `format_version` is above 1, in every open, writing nothing. Salvage covers only a read-only open whose pass 1 meets the read error, and it is never reported as complete. Faulty-build and deliberate records stay XX001.
- **Recovery design.** I found no new silent-loss path, no legitimate state newly refused, and no contradiction with S-THETA's, S-ETA's, S-ZETA's or R-1-SPEC's text (Scope 3).
- **Tests and mutants.** Every named mutant is judged on a Phase A row and is killable, and every test is passable on a correct build (Scope 4). One gate mutant of my own is uncaught: it is a test gap on the new salvage path, not a fault in the text.
- **Four non-blocking follow-ups.**
  - S-IOTA-F1: G-1, plus a gap that predates this card: a read-only open can serve reads below the `gc_w` floor.
  - S-IOTA-F2: G-2, creation's `page_size` parameter.
  - S-IOTA-F3: two test gaps on the read-error salvage.
  - S-IOTA-F4: two summary wordings.
- **Merge timing.** S-IOTA may be built and gated on its branch. Merging it before P0 PASS needs the owner's own words.

## Gate run

- **Load.** At 05:59:45 the 1-minute load average was 20.28 on this 16-CPU host. At 06:01:57 it was 17.45, with another project's process holding about 14 CPUs. A background runner polled every 30 s and started the gate at the first reading below 16: 12.77 at 06:02:22. The load, sampled each minute (`~/.cache/nv3-review/gate-S-IOTA/load1.log`), was 12.77 at the start, peaked at 28.33 at 06:08 when other jobs ran, and was 8.79 at the end, 06:18:27.
- **Scaffold extract** (GATE-PROMPT step 1, verbatim): the shallow `--no-local` clone of `nucleus/v3-scaffold` printed **13195d529aa13a30b2152334a8057ed4446ca9f8**, the card's base. There was no `EXTRACT FAILED`. sha256: `fresh-gate.sh` 40a5b232…4899, `git-env.sh` 8a784782…b977.
- **The run:** `bash "$fg/fresh-gate.sh" 934e5dd1137451a2add73a52e1e9bbda3ea2ccea` from `~/Workspace/neutron-v3`, with `PATH=$HOME/.cargo/bin:$PATH` and `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-IOTA`. Stdout and stderr went to separate files (`gate1.out`, `gate1.err` in the same directory). Its lines:
  - `fresh-gate: commit 934e5dd1137451a2add73a52e1e9bbda3ea2ccea`
  - `fresh-gate: base 13195d529aa13a30b2152334a8057ed4446ca9f8`
  - `fresh-gate: checks passed (4895 files hashed in 2.23 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`
  - The literal last line was **`GATE EXIT=0`**. The exit status was 0, and stderr was empty (0 bytes).
- **Flakes.** None of the known flakes occurred: the test step exited 0, and both ENF-TIMING self-tests printed `ok` (`premise: GUC: a -c core.untrackedCache=true beats the environment` and `runner refs: RV cost ten honest moved refs verify in under 2 s`). No re-run was needed. `$fg` was removed afterwards.

## Scope 1: mechanics

- **Scope.** `git diff nucleus/v3-scaffold...934e5dd1 --stat` lists only `nucleus/v3/docs/S-STORE.md` (+60/−41) and `nucleus/v3/docs/C-T0-txn-protocol.md` (+3/−2), the card's Touch only list. The base files match the card's sha256 (`a9978ac5…4b8`, 7.19; `58924b86…efb7`, 8.8). They are also a01d4922's files, the base the checker names.
- **The parser.** Mine stops at the next `## ` heading outside a fence, so the `## Changelog` lines inside E2's and T3's blocks are not taken for sections. It found 55 blocks for S-STORE and 3 for C-T0. Every Old occurs exactly once in the untouched base and once at its turn, and the blocks are in file order with no overlaps. With `<date>` = 2026-11-21, the commit's date, both results equal the commit's files byte for byte. Each reverse (E2 down to E1; T3 down to T1) gives the base file.
- **Acceptance.** I parsed the card with the branch's `tools/run_card.py` `parse_card` and `validate_card`, extracted with `ast`. They read `S-IOTA`, the two Touch only paths, 46 commands, the last `cd nucleus/v3 && bash scripts/gate.sh`, and no container, and `validate_card` returns `None`.
  - I ran the 45 commands before `gate.sh` with `/bin/sh -c` from the root of a scratch `--shared` sparse clone at 934e5dd1, whose local `nucleus/v3-scaffold` is 13195d52, with the tree clean.
  - They pass **45 of 45 under `LANG=en_CA.UTF-8` and 45 of 45 under `LC_ALL=C`**.
  - At 13195d52 (nothing applied) all 45 fail, in both locales.
- **The checker** (line 39) prints `174 rows; 274 mutants; 24 judged only on Phase B cards; 48 invariants; 55 tests` and `OK` in both locales.
- **My own §15 parse**, independent of the checker, agrees on every count:
  - 48 invariant rows; §15.1 grows from 172 to 174 rows; the mutants grow from 263 to 274, with eleven added and none removed;
  - each new mutant is listed under exactly one invariant and judged only on rows of that invariant, on the cards the card names: INVARIANT-AS-CORRUPT on A-3c; WRITE-CHECK-AS-CORRUPT on A-7c; REFUSAL-AS-INVARIANT, REFUSAL-AS-LATER and LATER-BY-INEQUALITY on A-2; LATER-FORMAT-AS-CORRUPT on A-2 and A-7d; LATER-FORMAT-SALVAGED, BOOT-GCW-AS-INVARIANT, WAL-READ-ERROR-RO-REFUSES, WAL-READ-ERROR-RO-AS-END and PASS2-READ-ERROR-AS-END on A-7d;
  - no mutant of 7.19 changes the cards that judge it;
  - the new rows are exactly I46's (`— (row)`, I-GCW, A-7d) and I47's (`t_wal_read_error`, I-RO-OPEN, A-7d), and their Needs columns match their sibling rows;
  - six rows change, as the card lists.
- **Omit-one, all 58 blocks.** I built each variant with my parser with one block left out, committed it on 13195d52 in a scratch clone (`~/.cache/nv3-review/gate-S-IOTA/omit`), and ran the 45 lines in both locales. All 58 are caught, each by its own content line, and the locales agree. The failing lines: E1 → 1,2,40,41; I1 → 7,36,37,38,40,41; I2 → 7,37,38,40,41; I3 → 8,37,38,40,41; I4 → 8,38,40,41; I5 → 9,37,38,40,41; I6 → 10,37,38,40,41; I7 → 11,37,38,40,41; I8 → 12,37,38,40,41; I9 → 12,37,40,41; I10–I12 → 13,37,40,41; I13 → 14,36,38,40,41; I14 → 15,37,40,41; I15 → 15,37,38,41; I16, I17 → 16,37,38,41; I18 → 17,37,38,40,41; I19 → 17,36,37,38,40,41; I20 → 18,37,38,40,41; I21 → 19,37,38,40,41; I22, I23 → 20,37,38,40,41; I24 → 21,37,38,40,41; I25 → 21,40,41; I26 → 22,37,38,40,41; I27 → 23,37,40,41; I28 → 23,38,40,41; I29 → 23,37,38,40,41; I30, I31 → 24,36,37,38,40,41; I32, I33 → 25,37,38,41; I34 → 25,37,38,39,41; I35, I36 → 26,37,38,41; I37 → 26,37,38,39,41; I38 → 27,37,41; I39 → 27,37,38,41; I40 → 27,37,38,39,41; I41 → 28,37,41; I42 → 28,37,38,39,41; I43 → 29,37,38,41; I44 → 29,37,38,39,41; I45 → 30,37,38,39,40,41; I46 → 31,36,37,38,39,40,41; I47 → 32,36,38,39,40,41; I48–I51 → 33 or 34,37,38,39,40,41; I52, I53 → 35,37,38,40,41; E2 → 3,4,5,6,40,41; T1 → 42,44; T2 → 44; T3 → 43,44.
- **Planted errors (12), each committed and run in both locales; all caught:**

  | Planted error | Lines that fail |
  |---|---|
  | §12.5: salvage on a read error in "the walk" (either pass), not pass 1's | 18, 41 |
  | §2.2: "other than 1" for "above 1" | 8, 41 |
  | §9.2: the boot checkpoint's `gc_w` exception dropped | 15, 41 |
  | §14.2: the later-format row's read-only cell becomes salvage | 24, 36, 41 |
  | C-T0: XX000 listed before XX001 | 44 |
  | a stray change outside every block (§12.5) | 40, 41 |
  | I47's row put on A-7c | 32, 36, 39, 41 |
  | LATER-FORMAT-SALVAGED dropped from I-META-COPIES | 29, 38, 39, 41 |
  | §14.2: the pass-1 read-error row's read-only cell back to 58030 | 24, 36, 38, 41 |
  | §14.1: "reported as a complete open when the coverage check passes" | 23, 41 |
  | §2.2: the open's refusal made XX000 | 8, 38, 41 |
  | §14 step 4: a pass-2 read error serves salvage read-only | 21, 41 |

- **The riders, re-searched.** All twelve of S-ETA's O1 to O12 Olds, parsed from `cards/S-ETA.local.md`, occur exactly once in 7.19. After this card only O2's, O6's and O8's survive, inside I20, I28 and I31, as the card says, so none of the riders may be applied again.
  - S-ETA-F6: (a)'s Old and (d)'s target are gone (folded). (c)'s Old survives only as I25's anchor. (b)'s Old (S-ETA's status-chain clause) still occurs once and is left as history.
  - S-ZETA-F3's two Olds and S-ZETA-F1's O-1 Old each still occur exactly once.

## Scope 2: fidelity to the owner's words

The authority is QUEUE's head line `# OWNER DECISION 2026-11-20 17:00 MST`, the owner's own words: "Yes to: XX000 for engine bugs; 55000 and full refusal for newer-version files; read-only salvage when part of the log is unreadable; …".

**"XX000 for engine bugs" (rule X): carried exactly.**
- **What is now XX000.** Every XX000 in 7.20 falls in one of three groups:
  - the engine's own bookkeeping (frames, pins, the page table, latches, locks, and C-T0's owner lookup and settle table);
  - bytes the engine made and is about to write: a sealed dirty page, the records that creation and step 4 check, step 4's `wal_ckpt_end ≤ durable_end`, and `gc_w ≤ T` at any checkpoint but a boot checkpoint;
  - a panic in store code.
  None of these can be failed by what the device returned. That is S-THETA's OP-1 as put to the owner ("its own internal state broken (a bug, or memory gone bad)").
- **What stays XX001.** I read every XX001 outside the status chain and the Changelog. Each one checks what the device returned, or is a check that device-returned data can make a correct engine fail:
  - page and decoder checks;
  - an unmapped referenced LPID, and a live frame of an allocated LPID (S-THETA's D-4);
  - §9.1's re-insert with a different value;
  - the header refusals;
  - WAL damage and the coverage check, whose second condition reads a meta and records that an earlier session left;
  - the tie rule;
  - the root-name path's unmapped root under an unchanged entry.
- **S-ZETA gate 3's note is honoured.** Creation's and step 4's well-formedness checks are XX000 (I3, I15, I22). The open's refusal of the same domains stays XX001, or 55000 for a later format version (I3, I6, I15).
- **The boot checkpoint's `gc_w` finding is right.**
  - §10.5's Bound gives `gc_w_used ≤ T` only for records that correct writers wrote. §2.2 has no `gc_w` domain, and §12.5 does not check a DropTree's `w` against its commit ts, so a CRC-valid meta from a faulty build reaches the check at every read-write open. XX001 is the right code there.
  - I traced step 4's other checks at a boot checkpoint. Each is engine-made:
    - the static fields come from the identity the open checked;
    - `T` and `ts_ckpt_hi` are both the frontier the boot started from, since nothing is appended before its flip (§13 step 0);
    - `wal_start_lsn` is the append position, since no record lies above `T` after replay (§12.6);
    - `wal_ckpt_end ≤ durable_end` holds by the boot checkpoint's own step 1;
    - `map_level` comes from a checked meta and only grows.
  - So `gc_w` is the only device-reachable check at step 4, as the card found.
- **Both codes still poison as S-THETA specified.**
  - XX000 poisons wherever it is raised, a client read included (§9.2's engine-invariant item, I-FAIL-STOP, R15).
  - XX001 poisons inside §9.2's contexts and never on a client read. The list of physical failures is unchanged.
  - The boot checkpoint's XX001 is raised inside a checkpoint step, so it poisons as before.
  - C-T0 8.9 adds only XX000 to its storage error list.
- Nothing in rule X goes beyond the owner's words. The one inexact edge, a caller's bad `page_size` at creation (G-2), predates the card. In 7.19 that case gave XX001, which was equally wrong.

**"55000 and full refusal for newer-version files" (rule N): carried exactly.**
- **Which records.** A record is of a later format version when it is intact and its `format_version` is above 1 ("Only a `format_version` above 1 is a later version's"). No text widens 55000 beyond that:
  - REFUSAL-AS-LATER and LATER-BY-INEQUALITY pin the line;
  - `format_version` 0, and format 1 otherwise malformed, stay XX001 in both modes, as S-ZETA wrote;
  - the owner's open question about faulty-build and deliberate records is left open.
- **Full refusal.** Every open refuses: read-write, read-only, and an explicit rollback, which meets step 1 before its own precondition. No open falls back past such a record, serves salvage from the file, or overrides the refusal. The refusal happens at step 1, before the fence, so it writes nothing (FENCE-BEFORE-REFUSAL still pins this).
- **The order** follows S-ZETA's (I4, I5, I6):
  - page 0 is judged first, so case 7 stays XX001;
  - the replica short-cut gives 55000 before slots 1 and 2 are located, but only when the intact replica copies carry one `creation_uuid`;
  - two `creation_uuid`s among the copies give XX001, case 11;
  - after page 0, a later-version copy of the file gives 55000 ahead of any XX001 another copy would give;
  - a foreign copy is ignored, whatever its `format_version`.
- **No change for a file 7.19 opened.** Every file this rule refuses was already refused by 7.19 with XX001, because `format_version` 2 is not well-formed. Only the code moves.
- **One inherent edge.** A faulty build's record that carries a `format_version` above 1 cannot be told apart from a later version's, so it gets 55000. That is what "newer-version files" has to mean to a build that cannot judge later versions, and the detail carries the `format_version`.

**"read-only salvage when part of the log is unreadable" (rule S): carried exactly.**
- **When.** Only a read-only open, and only when pass 1 meets the read error, from meta `k` or from meta `k−1` in a fall-back.
- **What it serves.** The ts-prefix at the frontier of the records accepted below the read cut. The read cut is a position the walk reached: a record's start, a hopped segment's first LSN, or `wal_start_lsn`, never an end-of-file. Pass 1's marker set, its largest `commit_ts`, the coverage check and the dependency check are all taken over the records below the cut.
- **How it is reported.** With the read error and the cut, and never as a complete open, even when the coverage check passes.
- **What still refuses.** A read-write open and an explicit rollback still fail with 58030, within S-ETA's write bound, which F6 (a) and (d) now carry. A read error that only pass 2 meets fails both opens with 58030.
- That matches S-ETA's OP-1 as put to the owner ("read and export the data that comes before it, clearly reported as incomplete … A read-write open would stay refused").
- **The pass-2 restriction is within the owner's words.** If part of the log stays unreadable, the next open's pass 1 meets it, and a read-only open then serves the salvage. Only a stretch that read a moment ago costs one retry.

## Scope 3: recovery design across the three rules

I walked each open under the faults the spec covers:
- **Read-write open.**
  - A later-version record: 55000 at step 1, nothing written.
  - A pass-1 read error: 58030. Nothing is written from meta `k`; in a fall-back from an image `k` that failed in pass 2, or in a rollback that a pass-2 read error let proceed, at most the fence and those pass-2 evictions. Step 3's and §14.2's write bounds allow no retire write before pass 1 from meta `k−1` has finished, so a pass-1 read error never follows a retire.
  - A pass-2 read error: 58030 after at most the fence, pass-2 evictions and the retire write. The next open selects meta `k−1`, exactly as for S-ETA's existing integrity failure after a retire.
  - A boot checkpoint whose `gc_w` exceeds `T`: XX001 at step 4. No meta is written, and every slot written lies outside images `k` and `k−1`.
- **Read-only open.**
  - A later-version record: 55000.
  - A pass-1 read error: salvage at the read cut.
  - A pass-2-only read error: 58030.
  - If image `k` fails while pass 2 replays below the cut, the open falls back to meta `k−1` and gets that meta's own cut. If the read error recurs there, it serves salvage from meta `k−1`. If it does not, the proof decides, and a later pass-2 failure gives 58030.
- **Explicit rollback.**
  - A later-version record: 55000 at step 1.
  - A pass-1 read error from meta `k`: 58030, before the rollback's precondition is judged.
  - After it proceeds, a read error from meta `k−1`: 58030 in either pass.
- **The other salvages** (WAL loss, a splice, a malformed record, a short segment, a missing start segment, a dependency, an unproven fall-back, an image read error) are unchanged. Where several cuts apply, the lowest one does.

**Safety of the new salvage.**
- Every commit at or below the frontier has its record below the cut (I-WAL-TS-ORDER). A chunked transaction whose marker lies at or past the cut is not applied.
- Image entries above the frontier belong to commits at or past the cut (I-CKPT-AT-T (b)), and no admitted snapshot sees them.
- So no path serves data as complete when an acknowledged commit may be missing.
- **New silent-loss path:** none. **Legitimate state newly refused:** none. 7.19 refused every case N refuses, rule X changes codes only, and rule S only adds service.

**Restatements.** I grepped 7.20 (outside the Changelog), C-T0 8.9, R-1-memory and C-T6-rehost for XX001, XX000, 55000, 58030, "salvage", "read error", "format version", "engine invariant" and "poison", and read each hit in context.
- **No contradiction with merged text.**
  - S-THETA's poison rules are as stated above.
  - S-ETA's 58030 sentences agree. §12.5's opening "a read error ends it with 58030" is the walk's own result.
  - S-ZETA's identity and refusal rules agree, and S-ZETA-F3 still applies.
  - R-1-memory has no XX001; its two 58030s are a temp-file write's. Its fall-back discard covers "a read-only open's salvage".
  - C-T6's codes are row-format decoding and an index build's 55000, none of the three rules.
- **The summary rows are imprecise in three places** (Findings N5 and S-IOTA-F4). §14.2's own preamble says the text rules wherever a row differs.

## Scope 4: tests, mutants, and the open points

**Each test passes on a correct build.** I walked each test on the text:
- `t_poison_before_unlatch`'s variants: A-3c's code makes the mark with XX000.
- The write form: step 4 and creation refuse with XX000 before any write.
- The table form: a pure function of the five blocks, covering 16^4 × 5 cases. Its run time is unmeasured.
- The later-format variant: three runs, each 55000 in every open, writing nothing.
- I-GCW's A-7d row: the forged meta is well-formed, so the open selects it. Per C-T0 §3.7, `W` is set only after the boot checkpoint, so the first check to meet the value is step 4, which gives XX001.
- `t_wal_read_error`:
  - (b) and (c) serve salvage read-only;
  - (d) re-reads in pass 2 and gets 58030 in both modes;
  - (d)'s fall-back form inherits (d)'s small pool, so a pass-2 eviction forces the retire write before the read fails.
  - One fixture caveat: (d)'s read-only open needs the pool that §14.1 requires for the replay, or it fails with 53200 first. The text does not force (d)'s small pool on it, so the test is passable; S-IOTA-F3 (b) makes the point explicit.

**Each named mutant fails its test deterministically.** All are listed in the header. The hooks are named (A-3c's latch-wait, eviction and pin hooks; A-7c's step-4 and creation hooks; A-7d's `Env` forgeries, its read-fault hook, its new end-of-pass-1 hook, and `t_fallback`'s corrupting hook). No test waits on time.

**My own six**, aimed at the riskiest logic, are in the header. G-B is the uncaught one. The spec text forbids that build, but the read-error tests do not catch it: their unreadable record lies past `wal_ckpt_end`, and rule S's case 7 (a read error below `wal_ckpt_end`) has no test. The malformed and short-segment salvages rely on the same shared frontier mutant, so this is the established pattern, not a new class of gap. Still, it is a new path that serves data. S-IOTA-F3 (a).

**Rulings on the author's open points:**
- **G-1 (a boot checkpoint's `gc_w ≤ T`): as written.** XX001 at a boot checkpoint and XX000 elsewhere is the minimal faithful reading of the owner's words.
  - The alternative (a `gc_w ≤ T` domain in §2.2, plus a check of a DropTree's `w` in §12.5) would add refusals at open. It deserves its own card, and it has a reason the card did not record.
  - The gap: a read-only open runs no checkpoint, so for a meta whose `gc_w` exceeds `T` it starts C-T0's `W` above `M`, against C-T0's I-M. C-T0 §3.6 registers `S = visible_ts` with no `W` check, so such an open serves reads below the floor that the `gc_w` value claims.
  - That gap predates this card (7.19 behaves the same), and only a faulty or deliberate meta reaches it. S-IOTA-F1.
- **G-2 (creation's `page_size`): agree, as a follow-up.** Until §2.3 refuses a `page_size` outside §2.1's list with 22023, a caller's bad value reaches creation's writer check and is reported as XX000, though no engine bug exists. Nothing is written. A-7c's write-form hook hands creation the identity past parameter handling, so the test is unaffected. S-IOTA-F2.
- **G-3 (page 0 judged first, case 7): as written.** It needs a faulty page 0 and a later-version copy in one file, and it is refused either way.
- **G-4 (pass 2's read error: 58030 in both modes): as written.** A transient case costs one retry, a persistent one reaches pass 1 at the next open, and it avoids a second cut-back protocol in the middle of replay.
- **G-5 (the replica short-cut): as written.** It differs only in case 12, which is refused either way, and it is the only order that works for a later version's page size that §2.1 does not list. The exhaustive table form pins the order (G-C).
- **G-6 (55000's two meanings): keep.** The detail tells them apart (`format_version` and a place, or the rollback's `ckpt_id`), and the later-format variant asserts that detail.
- **G-7 (S-ZETA-F3 not carried): agree.** Its two Olds still apply once. Its §14.2 New ("Every meta place unreadable with page 0 intact and well-formed, …") also removes the overlap between that 58030 row and this card's new 55000 row, for page 0 of a later version with every meta place unreadable. S-ZETA-F3's card should say so.

## Findings (none blocks)

- **N1** (S-IOTA-F1): the read-only `gc_w` floor gap behind G-1, above. It predates this card and needs a faulty or deliberate meta.
- **N2** (S-IOTA-F2): creation's `page_size` parameter (G-2). It predates this card.
- **N3** (S-IOTA-F3): the two test gaps, (a) G-B and (b) (d)'s read-only pool.
- **N4** (S-IOTA-F4): two summary wordings, each tried in scratch on 934e5dd1's file. Each Old occurs once, and only that line changes.
  - §14.2's pass-1 read-error row bounds an explicit rollback's writes only through "a fall-back from an image `k` that failed in pass 2". It omits the case of a rollback that a pass-2 read error let proceed, which step 3 (I24) covers.
  - The card's rule S says the read-error salvage reports "the coverage result against it", as the splice's salvage does, but §14.1 (I28) does not list it.
- **N5** (no follow-up; the text rules each time):
  - E1's status clause says a faulty writer's or deliberate record "still fails with XX001". §2.2 limits that to `format_version` 0 or format 1 otherwise malformed. The clause is status-chain history, which later cards do not rewrite (as for S-ETA-F6 (b)), and §2.2 governs.
  - §14.2's first row ("page 0 invalid and copies of two identities") does not mention the replica short-cut's 55000 in case 12.
  - §14.2's 58030 row overlaps the new 55000 row. S-ZETA-F3 removes that (G-7).

## Merge timing

S-IOTA is not among the four spec cards the owner allowed to merge before P0 ("The spec cards S-ETA, S-THETA, S-ZETA and R-1-SPEC may merge before P0 passes"). It may be built and gated on its branch, as it has been. Merging it before P0 PASS needs the owner's own words. The owner was asked on 2026-11-20 and had not answered at dispatch.

## Owner points (plain words)

- **Merge before P0.** May this card, which writes your three answers into the storage spec, merge before the P0 review passes, so that P0 is judged on a spec that already says them? It was asked on 2026-11-20 and is still open.
- **Still yours, untouched here:** what to do with a header record that a faulty build wrote, or that someone wrote on purpose (S-ZETA's OP-2). The text keeps refusing it with "data corrupted" (XX001), as before.
- No new owner point. S-IOTA-F1 to F4 are technical.

## Not verified

- No code exists for any of the three rules. Every "caught" above is walked on the text, and no test named here has run.
- The table form's run time at 327,680 cases.
- PostgreSQL's meanings of XX000 and 55000 (the card's claims; not re-read here).
- That card A-6b's walk delivers every record before a failed read (its card's text, not built code).
- I did not re-run the author's 74 planted variants. I ran my own: 58 omit-one variants and 12 plants. I did not run anything in the worker's worktree beyond `git` reads.
- The implementer's own report (46/46, `GATE EXIT=0` in both locales) was not re-checked as such. The fresh gate and my Acceptance runs replace it.

## Follow-ups

Each needs a QUEUE row (`- S-IOTA-Fn | …`) before the merge, since `merge-card.sh` checks every named follow-up against QUEUE.

- **S-IOTA-F1 (technical spec card, before A-7d; P): a meta whose `gc_w` exceeds its `T`.**
  - The record: a meta with `gc_w > T`, or a DropTree whose `w` is not below its commit ts, is one that no correct writer writes. §2.2's domains and §12.5's decode checks still admit it.
  - Today:
    - a read-write open fails at its boot checkpoint with XX001, every time, as this card writes it;
    - a read-only open runs no checkpoint, so C-T0 §3.7 step 3 starts `W` at `gc_w_used`, above `M` and `visible_ts`, against C-T0's I-M (`W ≤ M`);
    - C-T0 §3.6 registers `S = visible_ts` with no `W` check, so that open serves snapshots below the floor, a state the floor exists to refuse if the faulty build did prune up to `gc_w`.
  - This predates S-IOTA (7.19 behaves the same).
  - What the card should decide:
    - either G-1's alternative: `gc_w ≤ T` becomes a domain of a meta copy (§2.2), and a DropTree whose `w` is not below its commit ts becomes a malformed record (§12.5). Each is refused at open with XX001 under S-ZETA's existing default. Step 4's `gc_w ≤ T` then becomes XX000 everywhere, and BOOT-GCW-AS-INVARIANT's row moves to the open's refusal;
    - or, keeping 7.20's handling, state what a read-only open does when `gc_w_used` lies above its frontier (refuse, or refuse snapshots below `W`), and the code of any `W ≤ M` check there (XX001 by §9.2's criterion).
  - Neither needs an owner decision: the record is not well-formed under the existing rule, and refusing it refuses no legitimate state.
- **S-IOTA-F2 (next S-STORE card, before A-7c; P): creation's `page_size` parameter (G-2).**
  - §2.3 should refuse a `page_size` that is not one of §2.1's with 22023 before anything is written, as it already does for `wal_seg_size`, and the error table's 22023 row should name it. Without that, a caller's bad value reaches creation's writer check and is reported as XX000, though no engine bug exists.
  - A-7c's write-form hook already hands creation its identity past parameter handling, so `t_meta_copies` is unchanged. Add a mutant if wanted (for example, PAGE-SIZE-UNCHECKED: creation writes page 0 with a `page_size` outside §2.1's list).
- **S-IOTA-F3 (next S-STORE card, or A-7d's card; P): two gaps in `t_wal_read_error`.**
  - (a) Add a run with the unreadable record below meta `k`'s `wal_ckpt_end`, as `t_wal_malformed`'s second run has.
    - The fixture: a commit at or past the record, applied before the flip, whose entries image `k` holds.
    - Expected: the read-write open fails with 58030, and the read-only open serves salvage at the read cut. At every admitted snapshot it reads the model's state at or below the frontier, never an entry of that commit.
    - Who kills what: SALVAGE-ABOVE-FRONTIER, or a named read-error form of it (the gate's G-B), fails it. It is judged on I-RO-OPEN's new A-7d row.
  - (b) Make (d)'s read-only pool explicit. Old (in §12.5, `t_wal_read_error` (d), once in 934e5dd1's file): `the read-only open fails with 58030 and leaves every file byte-identical; and the next open, without the hook,` New: `the read-only open, with a pool that holds pass 2's replay (§14.1), fails with 58030 and leaves every file byte-identical; and the next open, without the hook,`.
- **S-IOTA-F4 (next S-STORE card; wording, no rule changes; P).** Each Old occurs once in 934e5dd1's file. Tried in scratch, each changes only its line.
  - (a) §14.2, the pass-1 read-error row. Old: ``58030, nothing written beyond the fence and the pass-2 evictions of a fall-back from an image `k` that failed in pass 2; an explicit rollback fails with 58030 too | salvage at the read cut, reported with the read error |`` New: ``58030, nothing written beyond the fence and the pass-2 evictions of a fall-back from an image `k` that failed in pass 2, or of a rollback that a read error in pass 2 let proceed; an explicit rollback fails with 58030 too | salvage at the read cut, reported with the read error |``
  - (b) §14.1, the read error's report. Old: `the salvage reports the read error, with its segment and the position the walk had reached, as well as its cut, and is never reported` New: `the salvage reports the read error, with its segment and the position the walk had reached, as well as its cut and the coverage result against it, and is never reported`
