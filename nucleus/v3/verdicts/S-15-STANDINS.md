Card: S-15-STANDINS
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 72933c32-6150-4cbc-9b5a-e1d558f18ec0 (a fresh gate run; this agent wrote neither the card nor any S-15-STANDINS commit; an earlier gate attempt died before writing a verdict)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: a5e1a2a1c8db7eba3b519fa86299f71d2d3631b3
Mutants: the card names none (spec text). The gate's own text mutants, each on a scratch copy of the tip, judged by the card's Acceptance lines 1-10 and by the gate's scripts (closure.py, mutants.py): TM2 (row 86 needs A-7a) -> Acceptance checker (line 7), closure.py; TM3 (row 8 needs B-3) -> Acceptance checker, closure.py; TM1 (PARK-DROPS-CANDIDATE removed from row 8) -> mutants.py, Acceptance UNCAUGHT; TM4 (row 62, the race variant's full form, deleted) -> UNCAUGHT; TM5 (S2 checks the CRC only, no content comparison) -> UNCAUGHT; TM6 (S2's counting rule negated) -> UNCAUGHT; TM7 (row 87's full form moved from A-7d to B-6) -> UNCAUGHT. Only reading catches the UNCAUGHT ones. Spec mutants judged on paper against every stand-in form: see "Stand-ins, one by one".
Followups: S-15-STANDINS-F1, S-15-STANDINS-F2, S-15-STANDINS-F3, S-15-STANDINS-F4

## Gate run
- **Script.** The scaffold's `fresh-gate.sh` and `git-env.sh` came out of the `--no-local` shallow clone (GATE-PROMPT step 1) with no `EXTRACT FAILED` line, scaffold id 30860cf1adee973fcff2f1208eaa57b20fc79e05. They ran from `~/Workspace/neutron-v3` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-15-STANDINS`, stdout and stderr captured to separate files.
- **Lines.**
  - `fresh-gate: commit a5e1a2a1c8db7eba3b519fa86299f71d2d3631b3`
  - `fresh-gate: base 30860cf1adee973fcff2f1208eaa57b20fc79e05`
  - `fresh-gate: checks passed (4801 files hashed in 1.66 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - STEP test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests all EXIT=0.
  - Literal last line `GATE EXIT=0`; exit status 0; stderr empty. `$fg` removed afterwards.
  - The diff is spec-only, so the code steps are a regression check.
- **The earlier attempt.** Its scratch held a `fg.Q51aFc` directory, which I removed, and a run whose stderr reads `Terminated`. I moved that run's output to `stale-attempt1/` and did not read it. I reused only its two extracted drafts, after `cmp` against `git show 30860cf1:nucleus/v3/docs/S-STORE.md` and `git show a5e1a2a1:…`. Both are identical, sha256 5d569f09… (7.8) and 0d6c68c0… (7.9).

## Scope
- **The diff.** `git diff nucleus/v3-scaffold...a5e1a2a1 --stat` lists `nucleus/v3/docs/S-STORE.md` only, +176/−21, five commits on 30860cf1. It is inside the Touch-only list, and no protected path is touched.
- **The scaffold moved during the gate.** It went from 30860cf1 to d026f95d, the merge of card/v3-A-1b-0 (nucleus-env and its verdict only). S-STORE.md there is still 5d569f09 (draft 7.8). So no other S-STORE card merged, no re-anchoring is needed, and the three-dot diff is unchanged.
- **Every changed line is accounted for.** A word-level diff gives 20 hunks:
  - the status line;
  - owner parentheses in §5.6, §12.5 (three), §13, §14 and §14.1 (two);
  - the Stand-ins paragraph, inserted after §15's unchanged first paragraph;
  - the twelve rows Limit 3 names (I-CURSOR, I-LATCH, I-ROOT-STABLE, I-FAIL-STOP, I-LPID-EXACT, I-LINK-ATOMIC, I-NO-INPLACE, I-CKPT-AT-T, I-WAL-COVER, I-WAL-CHAIN, I-RO-OPEN, I-ACK-PREFIX). Each changes only in Pinned-by cells and owner parentheses, plus I-CURSOR's A-4e form sentence, I-WAL-COVER's bulk-variant clause and I-LINK-ATOMIC's S4 wording;
  - §15.1;
  - the Changelog entry.
- **Confirmed by script:**
  - The Named-mutants columns of all 40 rows are byte-identical (169 distinct mutants).
  - The `t_*` names are the same set, in §15 (36) and in the whole file.
  - Each row's invariant statement, meaning the text before its first test, is byte-identical.
  - §15.1 equals the card's target table cell for cell (120 rows), and the Stand-ins paragraph equals the card's text.
  - The status line keeps the whole 7.8 line after "on".
  - Acceptance lines 1–10 pass on an export of the tip, the checker printing "120 rows / OK". Line 11 passes, and line 12 is the gate above.
- **Wording.** 7.9's status clause says "no invariant, mutant or test is added, removed, renamed or changed". Test forms and owners did change, so "or changed" is wrong (F2). The Changelog's "added, removed or renamed" is right.

## Phase checks (my own scripts, not the card's checker)
- **Depends.** I took PLAN §9 A's cells at 01472e3e (re-hashed at the end of the gate) and applied the card's six edge changes. The result:
  - no cycle: A-3c ↔ A-7a is gone, and A-4c has no edge back to A-4e;
  - no Phase A closure holds a Phase B card;
  - all 120 rows' Needs lie in their card's closure, together with each stand-in's implied need (S1 and S2 need A-3c's gate and step 2, S4 needs A-7b, a "real `write_guard`" needs A-7a);
  - the dependency chain grows by one card for A-4c and A-4e, and is unchanged for A-7c, A-7d and A-8a.
- **Mutants, 7.8 → 7.9.** 7.8 has no table, so I built its judged-on map by hand from the 7.8 text: owner parentheses, Pinned-by cells, the owner sentences in §10.4, §12.5 and §14, and draft 7.3's ROOT-NAME-KEPT rule. I compared it with §15.1, resolving "its own" by hand.
  - Every mutant is on some §15.1 row.
  - **One mutant that 7.8 judged on a Phase A card is judged only on a Phase B card at 7.9: GCW-BOUND-SKIP** (finding 1).
  - Others move between Phase A cards:
    - READER-IO-UNDER-LATCH, from A-3b to A-4a and A-4e;
    - CURSOR-LATCH-ACROSS-CALL, CURSOR-PIN-ACROSS-CALL and REVERSE-LATCH-LEFT, from A-4c to A-4e;
    - EXHAUST-DOUBLE-EVICT, EXHAUST-BLOCKING-CLAIM and CLAIM-LATCHES-MAP, from A-4c to A-7a;
    - FLIP-NO-GATE and CKPT-T-VISIBLE lose A-7a;
    - the three A-7c chain mutants go to A-7d only;
    - LSN-CHECK-SKIP and SEG-TAIL-SHORT lose A-7d and stay at A-6b.
- **Phase-B-only mutants at 7.9: 26, not the card's "eight".** The 26 are:
  - the eight the card lists;
  - 17 on rows that were wholly Phase B at 7.8: I-PROV-READ 2, I-NEWEST 2, I-GC-READEQ 5, I-GC-ORDER 2, I-GC-COMPLETE 1, I-GC-LOCAL 4, I-BULK-PUBLISH 1;
  - ROOT-NAME-KEPT, which has counted only at B-6 since draft 7.3.

  None of the 18 beyond the card's eight was Phase A at 7.8. The miscount is in the card's Goal and its Mutation-evidence section, not in the spec.
- **What the card's Acceptance does not check.** It checks closure, cycles, Phase B needs and test coverage. It checks no mutant placement, no full-form existence and no stand-in substance: TM1 and TM4–TM7 pass it. Those were read, below.

## Stand-ins, one by one
For every Phase A form that uses a stand-in, the question was whether it can kill each mutant §15.1 judges there, on real code. Every full-form card named is a PLAN §9 row.

| Row | Form | Mutants claimed there | Killable as specified? | Full form |
|---|---|---|---|---|
| 86 | `t_step2_waits_for_claimed_copy`, A-3c, S1–S3 with the S2 read-back | the twelve | yes, all twelve (item 1 below) | A-7d (row 87) |
| 84 | `t_epoch_reserve_two_exhausters`, A-3c | EXHAUST-DOUBLE-EVICT (S2: a copy missing), EXHAUST-BLOCKING-CLAIM (deadlock or budget), CLAIM-LATCHES-MAP (hook) | yes; claim and draw are the reserve's, by S3's first case | A-7a (row 85), still on S1 and S2, which is enough: the pass condition names only the image |
| 8 | `t_cursor_park_equiv`, A-4e | CURSOR-STALE-LPID (same-tree reuse), PARK-DROPS-CANDIDATE, SCROLL-RESUME-SKIP, BATCH-KEPT-AFTER-CLOSE (store close and hook) | yes for three; PARK-DROPS-CANDIDATE only for its found-flag half (item 2) | B-3, B-1d, B-2c, R-1 (O1) |
| 16, 17 | cold variant, A-4a and A-4e, S1 | READER-IO-UNDER-LATCH (hook) | yes; reader and cursor descent are real code, and S1's rule makes the form full | full under S1 |
| 19 | `t_latch_scope`, A-4c, S1 and S5 | seven (tracker, hook, deadlock) | yes; the real `write_guard` is in closure once A-4c depends on A-7a | none named for S5 (F2) |
| 20 | `t_latch_scope`, A-4e, S5 | four (tracker, deadlock) | yes | none named for S5 (F2) |
| 22 | `t_dir_root_no_stall`, A-7b, S4 | DIR-LATCH-ON-READ on Y's reader and writer (hook) | yes; on S4 it is fixture-only, as stated | B-3 (row 23) |
| 37 | `t_poison_before_unlatch`, A-3c, S3 | POISON-AFTER-UNLATCH | yes if the stand-in operation fails through A-3c's own fail-stop path (mark, then release); if it marks the poison itself, 7.9's rule makes the kill fixture-only | A-4c (row 38), a real split, so the mutant stays Phase A either way |
| 50 | I-GCW, A-7c, S3 | GCW-NOT-RECORDED (the meta's field against the value set) | yes | B-3 (row 51), which cannot kill GCW-BOUND-SKIP (finding 1) |
| 52 | I-LPID-EXACT, A-4b, S2 | LEAK-ON-PACK | yes (mapped LPIDs against a walk) | A-8, A-7d |
| 56, 58 | the drop tests, A-7d, S4 | the DROP-* mutants, on A-7b's walk (real code) | yes; ROOT-NAME-KEPT is fixture-only, as at draft 7.3 | B-6 |
| 60 | `t_split_reserve_order`, A-4a, S1 | three (hook) | yes, full under S1 | — |
| 61 | `t_alloc_restart_progress` race variant, A-4a, S2 | MAPLEAF-CREATE-UNCONDITIONAL | yes: the replaced ref leaves the first LPID unmapped in the current map and in image `k`, while the walk reaches it | A-7d (row 62) |
| 63 | `t_replace_free_shape`, A-5, S1–S3 | three (hook) | yes | A-7d (row 64) |
| 88 | I-CKPT-AT-T's row, A-7c, S2 and S3 | FLIP-NO-GATE (half a split in image `k`), WAL-SYNC-T-ONLY (crash at post-5, `c`'s record past A-6b's valid end), CKPT-T-VISIBLE (meta `T` against the provider's `M`) | yes | A-7d (row 89) for WAL-SYNC-T-ONLY only, so FLIP-NO-GATE is never judged again (F1); B-1c (row 90) |
| 103 | `t_seg_ack_durable`, A-6b, S3 | SEG-ACK-BEFORE-DURABLE | yes: A-1b's power cut, and the walk stops before a record already reported synced | A-7d (row 104) |
| 105 | `t_wal_loss_salvage`, A-7d | SALVAGE-ABOVE-FRONTIER | yes | B-6 (row 106) |
| 115 | `t_ckpt_sync_prefix`, A-7c, S2 | CKPT-SYNC-OVER-HOLE | yes | A-7d (row 116) |
| 102 | `t_ro_open_disk_full`, A-7d, "full; the full scan is a page walk" | RO-LOCK-FILE, SHORT-SEG-ERROR (both caught by the open, not the scan) | yes | none: no card runs the cursor scan (F2) |

**No stand-in form claims a mutant it cannot kill.** Two kills depend on the implementation:
- POISON-AFTER-UNLATCH at A-3c: 7.9's own rule ("A mutant counts where it is applied to the real code it names") settles it.
- PARK-DROPS-CANDIDATE at A-4e: this needs the clause in F1.

## The implementer's "look hardest" list
1. **`t_step2_waits_for_claimed_copy` at A-3c: all twelve count there, and none needs a crash.**
   - **Hook or in-process:**
     - ADOPT-LISTED-HOME: the hook checks one record per page and no adoption over a home the list names.
     - IMAGE-LIST-GROWS: an allocation hook gives 53200.
     - MAP-COPY-NOT-DETACHED: the hook, and §13's debug assertion.
     - RESERVE-DRAW-BLOCKS: a hook on the draw, which is the reserve's code.
     - MAP-HOME-SLOT-REUSED: reloading M from the reused slot `s1` fails its CRC with XX001, in process. The variant's slot-bitmap check is A-7b's and waits for A-7d.
   - **Through the S2 read-back:**
     - STEP2-NO-WAIT and IMAGE-RECORD-AFTER-RELEASE: image `k` names the page's pre-claim home, whose content differs from the oracle's epoch-`k` content. The late append also trips §13's debug assertion.
     - IMAGE-SLOT-REUSED: the listed slot now holds another page, so its id, its CRC against the map entry's `crc`, and its content all differ.
     - MAP-POSITION-AS-LPID: one record replaces the other.
   - **The three whose failure text reads "which a read at `T` after a crash at post-5 returns"** (MAP-PHASE-DROPS-CLAIMED-COPY, MAP-PHASE-SKIPS-VICTIM, MAP-SOURCE-UNCHECKED). Image `k` maps a page to a home that holds epoch-`k+1` content, or the victim's earlier content. S2 reads that home through the `Env` and its CRC passes, because the entry's `crc` matches what is there; the content check against the oracle's epoch-`k` content fails. The crash in the full form only forces recovery onto image `k`. S2 reads image `k` directly, so it sees the same failure. IMAGE-SLOT-REUSED's text says amcheck, not a read at `T`; S2's CRC check catches it.
   - **Conditions:**
     - (i) S2's content comparison carries at least five of the twelve. TM5 shows that a CRC-only S2 would let them survive and that no mechanical check notices.
     - (ii) The map-copy variant's evicted page must carry epoch-`k+1` content, as its failure text says. A previous-epoch victim's record would mask the mutant.
     - (iii) These are A-3c counts only if the claim, draw, list, data phase and map phase are A-3c's code (the card's R1), and the gate and S1 are A-3c's (R2). Neither reading is in the spec or the plan yet (F3). If the plan card gives any of them to A-7a or A-7b, the matching mutants become fixture-only at A-3c and count at A-7d instead, so they still stay in Phase A.
   - **One more A-3c point, for F3.** After an S1 flip, A-3c's own eviction and ref updates change map pages in epoch `k+1`. §4.1 routes those changes through `write_guard`'s copy on first touch, which is A-7a's code. S3's first case covers `write_guard` calling the reserve, not A-3c's code needing `write_guard`. A-3c's tests can pre-touch through S3 (the map-copy variant's "so that M is copied" then holds by construction). The plan card should say which way A-3c does it.
2. **PARK-DROPS-CANDIDATE at A-4e: a real kill of half the mutant.**
   - It is applied to A-4e's real park code, so it is not fixture-only in 7.9's sense.
   - With no provisional head, the only carried state is the found flag. "Keeps only the resume key" can be read two ways:
     - found := true fails case (ii): the key goes missing.
     - found := false fails case (i), but only if A-4e parks inside the group after a find. §6.6 lets it park "after group `k`" instead, and then this reading is equivalent.
   - For ordinary versions the flag is exactly "x's ts ≤ S". An A-4e that recomputes it rather than carrying it makes the mutant equivalent there.
   - Every non-equivalent reading is killed, which meets the standard. But A-4e's gate needs to be told, and I-CURSOR's new sentence does not say how the mutant fails at A-4e (F1).
   - The candidate half cannot run before B-1d. That half is the mutant's namesake and §15's only failure text ("a version older than `c`"), and the candidate rule is B-1d's code (I-PROV-READ). No Phase A code is left untested, because that code does not exist in Phase A; at 7.8 the A-4e claim could not be met at all. O1 should say that the candidate carry is first pinned in Phase B.
3. **"Its own" cells.** The spec never defines "its own"; only the card does.
   - **I-SPACE.** §10.4 already says A-4b catches SWEEP-PACK-LEFT, CASCADE-ONE-LEVEL and JUNCTION-SKIP "after a complete sweep", and SKIP-UNCOUNTED and PACK-OFF go with the bound. So A-4b's "its own" means all five. B-3's acceptance reaches CASCADE-ONE-LEVEL and JUNCTION-SKIP only if its trees reach height 4.
   - **The rule now bites here.** Under 7.9, a mutant applied to a test-only sweep is fixture-only. A-4b's counts therefore stand only if A-4b builds §10.4's pack traversal (pack right, the cascade and the junction walk). PLAN's A-4b row says only "delete/merge" (F3).
   - **I-CKPT-AT-T at B-1d and I-ACK-PREFIX at B-1c.** §15 names no S-STORE mutant for these cards, so the cell should read "—", as rows 11 and 95 do (F1).
   - **`t_drop_every_cut` at B-6.** "Its own" there means the six DROP-* names.
   - None of these moves anything between phases.
4. **STEP-MUTATE-BEFORE-CHAINS** is on two rows. Row 41 puts it under I-GC-READEQ, and row 44 puts it in I-GC-ORDER's row test. §15 lists it under I-GC-ORDER, killed by `t_sweep_step_shape`'s fixture (a), so row 44 claims a kill that the row's own directed test is never said to make. It is B-3 only either way, so this is cosmetic (F1). **ROOT-NAME-KEPT** is as in draft 7.3: fixture-only at A-7d (row 56) and counted at B-6 (row 57).
5. **Pinned-by cells.** I-RO-OPEN's cell lacks B-6 (row 106), and I-WAL-COVER's lacks B-4d (row 95). I-CURSOR, I-LATCH and I-CKPT-AT-T did gain their Phase B cards. The test parentheses and §15.1 name both cards, so PLAN §8's exemption still finds them. The cells are inconsistent but do no harm (F2).

## Findings
1. **GCW-BOUND-SKIP (row 51): the row's form cannot kill it, and the mutant leaves Phase A.**
   - **What it is.** The mutant removes "the `gc_w ≤ T` check at meta write" (§10.5). That check is in §13 step 4, which is A-7c's code.
   - **Why B-3's form cannot reach it.** Row 51 judges it at B-3 under "full (restart floor)". Real GC keeps `gc_w_used ≤ W ≤ M = T`, so no full form ever trips the check, and the restart floor never touches it. Only a forged `gc_w_used > T` at the flip trips it, and that tests A-7c's code.
   - **Who owned it at 7.8.** The 7.8 cell, "A-7c, B-3 (restart floor)", did not say which card took which mutant. Reading it by where the check lives gives the mutant to A-7c. Only the card's own reading ("B-3 keeps GCW-BOUND-SKIP … as today") puts it at B-3.
   - **Why it matters.** On the 7.8 reading, the Changelog and status claim "every mutant draft 7.8 judged on a Phase A card is still judged on one" fails for this mutant. The Phase A gate would also exempt a check whose code is in Phase A.
   - **Fix (F1).** Row 50 judges GCW-BOUND-SKIP at A-7c, with a forged `gc_w_used > T` that must fail-stop at meta write. That is fault injection, as `t_cover_ts_cross_check` forges metas. Row 51 keeps the restart floor with no mutant, or uses the same forging.
   - **Why it does not block the merge.** No card before A-7c reads this cell, and the fix is one cell inside Phase A.
2. **FLIP-NO-GATE is judged under S2 at A-7c and never again**, although 7.9's counting rule says the full-form card "judges it again". Row 89 (A-7d) lists only WAL-SYNC-T-ONLY (F1).
3. **The spec does not say how PARK-DROPS-CANDIDATE fails at A-4e** (item 2) (F1).
4. **Two stand-ins have no full form, and one is outside S1–S5.**
   - S5 (rows 19 and 20) has no full-form card and no rule like S1's.
   - Row 102 turns "a full scan of every published tree" into a page walk. That is not S1–S5, the row is labelled "full", and no card runs the cursor scan.
   - Row 105 does the same, but B-6 runs its full form.
   - No named mutant rests on either. But the Stand-ins paragraph says "Only these stand-ins are allowed" and that §15.1 names each full form (F2).
5. **S3's closed list of cases misses inputs the table uses:**
   - the owner's rewrite of its provisional tag-8 entry (A-5's main fixture, row 63);
   - A-8's fixture meta, written with A-2's codec (rows 53 and 82);
   - `t_wal_loss_salvage`'s directory standing in for the catalog.

   Row 82 also cites "N2", which is a note in the card. In the spec, "N2" is PLAN-v3's decision id (F2).
6. **"Full" labels that are not full.** Row 85 uses S1 and S2; row 102 uses a page walk; row 111 drops the short-tail variant. Row 113 drops 7.8's A-7d judgement of LSN-CHECK-SKIP, and row 111 does the same for SEG-TAIL-SHORT. Both mutants stay judged at A-6b (F2).
7. **Small text points.**
   - The status line's "or changed" (see Scope).
   - §15.1's intro lists B-2b, which is only a need, and omits B-2e, which is row 12's need.
   - The two Pinned-by cells (item 5).

   All three go to F2.
8. **The table rests on plan text that is not written yet:** the card's R1, R2 and R3, A-4b's sweep traversal (item 3), and `write_guard` for A-3c's own map-page changes (item 1) (F3).
9. **A-7d now runs 29 of the 120 rows.** The next largest is B-3, with 10 (F4).

## For the owner
**O1, the Phase A gate.**
- **Plan text needed in §8's Phase A row:**
  - Replace "(at draft 7.8: B-1a, B-1b, B-1c, B-1d, B-2b, B-3, B-4d, B-6, B-7, and R-1's account in the cursor close variant; card PL-8, PL-7-F1c)" with "(at draft 7.9: the Phase B rows of S-STORE §15.1, on B-1a, B-1b, B-1c, B-1d, B-2c, B-3, B-4d, B-6, B-7 and R-1)".
  - Add: "A test that §15.1 runs on a Phase A card in a stand-in form (S-STORE §15 "Stand-ins") is pinned for this gate by that form, with the mutants §15.1 judges on that card: I-CURSOR by A-4e's forms, and `t_dir_root_no_stall` by A-7b's form on S4."
  - Remove the PL-10 flag, both there and in §9 A's conflict 2, once the Depends cells change.
  - Record what Phase A then leaves to Phase B:
    - PARK-DROPS-CANDIDATE's candidate half (B-1d);
    - CURSOR-STALE-LPID over another tree's reused LPIDs (B-3);
    - the cursor close through `ROLLBACK TO` (R-1);
    - the SER recorded edges (B-2c);
    - DIR-LATCH-ON-READ on the real sweep (B-3);
    - CKPT-T-VISIBLE with the real frontier (B-1c);
    - the bulk-load variant's real load (B-4d);
    - salvage over the real catalog (B-6);
    - the 26 mutants judged only in Phase B (25 once F1 lands).
- **If the owner says no.** 7.9 still describes correctly who runs which form. But its sentence "A form that §15.1 names to a Phase B card is pinned by that card (PLAN-v3 §8, Phase A gate)" becomes false, and the deadlock comes back. The full forms need B-3's sweep, B-1d's candidate rule, B-2c's SSI and R-1's account, all Phase B code, and Phase B opens only at the Phase A gate. So a "no" also needs a plan change. Either B-3, B-1d, B-2c and R-1 (and B-1c, B-4d and B-6 for their own forms) open before the Phase A gate, or these criteria move to the Phase B gate, which is O2. §8 already exempts "the tests and mutants S-STORE §15 names to a Phase B card", so a "no" revokes an existing rule.
- **Recommendation: accept**, with the list above written into the plan.

**O2, the Phase B gate.**
- **Plan text.** Add to §8's Phase B row: "Every S-STORE §15.1 row named to a Phase B card pinned, with the mutants §15.1 judges there; the Phase B gate's P verdict checks each card's verdict against §15.1."
- **Without it.** 7.9 still holds together, because each Phase B card's own gate reads §15.1. But 35 rows would then have no check at the phase level, and the stand-ins' full forms are exactly what a card's text can drop without anyone noticing.
- **Recommendation: accept.**

## A-7d: split it
- **The split.** A-7d keeps recovery and the forms that 7.8 already gave it, about 20 rows. A new test-only card takes the full forms that 7.9 moves to A-7d from earlier stand-ins: rows 26, 54 (its pack part), 62, 64, 75, 87, 89, 104 and 116, and the A-7c-mutant half of row 100. That is about ten forms; the heaviest is `t_step2_waits_for_claimed_copy`, with its twelve mutants. The new card depends on A-7d, A-8 and A-1b, and adds no code.
- **What it buys.** A-8a then waits only for A-7d, and a REWORK round on the re-runs does not hold up recovery.
- **Order.** The plan card creates the row first, because §15.1's card ids must be §9 rows. A spec card then relabels the affected rows (F4).

## Anomalies, verbatim
- The earlier attempt's `fresh-gate.err` held one line: `Terminated`.
- `git rev-parse nucleus/v3-scaffold` read 30860cf1adee973fcff2f1208eaa57b20fc79e05 at the start of the gate and d026f95de415c3f98802164b45313b4476fa9d06 at the scope check: "nucleus v3: merge card/v3-A-1b-0 (MERGE-WITH-FOLLOWUPS, gate P/claude-opus-5-5)".
- QUEUE's S-15-STANDINS row says "NO mutant moves to Phase B". Finding 1 is one that does, on the reading given there.
- The card's Goal says "The eight mutants §15.1 judges only on Phase B cards". §15.1 judges 26 only on Phase B cards.

## Follow-ups (to list in QUEUE.local.md; this gate does not edit it)
- **S-15-STANDINS-F1** (spec, P; one S-STORE card, which may ride with bundle γ or merge with F2; due before A-4b is cut, and its GCW-BOUND-SKIP part before A-7c is cut and before the follow-on plan card writes A-7c's and B-3's rows): §15.1's mutant placement.
  - (a) GCW-BOUND-SKIP is judged at A-7c (row 50), with a forged `gc_w_used > T` that must fail-stop at meta write (§13 step 4, §10.5). Row 51 keeps the restart floor with no mutant.
  - (b) Row 89 (A-7d) judges FLIP-NO-GATE again: splits and packs across a flip, a crash at post-5, and image `k` through amcheck.
  - (c) I-CURSOR gains one clause on how PARK-DROPS-CANDIDATE fails at A-4e. It is applied to the found flag that the A-4e park carries (§6.6): found := true fails case (ii), and found := false fails case (i) where A-4e parks inside the group after a find. Where no flag is carried, the mutant is equivalent at A-4e. B-1d judges its candidate half.
  - (d) Names replace "its own": the five I-SPACE mutants at A-4b, the ones B-3's W4-shape acceptance can kill at B-3, "—" for I-CKPT-AT-T at B-1d and I-ACK-PREFIX at B-1c, and the six DROP-* names for `t_drop_every_cut` at B-6.
  - (e) STEP-MUTATE-BEFORE-CHAINS goes on one row, `t_sweep_step_shape` under I-GC-ORDER, and comes off row 44's row test.
- **S-15-STANDINS-F2** (spec, P; may be the same card as F1; due before A-4c is cut, and its S3 part before A-5 is cut): completeness and labels.
  - (a) S5 gets a rule like S1's (a test whose mutants all fail through the tracker, a hook or a deadlock, and that names no root through the root table, is full under S5), or `t_latch_scope`'s two-tree fixtures get a full-form card.
  - (b) `t_ro_open_disk_full`'s page walk becomes a stated stand-in, and a card runs §15's full scan with A-4e's cursor. B-6, which already re-runs `t_wal_loss_salvage` over the catalog, is one choice. The other is to put the narrowing to the owner. `t_wal_loss_salvage`'s directory-as-catalog and page walk are listed as stand-ins too.
  - (c) S3's list gains the owner's rewrite of its provisional tag-8 entry (A-5) and A-8's fixture meta written with A-2's codec (rows 53 and 82). Row 82's "N2" is replaced by its text.
  - (d) Rows 85, 102 and 111 drop "full" or say what they omit. Rows 111 and 113 either restore A-7d's 7.8 judgement of SEG-TAIL-SHORT and LSN-CHECK-SKIP or say it is dropped.
  - (e) I-RO-OPEN's Pinned-by gains B-6, and I-WAL-COVER's gains B-4d.
  - (f) §15.1's intro lists B-2e, or drops B-2b.
  - (g) The next status clause notes that 7.9's "or changed" covered names, not forms.
- **S-15-STANDINS-F3** (plan, P; this is the follow-on plan card the card describes, with additions; due before A-3b is cut, as PL-9-F2 was, since A-3b's row loses the cold variant and A-3c's Depends drops A-7a).
  - (a) The card's 19 items.
  - (b) R1 and R2 adopted verbatim in A-3c's row, and R3 in A-7c's. §15.1's A-3c counts and every S1 or S2 form before A-7a rest on them; if either reading goes another way, F1 relabels the affected rows before A-3c is cut.
  - (c) Who builds `write_guard`'s copy on first touch for the map pages that A-3c's own eviction and ref updates change after an S1 flip: either A-3c, or A-3c's tests pre-touch through S3.
  - (d) A-4b's row says A-4b builds §10.4's pack traversal (pack right, the cascade, the junction walk). §10.4's "A-4b catches all three" and 7.9's fixture-only rule both need it.
  - (e) O1's and O2's text as the owner rules, including the list of what Phase A leaves to Phase B.
  - (f) A-6c's "Also pins WAL-ERR-NOT-POISON at the store level" has no §15.1 row. Drop it, or ask F1 to add an A-6c row.
- **S-15-STANDINS-F4** (plan, then spec, P; due before A-7d is cut): split A-7d, which has 29 of the 120 rows, as in "A-7d: split it" above.
  - A new test-only card takes rows 26, 54 (its pack part), 62, 64, 75, 87, 89, 104 and 116, and the A-7c-mutant half of row 100. It depends on A-7d, A-8 and A-1b; A-8a still depends on A-7d alone.
  - The plan card creates the row first. A spec card then relabels those §15.1 rows.
