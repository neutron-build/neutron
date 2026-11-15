Card: S-15-STANDINS-2
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session e140d2e2-af14-4121-b2cf-5b28556dbd01 (a fresh gate run; this agent wrote neither the card nor the S-15-STANDINS-2 commit)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: f7c588968ec0cf9f25239071eb0f894bb32c554f
Mutants: the card names none (spec text). The gate's own text mutants, each on a scratch export of the tip, judged by the card's checker (Acceptance line 7): TMc (SEG-TAIL-SHORT dropped from row 111, A-7d) -> checker; TMe (GCW-BOUND-SKIP dropped from row 50, A-7c) -> checker; TMg (the new A-7d LSN-CHECK-SKIP row's Needs A-6b -> B-4a) -> checker; TMa (E6's "carries that flag ... does not recompute it" clause deleted) -> UNCAUGHT; TMb (the new B-6 `t_ro_open_disk_full` row deleted) -> UNCAUGHT; TMd (S6's "No cursor mutant is judged under S6" deleted) -> UNCAUGHT; TMf (S5's "names no root through the root table or the directory" condition removed) -> UNCAUGHT. Only reading catches the UNCAUGHT ones; they were read (below). Every text mutant reverted (export re-compared byte-identical with the tip).
Followups: S-15-STANDINS-2-F1, S-15-STANDINS-2-F2

Row numbers below are draft 7.9's §15.1 data rows, counted from 1, as in the card and in `verdicts/S-15-STANDINS.md`.

## Gate run
- **Script.** The scaffold's `fresh-gate.sh` and `git-env.sh` came out of the `--no-local` shallow clone (GATE-PROMPT step 1) with no `EXTRACT FAILED` line, scaffold id 50c34ff55a300955a4906b1e41b3939b2f258150. They ran from `~/Workspace/neutron-v3` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-15-STANDINS-2`, stdout and stderr captured to separate files.
- **Lines.**
  - `fresh-gate: commit f7c588968ec0cf9f25239071eb0f894bb32c554f`
  - `fresh-gate: base 50c34ff55a300955a4906b1e41b3939b2f258150`
  - `fresh-gate: checks passed (4803 files hashed in 2.02 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - STEP test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests all EXIT=0.
  - Literal last line `GATE EXIT=0`; exit status 0; stderr empty. `$fg` removed afterwards.
- The diff is spec-only, so the code steps are a regression check.

## Scope
- **Tip and worktree.** `~/.cache/nv3-cards/S-15-STANDINS-2` is on `card/v3-S-15-STANDINS-2` at f7c588968ec0cf9f25239071eb0f894bb32c554f, `git status --porcelain --untracked-files=all` empty, one commit on 50c34ff5 (the scaffold tip, which contains verdict c5617081).
- **The diff.** `git diff nucleus/v3-scaffold...HEAD --stat`: `nucleus/v3/docs/S-STORE.md` only, +71/−39. Inside Touch only; no protected path.
- **The diff is exactly E1–E36.** My own script parsed the 37 Old/New pairs (E21 has two) out of the card, applied them in order to draft 7.9 (sha256 0d6c68c0…) with `<date>` = 2026-11-15 (the commit date), each Old occurring exactly once at its turn, and the result is byte-identical to the tip's S-STORE.md (sha256 2e900200…).
- **§15's table.** All 40 rows present; every Named-mutants cell byte-identical to 7.9 (169 mutants, the same set as 7.8). Changed cells: the statement-and-tests column of I-CURSOR (E6, inserted after "Test `t_cursor_park_equiv`", so no invariant statement changes) and I-RO-OPEN (E9, E10); the Pinned-by cells of I-GCW, I-CKPT-AT-T, I-WAL-COVER and I-RO-OPEN (E7, E15, E14, E11). Nothing else.
- **§15.1.** 122 rows: 7.9's 120 plus E31's B-6 row and E34's A-7d row. The `t_*` sets of §15 and §15.1 are equal (36).
- **Status line and Changelog.** The status line starts `Status: draft 7.10 (card S-15-STANDINS-2, 2026-11-15: ` and keeps the whole 7.9 line after `on draft 7.9 (card S-15-STANDINS, 2026-11-15: ` byte for byte. The 7.10 Changelog entry matches the change; its counts are right (below). Its "No invariant, mutant or test is added, removed or renamed" is true, and the status clause now says test forms and judging cards change.
- **Acceptance.** Lines 1–7 pass on an export of the tip; the checker prints `122 rows; 169 mutants; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8` then `OK`. Line 8 holds (one file). Line 9 is the gate run above.

## Phase checks (my own scripts over 7.8, 7.9 and 7.10; not the card's checker)
- **Depends.** I took PLAN §9 A's cells at sha256 01472e3e… by hand, dropped the conflict-2 notes (A-3c's A-7a, A-4e's B-3/A-7b) and added the pending S-15-STANDINS edges (A-4c on A-4b and A-7a; A-6b on A-1b; A-7c on A-6c and A-1b; A-7d on A-8 and A-1b). No cycle; no Phase A closure holds a Phase B card; every Phase A row's Needs lie in its card's closure, so no Phase A row needs a Phase B card. Without the pending A-6b → A-1b edge, rows 99, 103 and 110 (A-6b) need A-1b outside the closure, exactly as at 7.9; this card does not touch them (the plan card adds the edge, S-15-STANDINS's plan item 10).
- **7.8 → 7.10, mutant by mutant.** A mutant counts on a 7.10 row when its name is in the Mutants cell outside a "fixture only" parenthesis. 25 mutants are judged only on Phase B cards at 7.10: the 17 on rows wholly Phase B at 7.8, plus POISON-IGNORED, XX001-NOT-POISON, CLIENT-READ-POISONS, POISON-WAITER-PARKED, WAL-ERR-NOT-POISON (all on 7.8 I-FAIL-STOP's B-1c/B-1b tests, whose Pinned-by gives A-3c and A-4c only "the latch and `try_latch` check"), CREATE-TREE-SPLIT (`t_create_every_cut`, B-6), LOAD-PAGE-SPLIT (`t_load_every_cut`, B-4d) and ROOT-NAME-KEPT (B-6 since 7.3). I read each of these eight in the 7.8 text; none was judged on a Phase A card there. So **every mutant 7.8 judged on a Phase A card is judged on a Phase A row at 7.10**, GCW-BOUND-SKIP included (row 50, A-7c).
- **7.9 → 7.10.** Card sets per mutant: only GCW-BOUND-SKIP loses a card (B-3). STEP-MUTATE-BEFORE-CHAINS leaves row 44 but stays at B-3 (row 41). Every other change adds a card, or names what "its own" / "the same twelve" meant (the five I-SPACE mutants at A-4b, PACK-OFF at B-3, the twelve at A-7d, the six DROP-* at B-6). Gains: GCW-NOT-RECORDED at B-3; FLIP-NO-GATE, SEG-TAIL-SHORT, LSN-CHECK-SKIP at A-7d; RO-LOCK-FILE, SHORT-SEG-ERROR at B-6. This matches the card's "Never weakened" list exactly.
- **The twelve and the six.** Row 87's twelve equal row 86's (A-3c) list; row 57's six equal row 56's (A-7d) list.

## Rulings on the hard points
**(a) GCW-BOUND-SKIP at A-7c: sound; fail-stop is the right observable.** §13 step 0 item 2 records `gc_w := gc_w_used`; step 4 checks `gc_w ≤ T` at meta write ("A violation is fail-stop (§9.2), as for `gc_w ≤ T` (§10.5)"); PLAN's A-7c row already owns "the step-4 checks". A test-set `gc_w_used > T` before the flip therefore flows through step 0 into step 4's check, and nothing between (steps 1–3) reads or clamps it. Under the mutant the meta is written with `gc_w > T`; the row checks both the fail-stop and that no such meta reaches the file, so either half catches it. The forged value is fault injection under E5's new sentence (a value no caller produces, as `t_cover_ts_cross_check` forges metas), and the check is A-7c's code, so it counts at A-7c. §10.5's Bound paragraph shows real GC keeps `gc_w_used ≤ W ≤ M = T`, so B-3 could never reach the check: the 7.9 placement was wrong and E22's correction is right.
**GCW-NOT-RECORDED at B-3: keep.** Its §10.5 failure text ("After a restart, `W` can then start below the image's pruning, and a snapshot in that gap reads a pruned state") is the restart-floor scenario with real GC; A-7c judges it on an S3 input (a test-set `gc_w_used`), and §15's Stand-ins lead sentence asks for the card that runs the full form, which is B-3. Judging it there adds a judgement and removes none. "—" would leave row 51 a full form with no mutant, which is weaker. One wording nit: row 51's "it is judged at A-7c" has an ambiguous antecedent (the bound check, not GCW-NOT-RECORDED); F1.

**(b) PARK-DROPS-CANDIDATE at A-4e: sound, and the carry sentence is now a requirement on A-4e's code; that is sound.** Case (ii) parks while the scan still skips versions above `S`; a resume that treats the key as found skips to `upper(k)` and the key's version goes missing. Case (i) parks after the newest `≤ S` was found; a resume that treats it as not found takes the next entry, an older version of the key, a second one. That second reading is killed only where A-4e parks inside the group after a find; where it parks "after group `k`" (§6.6 allows both) it is equivalent, and E6 says so. Every non-equivalent reading is killed. The sentence "A-4e carries that flag in the parked position and does not recompute it from the resume key" is normative. It is sound: §6.6 already defines the parked position as carrying "whether the newest version `≤ S` was already found"; the clause only forbids an implementation that departs from that state model, which for ordinary versions would make the mutant equivalent at A-4e, and which B-1d would have to undo anyway, since with a provisional head the flag is not a function of `x`'s ts. It goes beyond the verdict's suggested text ("Where no flag is carried, the mutant is equivalent at A-4e") by closing that escape rather than recording it; that is a strengthening, not a weakening. The plan card must put it in A-4e's row (the card's plan item 5). TMa shows no mechanical check protects the sentence.

**(c) S5's rule against rows 19–20: sound. Row 19 as it stands: sound in substance, mislabelled.** S5's rule is per fixture. Row 19's S5 fixture is the two-leaf fixture (two threads each holding a leaf and reading the other's tree); it judges LATCH-ACROSS-YIELD (tracker) and NESTED-READ-UNDER-LATCH (tracker, deadlock). Row 20's S5 fixtures are the index scan with lookups and the two cursors; they judge CURSOR-LATCH-ACROSS-CALL, CURSOR-PIN-ACROSS-CALL (tracker) and NESTED-READ-UNDER-LATCH. None of these is applied to directory or root-table code, and neither card's closure holds A-7b, so no fixture can name a root through the root table (A-4e's forms resume through the root LPID, I-ROOT-STABLE). MAP-DROP-BEFORE-REF is not on an S5 fixture: it is on row 19's map variant, which runs on S1. It fails through a stale lookup, not a hook, the tracker, a deadlock or the budget, so S1's rule does not make that form full; and row 19 names no full-form card for its S1 part. The kill itself holds: the mutant is in the eviction path, and the stale read reads nothing step 0's item 2 records. But E2 now says a stand-in row either names its full-form card or says why it is the test's last form, and row 19 does neither for its S1 part. This pre-dates the card (S-15-STANDINS's gate missed it) and the card flagged it honestly; it is small and goes to F1, due before A-4c is cut.

**(d) S6 against rows 102 and 105: sound; B-6 for the real scan: accept.** A-7d's closure lacks A-4e, so "a full scan" needs a stand-in there. S6 walks every reachable page through the store's own reads from §6.8 root names and decodes every leaf entry: it replaces a check, not code, and no named mutant rests on the scan (RO-LOCK-FILE and SHORT-SEG-ERROR fail the open; SALVAGE-ABOVE-FRONTIER fails the reads at `S`), so nothing leaves Phase A. For row 105, naming roots through §6.8 means a `dropping` entry stops without naming a page when the scan needs `live`, which matches the real catalog at `S ≥ S_ddl` not naming the dropped tree. The new Needs (A-7b, for root names) are in A-7d's closure. By my closure, A-9 is the only Phase A card holding A-4e, A-7d and A-1b; a crash-matrix card is a poor home for one test. B-6 already re-runs `t_wal_loss_salvage`. Accept B-6.

**(e) PACK-OFF alone at B-3: right.** §11 derives ≈1.2× for the W3/W4 shape against a 1.5× acceptance. SKIP-UNCOUNTED changes only the reported count (expected 0 in that shape); SWEEP-PACK-LEFT leaves a short leaf per junction, at most one per inner page; CASCADE-ONE-LEVEL and JUNCTION-SKIP leave short inner pages, only at height ≥ 4. All of that is inside the "under 1%" term and far inside the 25% headroom, so B-3 cannot see them. PACK-OFF leaves the post-GC tree at its post-update leaf count, which fails 1.5× at once. A-4b judges all five. **One consequence the plan card must carry:** four of the five (all but PACK-OFF) now count only on A-4b's own sweep traversal and skip count, and B-3 cannot judge them. So S-15-STANDINS-F3 (d) (A-4b builds §10.4's pack traversal) is load-bearing, not optional: without it those four are judged on no real code. F2.

**(f) `t_epoch_reserve_two_exhausters`' last form at A-7a: acceptable; no A-7d or split-card row needed.** The pass condition ("both copies reach the image") names no amcheck, open or read at `T` after a crash, so the S2 read-back is how that condition is read, not a weaker check. EXHAUST-DOUBLE-EVICT loses a copy (seen in image `k`'s content); EXHAUST-BLOCKING-CLAIM deadlocks or breaks the budget; CLAIM-LATCHES-MAP fails a hook. None touches step 0's item 2, which is all S1 leaves out. A real-flip form would add only `T`, `gc_w` and the WAL positions, which these mutants never touch. F4's split card may add one cheaply if the owner wants it; it is not needed for any mutant. Nit for F1: S2's closing sentence ("the card that §15.1 names for the full form judges it again") does not yet say "where there is one", though E2's lead sentence now covers the last-form case.

**(g) Row 106's Needs: a real but Phase-B-only inconsistency, which E32 widened.** E32 adds "the full scan by A-4e's cursor" to row 106's form but leaves its Needs at "A-7d", while E31's sibling B-6 row lists A-4e. Row 106 should need B-4a and A-4e. The checker does not check Phase B Needs, and adding B-4a forces the intro's Needs sentence to name B-4a too (TMg shows the checker enforces that sentence). PLAN gives B rows no Depends column; B-6's row says only "Pins I-RETIRE (with B-4a)". Nothing in Phase A depends on it: F1 for the spec cell, F2 for B-6's plan row.

## Findings
1. **Row 19's S1 part has no full-form card and no stated reason (ruling (c)).** Substance sound; label short of E2's new rule. F1.
2. **Row 106's Needs lack B-4a and A-4e (ruling (g)).** Phase B only. F1, F2.
3. **S-15-STANDINS-F3 (d) is now load-bearing (ruling (e)).** F2.
4. **Small text points, none affecting where a mutant counts:**
   - row 51's "it is judged at A-7c" (ruling (a));
   - S2's "judges it again" lacks "where §15.1 names one" (ruling (f));
   - row 100 (A-7d) still claims PREV-HOP-WRONG while its relabelled form now excludes "A-6b's two hops"; the kill there needs a remaining bullet's walk to cross a hop, which the row does not say. PREV-HOP-WRONG stays judged at A-6b (row 99), so nothing moves. F1;
   - the new A-7d LSN-CHECK-SKIP row's Needs omit A-7c (a recycled segment needs a checkpoint's step 6); A-7c is in A-7d's closure, so this is cosmetic.
5. **The card's text, not the spec.** "For the gate" quotes §12.5 as saying "a recycled segment's old records, whose magic and CRC are intact, are replayed as if they were new". No such sentence exists in 7.9 or 7.10; §12.5 says the `lsn` check "catches a recycled segment's old content". The A-7d row E34 adds is consistent with that sentence, so the spec is right; the quotation is not.
6. **Mechanical coverage.** The checker catches mis-placement (TMc, TMe, TMg) but not the substance of E4–E6 or the new B-6 row (TMa, TMb, TMd, TMf). Those were read above.

None of these blocks the merge: no mutant moves out of Phase A, no invariant changes, and the earliest card any of them touches (A-4c) is not cut.

## F1 and F2 of S-15-STANDINS, item by item
- F1 (a) GCW-BOUND-SKIP: done (E7, E8, E22), ruling (a).
- F1 (b) FLIP-NO-GATE at A-7d: done (E15, E28). Row 89's form names splits and packs across a flip, a crash at post-5 and image `k` through amcheck; A-8 is added to its Needs and is in A-7d's closure.
- F1 (c) PARK-DROPS-CANDIDATE: done (E6, E17, E18), ruling (b).
- F1 (d) "its own": done (E22, E24, E27, E29, E35). POISON-AFTER-UNLATCH at A-3c: done (E20), matching 7.9's counting rule; A-4c's split form (row 38) keeps it in Phase A either way.
- F1 (e) STEP-MUTATE-BEFORE-CHAINS once: done (E21). It is on row 41 with "(I-GC-ORDER's, through fixture (a))", as §15 says.
- F2 (a) S5: done (E4, E19), ruling (c).
- F2 (b) the page walk: done (E4's S6, E9–E13, E31, E32), ruling (d).
- F2 (c) S3's cases: done (E3, E23, E25), with an `on` ack added because row 103 uses it; "N2" is gone from §15.1.
- F2 (d) labels: done (E23, E25, E26, E30, E31, E33, E34). Rows 111 and 113 restore A-7d's 7.8 judgement, as F2 (d) allowed.
- F2 (e) Pinned-by: done (E11, E14).
- F2 (f) intro: done (E16); the computed Card/Needs sentence matches.
- F2 (g) "or changed": done (E1, E36).

## What remains for the follow-on plan card (S-15-STANDINS-F3, with F4's plan half)
- S-15-STANDINS-F3 (a)–(f) as written, F3 (d) now mandatory (finding 3).
- S-15-STANDINS-F4: split A-7d, which now runs 30 of 122 rows; FLIP-NO-GATE's new crash form belongs with the re-run forms, and the two restored WAL forms stay on A-7d, as the card says.
- This card's "For the plan card" items 1–11, all of which I checked against the spec and accept, including item 9 (A-8's fixture meta is now an S3 case; it replaces N2 and the earlier plan item 16).
- O1's list becomes 25 mutants and gains the cursor scan of `t_ro_open_disk_full` (B-6); GCW-BOUND-SKIP leaves it.
- B-6's row: it runs `t_wal_loss_salvage`'s and `t_ro_open_disk_full`'s full forms, after B-4a and with A-4e's cursor (finding 2).

## For the owner
1. **B-6 for `t_ro_open_disk_full`'s cursor scan.** Recommend accept (ruling (d)).
2. **`t_epoch_reserve_two_exhausters`' last form.** Recommend accept (ruling (f)).
3. **A-4b builds §10.4's sweep traversal and skip count** (S-15-STANDINS-F3 (d)). Without it, SKIP-UNCOUNTED, SWEEP-PACK-LEFT, CASCADE-ONE-LEVEL and JUNCTION-SKIP are judged on no real code anywhere. Recommend accept, before A-4b is cut.
4. O1 and O2 from S-15-STANDINS are still open; nothing here depends on them.

## Anomalies, verbatim
- None in the gate run. The one textual anomaly is the card's misquotation of §12.5 (finding 5).

## Follow-ups (to list in QUEUE.local.md; this gate does not edit it)
- **S-15-STANDINS-2-F1** (spec, P; one S-STORE card, riding with the next S-STORE card that lands before A-4c is cut, or with F4's spec half if that lands first; no third stand-ins card is needed; due before A-4c is cut). Row 19 (`t_latch_scope`, A-4c) states why its S1 map variant is the test's last form for MAP-DROP-BEFORE-REF (the stale lookup reads nothing that step 0's item 2 records), or S1's rule is widened to cover a stale read checked against the oracle that touches nothing item 2 records, or a full-form card is named. With it, as text tidies: row 106 (`t_wal_loss_salvage`, B-6) needs B-4a and A-4e, and §15.1's intro names B-4a among the Needs column's Phase B cards; row 51's parenthesis names GCW-BOUND-SKIP instead of "it"; S2's closing sentence says "where §15.1 names a full form"; row 100 (A-7d) either says which remaining bullet crosses a hop for PREV-HOP-WRONG or drops that name there (it stays at A-6b); the new A-7d LSN-CHECK-SKIP row's Needs add A-7c. No mutant moves between phases.
- **S-15-STANDINS-2-F2** (plan, P; folded into S-15-STANDINS-F3, the follow-on plan card; due before A-3b is cut, as F3 is, and its A-4b part in any case before A-4b is cut). Adopt this card's "For the plan card" items 1–11. Make S-15-STANDINS-F3 (d) explicit as a requirement: A-4b builds §10.4's pack traversal (pack right, the cascade, the junction walk) and the skip count, since B-3 cannot judge four of the five I-SPACE mutants. A-4e's row states that the found flag is carried in the parked position, not recomputed. B-6's row states it runs the full forms of `t_wal_loss_salvage` and `t_ro_open_disk_full` with A-4e's cursor, after B-4a. O1's list: 25 mutants judged only in Phase B, and the cursor scan of `t_ro_open_disk_full` at B-6.
