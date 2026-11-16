Card: S-EPSILON
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 33d36376-7eb7-4ef9-8157-8618ca79b5e4 (a fresh gate: this agent wrote neither the card, its commit, the S-DELTA appendix nor any earlier verdict)
Date: 2026-11-16
Gate-exit: GATE EXIT=0
Commit: 45e85d069f4b717568542aeaa7858095b179678e
Mutants: spec text, none in code. The card's spec mutant, walked as text on its owning row (t_fallback, A-7d) at 45e85d06: ROLLBACK-REFUSES-READ-ERROR -> t_fallback read-error case, caught in its full form (rollback 55000, read-only 58030); its partial form that accepts the read error only in step 2 -> UNCAUGHT (no case reads image k's fault in pass 2; finding 3), caught after the rework by the pass-2 case. Gate's own: G1 a plain read-write open that falls back on its own on a pass-2 read error -> UNCAUGHT at 45e85d06, caught after the rework (the pass-2 case's first assertion and write hook); G2 a rollback that answers a read error with no previous image with XX001 -> the stated behaviour at 45e85d06 (finding 2), caught after the rework (the no-previous-meta variant); G3 a read-only salvage after a read error that omits the shortfall against meta k -> UNCAUGHT and not required at 45e85d06 (O-3), caught after the rework; G4 a fixture hook keyed on the page's LPID (so image k-1's copy fails too) or on its slot for ever -> a correct build fails the case at 45e85d06, excluded by the reworked fixture text. Text mutants on the tip, judged by the card's Acceptance: TM1 (ROLLBACK-REFUSES-READ-ERROR dropped from t_fallback's 15.1 row) -> lines 18, 19, 22, 23 (checker: not judged on A-7d, Phase-B-only set, 0 t_fallback rows); TM2 (a stray edit to 14.2's Otherwise row) -> lines 21, 22; TM3 (F4-2's read-error disjunct removed) -> lines 9, 10, 21, 22. All reverted; no mutant was applied in the worktree
Followups: S-EPSILON-F1

## Verdict in brief

**REWORK.** The bytes are right. The branch is exactly the card's twelve blocks: my own script re-applied them to d7d41677's `docs/S-STORE.md` (draft 7.14, sha256 `6197762c…1c14`) and got the committed file byte for byte (sha256 `7e2d901b…c481`). F4-1 to F4-10 are the S-DELTA appendix's Old and New texts byte for byte. No invariant, test or mutant of 7.14 is removed, renamed or weakened, and no mutant judged on a Phase A row at 7.14 is now judged only in Phase B. The card's reverse section restores 7.14 byte for byte outside the status line and the Changelog.

The design is right and safe where it matters most. A plain read-write open still fails with 58030 on a read error and writes nothing beyond the fence and pass 2's evictions. Under honoured syncs, neither newly-succeeding open can serve or leave a state silently missing an acked commit. An explicit rollback re-reads image `k` at its own open, so it abandons only an image that fails to read there.

But the adopted text has the gaps the last five spec cards were reworked for:
- **Two exception lists that read as complete now omit the case**: I-RO-OPEN's row (finding 1), and §14.1's opening paragraph, which carries the same list (found by this gate).
- **A SQLSTATE that contradicts its definition.** A rollback that proceeds on a read error and finds no previous image is given "XX001 (detail: image `k` failed, with no previous image)", although no integrity check failed (finding 2).
- **A loss that can go unreported.** Under a device that drops syncs, the read-only salvage after a read error is not bound to report what it lacks against meta `k`, so commits that image `k` holds can be missing with no report (O-3).
- **Stated behaviour that no test or mutant pins.** The pass-2 half is untested: a build that accepts the read error only in step 2, or that falls back on its own on a pass-2 read error, passes every case (finding 3).
- **Sentences the case now contradicts literally**:
  - I-FALLBACK's "only a newest image that fails an integrity check at open falls back" (finding 5a);
  - §2.2's summary (O-4);
  - §4.4's "Neither is an error, unless image `k` fails at open" (found by this gate);
  - §14.2's rows, which give "opens meta `k`" for a refused case (finding 4), and "an explicit rollback opens it" where a check against meta `k−1` refuses (found by this gate).
- **A fixture that admits an instantiation on which a correct build fails**: the hook on a slot that image `k−1` shares (O-4 (i), in the implementer's numbering).

The rework is **12 exact blocks** (RW-1 to RW-12, below), 5 replaced Acceptance lines and 10 added ones. It adds no mutant, test, row or invariant: §15 keeps 47 rows, 226 mutants and 48 tests, §15.1 keeps 158 rows, and 24 mutants stay judged only in Phase B.

How I checked it:
- I applied all twelve blocks to a scratch copy of 45e85d06's file. Each Old occurs once at its turn, and the result is sha256 `a1376c9491cda2aa987a2736825e313318246aa97fba126181b021731bfa700f`.
- I re-read every changed paragraph in place and re-walked every test it touches (below).
- I committed the result in a throwaway `--shared` clone and ran the full new Acceptance list. All 34 commands before `gate.sh` pass, under the default locale and under `LC_ALL=C`, and the checker prints `OK`.
- Each of the 15 new or replaced lines fails on 45e85d06.
- Leaving out any one block fails at least one line besides the diff-hash line.
- The rework's inverse, composed with the card's reverse, gives back 7.14 outside the status line and the 7.15 Changelog entry, so the owner's choice stays reversible until A-7d is cut.

## Gate run

- **Extract.** A shallow `--no-local` clone of `nucleus/v3-scaffold` gave scaffold id `d7d4167777fe24e38b55bb63983b3a9033e72822`. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **Run.** From `~/Workspace/neutron-v3`, `bash "$fg/fresh-gate.sh" 45e85d069f4b717568542aeaa7858095b179678e`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-EPSILON` and stdout and stderr captured separately:
  - `fresh-gate: commit 45e85d069f4b717568542aeaa7858095b179678e`
  - `fresh-gate: base d7d4167777fe24e38b55bb63983b3a9033e72822`
  - `fresh-gate: checks passed (4823 files hashed in 1.37 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - every one of the eight steps `EXIT=0` (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests, api-surface);
  - stderr empty, exit 0, and the literal last line `GATE EXIT=0`.
- `$fg` was removed afterwards.
- **The worktree.** Before this verdict's commit it was at `45e85d069f4b717568542aeaa7858095b179678e` on `card/v3-S-EPSILON`, and clean. That commit is one commit on d7d41677 and changes only `nucleus/v3/docs/S-STORE.md` (+13/−7, sha256 `7e2d901b7bd3ebcf11154b71547e6f0ab14b19537cd291ac88853a67ab45c481`).

## Bytes, scope and names

- **Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` lists only `nucleus/v3/docs/S-STORE.md`, which is the card's Touch only.
- **The twelve blocks.** Parsed by my own script from the card's Edits and applied in order to d7d41677's file (`<date>` = 2026-11-16). Each Old occurs exactly once at its turn, and the result equals the committed file byte for byte.
- **F4-1 to F4-10** were parsed separately from `cards/S-DELTA.local.md`'s "Appendix: PROPOSED for S-ALPHA-F4, not adopted". Old and New are byte-identical to the card's blocks; only the headings' tag differs. The appendix alone gives `e20216a4…292f`, as the card says.
- **Names**, by my own §15/§15.1 parser, on 7.14 against 7.15:
  - 47 invariants and 48 tests, both sets unchanged;
  - 225 → 226 mutants, the only new one being ROLLBACK-REFUSES-READ-ERROR, judged on A-7d;
  - no mutant's set of judging cards changes except the new one's;
  - no mutant judged on a Phase A row at 7.14 loses it, and Phase-B-only stays 24;
  - the only §15 row changed is I-FALLBACK (statement and mutant cell), and the only §15.1 row changed is `t_fallback`'s (form and mutant cell).
- **The card's Acceptance on the tip.** 24 of 24 pass under both locales (`gate.sh` excluded). The checker prints the card's three lines, ending `OK`.
- **Status line and Changelog.** E1 inserts the 7.15 clause before S-DELTA's and keeps every byte after it, and E2 puts the 7.15 entry on top and keeps the older entries byte for byte. Both are right for 7.15 as committed; RW-12 adds the rework's bullet.
- **The reverse.** Applying F4-10 to F4-1 inverted gives 7.14 outside line 3 and E2's entry, and inverting E1 and E2 too gives 7.14 exactly. Each of the card's ten restore or delete texts equals the block's Old, or the text the block added. Checked by script.

## The change as recovery design: every open, every fault

Notation: RW is a plain read-write open, RO a read-only open, and RB an explicit rollback naming meta `k`. "Previous image" is §1's: meta `k−1` valid and its free set read in step 2. The table gives 7.15 as committed, and the fault is persistent unless the row says otherwise.

| # | Where the read error strikes | Previous image; proof; load | RW | RO | RB | Right? |
|---|---|---|---|---|---|---|
| 1 | a page of image `k` read in step 2, in a slot only image `k` references | yes; proof holds | 58030, writes nothing | salvage from `k−1`: the full recovered state | retires `k`, opens from `k−1`, every acked commit | right |
| 2 | as 1 | yes; proof fails (WAL damage in meta `k−1`'s span, or a dropped sync) | 58030, writes nothing | salvage at `k−1`'s frontier; **the shortfall against meta `k` is not bound to be reported** | opens at the frontier, reports what it abandons | **RO report gap (O-3)** |
| 3 | as 1 | yes; a publication names checkpoint `k` (I-CKPT-DEP) | 58030 | salvage at the dependency cut, reported | XX001 at step 3b against meta `k−1`, before any write from it | right; **§14.2's F4-7 row says "an explicit rollback opens it"** |
| 4 | as 1 | no previous meta | 58030 | 58030 | **XX001 "image `k` failed, with no previous image"** | **RB code wrong (finding 2); §14.2 falls to "Otherwise: opens meta `k`"** |
| 5 | as 1 | meta `k−1`'s free set fails an integrity check | 58030 (the free set alone refuses nothing) | 58030 | **XX001**, though a later open from meta `k` may succeed | **RB code wrong** |
| 6 | as 1 | meta `k−1`'s free set meets a read error | 58030 | 58030 | 58030 | right |
| 7 | a leaf of image `k` that pass 2 replays into | yes; proof holds | 58030, after the fence and pass 2's evictions into slots neither image references | salvage from `k−1` | runs pass 2 from `k`, meets the error, falls back, retires `k` before its first write from `k−1` | **right as stated, but no test reads it: a build that accepts it only in step 2, or that falls back on its own in pass 2, passes every case (finding 3)** |
| 8 | as 7 | no previous image | 58030 | 58030 | **XX001** | **as 4** |
| 9 | meta `k−1`'s free set only | — | opens meta `k`, protecting every slot below `slot_count` | opens meta `k` | 55000 | right, unchanged from 7.14 (the read is not through image `k`'s references) |
| 10 | a page of image `k−1` during the fall-back (a second bad sector) | — | (RW never falls back on a read error; in an integrity fall-back: 58030) | 58030 | 58030; if met in pass 2 after the retire, meta `k` stays retired | right, derivable but **not stated** (O-2 (c)); residual (B2) below |
| 11 | a slot both images reference (a page unchanged since `k−1`) | — | 58030 | 58030 (as 10) | 58030 (as 10) | right; the **test fixture must not pick such a page** (G4) |
| 12 | the WAL, pass 1 from meta `k` | — | 58030 | 58030 | 58030 before its precondition can hold; never a rollback (PLAN-v3 §10 item 8) | right, unchanged |
| 13 | the WAL, pass 1 from meta `k−1` during the fall-back | — | — | 58030 | 58030 | right (item 2: as if meta `k−1` were newest) |
| 14 | a meta place (step 1) | — | the fence, unchanged | unchanged | the fence precedes the retire write | right, unchanged |
| 15 | a transient error on image `k` that clears | — | the next plain open opens meta `k` | — | an RB whose reads now succeed gets 55000 and abandons nothing | right |

No cell is unsafe under honoured syncs. Cells 2, 3, 4, 5, 7, 8 and 10 are wrong or unpinned as marked. RW-3, RW-6, RW-8 and RW-9 fix them, RW-4 states cell 10, and RW-6's fixture excludes cell 11.

### (A) Silent loss

**Under honoured syncs: no.**
- Meta `k` is the newest synced meta. By I-TWO-GEN, image `k−1` and the WAL from meta `k−1`'s `wal_start_lsn` are intact: every open since meta `k` was synced protected image `k−1`'s slots in step 2, a refused open wrote nothing into them, and the RO writes nothing.
- By I-ACK-PREFIX and I-REDO-COMPLETE ("for a walk from an older meta"), every acked `synchronous_commit=on` commit has durable records below the valid end. So pass 1 from meta `k−1` reaches meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi`, item 3's proof holds, and both opens give the committed state meta `k` would have given, with more versions, not fewer.
- Unlogged entries that image `k` holds and image `k−1` lacks belong to an unpublished load, which no acked commit names, or to a publication naming checkpoint `k`. That publication is the one honoured-sync divergence: the RB is refused with XX001 at step 3b against meta `k−1`, before it writes anything from meta `k−1` (no retire), and the RO serves the dependency cut and reports it.

**Under a device that drops syncs: only partly, until O-3.**
- The RB reports what it abandons: meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi` against the valid end and the frontier. That is bound for every rollback.
- The RO salvage is bound only to "report what cut it".
- Trace: checkpoint `k`'s step-1 sync drops WAL bytes in [meta `k−1`'s `wal_ckpt_end`, meta `k`'s `wal_start_lsn`), meta `k` lands, then image `k`'s root sector goes bad. Meta `k−1`'s own checks pass, the walk stops at the dropped bytes, and the salvage silently lacks commits that image `k` holds and every later one, intact records included.
- That is more than the accepted dropped-sync trade-off, which loses only the tail past the opened meta's `wal_ckpt_end`. And the shortfall is detectable here, since meta `k` reads.
- RW-8 binds the report, and the read-error case pins it.

### (B) A transient read error

- **What the precondition requires.** F4-2's precondition is evaluated by the rollback's own open: it re-reads image `k` in step 2 and in pass 2. If every read succeeds, image `k` opens and the rollback gets 55000 ("otherwise"), so a rollback never abandons an image that reads at that moment. "Persistent" is not, and cannot be, a precondition: a transient error that strikes again during the rollback is indistinguishable from a persistent one.
- **One fault loses nothing.** With one fault under honoured syncs, abandoning an image `k` that was in fact fine loses no commit. The proof holds, as in (A), so the rollback recovers the same committed state and gives up only GC progress.
- **Two faults can.** A loss needs a second fault, and both cases are operator-initiated and not silent:
  - **(B1)** WAL damage in meta `k−1`'s span below meta `k`'s `wal_ckpt_end`, or a dropped sync. The rollback waives the proof and gives up commits that image `k` held and a retry would have kept. It reports them after its retire, and, with RW-8, the read-only open shows the same shortfall before it.
  - **(B2)** A second bad sector in a slot only the path from meta `k−1` needs, met in pass 2 after the retire, when the pool is small enough that pass 2 evicts and so retires first. The open fails with 58030 and leaves meta `k−1` as the newest; if image `k`'s error was transient, a store that a retried plain open would have opened is now stuck on that sector. A read-only open first meets the same sector, since it replays the same span from meta `k−1` in the pool.
- **Ruling.** Sufficient as spec text. The guard against both is to open read-only first, which the rework makes informative. I recommend it to whatever exposes the rollback (plan note below), not as a spec rule.
- **The retire write after a read-error rollback** is the integrity case's write, in the same order: misplaced copies, `sync_all`, own parity, `sync_all`, all before the first write from meta `k−1`. Only the fence and pass 2's evictions from image `k` come before it, into slots neither image references.
  - A crash between the retire and the boot checkpoint: the next open selects meta `k−1`, with no previous meta. Its image is intact: step 2 of the rollback's open protected it, and afterwards it was the durable image. Its WAL from its own `wal_start_lsn` is intact, since nothing recycles before the boot checkpoint's step 6. So that open recovers the same state with no rollback option.
  - A crash between the two retire writes leaves meta `k` in its own parity with image `k` as it was, and the next open decides again.
  - **Safe.**

## The implementer's five findings: rulings

1. **I-RO-OPEN's exception list omits the case: real.**
   - §14.1's opening paragraph has the same list, "WAL loss, a splice, a missing start segment, an unmet checkpoint dependency and an unproven fall-back", and the implementer did not name it.
   - Both read as complete, and both now omit "a newest image that a read error stops at open, with a previous image". RW-7 and RW-10 fix them.
   - The case's tests live under I-FALLBACK (`t_fallback`'s read-error case, its read-only half), as the unproven fall-back's do, so no I-RO-OPEN test needs a read error.
2. **XX001 for a rollback after a read error with no previous image: real.** §9.2 and the SQLSTATE table define XX001 as "an integrity check failed", and none did. A later open may not meet the read error and may open meta `k` (cells 4, 5 and 8). RW-3 makes it 58030.
3. **The pass-2 half is untested: real.** It is stated behaviour, in F4-1 and F4-2, that no test and no mutant pins, and the card's claim that "D-2's rule is pinned by the test" holds only for step 2. RW-6 adds the pass-2 case and the fixture constraints, and RW-5 names the partial form of ROLLBACK-REFUSES-READ-ERROR that only that case catches.
4. **§14.2 has no row for these cases: real, and wider than stated.**
   - The read-error cases with no previous image, or with image `k−1` failing or meeting a read error, fall to "Otherwise: opens meta `k`".
   - F4-7's own row says "an explicit rollback opens it" also where a check against meta `k−1` refuses (I-CKPT-DEP, meta `k−1`'s coverage). The integrity rows have a separate "a check against meta `k−1` refuses" row for that.
   - RW-9 adds both rows. The preamble's "they rule" does not excuse a summary row that states the opposite outcome.
5. **Minor tensions.**
   - (a) **Real.** I-FALLBACK's first clause is now literally false, and so are §2.2's summary and §4.4's "Neither is an error, unless image `k` fails at open". RW-11, RW-1 and RW-2 fix them.
   - (b) **Not a defect.** F4-5, F4-6 and F4-7 say "salvage" explicitly. When the proof holds, the salvage's state equals the proven state. With RW-8 the salvage reports against meta `k` as item 5 does, so "as they accept an integrity failure" holds in substance. No change.
   - (c) **Real while O-3 is open.** RW-8 makes E2's "each report what they serve" true, and RW-12 records it.

**The implementer's reading of the open points.**
- (1) A read error in image `k−1`: agreed, derivable but unstated. The previous image is defined relative to meta `k` (§1), the open rule's other-parity candidate is meta `k` (or zeros after the retire), never meta `k−2`, and I-FALLBACK says "never two generations". But F4-5 invites a recursive reading, so it is stated (RW-4).
- (2) Agreed (finding 2).
- (3) Agreed (O-3).
- (4) The hook: a defect in the spec's fixture text, not just detail for A-7d's card.
  - "One page of image `k`" naturally includes a page in a slot that image `k−1` shares. On it, a correct build fails all three opens with 58030 (cell 11), so the case cannot pass.
  - A hook keyed on the slot for ever would fail the rollback's later reads of the page written there; the text's "page of image `k`" already excludes that, but stating it costs nothing.
  - Since F4-4's text is rewritten for finding 3 anyway, both constraints go into RW-6, and A-7d's card need not carry them.

## The card's four open points: rulings

- **O-1, the tag "(card S-ALPHA-F4)": keep, as recommended.** E1 and E2 map card S-EPSILON to S-ALPHA-F4. The rework adds no tag, so Acceptance line 20's count stays 4.
- **O-2: adopt all three.**
  - (a) is RW-3. The wording is adjusted to "too, never XX001", with the reason "a later open may not meet it", so that the case where meta `k−1`'s free set failed an integrity check is covered.
  - (b) is RW-9, corrected and extended. It adds the "check against meta `k−1` refuses" row, and gives XX001 where image `k−1` fails an integrity check (item 2) and 58030 where it meets a read error.
  - (c) is RW-4, worded "not even to serve salvage".
- **O-3: adopt** (RW-8). The wording is changed so that each reported item is pinned by a test: the values for every salvage from meta `k−1` (item 5 already for the unproven case; the read-error case's new assertion), and the read error when one stopped image `k`.
- **O-4: adopt** (RW-1). The summary already omitted the read-only unproven salvage at 7.14. With F4 it restricts the one-generation fall-back to "fails at open", which the read-error rollback and salvage contradict. The New text covers both.

## Tests on A-7d's row, re-walked on the reworked text

**The reworked read-error case**, a correct build. The fixture is the proven one. A hook fails every read of one slot that image `k` references and image `k−1` does not, until that slot is next written.
- **The step-2 case** (a MapLeaf, a FreeSet page or a directory page in that slot):
  - *RW.* Step 2 meets the read error, still reads meta `k−1`'s free set, and fails with 58030. No write, so no fence and no eviction. **Passes.**
  - *RO.* Salvage from meta `k−1`. Item 2's checks pass, and pass 1 and pass 2 run from meta `k−1`'s `wal_start_lsn`, so every acked commit is served. The open reports the read error and the values against meta `k` (the proof holds, so no shortfall), and every file is byte-identical. **Passes.**
  - *RB.* The precondition holds, since the hook fails the read again. Items 1 and 2 pass, and the retire comes before the first write from meta `k−1`. Pass 2 or the boot checkpoint may reuse the faulted slot, after which the hook no longer fails it. Every acked commit is recovered. **Passes.**
- **The pass-2 case** (a leaf pass 2 replays into, in that slot):
  - *RW.* Pass 2 may evict, into slots neither image references, before it meets the error. Then it fails with 58030 and writes nothing more. **Passes.**
  - *RO.* Discards image `k`'s pool state and salvages from meta `k−1` as above. **Passes.**
  - *RB.* Runs pass 2 from image `k`, meets the error, falls back, retires, and recovers every acked commit. **Passes.**
- **With both copies of meta `k−1` lost** (no previous meta, as in the two-generations case), the step-2 case: RW 58030, RO 58030 (no previous image, and no salvage from meta `k−3`), and RB 58030 (RW-3). Nothing is written. **Passes.**

**The mutants and wrong builds it must catch:**
- ROLLBACK-REFUSES-READ-ERROR in full fails both cases (RB 55000, RO 58030). Its step-2-only form fails the pass-2 case.
- A build that falls back on its own, in step 2 or in pass 2, fails RW's 58030 and the write hook.
- A build that answers the rollback with XX001, or salvages from meta `k−3`, fails the no-previous-meta variant.
- A salvage that omits the report fails the RO assertion.
- No case needs code outside A-7d's closure: the hook is an `Env` wrapper over A-1b's SimEnv, and the row's Needs are A-7c, A-8 and A-1b.

**Every existing case and outcome is unchanged:**
- `t_fallback`:
  - *proven* (four cases, FALLBACK-OFF and RO-WRITES): integrity failures. RW-3 and RW-8 apply only to read errors and to salvage, and a proven read-only open is not salvage.
  - *unread variant*: XX001, or 58030 for the free set's read error. RW-3 covers only a rollback on a read error.
  - *unproven*: FALLBACK-NO-PROOF and ROLLBACK-SKIPS-PREV-CHECKS caught; "reports the shortfall" is RW-8's values; an RB on a store whose image opens still gets 55000.
  - *two generations* (XX001 "with no previous image", FALLBACK-TWO-GEN), *retire* and its *misplaced variant* (RETIRE-SKIP, RETIRE-ONE-COPY): no sentence they rest on changes.
- `t_two_gen` (a)–(c), including (c)'s 58030 run: image `k` reads, so RW-2 changes nothing there.
- `t_meta_copies`' read-error forms and fence variants, `t_ro_open`, `t_ro_open_disk_full`, `t_wal_loss_salvage`, `t_wal_splice`'s read-only variant and `t_ckpt_dep`'s read-only form: none has a read error through image `k`'s references, and none is a salvage from meta `k−1`.
- The checker still prints 158 rows, 226 mutants, 24 Phase B only, 47 invariants and 48 tests, then `OK`.

## The rework: 12 blocks

Apply on `card/v3-S-EPSILON` on top of 45e85d06, to `nucleus/v3/docs/S-STORE.md`, in order RW-1 to RW-12. Each Old occurs exactly once at its turn (checked by script), the draft number stays 7.15, and nothing else changes. The fences here are four tildes. The result's sha256 is `a1376c9491cda2aa987a2736825e313318246aa97fba126181b021731bfa700f`. The tested script and data are in `~/.cache/nv3-review/gate-S-EPSILON/rework/` (`rw_blocks.py`, `apply.py`); the blocks below are authoritative.

### RW-1. §2.2, "One fall-back, and only one": the summary names the read-error case (O-4; finding 5 (a)).
File: docs/S-STORE.md

Old:
~~~~text
except as §14 step 4a allows: one generation, to the previous meta, when the newest image fails at open, with proof that the log is complete or by explicit rollback.
~~~~
New:
~~~~text
except as §14 step 4a allows, one generation, to the previous meta: when the newest image fails at open, with proof that the log is complete; and, when it fails at open and the proof fails, or when a read error (58030) stops it, by explicit rollback or in a read-only open's salvage.
~~~~

### RW-2. §4.4, I-TWO-GEN: when the lack of a previous image is an error (this gate; finding 5 (a)).
File: docs/S-STORE.md

Old:
~~~~text
Neither is an error, unless image `k` fails at open (§14 step 4a item 1).
~~~~
New:
~~~~text
Neither is an error, unless image `k` fails at open or a read error (58030) stops it (§14 step 4a item 1).
~~~~

### RW-3. §14 step 4a item 1: a rollback on a read error with no previous image (finding 2; O-2 (a)).
File: docs/S-STORE.md

Old:
~~~~text
or with **58030** when that read failed with a read error (58030), which a later open may not meet: it may then read the free set and fall back.
~~~~
New:
~~~~text
or with **58030** when that read failed with a read error (58030), which a later open may not meet: it may then read the free set and fall back. An explicit rollback that proceeds on a read error through image `k`'s references (below) and finds no previous image fails with **58030** too, never XX001: a read error is not evidence of damage, and a later open may not meet it.
~~~~

### RW-4. §14 step 4a item 2: a read error in image k−1 (O-2 (c); the implementer's point (1)).
File: docs/S-STORE.md

Old:
~~~~text
An integrity failure in image `k−1` is XX001 in both opens: no open falls back two generations.
~~~~
New:
~~~~text
An integrity failure in image `k−1` is XX001, and a read error (58030) there is 58030, in both opens and for an explicit rollback: no open falls back two generations, not even to serve salvage.
~~~~

### RW-5. §14 step 4a, the mutants: ROLLBACK-REFUSES-READ-ERROR's step-2-only form (finding 3).
File: docs/S-STORE.md

Old:
~~~~text
   - **ROLLBACK-REFUSES-READ-ERROR**: an explicit rollback, or a read-only open, refuses a newest image that a read error stops at open, as at draft 7.14 (card S-ALPHA-F4).
~~~~
New:
~~~~text
   - **ROLLBACK-REFUSES-READ-ERROR**: an explicit rollback, or a read-only open, refuses a newest image that a read error stops at open, as at draft 7.14, or accepts the read error only in step 2, not in pass 2 (card S-ALPHA-F4).
~~~~

### RW-6. §14 step 4a, `t_fallback`: the read-error case, in step 2 and in pass 2, on a slot only image k references (finding 3; G4 / the implementer's point (4); O-3's pin; finding 2's pin).
File: docs/S-STORE.md

Old:
~~~~text
   - *Read error* (card S-ALPHA-F4): as proven, but a hook fails every read of one page of image `k` that step 2 reads with a read error (58030), a persistent fault. The read-write open fails with 58030 and writes nothing; the read-only open serves meta `k−1`'s recovered state as salvage and leaves every file byte-identical; an explicit rollback naming meta `k` opens read-write from meta `k−1` and recovers every acked commit. ROLLBACK-REFUSES-READ-ERROR fails it: the rollback is refused with 55000, and the read-only open fails with 58030.
~~~~
New:
~~~~text
   - *Read error* (card S-ALPHA-F4): as proven, but a hook fails every read of one slot with a read error (58030), a persistent fault until that slot is next written: a slot that image `k` references and image `k−1` does not, holding, in one case, a page that step 2 reads (a MapLeaf, a FreeSet page or a directory page) and, in another, a leaf pass 2 replays into. The read-write open fails with 58030 and writes nothing, or, in the pass-2 case, nothing but the evictions pass 2 made before the read error, each into a slot that neither image `k` nor image `k−1` references; the read-only open serves meta `k−1`'s recovered state as salvage, with every acked commit, reports the read error and the valid end and largest `commit_ts` against meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi`, and leaves every file byte-identical; an explicit rollback naming meta `k` opens read-write from meta `k−1` and recovers every acked commit. With both copies of meta `k−1` lost too, so that there is no previous meta, the step-2 case's three opens each fail with 58030 and write nothing. ROLLBACK-REFUSES-READ-ERROR fails both cases: the rollback is refused with 55000, and the read-only open fails with 58030; its step-2-only form fails the pass-2 case.
~~~~

### RW-7. §14.1, the opening paragraph: the cases a read-only open salvages (finding 1 (this gate's half)).
File: docs/S-STORE.md

Old:
~~~~text
WAL loss, a splice, a missing start segment, an unmet checkpoint dependency and an unproven fall-back (below; §14 steps 3, 3a, 3b and 4a).
~~~~
New:
~~~~text
WAL loss, a splice, a missing start segment, an unmet checkpoint dependency, an unproven fall-back, and a newest image that a read error (58030) stops at open when there is a previous image (below; §14 steps 3, 3a, 3b and 4a).
~~~~

### RW-8. §14.1, salvage: what a salvage from meta k−1 reports (O-3; finding 5 (c)).
File: docs/S-STORE.md

Old:
~~~~text
Each serves the ts-prefix at its frontier, by the argument above, and reports what cut it.
~~~~
New:
~~~~text
Each serves the ts-prefix at its frontier, by the argument above, and reports what cut it; a salvage from meta `k−1` also reports, as §14 step 4a item 5 does, the valid end and the largest `commit_ts` against meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi`, and, when a read error stopped image `k`, that read error.
~~~~

### RW-9. §14.2: the read-error case's refusal rows (finding 4; O-2 (b)).
File: docs/S-STORE.md

Old:
~~~~text
| The newest image meets a read error (58030) at open, with a previous image | 58030, nothing written beyond the fence and step 4a's pass-2 evictions; an explicit rollback opens it | salvage from meta `k−1` |
~~~~
New:
~~~~text
| The newest image meets a read error (58030) at open, with a previous image | 58030, nothing written beyond the fence and step 4a's pass-2 evictions; an explicit rollback opens it | salvage from meta `k−1` |
| The same, and a check against meta `k−1` refuses | 58030, nothing written beyond the fence and step 4a's pass-2 evictions; an explicit rollback gets that check's refusal | that check's salvage |
| The same, with no previous image (no previous meta, or its free set unread at step 2), or image `k−1` failing or meeting a read error too | 58030, nothing written beyond the fence and step 4a's pass-2 evictions; an explicit rollback fails with 58030, or XX001 when image `k−1` fails | 58030, or XX001 when image `k−1` fails |
~~~~

### RW-10. §15, I-RO-OPEN: the case among the exceptions (finding 1).
File: docs/S-STORE.md

Old:
~~~~text
an unmet checkpoint dependency (step 3b) or an unproven fall-back (step 4a), where the read-write open fails
~~~~
New:
~~~~text
an unmet checkpoint dependency (step 3b), an unproven fall-back (step 4a) or a newest image that a read error (58030) stops at open when there is a previous image (step 4a), where the read-write open fails
~~~~

### RW-11. §15, I-FALLBACK: only the automatic fall-back is integrity-only (finding 5 (a)).
File: docs/S-STORE.md

Old:
~~~~text
| I-FALLBACK | §14 step 4a, §14.1: only a newest image that fails an integrity check at open falls back, only to the previous meta, never two generations from the meta the open selects, with every check against it;
~~~~
New:
~~~~text
| I-FALLBACK | §14 step 4a, §14.1: an open falls back only to the previous meta, never two generations from the meta the open selects, with every check against it, and on its own only when the newest image fails an integrity check at open;
~~~~

### RW-12. Changelog, draft 7.15: what the rework adds (the Changelog for RW-1 to RW-11).
File: docs/S-STORE.md

Old:
~~~~text
Test `t_fallback` gains its read-error case; mutant ROLLBACK-REFUSES-READ-ERROR (A-7d).
  - **Counts.** §15: 47 rows, 226 mutants, 48 tests.
~~~~
New:
~~~~text
Test `t_fallback` gains its read-error case; mutant ROLLBACK-REFUSES-READ-ERROR (A-7d).
  - **What the case also needs** (this card's first gate, `verdicts/S-EPSILON.md`). An explicit rollback that proceeds on a read error and finds no previous image fails with 58030, not XX001, and a read error in image `k−1` is 58030 in every open, with no salvage from an older meta (§14 step 4a items 1 and 2). A salvage from meta `k−1` reports the valid end and largest `commit_ts` against meta `k`'s `wal_ckpt_end` and `ts_ckpt_hi`, and the read error that stopped image `k`, so that what it lacks against meta `k` under a device that drops syncs is reported (§14.1). §14.1's opening list and I-RO-OPEN name the case among those a read-only open salvages; §2.2's summary and I-FALLBACK's statement say that only the automatic fall-back is integrity-only, and §4.4 that a missing previous image is an error after a read error too; §14.2 gains the case's refusal rows. `t_fallback`'s read-error case faults a slot that only image `k` references, until that slot is rewritten, in step 2 and in pass 2, and once with no previous meta; ROLLBACK-REFUSES-READ-ERROR gains its step-2-only form.
  - **Counts.** §15: 47 rows, 226 mutants, 48 tests.
~~~~

## Acceptance changes

Lines 11, 12, 16, 21 and 22 of the card's Acceptance list are replaced, and 10 lines are inserted before the `gate.sh` line, which stays last (it becomes line 35). Lines 1–24 keep their numbers, so the card's references to line 23 (the checker), lines 21–22 (the diff) and line 24 (the file list) still hold. No command contains a backtick. On the reworked scratch commit the 34 commands before `gate.sh` pass under the default locale and `LC_ALL=C`, and the checker prints `OK`. On 45e85d06 each of the 15 new or replaced lines fails. Omitting any one block fails its own line (RW-1: 25, 26; RW-2: 26, 27; RW-3: 28; RW-4: 26, 29; RW-5: 11; RW-6: 12; RW-7: 26, 30; RW-8: 31; RW-9: 16; RW-10: 26, 32; RW-11: 26, 33; RW-12: 34), besides line 22 and, where the line count moves, line 21.

**Line 11 (the mutant, RW-5) becomes:**

~~~~text
- `cd nucleus/v3 && grep -A1 '^   - \*\*ROLLBACK-SKIPS-PREV-CHECKS\*\*: ' docs/S-STORE.md | tail -1 | grep -q '^   - \*\*ROLLBACK-REFUSES-READ-ERROR\*\*: an explicit rollback, or a read-only open, refuses a newest image that a read error stops at open, as at draft 7\.14, or accepts the read error only in step 2, not in pass 2 (card S-ALPHA-F4)\.$'`
~~~~

**Line 12 (the case, RW-6) becomes:**

~~~~text
- `cd nucleus/v3 && grep -n '^   - \*Read error\* (card S-ALPHA-F4): as proven, but a hook fails every read of one slot with a read error (58030), a persistent fault until that slot is next written: a slot that image .k. references and image .k−1. does not, holding, in one case, a page that step 2 reads (a MapLeaf, a FreeSet page or a directory page) and, in another, a leaf pass 2 replays into\. The read-write open fails with 58030 and writes nothing, or, in the pass-2 case, nothing but the evictions pass 2 made before the read error, each into a slot that neither image .k. nor image .k−1. references; the read-only open serves meta .k−1..s recovered state as salvage, with every acked commit, reports the read error and the valid end and largest .commit_ts. against meta .k..s .wal_ckpt_end. and .ts_ckpt_hi., and leaves every file byte-identical; an explicit rollback naming meta .k. opens read-write from meta .k−1. and recovers every acked commit\. With both copies of meta .k−1. lost too, so that there is no previous meta, the step-2 case.s three opens each fail with 58030 and write nothing\. ROLLBACK-REFUSES-READ-ERROR fails both cases: the rollback is refused with 55000, and the read-only open fails with 58030; its step-2-only form fails the pass-2 case\.$' docs/S-STORE.md`
~~~~

**Line 16 (§14.2, RW-9) becomes:**

~~~~text
- `cd nucleus/v3 && grep -A3 '^| The newest image meets a read error (58030) at open, with a previous image | 58030, nothing written beyond the fence and step 4a.s pass-2 evictions; an explicit rollback opens it | salvage from meta .k−1. |$' docs/S-STORE.md | tail -n +2 | tr '\n' '~' | grep -q '^| The same, and a check against meta .k−1. refuses | 58030, nothing written beyond the fence and step 4a.s pass-2 evictions; an explicit rollback gets that check.s refusal | that check.s salvage |~| The same, with no previous image (no previous meta, or its free set unread at step 2), or image .k−1. failing or meeting a read error too | 58030, nothing written beyond the fence and step 4a.s pass-2 evictions; an explicit rollback fails with 58030, or XX001 when image .k−1. fails | 58030, or XX001 when image .k−1. fails |~| An explicit rollback whose precondition does not hold | '`
~~~~

**Line 21 (the diff size) becomes:**

~~~~text
- `cd nucleus/v3 && test "$(git diff --numstat nucleus/v3-scaffold...HEAD -- docs/S-STORE.md | cut -f1,2 | tr '\t' ' ')" = '22 13'`
~~~~

**Line 22 (the diff bytes, dates normalised) becomes:**

~~~~text
- `cd nucleus/v3 && test "$(git diff --no-color --diff-algorithm=myers -U0 nucleus/v3-scaffold...HEAD -- docs/S-STORE.md | tail -n +5 | grep -v '^@@' | sed 's/20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/<date>/g' | sha256sum | cut -c1-64)" = '8148bba2030e4785f8869e0138f1ed490e86b51811e3b90a406d585781d41091'`
~~~~

**Inserted before `gate.sh`, as lines 25–34** (RW-1, the removed-text check for RW-1, RW-2, RW-4, RW-7, RW-10 and RW-11, then RW-2, RW-3, RW-4, RW-7, RW-8, RW-10, RW-11, RW-12):

~~~~text
- `cd nucleus/v3 && grep -n '\*\*One fall-back, and only one\.\*\* An open never selects an older meta than the newest, except as §14 step 4a allows, one generation, to the previous meta: when the newest image fails at open, with proof that the log is complete; and, when it fails at open and the proof fails, or when a read error (58030) stops it, by explicit rollback or in a read-only open.s salvage\. After meta' docs/S-STORE.md`
- `cd nucleus/v3 && ! sed -n '4,/^## Changelog/p' docs/S-STORE.md | grep -n 'except as §14 step 4a allows: one generation\|Neither is an error, unless image .k. fails at open (§14\|is XX001 in both opens: no open falls back\|an unmet checkpoint dependency and an unproven fall-back (below;\|an unmet checkpoint dependency (step 3b) or an unproven fall-back (step 4a), where\|only a newest image that fails an integrity check at open falls back, only'`
- `cd nucleus/v3 && grep -n 'Neither is an error, unless image .k. fails at open or a read error (58030) stops it (§14 step 4a item 1)\. Two states lie outside' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'or with \*\*58030\*\* when that read failed with a read error (58030), which a later open may not meet: it may then read the free set and fall back\. An explicit rollback that proceeds on a read error through image .k..s references (below) and finds no previous image fails with \*\*58030\*\* too, never XX001: a read error is not evidence of damage, and a later open may not meet it\. Image .k−1. is as it was' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'An integrity failure in image .k−1. is XX001, and a read error (58030) there is 58030, in both opens and for an explicit rollback: no open falls back two generations, not even to serve salvage\. Pass-2 evictions made from image' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'A read-only open recovers exactly the state a read-write open recovers, but keeps it in memory and writes nothing, except where a read-write open fails and a read-only open serves salvage instead: WAL loss, a splice, a missing start segment, an unmet checkpoint dependency, an unproven fall-back, and a newest image that a read error (58030) stops at open when there is a previous image (below; §14 steps 3, 3a, 3b and 4a)\.$' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'Where several cuts apply, the lowest one does\. Each serves the ts-prefix at its frontier, by the argument above, and reports what cut it; a salvage from meta .k−1. also reports, as §14 step 4a item 5 does, the valid end and the largest .commit_ts. against meta .k..s .wal_ckpt_end. and .ts_ckpt_hi., and, when a read error stopped image .k., that read error\. A proven fall-back is not salvage' docs/S-STORE.md`
- `cd nucleus/v3 && sed -n '/^## 15\. /,/^### 15\.1 /p' docs/S-STORE.md | grep '^| I-RO-OPEN | ' | grep -q 'except under WAL loss or a splice (§14 step 3a), a missing start segment (step 3), an unmet checkpoint dependency (step 3b), an unproven fall-back (step 4a) or a newest image that a read error (58030) stops at open when there is a previous image (step 4a), where the read-write open fails and the read-only open serves the salvage state at the recovered frontier (§14\.1)\.'`
- `cd nucleus/v3 && sed -n '/^## 15\. /,/^### 15\.1 /p' docs/S-STORE.md | grep -q '^| I-FALLBACK | §14 step 4a, §14\.1: an open falls back only to the previous meta, never two generations from the meta the open selects, with every check against it, and on its own only when the newest image fails an integrity check at open; a read-write open falls back only with proof'`
- `cd nucleus/v3 && grep -A1 '^  - \*\*A read error under the newest image\*\* (S-ALPHA-F4; I-FALLBACK)\. ' docs/S-STORE.md | tail -1 | grep -q '^  - \*\*What the case also needs\*\* (this card.s first gate, .verdicts/S-EPSILON\.md.)\. An explicit rollback that proceeds on a read error and finds no previous image fails with 58030, not XX001, and a read error in image .k−1. is 58030 in every open, with no salvage from an older meta (§14 step 4a items 1 and 2)\. A salvage from meta .k−1. reports the valid end and largest .commit_ts. against meta .k..s .wal_ckpt_end. and .ts_ckpt_hi., and the read error that stopped image .k., so that what it lacks against meta .k. under a device that drops syncs is reported (§14\.1)\. §14\.1.s opening list and I-RO-OPEN name the case among those a read-only open salvages; §2\.2.s summary and I-FALLBACK.s statement say that only the automatic fall-back is integrity-only, and §4\.4 that a missing previous image is an error after a read error too; §14\.2 gains the case.s refusal rows\. .t_fallback..s read-error case faults a slot that only image .k. references, until that slot is rewritten, in step 2 and in pass 2, and once with no previous meta; ROLLBACK-REFUSES-READ-ERROR gains its step-2-only form\.$'`
~~~~

## Card text the rework must update

The blocks are the spec change, and the card's prose must follow them:
- **Goal, "The fix", "Never weakened", D-4.** The case is completed by RW-1 to RW-12: O-2, O-3 and O-4 are adopted, and O-1 is kept. "Each reports what it serves" is now bound (RW-8).
- **The draft target.** The diff is 22 lines added and 13 removed; the counts are unchanged.
- **Edits.** Add "3. then RW-1 to RW-12 (verdict `verdicts/S-EPSILON.md`)".
- **Touch only, Limits 2–5.** By section: §2.2 RW-1; §4.4 RW-2; §14 step 4a RW-3 to RW-6 (items 1 and 2 now change); §14.1 RW-7, RW-8; §14.2 RW-9; §15 RW-10, RW-11; Changelog RW-12. Limit 5's "none of §2.2, … step 4a items 1–5 … changes" is amended to match.
- **Mutation evidence.** ROLLBACK-REFUSES-READ-ERROR's step-2-only form, and the pass-2 case that catches it. D-2 is now pinned in step 2 and in pass 2.
- **"For the gate".**
  - Item 1: the fixture points are now spec text.
  - Item 5: the salvage reports against meta `k`.
  - Item 6: stated (RW-4).
  - Item 7: resolved (RW-3, RW-9).
- **"For the plan card" item 3.** The fixture pins are spec text now; keep "step 2 goes on to read meta `k−1`'s free set" and the operator report for the rollback.
- **"If the owner withdraws the yes".** The reversal applies RW-11 to RW-1 inverted first; RW-12 sits in the 7.15 Changelog entry, which a reversal keeps, as it keeps E2's. Then it applies F4-10 to F4-1 inverted. Checked by script on the scratch result: 7.14 byte for byte outside the status line and the 7.15 entry.

## Owner points, in plain words

- **What users gain.** After one unreadable sector under the newest checkpoint (its page map, free-space map or tree directory), the database is no longer stuck:
  - an operator can open it read-only and read or export everything, from the previous checkpoint plus the log;
  - or the operator can roll back one checkpoint and keep running read-write.

  With a disk that keeps what it says it synced, neither path loses an acknowledged commit. The automatic open still refuses, so nothing changes unless an operator asks.
- **The risk.**
  - It needs the operator to roll back and a second fault to happen.
  - If the sector error was temporary and the log is also damaged, or the disk dropped a sync, a rollback gives up commits that a later retry would have kept.
  - If the previous checkpoint has a bad sector of its own, a rollback can leave the store refusing to open until the disk is repaired or a backup is used.
  - Neither is silent: the rollback reports what it gave up, or fails with 58030.
  - After this rework, a read-only open shows both beforehand. "Open read-only first, then roll back" avoids them, and I recommend that whatever tool exposes the rollback do that.
  - This is a trade-off only in those two-fault cases. S-ALPHA-F4 is not one of the four items the orchestrator's RECORD and follow-up question name as trade-offs, so the owner may want to know about it when the conditional "yes" is confirmed.
- **Reversible.** Until A-7d is cut, withdrawing costs one spec card. The card's reverse section, extended by the rework's inverse, gives back 7.14 byte for byte outside the status line and the Changelog (checked by script). After A-7d, it also costs A-7d's code and one test case.

## What R-1-SPEC must change once this card merges (S-EPSILON-F1)

R-1-SPEC (written on 7.14 and also planned as "7.15") becomes S-STORE **draft 7.16** on S-EPSILON's 7.15. I checked this by applying its 25 S-STORE blocks to both 45e85d06's file and the reworked file, and by running its checker on the result.
1. **The number, 7.15 → 7.16**, wherever it names its own S-STORE draft:
   - the title (line 1: "with S-STORE draft 7.16 on S-EPSILON's 7.15");
   - R-1-memory's status line (line 52) and Changelog (line 541);
   - line 243 ("as draft 7.16 words it": R-1-SPEC's own E3 changes that premise);
   - the heading at line 544 ("draft 7.15 → 7.16");
   - E1's New ("Status: draft 7.16 (card R-1-SPEC, ");
   - E25's heading and New ("- **Draft 7.16** (");
   - T9, C-T0 8.8's entry ("with S-STORE draft 7.16");
   - Work 2, 3 and 4 (lines 971, 992, 996);
   - Mutation evidence (line 1023, "at 7.16");
   - line 1027's checker output, "232 mutants" → "233 mutants".
2. **The name.** "card S-ALPHA-F4" or "S-ALPHA-F4's draft" becomes "card S-EPSILON" or "S-EPSILON's draft 7.15" (lines 4, 545, 971 and 992). Line 4's Base names the merged file's sha256.
3. **Line 545 says "E1 and E24".** It means E1 and E25: E24 is §19's T8, and lines 971 and 992 already say E25.
4. **No re-anchor is needed.** E1's Old ("Status: draft 7.1") and E25's Old ("## Changelog\n\n- **Draft 7.1") still occur once on 7.15. E1's New ends "on draft 7.1", so the line then reads "… on draft 7.15 (card S-EPSILON, …".
5. **Its Acceptance line** `grep -q ') on draft 7\.1[45] (card S-\(DELTA\|ALPHA-F4\), '` still passes on 7.16, but only by matching S-DELTA's older clause further along the line. Make it `') on draft 7\.15 (card S-EPSILON, '`.
6. **Its S-STORE checker** compares against 983a227b's file and requires the new mutants to be exactly its seven. On 7.15 plus its blocks it prints `160 rows; 233 mutants; 24 judged only on Phase B cards; 47 invariants; 50 tests` and then fails with `new mutants [... 'ROLLBACK-REFUSES-READ-ERROR']`. Compare against the S-EPSILON merge instead, or allow that name.
7. **Nothing else collides.** All 25 S-STORE Olds (E1–E25) occur exactly once in 45e85d06's file and in the reworked file. No RW block touches a paragraph R-1-SPEC edits (§5, §15's stand-ins, I-POOL-BOUND, I-CKPT-AT-T, the `t_pool_minimum` and `t_epoch_reserve_two_exhausters` rows, §17–§19). Its C-T0 and README edits are untouched by this card.

If R-1-SPEC merges first instead, the mirror applies: this card becomes 7.16, E1's and E2's Olds re-anchor on R-1-SPEC's head, its F4 and RW Olds are re-checked (none overlaps R-1-SPEC's), and lines 2, 21 and 22 change with R-1-SPEC's text.

## What the plan card must change (this verdict edits no plan row)

- **QUEUE.** S-EPSILON goes back to REWORK, with the 12 blocks. S-ALPHA-F4 and S-EPSILON close only at the merge.
- **§9, row S-ALPHA-F4.** It adopts "the appendix's ten blocks and the gate's twelve rework blocks (`verdicts/S-EPSILON.md`)".
- **§9 A-7d.**
  - The read-error case runs in step 2 and in pass 2, and once with no previous meta.
  - Step 2 goes on to read meta `k−1`'s free set after a read error, as after an integrity failure.
  - The rollback's operator report says whether an integrity failure or a read error let it through; the salvage's report is now spec (RW-8).
  - The count "34 rows" is unchanged.
- **§10 item 22 (O-1).** Whatever exposes the explicit rollback should run, or show, the read-only open's report first. It shows the shortfall against meta `k` and meets any read error on the path from meta `k−1` before meta `k` is retired ((B1), (B2) above).
- **§3.3's pointer** (PL-13-F1 (f)). The precondition includes a read error through the newest image's references, as the card says.
- **The R-1 row.** S-EPSILON-F1.

## Follow-ups

- **S-EPSILON-F1: R-1-SPEC on S-EPSILON's 7.15.**
  - Must precede A-3b (and A-7a), R-1-SPEC's own due points, and applies only if this card merges first.
  - R-1-SPEC becomes S-STORE draft 7.16 with the changes listed above: the number in about fifteen places, "card S-EPSILON" for "card S-ALPHA-F4", line 545's E24 → E25, its status-chain Acceptance line, and its checker's base or allowed set.
  - No S-STORE anchor of R-1-SPEC collides with this card, before or after the rework.
