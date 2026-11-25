Card: S-MU
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 47b63613-c037-47c2-bf6c-ecae7a0e65c3, dispatched from T3 thread 45584af2-e651-490e-862c-3402320cffa3 (an independent P gate: this agent wrote neither the card S-MU nor its implementation b1ecf549, and is not the orchestrator)
Date: 2026-11-24
Gate-exit: GATE EXIT=0
Commit: b1ecf549665c0b9edc51fcbc2361aef65e356148
Mutants: spec text, none in code; the six named spec mutants are walked on the text, and their text forms were committed one at a time in a throwaway clone and judged by Acceptance lines 1 to 67 in both locales. MAP-INNER-FROM-CURRENT -> lines 28, 57, 60; IMAGE-REF-NOT-SET -> 28, 60; MAPLEAF-HOME-LEAKED -> 12, 60; LEVEL-IMAGE-REF-STALE -> 10, 57, 60; DIRTY-MAPLEAF-UNDER-P -> 18, 60; DIRTY-MAPLEAF-PIN-DROPPED -> 18, 60; pre-gate O10 (R-1 Mp bound doubled) -> 66; pre-gate P4 / card plant (I-CKPT-AT-T "a coherent cut") -> 60; own X1 (source chain's last step without the image ref) -> 28, 57, 60; own X2 (an epoch copy holds a pin) -> 9, 60; own X3 (R-1 server R_MAP 243,422) -> 63, 66; gate's spec mutant MAPLEAF-CLEAN-NOT-COPIED (F-1) -> UNCAUGHT by every test the draft names
Followups: none (the rework carries R-1 to R-3 and the card-text items; S-MU-F1 is named below for what may ride later)

## Verdict in brief

**REWORK** on `b1ecf549665c0b9edc51fcbc2361aef65e356148` (S-STORE draft 7.20 → 7.21, R-1-memory draft 1 → 2).

- **SF-C1 is real, and silent.** Derived from the 7.20 text (`git show c81e84a1:nucleus/v3/docs/S-STORE.md`), not from the card: see "SF-C1 on 7.20".
- **The fix is the right design, and its explicit rules close SF-C1.** Image refs make image `k+1` a function of image `k` and the epoch's writes. I found no new path through the rules 7.21 states, and no legitimate state newly refused.
- **One unchanged sentence still defines "the image map" as 7.20 built it (pre-gate F-1, confirmed).** §5.6's copy test is phrased in that term. Read literally, a clean MapLeaf of content epoch `k` gets no epoch copy. Two of this draft's own new rules, U6's freeing and U22's last source step, then turn a path that was safe at 7.20 into a silent loss of acked commits. The next card, A-3c-C (its Work 6.2), already takes this reading, and no test or mutant in the draft would catch it. A draft whose purpose is to close a silent loss may not leave one open in its own text, so this is REWORK, as at S-THETA's first gate. The repair is mechanical: R-1 to R-3 below.
- **Everything else holds.** The blocks are applied once and exactly. The scope is the two documents. The fresh gate result is my own run. The Acceptance lines pass, and every mutant I ran is caught. The R-1 numbers recompute. The composition claims are confirmed.

## What I verified myself, and what I took from the pre-gate

**Myself:**
- **Fresh gate.** The scaffold's `fresh-gate.sh`, extracted through the shallow `--no-local` clone of GATE-PROMPT step 1 (scaffold `c140f463f9c4d1b1eccf76352e6f2bc86f577055`, no `EXTRACT FAILED`). Its `fresh-gate:` lines:
  - `fresh-gate: commit b1ecf549665c0b9edc51fcbc2361aef65e356148`
  - `fresh-gate: base c140f463f9c4d1b1eccf76352e6f2bc86f577055`
  - `fresh-gate: checks passed (4896 files hashed in 1.53 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - Steps: `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`. No `not ok` row, no failed test, and stderr is empty.
  - The literal last line is `GATE EXIT=0`, on the first run (1-minute load 1.84 at launch, peaking near 23 during the run); no re-run was needed.
- **Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` touches exactly `nucleus/v3/docs/R-1-memory.md` (17+, 16−) and `nucleus/v3/docs/S-STORE.md` (46+, 39−), both on the Touch only list. The merge-base is c81e84a1. One commit lies on top of it. The scaffold's two documents equal c81e84a1's.
- **Application.**
  - The base files have the card's sha256 values (`20bfce77…`, `c5927302…`).
  - My own parser read the card's 62 blocks (E1, U1 to U48, E2; V1 to V12). I applied them in order with `<date>` = 2026-11-21, the commit's date. Each Old occurs exactly once at its turn, and each New exactly once in the result.
  - The results equal the tip byte for byte: S-STORE `7f9fb33477a720d0d806c1f60bb4a105500729ddeaba8a1491914ea0d1cc8d5f`, R-1-memory `9adde1f29f86ce9b1adfba00b158fe14355bfa78719e99fc651163951a271bbf`. These are the dry run's hashes. There is not even a date difference.
- **Acceptance lines 1 to 67.** All pass at the tip, in a sparse throwaway clone under my scratch directory, under `LANG=en_CA.UTF-8` and `LC_ALL=C`. The checker (line 58) prints `174 rows; 280 mutants; 24 judged only on Phase B cards; 48 invariants; 55 tests` and `OK` in both locales.
- **Mutants.** I ran eleven text mutants (table below). Each was committed on a throwaway branch of that clone, so lines 59, 60 and 66 judged a commit, as they do in the real flow. Each was then reset, and the clone was checked back at the tip with a clean status. The card's worktree was never touched: its status is empty and its HEAD is the tip.
- **The judgments below:** SF-C1, the fix, F-1 to F-11, and the R-1 arithmetic. I recomputed the embedded and server rows, `H`, `R_EPOCH`, `R_MAP`, `F = 3n(H+2) + R_EPOCH + R_MAP`, and the three `B_min`.
- **Composition.** I ran `git merge-file -p` of S-MU against S-KAPPA (base 7.20) and applied S-MU's blocks onto the S-KAPPA tip and onto S-LAMBDA's R-1-memory.

**From the pre-gate, after opening its evidence:** the browser row's `R_file` decomposition (74,944 → 91,328 B, `15-r1-numbers.txt`); it is consistent with the +16 KiB the draft moves, and I did not re-derive it from §5.2. I did not re-run its other 13 mutants or the author's 81 plants.

## SF-C1 on 7.20: real, and silent

I walked the card's six steps against 7.20's text.

1. **Epoch `k`.** M is evicted to `mc`, reloaded, and X changes under M and is evicted to `sx1`. M is now dirty with content epoch `k`, and its current home `mc` holds X → `sx0`. Permitted by §5.2 ("written to a fresh slot … map entry is updated in memory") and §4.1 ("A dirty page may therefore be evicted at any time").
2. **Checkpoint `k`.** It writes M from its frame (content epoch `k ≤ k`) to `m_k`, and P's buffer records `m_k`. Image `k` is right.
   - M's current home stays `mc`: L1090, "The map phase never makes a slot a map page's current home"; L236, "only current refs are set this way".
   - M stays dirty. Nothing in 7.20 cleans it.
3. **Epoch `k+1`.** M is unchanged; N changes.
4. **Checkpoint `k+1`.** It writes pages "that changed since image `k`". M did not change and has no record, so it is not written. P is written, built "from the page's epoch-`k` content", which is P's current refs, M → `mc`. So image `k+1` names `mc`, whose X entry is `sx0`.
5. **Crash after step 5 of checkpoint `k+1`.**
   - `sx0` is intact: I-FREE-SAFE frees a slot of image `k−1` only at step 6 of checkpoint `k+1`, and image `k+1` references it anyway.
   - `mc` is intact: it is M's current home, and 7.20's §4.4 frees only "an earlier eviction home of a page in this epoch".
   - Replay skips `commit_ts ≤ T`.
   - Every ref's CRC matches the bytes it names, and the slot bitmap is "derived from its built map".
   - X reads `x0`, and nothing fails.

I found no sentence that forbids a step. "A map page whose epoch-`k` content it has taken counts as written" is about the page's own source, and L1087's definition is the permission. The growth facet ("whose first ref names `R`", with no slot for that ref) is real too: under the natural reading, a level made after flip `k` names R's image-`k−1` page after checkpoint `k` has rewritten R. SF-C1 is a defect of the step text, and I-CKPT-AT-T (a) contradicts it.

## The fix, worked on 7.21's explicit rules

- **The induction holds.** After checkpoint `k`, every image ref is the page's ref in image `k`. A MapLeaf that checkpoint `k+1` does not write has content epoch below `k+1` and no record, and image `k+1` names it by its image ref. A MapInner is built from image refs only, and it is written iff a child is.
- **Crash states I worked:**
  - a crash during either map phase (no meta names the half-built image; the open sets image refs from the image it opens);
  - a fall-back from image `k` to image `k−1` (§14 step 4a discards what it built and reruns step 2);
  - a level added before and after checkpoint `k`'s map phase (both give the old root's image-`k` page);
  - a dirty MapLeaf of content epoch `k−1` evicted after flip `k` (a self record; F-4's two readings differ on whether it is rewritten, and both are correct);
  - a MapLeaf of content epoch below `k`, mutated after the flip, with records in list `k` for its entries and its home moved. This is the case that the last source step, "then its image ref's slot", exists for. The step is live and correct: the MapLeaf's content equals its image-ref page modulo entries the records rewrite. So the step must stay. It cannot simply become an XX000.
- **Each of those ends correct.** Image refs are read by nothing but the map phase, so I-PIN-CURRENT is untouched, and no state that 7.20 admitted is newly refused.
- **The one exception is F-1.**

## Findings

**F-1 (blocking; pre-gate F-1, confirmed independently).** S-STORE L1087, unchanged from 7.20: "\"The image map\" in §5.2 and §5.6 means the epoch-`k` map pages with the list applied." §5.6 "When it copies" (L371) is phrased in that term: "copies only if the frame's content epoch `< e`, **and** that content is not yet at a slot the image map references".

- **The definition no longer matches the draft.** Taken as written, it is 7.20's construction: each map page's epoch-`k` content with the list applied. For a level-1 MapInner that content is its current refs, which U22 now forbids building from. U36 already re-describes the image map as built "from the image-record list and the image refs"; L1087 was not changed with it.
- **The loss, walked on 7.21's text:**
  1. In epoch `k`, MapLeaf M is mutated and evicted to `s2` (content epoch `k`; P's current ref names `s2`), then reloaded clean, or left cold.
  2. After flip `k`, an eviction under M mutates M. Under L1087's literal definition, M's content is at `s2`, a slot P's epoch-`k` content names, so no epoch copy is made.
  3. M is evicted again, to `s3` (content epoch `k+1`, so no record). U6 frees `s2`: "an eviction home of a MapLeaf that no image-record list names, whatever epoch its ref carries".
  4. The map phase must write M, whose content epoch is `k`. It finds no copy, no frame of content epoch `≤ k` and no record, so U22 sends it to "its current home, unless a write since the flip has moved it, and then its image ref's slot": M's image-`k−1` page.
  5. Image `k`'s M lacks M's epoch-`k` entries. After a crash past meta `k`, an acked commit is lost with every check passing.
- **Why this outweighs the explicit sentences.** At 7.20 the same reading was safe: `s2`, an eviction home of a past epoch, was never freed (the leak MAPLEAF-HOME-LEAKED names), and "its epoch-`k` home" was `s2`. This draft's U6 and U22 are correct only if a MapLeaf of content epoch `k` is always copied, and the only text that decides the copy is the one this draft left stale. U2's "an image names a MapLeaf … by its image ref" and U6's "no image names one" support the right reading, but they do not redefine the term §5.6 uses.
- **The reading is already taken.** Card A-3c-C's Work 6.2 ("`!dirty` → proceed", with `written_for` "cleared … by a load") takes exactly this reading. It was correct at 7.20.

**F-3 (blocking with F-1; confirmed).** No test or mutant reaches F-1's path.
- The two-checkpoint variant keeps M resident and unchanged through epoch `k+1`.
- The current-home variant's second eviction happens after the map phase.
- The map-copy variant's M is dirty at the flip, so a `!dirty` test copies it.
- A build that takes L1087 literally passes every test the draft names. My spec mutant MAPLEAF-CLEAN-NOT-COPIED is UNCAUGHT by them.

**F-2 (card text; confirmed).** The card's "Card text for the orchestrator" for A-3c-C leaves Work 6.2 and its `s3_first_touch` as they are. Both need the rule of R-2 below before A-3c-C is cut.

**Other pre-gate findings:**
- **F-4: confirmed, wording.** U22's lead-in ("changed since image `k−1` or that the list's records name") and its elaboration differ on a MapLeaf whose only record names the MapLeaf itself. Both readings give a correct image.
- **F-5: confirmed, wording.** U13's "which only step 2 sets" omits the open, which U2 names.
- **F-6: confirmed, wording.** §9.2 item 6, "returns it from every call until it is closed", lacks U17's exception. Harmless.
- **F-7: confirmed, test gap.** U19's "every writer checks the mark immediately before it writes" has no mutant. The fail-stop write-hook assertions are single-writer. This is not a weakening: 7.20's "nothing is written" could not be built.
- **F-8: confirmed, card prose only.**
  - "Changed rows (8)" and "changes eight": U43 to U48 change seven rows.
  - D-2's "with equality, as before": at draft 1 the embedded row had 2 frames of slack (5,878 against 5,876) and the server 48 (916,452 against 916,404). The equality is new.
- **F-9: confirmed.** `git merge-file -p` of S-MU against S-KAPPA (base 7.20) exits 7, with hunks at merged lines 3, 669, 1383, 1399, 1412, 1423 and 1777: five in the body. S-MU's 50 S-STORE blocks still apply, each Old once, onto the S-KAPPA tip. Merge both by blocks, not by `git merge`.
- **R-1-memory with S-LAMBDA: confirmed.** S-MU's V1 to V12 apply onto S-LAMBDA's R-1-memory. S-LAMBDA's M1 (Old `Status: draft 1 (card R-1-SPEC, `) no longer occurs after S-MU, so S-LAMBDA goes first or re-anchors (the card's Notes say so).
- **F-10: true and informational.** No worker log exists, as the request says.
- **F-11: dismissed.** §9.2 (L649) makes "another check that damaged data from the device can fail" an XX001. A correct engine meeting an oversized key in a record from the device raises XX001, a poisoning class, never 54000. So U16 and the XX001 rule do not meet.

**Acceptance lines 59, 60 and 66, as the card wrote them, are adequate.** They hash the committed diff. Committed, O10 fails 66 and P4 fails 60, as they must. The pre-gate's "caught only by working-tree forms" comes from its mutants being uncommitted, not from a gap in the lines. Line 59 counts lines and catches no in-line change; 60 and 66 carry that. Their one dependence, on the scaffold ref not having moved, is intended.

## Mutants (text forms; Acceptance 1 to 67, both locales, identical)

| Mutant | Change | Failing lines |
|---|---|---|
| MAP-INNER-FROM-CURRENT | U22: a MapInner built "from its epoch-`k` content, its children's current refs" | 28, 57, 60 |
| IMAGE-REF-NOT-SET | U22: the map phase "leaves the page's image ref as it was" | 28, 60 |
| MAPLEAF-HOME-LEAKED | U6: a MapLeaf's eviction home is reusable only "when its ref carries the current epoch" | 12, 60 |
| LEVEL-IMAGE-REF-STALE | U4: the old root's ref is "the image ref the old root had when the level was made" | 10, 57, 60 |
| DIRTY-MAPLEAF-UNDER-P | U12: a cold MapLeaf read "under an inner page's exclusive latch if it holds one" | 18, 60 |
| DIRTY-MAPLEAF-PIN-DROPPED | U12: "A restart drops these pins, keeping only the one it found cold" | 18, 60 |
| O10 (pre-gate) | R-1 §4.4: the (Mp) bound `2 × (H + 2)` | 66 |
| P4 (card plant) | I-CKPT-AT-T row: "a coherent cut" | 60 |
| X1 (own) | U22's last step: "its epoch-`k` home: its current home", no image ref | 28, 57, 60 |
| X2 (own) | U3: "An epoch copy is such a page and holds a pin" | 9, 60 |
| X3 (own) | R-1 §4.6: server `R_MAP` 243,422 | 63, 66 |

The text is well pinned: every sentence the blocks write is fixed by a line, and lines 60 and 66 fix everything else. F-1 is a gap in what the text says, not in how it is pinned.

## The rework (S-MU, draft 7.21 re-cut on 7.20), mechanical

Keep every block as it is, and add:

**R-1. §13 step 2, the definition (F-1).**

Old:
```text
"The image map" in §5.2 and §5.6 means the epoch-`k` map pages with the list applied.
```
New:
```text
"The image map" in §5.2 and §5.6 means image `k`'s map as the map phase builds it: each MapLeaf from its epoch-`k` content with the list's records for its entries applied, and each MapInner from its children's image refs (§4.1), never from a MapInner's epoch-`k` content. So it references no MapLeaf's eviction home (§4.4), and the epoch-`k` content of a MapLeaf whose content epoch is `k` is at no slot it references until the map phase has taken that content (card S-MU).
```

**R-2. §5.6 "When it copies" (F-1, F-2).** Append this to U13's New, after "(card S-MU).", and fold F-5 into the same New ("which only step 2 sets" → "which only step 2 and the open set"):
```text
 A MapLeaf whose content epoch is `e − 1` is copied at its first mutation in epoch `e`, whether or not its frame is dirty and whatever slot it was loaded from, unless the map phase has taken that content for image `e − 1`: its content is at no slot the image map references (§13 step 2), and without the copy an eviction could move its current home, which §4.4 then frees, leaving step 2 only its image ref's page, which lacks its epoch-`(e − 1)` mutations (card S-MU).
```

**R-3. A pin for it (F-3).**
- **The run.** In the §15 I-CKPT-AT-T row, after U38's growth-run sentence ("… and image `k+1` holds X's epoch-`k` content."), add:
  ```text
   Its reload run (card S-MU): in epoch `k`, X is mutated and evicted and M is then evicted to a slot `s2` and reloaded, so that M is clean at flip `k` with content epoch `k`; after the flip and before the map phase runs, another page M maps is mutated and evicted, which mutates M, and M is evicted again, so that its current home moves and `s2` is free (§4.4), and a slot allocation from a pool at its minimum takes `s2`. Image `k` must hold X's epoch-`k` content (S2's read-back; a read at `T` after a crash at post-5).
  ```
- **The mutant's failure.** In U40's New, after the MAPLEAF-HOME-LEAKED clause, add:
  ```text
  ; MAPLEAF-CLEAN-NOT-COPIED (`write_guard` makes no epoch copy of a MapLeaf of content epoch `k` whose frame is clean, as if its current home were a slot the image map references) fails the reload run: image `k`'s M is built from its image ref's page, so the read at `T` returns X's content from before its epoch-`k` mutation, and every check passes
  ```
- **Where it is judged.** Add MAPLEAF-CLEAN-NOT-COPIED to U41's list and to U46's and U47's rows, judged where IMAGE-REF-NOT-SET is (A-3c, through S3's first touch; A-7f). Add "the reload run" to those two rows' Form cells.
- **What else moves.** The mutants added become seven (§15: 281). E1, E2, "The draft target", the §15 rows section, D-7, and Acceptance lines 1, 6, 28, 44, 46, 47, 52, 53, 57 to 60 are recomputed.

**Card text for the orchestrator (F-2), added under A-3c-C:**
- Work 6.2's "`written_for == e − 1` → proceed; `!dirty` → proceed" becomes "`written_for == e − 1` → proceed; `content_epoch < e − 1` → proceed".
- `s3_first_touch`'s condition `g.is_dirty()` becomes, for a MapLeaf, `g.content_epoch() == e − 1`.
- Work 6.3's level-1 MapInner clause goes, since no MapInner is copied.
- `map_dirty_child_two_ckpt` also gains the reload run.

**May ride in the rework, or be S-MU-F1 (spec, wording and a test gap; non-blocking):**
- F-4: make U22's lead-in say what its elaboration says.
- F-6: add U17's exception to §9.2 item 6.
- F-7: add a mutant for U19's check, a writer that checked the mark at acquisition and writes after it (judged on A-3c-B's rows).
- F-8: fix the card prose ("seven rows"; D-2's "as before").

**Not to change.** Every rule the draft states explicitly. In particular, U22's last source step ("then its image ref's slot") is correct and needed for a MapLeaf of content epoch below `k` whose home moved after the flip. Do not replace it with an error.
