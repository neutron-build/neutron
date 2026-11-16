Card: S-ALPHA
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 77dc58e6-9586-4c9a-8729-91a868bedb3f (a fresh gate run; this agent wrote neither the card, the S-ALPHA commits, the two earlier verdicts nor the two reworks)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: 676c80f2c6d62957fcda439b70bc729c7ac5453e
Mutants: the card names none (spec text). The gate's own text mutants, each on a scratch copy of the tip, judged by the card's Acceptance (line 10 is the checker): TM1 (V1 reverted: I-TWO-GEN stated with no condition) -> UNCAUGHT; TM2 (V4 reverted: the unproven case's WAL loss "between meta `k−1`'s and meta `k`'s `wal_ckpt_end`") -> UNCAUGHT; TM3 (FALLBACK-NO-PROOF dropped from `t_fallback`'s §15.1 row) -> checker ("FALLBACK-NO-PROOF not judged on A-7d"); TM4 (`t_fallback`'s row moved from A-7d to B-6) -> checker ("ROLLBACK-SKIPS-PREV-CHECKS not judged on A-7d"). No Acceptance line reads prose such as TM1-TM2; the gate judged those by reading: TM1 restates the invariant verdict 2 showed false, and TM2 makes the unproven leaf cell unpassable on a correct build (G1)
Followups: S-ALPHA-F1, S-ALPHA-F2, S-ALPHA-F4, S-ALPHA-F5, S-ALPHA-F6

Rework 2 is exactly verdict 2's ten blocks, V1–V10. The bytes match the gate's own re-application to 41795fba, and no name is lost. All five defects verdict 2 named are fixed in the text, and the behaviour each sentence now states is right. `t_fallback` was walked case by case against §14 steps 1–4a: a correct build passes every case and variant, and every mutant the test is meant to judge fails it.

None of the six residuals is blocking. No sentence states behaviour the design does not have in a way that would lead an implementer or a test author to build the wrong thing, and no test is unpassable on a correct build or unfailable for its mutant.
- Five residuals are under-specification or wording. They ride in S-BETA as part of S-ALPHA-F6, as exact blocks F6-a to F6-m (below).
- One is a policy gap the spec does not decide: a 58030 on meta `k−1`'s free set. It goes to S-ALPHA-F2's card, before A-7d. It is not tied to S-ALPHA-F4.

The card merges with S-ALPHA-F1, F2, F4, F5 and F6 open. F4 is the owner's decision, alongside O-1, O-2 and O-3.

## History
- **3ccc8b3d**: the card's E1–E59, S-STORE draft 7.11.
- **d91802f6, first verdict: REWORK on 3ccc8b3d** (P, GATE EXIT=0).
  - Seven of the implementer's ten contradictions were real. Two of them changed behaviour: "writes nothing" became "writes nothing more", with the retire write before the first write from meta `k−1`; and the retire write zeroes misplaced copies first, under their own sync.
  - Eight mutants could not fail as specified.
  - The verdict gave 41 exact blocks, R1a–R18, and follow-ups S-ALPHA-F1 to F5.
- **41795fba, rework 1**: the 41 blocks applied by script in the verdict's order, +37/−36 in one file, draft number unchanged. R18 adds the Changelog's gate-corrections line.
- **b8f51964, second verdict: REWORK on 41795fba** (P, GATE EXIT=0).
  - Rework 1 was exactly R1a–R18.
  - The verdict rebuilt the open-rule model over 167,042 place combinations: no silent loss at 0, 1 or 2 faults; the only one-fault refusal is S-ALPHA-F5's.
  - Real and fixed by its blocks: N1 (`t_fallback`'s unread variant could not pass), N2 and N3 (I-META-COPIES and I-TWO-GEN overclaimed), G1 (the unproven case's WAL loss could fail step 3a before pass 2) and G2 (55000 "writes nothing" after a pass 2 that evicted).
  - It gave ten exact blocks, V1–V10, kept S-ALPHA-F1, F2, F4 and F5, and added S-ALPHA-F6 (riders for the next S-STORE card).
- **676c80f2c6d62957fcda439b70bc729c7ac5453e, rework 2**: V1–V10 applied by script, +10/−10 in `docs/S-STORE.md` only; file sha256 `20283ae8…28e4`, the second gate's dry run. Draft number unchanged; V10 extends the Changelog's gate-corrections line.
- This verdict (third gate) replaces the second verdict's text.

## Gate run
- Scaffold extract (GATE-PROMPT step 1): shallow `--no-local` clone of `nucleus/v3-scaffold` at **108e180ef4f3e557b68f8bfd65b4a1df78ba3317**. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- The run: `bash "$fg/fresh-gate.sh" 676c80f2c6d62957fcda439b70bc729c7ac5453e` from `~/Workspace/neutron-v3`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-ALPHA-3` and both streams captured. Its lines:
  - `fresh-gate: commit 676c80f2c6d62957fcda439b70bc729c7ac5453e`
  - `fresh-gate: base 108e180ef4f3e557b68f8bfd65b4a1df78ba3317`
  - `fresh-gate: checks passed (4814 files hashed in 2.09 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`
  - literal last line `GATE EXIT=0`; exit status 0; stderr empty. `$fg` was removed afterwards.
- The worktree `~/.cache/nv3-cards/S-ALPHA`, branch `card/v3-S-ALPHA`, was at tip 676c80f2 and clean before the verdict commit.
  - No assume-unchanged or skip-worktree entry exists.
  - The worktree's `docs/S-STORE.md` hashes to the committed blob, sha256 `20283ae8fcde9ec3bcf78b05a5e517ecdfbe85b8025624d0e86b9de3089a28e4`.
  - Every mutant, dry run and rider check used scratch copies under `~/.cache/nv3-review/gate-S-ALPHA-3/`.

## Scope and the rework's bytes
- `git diff --stat nucleus/v3-scaffold...HEAD` lists two files.
  - `nucleus/v3/docs/S-STORE.md`, +251/−56 against 7.10, is the card's Touch only list.
  - `nucleus/v3/verdicts/S-ALPHA.md` is the gate verdict, which this file replaces.
  - No protected path is touched.
- `git show --stat 676c80f2` is one file, `docs/S-STORE.md`, +10/−10.
  - The gate's own parser read V1–V10 from `git show b8f51964:nucleus/v3/verdicts/S-ALPHA.md` and applied them to 41795fba's S-STORE.md in the listed order. Every Old occurred exactly once at its turn.
  - The result is byte-identical to 676c80f2's file: sha256 `20283ae8…28e4`.
- A script compared names across 7.10 (108e180e), 3ccc8b3d, 41795fba and 676c80f2: headings, invariant rows, invariant references, mutant names, `t_*` tests, SQLSTATEs and §18 rows. Every name of 3ccc8b3d and 41795fba is present at 676c80f2, and every name of 7.10 except the heading `### 2.2 Slots 1–2: meta records`, which E7 renamed by design.
- Acceptance lines 1–10 pass in the worktree. The checker prints `141 rows; 200 mutants; 31 new, 31 of them judged on a Phase A row; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, then `OK`.
- Line 11 fails, and only because of the verdict file.
  - The line is `cd nucleus/v3 && git diff --stat nucleus/v3-scaffold...HEAD -- . | tail -1 | grep -q '1 file changed'`.
  - With `-- .` it reports `2 files changed, 622 insertions(+), 56 deletions(-)`. With `-- docs` it reports `1 file changed, 251 insertions(+), 56 deletions(-)`.
  - Any verdict commit on the branch makes it fail. It is not a defect of the card's work.
- Line 12 is this gate run.
- S-BETA's 44 Old blocks, applied in order to 676c80f2's file, each occur exactly once except E25 and E42 (0 times), as verdict 2 found at 41795fba. V1–V10 moved no further anchor.

## The five defects of verdict 2, re-checked against 676c80f2
1. **N1, `t_fallback`'s unread variant. Fixed (V5).** The variant now runs on the proven fixture, with its persistent post-sync fault on a page that step 2 reads (a MapLeaf on the directory's path, a FreeSet page or a directory page). The first open fails in step 2 before any write. The next open reads the free set, falls back with proof onto an image `k−1` nobody wrote, and recovers. The behaviour is right. It needs step 2 to read meta `k−1`'s free set even after image `k` failed there; that is residual 1.
2. **N2, I-META-COPIES overclaimed. Fixed (V6, V8).**
   - The row and §14's "A meta copy" bullet now speak only of a meta whose two copies were both written. That is true: in verdict 2's model, every one-fault state with a different selection or a refusal involves an in-flight copy.
   - V6's added sentence copies §2.2's R11 sentence and is too broad in one case (residual 2).
3. **N3, I-TWO-GEN overclaimed. Fixed (V1, V2, V9).**
   - §4.4 and the row now state the invariant only for the newest synced meta, and image `k−1` only while every open since then has read meta `k−1`'s free set.
   - Both of verdict 2's counterexamples now fall outside the statement: (a) an open with no previous image, and (b) a newer synced meta that was later lost.
   - §4.4's second paragraph (V2) carries the same condition, and so does step 4a item 1.
   - Nothing the invariant now says is false. It says nothing about two states the design relies on (residual 3).
4. **G1, the unproven case's WAL-loss placement. Fixed (V4).** The lost record lies in `[meta k−1's wal_ckpt_end, meta k's wal_start_lsn)`. So the walk from meta `k` never reads it, and only the walk from meta `k−1` does. In the leaf cell, step 3a from meta `k` passes and image `k` fails in pass 2, as the case needs. The range can be empty in a fixture with a long chunked transaction; that is residual 5.
5. **G2, 55000 after pass 2. Fixed (V3, V7).**
   - Item 5's paragraph and §14.2's row now admit the evictions of a pass 2 that ran from image `k` without a failure, into slots image `k` does not reference.
   - That is exactly what a correct build writes before it learns that the precondition does not hold. A refusal in steps 1, 3, 3a or 3b comes before pass 2, writes nothing, and keeps its own code (XX001), as the paragraph's last sentence says for WAL loss and a splice.
   - The remaining question, whether those evictions also avoid image `k−1`'s slots when step 2 could not read its free set, is S-ALPHA-F2's (residual 4).

### `t_fallback`, case by case against §14 steps 1–4a
A correct build reads meta `k−1`'s free set in step 2 whatever image `k` does: the proven step-2 cells need it, by item 1. Each case below is walked on such a build.

- **Proven, step-2 cells (MapLeaf, FreeSet page, directory page).**
  - Image `k` fails in step 2, and nothing has been written yet.
  - Item 1 holds: the free set was read. Item 2 runs steps 2–3b from meta `k−1`, and they pass, because the WAL is intact. Item 3's proof holds, because the acked commits after meta `k` lie past meta `k`'s `wal_ckpt_end`.
  - Item 4 retires meta `k` (two syncs), then runs pass 2 and steps 5–8 from meta `k−1`. The hooks see zeros, synced, before the first write, and no earlier write at all.
  - The read-only open retires nothing, and every file is byte-identical.
  - **Passes.** FALLBACK-OFF fails it (XX001). RO-WRITES fails the read-only form (a retire).
- **Proven, leaf cell.**
  - Steps 1–3b from meta `k` pass. Pass 2 evicts into slots that neither image references (step 2's protection), then fails on the leaf.
  - Item 4 follows as above. The hook's "every write before the retire went to a slot neither image references" holds.
  - **Passes.** FALLBACK-OFF fails it.
- **Unproven, step-2 cells.**
  - Item 2 from meta `k−1`: the walk stops at the lost record, at or above meta `k−1`'s `wal_ckpt_end`, so coverage against meta `k−1` passes. The proof fails: the valid end is below meta `k`'s `wal_ckpt_end`.
  - The read-write open gives XX001 and writes nothing at all, as "every file is" asks.
  - The read-only open serves salvage at the frontier. A lost commit's ts lies above it (I-WAL-TS-ORDER), so no read sees it.
  - **The explicit rollback naming `k`:** the precondition holds (the newest meta is `k`, and image `k` fails at open). It retires meta `k`, runs pass 2 from meta `k−1`, truncates at the valid end and opens. The next open selects the boot checkpoint's new `ckpt_id` `k`.
  - **The second LOST,** below meta `k−1`'s `wal_ckpt_end`: item 2's coverage check fails, and the open gives XX001.
  - **The 55000 sentence** is a separate store whose image opens; it gets 55000.
  - **Passes.** FALLBACK-NO-PROOF fails it (it opens without the lost commit). ROLLBACK-SKIPS-PREV-CHECKS fails the second-LOST sentence (it opens over meta `k−1`'s own WAL loss).
- **Unproven, leaf cell.** The walk from meta `k` starts above the loss, so step 3a passes, and image `k` fails in pass 2. From there the walk is the same as the step-2 cells. Byte identity is asserted only on meta places, WAL files and slots that either image references, which is what a correct build keeps. **Passes.** The same two mutants fail it.
- **Two generations.**
  - The other parity's candidate is `k−3`, so there is no previous meta, and step 2 protects nothing more. Image `k` fails, and item 1 gives XX001 "with no previous image".
  - **Passes.** FALLBACK-TWO-GEN fails it: it reads through meta `k−3`'s references, and its detail differs.
- **Retire.**
  - The first open falls back with proof and retires meta `k`. A crash follows a pass-2 eviction from meta `k−1` into a slot of image `k`.
  - Parity `k`'s places are zeros, so the next open selects meta `k−1` with no previous meta and replays from an intact WAL. **Passes.**
  - RETIRE-SKIP fails it: meta `k` opens over the overwritten slot (XX001 or amcheck). RETIRE-ONE-COPY fails it through the replica.
  - **A crash between the zeroing writes**, on either reading of "between": a copy of meta `k` survives in its own parity. Image `k` is untouched (only pass-2 evictions into unreferenced slots preceded the retire) and now reads, so the next open opens meta `k`, which is the same state. **Passes.**
  - **Misplaced variant.** The misplaced copy is zeroed and synced first, so after the retire parity `k−1` holds one valid `k−1` and no misplaced copy, and meta `k−1` opens. **Passes.** A retire of the own places only leaves misplaced `k` > `k−1`, which gives XX001 (I-META-SLOT), so the variant fails it.
- **Unread variant,** on the proven fixture.
  - At the first open, step 2 meets image `k`'s persistent fault and the hooked free-set read. Item 1 gives XX001 "no previous image", and nothing is written.
  - The next open reads the free set and falls back with proof. **Passes.**
  - A build that falls back without the free set opens at the first open and fails the variant.
  - This holds for an integrity failure from the hook. A 58030 is residual 1.

## The six residuals: rulings
1. **N1 / F6 (c): step 2's read of the previous free set, and what "a hook fails one read" means. Not blocking. Rides in S-BETA (F6-c, F6-d). The 58030 policy goes to S-ALPHA-F2's card, before A-7d.**
   - *Read order.* No sentence says step 2 reads meta `k−1`'s free set after image `k` has failed in it. But item 1 ("only when step 2 read meta `k−1`'s free set … wherever image `k` failed") together with the proven case's step-2 cells forces it: a build that stops step 2 at image `k`'s first failure fails those cells. So the tests pin the behaviour, and no sentence says otherwise. F6-c states it.
   - *The failure kind.* The spec says "its free set fails a read (§3.3)" in §4.4, "failing a read in step 2" in step 4a item 1, "its free set reads" in §2.2, "an unreadable free set" in I-TWO-GEN's row, and "unread" in §14.2.
     - The §3.3 citation makes an integrity failure the defined case: it is "no previous image", and the open carries on, protecting nothing more.
     - For a 58030 on that read, the spec does not decide. Step 4a's "a read error (58030) … handled as those steps and §9.2 say" is about image `k`'s pages, and step 2 and §9.2 say nothing about an open's read errors. The row's "unreadable", which elsewhere in the spec (§2.2) means a failed read, leans the other way.
     - The test is unaffected if its hook corrupts bytes. With an integrity failure, both readings give the variant's XX001. F6-d says so, as the Retire bullet already does ("a hook corrupts the bytes of one read").
   - *Who decides 58030.* S-ALPHA-F2's card, which owns what an open does when it cannot read the previous free set. It must decide before A-7d, which builds the open.
     - It is not tied to S-ALPHA-F4. F4 asks whether a 58030 on image `k`'s pages may let an explicit rollback or a read-only salvage proceed. A 58030 on meta `k−1`'s free set blocks every fall-back and every rollback under either F4 answer (item 1).
     - The gate recommends that a 58030 there ends the open with 58030, writing nothing. A read error is not evidence of damage (D35's reasoning). Carrying on would spend image `k−1`'s protection on what may be a transient error. Under F2's candidate (protect every slot below meta `k−1`'s `slot_count` that image `k` does not reference), carrying on would also be safe, so F2's card can choose with that.
2. **N2: V6's sentence, and §2.2's sentence it copies. Not blocking. Rides (F6-i, F6-j).**
   - A crash between step 4's two writes leaves one in-flight copy of meta `k+1`. If that copy is misdirected into a place of the other parity, the open fails with XX001 (I-META-SLOT; S-ALPHA-F5's shape), not "opens the meta before it". If it is misdirected within its parity, it is still valid, and meta `k+1` itself opens.
   - Both outcomes are stated where the rule lives (§2.2's misplaced-copy rule, §14.2's I-META-SLOT row, `t_meta_copies`' table form). An implementer builds the rule, not this summary.
   - No test asserts the summary: `t_meta_copies`' fault form puts its faults on copies of the newest synced meta.
   - It fails closed. If S-ALPHA-F5's candidate is adopted, the other-parity case becomes true as written.
3. **N3: I-TWO-GEN silent in two states. Not blocking. Rides (F6-k), and F6 (d) as F6-e to F6-h.**
   - *One surviving copy of meta `k+1`, never synced:* the open selects it, so neither meta `k+1` (not synced) nor meta `k` (not the durable image) meets the condition. The design's fall-back from it to meta `k` is still sound: step 6 of checkpoint `k+1` never ran, so image `k` and the WAL from meta `k`'s start are as I-TWO-GEN gives for a synced meta `k+1`.
   - *Between the retire write and the boot checkpoint's step 5:* meta `k−1` is the durable image, but meta `k`, retired, was the newest synced. What the design needs then is only meta `k−1`'s own WAL span (there is no previous meta: parity `k`'s places are zeros). Step 6 of checkpoint `k` recycled only below meta `k−1`'s start, and nothing recycles before the boot checkpoint's step 6.
   - Both are facts the step order gives. No sentence denies them, and no test depends on the invariant's wording there.
   - The four citations that call the previous image "intact (I-TWO-GEN)" inherit V1's condition through the citation. F6-e to F6-h add "as I-TWO-GEN states".
   - The Status line and the Changelog's "stay intact until meta `k+1` is durable" are draft 7.11's summary. S-BETA's Limit 1 keeps the Status line byte for byte, and the gate does not ask for those two to change.
4. **55000 after pass 2. Resolved (G2 above).** The residue, pass-2 evictions into image `k−1`'s slots when step 2 could not read its free set, is S-ALPHA-F2's.
5. **The unproven fixture's range can be empty. Not blocking. Rides (F6-l).**
   - Meta `k`'s `wal_start_lsn` falls below meta `k−1`'s `wal_ckpt_end` when a chunked transaction whose first chunk precedes checkpoint `k−1`'s flip is not yet below `T` at checkpoint `k`'s flip (§12.6). The range is then empty.
   - The sentence still names a record "holding an acked commit" in that range, so a fixture that has none does not meet it. A test author must build one (commits between the checkpoints, and no such chunked transaction).
   - A correct build passes every fixture that meets the sentence. F6-l states the precondition.
6. **The unread variant inside the Retire bullet. Cosmetic. Optional rider (F6-m)** moves it to the end of the Proven bullet. Unambiguous as written.

## What S-BETA must change after this card, checked against 676c80f2
Verdict 2's list stands, re-checked at 676c80f2:
1. Renumber S-BETA's §18 row R18 → **R23**. Re-anchor E42 on this card's lead-in ("…, R17 from card S-1-F7, and R18–R22 from card S-ALPHA; …", naming R23 from card S-BETA). Renumber E43's row, and change its Acceptance grep `^| R18 | ` to `^| R23 | `.
2. E25's Old is gone. Insert its clause after "crosses into newer boots' records (§12.5)" in this card's I-REDO-COMPLETE sentence, before ", when that meta's image and WAL span are intact and no replayed commit depends on a newer checkpoint (§12.8)" (S-BETA's R-5).
3. §12.5's I-WAL-START sentence ("it leaves `wal/` only by recycling") must admit E22's step-5 removal of segment files whose first LSN lies above the valid end. Such a removal never takes the start segment of the meta that open selected.
4. E28's splice cut-back list gains step 3b's dependency check (S-BETA's R-6). §14.1's "Where several cuts apply, the lowest one does" stands.
5. Each "lost after its sync" in E31's two tests and in R-4 becomes "both copies lost after their sync". E11's "(a meta rollback, §2.2)" cites "§2.2, §14 step 4a". E12's and E22's "a lost meta" may read "a meta lost in both copies".
6. §15.1 starts from 141 rows, 148 after S-BETA's seven. The checker's mutant set is 7.8's, plus this card's 31, plus S-BETA's own, with the 25 Phase-B-only mutants unchanged.
7. Nothing else to re-anchor: E26–E29 and E34 occur once at their turn.
8. Optional precision: E11's "`durable_end` at the walk's start, the opened meta's `wal_start_lsn`" means meta `k−1`'s after a fall-back.
9. **S-ALPHA-F6's riders, F6-a to F6-m, below.**
   - S-BETA's Limits must admit them. Add a Limits item: "7. S-ALPHA-F6's riders F6-a–F6-l (and F6-m if taken), from `verdicts/S-ALPHA.md`, byte for byte, in §1, §2.2, §4.4, §14 and §15. Nothing else outside items 1–5 changes." Also change item 6's "§2 … (S-ALPHA's) are not touched" to except F6-b and F6-j.
   - They change no mutant, invariant name, test name or §15.1 row. The §15 rows change only in F6-a's wording.
   - Checked on scratch copies: each Old occurs exactly once in 676c80f2's S-STORE.md, both before and after S-BETA's 42 applicable blocks, so the two sets commute. On the rider-applied copy, Acceptance lines 1–10 pass, with the same checker line, then `OK`. No rider Old overlaps an S-BETA Old.

### S-ALPHA-F6 riders (exact text; apply in this order)
**F6-a** (§15, I-FALLBACK's row; verdict 2's F6 (a)).
Old:
~~~~text
falls back, only to the previous meta, never two generations, with every check against it;
~~~~
New:
~~~~text
falls back, only to the previous meta, never two generations from the meta the open selects, with every check against it;
~~~~

**F6-b** (§2.2, `t_meta_copies`' tie variant; verdict 2's F6 (b)).
Old:
~~~~text
so that its `boot_id` is the older, so that the stale meta `k` sits beside the new one:
~~~~
New:
~~~~text
so that its `boot_id` is the older; the stale meta `k` then sits beside the new one:
~~~~

**F6-c** (§14 step 2; residual 1, verdict 2's F6 (c)).
Old:
~~~~text
those of them image `k` does not reference are the open's pending list (§13 step 6).
~~~~
New:
~~~~text
those of them image `k` does not reference are the open's pending list (§13 step 6). Step 2 reads meta `k−1`'s free set even when a read of image `k` has already failed in it, so that step 4a can fall back from a failure in step 2 (§14 step 4a item 1).
~~~~

**F6-d** (§14 step 4a, `t_fallback`'s unread variant; residual 1).
Old:
~~~~text
*Unread variant*, on the proven fixture rather than this one, with its fault on a page of image `k` that step 2 reads (a MapLeaf, a FreeSet page or a directory page): a hook fails one read of meta `k−1`'s free set in step 2 of the first open only:
~~~~
New:
~~~~text
*Unread variant*, on the proven fixture rather than this one, with its fault on a page of image `k` that step 2 reads (a MapLeaf, a FreeSet page or a directory page): a hook corrupts the bytes of one read of meta `k−1`'s free set (an integrity failure, §3.3) in step 2 of the first open only:
~~~~

**F6-e** (§1, "previous image"; verdict 2's F6 (d)).
Old:
~~~~text
It and its WAL span are kept intact, so that an open can fall back to it
~~~~
New:
~~~~text
It and its WAL span are kept intact, as I-TWO-GEN states, so that an open can fall back to it
~~~~

**F6-f** (§14, I-REDO-COMPLETE; F6 (d)).
Old:
~~~~text
The previous meta's image and WAL span are intact (I-TWO-GEN), and I-CKPT-DEP
~~~~
New:
~~~~text
The previous meta's image and WAL span are intact as I-TWO-GEN states, and I-CKPT-DEP
~~~~

**F6-g** (§14, post-sync damage, "A meta copy"; F6 (d)).
Old:
~~~~text
the open selects the previous meta, whose image and WAL span are intact (I-TWO-GEN), and its walk crosses
~~~~
New:
~~~~text
the open selects the previous meta, whose image and WAL span are intact as I-TWO-GEN states, and its walk crosses
~~~~

**F6-h** (§19 T4, "Two generations change one case"; F6 (d)).
Old:
~~~~text
the open selects the previous meta, whose image and WAL span are intact (I-TWO-GEN), and serves
~~~~
New:
~~~~text
the open selects the previous meta, whose image and WAL span are intact as I-TWO-GEN states, and serves
~~~~

**F6-i** (§14, post-sync damage, V6's sentence; residual 2).
Old:
~~~~text
Where a crash between step 4's two writes left one copy of the newest meta, a fault on that copy opens the meta before it, which recovers the same state (§2.2).
~~~~
New:
~~~~text
Where a crash between step 4's two writes left one copy of the newest meta, a fault that leaves no valid copy of it (lost, torn, unreadable, or misdirected to a page slot) opens the meta before it, which recovers the same state; a misdirect within its parity leaves it valid, and it opens; a misdirect into a place of the other parity is a misplaced copy newer than the newest valid meta, and the open fails with XX001 (I-META-SLOT, §2.2).
~~~~

**F6-j** (§2.2, the sentence after the table; residual 2).
Old:
~~~~text
a fault on that copy makes the open select the meta before it, whose image and WAL recover the same state (I-TWO-GEN).
~~~~
New:
~~~~text
a fault that leaves no valid copy of it makes the open select the meta before it, whose image and WAL recover the same state (I-TWO-GEN), and a misdirect of that copy into a place of the other parity fails the open with XX001 (I-META-SLOT).
~~~~

**F6-k** (§4.4, I-TWO-GEN; residual 3).
Old:
~~~~text
and has nothing to fall back to (§14 step 4a). That is not an error.
~~~~
New:
~~~~text
and has nothing to fall back to (§14 step 4a). That is not an error. Two states lie outside the condition above and rely on nothing more. A meta `k+1` that a crash between step 4's two writes left in one copy, before step 5's sync, is the durable image although it was never synced: image `k` and the WAL from meta `k`'s `wal_start_lsn` are then as this invariant states for a synced meta `k+1`, because step 6 of checkpoint `k+1` never ran. After a retire write (§14 step 4a), meta `k−1` is the durable image with no previous meta, and the WAL from its own `wal_start_lsn` stays intact, because step 6 of checkpoint `k` recycled only below it and nothing recycles before the boot checkpoint's step 6.
~~~~

**F6-l** (§14 step 4a, `t_fallback`'s unproven case; residual 5).
Old:
~~~~text
at or above meta `k−1`'s `wal_ckpt_end` and below meta `k`'s `wal_start_lsn`, so that only the walk from meta `k−1` reads it
~~~~
New:
~~~~text
at or above meta `k−1`'s `wal_ckpt_end` and below meta `k`'s `wal_start_lsn` (the fixture acks commits between checkpoints `k−1` and `k`, and no chunked transaction holds meta `k`'s `wal_start_lsn` below meta `k−1`'s `wal_ckpt_end` (§12.6), so that this range holds one), so that only the walk from meta `k−1` reads it
~~~~

**F6-m** (optional; §14 step 4a, `t_fallback`; residual 6). Two blocks, after F6-d. The first deletes the variant from the Retire bullet (its Old starts with one space; its New is empty). The second appends it to the Proven bullet.
Old:
~~~~text
 *Unread variant*, on the proven fixture rather than this one, with its fault on a page of image `k` that step 2 reads (a MapLeaf, a FreeSet page or a directory page): a hook corrupts the bytes of one read of meta `k−1`'s free set (an integrity failure, §3.3) in step 2 of the first open only: that open fails with XX001 (no previous image) and writes nothing, and the next open falls back with proof and recovers every acked commit.
~~~~
New:
~~~~text
~~~~
Old:
~~~~text
RO-WRITES fails the read-only form: it retires meta `k`.
~~~~
New:
~~~~text
RO-WRITES fails the read-only form: it retires meta `k`. *Unread variant*, with its fault on a page of image `k` that step 2 reads (a MapLeaf, a FreeSet page or a directory page): a hook corrupts the bytes of one read of meta `k−1`'s free set (an integrity failure, §3.3) in step 2 of the first open only: that open fails with XX001 (no previous image) and writes nothing, and the next open falls back with proof and recovers every acked commit.
~~~~

## The owner's points, for the record
The text is written for the recommended answer to O-1, O-2 and O-3, and for "no" on S-ALPHA-F4. Verdict 2's "The owner's points, in plain words" gives the full explanation. In brief:
- **O-1. Explicit rollback** (recommended: accept as written: a read-write open option naming the abandoned `ckpt_id`, only when the newest image fails at open, every check against meta `k−1` but the proof, 55000 otherwise).
  - **On "no, export only":** these change:
    - E2's 55000 row;
    - step 4a item 5's "Only an explicit rollback opens it read-write.";
    - the "Explicit rollback" paragraph;
    - `t_fallback`'s rollback sentences, with ROLLBACK-SKIPS-PREV-CHECKS and its §15 and §15.1 cells;
    - §14.2's "an explicit rollback opens it" and its 55000 row (V7's text);
    - I-FALLBACK's "and only an explicit rollback naming meta `k` opens it read-write";
    - the Changelog's "and an explicit rollback can open it";
    - the Status and Changelog phrases.
  - **On "also for WAL loss":** these change:
    - that paragraph's last sentence;
    - §14.1's WAL-loss remedy;
    - §14.2's WAL-loss and splice rows;
    - PLAN §10 item 8.
- **O-2. Two generations in every mode before measuring** (recommended: accept; measure at A-7e; §19 T7 the trigger).
  - **On "no":** a browser-mode condition is added to I-TWO-GEN (§4.4 and its row, now with V1's condition) and to I-FREE-SAFE, §4.4 item 4, §12.6's recycling sentence, §13 step 6, §14 step 2's protection, §14 step 4a item 1, §5.7's pending-free row, §14.2's fall-back rows, I-FALLBACK, R19, §19 T4's new bullet and T7's trigger.
  - No byte format changes.
- **O-3. A dropped sync of the newest meta now opens the previous meta** (for information; recommended: confirm).
  - **On "no":** two generations are dropped, which reopens the owner's decision (2). The sentences that change are §19 T4's "Two generations change one case", §14's "Both copies of the newest meta", and everything that rests on I-TWO-GEN and I-FALLBACK.
- **S-ALPHA-F4. A persistent 58030 on image `k`** (recommended: "yes" — an explicit rollback, and a read-only open's salvage from meta `k−1`, accept a 58030 on a page read through image `k`'s references; the automatic fall-back stays integrity-only, D35).
  - The text is written for "no", and nothing changes on that answer.
  - **On "yes":** these change:
    - step 4a's "Nothing else falls back: … a read error (58030) …" gains the exception;
    - the explicit rollback's precondition gains "or a read through its references fails with 58030";
    - §14.1's fall-back salvage and §14.2 gain the case (read-write 58030; an explicit rollback opens; a read-only open serves salvage from meta `k−1`);
    - I-FALLBACK's row changes;
    - `t_fallback` gains a case.
  - It is independent of residual 1's 58030 on meta `k−1`'s free set, which is S-ALPHA-F2's.

## Follow-ups
- **S-ALPHA-F1: unreadable meta places and the equal-`boot_id` tie.** Kept, unchanged.
  - An open whose read of a meta place fails may select an older meta and reuse the newer image's slots. When the read later succeeds, the newer meta reappears over a partly overwritten image, which is detected, not silent.
  - The three-fault equal-`boot_id` tie has the same mechanism.
  - Candidate: before its first write, a read-write open zeroes and syncs every meta place whose read failed. No format change; before A-7d.
- **S-ALPHA-F2: protect the previous image when its free set cannot be read.** Kept, and extended.
  - An open that cannot read meta `k−1`'s free set neither falls back nor protects image `k−1`. Its pass-2 and boot-checkpoint evictions may reuse those slots, and a later fall-back meets them as I-INTEGRITY failures (detected).
  - Candidate: protect every slot below meta `k−1`'s `slot_count` that image `k` does not reference, until the first step 6.
  - **Added by this gate:** the card must also decide whether a 58030 on meta `k−1`'s free set in step 2 is "no previous image" (carry on) or ends the open with 58030. The gate recommends the latter unless the candidate protection is adopted. Before A-7d.
- **S-ALPHA-F4: a persistent read error (58030) on the newest image.** Owner, with O-1. Kept; see above.
- **S-ALPHA-F5: I-META-SLOT under two generations.** Kept.
  - A misplaced in-flight copy of meta `k+1` with a crash is the only one-fault refusal of a recoverable store (verdict 2's model: 80 of 4,743 one-fault states). It fails closed, and it was so at 7.10.
  - Candidate: refuse only when a misplaced `ckpt_id` exceeds `k+1`. On adoption, F6-i's and F6-j's other-parity clause changes to "opens the meta before it".
  - It restates an invariant, so it needs a P card. Scope it with F1.
- **S-ALPHA-F6: riders for the next S-STORE card.** Rides in S-BETA, which merges next, with the Limits item above. The blocks are F6-a to F6-l, plus optional F6-m. If S-BETA cannot take them, they go to the stand-ins tidy card (S-15-STANDINS-2-F1 with S-15-RELABEL and PL-11-F2). None is blocking: each states what the design already does, or what the tests already pin.
