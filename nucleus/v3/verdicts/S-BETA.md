Card: S-BETA
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 13bc85f0-1b64-4435-9db1-f253fbdb8cdb (a fresh second gate; this agent wrote neither the card, the first verdict, nor any S-BETA commit)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: ea8be2ee782f23e88665dbd528649677807c4cbc
Mutants: spec text, none in code; each named mutant walked as text against its test on every owning row: TRUNC-NOT-DURABLE -> t_wal_trunc_durable (zeroing form: both runs; removal form: first run, equivalent on a writer with no first-run cut point, rider S-BETA-F3 R3), BOOT-DURABLE-AT-VALID-END -> t_boot_durable_start and its fall-back variant, WAL-START-PAIR-SPLIT-READ -> t_wal_chain_crash racing-chunk step, BOOT-PAIR-UNSET -> t_wal_chain_crash boot step, SEAL-STALE-HEAD -> t_wal_chain_crash two-batch step, cut after both batches are synced (A-6c, A-7d), SPLICE-MARKER-PAST-CUT -> t_wal_splice read-only, SPLICE-CUT-SAME-START-KEEP -> t_wal_splice read-only x same-start, DROP-NO-GCW -> t_drop_restart_floor A-7b direct check, A-7c (a), A-7d (a) (and (b), (c) once the image holds the frees or removal, rider R6), B-6 gc_w_used check, DROP-GCW-AT-WALK -> t_drop_restart_floor A-7b direct check and A-7c/A-7d (a), DROP-GCW-AT-TS -> t_drop_restart_floor A-7c (f), REPLAY-DROP-NO-GCW -> t_drop_restart_floor A-7d (d), (e) and B-6; gate's own: H1 page-local prune at a w >= S_ddl during the fixture (correct build) -> masks DROP-NO-GCW on any row unless excluded; excluded by RW-2's "in any form", parenthetical incomplete (rider R2), H2 zero-window publication before the AS OF (correct build) -> B-6's served check fails and its 72000 check is masked; B-6 only, the gc_w_used check still kills (rider R1), H3 replay raise left unconditional -> caught by t_drop_restart_floor (a) when c_d > T (REPLAY would mask DROP-NO-GCW; RW-1 removes that), H4 a W-from-meta-gc_w C-T0 build -> caught only by B-6's AS OF at S_ddl - 1 in (d), no named mutant (optional rider R7)
Followups: S-BETA-F1, S-BETA-F2, S-BETA-F3

## Verdict in brief

**MERGE-WITH-FOLLOWUPS.** The rework is exactly RW-1 to RW-8, byte for byte. It makes every ruling of verdict 1 true in the text, and the corrected behaviour is right. On every Phase A row, a correct build passes each test and each named mutant fails it.

None of the five residuals is blocking:
- **Residuals 1 and 2** are real fixture gaps. Residual 1 bites only at B-6. Residual 2 is already excluded by RW-2's "in any form"; only its parenthetical is incomplete.
- **Residual 3** is a missing "what to do": on the affected writers the uncovered mutant form is equivalent.
- **Residual 4** is ambiguous wording, not a wrong rule. The normative §12.2 is explicit.
- **Residual 5** is not a defect.

All of them ride in bundle γ as S-BETA-F3, with exact text below, together with one rider of this gate's own (R6) and one optional mutant (R7). S-BETA-F1 (before A-6c) and S-BETA-F2 (before A-7d) stand as verdict 1 wrote them.

The orchestrator's 8-byte call (S-BETA-O1) leaves nothing in 7.12 unsafe. The list of what changes is below.

## Gate run

- Scaffold extracted through the shallow `--no-local` clone: `6ae0c56596b69a5d6ff2c6731e85d0a5a2e20f60`. No `EXTRACT FAILED`.
- `bash "$fg/fresh-gate.sh" ea8be2ee782f23e88665dbd528649677807c4cbc`, `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-BETA-2`:
  - `fresh-gate: commit ea8be2ee782f23e88665dbd528649677807c4cbc`
  - `fresh-gate: base 6ae0c56596b69a5d6ff2c6731e85d0a5a2e20f60`
  - `fresh-gate: checks passed (4815 files hashed in 2.17 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - steps test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests each `EXIT=0`.
  - stderr: empty.
  - Literal last line: `GATE EXIT=0`.
- `$fg` removed afterwards.
- Worktree `~/.cache/nv3-cards/S-BETA` before this verdict: tip `ea8be2ee`, clean. History `6ae0c565` → `52bed8c8` → `1bba7099` → `9955fe10` → `5cb70935` (verdict 1) → `ea8be2ee` (the rework).

## Scope and bytes (each by this gate's own script)

- **Changed files.** `git diff 6ae0c565...ea8be2ee`: `docs/C-T0-txn-protocol.md`, `docs/S-STORE.md` (the Touch-only list) and `verdicts/S-BETA.md` (the gate's own). The rework commit touches `docs/S-STORE.md` only: +7/−7. C-T0 is byte-identical to `9955fe10`'s.
- **The rework is exactly RW-1 to RW-8.**
  - The script parsed the eight blocks out of `git show 5cb70935:nucleus/v3/verdicts/S-BETA.md`. Each Old occurs once in `9955fe10`'s `S-STORE.md`.
  - The result is byte-identical to `ea8be2ee`'s file (sha256 `233ad388…53ad`).
- **The card edits.**
  - The script applied RW-1 to RW-8, RW-A, RW-C1 and RW-C2 to the pre-rework card (`~/.cache/nv3-review/S-BETA.pre-rework.local.md`). Each Old occurs once. The result equals the current card byte for byte, with no other change.
  - The card's 65 Old/New pairs (63 headed blocks: E1–E44, E12a, E12b, F6-a–F6-m, C1–C4) were applied in order to `6ae0c565`'s files (sha256 `20283ae8…28e4`, `02d652cc…7aa`), with `<date>` = 2026-11-15. Each Old occurs once at its turn. Both results are byte-identical to `ea8be2ee`'s two files.
  - The pre-rework card reproduces `9955fe10` the same way.
  - **Ruling: the card edits were proper.** They are exactly what verdict 1's "How to apply" prescribed: the RW blocks applied to E2, E6 (twice), E9, E16, E31, E36 and E39, then RW-A, RW-C1 and RW-C2. They keep the card a faithful source for the spec text it ships.
  - The card prose that verdict 1 left to the author's choice (D2, D3, the mutation table) is unchanged. It still reads correctly.
- **No name lost.** Against 7.11 (`6ae0c565`) and against `9955fe10`, no `I-` token, `t_` token or all-caps mutant token is missing from `ea8be2ee`, in either file.
- **Acceptance** in a scratch clone at `ea8be2ee`: all 50 lines but `gate.sh` pass. The checker prints `148 rows; 211 mutants; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `42 new since 7.10 (31 at 7.11, 11 here), 42 of them judged on a Phase A row; 47 invariants, 45 tests`, `OK`. `gate.sh` is the gate run above.

## Verdict 1's rulings, re-derived at `ea8be2ee`

**1. `t_drop_restart_floor`: true in the text, and right.**

The fixture keeps every GC `w` below `S_ddl` (RW-2). The served check moved from `S_ddl` to the reopened `W` (RW-3, RW-7). Below, `w` is the published `W` at the mark (`> S_ddl`) and `c` is the drop-cleanup commit's ts. On a correct build `w ≤ c − 1`, because `W ≤ visible_ts < c` while `c` applies.

*The variants on a correct build:*
- **(a)** The image holds `dropping`, and meta `gc_w ≥ w`. If `c > T`, pass 2 finds `dropping` and, under RW-1's condition, does not raise. So the floor is `≥ w`.
- **(b), (c)** With a checkpoint after the mark, the image counts the mark. Without one, pass 2 replays the mark from `live` to `c − 1 ≥ w`. Both pass.
- **(d)** Replay from `live` gives `c − 1`. The boot meta records it.
- **(e)** As (d), and nothing is written. `W` never advances in a read-only open (C-T0 §3.7 step 1).
- **(f)** `w ≤ W ≤ M ≤ T`, so the meta write succeeds.
- **AS OF.** The reopened `W` is `gc_w_used ≥ w > S_ddl` and `≤ T ≤ visible_ts`. So the snapshot at `S_ddl − 1` gets 72000, and the one at the reopened `W` is admitted, before any publication (residual 1).

*Under each mutant, on each owning row:*

| Mutant | A-7b (direct, step 1 only) | A-7c (through the flip) | A-7d (through recovery) | B-6 (full form) |
|---|---|---|---|---|
| **DROP-NO-GCW** | `gc_w_used` stays at S3's value `< S_ddl < w`: fails | meta `k`'s `gc_w < w` in (a): fails | (a): image `dropping`, meta `gc_w < S_ddl`; pass 2 finds `dropping` and does not raise: fails | the direct `gc_w_used ≥ w` check fails, whatever publication does |
| **DROP-GCW-AT-WALK** | no walk step, no raise: fails | (a) has the walk held, so meta `gc_w < w`: fails | (a), as at A-7c | not judged |
| **DROP-GCW-AT-TS** | not judged | (f): `c − 1 > M = T`, so the meta write fail-stops: fails | not judged | not judged |
| **REPLAY-DROP-NO-GCW** | not judged | not judged | (d), (e): `gc_w_used` = meta `gc_w < S_ddl`: fails | the direct check fails, as at A-7d |

Notes on the table:
- *DROP-NO-GCW at A-7d, variants (b) and (c).* These fail too when the image holds the frees or the removal. They pass on a build whose replay raise is separate code, if the run has no checkpoint after the mark (rider R6). (a) alone kills the mutant.
- *B-6.* On top of the direct check, the AS OF at `S_ddl − 1` is admitted, if it is taken before publication (R1).

Verdict 1's claim holds: every mutant fails on a Phase A row. The masking verdict 1 found is closed by RW-2, provided "in any form" is read to include the page-local prune, as it must be (residual 2).

**2. The sealing rule: true and right.**
- RW-4 names the record reserved just before, with its `crc` taken once sealed. That agrees with §12.2's chain-head definition (the record reserved last) and with the fold-at-reservation note.
- SEAL-STALE-HEAD ("a chain head read before the previous batch's last record was sealed") is the negation of the rule.
- The rule's grep (Acceptance line 11) passes.

**3. The batch test: true and right.**
- RW-5 and RW-8 make the test order the writes and syncs. A correct writer can do that while it seals in LSN order: it seals batch 1, then seals batch 2, writes and syncs batch 2, then writes batch 1. `t_ack_prefix` already needs that control.
- The added cuts after both batches are synced are what kill SEAL-STALE-HEAD. Batch 2's first record carries a non-final `prev_crc`, which A-6b's walk reports as a splice. A cut while batch 1 is unwritten stops the walk before the check.

**4. The conditional raise: true and consistent.** §6.8 step 1 (RW-1), §10.5 "Drops" and §14 step 4 all raise only on `live` → `dropping`.
- Normal operation never finds `dropping`: a DropTree whose walk runs does nothing.
- A replay that finds `dropping` or absent follows an image whose `gc_w` already counts the mark.
- An unconditional replay raise would have masked DROP-NO-GCW in (a) when `c > T` (H3). RW-1 removes that mask as well.

**5. `t_wal_trunc_durable` on a build that removes segment files: true and right.**
- *A lazy writer.* The first run cuts while `F` has not been recreated and `wal/` has not been synced. Under the removal form, the unlink reverts and `F` returns with `b`'s records. `b′`'s differing records then make the chain mismatch at the hop, which gives XX001. On a correct build `F` stays removed.
- *The zeroing form* fails the first run. It also fails the second, on a writer whose making `F` durable does not `sync_data` its zeros.
- *A writer that makes `F` durable before the `SegmentEnd`'s batch is durable* is residual 3.

## The five residuals, ruled

1. **Zero-window publication races the served AS OF. Real; B-6 only; not blocking.**
   - With a zero window, C-T0 §9.1 can publish `W` up to the recovered `visible_ts` right after a read-write open (§9.4 does not say when publication runs). A correct build then gives 72000 at the old reopened `W`.
   - The same race also masks the 72000 check at `S_ddl − 1` against a C-T0 build that restarts `W` from meta `gc_w` (H4).
   - A-7d's row leaves the AS OF conditions to B-6, so no Phase A row is affected. A-7b and A-7c have none.
   - At B-6 the direct `gc_w_used` check still kills both named mutants. (e) is immune: a read-only open never advances `W`.
   - Rider R1.
2. **The page-local prune. Real; not blocking.**
   - §10.3 prunes "using a `w` that satisfies §10.1", and §10.5's `gc_w_used` counts every GC mutation. C-T0 §9.4 lists the prune among the GC jobs.
   - So the prune is a GC mutation, and RW-2's "in any form" already excludes it. The rule as stated is right and passable: keep leaves from splitting or being evicted once `W` passes `S_ddl`.
   - Only the parenthetical, B-6's row and the card's notes are incomplete. At B-6 the drop-cleanup commit's own catalog delete can split a leaf (card D3 step 2), so the omission would mislead there first.
   - Rider R2.
3. **`t_wal_trunc_durable`'s first run on an eager writer. Real omission; not blocking.**
   - On a writer that makes the next segment durable (syncing `wal/`) before the `SegmentEnd`'s batch is durable, no first-run cut point exists.
   - There the removal form is equivalent: that sync persists step 5's unlink before any walk can cross that `SegmentEnd`.
   - The zeroing form is judged by the second run, or is likewise equivalent where the writer's "durable" includes a `sync_data` of `F`.
   - The test cannot fail on a correct build. The text only lacks the instruction to skip. Rider R3.
4. **Old sealing wording. Ambiguous, not wrong; not blocking.**
   - "The chain head the previous batch left" most naturally means the head the previous batch's reservations left, which is its last record: the right record.
   - Only a temporal reading (the head at the moment that batch finished) gives the wrong one. That differs from 9955fe10's §12.2, which explicitly took the head "once the previous batch's last record is sealed".
   - The I-WAL-CHAIN row cites §12.2, which is now explicit.
   - Rider R4 for the row. The 7.12 Changelog entry stays as history, and γ's own entry records the wording fix.
5. **The B-3 row's "recovered `gc_w`". Not a defect.**
   - B-3's row is real GC with no drop, so `gc_w_used` as recovery leaves it is the recovered `gc_w`.
   - Rider R5 (optional), for uniform wording.

## Bundle γ riders (S-BETA-F3; exact text; each Old occurs once in `ea8be2ee`'s file, checked by script)

**R1 + R2 + R6** (§10.5 `t_drop_restart_floor`, three disjoint Olds in one paragraph):

Old:
~~~~text
with no GC mutation at a `w ≥ S_ddl` in any form (S3's GC `gc_w_used` stays below `S_ddl`, and B-6 holds B-3's sweep off)
~~~~
New:
~~~~text
with no GC mutation at a `w ≥ S_ddl` in any form (S3's GC `gc_w_used` stays below `S_ddl`; once `W` passes `S_ddl` no leaf splits and no current-epoch leaf is evicted, so that no page-local prune (§10.3), which is a GC mutation, runs at such a `w`, the drop-cleanup commit's own catalog delete included; and B-6 holds B-3's sweep off)
~~~~
Old:
~~~~text
(b) a crash part-way through the walk; (c) a crash after the walk's last step;
~~~~
New:
~~~~text
(b) a checkpoint whose image holds the mark and some freed pages, then a crash part-way through the walk; (c) a checkpoint after the walk's last step, whose image no longer holds the entry, then a crash;
~~~~
Old:
~~~~text
succeeds and reads a catalog that does not name the tree (one at `S_ddl` gets 72000 as well, since `W > S_ddl`; C-T0 v8 §3.6); in (f) the meta write succeeds.
~~~~
New:
~~~~text
succeeds and reads a catalog that does not name the tree (one at `S_ddl` gets 72000 as well, since `W > S_ddl`; C-T0 v8 §3.6). After a read-write reopen both AS OF snapshots are taken before `W` is first published above the floor, or with a non-zero AS OF retention window, which holds `W` at the floor for one window length (C-T0 v8 §9.1): with a zero window a publication can raise `W` to the recovered `visible_ts` at once. In (f) the meta write succeeds.
~~~~

**R1 + R2, B-6's row** (§15.1):

Old:
~~~~text
| full: B-6's drop cleanup at a published `W`, with B-3's sweep held off, B-4a's catalog and B-3's restart floor; after each reopen the AS OF snapshots at `S_ddl − 1` (72000) and at the reopened `W` (served, without the tree) |
~~~~
New:
~~~~text
| full: B-6's drop cleanup at a published `W`, with B-3's sweep held off and no page-local prune once `W` passes `S_ddl` (no leaf split, no eviction of a current-epoch leaf), B-4a's catalog and B-3's restart floor; after each reopen, before `W` is first published above the floor (or with a non-zero AS OF retention window), the AS OF snapshots at `S_ddl − 1` (72000) and at the reopened `W` (served, without the tree) |
~~~~

**R3** (§15 I-BOOT-DURABLE row, `t_wal_trunc_durable`):

Old:
~~~~text
and in a second run after `b′` has written records into `F` without a sync. The open must succeed
~~~~
New:
~~~~text
and in a second run after `b′` has written records into `F` without a sync. On a writer that makes the next segment durable, syncing `wal/`, before the `SegmentEnd`'s batch is durable, the first run has no cut point and is skipped: there the removal form is equivalent, since that sync persists step 5's removal before any walk can cross that `SegmentEnd`. The open must succeed
~~~~

**R4** (§15 I-WAL-CHAIN row):

Old:
~~~~text
a batch is sealed with the chain head the previous batch left,
~~~~
New:
~~~~text
a batch's first record is sealed with the previous batch's last record's final `crc` (§12.2),
~~~~

**R5** (§15.1, I-GCW's B-3 row; optional):

Old:
~~~~text
full (restart floor: real GC's `gc_w_used`, a restart, `W` starting at the recovered `gc_w`)
~~~~
New:
~~~~text
full (restart floor: real GC's `gc_w_used`, a restart, `W` starting at `gc_w_used` as recovery leaves it, which is the recovered `gc_w` when pass 2 replays no DropTree mark)
~~~~

**R7** (C-T0 §3.7 step 3 and §11.2's I-W-FLOOR row; optional, a C-T0 edit in γ. No named mutant pins C1's new term, though B-6's AS OF at `S_ddl − 1` in (d) catches it).

Old:
~~~~text
   - Mutant **W-FLOOR-ZERO**: `W` restarts at 0, so an AS OF at `t < gc_w` is admitted and reads a pruned state.
~~~~
New:
~~~~text
   - Mutant **W-FLOOR-ZERO**: `W` restarts at 0, so an AS OF at `t < gc_w` is admitted and reads a pruned state.
   - Mutant **W-FLOOR-META-ONLY**: `W` restarts at the recovered meta's `gc_w`, not at `gc_w_used` as pass 2 leaves it, so after a replayed drop an AS OF below the retiring DDL is admitted (S-STORE `t_drop_restart_floor` (d), B-6).
~~~~
Old:
~~~~text
(the recovered meta's `gc_w`, raised by replayed DropTree marks; S-STORE §10.5) | B-3 | W-FLOOR-ZERO |
~~~~
New:
~~~~text
(the recovered meta's `gc_w`, raised by replayed DropTree marks; S-STORE §10.5) | B-3 | W-FLOOR-ZERO, W-FLOOR-META-ONLY (judged at B-6) |
~~~~

## S-BETA-O1: the 8-byte watermark field in the DropTree op

**Is anything in 7.12 wrong once the field exists? No, nothing is unsafe.**
- Let `w_op` be the published `W` that the drop-cleanup commit records, read after the drop job saw `W > S_job`. Then `S_ddl < w_op ≤ W` at the record's build `≤ visible_ts < c`, so `w_op ≤ c − 1`. And `c ≤` the recovered frontier, which is `T` at the boot checkpoint.
- 7.12's `c − 1` is therefore always `≥ w_op`. It is conservative: it refuses more and never serves wrong data.
- **Statements that become false once the field lands:**
  - §10.5 "Which `w`": "has no published watermark".
  - The *Cost* bullet: "would remove the cost, at the price of a WAL format change".
- **A test bound that fails a field build:** "(at least `c − 1` in (d) and (e))". It must change in the same card.
- **No C-T0 sentence is wrong.** C1 and C3 name no replay value. C2 ("raises it to the published `W`") stays true. C4 is history.

**Recommended shape (γ's call; both are safe):** one value in both paths. The mark raises `gc_w_used` to its op's `w`, in normal operation and in replay, so the image and the replay agree and S3 supplies one input. Keeping the published `W` at the mutation in normal operation and `w_op` in replay is also sound.

**What γ must change in 7.12 (each Old below occurs once in `ea8be2ee`):**
1. §12.3 ops: `DropTree{tree_id}` becomes `DropTree{tree_id, w u64}`, with a sentence saying `w` is the published `W` read after the drop job saw `W > S_job`. A-6a's codec and fuzz targets then carry it.
2. §6.8 step 1: "or, for a DropTree that recovery replays, the commit ts it is replayed at minus one" becomes "… the `w` its op carries", or, with the one-value shape, the whole clause becomes "to the `w` its op carries (§12.3), the watermark C-T0 v8 published when the drop-cleanup commit was built".
3. §10.5 Bound: "a mark that pass 2 replays raises it to `c − 1`, where `c` is the commit ts the mark is replayed at, and `c ≤` the recovered apply frontier" becomes "… raises it to its op's `w`, which is `< c ≤` the recovered apply frontier".
4. §10.5 "Which `w`": the replay sentences ("A mark that pass 2 replays … has no published watermark, so it uses `w = c − 1` …, so `c − 1 > S_ddl` too, and `c − 1` lies below the recovered apply frontier") are rewritten on `w_op`. "Not `c − 1` in normal operation …" stays as DROP-GCW-AT-TS's rationale.
5. §10.5 *Cost*: delete it, or replace it with the field's cost (8 bytes per drop).
6. §10.5 mutants:
   - DROP-GCW-AT-TS is unchanged ("commit ts minus one" in normal operation).
   - REPLAY-DROP-NO-GCW is unchanged.
   - **New mutant REPLAY-DROP-AT-TS**: replay raises to `c − 1`, ignoring the op's `w`. It is safe but costly. A-7d's (d) kills it with an exact check: after the reopen, `gc_w_used = max(meta gc_w, w_op)`, with a fixture where `w_op < c − 1`. B-6 adds an AS OF in `[w_op, c − 1)` that must be served.
   - **Optional mutant DROP-OP-W-ZERO**: the codec drops the field. A-6a's round trip catches it, and so does (d).
7. `t_drop_restart_floor`:
   - "(at least `c − 1` in (d) and (e))" becomes "(exactly `max(meta gc_w, w_op)` in (d) and (e))".
   - Variant (d)'s fixture gains `w_op < c − 1`.
   - (f) is unchanged. A-7c/A-7d's S3 supplies `w_op` in place of "the published `W` the mark reads", or alongside it with the two-value shape.
8. §14 step 4 DropTree: "it raises `gc_w_used` to `c − 1`, where `c` is the commit ts it is replayed at" becomes "… to its op's `w`". Acceptance line 17 of this card greps the old sentence, so γ replaces that line.
9. §15 I-GCW row: "and one that pass 2 replays to the commit ts it is replayed at minus one (§14 step 4)" becomes "… to its op's `w` (§12.3, §14 step 4)", and the mutant list gains REPLAY-DROP-AT-TS.
10. §15.1 rows: A-7d's I-GCW row gains REPLAY-DROP-AT-TS, and A-6a's codec row gains the field.
11. **Unchanged:**
    - DROP-NO-GCW, DROP-GCW-AT-WALK, and variants (a)–(c), (e)'s writes nothing, (f).
    - §14.1 "Dropped trees". Its "pass 2's replay of `c_d` raised `gc_w_used` above `S_ddl`" still holds.
    - C-T0 C1–C4. C2 may optionally add "(the value its op records, S-STORE §12.3)".
    - 7.12's status line and Changelog (history).

## Follow-ups

- **S-BETA-F1** and **S-BETA-F2**: unchanged from verdict 1 (History below). F1 must merge before A-6c, and F2 before A-7d. Both ride in bundle γ, with S-BETA-O1's field.
- **S-BETA-F3: this gate's riders R1–R7** (exact text above). A P spec card: bundle γ.
  - R1–R6 must land before B-6's test is written, and R3 before A-7d. R4 and R5 are wording.
  - R7 is optional, and needs C-T0 in γ's Touch-only list.
  - None changes behaviour.
  - R6 (variants (b) and (c) name their checkpoint) is this gate's own: without it, DROP-NO-GCW's claimed failure of (b) and (c) depends on whether the replay raise shares code with the mark. (a) kills the mutant on every row regardless.

## What the follow-on plan card must change

Verdict 1's seven items stand, with item 6 replaced. Changes and additions:
1. **Item 6 (O-2).** Record the orchestrator's call of 2026-11-15 (QUEUE S-BETA-O1): the DropTree op carries the 8-byte watermark. It lands in bundle γ.
   - **A-6a's Depends gains γ.** A-6a fixes the op encoding.
   - A-6c's Depends also gains γ, through S-BETA-F1.
2. **A-7c and A-7d.** `t_drop_restart_floor`'s fixture keeps every GC `w` below `S_ddl`, the page-local prune included: no leaf split and no current-epoch eviction once `W` passes `S_ddl` (R2). A-7d also gets REPLAY-DROP-AT-TS's exact check once γ lands.
3. **A-7d.** `t_wal_trunc_durable`'s first run is skipped on a writer that makes the next segment durable before the `SegmentEnd`'s batch is durable (R3).
4. **B-6.** The full form holds B-3's sweep off and the prune off once `W` passes `S_ddl`. It takes both AS OF snapshots before `W`'s first publication after a read-write reopen, or with a non-zero retention window (R1).
5. **QUEUE.**
   - S-BETA closes S-1-F6, S-1-F5-F1, S-1-F4 and S-ALPHA-F6.
   - New row S-BETA-F3 (riders R1–R7) → bundle γ.
   - γ = S-BETA-F1 + S-BETA-F2 + S-BETA-F3 + S-BETA-O1 + the stand-ins tidies, before A-6a and A-6c.

## The owner's points, in plain words

1. **The time-travel cost after a crash is going away.** 7.12 is safe as merged: after a crash that replays a table drop, it can refuse more AS OF queries than needed, but it never returns wrong data. The 8-byte field you approved removes that cost. It has to land in the next spec card, before the WAL record format is coded (card A-6a). Nothing written before then depends on it.
2. **One silent-loss path from draft 7.8 is still open:** a large transaction starting at the exact moment a checkpoint flips (S-BETA-F1). It is fixed in the same next spec card, which must land before card A-6c, so no code is written against the gap.
3. **Nothing else needs you.** The remaining items are test-fixture wording for later cards.

## History

- **Verdict 1: REWORK, `5cb70935` on `9955fe10`** (gate P, claude-opus-5-5, session 2ec40280-e518-4700-9c44-6352eaa467c8, GATE EXIT=0).
  - It found the three fixes right as durability design, and six defects in the card's tests and sentences:
    1. `t_drop_restart_floor`'s served AS OF at `S_ddl` could not pass.
    2. GC at a `w ≥ S_ddl` masked DROP-NO-GCW, DROP-GCW-AT-WALK and REPLAY-DROP-NO-GCW.
    3. §12.2's sealing rule named the wrong record.
    4. `t_wal_chain_crash` asked for batches "sealed out of order".
    5. `t_wal_trunc_durable` could not kill the removal form of TRUNC-NOT-DURABLE.
    6. §6.8 step 1's raise was unconditional, against §10.5 and §14 step 4.
  - It gave the fixes as RW-1 to RW-8, RW-A, RW-C1 and RW-C2.
  - Follow-ups:
    - **S-BETA-F1:** step 0 reads §12.6's open-chunk set in the same entry as the append position, and a first `Chunk` enters the set inside its own reservation's entry. That closes a silent loss of an acked chunked commit present since 7.8. The append critical section is also a leaf: no lock, no I/O, no wait. Before A-6c.
    - **S-BETA-F2:** wording. §14 step 3a's splice-cut overclaim, §2.2's "last record appended", START-SEG-AS-END's "a lost meta". Before A-7d.
  - Owner point: `c − 1` against the 8-byte field.
- **The rework, `ea8be2ee`.** RW-1 to RW-8 applied by script to `docs/S-STORE.md` (+7/−7). The same blocks were applied to the card's E2, E6, E9, E16, E31, E36 and E39, together with RW-A, RW-C1 and RW-C2. No count, name or row changed. Drafts stay 7.12 and 8.5.
- **Verdict 2 (this one): MERGE-WITH-FOLLOWUPS** on `ea8be2ee`.
