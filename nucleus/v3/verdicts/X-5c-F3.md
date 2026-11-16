Card: X-5c-F3
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 8e5e1fb8-1517-4a74-8b5a-6abfd1af8713
Date: 2026-11-01
Gate-exit: GATE EXIT=0
Commit: fe798c0aab6e52f010bc27f1d71990edc466f351
Mutants: M-CXXa next_lpid decrement restored -> cxx_highest_lpid_free_never_shrinks_next_lpid + cxx_highest_lpid_free_never_shrinks_next_lpid_rounds, M-CXXb G3 probe reads open's error -> cxx_a_crash_at_any_recovery_yield_is_never_corruption, M-G8a poison mark removed -> g8_poison_ends_the_workload_instead_of_livelocking, M-G8b harness Poisoned arm removed -> g8_poison_ends_the_workload_instead_of_livelocking, (own) G-1 replay walk swallows a crash at its segment probe (Err -> break 'outer) -> UNCAUGHT (full release suite 158/158 pass), (own) G-2 G8 inverted (read ops poison, gated ops do not) -> UNCAUGHT (full release suite 158/158 pass)
Followups: X-5c-F3-F1, X-5c-F3-F2, S-1-F3

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F3 fe798c0a…`.
- **Checks before trusting it:**
  - the shared hooks dir (`~/Workspace/neutron/.git/hooks`) holds only `*.sample`;
  - no `filter.*`, `core.hooksPath`, `core.attributesFile` or `core.fsmonitor` config;
  - no `info/attributes` (common or per-worktree);
  - the only `.gitattributes` is the repo-wide line-ending file, which sets no filter;
  - `git write-tree` equals the commit's tree, and `status --porcelain` is empty;
  - all 284 `nucleus/v3` blobs match `git hash-object --no-filters` byte for byte.
- **The gate.** `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F3 bash scripts/gate.sh` ran in the foreground (3 m 39 s; the target dir was pre-seeded with a copy of gate-X-5c-F2's). Every STEP gave EXIT=0 (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests), ending `GATE EXIT=0`. The log is `~/.cache/nv3-review/runlogs/X-5c-F3-gate.gatesh.log`.
- **Mutants and diagnostics** ran in the same checkout after the gate. Each was an exact-string patch (drivers in `~/.cache/nv3-review/x5cf3-gate/`, outside every worktree) and was reverted with `git checkout`. The checkout was clean at the end, and it has been removed. Diagnostics were stderr-only (`E4_GATE_DIAG`, `E4_GATE_WAL`) and were reverted before the mutants ran.
- **Targeted seeds** (6149, 21, 1897, 2437, 4718, 2927, 4378, 7528, 8728) ran one seed each, under `ulimit -v 25165824`. No campaign and no verify ran.

## Provenance of the Acceptance campaign (request item 4): OK

`X-5c-F3.acc.prov`:
- `commit=fe798c0a…` is the branch tip;
- `status_lines=0`, `exit=0`, `verify_head=fe798c0a…`, `verify_exit=0`;
- `json_sha256=efb7835ec3b1e4c00495f74f7ac3496862ec34b26da42612daa70323e73906c5` equals `sha256sum X-5c-F3.acc.campaign.json`.

The run took 00:34:57–00:51:19 and verify 00:51:19–01:08:09. Both started at load 1.1. The verify output reads `VERIFY OK (--no-quotas) seeds=10000 digests reproduce`.

Final lines:
```
E4 SEEDS=10000 O1_FAIL=2654 O2_FAIL=43 O3_FAIL=559 DEADLOCK=0 PANIC=0 O1F_FAIL=2665 MONITOR=0
E4 FOLD FAILING_SEEDS=955 FAILING_ROUNDS=914 TWIN_FAILING_ROUNDS=71 DEFECT_SEEDS=0
```
Quotas missed: Q4 2/1000, Q7 167/200, Q11 0/100, Q12 1/100, SIZING 15,004 misses.

Step 0 was also checked: b9dd5a9a commits only `results/campaign.json`, its blob hashes to F2's `json_sha256` (`56e47fb2…cdf34033`), and the file is unchanged since.

## Scope and frozen surfaces

`git diff card/v3-X-5c...HEAD` touches 10 files, all under `spikes/e4-recovery/`. The oracle, `model.rs`, `taint_ranges`, the fault and crash model, D17's cells, X5-R14's fix and K are untouched.

`src/sim/env.rs` gains `E4Env::exists`. This is a new env call, not a change to the fault or crash model. It does add a yield point, so recovery's nested-crash draws shift. That is the mechanism behind 6149's new outcome (below).

There is no `unsafe`, no non-test `unwrap`/`expect`/`panic`, no hash-container iteration and no wall-clock. New pub items have docs.

## The classes (request item 1)

Counts are F2's file → F3's file. They are re-derived from the JSON with the README's field definitions, except where noted.

| Class | F2 → F3 | Gate classification |
|---|---|---|
| C-XX, class C | 130 → 64 | **Two causes fixed and pinned:** (A) `prims::free_lpid` shrank `next_lpid`, a prototype deviation from §4.2/§4.3; (B) the G3 probe and the two other segment probes read `open`'s error as "missing", which swallowed a delivered crash (an env/store instrumentation defect). Both are confirmed by caught mutants. **The remaining 64 rounds (43 at AmcheckIi, 21 at Open; e.g. 531 r2, 564 r2, 841 r1, 906 r0, 943 r0, 1252 r0) are neither root-caused nor escalated with evidence.** The worker could not see them, because they exist only in the Acceptance file → X-5c-F3-F1. |
| C-XX shape, MP/MW O-3(c) | 15+13 → 17 MP + 5 MW | **Harness/oracle defect, provisionally CONFIRMED** (cross-round inherited damage; the worker's 7367 evidence is accepted). The frozen O-3(c) attributes an XX001 only to the same round's fault targets, so a correct detection of an earlier round's unrewritable damage is called a false positive. 14 of the 17 MP rounds are G8 `poison` rounds (formerly livelocks, now judged) → X-5c-F3-F2 (P oracle ruling). |
| C-DIV | 424 → 388 | **Partly root-caused by the gate: a prototype deviation from §12.4/I-REDO-COMPLETE.** `wal::wal_writer_step` can run in two tasks at once: the writer task, and a committer helping from `wal_wait_durable`. Each takes its own batch, and `wal.written`/`wal.durable` advance with `max`. Seed 21 r0, traced: task 1 takes [225622, 226759), which holds ts=14, and has not written it at the crash. Task 2 takes [226759, 228068), which holds ts=15, writes and syncs it, and moves `durable` from 225622 to 228068, so ts=15 (SyncOn) is acked over a hole. §12.5's stop at the hole then loses it, and G2 zeroes it. The signature `frontier < lo` marks 93 of the 388 C-DIV rounds (88 (b), 5 (a)). The other 295 ((b) 144, (c) 93, (d) 58; r2 266/1,357) need a second mechanism → X-5c-F3-F1. Also, `harness::derive_written_acked` credits written/acked with `max` extents, not a contiguous prefix. This is harmless once the writer is serialized, but F3-F1 should cross-check model acks against client-observed acks. |
| MP-LIVELOCK | 41 → 0 | **G8 applied; the root cause is CONFIRMED as X5-R16's lead.** An XX001 in a gated op did not poison. All 64 `poison` rounds in F3's file are MP: 50 pass and 14 fail O-3(c) (row 2). No class C round poisoned. Eviction never reads, so G8 adds no path that could hide a class C XX001. **The G8 pin is weak:** gate mutant G-2 (the poison condition inverted) passes every test, so nothing pins "a read never poisons; a gated op does". |
| MP-A | 7 → 7 | ESCALATED as X5-R30 requires, and unchanged. Ruled below. |
| MW-ORPHAN | 446 → 451 | **Design finding, CONFIRMED** (below). |
| MP-E shape C | 6 → 5 | 9717 (separators out of order / lpid reachable twice) now passes. That fits LPID reuse, i.e. C-XX (A); the worker did not claim it. 688, 3718, 4858, 5238 and 5638 stand, escalated with F2's per-lpid table. The amcheck precision fix stays unmade (X5-R29). |
| Other MP | 4 → 1 | 6028 r0, O-1(b), recovery `Ok`, no O-3: the C-DIV shape in MP. 7388, 9548 and 8788 now pass. |
| Other MW | 26 → 13 + 1 new | 10 O-3(d)+O-1(a), 3399, 2019, 4049: the MW-ORPHAN family (published-set and frontier shapes); they go with S-1-F3. **New: 6149 r1, an MW served corruption** (below). |
| Twins | 103 → 71 (26 MP / 45 MW) | Echoes of the classes above. Classifying them as such is accepted. |

**New in F3's file: 6149 r1, a served corruption in class MW. It is a prototype deviation from §12.5/G2, exposed by F3's extra yield point** (`nested` 0 → 1).

In `ckpt::recover`, a `SegmentEnd` hop does not advance `valid_end`. When the next segment is missing, `valid_end` stays at the SegmentEnd's own start (lsn 62525). G2 then zeroes the very record the reader just accepted ([62525, +3011)). The nested crash drops those zeros before the sync, so life 2 re-accepts the surviving SegmentEnd. That accept is tainted against the dropped zeros, and the sim records it as served.

§12.5 puts the valid end after the hop, at the next segment's first LSN. The fix goes to X-5c-F3-F1.

**Reporting hazard.** The frozen oracle's `o3` text is last-writer-wins: (a) is set first, and (b), (c) and (d) overwrite it. 6149's text is O-3(d), so the README's text-based class definitions file this serve under MW-ORPHAN. **Every class table from F4 on must split out `served=true` rounds first.** For the same reason, an `O-3(a)` text can hide an (e) failure, because (e) is recorded only when nothing else failed.

**The record has two errors.** Neither is a blocker.
- The step-0 class-table block is duplicated after step 8.
- The MW-ORPHAN table's "opened T (lo)" column shows the model's `lo`, not the opened meta's T. In every one of the five rounds, the opened T equals the frontier (39: 5, 59: 4, 109: 4, 149: 4, 209: 8, from the worker's own `orph.*.err`).

**Nothing was hidden by loosening an oracle.** The oracle and K are byte-unchanged. No seed was filtered. The fixes move counts only through the mechanisms named.

## X5-R30 (c): RULED (request item 2)

The gate's instrumented runs show that all seven rounds served only during recovery: `workload_served=[]` and `r_taint=false` in every one.
- 1897 and 2437 served exactly page slot 4, and K={4}.
- 4718 served exactly slot 8, with K={8} and R={12,13}.
- 2927, 4378 and 7528 served meta slot 1 (4096) after a rollback.
- 8728 served meta slot 2 (8192) after a rollback.
- In each of those four meta rounds, the other meta slot was destroyed by the round's fault, and the served slot's newest write did not survive.

**Ruling.**
1. **(K serves) O-3(a) moves.** A served range does not count as served corruption if all of the following hold:
   - it lies wholly inside slots of K (X5-R28 (a));
   - it was accepted after open selected the rolled-back meta (in a recovery life or in extraction, never in the workload pass);
   - the round has no R-taint and no uncovered accept.

   **Why.** K's condition 2 proves from the DWrite history that the bytes are V_k(s), byte for byte: the opened image's own content, accepted through that image's own reference. The sim calls the range tainted only because a post-anchor reuse write never landed. That is the proxy flaw X5-R28 corrected for completeness, and the same correction applies to the same slots here.
2. **(Meta serves) O-3(a) moves, on the same principle, as follows.** A served range does not count as served corruption if:
   - it is exactly the opened meta's slot (1 or 2);
   - the round meets X5-R5's conditions 1 and 2 (class MP, a meta-slot fault, and `meta_rollback`);
   - the accepted bytes equal a DWrite the store issued to that slot and a completed `sync_all` covered.

   **Why.** X5-R5 pre-registered that open selecting an older durable meta under a meta-slot fault is a legitimate outcome. The meta it selects is genuine. Taint fires only because a newer write to that slot was dropped by F-CRASH or reverted by the fault. The rollback's consequences remain judged, unchanged, by R, K, O-1 and O-3(e).
3. **Nothing else moves.** R-taint, any serve outside K or the opened meta slot, any serve in the workload pass, any uncovered accept, and every MW serve (6149 included) stay served corruption.
4. **Applying it from recorded fields.** The current record cannot apply the ruling exactly:
   - `sk` is vacuously true on an R-taint-only round and is computed over workload-pass serves too;
   - the record carries no meta-serve field;
   - an `O-3(a)` text can hide an O-3(e) failure.

   So **X-5c-F3-F1 adds three diagnostic record fields**. They are never oracle inputs, and the oracle stays frozen:
   - `skx`: rule 1's predicate, exact;
   - `smx`: rule 2's predicate, exact;
   - `pass_na`: the round's verdict re-evaluated with the existing `OracleCfg::no_taint` knob, recorded only when `served ∧ (skx ∨ smx)`.

   The final file is judged with no re-run: a served round passes iff `(skx ∨ smx) ∧ pass_na`. RESULT item 6 must list this as a post-data amendment (X5-R30 (c)), with the rounds it moved.
5. **In F2's and F3's files,** the seven rounds stay recorded as failures, since the fields are absent. The gate's evidence says that 1897, 2437 and 4718 satisfy rule 1 and 2927, 4378, 7528 and 8728 satisfy rule 2's location conditions; the byte check remains `smx`'s to establish. Whether each then passes depends on `pass_na`.

## Design finding: MW-ORPHAN, G8 and post-sync WAL damage (request item 3): CONFIRMED. Open S-1-F3 now.

**The evidence.**
- 451 rounds in F3's file (446 in F2's).
- In all five evidence rounds, the opened T equals the frontier (39: 5, 59: 4, 109: 4, 149: 4, 209: 8, by the worker's own diag), and the orphan's max ts lies above it and ≤ `hi`.
- The damaged record is a synced `/wal/` write.
- `recover` stops at the first invalid record exactly as §12.5 says (`Ok(None) | Err(()) => break 'outer`).
- amcheck is clean on both images.
- The store returns `Ok` holding image entries of commits above the frontier, so the state is not a ts-prefix and O-3(d) fails it as silent.

**Why it is the spec, not the prototype.** I-CKPT-AT-T (b) rests on step 1's durability, which class MW defeats by construction. §14 then has no rule for image entries above the recovered frontier. X5-R10's sequencer restart at frontier+1 lands inside the orphans' ts range, an I-ENTRY-UNIQUE hazard.

**G8** is confirmed by 41 → 0 livelocks, with no class C poison, and must be written into §9.2.

**S-1-F3 must open now.** It is P-authored, Class: spec, and needs no campaign. It can run beside X-5c-F3-F1 and F4, and E4 cannot be judged without it (X5-R3). It must decide:
1. **(§13/§14) Recovery when the WAL's valid end lies below durable history the image depends on.** It must define how recovery learns of it: for example, meta k records the WAL end that step 1 made durable, or the max entry ts in the image. It must also pick the outcome: fail closed (an XX001-class "WAL loss" error, so the plan's clause 3 detection holds), or a declared degraded open that makes entries above the frontier dead and floors the sequencer above the image's max ts.
2. **(§14/C-T0) The sequencer floor after recovery,** so that I-ENTRY-UNIQUE holds whatever (1) chooses.
3. **(§9.2) G8:** "a read whose bytes fail §3.3 inside apply, a split, a pack, GC or a checkpoint poisons the store; a client read never poisons". It must also say how a poisoned process ends.
4. **(§12.4) An explicit invariant: a commit is acked durable only through the contiguous written-and-synced prefix of the WAL.** The C-DIV hole shows the "one writer" sentence is not enough for an implementer.
5. **Whether MW's oracle (X5-R1's `lo := T`) changes** under (1), pre-registered before the next campaign.

If S-STORE changes, the prototype implements it and the whole campaign re-runs (X5-R3, X5-R18).

## Follow-ups

**X-5c-F3-F1 (P-authored card, S worker; stacked on card/v3-X-5c after this merge; before F4).** The prototype fixes the gate root-caused, plus the pin gaps.

It must:
- (a) Serialize `wal_writer_step`, so that one batch is in flight and `written`/`durable` advance only over the contiguous prefix. Pin it with a literal two-task interleaving in which batch 2 syncs before batch 1 is written, then a crash; the later SyncOn commit must not be acked. Mutant: the `max` accounting restored. Then re-derive C-DIV from a ≤ 200-seed run and give the remainder X5-R16's treatment, under the budget valve.
- (b) Advance `valid_end` past an accepted SegmentEnd (§12.5). Pin it with a SegmentEnd, a missing next segment, and a nested crash between the truncate write and its sync; nothing may be served. 6149 r1 must clear its served corruption.
- (c) Pin the replay-walk and `truncate_wal` `exists` sites (G-1 uncaught), and add G8's negative half: a read-op XX001 never poisons, a gated-op XX001 does (G-2 uncaught).
- (d) Add X5-R30's record fields `skx`, `smx` and `pass_na`.
- (e) Give the 64 remaining class C C-XX rounds X5-R16's treatment.
- (f) Fix the README: remove the duplicated block, and correct the MW-ORPHAN T column.

Its Acceptance 10k becomes F4's triage file.

**X-5c-F3-F2 (P ruling, before F3-F1 launches).** An oracle amendment, to be ruled as X5-R32, for O-3(c) under cross-round inherited damage.

Shape: the false-positive test also counts the earlier rounds' media-fault targets that are still tainted and referenced by the opened image. Those come from SimDisk history, never from §3.3.

It must be pre-registered before F3-F1's Acceptance and demonstrated by must-fail and must-pass cases. The rounds it covers are 17 MP O-3(c) rounds (14 of them G8 `poison` rounds) and 5 MW O-3(c) rounds. P must check the 5 MW rounds share the shape before covering them.

**S-1-F3 (P, Class: spec, open now).** As above.

## X-5c-F4

Its counts are refreshed from F3's file. **It may not launch yet: it stays blocked on X-5c-F3-F1** (and on F3-F2's ruling).

X5-R17 orders the retune after the classes, because a retune voids every repro. Here the gate has root-caused two fixable prototype deviations whose repros (21 and 6149) hold only on this workload. In addition, X5-R30's fields must be in the final file.
