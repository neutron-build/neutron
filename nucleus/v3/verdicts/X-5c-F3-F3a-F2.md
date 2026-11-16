Card: X-5c-F3-F3a-F2
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (X-5c-F3-F3a-F2 P gate and ESCALATE ruling, 2026-11-04)
Date: 2026-11-04
Gate-exit: GATE EXIT=0
Commit: abc0b0c541b94e04663345475e8ff374ad1cc865
Mutants: GCW-RESET (fix line deleted) -> gc_w_survives_recovery_into_the_boot_checkpoint (worker; gate re-applied: left 0, right 4), FRONTIER-AS-FIRST-CANDIDATE (c = lo) -> o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2 (worker; gate re-applied), (own) GCW-OFF-BY-ONE (gc_w_used = meta.gc_w - 1) -> gc_w_survives_recovery_into_the_boot_checkpoint (left 3, right 4), (own) FRONTIER-AS-HI (c = hi) -> UNCAUGHT (1749 r2 has frontier = hi = 41), (own) LE-T-FROM-FRONTIER (le_T := ts <= frontier) -> UNCAUGHT (owner 4 is <= both T 38 and frontier 41)
Followups: X-5c-F3-F3a-F2-F1

## How the gate was run

- **Fresh checkout.** `git -C ~/Workspace/neutron-v3 -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-X-5c-F3-F3a-F2 abc0b0c541b94e04663345475e8ff374ad1cc865`.
  - The common git dir (`~/Workspace/neutron/.git`) has no non-sample hooks, no `filter.*`, `core.hooksPath` or `core.attributesFile` config, and no `info/attributes`.
  - The index equals the commit tree (`ls-files -s` against `ls-tree -r`).
  - Every `nucleus/v3` blob matches its working file (`hash-object --no-filters`). Status was clean.
- **Gate.** `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F3-F3a-F2 bash scripts/gate.sh` ran in the foreground. Every STEP was EXIT=0, and the literal last line is `GATE EXIT=0`.
- **Mutants and scratch tests** were applied in the gate checkout and reverted with `git checkout` or `rm`. Status was 0 lines before the checkout was removed. The run outputs are in `~/.cache/nv3-review/gate-F2/`.
- **Scope.** `git diff card/v3-X-5c...HEAD --stat` touches only `nucleus/v3/spikes/e4-recovery/`: README, `src/harness.rs`, `src/store/ckpt.rs`, `tests/harness.rs` and `tests/store.rs`.
  - `src/model.rs`, `src/oracle.rs`, `src/sim/` and `src/workload.rs` are untouched.
  - There is no `unsafe`, `unwrap`, `expect`, `panic!` or `set_var` in the non-test diff.

## Measurements re-run by the gate

All of these are single-seed runs under `ulimit -v 25165824`.

| Run | Digest | Outcome | RESULT says |
|---|---|---|---|
| 664 at the tip | 1522095950 | all rounds pass | 1522095950, PASS |
| 664 with GCW-RESET (pre-fix) | 1302965675 | r2 `O-1(d) at c=26, tree 2, S=5` | 1302965675, the same text |
| 112 at the tip | 3610336029 | r2 `O-1(b) at c=15, tree 3` | 3610336029, fails |
| 1749, diagnostics off / `E4_O1_AT_FRONTIER=1` | 2987258781 / 2987258781 | r2 fails; the O1F line: lo 40, hi 41, frontier 41, c 41, tree 4, `store=None` | 2987258781 |
| 112 with `E4_KEY_DIAG=1` | 3610336029 (= off) | FIRST-LOST at meta ckpt 3, T 11 | FIRST-LOST at meta 3 |
| 5549 with `E4_O1_AT_FRONTIER=1` | 2018609054 | O1F: lo 30, hi 33, frontier 32, c 32, tree 4 | 2018609054 |
| 104 with `E4_KEY_DIAG=1` | — | tree 5: create 1, drop 11, re-create 17; key owner ts 6 | d 11, r 17 |
| 7359 with `E4_KEY_DIAG=1` | — | tree 3: create 1, drop 11, re-create 16; key owner ts 5 | d 11, r 16, owner 3 (*) |

(*) The trace's key timeline shows owner ts 5. The RESULT's "owner 3" for 7359 is the frontier diagnostic's owner on another key; it is the same shape either way.

The diagnostics change no digest. Root cause A's before/after reproduces exactly.

**The minimal repro of the ESCALATE was run by the gate**, as a scratch test against the public `Model` API:
- ts 1: `CreateTree{1}`, `CreateTree{3}`, `Put(1, be64 3, "public")`, `Put(3, k, v)`;
- ts 2: `Delete(1, be64 3)` and `DropTree{3}`;
- ts 3: `CreateTree{3}` and `Put(1, be64 3, "public")`.

The result:
- `published_at(3) = {3}`, `trees_at(2) = {1}` and `trees_at(3) = {1, 3}`;
- `scan(3, 1) = scan(3, 2) = scan(3, 3) = {k: v}`.

So the model hands the re-created tree its pre-drop content at every S. That includes the O-1(d) samples below the drop, not only S ≥ r.

## Findings

1. **Root cause A is correct per S-STORE §10.5** (e1bd478b). §10.5 says verbatim: "At open it starts at the recovered meta's `gc_w`, so a checkpoint taken before the first GC mutation (including the boot checkpoint, §14 step 8) still records the image's bound". It names that defect GCW-NOT-RECORDED.
   - The fix is that one line in `ckpt::open`. It runs before `load_image` and replay, so pass 2 cannot lower `gc_w_used`.
   - The bound `gc_w ≤ T` is preserved, because the recovered `gc_w ≤` the recovered `T`.
   - The pin decodes the winning meta from durable bytes, so it is not store memory. It catches both the reset and an off-by-one. The gate's 664 pre/post re-run matches the RESULT digest for digest.
2. **The diagnostics satisfy "stderr only, default off"** (iv).
   - `E4_KEY_DIAG` and `E4_O1_AT_FRONTIER` are read-only env checks. Stderr output happens only when the variable is set.
   - The thread-local `O1F_CAPTURE` is `None` unless a test arms it on its own thread. The round record and the digest never read it.
   - The seam is the right call: edition 2024 makes `set_var` unsafe, and a process-global env write would leak across the parallel test harness.
   - Digests are identical with the diagnostics on and off (1749, 112).
3. **The frontier pin is weak on two axes** (non-blocking: it is a diagnostic, but F4's class table will read it). Both own mutants are UNCAUGHT.
   - `c = hi` survives, because 1749 r2 has frontier = hi.
   - `le_T` computed against the frontier survives, because the line does not print `T`.
   - Carried to F1, item 6.
4. **The README's folded-Work-2 table mislabels MW `lo`** (non-blocking).
   - Its "frontier/lo/hi" column copies the round record's `lo` field. In MW that field is X5-R1's `model.lower_bound(c_prev)` (`harness.rs:3050`), not X5-R33's `max(c_prev, T, h, a)`, which `check_round` and the O1F line use.
   - The oracle's MW `lo` is 30 for 5549 r2 (the O1F line, and `o1` "at c=30"), and at most 11 for 7359 r2 (`o1` "at c=11"). The record field says 32 and 14.
   - Read naively, 7359 r2 is then "frontier 12 < lo 14". F4's class table must take MW `lo` from X5-R33, not from the record field. This is carried to F1, item 7, and to the F4 changes below.
5. **Test comments are off by one** (cosmetic). `gc_w_survives_recovery_into_the_boot_checkpoint`'s doc comment says "GC'd at `w = 5`" and "must still carry gc_w = 5", while the fixture and every assert use 4. Carried to F1, item 8.
6. **The fold and the pointer are faithful** (the orchestrator edit).
   - Diffing against the pre-edit copies shows only two changes:
     - this card's Status line `waiting` → `ready`, plus the FOLD + POINTER block;
     - X-5c-F3-F3b-F1's Status line → `folded-into-X-5c-F3-F3a-F2`.
   - The block matches verdict X-5c-F3-F3b (649e7d57) on four points:
     - the base;
     - the repro file (`X-5c-F3-F3b.acc.*`, 86ecf8ff, sha 8332df8d…);
     - Work 1–3 here, Work 4–5 at the end;
     - F3b-F1's Touch only, Mutation evidence and Do not binding the folded part.
   - The fold permission comes from F3b-F1's pre-edit Status line ("may instead fold Work 1–3 into X-5c-F3-F3a-F2 if F2 has not launched").
   - The worker did the folded work in its own commit (204afb9a) and the stage record at the end (abc0b0c5).
   - The card's Work 2 text still says "the reworked X-5c-F3-F3a Acceptance file". The block overrides it explicitly, so this is no defect.
7. **The experiment is sound.**
   - Repros were taken from the pointed file, each confirmed twice. The (d) round 664 came from the 10k file, because 0..200 has none, and that was stated.
   - Every run was single-seed or ≤ 200 contiguous seeds.
   - There was no retune and no seed filter.
   - The 90-minute budget was met (01:56:30 → 02:56).
   - The two-cause split holds up under the gate's traces: (d) is A; (b), (c) and the three MW rounds still failing are B.

## ESCALATE ruling (root cause B)

**(i) This is a genuine model defect, and it gets X5-R16 treatment as an instrumentation (model) class, not a store failure.**
- **Confirmed.** `Model::scan` (`src/model.rs:178`) iterates every commit's `Put`/`Delete` on `t` and ignores `DropTree`/`CreateTree`. `trees_at` (`src/workload.rs:215`) does account them. The scratch repro above confirms it.
- **The store's side is what S-STORE §6.8 requires.**
  - DropTree marks the entry `dropping`, frees the pages bottom-up, then removes the entry.
  - CreateTree allocates a fresh empty root leaf.
  - In the spike, `ckpt::apply_op` runs `dir_drop_tree`'s whole walk synchronously, live and in pass 2. A later `CreateTree{t}` therefore finds no entry and builds an empty tree.
  - The traces for 112 (meta 3: directory `[1, 2, 50]`, tree 3 gone) and 63 show exactly that.
- **"Serves nothing" (pre-drop) is correct, and the model's expectation is unservable.**
- **The root of the shape is a prototype deviation in the frozen workload.** `build_round`'s setup commit re-creates a dropped tree under the **same id**, but S-STORE §6.8 says "Tree ids are C-T0 §10 storage ids, never reused". The model was never told what a re-used id means. The fix gives it the meaning the store implements.
- **One residual store-side hazard exists only under id reuse, and the fix vehicle must make it visible.**
  - `dir_create_tree` is a no-op on any existing entry, including a `dropping` one (§6.8 idempotence).
  - Suppose an opened image holds `t` as `dropping` (DropTree ≤ T) and pass 2 replays a `CreateTree{t}` above T. Then the new incarnation's ops land in the dropping tree, and §14 step 6 frees them.
  - No spec-conformant history can produce this, because ids are never reused. It is reachable only if a pre-boot image is opened after a later round's setup, for example after a meta fault.

**(ii) The fix vehicle is a new card, X-5c-F3-F3a-F2-F1** (P, experiment, Gate: P; written to `cards/`, Status: waiting).
- It is a P-commit to the frozen model and oracle wiring. It lands as one commit whose subject names X5-R36, after its step 0 commits the ruling text below verbatim into the README, pre-data.
- **It does not fold into F4.** F4's own Limits freeze "the oracle, the model". F4 is also the card that judges, and the X5-R33 precedent is that a ruling is implemented and gated before the run that judges by it.
- **No PLAN §10 owner item is touched.**
  - Items 8/9 (WAL loss, W-BP) are unaffected.
  - Item 5 is about the substrate, and the sim is unchanged.
  - The E4 row's oracle ("the recovered state equals the model at some c") is unchanged; only the model's reading of a drop is corrected.

**(iii) It blocks X-5c-F4's launch, not this merge.** Without X5-R36, F4's 10k would count every durable drop+re-create round as a failure under X5-R3, "any failure → redesign". Excluding those rounds instead would be a seed filter, which is banned. F2 merges now; F4 waits on F2-F1's P gate.

**(iv)** See findings 1 and 2: both are correct.

### X5-R36, verbatim (binding; pre-data; P, 2026-11-04)

**X5-R36 (the drop incarnation: what the model reads after `DropTree`).**
- **Source.**
  - S-STORE §6.8: DropTree frees the tree's pages and removes its directory entry, and CreateTree allocates a fresh root leaf.
  - This verdict's ESCALATE ruling.
  - The workload re-uses a dropped tree's id in the next round's setup commit. That is a prototype deviation from §6.8's "never reused", and it stands, because changing the workload would move every seed with a drop round.
- **(a) The rule.**
  - For a user tree `t` and a cut `c`, let `d(t, c)` be the largest ts `≤ c` of a model commit holding `DropTree{t}`, or 0 if there is none.
  - `Model::scan_at(t, S, c)` is `Model::scan(t, S)` computed only over ops on `t` from commits with `d(t, c) < ts ≤ S`, and over loads of `t` with `d(t, c) < S_L ≤ S`. For `S ≤ d(t, c)` it is empty.
  - `Model::scan(t, S)` becomes `scan_at(t, S, S)`.
  - The catalog (tree 1) is never dropped. `published_at` is unchanged.
- **(b) The wiring.**
  - O-1(b) at candidate `c` compares against `scan_at(t, c, c)`.
  - O-1(d) at sample `S` compares against `scan_at(t, S, c)`.
  - Every harness call site that compares a store scan with the model at a read point `x` (`harness.rs` near 3286, 4516, 5123, and `o1_at_frontier_lines`) uses the cut that the compared store state recovered or read at.
- **(c) What it does not touch.** `lo`, `hi`, the candidate set, `sample_points`, O-1(a)/(c), O-1f, O-2, O-3, K, R, X5-R1/R33/R34/R35, `trees_at`, the sim, the fault and crash models, and the workload stream.
- **(d) It loosens nothing a correct store can satisfy.** It removes only the expectation of pre-drop content in a re-created tree, which §6.8 makes unservable. A store that serves pre-drop content in a re-created tree now fails O-1(b)/(d), where before it was required to.
- **(e) The id-reuse hazard is surfaced, never excluded.** The round record gains `reuse_on_dropping`, computed from SimDisk/opened-image data and never from store memory. It is true when the opened image's directory holds `t` as `dropping` and the model holds `CreateTree{t}` at a ts in `(T, frontier]`.
  - Such a round still runs the full oracle and counts by its outcome.
  - Any `reuse_on_dropping = true` in F1's windows or in F4's 10k is an ESCALATE to P with the round.
- **(f) Pre-data.** No run that judges by X5-R36 has been read. The gate's and the worker's 0..200 and single-seed runs are diagnosis, never judgement evidence.

### X-5c-F4 changes (an orchestrator edit, verbatim)
- **Depends:** add X-5c-F3-F3a-F2 (this MERGE-WITH-FOLLOWUPS, then card/v3-X-5c fast-forwarded) and **X-5c-F3-F3a-F2-F1** (P gate MERGE or MERGE-WITH-FOLLOWUPS, then card/v3-X-5c fast-forwarded). Status stays blocked until both have landed.
- **Carried rulings:** add X5-R36 verbatim, from this verdict, beside X5-R33–R35. Limit 2's "the oracle, the model … are frozen" is read as including X5-R36 as landed by F2-F1.
- **Step 0:** the triage file of record stays `X-5c-F3-F3b.acc.*` (86ecf8ff, sha 8332df8d…), with its counts unchanged. Add a note: it predates root cause A's fix (e1bd478b) and X5-R36, so its C-DIV 256 and its MW five are expected to fall. The baseline that F2-F1's 0..200 records is the expectation: C-DIV 0 of the seven at F2's tip, and 1749/5549/7359 r2 passing. Neither count is quota or judgement evidence.
- **The class table:**
  - C-DIV's drop+re-create rows are expected 0 under X5-R36. Any C-DIV round that remains gets X5-R16 treatment as a new class.
  - Add a `reuse_on_dropping` row, which must be 0 or is an ESCALATE.
  - MW `lo` is X5-R33's `max(c_prev, T, h, a)` (the O1F line), never the record's `lo` field, which is X5-R1's lower bound.
  - MW is classified by the frontier candidate's failure (unchanged from the REFRESH).
- **The E4 judgement:** no round is excluded or re-counted by hand. Rounds whose only defect was root cause B pass under X5-R36. Every round that still fails counts under X5-R3 unchanged.
- **The retune list:** unchanged from the REFRESH. Root cause A changes which images carry a `gc_w`, but not Q18's definition. Q18 is re-read from F4's own probes.

## Follow-up

- **X-5c-F3-F3a-F2-F1 (P, experiment; Gate: P).** Its step 0 commits X5-R36's text verbatim into the README (pre-data). The steps after it:
  - One commit whose subject names X5-R36 adds `Model::scan_at` and the oracle and harness wiring.
  - It adds a unit pin (this verdict's minimal repro, plus `c < d` and `S < d ≤ c` cases) and the `reuse_on_dropping` record field.
  - Mutants: R36-REVERT, R36-FLOOR-BY-S (the floor taken at `S`, not `c`) and R36-FLOOR-IGNORES-CUT (the floor from drops above `c`).
  - It re-runs 112, 63, 104, 1749, 5549 and 7359 single-seed twice each, then 0..200, and records every outcome. A remaining failure gets X5-R16 treatment.
  - It strengthens the frontier pin (finding 3) with a 5549 r2 literal (frontier 32 < hi 33) and a printed `T` checked against `le_T`.
  - It relabels the README's MW `lo` (finding 4) and fixes the pin comments (finding 5).
