Card: X-5c-F1
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 06d170b8-6cf4-4aa8-abca-e99d724b8821
Date: 2026-10-31
Gate-exit: GATE EXIT=0
Commit: d8bbc8d765aed4b92f84e6e00ff643adb5055a83
Mutants: M1 r14 live-world S-DET gate off -> r14_pre_baton_host_step_is_refused, M2 r14 unwind grants any task -> r14_unwind_host_steps_run_in_taskid_order, M3 (own) unwind turn largest-first -> r14_unwind_host_steps_run_in_taskid_order, M5 shape A slot Some(1) back -> r20_mpe_no_opened_image_means_no_slotted_finding, M6 shape B event half off -> r20_mpe_r_reads_the_issued_write_history, M7 (own) R anchor always 0 -> d10_lost_meta/d10b_rollback_exception/d17_integrity, M8 (own) R anchor at the newer meta's issue instead of its sync_all -> UNCAUGHT, M9 header slot 0 back in the referenced set -> r16_mpe_referenced_set_excludes_the_file_header, M10 NO-TWIN supply only on Ok -> r16_no_twin_is_always_supplied, M11 NEVER-FIRED re-draw returns the drawn spec -> r16_never_fired_targeted_draws_reach_their_hook, M12 R21(c) re-pair skipped -> r21c_the_real_rounds_retry_re_pairs_its_twin, M13 (own) FOLD ignores twin failures -> UNCAUGHT (whole crate), M14 --no-quotas inverted -> r15b_verify_reports_all_three_checks_before_failing, M15 (own) digest mismatch exits 0 -> r15b_verify_reports_all_three_checks_before_failing
Followups: X-5c-F1-F1, X-5c-F1-F2

## How the gate was run

- **Fresh checkout.** `git -c core.hooksPath=/dev/null worktree add --detach /tmp/gate-x5cf1 d8bbc8d7`. Checks before trusting it:
  - the shared git dir has no non-sample hooks, no `filter.*` config and no `info/attributes`; the tree's `.gitattributes` holds only `text`/`eol`/`binary` rules;
  - the index equals `HEAD`'s tree;
  - all 282 `nucleus/v3` files hash (`hash-object --no-filters`) to their tree blobs.
- **The gate.** `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F1 bash scripts/gate.sh` ran in the foreground (3m37s, after a `--no-run` warm-up of the cold target dir). Every STEP gave EXIT=0 (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests), then `GATE EXIT=0`. Log: `~/.cache/nv3-review/runlogs/X-5c-F1-gate.gate.log`. The checkout was removed afterwards.
- **Mutants.** Applied in a separate copy with its own target dir and reverted after each one. The card worktree was never modified (clean before this verdict).
- **No campaign or verify was run** (per the request). Targeted seed runs under `ulimit -v 25165824`:
  - the 160 meta-rollback seeds at the tip, with stderr-only instrumentation (160/160 digests equal the committed file's);
  - 856 single-seed runs at 12b54965, from a hooks-off detached checkout that has since been removed.

## Findings

### Goal items
- **X5-R14: met.** The fix is in `Sched::acquire` (src/sim/sched.rs). The gate runs under the same lock as the step it authorises. In a live world only the baton holder may step, and any other task gets a loud `SimFail::Baton`. In a failed world the unwind runs in TaskId order: `unwind_turn` is set at all four fail sites and advanced in `finish`, waiters park on their condvar, and `HARNESS` is exempt. The `h:entry` barrier covers the service tasks.
  - Both pins are mutation-checked (M1, M2, M3).
  - The stress evidence and the root-cause record stand as esc-1 found them.
  - The digest-stability list: see finding 1.
- **X5-R15: met.**
  - I recomputed `E4 FOLD` from `results/campaign.json`: `FAILING_SEEDS=1088 FAILING_ROUNDS=1034 TWIN_FAILING_ROUNDS=103 DEFECT_SEEDS=0`, of which 54 seeds fail only on a twin. That matches the commit and the README.
  - `verify`'s exit decision is pinned (M14, M15).
  - The FOLD line's twin term is unpinned (M13): see X-5c-F1-F2.
- **X5-R16 NEVER-FIRED / NO-TWIN: standing.** `DEFECT_SEEDS=0`, and there are no no-twin rounds. Pins are mutation-checked (M10, M11). The README's record (X5-R21 (b)) describes 6cc53657/4fedc648 as the code is, including the uniform twin wrap.
- **X5-R20, shape A (837200a7): accepted (final).** No valid meta means no opened image, so X5-R5 makes O-3 (e) vacuous and the finding is structural. Pinned (M5).
- **X5-R20, shape B (80dc0ff3): accepted (final), including the anchor-0 fallback esc-2 left for this gate.**
  - **Why the fallback is sound.** Two facts make `same == false` (with an in-stream newer meta that was later synced) imply that `dw[g]` is a graduation from a prior life:
    - `events` is this round's workload-life stream (`new_round` clears it, and the harness clones it at the crash);
    - the page file is synced only by `sync_all` (`sync_data` is called only on WAL segments: wal.rs:512/603, ckpt.rs:1506), and `dwrites` is append-only (faults never remove entries).

    So the first in-stream newer meta, once a `sync_all` follows it, is in `dw` with `all=true`, and it is `dw[g]` unless an earlier life graduated a newer meta. In that case the whole stream comes after the graduation, so anchor 0 is the text's anchor.
  - **Empirically, the branch is never taken.** All 160 meta-rollback rounds in the 10k take the `same` branch (instrumented run). No other round reaches the code, because that needs a newer graduation, which is `mrb`.
  - **Two residual hazards (non-blocking):**
    - the soundness rests on the "page file only via `sync_all`" property, which nothing asserts;
    - the anchor's position (the sync, not the issue) is unpinned (M8 survives).

    Both are in X-5c-F1-F2. M7 (anchor always 0) is caught.
- **X5-R21 (c) (04063d1b): accepted (final).** The twin re-runs at the re-derived spec from `tch_pair` before the real round uses its supply and nested points. Pinned (M12).
  - One edge case: if the re-paired twin's recovery returns `Err`, `twin_chain` keeps the discarded first attempt's continuation, where it should become `None` (a re-seed). That needs a real-round retry, plus a re-pair `Err`, plus a later round. It is in X-5c-F1-F2.
- **X5-R23–R27: met.**
  - **Provenance.** The committed `results/campaign.json` sha256 is `98a7c8c5…2516`, which is the prov file's `json_sha256` and P's file. The prov file shows `commit=c8e578b7…`, `status_lines=0`, `exit=0` and `verify_exit=0`. `git diff --quiet c8e578b7 HEAD` over src, tests, examples, Cargo.toml and Cargo.lock exits 0. c8e578b7..HEAD touches only the README, `campaign.json` and the TSV.
  - **The attempt.** The r4 transcript shows only the allowed shape-C single-seed runs, with no campaign, verify or gate. The runner's Acceptance `verify --no-quotas` PASSED at the tip, and that is the verify of record.
- **README and escalation routing.**
  - The README records X5-R23's escalation with 31d68b5f's evidence: slots 10 and 42, dw[11]/dw[22], and the meta-slot fallout loop as an open P question. It also records shape C's `E4_MPE_DIAG` evidence (amcheck (ii), I-LPID-EXACT "mapped lpid unreachable").
  - The hand-off was checked against the file. Shape C is 6 rounds (688, 3718, 4858, 5238, 5638, 9717, all (ii)), and the completeness residual is 6 rounds (1787/0, 5808/0, 6157/0, 8598/1, 9098/1, 9647/1).
  - The repro seeds hold in the file: 987/1108 deadlock, 66/221 C-XX, 21/46 scan, 39/59 O-1(c)+O-3(d), 1897/2437 served, 339/849 twin.
  - Twins: 103 rounds, 91 seeds, 44 MP + 59 MW. MP-A: 7.
- **Scope and hygiene.** Every changed file is under `spikes/e4-recovery/`. The frozen files are untouched (oracle.rs, model.rs, tests/oracle.rs, sim/disk.rs), and `tests/directed.rs` is unchanged. Added src lines contain no `unwrap`/`expect`/`panic!`/`unsafe`/hash iteration/wall clock.

### Finding 1 (record defect, non-blocking): the TSV's step-4 causes are wrong for 54 seeds
- **What the worker did.** X5-R26 split 0a8f196b→bc4f3bba "by the record fields" and said "never force it into a cause". The worker overrode the field rule and assigned 155 field-rule "redesign" seeds to the MP-E referenced set. The basis was the claim that "the redesign provably derives the same final specs (6cc53657)".
- **That claim is false.** I ran all 856 seeds that changed at that step at **12b54965** (MP-E applied, NEVER-FIRED still in probe form). Of the 852 net-changed seeds:
  - 794 are MP-E only, and the TSV is correct for them;
  - **40 changed at both commits**, but the TSV lists only MP-E (e.g. 7, 2307, 668, 1108);
  - **13 changed by the redesign alone**, but the TSV labels them MP-E (e.g. 987);
  - **1 (6868)** is MP-E only, but the TSV says "both";
  - 4 are correct as redesign (4139, 7469, 7719, 9809).
- **Scope of the damage.** All 54 wrong seeds are MP. The X5-R14 attribution (step 1), which is what pin 3 exists for, is unaffected. No RESULT count comes from this column.
- **The fix is offline.** The evidence for every seed is at `~/.cache/nv3-review/runlogs/X-5c-F1-gate.step4-truth.tsv`. Even the field rule would have been wrong on 107 of the 852, so the per-seed run is the attribution to use.

### Finding 2 (latent hang, non-blocking): `reachable_hook_spec`
`src/workload.rs` `reachable_hook_spec` never returns when `masks` is empty or all-zero. With `union == 0`, `left` never decrements. I proved it with a probe test: both cases failed the 5 s timeout. It needs a targeted round whose unfired attempt reached no hook at all. It never occurred in the 10k runs (all finished, `DEFECT_SEEDS=0`), but it should be a loud defect, not a hang.

## Follow-ups

**X-5c-F1-F1 (mechanical, record-only; fold it into X-5c-F2 as step 0 so that only one branch edits the README).**
- Correct `results/digest-stability.tsv`'s step-4 cause for the 54 seeds above, from the gate's per-seed evidence file. Run nothing new.
- Rewrite the README's step-4 paragraph:
  - drop "provably derives the same final specs" and "the mechanism won";
  - state the per-step counts by the 12b54965 runs: 835 MP-E (795 MP-E only, counting 6868, + 40 both) and 57 redesign (13 + 40 + 4);
  - cite the evidence file.
- No code.

**X-5c-F1-F2 (harness hardening plus pins; inside X-5c-F2, before its final campaign, because these are code changes).**
- (a) `reachable_hook_spec`: on `union == 0`, return a loud defect (the fired-guard path) instead of looping. Pin it with an empty-mask literal call.
- (b) In the R21 (c) re-pair block, set `twin_chain = None` when the re-paired twin's `end_store` is `None`.
- (c) Pin `fold_line`'s twin term with a literal seed that fails only on its twin (M13).
- (d) Pin R's anchor at the `sync_all` return rather than the meta's issue (M8), with a literal event stream that has a page write between the two.
- (e) Make anchor-0's precondition explicit: assert, or record a defect, if a page-file `SyncReturned { all: false }` ever appears, because anchor 0 would then widen R beyond R5's text.

## X-5c-F2 launch
X-5c-F2 may launch **once the orchestrator has fast-forwarded `card/v3-X-5c` to this branch** (`git -C ~/.cache/nv3-cards/X-5c merge --ff-only card/v3-X-5c-F1`) **and P has amended its card.** Its Goal predates esc-1 and esc-2. It names C-XX, C-DIV, MP-LIVELOCK, MP-A, MW-ORPHAN and the twins only. The amendment must carry:
- (i) MP-E shape C, 6 rounds, provisionally a map-exactness store deviation, beside C-XX;
- (ii) the MP-E completeness residual, 6 rounds, and the X-5b meta-slot fallout loop. These are **P's open oracle-definition question**. F2 records evidence and never changes the oracle. P should rule on them before F2's final 10k run;
- (iii) X-5c-F1-F1 and X-5c-F1-F2 above.
