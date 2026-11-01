Card: B-0-F7-F1b-F2-F1
Class: test
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.3
Gate-session: nv3-B-0-F7-F1b-F2-F1-gate-1 (S review sub-agent, OpenCode/GLM)
Date: 2026-11-01
Gate-exit: GATE EXIT=0
Commit: eab1f34904cfd15e50ffa0daf16f3745b398f730
Mutants: POST-WAIT-CHECK-DELETED -> S6f+S6g (8 FAILs: rc 4->0, repro dir exists, refusal text gone, planted/committed row overwritten); POST-WAIT-REUSES-PRE-DATE -> S6g only (4 FAILs; S6f green, its row is at $D); ROW-GUARD-UNWIRED -> S6a+S6b+S6c+S6d+S6f+S6g (18); ROW-GUARD-EXISTS-ONLY -> S6a/S6b/S6f msg+S6g msg (5); ROW-GUARD-TRACKED-ONLY -> S6c (3); ROW-GUARD-A-ONLY -> S6d (1); ROW-GUARD-AFTER-MKDIR -> S6a+S6f+S6g no-repro (3); QUIET-NOT-RECORDED -> S6d+S7a+S7b (7); QUIET-REAL-PATH-UNRECORDED -> S7b (1); QUIET-OVERWRITE -> UNCAUGHT by selftest, verified by inspection (unchanged, pre-existing: single >> site; already routed to B-0-F7-F1b-F2-F2 / the F1-F2-F1-F1 widening); own OWN-MKDIR-BEFORE-POST-GUARD (mkdir -p above the post-window guard) -> S6f+S6g no-repro (2); own OWN-GUARD-LOCAL-DATE (post-window guard asks `date +%F`, local) -> S6g only (4)
Followups: none

Reviewer: GLM (zai-coding-plan glm-5.3), S gate, round 1, on card tip eab1f349.

## Gate run
Fresh detached checkout `/tmp/gate-B-0-F7-F1b-F2-F1` made with `core.hooksPath=/dev/null`.
Checked: no `filter.*` config, no `info/attributes` (root `.gitattributes` is eol/binary only, no
smudge drivers), worktree and common hooks samples-only; `git write-tree` == commit tree; all 531
`nucleus/v3` blobs re-hashed `--no-filters` match byte for byte. `scripts/gate.sh` in the
foreground: STEP test/clippy/fmt/deny/wasm32/core-paths-selftest/enforcement-tests all EXIT=0,
`GATE EXIT=0`. Checkout removed. `fresh-gate.sh` does not exist yet (X-6-F1-F2-F2-F1-F1 not
landed), so the manual steps apply.

## Scope
Diff vs `nucleus/v3-scaffold`: only `reproduce.sh` and `README.md` of the qdrant workload, both in
Touch only. Three hunks only: the `st_run_win_stubs` helper, `st_run_win` delegating to it
(explicitly permitted; S7b's `st_rc`/`st_ok` lines byte-unchanged, env/PATH identical), and the
S6f/S6g block between S6e and the S7 `echo`. Every existing line byte-unchanged outside them; no
run-path change, no exit-code change, no new stub-nbench mode, no `rm`; header comment untouched.
README: exactly one clause added to the `selftest` bullet.

## Findings
1. **The card's whole purpose is delivered.** Both verdict-B-0-F7-F1b-F2 uncaught mutants
   (OWN-NO-RECHECK, OWN-RECHECK-STALE-DATE) are now caught, with FAIL lines confined to the new
   rows exactly as the card's Mutation evidence section predicts (S6f+S6g vs S6g-only). The
   shipped script passes unmodified: 3/3 selftest runs `REPRO SELFTEST OK`, 85 `  ok` lines each
   (77+8, as predicted), 0 FAILs — S6f/S6g deterministic across runs.
2. **Stub hygiene verified on the generated artifacts** (scratch dir left in place by the
   selftest): PATH is set only as the per-command prefix in `st_run_win_stubs` (the selftest's
   only PATH assignment; no `export` — cannot leak into later rows or the host); every baked path
   is inside the per-scenario mktemp scratch; git runs only in the throwaway roots. The S6g date
   stub, invoked directly, answers only the exact two-arg `-u +%F` list and only once the sleep
   flag exists; `+%s`, `-u +%FT%TZ`, `+%F`, no-args, 3-arg and `-u +%F%T` all exec the baked real
   `/usr/bin/date` (resolved before any PATH override). OWN-GUARD-LOCAL-DATE is caught precisely
   because of that exactness — the row pins the guard's `-u` form.
3. **Parent's eight mutants re-run: all still caught** (UNWIRED now 18 FAILs, EXISTS-ONLY 5,
   AFTER-MKDIR 3 — the new rows widen coverage; no existing assertion weakened). QUIET-OVERWRITE
   remains inspection-only, unchanged and out of this card's Limits (already routed to
   B-0-F7-F1b-F2-F2 and the F1-F2-F1-F1 widening).
4. Minor: the host clock is MST, not UTC (local and UTC civil dates agreed during the gate); the
   rollover is the stub's, so nothing depends on it — the card's own midnight-straddle note
   (pre-existing, S1 guards it) still stands. No action.
5. Minor: `D2=$(date -u -d '+1 day' +%F)` is a GNU-ism, consistent with the existing
   `touch -d` in S5's stub; Linux-only is established for this selftest. No action.
