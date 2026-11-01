Card: B-0-F7-F1b-F2
Class: bench
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-B-0-F7-F1b-F2-gate-1 (P review sub-agent, Claude Code)
Date: 2026-11-01
Gate-exit: GATE EXIT=0
Commit: 74f8d0f41f9df79b98e871c2fecf0dfef1c913d8
Mutants: ROW-GUARD-UNWIRED -> S6a+S6c+S6d (10 FAILs); ROW-GUARD-EXISTS-ONLY -> S6a msg+S6b (3 FAILs); ROW-GUARD-TRACKED-ONLY -> S6c (3 FAILs); ROW-GUARD-A-ONLY -> S6d (1 FAIL); ROW-GUARD-AFTER-MKDIR -> S6a no-repro-dir (1 FAIL); QUIET-NOT-RECORDED -> S6d+S7a+S7b (7 FAILs); QUIET-REAL-PATH-UNRECORDED -> S7b (1 FAIL); QUIET-OVERWRITE -> UNCAUGHT by selftest, verified by inspection (single >> site, nothing else writes/removes the file); own OWN-GUARD-WRONG-PATH -> S6a+S6c+S6d (10 FAILs); own OWN-QUIET-IGNORED-NAME (.txt->.log) -> S6d+S7 (7 FAILs); own OWN-WINDOW-TAKES-BYPASS -> S7b (1 FAIL); own OWN-NO-RECHECK (delete post-window guard) -> UNCAUGHT; own OWN-RECHECK-STALE-DATE (post-window guard reuses pre-window date) -> UNCAUGHT; own OWN-WINDOW-UNGATED (LOADAVG override honoured without mode) -> UNCAUGHT
Followups: B-0-F7-F1b-F2-F1, B-0-F7-F1b-F2-F2

Reviewer: Opus (claude-opus-5-5), P gate (raised S->P: measurement protocol), round 1, on card tip 74f8d0f4.

## Gate run
Fresh detached checkout `/tmp/gate-B-0-F7-F1b-F2` made with `core.hooksPath=/dev/null`.
Checked: no non-sample hooks, no `filter.*` config, no `info/attributes`, no `.gitattributes`;
`git write-tree` == commit tree; every `nucleus/v3` blob re-hashed `--no-filters` matches.
`scripts/gate.sh` in the foreground: all seven STEPs EXIT=0, `GATE EXIT=0`. Checkout removed.
In the same checkout: `bash -n` + `shellcheck -S warning` clean; `reproduce.sh selftest` ->
`REPRO SELFTEST OK`, 77 `  ok` lines; real-repo `check-ignore` on `run-a.quiet.txt` -> not
ignored (`run-a.log` -> ignored by `.gitignore:28:*.log`). S7b line verbatim:
`2026-11-01T07:51:16Z quiet-host window satisfied (load1 0.50 -> 0.50, 13 consecutive samples; no cargo/rustc; no nbench unit)`.
Mutants were applied with `perl -0pi` in a /tmp copy of `bench/nbench` (own git repo), each
reverted, `git status` clean, copy deleted.

## Scope
`git diff nucleus/v3-scaffold...HEAD --stat`: only `reproduce.sh` and `README.md` of the
qdrant workload, both in Touch only. `copy_evidence`, `verdict`, `check`, exit codes, S1-S5
assertions unchanged (the selftest header's "no rm" sentence now names S6b's rm; accurate).
No results, no Rust.

## Findings
1. **UTC rollover during the wait: correct by inspection, untested.** The post-window call
   recomputes `$(date -u +%F)` fresh, and `nbench run` fixes its date at row start
   (`runner.rs:685`), so the guard and the write agree; the only residual gap is the few
   ms between the re-check and `"$BIN" run` (mkdir/printf/touch) - negligible. But
   OWN-NO-RECHECK and OWN-RECHECK-STALE-DATE both survive the selftest: half the card's stated
   purpose has no regression test. Non-blocking -> F1. (Pre-existing, not this card: run A's
   `REPRO` is still fixed before the window, so a rollover puts the repro dir on the old date;
   the header already says "run the protocol inside one UTC day".)
2. **`window` mode cannot leak silently from the selftest** (set per-command in `st_run_win`,
   never exported; exact-match `= "window"`; `:?` aborts before anything is created if the
   loadavg path is missing; no other file references `NB_REPRO_SELFTEST`). But if the env
   is set in a real run, the recorded satisfied line is **indistinguishable from a real one**
   - unlike `=1`, whose BYPASSED line is loud by design. That breaks the "loud on purpose"
   principle for exactly the evidence this card makes committed. Also OWN-WINDOW-UNGATED is
   caught only by reading. Non-blocking (needs a deliberate two-variable env plus a fake
   loadavg file) -> F2.
3. **Append-only: holds.** One write site, `>>`, after `mkdir -p "$REPRO"`; no `rm`/`mv`/`>`
   touches `run-*.quiet.txt` anywhere; the killed-attempt rename only touches `run-$x.log`.
   QUIET-OVERWRITE is uncaught by design (card says inspection); see the F1-F2-F1-F1 note.
4. Minor: a killed attempt that died after nbench wrote `results/<date>/$ROW.json` but before
   the `mv` leaves an untracked row; the retry now refuses (exit 4, "already exists"). That is
   the safe outcome (the leftover is evidence) but the operator message does not mention the
   killed-run case. No card.
5. S6e's check-ignore copies `bench/nbench/.gitignore`, which has no `*.log` rule, so it only
   proves the nbench-level file; the real repo-root rule is covered by the Acceptance line.
   Fine as is.

## On B-0-F7-F1-F2-F1-F1 (killed-run-then-retry, touch-if-missing marker)
Keep it separate - do not reopen this passing card (whose card text forbade a killed-attempt
stub). But widen it: the same killed-then-retry scenario should also assert that
`run-<x>.quiet.txt` holds **two** lines after the retry, which turns QUIET-OVERWRITE from an
inspection-only mutant into a caught one. One stub, two mutants.

## Follow-ups
- **B-0-F7-F1b-F2-F1** (test, S gate): selftest coverage for the post-window re-check. S6f:
  `window` mode with a `sleep` stub that, on its first call, plants (and in a git root,
  commits) `results/$D/$ROW.json`; `a` must exit 4 with no `repro/` dir. S6g: a `date` PATH
  stub whose `+%F` output advances to a committed date after the window starts; `a` must
  exit 4. Must catch OWN-NO-RECHECK and OWN-RECHECK-STALE-DATE.
- **B-0-F7-F1b-F2-F2** (bench, P gate - measurement protocol): make the `window`-mode
  satisfied line self-identifying, e.g. append ` [NB_REPRO_SELFTEST=window: stubbed loadavg,
  not a measurement]` only in that mode (S7b's regex is prefix-anchored, so it still matches;
  add an assertion that the marker is present), and have `check` flag any committed
  `run-*.quiet.txt` that contains `BYPASSED` or that marker. Real-mode line stays byte-for-byte.
