Card: RUN-F1
Class: core
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 task nv3-RUN-F1-gate-1 (claude-code review subagent, session 6a30b184-b730-44c9-b45b-0423cb005037)
Date: 2026-11-03
Gate-exit: GATE EXIT=0
Commit: 21f4188db563971ac1180accb7e58a17ada09d98
Mutants: RF1-TAIL-ONLY -> e2e M msg2 row, RF1-TAIL-SCAN -> STEP-EARLY + e2e M msg2 row, RF1-FIRST-ONLY -> STEP-EARLY, RF1-ALL-STEPS -> STEP-EARLY, RF1-ALWAYS-HEADER -> STEP-NONE + STEP-INDENTED, RF1-UNANCHORED -> STEP-INDENTED, RF1-HEADER-AFTER-TAIL (own) -> STEP-EARLY, RF1-DEDUP-SORT (own; sorted(set(steps))) -> STEP-EARLY, RF1-ONE-DIGIT (own; EXIT=[1-9]) -> STEP-EARLY, RF1-NO-SEP-NL (own; no newline between step list and tail) -> STEP-EARLY + e2e M msg2 row
Followups: none

## Gate run
Fresh detached checkout of 21f4188d made with `core.hooksPath=/dev/null` at
/tmp/gate-RUN-F1. Checked before use: no non-sample hooks in the common git dir,
no `filter.*`/`core.attributesFile`/`include` config, no `info/attributes`, no
`.gitattributes` in the tree; `git diff --cached` against the commit is empty; every
`nucleus/v3` blob matches `git hash-object --no-filters` (symlinks by target).
`bash scripts/gate.sh` ran in the foreground (PATH with ~/.cargo/bin,
CARGO_TARGET_DIR=~/.cache/nv3-target/gate-RUN-F1): all seven STEP lines EXIT=0,
`GATE EXIT=0`, test-enforcement "all passed", including the 3 new unit rows and 3 new
e2e M rows. The checkout was removed afterwards.

## Scope
`git diff nucleus/v3-scaffold...HEAD --stat`: only tools/run_card.py (+18/-1) and
scripts/test-enforcement.sh (+73). Both are in Touch only. run_card.py adds only the
`failure_text` helper and changes only the one `failures.append` line. `gate_passed`, the tail
length, the "no tests ran" rule, the attempt loop and the module docstring are untouched.
test-enforcement.sh only adds things: `ft` and its rows directly after the `py` rows, the
`msg` arm, `ecm`/`msg_premise`, and the M run after the last e2e row (Q). No existing row,
helper or arm is edited.

## Findings
- Correctness: matches Work step 1 exactly. Anchored `re.fullmatch` on
  `STEP \S+ EXIT=[1-9][0-9]*`, in output order, from the full output. Header only when
  something matches. Tail computed as before. With no match the result is byte-identical
  to the old expression (unit row STEP-NONE checks equality). `re` was already imported.
  Pass/fail semantics are unchanged: the helper only builds text and cannot raise on a str.
- `l.rstrip("\r")` is dead code, because `str.splitlines()` already splits on a lone `\r`. It is
  harmless and the card asks for it. A mutant removing it would be equivalent, so none was applied.
- Size: the step list is not capped. That follows the card ("every line"). In practice
  it is bounded by gate.sh's seven column-0 STEP lines: test-enforcement indents
  all nested output (`sed 's/^/     /'`), and the passing gate log has exactly 7
  column-0 STEP lines. Only a card whose own Acceptance command prints many column-0 failing
  STEP lines could make it grow, and that output is author-controlled. No follow-up.
- `msg` fake arm: it runs only when FAKE_OC has the `msg` token, and only the XMSG run
  sets that. The runner appends the message as the opencode command's last element (run_card.py
  `cmd.append(msg)`), so `${@: -1}` is correct. Other e2e runs are unaffected (the full
  suite passes).
- XMSG id: no other e2e id (XA…XV, XLF, XOW, XR1-5) is a substring of XMSG, and XMSG is
  not a substring of any of them.
- The e2e premise row recomputes the command's output and checks that the STEP line is
  outside the 60-line tail, so the msg2 row cannot pass through the tail (confirmed by
  RF1-TAIL-ONLY being caught).
- Merge with X-6-F1-F2-F3-F1 (card/v3-X-6-F1-F2-F3-F1 at e5362e07, base e63e6699,
  worktree clean): `git merge-tree --write-tree 21f4188d card/v3-X-6-F1-F2-F3-F1` is
  clean, and the merged run_card.py parses. X-6's run_card.py hunks are in the header,
  imports, worktree_changes/changed_files/gate_committed/scope_violations. None is
  near `failure_text` (after gate_passed) or the Acceptance loop in main(), and X-6 does not
  touch test-enforcement.sh. Semantically independent.
- All mutants were applied in /tmp copies and the full test-enforcement.sh was run for each.
  All were caught. The copies were deleted. The card worktree was never modified.
