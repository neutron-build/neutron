Card: MERGE-CHAIN-1
Class: core
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (6f839608-93ae-4eeb-83ae-211b4e9652ca, 2026-11-16)
Date: 2026-11-16
Gate-exit: GATE EXIT=0
Commit: 624665328d018891a4f5db17c36b32dad743dd29
Mutants: CARRY-ANYTIP -> CM2+CM6a+CM6b, CARRY-EDIT -> CM3, CARRY-DELETE -> CM4b, CARRY-S -> CM5a+CM5b, G-SWAP-LSTREE -> CM1+CM1b+CM4a, G-SEMI-CONTINUE -> "changing another card's verdict is refused"+CM2+CM3+CM4a+CM4b+CM5a+CM5b+CM6a+CM6b, G-NO-FULLTREE -> CM1 (audit row), G-NO-LITERAL -> UNCAUGHT (equivalent), G-NO-ID -> UNCAUGHT (equivalent), G-UNREADABLE-ABSENT -> UNCAUGHT (masked by object verification)
Followups: none

## Verdict in one paragraph

MERGE. The change is exactly the card's Work 1–3, byte for byte, in the three
Touch-only files, and it does what the card's Goal states.
- **One tip.** The exemption admits one commit only, `aa11ac2f…`, under one ID
  only, E4-CHAIN-1. A rebuilt, amended or stacked commit is refused, and so is
  the same tip under another name. That tip's own pinned verdict is P on a P
  card.
- **Only additions.** For every base a promotable pass can have, the files the
  tip can carry are additions of plain files: at most the chain's 36 verdict
  files. They are 27 when the nine per-card merges come first, as section B
  orders.
- **No refusal lost.** Every other card branch and every recorded card merge is
  judged byte-identically by the old and new libraries: 157 branches in merge
  mode and 132 merges in audit mode.
- **The mutants.** All four named mutants are caught by exactly the rows the
  card names. Of the gate's own six, three are caught, two are equivalent and
  one is masked by object verification.
- **Amendment 1 is sound.**
- **Section B works** end to end in a throwaway clone: ten `merged` lines,
  then `promote-pass.sh` gave `GATE EXIT=0`, `queue-check: ok`, `audit: ok
  (130 commits)` and a push to a throwaway origin. Its one unmet precondition
  today is the owner's confirming line, which the card already lists.

No follow-up. Five notes are under "Findings".

## The gate's run (the run of record, once)

- **Extract.** GATE-PROMPT step 1's command, as written, except that `$fg` was
  made under `~/.cache/nv3-review/gate-MERGE-CHAIN-1/`. There was no
  `EXTRACT FAILED` line.
  - Scaffold id: `48e52d3bb7f0489138a7eabcbb37cd71014e42d7`.
  - sha256: `fresh-gate.sh` `40a5b232…4899`, `git-env.sh` `8a784782…3977`, the
    same as E4-CHAIN-1's gate.
  - The card's own `fresh-gate.sh` is byte-identical to the scaffold's.
  - `$fg` was removed afterwards.
- **The scaffold moved just before the run.** pass166 (S-EPSILON) was promoted
  at 12:57:57 (the scaffold's reflog), moving the scaffold from `d7d41677` to
  `48e52d3b`.
  - S-EPSILON changes only `docs/S-STORE.md` and `verdicts/S-EPSILON.md`.
  - `scripts/`, `tools/`, `CORE_PATHS` and `verdicts/README.md` are
    byte-identical to `d7d41677`'s, so the card's Base clause holds without a
    re-check.
  - `git merge-tree --write-tree 48e52d3b 62466532` is clean, and the merge base
    is still `d7d41677`.
- **Command.** `cd ~/Workspace/neutron-v3 && PATH=$HOME/.cargo/bin:$PATH
  CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-MERGE-CHAIN-1 bash
  "$fg/fresh-gate.sh" 624665328d018891a4f5db17c36b32dad743dd29`.
  - Run once, 13:01:02 to 13:08:15 (-07:00), with a cold target dir.
  - stdout and stderr were captured apart. stderr is empty, and the exit status
    is 0.
- **Lines, verbatim:**
  ```
  fresh-gate: commit 624665328d018891a4f5db17c36b32dad743dd29
  fresh-gate: base 48e52d3bb7f0489138a7eabcbb37cd71014e42d7
  fresh-gate: checks passed (4823 files hashed in 1.43 s)
  fresh-gate: 2 file(s) matched through their eol conversion
  STEP test EXIT=0
  STEP clippy EXIT=0
  STEP fmt EXIT=0
  STEP deny EXIT=0
  STEP wasm32 EXIT=0
  STEP core-paths-selftest EXIT=0
  STEP enforcement-tests EXIT=0
  STEP api-surface EXIT=0
  GATE EXIT=0
  ```
  - The literal last line is `GATE EXIT=0`.
  - `enforcement-tests` printed 903 `ok` lines and `test-enforcement: all
    passed`.
  - `deny` printed `advisories ok, bans ok, licenses ok, sources ok`.

## Scope

- **The diff.** `git diff nucleus/v3-scaffold...HEAD --stat` lists three files:
  `scripts/enforce-lib.sh` (+50/−1), `scripts/test-enforcement.sh` (+118) and
  `verdicts/README.md` (+11). That is exactly Touch only, and no file mode
  changed.
  - `ea027351` is Work 1, and `62466532` is Work 2 and 3.
- **Byte for byte.** I rebuilt each file from `d7d41677`'s blob plus the card's
  fenced Work text, inserted where the card says. Each rebuilt file equals the
  committed blob.
  - Work 1 (a) goes before the `check_merge` header comment, followed by one
    empty line.
  - Work 1 (b) turns the one arm line into five.
  - Work 2 goes after the line `expect_ok "git state: none after the full-id
    rows" no_state`.
  - Work 3 follows one empty line.
- **The only deleted line** is the old `verdicts/*)` arm.
  - The `SHORT_COMMIT_MERGES` marker block is byte-identical; Acceptance 3 checks
    it as well.
  - Nothing else in `scripts/` changed, and nothing in `tools/`, `CORE_PATHS`,
    `docs/`, `crates/` or `spikes/`.
- **Protected paths.** The change touches `scripts/` and `verdicts/README.md`,
  and only the Touch-only files. Card Notes read GATE-PROMPT step 2 that way for
  this one card, under the owner's conditional freeze exception.
- **No pre-existing row was touched or weakened.**
  - The suite change is a pure insertion.
  - The unmodified suite (pass166's gate, on a byte-identical file) prints
    883 `ok` lines; the new suite prints 903, which is 883 plus 20.
  - With embedded hex ids normalised, all 883 pre-existing row names match, in
    the same order.
- **shellcheck 0.10.0.** `enforce-lib.sh` has no new finding at info level. Its
  one warning, SC2034 on `AUDIT_PATHS`, predates this card. In the new test
  block there are only SC2317 (indirect calls) and SC1091 (the sourced copy),
  as in the L rows.
- **Code against the Goal, line by line.**
  - Rule 1 is the `"$tip $1"` string match, where `tip` is
    `rev-parse "$HEAD^{commit}"`.
  - Rule 2 is the first line of `chain_carry`.
  - Rule 3 is the first `ls-tree`, on BASE, which is the merge base in merge
    mode and `sha^1` in audit mode.
  - Rule 4 is the second `ls-tree`, on HEAD.
  - The `README.md` arm stays above the new arm, and the own verdict is skipped
    before the `case`.
  - Under `set -euo pipefail`, `chain_carry` runs only on the left of `&&`, and
    every test inside it is `|| return 1` or an `if`.

## Acceptance

| Line | Result |
|---|---|
| 1 | PASS. Run in a `--no-local` clone at `62466532`, alone: exit 0 in 238 s, 903 `ok`, 0 `FAIL`, last line `test-enforcement: all passed` |
| 2 | PASS (same clone). `aa11ac2f…` is `card/v3-E4-CHAIN-1`'s tip, on `6d5df726`, and changes only `verdicts/E4-CHAIN-1.md`. Its verdict reads `Commit: 6d5df726…`, P/P and MERGE-WITH-FOLLOWUPS. The list sources to exactly that one entry, and there is one matching line |
| 3 | PASS (same clone). `--numstat` independently shows exactly one deleted line across the three files, which also covers the line's `^-[^-]` blind spot for removed blank lines |
| 4 | PASS as amended: exit 0 in 67 s, and `$d` was removed. Run from the worktree root, as the card's Notes require; it only reads the real repository, and its temp dirs were under the gate's scratch dir. As written before the amendment: exit 2, `cd: can't cd to …/c/nucleus/v3` |
| 5 | The fresh gate above is this line's run: the same `gate.sh` on the same commit, in a verified checkout. Per GATE-PROMPT step 1, it was not re-run in the worker's worktree |

## Mutants

Every mutant was applied as an exact-text edit to `scripts/enforce-lib.sh` in a
throwaway `--no-local` clone, never the worker's worktree. Each was followed by
one full `bash scripts/test-enforcement.sh`, run alone, and was reverted by
exact re-edit; `git diff --quiet` held after each.
- **A first attempt was discarded.** It set `TMPDIR` under `$HOME`, which the
  runner's container mounts read-only, so 80 container rows failed for that
  reason alone. I stopped it, reverted its mutant by exact re-edit, and re-ran
  all ten with the default `TMPDIR`, as Acceptance 1 ran.
- **None of the ten runs has an unrelated `FAIL`.**

| Mutant | Edit | Result: failing rows |
|---|---|---|
| CARRY-ANYTIP | the loop sets `hit=1` for any entry | caught: CM2, CM6a, CM6b (exactly the card's) |
| CARRY-EDIT | drop `[ -z "$o" ] \|\| return 1` | caught: CM3 (the card's) |
| CARRY-DELETE | drop `[ -n "$o" ] \|\| return 1` | caught: CM4b (the card's) |
| CARRY-S | drop the P/P line | caught: CM5a, CM5b (the card's) |
| G-SWAP-LSTREE | `"$2"`/`"$3"` swapped in the two `ls-tree` calls | caught: CM1 (merge), CM1 (files on the pass), CM1b, CM4a; with the swap a delete reads "absent at HEAD, present at BASE" and is carried |
| G-SEMI-CONTINUE | `chain_carry …; continue` | caught: the pre-existing row "changing another card's verdict is refused", plus CM2, CM3, CM4a, CM4b, CM5a, CM5b, CM6a, CM6b |
| G-NO-FULLTREE | drop `--full-tree` from both calls | caught: "CM1: the copy's audit accepts the chain merge". The audit runs inside `nucleus/v3`, so the path resolves below the cwd and nothing is ever carried. Merge mode runs from the top and is unaffected |
| G-NO-LITERAL | drop `--literal-pathspecs` from both calls | UNCAUGHT, equivalent: see below |
| G-NO-ID | compare the tip only (`"${e%% *}" = "$tip"`) | UNCAUGHT, equivalent: see below |
| G-UNREADABLE-ABSENT | first `ls-tree` failure reads as absent (`\|\| o=`) | UNCAUGHT, masked: see below |

Why each UNCAUGHT one is harmless:
- **G-NO-LITERAL.** Every PATH starts `nucleus/v3/verdicts/`, so no
  leading-colon magic can occur, and `ls-tree` applies no wildcard with or
  without the flag (probe E1).
  - `GIT_ICASE_PATHSPECS=1` and `GIT_GLOB_PATHSPECS=1` make that exact call
    fatal either way (rc 128), so `chain_carry` returns 1 (E2).
  - `core.ignoreCase=true` changes nothing (E3).
  - The flag is defence in depth only.
- **G-NO-ID.** I took a no-ID copy of the library and pointed it at the real
  `aa11ac2f…` under another ID whose verdict file the tip carries.
  - X-5c-F4-F2: refused, with "the branch tip must be the verdict commit
    (changing only …/X-5c-F4-F2.md); it changes: …/E4-CHAIN-1.md" and the
    `Commit:` mismatch.
  - X-5c-F4-F1-F1-F3: refused the same way, and its verdict is ESCALATE.
  - `E4-judgement`: refused; it has no verdict fields.
  - The tip changes only `verdicts/E4-CHAIN-1.md`, so the tip rule alone
    refuses any other ID. The ID half is defence in depth, as the card says.
- **G-UNREADABLE-ABSENT.** I removed the merge base's `verdicts/` tree object in
  a throwaway repo.
  - The real `chain_carry` returns 1 there (`ls-tree` exit 1, "Could not
    read"), and the mutant returns 0.
  - `check_merge` refuses anyway, in merge mode with the mutant: "verify_objects:
    git rev-list failed (exit 128; cannot verify objects)", rc 1.
  - The audit's `verify_reachable` fails the same way.
  - So both modes refuse an unreadable tree before `chain_carry` is consulted.

## Point 2: the exemption, attacked

**Exactly one tip.**
- The key is `git rev-parse "$HEAD^{commit}"`, string-equal together with the
  ID.
- These are refused:
  - one commit on top (CM6a);
  - a rebuild (CM6b);
  - the same verdict tree under a new id (Acceptance 4: 27 carry lines);
  - the tip under another name (CM7, and the G-NO-ID probe).
- An amended or re-signed commit is a new id, refused as in CM6b.
- That tip's verdict blob is pinned, and it is P on a P card, so the tier test
  is defence in depth for this entry.

**What it can carry.**
- The tip pins its whole history. Any pass that can be promoted contains the
  scaffold, so its merge base with the tip is `ee8298c3` or a chain commit. The
  candidates are therefore the chain's own changes under `verdicts/`.
- From `ee8298c3` there are 37 such paths (36 plus its own):
  - all `A`, all mode 100644, all named in `[A-Za-z0-9._/-]`;
  - no case-insensitive collision with each other or with the scaffold's
    verdict files;
  - none present on the scaffold.
- Across the whole range, every changed path is a regular file (mode 100644 or
  100755).
- I measured every merge base a promotable pass can have. Those are
  `ee8298c3` and the nine member tips, which can only merge in order.
  - Every change under `verdicts/` from each of them to the tip is an `A`:
    37 down to 28, its own file included.
  - None of those trees has a symlink or a gitlink under `nucleus/v3`.
  - For this entry, the merge only ever adds.

**Extension.**
- Nothing reads an environment variable, a file or a marker. A bash array
  cannot be exported, and the assignment overwrites any earlier value.
- CM9 pins one entry of the form `<40 hex> E4-CHAIN-1`, in both the marker block
  and the sourced array, so a second entry or an append later in
  `enforce-lib.sh` fails it.
- CM9 does not pin the id itself, just as R1 pins `SHORT_COMMIT_MERGES`' count
  and form but not its ids. Acceptance 2 pinned the id at merge time.
- An id swap, or an append in another sourced script, is a `scripts/` (CORE)
  change under a P verdict and the freeze.

**Path tricks.**
- Case, unicode, `..`, symlink and submodule: none occurs in the pinned range
  (above), and `ls-tree` matching is byte-exact (E1, E3).
- Pathspec magic: covered under G-NO-LITERAL.
- Renames: they are listed as a delete plus an add (`--no-renames`), and a
  delete of a file present at BASE is refused (CM4a/CM4b).

**Merge mode against audit mode.** I probed both directions in a throwaway repo
with a copy listing test tips.
- **(a) Card open point 2, reproduced.** The pass side already holds `X.md`, and
  the merge base does not.
  - Merge mode accepts.
  - Git then folds an identical file, and the audit refuses it ("present at
    `sha^1`"); or git stops on a conflict.
  - So it fails closed before promotion.
  - It cannot arise for E4-CHAIN-1, since none of its files is on the scaffold.
- **(b) The converse.**
  - The audit's BASE is `sha^1`. A listed tip that edits `OLD.md` therefore
    passes audit-mode `check_merge` when the first parent lacks `OLD.md`.
  - If the pass deleted `OLD.md`, `clean_merge` refuses the merge
    (modify/delete).
  - If the pass renamed `OLD.md` to `OLD2.md`, git merges the edit into
    `OLD2.md` cleanly, and the audit accepts that one merge. The merge that
    renamed `OLD.md` is itself refused by the audit, which names both paths.
  - Not reachable for this entry, whose range only adds. See Findings, item 2.

**Existing history.**
- **Merge mode.** Old (`d7d41677`) and new `check_merge` were run on all 158
  card branches against `48e52d3b`.
  - 157 give byte-identical output: 76 accepted and 81 refused.
  - The one difference is E4-CHAIN-1: the old library refuses with exactly 36
    carry lines and nothing else; the new one accepts.
- **Audit mode.** Every first-parent card merge on the scaffold and on all 166
  `scratch/*` refs: 132 distinct merges, all byte-identical. 131 are accepted.
  One is refused by both: `f177d05e` (X-1a-F1-F3 in pass75, a short-id verdict
  not listed in `SHORT_COMMIT_MERGES`).
- **The stock `audit-merges.sh`, old and new.** Both print `audit: ok (119
  commits)` on `48e52d3b` and `audit: ok (115 commits)` on pass153 (the nine
  chain merges).

## Point 3: orchestrator amendment 1 is sound

- **The change.** The pre-amendment line (in
  `~/.cache/nv3-review/MERGE-CHAIN-1.card-backup-pre-amend1.local.md`) and the
  amended one differ by exactly the two tokens `-b nucleus/v3-scaffold`. The
  card's other lines and the rest of line 4 are unchanged.
- **Why it was needed.** The real common dir's HEAD is
  `refs/heads/nucleus/v2-scaffold`, which has no `nucleus/v3`.
  - As written before the amendment, the line exits 2 at `cd`, before any
    check.
  - The author's probe repo `chain-merge-probe-2/repo` is detached at
    `d7d41677`, as the amendment says.
- **Why it changes nothing the line tests.** `-b` selects only the clone's first
  checkout.
  - Without `--single-branch`, every `origin/card/v3-*` ref still arrives.
  - The line then builds `scratch/dry` from `origin/nucleus/v3-scaffold`
    explicitly.
  - It takes this card's scripts from `git archive HEAD` and the scaffold's from
    the checked-out tree.
- **The amended line passes every check.**
  - It makes the nine merges.
  - The rebuilt id is refused with 27 lines.
  - The scaffold's `merge-card.sh` refuses with 27.
  - This card's merge adds 28 verdict files.
  - The audit is ok.

## Point 4: CM8's stray line

`Automatic merge went well; stopped before committing as requested` comes from
CM8's setup `git merge -q --no-ff --no-commit`, outside any row. Git prints it
on stderr despite `-q`.
- **Ruling: a note, not a follow-up.**
- Nothing parses the line:
  - `gate.sh` reads only exit codes;
  - the suite's last stdout line stays `test-enforcement: all passed`;
  - the `ok`/`FAIL` counts are unaffected.
- Silencing it is a `scripts/` edit under the freeze, which is not worth an
  owner decision. A later card that touches `test-enforcement.sh` under its own
  exception may add `>/dev/null 2>&1`.

## Point 5: section B, dry run

**Setup.** A throwaway `--no-local` clone, whose `origin` (fetch and push) is a
throwaway bare repo. Nothing in the real repository was written.

**Section A step 3, in the clone.**
- `card/v3-MERGE-CHAIN-1` got a stub verdict (`Gate-model:
  DRY-RUN-NOT-A-GATE`).
- `merge-card.sh MERGE-CHAIN-1` printed `merge-card: MERGE-CHAIN-1 merged into
  scratch/pass901`.
- Then `queue-check: ok` and `audit: ok (120 commits)`, and the clone's
  scaffold was fast-forwarded to `761b13dd`.
- `promote-pass.sh`'s gate was not repeated for this pass; the fresh gate above
  ran this code.

**Section B, run as written** (N=902):
- **The merges.** Ten `merge-card: <ID> merged into scratch/pass902` lines, in
  9 s.
- **The E4-CHAIN-1 merge.**
  - It is titled `nucleus v3: merge card/v3-E4-CHAIN-1 (MERGE-WITH-FOLLOWUPS,
    gate P/claude-opus-5-5)`.
  - Against its first parent it changes 25 paths under `spikes/` (9 `A`,
    16 `M`) and adds 28 files under `verdicts/`.
  - `spikes/e0-btree/Cargo.toml` is unchanged. All of this is as the card
    predicts.
- **`promote-pass.sh scratch/pass902`:** run from 14:03:26 to 14:13:40. It is
  the first gate on the merged tree.
  - Eight `STEP … EXIT=0` lines (test, clippy, fmt, deny, wasm32,
    core-paths-selftest, enforcement-tests, api-surface), then `GATE EXIT=0`.
  - `enforcement-tests` printed 903 `ok` lines and `test-enforcement: all
    passed`.
  - `deny` printed `advisories ok, bans ok, licenses ok, sources ok`.
  - Then `queue-check: ok`, `audit: ok (130 commits)` and `promote-pass:
    nucleus/v3-scaffold -> a80d2dc4`. The push went to the throwaway origin
    only.
  - `cb2aca0d`, `754c6536`, `48ac9920`, `6d5df726`, `aa11ac2f` and `11aea6a7`
    are then ancestors of the clone's scaffold.
  - The real repository's refs were unchanged afterwards: scaffold `48e52d3b`,
    `card/v3-E4-CHAIN-1` `aa11ac2f`, `card/v3-MERGE-CHAIN-1` `62466532`.

**Preconditions today.**
- **Met:**
  - `E4-CHAIN-1-F1` has a QUEUE row (`waiting`);
  - the E4-CHAIN-1 card is `Status: ready`, and its row is `gating`;
  - `N` computes to 167 (the one non-numeric name, `scratch/pass3-partial`, does
    not disturb it).
- **Unmet: the owner's confirming line** (E4-CHAIN-1 Open point 1). QUEUE's
  E4-CHAIN-1 row reads "NOT yet answered". The card already lists it.
- **None missing from the card's list.** Two are implicit and fail closed:
  - every branch named must still be at its verdict tip, since merge-card
    refuses a moved one;
  - the nine members' follow-up rows must still exist. §2.7 closes rows; it
    does not delete them.

## Findings (none blocking; no follow-up)

1. **The nine-first order is section B's, not the tool's.**
   - From today's scaffold without the nine, the new library accepts E4-CHAIN-1
     carrying all 36 chain verdict files, the nine members' included.
   - Nothing unreviewed rides then: each of the nine passes `merge-card` on its
     own (Acceptance 4, pass153). What is lost is per-card provenance.
   - Merge-card would then print "merged" for each of the nine although git
     makes no commit ("Already up to date").
   - The order in section B is the guard (For the orchestrator, item 1).
2. **Audit-mode BASE (Point 2 (b)).**
   - "Absent at `sha^1`" does not by itself prove that the range only adds.
   - It holds here for two reasons: the pinned range adds only, and the audit
     refuses a pass-side delete or rename of a verdict at its own merge.
     `README.md` is the only verdict path a card may delete, and a delete
     conflicts with any edit.
   - A future second entry, which needs its own owner decision and P card,
     should also require absence at the merge base in the audit.
3. **A comment slip in `enforce-lib.sh`.** It reads "three re-verdicts committed
   on another card's verdict". That holds for `26bcee4b` (on `60f919d7`) and
   `2e8d1b13` (on `b69e0781`). But `18d2530f` sits on its own card's first
   verdict, `04a269b4` (E4-CHAIN-1 §2.3). The comment's pointers (the ruling
   under `~/.cache`, and QUEUE) are local-only; the committed record is
   E4-CHAIN-1's verdict and this one.
4. **The card's prose miscounts the suite.** It says "882 pre-existing and 21
   new"; the actual count is 883 plus 20, and the total of 903 is right. The
   committed files carry no such count.
5. **CM8's stray line** (Point 4).

**Why there are no follow-ups.** Each item is a note. Fixing 2, 3 or 5 is a
`scripts/` edit, which needs a new owner decision under the freeze, and none is
worth one. A later card that touches these files under its own exception may
fold them in.

## For the orchestrator

1. **In the chain pass, run the nine before E4-CHAIN-1**, one at a time, and
   stop at the first non-zero exit, exactly as section B says. The tool will not
   stop a skipped order (Finding 1).
2. **Recommended: ask E4-CHAIN-1's owner question before this card's merge
   pass,** not only before the chain pass.
   - The exemption has no other use.
   - If the owner says no, this merge would leave on the scaffold a frozen entry
     that only another owner-approved card can remove.
   - This is the timing E4-CHAIN-1's gate recommended.
3. **This card's merge pass** runs from `48e52d3b` or later, after re-checking
   that the three Touch-only files there are still byte-identical to
   `d7d41677`'s.

## For the owner (stated plainly; not a ruling)

- **The question.** E4-CHAIN-1's verdict gives it under "For the owner",
  item 1. QUEUE says it is "NOT yet answered". Verbatim:

  > "Merging the E4 chain puts on the scaffold the commits of four cards whose own gates ended ESCALATE: X-5c-F4-F1-F1-F3, X-5c-F4-F1-F1-F3-F2, X-5c-F4-F1-F1-F3-F2-F1 and X-5c-F4-F3. Your decisions of 2026-11-07 and 2026-11-10 answered two of them. P answered the other two by amending rules P had written for those cards, after seeing the data and labelled so; these did not come to you and do not touch E4's pass definition. Continuation cards then passed their gates, and E4 was judged on the result. The four would reach the scaffold on one P verdict over the whole chain (E4-CHAIN-1), not on MERGE verdicts of their own. Do you accept that: yes or no?"
- **Why it matters now.** This card makes that merge possible, and it does
  nothing else. Whether the owner's conditional reply of 2026-11-16 also accepts
  the four ESCALATE members' commits onto the scaffold under one P chain verdict
  is a reading, not the owner's words.

## Not verified

- The real chain pass, its push and its promotion. Only the throwaway dry run
  above was performed.
- That the dry run's green gate repeats at the real pass, which runs at
  another time and under another load.
- That the host is quiet when the real passes run.
- A same-uid writer racing `card/v3-E4-CHAIN-1` during `merge-card.sh` is out of
  scope, as it is for every rule; the audit re-checks with fixed ids.
- Member `Gate-session` ids against T3 (the README's trust limit).
