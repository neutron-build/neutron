Card: ENF-ZOMBIE-1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session efc9ff86-46a9-4bd1-bcf0-75bcee4a2506 (T3 Code review sub-agent of T3 thread 45584af2-e651-490e-862c-3402320cffa3; P gate of record; it wrote neither the card nor any ENF-ZOMBIE-1 commit)
Date: 2026-11-26
Gate-exit: GATE EXIT=0
Commit: 99eff859da91e2f17db05ffdd0eb5be17d1c5db1
Mutants: each planted by this gate in its own `git archive` extract of 99eff859 (or of the scaffold 42683bc6 for the comparison runs), never the worktree; each extract's diff against the pristine extract checked to be the one planted change; whole suite `bash scripts/test-enforcement.sh` run per mutant. Card: Z-1 ZOMBIE-ALIVE -> 4 rows (Z: a zombie counts as dead; Z: a reaped leader whose zombie member holds the group counts as dead; Z: K's SIGTERM shape passes under a PID 1 that reaps no orphans; Z: BG's same-group background child shape passes under a PID 1 that reaps no orphans), 910 ok; Z-4 GONE-READ-AS-ALIVE -> 7 rows (K; BG; BG: the one-killpg run passes; Z: XZ-b; Z: a gone pid counts as dead; Z: K zns; Z: BG zns), 907 ok. Z-2, Z-3, Z-5 not re-run by this gate (the pre-gate's logs). Own: O-1 UNDECODABLE-COMM (input mutant: XZ-d's live child sets comm b'\xff\xfe') -> Z: a live one is killed first, whatever EXPR says (alone; "Z: a live process is not dead" stays ok, passing vacuously on res()'s crash); O-2 STOPPED-COUNTS-DEAD (`state in (None, "Z", "T")`) -> UNCAUGHT (913 ok; its one FAIL is the podman opt-in load row); O-3 RUNNER-LEADER-ONLY-NO-PIDFD (run_card.py kill_child: `os.killpg` -> `os.kill`, the pidfd `select` dropped) -> 18 rows at the tip, 17 at the scaffold, row K among the scaffold's but NOT the tip's; O-4 RUNNER-FINALLY-LEADER-ONLY (main()'s finally: `end_container(child)` -> `child.kill(); child.wait()`) -> 3 rows at the tip (CT G3-FINALLY-GUARDED; ACC second SIGTERM during a line's init wait; the podman opt-in row, load noise), 3 at the scaffold, row K among the scaffold's but NOT the tip's. Group-aware F1 text (below) under O-4 -> row K and the Z K zns row fail again.
Followups: ENF-ZOMBIE-1-F1, ENF-ZOMBIE-1-F2

## Summary

The fix does what the card asks: a zombie now counts as dead, and a live or stopped process still counts as alive, so rows K and BG stop going red under a PID 1 that reaps no orphans. I reproduced the worker condition on this host. The tip's `res()` answers dead and the scaffold's answers alive. The fresh gate is green, Acceptance lines 1 and 2 pass, and the commit stays inside the freeze lift: one script, two edits, and nothing in the load-timing rows. The four departures from the card's literal text are all needed, because the literal text fails its own Acceptance. I adopt them, and the card amendment is below.

The fix has one real cost. The old `dead()` also answered "the process group is empty", because its `killpg` arm failed only on an empty group. The new `dead()` reads only the leader's state, so row K no longer sees a live same-group member that outlives the leader. I measured this with two runner mutants: row K catches both at the scaffold and neither at the tip. Other rows still catch both at the tip, so suite coverage of those mutants did not drop and the gap does not block. F1 restores the group check, discounting zombies, and fixes finding 1 in the same block. I tried F1's text in the whole suite.

## What this gate did itself

- **Fresh gate.** Per GATE-PROMPT step 1: both scripts were extracted through the shallow `--no-local` clone, with no EXTRACT FAILED line. Printed scaffold id: `42683bc64ced9e87276da5a55bd09d052e67f529`. Host before the run: no process named cargo or rustc (I waited for the A-4c0 pre-gate's cargo), 1-minute load 2.3, 33 GB available. Run on `99eff859da91e2f17db05ffdd0eb5be17d1c5db1` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-ENF-ZOMBIE-1`. Lines printed: `fresh-gate: commit 99eff859…`, `fresh-gate: base 42683bc6…`, `fresh-gate: checks passed (4931 files hashed in 1.76 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`. Steps: `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`. Cargo tests: 855 passed. Enforcement: 914 `ok`, 0 `FAIL`; rows K and BG and all 11 Z rows `ok`. Literal last line: `GATE EXIT=0`, exit 0, stderr empty. One run; `$fg` removed afterwards.
- **Scope.** `git diff nucleus/v3-scaffold...HEAD --stat` lists only `nucleus/v3/scripts/test-enforcement.sh` (+123/−3), and the worktree is clean. The only removed lines are the old `for kill …`, `kill(int(pidf.read_text()), …)` and `dead = lambda: gone`. The load-timing rows of ENF-TIMING-1 (RV cost, GUC premise, `_verify_history` cost, ACC silent line, R9 packed resume, the silence budget) grep byte-identical at the scaffold and the tip. run_card.py is untouched.
- **Acceptance.** Line 1 was run from the card's text in the worktree: K `ok`, BG `ok`, 11 Z rows `ok`, suite 914 ok / 0 FAIL, exit 0. Line 2: exit 0. Line 3 is covered by the fresh gate.
- **Reproduction.** Under a python subreaper that never reaps, the card's repro gives `ps -o stat=` = `Z` (`bash -c 'sleep 300 & echo $!'`, then `kill -9` on that pid). Through the real `res()` functions lifted from each file, with EXPR `dead()`, the scaffold exits 1 (alive) and the tip exits 0 (dead). A SIGSTOPped child exits 1 at the tip (alive), and the kill arms then end it.
- **Finding 1, reproduced.** For a live child with comm `b'\xff\xfeab'`, the tip's `res()` exits 1 with a `UnicodeDecodeError` traceback, and the child is still in state `S` afterwards: the kill-first arms never ran. Mutant O-1 is the same shape inside the suite.
- **Departures, read and checked.** (1) `exec python3 -`: in my own two-function trial, `$!` without `exec` is a bash subshell whose python outlives `kill`; with `exec`, `$!` is python itself. (2) The extra comment lines change no behaviour. (3) `diff "$t/res.def" <(declare -f res)`: the suite has no `export -f`, so the card's `bash -c` child has no `res` to print. (4) `$e/XBGZ.n.bg` is written only by the XBGZ run's fake inside a fresh `$e`, so the premise row must come after that run. `expect_ok` rows are independent, so the row's position gates nothing.

## Rulings

(a) **The fix is right for K and BG.** A zombie counts as dead; a live process (S/R) or a stopped one (T) counts as alive; `?` falls back to exactly the old answer. Callers whose EXPR does not use `dead()` (the roughly 60 other `res()` callers) change only by one extra `/proc` read before the same two signals. The exception is finding 1's crash, which none of them can reach: their pid file always holds the fake's `bash`. XKP does not change. One `dead()` caller does change, as the summary says: row K is blind to a live same-group member when the leader has been reaped. O-3 and O-4 are caught elsewhere, so this is a follow-up (F1), not REWORK. The card's sentence "no row's meaning weakens" (Work 1, BG bullet) is not true of K's group coverage, and the amendment below corrects it.
(b) **Adopt all four departures**, as the card amendment below. Requiring the literal text would reintroduce two red rows and a keeper leaked for 300 s.
(c) **Finding 1 is a follow-up (F1), not a fix in this card.** It cannot be reached by any caller in the suite. It is a robustness regression all the same: the scaffold's `dead()` never read `/proc`, and the card's own Limit 4 says an unreadable `/proc` keeps the kill-based answer. F1's text reads the stat file as bytes and maps a decode or parse failure to `?`.
(d) **Follow-up F2** adds rows for the stopped case (PG-2 / O-2, UNCAUGHT), the missing pid file (PG-3), a reaped leader with a live member (F1's property), and an undecodable comm. It also adds an `expect_ok … "not dead()"` row beside XZ-d, because an `expect_no` row passes on any crash of `res()` (O-1). PROBE-AFTER-KILL stays a disclosed limit, as the card says.
(e) **The stale line numbers** ("(lines 3184-3187)", now 3304–3307) are comment-only. Leave them in this card; F1 replaces the number with a text anchor.
(f) **The commit stays inside the freeze lift:** one file; only `res()`'s `dead()` block plus new Z rows; no load-timing row, threshold or bound touched; run_card.py unchanged. Both follow-ups are in protected `scripts/`, outside this lift. Each needs the owner's own word, or must ride on a card the owner authorises for this script.
(g) **The record.** All of 99eff859 (Work 1, 2 and 3) is the Opus 5.5 implementer's commit of 15:07, and no other commit is on the branch. The GLM 5.4 finishing session applied nothing (its report says Work 2 and 3 were already in the commit and it stopped). The card's Implementer line planned an Opus review of the branch before the gate. By the owner's word of 2026-11-26 15:3x, as recorded in that line by the orchestrator, GLM sessions verified the branch instead: the finishing session and the pre-gate. That word is the orchestrator's record, not the owner's text; this gate did not see it. This gate treated both GLM reports as untrusted. Every claim it relies on was re-run here: the fresh gate, Acceptance, the repro, Z-1, Z-4, finding 1, and departures 1, 3 and 4. Departure 2 is comment-only, so reading it was enough. This P verdict is the first Opus review of the commit, and §7.1 is met.

## Card amendment (adopt the commit's text; the orchestrator records it in the card before merge)

1. Work 2: replace the fenced block with tip lines 3207–3264 verbatim. That is the card's block with (i) the comment line `# the corpses exist only while their non-reaping parent lives.` extended by the three lines ending `holding the suite's stdout open.`, and (ii) `zkeeper() { PYTHONDONTWRITEBYTECODE=1 exec python3 - "$e" <<'P'`.
2. Work 3: replace the fenced block with tip lines 3265–3301 verbatim. That is the card's block with (i) `expect_ok "runner e2e: Z: premise: the probe runs the suite's live res" diff "$t/res.def" <(declare -f res)`, and (ii) the line `expect_ok "runner e2e: Z: premise: bgforge recorded its background pid (Z row)" grep -qxE '[0-9]+' "$e/XBGZ.n.bg"` moved to after the BG zns row.
3. Mutation evidence Z-2 and Z-3: "in `_state()` (tip line 3043; the same text is also at 3316 in the CT `alive()`, which is not the target)".
4. Work 1, BG bullet, after "no row's meaning weakens": add "except that row K no longer checks the process group: with the leader reaped, a live same-group member reads dead (gate O-3, O-4; ENF-ZOMBIE-1-F1)".

## Findings

1. **dead() no longer checks the process group** (non-blocking; F1). The old `killpg` arm made `dead()` false while any member was in the group. The new `dead()` reads the leader alone. Measured: under O-3 and O-4, row K is `FAIL` at the scaffold and `ok` at the tip. Both mutants are still caught at the tip, by 17 other rows and by 2 other rows respectively.
2. **An undecodable `stat` skips the fallback and the kill arms** (non-blocking; F1). The `UnicodeDecodeError` is a `ValueError`, not an `OSError`, so it escapes `_state()` and `res()` dies before the kill loop. Reproduced here; the live process survives. The same applies to an `IndexError` from a stat line without `)`.
3. **Contract items without a row** (non-blocking; F2). The stopped process (O-2 UNCAUGHT) and the missing pid file (PG-3) have no row. XZ-d's `expect_no` cannot tell "alive" from a crash (O-1).
4. **A stale comment reference** (non-blocking; folded into F1): "(lines 3184-3187)" is now 3304–3307.
5. **Load note.** The podman opt-in row went red once in each of my concurrent mutant runs at the tip (O-3, O-4, the F1 trial) and and in O-2's run, never in the unmutated runs (the fresh gate and Acceptance line 1). It is a known ENF-TIMING-1 load row (11-21 22:4x); my runs went up to four suites at once beside the fresh gate.

## Follow-ups

- **ENF-ZOMBIE-1-F1: make dead() group-aware, zombie-discounted, and decode-safe.** (Protected `scripts/`; needs the owner's word.) Replace the tip's `_state()` (lines 3034–3047) with the text below. It was tried by this gate in a whole-suite run on an extract of 99eff859: 913 ok and 1 FAIL, the podman load row only, with all 11 Z rows and K, BG and XKP `ok`. Under O-4 it makes row K and the Z K zns row fail again.
  ```python
  def _state():
      if pid is None:
          return None  # no pid recorded: dead by default, as before
      if not os.path.isdir("/proc"):
          return "?"  # nothing to read: the kill-based answer
      def stat(p):  # (state, pgrp) of /proc/<p>/stat, read as bytes after the last ')'
          with open(f"/proc/{p}/stat", "rb") as f:
              s = f.read().rsplit(b")", 1)[1].split()
          return s[0].decode("ascii", "replace"), int(s[2])
      try:
          try:
              st = stat(pid)[0]
          except FileNotFoundError:
              st = None  # no such pid: fully gone, reaped
          for d in os.listdir("/proc"):
              try:
                  ms, pg = stat(d) if d.isdigit() else ("", 0)
              except FileNotFoundError:
                  continue  # exited while we listed
              if pg == pid and ms != "Z":
                  return "S"  # a live member still holds the group
          return st
      except (OSError, ValueError, IndexError):
          return "?"  # present but unreadable or undecodable: the kill-based answer
  ```
  Also: update res()'s ENF-ZOMBIE-1 comment to say "a pid counts as dead when it and every member of its group are gone or Z". Replace "(lines 3184-3187)" with "(the CT note: "The fake's pid file holds a namespace pid in these runs")". Retarget mutants Z-2 and Z-3 at `stat()`'s `rsplit(b")", 1)[1]`.
- **ENF-ZOMBIE-1-F2: pin the uncovered contract items with rows.** (Protected `scripts/`; needs the owner's word; after F1.) Have zkeeper also build XZ-e, a SIGSTOPped live child; XZ-f, a live child whose comm is `b'\xff\xfe'`; and XZ-g, a group leader killed and reaped by the keeper while its same-group member is still LIVE. Add these rows: `expect_ok "runner e2e: Z: a stopped process is not dead" res XZ-e "not dead()"`; `expect_ok "runner e2e: Z: a live process is not dead (res answers)" res XZ-d "not dead()"` (catches a crash that XZ-d's `expect_no` passes); `expect_ok "runner e2e: Z: an undecodable comm still gets an answer" res XZ-f "not dead()"`; `expect_ok "runner e2e: Z: a reaped leader whose LIVE member holds the group is not dead" res XZ-g "not dead()"`; `expect_ok "runner e2e: Z: a missing pid file counts as dead" res XZ-none "dead()"`, where no pid file is written for XZ-none. Named mutants: O-2 → the XZ-e row; PG-3 → the XZ-none row; O-1 → the XZ-d and XZ-f rows; dropping F1's member scan → the XZ-g row.
