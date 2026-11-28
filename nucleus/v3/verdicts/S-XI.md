Card: S-XI
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5 (max)
Gate-session: T3 review sub-agent, Claude Code session a0611d71-6c86-40a2-b33f-b529cf94da3d, dispatched from T3 thread 45584af2-e651-490e-862c-3402320cffa3 (an independent P gate for cards S-XI and S-2a-1 together: this agent wrote neither card, neither application (4eb010b1, 1da23bd3), nor the first S-2a-1 verdict (d28161e7), and is not the orchestrator)
Date: 2026-11-27
Gate-exit: GATE EXIT=0
Commit: 4eb010b1bfa5c7efac5193ca6aa249520646b9bb
Mutants: spec card, so no code to mutate; each named mutant judged by reasoning against its test as written. TIMED-EARLY -> t_timed_wait (OS form: a timed-out return with StdEnv::now below start + d; harness form: a timed-out return before the step that reaches D) | TIMED-NEVER -> t_timed_wait (harness: the waiter stays parked past every step, a harness deadlock; OS form: the 10 d watchdog) | TIMED-NOTIFY-LOST -> t_timed_wait (harness: the notified waiter returns timed-out at D; OS form: a notify after the parked hook returns timed-out) | SIM-TIMED-DEADLOCK -> t_sim_timed_advance | CKPT-PAIR-SPLIT-READ -> t_ckpt_pair_one_point (two entries to the I-WAL-TS-ORDER section, either order). Four of my own, by reasoning: SIM-ADVANCE-BACKWARD (the advance sets the clock to a parked deadline that an earlier sleep already passed) UNCAUGHT by t_sim_timed_advance as written (S-XI-F1 (a), (c)) | STEP-WAKES-LATER-DEADLINE (a clock step wakes every parked timed waiter, deadline reached or not) UNCAUGHT by t_timed_wait's harness form with one waiter (S-XI-F1 (d)) | STEP-WAKES-ONE-OF-TIED (of two waiters with one deadline, the step wakes one) allowed by T1's text, refused by I-TIMED-WAIT's (S-XI-F1 (b)) | PAIR-POS-OUTSIDE (step 0 reads the append position in its own append-section entry before entering the I-WAL-TS-ORDER section) UNCAUGHT with the hook at that section's exits only (S-XI-F1 (f)). Six text plants in a throwaway --no-local clone, each committed, judged by Acceptance lines 1-7 in the default locale (en_CA.UTF-8) and under LC_ALL=C, the same lines failing in both: "never returns timed-out before its deadline" dropped -> 6 | TIMED-NOTIFY-LOST renamed in I-TIMED-WAIT's mutants cell -> 4,6 | t_ckpt_pair_one_point's B-1c row dropped -> 4,5,6 | status 7.25 -> 1,6 | "which the harness reports as a deadlock" dropped -> 6 | a second file added -> 7
Followups: S-XI-F1, S-XI-F2, S-XI-F3

## Verdict in brief
**MERGE-WITH-FOLLOWUPS** on `4eb010b1bfa5c7efac5193ca6aa249520646b9bb` (S-STORE draft 7.24 on card S-LAMBDA's 7.23). The application is exactly the card's eight blocks, the scope is one file, and Acceptance passes in both locales. The design is sound, and it meets verdict S-2a-1 (d28161e7) R1's requirement for every build that requirement named (platform, harness, SimEnv):
- A timed waiter cannot miss a notify.
- It never times out early on the build's clock, and C-T0 v8's caller re-tests against `Env::now` anyway.
- The platform's "same monotonic clock" holds on Linux, which I verified with strace and a 30,000-wait probe. It can be tested on every target through `t_timed_wait`'s OS form.
- SimEnv's advance hides no deadlock.

The gaps are wording and test-text gaps, all on cards not yet cut: A-4c0-T, A-7c and A-8a (S-XI-F1); one plan edge (S-XI-F2); and N10's cooperative mode, which no backend serves yet (S-XI-F3). None is a defect of the merged text that a later card cannot close before it is used. The fresh gate is green on its first run.

## What I verified myself
1. **Fresh gate** (GATE-PROMPT step 1 with the Fast-track priority): the scaffold's `fresh-gate.sh` (sha256 40a5b232…) and `git-env.sh` (8a784782…), extracted through the shallow `--no-local` clone. It printed scaffold id `d155924534028dfc147aaad3d0991575fae4a2c3`, with no `EXTRACT FAILED`. It ran under `systemd-run --user --scope --quiet -p CPUWeight=10000 --` (the scope worked) with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-XI`, cold, from 23:30:31 to 23:47:06, after S-2a-1's run and not beside it. The extract was removed afterwards. Lines, verbatim:
   ```
   fresh-gate: commit 4eb010b1bfa5c7efac5193ca6aa249520646b9bb
   fresh-gate: base d155924534028dfc147aaad3d0991575fae4a2c3
   fresh-gate: checks passed (4944 files hashed in 1.51 s)
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
   - The literal last line is `GATE EXIT=0`, with exit status 0 and an empty stderr.
   - The output has 914 `ok` rows, no `not ok` or `FAIL` row, `test-enforcement: all passed`, and 72 cargo `test result: ok` lines with no `FAILED`.
   - FAST-TRACK §3's known rows all passed, so no re-run was needed: "ok   runner refs: RV cost ten honest moved refs verify in under 2 s"; "ok   (e cost) _verify_history on the widest range is under 2 s"; the three "premise: GUC" rows; "ok   runner e2e: K: SIGTERM kills opencode and closes the ledger range"; and the zombie row that failed in S-2a-1's first run, "ok   runner e2e: Z: K's SIGTERM shape passes under a PID 1 that reaps no orphans".
   - **The scaffold moved during this gate**, to `84a5cc707efcd42d6ecda2c46e2d77b063d8e2a3` (card ENF-FASTGATE-1: `scripts/`, `tools/`, CI, and its verdict). `docs/` is unchanged there, so S-STORE is still 7.23 (b55d4fc8…) and the card's Base condition holds. `git merge-tree` merges `card/v3-S-XI` onto it cleanly, and Acceptance lines 1–7, re-run in a fresh clone against it, pass in both locales.
2. **Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: `nucleus/v3/docs/S-STORE.md` only, +19/−3. The worktree is clean, and the branch is one commit on d1559245, which was the scaffold tip when this gate began (item 1 covers the move). `docs/` is protected, and this is a Class: spec card with a P gate.
3. **Mechanics, by my own parser** (`~/.cache/nv3-review/gate-S-XI/myparse.py`, independent of the card's `blocks.py`).
   - The card is sha256 85657c2e…, 186 lines, as briefed. It has 8 blocks: E1, T1, R1, R2, N1, Q1, Q2, E2.
   - Each Old occurs once in the untouched base, in file order, and once at its turn.
   - Applied with `<date>` = 2026-11-27 (the commit's date), the blocks give the tip byte for byte: sha256 642c27f6…, 2060 lines. The reverse gives the base, b55d4fc8…, 2044 lines.
4. **Acceptance.** Lines 1–7 pass in a fresh `--no-local` clone of the card branch, run by `/bin/sh -c`, in the default locale and under LC_ALL=C. Line 4 prints `180 rows; 300 mutants; 49 invariants; 58 tests` and `OK`. Line 8 is the gate run above.
5. **Counts and the Phase B rule, by my own scripts.** §15.1 grows from 176 to 180 rows. The Phase B cards in its Card column are unchanged, and its Needs column gains B-2a, as N1 says. Each new mutant is judged on a Phase A row: TIMED-* at A-4c0-T, SIM-TIMED-DEADLOCK at A-8a, CKPT-PAIR-SPLIT-READ at A-7c (and B-1c). The mutants judged only on Phase B rows are the same set before and after, so E2's "24 … as at drafts 7.18 to 7.23" holds.
6. **Claims checked against the code at d1559245.**
   - The sync layer's only backends are `StdPrimitives` (`sync.rs:437`) and the harness's `Shuttle` (`tests/harness/mod.rs:112`).
   - The store's SimEnv tests run one task each (`run_life(1, …)`; `wal_crash.rs`: "No test runs two tasks").
   - `StdEnv::now` is `Instant`-based (`std_env.rs:326`).
   - SimEnv's clock moves only on `sleep`, and all tasks blocked is DEADLOCK (`sim/env.rs`, `sim/sched.rs` `handoff_locked`).
   - `Condvar::wait_timeout` in shuttle-std 0.1.5, which the harness's shuttle 0.9.10 re-exports, never times out (`src/sync/condvar.rs`: "TODO support the timeout case -- this method never times out"). So the harness's model clock must be the harness's own, as T1 has it.
7. **The "same monotonic clock" assumption** (the card's "Unverified", the brief's question).
   - **Safe for C-T0.** C-T0 v8 §6.3 re-reads `Env::now` on every return, and its "deadline passed" term reads `Env::now`. So a timed-out return before the deadline on another clock is only a spurious wake, never an early 55P03 or an early search.
   - **True on Linux, shown two ways.** I traced a rustc 1.99 build of `Condvar::wait_timeout(20 ms)`: it calls `futex(FUTEX_WAIT_BITSET_PRIVATE, …, {tv_sec=339089, …})`, an absolute timeout on CLOCK_MONOTONIC (the value is the host's uptime, and there is no `FUTEX_CLOCK_REALTIME`), which is `Instant`'s clock. Its deadline is computed inside the call, after the caller's `Env::now`, so it cannot fall before the caller's deadline. A probe of 30,000 waits from 1 ns to 3 ms, run under the gate's load, found 0 timed-out returns before the duration on `Instant` and 0 spurious returns. The worst lateness was 337.7 ms (S-XI-F1 (e)).
   - **Testable everywhere.** `t_timed_wait`'s OS form fails any target where a timed-out return comes before `d` on `StdEnv::now`.
   - **Not verified on other targets.** On targets where std computes the timeout from the wall clock, the lateness half ("the OS's wake latency is its only lateness") does not follow (NIT).
8. **Design against S-STORE §9.2, §15 and the Env contract.**
   - **No lost notify.** The timed wait is a condition-variable wait that releases the mutex and sleeps in one step (§9.2 item 3's form). C-T0's woken flag, set and cleared under the parker mutex, keeps a wake made before the park, and every return re-tests the predicate.
   - **Poison.** The mark is in every predicate, and the broadcast takes each queue's mutex and signals it, so a timed waiter is woken by the broadcast or by its deadline. Either way it re-tests and returns the error. The "treat a timed wait as any other wait" sentence is enough, given §9.2 item 4.
   - **No polling.** A deadline is met only by a timed wait. D-1's case against a timer task holds under N10: the callers' threads may all be parked.
   - **R-1-memory.** No new consumer: the platform wait allocates nothing, and the harness registry and SimEnv's per-task deadline are test infrastructure.
   - **wasm32.** `std::sync::Condvar::wait_timeout` compiles there (F6 holds, and the gate's wasm32 step builds `nucleus-store`). At run time it panics on wasm32-unknown-unknown, as the untimed `wait` already does. That is S-XI-F3's ground, not a regression.
   - **SimEnv determinism.** The advance is decided under the world lock, in `handoff_locked`'s no-runnable branch, from the scheduling state and virtual time alone, so a seed replays. Two conditions are unstated: a tie-break among equal deadlines, and forward-only movement (S-XI-F1 (a), (b)).
   - **Hidden deadlocks.** None. With no timed waiter, all tasks parked is still DEADLOCK. Untimed tasks deadlocked beside a timed waiter that keeps re-arming end in YIELD_BUDGET's Livelock on SimEnv, or in the step budget under the harness, never in a pass. A lost wakeup on a waiter that holds a timed wait becomes a timeout, as it would in real time. A world that expects no timeout should count an unexpected 55P03 as a failure (NIT for A-8a and B-7's G0).
9. **The collision with card S-NU** (brief; card sha256 8a1ad2ab…).
   - S-NU has three blocks: E1 (status), N1 (an insertion in §15 "Stand-ins", S1's bullet) and E2 (Changelog). N1 shares no line with any S-XI block.
   - I applied both cards to the base in both orders. Every Old occurs once at its turn in each order. The two results are identical except for the status line and the order of the Changelog entries, and each has two "Draft 7.24" entries.
   - With S-NU first, line 4's deltas still hold (176 → 180 rows, 295 → 300 mutants, 48 → 49 invariants, 55 → 58 tests), and line 5's numstat is still `19 3`.

   The ruling is in Notes.

## Findings
**F-a. SimEnv's advance and the harness's clock step: direction, ties and the "step below D"** (S-XI-F1 (a)–(d)).
- **Direction.** T1 says the scheduler "moves SimEnv's virtual clock to the earliest such deadline". SimEnv's clock also moves on `sleep`. So in an ordinary world (task A parks with deadline 5, task B sleeps 10 and then parks), the earliest deadline lies below the clock, and "moves … to" it reads as moving the clock back, against E-CLOCK/RAND's monotonic `now`. It must be the later of the clock and that deadline. The same holds for the harness's step when a deadline passed before its waiter parked, which is T-2's own case.
- **Ties.** T1 wakes "that waiter". I-TIMED-WAIT says a timed wait returns "at the clock step that passes it". With two waiters on one deadline, which `t_park_timed` (b) produces whenever both owners park at the same model time, the two texts disagree. The tie-break must be stated and deterministic (S-DET; F2's replay), or every waiter whose deadline the step reaches must wake.
- **The step below D.** T1's step always goes to the earliest parked deadline, so with the one waiter `t_timed_wait` names, no step below `D` exists. The clause "is not woken timed-out by a step below D" is then vacuous, and STEP-WAKES-LATER-DEADLINE survives. A second timed waiter with an earlier deadline makes the step exist.
- **No sleep in `t_sim_timed_advance`.** Its fixture has no `sleep`, so SIM-ADVANCE-BACKWARD survives it.

**F-b. `t_sim_timed_advance`'s Needs omits A-4c0-T** (S-XI-F1 (c), S-XI-F2). §15.1's preamble: "*Needs* names the cards whose code the form uses besides its own, each, for a Phase A card, in that card's Depends closure". The form runs on the sync layer's timed wait, which is A-4c0-T's primitive in `Primitives`. The row names only A-1b, and no plan row puts A-4c0-T in A-8a's Depends: card S-2a-1's block P-A gives A-4c0-T no dependant but B-2b.

**F-c. `t_timed_wait`'s OS-form watchdog can fail a correct build under load** (S-XI-F1 (e)). "a watchdog fails the form if a wait has not returned by `10 d`". My probe saw 337.7 ms of wake lateness at `d ≤ 3 ms` under the gate's load. A small `d` would make the form flaky against a correct build, and FAST-TRACK §3's known list would not excuse that row. The watchdog needs a stated floor in seconds beside `10 d`, since it bounds failure only.

**F-d. `t_ckpt_pair_one_point`'s hook sits only at the I-WAL-TS-ORDER section's exits** (S-XI-F1 (f)). §13 step 0 reads the append position in an entry to the append critical section, taken inside the I-WAL-TS-ORDER section. Suppose a mutant moves that entry before the section: it reads `wal_ckpt_end`, then enters the section for `ts_ckpt_hi`. Then no hook runs between the reads, and the pair is split without a failure. The other order (`ts_ckpt_hi` first) is caught. The hook should also sit at step 0's exits from the append section.

**F-e. N10's cooperative mode has no timed wait** (S-XI-F3). Embedded and wasm builds run on the caller's thread (N10). In a browser leader worker, several sessions share one thread, so a lock-table wait there must park the task in a cooperative scheduler, not block the thread. std's `Condvar` on wasm32-unknown-unknown panics on any wait. The rule that serves that build is SimEnv's advance on the real clock: when every task of the thread is parked and one holds a timed wait, the thread waits on the build's clock until the earliest deadline (a worker's `Atomics.wait` with a timeout), or it returns to the host. No card states this. "Every build reads one clock" binds D-5a, but the advance does not. T1's "only test configurations" run timed terms off, which a browser build without it would contradict.

## Rulings on the card's open points
- **T-1. Agreed:** A-8a. It is the first Phase A row that runs the store on SimEnv's scheduler, and §15.1 needs a Phase A row for SIM-TIMED-DEADLOCK. Condition: A-8a depends on A-4c0-T, and the row names it (F-b).
- **T-2. Agreed:** a deadline may pass while its waiter is on its way to the park. That is real time's behaviour, and the predicate's re-test covers it. Condition: the clock moves forward only (F-a).
- **T-3. Agreed:** S-2a-1-F1 rides with §9.2's broadcast path (A-3c).

## Follow-ups
- **S-XI-F1 (the next batched S-STORE card; until it lands, the cards named here carry its items in their own text, which I-TIMED-WAIT and E-CLOCK/RAND already require).** Six items, each with the card that carries it until then:
  - (a) **Forward only.** SimEnv's advance and the harness's clock step move the clock forward, to the later of the clock and the earliest parked deadline. (A-4c0-T, A-8a)
  - (b) **Ties.** A clock step or advance wakes every parked timed waiter whose deadline it reaches; otherwise T1 states a deterministic tie-break (task or registration order, never thread timing). (A-4c0-T, A-8a)
  - (c) **`t_sim_timed_advance`.** It gains a variant in which a task sleeps past the timed waiter's deadline before every task parks, and the clock must not go back. Its Needs names A-4c0-T. (A-8a)
  - (d) **`t_timed_wait`'s harness form.** It names a second timed waiter with an earlier deadline, which is what makes "a step below D" exist. (A-4c0-T)
  - (e) **The OS-form watchdog.** It gets a stated floor of seconds beside `10 d`. (A-4c0-T)
  - (f) **`t_ckpt_pair_one_point`'s hook.** It also sits at step 0's exits from the append critical section. (A-7c)
- **S-XI-F2 (the plan card that applies card S-2a-1's blocks P-A to P-F, PL-19).** A-8a's Depends gains A-4c0-T (Q: `t_sim_timed_advance` runs on its timed wait), and A-8a's row names `t_sim_timed_advance`. A-7c's and B-1c's rows name `t_ckpt_pair_one_point` (this card's "For the plan card" item 3). A-4c0-T's row may carry S-XI-F1 (a), (b), (d) and (e) for its card.
- **S-XI-F3 (spec gap; due before any D-5 card or embedded binding that serves several sessions on one thread is written).** S-STORE §15 "Timed waits" gains N10's cooperative backend. When every task of the thread is parked and one holds a timed wait, the thread waits on the build's clock until the earliest deadline, or returns control to the host, instead of failing. That backend and its `Env` read one clock, so C-T0 v8 §6.3's timed terms stay on in browser and embedded builds. The test is like `t_sim_timed_advance`, on that backend.

## Notes
- **Collision with S-NU (the brief's question).** Yes, a renumbering re-cut of whichever card merges second, confined to the status line and its own Changelog entry, is all it takes. Neither card's other blocks touch the other's lines, and both orders apply with every Old exactly once. The re-cut includes:
  - **E1 and E2:** read 7.25, with E1's New ending "on draft 7.2" so that the result reads "on draft 7.24 (card …)".
  - **The Changelog entry's own references:** S-XI's "No rule of draft 7.23" and "as at drafts 7.18 to 7.23" become 7.24.
  - **S-NU's Counts sub-bullet, if S-NU is second.** "§15: 48 rows, 295 mutants, 55 tests. §15.1: 176 rows" would be false on S-XI's 7.24 and becomes 49, 300, 58 and 180.
  - **The second card's Acceptance lines that name the draft or hash the diff:** for S-XI, lines 1, 2 and 6, and its Base sha and Work step 1. Lines 3, 4, 5 and 7 hold unchanged, as I checked above.

  The re-cut is gated as S-ZETA's move was. Card S-2a-1's C-T0 text names no S-STORE draft number, so it is unaffected either way.
- NIT: T1's "No store or C-T0 v8 code meets a deadline by … re-testing in a loop" could be read as forbidding the wait loop itself. "re-testing in a loop without a wait" says what is meant.
- NIT: T1's platform bullet, "the OS's wake latency is its only lateness", holds where std measures the timeout on the monotonic clock, which is Linux, verified here. Where std computes it from the wall clock, a wall-clock step adds lateness. Only the never-early half is what `t_timed_wait` and C-T0 rely on.
- NIT: `t_timed_wait`'s OS form uses `StdEnv` (A-1a), which its Needs cell omits. Other rows that use StdEnv omit it the same way.
- NIT: `StdPrimitives`' timed wait waits on the platform clock through std's `Condvar`, a second such place beside `std_env.rs`. E-CLOCK/RAND's lint, when A-1a-B-F3 writes it, must allow `Condvar::wait_timeout` in `sync.rs`.
- NIT: A-4c0-T's card should extend the sync layer's F5 (a poisoned mutex is `Poisoned`, never a guard) to the timed wait, with a poisoned case. It should also say that shuttle's own `wait_timeout` is not used, because it never times out.
- NIT (A-8a, B-7's G0): in a world that expects no timeout, count an unexpected 55P03 or 40P01 as a failure, so that a lost wakeup on a waiter holding a timed wait cannot pass as a timeout.
- Scratch, probes and plants: `~/.cache/nv3-review/gate-S-XI/` (`notes.txt`, `myparse.py`, `runacc.py`, `plants.py`, `order.py`, `clockprobe/`).
