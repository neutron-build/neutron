Card: FLAKE-FIX-1
Class: test
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session 0e72e014-3ddd-40c9-b39a-e0fa9490f687 (T3 Code review sub-agent of T3 thread 45584af2-e651-490e-862c-3402320cffa3; P gate of record, third gate, after rework 2; it wrote neither the card, its amendments, the build, the move, either rework nor any GLM report; gates 1 and 2 are kept under History)
Date: 2026-11-26
Gate-exit: GATE EXIT=0
Commit: 81048c4564c074b185d1f8478b688bf2e1c52681
Mutants: written and run by this gate in the card worktree (python exact-string plants on whole lines, each anchor asserted unique; build untimed; `cargo test -q -p nucleus-env --test sim_env` and `--test sim` each under `timeout -k 5 120`; `git checkout --` and an empty `git status --porcelain --untracked-files=all` after each). Card: SE-5 JOIN-DROPPED -> sim_harness_wait_after_life_is_deadlock (sim_env.rs:1455, left Ok(()) right Err(Deadlock)); SE-4 GUARD-WIDE -> sim_harness_wait_before_start_is_deadlock (sim_env.rs:1407, left Ok(()) right Err(Deadlock)) and sim_harness_wait_after_life_is_deadlock (sim_env.rs:1455); SE-1 FALSE-DEADLOCK-BACK -> sim_start_after_finish_is_not_deadlock (sim_env.rs:1318); SE-3 DEADLOCK-MUTE -> sim_real_deadlock_reported (sim_env.rs:1367), sim_harness_wait_before_start_is_deadlock (sim_env.rs:1415), sim_harness_wait_after_life_is_deadlock (sim_env.rs:1462) and deadlock_detection (sim.rs:333). Own, aimed at the Join conjunct: OWN-ANYKEY (`== Some(&Some(WaitKey::Join))` -> `!= Some(&None)`, the harness parked on any key) -> sim_harness_wait_after_life_is_deadlock (sim_env.rs:1455, left Ok(()) right Err(Deadlock)); OWN-OR (the two conjuncts joined by `||`) -> sim_harness_wait_after_life_is_deadlock (FAILED), then caught by hang: sim_real_deadlock_reported, deadlock_detection and scheduler_determinism, both binaries killed at 120 s; OWN-WRONGKEY (`WaitKey::Join` -> `WaitKey::GateShared`) -> sim_start_after_finish_is_not_deadlock (sim_env.rs:1318). The rework's own logs, opened by this gate (`~/.cache/nv3-review/rework2-FLAKE-FIX-1/logs2/mutant-*.log`, plant diff, exit 101 and the failing row each): E1C-1 -> undo_order.rs:186 `scan saw [], want [(k, v0)]`; E1C-2 -> undo_order.rs:195 windows; E1C-3 -> undo_order.rs:187 `point read saw Some(..)`; E1C-4 -> undo_order.rs:186 `undo record 1 is missing from the log (AC-UNDO-FIRST broken)`; SE-2 -> sim_env.rs:37 deadlock=true. None UNCAUGHT.
Followups: none

## Summary

The second rework is R1′ to R4′ and nothing else, R5′ holds, the fresh gate is green on its first run, Acceptance 1 to 6 pass, and every mutant is caught, the card's nine and three of this gate's own aimed at the new `Join` conjunct. **The central question is closed.** This gate walked every state in which the guard now answers "completed" where the code before this card said DEADLOCK, by reading `handoff_locked`'s callers and by 14 probes run at the tip and under the pre-card condition. That set is exactly `cands = ∅ ∧ ¬crashed ∧ every workload task finished ∧ waiting[HARNESS] = Some(Join)`. In it, the only live thread is the harness, the fall-through sets `running = Some(HARNESS)` and signals it, and `WaitKey::Join`'s own condition ("The harness waiting for all workload tasks to end", `sched.rs:101`) holds. Nobody can wait for ever, so no real deadlock is masked on any path, direct `Sched` or `run_life`. The card's own prose is stale in five places. That is the orchestrator's text, not the work, so this gate writes it below (Amendment 5) for the orchestrator to apply before the merge, as R7′ was. No follow-up remains.

## Evidence (all run by this gate)

- **Fresh gate** (GATE-PROMPT step 1, extracted through the `--no-local` clone): scaffold `65573171e408f720a7cd2e7f4e52d50f8a3b4145`; `fresh-gate: commit 81048c4564c074b185d1f8478b688bf2e1c52681`, `fresh-gate: base 65573171…`, `fresh-gate: checks passed (4914 files hashed in 1.47 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`; STEP test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests and api-surface all EXIT=0; last line `GATE EXIT=0`, first run, launched at load 0.8.
- **Scope:** `git diff nucleus/v3-scaffold...HEAD --stat` gives the four Touch-only files (sched.rs +29, sim_env.rs +185, lib.rs +27, undo_order.rs +103) plus this verdict file (the gates' commits, which Limit 7 and Acceptance line 6 except). Merge-base `f24803a0`. `git merge-tree --write-tree` onto `65573171` is clean. `spikes/e4-recovery` is identical to `48ac9920`; `spikes/e1-arm-c` is unchanged since `b12de1ea`.
- **Acceptance as amended, all six run by this gate** (`CARGO_TARGET_DIR` = the gate's): 1: exit 0, 200/200, 19 s, load 0.7; 2: exit 0, 200/200, 12 s; 3: exit 0, 200/200, 28 s, load rising to 4.1; 4: exit 0, 200/200 with 6 tests each, 21 s, load 5.6; 5: exit 0, 138 passed; 6: exit 0.
- **The rework against R1′ to R5′ (my own read of `git diff 92b97a62..HEAD`):** R1′'s three lines; R2′ (a) line 1's tail; R2′ (b) the departure block; R2′ (c) the doc; R2′ (d) the comment; R3′'s two doc lines; R4′'s test, in the form given. Two files, nothing else. Every hunk is an R′ item's.
- **Probes** (a scratch test file in `tests/`, removed afterwards, tree clean). At the tip: (i) a normal-order life, then `wait_for(HARNESS, Tree(7))`: `Err(Deadlock)`, world failed. (i-r) the same after a race-order life (`finish(1)` then `start()`, which leaves a stale `waiting[HARNESS] = Some(Join)`): `Err(Deadlock)`. (w) the same after `wake(Join)`: `Err(Deadlock)`. (ii) before `start()`, task 1 alive on `Join`, harness on `Tree(1)`: `Err(Deadlock)`, task 1 `Err(Crashed)`. (ii-J) the same with the harness on `Join` itself: `Err(Deadlock)`, since a workload task is unfinished. (iii) `finish(1)` then `start()`: `Ok`. (iv) a zero-task `start()`: `Ok`; (iv-b) a zero-task `wait_for(HARNESS, Tree(7))`: `Err(Deadlock)`. (v) `start()`, then task 1 blocks on `Tree(7)` while the harness is parked on `Join`: task 1 `Err(Deadlock)`, `start` `Err(Crashed)`, world failed. (v-3) three tasks, task 1 finishes, task 2 blocks: reported the same way. (d) a normal life, then 10,000 calls of `wait_for(HARNESS, Join)`: 10,000 `Ok`, no deadlock; (d-r) the same after a race-order life: `Ok`. (y-r) a race-order life, then `yield_baton(HARNESS)`: `Ok`; (y-n) the same after a normal life: `Ok`. Under the pre-card condition (SE-1 planted), the race-order life fails at `start()` with the false DEADLOCK, as SIMENV-FLAKE-1 describes. (vi) `run_life`: `sim_halted_off_task` 200/200 alone and every `run_life` row green (Acceptance 2 and 5).

## Findings

**The masked set, by reading.** `handoff_locked`'s callers are `yield_baton_budget`, `park_locked` (from `start` and `wait_for`) and `finish`'s non-all-done branch. `finish` there has an unfinished workload task, so `life_complete` is false and nothing changes. A finished workload task never holds the baton, since a handoff never picks it and `finish` moves `running` off it, so it cannot reach `yield_baton_budget` or `wait_for`'s `park`. So every caller of the masked set is the harness, either parking on `Join` or yielding while a stale `Join` mark from a race-order `start()` is still set. In each case it holds the baton again at once, and Join's condition is true. The two pins and the three own mutants cover both directions of the conjunct: dropped (SE-5, OWN-ANYKEY), the wrong key (OWN-WRONGKEY) and `||` (OWN-OR).

**G3-1 (record; no action): the comment names one way into the state, and there are three.** The comment says it is "Reached by a `start` that parks after the last `finish`". A direct `wait_for(HARNESS, Join)` after the life also reaches it (probe d), and so does a `yield_baton(HARNESS)` after a race-order life, whose stale `Join` mark the fall-through does not clear (probe y-r). Before this card both answered DEADLOCK. Neither is a deadlock: the harness is runnable, and a Mesa-style re-check of Join's condition exits. The doc (R2′ (c)) states the rule by state ("then that `Join` is answered"), which covers all three, and the comment does not claim to list every way in. This differs from F1 and F6, whose comments asserted something false. Request (d)'s observation is right.

## Card text: ORCHESTRATOR AMENDMENT 5 (apply before the merge; no code)

This gate's rulings on the refresh's PA-1 to PA-6. Each adopted text is the refresh report's (`~/.cache/nv3-review/pregate-FLAKE-FIX-1-refresh2/report.md`, section (f)), byte for byte, unless a change is given.

- **PA-1 adopted as written.** In Work 3, replace `` "all tasks blocked" cannot be a deadlock when ~~the baton is already home with the harness and~~ every workload task has finished *(the struck words went with the first build's clause, per R1 and R2 (b) and (d))* `` with `` "all tasks blocked" cannot be a deadlock when ~~the baton is already home with the harness and~~ every workload task has finished and the harness is parked on `Join` *(Amendment 5: the second half is R1′'s conjunct, per the re-gate's F6; the struck words went with the first build's clause, per R1 and R2 (b) and (d))* ``.
- **PA-2 adopted as written.** In Work 3, the struck sentence's marker gains, before its closing `)*`: `` So is one that parks there after every workload task has finished, which the first rework's clause returned as `Ok` (the re-gate's F6); Work 4's fifth row pins that it is reported. ``. After that marker, add `` *(Amendment 5, per verdict `92b97a62`, F6)* ``.
- **PA-3 adopted as written** (this gate checked the numbers at `81048c45`: 677–678, 679, 680, 745–753 and 749–751). Append to the move note's (b): `` *(Amendment 5: those are the numbers at `cd6b45f3`; at `81048c45`, after the two reworks, the deadlock condition is line 679 (SE-1's and SE-4's site; SE-5's is lines 677–678), `core.deadlock = true;` is line 680 (SE-3), and `Sched::wake` is lines 745–753 with its loop body at 749–751 (SE-2).)* ``
- **PA-4 adopted as written.** In Read, replace `the new pin follows` with `that Work 4's fourth and fifth rows follow`.
- **PA-5 adopted as written.** In Goal (2), after `both interleavings now pass`, insert `` ; Work 4's fourth and fifth rows pin the other side on `Sched` directly: a harness parked on a key nobody can wake is reported, whether a workload task is still alive and blocked or every one has finished ``.
- **PA-6 rejected.** The QUEUE is local and its lines move, so new numbers would go stale too. The rows are named next to the numbers, and the merge closes them (the first refresh's H7).
- Add one dated note: `Amended 2026-11-26: ORCHESTRATOR AMENDMENT 5, per verdict FLAKE-FIX-1 (MERGE, third gate): PA-1 to PA-5 of the second pre-gate refresh, as that verdict rules; PA-6 rejected. Card text only; nothing built changes.`

Form: MERGE, with the text above applied by the orchestrator before the merge, as R7′ was. It changes no line of the work, so it is not a gap in what was built, and a follow-up card for prose would only queue a row.

## Rulings on the request's (a)–(f)

- (a) Yes. F1 (a live, blocked task, probe ii) and F6 (a non-`Join` park after the life, probes i, i-r and w) are both closed and pinned. The masked set is exactly "every workload task finished and the harness parked on `Join`", where nobody can wait for ever. No real deadlock is masked on any path, direct `Sched` or `run_life` (probes v and v-3, `sim_real_deadlock_reported`, `deadlock_detection`).
- (b) Amendment 4 is what R7′ and ruling (c) ordered. This gate checked Limit 3, Work 3, Goal (2), Work 4 rows 4 and 5, Limit 4, Acceptance line 4, SE-1, SE-4, SE-5, RESULT +5, and P2, P4 and P6 to P10 against the verdict. A GLM job typed it on the orchestrator's direction. The content is the ruling's, and who typed it does not change that.
- (c) PA-1 to PA-5 adopted as written; PA-6 rejected. Form: MERGE, with the text in this verdict.
- (d) Right, and benign: Join's condition holds, so the wait is satisfied, not masked (G3-1; probes d and d-r). A `yield_baton(HARNESS)` after a race-order life takes the same path (probe y-r).
- (e) True and complete. Comments stripped, the tip's code differs from E4's `sched.rs` at `48ac9920` in exactly two places: `await_first_baton` (S1) and the `life_complete` conjunct. Line 1 and the departure block name both, and they state the guard as R1′ is. E4's own files are untouched.
- (f) Nothing remains open from gates 1 and 2. F1, F6, R1 to R7 and R1′ to R7′ are closed. F2 to F5 and F7 were records needing no card. The Amendment 2 Note's "Not changed" sentence is superseded, as Amendment 4 says.

## History

The verdicts of record before this one, newest first. Each keeps its header with every line prefixed by "- ", so `vfield` reads only the header at the top of this file.

### Gate 2 (verdict commit 92b97a623204033622eda8a249d5f0a07dc96c99, on b12de1ea8bc584de96fe3f0ef368871f67436be0)

Gate 2's header, verbatim except that each line is prefixed with "- " (so `vfield` reads this verdict's header, not gate 2's):

- Card: FLAKE-FIX-1
- Class: test
- Card-gate: P
- Verdict: REWORK
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: claude-code session 5a4aa093-7903-4f90-a596-041e804ade1c (T3 Code review sub-agent of T3 thread 45584af2-e651-490e-862c-3402320cffa3; P re-gate of record after rework 1; it wrote neither the card, its amendments, the build, the move, the rework nor either GLM report; the first gate, session 843ab9f0, wrote R1 to R7, and this gate rules on R1's text below)
- Date: 2026-11-26
- Gate-exit: GATE EXIT=0
- Commit: b12de1ea8bc584de96fe3f0ef368871f67436be0
- Mutants: written and run by this gate in the card worktree (python exact-string plants, each anchor asserted unique; build untimed, tests under `timeout -k 5 120`; `git checkout --` and an empty `git status --porcelain --untracked-files=all` after each). SE-1 FALSE-DEADLOCK-BACK (R1's clause deleted) -> sim_start_after_finish_is_not_deadlock (sim_env.rs:1318); SE-3 DEADLOCK-MUTE -> sim_harness_wait_before_start_is_deadlock (sim_env.rs:1415, "the deadlock fails the world") and sim_real_deadlock_reported (sim_env.rs:1367), and deadlock_detection (sim.rs:333); SE-4 GUARD-WIDE -> sim_harness_wait_before_start_is_deadlock (sim_env.rs:1407, left Ok(()) right Err(Deadlock)); E1C-4 WINDOW-ONLY -> readers_never_see_past_their_snapshot_during_apply (undo_order.rs:186, `scan failed: engine: undo record 1 is missing from the log (AC-UNDO-FIRST broken)`). Own: OWN-1 SKIP2 (`skip(1)` -> `skip(2)` in R1's clause) -> sim_harness_wait_before_start_is_deadlock (assertion), then the binary hung and was killed at 120 s; OWN-2 HARNESS-OFF-JOIN-AFTER-LIFE (not a plant: the tip itself, probed; F6) -> UNCAUGHT by every test (sim_env 39, sim 26 pass); a harness `wait_for(HARNESS, Tree(7))` after a completed life returns Ok 100000 times in a row, deadlock=false. The rework's own logs, opened by this gate: E1C-1 -> undo_order.rs:186 `point read saw None`; E1C-2 -> undo_order.rs:195 windows; E1C-3 -> undo_order.rs:187 `point read saw Some(..)`; SE-2 -> sim_env.rs:37 deadlock=true.
- Followups: none

#### Summary

The rework applies R1 to R6 exactly, and every hunk of `git diff 94af6219..HEAD` is an R item's (R6's `cargo fmt` only reflows R4's two asserts, R4's import and R5's call). The fresh gate is green, Acceptance 1 to 6 as amended pass, and every mutant is caught. **F1 is not completely closed, and the fault is in this gate's own R1 text, not in the rework.** R1's clause is "every workload task has finished". The state it excuses is "every workload task has finished **and the harness is parked on `Join`**". When the life has completed and the harness then parks on any other key, the scheduler now returns `Ok` where the code before this card returned `Err(Deadlock)`. Only the harness is alive, it is blocked, and nothing can wake it: a real deadlock (S-CRASH, `sim/mod.rs:28-31`). Because `wait_for` counts no yield, a Mesa-style re-check loop then spins forever (S-LIVE: "A hang is never a pass, and never a hang"). This is the same API domain and the same shape of defect that F1 ruled blocking, and R2 (c) and (d)'s comments, now in the file, state the wide form as true. For consistency with the first verdict it is REWORK. The fix is one more conjunct and one pin, trial-applied green by this gate (below), so the rework stays mechanical: no spec or test changes, and nothing for ESCALATE.

#### Evidence (all run by this gate)

- **Fresh gate** (GATE-PROMPT step 1, extracted through the `--no-local` clone): scaffold `573e0f291eb277e1c16fc01248ef2eea9923cd81`; `fresh-gate: commit b12de1ea8bc584de96fe3f0ef368871f67436be0`, `fresh-gate: base 573e0f29…`, `fresh-gate: checks passed (4914 files hashed in 1.46 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`; STEP test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests and api-surface all EXIT=0; last line `GATE EXIT=0`, first run.
- **Scope:** `git diff nucleus/v3-scaffold...HEAD --stat` gives the four Touch-only files (sched.rs +24, sim_env.rs +144, lib.rs +27, undo_order.rs +103) plus `nucleus/v3/verdicts/FLAKE-FIX-1.md` (+162, the first gate's own commit). Merge-base `f24803a0`. `git merge-tree` onto `573e0f29` is clean. Nothing under `spikes/e4-recovery` differs from `48ac9920`.
- **Acceptance as amended, all six run by this gate** (load 0.4 at the start): 1: exit 0, 200/200, 19 s; 2: exit 0, 200/200, 11 s; 3: exit 0, 200/200, 27 s, load 4.1; 4: exit 0, 200/200 with 5 tests each, 22 s, load 5.5; 5: exit 0, 137 passed; 6: exit 0.
- **The central question (request item 4).** R4's pin passes at the tip, and SE-4 makes it fail. By reading `handoff_locked`, every state in which the tip says "completed" where the pre-card code said DEADLOCK is `cands = ∅ ∧ ¬crashed ∧ finished[1..] all true`, so a world whose workload tasks have all finished, as required. **That set is still too wide.** It includes `waiting[HARNESS] = Some(k)` with `k ≠ Join`: the harness reaches it through `wait_for(HARNESS, k)` after `finish`'s all-done branch handed it the baton.
- **Probe Q1** (a scratch test file, removed afterwards): `SchedCore::new(2, 991)`; task 1 `await_first_baton(1)` then `finish(1)`; harness `start()` returns `Ok`; then harness `wait_for(HARNESS, Tree(7))` in a loop. **Tip: `Ok(())` 100,000 times in a row, `deadlock=false crashed=false`.** With R1's clause deleted (the pre-card condition): `Err(Deadlock)` on the first call. With the R1′ trial: `Err(Deadlock)` on the first call. **Probe Q2:** a zero-task `start()` and finish-then-`start()` both return `Ok` at the tip and under R1′, so R1′ keeps what the card fixed.
- **R1′ trial-applied by this gate and reverted:** with R1′ plus R4′'s pin, sim_env 40 passed, sim 26 passed, the whole `-p nucleus-env` passed, `cargo clippy -p nucleus-env --all-targets -- -D warnings` exit 0, and `cargo fmt --check` was clean for sched.rs (for R4′ it is clean in the form given below). R4′'s pin at the tip FAILS (sim_env.rs:1455, left `Ok(())` right `Err(Deadlock)`).
- **The rework against R1 to R6 (my own read of `git diff 94af6219..HEAD`):** R1's line; R2 (a) line 1; R2 (b) the departure block; R2 (c) the doc; R2 (d) the comment; R3's two doc replacements; R4's import and test (fmt-reflowed); R5's import, field, init, arm body with the doc sentence appended to the doc's last line, and call site with its comment. Nothing else. The refresh's report on this matches.

#### Findings

**F6 (blocking; caused by this gate's own R1): the guard still masks a real deadlock on the direct `Sched` API.** Since S1, every `handoff_locked` call with `cands = ∅` and all workload tasks finished comes from `park_locked` with `me = HARNESS`. Through `start()`, the key is `Join`, and the world really is a completed life (the race, H1, and a zero-task life). Through `wait_for(HARNESS, k ≠ Join)` it is a harness blocked on a key nobody alive can wake, which the pre-card code reported. Probe Q1 shows the tip answers it with `Ok` forever. The comment at `sched.rs:668` ("every workload task finished is a completed life, not a deadlock") and the doc at `sched.rs:648-651` state the wide form as true, the same shape as F1's "unreachable by construction". No in-tree caller reaches it (no `wait_for(HARNESS, …)` outside R4's pin; `SimEnv::wait_for` refuses off-task threads, `env.rs:220-228`). But F1 was ruled blocking on exactly that domain. Fixed by R1′ to R4′.

**F7 (record): H1 confirmed.** `~/.cache/nv3-review/rework-FLAKE-FIX-1/logs/20-clippy.log` holds only the host check, the command, `exit=0 wall=2 s`, `clippy_exit=0` and an empty status. The rework report's description of clippy's output ("`Compiling` proc-macro2 … `Checking e1-arm-c`, `Finished`") is in no log. The exit code stands: this gate's fresh gate ran clippy on the tip (STEP clippy EXIT=0), and this gate's own `cargo clippy -p nucleus-env` was clean. A reporting fault, not a code fault, and no card is needed. The next rework should log what it describes.

**Other rulings:** H3 (the redundant `any_alive`): leave it. It is E4's own text, and R1′ keeps the departure to one added conjunct. H4 (the 136-column comment): fmt accepts it; no action. H8 (two wording nits): nothing. `sched.rs:17` is the scheduler's own term, and `undo_order.rs:216`'s range is the card's Work 2 text verbatim; neither weakens a check.

#### REWORK: exact text (mechanical; the orchestrator amends the card first, R7′)

All paths are under `nucleus/v3/`. Every R item applies to the tip `b12de1ea`.

**R1′** `crates/nucleus-env/src/sim/sched.rs`, the deadlock condition. Replace the line
`                if any_alive && !core.crashed && !core.finished.iter().skip(1).all(|f| *f) {`
with the three lines
```
                let life_complete = core.finished.iter().skip(1).all(|f| *f)
                    && core.waiting.get(HARNESS) == Some(&Some(WaitKey::Join));
                if any_alive && !core.crashed && !life_complete {
```

**R2′** `sched.rs` comments.
- (a) Line 1: replace its tail `` `handoff_locked` reports no deadlock once every workload task has finished.`` with `` `handoff_locked` reports no deadlock once every workload task has finished and the harness is parked on `Join`.``
- (b) Replace the whole departure block (lines 5–9) with:
```
// Departure from that copy (card FLAKE-FIX-1, 2026-11-21): `handoff_locked`'s deadlock condition
// gains `&& !life_complete`, "every workload task has finished and the harness is parked on
// `Join`": that world is a completed life, not a deadlock (SIMENV-FLAKE-1: the start/finish race
// of the pre-A-6b-0 `run_life`, which A-6b-0's `await_first_baton` has since closed from the life
// side). E4's own sched.rs copy keeps the latent race unchanged (frozen evidence record).
```
- (c) In the `handoff_locked` doc, replace everything from `/// still alive — a per-seed failure — **unless every workload task` through `/// \`sim_harness_wait_before_start_is_deadlock\`).` with:
```
    /// still alive — a per-seed failure — **unless every workload task
    /// has finished and the harness is parked on `Join`** (FLAKE-FIX-1):
    /// then that `Join` is answered (`finish`'s all-done branch handed the
    /// harness the baton), so a `start` that parks after the last `finish`
    /// falls through to the everyone-finished handoff below, a no-op. A
    /// harness that parks on any other key is a deadlock and is reported,
    /// whether a workload task is still alive and blocked (pin
    /// `sim_harness_wait_before_start_is_deadlock`) or every one has
    /// finished (pin `sim_harness_wait_after_life_is_deadlock`).
```
- (d) Replace the six-line `// FLAKE-FIX-1: every workload task finished is a completed …` comment above the condition with:
```
                // FLAKE-FIX-1: every workload task finished, with the
                // harness parked on `Join`, is a completed life, not a
                // deadlock (`finished[HARNESS]` is never set, so
                // `any_alive` alone cannot tell). Reached by a `start` that
                // parks after the last `finish`: direct `Sched` use and a
                // zero-task life. Since A-6b-0's `await_first_baton`,
                // `run_life` cannot produce that order. A harness parked on
                // any other key has nobody left to wake it: a deadlock.
```

**R3′** `tests/sim_env.rs`, the doc of `sim_harness_wait_before_start_is_deadlock`: replace its first two lines `/// **FLAKE-FIX-1 pin 4 (gate R4): the guard is exactly "every workload` / `/// task finished".** The harness holds the baton before \`start()\` and` with `/// **FLAKE-FIX-1 pin 4 (gate R4): a live, blocked workload task is` / `/// never a completed life.** The harness holds the baton before \`start()\` and`. The rest of that test stays byte-unchanged.

**R4′** `tests/sim_env.rs`: append a new top-level test (already in fmt's form):
```rust
/// **FLAKE-FIX-1 pin 5 (re-gate R4′): the guard needs the harness on
/// `Join`.** The life completes (task 1 runs and finishes, `start()`
/// returns), then the harness parks on a key nobody can wake. Only the
/// harness is alive and it is blocked: a real deadlock. It must be
/// reported on the first call, never answered with a quiet `Ok` that a
/// Mesa-style re-check loop would spin on forever (`wait_for` counts no
/// yields, so S-LIVE's budget never trips).
#[test]
fn sim_harness_wait_after_life_is_deadlock() {
    struct Host {
        sched: SchedCore,
    }
    impl SchedHost for Host {
        fn sched_mut(&mut self) -> &mut SchedCore {
            &mut self.sched
        }
    }
    let sched = Arc::new(Sched::new(Host {
        sched: SchedCore::new(2, 991),
    }));
    let task = Arc::clone(&sched);
    let t = std::thread::spawn(move || {
        task.await_first_baton(1)?;
        task.finish(1);
        Ok::<(), SimFail>(())
    });
    assert_eq!(sched.start(), Ok(()), "the life completes");
    assert_eq!(t.join().unwrap(), Ok(()), "task 1 ran and finished");
    assert_eq!(
        sched.wait_for(HARNESS, WaitKey::Tree(7)),
        Err(SimFail::Deadlock),
        "a harness parked on a key nobody can wake after the life is a deadlock"
    );
    let mut h = sched.lock();
    let core = h.sched_mut();
    assert!(
        core.deadlock && core.crashed,
        "the deadlock fails the world"
    );
}
```

**R5′** Nothing in `spikes/e1-arm-c` changes. R5 stands.

**R6′** Then, in order:
- `cargo fmt --all`: expected to be a no-op; report any reflow.
- `cargo clippy -p e1-arm-c -p nucleus-env --all-targets -- -D warnings`, with its full output in the log.
- Run every mutant of the amended card, each log holding the failing row and its message.
- RESULT gives nucleus-env +5, e1-arm-c +0.

**R7′** Card amendments (the orchestrator's, before the rework starts):
- Limit 3 and Work 3: the clause becomes R1′'s (`let life_complete = …` and `&& !life_complete`). Work 3's comments and provenance lines take R2′ (a)–(d)'s exact text.
- Goal (2), Amendment 4 P1 changed: "**a world whose workload tasks have all finished, with the harness parked on `Join`, is a completed life, not a deadlock**".
- Work 4 gains R4′'s row. Limit 4 counts five rows. Work 4 row 4 takes R3′'s doc change.
- Acceptance line 4 gains `sim_harness_wait_after_life_is_deadlock`.
- Mutant SE-1: delete `&& !life_complete`; it must FAIL at `sim_start_after_finish_is_not_deadlock`.
- Mutant SE-4: `!life_complete` replaced by `core.running != Some(HARNESS)`; `sim_harness_wait_before_start_is_deadlock` must FAIL.
- New mutant `SE-5 JOIN-DROPPED`: delete the line `&& core.waiting.get(HARNESS) == Some(&Some(WaitKey::Join));` and end the line before it with `;`; `sim_harness_wait_after_life_is_deadlock` must FAIL.
- RESULT: nucleus-env +5.
- Amendment 4, as ruled below (P2–P10).

#### Rulings on the request's (a)–(h)

- (a) No. R1 closes P1 (a live, blocked workload task), but a harness parked on a non-`Join` key after a completed life is still masked (F6, probe Q1). R1′ closes it.
- (b) Amendments 2 and 3 are sound. Amendment 3 is the house form (A-1a, A-2-0) and the smallest change. Amendment 2's texts are the verdict's.
- (c) Amendment 4: P1 changed (R7′'s text). P2 adopted with one change: "(the deadlock-declaring condition at sched.rs:641 at the Base)", with no tip number. P3 and P5 rejected as numbers, which R1′ moves again; folded into P10. P4 changed: "after the rework, +5 and 40". P6 adopted without its line range: "…so the file carries the same tokens, those asserts broken across lines". P7, P8 and P9 adopted as written. P10 adopted and widened: one dated sentence saying every line number in the card is the Base's (`c81e84a1`) unless marked otherwise, covering P3 and P5. Form: none of MERGE or MERGE-WITH-FOLLOWUPS applies. The text goes in with R7′, before the rework.
- (d) All four are sound and needed: the struck words went with the first build's clause; the `use` line is R5's; the R2 pointer and Work 4 row 4's import and R3 are R-items. The note's count is fixed by P9.
- (e) F7: the log holds the exit code only. The description is unsupported, the exit 0 is confirmed, and no card is needed.
- (f) H3: leave `any_alive`, E4's own text. R1′ adds one conjunct and changes nothing else.
- (g) H8: nothing.
- (h) Line 1 names both departures (S1 and the guard) and, after R2′ (a), names the guard truly. Against E4's copy at `48ac9920`, the tip's only code departures are `await_first_baton` and the condition; every other difference is a comment. E4's own files are untouched.

### Gate 1 (verdict commit 94af62195a01a62395c057c07da4e8d1c2a2eddd, on cd6b45f3372ade228f1944a5d4ef4148b25b4307)

Gate 1's header, verbatim except that each line is prefixed with "- " (so `vfield` reads this verdict's header, not gate 1's):

- Card: FLAKE-FIX-1
- Class: test
- Card-gate: P
- Verdict: REWORK
- Gate-tier: P
- Gate-model: claude-opus-5-5
- Gate-session: claude-code session 843ab9f0-3f76-4ec5-b4de-76633abe8a79 (T3 Code review sub-agent of T3 thread 45584af2-e651-490e-862c-3402320cffa3; P gate of record; it wrote neither the card, its amendment, the build, the move nor either GLM review)
- Date: 2026-11-26
- Gate-exit: GATE EXIT=0
- Commit: cd6b45f3372ade228f1944a5d4ef4148b25b4307
- Mutants: written and run by this gate in the card worktree (python exact-string plants, each anchor asserted unique; build untimed, tests under `timeout -k 5 120`; `git checkout -- .` and an empty `git status --porcelain --untracked-files=all` after each). Card: E1C-1 WRITE-BEFORE-UNDO -> readers_never_see_past_their_snapshot_during_apply (undo_order.rs:186, `scan saw [], want [(k, v0)]` after 18 samples; NOT the message the card predicts, see F2); E1C-2 GATE-REMOVED -> same (undo_order.rs:195, windows 0 vs 80); E1C-3 SNAPSHOT-DROPPED -> same (undo_order.rs:187, `point read saw Some(..300000..)`); SE-1 FALSE-DEADLOCK-BACK -> sim_start_after_finish_is_not_deadlock (sim_env.rs:1318); SE-2 WAKE-NOOP -> sim_wait_wake_completes (sim_env.rs:37, deadlock=true); SE-3 DEADLOCK-MUTE -> deadlock_detection (sim.rs:333) and sim_real_deadlock_reported (sim_env.rs:1367, run with --no-fail-fast). Pre-gate's uncaught: OWN-SE-B guard narrowed to `!core.finished.iter().skip(1).all(|f| *f)` -> UNCAUGHT (sim 26, sim_env 38 passed; equivalent inside both suites, F1); OWN-E1C-D gate moved after the tree write -> UNCAUGHT (1 passed). Own: GATE-G1-RUNNING-SOME (adds `&& core.running.is_some()`: deadlocks entered from `finish` muted) -> caught by hang (scheduler_determinism, killed at 120 s); GATE-G2-HARNESS-ON-JOIN (adds `&& waiting[HARNESS] != Some(Join)`: every in-life deadlock muted) -> caught by hang (deadlock_detection, scheduler_determinism); GATE-G3-ANY-FINISHED (`!any workload finished` in place of the clause) -> caught by hang (scheduler_determinism); GATE-EA-WINDOW-ONLY (tree value published before its undo record, pointing at the id the append returns; sleep and gate between) -> readers_never_see_past_their_snapshot_during_apply (undo_order.rs:186, `scan failed: engine: undo record 1 is missing from the log (AC-UNDO-FIRST broken)` after 24 samples).
- Followups: none

#### Summary

The diagnosis is right, the e1-arm-c handshake is right, and the scope, the move and the provenance are clean. The guard is not: `&& core.running != Some(HARNESS)` is wider than the invariant the card's own Goal states ("whose baton already sits with the harness **and whose workload tasks have all finished**"), its comment claims a masked deadlock is "unreachable by construction", which is false, and at this tip the guard's only remaining domain is exactly where it masks one. Since A-6b-0's `await_first_baton`, `run_life` can no longer reach the guard's state (measured below), so the guard now serves only direct `Sched` users and zero-task lives, and in that domain it turns a real deadlock (probe P1) into `Ok(())` with a task left blocked forever. That contradicts S-CRASH as the module states it (`sim/mod.rs:28-31`: "`DEADLOCK` (all tasks blocked) ... never passes"). The fix is one clause, already tested green by two parties, so this is REWORK with exact text, not ESCALATE: no spec or test needs to change. The rework also takes the hook off the unarmed commit path (R5) and brings the comments up to date (R2, R3), so no follow-up remains.

#### Evidence (all run by this gate)

- **Fresh gate** (GATE-PROMPT step 1, extract through the `--no-local` clone): scaffold `cce145f5b8e6df98d7f6daa16f5c753268f544bf`; `fresh-gate: commit cd6b45f3372ade228f1944a5d4ef4148b25b4307`, `fresh-gate: base cce145f5…`, `fresh-gate: checks passed (4913 files hashed in 1.48 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`; last line `GATE EXIT=0`, first run, with no failing row.
- **Scope:** `git diff nucleus/v3-scaffold...HEAD --stat` gives exactly the four Touch-only files (sched.rs +28/−3, sim_env.rs +103/−1, lib.rs +36/−0, undo_order.rs +81/−22); merge-base `f24803a0`. Nothing under `spikes/e4-recovery` changed since `48ac9920`.
- **The move:** `ed03e051` has parents `3bdba02f` (the original build) and `f24803a0` (the scaffold with A-6b-0). Against E4's `sched.rs` at `48ac9920` the tip's only code departures are `await_first_baton` (S1) and the guard; every other changed line is a comment. `sim_env.rs` keeps A-6b-0's tests and adds the card's three. The two cards' behaviour does not interact: `await_first_baton` is one `park` call and never reaches `handoff_locked`, and the life-ended flag lives on `SimEnv`/`SimFile`, which `handoff_locked` never reads.
- **Acceptance as amended, all six run by this gate:** 1: exit 0, 200/200, 19 s, load 1.7; 2: exit 0, 200/200, 12 s; 3: exit 0, 200/200 with 8 busy loops, 29 s, load rising to 4.6; 4: exit 0, 200/200 × 4 tests, 22 s, load 5.9; 5: exit 0, 136 passed (e1-arm-c 9, nucleus-env 127); 6: exit 0. Verbatim pre-amendment line 1: `error: unexpected argument '--exact' found`, exit 1.
- **SIMENV-FLAKE-1, reproduced:** `sim_halted_off_task`'s exact body, 50,000 lives in one process with 16 busy loops (load about 18): **base `c81e84a1`: 5 false DEADLOCKs** (`deadlock=5 crashed=5`); **tip: 0**; **tip with the guard removed (SE-1): 0**. So at this tip the race is closed through `run_life` by A-6b-0 alone. Whole-binary loops (`--exact sim_halted_off_task`, 10,000 runs each, 32 busy loops; and 3,000 runs pinned to one core with 12 busy loops) saw 0 failures at the base: the race is too rare there to show up, and a single core starves the task, not the harness.
- **Probes at the tip** (a scratch test file, removed afterwards): P1, a harness `wait_for(HARNESS, Tree(1))` before `start()` with task 1 alive and parked: **`Ok(())`, `deadlock=false crashed=false running=Some(0)`, task 1 still blocked after 3 s**, released only by `deliver_crash`. With the narrowed clause: `Err(Deadlock)`, world failed, task 1 `Err(Crashed)`. P3 (`finish(1)`, then `start()`): `Ok` under both forms. P6 (`run_life(0)`): no deadlock under both forms. P7 (3 non-parking tasks drawing `rand()`, 2,000 lives): **1** draw sequence and 1 trace under both forms.
- **The rework text was trial-applied by this gate and reverted:** the narrowed clause plus R4's pin: sim_env 39 passed, sim 26 passed. With the wide clause and the pin, `sim_harness_wait_before_start_is_deadlock` FAILS (sim_env.rs:1402). R5's `OnceLock` hook: e1-arm-c's whole suite passes, and `cargo clippy -p e1-arm-c --all-targets -- -D warnings` is clean.

#### Findings

**F1 (blocking): the guard masks a real deadlock on its only live path, under a false comment.** Every state the new guard calls "completed" where the old code said DEADLOCK is `cands = ∅ ∧ any_alive ∧ ¬crashed ∧ running = Some(HARNESS)`. Through `yield_baton*` the caller is its own candidate, and through `finish` `running` is `None`, so neither can reach that state. Only `park_locked` can. Its callers:
- `start()` clears every workload wait first, so there `cands = ∅` means every workload task finished: H1, the race, a completed life. Correct.
- `wait_for(me ≠ HARNESS)` runs with `running = Some(me)`: unchanged.
- `wait_for(HARNESS, k)` runs with `running = Some(HARNESS)`: before `start()`, or after a `wake(Join)` handed the harness the baton mid-life. With any live workload task blocked, that is a genuine deadlock, now returned as `Ok`. That is P1.

Through `run_life` no in-tree caller reaches it: the harness calls only `start()` and then joins, and since S1 even H1 is unreachable there. But the card's own Notes keep the guard "for direct `Sched` users and empty lives", which is exactly the API surface where the mask lives. Fixed by R1, pinned by R4.

**F2 (record): mutant E1C-1 is not the evidence the card says it is.** It moves the tree write ahead of the undo push carrying `undo_ptr = 0`, which reads as "absent at the snapshot" (`lib.rs:347-348`), so the break is visible at all times and needs no in-window sample. It was caught here by `scan saw []`, not by the predicted "undo record … missing" message. That message belongs to a window-only break, and GATE-EA-WINDOW-ONLY (this gate's own) and the pre-gate's OWN-E1C-EA (200/200 quiet, 50/50 starved; its logs opened and checked) produce it. Fixed by R6's row texts.

**F3 (pre-existing; closed at this tip, no card needed).** With a non-parking task in a multi-task life, the base and the original build gave 5 draw sequences per seed, against a control of 1 (the pre-gate's `race-*-quiet4-yieldfirst.log` and `race-*-loaded2-yieldfirst.log`, opened). That broke S-DET; the spec itself was right. At this tip P7 gives 1: A-6b-0's S1, whose `sim_no_body_before_first_baton` and `sim_first_baton` are the regression pins, closed it. The same goes for the timing-dependent trace (the pre-gate's F2).

**F4: the hook.** In the spike's non-test code it adds one field, one `#[doc(hidden)] pub` arming method and one call site per applied key. Any dependent of the spike can reach it (`e1-bench` depends on `e1-arm-c`); only `undo_order` arms it. The card's Limits 1 and 5 allow it, and the plan does not forbid it: no spec or verdict cites `e1-arm-c/src/lib.rs` by line (grep), so the +36-line shift breaks no citation. But the unarmed path takes a mutex and clones an `Arc` on every applied key, about +3% contended on arm (c) (the pre-gate's measurement, not re-run here). That is the shape X-1a-F1 Work 3 removed from the sibling hook, and on an evidence record (PLAN §7: "E1′ is decided … the spike is now an evidence record"). A `OnceLock` makes the unarmed cost one atomic load, like `apply_yield`. Fixed by R5.

**F5: stale text after the move.** The comments state the `run_life` race in the present tense, and the Notes "Overlap with A-6b-0 (the card must land first)" and "Merge discipline" describe an order that did not happen. A-6b-0's whole-line grep of line 1 (`cards/A-6b-0.local.md:155`) no longer matches. That card is merged, its Acceptance is historical, and no script or test greps line 1 (grep), so no action is needed: the A-6b-0 ruling (QUEUE row A-6b-0; item 12 of its first verdict) supersedes it.

**Provenance:** line 1 names both departures and is true. After R1 its wording must follow the new clause (R2). E4's own files are untouched.

#### REWORK: exact text (mechanical; the orchestrator amends the card first, see R7)

All paths are under `nucleus/v3/`.

**R1** `crates/nucleus-env/src/sim/sched.rs`, the deadlock condition. Replace
`if any_alive && !core.crashed && core.running != Some(HARNESS) {`
with
`if any_alive && !core.crashed && !core.finished.iter().skip(1).all(|f| *f) {`

**R2** `sched.rs` comments.
- (a) Line 1: replace its tail `; and the departure below (card FLAKE-FIX-1, 2026-11-21): \`handoff_locked\`'s deadlock condition gains \`&& core.running != Some(HARNESS)\`.` with `; and the departure below (card FLAKE-FIX-1, 2026-11-21): \`handoff_locked\` reports no deadlock once every workload task has finished.`
- (b) Lines 5–9: replace the whole block with:
```
// Departure from that copy (card FLAKE-FIX-1, 2026-11-21): `handoff_locked`'s deadlock condition
// gains `&& !core.finished.iter().skip(1).all(|f| *f)`: a world whose workload tasks have all
// finished is a completed life, not a deadlock (SIMENV-FLAKE-1: the start/finish race of the
// pre-A-6b-0 `run_life`, which A-6b-0's `await_first_baton` has since closed from the life side).
// E4's own sched.rs copy keeps the latent race unchanged (frozen evidence record).
```
- (c) In the `handoff_locked` doc, replace everything from `/// still alive — a per-seed failure — **unless the baton is already` through `/// suppressed.` with:
```
    /// still alive — a per-seed failure — **unless every workload task
    /// has finished** (FLAKE-FIX-1): then nothing is left to run and the
    /// harness's `Join` is answered (`finish`'s all-done branch handed it
    /// the baton), so a `start` that parks after the last `finish` falls
    /// through to the everyone-finished handoff below, a no-op. A harness
    /// that parks on any other key while a workload task is alive and
    /// blocked is a deadlock and is reported (pin
    /// `sim_harness_wait_before_start_is_deadlock`).
```
- (d) Replace the nine-line `// FLAKE-FIX-1: the baton already sitting home …` comment above the condition with:
```
                // FLAKE-FIX-1: every workload task finished is a completed
                // life, not a deadlock (`finished[HARNESS]` is never set, so
                // `any_alive` alone cannot tell). Reached by a `start` that
                // parks after the last `finish`: direct `Sched` use and a
                // zero-task life. Since A-6b-0's `await_first_baton`,
                // `run_life` cannot produce that order.
```

**R3** `tests/sim_env.rs`, the doc of `sim_start_after_finish_is_not_deadlock`.
- Replace `/// the racy interleaving itself — a workload task's wrapper runs the` / `/// body and \`Sched::finish\` before the harness thread (descheduled` / `/// between \`scope.spawn\` and \`sched.start()\` in \`run_life\`) has parked.` with `/// the interleaving of the pre-A-6b-0 \`run_life\` race, driven on` / `/// \`Sched\` directly: a workload task's \`finish\` runs before the harness` / `/// parks in \`start()\`.`
- Replace `baton-home-all-finished world` with `all-finished world`.

**R4** `tests/sim_env.rs`.
- Add `HARNESS, SimFail` to the `nucleus_env::sim` import.
- Append a new top-level test:
```rust
/// **FLAKE-FIX-1 pin 4 (gate R4): the guard is exactly "every workload
/// task finished".** The harness holds the baton before `start()` and
/// parks on a key nobody can wake while task 1 is alive and parked on
/// `Join`: nothing can ever run again, a real deadlock. It must be
/// reported and task 1 must unwind, never a quiet `Ok`.
#[test]
fn sim_harness_wait_before_start_is_deadlock() {
    struct Host {
        sched: SchedCore,
    }
    impl SchedHost for Host {
        fn sched_mut(&mut self) -> &mut SchedCore {
            &mut self.sched
        }
    }
    let sched = Arc::new(Sched::new(Host {
        sched: SchedCore::new(2, 991),
    }));
    let task = Arc::clone(&sched);
    let t = std::thread::spawn(move || task.await_first_baton(1));
    assert_eq!(
        sched.wait_for(HARNESS, WaitKey::Tree(1)),
        Err(SimFail::Deadlock),
        "a harness parked before start() while a live task is blocked is a deadlock"
    );
    {
        let mut h = sched.lock();
        let core = h.sched_mut();
        assert!(core.deadlock && core.crashed, "the deadlock fails the world");
    }
    assert_eq!(t.join().unwrap(), Err(SimFail::Crashed), "the blocked task unwinds");
}
```

**R5** `spikes/e1-arm-c/src/lib.rs`, the hook takes no lock.
- The field becomes `apply_window_gate: OnceLock<Arc<dyn Fn() + Send + Sync>>,` and its init `apply_window_gate: OnceLock::new(),`.
- `use std::sync::{Arc, Mutex};` becomes `use std::sync::{Arc, Mutex, OnceLock};`.
- The body of `arm_apply_window_gate` becomes `let _ = self.apply_window_gate.set(gate);`, and its doc gains the sentence `Arming is once per engine; a second call is ignored.`
- The call site becomes `if let Some(gate) = self.apply_window_gate.get() { gate(); }`, with its comment replaced by `// FLAKE-FIX-1 test hook: the apply-window gate (unarmed: one atomic load). It may block for the whole window by design.`

**R6** Then:
- `cargo fmt`.
- `cargo clippy -p e1-arm-c -p nucleus-env --all-targets -- -D warnings`.
- Run every mutant of the amended card. RESULT gives nucleus-env +4, e1-arm-c +0.

**R7** Card amendments (the orchestrator's, before the rework starts):
- Limit 3's and Work 3's clause become R1's.
- Work 3's "unreachable by construction … suppressed" sentence is struck.
- Limit 1 and Work 1 name `OnceLock` per R5.
- Work 4 gains R4's row.
- Limit 4 counts four new rows.
- Acceptance line 4 gains `sim_harness_wait_before_start_is_deadlock`.
- SE-1's text deletes R1's clause.
- New mutant `SE-4 GUARD-WIDE`: R1's clause replaced by `core.running != Some(HARNESS)`; `sim_harness_wait_before_start_is_deadlock` must FAIL.
- E1C-1's text: "must FAIL: the moved write carries undo pointer 0, which reads as absent, so the break is visible at all times (`point read saw None` or `scan saw []`); it shows the property assertion is load-bearing, not per-window coverage."
- New mutant `E1C-4 WINDOW-ONLY`: "for a key with a prior value, insert the tree value first with `undo_ptr = core.undo.record_count() + 1`, then the sleep and the gate, then the undo append; `undo_order` must FAIL with `undo record … is missing from the log (AC-UNDO-FIRST broken)` (point read or scan)."
- The Notes "Overlap with A-6b-0" and "Merge discipline" are marked superseded by the 2026-11-25 move note.

#### Rulings (a)–(h)

- (a) Amendment 1 is sound: it is the minimal fix of a cargo usage error, and all four lines pass as amended.
- (b) The diagnosis is right (base: 5 in 50,000, mechanism read and forced by P3). At `start()` the guard flips only H1. It also flips P1 (`wait_for(HARNESS)`), a real deadlock. Through `run_life` it hides nothing, and since S1 it is unreachable there.
- (c) REWORK: R1 plus the R4 pin. No in-tree caller reaches it, but the guard's only remaining domain is that API.
- (d) E1C-1's row text is rewritten per R7, and E1C-4 is added as the per-window evidence.
- (e) Confirmed at base and the original build. It was a code defect against S-DET, not a spec defect, and A-6b-0's S1 closed it (P7: 1 sequence). No card.
- (f) The hook is allowed by the card and not barred by the plan (no line citations). It is reachable by any dependent and costs about 3% contended unarmed, so R5 makes it lock-free.
- (g) Stale comments: R2 and R3. Notes: R7. A-6b-0's line-1 grep: no action (merged, historical, superseded by its ruling).
- (h) Line 1 is true and complete for the current code and must track R1 (R2a). The E4 prototype is untouched.
