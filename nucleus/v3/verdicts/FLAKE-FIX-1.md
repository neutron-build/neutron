Card: FLAKE-FIX-1
Class: test
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session 843ab9f0-3f76-4ec5-b4de-76633abe8a79 (T3 Code review sub-agent of T3 thread 45584af2-e651-490e-862c-3402320cffa3; P gate of record; it wrote neither the card, its amendment, the build, the move nor either GLM review)
Date: 2026-11-26
Gate-exit: GATE EXIT=0
Commit: cd6b45f3372ade228f1944a5d4ef4148b25b4307
Mutants: written and run by this gate in the card worktree (python exact-string plants, each anchor asserted unique; build untimed, tests under `timeout -k 5 120`; `git checkout -- .` and an empty `git status --porcelain --untracked-files=all` after each). Card: E1C-1 WRITE-BEFORE-UNDO -> readers_never_see_past_their_snapshot_during_apply (undo_order.rs:186, `scan saw [], want [(k, v0)]` after 18 samples; NOT the message the card predicts, see F2); E1C-2 GATE-REMOVED -> same (undo_order.rs:195, windows 0 vs 80); E1C-3 SNAPSHOT-DROPPED -> same (undo_order.rs:187, `point read saw Some(..300000..)`); SE-1 FALSE-DEADLOCK-BACK -> sim_start_after_finish_is_not_deadlock (sim_env.rs:1318); SE-2 WAKE-NOOP -> sim_wait_wake_completes (sim_env.rs:37, deadlock=true); SE-3 DEADLOCK-MUTE -> deadlock_detection (sim.rs:333) and sim_real_deadlock_reported (sim_env.rs:1367, run with --no-fail-fast). Pre-gate's uncaught: OWN-SE-B guard narrowed to `!core.finished.iter().skip(1).all(|f| *f)` -> UNCAUGHT (sim 26, sim_env 38 passed; equivalent inside both suites, F1); OWN-E1C-D gate moved after the tree write -> UNCAUGHT (1 passed). Own: GATE-G1-RUNNING-SOME (adds `&& core.running.is_some()`: deadlocks entered from `finish` muted) -> caught by hang (scheduler_determinism, killed at 120 s); GATE-G2-HARNESS-ON-JOIN (adds `&& waiting[HARNESS] != Some(Join)`: every in-life deadlock muted) -> caught by hang (deadlock_detection, scheduler_determinism); GATE-G3-ANY-FINISHED (`!any workload finished` in place of the clause) -> caught by hang (scheduler_determinism); GATE-EA-WINDOW-ONLY (tree value published before its undo record, pointing at the id the append returns; sleep and gate between) -> readers_never_see_past_their_snapshot_during_apply (undo_order.rs:186, `scan failed: engine: undo record 1 is missing from the log (AC-UNDO-FIRST broken)` after 24 samples).
Followups: none

## Summary

The diagnosis is right, the e1-arm-c handshake is right, and the scope, the move and the provenance are clean. The guard is not: `&& core.running != Some(HARNESS)` is wider than the invariant the card's own Goal states ("whose baton already sits with the harness **and whose workload tasks have all finished**"), its comment claims a masked deadlock is "unreachable by construction", which is false, and at this tip the guard's only remaining domain is exactly where it masks one. Since A-6b-0's `await_first_baton`, `run_life` can no longer reach the guard's state (measured below), so the guard now serves only direct `Sched` users and zero-task lives, and in that domain it turns a real deadlock (probe P1) into `Ok(())` with a task left blocked forever. That contradicts S-CRASH as the module states it (`sim/mod.rs:28-31`: "`DEADLOCK` (all tasks blocked) ... never passes"). The fix is one clause, already tested green by two parties, so this is REWORK with exact text, not ESCALATE: no spec or test needs to change. The rework also takes the hook off the unarmed commit path (R5) and brings the comments up to date (R2, R3), so no follow-up remains.

## Evidence (all run by this gate)

- **Fresh gate** (GATE-PROMPT step 1, extract through the `--no-local` clone): scaffold `cce145f5b8e6df98d7f6daa16f5c753268f544bf`; `fresh-gate: commit cd6b45f3372ade228f1944a5d4ef4148b25b4307`, `fresh-gate: base cce145f5…`, `fresh-gate: checks passed (4913 files hashed in 1.48 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`; last line `GATE EXIT=0`, first run, with no failing row.
- **Scope:** `git diff nucleus/v3-scaffold...HEAD --stat` gives exactly the four Touch-only files (sched.rs +28/−3, sim_env.rs +103/−1, lib.rs +36/−0, undo_order.rs +81/−22); merge-base `f24803a0`. Nothing under `spikes/e4-recovery` changed since `48ac9920`.
- **The move:** `ed03e051` has parents `3bdba02f` (the original build) and `f24803a0` (the scaffold with A-6b-0). Against E4's `sched.rs` at `48ac9920` the tip's only code departures are `await_first_baton` (S1) and the guard; every other changed line is a comment. `sim_env.rs` keeps A-6b-0's tests and adds the card's three. The two cards' behaviour does not interact: `await_first_baton` is one `park` call and never reaches `handoff_locked`, and the life-ended flag lives on `SimEnv`/`SimFile`, which `handoff_locked` never reads.
- **Acceptance as amended, all six run by this gate:** 1: exit 0, 200/200, 19 s, load 1.7; 2: exit 0, 200/200, 12 s; 3: exit 0, 200/200 with 8 busy loops, 29 s, load rising to 4.6; 4: exit 0, 200/200 × 4 tests, 22 s, load 5.9; 5: exit 0, 136 passed (e1-arm-c 9, nucleus-env 127); 6: exit 0. Verbatim pre-amendment line 1: `error: unexpected argument '--exact' found`, exit 1.
- **SIMENV-FLAKE-1, reproduced:** `sim_halted_off_task`'s exact body, 50,000 lives in one process with 16 busy loops (load about 18): **base `c81e84a1`: 5 false DEADLOCKs** (`deadlock=5 crashed=5`); **tip: 0**; **tip with the guard removed (SE-1): 0**. So at this tip the race is closed through `run_life` by A-6b-0 alone. Whole-binary loops (`--exact sim_halted_off_task`, 10,000 runs each, 32 busy loops; and 3,000 runs pinned to one core with 12 busy loops) saw 0 failures at the base: the race is too rare there to show up, and a single core starves the task, not the harness.
- **Probes at the tip** (a scratch test file, removed afterwards): P1, a harness `wait_for(HARNESS, Tree(1))` before `start()` with task 1 alive and parked: **`Ok(())`, `deadlock=false crashed=false running=Some(0)`, task 1 still blocked after 3 s**, released only by `deliver_crash`. With the narrowed clause: `Err(Deadlock)`, world failed, task 1 `Err(Crashed)`. P3 (`finish(1)`, then `start()`): `Ok` under both forms. P6 (`run_life(0)`): no deadlock under both forms. P7 (3 non-parking tasks drawing `rand()`, 2,000 lives): **1** draw sequence and 1 trace under both forms.
- **The rework text was trial-applied by this gate and reverted:** the narrowed clause plus R4's pin: sim_env 39 passed, sim 26 passed. With the wide clause and the pin, `sim_harness_wait_before_start_is_deadlock` FAILS (sim_env.rs:1402). R5's `OnceLock` hook: e1-arm-c's whole suite passes, and `cargo clippy -p e1-arm-c --all-targets -- -D warnings` is clean.

## Findings

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

## REWORK: exact text (mechanical; the orchestrator amends the card first, see R7)

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

## Rulings (a)–(h)

- (a) Amendment 1 is sound: it is the minimal fix of a cargo usage error, and all four lines pass as amended.
- (b) The diagnosis is right (base: 5 in 50,000, mechanism read and forced by P3). At `start()` the guard flips only H1. It also flips P1 (`wait_for(HARNESS)`), a real deadlock. Through `run_life` it hides nothing, and since S1 it is unreachable there.
- (c) REWORK: R1 plus the R4 pin. No in-tree caller reaches it, but the guard's only remaining domain is that API.
- (d) E1C-1's row text is rewritten per R7, and E1C-4 is added as the per-window evidence.
- (e) Confirmed at base and the original build. It was a code defect against S-DET, not a spec defect, and A-6b-0's S1 closed it (P7: 1 sequence). No card.
- (f) The hook is allowed by the card and not barred by the plan (no line citations). It is reachable by any dependent and costs about 3% contended unarmed, so R5 makes it lock-free.
- (g) Stale comments: R2 and R3. Notes: R7. A-6b-0's line-1 grep: no action (merged, historical, superseded by its ruling).
- (h) Line 1 is true and complete for the current code and must track R1 (R2a). The E4 prototype is untouched.
