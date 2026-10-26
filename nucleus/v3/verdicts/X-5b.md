Card: X-5b
Class: experiment
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-agent e0e23fc9-b7df-4d9c-aafb-e417c7ab8438
Date: 2026-10-25
Gate-exit: GATE EXIT=0
Commit: aa263472c0212f7da7ae3535972ee27f5cc2c42e
Mutants: B3 taint judged only on the most recent overlapping read -> accept_hook_judges_older_tainted_read, try_lock replaced by a blocking lock in Sched::acquire (card instrumentation check) -> UNCAUGHT, CRC-SKIP hard-coded in page::check -> UNCAUGHT, REFCRC-SKIP hard-coded -> UNCAUGHT, EPOCH-SKIP+REFCRC hard-coded -> UNCAUGHT, ID-SKIP+REFCRC hard-coded -> UNCAUGHT, TRUNCATE-LOGICAL-ONLY (D7 arm read) -> UNCAUGHT, FREE-EARLY / WRITE-GUARD-NO-COPY / EVICT-SKIP-IMAGE-MAP / PUBLISH-BEFORE-META / START-SEGMENT-MISSING-OK (arms read; they assert the mutant passes) -> UNCAUGHT, G1 S-BATON held-flag checks removed from yield_baton_at/wait_for -> baton_violation_is_reported (hangs; killed at 90 s), G2 gate_exclusive post-yield holders re-check always true -> d5_split_straddling_flip (+d17_integrity), G3 a yield inside a Cx::with step (gate_shared) -> d1..d18 (17 directed tests fail via task_error), G4 harness drops recovery-world served corruption from O-3(a) -> UNCAUGHT, G5 harness R-taint forced false -> UNCAUGHT
Followups: none

## Scope of this verdict
Stage X-5b (B1–B6) at aa263472, base 266b8f4c (X-5a merged). The gate run was
on the tip, in the foreground, with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-5b`.
Every step printed EXIT=0, and the last line was `GATE EXIT=0`. That is necessary,
not sufficient: several mutant arms that the gate runs assert the wrong
thing (R1, R2).

## What holds
- **Scope.** All 19 changed files are under `spikes/e4-recovery/`. X-5's frozen
  files (`src/oracle.rs`, `src/model.rs`, `tests/oracle.rs`) are unchanged, and
  `tests/sim.rs` only gains the two new tests at the end. `e4-campaign` is still
  a stub that exits 2. The drafts in `src/workload.rs` stay outside the module
  tree. There is no `unsafe`, no non-test `unwrap`/`expect`/`panic`, no
  `HashMap`, and no new dependencies. The README's flat-map bullet states Work 12's four
  conditions and names A-3c/A-7.
- **The B1 conversion (rule 1/2/4).** Store code takes the host in exactly one
  place: `Cx::with` → `Sched::acquire` (`store/mod.rs:517`). The guard
  is scoped to the closure, and every park goes through `yield_baton_at`/`wait_for`,
  both of which check the thread-local held flag. The wait loops check their predicate
  under the baton, then park with no yield in between. `scheduler_determinism` is
  unchanged and green, and `yield_baton_at` delegates to `yield_baton`. My mutants
  G1–G3 confirm that the detector and the gate re-validation work: a yield under a
  guard is loud, not a hang, in all 17 directed shapes.
- **Same-round reads (Work 13).** Every store accept site follows its own
  `read_at` in the same function and the same round:
  `ckpt.rs:210,232,351,398,1047`, `prims.rs:528` and
  `amcheck.rs:264,315,390`. Frame hits are never re-accepted. The store is rebuilt
  at every open, so nothing held from an earlier round gets accepted. The harness
  calls `new_round` after the crash and the faults (`harness.rs:1120`).
- **The B3 pin (Work 14).** `accept_hook_judges_older_tainted_read` is present,
  passes on the merged `SimDisk::accepted`, and fails under B3, with only that
  test failing.

## Findings: REWORK
**R1. Inverted and vacuous mutant arms (Work 14, "Do not weaken").** Work
14 says "a mutant arm that passes is a test failure". Mutation evidence says
"in this stage, every mutant must be caught by its directed test".
Instead:
- These arms assert that the mutant **passes**: D10 (`directed.rs:767`), D11 (`:835`),
  D14's FREE-EARLY (`:1105`) and D16's two arms (`:1291,1297`).
- D10 never applies its LOST fault. `out2` and `out3` are computed and then discarded, so the
  G3 premise is never built.
- D7's arm, `!m.pass || m.o1.is_ok()` (`:558`), cannot fail for any round
  that is clean on O-1. It also lacks the card's "recover; commit shorter
  records; crash" second round. TRUNCATE-LOGICAL-ONLY is therefore uncaught,
  although the RESULT says "D7 ✓".
- D17 asserts that leaf LOST/TORN is silent. The card wants XX001.

Rework these as real catching arms in the card's shapes (D10/D10b with a
directed LOST on the newer meta after a recycle; D7 multi-round; D16 with an
eviction inside the flip window; D14 with the M-NOINPLACE undercount
diagnosed). Where a mutant is truly not reachable in a directed shape, argue it as
an equivalent mutant in the RESULT, for the gate to confirm or reject. Never write it as an assert-pass.

**R2. D17's clean arm accepts served corruption.** It checks
`r.is_err() || !served.is_empty()` (`directed.rs:1409–1415`); the card says "XX001
and never served". The mutant arm asserts only when the mutant is
silent. With each of §3.3's checks hard-disabled in `page::check` (CRC,
REFCRC, EPOCH+REFCRC and ID+REFCRC), **the whole crate suite stays green**.
None of the four integrity mutants is demonstrated, although the RESULT claims all four.
D17 also `continue`s past targets that have no matching durable write. That skip must
be an assertion.

**R3. D10b is missing, and the harness half of X5-R5 is untested and off-ruling.**
- With the R-taint forced false (G5), every test still passes.
- With recovery-world served corruption dropped from O-3(a) (G4), every test still passes.
- `exception_set_r` (`harness.rs:1886`) omits X5-R5's third condition: no
  write to `s` drew an M-NOINPLACE hit.
- The R-taint (`:1272`) scans only the extraction world's accepts, from any life.
  The ruling's window is "from open until the store next writes it", and that
  window includes recovery's own accepts.

**R4. The §3.3 id check is self-referential on map and free-set pages.**
`walk_radix`/`walk_freeset` (`ckpt.rs:346–348, 393–395`) and amcheck
(`amcheck.rs:257, 308`) pass `expect_id = header_id(&bytes)`. That check can never fail.
§3.3 says "the expected id is known" from the parent reference. Derive the expected
id from the radix position, as the base of the child.

**R5. The `BulkLoad` script op deadlocks the driver.** The RESULT says this is diagnosed but unfixed.
Work 13 requires bulk loads with publication. D11, and PUBLISH-BEFORE-META with it,
cannot run without them. A known DEADLOCK in a required path is a prototype
defect to fix in this stage.

**R6. The instrumentation check is not met.** If `acquire`'s `try_lock` is replaced
by a blocking `lock`, `baton_violation_is_reported` still passes, and so does the
full suite: detection rests entirely on the held flag. The test's doc comment claims
a by-hand hang demonstration that this mutation does not reproduce, and the
RESULT records no demonstration. Make the test fail under a blocking
acquisition, for example a cross-thread holder case, and record the demonstration.

**R7. RESULT accuracy.**
- Item 2 does not give B1's `git diff --stat`. For 4b1e6068 it is 14 files,
  +4935/−2702, about 7.6k changed lines against the ~1.5k budget, including new amcheck and tests.
- Item 4's mutant table misreports caught arms (R1, R2).
- Item 3 omits D10b.

**Minor (fold into the rework).**
- `RoundOutcome.pass` (`harness.rs:1325,1382`) ignores `deadlock` and `panicked`.
  The `baton` flag is never set anywhere. Fold all three into `pass`.
- `EnvRef::accepted` silently drops the accept when `acquire` fails (`env.rs`).
  That fails open on an oracle input.
- Release paths swallow `SimFail::Baton`: `.ok()` and `.unwrap_or(false)` in
  `gate_release`, `unpin` and the `ckpt.rs:714–829` sites.
- Mutant arms accept `!m.pass` (any failure) where the card names the catcher.
- D1's clean arm does not assert that the monitors are empty.
- Limit 3: the `sim/disk.rs` additions (`dwrites_of`, `DWrite.all`, `Clone`,
  `apply_one_media_fault`) lack new tests in `tests/sim.rs`, and
  `apply_one_media_fault` is beyond the named additions. Justify it or test it.
- The `WorldGuard` `Drop` comment has the order wrong: the flag clears before the
  inner guard unlocks. This is harmless.

No ESCALATE: nothing here is a spec conflict. All mutants were reverted, and
`git diff` was clean before this file was written.
