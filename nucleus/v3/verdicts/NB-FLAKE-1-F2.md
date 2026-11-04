Card: NB-FLAKE-1-F2
Class: test
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: glm-5.3
Gate-session: nv3-NB-FLAKE-1-F2-gate-1
Date: 2026-11-03
Gate-exit: GATE EXIT=0
Commit: b350d551d494619ccd70b9dcc0d2b36241a52af4
Mutants: NBF2-G1-ONE-ATTEMPT -> load_retry_gives_up_after_the_budget, NBF2-G2-TRANSPORT-FATAL -> load_retry_gives_up_after_the_budget, NBF2-C1-4XX-RETRIED -> client_errors_are_not_retried, NBF2-C2-4XX-SWALLOWED-AS-ERR -> client_errors_are_not_retried, GATE-M1-BUDGET-INFLATED -> UNCAUGHT (disclosed trade, wall-time-only symptom), GATE-M2-429-NOT-TRANSIENT -> transient_status_classification
Followups: none

Gate run (manual steps; `scripts/fresh-gate.sh` does not exist at b350d551,
X-6-F1-F2-F2-F1-F1 not landed): fresh detached checkout of b350d551 at
/tmp/gate-NB-FLAKE-1-F2 with `core.hooksPath=/dev/null` on `worktree add`;
verified no hooksPath config, no non-sample hooks, no `filter.*` config, no
`info/attributes` (worktree and common git dir), index == commit tree, and
540/540 `nucleus/v3` files byte-identical to the commit (blob-hash compare).
`bash scripts/gate.sh` in the foreground with
`CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-NB-FLAKE-1-F2`: test, clippy,
fmt, deny, wasm32, core-paths-selftest, enforcement-tests all EXIT=0; literal
last line `GATE EXIT=0`; nbench 131 passed / 0 failed (count unchanged).

Scope: `git diff nucleus/v3-scaffold...HEAD --stat` is exactly
`nucleus/v3/bench/nbench/src/vector.rs` (+32/−7); merge base verified as
185ffff7c8e217cdeaaf248b78016a8a3bc60af4; both hunks sit inside
`#[cfg(test)] mod tests`; `load_retry_survives_dropped_connections` and
`transient_status_classification` absent from the diff (byte-unchanged);
nothing under protected paths.

ORCHESTRATOR AMENDMENT check: the pre-amend two-dot form
(`git diff nucleus/v3-scaffold -- .`) diffs scaffold↔worktree, so after the
scaffold moved it lists other cards' merged files and fails spuriously; the
scaffold has moved again since the amendment (now 1688df84). The amended
three-dot form diffs merge-base↔HEAD, which still covers every commit on this
branch, and the merge base did not move (no scaffold merge was pulled in). The
only formal difference — two-dot sees uncommitted worktree edits, three-dot
does not — is covered by this gate's index/tree/byte checks. The amendment
repairs the check to its original intent; it does not loosen it.

Correctness: the diff is exactly Work 1–4. F1: one comment line deleted from
`flaky_server`'s body, nothing else. F2: per-attempt timeout args 5 s → 30 s
in both tests (test-supplied bounds, not assertions); attempts (2 / 5),
`assert!(r.is_err())`, `.expect("response returned without retry")` and
`assert_eq!(resp.status, 400)` kept; `count == 2` → `count >= 2` and
`count == 1` → `count >= 1`; both comments reworded to the new proof. No
non-test change, no `unsafe`, no new non-test `unwrap`/`expect`/`panic`, no
new dependency.

The deliberate trade (== → >=, upper leg gone): consistent with verdict
NB-FLAKE-1's F2 ("convert to lower bounds or assert on the client-visible
outcome only") — the card does both: budget-spent → `count >= 2` event-synced
to each attempt's server-close; not-retried → the Ok-shape (a retried or
swallowed 4xx burns the budget into the loop's final `Err`, lines 203/214);
status-reported → `status == 400`. Not a weakening: G1/G2 show the count leg
is load-bearing (Err-shape alone passes both), C1/C2 show the Ok-shape carries
the no-retry proof. The upper leg is structurally unassertable at the server
(a phantom dial inflates the counter exactly like a local one); the apparent
fix — a `["Drop","Drop","Status:200"]` script catching inflation via the
Err-shape — is phantom-sensitive (a phantom eating one `Drop` desyncs the
client onto the `200` → spurious `Ok` → flake), so all-`Drop` repeat-last is
the only phantom-immune script and its inflation-blindness is the price.
GATE-M1-BUDGET-INFLATED (`attempts.max(1)` → `attempts.max(6)`) confirms the
disclosure: both hardened tests pass; the only symptom is wall time (gives-up
23.63 s vs ~1.2 s typical). Recorded as deliberate, not a defect.

Timeouts and hangs: each attempt is bounded (30 s), the loop by `attempts`,
backoff capped at 8 s. A starved server thread becomes a slow attempt or slow
success — the intended hardening. A genuine never-accept hang still fails
loudly: gives-up ends `Err` with count 0 → `>= 2` fails; client_errors
exhausts 5×30 s + 15 s backoff (~165 s) into `Err` → `.expect` panics. No
silent pass exists. Residual: a >30 s starve spanning attempts — the same
residual NB-FLAKE-1 accepted. Minor wording nit: the card's "worst ≈ 30 s"
for client_errors understates that loud all-attempts-time-out path (~165 s).

Mutation evidence (applied to the /tmp gate copy with perl -0pi, single named
test run, reverted with `git checkout --`, `git diff --quiet` + zero untracked
verified after every mutant and at the end; no cargo build of this tree raced
an application):
- NBF2-G1-ONE-ATTEMPT (`0..attempts.max(1)` → `0..1`): FAILED, vector.rs:1435
  `assertion failed: count.load(Ordering::SeqCst) >= 2` (count == 1). caught.
- NBF2-G2-TRANSPORT-FATAL (`Err(e) => last_err = Some(e)` → `return Err(e)`):
  FAILED, same assertion (early return ⇒ count == 1). caught.
- NBF2-C1-4XX-RETRIED (`status >= 500` → `status >= 400`): FAILED,
  vector.rs:1464 `.expect("response returned without retry")` after the full
  1+2+4+8 s backoff ladder (15.52 s — the budget really burned). caught.
- NBF2-C2-4XX-SWALLOWED-AS-ERR (guard `!` dropped): FAILED, same `.expect`
  panic at 1464 (15.53 s). caught.
- GATE-M1-BUDGET-INFLATED (gate's own, retry floor inflated:
  `attempts.max(1)` → `attempts.max(6)`): gives-up PASSED in 23.63 s
  (vs ~1.2 s), client_errors PASSED in 0.11 s. UNCAUGHT — the disclosed,
  deliberate trade; wall-time growth is the only symptom.
- GATE-M2-429-NOT-TRANSIENT (gate's own, `|| status == 429` dropped):
  transient_status_classification FAILED at vector.rs:1475 (`429 must
  retry`); client_errors itself passes (by design — classification is pinned
  by the byte-unchanged pure test). caught.

Measurement re-run (gate): with `cargo test --workspace -q` building/running
concurrently in `$HOME/.cache/nv3-target/gate-NB-FLAKE-1-F2-ws` and host load
5.2–6.6 (co-tenant gate run for X-5c-F3-F3a live throughout),
load_retry_gives_up_after_the_budget 20/20 in 32 s (~1.6 s/iter) and
client_errors_are_not_retried 20/20 in 3 s (~0.15 s/iter). Matches the card
RESULT (worker 30/30 each, 1.21–1.22 s and 0.10–0.12 s per iter, with and
without load; decision rule applied verbatim; timings not tuned — the
worker's C1 15.52 s ladder reproduces the gate's exactly).

Findings:
1. (non-blocking) GATE-M1-BUDGET-INFLATED is UNCAUGHT — budget inflation
   above `attempts` has no red assert anywhere. Disclosed in the card Notes,
   authorized by the parent verdict's wording, and structural: no server-side
   witness can pin an upper bound under phantom dials, and the client-visible
   alternative (a terminal `Status:200` entry) reintroduces phantom
   sensitivity. Symptom is acceptance/gate wall-time growth. No follow-up
   card; recorded here as the known limit.
2. (non-blocking, card-local wording) "client_errors worst ≈ 30 s" describes
   the lone useful attempt; the loud all-attempts-time-out failure path is
   ~165 s (5×30 s + 1+2+4+8 s backoff). Repo content is unaffected.
