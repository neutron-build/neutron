Card: NB-FLAKE-1
Class: test
Card-gate: S
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: S
Gate-model: glm-5.3
Gate-session: nv3-NB-FLAKE-1-gate-1
Date: 2026-11-03
Gate-exit: GATE EXIT=0
Commit: c2ace01852d76a590344c34ea9599244315127c2
Mutants: NB1-NO-RETRY -> load_retry_survives_dropped_connections, NB2-TRANSPORT-ERRORS-FATAL -> load_retry_survives_dropped_connections, MG1-ATTEMPTS-CAP-1 -> load_retry_survives_dropped_connections, MG2-NO-DROPS -> load_retry_survives_dropped_connections
Followups: NB-FLAKE-1-F1, NB-FLAKE-1-F2

Gate run (manual steps; `scripts/fresh-gate.sh` does not exist at this commit,
X-6-F1-F2-F2-F1-F1 not landed): fresh detached checkout of c2ace018 at
/tmp/gate-NB-FLAKE-1, `core.hooksPath=/dev/null` on `worktree add`; verified no
active hooks, no `filter.*` config, no `info/attributes` (worktree and common
dir), index == commit tree, and 537/537 `nucleus/v3` files byte-identical to
the commit (smudge-filter check). `bash scripts/gate.sh` in the foreground with
`CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-NB-FLAKE-1`: literal last line
`GATE EXIT=0` (test, clippy, fmt, deny, wasm32, core-paths-selftest,
enforcement-tests all EXIT=0; nbench 131/131 — count unchanged, no tests added
or removed). Checkout removed after the verdict work.

Scope: `git diff nucleus/v3-scaffold...HEAD --stat` is exactly
`nucleus/v3/bench/nbench/src/vector.rs` (+50/−10), the card's only Touch-only
entry; both hunks sit inside `#[cfg(test)] mod tests` (module at line 1303 of
2101); `load_retry_gives_up_after_the_budget`, `client_errors_are_not_retried`
and `transient_status_classification` are byte-unchanged; nothing under
protected paths.

Correctness: `flaky_server` now runs an unbounded accept loop indexing the
script by accepted-connection number (`i = counter.fetch_add(..)`,
`script[i.min(script.len() - 1)]` — repeat-last), `continue`s on accept errors,
keeps the 100 ms quiet drain and the `let _ =` write, and keeps its signature
and counter meaning; the server thread can no longer die before the client's
final attempt. The test raises only its per-attempt timeout argument (5 s →
30 s, a bound — not an assertion), keeps attempts = 5 and `status == 200`, and
replaces `count == 3` with the event-synced lower bound `count >= 3`. No
wall-clock, upper-bound, or exact-equality assertion load can break; no client
sleep; no `unsafe`, no new non-test `unwrap`/`expect`/`panic`; no new
dependency. The client's `Ok` proves ≥ 3 accepts (counter increments before
the response write), and entries 0/1 being `Drop`s keeps both drops
structurally pinned ahead of any 200.

Mutation evidence (applied to the /tmp gate copy with perl -0pi, single test
run, reverted with `git checkout --`, tree verified clean after each; logs
/tmp/opencode/nb1-mutant-*.log):
- NB1-NO-RETRY (`0..attempts.max(1)` → `0..1`): FAILED, panic at vector.rs:1404
  `.expect("retry succeeds")` — `retry succeeds: Parse { what: "http response",
  line: "no header terminator" }`. caught.
- NB2-TRANSPORT-ERRORS-FATAL (`Err(e) => last_err = Some(e)` → `return
  Err(e)`): FAILED, same panic at 1404. caught.
- MG1-ATTEMPTS-CAP-1 (gate's own, retry budget clamped to one attempt:
  `attempts.max(1)` → `attempts.min(1)`): FAILED, same panic at 1404. caught.
- MG2-NO-DROPS (gate's own, server never drops: `script[i.min(script.len() -
  1)]` → `script[script.len() - 1]`): FAILED, vector.rs:1406 `assertion failed:
  count.load(Ordering::SeqCst) >= 3` — the test still pins the two scripted
  drops. caught.

Measurement re-run (gate): 22/22 passes of
`cargo test -q -p nbench --bin nbench load_retry_survives_dropped_connections`
(82 s wall, ~3.7 s/iter) at host load ~22 (1-min avg 21.97 — the regime where
the unmodified test failed 2/5), while `cargo test --workspace -q` ran
concurrently in a separate target dir (45/45 `test result: ok`, WS_EXIT=0) and
the X-5c-F3-F3a runner was co-tenant. Matches the card RESULT (runner 30/30
under load, 1 attempt). The worker's disclosed mutant/workspace compile race
could not recur here: mutants and concurrent runs were strictly sequenced
across disjoint target dirs.

Findings:
1. (non-blocking) Leftover diagnosis comment at vector.rs:1334 — `// TEMP
   NB-FLAKE-1 diagnosis: process-global accept log` — dangling: no static/log
   exists anywhere in the module (the instrumentation was removed before
   commit; grep for `static` items confirms). Misleading dead text inside the
   test module this card just stabilized → NB-FLAKE-1-F1.
2. (pre-existing, out of scope, worker-escalated) The neighbours' exact
   accept-count assertions are phantom-accept-sensitive and
   `client_errors_are_not_retried` carries a >5 s single-attempt starvation
   fragility (card Notes) → NB-FLAKE-1-F2.
3. Worker honesty: acceptance-2's workspace run briefly compiled a mutant in
   (129/131, self-disclosed, root-caused, committed tree re-passed 131/131);
   no action needed, but future cards should never let a cargo build race a
   mutant application.

After merge: re-run merge pass 127's promote for X-6-F1-F1-F3 (QUEUE line 39).

## Follow-ups

NB-FLAKE-1-F1: delete the dangling line `// TEMP NB-FLAKE-1 diagnosis:
process-global accept log` at `nucleus/v3/bench/nbench/src/vector.rs:1334`,
first line of `flaky_server`'s body. The "process-global accept log" it
announces was diagnosis scaffolding removed before the commit; the comment is
dead text, and "process-global" contradicts the per-test listener design the
card documents. Test-module-only, one line, no behavior change; the neighbours
must stay byte-unchanged and the 30× acceptance loop re-run per the card.

NB-FLAKE-1-F2: harden the exact accept-count assertions in `flaky_server`'s
neighbours against phantom accepts: `load_retry_gives_up_after_the_budget`
asserts `count == 2` and `client_errors_are_not_retried` asserts `count == 1`
(the latter also passes a single 5 s attempt, so a >5 s starvation hits its
`.expect` — the card's own Notes). A process on this shared-netns host whose
own scripted listener died mid-retry can keep dialing its freed port; a fresh
`bind("127.0.0.1:0")` wins that port and the foreign retry lands as a phantom
accept, inflating the count (worker observed `count == 3` with exactly two
local dials twice; repro `/tmp/opencode/nb-flake1-phantom-repro.rs`, log
`/tmp/opencode/NB-FLAKE-1-gives-up-count3.log`). Convert to lower bounds or
assert on the client-visible outcome only, per B-0-F7-F2. Phantom pressure is
minted by dying finite-script listeners (this card removed one source;
http.rs's scripted test servers remain), so pressure drops as the pattern
propagates, but the exact assertions stay load-sensitive today.
