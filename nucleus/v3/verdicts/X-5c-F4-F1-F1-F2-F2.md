Card: X-5c-F4-F1-F1-F2-F2
Class: test
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: glm-5.3
Gate-session: T3 Code review sub-agent, OpenCode (nv3-X-5c-F4-F1-F1-F2-F2-gate-1, 2026-11-07)
Date: 2026-11-07
Gate-exit: GATE EXIT=0
Commit: 03eb5de1cb540f2e4f7fdd8539d27044bdc52209
Mutants: G-ACK-NOWRITE -> ack_prefix_two_batches_in_flight (driver timeout at the 300 s deadline, exact preserved message), G-ACK-MAXOVERHOLE (own) -> ack_prefix_two_batches_in_flight (mid-state assert, 3.06 s), G-DRIVER-OR (own) -> UNCAUGHT (benign, finding 2)
Followups: none

## Verdict in one paragraph

MERGE. The card did exactly what it promises, in scope, and the fresh gate is green end to end. The driver's two polls in `ack_prefix_two_batches_in_flight` back off 1 ms and the deadlines moved 30 s -> 300 s; nothing else in `tests/store.rs` changed (the only removed lines are the two 30 s `deadline` initialisations; every assertion, the schedule, the seed and the park budget are byte-identical). The timeout is raised, not deleted: a hung world still fails, and my G-ACK-NOWRITE run shows it failing at exactly the new deadline with the preserved message rather than hanging. The 104 s mystery is solved, and the answer exonerates the store: the busy-poll was starving the very tasks it waited on, and X5-R49 made this test faster, not slower (finding 1 corrects the card's premise).

## Gate run

- Scaffold `acc81c776183bd9853f7a69e2cec4262d300d89f`, extracted through the prescribed `--no-local` shallow clone; no `EXTRACT FAILED`, no refusal lines.
- One foreground run at `03eb5de1cb540f2e4f7fdd8539d27044bdc52209` from `~/Workspace/neutron-v3`: `fresh-gate: checks passed (4518 files hashed in 2.20 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`, then `STEP test EXIT=0` (the store suite 50/0 in 8.32 s, `tests/workload.rs` ran), `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0` (the ruled `runner e2e: K` row read ok this time). Literal last line `GATE EXIT=0`, stderr empty, `$fg` removed. Nothing failed, so no second run was needed; the worker's EXIT=1 was the same K row only, ruled in verdict X-5c-F4-F1-F1 ("do not chase").

## Scope

`git diff 3cef0055...03eb5de1` touches only `nucleus/v3/spikes/e4-recovery/tests/store.rs` (the card's Touch-only list; nothing protected). 13 insertions, 2 deletions: the two deadline lines replaced, two `std::thread::sleep(Duration::from_millis(1))` insertions placed after the non-breaking checks (so neither the success break nor the timeout path is delayed), and explanatory comments. No assertion, schedule, seed or park budget line moved; no `unsafe`, no new non-test `unwrap`/`expect`/`panic`; no pub items. The failure paths (`deliver_crash`, the joins, both panic messages) are untouched.

## Mutants (release, single test, `git archive` exports; every mutant reverted, checksums re-verified against the committed blobs)

| Mutant | Result |
|---|---|
| G-ACK-NOWRITE (card's): task B never sets `b_wrote` | caught — FAILED at 300.00 s, `driver timeout: B never completed batch 2 (a_enq=true b_go=true b_wrote=false)` (store.rs:2786, the preserved message). A hung world fails at the new deadline instead of hanging, which is exactly the Do-not the card kept. |
| G-ACK-MAXOVERHOLE (own): `absorb_interval` advances a frontier to the max interval end over a hole — ack-prefix ordering broken in the store | caught — FAILED in 3.06 s, `arm 0: two batches were never in flight at once (12.4 / S-1-F3 W3)` (store.rs:2813). The mid-state block catches out-of-order absorption before either arm's end state. |
| G-DRIVER-OR (own): second poll breaks on `a_acked \|\| b_acked` | UNCAUGHT — passes in 4.36 s. Benign: that loop is a wait, not the check. When `a_acked` holds, batch 2's synced interval already forces `durable >= b2_end` by the prefix rule, and the final durable assert, the mid-state block and the crash arm carry the ordering claim. The card's Limits froze the assertion set, so nothing was weakened or was available to strengthen. |

## Experiment re-run and the 104 s question

Re-measured alone, idle host (cargo-harness time for the single test), plus the before/after and pre-X5-R49 contrasts the card's Work asked for:

| tree | store | driver | debug | release |
|---|---|---|---|---|
| `03eb5de1` (tip) | X5-R49 | sleep 1 ms | 8.94 / 8.17 / 6.30 s | 5.05 / 5.06 / 4.98 s |
| `eb9c4328` + 300 s scratch | X5-R49 | busy-poll | 51.43 s | 7.86 s |
| `1524e284` + 300 s scratch | X5-R49 (src byte-identical to eb9c4328) | busy-poll | 40.63 s | — |
| `fe353ee6` + 300 s scratch | pre-X5-R49 | busy-poll | 93.93 s | 11.10 s |
| `fe353ee6` + sleep scratch | pre-X5-R49 | sleep 1 ms | 6.31 s | — |

- **It was the busy-wait, not the store: a test-harness problem.** Each `flag()` call takes the world lock, and the tasks need that lock for every baton handoff; a tight acquire loop monopolises it. Same store, driver the only variable: 51.4 s vs 6.3-8.9 s in debug (x6-8), 7.9 s vs 5.0 s in release. With a polite driver both stores run ~6-9 s in debug.
- **X5-R49's slot freeing made this test faster, not slower — no store performance regression.** Pre-X5-R49 busy-poll is 93.9 s vs 51.4 s after; pre-X5-R49 with the sleep driver is 6.31 s vs the tip's 6.30-8.94 s.
- **Worker's numbers reproduced**: alone-median release 4.56 s and debug 8.22 s (mine: 4.98-5.06 s, 6.30-8.94 s); the decision rule ("the wall time should drop; say so if it does not") was applied verbatim and the drop is confirmed at the same tree.

## Findings

1. **The card's premise misattributes the deterministic failure to X5-R49 ("deterministically since eb9c4328").** `1524e284`, named as "before X5-R49", already contains it: X5-R49 landed in `2e274d09`, three commits earlier, and `src/` is byte-identical from `1524e284` through the tip — its busy-poll reading (40.6 s) is the same regime. At idle in debug the busy-poll blew the 30 s deadline at *every* commit measured, including pre-X5-R49 `fe353ee6` (93.9 s). The test historically passed only under host load, because preemption interrupts the poller and breaks its lock monopoly (consistent with the card's own 12-44 s at load 25-45 and the 1-of-4 failure at `a4c1f869`); the base gate simply ran at idle (load ~2) and exposed a starvation that was always there. The fix is correct regardless — it removes the contention at every commit — and no in-repo change is needed, so this is a record correction, not a follow-up.
2. **G-DRIVER-OR is benignly uncaught** (see the mutant table): the second poll's `&&` is belt-and-braces; the ordering claim is carried by assertions the card's Limits froze.
