Card: X-1b
Class: experiment
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code review subagent, session 4e4af138-9125-4ec2-a2dc-5e9703cef3b4
Date: 2026-10-22
Gate-exit: GATE EXIT=0
Commit: 4c1f0f86
Mutants: SEED-PER-ARM (W1 rng salted by type_name::<E>()) -> UNCAUGHT, POOL-PER-ARM (make_c gets pool/4) -> UNCAUGHT, VERIFY-BYPASS (arm_c_passed forced true) -> UNCAUGHT, PCTL-OFF-BY-ONE (idx = rank, not rank-1) -> util::tests::percentiles_are_nearest_rank, W5-ALIVE-OFF (original never set false) -> UNCAUGHT, CPU-WINDOW-INCLUDES-PRELOAD (W1 cpu0 taken before make+preload) -> UNCAUGHT
Followups: none

## Findings

**Gate run.** Every step EXIT=0, ending in `GATE EXIT=0`, on 4c1f0f86 with
`CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1b`.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: 15 files, all under
`nucleus/v3/spikes/e1-bench/` (the workspace glob `spikes/*` picks the crate
up; `Cargo.lock` is gitignored). No protected path is touched. There is no
non-test `unwrap`/`expect`/`panic` and no `unsafe`. The pub items have docs.
The measured commit is d28582e8; the only change after it (to 4c1f0f86) is
the README.

**Fairness review (B-SAME).** Arms (b) and (c) run the same generic
workload code with one `Config`. The seeds are derived from
(workload, run, thread, purpose) only. Pages, pools, thread counts and the
64 MiB undo budget are identical. Every configuration gets a fresh engine
and preload, and the CPU window excludes the preload. Within a process,
(b) always runs before (c); the gate re-ran W1 as separate processes in
the order c then b and saw no order effect (t=16: c 117.3, b 71.6 µs).

**Re-measurement.** Quiet host (load1 < 4 before each run), same binary
built in release, 3 runs each, medians. Gate vs RESULT:

| metric | (b) gate / RESULT | (c) gate / RESULT |
|---|---|---|
| W1 CPU µs/txn t=1/4/8/16 | 4.0/8.7/15.7/67.3 vs 4.0/9.4/15.7/69.1 | 3.7/8.0/19.6/117.4 vs 3.8/7.9/19.5/117.0 |
| W2 reader ops/s w=0/10/100 | 729k/56.4k/5.77k vs 767k/53.9k/5.73k | 953k/74.5k/6.37k vs 905k/74.4k/6.44k |
| W2 p50 / p99 at w=100 (µs) | 1486 / 2271 vs 1518 / 2352 | 622 / 3775 vs 665 / 3668 |
| W3 p50 latest / old (µs) | 88.3/88.2 vs 88.0/87.6 | 10.5/144.9 vs 10.7/146.2 |
| W4 tree/logical after GC | 20.11 vs 20.11 | 2.54 vs 2.54 |
| W5 peak RSS; reads originals | 1356 MiB; yes vs same | 1075 MiB; yes vs 1080 MiB; yes |

Every cell is within about 8%. In both measurements, (c)/(b) on W1 never
reaches 2×. W2 throughput is 1.11–1.38× (c)/(b). The p50 at w=100 does
cross 2× (2.39× in the gate's run, 2.28× in the RESULT). The worker read
the rule's "≥ 2× on reads under writers" as a workload-level win and
flagged that reading. The rule's wording does not settle it, so it is
X-4's call; the gate did not change it. Whether "RSS within the N16
budget" is met cannot be judged until R-1 sets a number; that is also
flagged for X-4. Nothing was tuned.

**Defects (REWORK):**

1. **The W6 pool is too small for arm (b), and the README says it is
   not.** After the big commit, (b)'s tree is 5,323,112,448 B, about 650k
   pages. The shared W6 pool is 393,216 frames = 3,221,225,472 B. So (b)
   evicts during the fill and commit, while (c)'s 2.75 GB tree fits. The
   card's default is "pool large enough for the data unless stated
   otherwise", and this is not stated. The README claims a "~300k-page
   tree" for W6, and for W5 "~45k pages — no eviction". In fact (b)'s W5
   tree is 536,469,504 B, 99.9% of its 536,870,912 B pool. W6's commit
   time and head-of-line delay feed §3.12's pre-apply trigger, and (b) is
   the arm the RESULT picks.
   - **Fix:** size the W6 pool, identically for both arms, to hold (b)'s
     post-commit tree. The host has 38 GiB, so this fits. Give W5 real
     headroom, re-run W5 and W6, and correct the README.
   - **Also:** state the head-of-line sample count. The p99 is taken over
     5 txns for (b) and 16 for (c), so it is in effect the max.
2. **Nothing guards measurement integrity.** SEED-PER-ARM and POOL-PER-ARM
   both pass every test, so one arm can be handed a different key stream
   or configuration silently.
   - **Fix:** add a test that fails when the arms' W1/W2 key streams or
     engine configurations differ (for example, a recording `TxnEngine`
     wrapper driven through `w1::run`/`w2::run` for both arm types).
   - **Also:** cover the B-ABORT decision (VERIFY-BYPASS) and W5-ALIVE
     with a deliberately broken engine.
3. **Arm (a)'s seeds do not match its documentation.** A-SAME says arm (a)
   uses "the same seeds", but it draws keys from
   `Rng::new(rng_for(..).next_u64())` and derives values differently. Its
   key stream therefore differs from (b)/(c)'s. This is reference only and
   statistically equivalent, but either fix the code or fix the doc.

Do not change the W1/W2/W3/W5 method or the decision-rule text in the
rework. These have been re-measured and are sound.
