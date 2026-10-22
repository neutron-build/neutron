# E1′ measurement driver (`e1-bench`) — card X-1b

The measurement half of PLAN-v3 §4 **E1′** (deciding §2 **N3**, the MVCC
scheme). It drives arms

- **(a)** v2 today on `MemKv` (reference only — W1's single-row mix),
- **(b)** timestamp-in-key versions on the X-0 B+tree (card X-2),
- **(c)** newest version in place + undo log with spill (card X-3),

through the same `TxnEngine` surface (card X-1a) under identical
workloads W1–W6, and reports the numbers the §4 E1′ decision rule
consumes. **Numbers are reported exactly as measured; nothing is tuned
toward an outcome.**

## Reproduce

Release build, from `nucleus/v3`:

```bash
cargo build -q --release -p e1-bench

# Acceptance shape: both arms pass e1_mvcc::suite::run_all in this
# binary first, then every workload scaled down 100x:
cargo run -q --release -p e1-bench -- --verify --quick

# The real numbers (each workload group in its own process so peak RSS
# is each workload's own; see "RSS accounting" below):
cargo run -q --release -p e1-bench -- --arm a,b,c --only W1 --out w1.json
cargo run -q --release -p e1-bench -- --arm b,c   --only W2 --out w2.json
cargo run -q --release -p e1-bench -- --arm b,c   --only W3,W4 --out w34.json
cargo run -q --release -p e1-bench -- --arm b,c   --only W5 --out w5.json
cargo run -q --release -p e1-bench -- --arm b,c   --only W6 --out w6.json
```

A full run verifies first (a quick run verifies when `--verify` is
passed, as in the acceptance command): `e1_mvcc::suite::run_all` runs on
both arms in this binary, and a failing arm aborts the bench before
anything is measured.

Flags: `--quick` (scale ÷100), `--verify` (verify even when quick),
`--arm a,b,c`, `--only W1,W2,W3,W4,W5,W6`, `--out FILE`. The report is
one JSON document on stdout (hand-rolled writer, no serde).

## Methodology (card Work 3)

- Release build; **3 runs per workload**, median + min/max (each JSON
  `summary` field); nothing pinned; no CPU affinity.
- **Process CPU** per txn = (utime+stime of `/proc/self/stat`) / txns,
  measured around the worker window only (preload excluded); resolution
  `CLK_TCK` (100 Hz here).
- Hardware/software recorded in the report's `meta`: CPU model, `nproc`,
  `rustc -V`, git commit.
- Both arms share one `Config`: 8192-byte pages, the same pool sizes,
  thread counts, key space (1M 8-byte BE keys unless a workload says
  otherwise), 100-byte values, and the same PRNG seeds per
  (workload, run, thread) — arm (b) and arm (c) execute identical key
  streams. Arm (c) runs with an undo budget of **64 MiB** everywhere.
- Pool sizes: the E0 pool **preallocates** `frames × page_size` bytes.
  65,536 frames (512 MiB) for the 1M-key workloads (a 1M-version tree is
  ~15k pages; W5 grows to ~3M versions ≈ 45k pages — no eviction), and
  393,216 frames (3 GiB) for W6's ~300k-page tree. Identical on both
  arms.
- Arm (a) is driven exactly as v2's own tests drive it (the card's Read
  pointer: `ArbiterCycles::preload` / `Rig::update`): RC txns, one
  `row_op(Update)` with an always-apply EPQ per txn, `commit_submit` +
  `wait` on the spawned commit thread, the resolver running as a §9.3
  job. `bytes_applied_per_txn` is `null` for arm (a): v2 exposes no
  applied-bytes counter, and the report carries `null` rather than an
  estimate.

### RSS accounting

Peak RSS is `VmHWM` of `/proc/self/status`, reset (write `5` to
`/proc/self/clear_refs`) after each workload's preload so the peak is
the measured phase's own; the report says whether the reset worked.
Because a process's freed-but-retained allocator pages still count as
resident, the headline W5/W6 numbers come from `--only W5` / `--only W6`
invocations (one workload group per process). Running everything in one
process inflates later workloads' VmHWM by earlier workloads' retained
pages (measured: ~3 GiB retained after the quick matrix); the JSON of
the one-process run carries those inflated values as-is.

### Measurement notes

- **W1 bytes**: arm (b)'s `bytes_applied` counts the encoded tree entry
  (key + escape + terminator + ts + value tag ≈ 125 B at 8-byte
  keys/100-byte values); arm (c)'s counts logical key+value (108 B).
  The ~17 B difference is representation, not work; both are reported
  as measured, never normalized.
- **W2 reader op** = begin(RC) + one point get + abort (an autocommit
  point SELECT). Percentiles are nearest-rank over recorded samples
  (capped at 2M samples per reader; the report flags capping).
- **W3**: the "old snapshot" is one RR txn held from before the updates;
  "latest" is a fresh RC statement per scan. Verification (old reads the
  preload values, latest reads the last round's) runs untimed after the
  timed scans; a failure is reported as `values_verified: false`.
- **W4**: ratios are `tree_bytes / logical_bytes` and
  `(tree + undo + undo spill) / logical_bytes` — arm (c) keeps old
  versions in the undo log, and reporting tree bytes alone would hide
  that. The E0 tree never shrinks its page count on deletes (no page
  merging), so arm (b)'s `tree_bytes` does not drop after GC; that is
  the measured truth, not a bug in the ratio.
- **W5 sampler**: peaks are sampled every 500 ms plus the final reading;
  each sample calls `stats()`, which walks the tree on both arms
  (tens of ms at 1M–3M versions) — the sampler burns part of one core
  during W5, identically for both arms. W5 measures memory and
  correctness, not throughput.
- **W6 head-of-line**: the big txn holds every row lock from its first
  write until its commit's release, and the release itself is O(its
  keys). Probe txns (1-row updates) are stamped with the phase they
  started in — `locks` (big txn still filling) or `apply` (big commit
  running); two crews probe the two windows (a single crew parks on the
  first locked key during the fill and would never arrive during the
  apply). `head_of_line_p50/p99/max` are the percentiles over
  apply-window probe txns delayed ≥ 1 ms — an uncontended 1-row txn
  never reaches 1 ms (see W1/W2 in the same report) — and the 1-row
  txns that slip through the O(keys) release tail are counted as
  `apply_slipped_txns`, not as delayed. The mechanism (blocked through
  the apply) is pinned deterministically by
  `tests/w6_head_of_line.rs` using the suite's apply gate.

## The full table

Full-scale run (3 runs each; medians shown; the JSON carries min/max):

Hardware/software: see `meta` in each JSON —
`13th Gen Intel(R) Core(TM) i5-13500H`, 16 CPUs, rustc 1.99.0
(b940084d7 2026-09-28), git commit `<filled from the run>`.

<!-- TABLE: filled from the run below -->

## Decision rule (PLAN-v3 §4 E1′, applied verbatim)

> Pick (c) only if it passes every test **and** beats (b) by ≥ 2× CPU
> per txn on single-row writes, or ≥ 2× on reads under writers, **and**
> its undo spill keeps long readers alive with RSS within the N16
> budget. Otherwise pick (b).

<!-- RULE: filled from the run below -->
