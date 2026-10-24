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
# is each workload's own; see "RSS accounting" below). Original
# campaign (W1 a+b+c in one process, W2, W3/W4 — the W1 (b)/(c), W2 and
# W3/W4 rows below still carry it):
cargo run -q --release -p e1-bench -- --arm a,b,c --only W1 --out w1.json
cargo run -q --release -p e1-bench -- --arm b,c   --only W2 --out w2.json
cargo run -q --release -p e1-bench -- --arm b,c   --only W3,W4 --out w34.json

# Rework re-measurements (gate verdict 2026-10-22: W5/W6 pools, arm
# (a)'s stream) — the W1 (a), W5 and W6 rows below:
cargo run -q --release -p e1-bench -- --arm a   --only W1 --out w1a-rework.json
cargo run -q --release -p e1-bench -- --arm b,c --only W5 --out w5-rework.json
cargo run -q --release -p e1-bench -- --arm b,c --only W6 --out w6-rework.json
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
  65,536 frames (512 MiB) for the W1/W2/W3+W4 workloads (a 1M-version
  tree is ~15k pages); **131,072 frames (1 GiB) for W5** — 2× arm (b)'s
  measured peak tree of 65,487 pages (536,469,504 B), which the gate
  found exactly filling the old 65,536-frame pool; and **786,432 frames
  (6 GiB) for W6** — ≥ 1.2× arm (b)'s measured post-commit tree of
  5,323,112,448 B ≈ 649,794 pages, which the old 393,216-frame pool
  made (b) evict through (the gate's rework defect 1; the README
  previously — and wrongly — called that pool "~300k pages"). Identical
  on both arms. `--quick` scales the pools down with the data (÷100),
  so the acceptance shape stays no-eviction at 1/100th the size instead
  of preallocating GiBs it cannot fill.
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
  never reaches 1 ms (see W1/W2 in the same report) — and **the sample
  counts are tiny** (medians: 18 (b), 9 (c) delayed txns per run), so
  the p99 is in effect the maximum; the counts are stated in the table.
  The 1-row txns that slip through the O(keys) release tail are counted
  as `apply_slipped_txns`, not as delayed. The mechanism (blocked
  through the apply) is pinned deterministically by
  `tests/w6_head_of_line.rs` using the suite's apply gate.

## The full table

Full-scale runs, 2026-10-22, one process per workload group (`--out`
JSONs; medians of 3 runs shown — the JSONs carry min/max). **Row
provenance after the X-1b gate rework (verdict 2026-10-22):** the gate
re-measured W1/W2/W3/W4 at the original campaign's commit `d28582e8`
and found them sound (every cell within ~8%) — those rows keep the
original campaign's numbers. The rework changed three things that touch
numbers: the W5/W6 pools (defect 1), arm (a)'s key/value stream, now
identical to (b)/(c)'s by code rather than statistically (defect 3),
and the library factories (defect 2, construction-identical). W1 arm
(a), W5 and W6 were therefore **re-measured at commit `c39656fd`**
(JSONs `w1a-rework.json`, `w5-rework.json`, `w6-rework.json`, each
carrying its own passing `verify` block). Both arms passed
`e1_mvcc::suite::run_all` in this binary immediately before every
campaign group (the `verify` block of each JSON). Hardware/software
(meta of every JSON): `13th Gen Intel(R) Core(TM) i5-13500H`, 16 CPUs,
rustc 1.99.0 (b940084d7 2026-09-28), git `c39656fd` (re-measured rows)
/ `d28582e8` (original campaign), page 8192 B, pool 65,536 frames
(W1/W2/W3/W4; W5: 131,072; W6: 786,432), undo budget 64 MiB,
values 100 B, 1M preloaded keys (W3/W4: 100k; W6: 10M), 3 runs each.
Reproduce: the commands in "Reproduce" above — the original-campaign
commands at the original code, the rework re-measurements at this
commit.

### W1 — single-row write mix (µs CPU per txn; bytes applied per txn; txns/s)

100k txns/thread per configuration (the mix is the card's; the count is
the driver's, chosen for wall-clock budget — see `Config`). Arm (a)
re-measured at `c39656fd` after the stream-parity fix (defect 3); the
reference numbers moved ≤ 5% on five of eight cells and at most ±16% on
the rest (CPU/txn −10% at t=8, txns/s ±15% at t=4/8) — within the
run-to-run spread the gate measured on this reference arm (−13% at
t=4), and consistent with its "statistically equivalent" reading of the
old stream.

| threads | (a) v2 CPU/txn | (b) CPU/txn | (c) CPU/txn | (a) txns/s | (b) txns/s | (c) txns/s | (b) B/txn | (c) B/txn |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 55.7 | 4.0 | 3.8 | 28,305 | 251,482 | 264,914 | 124.07 | 108 |
| 4 | 76.6 | 9.4 | 7.9 | 20,710 | 331,927 | 486,161 | 124.07 | 108 |
| 8 | 108.1 | 15.7 | 19.5 | 17,480 | 287,277 | 300,001 | 124.07 | 108 |
| 16 | 137.1 | 69.1 | 117.0 | 14,020 | 69,887 | 67,716 | 124.07 | 108 |

((b)/(c) columns: original campaign `d28582e8`, gate-re-measured; their
code path is unchanged by the rework.) (a) has no applied-bytes counter
(null in the JSON). (b) counts encoded tree entries (~124 B), (c)
logical key+value (108 B) — see the notes above. Both spike arms are
lighter on CPU than v2 today — 13.9× (b) / 14.7× (c) at 1 thread,
falling to 2.0× (b) / 1.2× (c) at 16 threads.

### W2 — reads under writers (8 readers; ns/op p50/p99; reader ops/s)

| writers | (b) p50 | (b) p99 | (c) p50 | (c) p99 | (b) ops/s | (c) ops/s | c/b ops |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 9,058 | 30,089 | 7,337 | 28,721 | 766,882 | 905,212 | 1.18× |
| 10 | 157,688 | 328,107 | 107,765 | 256,051 | 53,942 | 74,413 | 1.38× |
| 100 | 1,518,129 | 2,351,779 | 665,443 | 3,668,136 | 5,728 | 6,440 | 1.12× |

Zero writer errors on either arm at any writer count.

### W3 — 100-row range scans at 10 versions/key (ns/scan p50/p99)

| snapshot | (b) p50 | (b) p99 | (c) p50 | (c) p99 |
|---|---:|---:|---:|---:|
| latest | 88,019 | 94,842 | 10,715 | 14,278 |
| old (held before the updates) | 87,627 | 94,305 | 146,156 | 174,737 |

`values_verified: true` on both arms and both snapshots.

### W4 — space amplification after W3 (ratios; before GC → after GC)

| arm | tree/logical | (tree+undo+spill)/logical |
|---|---|---|
| (b) | 20.11 → 20.11 | 20.11 → 20.11 |
| (c) | 2.54 → 2.54 | 14.63 → 2.54 |

(b)'s GC reclaims the old versions (versions 1.1M → 100k, 1.13 MB
freed) but the E0 tree never returns pages, so tree bytes do not move;
(c)'s undo trims completely (its tree was already minimal). Both arms
end at their pre-update page counts plus the newest-version bytes.

### W5 — long reader under 4 writers × 2M updates (64 MiB undo budget on (c))

Re-measured at `c39656fd` with the corrected 131,072-frame (1 GiB)
pool: (b)'s peak tree is 65,483 pages = 536,436,736 B — it exactly
filled (and evicted inside) the old 512 MiB pool, and now fits with 2×
headroom (`peak_tree_bytes ≤ pool` in every run). RSS grew by the
larger preallocated pool (+512 MiB on (b), +512 MiB on (c)).

| arm | peak undo B (c) / versions (b) | peak tree B | peak RSS | reader reads originals | reader get p50/p99 ns |
|---|---:|---:|---:|---|---|
| (b) | 0 / 3,000,000 | 536,436,736 | 1,874 MiB | true | 2,531 / 4,182 |
| (c) | 290,000,000 (64 MiB in memory + 226 MiB spilled) | 274,546,688 | 1,594 MiB | true | 2,905 / 6,330 |

Arm (c)'s peak-RSS cell is this campaign's median; the measured
spread across the campaign and the X-1b gate's re-run of W5 is
**1,592–1,770 MiB** — the gate's median was 1,728 MiB (+8%), its runs
2 and 3 at 1,770 MiB. The raw `--out` JSONs were never committed;
verdict `X-1b` is the record.

Both long readers stayed alive and read their original values; (c) was
never aborted (no snapshot-too-old exists in the arm), and its
in-memory undo stayed at the configured 64 MiB budget with the spill
file taking the rest.

### W6 — one txn UPDATEs 10M keys

Re-measured at `c39656fd` with the corrected 786,432-frame (6 GiB)
pool: arm (b)'s post-commit tree measured 5,318,975,488–5,323,743,232 B
= 649,289–649,871 pages, which the old 393,216-frame (3 GiB) pool made
(b) evict through its fill and commit (the gate's defect 1); it now
fits with ~20% headroom (`after_stats.tree_bytes ≤ pool` in every
run). Identically sized on both arms.

| arm | fill s | commit s | write-set B @commit | lock entries | escalations | HOL p50/p99 (delayed n) | apply-window txns | RSS |
|---|---:|---:|---:|---:|---:|---|---:|---:|
| (b) | 28.2 | 20.4 | 1,170,000,000 (117 B/key) | 1 | 1 (of 1 attempt) | 4.6 ms (unstable — see below) / 17.94 s (n=18) | 325,680 | 16.8 GiB |
| (c) | 19.8 | 23.3 | 1,170,000,000 (117 B/key) | 1 | 1 (of 1 attempt) | 4.1 ms (unstable — see below) / 21.16 s (n=9) | 437,257 | 14.4 GiB |

**Head-of-line sample counts** (gate rework defect 1): the p50/p99 are
over the *delayed* apply-window probe txns only — a median of 18 (b)
and 9 (c) samples per run (min/max 4–32 (b), 4–18 (c)); at those counts
the nearest-rank p99 **is the maximum**, so read p99 as "the longest
observed 1-row-txn delay", not a tail estimate. **The p50 does not
reproduce: it is bimodal** — the delayed samples sit either at a few ms
or at about the whole big commit — over those sample counts (delayed n:
min–max 4–32 (b), 4–18 (c) per run), so the median lands on whichever
mode a run's handful of samples favours: the round-2 gate re-run's p50
medians were 182 ms (b) and 20.8 s (c) against this campaign's 4.6 ms
and 4.1 ms. Only the p99 (= max, ≈ one commit time) is stable; read
p99/max only. The delayed few wait
out essentially the whole big commit (17.94 s vs a 20.4 s commit on
(b); 21.16 s vs 23.3 s on (c)); arrivals during the fill wait up to
48.4 s (b) / 42.7 s (c) (`locks_window_max_delayed_ns`, 2 delayed txns
each run). Hundreds of thousands of 1-row txns slip through the
O(10M)-key release tail (`apply_slipped_txns` medians: 325,676 (b) /
437,253 (c)) — the release is not free, which S-2's design should
account for. Arm (c)'s undo for the big txn sits at its 64 MiB in-memory
budget with ~1.35 GiB spilled to the spill file; arm (b) carries both
versions in the tree (20.3–20.5M versions after the commit on both
arms).

Both arms crossed the lock table's 1M-row escalation budget exactly
once and replaced their rows with the table-wide owner (`lock_entries`
reads 1 at commit).

## What the gate rework changed (verdict 2026-10-22, REWORK)

1. **W5/W6 pools (defect 1).** The old pools were exactly full for arm
   (b) — W5's tree hit 65,487 of 65,536 frames, W6's 649,794 of
   393,216 — so (b) evicted inside its measured phase while (c) fit,
   and this README wrongly described the W6 pool as "~300k pages". The
   pools are now sized over (b)'s measured peaks (W5: 131,072 frames =
   2×; W6: 786,432 frames ≈ 1.2×), identical on both arms, and every
   re-measured run shows `tree_bytes ≤ pool`. W5 and W6 were re-run in
   full (3 runs, medians above).
2. **Measurement-integrity guards (defect 2).** The arm factories moved
   into the library (`src/engines.rs`) forwarding the workload's pool
   and the shared config verbatim; the B-ABORT decision became
   `verify::{check_arm, report, report_passed}`; and
   `tests/measurement_integrity.rs` pins the five mutants the gate
   found uncaught: SEED-PER-ARM (two engine types through `w1::run` /
   `w2::run` must issue identical op streams), POOL-PER-ARM (pool 1
   must still build a working engine; the undo budget must not spill
   below its configured size), VERIFY-BYPASS (a deliberately broken
   engine must fail the decision), W5-ALIVE-OFF (a lying reader must
   be reported dead) and CPU-WINDOW-INCLUDES-PRELOAD (W1's CPU window
   covers the worker loop only). All five were applied, failed their
   named test, and were reverted.
3. **Arm (a)'s stream (defect 3).** The adapter re-seeded its RNG from
   the stream's first word, so its key stream differed from (b)/(c)'s
   while A-SAME claimed otherwise. It now consumes the same
   `rng_for(W1, run, tid, 0)` stream and the same value derivation as
   `w1`; the (a) column was re-measured (it moved ≤ 5% on five of eight
   cells, at most ±16%).

W1 (b/c), W2 and W3/W4 were **not** re-run: their method, seeds and
configurations are unchanged, and the gate re-measured them within ~8%
at the original commit.

## Decision rule (PLAN-v3 §4 E1′, applied verbatim)

> Pick (c) only if it passes every test **and** beats (b) by ≥ 2× CPU
> per txn on single-row writes, or ≥ 2× on reads under writers, **and**
> its undo spill keeps long readers alive with RSS within the N16
> budget. Otherwise pick (b).

Applied to the medians above:

1. **"(c) passes every test"** — met: `e1_mvcc::suite::run_all` passed
   on both arms in this binary before every campaign group (each JSON's
   `verify` block), and every workload verified its invariants
   (`values_verified`, `reader_reads_original_values`, zero writer
   errors).
2. **"beats (b) by ≥ 2× CPU per txn on single-row writes"** — NOT met:
   the best (c) advantage on W1 is 1.05× (1 thread, 3.8 vs 4.0 µs);
   at 16 threads (c) is 1.7× **slower** (117.0 vs 69.1 µs).
3. **"or ≥ 2× on reads under writers"** — NOT met: reader throughput at
   10 and 100 writers is 1.38× and 1.12× better. (The single number
   that does cross 2× is the p50 at 100 writers, 2.28× — alongside a
   p99 that is 1.56× worse and a 1.12× throughput gain; that is not a
   2× win on the workload, and I do not read the rule as cherry-picking
   one percentile. Judgment call, flagged for X-4.)
4. **"its undo spill keeps long readers alive with RSS within the N16
   budget"** — long readers: alive on (c), reading original values, no
   aborts; (c)'s in-memory undo stayed at its configured 64 MiB budget
   with 226 MiB spilled, peak RSS 1,594 MiB (vs (b) 1,874 MiB — both
   re-measured on the corrected no-eviction pools, each preallocating
   what its workload's pool sizes). N16's numeric budget is not yet
   fixed anywhere (it is card R-1's deliverable), so "within the N16
   budget" can only be checked against the undo budget the card names
   (64 MiB) — met. Judgment call, flagged for X-4.

Clause 2 fails and clause 3 fails, so the conjunction fails regardless
of clause 4. **The rule's "Otherwise" branch decides: pick (b).**

Context X-4 may weigh (not the rule): (c) is 8× faster on
latest-snapshot range scans and reaches 2.5× space amplification after
GC where (b) stays at 20×; (c) is 1.7× slower on contended single-row
writes and 1.7× slower on old-snapshot scans; W6 head-of-line delay is
one commit-time on both arms. X-4 (Opus) makes the final call; this
RESULT is the input.
