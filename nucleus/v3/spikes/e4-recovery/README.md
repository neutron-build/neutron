# E4 recovery spike (card X-5, stages 1–2 of the card's own split)

Status: **incomplete — two runner attempts were each killed at the
60-minute limit.** Attempt 1 delivered the sim and the oracle (stages 1–2,
green). Attempt 2 delivered the store as reviewed, compile-checked code
(Work 3–4) plus a draft workload driver, but could not wire them: the
store's methods yield — they park the OS thread at scheduler points —
while callers hold the world mutex, which deadlocks at runtime; converting
the call flow to internal-lock primitives is the remaining X-5b work. See
"Status" below and the card run's RESULT.

## Scope

Implemented, tested and green:

- **Work 0 (crate)**: `e4-recovery`, deps `thiserror` only (`proptest`
  dev), workspace lints, `#![deny(clippy::indexing_slicing)]` in every
  module under `src/sim/`. CRC32C (card vector passes) and the PRNG
  (splitmix64 seeding xoshiro256**) written in-crate. Determinism rules
  honoured: no wall clock, no `HashMap`/`HashSet` iteration in
  schedule-visible decisions, all namespace iteration over `BTreeMap`s.
- **Work 1 (sim)**: the baton scheduler (`src/sim/sched.rs`) — every task
  an OS thread, one baton, PRNG handoffs at every yield point, Mesa-style
  keyed waits, `DEADLOCK` and crash delivery per 1.2; the simulated disk
  (`src/sim/disk.rs`) with the 1.1 durability model, F-CRASH per-512-B-
  sector prefix resolution, the directory-op prefix rule,
  F-MEDIA-PAGE/F-MEDIA-WAL (LOST / MISDIRECTED / TORN with the ≥60 %
  referenced-slot bias), the accept hook and served-corruption detection
  (1.4); `E4Env` (`src/sim/env.rs`), the narrow I/O trait, every method a
  yield point. `nucleus-env`'s SimEnv (A-1a/A-1b) had not merged at
  launch, so this is the spike-local layer the card's "Substrate" clause
  prescribes.
- **Work 1.5**: all seven sim self-tests (`tests/sim.rs`) plus three
  extras.
- **Work 2 (model + oracle, P-written, binding)**: `src/model.rs` and
  `src/oracle.rs` (O-1 (a)–(d), O-1f, O-2, O-3 (a)–(e) with the twin rule
  and X5-R1's MW `lo := T`, `acked_lost` reporting, `seed_passes`).
- **Work 2.6**: the five must-pass and eleven must-fail oracle self-tests
  (`tests/oracle.rs`) plus the four instrumentation-removal demos.

Present and compile-checked, not wired (see "Status"):

- **Work 3–4 (`src/store/`, compiled clean when `pub mod store;` was
  enabled — see the commit history; it is unwired only because its runtime
  call pattern deadlocks, not for compile reasons)**: the §2.1/§2.2/§3.1
  page codec with §3.3's four checks as separate mutant sites; file
  create/open with no-fallback meta selection and G3; the pool (§5) with
  clock eviction, pins, dirty eviction to fresh slots (I-NO-INPLACE) and
  the `write_guard` epoch-copy reserve (R_EPOCH); the epoch gate (§5.6)
  with exclusive flips; the flat in-memory page map with first-touch image
  overrides standing in for the §4.1 CoW radix (the persisted radix is
  materialised at step 2 and parsed at open/amcheck — declared
  simplification); slot/LPID allocators (§4.2, §4.4–§4.5); the B+tree with
  the N3=(b) entry layout, stable roots, split and merge, the directory
  (§6.8) with bounded DropTree walks, and the GC sweep (§10.2/§10.4); the
  WAL (§12) with framing, group commit, §12.6 replay start and recycling;
  checkpoint steps 0–6 with hooks (§13, G1); recovery's two passes with
  G2 physical truncation; amcheck (3.8); the 27-mutant enum with single
  sites.
- **`src/workload.rs`**: the Work 5 round-engine draft (crash
  specification, two-pass uniform crashes, nested recovery crashes, the
  task bodies, written/acked derivation from disk events, the twin) — a
  draft that does not compile until the store conversion lands; kept as
  the X-5c starting point.

Not implemented: the monitors (M-*), the directed tests D1–D18, the
campaign (`run`/`verify`/`mutants`), the coverage quotas, and the campaign
RESULT items 1–7. `src/bin/e4-campaign.rs` and `src/campaign.rs` are
honest stubs that exit non-zero; `results/` contains nothing. No test is
`#[ignore]`d and no check weakened — the missing parts are absent, not
stubbed green.

## Status: the blocker found in attempt 2

The baton design (Work 1.2) serialises tasks through one world mutex, and
the card requires yields *inside* store operations (every `E4Env` call,
every `write_guard`). Those two facts conflict in the straightforward
implementation: a store method called under a held world guard parks its
thread inside the guard at its first yield and the baton can never pass —
a deadlock, in every store path. The fix is architectural, not local:
store operations must be restructured so each atomic step locks the world,
works, and unlocks (owned page bytes travel between steps — the pattern
`src/store/prims.rs` already follows internally), and every multi-step
operation (tree descent/split, checkpoint, recovery, GC) must hold its
intermediate state in owned locals while re-borrowing the store between
yields. That conversion, plus the workload, monitors, directed tests and
campaign, is exactly the card's X-5b/X-5c split (card Note "Size"). A
no-`unsafe` re-entrant world lock cannot express it: safe Rust has no way
to lend `&mut World` across a park.

## Declared simplifications so far

- **Sector size 512 B** for tearing; **directory-op prefix rule** models
  ext4's ordered metadata journal on n5000 (both per the card).
- **Taint gaps**: bytes never issued to cannot be tainted; §3.3 still
  rejects stray bytes there (see `src/sim/disk.rs` docs).
- **F-CRASH knob mapping**: per sector, `p_keep_latest` takes the latest
  write; else with `p_tear` a uniform earlier prefix endpoint; else the
  durable value. Directory prefixes: all with `p_keep_latest`, else a
  uniform prefix length.
- **MW MISDIRECTED** targets another 512-aligned offset of the same
  segment.
- **O-3 (e) reading** (X5-R2 "and only for those"): findings must cover
  every tainted referenced slot and may additionally name slots whose
  §3.3 failure is a direct consequence of a lost-meta fault (the opened
  image older than writes issued after its meta synced — the D10 shape).
- **The in-memory page map is flat** with per-entry image overrides; the
  persisted §4.1 radix is materialised at step 2 and parsed at open and by
  amcheck. MapLeaf pool residency/pinning is not exercised.
- **Per-tree sim mutex** replaces latch coupling (card 3.6); merge-only
  (no inner redistribution); no overflow chains; 4 KiB pages only.

## Fault model summary

F-CRASH at every crash; F-MEDIA-PAGE (1–3 faults) and F-MEDIA-WAL (1–2)
at the round's crash only, never on recovery crashes; taint references
survive lives. See `src/sim/disk.rs` docs.

## How to reproduce

```bash
cd nucleus/v3
cargo test -q --release -p e4-recovery      # stages 1–2: sim + oracle suites
cargo test -q -p e4-recovery                # same, debug
cargo run -q --release -p e4-recovery --bin e4-campaign -- verify \
  --results spikes/e4-recovery/results/campaign.json   # exits 2: not built
```

