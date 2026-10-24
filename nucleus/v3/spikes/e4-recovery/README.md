# E4 recovery spike (card X-5, stage 1–2 of the card's own split)

Status: **incomplete — the run was killed at the runner's 60-minute limit
during stage 3 (the store).** What is here is verified and passing; what is
missing is listed under "Scope" below. The card's Note ("Size") names the
split for exactly this case; see the RESULT block of the card run.

## Scope

Implemented and tested:

- **Work 0 (crate)**: `e4-recovery`, deps `thiserror` only (`proptest`
  dev), workspace lints, `#![deny(clippy::indexing_slicing)]` in every
  module under `src/sim/` (none exist under `src/store/` yet — the store
  has not landed). CRC32C (table-driven; card vector
  `"123456789" → 0xE3069283` passes) and the PRNG (splitmix64 seeding
  xoshiro256**) are written in-crate. Determinism rules honoured: no wall
  clock, no `HashMap`/`HashSet` iteration in schedule-visible decisions,
  all namespace iteration over `BTreeMap`s.
- **Work 1 (sim)**: the baton scheduler (`src/sim/sched.rs`) — every task
  an OS thread, one baton, PRNG handoffs at every yield point, Mesa-style
  keyed waits, `DEADLOCK` and crash delivery per 1.2; the simulated disk
  (`src/sim/disk.rs`) with the 1.1 durability model (durable bytes +
  length, volatile writes in issue order, per-range taint references kept
  across lives), F-CRASH per-512-B-sector prefix resolution, the
  directory-op prefix rule, F-MEDIA-PAGE/F-MEDIA-WAL (LOST / MISDIRECTED /
  TORN with the ≥60 % referenced-slot bias), the accept hook and
  served-corruption detection (1.4); `E4Env` (`src/sim/env.rs`), the
  narrow trait every store-side I/O call will go through, each method a
  yield point. The `nucleus-env` SimEnv (A-1a/A-1b) had not merged at
  launch, so this is the spike-local layer the card's "Substrate" clause
  prescribes, shaped for a mechanical swap.
- **Work 1.5**: all seven sim self-tests (`tests/sim.rs`), plus three
  extras (armed-crash delivery, env-call crash, media LOST pre-image).
- **Work 2 (model + oracle, P-written, binding)**: `src/model.rs` (commits
  in ts order with `written`/`acked`, bulk loads at `S_L`, catalog = tree
  1, `scan(t, S)`, round truncation, `lo`/`hi` bounds, the `samp` rule)
  and `src/oracle.rs` (O-1 (a)–(d) with candidate `c` search, O-1f, O-2,
  O-3 (a)–(e) with the twin rule and X5-R1's MW `lo := T`, `acked_lost`
  reporting, `seed_passes`). The `OracleCfg` knobs exist only for the
  instrumentation-removal demonstrations.
- **Work 2.6**: the five must-pass and eleven must-fail oracle self-tests
  (`tests/oracle.rs`), plus the four instrumentation-removal
  demonstrations (O-1 lower bound, O-1 (c), taint, twin rule) in
  `instrumentation_removals_are_load_bearing`.

Not implemented (the run died here):

- **Work 3–4**: the store (`src/store/`), WAL, checkpoint, recovery,
  amcheck, GC, monitors (M-*), the mutant enum and its sites.
- **Work 6**: the directed tests D1–D18.
- **Work 5, 7, 8**: workload, campaign `run`/`verify`, mutant sweep.
- **Work 9**: the full README (design-to-code map, fault-model README
  items, reproduction) and the campaign RESULT items 1–7.

`src/bin/e4-campaign.rs` and `src/campaign.rs` are honest stubs that exit
non-zero and say so; `results/` contains nothing. No test is `#[ignore]`d
and no check weakened — the missing parts are absent, not stubbed green.

## Declared simplifications so far

- **Sector size 512 B** for tearing (pessimistic for NVMe's usual 4 KiB
  atomic unit) — per the card.
- **Directory-op prefix rule** models ext4's ordered metadata journal on
  n5000 — per the card's stated assumption.
- **Taint gaps**: bytes never issued to cannot be tainted (nothing to
  differ from); §3.3 still rejects stray bytes there because they cannot
  match a reference (see `src/sim/disk.rs` module docs).
- **F-CRASH knob mapping** (the card prescribes the knob values, not their
  mapping): per sector, with `p_keep_latest` the sector takes its latest
  volatile write; otherwise with `p_tear` a uniform earlier prefix
  endpoint; otherwise the durable value. Directory prefixes: all ops with
  `p_keep_latest`, else a uniform prefix length. A volatile length change
  persists exactly as far as the surviving data requires.
- **MW MISDIRECTED** targets another 512-aligned offset of the same
  segment (within `/wal/`, per the card; cross-segment targeting not
  implemented).
- **O-3 (e) reading** (X5-R2, "and only for those"): amcheck findings must
  cover every tainted referenced slot, and may additionally name slots
  whose §3.3 failure is a direct consequence of a lost-meta fault (the
  opened image older than writes issued after its meta was synced — the
  D10 shape). The `stale_ref_slots` set records these. This reading keeps
  the check strict against genuine false positives while not failing
  legitimate detection; it is a judgement call and is flagged as one in
  the RESULT.

## Fault model summary

F-CRASH at every crash; F-MEDIA-PAGE (class MP, 1–3 durable page-file
writes still current) and F-MEDIA-WAL (class MW, 1–2) at the round's
crash only, never on recovery crashes; taint references survive lives.
See `src/sim/disk.rs` docs for the exact semantics.

## How to reproduce

```bash
cd nucleus/v3
cargo test -q --release -p e4-recovery      # self-tests + oracle tests
cargo test -q -p e4-recovery                # same, debug
cargo run -q --release -p e4-recovery --bin e4-campaign -- verify \
  --results spikes/e4-recovery/results/campaign.json   # exits non-zero: not built
```
