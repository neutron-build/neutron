# E4 recovery spike (card X-5, stage X-5a of three)

Status: **stage X-5a complete and green** — the crate, the sim, the model
and the P-written oracle (Work 0–2, including Work 2.7 / ruling X5-R5)
build, test and gate clean. This claims nothing about E4 itself: no
campaign exists until X-5c, and Opus judges E4 only on X-5c's RESULT.

## Scope: the three stages

P's ruling on X-5 run 1's ESCALATE (2026-10-23, task nv3-X-5-esc-1)
split the card in three, each stage depending on the previous, all with
the same Touch only:

- **X-5a (this stage, done):** Work 0–2 — the crate, the simulated disk
  and scheduler with the fault model, the reference model and the
  P-written oracle, and their self-tests.
- **X-5b (pending):** the S-STORE subset under the baton (including the
  store-internal-lock conversion), the round harness, the monitors and
  the directed tests (the old Work 3, 4 and 6).
- **X-5c (pending):** the seeded workload, the 10k campaign, the mutant
  sweep, the README and the RESULT that Opus judges (the old Work 5 and
  7–9).

The substrate stays spike-local for the whole chain (the card's
"Substrate" clause): `nucleus-env`'s SimEnv had not merged at launch, so
`src/sim/` is the layer the original card prescribed, and every
store-side I/O call goes through the narrow `E4Env` trait. No swap
mid-experiment.

## Implemented, tested and green (X-5a)

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
  yield point.
- **Work 1.5**: the seven sim self-tests of the card plus three extras
  (`tests/sim.rs`, 10 tests).
- **Work 2 (model + oracle, P-written, binding)**: `src/model.rs` and
  `src/oracle.rs` (O-1 (a)–(d), O-1f, O-2, O-3 (a)–(e) with the twin rule
  and X5-R1's MW `lo := T`, `acked_lost` reporting, `seed_passes`).
- **Work 2.6**: the six must-pass and seventeen must-fail oracle
  self-tests (`tests/oracle.rs`, 24 tests including the
  instrumentation-removal demo) and the five instrumentation removals.
- **Work 2.7 (X5-R5, this stage's addition)**: the exception set `R` with
  its two conditions (condition 1 — a page-file media fault on meta slot
  1 or 2 — derived by the oracle itself from the fault list, `FaultTargets`
  in `src/oracle.rs`; condition 2 `meta_rollback` harness-computed), the
  structural guard on `R`, O-3 (e) applied to amcheck (ii), and the
  completeness fix (the "every tainted referenced slot is reported" half
  now runs whenever amcheck (i) ran, whatever it reported). See the
  oracle map below for file:line.

## Carried drafts (X-5b's and X-5c's; not this stage's work)

Committed by run 1 (fabc1c0a), outside the module tree (`// pub mod
store;` in `src/lib.rs`, `workload` not declared), unchanged by this
stage and **not compiled**:

- **`src/store/` (Work 3–4)** — compiled clean when `pub mod store;` was
  enabled (see the commit history; it is unwired only because its runtime
  call pattern deadlocks, not for compile reasons): the §2.1/§2.2/§3.1
  page codec with §3.3's four checks as separate mutant sites; file
  create/open with no-fallback meta selection and G3; the pool (§5) with
  clock eviction, pins, dirty eviction to fresh slots (I-NO-INPLACE) and
  the `write_guard` epoch-copy reserve (R_EPOCH); the epoch gate (§5.6)
  with exclusive flips; the flat in-memory page map with first-touch
  image overrides standing in for the §4.1 CoW radix (the persisted radix
  is materialised at step 2 and parsed at open/amcheck — declared
  simplification); slot/LPID allocators (§4.2, §4.4–§4.5); the B+tree with
  the N3=(b) entry layout, stable roots, split and merge, the directory
  (§6.8) with bounded DropTree walks, and the GC sweep (§10.2/§10.4); the
  WAL (§12) with framing, group commit, §12.6 replay start and recycling;
  checkpoint steps 0–6 with hooks (§13, G1); recovery's two passes with
  G2 physical truncation; amcheck (3.8); the 27-mutant enum with single
  sites.
- **`src/workload.rs`** — the Work 5 round-engine draft (crash
  specification, two-pass uniform crashes, nested recovery crashes, the
  task bodies, written/acked derivation from disk events, the twin). It
  predates Work 2.7: filling the new `Extracted` fields (`meta_rollback`,
  `amcheck_i_ran`, `rollback_slots`, `image_slots_ii`,
  `tainted_image_slots_ii`) and the O-3 (a) R-taint is X-5b's harness
  work.

Not implemented anywhere yet: the monitors (M-*), the directed tests
D1–D18, the campaign (`run`/`verify`/`mutants`), the coverage quotas, and
the campaign RESULT items 1–7. `src/bin/e4-campaign.rs` and
`src/campaign.rs` are honest stubs that exit non-zero; `results/`
contains nothing. No test is `#[ignore]`d and no check weakened — the
missing parts are absent, not stubbed green.

## Status: the blocker found in attempt 2 (X-5b's input)

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
campaign, is exactly X-5b/X-5c's scope. A no-`unsafe` re-entrant world
lock cannot express it: safe Rust has no way to lend `&mut World` across
a park. This paragraph is the sizing fact X-5b budgets against; it is
kept verbatim from run 1.

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
- **O-3 (e)** is X5-R2's strict rule ("every tainted referenced slot is
  reported, and only those") with exactly one exception, ruling X5-R5
  (card Work 2.7/2.8): amcheck findings may additionally name slots of
  the harness-computed set `R` — non-tainted referenced slots written
  after the first completed `sync_all` covering a meta newer than the
  opened one, with no M-NOINPLACE hit — but only when the class is MP,
  at least one media fault of the round damaged meta slot 1 or 2 (the
  oracle checks this itself, from the fault list), and the opened meta
  is a rollback. The worker's run-1 "O-3 (e) reading" (a free-form
  stale-ref allowance) is **superseded**: it lacked conditions 1–2 and
  the M-NOINPLACE condition. X5-R5 also tightens O-3 (a): every slot in
  `R` counts as tainted from the moment open selects the rolled-back
  meta until the store next writes it (X-5b's harness supplies that as
  `served`).
- **The in-memory page map is flat** with per-entry image overrides; the
  persisted §4.1 radix is materialised at step 2 and parsed at open and by
  amcheck. MapLeaf pool residency/pinning is not exercised.
- **Per-tree sim mutex** replaces latch coupling (card 3.6); merge-only
  (no inner redistribution); no overflow chains; 4 KiB pages only.

## Fault model summary

F-CRASH at every crash; F-MEDIA-PAGE (1–3 faults) and F-MEDIA-WAL (1–2)
at the round's crash only, never on recovery crashes; taint references
survive lives. See `src/sim/disk.rs` docs.

## Oracle map (Work 2, with 2.7)

Every check of card Work 2, with its file:line (commit of stage X-5a;
`src/oracle.rs` unless noted):

| Check (card) | Where |
|---|---|
| 2.1 commit recording, `written`/`acked` flags | `src/model.rs:76` (`TxnRec`), `:136` (`mark_written`), `:143` (`mark_acked`) |
| 2.1 bulk loads, `model.scan(t, S)` | `src/model.rs:92` (`LoadRec`), `:165` (`Model::scan`) |
| 2.1 published set, drop = Delete+DropTree, rounds truncation | `src/model.rs:195` (`published_at`), `:157` (`truncate`) |
| 2.2 `lo` (`c_0 = 0`, max acked SyncOn) | `src/model.rs:214`, applied `src/oracle.rs:318` |
| 2.2 `hi` (max written) | `src/model.rs:226`, applied `src/oracle.rs:319` |
| 2.2 MW `lo := T` of the opened meta (X5-R1) | `src/oracle.rs:320`–`322` |
| O-1 candidate set `{lo} ∪ {ts ∈ H : lo < ts ≤ hi}` | `src/oracle.rs:324`–`356` |
| O-1 (a) published set equality | `src/oracle.rs:568`–`575` |
| O-1 (b) `scan(c)` equality | `src/oracle.rs:578`–`587` |
| O-1 (c) raw ts cut | `src/oracle.rs:588`–`595` (knob `no_raw_cut` `:206`) |
| O-1 (d) history equality on `samp([gc_w, c])` (X5-R4) | `src/oracle.rs:596`–`605`; `samp` at `src/model.rs:240` |
| O-1f frontier ∈ satisfying `c` (reported apart) | `src/oracle.rs:357`–`358` |
| O-2 amcheck clean in class C, (i) and (ii) | `src/oracle.rs:360`–`378` |
| O-3 (a) served corruption | `src/oracle.rs:380`–`385` (knob `no_taint` `:208`) |
| O-3 (b) XX001 only in MP/MW (detection) | `src/oracle.rs:386`–`393` |
| O-3 (c) twin false-positive rule | `src/oracle.rs:394`–`428` (knob `no_twin_rule` `:210`) |
| O-3 (d) silent `Ok` requires O-1 and O-2 | `src/oracle.rs:429`–`438` |
| O-1 lower bound `c ≥ lo` | `src/oracle.rs:326`–`331` (knob `no_lower_bound` `:204`) |
| O-3 (e) exception conditions (class, 1, 2) | `src/oracle.rs:439`–`451`; condition 1 derived from the fault list at `src/oracle.rs:240`–`267` (`FaultTargets::collect`, field `:234`; knob `no_meta_slot_cond` `:215`) |
| O-3 (e) structural guard on `R` (2.7) | `src/oracle.rs:455`–`461` |
| O-3 (e) completeness half, the completeness fix (2.7) | `src/oracle.rs:462`–`476` (gated by `amcheck_i_ran`, `Extracted` field `src/oracle.rs:154`) |
| O-3 (e) exactness half, tainted ∪ active `R` (2.7) | `src/oracle.rs:477`–`490` |
| O-3 (e) applied to amcheck (ii) (2.7) | `src/oracle.rs:491`–`515` (fields `image_slots_ii` `:166`, `tainted_image_slots_ii` `:169`) |
| X5-R5 harness-facing fields (`meta_rollback`, `rollback_slots`, …) | `src/oracle.rs:114`–`170` (`Extracted`) |
| Acked commits lost above the cut (reported, not judged) | `src/oracle.rs:521`–`529` |
| Round pass / seed pass (no DEADLOCK, no PANIC) | `src/oracle.rs:531`–`542`, `seed_passes` `:556` |

## How to reproduce

```bash
cd nucleus/v3
cargo test -q --release -p e4-recovery      # stage X-5a: sim + oracle suites (41 tests)
cargo test -q -p e4-recovery --test oracle  # the 2.6 + 2.7 self-tests (24)
cargo test -q -p e4-recovery --test sim     # the 1.5 self-tests (10)
cargo run -q --release -p e4-recovery --bin e4-campaign -- verify \
  --results spikes/e4-recovery/results/campaign.json   # exits 2: not built (X-5c)
```
