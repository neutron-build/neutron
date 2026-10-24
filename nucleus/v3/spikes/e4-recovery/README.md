# E4 recovery spike (card X-5, stage X-5a of three)

Status: **stage X-5a complete and green, including the gate's 2026-10-23
REWORK round and its 2026-10-24 round-2 REWORK** — the crate, the sim,
the model and the P-written oracle (Work 0–2, Work 2.7 / ruling X5-R5,
the six gate-REWORK fixes and the two round-2 fixes below) build, test
and gate clean. This claims nothing about E4 itself: no campaign exists
until X-5c, and Opus judges E4 only on X-5c's RESULT.

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

## Gate REWORK fixes (2026-10-23 verdict, all six applied)

The P gate's verdict on stage X-5a (commit 1ca7ac34) was REWORK with six
items; all are fixed, each pinned by a new test that fails when the fix
is reverted (verified mutant-style, recorded in the stage RESULT):

1. **Taint over mid-batch reads.** `Inode::taint_ranges`
   (`src/sim/disk.rs:277`) now includes the issued interval that starts
   before the read's offset; run 1's `issued.range(off..end)` walk
   skipped it, blinding O-3 (a) to every read that begins inside an
   issued range (and `range_tainted`). Pin:
   `taint_covers_read_starting_inside_issued_range`.
2. **The accept hook judges sub-ranges.** `SimDisk::accepted`
   (`src/sim/disk.rs:629`) judges the accepted range against the most
   recent read that **covers** it, at sub-range granularity (`ReadRec`
   carries `tainted_ranges`); run 1 matched only an exact `(off, len)`
   read, which the WAL reader shape (read 16 KiB, accept one record)
   could never hit, and scanned all earlier reads instead of the most
   recent one. Pins: `accept_hook_judges_subrange_of_covering_read`,
   `accept_hook_subrange_taint_only`. (Extended per-byte and made to
   fail closed by round 2, item B, below.)
3. **Loads are versions at `S_L`.** `Model::scan` (`src/model.rs:178`)
   applies a load's entries at their `S_L`: a Put or Delete at
   `ts > S_L` on a loaded key wins above it, and the load shows between
   `S_L` and that op. Run 1 applied loads after every commit op, so the
   model agreed with a store that silently lost post-load updates. Pin:
   `model_load_entries_are_versions_at_s_l`.
4. **The (ii) completeness half is pinned.** Must-fail 18
   (`mf18_ii_missed_detection_fails`): amcheck (ii) ran, a slot in
   `tainted_image_slots_ii` is not reported, everything else passes. The
   gate's G2 mutant (removing that half) was UNCAUGHT at verdict time;
   it is caught now.
5. **O-3 (c) fails closed.** A missing twin in an MP/MW round that has
   XX001 fails the round as a harness defect (`src/oracle.rs:460`); run 1
   skipped the false-positive rule, failing open. Pin:
   `mf19_missing_twin_fails`.
6. **Sim/oracle minutiae.** (a) a synced shrink survives the crash:
   `apply_crash` truncates `durable` to `durable_len`
   (`src/sim/disk.rs:740`), pin `synced_shrink_survives_crash`;
   (b) a MISDIRECTED page fault targets its landing slot as well as its
   source (`FaultTargets::collect`, `src/oracle.rs:263`), pin
   `misdirected_landing_slot_is_a_fault_target`; (c) F-MEDIA-WAL draws
   1–2 candidates uniformly over **every** `/wal/` segment
   (`src/sim/disk.rs:820`), pin `media_wal_draws_across_all_segments`
   (run 1 faulted only the lowest-id segment with candidates, so the
   active segment was never hit).

The verdict's re-gate mutant table (R1–R5, G1–G4) was re-applied in
full; every mutant is caught by its named test, including G2.

## Gate REWORK round 2 fixes (2026-10-24 verdict, both applied)

The P gate's round-2 verdict on the rework (commit ab1b4714) was REWORK
with two items, both in files this stage still owns (X-5b freezes
`src/oracle.rs` and limits `src/sim/` to named additions); both are
fixed, each pinned by a new test that fails when the fix is reverted
(verified mutant-style, recorded in the stage RESULT):

- **A. A WAL MISDIRECTED's landing range is an O-3 (c) target.**
  `FaultTargets::collect` (`src/oracle.rs:278`) now also pushes
  `(inode, to_off, len)` for WAL MISDIRECTED faults, matching the
  page-file landing-slot fix of round 1 (item 6b). Run 1 collected only
  the source range, so in MW a detection caused by the stray bytes
  landing inside a range the twin's recovery read was reported as a
  false positive — failing closed, but spuriously, seed after seed.
  Pin: `mw_misdirected_landing_range_is_a_fault_target`.
- **B. The accept hook fails closed.** `SimDisk::accepted`
  (`src/sim/disk.rs:629`) judges each accepted byte against the most
  recent read that covers **that byte** (walking the reads most-recent
  first), so a record reassembled across two batch reads is judged
  (the gate probe: two tainted reads `[0,4096)` and `[4096,8192)`,
  `accepted(f, 4000, 200)` gave `served = 0`). Accepted bytes no read of
  the round covered were accepted unread — a harness or store defect —
  and fail closed: recorded in `served` (so O-3 (a) fails the round
  whatever the harness does) and in `uncovered_accepts`
  (`src/sim/disk.rs:376`, for diagnosis). Pins:
  `accept_hook_judges_across_reassembly_reads`,
  `accept_hook_fails_closed_without_covering_read`.

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
- **Work 1.5**: the seven sim self-tests of the card, three extras, the
  five gate-REWORK pins and the two round-2 pins (`tests/sim.rs`, 17
  tests).
- **Work 2 (model + oracle, P-written, binding)**: `src/model.rs` and
  `src/oracle.rs` (O-1 (a)–(d), O-1f, O-2, O-3 (a)–(e) with the twin rule
  and X5-R1's MW `lo := T`, `acked_lost` reporting, `seed_passes`).
- **Work 2.6**: the six must-pass and nineteen must-fail oracle
  self-tests (must-fail 18–19 are the gate-REWORK additions) plus the
  model, MISDIRECTED-landing-slot and MW-landing-range pins
  (`tests/oracle.rs`, 29 tests including the instrumentation-removal
  demo) and the five instrumentation removals.
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
  segment; F-MEDIA-WAL candidates are pooled over every `/wal/` segment
  and drawn uniformly (gate REWORK item 6c).
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

## Oracle map (Work 2, with 2.7 and the gate REWORK)

Every check of card Work 2, with its file:line (stage X-5a plus the
2026-10-23 REWORK round and its 2026-10-24 round 2; `src/oracle.rs`
unless noted):

| Check (card) | Where |
|---|---|
| 2.1 commit recording, `written`/`acked` flags | `src/model.rs:79` (`TxnRec`), `:139` (`mark_written`), `:146` (`mark_acked`) |
| 2.1 bulk loads, `model.scan(t, S)` — loads are versions at `S_L` (REWORK 3) | `src/model.rs:95` (`LoadRec`), `:178` (`Model::scan`) |
| 2.1 published set, drop = Delete+DropTree, rounds truncation | `src/model.rs:219` (`published_at`), `:160` (`truncate`) |
| 2.2 `lo` (`c_0 = 0`, max acked SyncOn) | `src/model.rs:238`, applied `src/oracle.rs:345` |
| 2.2 `hi` (max written) | `src/model.rs:250`, applied `src/oracle.rs:346` |
| 2.2 MW `lo := T` of the opened meta (X5-R1) | `src/oracle.rs:347`–`349` |
| O-1 candidate set `{lo} ∪ {ts ∈ H : lo < ts ≤ hi}` | `src/oracle.rs:351`–`365` |
| O-1 (a) published set equality | `src/oracle.rs:605`–`612` |
| O-1 (b) `scan(c)` equality | `src/oracle.rs:618`–`624` |
| O-1 (c) raw ts cut | `src/oracle.rs:625`–`633` (knob `no_raw_cut` `:210`) |
| O-1 (d) history equality on `samp([gc_w, c])` (X5-R4) | `src/oracle.rs:634`–`642`; `samp` at `src/model.rs:264` |
| O-1f frontier ∈ satisfying `c` (reported apart) | `src/oracle.rs:385` |
| O-2 amcheck clean in class C, (i) and (ii) | `src/oracle.rs:387`–`405` |
| O-3 (a) served corruption | `src/oracle.rs:410` (knob `no_taint` `:212`); the sim side is `SimDisk::accepted` (`src/sim/disk.rs:629`, fail-closed per REWORK round 2 B, with `uncovered_accepts` `:376`) |
| O-3 (b) XX001 only in MP/MW (detection) | `src/oracle.rs:413`–`417` |
| O-3 (c) twin false-positive rule; MISDIRECTED landing slot (REWORK 6b) and landing range (REWORK round 2 A) are targets, and a missing twin fails closed (REWORK 5) | `src/oracle.rs:420`–`464` (landing slot `:257`–`265`, landing range `:278`–`286`, missing twin `:460`–`464`; knob `no_twin_rule` `:214`) |
| O-3 (d) silent `Ok` requires O-1 and O-2 | `src/oracle.rs:466`–`474` |
| O-1 lower bound `c ≥ lo` | `src/oracle.rs:354`–`362` (knob `no_lower_bound` `:208`) |
| O-3 (e) exception conditions (class, 1, 2) | `src/oracle.rs:476`–`487`; condition 1 derived from the fault list at `src/oracle.rs:249`–`291` (`FaultTargets::collect`, field `:243`; knob `no_meta_slot_cond` `:219`) |
| O-3 (e) structural guard on `R` (2.7) | `src/oracle.rs:492`–`498` |
| O-3 (e) completeness half, the completeness fix (2.7) | `src/oracle.rs:499`–`512` (gated by `amcheck_i_ran`, `Extracted` field `src/oracle.rs:158`; pinned for (ii) by must-fail 18, REWORK 4) |
| O-3 (e) exactness half, tainted ∪ active `R` (2.7) | `src/oracle.rs:514`–`527` |
| O-3 (e) applied to amcheck (ii) (2.7) | `src/oracle.rs:530`–`555` (fields `image_slots_ii` `:170`, `tainted_image_slots_ii` `:173`) |
| X5-R5 harness-facing fields (`meta_rollback`, `rollback_slots`, …) | `src/oracle.rs:118`–`175` (`Extracted`) |
| Acked commits lost above the cut (reported, not judged) | `src/oracle.rs:558`–`566` |
| Round pass / seed pass (no DEADLOCK, no PANIC) | `src/oracle.rs:568`–`589`, `seed_passes` `:593` |

## How to reproduce

```bash
cd nucleus/v3
cargo test -q --release -p e4-recovery      # stage X-5a: sim + oracle suites (53 tests)
cargo test -q -p e4-recovery --test oracle  # the 2.6 + 2.7 + REWORK self-tests (29)
cargo test -q -p e4-recovery --test sim     # the 1.5 self-tests + REWORK pins (17)
cargo run -q --release -p e4-recovery --bin e4-campaign -- verify \
  --results spikes/e4-recovery/results/campaign.json   # exits 2: not built (X-5c)
```
