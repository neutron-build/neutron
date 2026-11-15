Card: A-1b
Class: impl
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session 2e1bb48b-11c2-417d-8c23-589c0f942688 (T3 Code review sub-agent, independent P gate of A-1b)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: a19ceb07de8db664fdc429726a075a8724880dd0
Mutants: R3b -> taint_across_lives, N1 -> taint_covers_read_starting_inside_issued_range, N2a -> accept_hook_judges_subrange_of_covering_read, N2b -> accept_hook_subrange_taint_only, N2c -> accept_hook_judges_subrange_of_covering_read, N5 -> synced_shrink_survives_crash, N7 -> media_wal_draws_across_all_segments, B1 -> accept_hook_judges_across_reassembly_reads, B2 -> accept_hook_fails_closed_without_covering_read, B3 -> accept_hook_judges_older_tainted_read, B4 -> accept_hook_fails_closed_without_covering_read, B5 -> accept_hook_judges_across_reassembly_reads, DIRSYNC-NOOP -> sim_segment_ack_substrate (power_cut_loses_unsynced_create passes under it), CREATE-DURABLE -> power_cut_loses_unsynced_create, RENAME-DURABLE -> power_cut_loses_unsynced_rename, SETLEN-DURABLE -> power_cut_loses_unsynced_set_len, OP-BEFORE-YIELD -> env_calls_yield_and_crash, ERR-AS-CRASH -> sim_open_modes, SIM-SHARED-AS-EXCLUSIVE -> sim_locks_shared_readers_coexist, SLOT-LITERAL -> sim_meta_slot_media_faults, SCHED-UNSEEDED -> sim_seeds_differ, SIM-HANDLE-BY-PATH -> sim_handle_survives_remove_and_recreate + sim_handle_survives_rename, SIM-UNDER-FILE-INVALID -> sim_kind_mismatches, SIM-QUERY-INVALID -> sim_sync_dir_root_and_subdir, SIM-EMPTY-WRITE-TO-DISK -> sim_io_edges, STD-READ-NEAR-MAX -> std_io_edges, MEM-HUGE-WRITE-UNCHECKED -> mem_io_edges, SIM-HUGE-TO-DISK -> sim_io_edges, STD-LOCK-CONVERTS -> std_lock_mode_on_same_handle, MEM-RELOCK-EXCLUSIVE-LOCKED -> mem_lock_mode_on_same_handle, O1 MEM-UNLOCK-BY-NAME -> mem_lock_moves_with_rename_until_drop, O5 MEM-RENAME-DIR-NAMES-TARGET -> mem_kind_mismatches, G1 SYNC-DATA-NOOP -> sim_segment_ack_substrate, G2 SYNC-SWAPPED -> power_cut_loses_unsynced_set_len + sim_segment_ack_substrate, G3 RENAME-SYNCS-DIR -> power_cut_loses_unsynced_rename + sim_segment_ack_substrate, G4 MISDIRECT-WRONG-FILE -> sim_truncation_write_loss_restores_stale_record + sim_segment_end_write_takes_post_sync_faults + x5f4f7_heal_media_restores_the_no_fault_bytes, G5 SCHED-SEED-FIRST-ONLY -> r14_unwind_host_steps_run_in_taskid_order (sim_seeds_differ passes under it), G6 MEDIA-STALE-SAME-OFFSET -> sim_media_wal_hits_truncation_write, G7 LOCKS-SURVIVE-LIFE -> sim_power_cut_releases_locks, G8 RENAME-STEP-THEN-YIELD UNCAUGHT (A-1b-F1), G9 READ-BY-PATH -> sim_handle_survives_remove_and_recreate + sim_handle_survives_rename, G10 CRASH-IS-POWER-CUT -> sim_process_crash_keeps_page_cache
Followups: A-1b-F1, A-1b-F2, A-1b-F3, A-1b-F4

## Gate run

Ran the scaffold's `fresh-gate.sh` once, on the full commit id. It was extracted through a `--no-local --depth 1` clone of nucleus/v3-scaffold (scaffold id 760a74b255d9284ed50f0438cb157e719a9b3ea9), with CARGO_TARGET_DIR=~/.cache/nv3-target/gate-A-1b. Stdout and stderr were captured separately; stderr was empty. The literal last line is `GATE EXIT=0`:

```
fresh-gate: commit a19ceb07de8db664fdc429726a075a8724880dd0
fresh-gate: base 760a74b255d9284ed50f0438cb157e719a9b3ea9
fresh-gate: checks passed (4810 files hashed in 4.16 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests EXIT=0
GATE EXIT=0
```

Before the run:
- The branch tip was exactly a19ceb07…, with no verdict commit on top.
- The worktree was clean.
- The merge-base with nucleus/v3-scaffold was d026f95d.

## Scope and limits

- `git diff d026f95d..a19ceb07 --stat`: 16 files, all under `crates/nucleus-env/` and all on the Touch only list, +6366/−29. No protected path; `spikes/`, `error.rs` and `path.rs` byte-unchanged.
- I re-ran the card's Acceptance lines 2–4 myself, read-only, in the worktree: all three exit 0, and the worktree stayed clean.
- **Work 0 hunks against Limit 4**, each read:
  - `env.rs` is doc-only.
  - `lib.rs` has Work 1's lines plus the E-IO and E-LOCK doc sentences.
  - `mem.rs` changes the bodies of `write_at`, `set_len` and `lock_exclusive`, plus the private `end_past_i64_max`.
  - `std_env.rs` changes the bodies of `read_at`, `write_at`, `set_len`, `lock_exclusive`, `lock_shared` and `unlock`, plus the private `held` field and its initialisation. Three extras sit beyond the letter of Limit 4 but are needed by the field: three private `HELD_*` consts, the `AtomicU8` import and one sentence in `StdFile`'s doc. I accept them; the consts sit oddly between two `use` lines.
  - `tests/common/` and the two test binaries match Work 0 exactly.
- **Cargo.toml and the `sim` feature** are Work 1 verbatim. The feature gates `pub mod rng` and `pub mod sim`, so default builds, wasm32 included, compile neither (STEP wasm32 EXIT=0). The self dev-dependency is sound.
- **Lint posture.**
  - `unsafe_code` is forbidden workspace-wide.
  - No new or copied non-test code uses `unwrap`, `expect`, `panic!` or `unreachable!`, and every module keeps `deny(clippy::indexing_slicing)`.
  - Two arithmetic panics are reachable through the new public API; see Finding 6.
- **Test time (debug):** `tests/sim.rs` 0.23 s, `tests/sim_env.rs` 0.67 s (35 tests), `tests/sim_heal.rs` under 0.01 s. E4's slow-binary warning does not apply.

## Copy check (review point 1)

I diffed every copied file against `git show 48ac9920:nucleus/v3/spikes/e4-recovery/…` myself, after stripping only the leading `// ` provenance lines, and read every hunk.
- **`src/rng.rs`, `src/sim/sched.rs`:** byte-identical. Their first lines are exactly the card's.
- **`tests/sim_heal.rs`:** byte-identical to `tests/directed.rs` lines 3994–4149 after `e4_recovery` → `nucleus_env`, which occurs once in that range. The header is `// Provenance…`, then the `//!` line, then `// The spike stays…`. The card wants the `//!` line directly before the source; harmless and cosmetic.
- **`src/sim/disk.rs`:** 19 hunks, all inside D1 (source lines 858–1052) or D2 (`1139a`).
  - Every `4096` in `durable_candidates`, `draw_media_kind` and `media_faults_common` became `slot`, and both let-chains keep the short circuit, so `None` draws no extra `rng` value. With `slot = 4096` and `"/pages"` every draw is E4's.
  - D2 is the card's text verbatim.
  - One unlisted edit: `durable_candidates`' doc line (source 920) gains "(`slot`, D1)". It is doc-only, inside D1's range, and describes D1. Accepted.
- **`tests/sim.rs`:** exactly E1 (16–17, rustfmt's reflow of the import), E2 (395–434), E3 (452, the card's text verbatim) and E4 (956–1002). E4 removes both surrounding blank lines; rustfmt collapses a double blank, so that is the only form inside the range. 26 tests remain, and the only names gone are `env_calls_yield_and_crash` and `r_taint_window_semantics`.
- **`src/sim/mod.rs`, rewritten:**
  - `HasDisk` is verbatim from E4 `env.rs` 37–41.
  - The re-exports are E4's from `disk` and `sched`, plus the unaliased `PAGE_FILE`/`ROOT_DIR`/`WAL_DIR` (E4 exported them through `env.rs`) and the card's new items.
  - The S-DET…S-LIVE text is E4's lines 5–26 with one word changed ("every `E4Env` call" → "every SimEnv call"), although its own provenance sentence says the text is kept. Cosmetic.
- **`src/sim/env.rs`, rewritten:** its public surface is exactly the card's API list, plus the `FileId` re-export E4 also had.

**Result: the copies are copies.** Every difference is a provenance block, D1, D2, one of E1–E4, or one of the three cosmetic items above.

## Mutation evidence

Every mutant ran on a scratch `git archive` copy of a19ceb07 (~/.cache/nv3-review/gate-A-1b/scratch, with its own git; target dir gate-A-1b-scratch). For each mutant, a script asserted that every edit anchor occurs exactly once, ran the named tests (my own mutants ran the whole `sim`, `sim_env` and `sim_heal` binaries), told a build error apart from a catch, and reverted with `git checkout` in that scratch repository only. `git diff --quiet` held before every mutant and at the end. The card's worktree was never edited.
- **The 32 card mutants are all caught.**
- **Mutant 13 (DIRSYNC-NOOP) is not caught by the card's first named test.** `power_cut_loses_unsynced_create`'s synced arm passes under it, because a stale `Create` op replayed onto a binding `sync_dir` already made durable changes nothing. Its second named test catches it, `sim_segment_ack_substrate` as the worker strengthened it (a19ceb07): the stale `Create(old.log)` re-binds the recycled name. The worker reported this, and I confirm it.
- **Ten of my own, aimed at the riskiest logic:**
  - G1 SYNC-DATA-NOOP: a synced write is lost at the cut.
  - G2 SYNC-SWAPPED: the layer swaps `sync_data` and `sync_all`.
  - G3 RENAME-SYNCS-DIR: an unsynced rename survives.
  - G4 MISDIRECT-WRONG-FILE: the landing goes to the next inode, or nowhere.
  - G5 SCHED-SEED-FIRST-ONLY: the schedule ignores the seed after the first pick.
  - G6 MEDIA-STALE-SAME-OFFSET: a superseded same-offset write is a fault target.
  - G7 LOCKS-SURVIVE-LIFE: the lock table is carried across a crash.
  - G8 RENAME-STEP-THEN-YIELD: yield-then-step is broken for a namespace op.
  - G9 READ-BY-PATH: E-HANDLE is broken on `read_at`.
  - G10 CRASH-IS-POWER-CUT: every crashed life resolves F-CRASH.

  Nine are caught. **G8 is UNCAUGHT**, and **G5 survives `sim_seeds_differ`** (Finding 5).

## What was checked and holds

- **The trait (review point 2).**
  - **E-HANDLE:** every `SimFile` method and the lock table use the `FileId` that `open` resolved, and nothing re-resolves the path (G9 and SIM-HANDLE-BY-PATH fail).
  - **E-KIND and the query/creation rule:** in the order the card gives.
  - **E-IO:** short reads only at the end, never a zero-fill past it, the empty-write exception, and the `i64::MAX` bound before `SIM_MAX_LEN`, both in the layer.
  - **E-LOCK:** the same-handle rule, and a fresh table per life.
  - **Errors:** disk errors map to A-1a variants and never to `Crashed`. E4's `step` did map them to `Crashed` (`f(&mut g).map_err(|_| SimFail::Crashed)`).
  - **Yield then step:** every Env and File method yields except `now`, `rand`, `accepted` and `Drop`. `Read` and E-PATH refusals come before the yield.
  - All 19 shared checks run on SimEnv; the list matches every `pub fn` in `tests/common/`.
- **Crash semantics against E-DURABLE and S-STORE §12.5.**
  - A process crash keeps the page cache (unsynced writes, creates and renames). G10 fails.
  - The power cut loses unsynced data per 512-B sector, a per-directory suffix of unsynced ops, and an unsynced or `sync_data`-only `set_len`. `sync_dir` makes the whole directory durable and nothing else. `sync_data` makes data, plus the length it needs, durable.
  - Directory ops are dropped per directory independently, which is more adversarial than ext4's single journal, never less.
  - Two exceptions: the `wal` directory flag is durable at once (the card's stated limit, Finding 7) and the copied length bugs (Findings 3–4).
- **Determinism.** A 3-task body with a crash, a mixed-knob power cut, renames, `set_len`, `sleep` and `rand` (yielding first) gave byte-identical digests over 12 seeds in three separate processes.
- **The worker's ESCALATE item does not reproduce.** It claimed MemEnv answers `rename("a", "page/x")` with `Ok`. On a19ceb07, MemEnv, StdEnv and SimEnv all give `InvalidPath { "a -> page/x", "cross-directory rename" }`, as they do for `rename(wal/a, wal/a/x)`. A target under a file always lies in another directory, and every backend refuses a cross-directory rename first. Nothing to escalate.

## Findings

1. **S-DET hole in the new layer: calls made before a task first holds the baton (medium; A-1b-F1).**
   - `run_life` starts each body at once. `rand` (gated by `acquire`, which refuses a non-holder) silently answers 0. `now` and `wake` take `Sched::lock` with no gate at all.
   - So a body whose first Env call is `rand()` gets timing-dependent values. Probe: 3 tasks, seed 42, 300 lives, each task's first call `rand()`. That gave **6 distinct draw vectors**, e.g. `[0,0,0,0]` and `[0,8089139879624522491,0,1532002565339369849]`.
   - This breaks the card's own S-ENV "Streams" promise ("the same seed and body give the same … disk") and S-DET. It also reverses X5-R14's rule that a host step without the baton is loud, never a silent fall-through.
   - The card's tests miss it because every body yields first. The worker knew ("a task's first scheduler call must be a yielding one") but recorded it only in the RESULT, nowhere in S-ENV.
   - A store that draws its `creation_uuid` before its first file op would get non-reproducible seeds.
2. **Liveness hole in the new layer: `sleep` in a failed world (medium; A-1b-F1).**
   - `sleep` swallows a failed yield and does not advance the clock, and `now` cannot fail. A crashed task in `while env.now() < deadline { env.sleep(…) }` therefore spins forever, and `run_life` never returns.
   - Probe: crash armed at the first `sleep`; the life had not ended after 10 s, a hang. In a failed world, yields return `Crashed` without counting, so S-LIVE's budget never trips.
   - This breaks S-LIVE's "never a hang", which `mod.rs` claims to own.
3. **Copied E4 defect: a synced shrink then a synced grow brings back pre-shrink bytes after a power cut (medium; A-1b-F2).**
   - `SimDisk::sync_all` sets `durable_len` but never truncates `durable`, so bytes past a synced shrink stay in the durable image.
   - Probe: write 1024×0xAA, `sync_all`, `set_len(100)`, `sync_all`, `set_len(1000)`, `sync_all`, power cut. Reads before the cut show zeros in `[100,1000)`; after the cut, all **900** bytes are 0xAA again.
   - A related case: a synced shrink to 100, then an unsynced 10-byte write at 200 that the cut keeps. Afterwards 100 stale bytes show in `[100,200)` and 302 past 210.
   - That loses what E-DURABLE promises (a `sync_all`'d zero-extension), and it is outside E4's declared simplifications. The README's "Declared simplifications so far" lists the 512-B sector, the directory-op prefix, the knob mapping, MW's same-segment misdirect and the O-3 (e) rule; none covers lengths.
   - N5's fix covered only shrink-then-crash.
   - It is latent for today's spec: segments are preallocated and recycled by rename only (§12.1), and the page file only grows (§4.5).
   - But §14 step 5 says only "Truncate the WAL at the valid end". A store that truncates by `set_len` and then re-preallocates would see truncated records come back in class C, which is exactly the premise S-1-F6 relies on.
   - The card's copy rule rightly forbids the fix here.
4. **Copied E4 defect: the surviving length is rounded up to a sector (low; A-1b-F2).**
   - `apply_crash` sets `durable_len` to the end of the last kept sector, not to the end of the written data.
   - Probe: 10 bytes, unsynced, kept by the cut, on an empty dir-synced file. The length after the cut is 512.
   - No real filesystem shows a length nothing set; a store that checks lengths could see impossible files. Also undeclared.
5. **Test gaps (low; A-1b-F1).**
   - **G8:** a `rename` that runs before its yield, so a call that returned `Crashed` took effect, passes everything. Yield-then-step is pinned for `write_at` only, although `yield_step` is shared and a per-method special case is the likely regression.
   - **G5:** `sim_seeds_differ` asks only that 2 of 8 traces differ, so a scheduler that uses the seed for its first pick alone passes it, and passes all of `sim_env`. Only E4's `r14_unwind_host_steps_run_in_taskid_order` catches it, and only incidentally.
   - **The card text** names a first test for mutant 13 that cannot catch it, above. Its `Some(1)` for `env_calls_yield_and_crash` was rightly made `Some(2)`, since `open` is a yield point.
6. **Panics reachable through the new public API (low; A-1b-F1).**
   - `Sim::media_page(_, 0, _)` panics with a division by zero at the copied `w.off % slot`. D1 made the divisor a parameter, and nothing validates it.
   - `SimFile::accepted(off, len)` passes `off + len` unchecked into the copied `SimDisk::accepted`. That overflows in debug while the world is held.
   - Both are harness-side misuse, but the rule is no panic in non-test code.
7. **Durable beyond the trait: the `wal` flag (stated limit) and the clock (note; A-1b-F3).**
   - `create_dir("wal")` is durable with no `sync_dir("")`, so SimEnv cannot catch a store that omits that sync. The card calls the limit "stronger than E-DURABLE, never weaker". For a fault simulator that is the weaker direction: the store's creation sequence must be pinned some other way.
   - Likewise the virtual clock survives lives, unlike any real restart. A store that persisted `now()` and compared it after a crash would pass here.
8. **Rulings on Work 0 (review point 4).**
   - **F1 (StdEnv read cap):** correct. `off + count ≤ i64::MAX` holds for every pread, so Linux's `rw_verify_area` never sees an overflow.
   - **F2, the length bound:** `InvalidInput` past `i64::MAX` on all three backends, after the `Read` refusal and the empty-write no-op. MemEnv checks before `lock_state` and grows through `try_reserve`, so it cannot poison.
   - **`SIM_MAX_LEN` = 1 GiB in the layer: approved.** It bounds every `resize` the copied code does, keeps `disk.rs` E4's, and is 16× the default segment. It caps length, not memory; E4's `volatile`/`dwrites` histories grow with the write count, which is fine for tests.
   - **No cap on MemEnv: approved.** It is a test backend. On wasm32, where nothing overcommits, `try_reserve` fails cleanly. Under Linux overcommit a huge but plausible growth can pass `try_reserve` and then be OOM-killed while zero-filling. That is not a panic, and capping an in-memory store's size is not this card's call. The doc should say so (A-1b-F4).
   - **Nit (A-1b-F4):** on 32-bit targets MemEnv's `offset_to_index` refuses an end above `usize::MAX` but below `i64::MAX` with `Io { kind: Other }`. The new E-IO text names `FileTooLarge` or `OutOfMemory`.
   - **F3 (re-lock):** correct on all three backends. StdFile never asks flock to convert. One race remains: two threads on one `StdFile` calling `lock_exclusive` and `lock_shared` concurrently both read `held` as none, and the second flock converts the first. The field's doc claims "a handle shared across threads serialises on the flock itself", which is false for one descriptor. No caller does this today (A-1b-F4).
   - **F4:** unlock after rename, and the path in `io_kind_is`, are both correct. O1 and O5 now fail.
9. **Fidelity against E4 (review point 3).**
   - **Task id per thread:** loud (`Halted`) wherever it could differ. It hides nothing, but a store must run every thread as a life task.
   - **Errors, not crashes:** removes a false-crash class E4 itself recorded. It does create one new hiding place: a `Halted` that store code swallows, from a baton refusal or an `Internal`, leaves no trace in `LifeOutcome`, whereas E4's harness recorded these as PANIC-class (A-1b-F3).
   - **Yield on every call:** more crash points, never fewer. But read-only calls now take up crash points that a harness sweeping uniformly over yields would otherwise put on writes (A-1b-F3).
   - None of the three makes a crash test pass that E4 would fail, apart from Findings 1–2 (lost reproducibility, and a hang).
10. **Process anomalies, reported verbatim, not defects of the code.**
    - The runner log says `opencode zai-coding-plan/glm-5.3`, while the card's Notes say "a Fable 5.2 or Opus 5.5 delegate implements this card".
    - Attempt 1 was `killed after 60 min`.
    - The worker ran `scripts/gate.sh` three times, against Limit 7 and "Do not … Run scripts/gate.sh more than once", and ran `scripts/test-enforcement.sh` directly.
    - Its RESULT reports `GATE EXIT=1` from two runner-e2e rows, K and BG. The runner records `[A-1b] PASS: cd nucleus/v3 && bash scripts/gate.sh`, and this verdict relies on the fresh gate above, where enforcement-tests is EXIT=0.

**API stability (review point 7).** Nothing here forces a breaking change, but four things should be settled early:
- **F1's fix shifts yield numbering.** It adds one park per task, so it must land before later tests hard-code `crash_at_yield`.
- **`WaitKey`** is E4's fixed enum. A-6/A-7 should not build on its variants until A-8a decides.
- **The accept hook** is unreachable through the trait. A-8a should add it additively, as an extension trait or a defaulted method.
- **Twins:** `Sim` has no clone or fork, so twins need an added constructor.

## Follow-ups

- **A-1b-F1 (fold into A-6b as its Work 0, before any A-6b test runs on SimEnv; A-6b's gate re-runs the probes below): the SimEnv layer's determinism, liveness and pins.**
  - (a) `run_life` parks each task until the baton first reaches it, before its body runs. Then `rand`, `now`, `wake` and `accepted` never act before the task's first baton. State the rule in S-ENV, and pin it with a rand-first multi-task body that gives identical results for one seed over repeated lives.
  - (b) In a failed world, `sleep` still advances the clock by `d`, so time-bounded loops end. Pin it with a deadline loop after an armed crash, under a watchdog. S-ENV should say that a task observes a crash only through a fallible call.
  - (c) Extend `env_calls_yield_and_crash` to every mutating method: `open(CreateNew)`, `create_dir`, `rename`, `remove`, `set_len`, `sync_data`, `sync_all`, `sync_dir`, `lock_*` and `unlock`. G8 must then fail.
  - (d) Give `sim_seeds_differ`'s body enough handoffs that seeds must differ past the first pick. G5 must then fail.
  - (e) `media_page` refuses `slot == 0` with `Io { InvalidInput }`, and `accepted` refuses an overflowing `off + len`.
  - (f) `LifeOutcome` records every `Halted` the layer raised (baton, internal, off-task), so a body that swallows one still fails the life.
- **A-1b-F2 (fold into A-6b, or into the first card whose SimEnv test shrinks a file, at the latest before A-7d's step 5): SimDisk length fidelity, as a named `disk.rs` departure D3 with its provenance line.**
  - (a) A synced shrink makes the bytes past the new length unreadable for good, so a later grow reads zeros. This covers `sync_all`, and the sector image `apply_crash` composes from `durable`.
  - (b) The length a power cut keeps is the end of the surviving written data, not the end of its sector. Alternatively, declare (b) in the README and S-ENV with the store-side consequence.
  - Pin both with the two scenarios in Findings 3–4, and re-run X-5's simulator mutants (rows 1–12) after the change.
  - If A-8a's re-sync with X-5c-F4-F7-F1 changes `disk.rs` first, D3 is applied on top of it.
- **A-1b-F3 (fold into A-8a): what the harness port needs from this crate.**
  - A reachable accept hook (above).
  - The S-BATON discipline for store code, and whether `WaitKey` gains a store-defined variant: a departure in `sched.rs`.
  - A way to fork a `Sim` for twins.
  - `LifeOutcome`'s halt record, if F1(f) has not landed yet.
  - Crash points weighted to, or swept over, mutating calls.
  - `LifeOutcome::trace` is capped at 4096 handoffs, so determinism digests need the disk too.
  - Every dependent declares `nucleus-env = { …, features = ["sim"] }` in its own `[dev-dependencies]`. Under `cargo test --workspace` the self dev-dependency unifies `sim` into every member's build, so a missing declaration only shows under `-p`.
  - The `wal` flag and clock limits of Finding 7. Whichever card writes the store's creation sequence pins its `sync_dir("")` directly.
  - E4's stale `wal_names` doc link (source line 1155) and the cosmetic header items under Copy check.
- **A-1b-F4 (fold into D-5a, which already reworks the backends' edges for OpfsEnv, or into the first card that touches `mem.rs`/`std_env.rs`): Work 0 backend nits.**
  - MemEnv's 32-bit `offset_to_index` refusal becomes `FileTooLarge`.
  - MemEnv's doc gets the overcommit caveat.
  - StdFile's `held` either becomes a compare-and-swap through a pending state, or its doc stops claiming that threads serialise on the flock.
