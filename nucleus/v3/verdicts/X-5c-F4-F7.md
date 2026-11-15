Card: X-5c-F4-F7
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (b446fa14-5471-4946-8ccd-310af50a6487, 2026-11-14)
Date: 2026-11-14
Gate-exit: GATE EXIT=0
Commit: 48ac99203f97d9dd5a53b1370ed141a294c4df0c
Mutants: M-TWIN-NO-HEAL -> x5f4f7_twin_reseed_heals_17948 (twin 2 pass=false), M-HEAL-NO-LANDING -> x5f4f7_twin_reseed_heals_17948 and x5f4f7_heal_media_restores_the_no_fault_bytes, M-HEAL-REAL-CHAIN -> x5f4f7_twin_reseed_heals_17948 (digest 165328067) and x5f4f7_damaged_start_digests_unmoved (2399 digest 1307406164), M-HEAL-REVERSED -> x5f4f7_twin_reseed_heals_17948, x5f4f7_damaged_start_digests_unmoved and x5f4f7_heal_media_restores_the_no_fault_bytes, M-NO-SELFCHECK -> UNCAUGHT (whole crate suite), M-HEAL-ALWAYS -> UNCAUGHT (whole crate suite; the gate's scratch probe D catches it), M-CLEAR-HISTORY -> UNCAUGHT (behaviour-neutral: 17948's digest is unchanged)
Followups: X-5c-F4-F7-F1

**Where this verdict lives.** The request named this file and made every branch read-only for the gate. So GATE-PROMPT step 7's commit to `nucleus/v3/verdicts/X-5c-F4-F7.md` was **not** made. The orchestrator may commit this file as the branch verdict.

## The verdict in one paragraph

The card does what the owner's ruling of 2026-11-14 and P's diagnostics §A.3 and §B.4 ask:
- **The heal.** `SimDisk::heal_media` restores, at every still-live damaged range of a re-seeded twin's **cloned** disk, the in-order composition of the file's synced writes. It is called only from `reseeded_twin_chain`, which borrows the real chain immutably and is reached only from the two re-seed arms.
- **The self-check.** A range still tainted after the heal, or a poisoned cloned store, sets `rec.defect`: loud, never a pass.
- **The table.** It differs from the frozen one in exactly the two `rlen` values (3 bytes) and hashes to `a05534c7…0fdea`. The matcher's logic is unchanged.
- **The evidence.** Main reproduces byte for byte. Held-out differs from the unfixed file in seed 17948 only (its digest and twin 2's `pass`/`o1`), and all three of 17948's twins pass. Every real-round record in both sets is unchanged. 38 of 38 sampled seeds reproduce at HEAD.
- **Gate run and mutants.** The fresh gate reads `GATE EXIT=0`. M-TWIN-NO-HEAL and three of the gate's own mutants are caught.

**Follow-ups, none blocking E4.** Two safeguards have no pin (M-NO-SELFCHECK and M-HEAL-ALWAYS are UNCAUGHT). The self-check can raise a false loud defect when the last write issued to a fault range was dropped by F-CRASH. Stray bytes landing on never-written bytes are invisible to the heal and to the self-check. None of these occurs in the E4 data: no defect anywhere, and every live range was healed clean, measured. All three are follow-up X-5c-F4-F7-F1, to land before the harness is reused (A-9).

## 1. Fresh gate (GATE-PROMPT step 1)

- **The extract.** Shallow `--no-local` clone of `nucleus/v3-scaffold`, scaffold id **`187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`**. No `EXTRACT FAILED` line. `$fg` was `/tmp/tmp.mZfIIYVdPV`, removed afterwards.
- **The run.** `cd ~/Workspace/neutron-v3 && PATH=$HOME/.cargo/bin:$HOME/.local/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-X-5c-F4-F7 bash "$fg/fresh-gate.sh" 48ac99203f97d9dd5a53b1370ed141a294c4df0c`, 21:01:31 to 21:12:20, rc=0, stderr empty (0 bytes).
- **Its lines:**
```
fresh-gate: commit 48ac99203f97d9dd5a53b1370ed141a294c4df0c
fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b
fresh-gate: checks passed (4541 files hashed in 2.92 s)
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
- **Literal last line:** `GATE EXIT=0`.
- **The runner's line 8** (`scripts/gate.sh`) also passed.
- **Debug test time.** The `tests/workload.rs` binary ran 98 tests in 259.70 s, on a host loaded by another session (load 24–31).

## 2. Scope and Limits (GATE-PROMPT step 2)

**Diff.** `git diff 754c6536...HEAD` lists 7 files, +559/−17, every one in Touch only. There are four commits, each with the `nucleus v3: X-5c-F4-F7` prefix. The worktree is clean, and HEAD and the branch ref are both `48ac9920…`.

**The worker attempt.** Its log has 60 shell commands and no campaign-class command. The only `run --set|verify|seeded-faults` match is a `grep` of `gate.sh`. Its mutant ran on a scratch export.

**Limit 1. The heal acts only on a re-seeded twin's clone.**
- `heal_media` has one caller in `src/`: `reseeded_twin_chain(ch: &Chain, …)`, which heals `ch.disk.clone()`.
- `reseeded_twin_chain` has exactly the two re-seed call sites (`workload.rs:2294` and `:2320`).
- The real chain is borrowed immutably. The real round's retry clone (`ch_retry`) is taken from the unhealed disk.
- The removed `src/` lines are only the two old `Chain { disk: ch.disk.clone(), … }` literals, which `reseeded_twin_chain` now builds identically plus the heal, and one rewritten comment line.
- Every other twin-chain build comes from the twin's own end disk, and twins run `MediaDraw::None`. So no other route lets real-chain media damage into a twin.

**Limit 4. `src/sim/disk.rs` gains only the heal.**
- The diff is +93/−0: a module-doc paragraph and the method. No accessor was added, and no existing SimDisk line changed.
- No other `src/` file but `workload.rs` changed.
- There is no `unsafe`, `unwrap`, `expect` or `panic` in the additions; the slicing is `get`-based under `deny(clippy::indexing_slicing)`. `heal_media` is documented.

**Limit 2. The table and the matcher.**
- **Table.** Base `55ae47347b77…1158` (equal to T's) becomes HEAD `a05534c7a34c1daa243eb2f2dc23f5f0776abe6c84af8c205af393d00e10fdea`. A line-by-line check finds exactly two changed lines:
  - line 22 (`refused_old_page`): `{"eq": 0}` → `{"ge": 0}`;
  - line 58 (`refused_old_header`): `{"ge": 1}` → `{"ge": 0}`.

  `cmp` counts 3 bytes. Class 3's `"rlen": {"eq": 0}` at line 94 is unchanged.
- **The table matches P's earlier what-if copy.** It is byte-identical to P's `diag-E4-final/whatif/e4-class-table.amended.json`.
- **Matcher.** Its non-comment lines differ from the base only in:
  - `TABLE_SHA256`;
  - `unmatched-ge-below`'s literal (`rlen 0` → `rlen -1`);
  - the two new cases `page-any-rlen` and `header-any-rlen`.

  No matching logic changed. The two added cases go a word past Limit 2's "changes only … any self-test literal". They are the cases §B.4 names, and P's own option (b) counted "three self-test edits", so this gate reads them as in scope.
- `self-test` at HEAD: `SELF-TEST 132 cases, 0 failed`, with an empty stderr.

**Limit 3.** No pin failed. No main digest moved. No held-out digest but 17948's moved (§4).

## 3. Correctness of the heal (GATE-PROMPT step 3)

**SimDisk's model, as read at HEAD.**
- `dwrites` records only writes a sync graduated, in issue order.
- F-CRASH resolutions (`apply_crash` → `write_durable`) and media faults (`record_media` → `write_both`) change the durable bytes without touching `dwrites`. A faulted write stays in `dwrites` with its intended bytes.
- Taint compares the durable bytes with the **last issued** bytes, including writes F-CRASH dropped.

**Restoring the no-fault bytes, case by case:**

| Case | Result |
|---|---|
| LOST, and TORN's own range | Fully covered by the faulted synced write. The composition is the no-fault value. ✓ |
| MISDIRECTED landing | Covered bytes are restored ✓. Covered-but-never-issued bytes cannot occur: a synced write is issued. Uncovered bytes keep their content (see C) |
| Order | Composition in `dwrites` order equals sync order equals issue order within a file. So the latest synced write wins ✓ (M-HEAL-REVERSED is caught) |
| Overlapping faults | The composition does not depend on processing order, and every range is re-checked at the end ✓ |
| A fault later overwritten by a good write | The range is not live, so it is skipped. A partial overwrite is composed correctly ✓ (the unit pin's `disk2`, gate probe D) |
| Ranges past EOF | Landings fall inside `durable_len` (page: slot ≤ slots−1 when slots ≥ 3; WAL: `to_off ≤ dlen − len`). The heal never extends a file ✓ |

**What the gate's scratch probes measured.** `tests/gate_probe.rs` lives on a scratch export. Each probe compares against a no-fault disk built with the identical history minus the fault.
- **A. A dropped rewrite.** A later life rewrites the faulted slot unsynced, and the crash drops it.
  - The heal leaves exactly the no-fault bytes (`healed_equals_no_fault=true`).
  - The self-check still returns the range: it is tainted against the dropped write, as the no-fault disk is too (`no_fault_disk_tainted_there=true`).
  - So this is a **false loud seed defect**. It can never pass a seed.
  - The worker's RESULT names this as a judgment call ("a range whose live difference is crash-caused is also restored…"). It follows the card's self-check text verbatim.
- **B. A crash-persisted partial rewrite.** The composition ignores F-CRASH-persisted sectors, so it reverts one (`healed=0x22`, `no_fault=0x33`). **The self-check catches it** (loud).
- **C. A misdirect landing on a never-written slot.** Nothing was ever issued there, so the range is not tainted. The heal leaves the stray bytes (`0x44` against `0x0`), and the self-check passes.
  - P's census uses the same `range_tainted` test, so it has the same blind spot.
  - It is inert in this harness: the slot allocator reads only in-memory sets (`prims.rs` `slot_free`), recovery reads only image-referenced slots and the meta slots, and the WAL syncs with `sync_data`, so a segment's durable length is its issued extent.
- **D. A crash-persisted full rewrite.** The range is not live, and the heal leaves it alone ✓. Under M-HEAL-ALWAYS the heal reverts it, and the self-check trips. So the live check matters, and no committed test pins it.

**At every measured re-seed in the data.** A stderr-only instrumented scratch build covered 19 re-seed events: the 6 damaged starts and 12 damage-free re-seeds (16809 re-seeds twice).
- `volatile_inodes=[]` and `cur_ne_durable_inodes=[]` every time. So the heal's write to the current view is exact here too.
- The live ranges are exactly P's 11:
  - 17948: `0/16384`, `0/327680`, `0/499712`;
  - 2399: `2/47616+4566`;
  - 5359: `2/0+206`, `2/32768+206`;
  - 15249: `7/7232+1517`, `7/10240+25234`;
  - 18119: `2/2746+14793`;
  - 18689: `4/0+22044`, `4/17920+22044`.
- The 12 damage-free re-seeds had none, so the heal was a no-op there (Limit 1).
- All 18 records still equal the files'.

**Can the heal mask real damage?**
- **It cannot touch the real chain.** The borrow is immutable; M-HEAL-REAL-CHAIN, which forces the leak, is caught by both seed pins.
- **The one route to a real round** is Work 2.5's own: the twin's supply (O-3 (c)'s sets) and its nested crash points. A healed twin supplies a clean recovery's sets, which is what Work 2.5 defines.
- **In the data no real round moved.** Main is byte-identical. In held-out only `twin[2]` and the digest of 17948 changed, and its real round 2 is still the same refused open with `pass=true`.
- **A twin observes damage only as a failure.** For a class-C twin, `check_round` never reads the fault targets: O-3 (c)/(e), `r_active_r6` and R32 are all MP/MW-gated, and twin records are `(pass, o1)` only.

**`disk.media` on the clone: kept.**
- It is documented in the README and pinned at `heal_media` level (the unit pin asserts `disk.media.len() == 3` and the three kinds).
- The README's argument is right: the only reader of the history beyond the round's own faults, `prior_page_slots`, is used only inside class MP's R32 arm.
- M-CLEAR-HISTORY (clear it in `reseeded_twin_chain`) passes both seed pins, with 17948's digest unchanged. So the choice is behaviour-neutral for E4. The pin fixes `heal_media`'s behaviour, not the re-seed's.

**The poison half of the self-check** never fired in the data (`DEFECT_SEEDS=0` in both fresh runs). The real chain continues only after a passing round, with the store cloned after the harness checkpoint.

## 4. Evidence (GATE-PROMPT step 5)

**Provenance.**
- The runlogs copies are byte-identical (`cmp`) to the runner's own outputs in `~/.cache/nv3-target/X-5c-F4-F7/x5f4f7/` (json, out, err and guard, for both sets). The ledger run `c14632b0…` runs from `754c6536` to `48ac9920`.
- The runner's binary (18:40:43) predates the `src/` commit (18:44:33). The worker built it from the then-uncommitted tree, and the runner's line-4 `cargo build` found nothing newer. It contains the self-check string. The decisive check is below: the gate's own HEAD-export binaries reproduce the files' records.

**The files.**
- **Main.** `cb947c19…ae45` is byte-identical to the final run's file of record (whose `.prov` agrees). `.out` and stderr are identical apart from the progress lines.
- **Held-out.** The fresh `f572e1da…6a` against the final run's `e655a61b…621a`: the same 10,000 seeds in the same order, the same top-level keys, and **`MOVED [17948]`**, with exactly three fields:
  - `digest` 2914550144 → 2541279735;
  - `twin[2].pass` false → true;
  - `twin[2].o1` (the O-1(a) text) → null.

  17948's twins read `[true, true, true]` and `defect: null`. Its rounds 0–2 are unchanged, and round 2 is still `c=null`, `frontier=0`, `pass=true`.
- **Held-out `.out`.** Only the fold line differs: `FAILING_SEEDS=15 … TWIN_FAILING_ROUNDS=1` becomes `FAILING_SEEDS=14 … TWIN_FAILING_ROUNDS=0`. The census line `JOINT_ARM_CENSUS set=held-out seeds=10000 arm_rounds=3 stale_reads=0 arm_seeds=[10669,13469,16669]` is unchanged.

**The gate's sample.** Each seed ran alone (`run --seeds s..s+1 --threads 1`), and its whole record was compared, with every file's sha checked first. Binaries:
- main: an export of HEAD;
- held-out: HEAD plus the probe bound lifted, at FINAL;
- MP: HEAD plus the probe bound plus `MP_PRESSURE`.

The plan was drawn by `random.Random(202611147)` (`gate-X-5c-F4-F7/sample/plan.json`).

| Set (compared against) | Seeds | Equal |
|---|---|---|
| main (fresh = record) | 2399, 5359; damage-free re-seeds 839, 1199, 3359, 3429, 8029, 9599; random MP/MW 449, 3549, 5188, 7158, 9199, 9527 | 14 / 14 |
| held-out (fresh) | 17948, 15249, 18119, 18689; damage-free re-seeds 10999, 13039, 14879, 16809, 16819, 19859; random MP/MW 10148, 12429, 13839, 14759, 14859, 17509 | 16 / 16 |
| held-out (unfixed `e655a61b`) | the same 16 | 15 / 16; only 17948 differs (`digest`, `twin`) |
| MP-pressure (final `75d719b8`) | 26018, 26958; random 22438, 23097, 23108, 24578, 29628, 29837 | 8 / 8 |

All 38 processes exited 0 with an empty stderr. The digests: 17948 2541279735; 2399 642331466; 5359 1958172881; 15249 914096547; 18119 3724720501; 18689 4019746642.

## 5. The two VOID guards, ruled

The re-check runs read `foreign_pct=19.42` and `26.79` (`verdict=VOID`), because another session's builds loaded the host. **That does not matter for what they are used for.**
- **They are correctness re-checks, compared by digest.** Every seed is a pure function of `(seed, mutant)` (W-DET), and nothing in a record reads a clock.
- **The main re-run proves it end to end.** Run under the same load, it is byte-identical to the file of record.
- **No timing claim rests on them.** Box, wall and CPU evidence stays with the final run's three CLEAN-guard runs. Held-out's rows are INFO, and its wall time is not a judged input of this card.
- **VOID would matter** only if either file were offered as timing evidence. Neither is.

## 6. Mutants (GATE-PROMPT step 4)

**How they ran.** Each mutant ran on the scratch export `gate-X-5c-F4-F7/mut/`, target `…/s-mut`, applied by an exact-count replacement (`scripts/mutate.py`). The tests were `cargo test --release -p e4-recovery --test workload x5f4f7` and `--test directed x5f4f7`, plus the whole crate suite for the two that survived. A clean control passed first.

**Reverted.** `diff -r exp-clean mut` is empty after the last mutant. The worktree was never touched, and its `git status` is empty.

| Mutant | Change | Result |
|---|---|---|
| **M-TWIN-NO-HEAL** (card) | `let still = disk.heal_media();` becomes an empty `Vec` (step 1 skipped) | **caught**: `seed 17948: every twin must pass under the heal … [TwinRound { pass: true, o1_fail: None }, TwinRound { pass: true, o1_fail: None }, TwinRound { pass: false, o1_fail: Some("O-1(a) at c=25: published {} != model {2, 3, 4}") }]`, identical to the README's message. The five-seed pin still passes, consistent with P's "never observed" |
| **M-HEAL-NO-LANDING** (gate) | a misdirect's landing is neither healed nor self-checked | **caught**: the 17948 pin (twin 2 `pass=false`) and the unit pin (`directed.rs:4094`, byte equality) |
| **M-HEAL-REAL-CHAIN** (gate) | `ch.disk.heal_media()` on the real chain at both re-seed arms | **caught**: 17948 digest 165328067 ≠ 2541279735; 2399 digest 1307406164 ≠ 642331466 |
| **M-HEAL-REVERSED** (gate) | compose `dwrites` in reverse, so stale bytes win | **caught**: 17948 twin 2 fails; 2399's digest moves (2132617514); the unit pin reports 4 ranges still tainted |
| **M-NO-SELFCHECK** (gate) | the still-tainted check never sets `rec.defect` | **UNCAUGHT** by the whole crate suite (workload 98 passed, all binaries green) |
| **M-HEAL-ALWAYS** (gate) | the live check is dropped, so every fault range is re-composed | **UNCAUGHT** by the whole crate suite; the gate's scratch probe D fails under it |
| **M-CLEAR-HISTORY** (gate) | `disk.media.clear()` after the heal | **UNCAUGHT**, as expected: both seed pins pass with 17948's digest unchanged, which confirms the README's behaviour-neutral claim |

## Findings

1. **No defect in the work.** It implements the card and P's §A.3 as written. Every Acceptance line passed, and the fresh gate passed.
2. **The self-check's two safeguards are unpinned** (M-NO-SELFCHECK and M-HEAL-ALWAYS are UNCAUGHT).
   - The defect path never runs in any test.
   - The unit pin's not-live case uses a synced rewrite, where re-composing changes nothing. Probe D shows the case where the live check matters.
   - Neither safeguard is exercised by the E4 data: no defect occurred, and every live range healed clean. → X-5c-F4-F7-F1.
3. **The self-check can raise a false loud defect** (probe A).
   - It measures taint against the last *issued* bytes, so a fault range whose last issued write F-CRASH dropped stays "tainted" even when its bytes are exactly the no-fault bytes.
   - It fails closed, never open.
   - Probe B shows the same check is load-bearing, catching the heal's one real inexactness (synced-writes-only composition). → X-5c-F4-F7-F1.
4. **Bytes never issued are invisible** (probe C), to the heal, to its self-check and to P's census alike. The census's "damage-free at start" means no damaged *issued* byte. This is inert in this harness (§3), but must be re-argued for the engine (A-9). → X-5c-F4-F7-F1.
5. **README wording.** Record item 7 says "the heal implements P's pre-registered fix (§A.3)".
   - §A.3 was written after the final run's data.
   - The heal is a post-data harness fix, ruled by the owner, whose effect ("only 17948 moves") was predicted before the re-check and then confirmed.
   - The card's own count ("post-data amendments: one", the table) stands as the card defines it. The E4 judgement names the harness fix plainly. → fold into X-5c-F4-F7-F1.
6. **The debug test budget.** The workload binary took 259.70 s in debug on a loaded host (earlier: 167 s and 201 s). The five-seed pin adds weight. This continues the gap recorded in the final verdict §8 item 6; it is not a finding against this card.

## Follow-up

### X-5c-F4-F7-F1: harden the twin re-seed heal before the E4 harness is reused (A-9). Test plus harness, P gate. Not an E4 input.

**Work.**
- **Pin the self-check's defect path.** A directed test re-seeds from a disk whose fault range cannot be healed, and asserts `rec.defect` (M-NO-SELFCHECK must fail it).
- **Pin the live check.** Add probe D's case, a fault range later re-covered by an F-CRASH-persisted write: the heal must leave it byte-identical (M-HEAL-ALWAYS must fail it).
- **Fix the false defect.** Make the self-check judge against the no-fault reference rather than last-issued taint, or otherwise exempt dropped-write taint (probe A).
- **Include F-CRASH-persisted sectors.** Either compose them, or keep the self-check catching them (probe B), and say which.
- **Resolve never-issued landings.** Restore them to the pre-fault bytes, which `MediaFault` would need to record, or argue inertness for the engine port (probe C).
- **Correct README record item 7's "pre-registered".**

**Gate.** It re-runs the six damaged-start pins and must find every E4 digest unmoved, or explain any move before anything is reused. Scratch probes A–D are in `~/.cache/nv3-review/gate-X-5c-F4-F7/probe/…/tests/gate_probe.rs`.

## What this gate did not do

- No campaign set, `verify`, `seeded-faults`, `--probe` or stress loop. Every run was a single seed per process, or a test.
- No verdict commit, no branch write, no merge, no push.
- No write to the card, QUEUE, PLAN, `runlogs/` or `~/.cache/nv3-target/X-5c-F4*/`; that directory was only read, with `cmp`.
- The 86 damage-free re-seeds were not all replayed under the heal: 12 were, plus all 6 damaged starts. The fresh full-set runs are the evidence for the rest.
- Theoretical gaps not chased: byte-coincidence edge cases of the self-check, and the tiny-page-file landing past EOF (fewer than 3 slots, which does not arise for the store's page file).

## Files (under `~/.cache/nv3-review/gate-X-5c-F4-F7/`)

- **Fresh gate:** `fresh-gate.{out,err,rc,start}`.
- **Exports:** `exp-clean/` (HEAD), `exp-heldout/` and `exp-mp/` (with `exp-*.diff`), `mut/` (restored), `probe/` (`tests/gate_probe.rs`, `examples/gate_rows.rs`), `instr/` (`instr.diff`) and `match-head/`.
- **Builds:** `~/.cache/nv3-target/gate-X-5c-F4-F7/s-*`.
- **Sample:** `sample/` (`plan.json`, `REPRO.txt`, per-seed outputs) and `instr-runs/`.
- **Mutants:** `mutants/<name>/` (`mutant.diff`, test outputs), `mutants/run.log`, `run2.log` and `final-restore.diff` (empty).
- **Scripts:** `scripts/` (`replay.sh`, `cmp_records.py`, `cmp_sets.py`, `mutate.py`, `run-mutants.sh`).
- **Other:** `probe.out`, `probe-heal-always.out`, `rows.*.txt`, `f6-pin.out`, `selftest.out`, and `table.{base,head}.json`.
