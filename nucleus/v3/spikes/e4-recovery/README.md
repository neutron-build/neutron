# E4 recovery spike (card X-5, stage X-5c of three)

Status: **stage X-5c — X5-R13 (a)–(d) landed (each pinned); the gc_step
livelock fixed (pin `gc_sweep_crosses_subtree_boundaries`); the first
complete 10k campaign committed (exit 0, ~13 min, RSS 5.1 GiB; see
"The first complete 10k campaign" below): `E4 SEEDS=10000 O1_FAIL=2712
O2_FAIL=98 O3_FAIL=1029 DEADLOCK=46 PANIC=0 O1F_FAIL=2722 MONITOR=0`,
1,560 failing seeds in six recorded classes, five coverage quotas
missed (Q4, Q7, Q11, Q12, SIZING) — **X5-R12's budget valve applied:
the classes are recorded with deterministic repros, the file is
committed, and the retune/final-run/mutant-sweep wait for P's
triage.** `cargo test` green (139 tests); the gate green at this code
state.** The stages before it (X-5a, X-5b) are complete and merged.
Opus judges E4 on the RESULT with P's rulings X5-R1-R7 (and X5-R9's sim
fix).

## P's rulings, verbatim (X-5c-esc-1, 2026-10-31; binding)

Copied verbatim from the card before any campaign output was read
(X5-R8). P-commit c1d1ab40 delivered X5-R6, X5-R7 and X5-R9's oracle /
sim halves; the worker delivered X5-R10 and X5-R11 under X5-R12's
order.

**X5-R6 (O-1 when recovery returns `Ok` alongside an XX001).** Applies
to a class MP or MW round where `res` is `Ok` and at least one XX001
occurred. That includes an XX001 raised by the store on one of the
oracle's own O-1 reads.
1. O-1 is required. Its bounds and candidate set are unchanged (Work
   2.2 with X5-R1: in MP, `lo` stays; in MW, `lo := T`). Exactly one
   `c` must satisfy every comparison.
2. A comparison is one of: (a)'s published-set read; one (b) scan at
   `c`; one (c) raw read; one (d) scan at `S`. It is satisfied when
   its read returns and matches exactly (under X5-R7). **In class MP
   only**, it is also satisfied when its read fails with XX001
   (`Fail::Corrupt`, nothing else). That is a **detected loss**. In
   MW and C, a failed O-1 read fails O-1.
3. A detected loss counts as a Read23 detection, so O-3 (c) applies.
   It must also be **explained**: the opened image references a
   damaged slot (`tainted_image_slots` non-empty, or an active X5-R5
   `R` that is non-empty). Otherwise O-3 fails the round.
4. If the published-set read failed (MP), (a) is a detected loss, and
   (b)-(d) run over the model's published set at `c`.
5. Nothing else moves:
   - O-3 (a) (never served) and O-3 (e) (amcheck names every tainted
     referenced slot) are unchanged;
   - O-3 (d) still governs rounds with no XX001;
   - a mismatch on a read that **returns** is a failure, whatever
     else was detected.
6. RESULT items 2 and 7 report, beside the rule:
   - the number of R6 rounds (MP or MW, `Ok`, any XX001);
   - the number of detected-loss comparisons (`o1_detected_loss`).

   The per-round record carries both fields.

*Why.* In MP, a LOST or TORN write on a referenced leaf leaves bytes
that no single-copy store can recover. The strict reading (O-1 in full
on the damaged tree) therefore fails every correct implementation on
each of Q13's 900 mandated rounds: E4 would fail by construction. The
plan's clause 3 asks for exactly what such a round shows: the page is
detected (XX001) and never served. X5-R1 states MP's limit as "page-file
damage must never cause **silent** loss", and a detected loss is not
silent.

The ruling relaxes nothing else:
- every readable comparison stays exact at a single `c`;
- an unexplained XX001 fails the round;
- MW and C keep the strict form;
- the faultless twin of every MP round is judged in full as class C,
  so the same store logic is checked with no damage.

*Is this a loosening after data?* The case was open by the card's own
text: "O-3 (d) … is silent on this case". The frozen code's failure
was the pass fold's default, not a pre-registered reading. This is
P's first and only reading. Its basis is the paragraph above, not
any count.

**X5-R7 (tombstone elision vs the frozen oracle).**
- **The ruling.** O-1 (b) and (d) compare the S-STORE §8.3
  projection: per key, the live value, with a tombstone or an elided
  tombstone reading as absent (`oracle::live`). `Model::scan` and the
  store's `tree_scan` keep their shapes. Only the comparison changes.
- **The fix.** It is in `src/oracle.rs` (P-commit c1d1ab40), the one
  frozen file that changes, and only there.
- **Pins.**
  - `r7_elided_tombstone_reads_absent` must pass, and fails under the
    structural compare.
  - `r7_resurrected_version_fails` must fail. It is G-RESURRECT: the
    tombstone gone but the older version kept.
- **What it costs.** The structural compare could also see
  GC-KEEPS-TOMB (a tombstone not elided). That was never a recovery
  invariant; S-STORE §10.2 catches it with a directed test. No §15
  durability row loses its catcher.
- **Why.** §10.2 mandates the elision and §8.3 omits tombstones, so
  the "state" that the plan's clause 1 compares is what reads
  observe. Comparing an unobservable tombstone was an oracle defect,
  and repairing it does not loosen the rule.
- **Probe.** With R7 alone, seed 14 passes.

**X5-R8 (the attempt-4 protocol; the stop clause).**
- The protocol in f2959249 is void, and so are its self-rules.
- On this card, an explicit stop clause outranks the worker prompt's
  "decide, document, continue" and the runner's retry message. A
  worker that reaches one ESCALATEs and ends the attempt.
- The data exposure above goes into the RESULT's anomalies, verbatim.
- No campaign output may be read before X5-R6-R11 are in the crate
  README, in a commit. After that, campaign runs are allowed: R6 has
  now been delivered.

**X5-R9 (seed 9 round 1, the MW "served corruption") is a sim defect.
Neither of the worker's candidates is right.**
- **The cause.** `Inode::taint_ranges` (`src/sim/disk.rs`) pushed the
  **whole overlap of an issued interval** with a read whenever *any*
  byte in it differed.
  - Round 1's MISDIRECTED fault (31414 → 15872) changed bytes from
    15872 on. Those bytes sit inside the issued batch `[7515, 29745)`.
  - So the WAL record at `[7515, 14331)` was marked tainted. Its
    bytes equal the last bytes issued there: `range_tainted` is false
    at accept, the issued interval at 7515 is that same batch, and
    the first differing byte is 15872.
- **Not candidate (b), a Q11-shape stale read.** A stale read means
  accepting bytes that differ from the last ones issued. These do not
  differ.
- **Not candidate (a) as stated (segment generations).** The defect
  is granularity, not generations.
- **The fix.** 1.4's definition is "bytes … differ", so taint is now
  byte-precise: maximal runs of differing bytes. The fix is in
  P-commit c1d1ab40.
  - Pin: `taint_is_byte_precise_within_an_issued_batch`, which fails
    on the interval-granular form.
  - Every earlier sim pin still passes, including X-5's N1/N2/B-series.
- **Classification for RESULT item 5:** an instrumentation (sim)
  defect, fixed by P. Seed 9 passes after the fix.

**X5-R10 (seed 10 round 1) is a prototype deviation from S-STORE §14.
The worker fixes it.**
- **The deviation.** `recover` set `next_ts = visible_ts + 1`, and
  `visible_ts` advances only through *contiguous* replayed ts.
  - In seed 10 round 0, T = 3, the replay applied {4, 6, 7}, and ts 5
    has no commit record. So `next_ts` became 5 with the frontier
    at 7.
  - Round 1 then reused ts 5-7. The store holds 7 entries at ts 6 in
    tree 2; the model holds 4, because `Model::commit` silently
    overwrote ts 6.
  - §14: "the apply frontier is the largest replayed `commit_ts` …
    C-T0 v8 restarts its sequencer … from that value".
- **The fix.** After replay, `visible_ts := frontier`,
  `next_ts := frontier + 1`, and the leftover `applied` set is
  cleared. A P probe with exactly this change makes seed 10 pass.
- **Pins.**
  - A directed regression with a literal config, not a seed number:
    a ts in `(T, frontier]` with no commit record, then a crash, then
    recovery, then a new commit, whose ts must exceed the frontier.
  - A harness guard: recording a model commit at a ts the model
    already holds is a loud `task_error`, never a silent overwrite.
- **Note.** The 200-seed O1_FAIL count is not evidence for or against
  this. The fix rests on §14's text.

**X5-R11 (harness defects that seeds 7-8 and D17 expose; the worker
fixes each, pinned).**
- **(a) amcheck (i) does not reach a tainted referenced tree leaf.**
  The image maps it, and the store's read and amcheck (ii) both find
  it, but `harness::durable_image` never reports it:
  - seed 7 round 0: tree 5, root lpid 6, lpid 75 in slot 87;
  - seed 8 round 0: tree 50, lpid 18 in slot 29;
  - D17 leaf LOST and TORN: slot 92.

  This has been hidden behind O-3 (d) since X-5b. Root-cause the tree
  walk. Whatever the cause, amcheck (i) must apply §3.3 to **every
  page the opened map references** (every mapped LPID's `Ref`).
  - A P prototype of exactly that, plus (b), makes D17's ruled R6
    cells pass: `o1 = Ok(2)`, never served, `pass`.
  - Pins: D17's leaf cells (P-written in c1d1ab40, **red until this
    lands**), and a harness test with a torn leaf under an inner page.
- **(b) amcheck findings are never recorded as XX001 sites.** Only
  `Read23` and `Open` are ever pushed, so RESULT item 2's detection
  table and O-3 (b)-(d) cannot see an amcheck detection.
  - A slotted amcheck (i) finding pushes `XxSite::AmcheckI`.
  - A slotted (ii) finding pushes `AmcheckIi`.
  - This applies on the real, twin and failed-recovery paths.
- **(c) `StoreReader` maps every `Fail` to `ReadFail`.** Only
  `Fail::Corrupt` may become `ReadFail`. Any other failure during the
  2.3 reads (`Sim`, `Poisoned`, `Invalid`) is a harness defect:
  record a `task_error`, and the seed fails. The same applies to the
  published-set read.
- **(d) The published-set read.** On a `Corrupt` failure, set
  `Extracted::published_read_failed` and keep pushing `Read23`.
- **(e) D10's mutant arm (START-SEGMENT-MISSING-OK).**
  - After (b), its round carries an AmcheckI site from the meta
    fault. O-3 (d) therefore no longer fires, and with no twin
    supplied the round fails as "no twin (harness defect)".
  - Supply the twin, and keep the catch: the arm must still assert
    `!pass` and `o1.is_err()`. The mutant fails on O-1, which X5-R6
    item 5 keeps strict.
  - Re-pointing the arm's message is not weakening. Dropping the
    O-1 assertion would be.
- **(f) The record.** `RoundRec` gains `o1_detected_loss` and an
  `r6` flag. `campaign.json` and `verify`'s digest include them.
- **(g) The README.** Correct its Status line. Record X5-R6-R12
  verbatim. Classify every development failure for RESULT item 5 as
  one of:
  - **prototype deviation:** seed 10's ts restart, the GC pack split;
  - **instrumentation defect** (oracle, sim or harness; a third class
    beside X5-R3's two, handled like a prototype deviation: fixed,
    pinned, whole campaign re-run): R7, R9, R11 (a)-(c);
  - **spec.**

**X5-R12 (sizing).**
- **The ruling.** Relaunch `--keep`. Do not split, and do not rewrite.
- **Why.** The card's build work (Work 5, 7 and 8 code) is done and
  green. Its two runs failed for reasons other than size:
  - the missing ruling (the stop clause, twice);
  - two 60-minute kills in run 1, where attempt 1 did the bulk build;
  - a provider DNS/transport failure that ended run 2 mid-campaign.
- **What is left.** The R10/R11 fixes (small, harness-local); the
  campaign (about 7.5 min at 16 threads by the worker's 200-seed
  timing); at most one retune and re-run; the sweep; the README.
- **Order inside the card.**
  1. Pin R10 and R11 (a)-(f), each in its own commit.
  2. The 16-seed smoke must show all 16 seeds passing. A P probe with
     R6/R7/R9/R10 applied had 14 of 16; seeds 7-8 need R11 (a)/(b).
  3. Then `run`, `verify`, retune if a quota misses, the final `run`,
     `mutants`, then the README and RESULT.
- **The 577-s exit.** First re-run the 10k campaign **capturing
  stderr whole**. If it exits early again, that is a harness defect
  to root-cause before anything else.
- **A budget valve (a tightening, not a scope change).** If the first
  full campaign shows failing seed classes that the attempt cannot
  root-cause, commit `results/campaign.json`, record the failing
  classes with deterministic repros in the README, and ESCALATE. Do
  not spend the remaining attempts. P triages from the committed,
  digest-reproducible file.

## P's ruling on the OOM kills, verbatim (X-5c-esc-2, 2026-10-31; binding)

Copied verbatim from the card. It replaces steps 2–3 of X5-R12's order;
the worker landed (a) with this copy in the same commit, before the next
`run`, as the ruling requires.

**P ruling on the OOM kills of runs 2 and 3 (nv3-X-5c-esc-2, 2026-10-31): binding. It replaces steps 2–3 of X5-R12's order.** The full record is `nucleus/v3/verdicts/X-5c-esc-2.md` in the main checkout. Everything binding is below. Runs 2 and 3 were OOM-killed, not stopped by the provider: the systemd journal shows the runner's scope killed by the OOM killer, with a 32.5 GiB memory peak and a 60.7 GiB swap peak in run 3 (04:05:48). The kill took the agent session with it.

**X5-R13 (memory and liveness: the campaign must terminate and stay bounded).**
- **Measured by P at 5d99c141** (release build, `--threads 1`, each seed in its own process, under a memory cap):
  - Seeds 0–49 except 36: max RSS 80–271 MiB (mean 76 MiB), 0.25–1.6 s each.
  - **No accumulation across seeds.** One process over seeds 0–35 peaks at 291 MiB, against a single-seed max of 265 MiB in that range. A record is about 690 B, so 10k records are about 7 MB.
  - **Threads.** At `--threads 16`, seeds 37–147 peak at 2.4 GiB RSS and 9.8 GiB VmPeak, at about 7.6 seeds/s. At 8 and 16 threads, seeds 0–35 give `campaign.json` byte-identical to `--threads 1`.
  - **Seed 36 does not terminate.** RSS grows linearly at about 94 MiB/s, and the seed was OOM-killed at the 14 GiB cap after 117 s wall (2 min 31 s CPU).
  - **Seeds 0–199** (20 s and 2 GiB per seed): 198 seeds finish, and every scheduler world in them uses fewer than 25,000 yields. Seeds **36 and 148** run forever: 2.94 M and 9.96 M yields at 20 s.
  - **stderr** is about 0.8 MB per seed, so about 8 GB per 10k pass. All of it comes from the `TEMP-PROBE (reverted)` `eprintln!("SCRIPT-OP FAIL …")` at `src/harness.rs:839`, which was never reverted.
- **The cause is neither the per-worker footprint nor accumulation.** 16 × 271 MiB ≈ 4.3 GiB. The cause is **livelocked seeds** (about 1 in 100). In them, some task keeps yielding and never parks, so "all tasks blocked" never holds and the DEADLOCK detector never fires.
  - **Seed 36.** `tree::gc_step` hands back its own cursor (`next == resume`; tree 2, root lpid 3, `w = 30`), so `harness::gc_sweep` loops at `h:gcyield` forever. The checkpointer spins at `h:ckpt` meanwhile.
  - **Seed 148.** The checkpointer spins at `h:ckpt` (`checkpointer_body`) waiting for `workload_done`. Its RSS stays flat: a pure spin, which costs a worker but no memory.
  - **Why it kills.** Each such seed holds a worker forever and grows without bound. At about 1 per 100 seeds, all 16 workers are captured within the first few thousand seeds. That is run 2's exit at about 3,000/10,000 after 577 s, and run 3's 32.5 GiB + 60.7 GiB.
  - **These are failing seeds under X5-R3, not sizing.** X5-R12's "577-s exit" item is answered by this ruling: P has root-caused the exit.
- **The ruling.** Each item is an instrumentation fix (X5-R11 (g)'s class), in its own pinned commit, before any further campaign `run`:
  1. **(a) stderr.** Delete the TEMP-PROBE `eprintln!`. Campaign stderr carries only:
     - the progress lines;
     - real errors;
     - (d)'s abort line.
  2. **(b) A liveness budget (a tightening: a hang becomes a recorded failure, never a pass).**
     - **The budget.** `Sched` gets a fixed per-world yield budget, `YIELD_BUDGET = 1_000_000`: 40× the largest world measured.
     - **On a trip.** When a world's `yields` exceeds the budget, the scheduler fails the world exactly as the deadlock path does (crashed, waiters cleared, all notified) and returns a new `SimFail::Livelock`.
     - **The record.** The harness records it on the round, as `livelock: true` plus the site of the yield that tripped it. The round and seed `deadlock` flags fold it in, so it counts in `DEADLOCK=` and in RESULT item 7's n. `livelock` goes into `campaign.json` and the digest.
     - **Determinism.** Yields are counted in sim order, so the S-DET/W-DET thread-count pin must still pass.
     - **Changing the budget.** The constant is fixed now and goes in the README. It may change only as the fix for a seed whose root cause is a legitimately long world (an instrumentation defect), followed by a whole-campaign re-run.
     - **Pins.**
       - A directed test with a literal config: a task that only yields and never parks. Its round must record `livelock` and `deadlock`, and the test must fail with the budget check removed.
       - `run --seeds 36..37` must finish, with seed 36 recorded as DEADLOCK/livelock.
  3. **(c) A memory self-check (process-level; never a record, a digest or a sim input).**
     - **The watchdog.** `run`, `verify` and `mutants` start a watchdog thread. It reads `VmRSS` from `/proc/self/status` (std only) once per second.
     - **On a trip.** Above `E4_MEM_LIMIT_MIB` (default 20480), it stops dispatch, then:
       - writes the completed records, sorted by seed, to `<out>.partial` (for `mutants`: the completed mutant records);
       - prints `E4 ABORT rss=<MiB> limit=<MiB> done=<k>/<n> inflight=<seeds>` to stderr;
       - exits 3, without writing `<out>` and without a final `E4` line.
     - **Partial files.** A `.partial` file is diagnostic only. It is never `verify`'s input and never a RESULT count.
     - **Progress lines.** Every progress line also prints `rss=<MiB>`.
     - **Pin.** The check is a function of `(rss, limit)`, unit-tested on both sides of the limit.
     - **Why it is allowed.** The watchdog's 1-s sleep is wall-clock, but it decides only whether the process lives. It never feeds a sim-visible decision, so the "Do not" ban on wall-clock time does not apply to it, and only to it.
  4. **(d) The seed range is half-open.** `--seeds A..B` means A ≤ seed < B, as in Rust and in this card: `0..10000` is 10,000 seeds and `0..500` is 500.
     - **The defect.** At 5d99c141, `parse_seeds` reads the range inclusive. `run --seeds 0..10000` would write 10,001 records, and `verify` would reject them ("not exactly 0..9999").
     - **Pins.** `0..10000` gives 10,000 seeds; `5..5` is empty, which is an error.
- **Bounds, derived from the measurements.**
  - **Threads.** `--threads 16` stays. Steady state is at most 16 × 271 MiB ≈ 4.3 GiB.
  - **The worst case.** A world that hits the budget is small, measured by P with the budget's yield count:
    - seed 36 is at 509 MiB RSS when it reaches 1 M yields (after 5.5 s);
    - seed 148 stays flat at 133 MiB (it reaches 1 M yields after 2.2 s).

    Even with 16 livelocked seeds in flight at once, the total stays at about 8 GiB, well under the **24 GiB budget**.
  - **Lowering threads.** If any later measurement shows a single-seed peak above 1.5 GiB, the worker sets `threads := floor(24 GiB / peak)`. Digests do not depend on the thread count, so this changes no result.
  - **Not required.** Chunking, per-chunk commits and streamed records. The records are about 7 MB, and the run is about 20–25 min wall at 16 threads. X5-R12's "do not split" stands.
- **The backstop and the logs.** The worker runs every campaign-class command (`run`, `verify`, `mutants`, `mutants --check`) like this:
  - inside `( ulimit -v 25165824; … )`, a 24 GiB address-space cap. The measured VmPeak is 9.8 GiB at 16 threads. A runaway then fails an allocation inside the campaign process only, never in the agent's session;
  - with stdout and stderr redirected to files **outside the worktree**. Read them with `tail` or `grep`; never stream them into the tool output.
  - Capturing stderr whole (X5-R12) means that file.
  - `systemd-run` is not used (an owner rule).
- **Order inside the card** (replaces X5-R12's steps 2–3; step 1 is done):
  1. (a)–(d), each pinned in its own commit;
  2. `cargo test` green, and the 16-seed smoke passing;
  3. `run`, then the X5-R12 sequence. Seeds 36 and 148 are failing seeds under X5-R3: root-cause each one (prototype deviation → fix, pin with a literal config, whole re-run; spec → ESCALATE), and X5-R12's budget valve applies unchanged.
  4. **An `E4 ABORT` line or exit 3 is a harness defect.** Root-cause it before anything else, and if that is not possible inside the attempt, ESCALATE with the `.partial` file and the abort line.
- **RESULT.**
  - Item 1: `YIELD_BUDGET`, the threads, and the peak RSS.
  - Item 2: the livelock count inside `DEADLOCK`.
  - Item 5: every (a)–(d) item (instrumentation), and seeds 36 and 148 with their root causes.
- **Nothing else moves.** The oracle, the model, the classes, the quotas and X5-R1–R12 are unchanged. Recording a livelock as DEADLOCK tightens the rule (before, it was a hang); it loosens nothing. The README gets X5-R13 verbatim in the same commit as (a), before the next `run`.

## The first complete 10k campaign (run 3 of the attempt, committed)

`run --seeds 0..10000 --threads 16` after the gc fix and X5-R13
(a)–(d): **exit 0, ~13 min wall on a quiet host (16 threads), peak
RSS 5.1 GiB** (the gc fix took the pre-fix runs' ~14 GiB peak with
it). `results/campaign.json` (7.8 MB, digest-reproducible) is
committed at d2c8bba7. Final line, verbatim:

```
E4 SEEDS=10000 O1_FAIL=2712 O2_FAIL=98 O3_FAIL=1029 DEADLOCK=46 PANIC=0 O1F_FAIL=2722 MONITOR=0
```

Per class (rounds: C 11,777 / MP 2,183 + 2,183 twins (2,137 passing) /
MW 1,415 + 1,414 twins (1,360 passing)):

| | O-1 | O-2 | O-3 | O-1f | DEADLOCK | livelock rounds | acked lost (SyncOff) |
|---|---|---|---|---|---|---|---|
| C | 474 | 98 | 133 | 474 | 0 | 0 | 140 (140) |
| MP | 1784 | 0 | 436 | 1785 | 46 | 46 | 46 (46) |
| MW | 454 | 0 | 460 | 463 | 0 | 0 | 1962 (618) |

X5-R6 beside the rule: 308 R6 rounds (MP 264 + MW 44), 5,668
detected-loss comparisons (all MP).

**1,560 failing seeds, in classes (the budget valve, X5-R12):**
deterministic repros — every line below is re-runnable from the
committed file (`run --seeds N..N+1`):

1. **Class C scan mismatches with no faults at all** (the gravest
   class: O-1 (b)/(c)/(d) "scan mismatch" / "raw entry above the
   cut" — e.g. seeds 21, 36, 46, 63, 93, 104, 112, 152, 186, 200).
   Class C applies F-CRASH only; every one of these is a genuine
   divergence between the recovered store and the model at every
   candidate cut, or an oracle/model-semantics gap. Not root-caused
   in this attempt.
2. **MP O-1(a) "published {} != model {…}" with O-3 (e) exactness or
   completeness failures** (1,784 O-1 records; the pass-failing arm is
   O-3 (e): "amcheck reported slot … neither tainted nor in R" /
   "tainted referenced slot … not reported" — e.g. seeds 37, 38, 47,
   67, 118, 137, 157, 198, 228). The O-1(a) text is the frozen
   oracle's recovery-`Err` branch (an empty default published set
   compared against the model — recorded, not pass-failing by
   itself); the O-3 (e) findings are the failures. The O-3 (e) /
   X5-R5 interplay under page damage is not root-caused here.
3. **MW O-1 (c) "raw entry above the cut" + O-3 (d) silent-Ok**
   (e.g. seeds 39, 59, 69, 109, 149, 159, 199, 209, 239). No
   candidate cut exists: the store holds a raw entry above every
   legal cut. 1,962 acked commits lost (618 SyncOff) — X5-R1's known
   cost is reported beside the rule, but these rounds fail O-1/O-3.
4. **The Mid2 crash point never fires** (a harness defect class the
   fired-guard catches: seeds 60, 62, 64, 262, 268… defect "the
   crash spec never fired (Mid2@2)"). The targeted draw picks
   checkpoint index 2 / hook Mid2, which the round's own checkpoints
   never reach.
5. **The 46 DEADLOCK seeds are all MP livelocks** (the seed-148
   hidden-deadlock class, `ll=True`; 46/2000 MP seeds ≈ the ~1/100
   P measured).
6. **Twins: 46 MP + 54 MW twin rounds fail** (the same store logic
   with no damage — classes 1–3 echo there).

**Coverage quotas (verify's three exit conditions):** the seed set is
exactly 0..9999 and the digests reproduce (verify's re-run checks
them; the determinism pins hold), but **5 quotas miss**: Q4 0/1000
(no interrupted checkpoint has ≥1 split **and** ≥1 merge in the
window), Q7 155/200 (DropTree-walk crashes), Q11 0/100 (no stale
record ever read at a renamed segment's start), Q12 2/100 (G1 renames
dropped by F-CRASH), and **SIZING: 14,988 rounds miss Work 5.3's ≥ 4·F
distinct dirtied LPIDs** — the workload never dirties 4× the pool in
one interval (a harness defect per 5.3, i.e. a workload-sizing
retune, which the card allows only before a final run). Q1 (all 10
hooks), Q2, Q3, Q5a–c, Q6a/b, Q8, Q9, Q10, Q13a–c, Q14, Q15, Q16,
Q17, Q18 are all met.

**Where this leaves the stage (X5-R12's budget valve, applied
verbatim):** the first full campaign shows failing seed classes this
attempt cannot root-cause (classes 1–3 above; 1,560 seeds). The
campaign file is committed; the classes are recorded here with
deterministic repros; the attempt does not spend itself on a retune +
final run + sweep whose outcomes P must triage first — the retune
(SIZING/Q4/Q7/Q11/Q12) and the mutant sweep stay to run after that
triage. The E4 rule's verdict for THIS run is in the stage RESULT.



- **X5-R13 (a)–(d)** (commits e25073a2, 9f581381, 20781b78, ca77230e):
  each landed pinned in its own commit, before any campaign `run` (the
  live checks are in the Status line above and the commit messages:
  the abort path `E4 ABORT rss=2 limit=1 done=0/4 inflight=2`, exit 3,
  the `.partial` written, no `<out>`, no `E4` line; `run --seeds 36..37`
  finishes — see the seed-36 note below for how its second half read
  out).
- **Seed 36, root-caused and fixed (a prototype deviation from S-STORE
  §10.4, classified per X5-R11 (g)/X5-R13 item 3).** P's diagnosis
  confirmed exactly: `tree::gc_step` handed back its own cursor. The
  mechanism: the sweep's resume was `next_after` of the last processed
  cell, which cannot cross the **subtree boundary** the leaf closes
  when the key region ahead is empty (a key gap across an inner
  boundary) — the descent re-enters the same subtree forever, a hang
  the DEADLOCK detector can never see because the looping task never
  parks (pre-(b): an unbounded hang and the OOM kill; post-(b): a
  recorded livelock). The fix (`src/store/tree.rs`): when the leaf is
  exhausted, is its parent's right child (no in-parent sibling pair —
  its groups were already decided as the previous step's R), and the
  path is not the tree's rightmost, the step resumes at the
  **subtree ceiling** (the smallest separator bounding the leaf from
  above at any ancestor) instead. Pin:
  `gc_sweep_crosses_subtree_boundaries` (a literal config: a depth-3
  tree of 3,000 keys spaced 7 apart — an inner page caps at 254
  children, so ~334 leaves force the third level, and every inner
  boundary crossing has an empty key region ahead; fails on the step
  bound with the ceiling reverted, demonstrated mutant-style both for
  the draft resume and for the minimal branch, reverted clean). The
  empty-leaf and exhausted-with-sibling flows are byte-identical to
  the draft (gc_merges_and_root_collapse stays green).
  - **The `run --seeds 36..37` pin, honestly:** the run finishes, but
    seed 36 is **not** recorded DEADLOCK/livelock — after (b), the
    uniform draw's counting pass is bounded by the budget, so every
    round's crash point (y:3494, y:15821, y:472667) fires **before**
    the livelock begins, and all three rounds pass (the 36..37 run at
    the (b) commit: `E4 SEEDS=2 O1_FAIL=1 O2_FAIL=0 O3_FAIL=1
    DEADLOCK=0 PANIC=0 O1F_FAIL=1 MONITOR=0`, the failures being seed
    37's pre-fix round). The termination half of the ruling's pin
    holds; the DEADLOCK/livelock half described the pre-(b) behavior
    (the hang P measured was the counting pass, which never
    terminated without a budget).
- **Seed 148, root-caused to its class (unresolved key → the budget
  valve):** round 1 livelocks at `h:evictor` with the pre-trip park
  table showing **every task runnable** — but the per-task yield
  counters added for the diagnosis tell the real story:
  `yields=[0, 16, 123, 20, 274, 86, 333778, 332263, 333441]` — the
  three **service tasks** (writer, evictor, checkpointer) burn ~333k
  yields each spinning for `workload_done`, while all five script
  tasks are stuck with almost no yields: parked on conditions that
  never arrive (the runnable park-table entries are the wake-storm
  microstate — the spinners' wakes flip them; a single snapshot
  cannot name the key). This is a **real deadlock hidden behind the
  service spins**, exactly P's seed-148 diagnosis ("the checkpointer
  spins at h:ckpt waiting for workload_done"): the spin masks the
  DEADLOCK detector, the budget bounds it, and the seed fails as
  DEADLOCK/livelock. The specific stuck wait is not yet identified
  (the gc fix did not clear it — 148 still fails after it); the
  deterministic repro is `run --seeds 148..149` (round 1, ~30 s).
  Recorded here for the next attempt; X5-R12's budget valve applies.
  The round's recovery-`Err` O-1 text ("published {} != model {2, 3,
  4}") is the frozen oracle's Err-branch default (the pass fold
  ignores O-1 for a recovery `Err` in MP; the round fails through the
  deadlock fold).

## This stage's delivery of the rulings (the worker's half)

- **X5-R10** (commit 9488e1ed): `recover` restarts at the frontier
  (`visible_ts := frontier`, `next_ts := frontier + 1`, `applied`
  cleared); the model-commit guard makes a held-ts commit a loud
  `task_error`. Pin: `r10_sequencer_restarts_at_the_frontier` (the
  literal S_L-hole config: T=5, the hole at 6, the publish at 7;
  `next_ts == frontier + 1`; round-1 commits at 8-10; the guard fires
  under the reverted fix — demonstrated mutant-style, `te="model
  commit at held ts 7 (sequencer reissue)"`).
- **X5-R11 (a)** (commit be2f9f4c): `durable_image` §3.3-checks every
  mapped LPID (the draft's directory→tree walk never descended inner
  pages — the map walk pre-filled `used`, so the dedup skipped them).
  Pin: `r11a_amcheck_reaches_a_torn_leaf_under_an_inner_page` + D17's
  P-written R6 cells.
- **X5-R11 (b)** (be2f9f4c): slotted (i)/(ii) findings push
  `AmcheckI`/`AmcheckIi` on all three paths.
- **X5-R11 (c)(d)(f)** (commit 501e39f1): the reader mapping, the
  published-read flag, the `o1dl`/`r6`/`cxx` record fields.
- **Two harness defects the smoke exposed once (a) let seed 7's
  round 0 pass** (be2f9f4c; classified **instrumentation defects**
  under R11 (g)):
  1. **The park loops** (P1): a failed script op woke its parked
     peers once (Mesa) and they re-parked forever — DEADLOCK hiding
     the store error. Every harness park loop now exits loudly
     (`round_failed`). Pins:
     `park_loops_exit_loudly_on_a_failed_round` (unit) and the
     smoke's no-DEADLOCK assertion (seed 7 deadlocked pre-fix,
     demonstrated by hand and reverted).
  2. **The abort-on-Corrupt policy** (the seed-7 class): the harness
     aborted the whole round when a script op failed with XX001 — but
     in a media round that is the plan's clause 3 working (detected,
     never served; seed 7 round 1's **drop of the damaged tree**, the
     store's drop walk reading the torn leaf, `lpid 75 slot 87`). The
     abort overrode the ruled oracle and broke 5.5's round shape (the
     crash never fired; `fired=false`). Now the task unwinds as a
     client-visible detection (`client_xx001` counted, recorded beside
     the rule), its commit ts fills the visibility slot (the record
     stays in the WAL; recovery is the authority for its effects —
     R10's principle), and the round completes to its crash, faults,
     recovery and oracle. Class C keeps the loud task error. With the
     fix all 16 smoke seeds pass (seed 7 stops early after round 1's
     recovery-`Err`, every round passing).
     **ESCALATED (the budget valve):** the fix is harness-side only —
     the store is untouched — but S-STORE is silent on (i) DDL/write
     ops over detected-damaged pages (§6.8's drop walk, §8 reads) and
     (ii) the checkpoint/visibility semantics of a durable commit
     whose apply fails on detected damage (§12.4/§13/§14). Deterministic
     repro: `run_seed(7, None)` — round 0 passes (the R6 detection
     round), round 1's DDL task drops tree 5 whose leaf at lpid 75 /
     slot 87 round 0's MP fault damaged; the drop walk raises XX001.

## What this stage built (attempt 2, completed and carried by attempt 3)

- **Work 5 (`src/workload.rs`)**: the knob derivation (5.1: 2–4
  committers + writer/checkpointer/GC/evictor/DDL, 2–4 user trees +
  catalog, keys 0–3,000, values 8–400 B, commits 1–20 ops, SyncOff ∈
  {0, .3, .7}, long readers as `SnapshotHold` spans), the feature mix
  (5.2: one 320–700-op chunked txn per round spanning ≥ 2 segments by
  value sizing, `BulkLoad` on 25% of rounds, `DropTree` of a published
  tree, GC sweeps concurrent with checkpoints), the classes (5.4:
  `seed % 10` → C/MP/MW = 7000/2000/1000), the rounds (5.5: 1–3 at
  50/30/20, every round crashes; targeted at `CKPT_HOOKS[(seed/20)%10]`
  of a seeded checkpoint when `(seed/10)%2 == 0`, else uniform; nested
  recovery crashes drawn 60/30/10 over the recovery's **counted** yield
  length so a drawn point always fires), the twins (Work 2.5: every
  MP/MW round runs its faultless twin first — the same script, crash
  and F-CRASH resolutions, judged as class C — supplying the real
  round's O-3 (c) read/referenced sets and its nested points), and the
  per-seed driver with the canonical digest. **Judgment call,
  documented:** the chain stops after a round that failed the oracle
  (X5-R3's root-cause unit — continuing on a failed round's base would
  stack fallout the R6 ruling must judge first); recorded as
  `stopped_early`, never hidden.
- **The uniform crash is scheduler-yield-level (attempt 3, kept from
  the killed run, re-validated)**: 5.5's "uniform over the round's
  yield points" reads on the **scheduler's** yield counter (every
  `E4Env` call, every baton handoff — the service tasks' windows, the
  checkpoint's internals, the rename/dirsync gaps), not only the script
  tasks' op boundaries (attempt 2's `TaskYield` subset, which could not
  address Q12's rename window at all). The driver runs a **counting
  pass** (`RoundFaults::probe_pass`: the workload only, crash disabled,
  the outcome carries `sched_yields` and the crash-time observables),
  draws `k` uniform in `1..=counted` (`uniform_yield_point`), and arms
  `CrashSpec::Yield(k)` — the same counted-pass pattern the card
  prescribes for the nested draws. The arm lives in `run_script` (which
  resets the scheduler core to size the task set; arming before it was
  silently wiped — the defect the fired-guard caught immediately).
  **The fired-guard** (`crash_fired_defect`): a round whose crash spec
  never fired is a **harness defect** that fails the seed (5.5 step 2
  requires the crash; a silent no-crash round would leave every
  downstream field meaningless while still being judged) — wired for
  both the twin and the real round, pinned by the smoke's `fired`
  assertions and
  `uniform_crash_fires_at_the_counted_yield`.
- **W-SCRIPT (the generator's invariant, pinned)**: a round's script
  references only trees the model at the previous cut has, and every
  non-setup task opens with `ScriptOp::WaitTrees` (the round-setup
  barrier): the seed-3 smoke failure was the generator letting a
  chunked txn's chunks **and marker** reach the WAL below the setup
  commit's `CreateTree` — LSN-ordered replay then applied the marker's
  Puts against a directory without the tree. Pinned by
  `setup_barrier_guards_the_wal_order` (seed 3's generated script, the
  barrier stripped vs in force).
- **Work 7 (`src/campaign.rs`)**: `run --seeds A..B --threads N --out
  results/campaign.json` (per-seed records, the final `E4 SEEDS=…`
  line, byte-identical digests across thread counts — pinned), `verify
  --results …` (re-runs every seed, checks digest reproduction, the
  exact 0..9999 set and every 5.6 quota — the SIZING row counts
  **misses** and passes only at zero; exits non-zero **only** on those
  three, never on oracle failures), `mutants --seeds 0..500 --out` and
  `mutants --check` (the sweep, the directed column re-run, the
  first-catch check). The JSON is hand-rolled (deps stay
  `thiserror`+proptest); the round-trip is pinned.
- **Work 8 (the directed column)**: `campaign::directed_arms::TABLE`
  names every mutant's directed test (all 28, asserted complete); two
  arms are ported and re-run in-process (D2's START-LSN-MARKED-CHUNK
  and D17's CRC-SKIP first half, both asserted catching); the rest
  record `directed_encoded: false` honestly — **the sweep attempt
  completes the table before the final run** (the campaign column is
  the Work-8 proof either way).
- **The quota machinery (5.6)**: `workload::quotas` computes Q1–Q18
  plus the Work-5.3 sizing assertion (`SIZING`, a miss is a harness
  defect) from the per-round records; `verify` fails on any miss.
  Coverage **tuning** has not run (it needs campaign output, which the
  R6 guard stops) — the quotas are implemented, not yet achieved.

## The mutant sites (Work 8's README list)

One site each, in `src/store/`: CRC-SKIP `page.rs` check (1);
REFCRC-SKIP check (2); EPOCH-SKIP check (3); ID-SKIP check (4);
FREE-EARLY `ckpt.rs` (the frees at the flip); BITMAP-OMITS-FREESET-PAGES
`ckpt.rs` `write_free_set`; META-BEFORE-SKIP `ckpt.rs` step 3's sync;
FLIP-NO-GATE `prims.rs` gate_exclusive; WAL-SYNC-T-ONLY `wal.rs`
`wal_step1`; WRITE-GUARD-NO-COPY / EVICT-SKIP-IMAGE-MAP `prims.rs`
`store_page`'s freeze and `record_image_copy`; START-LSN-PAST-CHUNK /
START-LSN-MARKED-CHUNK `wal.rs` `wal_start_lsn`; TS-AFTER-APPEND
`harness.rs` `commit_txn`; LSN-CHECK-SKIP `wal.rs`
`dec_record_with_lsn_check`; PUBLISH-BEFORE-META `harness.rs`
`bulk_load`; GCW-NOT-RECORDED / GCW-BOUND-SKIP `ckpt.rs` the flip;
REPLAY-DUP-CONFLICT / DUP-DIFFERENT-ACCEPTED `ckpt.rs` `apply_op`;
INPLACE-EVICT `prims.rs` dirty eviction; DROP-ROOT-FIRST `tree.rs`
`dir_drop_tree`; RENAME-NO-DIRSYNC `wal.rs` the dirsyncs;
TRUNCATE-LOGICAL-ONLY `ckpt.rs` recovery's truncation;
START-SEGMENT-MISSING-OK `ckpt.rs` open's G3 check; META-WRONG-SLOT
`ckpt.rs` open's `select_meta` call.

## What is NOT done this attempt (the guard, honestly)

- The 10k campaign `run` (no `results/campaign.json` exists),
  coverage tuning, the mutant sweep (`results/mutants.json`), the
  RESULT items that need campaign numbers (2, 3, 4, 7's table), and
  the ≤ 20-min wall verification. Attempt 3's killed run produced a
  100-seed quota file outside the crate; it is not used and no
  campaign output exists in the worktree.
- The instrumentation checks (the card's list) were re-run green on
  the final code as ordinary suite members: O-1's lower bound, O-1
  (c), the taint comparison and the twin false-positive rule
  (`tests/oracle.rs`'s knob demos and must-fail table), X5-R5's
  meta-slot-fault condition (must-fail 13), the D10b arm and the
  S-BATON `try_lock` check (`tests/directed.rs`, `tests/sim.rs`).

## Stage X-5b (complete, merged) — gate rounds and inheritance

**X-5c inherits from X-5b's verdict (MERGE-WITH-FOLLOWUPS):** the R6
cells (D17's leaf LOST/TORN: assert only the reading-independent
facts, print the measured outcome — this stage's smoke survey is their
campaign-scale echo), D16's argued uncaught directed pair (the
campaign's Q10 rounds are its catcher per the mutation table), and the
`run`/`verify`/`mutants` surfaces (implemented above). The verdict's
non-blocking notes (dead TORN-sector code, the stale D17 comment, the
`run_round` M-NOINPLACE pin, the REFCRC arm's any-error acceptance)
fold into the sweep attempt's table completion.


## Gate round 3 (2026-10-26 verdict f8a6b6eb, applied this run)

- **F1 (the §12.2 record cap).** `wal_enqueue` refuses a payload whose
  record exceeds `wal_seg_size / 4` (16 KiB) with `Fail::Invalid`
  before any placement; the Work 2.1 derivation records each commit's
  `(lsn, len)` extent and returns a third list — commits whose record
  header sits issued at its segment-relative position with its own lsn
  and a length past the segment end (written, never parseable) — which
  `run_round` folds into a loud `task_error`. The demonstration's shape
  (a 177-KiB `Commit` spread across segments, acked and lost silently)
  is now impossible twice over. Pins: `wal_record_cap_is_refused` and
  `derivation_reports_written_unparsable_commits` (both verified
  mutant-style, failing under their named probes). D14's two 60-put
  commits and D17's 420-put load are chunked (§12.7).
- **F2 (the misdiagnosed open finding).** With the chunked script the
  D17 faultless base passes `Ok(2)`: the round-2 "open finding" (a
  rolled-back `Ok` whose raw entries sat above every cut) was F1, not a
  meta-rollback inconsistency — **withdrawn**. The meta LOST and TORN
  cells are clean rollbacks that pass; the grid completes its TORN
  cells (the freeset root and the meta hold all their content in
  sector 0; a leaf in its cell sectors — the tear sector is chosen by
  proving the bytes differ).
- **F3 (G7, P's ruling of 2026-10-26).** A meta is valid at open only
  if it sits in its own slot, `1 + (ckpt_id mod 2)`; a misplaced valid
  meta outranking every placed-valid one is XX001 — never opened, never
  fallen back past. One shared helper (`page::select_meta`) serves the
  store's open, amcheck, the monitors and the harness's two walks (they
  can never disagree — the lesson of R4); open raises XX001 before any
  meta bytes are accepted and accepts the slot the winner was read
  from; a misplaced older meta is a loser, ignored. The mutant
  META-WRONG-SLOT (28 now) skips the positional test at the store's
  open only; D17's meta MISDIRECTED cell (plant on the twin slot, the
  base's newest meta asserted ≥ 2) is the catching test: clean — XX001
  at open, never served, passing; mutant — the misplaced copy opened
  from the tainted landing slot, served, O-3 (a) fails the round. A
  spec card for the §2.2 sentence is queued for P (the ruling's
  routing); until it merges, this crate tests "S-STORE as merged plus
  G1–G7".
- **F4 (X5-R6).** Every D17 cell's `o1`-error arm now accepts only the
  read-failed message forms (O-1 (b)/(c)/(d) scan-or-raw read failed —
  an XX001 that surfaced through O-1's own reads); the two cells that
  reach it (leaf LOST and leaf TORN: recovery `Ok`, the extraction's
  scan hits the faulted page, O-3 (d) fails the round) are X-5c's
  pre-registered ruling case and assert only the reading-independent
  facts — recovery `Ok`, read-failed `o1`, never served — neither
  `pass` nor `!pass`; the test prints their measured outcome for the
  record.
- **F5 (the four unpinned fixes, each pinned and verified
  mutant-style).** R4's positional id — the empty-leaf plant
  (`r4_positional_id_discriminates_the_empty_leaf_plant`): two
  fully-unmapped middle radix leaves are byte-identical except the
  positional id, the only shape where check (3) alone discriminates;
  the all-three and store-walkers-only header_id reverts both fail the
  pin (served through the tainted landing slot), the harness-walks and
  amcheck single reverts are subsumed by construction (argued in the
  test's doc). Condition 3 at `exception_set_r`'s call site
  (`exception_set_r_consults_its_noinplace_hits`). The step-2 fixpoint
  re-scan (`step2_fixpoint_writes_late_epoch_copies`, the first-touch
  performed inside step 2 via the checkpoint's own Mid2 hook). And
  `fetch_page`'s `Stale` re-validation (`fetch_page_revalidates_its_
  carried_ref`, over a new named park point `fetch:inflight` — the
  in-flight window between a fetch's read and its re-validating
  install).
- **Two harness defects the G7 clean arm exposed, both fixed:** the
  failed-recovery path never collected its reads into the twin (open
  had read both meta slots before failing; the Err branch's TwinInfo
  hardcoded `recovery_reads` empty, so O-3 (c) called the G7 detection
  a false positive) — the final Err arm and the Err-branch TwinInfo now
  carry the attempt's reads. D10b's doc item 1 corrected (the reuse
  pairs the **same** radix base; the N5 note).
- **D16's pair stays reported-uncaught (directed)** with its argument
  in the test's doc comment; the campaign's Q10 rounds remain its
  catcher per the mutation table.

## Gate round 2 (2026-10-26 verdict, applied this run)

- **N1 (FreeSet sizing).** §13 step 2's convergence loop: the page count
  is recomputed after growth and the difference allocated until the
  bitmap span covers every free-set slot; the meta's `slot_count` is the
  converged span. Pinned by `freeset_growth_covers_its_own_slots` (the
  verdict's 8-seed shape at every `R_EPOCH`, After5 and After6 — the old
  code failed with both the amcheck finding and an O-1 (b) scan
  mismatch).
- **Three more Work-11 rule-1 violations the same shape exposed, all
  fixed:** (a) `ckpt:freeze` snapshotted `cur` while an eviction held
  the frame latched — the eviction's write and `record_image_at` landed
  after the snapshot, so the radix embedded the pre-eviction home and
  the image lost the leaf's newest entry (the checkpointer now drains
  eviction latches before the one-step freeze); (b) step 2's single
  scan missed first-touch epoch copies installed into reserve frames
  behind the cursor — the fixpoint loop re-scans until a pass writes
  nothing; (c) `ckpt2:record`'s pinned branch and `fetch_page`'s carried
  load both skipped re-validation after yields.
- **N2 (O-2 (ii) judged).** The harness checkpoint and amcheck (ii) now
  feed the verdict: a first `check_round` keeps the card's
  extraction→oracle→checkpoint order, the (ii) fields then go in, and
  the final verdict re-judges with them (the (ii) image's referenced and
  tainted slots from the durable bytes). A failed harness checkpoint is
  a loud `task_error`. N2's wiring exposed two more deviations, both
  fixed: the free set built its inner pages with `write_fresh` onto
  slots the bitmap never marked used (I-FREE-EXACT; the pages now go to
  the pre-allocated slots), and the durable-image walk silently skipped
  §3.3 failures on tree and directory pages (the directory walk also
  checked a hardcoded expected id of 0).
- **N3 (D18 vacuous).** `fail_of` no longer masks store errors as
  `SimFail::Crashed` (the real "tree 2 missing in directory" was hidden
  behind task_error="Crashed"), and the harness's apply loop waits for
  the creating commit's directory entry (applies finish out of ts order,
  §12.4; a malformed script fails loudly instead of spinning the service
  tasks past the deadlock detector). D18 reshaped on seed 2023: clean
  passes with M-WAL-ORDER silent; TS-AFTER-APPEND gives the lower-ts
  record the higher LSN — **M-WAL-ORDER fires and the oracle fails**,
  both named catchers asserted.
- **N4 (assert discipline).** Every clean arm asserts `pass` (except
  D12b's designed fail-stop); every mutant arm names its catcher.
- **N5 (the R-taint refutation).** D10b arm 4 is the card's own arm:
  REFCRC-SKIP serves a reused slot through the R-taint (O-3 (a) fails);
  ID-SKIP+REFCRC serves identically; the new `no_r_taint` knob (the G5
  call-site pin) shows the same round **passes silently** with the
  R-taint forced off.
- **N6 (the D7 equivalence argument rejected).** D7 is the real
  two-round zombie: round 1 tears tail 3's first sector with a directed
  TORN fault on sector-aligned 1536-byte records (an in-place tear
  cannot be drawn from F-CRASH here — the writer syncs every batch, so
  each sector has one volatile image and the draw is all-or-nothing per
  batch); round 2 appends the same-length record, §12.5's position check
  lines the intact tail 4 up with the new append — clean passes (G2
  zeroed the tail), TRUNCATE-LOGICAL-ONLY replays the zombie and O-1
  fails on a commit the truncated model no longer has. Two harness
  deviations fixed en route: model commits are recorded at the enqueue
  (where `wal_commits`'s contract places them — a committer crashing
  mid-ack with its record durable never entered the model), and
  `derive_written_acked` starts at the round's first write and jumps
  undecodable chunk boundaries (a chained round's events hold only its
  own writes).
- **N7 (the unpinned rework fixes).** Condition 3 (`r_slot_qualifies`),
  the pass fold (`fold_pass`) and the abort path's `ckpt_open` clear
  (D12b asserts no DEADLOCK) are pinned; the R4 positional-id pin runs a
  free-set-inner MISDIRECTED onto a free-set leaf's referenced slot
  under REFCRC-SKIP, where only the positional id discriminates.
- **N8 (D17's grid).** LOST and MISDIRECTED on a referenced tree leaf,
  the free-set root and the newest meta slot join the map-leaf TORN,
  LOST and MISDIRECTED cells; a meta MISDIRECTED onto the twin slot is
  a true serve (a valid copy through the fault's tainted landing range)
  that O-3 (a) fails. *(Round 3 withdrew this item's "open finding"
  misdiagnosis and rebuilt the grid on the F1/F2-fixed base — see
  "Gate round 3"; the meta MISDIRECTED cell is now G7's catching test,
  and the TORN cells exist for every target.)*

## Verification round (2026-10-26)

The re-run re-applied the gate's own probes by hand before finishing:

- **G4 (recovery-world served corruption dropped from O-3 (a)).** The
  faithful application — the extraction world's carried `served` set
  cleared **and** the `recovery_served` term dropped — passed the whole
  suite: D17's CRC-SKIP arm asserted any-failure and the round fails
  via the silent-loss path regardless of the serve. Fixed properly:
  `RoundOutcome.served` now carries O-3 (a)'s input, and D17's clean
  and CRC-SKIP arms assert it (`!clean.served` / `m.served`). The
  demonstration: with the faithful probe applied, `d17_integrity`
  fails at the CRC-SKIP `m.served` assertion; with only the
  `recovery_served` term dropped, nothing fails — the serve also flows
  through the extraction world's carried set, so that application is
  subsumed by design (two independent paths, no single droppable
  site). Probes reverted; `git diff` clean of them.
- **G5 (the R-taint forced false).** Function-level (the body of
  `r_taint_of` forced to return `false`): caught by
  `r_taint_window_semantics` in `tests/sim.rs` (cases (1) and (3)
  assert true). Wiring-level (`let r_taint = false;` at the call
  site): **UNCAUGHT-directed** — a positive end-to-end case needs
  exactly D10b arm 4's same-position/same-epoch reuse coincidence
  (argued unreachable directed; the campaign's Q14 rounds are the
  catcher). Reported to X-5c with the arm.
- **The four hard-coded integrity disables (the gate's R2 probes).**
  All four are now caught by `d17_integrity`, each demonstrated by
  hand and reverted: check (1) hard-off → the clean arm's
  `!clean.served` (the torn leaf is accepted); check (2) hard-off →
  the new second half's clean arm (below); checks (2)+(3) hard-off →
  the same assertion. **The new second half** is the card's REFCRC
  shape ("a slot reused for the same LPID in the same epoch"): churn
  with the evictor on makes an older same-LPID same-epoch tree page
  durable at a superseded slot (400 big entries × two versions over a
  16-frame pool), and a directed MISDIRECTED fault plants those stale
  bytes on the referenced slot — checks (1), (3) and (4) pass (a valid
  page of the same LPID and epoch), so only the reference CRC
  distinguishes. Clean: XX001 at open, never served. Under
  REFCRC-SKIP, EPOCH-SKIP+REFCRC and ID-SKIP+REFCRC the stale page is
  accepted through the tainted read — served (`RoundOutcome.served`)
  and the round fails. This fills the directed column for all three
  reference-CRC mutants; D10b arm 4 stays as the X5-R5-specific probe.
- **D8's doc comment** claimed an end-to-end arm the body never ran;
  rewritten to state what runs (the reader-level arms) and where the
  end-to-end case lives (D9's recycle shape, `tests/store.rs`'s
  checkpoint-with-recycle, X-5c's multi-round campaign seeds).

## Gate REWORK on stage X-5b (2026-10-25 verdict, applied)

- **R1/R2 (mutant arms).** Every inverted or vacuous arm is gone: D10
  runs the real G3 shape (the recycle removes meta k−1's start segment,
  a LOST fault on meta k forces the rollback open, and
  START-SEGMENT-MISSING-OK's silent empty WAL loses the acked commits —
  O-3 (d) fails the round); D11 drives the real `BulkLoad` with
  PUBLISH-BEFORE-META's single site at the load's forced-meta wait
  (caught); D14's FREE-EARLY site now frees **at the flip** (the
  draft's just-before-step-5 window was two yields wide and no
  allocation could enter it) — step 2's own allocations reuse
  durable-referenced slots and M-NOINPLACE fires; and D17 runs through
  `run_round` with directed faults in **two halves**: the torn map leaf
  at `map_leaf_cap` 254 (clean XX001 never served; CRC-SKIP serves the
  tainted leaf — pinned on `RoundOutcome.served` — and fails the
  round; the EPOCH/ID single sites stay subsumed, the expected-0 hit
  rates demonstrated; LOST and MISDIRECTED arms) and the verification
  round's REFCRC shape (below). D10b exists (below). D7's
  multi-round arm and D16's pair are **reported** UNCAUGHT with
  arguments in their doc comments — never asserted as passes.
- **R3 (X5-R5's harness half).** `exception_set_r` implements all three
  conditions (the third — no M-NOINPLACE hit on the slot's writes — via
  the monitor's new per-write verdicts, `monitor::noinplace_slot_hits`);
  the R-taint is computed from the interleaved write/accept history
  (`DiskEvent::Accepted`, one `DiskEvent::Crash` per life) over every
  post-crash life — recovery's own accepts included, each nested crash
  restarting the window (`harness::r_taint_of`, pinned in
  `tests/sim.rs`); D10b runs the rollback shape end to end. (The
  rework's claim that "G4's catcher is D17's recovery-world serve" was
  wrong as then asserted — D17's arm took any failure; the
  verification round added the real pin, `RoundOutcome.served`, above.
  G5's function is pinned by `r_taint_window_semantics`; its call-site
  wiring is the uncaught probe reported to X-5c.)
- **R4 (the self-referential id check).** Every radix walk (open,
  amcheck, the harness walks and the monitor's shadow walk) derives the
  expected id **positionally** (`ckpt::radix_child_span`; the child of
  an inner at base `b`, index `i`, span `s` expects id `b + i·s`), and
  the builders' base formulas agree (the free-set inner's `2^depth`
  understatement fixed to `fan^(depth+1)`).
- **R5 (the BulkLoad deadlock).** Root cause: the S_L draw consumed a
  ts from the commit sequencer without marking it applied, so
  `visible_ts` stalled below every later commit forever (the publish
  commit parked in `mark_applied` on a ts that never arrives). The
  drawn ts is pre-marked applied; `ScriptOp::BulkLoad` runs (D11).
- **R6 (the instrumentation check).** `baton_violation_is_reported`
  gains a cross-thread arm (`baton_violation_cross_thread_holder_is_
  reported`): a world holder on another OS thread makes `acquire`'s
  `try_lock` loop report `Baton` at the call site. Demonstrated by
  hand: with the `try_lock` replaced by a blocking `lock`, the test
  hangs (the run was killed at 60 s with no result) and reverts clean.
- **R7 (RESULT accuracy).** B1's `git diff --stat` (4b1e6068): 14 files,
  +4935/−2702 ≈ 7.6k changed lines against the ~1.5k budget; the stage
  RESULT's mutant table reports the arms as above, item 3 includes
  D10b.
- **Minor.** `RoundOutcome.pass` folds `deadlock`/`panicked`/`baton`
  (the flag is now actually set — from task errors and from
  `StoreInner::baton_fail`, which the release paths record instead of
  swallowing); `EnvRef::accepted` propagates its acquisition failure
  (fail-closed on an oracle input); D1's clean arm asserts the monitors
  are empty; every new sim surface (`dwrites_of` history, `DWrite`
  pre/`all`/`Clone`, `apply_one_media_fault`, the Accepted/Crash
  events, the R-taint window) has its own test in `tests/sim.rs`
  (`apply_one_media_fault` is Work 6/D17–D10b's deterministic fault
  entry, the drawn-fault path's equal); the `WorldGuard` `Drop` comment
  states the real order.

**Failures found and fixed while reworking** (each pinned by the test
that exposed it): the `ckpt_open` strand (a checkpoint that fails
without a crash — the I-GCW fail-stop — left the flag set and parked
`wait_ckpt` forever; D12b's hang); the checkpointer swallowed non-crash
checkpoint failures (a poisoned round passed with no meta written —
now `task_error`); `RoundOutcome` never carried `meta_rollback`,
`rollback_slots`, `amcheck_i` or the crashed disk (the directed tests
judged defaults); the monitor's shadow walk recursed with the parent's
bytes, so `flat` never filled past one level and the durable slot set
undercounted every tree page.

## Stage X-5b (B1–B6)

- **B1 — the S-BATON conversion (Work 11):** every store operation
  restructured to lock-work-unlock steps over `Cx::with` (the
  lock-step-unlock shape of rule 3); `try_lock` at every host
  acquisition with `SimFail::Baton` naming the call site (rule 4);
  `pub mod store;` wired. `tests/store.rs`: create/open/scan, 10k
  inserts with leaf and root splits (scan equals a `BTreeMap`), a GC
  sweep with merges and root collapse, a checkpoint with a concurrent
  insert task and the background evictor at minimum `F`, reopen, and
  three seeds with no DEADLOCK/PANIC/Baton. Two new sim pins in
  `tests/sim.rs`: `baton_violation_is_reported` and
  `accept_hook_judges_older_tainted_read` (the B3 pin).
- **B2 — the store (Work 3 + 4, with Work 12):** the §2/§3/§4 codec with
  the four §3.3 checks as separate mutant sites, no-fallback open with
  G3, the pool (§5) with clock eviction, dirty eviction to fresh slots
  and the `R_EPOCH` epoch-copy reserve, the §5.6 gate, the flat map with
  the CoW §4.1 radix (Work 12's four conditions: the §4.1 byte layout,
  copy-on-write step 2 with unchanged leaves shared, `map_leaf_cap` ∈
  {8, 16, 254} giving ≥ 2 radix levels, `R_MAP = 0`), the B+tree with
  stable roots, N3=(b) entries, merge and root collapse, the §6.8
  directory with bounded DropTree walks, §10.2/§10.4 GC with `gc_w`,
  §12 WAL with framing/group commit/§12.6 replay start/recycling,
  §13 steps 0–6 with hooks, §14 recovery with G2 physical truncation,
  amcheck (3.8), the 27-mutant enum plus G7's META-WRONG-SLOT (28).
- **B3 — the round harness (Work 13):** `Script` tasks with commits,
  chunked transactions, bulk loads, GC, checkpoints and named park
  points (`chunked:premark`/`preapply`, `commit:preapply`,
  `writer:presync`, `split:mid`, `apply:mid`), writer/evictor/
  checkpointer service tasks, crash delivery at `CkptHook`s and task
  yield points, nested recovery crashes, the 2.3 extraction through the
  recovered store, X5-R5's `meta_rollback`/`R`/R-taint, the Work 2.1
  `written`/`acked` derivation from disk events, the twin supply, the
  canonical digest (determinism test in `tests/harness.rs`).
- **B4 — the monitors (Work 3.10):** M-NOINPLACE, M-META-ATOMIC,
  M-WAL-ORDER and M-CKPT-T over the disk events and the flip's reported
  enqueue position.
- **B5/B6 — the directed tests (Work 6 + 14):** D1–D18 in
  `tests/directed.rs`, each with its clean arm and (where caught) its
  mutant arm; the four uncaught arms are marked in place.

**Prototype deviations fixed in B5** (each pinned by the directed test
that exposed it): the stale-embed race of step 2 (the frozen image
table: the radix, the free set and the step-5 durable set read one
atomic snapshot and every referenced slot is pinned, so an eviction in
flight across the freeze cannot unpin or redirect an embedded ref);
`ckpt:end` now wakes `MetaDurable` (a completed checkpoint stranded its
forced waiter); `wal_enqueue`'s segment-boundary placement (a record
only lands in this segment when it ends at the boundary or leaves room
for the 45-byte SegmentEnd filler; the draft emitted the filler at `lsn0`
regardless of room, so a record could straddle the boundary while
carrying the next segment's lsn); the free-set page lifecycle
(`fs_building`/`fs_prev`: the step-5 durable set covers the image's own
free-set pages and the previous image's are unpinned — the leak made
every early free undetectable); the monitors' durable-image walk now
descends the directory's leaf cells into the trees; the Work 2.1
derivation stitches each WAL file's bytes from the write events and
walks records (the self-contained-batch parse missed every record a
writer's chunk split mid-record, and recycled segments need
file-relative positions).

## Stage X-5a (complete)

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

Not implemented anywhere yet: the campaign's **execution** (`run` over
seeds 0..10000, coverage tuning, the mutant sweep's records — the
machinery exists in `src/campaign.rs`; the X5-R6 guard stops their
first judged run), and the campaign RESULT items 2, 3, 4 and 7's
numbers. No test is `#[ignore]`d and no check weakened — the missing
parts are stopped at the card's own guard, not stubbed green.

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
- **The in-memory page map is flat** with per-entry image overrides;
  the persisted §4.1 radix is materialised at step 2 and parsed at open
  and by amcheck. Work 12's conditions hold: the persisted radix uses
  the §4.1 byte layout and passes §3.3; step 2 is copy-on-write (only
  the map pages whose content changed since image k−1 are rewritten, to
  fresh slots, up to the root; unchanged map pages keep their image k−1
  slots and are shared between the two images, the §4.4
  three-reference rule); `map_leaf_cap` ∈ {8, 16, 254} gives the radix
  ≥ 2 levels; `R_MAP = 0` (`F = tasks × (max_height + 2) + R_EPOCH`).
  **Not exercised** (A-3c's and A-7's): MapLeaf pool residency and
  pinning (§4.1, §5.4's `R_MAP`), map-page dirty eviction between
  checkpoints, and map-page epoch copies.
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
cargo test -q --release -p e4-recovery      # the whole crate (109 tests)
cargo test -q -p e4-recovery --test workload  # the Work 5 self-tests + smoke (9)
cargo run -q --release -p e4-recovery --bin e4-campaign -- verify \
  --results spikes/e4-recovery/results/campaign.json   # exits 2: no campaign
                                                       # exists (the X5-R6 guard)
```

