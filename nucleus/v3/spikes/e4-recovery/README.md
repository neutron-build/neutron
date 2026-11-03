# E4 recovery spike (card X-5, stage X-5c of three)

Status: **stage X-5c-F3-F3a — steps 0–4 done — step 0 committed
F3-F1's Acceptance 10k as the triage file of record (929ade38), the
X5-R33/X5-R34 rulings verbatim before any run (31effe00), F3-F1-F5's
record fixes (97e85cab) and the class table re-derived (b68d948c);
step 1 X5-R34 — I-ACK-PREFIX's `durable_end` from SimDisk history, the
judged `ack_over` cross-check, the 37-round table holding P's
prediction exactly (13da010b, b9d6824c); step 2 the exact-segment-end
hop clearing all four boundary store losses (ce8f8bbc, 51947e16); step
3 I-ACK-PREFIX with two or more batches in flight, interval-set
frontiers, the `writer:prewrite` pin (cbf0754c); step 4 per-boot ids —
the three-way boot-id rule, W4 (i)'s ordered stop, the five pins and
their five mutants, 186's shape gone and 405 moved (8b71e560 + this
commit's pins). Remaining: step 5 (the F3-F1-F3 pins + mf29), step 6
(the C-XX/C-DIV remainder pass), step 7 (the README) and the runner's
Acceptance. The stage before it (X-5c-F3-F1) is complete: steps 0–9
done — step 0 committed
F3's Acceptance 10k as the triage file of record (7b709ccb:
`E4 SEEDS=10000 O1_FAIL=2654 O2_FAIL=43 O3_FAIL=559 DEADLOCK=0
PANIC=0 O1F_FAIL=2665 MONITOR=0` / `E4 FOLD FAILING_SEEDS=955
FAILING_ROUNDS=914 TWIN_FAILING_ROUNDS=71 DEFECT_SEEDS=0`, sha256
`efb7835e…73906c5`) and re-derived the class table (served rounds split
out first); step 1 the WAL's serialized writer (§12.4 contiguous ack,
e6ebbdda) with its two-task pin and the ESCALATED ack cross-check
(55 disagreeing commits over 4,656); step 2 SegmentEnd's valid end
(001d3b7f) — 6149 r1's serve is gone, now MW-ORPHAN's shape; step 3
G-2 pinned, G-1/G-1b demonstrated equivalent (G-1 and a §12.2/§12.5
silence ESCALATED), 26640ebf; step 4 the twin supply carries a failed
recovery's reads (e81ad13d); step 5 X5-R32 transcribed literally
(e34b75ef — the one oracle commit; the 17 named rounds: 4 ruled, 13
moved, no ESCALATE); step 6 X5-R30 (c)'s skx/smx/pass_na fields
(86b716b4; skx 1897/4718, smx 2927/7528/8728, all pass_na=true); steps
7–8 within X5-R19's budget: C-XX sub-shape 1 root-caused (the
directory's duplicate re-put, §6.8) and fixed with its pin, sub-shape 2
(a replay-made stale parent) and C-DIV's remainder ESCALATED with
evidence; step 9 this README (the duplicate table removed, the
MW-ORPHAN column corrected, the rulings copied verbatim). The stage
before it (X-5c-F3) is complete and merged (all
its steps 0–8 done): step 0 committed
F2's Acceptance 10k as the triage file of record (b9dd5a9a:
`E4 SEEDS=10000 O1_FAIL=2712 O2_FAIL=101 O3_FAIL=624 DEADLOCK=41
PANIC=0 O1F_FAIL=2723 MONITOR=0` / `E4 FOLD FAILING_SEEDS=1070
FAILING_ROUNDS=1015 TWIN_FAILING_ROUNDS=103 DEFECT_SEEDS=0`, sha256
`56e47fb2…f34033`) and re-derived the class table; step 1 fixed two
C-XX root causes with pins (ad82f7d5: the `next_lpid` shrink — a
prototype deviation from §4.2/§4.3; open's G3 probe swallowing a
delivered recovery crash — an env/store instrumentation defect) and
escalated the O-3(c) cross-round inherited-damage family; step 2
recorded C-DIV's evidence (an acked SyncOn commit lost through §12.5's
replay stop — seed 21) and escalated it; step 3 applied G8 (89787d24:
an XX001 inside a gated operation poisons the store, the poison ends
the workload at the crash point `poison`); step 4 escalated all 7 MP-A
rounds with the X5-R16/X5-R30 table; step 5 recorded MW-ORPHAN's
five-round evidence (never fixed); step 6 classified the twins against
the primary classes (no separate fix); step 7 recorded the other
rounds with their families and escalated MP-E shape C unchanged; step
8 wrote the F4 hand-off. The last class completed: **step 8 (the
hand-off)**. The Acceptance 10k and its `verify --no-quotas` are the
runner's (X5-R31): no campaign-class command ran in this card's
attempts (single-seed and ≤ 200-seed runs only, under the backstop,
output outside the worktree). The stage before it (X-5c-F2) is
complete and merged (all its steps 0–4
done, amended by
nv3-X-5c-F2-amend-1, 2026-10-31) — step 0 corrected the
digest-stability step-4 record (54 rows, from the X-5c-F1 gate's truth
file); step 1 landed the harness hardening (a)–(e) with pins; step 2
landed X5-R28's kept set `K` (harness + the one oracle commit) and
X5-R30 (a)'s `served_k` record field — the six completeness-residual
seeds clear via K, the MP-A served_k table is recorded for F3; step 3
took X5-R29's branch (c) — seed 4858's faultless twin holds the same
two orphan map entries, so amcheck is unchanged and shape C is handed
to F3 with the per-lpid evidence; step 4 is this stage record. The
Acceptance 10k and its `verify --no-quotas` are the runner's (X5-R31):
no campaign-class command ran in that card's attempts. Its triage file
of record was P's c8e578b7 run:
`E4 SEEDS=10000 O1_FAIL=2711 O2_FAIL=101 O3_FAIL=635 DEADLOCK=41
PANIC=0 O1F_FAIL=2722 MONITOR=0` / `E4 FOLD FAILING_SEEDS=1088
FAILING_ROUNDS=1034 TWIN_FAILING_ROUNDS=103 DEFECT_SEEDS=0`, digests
reproducing (`verify --no-quotas`, exit 0). Per-class state: shape A
**fixed** (837200a7, accepted X5-R24), shape B **fixed** (80dc0ff3,
accepted X5-R24, the anchor-0 judgement call left for P's gate), X5-R21
(c) **fixed and pinned** (04063d1b, accepted X5-R24), the completeness
residual **ESCALATED to P** as an oracle question (X5-R23; 31d68b5f
reverted — no harness-side ground truth may stand in for amcheck),
shape C **ESCALATED to F2** evidence-only (X5-R24: 6 rounds, the
provisional classification is a store-side map-exactness deviation),
NEVER-FIRED and NO-TWIN standing (0 defects / 0 no-twin rounds in the
esc-2 file). Not this card (X-5c-F2's): C-XX, C-DIV, MP-LIVELOCK,
MP-A, MW-ORPHAN, the twins, the retune (X5-R17), the sweep, the
RESULT.** The stages before it (X-5a, X-5b) are complete and merged.
Opus judges E4 on the RESULT with P's rulings X5-R1–R27.

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

## P rulings X5-R14–R19, verbatim (nv3-X-5c-esc-3, 2026-10-31; binding)

Copied verbatim from the card X-5c-F1 (its "Carried: X5-R14–R19" section, itself verbatim from the ruling) before any other change of this stage, per the card order step 0. The full record is `nucleus/v3/verdicts/X-5c-esc-3.md` in the main checkout.

**P rulings on run 4's ESCALATE (nv3-X-5c-esc-3, 2026-10-31): binding, read before anything else.** The full record is `nucleus/v3/verdicts/X-5c-esc-3.md` in the main checkout. Everything binding is below. Run 4 committed the first complete 10k campaign (d2c8bba7), recorded its failing classes (a06e7ca4) and a seventh class, non-reproducing digests (66f33c32), and applied X5-R12's budget valve correctly. P triaged from the committed file, read-only (a latency measurement held the host). Every ruling below was made **after** seeing campaign data, so each one is an instrumentation fix, a reporting correction, a tightening, or a conditional design fill with its basis stated. None loosens X5-R1–R13, the oracle or the quotas.

**X5-R14 (non-reproducing digests: an instrumentation defect that voids the campaign as a RESULT input).**
- **The finding.** `verify` re-ran d2c8bba7's 10,000 seeds. Seeds 180, 559, 7474 and 8832 gave different digests. All four are multi-round seeds that passed every round in the committed file. Three of the four have nested recovery crashes.
- **The ruling.** The digest is a pure function of `(seed, mutant)` (S-DET/W-DET, Work 7), and `verify` is an Acceptance command. A non-reproducing seed means that some sim-visible decision depends on OS timing. That decision can sit in any seed, not only in the four that showed it.
  - So `results/campaign.json` at d2c8bba7 is **triage data only**. No RESULT count may come from it, except as development failures in RESULT item 5.
  - This is fixed **first**, before any other fix is verified.
- **Root-cause it; do not mask it.**
  - **Method.** Re-run each of the four seeds repeatedly while a 16-thread `run` of other seeds loads the host, until two digests differ. Then compare the two runs' per-world schedule traces (`SchedCore.trace`) and a host-mutation log. The log is diagnostic only: never a digest input and never committed enabled. The first divergence names the cause.
  - **P's leads (not a ruling of cause):**
    - (a) **The release paths.** The crash, deadlock and livelock paths (`deliver_crash`, the crash and budget branches of `yield_baton_budget`, and `handoff_locked`'s deadlock branch) all release every task at once with `running = None`. From then on, the unwinding tasks run **concurrently**. Any host mutation on an unwind path happens in OS order. That includes store error-path cleanup, `finish`, harness records, and an `E4Env` call, because `Sched::acquire` does not check `crashed`.
    - (b) **Nested recovery crashes**, which three of the four seeds have.
    - (c) **Reads outside the baton.** Anything that reads `finished` or `waiting` outside the baton.
- **The fix.** It makes every sim-visible decision a function of `(seed, mutant)`. One example is a deterministic unwind: the baton passes through the unwinding tasks in `TaskId` order. Another is refusing, loudly, every host mutation after `crashed` other than the scheduler's own. The worker chooses, and the README documents the choice.
  - The fix may touch `src/sim/sched.rs` and `src/harness.rs`.
  - `taint_ranges`, the oracle and the model stay frozen.
- **Pins.**
  1. **A structural test with a literal config** that fails deterministically on the pre-fix code. For example: two tasks whose unwind paths each perform a host mutation, and the test asserts their order, or the refusal. Revert the fix to show the test fail, then restore it.
  2. **Stress evidence** (README, not `cargo test`): the four seeds re-run 25 times each at 16 threads under a concurrent 16-thread `run` load give one digest per seed.
  3. **Digest stability.** Compare a fresh 10k `run` at the fixed code with d2c8bba7, seed by seed. List every seed whose digest changed, and explain each change by the fix's mechanism. A fix that shifts many digests without an explanation is not root-caused.
- **Classification for RESULT item 5:** an instrumentation defect (sim scheduler or harness).

**X5-R15 (counting, reporting and the record: corrections, not rule changes).**
- **(a) The rule's n on its own line.**
  - In d2c8bba7, the final line's `O1_FAIL=2712` counts every record that carries an O-1 text. That includes 1,315 MP rounds that **pass** under the frozen pass fold: they are recovery-`Err` rounds, carrying the Err branch's default O-1(a) text.
  - Keep the Work 7 line exactly as it is. After it, print one more line: `E4 FOLD FAILING_SEEDS=<n> FAILING_ROUNDS=<n> TWIN_FAILING_ROUNDS=<n> DEFECT_SEEDS=<n>`. Its counts come from the pass fold and the seed-level `defect`/`deadlock`/`panic` flags.
  - RESULT item 7's n is `FAILING_SEEDS`. The per-check round counts that the fold counts go beside it.
- **(b) `verify` reports everything before it exits.** It always prints the outcome of all three checks before exiting non-zero: the seed set; the digests, with the list of non-reproducing seeds; and the full quota table.
  - `verify --no-quotas` checks the seed set and the digests, and prints the quota table without failing on it. It exists only for X-5c-F1's Acceptance. X-5c's own Acceptance keeps plain `verify`.
- **(c) The README must be accurate.**
  - **The quota paragraph.** It says "the digests reproduce … the determinism pins hold". That is false: X5-R14. Correct it.
  - **Every failing round belongs to a listed class.** The a06e7ca4 list left out three groups:
    - MP O-3(a), **served corruption**: 9 rounds;
    - O-3(c), XX001 with no damaged referenced slot: 25 MP and MW rounds;
    - O-3(c), "no twin (harness defect)": 19 rounds.
  - **The failing-seed count** is 1,598 once the seed-level `defect` seeds are included, not 1,560.
- **(d) RSS.** In run 3, the progress lines' `rss=` rose steadily from 3.5 to 5.3 GiB, about 180 KiB per seed. RESULT item 1 reports this beside the peak. It is not a blocker under X5-R13's 24 GiB budget.
- **(e) Scratch out of the crate.**
  - Delete `restore_probe.local.py` (untracked).
  - `examples/` keeps only what the README's "How to reproduce" uses. 823d4f51 called `examples/dbg-seed.rs` scratch, to be removed.

**X5-R16 (the failing classes: P's provisional classification, the evidence, and the owner).** The repro seeds are d2c8bba7's. Before root-causing any class, confirm that its repro reproduces twice at the fixed code from X5-R14. If a fix moved a repro, re-derive it from the triage file. Each class is either root-caused (the cause at file:line plus the S-STORE § it deviates from, the classification, the fix, and a literal-config pin that fails on revert) or escalated with the evidence listed for it. Under X5-R3, the judgement classifies. The worker's classification is provisional until P's gate.
- **NON-REPRO.** X5-R14 above.
- **NEVER-FIRED (instrumentation: workload).**
  - **The count.** 150 seeds carry the defect "the crash spec never fired": 110 class C, 35 twin rounds, 5 other MW rounds. 146 of the 150 target hook `Mid2`.
  - **The cause.** The targeted checkpoint never reaches that hook. Its step 2 has too little to write.
  - **The fix.** Make every targeted draw reach its hook, deterministically from the seed. A deterministic counting pass, like the uniform draw's, is allowed.
  - **Pin and target.** A literal-config pin. The final file has `DEFECT_SEEDS=0`.
- **NO-TWIN (instrumentation: harness).** 19 MP and MW rounds. The oracle's own text is "MP/MW round with XX001 and no twin (harness defect)". Supply the twin, pinned.
- **MP-E: O-3(e) under page damage (provisionally instrumentation: harness).**
  - **The data.**
    - **Exactness.** 199 rounds report "amcheck reported slot … neither tainted nor in R" (4 of them from (ii)). Of these:
      - 195 are in Q14 meta-fault rounds (33% of all Q14 rounds);
      - **195 are recovery-`Err` rounds**;
      - only 39 are meta rollbacks;
      - **none has a non-empty R**.
    - **Completeness.** 205 rounds report "tainted referenced slot … not reported", and **every one is a recovery-`Err` round**.
  - **The lead.** The image that `harness::durable_image` and amcheck (i) analyse is not the image the store opened, or tried to open. Two candidates:
    - meta selection when a meta slot is faulted;
    - R11 (a)'s rewritten loop over the flat map.

    The lead is not R5's text. R5 says that if open fails on both metas, O-3(e) is vacuous.
  - **Seeds.** Exactness: 37, 38 and 67, each round 0. Completeness: 47, 118 and 198, each round 0.
  - **The limit.** R5 is not reopened. Suppose a finding lies outside R5's **text** with R computed correctly. Then it is a failure. No worker widens R. ESCALATE with the round.
- **C-XX: XX001 or amcheck findings in class C, with no fault at all (provisionally a prototype deviation; the gravest class).**
  - **The data.**
    - 133 class C rounds: O-3(b) at `Open` in 53, at `AmcheckIi` in 80.
    - O-2 "amcheck not clean" in 98 class C rounds.
    - 25 MP and MW rounds where an XX001 has no damaged referenced slot (O-3(c)).
  - **What it means.** A referenced page fails §3.3 with no media fault. That violates I-FREE-SAFE, I-NO-INPLACE, I-META-ATOMIC or I-FREE-EXACT, or the sim's F-CRASH semantics.
  - **The skew.** Q10 rounds (an epoch copy dirty-evicted into the image map) fail at 6× the base rate. That points at the write-guard and epoch-copy path.
  - **Seeds.**
    - 221, round 0: recovery returns `Err` at `Open` after `After0@1` with no fault, the simplest shape;
    - 66, round 0;
    - 252, round 0.
  - **A root cause in the sim's crash or fault model** is ESCALATE. P fixes the sim, as with X5-R9.
- **C-DIV: the recovered state differs from the model in class C (provisionally a prototype deviation or a harness defect).**
  - **The data.** 474 class C rounds fail O-1:
    - (b) scan mismatch: 262;
    - (d): 58;
    - (c): 93. In every (c) round the reported max ts is ≤ the frontier, which is `hi`. So (c) is the symptom reported for a lower candidate, not an orphan.
  - **The rate by round index:** 1.7% for round 0, 2.6% for round 1, **20.2% for round 2**. So the lead is state that crosses rounds:
    - the harness checkpoint between rounds;
    - the carried model;
    - X5-R10's sequencer restart;
    - G2 truncation.
  - **Seeds.** 21 round 0, 46 round 1, 36 round 2, 104 round 2.
  - **The twins.** The faultless twins' failures (46 MP and 54 MW twin rounds; 4 of them carry no O-1 text and belong to NEVER-FIRED) are classified against C-DIV or C-XX, or as a class of their own.
- **MP-LIVELOCK: 46 DEADLOCK/livelock rounds (lead: a gap in S-STORE §9.2; conditional fill G8).**
  - **The data.**
    - All 46 are MP, at round index 1. **Every one comes directly after an R6 round**, recovered `Ok` with a detected loss: 46 of the 128 MP rounds that follow an R6 round 0.
    - None records a client XX001.
    - Trip sites: `h:writer` 20, `h:ckpt` 16, `h:evictor` 10.
  - **The lead.** The store keeps running on an image that holds an unreadable referenced page. An XX001 inside apply, eviction or a checkpoint does not poison the store: the prototype poisons only on `gc_w > T` (`ckpt.rs:582`). The failing task leaves, and the rest park forever behind the service spins.
  - **Seeds.** 148, 987 and 2307, each round 1.
  - **Conditional fill G8 (P's reading of I-FAIL-STOP).** It applies only if the root cause is confirmed as an XX001 inside apply, eviction or a checkpoint that does not poison the store. §9.2 names "an I/O error, 53100 or 53200" and does not name XX001. P's reading: a read that returns bytes failing §3.3 is a failed physical read, so XX001 there poisons the store.
    - **What the harness does.** It treats a poisoned store as the end of the round's workload. It delivers the round's crash at once and records the crash point as `poison` (never as NEVER-FIRED). Then the faults, the recovery and the oracle run as usual.
    - **Where it goes.** G8 is listed in RESULT item 6 and carried into S-STORE §9.2 by spec card S-1-F3.
    - **Any other root cause.** Fix it if it is a prototype deviation; otherwise ESCALATE.
- **MP-A: served corruption, 9 rounds (the plan's clause 3; P classifies).**
  - **The rounds.** Seeds 998, 1897, 2927, 3777, 4327 and 5488 (round 0 each), plus three more in the file. Every one is a Q14 meta-fault round.
  - **What to record, per round:**
    - the slot;
    - whether it is in R or is tainted;
    - the reference that was accepted, and the bytes and CRC on disk;
    - the read path.
  - **Then:** fix a prototype deviation found here with a pin. Either way, **ESCALATE all 9 with this evidence.** The worker never classifies a served corruption as a non-failure.
- **MW-ORPHAN: 437 MW rounds fail O-1(c) "raw entry above the cut" with O-3(d) (P's provisional classification: SPEC, an E4 finding against S-STORE §13/§14).**
  - **The data.**
    - In 422 of the 437, the orphan's max ts is above the recovered frontier, and every one is ≤ `hi`.
    - 434 are Q9 rounds (the image holds part of a commit > T).
  - **The mechanism.**
    - I-CKPT-AT-T (b) rests on step 1's guarantee that every commit > T with entries in the image has a durable record.
    - Class MW destroys a record after its sync, by construction.
    - §14 replays up to the first damaged record (`ckpt.rs`: `break 'outer`). It has no rule for ordinary image entries above the recovered frontier.
    - X5-R10 then restarts the sequencer at frontier + 1, inside the orphans' ts range: an I-ENTRY-UNIQUE hazard.
  - **Why X5-R1 does not cover it.** X5-R1 makes lost acked commits a known cost. A non-prefix state is not a lost acked commit, so it stays a failure.
  - **The worker does not fix it.** Any fix (making entries above the frontier dead, a sequencer floor above them) is a mechanism beyond G1–G7.
  - **Evidence instead,** for seeds 39, 59 and 109 (round 0 each) and two more:
    - T, the frontier, and every orphan entry (tree, key, ts);
    - the commits and records that were damaged, with the fault kind and LSN;
    - proof that replay stopped at the first invalid record, per §12.5 (no store deviation);
    - whether a later round reissues an orphan's ts.
  - **The other 15 rounds** (max ts ≤ the frontier) are root-caused as their own class.
  - **On confirmation,** P opens S-1-F3.

**X5-R17 (coverage, after the classes).**
- **When.** Retune (Work 5.6) only after every X5-R16 class is fixed or escalated. A retune changes every seed's workload, and the repro seeds above hold only for d2c8bba7's workload.
- **The targets:**
  - **SIZING**, per seed, as Work 5.3 says: every seed reaches ≥ 4·F distinct dirtied LPIDs in at least one checkpoint interval. d2c8bba7 reached it in **362 of 10,000 seeds**, so the plan row's defining condition ("a dirty set of 4× the pool") is still untested. `verify`'s SIZING row counts seeds.
  - **Q4:** 0 of 1,000. It is the plan row's "splits and merges during a checkpoint".
  - **Q7:** 155 of 200.
  - **Q11:** 0 of 100.
  - **Q12:** 2 of 100.
- **The pool stays at §5.4's formula, which is its minimum.** Reach 4·F with wider or longer checkpoint intervals, never with a smaller pool.
- **The time budget.** Work 5.3's limit of 20 min becomes **≤ 40 min wall at 16 threads**, so that `verify` stays inside the runner's 60-min limit. This is a time budget, not a correctness rule.
- **The record.** Every retune is listed in RESULT with the quota that prompted it.
- **New classes.** A failing class that first appears on the retuned workload gets X5-R16's treatment.

**X5-R18 (structure: split; this supersedes X5-R12's "do not split").**
- **Why.** X5-R12 held because the remaining work was a known campaign. What remains now is diagnosis:
  - one determinism defect;
  - eight failing classes, at least one of them spec;
  - a retune that the plan row's headline condition depends on;
  - the sweep.

  X-5c's Acceptance (`verify` and `mutants --check`) cannot pass until all of it is done. So every relaunch fails by construction and comes back to P.
- **Two stacked cards**, each with an Acceptance it can pass and its own P gate:
  - **X-5c-F1** (Base `card/v3-X-5c`): X5-R14, X5-R15, a fresh triage campaign, and the instrumentation classes (NEVER-FIRED, NO-TWIN, MP-E).
  - **X-5c-F2** (Base `card/v3-X-5c` after F1 lands): the store and design classes (C-XX, C-DIV, MP-LIVELOCK, MP-A, MW-ORPHAN evidence, the twins), then X5-R17's retune and a full 10k `run` that `verify` passes.
- **Advancing X-5c's branch.** After each stacked card's P gate returns MERGE, the orchestrator fast-forwards X-5c's branch to it: `git -C ~/.cache/nv3-cards/X-5c merge --ff-only card/v3-X-5c-F<n>`. Never a force, never a rebase.
- **Then** X-5c is relaunched `--keep` for what is left:
  - the final `run`, only if code changed after F2's committed file;
  - `mutants`;
  - the README and RESULT.
- **Spec findings** go to **S-1-F3**: P-authored, S-STORE §9.2 (G8) and §13/§14 (MW-ORPHAN), opened by P on F1/F2's evidence. If S-STORE changes, the prototype implements the change and the **whole** campaign is re-run (X5-R3).

**X5-R19 (stop clauses inside the stacked cards).**
- **X5-R8 still governs a stop clause.** A worker who reaches one does not decide it.
- **An escalation about one class does not end the attempt.** Record it in the README with its evidence, and continue with the next class.
- **At the end of the attempt,** if any class is escalated, finish the attempt's remaining Work, then ESCALATE.
- **Never** fix around an escalated class. Never exclude its seeds, retune to avoid it, or re-run only some seeds.

## P ruling nv3-X-5c-F1-esc-1, verbatim (relaunch, 2026-10-31; binding)

Copied verbatim from the card before any other change of this relaunch
(step-0 discipline, as X5-R14–R19 before it). The full record is
`nucleus/v3/verdicts/X-5c-F1-esc-1.md` in the main checkout.

**RELAUNCH `--keep` under nv3-X-5c-F1-esc-1 (P, 2026-10-31): binding, read this first.** Run 2 committed steps 0–5 (tip c3f292cc), and P's rustfmt P-commit 6e689374 made the full gate green (`GATE EXIT=0`). But the Goal is **not** met, for two reasons:
- **X5-R16 MP-E.** The exactness escalation rests on seed 37, which **passes** in the committed triage file bc4f3bba. Even where it held, that evidence covers only 39 of 222 rounds. The completeness residual (6 rounds) is neither root-caused nor escalated.
- **RESULT item 1.** It lacks X5-R14 pin 3's seed list.

**Resume at step 4 (MP-E), then step 5.** Set the README's Status line to name this relaunch. Steps 0–3 and NEVER-FIRED/NO-TWIN stand, except where X5-R21 (b)/(c) touches them. X5-R1–R19 bind unchanged. The full record is `verdicts/X-5c-F1-esc-1.md` in the main checkout; everything binding is carried below.

P's reading of the committed file (bc4f3bba), carried for X5-R20:

**The class in the committed file** has 222 rounds of "amcheck reported slot … neither tainted nor in R". All are MP. They split into three shapes:

| Shape | d2c8bba7 | bc4f3bba | Examples (bc4f3bba) |
|---|---|---|---|
| **A** reported slot is a **meta slot (1 or 2)**, `mrb=false`, `rlen=0` | 156 | **177** | 67, 268, 328, 337, 377, 378 (round 0, slot 1) |
| **B** a non-meta slot, `mrb=true`, `rlen=0` | 39 | 39 | 38, 137, 598, 738 (slot 4), 1038 (slot 60) |
| **C** a non-meta slot, `mrb=false` | 4 | 6 | 688, 3718, 4858, 5238, 5638, 9717 |

- **The evidence covers shape B only.** The worker's escalation rests on one seed, 37 round 0 (`mrb=true`, slot 11, "C-XX's mechanism inside a Q14 rollback round"). So at most 39 of 222 rounds are covered.
- **That one seed no longer reproduces.** In the committed triage file, seed 37 round 0 **passes** (`mrb=false`, no O-3 text). The fixes moved it, and the README still lists it as "the deterministic repro: `run --seeds 37..38`". That breaks X5-R16's instruction: "confirm that its repro reproduces twice at the fixed code … If a fix moved a repro, re-derive it from the triage file".
- **Shape A, 80% of the class, was never examined.** In it, amcheck reports a **meta** slot as a finding, there is no meta rollback, and recovery returns `Err` (216 of the 222 rounds carry the recovery-`Err` default O-1(a) text). P's ruling named seed 67, a shape-A seed, as a repro, and it still reproduces (its digest is unchanged).
- **Shape A's lead** points back at this card's own MP-E completeness fix, 12b54965. That fix changed "the referenced set is now the image's own slots (map, free set, **metas**)". With the metas in the referenced set, the question is what amcheck (i) should say about the meta slot that open did **not** select, or that it rejected, in a Q14 round whose fault is on a meta slot. This is a harness/amcheck-input question. It is X5-R16's original lead ("the image that `harness::durable_image` and amcheck (i) analyse is not the image the store opened"), not C-XX.
- **The completeness residual** is 6 rounds, all `mrb=true` and `rlen=0`: 1787, 5808, 6157, 8598, 9098, 9647. They are neither root-caused nor escalated with evidence.

**X5-R20 (MP-E is not finished: re-derive, cover every shape, then fix or escalate).**
- **Seeds.** Re-derive the repro seeds from bc4f3bba's triage file, and confirm each reproduces twice:
  - shape A: 67 and 268;
  - shape B: 38 and 137;
  - shape C: 688 and 3718;
  - completeness residual: 1787 and 6157.
- **Shape A first.** For seed 67 round 0, record:
  - which meta open selected, and why the other was rejected;
  - which meta slot amcheck (i) reported, and what it read there;
  - the fault list;
  - whether that slot's bytes are the last bytes the store issued to it.

  Then classify. If the cause is the harness feeding amcheck a slot outside the opened image (an instrumentation or harness defect), fix it with a literal-config pin that fails on revert, as X5-R16 requires.
- **Shapes B and C, and the completeness residual.** Root-cause each, or escalate each with its **own** current-file evidence. Seed 37's stale evidence may be kept only as history.
- **The limits are unchanged.** R5 is not reopened and R is not widened. A served corruption is never classified as a non-failure.
- **If any MP-E fix changes code:** re-run the 10k triage campaign under X5-R13's backstop, commit it, `verify --no-quotas` it, and re-derive F2's hand-off seeds from it.

**X5-R21 (record corrections: no code unless stated).**
- **(a) Digest stability.** Commit a seed list under `spikes/e4-recovery/results/` (for example `digest-stability.tsv`: seed, old digest, new digest, cause). Each change is attributed to exactly one cause:
  - NEVER-FIRED re-draw: the round's crash spec changed and the old round was unfired;
  - the X5-R14 entry park: uniform `k` changed, or the schedule changed with the spec unchanged;
  - NO-TWIN supply;
  - MP-E referenced set.

  To separate X5-R14's own shift, run a 10k `run` at **9b041a3d** (the X5-R14 fix alone) into a file **outside the worktree**, and diff it against d2c8bba7.
  - Use a detached checkout outside the card worktree: `git worktree add --detach $HOME/.cache/nv3-review/x5cf1-r14only 9b041a3d`.
  - Give it its own `CARGO_TARGET_DIR`, and run it under X5-R13's backstop, in the foreground.
  - Remove the checkout afterwards with `git worktree remove`.
  - Commit only the derived list, never that run's file. Then demonstrate the "stably wrong old order" reading on two changed seeds that d2c8bba7's `verify` did reproduce: show that the old code's `E4_SCHED_LOG` has the pre-baton `h:entry`-class step at the same position on every run.
- **(b) The README's NEVER-FIRED record** must describe the code as it is (6cc53657 and 4fedc648), including the uniform twin wrap.
- **(c) The real-round-only retry** (2b (i)) is a Work 2.5 pairing break. Fix it: when the real round re-derives a targeted spec, the twin re-runs at that same spec, from `tch_retry`, before the real round uses its supply and nested points. Pin it with one of 2669, 4049, 4309, 7469 or 9809, and show the pin failing on revert. This is a code change, so X5-R20's campaign re-run applies.
- **(d) Fix the twins count** (102 rounds, 91 seeds, 44 MP + 58 MW). Answer 2e's re-seed question.

**X5-R22 (run hygiene for this card).**
- Run every Acceptance-class command (the full gate, `verify`, 10k runs) in the **foreground**, in one shell call that blocks until it exits. Never end a turn while a job you are waiting on is still running in the background: the runner kills it.
- After any edit, including a lint fix, re-run `cargo fmt --all -- --check` and `cargo clippy -q --workspace --all-targets -- -D warnings` before committing.
- Read gate failures with `grep 'STEP .* EXIT=[1-9]'` on the full log. The runner's retry message shows only the tail.

## P rulings X5-R28–R31, verbatim (nv3-X-5c-F2-amend-1, 2026-10-31;
## binding)

Copied verbatim from card X-5c-F2 before any Acceptance output was
read. They supersede that card's earlier Goal, which predated esc-1
and esc-2.

**X5-R28 (O-3 (e) completeness in class MP: the kept set `K`; the meta slots leave the per-slot halves).** This tightens X5-R2/X5-R5 where they were circular, and corrects them where they were unsatisfiable. It is P's, and is recorded under RESULT item 6 as a post-data oracle amendment. The six rounds were seen before it was made. Every campaign from F2's on was not.
- **Why.** The frozen completeness half requires amcheck (i) to report every referenced slot that is tainted (1.4: the durable bytes differ from the **last bytes issued**). After a meta rollback, a slot that the newer meta freed and the store reissued, where the reissue **did not land** (dropped by F-CRASH, or a LOST fault reverted it), is tainted, yet its bytes are the opened image's own content. §3.3 passes, and no correct amcheck can report it. This is the mirror of X5-R5's R: there the reissue landed, so the slot is untainted but has a stale reference. Seed 1787 round 0 (P, cd3c60e9): a MISDIRECTED fault clobbers meta slot 2 (k=3), and open selects k=2. Slot 10's durable bytes are dw[22], before k=2's meta write. Its reissue at ev[2415], after k=3's graduation sync ev[2402], was dropped by the After6@2 crash. `§3.3=true tainted=true`.
- **(a) The kept set `K`.** The harness computes it from SimDisk history only: never from store memory, never from §3.3 or any amcheck output, and never from the taint map except as condition 1 below. `K` is non-empty only when X5-R5's conditions 1 and 2 hold. It is the set of slots `s` such that:
  1. `s ∉ {0, 1, 2}`, `s` is referenced by the opened image, and `s` is tainted at amcheck (i);
  2. **the durable bytes of `s` at (i) equal `V_k(s)`, byte for byte.** `V_k(s)` is the page's content as the durable-write history had it when the opened meta was written: start from 4096 zero bytes and apply, in order, every `DWrite` of the page file that overlaps `[s·4096, (s+1)·4096)` and precedes the opened meta's own `DWrite`. The opened meta's `DWrite` is the last `DWrite` at offset 4096 or 8192 whose bytes parse as a meta with the opened `ckpt_id`. If no such `DWrite` exists, `K` is empty for the round: fail closed;
  3. SimDisk records a page-file write to `s` issued after X5-R5's anchor: the first completed `sync_all` covering a meta newer than the opened one. Use the same anchor function as R (`exception_set_r`, after step 1 (d)/(e));
  4. no write to `s` drew an M-NOINPLACE hit.

  So `K` ⊆ tainted ∩ referenced, and `K ∩ R = ∅`. At the (ii) point, `K_ii` is computed the same way, against the end image's meta (`durable_image`'s third value on the end disk) and the (ii) taint.
- **(b) The oracle amendment.** The frozen `src/oracle.rs` and `tests/oracle.rs` change **only by this text**, in one commit whose subject names X5-R28:
  - `Extracted` gains `kept_slots` and `kept_slots_ii`, each a `BTreeSet<u64>`, with doc comments citing X5-R28.
  - **Structural guard:** a `K` slot that is not referenced, not tainted, in R, or 0, 1 or 2 fails with `O-3(e): harness K set malformed (slot N)` (and likewise for (ii) against its own sets). It is checked whether or not `r_active` holds, like R's guard.
  - **The completeness halves, (i) and (ii):** skip slots 1 and 2. When `r_active` holds, also skip slots in `K` (or `K_ii`). Every other tainted referenced slot must still be reported.
  - **The exactness halves (a tightening):** when `r_active` holds, a slotted finding on a `K` slot fails with `O-3(e): amcheck reported kept slot N (its bytes are the opened image's content)`. The `tainted ∪ R` rule stands for everything else.
  - **X5-R6's `explained` (a tightening):** a tainted slot in `K` does not explain an O-1 read's XX001 when `r_active_r6` holds. The rule becomes "some tainted referenced slot outside K, or an active non-empty R".
  - `tests/oracle.rs` gains these cases on `rollback_world`. Each is demonstrated against its revert:
    - mf20: a tainted slot outside K that is not reported fails;
    - mf21: a reported K slot fails;
    - mf22: a malformed K (an untainted slot) fails;
    - mf23: an unreported K slot with condition 1 false (no meta-slot fault) fails;
    - mp7: an unreported K slot passes.
    `instrumentation_removals_are_load_bearing` gains the K arm.
- **(c) The harness.** Delete the meta-slot rollback-fallout loop (`durable_image`, "stale durable meta slot (rollback fallout)"). The walk never pushes a finding derived from `range_tainted`, `issued`, `dwrites` or any other ground truth. Slots 1 and 2 stay in `image_slots`, because O-3 (c)'s twin rule and R's guard read that set. Only the per-slot completeness halves skip them, under (b). A meta-slot fault is judged where it acts: open's selection (S-STORE §2.2), O-1 (the MP lower bound) and O-3 (a)–(d).
- **(d) No other clause moves.** X5-R28 does not touch O-3 (a), which is X5-R30's question, nor O-1, O-2, O-3 (b)–(d) or R. It is empty in classes C and MW. **Never** satisfy completeness by reporting ground truth (X5-R23 stands).
- **(e) The decision rule, pre-registered.** At this card's tip, seeds 1787, 5808, 6157, 8598, 9098 and 9647 must clear O-3 (e) **because their named slot is in K**. Their single-seed records show `K ∋` that slot, and the round's other outcomes are recorded unchanged. Any of the six whose slot is **not** in K stays failing. Record its `V_k(s)` against the durable bytes, then ESCALATE it to P with that evidence. Never widen K to catch it.
- **Why it is neither circular nor unsatisfiable.** The expected set (tainted \ K) comes from the write history. The observed set comes from amcheck's §3.3 verdicts against the store's own references. They agree exactly when the store's references are the image's true content, which is what I-NO-INPLACE and I-META-ATOMIC promise. If a reference is wrong, the two sides disagree, and either the completeness half or the exactness tightening fails.

**X5-R29 (shape C: test the subtree-fallout reading first; then fix amcheck's precision, or root-cause).** In `src/store/amcheck.rs`, `check_tree` `continue`s on a §3.3, read or accept failure, so it never pushes the failed node's children. The I-LPID-EXACT loop that follows then reports every mapped LPID it never reached as `mapped lpid N unreachable`, each with its slot. In seeds 688 and 3718, one or two damaged pages (lpid 5; lpids 7 and 40) are followed by 15–16 "unreachable" findings on contiguous later lpids. That pattern is subtree fallout. F1's provisional reading was a map-exactness store deviation.
- **(a) The test (evidence before any change).** For each of the six rounds, compute the descendant set of every node that failed in the (ii) walk. Use the faultless twin's end image, or the failed slot's `V_e(s)` composed as in X5-R28 (a), parsed with the store's own page decoder in a diagnostic (stderr only, never a digest input). Compare it with the set of "unreachable" LPIDs.
- **(b) If the two sets are equal in all six rounds:** this is a prototype precision defect. S-STORE §15 defines I-LPID-EXACT "on a quiescent file", and a walk that cannot pass a failed node cannot decide reachability below it.
  - **The fix,** in `src/store/amcheck.rs` only: when any node of the tree walks failed, emit the unreached mapped LPIDs as **one structural finding** (`slot: None`, `I-LPID-EXACT inconclusive: N mapped lpids unreached below failed lpids [...]`). Emit no slotted findings for them. With no failed node, the output is unchanged.
  - **Pins (literal configs):**
    - a damaged interior node gives exactly its own slotted §3.3 finding plus the one structural finding. Revert-demonstrated;
    - an undamaged image with an orphan map entry still gives the slotted `mapped lpid N unreachable`. This guards against an "always structural" mutant.
  - **What it does not weaken:** class C still fails on any finding; MP detection is still credited through the failed node's slotted finding; the oracle is untouched; and a true orphan beside damage is still caught on the faultless twin.
- **(c) If the sets differ in any round:** the end image really holds orphan map entries, which violates I-LPID-EXACT. Do not change amcheck. Hand it to F3 as a C-XX-family store deviation, with the per-lpid evidence.

**X5-R30 (MP-A: evidence now, the O-3 (a) question kept open).** All seven MP-A rounds are `mrb=true`. A `K` slot is tainted by definition, and its bytes pass §3.3 against the opened image, so a correct store that reads one accepts it, and the sim records a served corruption. O-3 (a) may have the same proxy flaw as completeness. **P does not rule that before F3's evidence.** It is the gravest clause, and the seven rounds have been seen.
- **(a)** This card adds a per-round record field `served_k`: true iff every served range of the round lies wholly inside slots of `K`. It is computed by the harness from the sim's served list and K. It is a diagnostic in the record, never an oracle input.
- **(b)** O-3 (a) is unchanged. Every served corruption still fails, and F3 ESCALATEs all of them with X5-R16's evidence plus `served_k` and `V_k(s)`.
- **(c)** P rules at F3's gate. If O-3 (a) moves, the move is applied to the final file from the recorded fields, with no re-run, and is listed in RESULT item 6 as a post-data amendment.

**X5-R31 (structure and sizing: three stacked cards; the Acceptance hosts every 10k).**
- **Why.** The runner kills an attempt at 60 min, and runs each Acceptance command under its own 60-min limit after an attempt finishes. A 10k `run` or a `verify` takes about 20 min each at load 20. X-5c-F1 lost three runs to campaign-class commands inside attempts.
- **The rule.** Inside an attempt, no worker runs any of these:
  - `e4-campaign run` over more than **200** contiguous seeds;
  - `e4-campaign verify`;
  - a stress loop;
  - `scripts/gate.sh`.

  Each 10k is an **Acceptance command**: run at the card's final tip under X5-R13's backstop, with its output and a provenance file outside the worktree (`$HOME/.cache/nv3-review/runlogs/<CARD>.acc.*`). It is followed by `verify` of that file, then by the gate. The next card's step 0 commits that file after the provenance checks (the X5-R25 pattern).
- **The cards:**
  - **X-5c-F2** (this card): steps 0–4 above, with `verify --no-quotas`;
  - **X-5c-F3**: the classes, with `verify --no-quotas`;
  - **X-5c-F4**: X5-R17's retune and the final file, with plain `verify`. The worker tunes on contiguous ranges of at most 200 seeds. These are development probes only, never quota evidence. A quota miss fails the Acceptance, which feeds the table back to the next attempt. This amends the old F2 Goal's "each try is a whole 10k run", which was a process rule, not X5-R17's text.
  - **Then X-5c `--keep`**: it commits F4's file, runs plain `verify` on it, runs `mutants`, and writes the RESULT.

## P rulings X5-R30 (c) and X5-R32, verbatim (nv3-X-5c-F3-gate-1 and
## nv3-X-5c-F3-F2, 2026-11-01; binding)

Copied word for word from card X-5c-F3-F1 (its "Carried" sections, verbatim
copies of the rulings) before this card's Work began, beside X5-R28–R31.

**X5-R30 (c), RULED:** 1. **(K serves) O-3(a) moves.** A served range does
not count as served corruption if all of the following hold: it lies wholly
inside slots of K (X5-R28 (a)); it was accepted after open selected the
rolled-back meta (in a recovery life or in extraction, never in the
workload pass); the round has no R-taint and no uncovered accept. **Why.**
K's condition 2 proves from the DWrite history that the bytes are V_k(s),
byte for byte: the opened image's own content, accepted through that
image's own reference. The sim calls the range tainted only because a
post-anchor reuse write never landed. That is the proxy flaw X5-R28
corrected for completeness, and the same correction applies to the same
slots here. 2. **(Meta serves) O-3(a) moves, on the same principle, as
follows.** A served range does not count as served corruption if: it is
exactly the opened meta's slot (1 or 2); the round meets X5-R5's
conditions 1 and 2 (class MP, a meta-slot fault, and `meta_rollback`);
the accepted bytes equal a DWrite the store issued to that slot and a
completed `sync_all` covered. **Why.** X5-R5 pre-registered that open
selecting an older durable meta under a meta-slot fault is a legitimate
outcome. The meta it selects is genuine. Taint fires only because a newer
write to that slot was dropped by F-CRASH or reverted by the fault. The
rollback's consequences remain judged, unchanged, by R, K, O-1 and O-3(e).
3. **Nothing else moves.** R-taint, any serve outside K or the opened meta
slot, any serve in the workload pass, any uncovered accept, and every MW
serve (6149 included) stay served corruption. 4. **Applying it from
recorded fields.** The current record cannot apply the ruling exactly: `sk`
is vacuously true on an R-taint-only round and is computed over
workload-pass serves too; the record carries no meta-serve field; an
`O-3(a)` text can hide an O-3(e) failure. So **X-5c-F3-F1 adds three
diagnostic record fields**. They are never oracle inputs, and the oracle
stays frozen: `skx`: rule 1's predicate, exact; `smx`: rule 2's predicate,
exact; `pass_na`: the round's verdict re-evaluated with the existing
`OracleCfg::no_taint` knob, recorded only when `served ∧ (skx ∨ smx)`. The
final file is judged with no re-run: a served round passes iff
`(skx ∨ smx) ∧ pass_na`. RESULT item 6 must list this as a post-data
amendment (X5-R30 (c)), with the rounds it moved. 5. **In F2's and F3's
files,** the seven rounds stay recorded as failures, since the fields are
absent. The gate's evidence says that 1897, 2437 and 4718 satisfy rule 1
and 2927, 4378, 7528 and 8728 satisfy rule 2's location conditions; the
byte check remains `smx`'s to establish. Whether each then passes depends
on `pass_na`.

**X5-R32 (O-3(c) in class MP: the damaged-referenced arm; post-data;
governs every campaign from X-5c-F3-F1's Acceptance on).** It corrects the
twin rule where the twin's sets are a failed stand-in for the real round's.
It is P's, and it is recorded under RESULT item 6 as a **post-data oracle
amendment**: the 22 rounds were seen before it was made, while F3-F1's
Acceptance and F4's final campaign were not. (a) The harness supply comes
from SimDisk history only: never from store memory, never from §3.3 or
amcheck output, and never from a store error text. 1. `FaultTargets` gains
`prior_page_slots: BTreeSet<u64>` — the page-file slots of every media
fault applied to this round's disk before this round's own draw:
`disk.media[..media_base]` in `harness::run_round`; each fault maps by
`page_slots`' rule (`off / 4096`, plus `to_off / 4096` for a MISDIRECTED,
and only for faults whose inode is the page file); it is empty in round 0,
in classes C and MW (which have no page faults), and in every twin round
(which applies no media faults). 2. `Extracted` gains `recovery_reads:
Vec<(usize, u64, u64)>` and `page_file: usize` — exactly the harness's
`recovery_reads` (the reads of the recovery life whose result the round
judges, on the `Ok` arm and the final `Err` arm alike; the real round's,
never the twin's); both arms set both fields before the verdict's
`check_round`. (b) The oracle amendment (the frozen `src/oracle.rs` and
`tests/oracle.rs` change only by this text, in one commit whose subject
names X5-R32): the definitions, computed inside `check_round` —
`H = faults.page_slots ∪ faults.prior_page_slots`;
`D = { s ∈ ex.tainted_image_slots : s ∉ {0, 1, 2}, s ∈ H, s ∉ ex.kept_slots }`;
`D_ii` the same against the (ii) sets (empty unless `ex.amcheck_ii_ran`);
`D_read` iff some `s ∈ D` has a read in `ex.recovery_reads` on
`ex.page_file` overlapping `[s·4096, (s+1)·4096)`; `sites = ex.xx001`, plus
`Read23` if `lost_at_match > 0`. A site is explained when: `Open` and
`Replay`: iff `D_read`; `AmcheckI` and `Read23`: iff `D ≠ ∅`; `AmcheckIi`:
iff `D_ii ≠ ∅`. In the twin rule's `Some(t)` branch, `any_fault &&
all_uncovered` keeps its failure text, except when all of these hold:
`class == FaultClass::Mp`; `!cfg.no_r32`; `sites` is non-empty; every site
is explained. Then O-3(c) does not fail, and the verdict's new field `r32`
is `true` (false in every other case). Unchanged: the `None` branch (no
twin), which still fails closed; the twin's sets and the per-fault
referenced/read test; the WAL ranges; class MW; X5-R6's `explained` rule;
every other clause. The record: `RoundVerdict` gains `pub r32: bool`; the
campaign record gains the round field `r32`, written by `run` and parsed by
`verify` — a diagnostic the pass fold does not read. The knob:
`OracleCfg` gains `no_r32: bool`, which removes the arm; it exists only for
the load-bearing demonstration. `tests/oracle.rs` gains one must-pass (mp8)
and five must-fail cases (mf24–mf28) on one literal world, class MP round
1: the round's only fault is on slot 50, which the twin neither read nor
references; `prior_page_slots = {6}`, slot 6 is in `image_slots` and
`tainted_image_slots = {6}`; amcheck (i) reports slot 6 (so O-3(e) holds);
`xx001 = [Open, AmcheckI]`, and `recovery_reads` contains
`(page_file, 6·4096, 4096)`. (c) Why it is neither circular nor a
loosening: `D`'s members are media-fault targets, from the disk's
applied-fault list; they are tainted (durable bytes ≠ the last bytes
issued, from SimDisk's history) and referenced by the image recovery
opened (the harness walk of the durable bytes); none of these inputs is
§3.3, amcheck, or store memory. A detection on such a slot is the plan's
clause 3. O-3(e)'s completeness half already *requires* amcheck (i) to
report it, so the frozen rule was unsatisfiable on every round where the
twin rule fired and `D ≠ ∅`. What still fails: a recovery that fails
without having read damage (`Open`/`Replay` need `D_read`); a detection
whose only "damage" is F-CRASH or store-made taint (`H`); a K slot (its
bytes are the image's own content); any MW round; any round with no twin.
The same granularity as before: one explained site per kind is enough,
just as one covered fault was enough before; the exactness of amcheck's
findings stays O-3(e)'s. (d) The decision rule, pre-registered: 1. at
X-5c-F3-F1's tip, single-seed runs of the 17 named seeds; each named round
must have `r32 = true` and no O-3(c) text, or be recorded "moved by
<commit>" with its new outcome (not a failure of this rule); a round that
keeps the twin-rule shape with `r32 = false` is an ESCALATE — never widen
`D`. 2. Not covered: 1889 r2, 3509 r0, 4699 r0, 5229 r1 and 8399 r1 stay
O-3(c) failures under X5-R32 (C-XX family). 3. From F3-F1's Acceptance
file on: every `r32 = true` round must be class MP, and any other is a
harness defect that fails the gate; the gate lists the `r32 = true` rounds
in RESULT item 6 as moved by this post-data amendment; an O-3(c) round
with `r32 = false` stays a failure, classified under C-XX unless P rules
otherwise. 4. The WAL side is not ruled: no worker extends `D` to the WAL.
(e) X5-R32 does not move O-3(a), (b), (d) or (e), O-1, O-2, R, K or
X5-R30. It is empty in classes C and MW. Never satisfy O-3(c) by making a
harness walk report ground truth as a store finding (X5-R23).

## P rulings X5-R33 and X5-R34, verbatim (nv3-X-5c-F3-F3-write-1, 2026-11-03;
## binding; pre-registered)

Copied word for word from card X-5c-F3-F3a (its "Carried verbatim" section,
itself verbatim from `verdicts/X-5c-F3-F3-pre.md`) before any `e4-campaign`
command ran (X5-R33 (a), X5-R34 (a)) — beside X5-R28–R32 above.

### Carried: X5-R34, verbatim (binding; post-data; `verdicts/X-5c-F3-F3-pre.md`)
**X5-R34 (the acked-commit derivation: I-ACK-PREFIX's `durable_end` from SimDisk history; the client-ack cross-check is judged)**

P (claude-opus-5-5, nv3-X-5c-F3-F3-write-1), 2026-11-03, from the S-1-F3 draft-2 gate's ruling (b) (`verdicts/S-1-F3-draft2-gate.md`, "Ruling on (b)"). **Post-data:** the 37 class-C `0 < frontier < lo` rounds of X-5c-F3-F1's Acceptance file (commit 62d4e739, json_sha256 `f3a1e605…a1af17`) were seen before it was made. It governs every run from the commit that lands it. RESULT item 6 records it as a post-data amendment of the harness's `lo` input. The oracle's text does not change.

**(a) Scope.** It replaces the SyncOn `acked` half of `harness::derive_written_acked`: the per-inode `max`, where `durable_end[inode]` is the max over `SyncReturned` of the returned extent and `acked ⇔ durable_end[inode] ≥ end`. Unchanged:
- `written` (the `hi` input);
- SyncOff's `acked` (= written);
- the `unparsed_written` report;
- the oracle's `lo = max(c_{r−1}, max{ts : SyncOn ∧ acked})`;
- X5-R1 (until X5-R33 lands).

**(b) Inputs.** All are harness-side and none is store memory, except `p0`.
1. **`E`:** the workload pass's `disk.events`, as `run_round` captures them. These are this round's events only, because `new_round` cleared the list.
   - Index order is time order: every env op is one atomic sim step (`WriteIssued`+`WriteReturned` in one step, `SyncIssued`+`SyncReturned` in one step).
   - Media faults are applied after `E` is captured, so `E` never contains them.
2. **`p0`:** the WAL append position the round's workload started from, `start_store.wal.append`. That is the valid end of the recovery that produced the round's base; in round 0 it is the created store's append position.
   - *Guard:* every record in `wal_commits` has `lsn ≥ p0`. A violation is a harness defect, and it fails the round loudly.
3. **`B(n)`:** for a segment file name `n` (`{seg:016x}.log` in the sim directory `"/wal/"`), the inode that SimDisk's **durable** directory bindings map `n` to at the end of the workload pass, or none.
   - That is `durable_bindings["/wal/"][n]`, the binding made by a create or rename that a later `sync_dir` made durable.
   - It is read through one new read-only observer accessor on `SimDisk`. Nothing else in `src/sim/` changes.
4. **`β(n)`:** the index in `E` of the last `DirOpIssued` whose `what` is `create /wal/n`, or ends with `-> /wal/n`, and which precedes the last `SyncDirReturned { dir: "/wal/" }` in `E`. It is none if no such event exists, meaning the binding predates the round.
5. **`Rc`:** the segment names `n` for which `E` holds a `DirOpIssued` whose `what` is `rename /wal/n -> …`. These are the sources of step 6's recycling renames.

For an LSN `x`: `seg(x) = x − (x mod WAL_SEG)`, `name(x) = {seg(x):016x}.log`, and `off(x) = x mod WAL_SEG`.

**(c) Durable bytes.** A byte at LSN `x` is **durable** iff one of the following holds.
- **(R) Recycled:** `name(x) ∈ Rc`. This is an exemption. A segment is recycled only once it lies wholly below a durable meta's `wal_start_lsn`, so `T` covers its commits.
- **(W) Written and synced:** all of these hold:
  - `i = B(name(x))` exists;
  - the last `WriteIssued` in `E` to inode `i` whose range covers `off(x)` exists;
  - that write's index is greater than `β(name(x))`, when `β` exists;
  - a `SyncReturned { inode: i, .. }` (from `sync_data` or `sync_all`) follows it later in `E`.
- **(S) Slack:** `x` lies after the end of a `SegmentEnd` record that starts at some LSN `q` with `seg(q) = seg(x)`, where:
  - the `SegmentEnd`'s own bytes are durable by (W); and
  - its last-issued bytes at `off(q)` in `B(name(q))` decode (`wal::dec_record`) as `K_SEGEND` with `lsn = q`.

*Why the binding index.* A recycled file keeps its previous life's synced bytes at every offset its new life has not yet overwritten. Seed 20 r2 shows this:
- inode f4 was synced as segment 0x60000 (EV 121);
- it was renamed to 0xa0000 (EV 7309, dir-synced at EV 7310);
- ts 64 was written at f4@5952 (EV 7837) and never synced.

Only writes made after a name was bound to its inode belong to the stream at that name. Without the index, an unwritten record lying over stale synced bytes would count as acked.

**(d) `D`.** `D` is the largest `L ≥ p0` such that every byte in `[p0, L)` is durable, scanning in LSN order. A segment boundary is crossed like any other position: an exact segment end, and `SegmentEnd` slack, both continue at the next segment's first LSN. `D` is computed over write intervals (per-inode interval maps, one pass over `E`), never with a per-byte loop over the WAL span.

**(e) Acked.**
- A SyncOn commit of this round is acked iff `lsn + len ≤ D`, or `name(lsn) ∈ Rc`. The commit is a `Commit`, or a chunked transaction's `CommitMarker`; `(lsn, len)` come from `wal_commits`.
- A SyncOff commit is acked iff it is written (unchanged).
- X5-R33's `a` uses this definition of acked.

**(f) The judged cross-check (ACK-OVER-HOLE).** `HarnessState::client_acked` becomes a judged input. It holds every commit whose own `wal_wait_durable`/`wal_wait_written` returned before the crash.
- **The failure.** Take every SyncOn `ts ∈ client_acked` with `lsn + len > D` and `name(lsn) ∉ Rc`. For each one, the round fails as a store failure, through the same path as X5-R10's guard (the round's task error, so the pass fold fails it), with the text `ACK-OVER-HOLE: client-acked SyncOn ts=<ts> ends at <lsn+len> above durable_end <D> (I-ACK-PREFIX, X5-R34)`.
- **The record.** The round record gains `ack_over`: the list of such ts, empty when there are none. `run` writes it and `verify` parses it; the pass fold does not read it.
- **Not a failure:** a commit acked by (e) but absent from `client_acked`, because its committer crashed in its wait. Its record is durable, and recovery must replay it.

**(g) Self-tests** (literal event lists, in `tests/harness.rs`). Each must-fail case must also fail on the frozen derivation. That is shown by restoring the old function, recording the result, and reverting.
1. **20 r2's shape:** an inode is synced in its life as segment A, renamed to B and dir-synced; then a SyncOn record is written at B's offset with no later sync. It must not be acked.
2. **Contiguity:** batch 2 is written and synced while batch 1 below it is unwritten. Batch 2's SyncOn commit must not be acked, with batch 2 on the same inode and, as a variant, on the next segment's inode.
3. **The directory:** a segment is created, or renamed into place, with no later `sync_dir("/wal/")`. Its records must not be acked, even though they are synced.
4. **A forged early ack:** `client_acked` holds a ts whose record ends above `D`. The round must fail with the ACK-OVER-HOLE text.
5. **Must pass:**
   - a record written after its segment's durable binding and then synced, contiguous from `p0`, is acked;
   - a record in an `Rc` segment is acked, and exempt from (f);
   - a SyncOn record placed after an exact segment end, in a durably bound and synced next segment, is acked.

**(h) The decision rule (pre-registered).**
1. **At the commit that lands (a)–(g), before any store change of its card** (the store is still X-5c-F3-F1's), run single seeds for the 37 named rounds:
   - 20 r2, 115 r1, 403 r1, 622 r0, 780 r2, 801 r1, 825 r2, 830 r1, 1001 r0, 1064 r1;
   - 1585 r1, 1801 r1, 2183 r0, 2525 r2, 2730 r2, 2765 r0, 2812 r1, 3224 r1, 3806 r1, 4770 r2;
   - 4930 r0, 5054 r1, 5601 r1, 5604 r0, 6154 r0, 6200 r2, 6263 r2, 6623 r0, 7405 r1, 7531 r0;
   - 7803 r1, 7823 r1, 8220 r2, 8363 r0, 9200 r0, 9484 r2, 9594 r0.

   Record frontier, `lo` before and after, pass, and `ack_over` per round. Then run 0..200 and record `ack_over` and the count of failing class-C rounds with `0 < frontier < lo`.
   - **Prediction** (P's prototype of this text):
     - 33 named rounds get `lo ≤ frontier`;
     - 1064 r1, 4770 r2, 5054 r1 and 6263 r2 keep `frontier < lo`, with `lo` unchanged;
     - `ack_over` is empty on all 37 and everywhere in 0..200;
     - no new failing class-C `0 < frontier < lo` round appears in 0..200.
   - A named round that then fails another clause is classed under that clause, and never re-ruled here.
2. **The four exact-boundary rounds are store losses** until the §12.2/§12.5 exact-segment-end hop lands (S-1-F3 gate G2 and edit 2). They are class-C I-REDO-COMPLETE breaks with no media fault. After the hop they must leave the `frontier < lo` shape.
3. **At the card's tip,** re-run the 37. Any **failing** class-C round that still has `0 < frontier < lo`, whether named or new in a ≤ 200-seed range, is escalated as a store loss with its trace: `D`, the reason `D`'s walk stopped, the record, and its events. It is never re-derived: (b)–(e) do not change, and no exemption is added.
4. **Any non-empty `ack_over`** is an ACK-OVER-HOLE store failure: record it and ESCALATE. The cross-check is never relaxed.
5. **From X-5c-F3-F3a's Acceptance file on,** every failing class-C `0 < frontier < lo` round is a store loss, for P's triage. A passing round with `frontier < lo` (O-1 matched at some `c ≥ lo`; e.g. 6851 r2 in F3-F1's file) is not a loss. RESULT item 6 lists X5-R34, with the 33 rounds it moved.
6. **A round whose workload moved,** because a store fix in the same card shifted it, is recorded "moved by <commit>" with its new outcome. That is not a failure of this rule.

**(i) What it does not touch:**
- `written`/`hi`, and the oracle's text;
- X5-R1, until X5-R33 replaces its MW bound;
- X5-R5's R, and X5-R28's K;
- `model.rs`, and the sim's fault model, crash model and `taint_ranges`.

It never reads `wal.durable` or `wal.written`, which are store memory. A diagnostic may print them beside `D`.

### Carried: X5-R33, verbatim (binding; pre-data; `verdicts/X-5c-F3-F3-pre.md`; implemented by X-5c-F3-F3b, committed as text by this card's step 0)
**X5-R33 (X5-R1's MW lower bound, tightened; the WAL-loss `Err`; the `wal_ckpt_end` cross-check)**

**Source.**
- S-1-F3 draft 2, Implications (e), with the draft-2 gate's edits 4 and 5 applied (`verdicts/S-1-F3-draft2-gate.md`, 2026-11-03).
- Its two open items are closed by P here, in (h) and (i).
- Precision added by P: the truncation point in (b), the no-damage case of `a`, and the exact bounds form of the cross-check in (d).

**Status.** It is **pre-data**: no run that judges by it has been read. It tightens X5-R1, which the X-5c card's "may tighten, may not loosen" allows. It is a P-commit to the frozen oracle and its self-tests, landed in one commit whose subject names X5-R33.

**(a) When.**
- This text is committed before the output of any run that judges rounds by it is read. X-5c-F3-F3a's step 0 commits the text.
- X-5c-F3-F3b's code commit lands it before F3-F3b's ≤ 200-seed MW smoke.
- A run whose output is read before that code commit is diagnostic-only, and cannot be cited as a pin or as evidence.

**(b) The stream as it stood at a moment `m`.**
- It is the records appended by the life writing at `m`, plus, for each earlier life, its records below the **lowest truncation point of every later life up to `m`**. A record that a later truncation superseded never belongs to it.
- A life's truncation point is the LSN at which it began appending, which is the valid end its recovery found.
- A recovery life that appends nothing truncates nothing under this definition. If its zeros survive, they stop the next life's walk, and that sets the next life's truncation point.

**(c) The lower bound for an MW `Ok` recovery: `lo_MW := max(c_{r−1}, T, h, a)`.**
- **`h`** is the largest ts whose `Commit`/`CommitMarker` record lies wholly below the opened meta's `wal_ckpt_end`, in the stream as it stood when the opened meta was written.
- **`a`** is the largest ts that is SyncOn, acked by X5-R34, and has its record wholly below the round's **first damaged WAL byte**, in the stream as it stood at the round's crash.
  - Damaged bytes are the MW fault targets and misdirect destinations. They map to LSN through the first LSN of the segment whose name was bound to the target inode at the crash.
  - If the round damaged no WAL byte, `a` is the round's largest acked SyncOn ts.
- **The derivation.** The harness derives both values from SimDisk history and the model's record positions. It reads `wal_ckpt_end` and `ts_ckpt_hi` from the meta bytes on SimDisk, never from store memory.

**(d) The harness cross-check.**
- **What is checked.** For every meta the store writes (every meta-slot `DWrite` whose bytes parse as a meta), with `w` = its `wal_ckpt_end`, `t_hi` = its `ts_ckpt_hi` and `T` = its `T`:
  1. no `Commit`/`CommitMarker` record that the writing life had appended straddles `w`: each ends at or below `w`, or starts at or above it;
  2. every such record with ts `≤ T` ends at or below `w`;
  3. `w` is at or below that life's append position at its crash or end;
  4. `t_hi` is the largest ts among that life's `Commit`/`CommitMarker` records ending at or below `w`, or, if there is none, the frontier the writing life started from.
- **Why these bounds are exact.** This is the gate's bounds form, G4 (c). It is exact because step 0 reads both values in one step, inside the commit critical section (§13 step 0 as amended, W5 (i)). `Chunk` records are excluded: they may fall on either side of `w`.
- **A mismatch** is a store failure of the round, with the text `X5-R33 cross-check: meta <ckpt_id> wal_ckpt_end <w> / ts_ckpt_hi <t_hi> vs <expected>`. It is not a harness error.
- **What it catches.** A store that writes `wal_ckpt_end` too low (COVER-END-AT-T), without trusting store memory.
- **Self-tests.** A forged low `wal_ckpt_end` fails it, and so does a forged `ts_ckpt_hi`.

**(e) A recovery `Err` in class MW that is I-WAL-COVER's refusal.**
- **What it is.** XX001 for WAL loss, identified by the store's typed error, never by its text. It is counted as detection under O-3 (b).
- **The rule.** It passes iff it is **explained**: a fault of the round damaged a WAL byte in `[wal_start_lsn, wal_ckpt_end)` of the opened meta, in the stream as it stood when that meta was written. Otherwise O-3 (c) fails it as a false positive, with the text `O-3(c): WAL-loss XX001 with no fault in [wal_start_lsn, wal_ckpt_end) (X5-R33)`.
- **What follows.**
  - O-1 is not run on the `Err` open, as for an MP recovery `Err`.
  - The seed ends at that round, as an MP `Err` round's does.
  - Every other MW `Err` is judged as before, by the twin rule.

**(f) Self-tests.**
- **Must fail:**
  - MW `Ok` with `c = h − 1`;
  - MW `Ok` with `c = a − 1`;
  - a MW WAL-loss `Err` where every fault lies outside `[wal_start_lsn, wal_ckpt_end)`;
  - a record superseded by a later truncation counted into `h` (a fixture with a reissued ts below `wal_ckpt_end`);
  - the cross-check with a forged low meta value, and with a forged `ts_ckpt_hi`.
- **Must pass:**
  - a MW WAL-loss `Err` that is explained;
  - MW `Ok` with `max(h, a) ≤ c <` the max acked.

**(g) Unchanged.**
- "O-3 requires that no damaged record is replayed";
- class C;
- class MP's lower bound.

**(h) Class MP.** This closes S-1-F3's "MP side effect for X5-R33 to rule". P, 2026-11-03, pre-data.
- **MP rollback rounds** (X5-R5) keep MP's lower bound: the max acked SyncOn, acked by X5-R34. Nothing in X5-R33 excuses them.
- **A WAL-loss `Err` in class MP** (for example a Q14 rollback to a meta whose WAL span the newer meta's step 6 recycled) is judged by MP's unchanged O-3 (b)/(c): the twin rule, with X5-R32. It is never judged by (e). The rollback's meta-slot fault lies on a slot that every recovery reads, so the twin rule does not call the detection a false positive.
- **X5-R5's R and K** and amcheck (i) are computed as before, on the opened image.
- **Counting.** The round record's `cover_err` field lets RESULT count such rounds per class.
- **Class C.** Any WAL-loss `Err` fails O-3 (b), as every class-C XX001 does. Once the exact-segment-end hop exists, no class-C open fails the coverage check (S-1-F3 gate G5), so such a round is a store failure.

**(i) Salvage.** This closes the conditional "if the prototype gains a read-only salvage open".
- X-5c-F3-F3 adds no read-only open to the prototype. S-1-F3's salvage (W7) is A-7d's, so the conditional is not triggered.
- The prototype models Q1 = A's read-write half only: XX001, and nothing written.
- Any later card that adds a read-only open must first have P pre-register O-1 on it: the state equals the model at `c` = the frontier, with `max(c_{r−1}, T, a) ≤ c`.

**The reporting rule (this card's step 0, standing):** every class table
splits out `served=true` rounds first (the oracle's `o3` text is
last-writer-wins, so a serve can read as O-3(d), and an `O-3(a)` text can
hide an O-3(e) failure).

## Stage X-5c-F1's record (X-5c-F1)

### X5-R14: the non-reproducing digests — root-caused, fixed, pinned

**The reproduction.** `verify`'s re-run of d2c8bba7 found seeds 180,
559, 7474 and 8832 with different digests. On this host, seeds 559 and
8832 re-run **stably** to a different digest than the committed file
both quiet and loaded (559: committed `1004855635`, re-run
`2039283878`; 8832: committed `551982438`, re-run `1502800514`);
seeds 180/7474 reproduce. The committed variants are the rare ones —
the product of d2c8bba7's specific 16-worker co-tenant mix.

**The record-level divergence.** Seed 559 round 2: the **uniform crash
point itself moved** (`uniform@y:2706` → `uniform@y:15841`) — the
probe pass measured a different total scheduler-yield count, so the
workload pass ran a different schedule — and with it `lo`/`hi`/`c`/
`frontier`/`gc_w`/`ackl` and the quota flags. Seed 8832 round 1: `hi`
8 → 7 — a commit record's WAL write was issued in one run and not the
other.

**The root cause (the host-mutation log, X5-R14's method).** The
diagnostic `E4_SCHED_LOG` (committed, **default off**, never a digest
input) logs every `Sched::acquire` with the yield count, the baton
holder, the crashed flag and the OS thread. Two windows put host steps
outside baton ownership, and OS scheduling decided their interleaving:

- **Window A — the thread-start window.** A spawned **service** task's
  first scheduler contact was its loop-entry `Cx::with` (`h:writer`,
  `h:ev` + `evict:pick`, `h:ck`), not a park: the log shows them at
  `y=0` with `run=Some(0)` (the harness still holds the baton), and
  mid-round (`h:writer` at `y=9` with `run=Some(5)`).
  `wal_writer:take` can drain the WAL buffer and `evict:pick` advances
  the pool clock hand — sim-visible decisions in OS start order. A
  controlled flip proves it end-to-end: a diagnostic entry spin
  (removed) of 10 OS yields before the service bodies' first
  instruction changed seed 559's digest to a third value
  (`1476281624`) at the same code.
- **Window B — the post-crash unwind window.** All four world-fail
  paths (`yield_baton_budget`'s crash and livelock branches,
  `handoff_locked`'s deadlock branch, `deliver_crash`) set
  `running = None` and make every waiter runnable at once; from then
  on the unwinding tasks' host steps ran concurrently — the log shows
  `unpin`, `tree_unlock`, `gate_release`, `h:taskerr`, `h:taskcxx` at
  `run=None crashed=true`, interleaved in OS order across threads.

`Sched::acquire` (src/sim/sched.rs) granted the world lock to **any**
caller: it never checked `running == Some(me)` and never checked
`crashed` — exactly the ruling's lead (a), with lead (c) (reads
outside the baton) the same defect's read side. Lead (b) (nested
recovery crashes) is a correlation, not a cause: recovery worlds are
single-task.

**The fix (commit 9b041a3d).** One gate in `Sched::acquire`, which now
carries the caller's task id:

- **a live world** grants a host step only to the baton holder
  (`running == Some(me)`); anything else is a loud
  `SimFail::Baton` naming the site (window A closed). The harness's
  `run_task` parks every service task before its first host step
  (`h:entry`), and `tests/store.rs`'s `run_world` driver gained the
  same entry barrier.
- **a failed world** grants a host step only to the **unwind turn** —
  `SchedCore.unwind_turn`, set at every world-fail site to the smallest
  unfinished `TaskId` and advanced by `finish` (window B closed): the
  ruling's "deterministic unwind: the baton passes through the
  unwinding tasks in TaskId order". `HARNESS` is exempt (single-task
  recovery/extraction/checkpoint worlds; the multi-task harness thread
  never acquires after the world fails — it joins, and its post-join
  reads use `Sched::lock`).

**Pins.** `r14_pre_baton_host_step_is_refused` (a live world's
non-holder acquire is refused; a hand-rolled world whose `start` is
never called makes the state deterministic) and
`r14_unwind_host_steps_run_in_taskid_order` (task 2 delivers the crash
while holding the baton; both tasks' unwind host mutations land in
`TaskId` order). Both demonstrated failing on the pre-fix acquire,
3/3 runs: the first returns `Ok`, the second logs `[2, 1]`.

**Stress evidence (the ruling's pin 2).** The four seeds re-run 25
times each at `--threads 16` while a concurrent 16-thread `run`
(seeds 3000..10000) loaded the host: **one digest per seed** — 180 →
`3787622012`, 559 → `2325742370`, 7474 → `2750567368`, 8832 →
`1502800514` (all four differ from d2c8bba7's, as the fix must).

**Digest stability vs d2c8bba7 (the ruling's pin 3).** See the
campaign section below: the shift is total and explained by the
mechanism — every multi-task world previously contained window-A
service reads (the log shows them in every workload world), so every
seed whose digest depends on a multi-task schedule changes; the
fresh campaign's digests reproduce exactly (verify's re-run).

### X5-R15: the reporting corrections — landed, pinned

- **(a)** `E4 FOLD FAILING_SEEDS=<n> FAILING_ROUNDS=<n>
  TWIN_FAILING_ROUNDS=<n> DEFECT_SEEDS=<n>` after the unchanged Work 7
  line (`campaign::fold_line`; commit dffe6744). Validated against
  d2c8bba7: FAILING_SEEDS=1598 (the ruling's corrected count — the
  1,560 of the first record plus the twin-failing seeds), 
  FAILING_ROUNDS=1427, TWIN_FAILING_ROUNDS=100 (46 MP + 54 MW),
  DEFECT_SEEDS=150. Pin:
  `r15a_fold_line_from_a_literal_2_seed_record_set` — a passing round
  carrying the recovery-`Err` default O-1(a) text contributes nothing
  (the 1,315 rounds `O1_FAIL` wrongly counted).
- **(b)** `verify` reports all three checks (seed set; digests with
  the non-reproducing list; the full quota table) before one exit
  decision (`verify_seed_set_ok`/`verify_digest_report`/
  `verify_exit_code`), and `--no-quotas` waives only the quota check
  (X-5c-F1's Acceptance form; X-5c's own keeps plain `verify`). Pin:
  `r15b_verify_reports_all_three_checks_before_failing`.
- **(c)** The campaign record corrected in place: the quota paragraph
  no longer claims the digests reproduce; the failing-seed count is
  1,598 with the seed-level defect seeds; the three missing
  failing-round groups recorded (MP O-3(a) served corruption, 9
  rounds — MP-A; O-3(c) XX001 with no damaged referenced slot, 25
  rounds; O-3(c) "no twin", 19 rounds — NO-TWIN). Commit 20fc5370.
- **(e)** `examples/` keeps only what "How to reproduce" uses
  (`pin.rs`, referenced there); `dbg-seed.rs` (823d4f51's named
  scratch) removed; `restore_probe.local.py` was never in the tree.
- **(d)** RSS: recorded beside the campaign below.

### X5-R16's instrumentation classes

**NEVER-FIRED (instrumentation: workload) — fixed, pinned; the record
below describes the code as it is after the retry redesign (X5-R21
(b)).** 150 seeds carried "the crash spec never fired" in d2c8bba7
(110 class C, 35 twin rounds, 5 other MW; 146 targeting `Mid2`). P's
stated cause: the targeted checkpoint's step 2 had too little to write
(the tiny pool's evictor had written every dirty page, so `Mid2` —
fired after every step-2 write — never fired). The class closed in
three commits, each still in force:

- **2da915a8 (the counting pass).** The checkpointer records, per
  checkpoint completion index, the mask of hooks it reached
  (`WorkloadObs::ckpt_hooks_hit`, `ckpt::hook_bit`), and the targeted
  draw keeps the seed's drawn hook but lands its index on one that
  reaches it — the drawn index if it does, else the smallest — falling
  back, deterministically from `(seed, round)` over the union mask
  (`reachable_hook_spec`), to a hook the round does reach when the
  drawn hook is unreachable at all. Pin
  `r16_never_fired_targeted_draws_reach_their_hook` (literal configs 60
  and 660), demonstrated failing on the reverted draw. This form ran a
  probe pass before every targeted round; at ~3 seeds/s it would have
  blown every 60-minute command budget, `verify` included.
- **6cc53657 (the redesign: no probe pass on the targeted path).** The
  drawn spec now runs directly — the twin first for MP/MW (Work 2.5
  pairs their crash points), the real round itself for class C. If it
  fires (the overwhelming majority) nothing extra happens; only an
  unfired round re-derives its spec from its **own complete** hook
  masks (an unfired crash means the workload ran to completion, so
  every checkpoint completed) and re-runs — the failed attempt is
  discarded and never reaches the fired-guard. Same derivation and
  same final specs as the counting form (the repro seeds'
  crash descs are identical), back at ~12 min per 10k.
- **4fedc648 (the residual 7 + the uniform twin wrap).** The MP/MW
  **real** round retries on its own too: the twin and real chains can
  diverge — the twin chain re-seeds after a failed twin recovery — so
  the twin's firing does not guarantee the real round's (the five
  ruling seeds 2669/4049/4309/7469/9809, all MW round 1). And a
  **uniform twin** whose counted point overshot (its `k` was counted on
  the real chain, which may have diverged from the twin chain the
  re-seed built) **wraps `k` into its own measured total** — `1 +
  (k−1) % sched_yields`: an unfired crash means the attempt ran to
  completion, so its own `sched_yields` **is** its total (seeds
  4139/7719). `verify`'s re-run default became 16 workers (digests are
  thread-count independent, pinned; a 4-thread re-run of 10k seeds
  does not fit a 60-minute budget). A re-run that still does not fire
  trips the fired-guard loudly — never a silent pairing break — and
  X5-R21 (c) (04063d1b) then re-pairs the real round's retry with its
  twin (below).

Result in every 10k file since: **0 defect seeds, 0 unfired rounds**
(`DEFECT_SEEDS=0` in bc4f3bba and in P's esc-2 campaign). No workload
change (X5-R17's retune is X-5c-F4's, per X5-R31; F1's text said
X-5c-F2's, before the split).

**NO-TWIN (instrumentation: harness) — fixed, pinned (commit
0a8f196b).** 19 MP/MW rounds failed as "MP/MW round with XX001 and no
twin (harness defect)". Root cause: the twin supply was built only
when the twin round's recovery returned `Ok`; a twin whose recovery
returned `Err` supplied nothing and broke the twin chain, so the real
round (and every later round of the seed) reached the oracle with no
O-3 (c) sets. The fix: a failed twin still supplies its recovery
reads (collected on both arms) and its crashed-disk image slots (the
image its recovery tried to open), and a broken twin chain re-seeds
from the real round's own start (Work 2.5's twin: the same script,
crash and F-CRASH resolutions, faultless, judged as class C). The
repro seeds (1267, 1769, 2149 checked) clear the text; their
remaining failures are real O-3 findings — F2's classes. Pin
`r16_no_twin_is_always_supplied` (literal configs 1267 and 1769),
demonstrated failing on the reverted supply.

**X5-R21 (d), the esc-1 verdict's 2e re-seed question, answered from
the code as it is:** *when a broken twin chain re-seeds from the real
round's own start, is the round's twin still Work 2.5's twin (the same
script, crash and F-CRASH resolutions)?* Yes, on every path, and no
path breaks silently:

- the re-seed (`workload.rs`, the `twin_chain.is_none()` arm at each
  MP/MW round) rebuilds the twin chain from the **real round's own
  start** — the same disk, store, model and cut the real round runs
  from — so the re-seeded twin runs the same script base and the same
  drawn crash spec as its real round;
- a **targeted** twin on the re-seeded chain that does not fire
  re-derives from its own complete masks (`tch_retry`), and the real
  round then **follows the twin's fixed spec** (Work 2.5: the same
  crash point). When the real round's own draw does not fire either —
  the divergence 4fedc648 closed — the real round re-derives, and
  X5-R21 (c) (04063d1b) re-runs the twin **at that same re-derived
  spec, from its own start (`tch_pair`), before the real round uses
  its supply and nested points**, replacing the first attempts' twin
  record, supply, nested points and chain continuation. Pin
  `r21c_the_real_rounds_retry_re_pairs_its_twin` (the ruling's five
  seeds 2669/4049/4309/7469/9809), demonstrated failing with the
  pairing re-run disabled (`E4_R21C_REVERT=1`, gate reverted clean);
- a **uniform** twin whose counted point overshot on the re-seeded
  chain wraps its `k` into its own measured total (4fedc648; the real
  round's `k` is counted on its own chain and fires there — the
  record's crash desc is the real round's point);
- a re-run that still does not fire trips the fired-guard loudly
  (`crash_fired_defect` is checked on every re-run's outcome), so a
  re-seed can never leave the round silently unpaired, and the
  `SeedRec.twin_paired` diagnostic (never a digest input, never
  serialized; `E4_PAIR_DIAG` prints it) records the invariant per
  round — the twin's final attempted spec equals the real round's
  final attempted spec on every targeted round.

In P's esc-2 campaign the guard holds at 10k scale:
`DEFECT_SEEDS=0` (no fired-guard trip anywhere) and every MP/MW round
carries a judged twin (2,180 + 1,415 twin records; 103 failing —
store classes, not pairing).

**MP-E: O-3 (e) under page damage — the record after run 3 and the
esc-2 relaunch (X5-R20, X5-R23, X5-R24).** The class had two halves;
each is now fixed, escalated, or re-attributed, per shape:

- **Completeness, the header half (205 d2c8bba7 rounds, "tainted
  referenced slot … not reported") — a harness defect, fixed
  (12b54965).** Every one is a recovery-`Err` round, and the repro
  seeds 47/118/198 each name **slot 0**: the §2.1 **file header**,
  which §3.3's reference model has no reference for, so amcheck (i)
  can neither check nor report it — but the harness's referenced set
  hardcoded it, making every media fault on its range an unreportable
  "tainted referenced slot". The referenced set is now the image's own
  slots (map, free set, metas); the header stays covered by open's own
  read (XX001 at `Open`). Pin
  `r16_mpe_referenced_set_excludes_the_file_header` (literal configs
  47/118/198), demonstrated failing on the reverted `used.insert(0)`.
- **Exactness, shape A (177 bc4f3bba rounds: the reported slot is a
  meta slot 1 or 2, `mrb=false`, `rlen=0`) — a harness defect, fixed
  (837200a7; accepted, X5-R24).** In every one, a Q14 fault kills the
  only written meta (seed 67: LOST on the create's k=0 write, slot 2
  never written; seed 268: two MISDIRECTED with the same effect), so
  open finds no valid meta, recovery returns `Err` at `Open`, and
  X5-R5 makes O-3 (e) vacuous — but the harness's walk hardcoded
  `slot: Some(1)` on the NoValid arm, and the frozen oracle's
  exactness half judged that bogus slotted finding against empty
  taint/R sets. The finding is now structural (`slot: None`), like the
  `MisplacedNewer` arm; the damage stays detected at `Open`, never
  served. The four ruling questions are answered from `E4_MPE_DIAG`
  evidence (per-slot meta parse/pick, the fault list, the amcheck
  sets, the per-slot durable-write AND issued-write histories). Pin
  `r20_mpe_no_opened_image_means_no_slotted_finding` (literal configs
  67/268), demonstrated failing on the reverted `slot:Some(1)`.
- **Exactness, shape B (39 rounds: a non-meta slot, `mrb=true`,
  `rlen=0`) — a harness defect, fixed (80dc0ff3; accepted, X5-R24).**
  R5's condition 3 reads "SimDisk **records a page-file write to `s`
  issued** after the first completed `sync_all` that covered a meta
  newer than the opened one", but the draft computed R from
  `dwrites_of` — the sync-covered list — only, so a slot reused after
  the newer graduation whose reuse writes were unsynced at the crash
  never entered R. Seed 38 round 0 is the ruling's narrative word for
  word: the Q14 fault removes meta k=1, open rolls back to the
  create's k=0, the epoch-2 checkpoint reused k=0's freed map-root
  slot 4 (ten `WriteIssued` after the graduation sync ev[128], the
  crash keeping ev[287]'s id-6 page; untainted by 1.4, stale
  create-era reference CRC — §3.3 fails). R now consults both
  histories (dwrites cross-life, plus this life's event stream
  anchored at the in-stream graduation sync when that pair is the
  earliest covered newer meta, else at 0). Seeds 38/137: `rlen 0→1`,
  O-3 (e) clears. Pin `r20_mpe_r_reads_the_issued_write_history`
  (literal configs 38/137, asserting `rlen ≥ 1` on the rollback
  round), demonstrated failing with the events half disabled.
  **Judgment call for P's gate, standing:** this computes R per the
  text's word "issued" rather than the committed sync-covered
  reading — R5 not reopened, R's definition unchanged, the structural
  guard (R ⊆ untainted referenced slots) and the M-NOINPLACE
  condition intact; the P gate will also examine the anchor-0
  fallback (an in-stream newer meta exists but `same` is false) —
  **not changed in this card** (X5-R24).
- **The completeness residual (6 bc4f3bba rounds: 1787, 5808, 6157,
  8598, 9098, 9647; all `mrb=true`, real slots 10/42/43/45/7/130) —
  ESCALATED to P as an oracle-definition question (X5-R23).** Run 3
  "fixed" it (31d68b5f) by making the harness's amcheck (i) walk push
  a ground-truth finding for every tainted slot of the referenced
  set. P reverted that commit (c8e578b7): the pushed set is exactly
  what the frozen oracle's completeness half compares amcheck (i)
  against, so in class MP the check became circular for every
  non-meta referenced slot — a §3.3 detection miss at (i), the very
  failure the half exists to catch, would pass. That weakens a Work 2
  check. The 6 rounds return to failing O-3 (e): in each, a slot's
  durable bytes equal the content the opened, rolled-back image
  references (for example a crash-dropped volatile write), so §3.3
  passes against that image, yet 1.4 calls the slot tainted — which
  "tainted" O-3 (e) completeness should use under a meta rollback is
  a question only P can decide; the oracle is frozen. The evidence
  (from 31d68b5f's record): seeds 1787/6157 round 0 — slot 10 with
  two sync-covered writes dw[11]/dw[22] and the dropped write issued
  after them, and slot 42, both §3.3-clean and tainted. Handed to
  X-5c-F2 as its own item, "MP-E completeness residual (oracle
  question)". **Never re-implement it: no harness-side ground-truth
  report may stand in for amcheck to satisfy the oracle.** The
  meta-slot rollback-fallout loop (X-5b, `durable_image`, slots 1 and
  2) has the same structure, is unchanged, and is likewise listed as
  an open P question in the hand-off.
- **Exactness, shape C (6 rounds: a non-meta slot, `mrb=false` —
  seeds 688, 3718, 4858, 5238, 5638, 9717) — ESCALATED to F2,
  evidence-only (X5-R24).** New `E4_MPE_DIAG` evidence at c8e578b7
  (one single-seed run each, allowed under X5-R25, stderr outside the
  worktree):
  - **Seed 688 round 0** (MP, crash `After2@2`, one MISDIRECTED on
    page slot 46 = mapped lpid 5): amcheck (i) finds exactly slot 46
    (`tainted {46}`) — the damage correctly detected; recovery opens
    meta k=2 (slot 1); O-3 (e)'s text names **slot 19**, which comes
    from **amcheck (ii)** on the post-recovery, post-harness-checkpoint
    end disk (image k=3): the store's own amcheck reports slot 46
    ("tree: §3.3 fail on lpid 5") **plus 16 "mapped lpid N
    unreachable" findings** (slots 3, 19, 20, 24, 25, 26, 28, 29, 31,
    34, 36, 39, 41, 49, 51 → lpids 8–37) — I-LPID-EXACT violations in
    the store's own graduated image, on slots that are neither tainted
    nor in R.
  - **Seed 3718 round 0** (MP, crash `uniform@y:8906`, TORN slot 49 =
    lpid 40 + LOST slot 37 = lpid 7): (i) finds exactly 37 and 49,
    both tainted — correct; recovery opens k=1 (slot 2); amcheck (ii)
    on the end disk (image k=2) reports slot 37 (§3.3, correct) plus
    15 "mapped lpid unreachable" findings (slots 8, 10, 17, 19, 22,
    25, 31, 36, 38, 40, 42, 44, 49, 52, 54 → lpids 12–46). The
    reported slot 10 ("mapped lpid 12 unreachable") is the round's
    O-3 (e) text.

  **Provisional classification (P's gate decides):** not an
  instrumentation artifact — this is the store's own amcheck on the
  store's own end disk, and the (i)-half detections of the actual
  damage are correct and complete. The recovered store's next
  checkpoint graduates an image whose **map holds entries no tree
  reaches** (I-LPID-EXACT, S-STORE's map-exactness invariant;
  plausibly the interrupted checkpoint's step-2 page writes adopted
  into the crashed image's map and copied forward, or replay dropping
  tree references the map kept). That is a prototype deviation or an
  S-STORE silence on the recovered map's exactness — F2's to
  root-cause beside C-XX (the same "findings with no fault" family,
  here structural rather than §3.3). **No code change was made
  (X5-R24); no proposed diff is offered** — no fix was verified, and
  the two candidate mechanisms above are F2's first fork. The six
  rounds are unchanged in the esc-2 file (still the class's only
  exactness rounds, `mrb=false rlen=0`).
- **History — the run-2 exactness escalation on seed 37 is superseded
  (X5-R20).** Seed 37 round 0 **passes** in bc4f3bba and in the esc-2
  file (`mrb=false`, no O-3 text): the 12b54965 referenced-set change
  shifted the fault draw off the meta slot, so the rollback it
  described no longer occurs. Its mechanism record (a map-reference /
  durable-content mismatch inside a Q14 rollback round, slot 11, R
  correctly empty per R5's text) is kept as history for F2's shape-B
  analysis; it is no longer a repro.

## The fresh 10k triage campaign (X-5c-F1's run 2, committed at
## bc4f3bba — superseded as the triage file of record by P's esc-2 run
## below; its digest-stability record and class counts stand)

`run --seeds 0..10000 --threads 16` at the fixed code (X5-R14's gate
and unwind, X5-R15's reporting, NEVER-FIRED, NO-TWIN and MP-E's
completeness half): **exit 0, ~12 min wall at 16 threads, peak RSS
5.6 GiB** — the `rss=` progress lines rose steadily from 4,128 MiB at
500 seeds to 5,632 MiB at 9,500 (about 150 KiB per seed; X5-R15 (d)'s
recorded trend, not a blocker under X5-R13's 24 GiB budget). Final
lines, verbatim:

```
E4 SEEDS=10000 O1_FAIL=2711 O2_FAIL=101 O3_FAIL=850 DEADLOCK=41 PANIC=0 O1F_FAIL=2722 MONITOR=0
E4 FOLD FAILING_SEEDS=1296 FAILING_ROUNDS=1247 TWIN_FAILING_ROUNDS=102 DEFECT_SEEDS=0
```

(d2c8bba7: `O1_FAIL=2712 O2_FAIL=98 O3_FAIL=1029 DEADLOCK=46 PANIC=0
O1F_FAIL=2722 MONITOR=0`; the honest fold of that file was
`FAILING_SEEDS=1598 … DEFECT_SEEDS=150`.) Rounds: C 11,781 / MP 2,180
(+ 2,180 twins) / MW 1,415 (+ 1,415 twins).

**`verify --no-quotas`** re-ran every seed at 16 workers: **the digests
reproduce exactly and the seed set is exactly 0..9999** (the four
X5-R14 seeds now stable); the quota table prints below without judging
the run (X5-R15 (b) — the retune that would meet the missed quotas is
X-5c-F2's, X5-R17).

**Digest stability vs d2c8bba7 (X5-R14's pin 3):** 3,507 of 10,000
digests changed, 6,493 are unchanged — **the seed list is
`results/digest-stability.tsv`** (X5-R21 (a)/X5-R26): every changed
seed with its old and new digest and its cause(s) in commit order
(2,627 at the entry-park step alone; 204 more at the NEVER-FIRED
re-draw; 19 at the NO-TWIN supply; 856 at the MP-E
referenced-set/redesign step — the esc-2 section's digest-stability
record gives the split and the mechanism notes). The mechanism
explains the shift
uniformly, not seed by seed: every multi-task workload world previously
contained window-A service reads (the host-mutation log shows them in
every such world), and every crashed world contained window-B unwind
steps, both interleaved by OS timing; the fix removes both, so every
seed whose schedule depended on one changed. The unchanged 6,493 are
the seeds whose service reads happened not to perturb the recorded
outcome. The fresh file's digests reproduce across runs and thread
counts (verify's re-run; the thread-count pin).

**The instrumentation classes after the fixes (round counts before →
after):**

| Class | d2c8bba7 | this run |
|---|---|---|
| NEVER-FIRED (defect seeds) | 150 | **0** |
| NO-TWIN (rounds) | 19 | **0** |
| MP-E completeness (rounds) | 205 | **6** (residual: real slots 10/42/43/45 — seeds 1787, 5808, 6157, 8598, 9098, 9647; F2's triage) |
| MP-E exactness (rounds) | 199 | 222 — shapes A 177 / B 39 / C 6 (A and B fixed in run 3; C and the exactness record are in the class section; the esc-2 table below has the final counts) |

**The hand-off for X-5c-F2** (counts in this file; two repro seeds
each, `run --seeds N..N+1`):

- **C-DIV** (class C scan-mismatch / raw-above-cut O-1): 324
  scan-mismatch seeds + 95 raw-above-cut seeds — repros 21, 46
  (unchanged from d2c8bba7's list). **Correction of this line's first
  record** ("334 scan + 544 raw-above"): the 544 counted 447 MW
  raw-above seeds beside the class C ones (MW-ORPHAN's superset,
  double-counted here); the class C counts are 324 and 95.
- **C-XX** (class C amcheck/XX001 findings with no fault): 101 O-2
  seeds + 127 O-3 seeds (overlapping sets) — repros 221, 66.
- **MP-LIVELOCK** (DEADLOCK seeds, all MP round 1 after an R6 round):
  41 — repros 987, 1108.
- **MP-A** (served corruption): 7 seeds — repros 1897, 2437 (every one
  a Q14 meta-fault round; P classifies).
- **MW-ORPHAN** (MW raw-entry-above-the-cut with O-3): 439 — repros
  39, 59.
- **The twins**: 102 failing twin rounds across 91 seeds (44 MP +
  58 MW) — **X5-R21 (d) correction** of this line's first record (it
  said "91 failing twin rounds (43 MP + 48 MW)", mixing the seed count
  into the round count and mis-splitting the classes; P's numbers,
  re-derived from the committed file). The classes echo there.
- **MP-E exactness** (this card's escalation): 222 rounds — the
  shape-A/B/C record above supersedes the "repros 37, 38" of this
  line's first record (seed 37 passes in this file; the shapes are A
  177 / B 39 / C 6, repros 67/268, 38/137, 688/3718).

No new class appeared on the fixed code beyond the residual MP-E
completeness six above.

**RESULT item 5, the interim reading (not E4's RESULT):** the rule
applied verbatim to this file — "*Oracle:* the recovered state equals
the model at some commit c, with (max acked sync-on commit) ≤ c ≤
(last WAL-written commit); amcheck is clean; every corrupted page is
detected (XX001, never served). Any failure means redesign before
Phase A" — gives **n = FAILING_SEEDS = 1,296** (of 10,000), with
1,247 failing primary rounds, 102 failing twin rounds and 0 defect
seeds beside them. The failing classes are X-5c-F2's to root-cause or
escalate; this card's instrumentation fixes removed 302 of d2c8bba7's
1,598 failing seeds and made every remaining one a reproducible
function of `(seed, mutant)`.

## The esc-2 triage campaign (P's run at c8e578b7, committed — the
## file of record)

Run 3 of this card landed X5-R20 shapes A and B and X5-R21 (c), then
all three of its attempts hit the runner's 60-minute limit inside
campaign-class runs. P reverted 31d68b5f (X5-R23, commit c8e578b7),
ran the 10k triage `run` and its `verify --no-quotas` himself at that
tip on a clean tree under X5-R13's backstop, and the esc-2 relaunch
commits that file (X5-R25). **This relaunch ran no campaign-class
command**; the provenance checks before the copy (the prov file's
`commit=c8e578b7… status_lines=0 exit=0 verify_exit=0`, the JSON's
sha256 equal to the prov file's `json_sha256`
`98a7c8c5…bc62516`, and `git diff --quiet c8e578b7 HEAD` over
`src/`, `tests/`, `examples/`, `Cargo.toml`, `v3/Cargo.lock` exiting
0) all passed. **The verify of record is the runner's Acceptance
`verify --no-quotas` at the committed tip** (X5-R25): it re-runs all
10,000 seeds and must reproduce every digest and the exact seed set.
P's own verify (17:37–17:57, exit 0) already did: *digests reproduce
exactly; the seed set is exactly 0..9999; 5 quotas missed, waived*.

Final lines, verbatim (from `X-5c-F1-esc2.campaign.out`):

```
E4 SEEDS=10000 O1_FAIL=2711 O2_FAIL=101 O3_FAIL=635 DEADLOCK=41 PANIC=0 O1F_FAIL=2722 MONITOR=0
E4 FOLD FAILING_SEEDS=1088 FAILING_ROUNDS=1034 TWIN_FAILING_ROUNDS=103 DEFECT_SEEDS=0
```

Wall ~20.6 min at 16 threads (17:16:40→17:37:19), verify ~19.7 min;
the run's `rss=` lines rose from 4,187 MiB at 500 seeds to a peak
5,957 MiB at 9,500 (X5-R15 (d)'s trend, ~180 KiB per seed — not a
blocker under X5-R13's 24 GiB budget). The quota table is byte-equal
to bc4f3bba's (the same five misses — Q4 2/1000, Q7 160/200, Q11
0/100, Q12 1/100, SIZING 14,959 misses; every other quota met, Q1's
ten hooks 751–795) and is carried verbatim in the campaign commit's
message. No code differs from c8e578b7 in any commit of this relaunch.

**The failure table** (rounds: C 11,781 / MP 2,180 + 2,180 twins /
MW 1,415 + 1,415 twins — the same round counts as bc4f3bba; only MP
O-3 moved, 254 → 38 failing MP seeds):

| | O-1 | O-2 | O-3 | O-1f | DEADLOCK | livelock rounds | acked lost (SyncOff) |
|---|---|---|---|---|---|---|---|
| C | 470 | 101 | 127 | 471 | 0 | 0 | 127 (127) |
| MP | 1778 | 0 | 38 | 1778 | 41 | 41 | 28 (28) |
| MW | 463 | 0 | 470 | 479 | 0 | 0 | 1965 (626) |

X5-R6 beside the rule: 360 R6 rounds (MP 314 + MW 46), 5,677
detected-loss comparisons (all MP).

**RESULT item 2 — every 10k run this card made** (E4 lines verbatim;
this relaunch made none):

- **Run 2's triage campaign** (the worker, at 4fedc648; committed at
  bc4f3bba): `E4 SEEDS=10000 O1_FAIL=2711 O2_FAIL=101 O3_FAIL=850
  DEADLOCK=41 PANIC=0 O1F_FAIL=2722 MONITOR=0` / `E4 FOLD
  FAILING_SEEDS=1296 FAILING_ROUNDS=1247 TWIN_FAILING_ROUNDS=102
  DEFECT_SEEDS=0` — exit 0, ~12 min, peak RSS 5.6 GiB, `verify
  --no-quotas` exit 0 (the section above).
- **Run 3's attribution runs** (the worker; labelled as such — they
  exist to separate the digest-stability ladder's steps, and are
  never committed): 9b041a3d seeds 0..10000
  (`O1_FAIL=2705 O2_FAIL=101 O3_FAIL=1036 DEADLOCK=45 PANIC=0
  O1F_FAIL=2716 MONITOR=0`, no FOLD line — it predates dffe6744);
  2da915a8 seeds 0..5000 (`O1_FAIL=1365 … DEADLOCK=3 … O1F_FAIL=1373`)
  and 5000..10000 (`O1_FAIL=1356 O2_FAIL=60 O3_FAIL=545 DEADLOCK=5 …`),
  FOLD 705/687/48/1 and 746/722/54/3; 0a8f196b seeds 0..5000
  (`O3_FAIL=486…`) and 5000..10000 (`O3_FAIL=540…`), FOLD
  705/683/48/1 and 746/717/54/3. A 12b54965 0..5000 run was killed at
  4,000 seeds (X5-R26 bans its use; it was never used).
- **Run 3's campaign attempts** — three, each killed at the runner's
  60-minute limit (no `E4` line exists for them; that is why P ran
  the esc-2 campaign himself).
- **P's esc-2 campaign** (nv3-X-5c-F1-esc-2, at c8e578b7 on a clean
  tree; committed in this relaunch): the two lines at this section's
  top.

**The instrumentation classes, before → after** (round counts;
d2c8bba7 → bc4f3bba → this file):

| Class | d2c8bba7 | bc4f3bba | esc-2 (c8e578b7) |
|---|---|---|---|
| NEVER-FIRED (defect seeds) | 150 | 0 | **0** |
| NO-TWIN (rounds) | 19 | 0 | **0** |
| MP-E completeness (rounds) | 205 | 6 | **6** (ESCALATED to P, X5-R23) |
| MP-E exactness — shape A | 156 | 177 | **0** (fixed, 837200a7) |
| MP-E exactness — shape B | 39 | 39 | **0** (fixed, 80dc0ff3) |
| MP-E exactness — shape C | 4 | 6 | **6** (ESCALATED to F2, X5-R24) |

(Shapes A/B grew d2c8bba7 → bc4f3bba because 12b54965's referenced
set changed the fault draw; they then cleared entirely.)

**Digest stability (X5-R21 (a) as amended by X5-R26) —
`results/digest-stability.tsv`, built offline.** Columns: seed,
d2c8bba7 digest, bc4f3bba digest, cause(s) in commit order. **3,507
changed / 6,493 unchanged** (the count the bc4f3bba commit recorded).
Per step: d2c8bba7→9b041a3d **X5-R14 entry park 2,627 seeds**;
9b041a3d→2da915a8 **NEVER-FIRED re-draw 204**; 2da915a8→0a8f196b
**NO-TWIN supply 19**; 0a8f196b→bc4f3bba **856 changed**. **The
step-4 attribution is X-5c-F2 step 0's corrected record (offline, from
the per-seed 12b54965 runs of the X-5c-F1 gate's truth file,
`$HOME/.cache/nv3-review/runlogs/X-5c-F1-gate.step4-truth.tsv`, 852
rows): 835 MP-E (795 MP-E only, counting 6868, plus 40 both) and 57
NEVER-FIRED redesign (13 + 40 + 4 — the 4 being 4139/7469/7719/9809,
the 4fedc648 wrap/retry seeds). The first form of this list
mis-attributed 54 of the 852 by the card's field rule refined with
mechanism reading; they are rewritten (MPE → `MP-E referenced set`,
RED → `NEVER-FIRED redesign`, MPE+RED → both, in commit order —
12b54965 MP-E referenced set precedes 6cc53657/4fedc648 NEVER-FIRED
redesign), and every step-4 label now matches the truth file on all
852 of its rows. The gate's truth run is the authority: the field rule
would have been wrong on 107 of the 852.** The mechanism evidence the
first form recorded, kept for P's gate: **every referenced-set channel
is MP-only** — the page-file fault draw biases 60/40 over the
referenced set (`media_faults_common`), so 12b54965 also shifted fault
*targets* at equal counts, and the (i)-walk findings and Q13/Q14 flags
are page-file — while the redesign shows as fired-guard defect / twin
/ nested signatures. Where the letter of the field rule
("crash/hook/fired or round count differs ⇒ NEVER-FIRED") and the
mechanism disagree, the seeds are named: **38 MP seeds whose later
round's crash desc moved at the same hook to a different checkpoint
index with all earlier rounds record-identical** (7, 987, 2307, 8168,
9168…) — the fault-draw shift moving the landed index through a
chain the record cannot see; **the round-count cascades** (97, 148,
158…: a recovery-`Err` flip stops the chain) — referenced-set, not
redesign; and **17 MP nested-only seeds** (318, 498, 587…) — the
shifted fault moves the real recovery's length, so more of the
twin-supplied nested points fire. No seed remained unattributed.
**The "stably wrong old order" demonstration is waived (X5-R26)**;
cited instead: X-5c's own `verify` at 66f33c32 re-ran d2c8bba7's
10,000 seeds at the old code and reproduced every changed seed's
digest except the four X5-R14 seeds (180, 559, 7474, 8832) — the old
file's digests were stable under the old code.
**bc4f3bba → this campaign: 219 seeds changed** (the X5-R20 shape-A/B
fixes, X5-R21 (c)'s re-pairing and the X5-R23 revert; one count line
only — no per-seed attribution, X5-R26).

**The hand-off for X-5c-F2, re-derived from this file** (counts in
`results/campaign.json`; two repro seeds each, verified failing in
this file, `run --seeds N..N+1`):

- **C-DIV** (class C scan-mismatch / raw-above-cut O-1): 324
  scan-mismatch seeds + 95 raw-above-cut seeds — repros 21 (r0 scan
  mismatch), 46 (r1 scan mismatch).
- **C-XX** (class C amcheck/XX001 findings with no fault): 101 O-2
  seeds + 127 O-3 seeds (overlapping sets) — repros 66 (r0, `O-3(b)`
  at `AmcheckIi`), 221 (r0, `O-3(b)` at `Open`).
- **MP-LIVELOCK** (DEADLOCK seeds, all MP round 1 after an R6 round):
  41 — repros 987 (r1, spinning at `h:ckpt`), 1108 (r1, at
  `h:writer`); seed 148 now passes in this file.
- **MP-A** (served corruption, never classified by a worker): 7 seeds
  — repros 1897, 2437 (r0 `O-3(a): served corruption`; every one a
  Q14 meta-fault round; P classifies).
- **MW-ORPHAN** (MW raw-entry-above-the-cut with O-3 (d)): 439 —
  repros 39, 59 (r0 `O-1(c)` + `O-3(d)`).
- **The twins**: 103 failing twin rounds across 91 seeds (44 MP +
  59 MW; bc4f3bba: 102 rounds, 44 + 58) — repros 339, 849.
- **MP-E shape C** (X5-R24's escalation, evidence-only): 6 rounds —
  repros 688, 3718 (both r0 `O-3(e): (ii) reported slot … neither
  tainted nor in R`, `mrb=false rlen=0`; the provisional
  classification and the `E4_MPE_DIAG` evidence are in the class
  record above).
- **MP-E completeness residual** (X5-R23's oracle question, F2's own
  item): 6 rounds — repros 1787 (r0 `tainted referenced slot 10 not
  reported`), 6157 (r0, slot 42); all `mrb=true`. **An open P
  question, not a worker fix: which "tainted" O-3 (e) completeness
  should use under a meta rollback.** The X-5b meta-slot
  rollback-fallout loop (`durable_image`, slots 1 and 2) has the same
  structure, is unchanged, and is listed with it.

No new class appeared on the esc-2 code: class C is byte-equal to
bc4f3bba's counts, class MW moved only inside the X5-R21 (c)
re-pairing's reach (O-3 rounds 469 → 470, failing twin rounds 58 → 59,
acked lost 1,969 → 1,965 — the pairing re-run re-runs twins, whose
outcomes can move), and MP O-3 fell 254 → 38 seeds (shapes A and B
cleared).

**RESULT item 5, the interim reading (not E4's RESULT):** the rule
applied verbatim to this file — "*Oracle:* the recovered state equals
the model at some commit c, with (max acked sync-on commit) ≤ c ≤
(last WAL-written commit); amcheck is clean; every corrupted page is
detected (XX001, never served). Any failure means redesign before
Phase A" — gives **n = FAILING_SEEDS = 1,088** (of 10,000): 1,034
failing primary rounds + 54 seeds failing only through their twins
(103 failing twin rounds), 0 defect seeds. Since d2c8bba7 this card's
instrumentation fixes removed 510 of the 1,598 failing seeds
(1,296 at bc4f3bba; 1,088 here) and made every remaining one a
reproducible function of `(seed, mutant)`.

## Stage X-5c-F2's record (this card)

Status: complete through step 4. Commits (this branch, in order):
acc9c858 (step 0), 962154eb (1a), f908bc69 (1b), 26048928 (1c),
bd0161dd (1d), 57953538 (1e), fe74fc5f (step 2 harness), 5bcb1701
(X5-R28 (b) oracle), 6d40085d (X5-R28 (e) evidence + pin), bdd46cef +
97806d0d (step 3).

**Step 0 — the digest-stability step-4 record corrected (X-5c-F1-F1).**
Exactly the 54 seeds where the X-5c-F1 gate's truth file
(`$HOME/.cache/nv3-review/runlogs/X-5c-F1-gate.step4-truth.tsv`, the
per-seed 12b54965 runs) disagrees with the TSV's old label: 13
RED-truth rows relabeled `NEVER-FIRED redesign`, 40 MPE+RED-truth rows
given both causes in commit order (12b54965 `MP-E referenced set`
precedes 6cc53657/4fedc648 `NEVER-FIRED redesign`), and 6868 (truth
MPE) dropped its spurious redesign half. Earlier steps' causes are
untouched; all 852 truth-file rows match their step-4 labels
afterwards. **The corrected counts: 835 MP-E (795 MP-E only, counting
6868, plus 40 both) and 57 NEVER-FIRED redesign (13 + 40 + 4 — the 4
being 4139/7469/7719/9809). The field rule would have been wrong on
107 of the 852** (P's count, from the gate's truth run). The
paragraph's "provably derives the same final specs" and "the mechanism
won" are deleted; the mechanism evidence itself is kept for P's gate.

**Step 1 — harness hardening (X-5c-F1-F2), one commit per item.**

| Item | Change | Pin | Revert demonstration |
|---|---|---|---|
| (a) | `reachable_hook_spec` returns `Err` when the mask list is empty or its union is 0 (the draft's fallback loop spun forever — a hang in the repair path); the callers record a seed-level defect beside the fired-guard's, and no re-run happens | `f1f2a_empty_masks_are_a_defect_not_a_hang` (spawned thread, 5 s channel timeout; `&[]` and `&[0, 0]`) | early return disabled (`if false && union == 0`): FAILED in 5.00 s — the timeout itself |
| (b) | the R21 (c) re-pair block's update factored into `twin_chain_after_repair(&RoundOutcome) -> Option<Chain>`: a re-paired twin whose recovery returned no store sets `twin_chain = None` (a re-seed from the real round's own start), never keeps the first attempt's chain at the old spec | `f1f2b_repairs_without_a_store_reseed_the_twin_chain` | the None arm synthesized a chain anyway (the draft's keep-the-chain defect): FAILED |
| (c) | none — `fold_line` (X5-R15 (a)) already counts the twin term; the pin locks it | `f1f2c_fold_counts_a_twin_only_failure` (a literal `SeedRec`, primaries passing, one failing twin → `FAILING_SEEDS=1 FAILING_ROUNDS=0 TWIN_FAILING_ROUNDS=1`) | M13 (the twin term dropped from the fold): FAILED |
| (d) | none — the events-half anchor is extracted into the shared `r5_anchor` (behavior unchanged); the pin locks it at the sync's return | `f1f2d_r_anchor_is_the_sync_not_the_issue` (a literal stream: newer-meta issue, slot-7 write, `SyncReturned{all:true}`, slot-9 write → 7 ∉ R, R = {9}) | M8 (anchor at the meta's issue, `w` instead of `w+1+s`): FAILED |
| (e) | `r5_anchor0_defect(events, pf)` returns `"page file synced by sync_data: X5-R5 anchor precondition"` (`R5_ANCHOR0_DEFECT`); `run_round` flags it on `RoundOutcome.r5_anchor0_defect` only when the fallback anchor 0 is actually in force (`r5_anchor` → `Some((_, 0))`) **and** the stream holds a page-file `sync_data` return; `run_seed` lifts it to `rec.defect` verbatim. R's own computation is unchanged — never silently widened. This store only ever `sync_all`s the page file, so the tripwire cannot fire on a real campaign | `f1f2e_page_file_sync_data_yields_the_anchor0_defect` (with the event → the exact text; a sync_all or another file's sync_data → None) | the check removed (the function answering `None` unconditionally): FAILED |

**Step 2 — X5-R28 + X5-R30 (a).** The harness (fe74fc5f):
`kept_set_k` computes `K` beside `exception_set_r` from SimDisk history
only, with the four conditions as ruled; `v_at_meta`/`meta_dwrite_cutoff`
compose `V_k(s)`; `K_ii` at the (ii) point against the end image's own
meta; the meta-slot rollback-fallout loop is **deleted** from
`durable_image` (X5-R28 (c) — the walk never pushes a ground-truth
finding; slots 1/2 stay in `image_slots`); `served_k` (X5-R30 (a)) is
computed from every served range of the round (the workload pass's
list captured before `new_round`, the recovery passes', the extraction
world's) against `K`, and rides the round record (`sk` in the JSON,
beside `served` in the digest input) — a diagnostic, never an oracle
input. The oracle amendment (5bcb1701) changes `src/oracle.rs` and
`tests/oracle.rs` **only by X5-R28 (b)'s text**: `Extracted` gains
`kept_slots`/`kept_slots_ii`; the structural guard
(`O-3(e): harness K set malformed (slot N)`, both halves, whether or
not `r_active` holds); the completeness halves skip slots 1 and 2 and,
under `r_active`, `K`/`K_ii`; the exactness tightening
(`O-3(e): amcheck reported kept slot N (its bytes are the opened
image's content)`); and X5-R6's `explained` becomes "some tainted
referenced slot outside K, or an active non-empty R". New oracle
cases: mf20–mf23 and mp7, plus `instrumentation_removals_are_load_
bearing`'s K arm — each demonstrated on its own clause's revert (see
the mutant table below). Interim note, recorded honestly: between
fe74fc5f and 5bcb1701 the directed d10b/d17 and workload r20 (seed 38)
tests failed with "tainted referenced slot 1/2 not reported" — the
card's ordered steps (harness first, then the one oracle commit);
cleared by the amendment.

**X5-R28 (e) — the six residual seeds clear via K** (single-seed runs
at 6d40085d's parent tip, `E4_MPE_DIAG=1`, artifacts at
`$HOME/.cache/nv3-review/runlogs/X-5c-F2.r28e.<seed>.*`):

| Seed/round | K | V_k(s) = durable (crc32c) | new O-3 text | served_k |
|---|---|---|---|---|
| 1787 r0 | {10} | `8186a812` | none (clear) | true |
| 5808 r0 | {43} | `0174e829` | none (clear) | true |
| 6157 r0 | {42, 56} | `6587a85e` / `420d0f49` | none (clear) | true |
| 8598 r1 | {45, 80, 94} | `77083692` / `b10d7ba7` / `d7a8afb9` | none (clear) | true |
| 9098 r1 | {7} | `d76bf9ea` | none (clear) | true |
| 9647 r1 | {130} | `f4e1951f` | none (clear) | true |

Every named slot (10, 43, 42, 45, 7, 130 — F1's hand-off list) is in
`K`; all six rounds pass with no O-3 text and `served=false`
(`served_k` vacuously true). **No ESCALATE under (e): no seed's slot
fell outside K.** The named slot is in K by the M-R28g demonstration:
with the harness K disabled, seed 1787 fails again with exactly
"O-3(e): tainted referenced slot 10 not reported". Pin:
`r28_residual_seeds_clear_via_k` (seeds 1787/6157).

**X5-R30 (a) — the MP-A `served_k` table** (read-only, single-seed
runs at `$HOME/.cache/nv3-review/runlogs/X-5c-F2.mpa.<seed>.*`; all
round 0, all `mrb=true`, all `served=true`, O-3 (a) unchanged):

| Seed | served_k |
|---|---|
| 1897 | true |
| 2437 | true |
| 4718 | true |
| 2927 | false |
| 4378 | false |
| 7528 | false |
| 8728 | false |

Three of the seven served corruptions lie wholly inside `K` — the
X5-R30 proxy-flaw candidates; the other four have served ranges
outside `K`. Evidence for F3's gate; no judgement here.

**Step 3 — X5-R29 shape C: the branch is (c).** The diagnostic
(`r29_subtree_diag`, `E4_MPE_DIAG` stderr only) computes, on the end
image, the failed lpids, each failed node's descendant set (children
parsed through the store's own decoder — from the failed node's
`V_e` composed as X5-R28 (a), intact descendants from their own
durable bytes), and the store's "mapped lpid N unreachable" set.
Per-round (the Mp round; artifacts at
`$HOME/.cache/nv3-review/runlogs/X-5c-F2.r29.<seed>.*`):

| Seed/round | failed lpids | descendants | unreachable | equal |
|---|---|---|---|---|
| 688 r0 | {5} | {8,9,11,14,16,18,20,22,24,26,27,30,32,35,37} | same 15 | yes |
| 3718 r0 | {7, 40} | {12,13,17,19,22,24,26,29,32,34,36,39,40,44,46} | same 15 | yes (40 is both failed and a descendant of 7) |
| 4858 r0 | {2} (its `V_e` is a leaf — no children) | {} | {6, 7} | **no** |
| 5238 r0 | {6} | {17,18,22,25,27,30,33,36,38,41,43} | same 11 | yes |
| 5638 r0 | {5, 35} | {9,10,13,14,17,19,21,24,27,31,33,35,38} | same 13 | yes (35 both) |
| 9717 r0 | {} | {} | {} | vacuously — no unreachable findings at all |

The deciding evidence is 4858: the **faultless twin's** own amcheck
(ii) reports exactly the same two orphans — "mapped lpid 6
unreachable" (slot 18) and "mapped lpid 7 unreachable" (slot 9) —
with no §3.3 failure anywhere on its end disk, and its O-1 fails
("O-1(b) at c=9, tree 2: scan mismatch"). The lpids are true orphan
map entries in the recovered image (I-LPID-EXACT), not fallout of the
damaged node. **Per X5-R29 (c): amcheck is NOT changed; shape C is
handed to F3 as a C-XX-family store deviation with this per-lpid
evidence.** Four rounds (688/3718/5238/5638) show exact subtree
fallout and would have supported (b)'s precision fix; the ruling's
fork is strict — differ in any round → (c). 9717 is a third mechanism
inside the same record-field class: no orphans, but "tree: separators
out of order" (slot 50, lpid 3) and "tree: lpid 36 reachable more
than once" (slot 36) — F3's beside the orphans.

**Every pin of this card, with its revert demonstration.**

| Pin | Catches | Revert demonstration |
|---|---|---|
| `f1f2a_empty_masks_are_a_defect_not_a_hang` | the empty/zero-mask hang | early return disabled: FAILED in 5.00 s (the timeout) |
| `f1f2b_repairs_without_a_store_reseed_the_twin_chain` | the re-pair block keeping a stale chain | the None arm synthesized a chain: FAILED |
| `f1f2c_fold_counts_a_twin_only_failure` | M13, the FOLD twin term | the twin term dropped: FAILED |
| `f1f2d_r_anchor_is_the_sync_not_the_issue` | M8, the anchor at the meta's issue | anchor = `w`: FAILED |
| `f1f2e_page_file_sync_data_yields_the_anchor0_defect` | the precondition assert removed | the check answering `None` unconditionally: FAILED |
| `r28_v_k_is_the_history_at_the_opened_meta` | M-R28e, `V_k` using the latest DWrite | cutoff = `dwrites.len()`: K = {} — FAILED |
| `r28_no_ground_truth_meta_finding` | M-R28f, the fallout loop restored | the loop restored pushed slot `Some(2)`: FAILED |
| `r28_residual_seeds_clear_via_k` | M-R28g, the harness K disabled | `r5_active` forced false: seed 1787's completeness text returned — FAILED |
| `mf20_tainted_outside_k_not_reported_fails` | M-R28d, completeness skipping every tainted slot | the skip made unconditional: FAILED |
| `mf21_reported_k_slot_fails` | M-R28b, the exactness tightening removed | the tightening arm disabled: FAILED |
| `mf22_malformed_k_fails` | M-R28c, the K guard removed | the guard disabled: FAILED with the wrong text |
| `mf23_unreported_k_without_meta_slot_fault_fails` | M-R28a, the K skip without `r_active` | the skip unconditional: FAILED |
| `mp7_unreported_k_slot_passes` | the amendment's positive case | (must-pass; guarded by mf20–mf23's reverts) |
| `instrumentation_removals_are_load_bearing` (K arm) | the K clauses removed from the record | the kept set cleared: the same findings pass |

M-R29a/M-R29b do not apply: branch (c) made no amcheck change.

**The Acceptance 10k (X5-R31): not this worker's to run.** The runner
executes, at this card's final tip: `cargo test -q --release -p
e4-recovery`; then the 10k (`run --seeds 0..10000 --threads 16 --out
$HOME/.cache/nv3-review/runlogs/X-5c-F2.acc.campaign.json`, under
`ulimit -v 25165824`) with its provenance in
`$HOME/.cache/nv3-review/runlogs/X-5c-F2.acc.prov` (commit,
`status_lines`, start/end, load, exit, the JSON's sha256); then
`verify --no-quotas` of that file (its head and exit appended to the
prov file); then `bash scripts/gate.sh`. X-5c-F3's step 0 commits
that file after the provenance checks. No campaign-class command ran
inside this card's attempts (only single-seed runs); the committed
`results/campaign.json` stays P's c8e578b7 esc-2 file.

## Stage X-5c-F3's record (this card)

**Step 0 — F2's Acceptance 10k committed as the triage file of record
(b9dd5a9a), and this card's class table re-derived from it.** The three
provenance checks passed before the copy (the prov file's
`commit=1f8e7c3f… status_lines=0 exit=0 verify_exit=0`; the JSON's sha256
equal to its `json_sha256` `56e47fb2…f34033`; `git diff --quiet
1f8e7c3f HEAD` over `src/`, `tests/`, `examples/`, `Cargo.toml` and
`v3/Cargo.lock` exiting 0 — the merged tip 32dc3375 is F2's verdict
commit on top of 1f8e7c3f, which touches only `nucleus/v3/verdicts/`).

Final lines, verbatim:

```
E4 SEEDS=10000 O1_FAIL=2712 O2_FAIL=101 O3_FAIL=624 DEADLOCK=41 PANIC=0 O1F_FAIL=2723 MONITOR=0
E4 FOLD FAILING_SEEDS=1070 FAILING_ROUNDS=1015 TWIN_FAILING_ROUNDS=103 DEFECT_SEEDS=0
```

**The class table, re-derived from the committed file (every count
reproduces P's Goal re-derivation exactly).** Field definitions: a
**failing** primary round has `pass=false` in the pass fold; a **failing
twin** round has `pass=false` in the twin record (which carries only
`pass` and the `o1` text). All counts are over primary rounds unless
"twin" is in the name.

| Class | Rounds | Record-field definition (re-derived, verifiable from the JSON) | Repros (round) |
|---|---|---|---|
| **C-XX** (class C) | 130 | class C failing round with `o2 != null` ("class C amcheck(i) not clean") or `o3` = `O-3(b): XX001 in class C at [site]` — sites: `Open` 45, `AmcheckIi` 82, and 3 rounds with `o2` only (no O-3(b)); 98 carry both `o2` and O-3(b), 29 O-3(b) only, 8 have no `o1` at all | 221 (r0, `Open`), 66 (r0, `AmcheckIi`) |
| **C-XX** (the same shape in MP/MW: O-3(c) with no damaged referenced slot) | 15 MP + 13 MW | MP/MW failing round with `o3` = `O-3(c): XX001 with every media fault on unread, unreferenced targets`; in MP 7 stand alone (907/1167/4167/7367/7748/8837/9508 r0–r1) and 8 sit inside MP-LIVELOCK rounds | MP 907 (r0; 528 now passes), MW 1769 (r0) |
| **C-DIV** | 424 | class C failing round carrying an `o1` text, **excluding** rounds whose `o1` is an O-1(a) inside a C-XX round (46 of the 50 (a) rounds: 45 are the recovery-`Err` default `published {} != model {…}`, plus seed 8764 r1 whose genuine (a) sits beside its O-3(b)). Kinds: (b) 267 (266 "scan mismatch" + 1382 r0's "scan read failed", itself an O-2 round), (c) 95, (d) 58, (a) 4 (4004 r0, 7531 r1, 8480 r2, 8804 r1). By round index: r0 88/7,000 (1.3%), r1 70/3,424 (2.0%), r2 266/1,357 (19.6%) | 21 (r0, b), 46 (r1, b), 36 (r2, d), 10 (r2, c) |
| **MP-LIVELOCK** | 41 | MP failing round with `ll` or `dl` set; livelock site `lls`: `h:writer` 19, `h:evictor` 15, `h:ckpt` 7; 38 at round 1, 3 at round 2; every one's previous round has `r6=true, pass=true, o1dl>0`; 8 also carry the C-XX O-3(c) text | 987 (r1, `h:ckpt`), 1108 (r1, `h:writer`), 1247 (r1, `h:evictor`) |
| **MP-A** | 7 | MP failing round with `served=true`: 1897/2437/2927/4378/4718/7528/8728, all round 0, all `mrb=true`, all recovery-`Err` (Q14 meta-fault rounds) | 1897, 2437 |
| **MW-ORPHAN** | 446 | MW failing round with `o1` = `O-1(c) … raw entry above the cut` and `o3` = `O-3(d): silent Ok with O-1 failing` | 39 (r0), 59 (r0) |
| **Other MW** | 26 | the MW failing rounds outside MW-ORPHAN: 13 O-3(c) (the C-XX shape above), 10 O-3(d) with O-1(a), 1 O-1(a) only (3399 r0), 2 O-1f only (2019 r2, 4049 r1) | 3399, 2019 |
| **Other MP** | 4 | MP failing rounds outside every class above: 6028 r0, 7388 r1, 9548 r1 (O-1(b) scan mismatch) and 8788 r1 (O-1(c)); all recovery-`Ok`, no O-3 text | 6028 |
| **MP-E shape C** (handed on by X5-R29 (c)) | 6 | MP failing round with `o3` starting `O-3(e): (ii)`; all `mrb=false, rlen=0`: 688/3718/4858/5238/5638/9717 (all r0) | 688, 4858, 9717 |
| **The twins** | 103 | failing twin rounds: 44 MP over 43 seeds + 59 MW over 48 seeds; their `o1` texts: MP (b) 28, (a) 10, (c) 2, (d) 1, none 3; MW (b) 23, (a) 21, (c) 8, (d) 6, none 1 — the classes echo there (the twin is judged as class C) | 339 (MW r0, (c)), 849 (MW r1, (a)), 907 (MP r0, (b)) |

Partition check: C 478 = 130 C-XX ∪ 424 C-DIV (overlap 76 rounds); MP 65
= 41 LIVELOCK + 7 MP-A + 7 C-XX-alone + 6 shape C + 4 other (the 8
C-XX-inside-LIVELOCK counted once, in LIVELOCK); MW 472 = 446 ORPHAN +
26 other. Total failing rounds 1,015; failing seeds 1,070 (1,015 failing
primary rounds + twin-only failures and seed-level flags).

**Step 1 — C-XX: two root causes fixed, one sub-family escalated (commit
ad82f7d5).**

- **Sub-class A, the LPID allocator's highest-LPID shrink — root-caused
  as a prototype deviation from S-STORE §4.2/§4.3 and fixed.** The cause
  is `prims::free_lpid`'s `next_lpid -= 1` branch (present since the
  X-5 attempt-2 draft, fabc1c0a): freeing the highest LPID decremented
  the live counter while a flip-window free's `map.img` override keeps
  the page's epoch-k content for the image being built, so the frozen
  radix mapped an LPID the meta's own span `[1, next_lpid)` says does
  not exist — amcheck (i)'s structural "next_lpid not above every
  mapped lpid" in faultless class C rounds (seeds 221/66; the MP shape
  907 — verified: `free_lpid 6 decrements next_lpid 7 → 6` in the
  workload, the image k=1 radix mapping lpid 6 under `next_lpid: 6`),
  and a recovery that re-freed the LPID hit `lpid N unmapped`
  (`fetch_page`'s map miss — an XX001 at Open with no fault at all).
  The fix: `free_lpid` always inserts into `free_lpids` (§4.2: "Reuse
  is immediate, in memory" — through the free set; the counter is
  monotone, which is why §4.3's bitmap is defined over `[1, next_lpid)`).
  Pins: `cxx_highest_lpid_free_never_shrinks_next_lpid` (structural:
  three allocations, the highest freed — monotonicity, the free-set
  entry, immediate lowest-free reuse) and
  `cxx_highest_lpid_free_never_shrinks_next_lpid_rounds` (221/66/907:
  every round passes). Mutant M-CXXa (the decrement restored): the
  structural pin fails "freeing the highest LPID shrank next_lpid"
  (left 5, right 6); the seed pin fails on 221 with O-2 +
  `O-3(b): XX001 in class C at [Open]`. Both revert demonstrations
  run, then reverted clean.
- **Sub-class B, open's G3 probe swallowed a delivered recovery crash —
  root-caused as an instrumentation defect (sim env + store) and
  fixed.** `EnvRef::step` maps both a missing file ("open: … does not
  exist") and a crash delivered at the yield to
  `Err(SimFail::Crashed)`, and the G3 check read `open`'s error as
  "segment missing" — so a nested recovery crash landing on that yield
  became `Corrupt("start segment … missing (G3)")`, the nested-crash
  retry arm never fired (`nested` recorded 0), and the round failed
  XX001-at-Open with no fault (seed 1769's family: the faultless twin
  failed `O-1(a)` through the recovery Err, the MW round failed O-3(c)
  beside faults the crashed twin never covered — the file's directory
  listing even shows the segment present while `open` failed). The
  replay walk's and `truncate_wal`'s segment opens had the same
  swallow. The fix: `E4Env::exists` (a yield point; `Ok(false)` for a
  missing file, `Err` only for a world failure) + the three sites use
  it, so a crash propagates as `Fail::Sim` and only a genuinely
  missing segment takes the missing-segment path. Pins:
  `cxx_a_crash_at_any_recovery_yield_is_never_corruption` (a fixture
  with a checkpoint then 18 commits above T; one nested crash point
  swept over k=1..=120 — every round passes) and
  `cxx_g3_probe_never_swallows_a_recovery_crash` (1769: the twin
  passes, no O-3(c) text; the round's own remaining `O-1(c)`+`O-3(d)`
  is its MW-ORPHAN shape, deliberately not judged). Mutant M-CXXb (the
  G3 probe reading `open`'s error again): the sweep fails at k=8 with
  exactly `O-3(b): XX001 in class C at [Open]`. Demonstrated, reverted
  clean.
- **Movement measured** (allowed runs, output outside the worktree):
  seeds 0..200 — only 3 digests change (30, 66, 198; 30 and 198 pass
  before and after, their allocation numbering shifted), seed 66's
  round clears, failing rounds 22 → 21, `O2_FAIL` 1 → 0. Single-seed:
  1769's twin passes and its O-3(c) is gone (the round keeps its
  MW-ORPHAN `O-1(c)`+`O-3(d)`); 2149/4309/7469's primary O-3(c) rounds
  clear (7469's two failing twins are F2-file failures, unchanged);
  987/1108 (MP-LIVELOCK) and 688/4858/5238 (MP-E shape C) untouched,
  as their classes require.
- **Sub-class C, the remaining O-3(c) "unread, unreferenced" family —
  ESCALATED to P (the oracle is frozen; no worker widens the twin
  rule).** 7367/7748/8837/9508/1167/4167 (MP) and 1889/8399 (MW),
  digest-identical to F2's file at the fixed tip: **cross-round
  inherited damage**. Repro 7367 r1: round 0's LOST fault destroyed
  page slot 6; round 0 recovered `Ok` and passed as the detection
  round it is; the slot stays referenced and can never be rewritten
  (unreadable ⇒ never dirty), so round 1's amcheck (i) correctly
  reports slot 6 again (the plan's clause 3: detected, never served,
  `served=false`), but the frozen O-3(c) twin rule attributes XX001s
  only to the **same round's** fault targets (round 1's faults target
  other slots, which the twin shows unread+unreferenced), so the
  genuine detection is called a false positive and the round fails.
  The store's behaviour is correct per clause 3; the question is
  whether an MP/MW round inheriting prior-round damage passes O-3(c)
  when its own faults are uncovered — P's to rule (X5-R30's gate is
  the natural home). **Evidence**:
  `$HOME/.cache/nv3-review/runlogs/X-5c-F3.cxx3.7367.{err,json}`
  (E4_MPE_DIAG: round 0 `Lost inode 0 off 24576 (page slot 6)` →
  amcheck (i) `§3.3 fail on mapped page slot 6`; round 1 `Misdirected
  29→87, 44→73, Torn 90` → amcheck (i) slots {6, 29, 44, 73, 90}, slot
  6 not a round-1 target; `kept K {}`; the class C twin passes).

**Step 2 — C-DIV: evidence recorded, root cause ESCALATED to P (the
budget valve, X5-R19).** The repros still reproduce at the fixed tip
(21 r0, 46 r1, 36 r2, 10 r2 — digests unchanged; the class's
round-index skew stands: r0 88, r1 70, r2 266 in F2's file). The
deepest shape (seed 21 r0, class C, no fault) is an **acked
`synchronous_commit=on` commit lost** — `I-REDO-COMPLETE` violated:

- The model at verdict time: commits 1–13 written+acked, **ts=14
  written=false acked=false**, **ts=15 written=true acked=true
  (SyncOn)**, ts=16 written=true acked=false (SyncOff). `lo` = 15.
- The recovered store's raw entries hold none of ts=14's keys (never
  written — expected) **and none of ts=15's keys** (acked — the loss).
  O-1 therefore fails at every candidate c ≥ 15.
- The durable WAL (walked from lsn 0, the same `dec_record_with_lsn_check`
  the store uses): a 300–1,500-op chunked txn (txn (1,3)) spanning
  segments 0–3 (~200 KB of `Chunk` records), its `CommitMarker` at ts=5,
  Commits ts=6–13 — **then the walk stops at lsn 225622** (ts=14's
  position) on zero bytes; ts=15/16's records sit beyond the stop, never
  replay, and G2's truncation then zeroes from the valid end — recovery
  itself destroys them.
- The open question for P: how ts=15's record can be derived
  written+acked (its write returned, a sync returned after it) while
  ts=14 — the **lower** LSN in the same segment — resolved to pre-write
  zeros at the crash. Either the harness's write path lets a later
  record's write+sync complete past an earlier unreturned write (a
  prototype deviation in the writer's ordering), or
  `derive_written_acked`'s sync-extent accounting marks a record acked
  whose bytes were not durable (an instrumentation defect), or §12.5's
  stop-at-first-bad-record rule legitimately loses a durable later
  record when an earlier record's write was cut by the crash — a
  §12.5/§14/I-REDO-COMPLETE question only P can rule on. **Evidence**:
  `$HOME/.cache/nv3-review/runlogs/X-5c-F3.cdiv.21{c..n}.{err,json}`
  (the per-key NOT-IN-RAW list with each key's commit ts/flags, the
  bounds print, the full WAL record listing with the stop position and
  its zero bytes). The other C-DIV kinds ((c) 95, (d) 58, (a) 4) were
  not re-derived deeper in this attempt; the card's lead (cross-round
  carry: the harness checkpoint, the carried model, X5-R10's sequencer
  restart, G2 truncation) stands, and the r2 skew (19.6%) is consistent
  with it.

**Step 3 — MP-LIVELOCK: G8 applied (commit 89787d24).** The root cause
is exactly X5-R16's lead, confirmed on 987/1108/1247 (round 1 each,
every one directly after an R6 Ok-with-detected-loss round): the store
poisoned only on `gc_w > T` at the meta write, so an XX001 raised by a
gated operation's own read on an inherited unreadable page left the
store usable, the failing task unwound as a client detection, and the
rest parked forever behind the service spins (the budget tripped at
`h:writer`/`h:evictor`/`h:ckpt`). G8 (P's reading of I-FAIL-STOP §9.2 —
the condition holds): `fetch_page` marks the store poisoned when a read
whose bytes fail §3.3 happens inside a gated operation
(`inside_op=true`: apply, splits, packs, GC; a **read** op never
poisons — the XX001 is the client's detection, the plan's clause 3);
every later mutation refuses with `Fail::Poisoned` through the existing
`alive()` gate; and the harness treats `Fail::Poisoned` as the end of
the round's workload — the round's crash is delivered at once, the
crash desc of record is `poison` (never a NEVER-FIRED: `crash_fired`
follows the world's `crashed` flag), and the faults, recovery and
oracle run as usual. Pin
`g8_poison_ends_the_workload_instead_of_livelocking` (987/1108: no
deadlock/livelock, `crash == "poison"`, fired, the round passes as the
detection round it is). Mutants: **M-G8a** ("XX001 in apply does not
poison" — the poison mark removed) → 987 r1 livelocks again at
`h:ckpt`, FAILED; **M-G8b** (the harness `Poisoned` arm removed) → the
crash desc stays `After6@0`, FAILED. Both demonstrated, reverted clean.
Movement: 987/1108/1247 all now pass every round (`FAILING_SEEDS=0`
each; r1's crash desc `poison`). The class's 41 rounds are this
mechanism; the Acceptance 10k measures the total. G8 is listed for
RESULT item 6 and S-STORE §9.2 by spec card S-1-F3 (P's).

**Step 4 — MP-A: all 7 rounds ESCALATED to P with the evidence (X5-R16
+ X5-R30; no judgement here — a served corruption is never classified
as a non-failure by a worker).** All 7 reproduce at the fixed tip,
digest-identical to F2's file, all round 0, all `mrb=true`, all
recovery-`Err`, all `served=true`, `O-3(a): served corruption`
unchanged (X5-R30 (b)). Per-round table (the `served_k` and K halves
from F2's instrumented runs; the `V_k` CRCs and the fault lists from
this attempt's `E4_MPE_DIAG` runs at
`$HOME/.cache/nv3-review/runlogs/X-5c-F3.mpa.<seed>.*`):

| Seed | served slot | in K? | `served_k` | `V_k(s)` crc32c | the round's media faults |
|---|---|---|---|---|---|
| 1897 | 4 | K={4} | true | `9020a7cc` | MISDIR 10→8192, LOST 21, TORN 25 |
| 2437 | 4 | K={4} | true | `9020a7cc` | LOST 2, TORN 10, MISDIR 12→57344 |
| 4718 | 8 | K={8}, R={12,13} | true | `8ad5dcf1` | MISDIR 1→159744 |
| 2927 | meta 1 (4096) | K={}, R={4} | false | — (a meta slot: K excludes 1/2) | TORN 117, LOST 127, MISDIR 2→12288 |
| 4378 | meta 1 (4096) | K={}, R={4} | false | — | MISDIR 1→57344, MISDIR 2→110592 |
| 7528 | meta 1 (4096) | K={}, R={4} | false | — | MISDIR 67→65536, MISDIR 6→8192, LOST 101 |
| 8728 | meta 2 (8192) | K={}, R={11,13} | false | — | TORN 52, MISDIR 32→4096 |

The read paths (F2's instrumented runs): the three `sk=true` rounds
served a page through the map-reference fetch of the rolled-back
image's stale reference (the bytes equal `V_k(s)` — §3.3 passes, which
is why a correct store accepts them; the X5-R30 proxy-flaw candidates);
the four `sk=false` rounds served **open's own meta read** of a tainted
meta slot (the fault's source range is the meta slot; the accept hook
records the serve — the second shape for P at X5-R30 (c)). `served_k`
is vacuously true on nothing-served rounds; read it only on
`served=true` rounds (all 7 here).

**Step 5 — MW-ORPHAN: evidence only, never fixed (X5-R16's SPEC
classification stands; P opens S-1-F3 on confirmation).** Five rounds'
evidence at the fixed tip (record fields + the MPE diag's fault list;
artifacts at `$HOME/.cache/nv3-review/runlogs/X-5c-F3.orph.<seed>.*`):

| Seed/round | opened `T` (the opened meta's `T`, equal to the frontier in every row) | `hi` | frontier | orphan max ts (the O-1(c) text) | the damaged WAL record | later round reissues its ts? |
|---|---|---|---|---|---|---|
| 39 r0 | 5 | 8 | 5 | 7 | MISDIRECTED `/wal` inode 3 off 25724 len 22446 → 26112 | no later round (r0-only seed) |
| 59 r0 | 4 | 8 | 4 | 6 | MISDIRECTED inode 4 off 58211 len 49 → 53760; TORN same inode | no later round |
| 109 r0 | 4 | 14 | 4 | 5 | MISDIRECTED inode 4 off 25933 len 848 → 11776 | no later round |
| 149 r0 | 4 | 6 | 4 | 5 | MISDIRECTED inode 3 off 55140 len 10396 → 8704 | no later round |
| 209 r0 | 8 | 22 | 8 | 11 | MISDIRECTED inode 2 off 26151 len 1211 → 0; 47190 len 6463 → 37376 | no later round |

In every round: the orphan's max ts is above the recovered frontier and
≤ `hi` (P's own finding, on all 422 of the 437 in F2's file); the
damaged record is a `/wal/` write (class MW destroys a record after its
sync, by construction); `ackl=0` (no acked commit lost — these are not
X5-R1 costs); amcheck is clean on both images (the faultless twin
passes; no §3.3 finding — this is a prefix-state question, not a
detection question); and the replay's stop is §12.5's first-invalid-
record rule at the misdirected record's bytes (the same walk printed
above for C-DIV's seed 21 shows the mechanism: the walk stops at the
first undecodable record, and every entry of the commits whose records
sit beyond the stop stays in the image as an orphan above the cut).
X5-R10's sequencer restarts at frontier+1, inside the orphans' ts
range (the I-ENTRY-UNIQUE hazard P recorded; the r0-only repro seeds
cannot show a reissue — a multi-round confirmation needs P's triage of
the Acceptance file). **No fix was made and none may be (X5-R16:
"Any fix … is a mechanism beyond G1–G7").**

**Step 6 — the twins: classified, no separate fix.** The 103 failing
twin rounds (44 MP over 43 seeds + 59 MW over 48 seeds) are not a class
of their own: the twins are faultless rounds judged as class C, and
their `o1` texts echo the primary classes — (b) "scan mismatch" 51
rounds (C-DIV's shape; 907's twin among them), (a) the recovery-`Err`
default `published {} != model {…}` 31 rounds (the C-XX recovery-Err
family — 1769's twin was exactly sub-class B and now passes), (c)
"raw entry above the cut" 10 (MW-ORPHAN's echo in the faultless twin:
the twin inherits the *image* state, not the WAL damage — the twin's
own O-1(c) rounds are the inherited-orphan shape), (d) 7 (C-DIV), and
4 with no `o1` text (O-1f/O-2 failures). The classes' fixes therefore
move the twins with them (the Acceptance 10k measures the rest);
nothing here is fixed separately.

**Step 7 — the other MW and MP rounds, and MP-E shape C.**

- **The 4 MP O-1 rounds** (6028 r0, 7388 r1, 9548 r1 — O-1(b) scan
  mismatch; 8788 r1 — O-1(c)): recovery `Ok`, no O-3 text, no XX001 —
  the C-DIV shape inside class MP (the same O-1 failure family, with
  MP's detected-loss arm not applicable because the reads returned).
  Recorded with C-DIV's escalation.
- **The 10 MW O-3(d)+O-1(a) rounds** (published-set divergence with a
  silent Ok): the MW-ORPHAN family under a different O-1 kind — the
  store's published set at every legal cut differs from the model's
  after the same §12.5 replay stop (a published tree's records sit
  beyond the damaged record). Recorded with MW-ORPHAN's evidence;
  P's S-1-F3 ruling covers both O-1 kinds.
- **3399 r0** (`O-1(a)` only, `published {2} != model {2, 3}`): the
  same published-set shape; **2019 r2 / 4049 r1** (O-1f only: the
  frontier is not among the satisfying cuts): the frontier/orphan
  interaction of §14's apply-frontier rule — with MW-ORPHAN for P.
- **MP-E shape C (handed on by X5-R29 (c)): no amcheck change, no
  fix.** F2's evidence stands (4858's faultless twin holds the same two
  orphan map entries with no §3.3 failure anywhere — true orphans, an
  I-LPID-EXACT store deviation; 9717's "separators out of order" +
  "reachable more than once" is a third mechanism). The card's fork was
  strict: the precision fix may be made **only** if F3 shows 4858 and
  9717 to be something other than real image defects — they are real
  (nothing in this attempt's evidence moves them: 688/4858/5238 still
  fail O-3(e) (ii) at the fixed tip, digests unchanged) — and P rules
  first. ESCALATED with F2's per-lpid table as the record.

**Step 8 — the hand-off for X-5c-F4 (the retune and the final file).**

- **What F4's retune must not disturb:** the five fixed classes' pins
  and their repros — `cxx_highest_lpid_free_never_shrinks_next_lpid`
  (structural), `cxx_highest_lpid_free_never_shrinks_next_lpid_rounds`
  (221/66/907), `cxx_a_crash_at_any_recovery_yield_is_never_corruption`
  (the sweep fixture), `cxx_g3_probe_never_swallows_a_recovery_crash`
  (1769) and `g8_poison_ends_the_workload_instead_of_livelocking`
  (987/1108, with the `poison` crash desc now a *legitimate* crash
  point any coverage accounting must expect); the frozen surfaces (the
  oracle as amended by X5-R28, `src/model.rs`, `taint_ranges` and the
  fault/crash model, D17's two R6 cells, X5-R14's fix, X5-R28's K
  computation); G8's semantics (a poisoned store ends the workload at
  the `poison` crash point — the retune's targeted draws must tolerate
  a round whose crash fires before its drawn hook; the fired-guard
  already does, `crash_fired` follows the world's `crashed` flag); and
  the never-touch rules (no harness walk may report ground truth as an
  amcheck finding — X5-R23/X5-R28; MW-ORPHAN is never fixed; a served
  corruption is never classified as a non-failure — X5-R30).
- **The classes still open (F4 inherits them as P's queue, not as
  work):** C-DIV (an acked SyncOn commit lost through §12.5's replay
  stop — the seed-21 evidence above, an instrumentation-or-spec
  question); the O-3(c) cross-round inherited-damage family; MW-ORPHAN
  (SPEC — S-1-F3 is P's); MP-A (7 rounds, P rules at X5-R30 (c)); MP-E
  shape C (real orphan map entries + 9717's third mechanism). F4 runs
  no class work: its steps are the retune (X5-R17: SIZING, Q4, Q7,
  Q11, Q12; the pool stays at §5.4's formula; ≤ 40 min wall at 16
  threads), the final 10k with plain `verify`, and its README record.
- **This card's Acceptance 10k** (the runner's, X5-R31) becomes F4's
  step-0 triage file; its class counts re-derive with this README's
  field definitions, and the deltas against F2's file measure the
  three fixes' full-class movement (C-XX A and B clear the 221/66/907
  family; G8 clears the 41 MP-LIVELOCK rounds as `poison`-crash
  detection rounds; C-DIV, MW-ORPHAN, MP-A, shape C and the twins'
  remainders stand as escalated).

## Stage X-5c-F3-F1's record (this card)

**Step 0 — F3's Acceptance 10k committed as the triage file of record
(7b709ccb), and this card's class table re-derived from it.** The three
provenance checks passed before the copy (the prov file's
`commit=fe798c0a… status_lines=0 exit=0 verify_exit=0`; the JSON's
sha256 equal to its `json_sha256` `efb7835e…73906c5`; `git diff --quiet
fe798c0a HEAD` over `src/`, `tests/`, `examples/`, `Cargo.toml` and
`v3/Cargo.lock` exiting 0 — the merged tip 038bb9cc is F3's verdict
commit on top of fe798c0a, which touches only `nucleus/v3/verdicts/`).

Final lines, verbatim:

```
E4 SEEDS=10000 O1_FAIL=2654 O2_FAIL=43 O3_FAIL=559 DEADLOCK=0 PANIC=0 O1F_FAIL=2665 MONITOR=0
E4 FOLD FAILING_SEEDS=955 FAILING_ROUNDS=914 TWIN_FAILING_ROUNDS=71 DEFECT_SEEDS=0
```

G8's effect shows in `DEADLOCK=0` (F2's file: 41; MP-LIVELOCK is gone).
Rounds: C 11,822 / MP 2,183 (+ 2,183 twins) / MW 1,416 (+ 1,416 twins).

**The class table, re-derived from the committed file (the reporting
rule: `served=true` rounds are split out first — the oracle's `o3` text
is last-writer-wins, so 6149 r1's serve reads as O-3(d), and an
O-3(a) text can hide an O-3(e) failure).** Field definitions: a
**failing** primary round has `pass=false`; a **failing twin** round has
`pass=false` in the twin record. All counts are over primary rounds
unless "twin" is in the name.

| Class | Rounds | Record-field definition (re-derived, verifiable from the JSON) | Repros (round) |
|---|---|---|---|
| **Served (split out first)** | 7 MP + 1 MW | MP/MW failing round with `served=true`: 1897/2437/2927/4378/4718/7528/8728 (all r0, all `mrb=true`, all `O-3(a): served corruption` — MP-A, judged under X5-R30 (c) from step 6's fields) and 6149 r1 (MW, `O-3(d)` text — the serve is step 2's target: the SegmentEnd valid-end fix) | 1897, 2437; 6149 r1 |
| **C-XX** (class C) | 64 | class C failing round with `o2 != null` ("class C amcheck(i) not clean") or `o3` = `O-3(b): XX001 in class C at [site]` — sites: `Open` 21, `AmcheckIi` 43 (no `o2`-only round in this file) | 943 (r0, `Open`), 906 (r0, `AmcheckIi`) |
| **C-DIV** | 388 | class C failing round carrying an `o1` text, **excluding** rounds whose `o1` is an O-1(a) inside a C-XX round (the recovery-`Err` default). **93 carry `frontier < lo`** (88 (b) + 5 (a)) — the WAL hole, this card's step 1; the other 295: (b) 144, (c) 93, (d) 58. By round index: r0 58, r1 64, **r2 266 of 1,373** (19.4%) | 21 (r0, b, `frontier < lo`), 46 (r1, b), 36 (r2, d), 10 (r2, c) |
| **MP O-3(c)** (X5-R32's) | 17 | MP failing round with `o3` = `O-3(c): XX001 with every media fault on unread, unreferenced targets`: 7, 1167, 1628, 2047, 2307, 3307, 3887, 4348, 4707, 6047, 6767 (r2), 7367, 7748, 8837, 9168, 9508, 9928 — exactly X5-R32 (d) 1's named list (all r1 except 6767 r2) | 7 r1, 1167 r1 |
| **MW O-3(c)** (the same family, X5-R32 (d) 2) | 5 | MW failing round with the O-3(c) text: 1889 r2, 3509 r0, 4699 r0, 5229 r1, 8399 r1 — step 7 takes them with C-XX | 3509 r0, 8399 r1 |
| **MP-E shape C** | 5 | MP failing round with `o3` starting `O-3(e): (ii)`: 688, 3718, 4858, 5238, 5638 (all r0). **9717 passes in this file** (round and twin) — moved by F3's step-1 fix; the card's Goal expected it here | 688, 4858 |
| **MW-ORPHAN** | 451 | MW failing round with `o1` = `O-1(c) … raw entry above the cut` and `o3` = `O-3(d): silent Ok with O-1 failing`: 450 non-served + 6149 r1 (served, split out above) — P's count. S-1-F3's; never fixed here | 39 (r0), 59 (r0) |
| **Other MW** | 13 | the MW failing rounds outside the classes above: 10 O-3(d) with O-1(a) (2769, 3099, 3159, 4239, 5299, 6179, 7219, 8389, 9309, 9699), 1 O-1(a) only (3399 r0), 2 O-1f only (2019 r2, 4049 r1) — the MW-ORPHAN published-set family | 3399, 2019 |
| **Other MP** | 1 | 6028 r0: `O-1(b) at c=13, tree 2: scan mismatch`, recovery `Ok`, no O-3 text — C-DIV's shape inside class MP | 6028 |
| **The twins** | 71 | failing twin rounds: 26 MP + 45 MW; `o1` texts: MP (b) 22, (a) 3, (c) 1; MW (a) 17, (b) 16, (c) 6, (d) 6 — the classes echo there (the twin is judged as class C) | 339/849 family |

Partition check: C 415 = 64 C-XX ∪ 388 C-DIV (overlap 37 rounds); MP 30
= 7 served + 17 O-3(c) + 5 shape C + 1 other; MW 469 = 450 ORPHAN
(non-served) + 5 O-3(c) + 13 other + 1 served (6149 r1). Total failing
rounds 914 = `FAILING_ROUNDS`; failing seeds 955 = `FAILING_SEEDS`;
`TWIN_FAILING_ROUNDS=71`; 0 livelock/deadlock rounds, 0 defect seeds.

Deviations from the card's Goal table, measured: the C-DIV r2
denominator is **1,373** class C r2 rounds in this file (the Goal says
"266 of 1,357" — 1,357 was F2's file's denominator; the numerator 266
matches exactly), and **9717 passes** here (the Goal's MP-E shape C
list expected it; its findings were "separators out of order" and
"lpid 36 reachable more than once", the family F3's step-1 `next_lpid`
fix plausibly cleared — recorded, not re-derived deeper).

**Step 1 — the WAL's serialized writer (§12.4 / I-REDO-COMPLETE;
commit e6ebbdda).** The gate's S-1-F3 text for §12.4: "a commit is
acked durable only through the contiguous written-and-synced prefix of
the WAL". The draft let `wal_writer_step` run in two tasks at once (the
writer task and a committer helping from `wal_wait_durable`/
`wal_wait_written`), each taking its own batch and advancing
`written`/`durable` with `max`, so a later batch's write+sync acked its
SyncOn commit over an unwritten (or unsynced) earlier batch — seed 21
r0's ts=15 over ts=14's hole; §12.5's replay stop then lost the acked
record and G2 zeroed it.

- **The shape chosen: (b), the gate's preferred — serialize the
  writer** (src/store/wal.rs). `WalState.in_flight: Option<(u64,u64)>`
  holds the taken batch's LSN range; `wal_writer_step` takes no batch
  while one is in flight and parks the caller on `WalAcks` (the
  existing "batch taken but not yet written" rule); the holder clears
  the flag and wakes `WalAcks` on **every** exit (its error paths
  included — a `wal_writer:release` step after the body, whose failure
  is swallowed because a failed world unwinds every waiter anyway);
  `written` advances to the batch end only when the batch starts at the
  current `written` (`wal_writer:written`, a loud `Fail::Invalid` on any
  other case), and `durable` advances only within `[start, end]`
  (`wal_writer:durable` — a waiter's `wal_sync_through` may already
  have covered the written bytes while the holder sat at
  `writer:presync`, D4's own shape, so `durable` may legitimately sit
  inside the batch). No existing test was changed to fit the shape: D4
  still passes at the fixed code (step 1 must still make the parked
  batch durable, and it does — through `wal_sync_through`).
- **The pin** `cdiv_syncon_ack_never_crosses_an_unwritten_batch`
  (tests/store.rs, a literal two-task config): segment 0 holds four
  durable setup commits ending exactly at LSN 65536; task A enqueues
  ts=5..8 filling segment 1 exactly, takes batch 1, writes it and is
  held at `writer:presync` (budget 900_000, far under the 1 M livelock
  budget) — written, unsynced; task B waits for exactly that hole
  state, enqueues ts=9 (segment 2, a fresh segment whose sync can never
  cover batch 1's bytes) and runs its §12.4 SyncOn ack wait; the driver
  crashes the world the moment B parks on `WalAcks` (the rule in force)
  or B's ack returns (the mutant). Asserts: ts=9's `wal_wait_durable`
  has not returned; `wal.durable` never passed batch 1's start (65536);
  and after F-CRASH resolves (`keep_latest_pm: 0, tear_pm: 0`) recovery
  returns the ts-prefix through the setup commits (`frontier == 4`, the
  WAL restarted at the valid end 65536). **Mutant M-WALa** (the
  concurrent take with `max` accounting restored): the pin fails on the
  first assertion — "ts=9's wal_wait_durable returned before the crash"
  — applied, shown failing, reverted clean (`git diff` free of the
  mutant).
- **Movement (f).** Seed 21 r0 **passes** at the tip (`frontier=14 ≥
  lo=13`; the `frontier < lo` signature is gone). The fix parks every
  helper that previously took its own batch, so each workload world's
  yield stream changes and every uniform crash point moves — the F2/F3
  seed pins whose literal configs moved were re-derived at the tip with
  their assertions byte-identical, and each pin's revert was
  re-demonstrated on the new configs:
  `g8_poison_ends_the_workload_instead_of_livelocking` 1108 → **7**
  (1108's r0 flipped to a recovery-Err detection round, its chain stops
  before r1; 7 r1 is the same MP-LIVELOCK shape after an R6 r0 — M-G8a
  re-demonstrated: 987 r1 livelocks at `h:ckpt` and 7 r1 at `h:ckpt`
  with the poison mark removed);
  `r20_mpe_r_reads_the_issued_write_history` 38/137 → **48/308** (both
  lost their rollback round; 48 r0 and 308 r0 carry the shape-B
  rollback with `rlen ≥ 1`, all rounds passing — the events-half revert
  re-demonstrated: the pin fails on 48 with the exactness text; 308's R
  has a dwrites-half source and does not catch that revert alone);
  `r28_residual_seeds_clear_via_k` 6157 → **5808** (6157's round moved
  to a no-rollback shape and its twin now fails O-1(c) — the inherited
  C-DIV echo; 5808 r0 carries the residual K shape, K slot 53 — M-R28g
  re-demonstrated: 1787 fails "tainted referenced slot 14 not reported"
  and 5808 "…slot 53 not reported" with the harness K disabled).
  **0..200 at the tip: 18 failing rounds** (F3's file had 22 in 0..200);
  **C-DIV 10 rounds, of which `frontier < lo`: 2** (20 r2, 115 r1 — both
  O-1(b) with `frontier == lo − 1`) — not 0, so the remainder goes to
  step 8. DEADLOCK 0, DEFECT_SEEDS 0, no livelock.
- **The cross-check (e) — `E4_ACK_DIAG`, ESCALATED to P.**
  `HarnessState.client_acked` (a diagnostic, never serialized, never a
  digest input, never an oracle input) records every commit ts whose
  own §12.4 ack wait returned before the crash (`h:cliack1`/`h:cliack2`
  in `commit_txn`, both the chunked marker's §12.7 wait and the plain
  ack). Over `run --seeds 0..200` at the tip, comparing every commit's
  derived `acked` flag against it over the **fired, judged** rounds (a
  discarded NEVER-FIRED retry attempt never reaches the judge, so its
  comparison is noise; the diag gates on `obs.crash_fired`):
  **4,656 commits, 4,571 agreeing, 55 disagreeing commits across 31
  rounds** — not "0 disagreements", so per the card the derivation is
  escalated, unchanged. Two shapes, each with the first three recorded
  (seed, round, class; ts, lsn, len, sync_on; the events over the
  record's segment):
  1. **derived-only (9 rounds; e.g. seed 9 r0 Mw ts=5 lsn=182270
     len=49 SyncOff; seed 20 r2 C ts=64 lsn=661312 len=3573 SyncOn;
     seed 115 r1 C ts=25 lsn=411423 len=2937 SyncOn):** the event
     history says the write (and covering sync) returned — the
     commit **is** durable — while the committer's own wait never
     returned (it crashed in or before the wait, `commit:preapply`
     included, while another task completed the batch). The derivation
     measures durability; the client measures observation; a crash
     between the two is inherent asynchrony, not obviously a defect —
     but the two remaining `frontier < lo` rounds (20 r2, 115 r1) are
     exactly derived-only rounds whose derived-acked SyncOn commit sits
     beyond a §12.5 replay stop (step 8 takes them).
  2. **client-only (23 rounds, always the round's first commits at
     lsn 0..~1 KiB, ts 1..4; e.g. seed 0 r0 C ts=1 lsn=0 len=206
     SyncOn):** the events show the record written and synced
     (`W+ f2@0+206 W= f2@0+206 S+ f2 S= f2`), the client's wait
     returned, yet the walk never yields the record — a **same-round
     segment recycle** later reissued the same file offsets (the
     recycled segment's next life starts at file offset 0), and the
     derivation's per-offset latest-write map (`files.insert(off, …)`)
     keeps only the recycled life's bytes, so the original record's
     header is masked. The record is durable and graduated into the
     image (a recycle frees only segments wholly below `wal_start`),
     so O-1 sees it through the image and the under-marked `acked`
     flag can only lower `lo` (never raise `hi` — the newest record
     cannot be below the recycle line); 21 of the 23 rounds pass.
     Still a real derivation-vs-observation divergence, which is P's
     to rule on: **ESCALATE (the derivation, unchanged).**

**Step 2 — SegmentEnd advances the valid end (§12.5 as written; commit
001d3b7f).** `ckpt::recover`'s walk now sets `valid_end = seg + WAL_SEG`
when it accepts a `SegmentEnd` ("after `SegmentEnd`, the reader moves on
to the next segment's first LSN … The stopping point is the valid end"),
so a missing next segment leaves the valid end **after** the SegmentEnd
(`truncate_wal` then starts at the missing segment and zeroes nothing in
this one) and an invalid first record in the next segment leaves it at
that segment's first LSN. The draft never advanced `valid_end` over the
filler, so G2's truncation zeroed the very SegmentEnd the reader had
just accepted — seed 6149 r1's nested crash dropped the zeros before
their sync and life 2 re-accepted the surviving SegmentEnd, tainted
against the dropped zeros (an MW served corruption). The pending spec
home is S-1-F3 (P's card; this implements §12.5's text as written).

- **The pin** `segend_valid_end_survives_a_missing_next_segment`
  (tests/store.rs; a literal config: segment 0 holds one durable Commit
  + one durable SegmentEnd at [45, 90), segment 1 does not exist; a
  nested recovery crash swept over k=1..=120, each with F-CRASH
  resolved at keep-latest 0 and a life-2 retry, exactly the harness's
  nested arm): asserts no served range in any life, the SegmentEnd's
  bytes durable and byte-equal, and `wal.append` = segment 1's first
  LSN. Under the fix `truncate_wal` issues no write at all (its first
  probe is the missing segment). **Mutant M-SEGa** (the `valid_end`
  advance removed): the pin fails at k=1 with the SegmentEnd's bytes
  zeroed (and at every k: the zeroing truncation either lands durably
  or is cut between its write and its sync and life 2 then serves the
  tainted SegmentEnd — 6149's exact mechanism) — applied, shown
  failing, reverted clean.
- **Movement:** seed 6149 r1's served corruption is **gone**
  (`served=false`); its new outcome is **MW-ORPHAN's exact shape**
  (`O-1(c) at c=2, tree 2: raw entry above the cut (max ts 4)` +
  `O-3(d)`), which is S-1-F3's and not judged here, exactly as the card
  predicted.

**Step 3 — the gate's uncaught mutants: G-2 pinned; G-1 and G-1b
demonstrated equivalent (G-1 ESCALATED).**

- **G-2 (the gate's exact patch, `if !inside_op {` at
  `fetch_page:poison`) — pinned.** The pin
  `g8_read_never_poisons_gated_op_does` (tests/store.rs; a literal
  config: two single-leaf trees checkpointed, tree 2's leaf slot
  corrupted on the durable disk so every read of it fails §3.3 against
  the image's reference CRC): (i) a client read (`tree_scan`,
  `inside_op=false`) gets XX001, the store stays usable and a mutation
  of the untouched tree succeeds; (ii) a gated op (`tree_insert` into
  tree 2: apply, `inside_op=true`) gets XX001 **and poisons** — the
  next mutation, of the untouched tree, returns `Fail::Poisoned`.
  G-2 fails (i) ("a client read must never poison the store" — the scan
  poisons); **M-G8a** (the poison mark removed) fails (ii) and the
  existing `g8_poison_ends_the_workload_instead_of_livelocking`
  (987 r1 livelocks at `h:ckpt`) — both applied, shown failing,
  reverted clean.
- **G-1 (the gate's exact patch, `Err(_) => break 'outer` at the replay
  walk's segment probe) — an equivalent mutant; ESCALATED to P.** The
  card's pin `replay_walk_probe_never_swallows_a_recovery_crash` exists
  (tests/store.rs; a literal two-segment config: segment 0 filled
  ts=1..4 and ended by a `SegmentEnd` filler, segment 1 holding the
  durable, acked ts=5; a nested crash swept over k=1..=120): every fired
  point must propagate as `Err(Sim(Crashed))` and the retry (F-CRASH at
  keep-latest 0, a fresh recovery) must recover the acked commit
  (`frontier == 5`); an unfired point finishes clean with the same
  frontier. **The pin passes on the mutant too** — demonstrated with
  the exact patch applied: the full suite (all 9 test binaries,
  including this pin's 120-point sweep) is green, and targeted runs
  (seeds 2 — two nested-crash rounds — 7 and 21) are byte-identical to
  the fixed code. Why it is equivalent: after the swallowed `break`,
  the very next env call — pass 2's page fetch for any replayed op
  above `T`, or `truncate_wal`'s first `exists` probe when no record
  applies (it always runs, and its first probe is an env yield) —
  re-raises `Err(SimFail::Crashed)` with the same error type, so the
  nested-crash retry arm fires identically; and the disk cannot differ,
  because every env write after the crash fails before executing. A
  pin that catches G-1 needs a `recover` that can make no env call
  after the walk's probe — impossible as the code stands. **The gate's
  demand for a catching pin is therefore ESCALATED with this
  demonstration** (the pin itself stays: it catches any swallow that
  yields `Ok` or a different error, and pins the propagation + recovery
  behavior).
- **G-1b (the same swallow in `truncate_wal`'s probe, `Err(_) => break`)
  — an equivalent mutant, per the card's own clause.** Demonstrated the
  same way (the exact patch applied: full suite green, seed 2 targeted
  run identical — that run also carried G-1's patch, both swallows at
  once, and still nothing moved). Why: the swallow skips the zeroing of
  later segments, but the world is crashed — the very next statement is
  `cx.env().sync_dir(WAL_DIR)`, whose yield re-raises the crash, and no
  env write between them can execute. Left as an equivalent mutant with
  this demonstration, exactly as the card's G-1b clause prescribes.
- **A §12.2/§12.5 silence found while building G-1's fixture
  (ESCALATED, not fixed):** §12.2's enqueuer explicitly allows a record
  that ends **exactly at the segment boundary** (`room − probe == 0`),
  which leaves no room for the §12.5 hop filler — and `ckpt::recover`'s
  walk then exits its inner loop at `off == WAL_SEG` and stops: every
  record in the next segment is unreachable, whatever its durability
  (an acked-commit-lost shape). The first form of this card's G-1
  fixture hit it (4 × REC_MAX = WAL_SEG exactly; the retry recovered
  `frontier == 4`, never ts=5 in segment 1). It is unreachable in
  practice through the campaign's workload draws (record sizes are
  value-sized; landing a byte-exact 64-KiB boundary sum is
  astronomically unlikely), but the writer/reader disagreement is real
  and is P's to rule on (S-1-F3's natural home). **No fix was made:
  a spec silence is never a local design choice.**

**Step 5 — X5-R32, transcribed literally (commit e34b75ef; the one
oracle commit, its subject naming X5-R32).**

- **(a) The harness supply:** `FaultTargets` gains `prior_page_slots`
  (+ `FaultTargets::with_prior`, called where `media_targets` is built —
  `disk.media[..media_base]`, the disk's own applied-fault list, mapped
  by `page_slots`' rule); `Extracted` gains `recovery_reads` and
  `page_file` — the **real** round's own recovery reads (the `Ok` arm
  and the final `Err` arm both set them before the verdict's
  `check_round`; never the twin's).
- **(b) The oracle amendment** (src/oracle.rs + tests/oracle.rs change
  only by the ruling's text): `H`, `D`, `D_ii`, `D_read` and the site
  list computed inside `check_round`; `Open`/`Replay` explained by
  `D_read`, `AmcheckI`/`Read23` by `D ≠ ∅`, `AmcheckIi` by `D_ii ≠ ∅`;
  in the twin rule's `Some(t)` branch, `any_fault ∧ all_uncovered`
  keeps its failure text except in class MP with `!no_r32` and a
  non-empty site list all of whose sites are explained — then O-3 (c)
  does not fail, and the verdict's new `r32` is `true` (a diagnostic;
  the record's `r32` field is written by `run` and parsed by `verify`;
  the pass fold never reads it). `OracleCfg` gains `no_r32`.
  `tests/oracle.rs` gains **mp8** (must-pass, `r32 = true`) and
  **mf24–mf28** (must-fail, each asserting the exact O-3 (c) text), all
  on one literal world (class MP round 1: the round's only fault on
  slot 50, unread and unreferenced by the twin; `prior_page_slots =
  {6}`; slot 6 referenced, tainted and reported by amcheck (i);
  `xx001 = [Open, AmcheckI]`; the real recovery's reads holding the
  slot-6 page read). `instrumentation_removals_are_load_bearing` gains
  the X5-R32 arm.
- **Demonstrations:** `no_r32` turns mp8 red (the instrumentation
  arm); **M-R32a** (`Open` explained by `D ≠ ∅`) turns mf24 green;
  **M-R32b** (the `H` filter dropped) mf25; **M-R32c** (the K exclusion
  dropped) mf26; **M-R32d** (the class test widened to MP ∨ MW) mf27;
  **M-R32e** (`AmcheckIi` explained by `D`) mf28 — each applied, its
  case shown turning green, reverted clean.
- **(d) 1 — the 17 named rounds at the tip** (single-seed runs; the
  first two columns are the card's decision rule's inputs):

  | Seed/round | r32 | O-3 text | pass | Reading |
  |---|---|---|---|---|
  | 1167 r1 | true | none | true | the ruled shape (control: 1167 r1 still shows O-3 (c) at e6ebbdda) |
  | 4348 r1 | true | none | true | the ruled shape |
  | 6047 r1 | true | none | true | the ruled shape |
  | 7367 r1 | true | none | true | the ruled shape |
  | 7 r1 | false | none | true | moved by e6ebbdda (F3-F1 step 1): passes with no O-3 (the twin-rule shape is gone) |
  | 3307 r1 | false | none | true | moved by e6ebbdda (F3-F1 step 1): passes with no O-3, as 7 |
  | 8837 r1 | false | none | true | moved by e6ebbdda (F3-F1 step 1): passes with no O-3, as 7 |
  | 7748 r1 | false | `O-3(e): (ii) reported slot 12 …` | false | moved by e6ebbdda (F3-F1 step 1): fails O-3 (e) (ii) — classed under that clause, never re-ruled |
  | 1628, 2047, 2307, 3887, 4707, 6767, 9168, 9508, 9928 | — | — | — | moved by e6ebbdda (F3-F1 step 1): each loses the named round, because the chain stops after an r0/r1 recovery-`Err` detection |

  **No round keeps the twin-rule shape with `r32 = false`: no
  ESCALATE.** P's prediction ("all 17 have `r32 = true` and pass")
  held wherever the shape survived; the card's own "a round that
  moved" clause covers the rest — this card's store fixes (steps 1–4)
  shifted the workloads wholesale (the uniform crash points are
  yield-counted).
- **(d) 2 — the 5 MW rounds:** unaffected by the arm (class MP only);
  their step-4 outcomes stand: 1889 r0, 4699 r1 and 8399 r0 fail
  MW-ORPHAN's exact text (their XX001 cleared with the store fixes —
  "if a C-XX fix clears their XX001, their O-3 (c) clears with it"),
  and 3509 and 5229 pass entirely.

**Step 6 — X5-R30 (c)'s record fields `skx`, `smx` and `pass_na`.**
`harness::r30_predicates` computes both predicates exactly as ruled
(the class and X5-R5 conditions 1+2; `skx`: every serve on the page
file wholly inside slots of `K`, no workload-pass serve, no R-taint, no
uncovered accept on any recovery or extraction disk — the recovery loop
now collects every life's `uncovered_accepts`; `smx`: every serve
exactly `(page file, s·4096, 4096)` with `s` the opened meta's slot
(`opened_meta_slot_of`), the slot's durable bytes equal to some
`sync_all`-covered DWrite at that offset); `pass_na` re-runs the final
verdict with `OracleCfg { no_taint: true, ..Default::default() }`
(fresh, identically seeded rngs), recorded only when
`served ∧ (skx ∨ smx)`, else null. All three ride the round record
(`run` writes them, `verify`'s parser reads them) — **diagnostics,
never oracle inputs**; the oracle is frozen apart from X5-R32.

- **Pins** (tests/harness.rs): `r30_skx_and_smx_record_fields` (the
  seed pins — skx for 1897 r0 and 4718 r0, smx for 2927 r0, 7528 r0
  and 8728 r0, each with `pass_na = true`; 6149 r1 not served, both
  false, `pass_na` null) and `r30_predicate_conditions_are_load_bearing`
  (a literal fixture: a page file whose meta slot 2 was written and
  `sync_all`ed — smx holds; the workload-serve and R-taint conditions,
  the sync_all-covered byte match (both a `sync_data`-only rewrite and
  a LOST revert), and the class condition each turn it off).
  **Mutants M-R30a–e** (the workload condition, the R-taint condition,
  the byte-match check, the class condition, and skx's K-containment —
  each dropped): each turns its pin red — all applied, shown, reverted
  clean.
- **The MP-A record at the tip** (single-seed runs, the judging P's
  from the final file):

  | Seed | served | skx | smx | pass_na | The gate's prediction |
  |---|---|---|---|---|---|
  | 1897 r0 | true | **true** | false | true | skx — held |
  | 2437 r0 | false | false | false | null | skx — **moved** (no longer served) |
  | 4718 r0 | true | **true** | false | true | skx — held |
  | 2927 r0 | true | false | **true** | true | smx's location — held, the byte check settled |
  | 4378 r0 | false | false | false | null | smx's location — **moved** (no longer served) |
  | 7528 r0 | true | false | **true** | true | smx's location — held, the byte check settled |
  | 8728 r0 | true | false | **true** | true | smx's location — held, the byte check settled |
  | 6149 r1 | false | false | false | null | "not served after step 2, or else skx = smx = false" — held exactly |

  Judging stays P's: a served round passes iff `(skx ∨ smx) ∧ pass_na`
  — every served round above would pass under the ruling. **Never
  adjusted a predicate to fit a round.**

**Steps 7–8 — the C-XX and C-DIV remainders (the budget: started
2026-11-02T18:34:03-07:00, root-causing stopped at 18:50 per X5-R19's
50-minute valve).** The 0..200 re-derivation at the tip (after steps
1–6): **17 failing rounds** (F3's file had 22 in 0..200), **C-XX 2**
(150 r1 `Open`, 151 r1 `AmcheckIi` + O-2), **C-DIV 10** ((b) 6, (c) 4,
**(d) 0** — was 58 in F3's file), 0 livelock, 0 defect seeds; seed 21 no
longer fails. **Every MP-E shape C seed passes at the tip** (4858, 688,
3718, 5238, 5638, 9717 — the orphan-map-entry family cleared with this
card's store fixes), and all 5 MW O-3(c) rounds moved or pass (step 4's
record).

- **Step 7, C-XX sub-shape 1 — root-caused as a prototype deviation
  from §6.8 (§9.1's identical-entry rule on the unversioned dir path)
  and fixed.** The method (P's): the store's own amcheck after each
  replayed record, each harness checkpoint and each recovery
  (`E4_CXX_DIAG`, stderr only, never a digest input) names the first
  violating operation: seed 151 r1's first violating record is the
  chunked apply of **K_CHUNK txn (1,4) ts=13 at lsn 221562**, and the
  directory keys at that instant are **`[1, 2, 3, 4, 4]`** — a
  duplicate directory entry. The cause is `tree::dir_put_locked`
  (src/store/tree.rs, the `hit` computation): it looked for the equal
  entry at `pos` — the first strictly-greater position — but an
  existing copy sits at `pos − 1`, exactly the B5 prototype deviation
  `insert_locked`'s own comment documents for the versioned path, left
  unfixed on the directory path. A directory put for an id the durable
  leaf already held — with the in-memory directory missing it (an
  interrupted drop's entry-removal step never landed; `resume_drops`
  walks after replay) — appended a duplicate whose two entries named
  **different roots**, and amcheck (ii) then reported "dir: leaf keys
  out of order", "tree: child lpid N unmapped", "separators out of
  order" and "lpid 38 reachable more than once" (the two subtrees). The
  fix: the equality check covers `pos − 1` (a replace there, §6.8's
  "a directory entry is a replace, never an append"). The pin
  `cxx_dir_reput_never_appends_a_duplicate` (a literal config: tree 2
  created, the in-memory entry removed — the interrupted drop's state —
  `dir_create_tree(2)` again; the leaf must hold exactly one entry and
  amcheck stays clean). **Mutant M-CXXc** (the `pos − 1` check
  removed): the pin fails with two entries — applied, shown, reverted
  clean. Post-fix, 151 r1's directory keys are `[1, 2, 3, 4]` and the
  duplicate-driven findings are gone.
- **Step 7, C-XX sub-shape 2 — ESCALATED to P with the evidence.** A
  second, distinct defect remains in 151 r1: the recovered tree holds a
  mapped inner page whose child reference names an **unmapped** lpid
  ("tree: child lpid 38 unmapped", a single finding, present
  post-recovery **after** `resume_drops`, and the opened image's (i)
  walk was clean — so the stale parent is **produced during the
  chunked replay itself**, not inherited), which the harness checkpoint
  then amplifies into the full structure cascade (separators, subtree
  bounds, reachable-more-than-once) in the graduated image. The
  alloc-reuse-while-mapped reading was tested and excluded (the
  diagnostic logs free-set reuses still mapped in `map.cur`: none
  occur). P's lead (i) first half (a `free_lpid` whose map removal
  reaches image k while a page of k still references the LPID) is the
  open candidate; the root cause sits in the replay/flip interaction
  and is **not** fixed within the budget. Evidence: the `E4_CXX_DIAG`
  artifacts at
  `$HOME/.cache/nv3-review/runlogs/X-5c-F3-F1.7.cxxd151*.err` and
  `X-5c-F3-F1.7.cxxfix151*.err` (the per-record replay findings, the directory keys,
  the post-recovery and post-checkpoint findings, before and after the
  dir fix). **150 r1 (XX001 at
  Open, two nested crashes) was not reached within the budget** —
  recorded, reproducible twice.
- **Step 8, C-DIV — the cut recorded, the class ESCALATED (the
  r2 skew stands).** The 10 tip repros (each confirmed in the 0..200
  run): (b) 20 r2, 112 r2, 115 r1, 120 r2, 151 r1, 152 r2; (c) 63 r2,
  104 r2, 125 r2, 186 r2; **(d) none at the tip** (was 58 in F3's
  file). Field-level cut: the four (b) rounds with `frontier == lo ==
  hi` (112, 120, 151, 152 — the scan mismatches at the only candidate
  cut, three with nested crashes, one without); the two `frontier < lo`
  rounds (**20 r2: lo=64, frontier=63; 115 r1: lo=25, frontier=24**) —
  both are **derived-only cross-check rounds** (step 1 (e)'s shape A:
  the derivation says a SyncOn commit's write and covering sync
  returned — it is durable by events — while the committer's own wait
  never returned; the round then fails O-1(b) at every candidate with
  the frontier one below `lo`), i.e. an acked-by-derivation commit
  sitting beyond a §12.5 replay stop — the same question class as
  MW-ORPHAN, and P's to rule on; and the four (c) rounds
  (`raw entry above the cut` with `frontier == hi` in all four — the
  orphan-above-the-cut shape, MW-ORPHAN's mechanism inside class C,
  three with nested crashes: entries above every legal cut after a
  §12.5 stop, 3 of 4 with `nested > 0`). **The per-key diff the card's
  cut prescribes was not built within the budget** (it needs a per-key
  model-vs-recovered-state diagnostic); the class is ESCALATED with
  this evidence under X5-R16/X5-R19, the leads standing (cross-round
  carry, the §12.5 stop, G2 truncation — the step-1 fix removed the
  WAL-hole explanation for every round that had it, and the two
  `frontier < lo` remainders are the derivation-vs-durability question
  the cross-check escalated). **If step 1 (e)'s escalation is ruled a
  derivation defect, it would explain the two `frontier < lo` rounds
  (20 r2, 115 r1) directly.**

**RESULT 5 — the Acceptance 10k:** not this card's to report; **see
X-5c-F4 step 0** (this stage's Acceptance file
`X-5c-F3-F1.acc.campaign.json` — `E4 SEEDS=10000 O1_FAIL=2617
O2_FAIL=54 O3_FAIL=542 DEADLOCK=0 PANIC=0 O1F_FAIL=2627 MONITOR=0` /
`E4 FOLD FAILING_SEEDS=901 FAILING_ROUNDS=860 TWIN_FAILING_ROUNDS=68
DEFECT_SEEDS=0`, sha256 `f3a1e605…a1af17`, prov `commit=62d4e739`
(code-identical to a96620bd: 62d4e739 adds 8 README lines; the same
sha256 `f3a1e605…a1af17`) — is
F4's triage input, per the card's Goal and Notes).

## Stage X-5c-F3-F3a's record (this card)

**Step 0 — the triage file, the rulings and the F3-F1-F5 record fixes.**

- **(a) The provenance checks passed before the copy:** the prov file
  shows `commit=62d4e7396f544c17d6d7c1f93875cd5b00275699`,
  `status_lines=0`, `exit=0`, `verify_exit=0`; the JSON's sha256 equals
  its `json_sha256` (`f3a1e6058fbccc5025b5be786280917cf1344aea301d9152842518025ea1af17`);
  `git diff --quiet 62d4e739 HEAD` over `src`, `tests`, `examples`,
  `Cargo.toml` and `v3/Cargo.lock` exits 0 (6db60b2b adds only
  `verdicts/X-5c-F3-F1.md`).
- **(b) The triage file:** committed at 929ade38
  (`results/campaign.json` only), the message carrying the two E4 lines,
  the quota table and the prov lines.
- **(c) The rulings commit — before any `e4-campaign` command ran:**
  **31effe00** ("P rulings X5-R33 and X5-R34, verbatim (nv3-X-5c-F3-F3-write-1,
  2026-11-03; binding; pre-registered)", beside X5-R28–R32 above; the
  two texts verified word for word against the card before the commit).
- **(d) The F3-F1-F5 record fixes:** committed at 97e85cab (the
  17-round table's 13 moved rounds each name e6ebbdda with the gate's
  attribution; RESULT 5's prov commit is 62d4e739; the step-7/8 diag
  artifacts copied to `X-5c-F3-F1.7.cxxd151*.err` /
  `X-5c-F3-F1.7.cxxfix151*.err`, the README pointing at the copies).

**(e) The class table, re-derived from `results/campaign.json` (the
reporting rule: served rounds split out first), judged under X5-R30 (c)
from the recorded fields — a served round passes iff
`(skx ∨ smx) ∧ pass_na` (no re-run).** Field definitions as F3-F1's:
a **failing** primary round has `pass=false` as amended; a **failing
twin** round has `pass=false` in the twin record.

| Class | Rounds | Record-field definition (re-derived, verifiable from the JSON) | Repros (round) |
|---|---|---|---|
| **Served (split out first)** | 7 MP, **all moved to pass** | MP/MW round with `served=true`: 1057, 1897, 2927, 4718, 7238, 7528, 8728 (all r0, all `mrb=true`, all carrying `O-3(a): served corruption` as recorded) — each passes the amendment: skx 1057/1897/4718, smx 2927/7238/7528/8728, all `pass_na=true`. **No failing served round remains.** | 1897 r0, 2927 r0 |
| **C-XX** (class C) | 76 | class C failing round with `o2 != null` or `o3` = `O-3(b): XX001 in class C at [site]` — sites: `Open` 22, `AmcheckIi` 54 (no `o2`-only round) | 150 (r1, `Open`), 151 (r1, `AmcheckIi`) |
| **C-DIV** | 314 | class C failing round **outside C-XX** carrying an `o1` text. **37 carry `frontier < lo`** (36 (b) + 1 (a) — 5054 r1) — the X5-R34 set; the rest: (b) 115, (c) 99, (d) 62, (a) 1. By round index: r0 19, r1 28, **r2 267**. (F3-F1's wider definition — carrying an `o1`, excluding only an O-1(a) inside a C-XX round — gives **364**, overlapping C-XX in 50 rounds: 42 O-1(b) + 8 O-1(c); both counts recorded per the card's step 0 (e).) | 20 (r2, b, `frontier < lo`), 63 (r2, c), 273 (r2, d), 5054 (r1, a, `frontier < lo`) |
| **MW-ORPHAN** | 445 | MW failing round with `o1` = `O-1(c) … raw entry above the cut` and `o3` = `O-3(d): silent Ok with O-1 failing` — F3-F3b's (the `wal_ckpt_end`/I-WAL-COVER work), never fixed here | 19 (r0) |
| **Other MW** | 8 | the MW failing rounds outside ORPHAN: 7 O-1(a)+O-3(d) (4399/4919/5619/7219/7979 r0, 8039/8649 r1), 1 O-1(d)+O-3(d) (6399 r2) — the ORPHAN published-set family | 4399, 6399 |
| **MP O-3(e)(ii)** | 6 | MP failing round with `o3` starting `O-3(e): (ii)`: 4027, 4767, 6647, 6897, 9978 (r0) and 7748 (r1), each "reported slot N that is neither tainted nor in R" | 4027 r0, 7748 r1 |
| **Other MP** | 4 | O-1(b) 3 (3607 r1, 4818 r0, 5227 r1), O-1(a) 1 (5928 r1) — no O-3 text, recovery `Ok` | 3607, 5928 |
| **The twins** | 68 | failing twin rounds: 24 MP + 44 MW; `o1` kinds: MP (a) 3, (b) 16, (c) 4, (d) 1; MW (a) 12, (b) 16, (c) 8, (d) 7, (none) 1 | 339/849 family |

Partition check: C 390 = 76 C-XX ∪ 314 C-DIV (disjoint by the
definition above; 73 of the 76 C-XX rounds also carry an `o1` — 23
O-1(a), 42 O-1(b), 8 O-1(c)); MP 10 = 6 +
4; MW 453 = 445 + 8. Total failing rounds **853** (the recorded
`FAILING_ROUNDS=860` minus the 7 served rounds X5-R30 (c) moves);
failing seeds **894** (the recorded `FAILING_SEEDS=901` minus the same
7 — 41 of the 901 fail only through their twin rounds);
`TWIN_FAILING_ROUNDS=68`; 0 livelock/deadlock rounds, 0 defect seeds.
**Every count equals the card's Goal table** (P's counts at F3-F1's
gate); no deviation to record.

**Step 1 — X5-R34, the acked-commit derivation from SimDisk history
(commit 13da010b; the store unchanged).**

- **(a) What landed:** the SimDisk observer accessor
  `durable_dir_bindings` (the one permitted `src/sim/` change — the
  durable `/wal/` name→inode map, exactly as a crash would leave it);
  `derive_written_acked` takes `p0` (the round's starting append
  position, with the (b) 2 guard: any `wal_commits` record below `p0`
  fails the round loudly) and the bindings, and its **SyncOn acked
  half** is now `D`, the durable prefix of X5-R34 (c)'s R/W/S byte
  rules computed over per-inode interval maps in one pass over `E`
  (never a per-byte loop) — `written`/`hi`, SyncOff's acked and the
  `unparsed_written` report kept exactly. The round record gains
  `ack_over` (the (f) cross-check's list: SyncOn ts's whose own §12.4
  wait returned while their record ends above `D` outside `Rc`);
  non-empty fails the round through the task-error path (X5-R10's
  guard shape) with the ACK-OVER-HOLE text verbatim; `run` writes it,
  `verify`'s parser reads it, the pass fold never reads it. The
  `E4_ACK_DIAG` cross-check diagnostic stays; a new `E4_D_DIAG`
  (stderr, default off) prints `D`, its stop reason, `Rc` and each
  commit's verdict.
- **(b) The frozen-derivation demonstration.** With the frozen per-inode
  `max` acked half restored in place (and the cross-check removed with
  it — both halves of the same ruling), `cargo test -q --release -p
  e4-recovery --test harness x5r34` gives **exactly the four must-fail
  cases red and the must-pass case green**:

  ```
  thread 'x5r34_contiguous_prefix_required_two_batches' panicked … (g) 2
  thread 'x5r34_stale_bytes_of_a_renamed_segment_never_ack' panicked … (g) 1
  thread 'x5r34_unsynced_directory_binding_blocks_acked' panicked … (g) 3
  thread 'x5r34_forged_ack_over_hole_fails_loudly' panicked … (g) 4
  test result: FAILED. 1 passed; 4 failed  (x5r34_genuine_durability_acks green)
  ```

  Reverted clean afterwards (`git diff` free of the demonstration; the
  first test build had dummy record bytes that the frozen walk never
  yields, which would have hidden cases 1–3 — the tests encode genuine
  records so **both** derivations' walks yield them, and the recorded
  demonstration above is with that fix in place).
- **(c) (h) 1's decision rule, at 13da010b before any store change —
  single-seed runs of the 37 named rounds (the D stop reason from
  `E4_D_DIAG`):**

  | Seed/round | lo before | lo after | frontier | pass | outcome | ack_over | D's stop reason |
  |---|---|---|---|---|---|---|---|
  | 20 r2 | 64 | 62 | 63 | **true** | none | [] | `00000000000a0000.log@5952`: no sync of inode 4 after the last covering write |
  | 115 r1 | 25 | 24 | 24 | **true** | none | [] | `…060000.log@18207`: no sync of inode 4 after the last covering write |
  | 403 r1 | 6 | 4 | 4 | **true** | none | [] | `…000000.log@5774`: no sync of inode 2 after the last covering write |
  | 622 r0 | 14 | 12 | 13 | **true** | none | [] | `…030000.log@2316`: no sync of inode 2 after the last covering write |
  | 780 r2 | 59 | 58 | 58 | **true** | none | [] | `…080000.log@22304`: no sync of inode 3 after the last covering write |
  | 801 r1 | 30 | 29 | 29 | **true** | none | [] | `…060000.log@4665`: no sync of inode 4 after the last covering write |
  | 825 r2 | 54 | 52 | 53 | **true** | none | [] | `…090000.log@44524`: no sync of inode 5 after the last covering write |
  | 830 r1 | 30 | 28 | 29 | **true** | none | [] | `…060000.log@21552`: no sync of inode 4 after the last covering write |
  | 1001 r0 | 12 | 11 | 11 | **true** | none | [] | `…030000.log@5740`: no sync of inode 2 after the last covering write |
  | **1064 r1** | 17 | **17** | 14 | false | O-1(b) c=17 | [] | beyond the last bound or recycled segment (0x60000) |
  | 1585 r1 | 18 | 17 | 17 | **true** | none | [] | `…060000.log@18843`: no sync of inode 4 after the last covering write |
  | 1801 r1 | 36 | 35 | 35 | **true** | none | [] | `…070000.log@2762`: no sync of inode 5 after the last covering write |
  | 2183 r0 | 18 | 13 | 17 | **true** | none | [] | `…030000.log@10881`: no sync of inode 2 after the last covering write |
  | 2525 r2 | 76 | 75 | 75 | **true** | none | [] | `…0b0000.log@24886`: no sync of inode 5 after the last covering write |
  | 2730 r2 | 44 | 43 | 43 | **true** | none | [] | `…0a0000.log@17970`: no sync of inode 8 after the last covering write |
  | 2765 r0 | 13 | 12 | 12 | **true** | none | [] | `…030000.log@3102`: no sync of inode 2 after the last covering write |
  | 2812 r1 | 9 | 8 | 8 | **true** | none | [] | `…020000.log@37250`: no sync of inode 4 after the last covering write |
  | 3224 r1 | 7 | 5 | 5 | **true** | none | [] | `…000000.log@9093`: no sync of inode 2 after the last covering write |
  | 3806 r1 | 30 | 29 | 29 | **true** | none | [] | `…060000.log@0`: no sync of inode 4 after the last covering write |
  | **4770 r2** | 57 | **57** | 51 | false | O-1(b) c=57 | [] | `…090000.log@10655`: no issued write covers the offset |
  | 4930 r0 | 28 | 26 | 27 | **true** | none | [] | `…030000.log@28447`: no sync of inode 2 after the last covering write |
  | **5054 r1** | 23 | **23** | 20 | false | O-1(a) c=23 | [] | `…050000.log@34121`: no issued write covers the offset |
  | 5601 r1 | 47 | 46 | 46 | **true** | none | [] | `…070000.log@3981`: no sync of inode 2 after the last covering write |
  | 5604 r0 | 24 | 23 | 23 | **true** | none | [] | `…030000.log@9149`: no sync of inode 2 after the last covering write |
  | 6154 r0 | 34 | 33 | 33 | **true** | none | [] | `…030000.log@13669`: no sync of inode 2 after the last covering write |
  | 6200 r2 | 47 | 46 | 46 | **true** | none | [] | `…080000.log@8005`: no sync of inode 3 after the last covering write |
  | **6263 r2** | 21 | **21** | 20 | false | O-1(b) c=21 | [] | `…060000.log@23222`: no issued write covers the offset |
  | 6623 r0 | 15 | 14 | 14 | **true** | none | [] | `…030000.log@9179`: no sync of inode 2 after the last covering write |
  | 7405 r1 | 36 | 35 | 35 | **true** | none | [] | `…070000.log@10370`: no sync of inode 5 after the last covering write |
  | 7531 r0 | 37 | 36 | 36 | **true** | none | [] | `…030000.log@37339`: no sync of inode 2 after the last covering write |
  | 7803 r1 | 5 | 4 | 4 | **true** | none | [] | `…000000.log@6103`: no sync of inode 2 after the last covering write |
  | 7823 r1 | 28 | 27 | 27 | **true** | none | [] | `…060000.log@63292`: no sync of inode 8 after the last covering write |
  | 8220 r2 | 56 | 55 | 55 | **true** | none | [] | `…070000.log@64072`: no sync of inode 2 after the last covering write |
  | 8363 r0 | 15 | 14 | 14 | **true** | none | [] | `…030000.log@0`: no sync of inode 2 after the last covering write |
  | 9200 r0 | 24 | 21 | 23 | **true** | none | [] | `…030000.log@13082`: no sync of inode 2 after the last covering write |
  | 9484 r2 | 74 | 70 | 73 | **true** | none | [] | `…0b0000.log@4070`: no sync of inode 5 after the last covering write |
  | 9594 r0 | 33 | 32 | 32 | **true** | none | [] | `…030000.log@30106`: no sync of inode 2 after the last covering write |

  **P's prediction holds exactly:** 33 named rounds get `lo ≤ frontier`
  (each now passing with no O-1 text); 1064 r1, 4770 r2, 5054 r1 and
  6263 r2 keep `frontier < lo` with `lo` unchanged (17 / 57 / 23 / 21 —
  the §12.2/§12.5 exact-boundary store losses, step 2's targets);
  `ack_over` is empty on all 37. **No ESCALATE** (none of the four
  boundary rounds is a new keep, no non-empty `ack_over`).
- **(c) the 0..200 run at the tip** (`E4_D_DIAG=1`, head 13da010b):
  **`ack_over` empty over every judged round (0 rounds)**; **failing
  class-C rounds with `0 < frontier < lo`: 0** (F3-F1's tip had 2 in
  0..200: 20 r2 and 115 r1 — both now pass); FAILING_ROUNDS 15 (was
  17), C-XX 2, MW-ORPHAN 6, DEADLOCK 0, DEFECT_SEEDS 0 — **no new
  failing class-C `0 < frontier < lo` round appeared**, exactly as
  predicted.
- **(d) Runtime:** the 0..200 run at the X5-R34 commit took **18 s**
  wall against the step-0-tip baseline of **22 s** (timing-only run,
  diagnostic: it judges by the old derivation and is never cited) —
  no slowdown; the derivation is one extra pass over the round's
  events. Well inside X5-R31's 60-min Acceptance limit.

**Step 2 — the exact segment end (S-1-F3 gate G2, edit 2; commit
ce8f8bbc).** The writer's `fits` rule is already the carried §12.2
tail rule (its comment now cites the carried sentence); the reader
(`ckpt::recover`'s walk) hops into the next segment after an accepted
record that ends exactly at its segment's end (`off == WAL_SEG` after
the advance → `valid_end = seg + WAL_SEG`, `lsn = seg + WAL_SEG`,
`continue 'outer`), exactly as the `K_SEGEND` hop does; the loop's
other exit (a tail shorter than a header at `off < WAL_SEG`) stays a
stop at `off`.

- **The pins** (literal configs, tests/store.rs):
  `wal_exact_segment_end_hops` — four max-size records ending exactly
  at LSN 65536, then two client-acked SyncOn commits in segment 1
  (synced before the crash): recovery replays them (`frontier = 6` ≥
  the max acked) and segment 1's record bytes are not zeroed; and
  `wal_tail_short_never_placed` — an enqueue that would leave a tail
  of exactly `REC_FIXED − 1` (44) bytes is placed at the next
  segment's first LSN behind a `SegmentEnd` (the filler's magic sits
  at the dead tail), and the SyncOn commit after it is acked
  (`wal_wait_durable` returns) and recovered (`frontier = 5`).
- **The mutants, demonstrated and reverted clean:**
  **SEG-EXACT-NO-HOP** (the hop disabled) fails
  `wal_exact_segment_end_hops` — "recovery replays the acked SyncOn
  commits in segment 1 (frontier ≥ max acked)" (frontier stalls at 4
  and `truncate_wal` zeroes segment 1); **SEG-TAIL-SHORT** (`fits`
  relaxed to `probe ≤ room`) fails `wal_tail_short_never_placed` —
  the record is placed at 65437 (the dead tail), not 65536.
- **(e) Movement (X5-R34 (h) 2): single-seed 1064, 4770, 5054 and
  6263 at ce8f8bbc — every named round leaves the `frontier < lo`
  shape, with `lo` unchanged:**

  | Seed/round | lo (before → after) | frontier | pass |
  |---|---|---|---|
  | 1064 r1 | 17 → 17 | 17 | **true** (was O-1(b) c=17) |
  | 4770 r2 | 57 → 57 | 58 | **true** (was O-1(b) c=57) |
  | 5054 r1 | 23 → 23 | 23 | **true** (was O-1(a) c=23) |
  | 6263 r2 | 21 → 21 | 21 | **true** (was O-1(b) c=21) |

  All four boundary store losses are cleared. One workload moved:
  **5054 r2 now fails O-1(b) at c=29 (tree 5, scan mismatch,
  `frontier == lo == 29`)** — moved by ce8f8bbc; not a `frontier < lo`
  round, so it is C-DIV's remainder (step 6), never an X5-R34
  escalation.

**Step 3 — I-ACK-PREFIX with two or more batches in flight (S-1-F3
W3; gate (b) item 4).**

- **(a) The fix** (`src/store/wal.rs`): `wal_writer_step` takes a
  batch whenever the buffer is non-empty — **no `Busy` park on another
  task's batch** — and `WalState.in_flight` is the **set** of taken,
  unfinished batches. The accounting is interval-set based:
  `written_iv`/`synced_iv` hold completed write / synced batch
  intervals; `written` advances while a completed write starts at
  `written` (`absorb_interval`), `durable` while a synced batch starts
  at `durable` (so at or below `written`); a batch counts synced once
  its group `sync_data` of each touched segment completes after its
  write returned. The invariant `durable ≤ written ≤ append` is
  checked at every advance (`check_wal_inv`, a loud `Fail::Invalid`).
  Every advance and every exit (error paths included) wakes `WalAcks`.
  `wal_sync_through` credits `[durable, written)` as it stood when its
  first sync started, capped below the lowest in-flight batch's start
  — it never credits bytes above `written`, nor a held batch's bytes
  (the accounting's units are each batch's **own** group sync:
  "completed own-segment syncs"). The ack rules are unchanged: a
  SyncOn wait returns iff `durable ≥` its record's end, SyncOff iff
  `written ≥` it; a new `writer:prewrite` park point holds a taken
  batch before its write.
- **Judgment call (documented, the frozen pin's requirement):** the
  *waiter's* helper take in `wal_wait_durable`/`wal_wait_written` fires
  only when the buffer is non-empty **and no batch is in flight**
  (`wal_writer_step` itself never parks, so the service writer and any
  direct caller still overlap batches — the two-in-flight shape). Two
  reasons, both recorded: (i) the unchanged
  `cdiv_syncon_ack_never_crosses_an_unwritten_batch` pin holds task A
  at `writer:presync` through a **shared park budget** — a committer
  taking its own batch would pass the same park point and drain it,
  deterministically breaking the pin's hold (measured: with an
  unguarded waiter take, ts=9's ack returns and the pin fails); (ii)
  the card's ack-rule clause — "a waiter whose record lies in another
  task's in-flight batch parks on `WalAcks`" — reads the waiter's
  *gap* through a held batch as exactly that case. "The rule below
  holds however batches are scheduled": a conservative waiter is a
  legal scheduling. Single-task worlds are unaffected (they never
  observe another task's batch, so the helper take always fires
  there).
- **(b) The pins:** the unchanged
  `cdiv_syncon_ack_never_crosses_an_unwritten_batch` passes (task B
  parks on `WalAcks` over the held, written-unsynced batch; `durable`
  never passes batch 1's start; recovery returns the ts-prefix
  `frontier == 4`); D4 passes unchanged (step 1 waits; the writer's
  own group sync after its park budget makes c1 durable — the mutant
  arm still catches WAL-SYNC-T-ONLY); the new
  **`ack_prefix_two_batches_in_flight`** (S-1-F3's `t_ack_prefix`
  ported; a literal two-task config, both arms): task A takes batch 1
  (a SyncOn commit at ts=n) and parks at `writer:prewrite`; task B
  takes batch 2 (SyncOn, ts=n+1) above it, writes and syncs it.
  Asserted: two batches were in flight at once (B's step took batch 2
  while batch 1 was held); after B's write+sync, `written` and
  `durable` both sit at or below batch 1's start and ts=n+1's wait has
  not returned (it parks on `WalAcks`); after A is released, writes
  and syncs, `durable ≥` batch 2's end and both waits return; and in
  the crash arm (a crash before A's write) recovery is a ts-prefix
  with no hole (`frontier == 4`, the WAL restarted at segment 1's
  first LSN) and ts=n+1 was never client-acked.
- **(c) The mutants, demonstrated and reverted clean:** **M-WALa**
  (the concurrent take with `max` accounting restored) fails the
  unchanged cdiv pin — "ts=9's wal_wait_durable returned before the
  crash"; **WAL-ONE-IN-FLIGHT** (F3-F1's `Busy` park restored) fails
  the new pin — "arm 0: two batches were never in flight at once
  (12.4 / S-1-F3 W3)" (B's take is refused; its completed-interval
  witness never appears); **ACK-OVER-HOLE** (the frontiers advanced
  by `max` over completed batches) fails the new pin — "written/
  durable advanced past batch 1's start over its unwritten bytes".
  **One deviation from the card's mutant table, recorded:** under
  ACK-OVER-HOLE the *unchanged* cdiv pin passes (not fails), because
  the guarded waiter never takes a batch above a held one in that
  scenario, so the mutant's `max` accounting never engages there; the
  mutant is caught by the new pin. (The pin's arms are deterministic:
  B itself releases A in the clean arm and arms the scheduler's
  `crash_at_yield` one yield ahead in the crash arm — A's prewrite
  spin trips it before its write — so no driver-side race exists; the
  first version's park-budget hold was flaky under mutex starvation
  and was rebuilt before commit.)
- **(d) Movement (0..200 at the step-3 tip):** `ack_over` **empty
  over every judged round** (no ESCALATE, X5-R34 (h) 4); **failing
  class-C rounds with `0 < frontier < lo`: 0**; the counts beside
  step 1's: FAILING_ROUNDS 18 (was 15), C-XX **0** (was 2 — both 150
  r1 and 151 r1 moved), C-DIV 7, MW-ORPHAN 9, MP O-3(e)(ii) 2,
  DEADLOCK 0, DEFECT_SEEDS 0.
- **Seed-pin maintenance (the F3-F1-gate-accepted shape):** step 3's
  store change moved the workloads wholesale; the MP-A seed pins of
  `r30_skx_and_smx_record_fields` were re-pointed with their
  assertions unchanged: the smx seeds **2927 r0, 7528 r0 and 8728 r0
  still hold** (served, smx, `pass_na=true`), as does 6149 r1 (not
  served, both predicates false, `pass_na` null); the skx seeds
  **1897 r0 and 4718 r0 moved** (no longer served — they fail only at
  the test's shape precondition, `assert!(r.served)`), re-pointed at
  **537 r0 and 1887 r0** (found by ≤ 200-seed scans; each served with
  `skx = true` and `pass_na = true`), assertions byte-identical.

**Step 4 — per-boot ids (S-1-F3 D6, W1's `boot_id` row, W2, W4 (i)–(ii);
gate edit 1; the (f′) three-way amendment).** The fix: **8b71e560**
(step 4a); the pins and mutants: this commit (step 4 e–g).

- **(a)–(d) What landed (8b71e560):** `page::Meta` gains `boot_id`
  (u32, inside the CRC at offset 40 — `build_meta`/`parse_meta`, every
  harness parser already going through `parse_meta`; meta 0 carries 0,
  the creating session's, §2.2 W1); `StoreInner` gains the session's
  `boot_id` (`src/store/mod.rs`, 0 at `create`); every record's `txn`
  is `(boot_id, n)` with `n ≥ 1` dense per boot (`harness.rs`
  `commit_txn` reads the store's session id — the harness's per-round
  `txn_counter` is per boot, a boot being a round's life — and
  `SegmentEnd` carries `(boot_id, 0)`, `wal.rs`'s filler in
  `wal_enqueue`); the boot checkpoint's id per §14 step 8's **three-way
  rule** — the newest, in serial-number order, of the `ckpt_id` the
  boot checkpoint writes (low 32 bits), 1 + the opened meta's
  `boot_id`, and 1 + the newest boot id among the records the recovery
  walk accepted (`ckpt.rs` `recover`'s `recover:boot` block;
  `newest_rec_boot` recorded by the walk) — with every meta of the
  boot carrying it (`checkpoint_inner`'s meta literal); and `recover`'s
  walk gains W4 (i)'s **ordered boot-id stop** (`ckpt.rs` walk,
  `floor = last_boot.unwrap_or(meta.boot_id)`, `a` older than `b` iff
  `(a − b) mod 2³² ≥ 2³¹`): a record of any kind, `SegmentEnd`
  included, whose boot id is older stops the walk at its start; newer
  ids are accepted (the walk crosses into newer boots, W4 (ii));
  **never the equality rule**. The harness checkpoint after a recovery
  (G4) is the §14 step 8 boot checkpoint and always writes a meta, so
  (c)'s "can finish without writing a meta" case cannot arise (no
  ESCALATE).
- **Judgment call (D7's mutant arm, recorded):** 8b71e560 also updated
  `tests/directed.rs` `d7_zombie_tail`'s round-2 arm to the designed
  outcome: its zombie is round 1's record — an older boot's — so W4's
  ordered stop refuses it even under TRUNCATE-LOGICAL-ONLY ("the stop
  is by identity, not by zeroing"); the round now **passes** under the
  mutant (the pre-W4 catcher described a same-boot zombie; this
  fixture's is cross-boot by construction, each round's workload being
  its own boot). **Consequence, escalated below:** TRUNCATE-LOGICAL-ONLY
  is now caught by no failing assertion (its `directed_arms` row was
  already display-only — `run_arm` encodes no arm for it); re-catching
  it needs the §12.5 "Residual" shape (post-sync WAL damage **and** a
  lost meta together), which is new fixture design, not this card's.
- **(e) The pins** (literal configs, `tests/store.rs`; the
  rollback fixtures fault through SimDisk's own API — a post-sync LOST
  on the meta write (`apply_one_media_fault`, built from
  `dwrites_of`), exactly "a post-sync LOST write on meta k+1's slot
  reverts it"):
  - `txn_ids_never_collide_across_lives` — P's finding: life 1 (a
    recovery boot) leaves the unmarked chunks of txn `(id₁, 1)`, life 2
    (a later boot, its id raised per §14 step 8) commits a chunked txn
    with `n = 1` again; recovery attaches only life 2's chunks (key B
    present, key A absent, frontier = the marker's ts).
  - `stale_boot_record_stops_the_walk` (`t_stale_boot_record`) — a
    boot-2 commit at LSN 0, then a complete boot-1 record at the next
    position (the stale bytes a reverted truncation exposes; built
    directly on the segment, the end state §12.5 describes — judgment
    call, recorded: the card's prose builds it through a reverted
    step-5 zero write, and the direct write produces the identical
    walk input); the walk stops at its start (frontier 1, append =
    its LSN, its key excluded).
  - `meta_rollback_walk_crosses_into_newer_boot`
    (`t_meta_rollback_crosses_boot`) — boot b−1 writes meta 1 and
    crashes; boot b recovers, truncates at p_b, writes boot meta 2
    through the harness checkpoint, then appends and acks a SyncOn
    commit from p_b; a post-sync LOST reverts meta 2's slot (the open
    then selects meta 1); the walk crosses into boot b's records and
    the frontier is at or above the max acked. The fixture keeps
    everything inside segment 0 (no recycling touches meta 1's WAL
    span) and boot b's only page writes are its boot checkpoint's
    image pages (off image 1's slots by the allocator's
    `used_durable` rule).
  - `boot_id_fresh_after_rollback` (`t_boot_id_after_rollback`) — two
    successive rollbacks to meta 1 across boots b (id 2) and b+1 (id 3,
    fresh through the walk's record term); b leaves the unmarked
    chunks of `(2, 1)`, b+1 commits a chunked txn `(3, 1)`; recovery
    from meta 1 never attaches b's chunks to b+1's marker (B1/B2
    absent, C1/C2 present).
  - `boot_id_after_rollback_raised_then_no_records` (the (f′) variant,
    S-STORE §14 step 8's fixture at 7b3d3ac1): boots b and b+1 each
    write meta 2, append and ack one commit, and lose the meta write;
    boot b+2 crosses both boots' records (id 4), its meta 2 (`boot_id`
    4, `wal_start_lsn` = the append position p) is durable and it
    writes no record; boot b+3 opens meta 2, its walk from p accepts
    nothing, writes meta 3, appends and acks from p, and crashes; a
    LOST write on meta 3 leaves its slot holding meta 1. The open
    selects meta 2 and replays b+3's acked commit (frontier 4 = max
    acked; P1–P4 all present), and b+3's id is 5, newer than the
    opened meta's `boot_id` 4.
- **(f)/(f′) The mutants,** each applied to the real code, shown
  failing its pin, and reverted (`git diff` clean of the mutant
  afterwards; command: `cargo test -q --release -p e4-recovery --test
  store <pin>`):
  - **BOOT-ID-STOP-SKIP** (the ordered stop disabled) →
    `stale_boot_record_stops_the_walk` FAILED — "the older boot's
    record is not replayed (12.5 W4 (i)): left 2, right 1";
  - **BOOT-ID-EQ-STOP** (stop at any id ≠ the opened meta's) →
    `meta_rollback_walk_crosses_into_newer_boot` FAILED — "the frontier
    is at or above the max acked (Z1, ts 3): left 2, right 3" (the walk
    stopped at p_b);
  - **BOOT-ID-CKPT-ONLY** (the id is the low 32 bits of `ckpt_id`
    only) → `boot_id_fresh_after_rollback` FAILED — "boot b+1's id 2 is
    newer than boot b's 2: left false, right true";
  - **TXN-BOOT-CONST** (every recovery assigns one constant id, the
    pre-card `(1, n)`) → `txn_ids_never_collide_across_lives` FAILED —
    "life 2's boot id is one past life 1's: left 1, right 2";
  - **BOOT-ID-NO-META-FLOOR** (the opened meta's `boot_id` term
    omitted) → `boot_id_after_rollback_raised_then_no_records` FAILED —
    "boot b+3's acked commit replays (the max acked; never a stop at
    p): left 3, right 4", the exact harm §14 step 8's variant names
    (an `Ok` open missing the acked commits).
- **(g) Movement (P's prediction: 186 r2 and 405 r2 lose their C-DIV
  shape).** Single-seed runs at the step-4 tip
  (`X-5c-F3-F3a.s4.seed186/405.*`):
  - **186 r2** — the outcome text is unchanged (O-1(c) at c=15, "raw
    entry above the cut (max ts 16)", frontier 16 ≥ lo 15 — the same
    text F3-F1's triage file records), but the shape is no longer
    `frontier < lo` and the attach mechanism is gone: the failure is
    the MW-orphan shape, not the cross-life chunk attach. Step 6's
    `E4_TXN_DIAG` re-verifies the attach count directly (its
    `0..200`-window check below is the judged evidence here).
  - **405 — moved by 8b71e560** (X5-R34 (h) 6): the triage file's 405
    r1 passed (frontier 7 = lo 7); at the step-4 tip 405 r1 now fails
    **O-1(b) at c=7 with frontier 4 < lo 7** and the chain stops before
    the named r2. Trace recorded (`E4_D_DIAG`,
    `X-5c-F3-F3a.s4.seed405.ddiag.err`): p0=128515, **D=318537**, stop
    = "byte at lsn 318537 (0000000000040000.log@56393): no sync of
    inode 6 after the last covering write (ev 1866)", `rc={}`,
    `ack_over=[]`. (h) 6 applies — the workload moved by this card's
    own store fix, so it is not an (h) 3 escalation — but from the
    Acceptance file on, (h) 5 makes every failing class-C
    `0 < frontier < lo` round a store loss for P's triage, and this one
    will be in it.
- **(g) The 0..200 run at the step-4 tip** (`X-5c-F3-F3a.s4.0200.*`):
  `ack_over` empty over every judged round (0); failing class-C rounds
  with `0 < frontier < lo`: **0**; **C-XX 0, C-DIV 7 (b 3, c 4)** —
  beside step 3's identical C-XX/C-DIV counts (step 3: FAILING_ROUNDS
  18, C-XX 0, C-DIV 7; step 4: FAILING_ROUNDS 18, MW-ORPHAN 9, MP
  O-3(e)(ii) 2, DEADLOCK 0, DEFECT_SEEDS 0).


## The first complete 10k campaign (run 3 of the attempt, committed —
## d2c8bba7, superseded as the triage file by X-5c-F1's run above)

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

**1,560 failing seeds, in classes (the budget valve, X5-R12) —
X5-R15 (c) correction: the failing-seed count is 1,598 once the
seed-level `defect` seeds are included (the 150 NEVER-FIRED seeds of
class 4 carry `defect`; 38 of them had all primary rounds passing),**
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
7. **X5-R15 (c) correction — every failing round belongs to a listed
   class.** The a06e7ca4 list left out three groups, recorded here:
   - **MP O-3 (a), served corruption: 9 rounds** (every one a Q14
     meta-fault round; X5-R16's MP-A — escalated with per-round
     evidence by X-5c-F2, never reclassified by a worker);
   - **O-3 (c), XX001 with no damaged referenced slot: 25 MP and MW
     rounds** (a detection the twin rule could not explain — F2's
     C-XX-adjacent triage);
   - **O-3 (c), "no twin (harness defect)": 19 rounds** — X5-R16's
     NO-TWIN class, X-5c-F1's to fix.

**Coverage quotas (verify's three exit conditions):** the seed set is
exactly 0..9999, but **the digests did not reproduce** — verify's
re-run found four non-reproducing seeds (180, 559, 7474, 8832; class 7
below, X5-R14's subject; the determinism pins were **not** holding).
The quota table below stands as computed from the committed file.
**X5-R15 (c) correction of this paragraph's first record** (it claimed
"the digests reproduce … the determinism pins hold" — false). **5
quotas miss**: Q4 0/1000
(no interrupted checkpoint has ≥1 split **and** ≥1 merge in the
window), Q7 155/200 (DropTree-walk crashes), Q11 0/100 (no stale
record ever read at a renamed segment's start), Q12 2/100 (G1 renames
dropped by F-CRASH), and **SIZING: 14,988 rounds miss Work 5.3's ≥ 4·F
distinct dirtied LPIDs** — the workload never dirties 4× the pool in
one interval (a harness defect per 5.3, i.e. a workload-sizing
retune, which the card allows only before a final run). Q1 (all 10
hooks), Q2, Q3, Q5a–c, Q6a/b, Q8, Q9, Q10, Q13a–c, Q14, Q15, Q16,
Q17, Q18 are all met.

7. **Four seeds do not reproduce across runs** (verify's digest
   re-run: seeds 180, 559, 7474, 8832 — "did not reproduce", 4/10000).
   A W-DET/S-DET violation: the digest is a pure function of
   `(seed, mutant)` by construction, so some rare timing path leaks
   into a sim-visible decision (a scheduler race, or an ordering the
   record reads before it is canonical). Not root-caused here; the
   deterministic-looking repro (`run --seeds 180..181` twice, compare
   digests) is itself only sometimes reproducing — the class is
   recorded as the seventh failure class, and it is also why
   `verify` exits 2 before its quota checks print.

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
cargo test -q --release -p e4-recovery      # the whole crate (172 tests)
cargo test -q -p e4-recovery --test workload  # the Work 5 self-tests + smoke (20)
cargo run -q --release -p e4-recovery --example pin   # the pinned CRC32C /
                                                      # splitmix64 values (Work 0)
cargo run -q --release -p e4-recovery --bin e4-campaign -- verify \
  --results spikes/e4-recovery/results/campaign.json
# verify re-runs all 10,000 seeds at 16 workers (on this host the esc-2
# campaign took ~20.6 min and its verify ~19.7; each is its own
# command under the runner's 60-min limit). On this file it
# exits 2 on the five missed quotas alone (Q4/Q7/Q11/Q12/SIZING — X5-R17's
# retune is X-5c-F4's); the digests and the seed set reproduce. Its exit
# is 2 for any failed check (seed set, digests, quotas), 0 otherwise.
# `--no-quotas` (X5-R15 (b)) waives only the quota exit: X-5c-F1's
# Acceptance form.
```

**X-5c-F3-F1's Acceptance** (the runner's, X5-R31 — this card runs no
campaign-class command in its attempts; single-seed and ≤ 200-seed runs
only, under the backstop, output outside the worktree):
`$HOME/.cache/nv3-review/runlogs/X-5c-F3-F1.acc.campaign.json` (the 10k
`run --seeds 0..10000 --threads 16`, `ulimit -v 25165824`), its prov in
`X-5c-F3-F1.acc.prov`, then `verify --no-quotas` of that file
(`X-5c-F3-F1.acc.verify.out`), then the record check and
`bash scripts/gate.sh`. Its 10k becomes **F4's step-0 triage file**. The
class-table repros of this card's steps re-derive from a ≤ 200-seed run
at the tip (the recorded commands live in the stage record above).

**The X-5c-F2 Acceptance 10k is the runner's, never a worker's
(X5-R31).** Inside an attempt no worker runs `e4-campaign run` over
more than 200 contiguous seeds, `e4-campaign verify`, a stress loop,
or `scripts/gate.sh` — X-5c-F1 lost three runs to campaign-class
commands inside attempts (a 10k `run` and a `verify` each take ~20 min
at load 20, against the runner's 60-minute attempt limit). The
Acceptance, at the card's final tip, is:

```bash
cd nucleus/v3
O=$HOME/.cache/nv3-review/runlogs/X-5c-F2.acc
{ echo commit=$(git rev-parse HEAD); echo status_lines=$(git status --porcelain -- spikes/e4-recovery Cargo.lock | wc -l); echo start=$(date -Iseconds); echo load_at_start=$(cut -d' ' -f1-3 /proc/loadavg); } > $O.prov
(ulimit -v 25165824; cargo run -q --release -p e4-recovery --bin e4-campaign -- run --seeds 0..10000 --threads 16 --out $O.campaign.json > $O.campaign.out 2> $O.campaign.err)
{ echo exit=$?; echo end=$(date -Iseconds); echo load_at_end=$(cut -d' ' -f1-3 /proc/loadavg); echo json_sha256=$(sha256sum $O.campaign.json | cut -d' ' -f1); } >> $O.prov
echo verify_head=$(git rev-parse HEAD) verify_start=$(date -Iseconds) >> $O.prov
(ulimit -v 25165824; cargo run -q --release -p e4-recovery --bin e4-campaign -- verify --no-quotas --results $O.campaign.json > $O.verify.out 2> $O.verify.err)
echo verify_exit=$? verify_end=$(date -Iseconds) >> $O.prov
bash scripts/gate.sh
```

The prov file `$O.prov` records the commit, `status_lines` (the crate
and the lock only), start/end, load, exit, the JSON's sha256, and the
verify head and exit; on a retried attempt the last write wins, at the
then-current tip. The five known quota misses (Q4, Q7, Q11, Q12,
SIZING) are expected and waived by `--no-quotas` — they are X-5c-F4's.
X-5c-F3's step 0 commits `$O.campaign.json` after the provenance
checks (the X5-R25 pattern). Single-seed diagnostics stay allowed
inside attempts: `E4_MPE_DIAG=1 cargo run -q --release -p e4-recovery
--bin e4-campaign -- run --seeds N..N+1 --threads 16 --out <outside
the worktree>` (the K/K_ii/served_k and R29 diag lines are stderr
only, never digest inputs).

X5-R15 (e): `examples/` keeps only what this block uses (`pin.rs`;
`dbg-seed.rs`, which 823d4f51 called scratch, is removed), and
`restore_probe.local.py` (untracked) is not in the tree.

