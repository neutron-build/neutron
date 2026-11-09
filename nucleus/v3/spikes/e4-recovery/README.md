# E4 recovery spike (card X-5, stage X-5c of three)

Status: **stage X-5c-F4-F1-F1-F3-F2-F1-F1-F1 — the X5-R53 card: P's
post-data amendment to X5-R52 (verbatim below), written after the
re-verdict at `26bcee4b` (MERGE-WITH-FOLLOWUPS) named two follow-ups on
X5-R52's comparator and rule. This card narrows `check-twin` to the
twin's own segments (T1 and T3 as X5-R53 (b) writes them), adds the
sixth check T6 (the ts-level link, on one clock per chain), caps
exemption (iii) on the MP-A class size and prints `MPA_CLASS`
(X5-R53 (d)), makes a MONITOR>0 run stop the comparison by itself
(X5-R53 (e)), closes the four self-test coverage holes the gate's
mutants found, and re-reads the same F2-F1 10k again requiring the same
reading (no campaign re-run; nothing under `src/`, `tests/`,
`examples/`, `results/` or `x5r52-twin-diag.patch` changes).** Before
it: stage X-5c-F4-F1-F1-F3-F2-F1-F1 — the X5-R52 card: P's
post-data amendment to X5-R51 (verbatim below, with X5-R51 quoted
unchanged beside it), written after the F2-F1 gate verdict's ESCALATE;
the comparator committed as a tool with bullet 1 testable, and the same
10k re-read under the amendment (no campaign re-run). F2-F1's commits
merge through this card, on its chain, with no code change. Before it:
stage X-5c-F4-F1-F1-F3-F2-F1 — this card continues F2
(X5-R49) from its tip `1524e284` under the gate verdict's P ruling
X5-R51, the step-4 movement exemption (fixed verbatim below, with the
g8 re-selection rule, before any code or literal of this card).**
F2 stopped because its own step-4 rule fired on seven rounds; the
gate's finding (verdict `X-5c-F4-F1-F1-F3-F2.md`) is that those are
schedule movement, not a loss X5-R49 introduced — X5-R49's code at
`b5c9a840` is correct as written and stays untouched. This card
re-reads the movement under X5-R51, runs F2's step-4 "Otherwise"
branch (the literals, the share-1000 comparison, F3's mutants, the
diagnostic), adds the verdict's -F2 pin for the uncaught
O-FREE-DROP-IMGREFS window (-F2 folded in here, no separate card), and
re-selects `g8`'s second seed by a fixed rule. Before it: stage
X-5c-F4-F1-F1-F3-F2 (X5-R49) implemented and pinned (three new pins,
six mutants caught, D14 and rework 6 unchanged), stopped at step 4's
gate — the record and the rounds are below, with this card's
correction of its reading per verdict finding 2. ESCALATE resolved by
P ruling X5-R51.** Before it:
stage X-5c-F4-F1-F1-F3 (X5-R41/X5-R42/X5-R43) complete — its decision
rule NOT MET by all three shares, the Joint fault CAUGHT at share 1000
and the Slot fault UNCAUGHT at every setting measured (both ESCALATEs,
the record below; the Slot fault's cause — this card's store deviation
— is what X5-R49 fixes). `Tuning::FINAL == Tuning::BASE` at this tip
(no setting was handed off); the X5-R37 pin holds the **old** literals
at this tip (13 of its 54 seeds moved with the fix; the measured new
digests are recorded below, unapplied). The stages before it
(X-5c-F4-F1-F1 and its follow-ups, X-5c-F4-F1, X-5c-F4, X-5c-F3-F3b
and back to X-5c) are complete and merged. Opus judges E4 on the
RESULT with P's rulings X5-R1–R49.
**Card X-5c-F4-F1-F1-O1C-F1 (X5-R47) has since replaced the `written`
half and the F1 report of `derive_written_acked` with the replay in
issue order — the three faultless class C O-1(c) rounds (1066, 6555,
7590 r0) now pass; the record is below.** **Card
X-5c-F4-F1-F1-CXX-F1 (X5-R48) has since repaired X5-R21 (c)'s
re-pair for the twin that never fires (the joint spec): 7469, the
one defect seed of CXX's 10k window, now passes with all three
rounds; the record is below.**

## P ruling nv3-X-5c-F4-esc-1, verbatim (2026-11-04; binding)

Copied verbatim from the card before any other change of this stage
(the step-0 discipline, as X5-R6–R36 before it). The four ESCALATE
rulings and the two amendments:

- **ESCALATE 1 → X5-R37.** A seed pin's identity is (generator, seed),
  not seed alone. Every seed pin runs `Tuning::BASE`, 1b0ce54e's
  generator bit for bit, which a digest-literal pin proves. The retune
  changes only `Tuning::FINAL`.
  - BASE is never edited.
  - A later store fix that moves a BASE digest updates that literal in
    its own commit, naming the seeds and why. The pinned assertions
    must still hold under BASE, or the fix is wrong.
  - Under BASE, re-pointing a shape is a lookup in F4's file (its ten
    served rounds are 537, 1887, 2927, 4098, 7238, 7528, 8107, 8578,
    8728 and 9358, all r0), never a luck scan.
  - The final workload's served rounds are judged from the final file
    by X5-R30 (c) rule 4, as before. The predicates' semantics stay
    pinned by `r30_predicate_conditions_are_load_bearing`.
- **ESCALATE 2.**
  - **The accounting is correct:** pool LPIDs only.
  - **"Every seed" cannot be met as defined.** 5.5's uniform crash can
    land before any interval reaches 4·F, and 50% of seeds have one
    round. So zero misses is unreachable by construction, not only by
    budget.
  - Changing SIZING's definition is an **owner question (OQ-1)**.
    Until it is decided, `szc` is a diagnostic.
- **ESCALATE 3.** The quota is sound and the counter was never wired
  (re-measured by P: no increment site; the only writes are the
  declaration and the initialiser). It is wired here. X5-R38's W11
  window supplies the rate.
- **ESCALATE 4 is refuted by measurement.** Merges need underfull leaf
  pairs, and deletes make them; skewing the value size does not.
  - *P's probe, 0..200 at 1b0ce54e plus: one burst per round deleting
    a contiguous 60% of one tree's chunked keys (commits of ≤ 20 ops),
    and 8 extra GC sweeps on that tree.*
  - *Results: Q4 132 of 309 rounds (×50 ≈ 6,600 per 10k against
    1,000), Q18 49 rounds, 0 failing rounds, 19.3 s against 15.4 s per
    200 seeds (quiet host). Probe, not evidence.*
  - Q4 is reachable by retune (F4).
  - **Correction (P gate verdict X-5c-F4-F1, finding 3, 2026-11-04) —
    P's error, not the worker's: ESCALATE 4's refutation is withdrawn.**
    The "Q4 132 of 309" in ruling nv3-X-5c-F4-esc-1 is q[3],
    **Q5-partial**. In P's saved file (`X-5c-F4-esc1-q4probe-0..200.json`)
    q[0] is 0 and q[3] is 132; its 309 rounds and q[19] = 49 (Q18) match
    the ruling's other figures. P's patch re-run here: Q4 0 of 309. The
    worker's burst knobs reproduce that: 600/8 gives 0 of 309, 900/16
    gives 0 of 308. In-window hook rounds at 900/16: 88, of which 5
    have a split and 0 a merge. Probes top out at 62 per 10k against
    1,000. REFRESH 4's Q4 bullet is wrong and F4 cannot meet Q4 with
    it. See follow-up X-5c-F4-F1-F1.
- **X5-R38 (window crashes; a 5.5 amendment, pre-registered before the
  final run).** In uniform-mode rounds, a seeded share of crash points
  is drawn uniformly over the recycle's rename→dirsync yields (W12) or
  over the yields where the synced WAL end sits at a recycled segment's
  untouched start (W11). The rest stays 5.5's uniform draw. Targeted
  (hook) rounds are untouched.
  - **Why.** Q11 and Q12 are sub-step windows of one or two yields
    among thousands, which uniform crashes reach at about 1–15 per
    10k. The plan row's "a crash between every step 0–6" covers step
    6's recycle, and these windows are its two G1/§12.5 edges.
  - **Bounds.** The shares are F4's knobs, each in [0.05, 0.20]. Every
    quota is still checked on the whole file.
- **(c)** The poison_fail_stop fix is accepted.
  - The flag only routes an external G8 mark to the poison end that
    G8's contract prescribes. The oracle still judges the round.
  - Fail-stops keep the loud path: D12b and D4 were re-verified by the
    worker.
  - The single-arm gap is closed in step 5.

## P gate verdict X-5c-F4-F1 (REWORK, 2026-11-04) — ruling (i), verbatim

The gate returned REWORK (commit 46443a6d; verdict
`verdicts/X-5c-F4-F1.md`). Its ruling (i) replaces this card's step-3
W11 bullet and X5-R38's W11 clause above — carried verbatim, binding:

> **W11.** Let `b` be a segment boundary whose segment a `wal_recycle`
> rename bound and which has had no completed `sync_data` since. W11
> opens when a `sync_data` of the segment below `b` completes with
> `wal.written ≥ b`: that segment's whole extent, filler or
> boundary-ending record included, is then durable, so the walk reaches
> `b`. It closes when a `sync_data` of a segment at or above `b`
> completes. "Untouched" means the durable image: a write at `≥ b` may
> be issued and still volatile, and F-CRASH decides whether it survives.
> Q11 then requires that `b`'s first sector does not survive with new
> content.

## Stage X-5c-F4-F1's record (this card)

**Step 1 — X5-R37's `Tuning` (commit 5270264a).** Every generator
constant F4's retune may change lives in `pub struct Tuning` (5.1's
ranges and tables, the commit and chunked op counts and the value
range, the delete rate, the GC sweep count, the bulk-load/drop shares,
and two new knob pairs both 0 in BASE and so never drawn: the
delete-burst `burst_frac`/`burst_gc_loops` — P's Q4 probe above — and
X5-R38's `w11_share`/`w12_share` in permille). `Tuning::BASE` is
1b0ce54e's generator bit for bit; `Tuning::FINAL` equals it at this tip
(pinned by `x5_r37_final_equals_base_at_this_tip`).
`run_seed_with(seed, mutant, &Tuning)` runs a seed under an explicit
generator; `run_seed` delegates to FINAL, so the campaign's
`run`/`verify` are unchanged. **The digest-literal list** (pin
`x5_r37_base_tuning_reproduces_1b0ce54e`, the literals copied from
X-5c-F4's Acceptance file, sha `bec7056c…3ab6c5`, commit 1b0ce54e):
all **51** seeds any test passes to a seed runner — the smoke and
round-trip seeds 0–15, and 47, 48, 60, 66, 67, 112, 118, 198, 221,
268, 308, 387, 403, 537, 660, 747, 907, 967, 987, 1267, 1749, 1769,
1787, 1887, 2669, 2927, 4049, 4309, 5549, 5808, 6149, 7469, 7528,
8728, 9809 — cross-checked against P's 43-seed list: the grep's
additions from the file are 0–6 and 8–9 (the smoke/round-trip seeds),
and P's 7359 — one of the C-DIV-in-MW five — is run by no test at this
tip (no pin). Every seed pin (the X5-R36 pins, the 1749/5549 frontier
pins, `r30_skx_and_smx_record_fields`,
`g8_checkpoint_poison_ends_the_workload`, the R16/R20/R21/R28/G8/C-XX
pins) re-points to `run_seed_with(…, &Tuning::BASE)` — only the calls
changed; the 16-seed smoke, the uniform-fired property test, the JSON
round-trip and `campaign.rs`'s own runs stay on FINAL. M-BASEDRIFT
(GC sweeps `2 + below(3)` → `3 + below(3)` in BASE) fails the pin
(seed 3's digest moved) — applied, shown, reverted clean. The knob
property tests derive under BASE (they assert 5.1's card ranges).

**Step 2 — the Q11 counter (commit f10d4d63).**
`wal::is_stale_record(b, lsn, seg_end)` — true iff magic, `len` and
CRC are valid and the embedded lsn ≠ the position — implemented through
`dec_record_inner` (the skip-lsn-check decode succeeds where the
checked decode fails; never a second copy of the check). In `recover`'s
walk, where the decode fails at `off == 0` and the bytes are a stale
record, `st.stale_seg_reads` increments and the walk stops exactly as
before (stale content at a segment's start can only be a recycled
segment's: a fresh segment starts zeroed). Pins:
`q11_stale_record_at_recycled_segment_start_counts` (a literal fixture
— a meta replaying from a `SegmentEnd` at 64 KiB+100 that hops into a
recycled segment B whose offset-0 bytes are its previous life's valid
record: the counter is 1 and `wal.append` — the valid end — is B's
first LSN, identical to the walk without the increment) and
`q11_mid_segment_stale_does_not_count` (a valid Chunk then stale bytes
at +45, mid-segment: the counter stays 0, the valid end is the stale
record's start). M-Q11UNWIRED (the increment removed) fails the first;
M-Q11ANY (the `off == 0` guard removed) fails the second — both
applied, shown, reverted clean. Seed pin: `run_seed_with(4, None,
&Tuning::BASE)` — r0's Q11 flag is true (P's probe hit, 2026-11-04).
**The movement list:** all 51 pinned seeds re-run under BASE — exactly
one digest moved, **seed 4** (`3047956786 → 1441374082`), its r0 Q11
flag flipping 0→1, the only digest input the wiring touches; the
literal is updated in the wiring's own commit per X5-R37's
moved-digest rule. Every other seed's digest is byte-identical.
**Rework 4's correction (the gate's finding 4): the movement list above
was the pinned subset — the file has four.** The gate's full 10k
re-run at the first attempt's tip (against F4's file): exactly **4
digests moved — 4, 8422, 9226, 9606 — each differing only in the q11
flag, 6 rounds in total**: seed 4 r0 (`Before0@2`), seed 8422 r1 and
r2 (`After0@2` both), seed 9226 r0 and r1 (`After0@2` both), seed
9606 r0 (`Before0@2`), every one of them a passing round. 8422, 9226
and 9606 are run by no test at this tip and stay unpinned (the
digest-literal list keeps F4's literals out for them; per X5-R37's
rule they join only when a test runs them).

**Step 3 — X5-R38's window crashes (commit a5fc0eb1).** Store
observables (flags only, never behaviour inputs): **W12** opens when a
`wal_recycle` rename returns and closes when its `sync_dir` completes;
**W11** opens when a completed WAL `sync_data` of the segment below a
boundary `b` leaves the synced end durable exactly through `b` while
`b` is a recycled segment whose own content is not yet durable
(`wal.recycled_unsynced`; the note fires at both sync sites — the
writer's group-sync loop and `wal_sync_through`'s helper loop) and
closes when a sync at or above `b` completes. The counting pass records
each opening's first crashable yield index (`current + 1`; the set runs
between two yield events) into `WorkloadObs::w11_yields`/`w12_yields`.
In a uniform-mode round with `w11_share + w12_share > 0`, one draw from
the new per-round stream `S_CWIN` picks W12 (prob `w12_share`), W11
(prob `w11_share`) or the unchanged uniform point; a picked window
takes its point uniformly over its list, an empty list falls back.
Record field `cwin` (`u`/`w11`/`w12`/`w11-`/`w12-`), a digest input
only when it is not `u` — BASE digests never move (the whole pre-step
suite is unchanged); `campaign.json` gains `cwin` (old files parse
unchanged). **Judgment call, for P's gate:** the card's W11 text ("set
when a completed WAL sync leaves the synced end exactly at a segment
boundary `b` … cleared when any write at ≥ `b` is issued") implemented
literally never opens — measured **0 openings in 500 rounds** (its
precondition is a batch ending exactly at a boundary, which the
placement's filler+record atomicity makes never happen) — while the
natural Q11 hit (seed 4 r0) crashes in the per-segment sync loop's
window: the segment below `b` synced through `b`, `b`'s own record
written but its sync pending, the crash dropping it. X5-R38's own
summary names "the yields where the synced WAL end sits at a recycled
segment's untouched start" — exactly the implemented window; the
deviation (the close is the sync of a segment at or above `b`, not the
write's issue) is recorded here. Pins:
`x5_r38_w12_crash_drops_the_rename` (keep-latest table zeroed,
`w12_share` 1000: seed 11 r0, cwin `w12`, fired, Q12 true, passes) and
`x5_r38_w11_crash_reads_stale_at_start` (`w11_share` 1000: seed 276
r0, cwin `w11`, fired, Q11 true, passes). M-W12ALL (the W12 draw over
all yields) fails the W12 pin — applied, shown, reverted clean.
**Probe, not evidence** (FINAL's shares temporarily 0.10/0.10, then
reverted): `run --seeds 0..200 --threads 16`, 305 rounds — **Q11 1,
Q12 5, Q5a 3; cwin picks w11 14 / w12 18; empty fallbacks `w11-` 1 /
`w12-` 0**; `FAILING_SEEDS=2` (the pre-existing 8/178 pair),
`DEFECT_SEEDS=0`, 34 s wall.
**Rework 2 + 4 (the gate's finding 2 and ruling (i)): the as-built W11
window opened on any completed sync of the segment below `b`, so the
crash landed where the walk stops below `b` — the gate measured 0 of
150 picks hitting Q11 at share 1000 (0..200) as built, against 45 of
79 (57%) with the `wal.written ≥ b` fix.** The fix landed (ruling (i)
carried verbatim above); the probe **re-measured post-fix** with the
same command and shares (0..200, 304 rounds, 29 s): **Q11 8, Q12 5,
Q5a 3; cwin picks w11 9 / w12 18; empty fallbacks `w11-` 5 / `w12-`
0; W11 yield 4 of 9 picks hitting Q11; W12 5 of 18 picking Q12**;
`FAILING_SEEDS=2`, `DEFECT_SEEDS=0`. Probe, not evidence. The rework's
added pin `x5_r38_w11_crash_walk_reaches_the_boundary` (seed 10 r0,
`w11_share` 1000, the driver's counting-pass machinery by hand: the
crash at the first recorded W11 yield, the recovered store's
`wal.append` — the walk's valid end — at or above the boundary the
crash-time `w11_open` snapshot names) pins the walk property itself;
M-W11-ANYSYNC (the fix removed) fails it with the walk stopping at
233,459 below `b` = 262,144. Seed 276 was re-verified post-fix (the
pin still hits; no re-derivation). `E4_W11_DIAG` is removed.

**Step 4 — SIZING crash-cut accounting (commit 02bf978f).** Store
observable `ckpt_closed_dirty`: the `n` the **open** checkpoint's begin
step folded (0 when no checkpoint is open — reset at `ckpt:end` and on
the abort path). Round field `szc`: the crash fired and either
`dirty_now_len ≥ 4·F`, or a checkpoint was open at the crash with
`ckpt_closed_dirty ≥ 4·F` (`szc_of`/`sizing_ok_of` are pub and pure in
the observables; serialized in `campaign.json`, never a digest input,
never an exit input). `verify` prints exactly one INFO line — `INFO
SIZING-C crash rounds cutting a >= 4*F interval: C <n> MP <n> MW <n>` —
after the quota table, before the exit decision. The accounting itself
is unchanged (P's ruling: pool LPIDs only). Pins:
`szc_crash_cuts_an_open_4f_interval` (pool 6 so 4·F = 24; a 3,000-put
chunked txn, no checkpoint, crash at the next op — `szc` and
`sizing_ok` both true) and `szc_a_completed_4f_interval_and_a_small_cut`
(the same txn, then a completed forced checkpoint folding the ≥ 4·F
interval into `dirty_max`, then one small commit before the crash —
`sizing_ok` true, `szc` false, `ckpt_closed_dirty` 0). M-SZC-ANY
(`szc := sizing_ok`) fails the small-cut pin — applied, shown, reverted
clean. **The MP gap root-caused — a workload property, not an
accounting defect:** in F4's file the flag is 0 in **all 10,000 round-0
rounds** (C 0/7000, MP 0/2000, MW 0/1000) — one round's universe cannot
dirty 4·F = 172–232 distinct pool LPIDs in one interval; the flag
concentrates in later rounds (C r1 47/3468 = 1.4%, r2 331/1393 =
23.8%), and MP/MW chains stop after r0 far more often (MP 1,792 of
2,000 seeds stop early), leaving MP 190 r1+ rounds against C's 4,861;
per-index rates are comparable (MP r1 1/182, r2 1/8). Single seeds: MP
537 stops at its failing r0 (no later round exists); C 10 runs three
passing rounds and its r2 is the `sizing_ok` hit. The `obs` snapshot is
the same store — no accounting defect to fix.
**Rework 1 + 4 corrections (the gate's findings 1 and 4, ruling (ii)
and (iii)):** (a) the abort path above had reset `ckpt_closed_dirty`
on every `Err`, so the field was **0 in all 15,386 rounds** at the
first attempt's tip and `szc`'s second disjunct was dead — fixed (a
sim-crash unwind keeps the folded `n`; the rework's pins
`szc_survives_the_sim_crash_unwind_into_the_record`, seeds 41 r2
`After1@0` and 303 r2 `After3@0`, both with the open interval at 1 and
3 against 4·F 208/196 — so the record's `szc` can only be true through
`ckpt_closed_dirty`; M-SZC-ABORTRESET and M-SZC-NOCK each fail it, and
the card's literal `szc := sizing_ok` at `round_rec` — UNCAUGHT at the
gate — now fails `szc_a_completed_interval_never_sets_the_record_flag`,
seed 660 r2: `sizing_ok` true from `dirty_max` 250 ≥ 184, `szc` false).
No digest moves (`szc` is not an input; 41/303 joined the X5-R37 list
with F4's own literals). (b) The MP stop count's cause above was
wrong: it is **1,771 of the 1,792 stopped MP seeds passing with no
recovered state — recoveries legitimately refusing (detection), with
only 21 failures** (the gate's finding 4); per ruling (iii) the larger
half is that `szq` excludes refused rounds and **81% of MP rounds
refuse (409 of 2,190 hold a state)** — MP is raised by F4's retune
(r0 can reach `szq` on retuned knobs: k5 measured 4 r0 hits), with the
reachability question the verdict's owner item 1.

**Step 5 — P ruling (c), the M-CKPOISON arms (commit 5392901d).** The
`h:ck` **poll arm is deleted** (unreachable alone): with the
mid-checkpoint `Fail::Poisoned` arm in place, every schedule it covered
— an external G8 mark observed at the poll — produces an identical
record through the draft poll path's final checkpoint, whose
`ckpt:begin` refuses on `alive()` with the same `Fail::Poisoned` and
ends the workload at the same poison crash; the checkpointer holds the
baton throughout (`cx.with` steps are atomic), so no interleaving
window and no record field can differ. **Demonstrated** with the
literal poll-side schedule (round 0 an R6 detection round — a directed
LOST fault on a mapped tree page, recovery Ok, passing; round 1 a
600-put chunked txn on the damaged tree with `every` unreachable): the
round digest is **1585665090 both with the arm present and with it
deleted**, byte-identical. The **mid-checkpoint arm keeps its behaviour
and is pinned alone**
(`g8_poison_mid_checkpoint_ends_the_workload_quietly`: a 4,000-put
SyncOff chunked txn on a fresh clean tree triggers the `every = 1`
checkpoint with a large dirty set — step 2 runs long — and the
600-put poisoner on the damaged tree lands the mark from the LAST
script task's apply while the checkpointer is inside its checkpoint:
fired, crash `poison`, no task error). **M-CKPOISONb** (the arm forced
to the draft's loud path) fails it with the exact 17-round class text
("checkpoint failed: Poisoned(\"XX001 in gated op: lpid 14 slot 10 (G8,
9.2)\")", no crash delivered) — applied, shown, reverted clean. **The
full revert** (the remaining quiet path gone) still fails
`g8_checkpoint_poison_ends_the_workload` (seed 747 r1, the same class
text) — shown. The post-deletion poll path stays quiet and pinned
(`g8_poison_poll_path_stays_quiet_through_the_final_checkpoint`).
Neither arm's behaviour changed.

**The hand-off to X-5c-F4's relaunch (REFRESH 4, `--keep` once OQ-1 is
decided):**
- `Tuning::FINAL` is F4's to change (BASE is frozen; the seed pins run
  BASE and cannot move).
- The X5-R38 shares `w11_share`/`w12_share` (permille, each in
  [50, 200]); **W11 is ruling (i)'s window (carried verbatim above)**.
  At 100/100 the re-measured post-fix 200-seed probe measured Q11 8 /
  Q12 5 / Q5a 3 with 9/18 window picks, 5 `w11-` empty fallbacks and a
  W11 yield of 4 of 9 (above, "probe, not evidence"); at share 1000
  the gate measured the W11 yield at 45 of 79 picks (57%) over 0..200
  — F4 tunes from there.
- The delete-burst knobs `burst_frac`/`burst_gc_loops` (P's probe:
  600/8 — a contiguous 60% of one tree's chunked keys as commits of
  ≤ 20 ops, plus 8 extra GC sweeps on that tree). **Do not tune Q4 on
  the esc-1 ruling's "Q4 132/309" — that number was Q5-partial (the
  gate verdict's finding 3, carried beside ESCALATE 4 above); Q4
  measured 0 of 309 under P's re-run and the burst knobs, topping out
  at 62 per 10k. Q4's reachability is X-5c-F4-F1-F1's, and its
  threshold is unchanged.**
- `verify`'s INFO SIZING-C line prints the crash-cut diagnostic per
  class; `szc`/`cwin` are in `campaign.json` for the file-side
  judgement.
- Step 3's W11 judgment call is **resolved**: ruling (i) accepted the
  reading in substance and prescribed the implementation (the
  `wal.written ≥ b` open term, landed in rework 2).
- REFRESH 6 of the F4 card (pasted by the gate verdict) supersedes the
  older REFRESH text where they differ — F4 relaunches only after this
  rework's gate and X-5c-F4-F1-F1.

## Card X-5c-F4-F1-F1's record (Q4 reachability: the halves, X5-R40)

The question: is Q4 ≥ 1,000 per 10k reachable without changing Q4's
definition or threshold? **Answer, by this card's rule: not by any of
its three settings — ESCALATE (step 4 below).** The halves measurement
says why: the merge half never reaches the crash's window at BASE or
at the burst knobs alone, and even with the X5-R40 alignment the two
halves land in-window in *different* rounds — only 1 round in 1,239
carries both (the pin hunt).

**Step 0 — the re-gate's findings closed (verdict X-5c-F4-F1
MERGE-WITH-FOLLOWUPS, findings 1, 2 and 4; commits dc156302, ebf30ee9,
26a969e0), before any probe.**
- **(a) `wal_recycle` must not swallow a delivered crash.** The
  rename's error now returns through a crashed-flag check — never a
  plain `?`, which would also return the quiet uncrashed-world failure
  ("`to` exists") the store relies on skipping. Pin
  `szc_rename_crash_keeps_the_open_fold`: `run_seed_with(2376, None,
  &Tuning::BASE)`, r2 crash `uniform@y:17768`, `szc` true; 2376 joined
  the X5-R37 literal list (54 seeds now) with F4's own literal — no
  digest moves (the gate ran the change over the whole 10k: only seed
  2376 r2's non-digest `szc` changed). M-RENAME-SWALLOW (`is_ok()`
  restored) fails the new pin, and M-RENAME-ANYERR (a plain `?`)
  fails `x5_r37_base_tuning_reproduces_1b0ce54e` — applied, shown,
  reverted clean.
- **(b) `x5_r38_w11_needs_no_completed_sync_since`:** BASE with
  `w11_share` 1000, seed 30 r0, `cwin` `w11-` (the W11 list empty —
  the `recycled_unsynced.retain` retire line is what empties it).
  G-W11-NORETIRE fails it — applied, shown, reverted clean.
- **(c) `szc_non_crash_abort_resets_the_fold`:** a literal
  `wal_io_fail` schedule whose open checkpoint has folded 2 (proven by
  `dirty_max`) fails with a non-`Sim` error and ends the round with
  `ckpt_closed_dirty` 0. G-SZC-ABORTNORESET fails it — applied, shown,
  reverted clean. The README's ruling (i) and the `wal.rs` doc now
  read `≥ b` exactly as the verdict (finding 4's backtick).

**Step 1 — the two halves (commit a2c2d979).** Per-round record fields
`q4sp` (splits) and `q4mg` (merges): the store's
`ckpt_splits`/`ckpt_merges` at the crash-time `obs` snapshot — reset at
each `ckpt:begin`, counting only while a checkpoint is open, so at the
crash they read the interrupted window's own counts. Serialized in
`campaign.json` (old files parse unchanged), never digest inputs,
never quota inputs; the JSON round-trip asserts them. **Probe, not
evidence** (`run --seeds 0..200 --threads 16`, output outside the
worktree, re-verified at this tip):

- **BASE:** 305 rounds, 84 in the Q4 window (crash at position code
  1–6), **7 with a split, 0 with a merge, 0 with both** (Q5a 3, Q7 5,
  Q18 20; failing rounds 2 — the pre-existing 8/178 pair; 72 s).
- **bursts 600/8:** 309 rounds, 87 in the window, **7 splits, 0
  merges, 0 both** (Q5a 4, Q7 10, Q18 47; 0 failing; 75 s) — matching
  the gate's re-measurement (600/8: 0 of 309; 900/16: 0 of 308).

**The cause of the rare in-window merge, found by single seeds** (the
`E4_SMO_DIAG`/`Q4DIAG` probes, since removed): **`w` held back — by
the chunked transaction's single atomic apply, not by readers — plus
the merge-capable sweep landing after the windows.** The burst's
deletes are durable in the WAL but sit above the apply frontier until
the chunked txn's marker syncs, and `gc_locked` elides (the step that
makes leaf pairs underfull — `count_merge`'s precondition, the GC's
merge condition) only below the boot watermark: the early sweeps read
`w` = 0–2 with the deletes durable but unapplied (seed 11's diag: ten
sweeps, all `w` ≤ 2, all merges 0; probe s1000b: 0 in-window merges in
43 rounds). When `w` does advance, later in the round, the sweeps'
merges land with no window open (seed 81's diag: 13 merges in the
round, every one at `ckpt_open=false`, the in-window counters 0). At
BASE the merge condition itself rarely has material — the 1-in-4
deletes scatter over random keys of 2–4 trees and seldom leave
adjacent underfull pairs — so BASE shows no merge half at all (0 of
84 window rounds).

**Step 2 — X5-R40, the SMO alignment (commit f9c4405d; a 5.2
amendment, pre-registered before every probe above).** `Tuning` gains
`smo_share` (permille, **0 in BASE and FINAL**); the independent
per-round stream `S_SMO` (`smo_pick`) draws only when the share is
non-zero, so BASE consumes no stream state and an unselected round's
script is bit-for-bit the pre-X5-R40 generator's. In a selected round
the GC task's front becomes `AwaitCkptOpen` (a spin-yield gate on
`ckpt_open`, the checkpointer's own poll pattern — no wait key is
woken at `ckpt:begin`, so a parked waiter could only observe a window
after it closed; a deterministic turn budget `AWAIT_CKPT_OPEN_TURNS`
1024 backstops the circular livelock where the spinner itself is the
last unfinished task, probe s1000a seeds 4/20) → half the burst's
sweeps → one split-seeding commit (≤ 20 Puts into a dense run of
consecutive fresh keys at the value ceiling of a tree *other* than the
burst tree, so it cannot refill the holes the sweeps merge) → the
other half → the base sweeps. Targeted rounds keep their hook and
uniform rounds keep their draw: the crash spec is untouched. Pins:
`x5_r40_smo_not_drawn_at_base` (the draw-count contract — the `S_SMO`
stream is independent, so no digest can see the draw),
`x5_r40_awaitckptopen_parks_until_a_window` (a gated commit must not
record while no window can open), `x5_r40_selected_round_digest_is_pinned`
(seed 0 under the literal SMO tuning, digest 3330201367) and the
card's named `x5_r40_aligned_round_has_split_and_merge_in_window`:
**seed 886 round 2** (crash `After2@2`), `q4_split_merge` true with
`q4sp` 2 / `q4mg` 1, digest 2522830599 — the pin-hunt hit (seeds
200..1000 at smo 1000 / burst 900/8). M-SMO-NOWAIT (the gate returns
at once) and M-SMO-NOMERGE (the aligned sweeps removed) both fail the
named pin and the digest pin; M-SMO-DRAW (`S_SMO` drawn at share 0)
fails the draw-count pin while
`x5_r37_base_tuning_reproduces_1b0ce54e` still passes — all three
applied, shown, reverted clean.

**Step 3 — the three settings (probe, not evidence).** Each is
`run --seeds 0..200 --threads 16` with only (`smo_share`, `burst_frac`,
`burst_gc_loops`) changed, then reverted; BASE re-measured the same
way, back-to-back on one host (72 s). "In window" = the crash at a
position code 1–6 hook; split/merge = `q4sp`/`q4mg` ≥ 1.

| setting (smo/burst/gc) | rounds | in window | split | merge | Q4 | Q5a | Q7 | Q18 | failing rounds | wall |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 / 600 / 8   | 313 | 87 | 10 | 2 | **0** | 2 | 4 | 184 | 1 (seed 151 r1, O-1(a)) | 85 s |
| 1000 / 900 / 16  | 306 | 83 | 12 | 2 | **0** | 5 | 5 | 176 | 1 (151 r1) + 1 twin (seed 9) | 94 s |
| 1000 / 900 / 8   | 309 | 85 | 8  | 4 | **0** | 3 | 3 | 180 | 2 (151 r1, 190 r1) | 88 s |

The alignment works as designed — each half now reaches windows (8–12
split rounds, 2–4 merge rounds per ~310) — but **never the same
round**: 0 rounds with both at every setting. The pin hunt at
1000/900/8 over seeds 200..1000 measured the joint rate: 1,239 rounds,
33 split rounds, 11 merge rounds, **exactly one with both** (seed 886
r2) ≈ 1.25 per 1,000 seeds ≈ 12–13 per 10k against the 1,000 required.
The two failing seeds (151 r1, 190 r1) are new under these
probe-only schedules (BASE keeps 8/178 alone); `defect` is none of
them — they are ordinary oracle failures on modified generators, and
the settings are reverted.

**Step 4 — the decision rule, applied verbatim (fixed before any
probe).** *Met* needs: Q4 × 50 ≥ 1,500, at wall ≤ +25% of BASE (72 s →
90 s), with no Q5a, Q7 or Q18 below the lower of its row and BASE's
own count × 50 on the same seeds (BASE: Q5a 3, Q7 5, Q18 20 — floors
150, 200, 1,000).
- Setting 1 (1000/600/8): Q4 0 → 0 × 50 = 0 ≥ 1,500 **false**
  (and Q5a 2 × 50 = 100 < the 150 floor).
- Setting 2 (1000/900/16): Q4 0 → **false** (and wall 94 s > 90 s).
- Setting 3 (1000/900/8): Q4 0 → **false** (and Q7 3 × 50 = 150 <
  the 200 floor).

**Not met by all three → ESCALATE with the numbers.** Q4's definition
and threshold are unchanged — no card, gate or orchestrator lowers or
redefines them. P takes the reachability question to the owner.

**The hand-off: none.** No setting meets the rule, so F4's REFRESH
gets no Q4 knob from this card. The X5-R40 machinery stays in the
tree, inert at BASE and FINAL (`smo_share` 0, no draw, no op
inserted), for whatever the owner decides.

## Card X-5c-F4-F1-F1-CXX's record (C-XX's two remaining causes: the DropTree straddle and the exhausted reserve's double evict)

QUEUE row X-5c-F3-F1-F2: 51 faultless class C rounds failing O-3(b)
XX001 in `X-5c-F4.acc.campaign.json` (16 `[Open]`, 35 `[AmcheckIi]`).
P classified all 51 on a clean export of a4c1f869 (the census,
`~/.cache/nv3-review/pxx/evidence/census-51.txt`; the per-seed class
table is reproduced below). Three causes:

| Cause | Rounds | Mechanism | Fixed by |
|---|---|---|---|
| **A** (spec) | 27, plus 3 with B1 | S-6.8's cut between the root free and the entry removal | this card, per S-1-F7 |
| **B1** (prototype) | 18, plus 3 with A | two exhausted first touches evict the same copy; the second clears the frame the first refilled | this card |
| **B2** (prototype) | 3 | a late copy outlives its checkpoint | -F2's install guard (the previous section) |

The 3 with both are 4784 (needs both fixes), 1612 and 9433 (either fix
alone moves the schedule off them). Probes over the 51 seeds
(single-seed runs, P's `pxx` toggles on scratch exports): A-fix alone
21 rounds still fail; B1-fix alone 31; -F2's guard alone 48 (it clears
exactly 5706, 8725, 8956); all three **0** — and this card's re-run of
the 54 seeds (51 + 190, 6195, 6990, the three the -F2 guard moved onto
B1) shows **0 failing rounds, no defect, deadlock or panic**.

**Census (class per seed, P's, verbatim from the evidence):** 27 `A
need:A`, 16 `B1 need:B1`, 2 `A/B1 need:either`, 2 `A/B1 need:B1`, 2
`B2 need:B2`, 1 `A/B1 need:A/B1` (4784), 1 `B2 need:either`.

**Cause B1's mechanism — a correction to the older "late copy" account
for B1's rounds.** The -F2 record above left the exhausted path's
*record* as its ESCALATE sibling; the draft's Cause B reading ("the
`ckpt:freecopies` clear destroys these copies") was wrong for most of
its rounds. `ckpt:freecopies` is not what destroys them: in
`record_image_copy`'s exhausted path, tasks T1 and T2 both read
`oldest = frame i` holding copy X and both proceed into `write_fresh`;
T1's `epoch_copy:record` records X and clears frame i, and T1's retry
installs copy Y there; T2's `epoch_copy:record` then clears frame i
**again**, with no re-validation after its yield — Y is destroyed
unwritten, `map.img[Y]` stays `CopyPending`, and the freeze maps Y to
nothing. 3534 r1: `exhaustion frame 0 no longer holds 16`, then
`FREEZE unresolved CopyPending lpids=[29] k=3` — an `[Open]`/`[AmcheckIi]`
XX001 with no injected fault. The same step has a **second, silent
outcome** (verdict X-5c-F4-F1-F1-F2's sibling): when the record lands
after its checkpoint's post-5 it inserts `Img::At(flip-k bytes)` into
the cleared map, nothing clears it, the next freeze takes it over
`cur`, and the next image loses the mutations between the flips with
no XX001. One re-validation closes both (Work 1).

**The two schedules, with traces.**

- **Pin 1, `drop_cut_never_names_a_freed_root` (310 r0's shape, seed
  1).** Phase A: trees 2 (a leaf root, one entry) and 4, one durable
  checkpoint. Phase B: t1 drops tree 2 — the walk's first step *is*
  the last (a leaf root), so at this tip the root frees inside the
  walk's gate hold and `dir_remove_entry` runs in its **own** hold; t2
  (the checkpointer) spins for the straddle and flips inside the gap.
  The image: `dropping tree2 -> root 1` with `free = {1}`. Crash,
  `ckpt::recover`: the resumed walk descends the unmapped root —
  `Corrupt("lpid 1 unmapped")` at `[Open]` (rule: I-LPID-EXACT /
  I-LINK-ATOMIC, S-6.8 as amended). The pin failed at the pre-fix tip
  and passes at this one.
- **Pin 2, `drop_cut_never_names_a_reused_root` (8010 r1's shape, seed
  77).** The same cut with t3 splitting tree 4 onto the freed LPID
  before the flip (S-4.2's immediate reuse: the freed root is the
  lowest free LPID; tree 4 holds eight 400-byte cells, t3's ninth goes
  in pre-positioned at the mark, and the tenth splits). The opened
  image reaches lpid 1 from trees 2 and 4 — rule: one page, two
  references — and the resumed walk frees tree 4's live child; its
  scan after recovery fails (`Corrupt`, the page gone). Under the fix
  the reuse happens only after the entry removal, so no image names a
  reused root.

**The fixes.** Work 1 (B1): `epoch_copy:record` re-validates its
reserve frame after the write-out's yield, in the record's own atomic
step — if the frame no longer holds `(key, is_epoch_copy)` another
task resolved it, so the write is an orphan: its slot leaves
`used_cur` and neither the image map nor the frame changes
(`PR-EXHAUST-REVALIDATE`). Work 2 (A, per S-1-F7's amended S-6.8): a
walk step reads first and frees nothing until it knows the topmost
page it empties; the last step, in its one gate hold, removes the
directory entry and the in-memory record **first**, then frees the
path top-down root first — the two mutexes never nest, the gate is
never taken twice. Work 3 (S-14 step 4 as amended): pass 2's
`DropTree` sets `dropping` only (`dir_drop_tree_replay`); the walk is
step 6's; the live apply is unchanged.

**The pins and their mutants** (each mutant applied to the real code,
shown failing, reverted clean):

| Mutant | Failing pins |
|---|---|
| M-DROP-ENTRY-SPLIT (the entry removal in its own gate hold — today's code; also a `mutants`-table row) | `drop_cut_never_names_a_freed_root` ("recover: Corrupt(lpid 1 unmapped)"), `drop_cut_never_names_a_reused_root` (tree 4's scan Corrupt on the freed page) |
| M-EXHAUST-NO-REVALIDATE (`epoch_copy:record` clears unconditionally — today's code; also a row) | `exhausted_reserve_keeps_both_copies` (a `CopyPending` at `ckpt:freeze`), `exhausted_record_stays_out_of_the_next_window` (image 2 maps P to its flip-1 leaf) |
| M-EXHAUST-RECORD-FIRST (the clear guarded, `record_image_at` unconditional) | `exhausted_record_stays_out_of_the_next_window` |
| M-REPLAY-DROP-WALK (pass 2 walks) | `drop_replay_only_marks` (the freed root mapped again after recovery) |
| M-DROP-FREE-FIRST (inside the one hold, the root freed before the entry is removed) | `drop_entry_gone_before_root_free` (the watcher sees an entry name an unmapped root) |

Plus two more directed pins: `exhausted_reserve_keeps_both_copies`
itself (3534 r1's shape — one frame, two first touches in
`write_fresh`; no `CopyPending` at `ckpt:freeze`, both LPIDs in the
image), and `drop_entry_gone_before_root_free`'s watcher (at each
atomic step, no entry — persisted or in `dir` — names an unmapped
root; the in-memory record outlives the persisted one, so one check
covers both).

**Digest movement (Work 5).** The 54 seeds re-run one seed per process
under `ulimit -v 25165824`: 0 failing rounds. X5-R37's literals moved
at 9 seeds — 66 `123257877 -> 3628190779`, 112 `2157138087 ->
2438489582`, 198 `649857276 -> 95622311`, 303 `859193035 ->
4147169426`, 907 `2332284720 -> 1749572610`, 1769 `2372375821 ->
1311732889`, 6149 `2623005576 -> 123186068`, 7469 `3635220225 ->
3658720859`, 7528 `1612546402 -> 122281874` (the drop rework
restructured the walk's steps, so every drop-bearing schedule
re-derived; each value is this card's own measurement).
`late_copy_count_is_zero_at_base` moved at one seed: 190
`4013865743 -> 679383372` — the -F2 verdict's probe value under Work 1
alone, now with all three fixes in.

**The rework's re-measured attribution (rework step 4; the REWORK
verdict's finding 6, whose per-step table this now matches).** Each
move of 1b0ce54e's baseline has exactly one first owner, and the
rework's pin hold (step 1) is a fourth:

| First moves at | Seeds |
|---|---|
| Work 1 (B1) | 190 |
| Work 2 (A) | 66, 907, 1769, 6149, 7469, 7528 |
| Work 3 (replay) | 112, 198, 303 |
| Rework step 1 (the pin hold) | 112, 1769, 7469 |

The rework's three are second moves: 112 `2438489582 -> 3003462667`,
1769 `1311732889 -> 2372375821` (back to 1b0ce54e's own value — the
pin hold moved the schedule off the drop-arm change and onto the base
draw), 7469 `3658720859 -> 1670717970` (the seed the verdict's trial
flagged: its twin round 1's re-derived spec `Mid2@1` never fires —
the NEVER-FIRED shape on a moved schedule; the re-gate reads it).
Pass/fail does not change on any of the three (all rounds pass; 112's
R36 pin and 1769's no-twin pin still hold), and
`late_copy_count_is_zero_at_base`'s four digests did not move again
(96, 104, 161, 190 re-measured identical). Attempt 1's "every
drop-bearing schedule re-derived" was loose, as the verdict said: of
the ten, three first moved at the replay change and one at B1, not
the drop arms — as the table above itself shows (the "nine/four"
count was the verdict's finding 4; corrected by card
X-5c-F4-F1-F1-CXX-F1's step 7).

**The rework (P gate verdict REWORK 10567da6, 2026-11-06; finding 1,
blocking).** Attempt 1's last step dropped its `PinGuard` at the walk
step's edge, before the directory descent: unpinned, the root's dirty
frame was a legal eviction victim during that I/O — the evictor
latches it and yields in `write_fresh`, `free_lpid` clears the frame
and puts the LPID in the free set, and `evict:finish` (whose
`map.cur.insert` is unconditional) resurrects the freed root at its
write's fresh slot. Mapped and in the free set, referenced by
nothing; amcheck (ii) reports "mapped lpid N unreachable". Two new
faultless class C XX001 at BASE — **444 r1** (`LAST tree 3 path
[4, 63]`, slot 109) and **3005 r1** (`LAST tree 4 path [5, 75]`,
slot 132) — are the **census's blind spot**: neither is in the
51-round file nor the 54 seeds, which is why Work 5 read 0 while the
10k carried them; S-1-F7's trial range and 50000..50200 miss them
too. The fix (rework 1): the guard stays live across the whole one
gate hold, through the frees; every other arm unchanged, and
`evict:finish` untouched (Limit 1 does not name it — §6.8 is the
rule). The gate's own trial with the pins held read C-XX 0 over
0..10000, and one defect: 7469's twin round 1 (`Mid2@1` never fires,
the NEVER-FIRED shape) — the re-gate reads that seed; at this tip its
digest moved again (above) and `r21c`'s literal moved to 9469 (the
judgment call below). The rework's new directed evidence:

| Mutant (hand-applied, shown failing, reverted clean) | Failing pin |
|---|---|
| M-DROP-UNPIN-EARLY (the `PinGuard` dropped at the step's edge — f703c08d) | `drop_last_step_keeps_its_pages_pinned` (seed 38: "root 2 is mapped and in the free set"; 38, 85, 444, 467 of 1..=500 at f703c08d, 0 held) |
| G3-FREE-IN-SECOND-HOLD (the gate released and re-taken between the entry removal and the frees) | `drop_one_hold_the_flip_never_finds_the_straddle` (seed 5: "the flip found the straddle: entry absent, root 2 mapped, gate free"; 105 of 1..=200 under the mutant, 0 clean) |
| G2-ORPHAN-KEEPS-SLOT (the orphan's `used_cur` release removed) | `exhausted_reserve_keeps_both_copies` ("seed 1: slots [13] entered used_cur referenced by nothing") |
| G-W11-NORETIRE (the `recycled_unsynced` retain removed) | `x5_r38_w11_needs_no_completed_sync_since` (seed 30 reads `w11-` clean, `w11` under the mutant) |

Finding 3's two rules now have directed pins: the one-hold pin above
(a flip that waits for "entry absent, root mapped" must never find it
— under the fix the straddle exists only inside the hold, so the
check can never pass), and pin 3's world-end orphan assertion (the
used-but-unreferenced slots are exactly the phase-A baseline plus the
one stale flip-window write-out, slot 5 at seed 1, measured and
documented in the pin). Finding 4: pin 2's assertion is eight cells,
matching its comment. **The r21c literal (a judgment call, recorded
in the RESULT):** the pin hold moved 7469 onto the NEVER-FIRED shape
— the fired-guard defect is by design a seed failure, so the ruling's
fifth literal could not stay; it moved to 9469 under the gate's
Limit-3 ruling class (a schedule literal of the X5-R37 kind; the
assertions unchanged), measured: class MW, twin round 0's re-derive
**succeeds**, real round 1 re-derives its spec (the R21 (c) re-pair
path 7469 was pinned for), no defect, every twin paired. The W11 fallback literal
(`x5_r38_w11_needs_no_completed_sync_since`) moved from seed 30 to
seed 11 — a Limit-3 deviation recorded in the card's RESULT (the pin
is a schedule literal of the X5-R37 class that Limit 3 did not name;
the invariant and the assertion are unchanged).

**The rework's correction to that W11 move (the REWORK verdict's
finding 2, binding):** seed 11 was vacuous — it reads `w11-` with the
retain line in **and** with it removed (no boundary to retire), so the
pin held nothing. The rework's pin hold moved the schedule again and
the literal is back at **seed 30** (measured at the rework tip, share
1000: seed 30 reads `w11-` clean and `w11` under G-W11-NORETIRE, shown
failing; the other flippers of 0..200 are 37, 98, 111, 138, 156, 170
and 192, and 51 uniform-mode seeds read `w11-` clean).

**A correction to -F2's record above:** its "no seed fails before or
after" is false — at -F2's tip, 190 r1 fails and passed before (the
verdict's finding 1). B1 was already there; the -F2 guard moved the
schedule onto it. This card clears it (190 passes above).

**What did not change:** the oracle, the model, the sim, K, R, the
generators, every quota's definition and threshold, `verify`'s exit
rule, `ckpt2:scan`'s condition, `resume_drops`' error handling,
DROP-ROOT-FIRST, and `docs/` (S-1-F7 carries the spec text; it is not
merged into this branch's `docs/`, which this card may not edit).

## Card X-5c-F4-F1-F1-F2's record (finding 1: the late epoch copy)

Verdict X-5c-F4-F1-F1, finding 1, confirmed by a literal schedule and
fixed: a first touch's epoch copy can still be on its way (the only
yield between the freeze and the install is the exhausted reserve
path's write-out of the oldest copy) while step 2 writes the page's
normal frame — the parked mutator is the one that would have replaced
its bytes — and resolves `Img::CopyPending` to `Img::At`; then
`ckpt:freecopies` frees the reserve and the checkpoint completes. The
install then lands **late**: a dirty, `is_epoch_copy` frame with
`f.epoch = e − 1` past its own checkpoint's cleanup. The next
checkpoint's scan accepts it (`f.is_epoch_copy || f.epoch <= k`) and
its record path maps the LPID to epoch-`k` content while `T` says
replay need not restore it — a silent class-C loss, no injected fault
(the gate's trace: seed 845 r0 under the X5-R41 prototype).

**The fix (step 1, one atomic step in `epoch_copy:install`):** install
only while `map.img[lpid]` is still `Img::CopyPending`; otherwise drop
the bytes — the image's content is already recorded (step 2's write, a
previous-epoch eviction's `At`, or a closed window). This is S-STORE
§5.6's copy condition ("copies only if that content is not yet at a
slot the image map references") **re-tested after the wait**, exactly
as P read it. Invariant `PR-LATECOPY` added to `prims.rs`'s header. A
new named park point, `epochcopy:prewrite`, holds a copy in the
exhausted path for the directed pins (no budget set: returns at once).

**The literal schedule (step 0,
`late_epoch_copy_must_not_enter_the_next_image`)** — twelve one-page
trees, `R_EPOCH = 1`: the first `Mid2` hook performs P's first touch
and fills the reserve deterministically; the mutator task's touch of B
(the highest frame, above ten padding trees) parks in the exhausted
path; step 2 writes P's copy (the fixpoint re-scan) and B's normal
frame (resolving `CopyPending` → `At(slot 17)`); the `After2` hook —
past `ckpt:freecopies` — releases the parked copy, which installs late
(img `At`, past freecopies, dirty, `is_epoch_copy`, epoch 1); B's frame
is evicted between the checkpoints so checkpoint 2's scan cannot
overwrite the leaked entry; a crash after checkpoint 2's meta; the
reopen from meta 2 read the leaked copy's flip-1 leaf (slot 24, the
flip-1 crc) and **lost the window-1 key** — the pin failed at the
pre-fix tip exactly as the card demands, with the path matching the
Goal's reading (recorded by a trace before the fix). The pin also
reopens from meta 1 (reverted): the flip-1 cut must hold one entry per
tree, which is the half that catches an over-eager drop.

**The freed-LPID pin (`freed_lpids_late_copy_never_remaps`)** — the
same park holds a raw `store_page` (not a tree insert: an insert holds
the epoch gate across the parked wait and the next flip cannot start —
the first draft deadlocked the flip against the park until the budget
self-drained) across two flips; checkpoint 1 completes, B's LPID is
freed between the checkpoints, and window 2 opens with the map empty
for B. The real guard drops the copy (an absent entry is not
`CopyPending`); the reopened store's `cur` has no entry for B and the
free set holds it. Under **M-LATECOPY-ATONLY** (the guard testing
`Img::At` only) the copy installs onto the absent entry and
checkpoint 2's scan records it — `img[B] = At(slot 33)`, the flip-1
crc — the pin fails.

**Late and stale counts at BASE (step 2), seeds 0..200, measured with
the gate prototype's `LATECOPY`/`STALECOPY` counters (env-gated, on
`git archive` exports of the pre-fix base `a4c1f869` and the post-fix
tip; `e4-campaign run --seeds 0..200 --threads 16` under
`ulimit -v 25165824`):**

| | LATECOPY (late installs → drops) | STALECOPY (a stale copy reaching a later scan) |
|---|---|---|
| pre-fix (a4c1f869) | 7 | 0 |
| post-fix (this tip) | 7 | **0** |

The seven events sit on seeds **96 (×2), 104 (×1), 161 (×1), 190
(×3)** (located by single-seed runs). Post-fix the same seven installs
are drops — no stale copy reaches a scan over 0..200, so **the
`ckpt2:scan` guard is not needed** and the scan condition is
untouched. (The verdict's 0..1000 probe counted 34 late installs and 7
stale copies pre-fix; my 0..200 slice shows 7 and 0 — consistent, the
leak-into-scan is the rarer tail.) No seed fails before or after (the
runner's fold: 2 vs 3 *failing rounds*, both known classes on moved
schedules; the per-seed records pass).

**Digest movement (step 4).** Over 0..200 the digests that move are
**96** (`83795711 → 4266526636`) and **190** (`813405985 →
4013865743`) — the drop is the only difference. Seeds 104 and 161 hit
the path too but their late installs were observably benign pre-fix
(same digest both sides). **No seed in X5-R37's literal list moves**:
the whole 54-seed list re-ran green at the tip twice — once as the
in-suite pin (`x5_r37_base_tuning_reproduces_1b0ce54e`, all 55
assertions) and once as an independent campaign-binary pass over the
same seeds (chunks 200..1000 plus singles through 9809, digests
compared against the literals) — so no literal changed and F4's new
baseline is the old one. `late_copy_count_is_zero_at_base` (step 3)
pins the four hitting seeds' post-fix digests; the count itself is not
observable inside this card's Limit 1 (a store counter would touch
`StoreInner`), which the pin's doc states.

**Mutation evidence** (each applied to the real code, shown failing,
reverted clean — `git diff` empty after each):

| Mutant | Failing pins |
|---|---|
| M-LATECOPY-INSTALL (the guard removed) | `late_epoch_copy_must_not_enter_the_next_image` (missing key), `late_copy_count_is_zero_at_base` (seed 96's digest reverts) |
| M-LATECOPY-DROPALWAYS (every copy dropped) | `late_epoch_copy_must_not_enter_the_next_image` ("scan P after meta 1: lpid 12 unmapped" — the unresolved `CopyPending`), `step2_fixpoint_writes_late_epoch_copies` ("scan B after reopen: lpid 3 unmapped") |
| M-LATECOPY-ATONLY (the guard tests `Img::At` only) | `freed_lpids_late_copy_never_remaps` (the freed LPID remapped) |

**ESCALATE (a sibling this card's prescribed fix does not cover).**
The exhausted path's own **record** for the oldest copy has the same
late-landing shape: `record_image_at(key, r)` runs right after the
park's write-out, and when the wait crossed its checkpoint's post-5
(the map cleared), it inserts a stale `Img::At` — the oldest copy's
flip-`k` content — into the *next* window's map. Nothing clears it
(the flip does not clear `map.img`), the next freeze lets it override
`cur`, and the next image maps that LPID to flip-`k` content while the
mutations between the flips (committed, ts ≤ T) are not replayed: the
same silent class-C loss, through the record rather than the install.
A deterministic reproduction at this (fixed) tip — preserved at
`~/.cache/nv3-review/x-5c-F4-F1-F1-F2/sibling_stale_record_repro.rs`
(scratch, never committed; a self-contained `#[test]` built on the
committed pin's world) — drives the same park across post-5, releases in
the post-5→next-flip gap, evicts the oldest copy's LPID between the
checkpoints (so the next scan cannot self-heal the entry by re-writing
the frame), and reopens after the second checkpoint's meta: image 2
maps P to `Ref { slot: 32, epoch: 1 }` — the flip-1 leaf, crc
4182374088 — and P's scan loses the window-1 key. The committed
`freed_lpids_late_copy_never_remaps` pin rides on the same world with
its release inside window 2, where the record's stale insert is
unobserved (its doc says so); a guard at the record (the same
`CopyPending` re-test, or a window re-test) would close it, but that
is outside this card's prescribed fix ("at `epoch_copy:install`") and
Limit 1's letter leaves the decision to P.

**What did not change:** the oracle, the model, the sim, K, R, the
generators, every quota, `verify`'s exit rule, `ckpt2:scan`'s
condition, and `docs/` (P decides at this card's gate whether S-STORE
§5.6 needs its "re-test the copy condition after any wait on the
reserve" sentence, as its own Class: spec card).

## Card X-5c-F4-F1-F1-O1C's record (the three faultless class C
## O-1(c) rounds with no owner: the recovered frontier above the
## model's upper bound)

Diagnosis only, per the card's Limits: **no fix** (the oracle, the
model, `upper_bound`, the sim, K, R, the generators, every quota and
the store are untouched; `docs/`, `cards/` and `verdicts/` untouched).
The one code change is the diagnostic the card prescribes:
**`E4_O1C_TRACE`** (stderr only, default off, never a digest input,
verified below) — the harness side prints the trace of any round whose
`frontier > model.upper_bound(lower_bound(c_prev))` (the exact bounds
`check_round` derives; `hi` is class-independent in the oracle), and
`ckpt::recover`'s walk prints its own accepted-record list beside the
opened meta's bounds (`O1C walk:` lines), so the replayed side and the
event side can be matched record for record. The harness side also
re-runs `derive_written_acked`'s stitched-write walk step for step
(`o1c_stitched_walk`) with a per-inode path log, so a record the
`written` half missed is located on the walk's own path.

**The three rounds, each reproduced twice** (single seeds under
`ulimit -v 25165824`, output outside the worktree; digests identical
across the two runs and equal to `X-5c-F4.acc.campaign.json`'s and P's
`o_base.json`/`o_allf.json` — the rounds are unchanged at this tip):

| Seed/round | crash | lo | hi | frontier | digest (×2) | o1 |
|---|---|---|---|---|---|---|
| 1066 r0 | `Mid2@2` | 20 | 20 | **21** | 1383263257 | `O-1(c) at c=20, tree 2: raw entry above the cut (max ts 21)` |
| 6555 r0 | `uniform@y:11624` | 22 | 23 | **24** | 3687676066 | `O-1(c) at c=22, tree 2: raw entry above the cut (max ts 24)` |
| 7590 r0 | `uniform@y:10872` | 19 | 20 | **21** | 3340396508 | `O-1(c) at c=19, tree 2: raw entry above the cut (max ts 21)` |

All three are round 0, faultless (`lost/mis/torn` 0), no nested
recovery crashes, `served=false`, amcheck clean — REFRESH 8's "a class
C O-1 **with no XX001**" shape, escalated below.

**The trace, one mechanism in all three.** Each round's chunked
transaction fills segment 0 early (writes spanning tens of KiB of the
file); a mid-round checkpoint recycles segments 0 and 1 (§13 step 6 —
renamed to segments 3 and 4, each rename `sync_dir`'d); the renamed
file then receives the round's last commits as its **new life**. The
record above `hi` is always the round's last commit, its bytes written
and synced before the crash inside the new life — and the harness's
`written` derivation never sees them:

- **1066 r0.** The old life of inode 2 (created as segment 0 at E[0])
  wrote `E[31] W+ f2@206+10691` — file bytes [206, 10897). The recycle
  `rename /wal/0000000000000000.log -> /wal/0000000000030000.log`
  lands at E[1685] (dir-synced E[1687]). The new life's single batch
  `E[1806] W+ f2@0+5046` / `E[1807] W=` (returned), sync `E[1812]/E[1813]`,
  with `|E| = 1819` — **the write returned 12 events before the crash,
  the sync 6 before** (the crash is `Mid2@2`, the checkpointer's
  step-2 write hook). The batch holds ts 20's record [0, 2453) and
  **ts 21's record [2453, 5046) — `Commit`, txn (0,19), lsn 199061,
  len 2593**. The model recorded ts 21 at the enqueue's `h:model` step
  (`owner_in_model=true`, mode `SyncOff`); the derivation's stitched
  walk decodes ts 20 at off 0 (the new write's key 0 replaced the old
  life's key-0 write, and the whole [0,2453) fetch is one chunk), then
  at **off 2453 `stitched` picks the greatest map key ≤ 2453 — the old
  life's key 206 (issued E[31]), which still covers 2453 — over the
  later-issued new write at key 0** (issued E[1806]). The fetched
  header is the old segment-0 body: magic fails, the walk jumps to
  10897, and **ts 21's record never enters `ts_pos`** → `written=false`
  → `hi = 20`. The walk path, verbatim:
  `off=0: rec ts=20 … end=2453`, `off=2453: undecodable, jump to
  10897`, then the old life's chunks to 58239, stop.
- **6555 r0.** Old life write `E[58] W+ f2@24454+30070` ([24454,
  54524)); recycle at E[1689] (dir-sync E[1692]); the new life's last
  batch `E[2269] W+ f2@22864+5037` / `E[2270] W=`, sync
  `E[2271]/E[2272]`, `|E| = 2301` (**write returned 31 events before
  the crash, sync 29**). The batch holds ts 23's record [22864,
  24895) and **ts 24's record [24895, 27901) — `Commit`, txn (0,24),
  lsn 221503, len 3006**; the model holds ts 24 (`SyncOff`). The walk
  decodes the new life's records up to ts 23 (each record's start is
  its own write's key: 17919, 21164, 22864), then at **off 24895 the
  greatest covering key ≤ 24895 is the old write's 24454** (issued
  E[58], 2200 events earlier) — magic fails, jump to 54524 (the old
  life's chunks), and ts 24 never enters `ts_pos` → `hi = 23`.
- **7590 r0.** Old life write `E[16] W+ f2@2379+29226` ([2379,
  31605)); recycle at E[1770] (dir-sync E[1774]); the new life's batch
  `E[1981] W+ f2@2120+4408` / `E[1982] W=`, syncs `E[1983]/E[1984]`
  and `E[1985]/E[1986]`, `|E| = 2084` (**write returned 102 events
  before the crash, the first sync 100**). The batch holds ts 20's
  record [2120, 4994) and **ts 21's record [4994, 6528) — `Commit`,
  txn (0,20), lsn 201602, len 1534**; the model holds ts 21
  (`SyncOff`). The walk decodes ts 19 at 0 and ts 20 at 2120 (the new
  write's own key), then at **off 4994 the greatest covering key ≤
  4994 is the old write's 2379** (issued E[16], ~1965 events earlier)
  — magic fails, jump to 31605, and ts 21 never enters `ts_pos` →
  `hi = 20`.

**The store's side, all three.** `ckpt::recover`'s walk reads the
renamed file's **current** view (`read_at` — later writes overwrite
earlier ones in the disk), so it decodes the new life's records
correctly and stops where the old bytes begin: 1066 replays ts 11–21
(`valid_end = 201654` = ts 21's end), 6555 ts 17–24 (`valid_end =
224509`), 7590 ts 19–21 (`valid_end = 203136`). The frontier equals
the last replayed ts in each — the records' bytes are durable (X5-R34's
`D` covers each record's end exactly; `D`'s own stop reason names the
same old write and the recycle binding: "the last covering write (ev
31, [206,10897)) precedes the durable binding (ev 1685) — stale bytes
of an earlier segment name"), the recycle is consistent with §12.6's
rule (segments 0 and 1 lie wholly below the opened meta's
`wal_start_lsn`: 179837 / 203646 / 196608), and the replays are
ts-ordered with LSN (I-WAL-TS-ORDER holds across every walk record).

**Work 2's answer.** In all three rounds the replayed commit is **a
commit the model should have counted as WAL-written** — not a commit
that should not be in the WAL. Evidence per card Work 1, holding for
all three: the commit was enqueued and the model recorded it before
the crash (`owner_in_model=true`; `h:model` runs the step after
`wal_enqueue` returns, so the recording itself dates the enqueue
before the crash); the record's bytes were **written and synced
before the crash** (the write and sync events with their returns sit
inside `E`, 6–102 events before its end — the exact indices above);
the bytes are durable by X5-R34's own rules (`D` ends exactly at the
record's end); and recovery replayed them per §12.5/§14, which is
what a correct store must do with a durable record. The commit is not
spurious, stale or mis-positioned: the record decodes at its own lsn
under the file's current view, its boot id is this round's boot 0,
and its ts is the largest in the stream (a previous life's stale bytes
would carry an older lsn and fail §12.5's position rule — the walk's
stop at the old bytes shows that rule working).

**Which side is at fault under S-STORE §14's oracle text.** **Neither
`upper_bound` nor the store.** §14's oracle bounds `c` by "the last
WAL-written commit" — a property of the durable WAL, which in these
three rounds **includes** the replayed ts (its write and sync
returned before the crash; `D` covers it). The store's replay is
exactly §14 step 4's rule applied to a durable, correctly-positioned
record, and the frontier is §14's "largest replayed `commit_ts`". The
defect is in the **harness accounting that feeds `hi`**:
`derive_written_acked`'s written half builds a per-inode,
per-offset-latest-issued-bytes map (`files.insert(off, …)`) and
`stitched` resolves a byte by the **greatest map key ≤ the cursor**,
ignoring issue order — so when a recycled file's earlier-life write
starts at a key between the new life's write key and the new record's
start, and still covers that start, the **old** bytes shadow the
**new** record (the exact masking family the `E4_ACK_DIAG`
cross-check's escalation already recorded for `acked`, X-5c-F3-F1
step 1 (e) shape 2 — there the same per-offset map hid an original
record behind a recycled life's bytes; here it hides the new life's
record behind the old life's write, and the victim is `written`, the
`hi` input). `upper_bound` itself faithfully reports
`max{ts : recorded ∧ written}` over what it is given; the derivation
never marked ts 21/24/21 written, so `hi` sits one commit below the
true last WAL-written commit, the candidate set excludes the
frontier, and O-1(c) fails at every candidate. A fix (resolving
`stitched`'s cover by issue order, or keying the map by interval
supersession, as X5-R34's per-NAME view with its β binding-index
already does correctly for `acked`) is P's ruling — this card's
Limit 1 forbids it, and the oracle and model are frozen.

**What did not change:** the oracle, the model, `upper_bound`, the
sim, K, R, the generators, every quota, the store, `docs/`, `cards/`,
`verdicts/`. The diagnostic is default-off and moves no digest
(verified: seeds 1066/6555/7590 produce identical digests with
`E4_O1C_TRACE` on and off, and both trace runs are byte-identical in
stderr). Artifacts: `/tmp/opencode` is not durable, so the trace
stderrs live beside the runner's logs at
`$HOME/.cache/nv3-review/runlogs/X-5c-F4-F1-F1-O1C.*` (copied there
by this card's commands: `t<seed>.{a,b}.{err,json}`).

## Card X-5c-F4-F1-F1-O1C-F1's record (X5-R47: `written` by replay in issue order)

## X5-R47 (P ruling, verbatim, fixed before this card's code; any change is an ESCALATE)

Copied verbatim from the card before any code of this card (the
step-0 discipline, as X5-R6–R46 before it). It amends X5-R34's
"Unchanged: `written`" and supersedes the O1C record's diagnosis
above: the fix it prescribes replaces the `written` half and the F1
report of `derive_written_acked`.

**X5-R47 (amends X5-R34's "Unchanged: `written`").** A commit `(ts, lsn, len)` of `wal_commits` is **written** iff, at some event of `E`, immediately after a `WriteIssued` is applied in `E`'s order to a per-inode byte image, some inode's image holds at `[lsn mod WAL_SEG, lsn mod WAL_SEG + len)` a record that `dec_record` accepts with its own `lsn` and with `rec.ts == ts` and kind `Commit` or `CommitMarker`.
- **Monotone.** Once written, always written: a later life overwriting the bytes does not unwrite the commit. The test is never made against the final image alone.
- **No resolver by key.** No per-offset map and no greatest-key cover take part. Later bytes replace earlier bytes because they were issued later.
- **Issued is returned.** `SimDisk::write_at` emits `WriteIssued` and `WriteReturned` in one step, so no crash separates them. The old `returned_end` test is removed. Add a debug-free guard: a `WriteIssued` in `E` without its immediately following `WriteReturned` fails the round through `task_error`.
- **F1.** A commit is reported in `unparsed_written` iff it is not written and, at some such point, an image holds at `lsn mod WAL_SEG` 24 header bytes with correct magic, its own embedded `lsn`, and `lsn mod WAL_SEG + len_in_header > WAL_SEG`.
- **SyncOff's acked** stays "iff written". SyncOn's acked half is untouched.
- **Rejected forms.** A final-image walk, an interval-superseded view, and resolving `stitched`'s cover by issue order over the whole history all drop earlier-life records (measured: 1,028 of 19,118 rounds).

**Step 0 — the diagnosis brought in.** `git cherry-pick c13299c0
82062a02`, card X-5c-F4-F1-F1-O1C's two commits (not its verdict),
clean on this base.

**Steps 2–3 — the implementation (commit a3b64979).**
`derive_written_acked`'s `written` half and F1 report are now one
pass over `E`: each `WriteIssued` is applied in `E`'s order to its
inode's byte image (`r47_apply_write`), and immediately after the
application the just-written image is tested — `r47_image_holds` for
`written` (the record at `[lsn mod WAL_SEG, +len)` decoding with its
own lsn, `rec.ts == ts`, kind `Commit`/`CommitMarker`, inside its
segment), `r47_header_runs_past_segment` for F1 (the 24 header bytes,
correct magic, its own embedded lsn, `len_in_header` past the segment
end). Each write is tested only against commits whose record overlaps
it (the union window `[pos, pos + max(len, 24))`; the geometry
hoisted out of the loop), and both verdict sets are monotone — a
point that qualified at some event of `E` is never re-tested and
never unqualified. **Issued is returned:** the `returned_end` extent
test is removed and `r47_issue_return_paired` (always on,
debug-free) takes its place in `run_round`'s `task_error` cascade —
a `WriteIssued` whose immediately following event is not its own
`WriteReturned` (same inode, offset, length) fails the round. The
SyncOn acked half, `D`, `Rc`, the oracle, the model, the sim, K, R,
the generators, the quotas and the store are untouched. The
diagnostics: `o1c_stitched_walk` and its per-inode path log are
deleted (they mirrored a reader that no longer exists), as are
`issued_header_len`, `stitched` and the `FileWrites` alias (no
remaining readers); `ack_record_present` replays the same way and its
doc now names the derivation itself, never a mirror; the false "the
walk sees exactly the latest life" doc is gone; `o1c_frontier_trace`
keeps its frontier-above-`hi` shape but computes its derivation rows
by **calling `derive_written_acked` directly** (the trace can no
longer drift from the derivation), and its inode histories come from
the durable bindings of above-`hi` commits.

**Step 4 — the pins (commit d8f9f7a0).** Seven, hand-built events
unless stated, beside the x5r34 self-tests in `tests/harness.rs`:

- `r47_new_life_record_behind_old_write` — 1066's literal shape (old
  write `[206, 10897)`, new write `[0, 5046)` holding records at 0
  and 2453): both written, both acked (SyncOff), no F1.
- `r47_earlier_life_record_stays_written` — a record overwritten
  wholesale by a later life stays written (monotone).
- `r47_never_issued_stays_unwritten` — no bytes issued, and a last
  byte never issued (a torn tail), stay unwritten and unreported;
  a whole record is the positive control.
- `r47_wrong_record_at_position_stays_unwritten` — a valid record at
  the position with another lsn (the §12.5 stale rule) and one with
  the same lsn but another ts; a correct record is the control.
- `r47_f1_on_recycled_file` — a past-segment header behind an
  old-life write (the new batch's key below the header, the old
  write's key between the two) is reported and never written.
- `r47_pairing_guard_names_an_unpaired_write` — a paired stream
  passes `r47_issue_return_paired`; a missing return and a
  different-length return are both named (the guard is pub for the
  pin, as the X5-R34 guards are — a pin beyond the card's named six,
  judgment call: the guard is new judged code and deserves its own).
- `r47_three_rounds` — seeds 1066, 6555, 7590 under
  `Tuning::BASE`: r0 passes with `(lo, hi, c, frontier)` =
  (20, 21, 21, 21), (22, 24, 24, 24), (19, 21, 21, 21) — **the
  F2-tip values the card quotes, unmoved by CXX** (this tip's
  pre-change values were (20, 20, —, 21) fail, (22, 23, —, 24) fail,
  (19, 20, —, 21) fail); 6555's rounds 1–2 continue passing.

**Step 5 — the mutants** (each applied to the real code, shown
failing its pin, reverted completely — `git diff` clean after every
revert):

| Mutant | Failing pin (measured) |
|---|---|
| M-R47-KEY-COVER (the old greatest-key resolver: per-offset map, each byte by the greatest key ≤ the cursor, tested once over the history) | `r47_new_life_record_behind_old_write` — `left: [20] right: [20, 21]`: ts 21's record masked by the old write's key 206 |
| M-R47-FINAL-IMAGE (each commit tested against the final images only) | `r47_earlier_life_record_stays_written` — `left: [] right: [9]`: the overwritten record unwritten |
| M-R47-ALL-WRITTEN (every commit written) | `r47_never_issued_stays_unwritten` — `left: [30, 31, 32] right: [30]` |
| M-R47-NO-TS (`rec.ts == ts` dropped) | `r47_wrong_record_at_position_stays_unwritten` — `left: [40, 42] right: [40]`: the same-lsn record with another ts counts (the stale-lsn case stays unwritten through `dec_record`'s own lsn check) |
| M-R47-NO-F1 (the header-past-end set never recorded) | `r47_f1_on_recycled_file` — `left: [] right: [51]` |

**Step 6 — digest movement** (single seeds, one process each, under
`ulimit -v 25165824`, output outside the worktree; pre = this tip
without the change — the cherry-picked diagnosis commits, post = the
X5-R47 commits):

| Seed | pre digest | post digest | verdict |
|---|---|---|---|
| 1066 r0 | 1383263257 | 423674518 | **fail → pass** (O-1(c) cleared; (lo, hi, c, frontier) (20, 20, —, 21) → (20, 21, 21, 21)) |
| 6555 r0 | 3687676066 | 3744559595 | **fail → pass** ((22, 23, —, 24) → (22, 24, 24, 24); r1, r2 keep passing) |
| 7590 r0 | 3340396508 | 3293778794 | **fail → pass** ((19, 20, —, 21) → (19, 21, 21, 21)) |
| 8852 r0 | 564931007 | 564931007 | **no move** (see below) |

The X5-R37 literal list (54 seeds, run singly before and after):
**zero digests moved** — the pin `x5_r37_base_tuning_reproduces_1b0ce54e`
holds byte for byte at this tip. The contiguous `run --seeds 0..200
--threads 16`: **zero digest moves, zero verdict changes** (the
failing rounds stay exactly the known 8/178 pair, `DEFECT_SEEDS=0`),
so no literal was re-derived — no pinned seed's digest moved, and
X5-R47 explains every movement there was.

**8852, reported as measured (a judgment call).** The card expects
8852's "only change `hi` 32 to 33" (the O1C gate's F2-tip
measurement: a SyncOn record issued 2 events before the crash, not
durable, `D` below its end, the frontier staying put). At **this**
tip 8852 r0 runs a different schedule — CXX moved the seed's rounds
(the O1C verdict itself warned "CXX and X5-R41 change the rounds") —
and reads `(lo, hi, c, frontier)` = (8, 8, 8, 8), passing, with **no
replay/walk divergence at all**: its digest is byte-identical before
and after (564931007). No verdict changed in either direction on
8852, so the card's stop clause ("any other verdict change") is not
reached; the expected movement simply does not exist at this tip. No
other seed in any run changed verdict.

**The 0..200 cost, probe, not evidence** (`run --seeds 0..200
--threads 16`, same host; pre = the tip's binary built before the
change, post = after it): pre wall **12.6 s**, CPU 127.4 s user +
29.5 s sys (≈ 157 s); post wall 18.5–38.5 s across three runs (the
host carries other load; the quiet-host run is 18.5 s), CPU 181–192 s
user + 41–45 s sys (≈ 226–233 s). The replay costs about +45% CPU on
the campaign's WAL-heavy rounds — the price of the ruling's per-write
record tests; no `E4 ABORT`, no livelock, no deadlock.

**The gate's enforcement step in this container (a judgment call,
recorded with its evidence).** `bash scripts/gate.sh` failed at
`STEP enforcement-tests EXIT=1` three times, deterministically, on
one test only: "runner e2e: K: SIGTERM kills opencode and closes the
ledger range". Root cause, measured on a preserved scratch run:
**every real assertion of K held** — the driver exited rc 143, the
ledger range closed at the branch tip, and the fake opencode's pid
was dead (`kill(pid, 0) → ESRCH`) — but `dead()` also probes
`killpg(pgid, 0)`, which succeeded because the killed `sleep 30`
(orphaned when its parent died in the same group kill) stayed a
**zombie**: `ps` shows `[sleep] <defunct>` at exactly the run's pgid,
and the container carries 706 unreaped zombies, because **this
container's PID 1 is the card session's own `opencode` process**,
which never `wait()`s — no adopted orphan is ever collected, and a
zombie keeps its process group visible to `killpg`. The test is
self-contained (a throwaway repo under `mktemp -d`, copying only
`scripts/*.sh`, `CORE_PATHS` and `tools/run_card.py` — nothing this
card touched), and the same suite passed for the CXX and O1C gates on
hosts whose init reaps. The card's Touch-only list forbids editing
`scripts/` or `tools/`, so the fix here is **environmental**: the
Acceptance gate runs under a stdlib subreaper wrapper
(`PR_SET_CHILD_SUBREAPER` + a tight `waitpid(-1, WNOHANG)` loop,
preserved at `/tmp/…/reaper.py`, never in the repo) that adopts and
reaps orphaned descendants — the service a normal init provides. It
changes no gate step, no file and no test; `bash
scripts/test-enforcement.sh` under it prints `test-enforcement: all
passed` (exit 0), and the wrapper is not needed for any other step.

## Card X-5c-F4-F1-F1-CXX-F1's record (X5-R48: the re-paired twin that
## never fires)

## X5-R48 (P ruling, verbatim, fixed before this card's code; any change is an ESCALATE)

Copied verbatim from the card before any code of this card (the
step-0 discipline, as X5-R6–R47 before it). It amends X5-R21 (c)'s
re-pair (P ruling nv3-X-5c-F1-esc-1, above): when the re-paired
twin's crash does not fire at the real round's re-derived spec, the
re-pair itself re-derives, from both complete mask lists. The
diagnosis is verdict X-5c-F4-F1-F1-CXX's finding 1 (seed 7469).

**X5-R48 (amends X5-R21 (c)).** When the re-paired twin's crash does not fire at the real round's re-derived spec, both unfired attempts ran to completion, so both mask lists are complete.
- **Joint spec.** Take the per-index AND of the twin's masks and the real round's first-attempt masks, over the shorter length. Call `reachable_hook_spec` on it with the same drawn `(hook, ckpt_index)` and the same `(seed, round)`.
- **Re-run.** The twin re-runs at the joint spec from its own start. The real round then runs at that same spec. `twin_final_spec` and `real_final_spec` both name it, and `crash_desc` describes it.
- **Refusal.** A refused re-derive (union 0, or an empty list) leaves the unfired twin and its fired-guard defect standing. A joint-spec twin that still does not fire also trips the guard. There is no third attempt.
- **Unchanged.** Every round whose re-paired twin fires, the twin's first-attempt re-derive, the uniform wrap and the class C retry. No other arm draws anything.

**Step 0 — the ruling carried (commit 1c600515).** The text above,
verbatim, before any code of this card.

**Step 1 — the pin, failing at this tip (commit 42801a1f).**
`r48_repaired_twin_fires_at_the_joint_spec` (seed 7469 under
`Tuning::BASE`): no defect, `twin_paired` true on every round, round
1's crash `Mid2@4` and fired. At this tip it failed on
`Some("twin round 1: the crash spec never fired (Mid2@1)")` — 7469
was CXX's one defect seed of 0..10000 (the pre-change single-seed
run: `DEFECT_SEEDS=1`, the seed stopping after round 1, digest
1670717970).

**Step 2 — X5-R48 implemented (commit 45ab6b49).** The R21 (c)
re-pair block of `run_seed_with` (`src/workload.rs`), the only code
this card touches: when the re-paired twin does not fire at the real
round's re-derived spec, the per-index AND of the twin's complete
masks and the real round's first-attempt complete masks (over the
shorter length) goes to one `reachable_hook_spec` draw — the same
drawn `(hook, ckpt_index)`, the same `(seed, round)` — and on `Ok`
the twin re-runs at that joint spec from its own start (kept beside
`tch_pair` as `tch_retry` is), `spec2`/`script2`/`crash_desc` name
it, the real round then runs at it, and `twin_final_spec` and
`real_final_spec` both name it. On `Err` nothing changes: the unfired
attempt's fired-guard defect stands (the Refusal clause); a
joint-spec twin that still does not fire trips the guard the same
way; there is no third attempt. Every other arm — a firing re-pair,
the twin's first-attempt re-derive, the uniform wrap, the class C
retry — is untouched, and no other arm draws anything. Measured on
7469 (an instrumented scratch copy, single seed): the re-pair's own
masks `[3ff, 3f7, 3ff, 3f7, 3ff, 3ff, 3ff, 3ff]` AND the real first
attempt's `[3f7, 3ff, 3f7, 3ff, 3ff, 3ff, 3ff, 3ff]` = `[3f7, 3f7,
3f7, 3f7, 3ff, 3ff, 3ff, 3ff]`, and the draw moves `Mid2@0 ->
Mid2@4` (`Mid2` is `CKPT_HOOKS[3]`, bit 3 — the one bit `0x3f7`
lacks; the joint first carries it at index 4, and the drawn index 0
does not), twin and real both fire, three rounds all pass, no defect,
digest **1393729414** — the gate trial's exact value
(`trial-joint-repair.diff`, a diagnostic; this code was written
against the ruling, not copied from it).

**Step 3 — the mutant.** **M-R48-REAL-MASKS-ONLY** (the second
re-derive consults the real round's masks alone): the draw returns
`Mid2@1` again, the re-paired twin re-runs there and never fires, and
the pin fails on `Some("twin round 1: the crash spec never fired
(Mid2@1)")`. Applied to the real code, shown, reverted completely
(`git diff` empty, the pin green again at the reverted tip).

**Step 4 — `r21c` (commit 70e87d9a).** 7469 is back in the list
beside 9469, and the pin's doc is corrected per the verdict's finding
2: at CXX's tip only 9469 reaches the re-pair path with a firing
twin; 2669 re-derives in the twin's first attempt; 4049, 4309 and
9809 re-derive nothing.

**Step 5 — the last-step pin's second seed (commit 39ddccdd).**
`drop_last_step_keeps_its_pages_pinned` runs its unchanged body at
seeds 38 and 467. **G6-UNPIN-BEFORE-FREES** (the guard dropped after
the directory descent, before the tree mutex is re-taken, applied by
hand to `dir_drop_tree`'s last step): the pin **passes at 38** and
**fails at 467** — `seed 467: root 2 is mapped and in the free set —
the last step freed a page whose frame an evictor had already
latched (evict:finish resurrected it)`. Reverted clean (`git diff`
empty, the pin green at both seeds). The CXX verdict's
"UNCAUGHT at the literal seed 38" row is closed by the 467 literal,
exactly as its finding 3 prescribed.

**Step 6 — the digests (commit ba4aac1f).** Single seeds and the one
contiguous 0..200, under `ulimit -v 25165824`, output outside the
worktree; pre = CXX's tip (= this card's base, whose literals the
X5-R37 pin holds byte for byte), post = this tip:

| Run | Movement |
|---|---|
| 7469 singly | `1670717970 -> 1393729414`; `DEFECT_SEEDS 1 -> 0`; 2 rounds -> 3, all passing; r1's crash `Mid2@1 -> Mid2@4`, fired; the twin rounds all pass |
| the X5-R37 list (54 seeds), singly | 7469 alone; the other 53 byte-identical |
| contiguous 0..200, 16 threads | zero digest moves, zero record changes; the failing rounds stay the known 8/178 pair; `DEFECT_SEEDS=0` |

7469's literal is re-derived in the pin and the movement attributed
in its doc (X5-R48's own row).

**Step 7 — this record, and the correction above.** CXX's movement
passage said "of the nine, four first moved at the replay change";
its own table shows ten movers — three first at the replay change,
one at B1 — and the passage now says so (the verdict's finding 4).

## Card X-5c-F4-F1-F1-F3's record (X5-R41 the window-targeted SMO
## alignment, X5-R42 the Q4-I row, X5-R43 the seeded faults)

**Limit 5 (first commit, b7651f73) — X5-R40 removed.** `smo_share`,
`S_SMO`, `ScriptOp::AwaitCkptOpen`, `AWAIT_CKPT_OPEN_TURNS`, its four
pins and the seed 886 pin: it never handed off a setting and has no
reader. Its 0..200 result stays in `verdicts/X-5c-F4-F1-F1.md`.

**Step 1 — X5-R41 (commit f69e94b5).** A round is selected when all
hold: a targeted crash (`seed / 10` even), no drop this round, and a
draw from the new `S_R41` stream below `r41_share` permille — nothing
is drawn at share 0, and every content draw comes from the second new
stream `S_R41C`, so an unselected round consumes nothing (BASE's
scripts are bit-for-bit the generator's; the 54-seed BASE pin still
passes). The block, at the front of the GC task after the barrier:
two sync-on commits of 20 Puts each at the value ceiling (a dense run
of 40 keys in tree `tm`), two sync-on commits of Deletes of those
keys, `R41Wait`, `Gc { tree: tm }`, one sync-on commit of 20 Puts at
the ceiling (a dense run in a tree other than `tm`), `R41Done`; a
`R41Arm { idx: knobs.target_ckpt }` goes before the barrier. The hold
lives in the checkpointer's hook closure: at `After0` of the armed
checkpoint, before the crash arm, it sets the window flag and
spin-yields until `R41Done`, `workload_done`, a task error, or the
fixed 20,000-turn budget (`R41_TURNS`; no wall clock). `R41Wait`
spin-yields on the flag, `workload_done`, `tasks_left <= 1`, or the
same budget. Why this is not contrived: a real checkpoint over a
large dirty set stays open far longer than one tree sweep; the spike's
window is short only because its dirty set is small.

**Step 2 (commit 58e99890) — the record fields, never digest inputs.**
`q4reuse` (the store's new `ckpt_reuse` observable: `alloc_lpid`
handing out an LPID freed since this flip, reset per window beside
`ckpt_splits`/`ckpt_merges`), `imgsp`/`imgmg`/`imgreuse` (the same
counts for the window of the last checkpoint whose step 5 returned,
from the harness's per-checkpoint ledger), `q4i_raw` (the window of
the checkpoint whose meta the round's **final recovery opened**, by
that meta's `ckpt_id` through a per-seed ledger kept for the whole
seed — a meta rollback reads the older image's window, never the last
step-5 checkpoint's, and the harness checkpoint's id is dropped after
each round so its meta finds no row) and `q4i` (`q4i_raw` ∧ the
round's crash fired ∧ Q18's `end.is_some()`, in `q4i_of`). The JSON
round-trips all six. The store also gains exactly the two X5-R43
`Mutant` variants, inert when `cfg.mutant` is `None`, at the gate's
`patch.py` sites. **The reuse window is the whole flip window (the
flip through step 5), the field definition of step 2 — the Q4-I row's
"reused before its step 2 froze the table" is the coverage intent it
serves; a reuse after the freeze but before step 5 counts.**

**Step 4/5 — the three shares over 0..200, each with only that knob,
then reverted; probe, not evidence.** Same session, 16 threads,
`ulimit -v 25165824`, outputs outside the worktree. CPU is user+sys of
the run command; wall beside it.

| probe, not evidence | BASE | R41 1000 | R41 800 | R41 600 |
|---|---|---|---|---|
| rounds | 304 | 308 | 307 | 304 |
| Q4 | 0 | 28 | 23 | 18 |
| Q4 × 50 | 0 | **1,400** | 1,150 | 900 |
| Q5a (row 200) | 3 | 4 | 2 | 2 |
| Q7 (row 200) | 6 | 6 | 7 | 7 |
| Q18 (row 1,000) | 19 | 94 | 84 | 63 |
| q4i_raw / q4i | 4 / 4 | 30 / 30 | 27 / 27 | 18 / 18 |
| **Q4-I × 50** | 200 | **1,500** | 1,350 | 900 |
| imgboth (imgsp ∧ imgmg rounds) | 1 | 35 | 31 | 21 |
| imgru (imgreuse ≥ 1 rounds) | 4 | 35 | 32 | 22 |
| CPU s (wall s) | 163 (13.3) | 331 (36.8) | 299 (35.3) | 251 (34.0) |
| CPU vs BASE | — | +103% | +83% | +54% |
| failing rounds | 8 r0, 178 r1 | 178 r1 | 178 r1 | 8 r0, 178 r1 |

The failing rounds are the known pair (both MP O-3 (e) (ii) amcheck
findings, F4's MP-A class); the alignment makes 8 r0 pass at 1000/800
and leaves 178 r1 untouched. No new failure class appeared under any
share (no class C O-1 without XX001, no C-XX, no deadlock, no livelock
— `DEADLOCK=0 PANIC=0 DEFECT_SEEDS=0` everywhere).

**The decision rule, verbatim (fixed before any probe).** *Met: Q4 ×
50 ≥ 1,500, with no Q5a, Q7 or Q18 below the lower of its row and
BASE's own count × 50 on the same seeds, and CPU ≤ +60% of BASE. Also,
because the owner chose option A: the image arm with reuse × 50 ≥
1,500. Hand off the setting for F4's REFRESH. Not met by all three:
ESCALATE with the numbers. Q4 is never lowered or redefined by a card,
a gate or an orchestrator.*

Applied (the floors: Q5a min(200, 3×50=150)=150; Q7 min(200, 6×50=300,
row 200)=200; Q18 min(1000, 19×50=950)=950; "the image arm with reuse"
read as `q4i`, the row's own quantity — `q4i_raw` and `imgreuse` are
reported beside it):

- **1000: NOT MET.** Q4 ×50 = 1,400 < 1,500; CPU +103% > +60%; the
  image arm ×50 = 1,500 ≥ 1,500 (exactly); Q5a 200 ≥ 150, Q7 300 ≥
  200, Q18 4,700 ≥ 950.
- **800: NOT MET.** Q4 1,150; CPU +83%; image arm 1,350; floors held.
- **600: NOT MET.** Q4 900; CPU +54% (met); image arm 900; floors held.

**NOT MET by all three → no setting is handed off; ESCALATE with the
numbers** (Q4 at the best share reads 1,400 per 10k against the
1,500 bar; the prototype's 1,760-per-10k reading predates this card's
own `!knobs.drop_tree` selection clause, which removes a third of the
selected rounds). Q4's definition and threshold are untouched.

**Step 6 — X5-R42 (commit dc11158d).** `verify`'s exit rule gains the
Q4-I row (≥ 1,000 `q4i` rounds over the whole 10k, printed beside the
5.6 table and counted in its miss total) and its reproduction check
(a per-round `q4i` mismatch between the file and the re-run counts the
seed as non-reproducing; `q4i` stays out of the digest so X5-R37's
BASE literals cannot move). At this tip FINAL == BASE, so the row
reads short there; no Acceptance line runs `verify`.

**Step 7 — X5-R43 (commits f921a2a2 and the correction c3d1b4b5).**
The `seeded-faults` subcommand runs the range three times at the
campaign's tuning (clean, Joint `M-REUSE-DROPS-IMG`, Slot
`M-FREE-HOME-ALLOC`). A **new failing round** fails O-1, O-2 or O-3 in
the mutant's arm and **passes the oracle** in the clean arm
(no O-1/O-2/O-3 text — the pin's own wording, "a literal seed the
oracle fails on with the mutant and passes on without it"; the first
draft subtracted the clean arm's *fold* pass instead, which counted 46
MP detection rounds whose failure texts are identical between the
arms — a fault that changed nothing — and is corrected in c3d1b4b5).
Deadlock, panic and monitor-only hits never count. Caught is ≥ 5 new
failing rounds with ≥ 1 arm-predicate hit (`q4i_raw` for Joint,
`q4_split_merge` for Slot); the exit is non-zero if either fault is
not caught. Measured over 0..200, 16 threads:

| arm (probe, not evidence) | at BASE (share 0) | at share 1000 |
|---|---|---|
| Joint new failing rounds | 1 (`63r1`) | **19** (all at After4–After6/Mid6) |
| Joint with `q4i_raw` | 1 | **6** |
| Joint verdict | NOT CAUGHT (< 5) | **CAUGHT** |
| Slot new failing rounds | 0 | **0** |
| Slot with `q4_split_merge` | 0 | 0 |
| Slot verdict | NOT CAUGHT | **NOT CAUGHT** |

The Joint catches at share 1000, by crash position: After4@0 ×1,
After4@1 ×2, After5@1 ×3, After5@2 ×1, After6@0 ×1, After6@1 ×2,
After6@2 ×3, Mid6@0 ×2, Mid6@1 ×4 — exactly the gate's "all at
After4–After6" shape, and the gate's own "0 failing among Q4-window
reuse rounds" held too: **no Q4 round (a steps-0–3 crash) catches the
Joint fault**; its 6 arm-predicate hits are `q4i_raw` rounds (the
completed-window arm), consistent with the verdict's finding that
Q4's crash position cannot see the joint case. The Slot fault reads
**zero new failing rounds at every setting measured** (BASE and share
1000): its damage lands only in rounds that already carry oracle
texts (MP media detections) or pass — with no catch, the Caught rule
fails and with it the card's own bar, "**A fault not caught at the
chosen share is an ESCALATE, with no hand-off.**"

**M-FREE-HOME-ALLOC is UNCAUGHT** (no catcher exists over the card's
probe range; the named pin `x5_r43_free_home_alloc_is_caught` cannot
be written and is not — reported, never hidden). All other named
mutants were applied to the real code, shown failing their named
test, and reverted clean (`git diff` empty of the mutant each time):

| mutant | catching test (each shown failing, then reverted clean) |
|---|---|
| M-R41-NOHOLD (hold returns at once) | `x5_r41_aligned_round_has_q4_and_reuse` (digest 47420405 ≠ 1771266996) |
| M-R41-WRONGWINDOW (hold/gate key on checkpoint 0) | the same pin (digest 4213423696; the pin's seed has `idx` 1) |
| M-R41-NOPREP (the block's Deletes removed) | the same pin (digest 2823454535; `q4mg` falls) |
| M-R41-DRAW (`S_R41` drawn at share 0, fed into `S_GEN`) | `x5_r37_base_tuning_reproduces_1b0ce54e` (seed 0's BASE digest moved) |
| M-R41-AFTERCRASHARM (hold after the crash arm) | the same pin (digest 47420405; the pin's seed's hook is `After0`) |
| G-Q4MG-FROM-SPLITS (`q4mg` from `obs.splits`) | the exact-value pin (`q4mg` 4 ≠ 8) |
| M-REUSE-DROPS-IMG | `x5_r41_completed_window_arm` (seed 142's digest 2081828015 ≠ 1664460399 and the clean round fails the oracle) |
| M-FREE-HOME-ALLOC | **UNCAUGHT** — no catcher exists over 0..200 at BASE or share 1000 |
| M-Q4I-LASTCKPT (`q4i_raw` reads the last step-5 checkpoint) | `x5_r42_q4i_keys_on_the_opened_image` |
| M-Q4I-REFUSED (`q4i` drops `end.is_some()`) | `x5_r42_refused_round_is_not_q4i` |
| M-Q4I-NOFLOOR (the row's minimum to 0) | `x5_r42_row_at_999_and_1000` |
| M-Q4I-NOREPRO (the re-run comparison removed) | `x5_r42_flipped_q4i_fails_reproduction` |
| M-SEEDED-NOBASELINE (no clean-arm subtraction) | `x5_r43_known_failures_are_not_new` |
| M-SEEDED-NOARMRULE (the arm predicates dropped) | `x5_r43_caught_needs_the_arm_predicate` |

**Pins (step 3, commit c1e879f5).** `x5_r41_not_drawn_at_base`
(`r41_pick` draws nothing at share 0; the BASE-digest half is the
54-seed pin), `x5_r41_aligned_round_has_q4_and_reuse` (seed 21 r0 at
`r41_share` 1000: crash `After0@1`, `q4sp` **4** and `q4mg` **8**
exact — verdict finding 3: exact values, 4 ≠ 8 — `q4reuse` ≥ 1,
digest 1771266996), `x5_r41_completed_window_arm` (seed 142 r1:
`After5@1`, `imgsp`/`imgmg`/`imgreuse` **15/12/9** exact, `q4i_raw`,
clean oracle-clean, and the Joint fault's arm fails the oracle on the
same round), `x5_r42_q4i_keys_on_the_opened_image` (the ledger lookup's
discriminating case, and seed 148 r0: `imgreuse` **5** with `q4i_raw`
**false**), `x5_r42_refused_round_is_not_q4i`, and the campaign-side
pins of steps 6/7. The literals are probe-found at the first probe
share (1000) — no setting met the rule, so nothing was handed off;
the pins pin the machinery, not a chosen setting, and are labelled
probe-found, never evidence for a quota claim.

**What F4 inherits.** No `r41_share` is handed off (the rule is NOT
MET by all three; the ESCALATE carries the table above). The
machinery is in place and inert at share 0; X5-R42's row and its
reproduction check are in `verify`; the `seeded-faults` subcommand is
F4's to run on FINAL in its Acceptance (REFRESH 9), where the Joint
fault must be caught and the Slot fault's fate is the owner's to
decide (this card's ESCALATE).

## Card X-5c-F4-F1-F1-F3-F2's record (X5-R49: step 6 frees a freed
## LPID's home slot)

## X5-R49 (P ruling, verbatim, fixed before this card's code; any change is an ESCALATE)

Copied verbatim from the card before any code of this card (the
step-0 discipline, as X5-R6–R48 before it). The premise ruling first,
then the ruling itself.

**P ruling on the premise: yes, this is a prototype deviation from S-STORE §6.5 item 3 and §13 step 6, not a spec question.**
- §6.5 item 3's merge "frees `R`'s LPID (§4.2)". §4.4 lets a slot be written once nothing references it among the durable image, the image being built and the current map.
- After `free_lpid`, the current map no longer maps the LPID, so the third reference is gone. §13 step 6 then moves the slot from pending to free once meta `k` is synced and image `k` does not reference it.
- The spike keeps the slot in `used_cur`, the set that stands for that third reference, forever. It is a leak, not a loss: I-FREE-SAFE holds and nothing is overwritten. But the clean store never reuses a freed page's slot within a session, so I-FREE-SAFE's reuse path is never exercised, and a fault in it cannot be seen. That is why the Slot mutant can only show up through -F1's extra `used_cur` line.
- The spec text is unambiguous, so no spec change is needed.

**X5-R49 (a prototype deviation fixed; S-STORE §4.4, §6.5 item 3, §13 step 6).**
- **(a) Release at the free.** `free_lpid` removes the freed LPID's current home from `used_cur` in the same atomic step that removes `map.cur[lpid]`, **unconditionally**. It reads the home before the map drops it.
  - **Why unconditional, not `unpin_home`.** `free_lpid` runs `freeze_first_touch` first, so during a flip window the image being built already holds its own reference: an `Img::At` whose slot is in `image_refs`, or an epoch copy at another slot. (`Img::Same` is never constructed.) The durable image's reference is in `used_durable`.
  - `unpin_home` returns early on an `image_refs` slot. That would keep a window-freed home in `used_cur` after post-5 clears the pins: the same leak, only rarer. The gate's scratch (`E4G_SLOT=fixstore`) used `unpin_home` and left exactly those slots pinned.
- **(b) Step 6 is the release point.** `alloc_slot` also refuses a slot in `pending_free`. `ckpt:step6` clears `pending_free`, as it does today. So a slot that image `k−1` references and image `k` does not becomes allocatable at step 6, not at post-5. One pure predicate, `slot_free(&SlotState, slot)`, is `alloc_slot`'s only test.
- **(c) FREE-EARLY keeps its meaning.** Its apply makes its early frees allocatable at the flip, so it does not route them through `pending_free`, which (b) now protects. D14 still sees M-NOINPLACE.
- **(d) Nothing else changes:** not `unpin_home`, not post-5, not the step-6 closure, not the open's slot-set rebuild, and not `Mutant::FreeHomeAlloc`'s site (that site is -F1's).

**Step 0 — the ruling carried.** The text above, verbatim, before any
code of this card.

**Step 1 — `slot_free`, then the pins failing (commit cfd9fa69).**
`slot_free(&SlotState, slot)` is now `alloc_slot`'s only test, as one
pure predicate over the slot sets (a pure refactor: the 54-seed BASE
pin `x5_r37_base_tuning_reproduces_1b0ce54e` still passes, bit for
bit). The three pins were added and shown failing at the Base:
`x5_r49_freed_home_frees_at_step6` (its Mid6 check: slot 6 not free),
`x5_r49_window_freed_home_frees_at_next_step6` (its k+1 Mid6 check),
`x5_r49_unimaged_home_frees_at_once` (its free-at-once check).

**Step 2 — X5-R49 implemented (commit 2e274d09).** (a) `free_lpid`
releases the freed home from `used_cur` unconditionally, in the same
atomic step that drops `map.cur[lpid]`, reading the home before the
drop (`Mutant::FreeHomeAlloc`'s site above it untouched); (b)
`slot_free` refuses `pending_free`, so post-5 keeps a superseded slot
pending and §13 step 6's clearance (unchanged) is the release point;
(c) FREE-EARLY's apply frees into no set — `pending_free` is step 6's,
and routing there would hide the mutant behind (b)'s refusal. The
`SlotState` doc comments state the four sets' discipline. The three
pins pass; D14's FREE-EARLY still fires M-NOINPLACE; rework 6's
literal did **not** move and its pin is green. Seven digest-literal
pins fail at this commit pending step 4's re-derivation (the X5-R37
table, 1749 r2 and 5549 r2's O-1f lines, 7469, the late-copy seeds,
`szc`). **The eviction in flight across a free:** its `evict:finish`
re-inserts `map.cur[key] = r` — the free is undone in the map, the
documented "evict:finish resurrected it" shape, demonstrated by
`drop_last_step_keeps_its_pages_pinned` at seed 467 (CXX-F1 step 5) —
and `used_cur.insert(r.slot)`: the new slot stays in `used_cur`
**referenced by the resurrected entry** (inside a window,
`record_image_at` also pins it in `image_refs`, and post-5 carries it
into the durable set), so it is *not* rework 6's unreferenced class;
nothing to fix here, and (a) correctly released the *old* home at the
free (a later re-free of the resurrected LPID releases the new one).
Through the store's own paths the interleave cannot precede a free: a
pack or drop walk fetches the page first, and a fetch parks on an
`evicting` frame, so the free observes the finished eviction.

**Step 3 — the mutant table (this commit).** Each mutant applied to
the real code, shown failing its named test, reverted completely
(`git diff` empty after each; all named tests green at the reverted
tip):

| mutant | catching test (shown failing) |
|---|---|
| M-FREE-LEAK (no release at the free — the Base's code) | `x5_r49_freed_home_frees_at_step6` (Mid6 check) and `x5_r49_unimaged_home_frees_at_once` (at-once check); pin 2's k+1 check also fails |
| M-FREE-UNPIN-ONLY (the release through `unpin_home`, the gate's scratch form) | `x5_r49_window_freed_home_frees_at_next_step6` (k+1's Mid6 check) — pins 1 and 3 pass, exactly the ruling's "the same leak, only rarer" |
| M-STEP6-AT-POST5 (`slot_free` ignores `pending_free`) | `x5_r49_freed_home_frees_at_step6` (its After5 check: "must be pending at After5") |
| M-FREE-RELEASE-DURABLE (the release also removes `used_durable`) | `x5_r49_freed_home_frees_at_step6` (its before-the-flip check: image 1 still references the slot) |
| G-FE-PENDING (FREE-EARLY's apply routes its frees through `pending_free`) | D14's FREE-EARLY assertion (`monitors=[]` — M-NOINPLACE never fires, the mutant is inert behind (b)) |
| G2 (rework 6's own: the orphaned write-out's release removed) | `exhausted_reserve_keeps_both_copies` at its unmoved literal (`slots [13] entered used_cur referenced by nothing`) |

**Step 4 — movement measured, then the STOP (this commit).** Base =
`1180d761` (this card's Base), built from a `git archive` export at a
target dir outside the worktree; tip = this branch. Every run under
`ulimit -v 25165824`, 16 threads, output outside the worktree.

- **The Base reproduces `m/b1.json` exactly**: BASE 0..200 at the Base
  binary is byte-identical (sha256 `3480c86b…c3c6`, `E4 FOLD
  FAILING_SEEDS=2 FAILING_ROUNDS=2`), so the reference files stand.
- **X5-R37's 54 seeds, singly**: 41 byte-identical, **13 moved** —
  measured new digests (recorded here, **not applied** — see the stop):
  3 `923362676→4196035152`, 47 `2526989435→1878234829`, 112
  `3003462667→841173046`, 198 `95622311→3654049329`, 303
  `4147169426→144621379`, 967 `2847369120→3916549382`, 987
  `2492798953→1449471436`, 1749 `1584051239→3785784973`, 2376
  `3116539881→926921172`, 2927 `1901399424→1916772534`, 5549
  `308297210→3614535945`, 7469 `1393729414→1742707490`, 7528
  `122281874→3346573892`.
- **BASE 0..1000** (five 200-seed blocks at the tip vs `m/b1k.json`):
  243 digests moved; failing rounds Base 5 (`8r0, 178r1, 457r0, 537r0,
  688r0`, all the known MP class) → tip 4 (`8r0, 178r1, 537r0, 688r0`);
  twin failing rounds both sides `609r1, 849r1`; `DEFECT_SEEDS=0`,
  `DEADLOCK=0`, `PANIC=0` everywhere. **BASE 0..200** (tip block 1 vs
  `m/b1.json`): 51 moved, failing rounds 2 vs 2 — the known `8r0` /
  `178r1` pair on both sides.
- **New failures (the card's bullets, applied per (seed, round) and per
  twin) — not zero, so the card's stop rule fires.** All of them are
  bullet-2 (a round with no O-1/O-2/O-3 text at the Base that has one
  here); no fold regression (no round that passed the fold at the Base
  fails it here), and no new DEADLOCK, PANIC, MONITOR, DEFECT or baton
  hit anywhere:

  | seed, round | class | Base | tip |
  |---|---|---|---|
  | 47 r1 (round count 1→2) | MP | (no round 1; the seed's r0 carried the detection) | `pass=true`, `o1='O-1(a) at c=19: published {} != model {2, 3, 4, 5}'`, `crash=poison`; r0 is now text-free |
  | 339 r0 | MW | `pass=true`, no text, `crash=uniform@y:9569` | `pass=true`, `o1='O-1(a) at c=5: published {} != model {2, 3, 4}'`, `crash=uniform@y:6802` |
  | 547 r0 | MP | `pass=true`, no text, `crash=After5@2` | `pass=true`, `o1='O-1(a) at c=26: published {} != model {2}'`, `crash=After5@2` |
  | 779 r0 | MW | `pass=true`, no text, `crash=uniform@y:369` | `pass=true`, `o1='O-1(a) at c=7: published {} != model {2}'`, `crash=uniform@y:5896` |
  | 797 r0 | MP | `pass=true`, no text, `crash=uniform@y:9889` | `pass=true`, `o1='O-1(a) at c=21: published {} != model {2, 3}'`, `crash=uniform@y:11872` |
  | 799 r0 | MW | `pass=true`, no text, `crash=uniform@y:13386` | `pass=true`, `o1='O-1(a) at c=5: published {} != model {2}'`, `crash=uniform@y:9670` |
  | 967 r0 (round count 2→1) | MP | `pass=true`, no text, `crash=Mid6@0` | `pass=true`, `o1='O-1(a) at c=7: published {} != model {2, 3}'`, `crash=Mid6@0` |

  Reading (the rounds' own shapes, not a fix attempt): every new text
  is an **O-1(a) detected-and-explained loss** — X5-R6's MP/MW rule:
  a loss the round's media faults explain passes with the text
  recorded (`o1_detected_loss`). The fix moves the store's yield
  pattern (allocations differ from the free onward), so the drawn
  uniform crash points landed elsewhere (`9569→6802`, `369→5896`,
  `9889→11872`, `13386→9670`) and the media-fault ∩ recovery
  intersections changed in both directions: 47 and 967 each moved an
  existing detection one round (47's from r0 to a new r1, 967's from
  r1 into r0); 339/547/779/797/799 each gained one on a round that had
  none; and 457 r0's Base failure — an **unexplained** `O-3(a)` served
  corruption beside its `O-1(a)` — became explained at the tip
  (`crash=uniform@y:14318→6537`, `pass=false→true`), which is why the
  tip has one fewer failing round. Seeds 47 and 967 are reported under
  the strict reading (an added round carrying a text has vacuously
  "no text at the Base"); 339/547/779/797/799 (and 967 r0) meet the
  bullet as written, so the stop is mandatory under any reading.

**The stop, and what did not run.** Per Work 4: no fix was attempted.
The step's remaining items are machinery of the "Otherwise" branch and
did not run: the `r41_share` 1000 over 0..200 comparison against
`m/a1.json`; the literal re-derivation commits (the 13 measured new
digests above are recorded, not applied — the pins keep the old
literals and are red at this tip: `x5_r37_base_tuning_reproduces_1b0ce54e`,
`o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2` (its new
line: r2 tree 4 `lo=38 hi=38 frontier=38 T=35 c=38`),
`x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2` (its new line:
r2 `lo=34 hi=35 frontier=34 T=31 c=34`, owner ts 34 > T 31, the clamp
still not the identity), `r48_repaired_twin_fires_at_the_joint_spec`,
`late_copy_count_is_zero_at_base`, `szc_rename_crash_keeps_the_open_fold`,
`g8_checkpoint_poison_ends_the_workload`); F3's 14 named mutants against
re-derived pins; and the frees / freed-homes-released-at-step-6 /
handed-out-again diagnostic over BASE 0..200 (the gate read 0 and 0 at
the Base). Steps 5's re-derivations are therefore also not done; this
record is the card's end state.

## Card X-5c-F4-F1-F1-F3-F2-F1's record (X5-R51 the movement
## exemption, the literals re-derived, the image_refs pin, the 10k)

## X5-R51 (P ruling, verbatim, fixed before this card's code and before any literal; any change is an ESCALATE)

Copied verbatim from the card before any code, run or literal of this
card (the step-0 discipline, as X5-R6–R50 before it). **Numbering.**
The verdict proposes calling this rule X5-R50. But X5-R50 is already
taken: card X-5c-F4-F1-F1-F3-F1 uses it for its prior-window generator
block. The highest X5-R number in use in the README, the cards and the
verdicts is R50, so **this rule is X5-R51**. The numbers do not follow
landing order: X5-R51 lands before F3-F1's X5-R50, and that is
intended.

**X5-R51 (the step-4 movement exemption; a P rule on this card's comparisons only; X5-R49 unchanged).**
- **(a) Scope.**
  - X5-R51 amends only bullet 2 of F2's step-4 "new failure" list: "a round with no O-1, O-2 or O-3 text at the Base that has one here".
  - It applies to `rounds` entries, never to `twin` entries. It compares the same seed at the Base and at the tip under the same `Tuning`.
  - A round that exists at the tip but not at the Base counts as text-free at the Base.
  - Bullet 1 (a fold regression) and bullet 3 (a DEADLOCK, PANIC, MONITOR, DEFECT or baton hit the Base lacks) are unchanged.
  - The rule that a changed text in a round that already carried one is reported, not counted, is unchanged.
- **(b) Moved, not new.** A bullet-2 round, seed `s` round `i`, is **moved** only when every check below holds. Each check is read from the named output and nothing else:
  1. **Class.** The seed's `class` in the run JSON is `MP` or `MW`.
  2. **Pass.** The round's `pass` is `true`.
  3. **The text.** `o2` and `o3` are null, and `o1` begins with `O-1(a)`.
  4. **No monitor hit.** The run's `E4 SEEDS=… MONITOR=` count is 0. For a single-seed run, that run's own line counts. A non-zero count exempts nothing in that run.
  5. **Fully explained.** Run seed `s` alone at the tip with `E4_MPE_DIAG=1 E4_X33_DIAG=1`.
     - **MP:** take the `amcheck_i findings … tainted …` line inside the `=== seed s round i class Mp ===` block. It must hold:
       - at least one finding;
       - a non-empty `tainted` set;
       - every finding's `slot` is `Some(n)` with `n` in `tainted`.

       A `None` slot, a slot outside `tainted`, an empty finding list or a missing block means the round is **not** explained.
     - **MW:** the round's JSON `x33x` and `cover_err` are both `true`, and the `X33 diag: seed=s round=i ERR arm: damaged=[…]` line has a non-empty `damaged` list.
- **(c) Lost.** The reverse direction is a Base round with a text whose tip round is text-free and `pass`, or missing.
  - It is **moved-lost** when the Base round meets checks 1–3, plus the MW part of check 5 (the JSON fields only).
  - Any other such round is **healed**.
  - A Base round with `pass=false` whose tip round has `pass=true` and still carries a text is also **healed**. This is 457r0's shape.
- **(d) Reporting.** Moved-gained, moved-lost and healed rounds are listed in full, never netted and never counted as new failures or as gains.
  - **Moved-gained** rows give: seed, round, class, the Base and tip crash, the tip text, and for MP the findings and `tainted`, or for MW `damaged`.
  - **Moved-lost** and **healed** rows give: seed, round, class, the Base and tip crash, and the Base text.
- **(e) Outside the rule.** A bullet-2 round that fails any check in (b) is a **new failure**. So is any bullet-1 or bullet-3 hit.
  - STOP: write the rounds into the README, commit, and ESCALATE. **Do not try a fix.**
  - This applies at every comparison in this card: the 54 seeds, BASE 0..1000, and share 1000 over 0..200.
- **(f) The 10k comparator.** The JSON does not carry findings or `tainted`. So the comparator in Acceptance applies checks 1–4, plus the MW part of check 5. It lists every MP round that passes those checks as `MOVED_MP_PENDING`.
  - **The gate**, not a worker, runs each `MOVED_MP_PENDING` seed alone under (b)5 (seeds < 10000 only). A round that fails that check is a new failure: ESCALATE.
  - The comparator's exit code is 0 only if nothing outside the rule is found. That is necessary, not sufficient.
- **(g) Not an oracle change.**
  - `pass`, the texts, the fold, `verify`, every quota and X5-R1–R50 are unchanged.
  - X5-R51 decides only what this card's comparisons stop on. It carries to no other card unless P re-issues it.

## g8's re-selection (P rule, fixed with X5-R51)

`g8_checkpoint_poison_ends_the_workload` runs on seeds `[747, 967]`,
round 1 of each. At the tip, 967 has only one round, so its second
seed must be re-selected. This is not a literal edit.
- **Rule.** The replacement for 967 is the **lowest seed in 0..1000,
  other than 747, whose BASE round 1 passes every assertion of the
  pin** at the tip, with the pin body unchanged: `crash == "poison"`,
  `fired`, no `task_error`, no deadlock or livelock, `pass`, and no
  defect.
- The verdict expects seed 47. Record old → new, and give the reason:
  967 r1 no longer exists because X5-R49 moved checkpoint 1's
  `map_root` from 11 to 10.
- 747 is not re-selected. **If 747 r1 no longer passes the pin, STOP
  and ESCALATE.**

**Step 0 — the rulings carried.** The text above, verbatim, before any
code, run or literal of this card. F2's attempt-2 uncommitted edits
were never imported (this worktree started clean at the Base).

**Step 1 — the movement under X5-R51 (this commit).** The tip built at
the runner's `CARGO_TARGET_DIR` (outside the worktree); the Base was
**not** rebuilt (F2 and the gate both reproduced `m/b1.json` byte for
byte, so `m/b1k.json` stands). Every run under `ulimit -v 25165824`,
output outside the worktree (this sandbox's `~/.cache/nv3-review` is a
read-only mount, so the card's scratch dir lives at
`/tmp/nv3-container-jc_2ajyd/opencode/X-5c-F4-F1-F1-F3-F2-F1/`; the runs
and their content are unchanged by the relocation).

- **X5-R37's 54 seeds, singly** (`--threads 1`, one run each): exactly
  the 13 F2 measured moved — 3 `923362676→4196035152`, 47
  `2526989435→1878234829`, 112 `3003462667→841173046`, 198
  `95622311→3654049329`, 303 `4147169426→144621379`, 967
  `2847369120→3916549382`, 987 `2492798953→1449471436`, 1749
  `1584051239→3785784973`, 2376 `3116539881→926921172`, 2927
  `1901399424→1916772534`, 5549 `308297210→3614535945`, 7469
  `1393729414→1742707490`, 7528 `122281874→3346573892` — and the other
  41 are byte-identical. `MONITOR=0` in all 54 runs. No other
  difference, so no STOP here.
- **BASE 0..1000** as five 200-seed runs at 16 threads, compared with
  `m/b1k.json` per (seed, round) and per twin: **243 digests moved**;
  `MONITOR=0`, `DEADLOCK=0`, `PANIC=0`, `DEFECT_SEEDS=0` in every
  block; failing rounds Base 5 (`8r0 178r1 457r0 537r0 688r0`) → tip 4
  (`8r0 178r1 537r0 688r0`); twin failing rounds `609r1 849r1` on both
  sides, and **no twin round changed a text at all**. No fold
  regression (no round that passed the fold at the Base fails here), no
  class changed, no bullet-3 hit. The Acceptance comparator, its paths,
  its `MONITOR` source and its `range` pointed at these files, is the
  reference reading:

  ```
  DIGESTS_MOVED 243 FAILING_ROUNDS 4 MONITOR 0
  MOVED_GAINED 7 [(47, 1), (339, 0), (547, 0), (779, 0), (797, 0), (799, 0), (967, 0)]
  MOVED_MP_PENDING [(47, 1), (547, 0), (797, 0), (967, 0)]
  MOVED_LOST 5 [(47, 0), (179, 0), (397, 0), (579, 0), (967, 1)]
  HEALED 1 [(457, 0)]
  NEW_FAILING 0 [] BAD_SEEDS []
  ```

  (exit 0) — the gate's expected reading. My own per-round table,
  both directions, follows.

- **Moved-gained (7), each with its (b)5 evidence** — one single-seed
  run per seed with `E4_MPE_DIAG=1 E4_X33_DIAG=1` (each run's own
  `E4 SEEDS=1 … MONITOR=0` line, so (b)4 holds run by run):

  | seed, round | class | Base crash | tip crash | tip text (O-1(a)) | (b)5 evidence |
  |---|---|---|---|---|---|
  | 47 r1 | MP | (round 1 did not exist at the Base; r0 carried the text) | `poison` | `at c=19: published {} != model {2, 3, 4, 5}` | amcheck (i): 5 findings, `tainted {9, 11, 36, 47, 85}`, slots `[9, 47, 85, 36, 11]` all in tainted |
  | 339 r0 | MW | `uniform@y:9569` | `uniform@y:6802` | `at c=5: published {} != model {2, 3, 4}` | `x33x=true cover_err=true`; `X33 diag: seed=339 round=0 ERR arm: damaged=[(145943, 171812), (74181, 126189)]` (WAL inodes; no page slot) |
  | 547 r0 | MP | `After5@2` | `After5@2` | `at c=26: published {} != model {2}` | amcheck (i): 2 findings, `tainted {66, 167}`, slots `[66, 167]` |
  | 779 r0 | MW | `uniform@y:369` | `uniform@y:5896` | `at c=7: published {} != model {2}` | `x33x=true cover_err=true`; `damaged=[(1504, 2226), (50688, 51410), (2226, 2299), (5120, 5193)]` |
  | 797 r0 | MP | `uniform@y:9889` | `uniform@y:11872` | `at c=21: published {} != model {2, 3}` | amcheck (i): 2 findings, `tainted {13, 40}`, slots `[13, 40]` |
  | 799 r0 | MW | `uniform@y:13386` | `uniform@y:9670` | `at c=5: published {} != model {2}` | `x33x=true cover_err=true`; `damaged=[(48768, 65536)]` |
  | 967 r0 | MP | `Mid6@0` | `Mid6@0` | `at c=7: published {} != model {2, 3}` | amcheck (i): 1 finding, `tainted {10}`, slot `[10]` — checkpoint 1's `map_root` itself, moved 11→10 by X5-R49's slot reuse |

- **Moved-lost (5)** — Base rounds with a text whose tip round is
  text-free and `pass`, or missing; each meets (c)'s checks 1–3 plus
  the MW part of check 5 on the Base round's own JSON (`pass=true`,
  O-1(a) only; MW rows carry `x33x=true cover_err=true` at the Base):

  | seed, round | class | Base crash | tip crash | Base text (O-1(a)) |
  |---|---|---|---|---|
  | 47 r0 | MP | `After1@1` | `After1@1` | `at c=17: published {} != model {2, 3, 4}` (the detection moved to the new r1) |
  | 179 r0 | MW | `uniform@y:9755` | `uniform@y:132` | `at c=6: published {} != model {2}`, `x33x`/`cover_err` true at the Base |
  | 397 r0 | MP | `uniform@y:12142` | `uniform@y:10240` | `at c=4: published {} != model {2, 3}` |
  | 579 r0 | MW | `uniform@y:14505` | `uniform@y:12430` | `at c=18: published {} != model {2}`, `x33x`/`cover_err` true at the Base |
  | 967 r1 | MP | `poison` | (round 1 no longer exists; the seed now stops at r0) | `at c=8: published {} != model {2, 3, 4}` |

- **Healed (1):** `457 r0` MP — Base `uniform@y:14318` `pass=false`
  with `O-1(a) at c=30` **and** an unexplained `O-3(a)` beside it; tip
  `uniform@y:6537` `pass=true` with `O-1(a) at c=6`. The (c) pass-flip
  shape: the crash draw moved, the known served-corruption draw is no
  longer reached. The class itself remains (`8r0 537r0 688r0`).

- **Changed texts in rounds that already carried one (reported, not
  counted, 9):** 97 r0 `c=7→c=19`, 117 r0 `c=17→c=7`, 199 r1
  `c=26→c=12`, 457 r0 `c=30→c=6` (the healed row above), 527 r1
  `c=23→c=22`, 617 r0 `c=20→c=27`, 678 r0 `c=10→c=33`, 798 r0
  `c=5→c=18`, 857 r0 `c=7→c=12` — all `O-1(a)` on both sides.

**New failures under X5-R51 (e): 0.** The "Otherwise" branch of F2's
step 4 therefore runs, as this card's steps 2–6.

**Step 2 — the literals re-derived (X5-R49) (commit 54b7c105).** The
seven red pins at the Base tip, each literal re-derived once with its
cause (X5-R49's freed-home release moves the allocation scan's yields
and the slot numbering from a workload's first free onward; no pin's
body or predicate changed):

| pin | old → new | how the move shows (one line) |
|---|---|---|
| `x5_r37_base_tuning_reproduces_1b0ce54e` | 13 digests (3, 47, 112, 198, 303, 967, 987, 1749, 2376, 2927, 5549, 7469, 7528; e.g. 47 `2526989435→1878234829`) | the yield shift moves each seed's drawn crash points; the 41 others are byte-identical (step 1's 54-seed run) |
| `late_copy_count_is_zero_at_base` | 96 `4266526636→1886292663`, 190 `679383372→3730452910` (104/161 identical) | the exhausted-reserve interleavings that park these seeds' first-touch copies land at other yield points; the drop itself is unchanged |
| `szc_rename_crash_keeps_the_open_fold` | 2376 r2 crash `uniform@y:17768→uniform@y:15359` | the drawn uniform point moved with the yields; at the new point the crash is still at the rename's yield with the open checkpoint's fold (`szc` true, `fired`, measured) |
| `r48_repaired_twin_fires_at_the_joint_spec` | 7469 r1 `Mid2@4→Mid2@3` | checkpoint 1's slot numbering shifted, so the per-index AND of the two complete mask lists re-derives `Mid2@3`; both sides still fire there (no defect, all paired) |
| `o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2` | `lo=40 hi=41 frontier=41 T=38 c=41 → lo=38 hi=38 frontier=38 T=35 c=38` | the round's frontier moved onto its window bound, collapsing the window (owner ts 38 > T 35, `le_T=false`: the `le_T`-from-frontier mutant is still caught here) |
| `x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2` | `lo=30 hi=33 frontier=32 T=29 c=32 → lo=34 hi=35 frontier=34 T=31 c=34` | the window slid up to what the twin's line was at the Base; `c=34 < hi=35` still catches FRONTIER-AS-HI and ts 34 > T 31 the `le_T` mutant; at this tip r2 prints one window only |
| `g8_checkpoint_poison_ends_the_workload` | seeds `[747, 967] → [747, 47]` (P's re-selection rule; not a literal) | 967 r1 no longer exists — X5-R49 moved checkpoint 1's `map_root` 11→10, so the same `Mid6@0` draw fails 967's open in r0 and the seed stops; 47 is the lowest 0..1000 seed ≠ 747 whose BASE r1 passes every pin assertion at the tip (qualifying: 47, 67, 387, 527, 567, 747, 767, 788, 987); 747 unmoved and green |

`cargo test -q --release -p e4-recovery` green after the edits — 261
tests, 0 failed — and **F3's share-1000 pins are green at the tip**
(not red), so step 4 re-selects none of them. fmt and clippy clean.

**Anomaly (the 1749 window collapse).** With `lo = hi = frontier =
c = 38` the 1749 r2 line no longer separates the `c = lo`/`c = hi`
clamp mutants on its own, and 5549's `lo = frontier` does not separate
`c = lo` either (5549 still catches `c = hi`: `c=34 ≠ hi=35`, and
both pins still catch the `le_T`-from-frontier mutant). Those two
mutants are F3-F3a-F2-F1 gate rows, not part of this card's 14-row
binding table (`mut.py`'s set), so nothing this card must show is
weakened; recorded so P can re-point that gate's clamp rows if it
re-runs them.

**Step 3 — pin 4, `x5_r49_window_freed_unimaged_home_held_by_image_refs`
(verdict finding 4's follow-up -F2, folded here; this commit).**
Test-only, `tests/store.rs`. The shape, exactly as the verdict
specified: the leaf's home `s` is written by an eviction in epoch k
**before the flip** (checkpoint k's `Before0` hook), so no image ever
adopted it (`used_cur` alone); the page is **touched clean inside
checkpoint k's window** at `After0` (a `fetch_page` plus a
`store_page` of the same bytes — the first-touch freeze records
`Img::At(s)` and pins `s` in `image_refs`, with the premises asserted
inside the window, where they hold); and the page is **freed inside
the same window** at `After1`. Assertions, body in the pin's spirit of
pins 1–3: `slot_free(s)` false at the free itself, at every `Mid2`
after it, at k's `After5` (durable now) and through k's `Mid6`; true
first at checkpoint k+1's `Mid6` (pending at its `After5`).

- At the tip: **passes** (seed 5; slot 3; the store test target 50/50
  green).
- **O-FREE-DROP-IMGREFS applied to the real code** (the release in
  `free_lpid` also does `image_refs.remove(&r.slot)`): the pin fails
  at exactly the at-the-free check — `slot 3 must not be free at the
  free itself … left: Some(true) right: Some(false)` — while pins 1–3
  still pass under it (pin 2's slot is masked by `used_durable`, the
  verdict's finding). Reverted completely: `git diff` on
  `src/store/prims.rs` empty after the revert; the named tests green
  at the reverted tip.

**Step 4 — share 1000 over 0..200 (this commit).** A scratch export
of the whole workspace outside the worktree (this sandbox's
`~/.cache/nv3-review` is read-only, so the export lives under
`/tmp/nv3-container-jc_2ajyd/opencode/X-5c-F4-F1-F1-F3-F2-F1/src/`,
target dir beside it), with the probe knob the only edit
(`r41_share: 0 → 1000` in both `Tuning::BASE` and `Tuning::FINAL`;
the gate's own a1 used its `E4G_R41=1000` scratch form — same run).
The committed crate is untouched (the knob reverted, `git diff`
clean). One 200-seed run at 16 threads under `ulimit -v 25165824`:
`MONITOR=0`, `DEADLOCK=0`, `PANIC=0`, `DEFECT_SEEDS=0`, failing
rounds 1 (the known `8r0`).

- **vs `m/a1.json`, per (seed, round) and per twin under X5-R51
  (a)–(e):** 62 digests moved; no fold regression, no bullet-3 hit, no
  class change, and **no twin round changed a text**. The reference
  comparator form (paths/range pointed at these files) agrees with my
  per-round table:

  ```
  DIGESTS_MOVED 62 FAILING_ROUNDS 1 MONITOR 0
  MOVED_GAINED 1 [(47, 1)]
  MOVED_MP_PENDING [(47, 1)]
  MOVED_LOST 2 [(47, 0), (179, 0)]
  HEALED 0 []
  NEW_FAILING 0 [] BAD_SEEDS []
  ```

- **Moved-gained (1), with its (b)5 evidence:** 47 r1, MP, `poison`,
  `O-1(a) at c=19: published {} != model {2, 3, 4, 5}` — single-seed
  diag run at share 1000 (`E4_MPE_DIAG=1 E4_X33_DIAG=1`, its own line
  `E4 SEEDS=1 … MONITOR=0`): amcheck (i) 5 findings, `tainted {9, 11,
  36, 47, 85}`, slots `[9, 47, 85, 36, 11]` all in tainted — the same
  explained shape as at BASE 0..1000.
- **Moved-lost (2):** 47 r0 MP `After1@1`, Base text `O-1(a) at
  c=17: published {} != model {2, 3, 4}` (the detection moved to the
  new r1); 179 r0 MW `uniform@y:9755→132`, Base text `O-1(a) at c=6`
  with `x33x`/`cover_err` true at the Base — the WAL-cover-loss shape,
  exactly the verdict's finding 1.
- **Healed: none.** **New failures: 0.**
- **F3's share-1000 pins: none re-selected.** All of them
  (`x5_r41_aligned_round_has_q4_and_reuse`,
  `x5_r41_completed_window_arm`, the `x5_r42_*` and `x5_r43_*` rows)
  meet their own predicates at their own seeds at this tip — the full
  suite is green with them in it (262 passed, 0 failed, this step's
  re-run).

**Step 5 — the mutants re-run (this commit).** F3's 14 rows with
`mut.py`'s exact edits, and F2's six plus its two `O-*` rows and this
card's `O-FREE-DROP-IMGREFS`, each applied to a scratch export of this
tip outside the worktree (the only edit per run; restored after — the
export's sources verified byte-identical to the worktree tip at the
end), built at a scratch target dir, `cargo test -q --release
-p e4-recovery --no-fail-fast` filtered to each row's named test:

| mutant | rc | catching test(s) shown failing | vs the record |
|---|---|---|---|
| M-R41-NOHOLD | 101 | `x5_r41_aligned_round_has_q4_and_reuse` | as at F3 |
| M-R41-WRONGWINDOW | 101 | the same pin | as at F3 |
| M-R41-NOPREP | 101 | the same pin | as at F3 |
| M-R41-DRAW | 101 | `x5_r37_base_tuning_reproduces_1b0ce54e` (against the re-derived literals) | as at F3 |
| M-R41-AFTERCRASHARM | 101 | `x5_r41_aligned_round_has_q4_and_reuse` | as at F3 |
| G-Q4MG-FROM-SPLITS | 101 | the same pin | as at F3 |
| M-REUSE-DROPS-IMG | 101 | `x5_r41_completed_window_arm` | as at F3 |
| M-FREE-HOME-ALLOC | 101 | **16 tests**, incl. `x5_r49_freed_home_frees_at_step6`, `x5_r49_window_freed_home_frees_at_next_step6`, pin 4, `x5_r37_…`, `late_copy_…`, `r48_…`, `szc_…`, `x5_r42_q4i_…`, `d14`-adjacent digest pins | **no longer UNCAUGHT** — see below |
| M-Q4I-LASTCKPT | 101 | `x5_r42_q4i_keys_on_the_opened_image` | as at F3 |
| M-Q4I-REFUSED | 101 | `x5_r42_refused_round_is_not_q4i` | as at F3 |
| M-Q4I-NOFLOOR | 101 | `campaign::x5_r42_row_at_999_and_1000` | as at F3 |
| M-Q4I-NOREPRO | 101 | `campaign::x5_r42_flipped_q4i_fails_reproduction` | as at F3 |
| M-SEEDED-NOBASELINE | 101 | `campaign::x5_r43_known_failures_are_not_new` | as at F3 |
| M-SEEDED-NOARMRULE | 101 | `campaign::x5_r43_caught_needs_the_arm_predicate` | as at F3 |
| M-FREE-LEAK | 101 | all four `x5_r49_*` pins (1's Mid6, 3's at-once, 2's k+1, 4's k+1) | as at F2 (pins 1 and 3 named there; 2 and 4 also fail) |
| M-FREE-UNPIN-ONLY | 101 | `x5_r49_window_freed_home_frees_at_next_step6` **and pin 4** (pins 1, 3 pass) | as at F2; pin 4 adds a second catcher for the same leak |
| M-STEP6-AT-POST5 | 101 | `x5_r49_freed_home_frees_at_step6` (After5), pins 2 and 4 | as at F2 (more catchers) |
| M-FREE-RELEASE-DURABLE | 101 | `x5_r49_freed_home_frees_at_step6` (before the flip) | as at F2 |
| G-FE-PENDING | 101 | `d14_monitors` (FREE-EARLY, `monitors=[]` — M-NOINPLACE inert behind (b)) | as at F2 |
| G2 (the orphaned write-out's release removed, `epoch_copy:record`) | 101 | `exhausted_reserve_keeps_both_copies` | as at F2 (site re-identified at this tip: the orphan's `used_cur.remove` in the `!same` arm) |
| O-STEP6-NOCLEAR | 101 | pins 1 and 2 (Mid6) and pin 4 | as at F2 (more catchers) |
| O-SLOTFREE-NO-DURABLE | 101 | pin 1 (before the flip), pins 2 and 4 | as at F2 (more catchers) |
| **O-FREE-DROP-IMGREFS** | 101 | **`x5_r49_window_freed_unimaged_home_held_by_image_refs`** (pin 4 alone; pins 1–3 pass — pin 2 masked by `used_durable`) | **caught** (UNCAUGHT at F2; step 3's pin closes the gap) |

**M-FREE-HOME-ALLOC is no longer UNCAUGHT at this tip.** The card and
F3's README expect it UNCAUGHT (F3-F1's job). At F2's Base it was
inert because the freed home stayed in `used_cur` for the session (the
leak X5-R49 fixed), so dropping `used_durable`/`image_refs` at the
free still left the slot refused. With X5-R49 (a) in place the same
edit makes every freed home allocatable at once while its durable
image references it — 16 tests fail, the three `x5_r49_*` pins among
them. The direction is favourable (a mutant that gained catchers, none
lost); nothing was weakened and F3-F1's fix is still pending. Recorded
as a deviation from the card's expected row, not hidden.

**M-R51-NOEXPLAIN** (the checker mutant, scratch-only, applied to this
worker's step-1 checker logic, never the crate): with (b)5 dropped it
classifies as MOVED exactly the rounds the full checker stops on —
shown on synthetic diag lines: an MP finding on an untainted slot
(`slot: Some(9)` in `tainted {10}`), an empty findings list, and an MW
`damaged=[]` line all read "MOVED (explained)" under the mutant and
"NEW FAILURE → STOP" under the full checker (`mr51.py` in the scratch
dir, exit 0 on the divergence).

**Step 6 — the frees diagnostic, and this card's end state (this
commit).** Over BASE 0..200 at the tip, in a scratch export whose only
edits are three `GEV` `eprintln!` counters and a diag-only
`SlotState::freed_homes` set (never committed; the worktree untouched,
`git status` clean; the instrumented run's `E4` line and all 200
digests byte-identical to the plain tip run, so the counters changed
nothing):

- `GEV FREE` — `free_lpid` home releases: **2209**
- `GEV STEP6` — freed homes that became free at a §13 step 6:
  **2091**
- `GEV REUSE` — allocations that handed a freed home out again in the
  same session: **4212**

The gate read 0 and 0 for the last two at the Base (the leak X5-R49
fixes). All three are non-zero here, so X5-R49 does real work on the
campaign — no ESCALATE. (The reuse count exceeds the free count
because a slot can be freed and re-handed more than once in a session;
the counters are raw event counts, reported as measured.)

**Correction of F2's step-4 record (verdict finding 2).** F2's README
table called the seven gained texts "X5-R6 detected-and-explained
losses" with `o1dl`/`r6` framing; the verdict corrects this: they are
**failed-recovery rounds** (`published {}`, no `r6` detection, `o1dl`
0) that pass with the O-1(a) text recorded, 547 r0 and 967 r0 were the
X5-R6 rounds **at the Base**, and the record omitted the reverse
direction entirely (the five moved-lost rounds this card's step 1
lists). F2's measured numbers themselves were right; this card's step
1 supersedes its reading.

## RESULT

card: X-5c-F4-F1-F1-F3-F2-F1   branch: card/v3-X-5c-F4-F1-F1-F3-F2-F1   commit: (this commit; the chain d43236cc → 6023ccbc → 54b7c105 → b53f9ed3 → da4808a4 → ba255388 → here)

acceptance (this attempt runs `cargo test` and `scripts/gate.sh` last;
the 10k baseline lines are the runner's per X5-R31 — this sandbox's
`~/.cache/nv3-review` is a read-only mount, so their `--out` paths
cannot be written here):
- `cd nucleus/v3 && cargo test -q --release -p e4-recovery` — 262
  passed, 0 failed (261 before pin 4; every target green).
- `cd nucleus/v3 && bash scripts/gate.sh` — see the run log below; the
  two known reds the card's Notes name (`runner e2e: K`, and
  `ack_prefix_two_batches_in_flight` under load) are pre-existing, not
  this card's.
- The 10k baseline, the runner's once, at this tip; the comparator
  with X5-R51 (f) as written, `MOVED_MP_PENDING` then checked by the
  gate under (b)5.

mutants: F3's 14 — 13 caught as recorded; **M-FREE-HOME-ALLOC caught**
(16 tests; was expected UNCAUGHT — X5-R49 (a) unmasked it; favourable,
recorded, F3-F1 still pending). F2's six: M-FREE-LEAK (all four
`x5_r49_*` pins), M-FREE-UNPIN-ONLY (pin 2 + pin 4), M-STEP6-AT-POST5
(pin 1 + 2 + 4), M-FREE-RELEASE-DURABLE (pin 1), G-FE-PENDING
(`d14_monitors`), G2 (`exhausted_reserve_keeps_both_copies`; site
re-identified). O-STEP6-NOCLEAR (pins 1, 2, 4),
O-SLOTFREE-NO-DURABLE (pins 1, 2, 4), **O-FREE-DROP-IMGREFS caught by
pin 4 alone** (UNCAUGHT at F2). M-R51-NOEXPLAIN: shown on synthetic
diag lines. Every mutant applied and reverted clean; the export
verified byte-identical to the tip afterwards.

numbers: BASE 0..1000 vs `m/b1k.json`: 243 digests moved, moved-gained
7 / moved-lost 5 / healed 1 / new failures 0 (the comparator's exact
reading). Share 1000 over 0..200 vs `m/a1.json`: 62 moved, gained 1 /
lost 2 / healed 0 / new 0. 54 seeds: 13 moved, exactly F2's set.
Diagnostic over BASE 0..200: frees 2209, step-6 releases 2091,
in-session reuses 4212 (all non-zero; the Base read 0 and 0).

judgment calls: (1) the card's scratch dir `~/.cache/nv3-review/
X-5c-F4-F1-F1-F3-F2-F1/` is a read-only mount in this sandbox, so all
scratch (exports, targets, run output, scripts) lives under
`/tmp/nv3-container-jc_2ajyd/opencode/X-5c-F4-F1-F1-F3-F2-F1/` — the
runs themselves are unchanged. (2) The share-1000 probe used a
`Tuning` patch (`r41_share: 1000` in both consts) instead of the
gate's `E4G_R41` env form — the same run. (3) G2's edit site was
re-identified at this tip (the orphan's `used_cur.remove` in
`epoch_copy:record`'s `!same` arm): the first attempt at the row
(evict:finish's idempotent re-insert) was inert and discarded, and the
correct site catches as at F2. (4) `szc`'s doc comment keeps F4's
"folded 235 ≥ 4·F = 184" measurement with a note that the crash
literal moved and the `szc` assertion holds at the new point.

ESCALATE: none.

anomalies: (1) the 1749 r2 frontier window collapsed to
`lo=hi=frontier=c=38`, so the two O1F pins no longer separate `c=lo`
(5549 still catches `c=hi`; both still catch the `le_T`-from-frontier
mutant) — those are F3-F3a-F2-F1 gate rows, not this card's 14;
recorded for P. (2) M-FREE-HOME-ALLOC is no longer UNCAUGHT at this
tip (see mutants). (3) `git commit` prints a benign
`packed-refs.lock: Read-only file system` warning (the shared main
repo's refs are read-only here); each commit lands on the branch
correctly — `git log` and `git status` verified after every step.



## Card X-5c-F4-F1-F1-F3-F2-F1-F1's record (X5-R52, P's post-data
## amendment to X5-R51; the comparator in `tools/`; the re-read of
## F2-F1's 10k)

The F2-F1 gate verdict (`verdicts/X-5c-F4-F1-F1-F3-F2-F1.md`, at
`8992f77f`) ESCALATED: seven rounds fall outside X5-R51 as written — the
two twins it names (2599 r2, 2859 r1), 7078 r0 (a pass→fail the
comparator never tested), and four `MOVED_MP_PENDING` rounds whose only
finding is `slot: None` (1927 r1, 7257 r0, 7658 r0, 8278 r0). None is a
loss caused by X5-R49; each is a pre-existing class the moved schedule
reaches. X5-R51 was fixed in advance and "any change is an ESCALATE", so
only P could amend it. **X5-R52 is that amendment, verbatim below.** If
the re-read and the gate's single-seed checks find nothing outside
X5-R52, F2-F1's commits merge through this card, on its chain, with no
code change.

**Numbering.** The highest X5-R number in use in the README, the cards,
the verdicts, QUEUE and PLAN is R51 (checked on every `card/v3-*` branch
on 2026-11-07). **This rule is X5-R52.**

### X5-R51, quoted unchanged (the rule this card's record reads against)

Copied verbatim from F2-F1's card and its step-0 quote above; not edited
by this card:

**X5-R51 (the step-4 movement exemption; a P rule on this card's comparisons only; X5-R49 unchanged).**
- **(a) Scope.**
  - X5-R51 amends only bullet 2 of F2's step-4 "new failure" list: "a round with no O-1, O-2 or O-3 text at the Base that has one here".
  - It applies to `rounds` entries, never to `twin` entries. It compares the same seed at the Base and at the tip under the same `Tuning`.
  - A round that exists at the tip but not at the Base counts as text-free at the Base.
  - Bullet 1 (a fold regression) and bullet 3 (a DEADLOCK, PANIC, MONITOR, DEFECT or baton hit the Base lacks) are unchanged.
  - The rule that a changed text in a round that already carried one is reported, not counted, is unchanged.
- **(b) Moved, not new.** A bullet-2 round, seed `s` round `i`, is **moved** only when every check below holds. Each check is read from the named output and nothing else:
  1. **Class.** The seed's `class` in the run JSON is `MP` or `MW`.
  2. **Pass.** The round's `pass` is `true`.
  3. **The text.** `o2` and `o3` are null, and `o1` begins with `O-1(a)`.
  4. **No monitor hit.** The run's `E4 SEEDS=… MONITOR=` count is 0. For a single-seed run, that run's own line counts. A non-zero count exempts nothing in that run.
  5. **Fully explained.** Run seed `s` alone at the tip with `E4_MPE_DIAG=1 E4_X33_DIAG=1`.
     - **MP:** take the `amcheck_i findings … tainted …` line inside the `=== seed s round i class Mp ===` block. It must hold:
       - at least one finding;
       - a non-empty `tainted` set;
       - every finding's `slot` is `Some(n)` with `n` in `tainted`.

       A `None` slot, a slot outside `tainted`, an empty finding list or a missing block means the round is **not** explained.
     - **MW:** the round's JSON `x33x` and `cover_err` are both `true`, and the `X33 diag: seed=s round=i ERR arm: damaged=[…]` line has a non-empty `damaged` list.
- **(c) Lost.** The reverse direction is a Base round with a text whose tip round is text-free and `pass`, or missing.
  - It is **moved-lost** when the Base round meets checks 1–3, plus the MW part of check 5 (the JSON fields only).
  - Any other such round is **healed**.
  - A Base round with `pass=false` whose tip round has `pass=true` and still carries a text is also **healed**. This is 457r0's shape.
- **(d) Reporting.** Moved-gained, moved-lost and healed rounds are listed in full, never netted and never counted as new failures or as gains.
  - **Moved-gained** rows give: seed, round, class, the Base and tip crash, the tip text, and for MP the findings and `tainted`, or for MW `damaged`.
  - **Moved-lost** and **healed** rows give: seed, round, class, the Base and tip crash, and the Base text.
- **(e) Outside the rule.** A bullet-2 round that fails any check in (b) is a **new failure**. So is any bullet-1 or bullet-3 hit.
  - STOP: write the rounds into the README, commit, and ESCALATE. **Do not try a fix.**
  - This applies at every comparison in this card: the 54 seeds, BASE 0..1000, and share 1000 over 0..200.
- **(f) The 10k comparator.** The JSON does not carry findings or `tainted`. So the comparator in Acceptance applies checks 1–4, plus the MW part of check 5. It lists every MP round that passes those checks as `MOVED_MP_PENDING`.
  - **The gate**, not a worker, runs each `MOVED_MP_PENDING` seed alone under (b)5 (seeds < 10000 only). A round that fails that check is a new failure: ESCALATE.
  - The comparator's exit code is 0 only if nothing outside the rule is found. That is necessary, not sufficient.
- **(g) Not an oracle change.**
  - `pass`, the texts, the fold, `verify`, every quota and X5-R1–R50 are unchanged.
  - X5-R51 decides only what this card's comparisons stop on. It carries to no other card unless P re-issues it.

### X5-R52 (P ruling, verbatim, fixed before this card's code; any change is an ESCALATE)

Copied verbatim from the card before any code of this card (the step-0
discipline, as X5-R6–R51 before it):

**X5-R52 (the X5-R51 amendment: twin DDL divergence, meta-only detections, MP-A under X5-R30 (c), and bullet 1 made testable; a P rule, post-data; X5-R49 and X5-R51's text unchanged).**
- **(0) Written after seeing the data.** P wrote X5-R52 **after** reading all of the following. It is a post-data amendment, and RESULT item 6 lists it as one, in the pattern of X5-R30 (c) and X5-R32.
  - The verdict, `nucleus/v3/verdicts/X-5c-F4-F1-F1-F3-F2-F1.md` at `8992f77f`.
  - F2-F1's acceptance 10k: `~/.cache/nv3-review/runlogs/X-5c-F4-F1-F1-F3-F2-F1.acc.campaign.json` (sha256 `6b6862665f9609e3513a6bc7f84b2bfe3370e919a775bf9a1e91450482dc739a`; `.prov`: commit `eb9c4328`, `status_lines=0`, `exit=0`, `verify_exit=0`), its `.campaign.out` (MONITOR=0), and the comparator's output, `X-5c-F4-F1-F1-F3-F2-F1.10k-verify-compare.out`.
  - The reference, `~/.cache/nv3-review/X-5c-F4-F1-F1-CXX-F1/gate/tip-10k.json` (sha256 `f4d6f1ff0c2731b9ecbb3ca862534e168a11dd2f99beb1cdb1a3828b5310c923`).
  - The gate's single-seed evidence in `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F3-F2-F1/m/`: `b5.txt`, `pend/`, `d_*`, `e_*` and `tip_{2599,2859,7078}.*`.
  - P's own reference implementation of the amended comparator, run over those files. Its readings are under "Expected" in Work step 2.

  **The seven rounds it exempts were seen before it was written:** 2599 twin r2, 2859 twin r1, 7078 r0, 1927 r1, 7257 r0, 7658 r0 and 8278 r0. Each exemption below is drawn around a class that already exists in the reference file, never around those seven rounds.
- **(a) Scope.**
  - X5-R52 amends X5-R51 for two sets of comparisons: this card's re-read of F2-F1's 10k, and every comparison in card X-5c-F4-F1-F1-F3-F2-F1-F2. **For -F2, P re-issues X5-R51 here**, read with this amendment.
  - It carries to no other card unless P re-issues it.
  - **It is not an oracle change.** `pass`, the texts, the fold, `verify`, every quota and X5-R1–R51 are unchanged.
  - **It is not an E4 judgement rule.** F4's class table and E4's owner rule are unchanged: "a class outside that table keeps E4 open", and "a class that a landed card claims to have fixed and that recurs counts as new". X5-R52 decides only what these comparisons stop on.
- **(b) Bullet 1, made testable.**
  - A **fold regression** is any `rounds` or `twin` entry that has `pass=true` at the Base and `pass=false` at the tip, **whatever text the Base round carried**. An entry missing at the Base counts as `pass=true` and text-free there.
  - X5-R51 (a) keeps bullet 1 unchanged, and it has no exemption. X5-R52 grants exactly two bullet-1 exemptions, (c)(i) and (c)(iii), and nothing else.
  - A round that carried a text at the Base and still passes at the tip with a different text is reported (`TEXT_CHANGED`), not counted. X5-R51 (a)'s last bullet is unchanged.
- **(c) The three exemptions.** Each one is checked only from recorded fields: the two JSON files, the run's `.campaign.out`, and the named single-seed diagnostic output. An exemption that cannot be checked from them exempts nothing.
  - **(i) Twin DDL divergence.** It applies to `twin` entries only. X5-R51 (a)'s "never to `twin` entries" is amended for this shape alone.
    - **JSON prefilter (the comparator):**
      - the seed's `class` is `MW`;
      - the tip twin entry `i` has `pass=false`, and its `o1` begins with `O-1(a)` and contains `published {}`;
      - some real round `j < i` has `lost + mis + torn ≥ 1` and either `cover_err` is true or `frontier < hi`;
      - the run's MONITOR count is 0.

      An entry that passes the prefilter is `TWIN_DDL_PENDING`. One that fails it is a new failure.
    - **The single-seed check (the gate; `check-twin`).** Run seed `s` alone, at the commit under test, in a scratch export with `tools/x5r52-twin-diag.patch` applied. All five checks must hold:
      - **T1.** The class-C (twin) recovery `Err` line for round `i` is exactly `Corrupt("tree N missing in directory")`.
      - **T2.** The `X5R52 round-start` line for round `i` has `N` in `real_dir` and `N` absent from `twin_dir`.
      - **T3.** The twin chain dropped `N` itself: a class-C replay listing of this seed, at a round `≤ i`, shows a commit record with `ddl` containing `D<N>`.
      - **T4.** Let `k` be the first round start at which `N ∈ real_dir` and `N ∉ twin_dir`. Then `k ≥ 1`, and real round `k−1` in the 10k JSON has `lost + mis + torn ≥ 1` and either `cover_err` or `frontier < hi`. So the divergence first shows right after a WAL-faulted real round whose recovery stopped short.
      - **T5.** The diagnostic run's digest for `s` equals the 10k file's, and its `E4 … MONITOR=` count is 0. A run whose digest differs is not evidence.
    - **Why this is a pre-existing class and not a new loss.**
      - The reference file already has six twin failures of exactly this shape: 609, 849, 3449, 5049, 6539 and 7179, all r1. The gate re-ran each at the Base and at the tip and got the same error.
      - The failing replay applies a WAL record to a tree that the twin's image correctly dropped. No page slot, no freed home and no `image_refs` is involved, so X5-R49's slot freeing cannot reach it.
      - X5-R49 moved crash points (2859 r1: `uniform@y:10565` → `6280`), and that is all. 6539 heals by the same movement.
      - It is the C-XX "recovery-Err default" twin family, a Work 2.5 pairing artefact in the harness. -F2 fixes it.
    - **Still caught:**
      - a "tree N missing" twin failure where the real chain lacks `N` too at round `i`'s start. T2 fails: that is a replay bug, not divergence;
      - a divergence that first shows after a real round with no recorded media fault, which is a crash-cut-only loss. T4 fails;
      - any other twin `Err` (another `Corrupt` text, `WalLoss`, a panic). T1 fails;
      - a twin failure in a class MP or C seed. The prefilter fails;
      - a twin with `pass=false` and an O-2 text, or an O-1 text that is not a failed recovery. The prefilter fails;
      - any twin failure at a commit that carries -F2's re-seed (see (g)).
  - **(ii) Meta-only detected loss.** It amends X5-R51 (b)5 for MP moved-gained rounds only.
    - **JSON prefilter (the comparator; `META_ONLY_ELIGIBLE`).** The round is already `MOVED_MP_PENDING` under X5-R51 (b)1–4, and:
      - `c` is null and `frontier` is 0, so recovery failed;
      - `mrb=false`;
      - `served=false`;
      - `q[15]` (Q14, a meta-slot fault) is 1;
      - `q[12]`, `q[13]` and `q[14]` (Q13: a fault on a referenced slot) are all 0.
    - **(b)5′, the single-seed check (the gate; `check-mp`).** Inside the `=== seed s round i class Mp ===` block:
      - the `amcheck_i findings` line has at least one finding;
      - **every** finding is `slot: None` with `what` beginning `no valid meta` (the G7 misplaced-meta form included);
      - `tainted {}` and `meta_k None`;
      - the block's `media[…]` lines include at least one fault on page slot 1 or 2, either by `off` or by a Misdirected `to_off` of 4096 or 8192 on inode 0.

      T5's digest and MONITOR checks apply here too. A round outside `META_ONLY_ELIGIBLE` must pass (b)5 exactly as X5-R51 wrote it.
    - **Why this is a pre-existing class and not a new loss.**
      - X5-R5 pre-registered meta-slot faults, and Q14 counts them (611 rounds in this 10k).
      - The reference file has **187** MP rounds of exactly the prefilter's JSON shape, and the tip has 190.
      - With both meta slots faulted, or the only valid meta misplaced, open has no meta to open. The store reports a detected loss.
      - The finding names no page slot, `tainted` is empty, and no referenced page slot was hit. The free path is never on the read path.
    - **Still caught:**
      - "no valid meta" with `q[15]=0`, meaning the store lost its own meta with no fault drawn. Not eligible, so (b)5 applies and fails;
      - a meta fault plus any `Some(n)` finding, or any `None` finding whose text is not "no valid meta". (b)5′ fails;
      - a round that also hit a referenced page slot (`q[12..14]` not all 0). Not eligible, so it must pass (b)5;
      - `mrb=true` or `served=true`. Not eligible;
      - `pass=false`. That is bullet 1, never (ii).
  - **(iii) MP-A under X5-R30 (c).** It applies to bullet-1 `rounds` entries only.
    - **The check, exact from the JSON (the comparator; no single-seed run):**
      - the seed's `class` is `MP`;
      - **the Base round** has `pass=true`, `mrb=true`, `c` null, `q[15]=1`, `o2` and `o3` null, and `o1` beginning with `O-1(a)`. It was already a meta-rollback detected loss;
      - **the tip round** has `pass=false`, `served=true`, `mrb=true`, `c` null and `q[15]=1`; `o2` is null; `o3` is exactly `O-3(a): served corruption`; `skx` or `smx` is true; and `pass_na` is exactly `true`;
      - the run's MONITOR count is 0.

      A round that meets every check is `MPA_PASS_FAIL`. Any other bullet-1 round is a new failure.
    - **Why this is a pre-existing class and not a new loss.**
      - X5-R30 (c) rule 4 (RULED, binding) says that "a served round passes iff `(skx ∨ smx) ∧ pass_na`". Such a round passes under the judgement that already governs these fields. Its `pass=false` is the recorded-failure form that X5-R30 (c) rule 5 describes, not a fold regression in the judged sense.
      - MP-A has 11 rounds in the reference file and 10 in the tip's 10k, and every one of the 21 meets `(skx ∨ smx) ∧ pass_na`.
      - In 7078 r0, the overwritten slot is checkpoint 0's map root (slot 4), rewritten after checkpoint 1's meta was durable. That is superseded-image reuse, which predates X5-R49, and not a `free_lpid` home.
    - **Still caught:**
      - a serve outside K and outside the opened meta slot (`skx=smx=false`);
      - `pass_na` false or null;
      - a Base round that was text-free, or a Base round with `mrb=false` or a non-null `c`. That is new reach for MP-A;
      - any O-2 text, or any O-3 text other than exactly `O-3(a): served corruption`;
      - any class MW serve. X5-R30 (c) rule 3 keeps every MW serve a served corruption.
- **(d) Reporting.** The comparator lists every row in full, never netted and never counted as a gain.
  - Rows: `TWIN_DDL_PENDING`, `MOVED_MP_PENDING` with its `META_ONLY_ELIGIBLE` subset, `MPA_PASS_FAIL`, `TWIN_HEALED` and `TEXT_CHANGED`, beside X5-R51's lists.
  - The README gives each exempt row's fields:
    - for (i), `N`, `k`, the T2 line and real round `k−1`'s `lost/mis/torn/frontier/hi/cover_err`;
    - for (ii), the findings, `media[…]` and `q`;
    - for (iii), the Base and tip `crash/o1/o3/mrb/served/skx/smx/pass_na`.
  - **Class sizes at both ends, reported and never netted:** the twin family's failing entries, MP-A, and (ii)'s JSON shape.
- **(e) Outside the rule.** Each of these is a **new failure**:
  - a bullet-1 entry that is neither (i) nor (iii);
  - a pending row whose single-seed check fails;
  - a bullet-2 round outside X5-R51 (b) and outside (ii);
  - any bullet-3 hit.

  **STOP:** write the rounds into the README, commit, and ESCALATE. Do not try a fix.
- **(f) The comparator, and who checks what.**
  - The comparator is `spikes/e4-recovery/tools/x5r52_compare.py`. Its exit code is 0 only if nothing outside the rule is found. That is necessary, not sufficient.
  - **The gate**, not a worker, runs `check-mp` on every `MOVED_MP_PENDING` row and `check-twin` on every `TWIN_DDL_PENDING` row, singly, on seeds below 10000 only. It may re-read single-seed outputs that a gate already recorded at the same commit, provided T5 holds for each.
  - A worker may produce twin evidence to validate the patch. The gate reproduces it on its own.
- **(g) -F2's re-seed voids (i).**
  - At any commit that carries -F2's re-seed, the twin chain is re-seeded whenever the two directories differ at round start, so T2 cannot hold.
  - Comparisons at such a commit run the comparator with `--no-twin-exemption`, and (i) exempts nothing there.
  - After -F2 lands, a twin failure of this family is a recurrence of a class that a landed card claims to have fixed. Under E4's owner rule, F4 counts it as new.

**Step 1 — the comparator and the patch (commit 8bbbfcec).**
`tools/x5r52_compare.py` (Python 3, stdlib only, `python3 -I`) with the
four subcommands; `tools/x5r52-twin-diag.patch` (eprintln-only, paths
relative to `nucleus/v3`, applied only to scratch exports). The patch
starts from the gate's `GATEDIAG` edits (the recovery-`Err` line, the
replay listing with `ddl`/`txn`, the apply-FAIL lines — the `tip/`→
`tipd/` diff) and adds the two X5R52 markers: `X5R52 run seed=<s>
round=<i> class=<C|Mp|Mw>` at `run_round`'s entry and `X5R52
round-start seed=<s> round=<i> real_dir=[…] twin_dir=[…]` in
`run_seed_with` after the broken-chain re-seed (MP/MW only; class C has
no twin chain).

- **Patch validation:** `git apply --check` clean on a fresh `HEAD`
  export; built there; single-seed runs of 609, 2859 and 2599 reproduce
  the 10k file's digests byte-identically (887984205, 2789281219,
  1546217554) — the patch moves no digest.
- **self-test:** 38 cases, 0 failed (every X5-R52 (c) still-caught
  bullet, an MW gain with and without `x33x ∧ cover_err`, MONITOR > 0,
  a pass→fail with a Base text that is not MP-A, `--no-twin-exemption`,
  a deadlock tip seed, a range mismatch and a sha256 mismatch).
- **Comparator mutants** (scratch copies under the run dir, never
  committed; each must fail `self-test`):

  | mutant | self-test |
  |---|---|
  | M-CMP-NO-PF (no bullet-1 check for rounds with a Base text) | FAIL (9 cases, incl. `pass-fail-base-text-not-mpa` and `mpa-pass-fail`) |
  | M-CMP-MPA-NOJUDGE (drop `(skx ∨ smx) ∧ pass_na`) | FAIL (`mpa-skx-smx-false`, `mpa-pass-na-false`) |
  | M-CMP-META-ANYSLOT ((b)5′ accepts `Some(n)`) | FAIL (`meta-only-some-finding`) |
  | M-CMP-TWIN-NODIR (`check-twin` skips T2) | FAIL (`twin-t2-both-dirs-lack-N`) |
  | M-CMP-TWIN-ANYCLASS (the prefilter accepts MP seeds) | FAIL (`twin-mp-seed-prefilter-fails`) |

  **M-CMP-NO-PF over the real 10k:** its output reads `MPA_PASS_FAIL 0
  []` — 7078 r0 drops out entirely (not a new failure either; it
  vanishes), which is the original inline comparator's gap, reproduced.

**Step 2 — the re-read of the same 10k under X5-R52 (this commit).**
No campaign was run. The sha256 of the run JSON checks against
`6b686266…739a` and the reference's against `f4d6…c923` (the Acceptance
line passes both to the comparator and exits 0). The `compare` output,
in full:

```
DIGESTS_MOVED 2589 FAILING_ROUNDS 19 MONITOR 0
MOVED_GAINED 87 [(47, 1), (339, 0), (547, 0), (779, 0), (797, 0), (799, 0), (967, 0), (1299, 0), (1319, 0), (1367, 0), (1567, 0), (1578, 1), (1708, 1), (1747, 1), (1749, 2), (1768, 0), (1927, 1), (1939, 0), (1999, 0), (2079, 0), (2189, 0), (2208, 0), (2258, 0), (2307, 1), (2517, 0), (2859, 1), (2979, 0), (3099, 0), (3148, 0), (3377, 0), (3759, 2), (3838, 0), (3948, 0), (4148, 0), (4179, 0), (4279, 0), (4399, 1), (4439, 0), (4499, 0), (4537, 0), (4548, 0), (4558, 0), (4567, 1), (4647, 0), (4659, 1), (4749, 0), (5077, 0), (5279, 0), (5328, 1), (5418, 0), (5499, 0), (5549, 2), (5798, 1), (6178, 1), (6189, 0), (6368, 0), (6369, 0), (6387, 2), (6417, 0), (6539, 0), (6719, 0), (6877, 0), (6917, 0), (7219, 0), (7257, 0), (7368, 1), (7419, 1), (7469, 1), (7658, 0), (7669, 0), (7727, 1), (7958, 1), (8038, 0), (8139, 2), (8278, 0), (8639, 0), (8748, 2), (8788, 1), (8869, 2), (8898, 0), (9388, 1), (9508, 0), (9547, 0), (9637, 0), (9767, 0), (9818, 1), (9967, 1)]
MOVED_MP_PENDING 52 [(47, 1), (547, 0), (797, 0), (967, 0), (1367, 0), (1567, 0), (1578, 1), (1708, 1), (1747, 1), (1768, 0), (1927, 1), (2208, 0), (2258, 0), (2307, 1), (2517, 0), (3148, 0), (3377, 0), (3838, 0), (3948, 0), (4148, 0), (4537, 0), (4548, 0), (4558, 0), (4567, 1), (4647, 0), (5077, 0), (5328, 1), (5418, 0), (5798, 1), (6178, 1), (6368, 0), (6387, 2), (6417, 0), (6877, 0), (6917, 0), (7257, 0), (7368, 1), (7658, 0), (7727, 1), (7958, 1), (8038, 0), (8278, 0), (8748, 2), (8788, 1), (8898, 0), (9388, 1), (9508, 0), (9547, 0), (9637, 0), (9767, 0), (9818, 1), (9967, 1)]
META_ONLY_ELIGIBLE 4 [(1927, 1), (7257, 0), (7658, 0), (8278, 0)]
MPA_PASS_FAIL 1 [(7078, 0)]
TWIN_DDL_PENDING 2 [(2599, 2), (2859, 1)]
MOVED_LOST 94 [(47, 0), (179, 0), (397, 0), (579, 0), (967, 1), (1018, 0), (1107, 0), (1219, 0), (1367, 1), (1387, 0), (1578, 2), (1588, 0), (1598, 0), (1708, 2), (1747, 0), (1919, 1), (1927, 0), (1949, 0), (2099, 0), (2159, 0), (2259, 0), (2307, 0), (2389, 0), (2397, 0), (2517, 1), (2778, 0), (3029, 0), (3289, 0), (3377, 1), (3397, 0), (3548, 0), (3739, 0), (3759, 0), (3768, 0), (3947, 0), (4118, 0), (4359, 0), (4398, 0), (4399, 0), (4419, 1), (4518, 0), (4537, 1), (4567, 0), (4989, 1), (5129, 0), (5188, 0), (5328, 0), (5427, 0), (5479, 0), (5499, 1), (5619, 0), (5769, 2), (5798, 0), (6178, 0), (6387, 1), (6417, 1), (6458, 0), (6557, 0), (6909, 0), (6917, 1), (6938, 0), (7197, 0), (7219, 1), (7278, 0), (7298, 0), (7368, 0), (7419, 0), (7439, 1), (7447, 0), (7727, 2), (7889, 0), (7958, 0), (8257, 0), (8278, 1), (8307, 0), (8547, 0), (8639, 1), (8748, 0), (8788, 0), (9059, 0), (9159, 2), (9177, 0), (9329, 1), (9387, 0), (9388, 0), (9419, 0), (9429, 1), (9508, 2), (9618, 0), (9637, 1), (9717, 0), (9818, 0), (9967, 0), (9999, 0)]
HEALED 3 [(457, 0), (1298, 1), (8777, 0)]
TWIN_HEALED 1 [(6539, 1)]
TEXT_CHANGED 131 [(97, 0), (117, 0), (199, 1), (527, 1), (617, 0), (678, 0), (798, 0), (857, 0), (1117, 0), (1127, 1), (1159, 0), (1197, 0), (1217, 0), (1337, 0), (1417, 0), (1447, 0), (1477, 0), (1628, 0), (1637, 0), (1798, 0), (1899, 0), (1917, 0), (1948, 1), (1959, 0), (1977, 0), (2028, 0), (2078, 0), (2097, 1), (2268, 0), (2377, 0), (2537, 0), (2567, 0), (2597, 0), (2638, 0), (2698, 0), (2757, 0), (2787, 0), (2828, 0), (2939, 0), (2977, 2), (2978, 0), (3068, 0), (3118, 0), (3227, 0), (3259, 1), (3357, 0), (3358, 0), (3438, 0), (3638, 0), (3817, 0), (3927, 0), (4038, 0), (4127, 1), (4247, 0), (4268, 0), (4297, 0), (4337, 0), (4378, 0), (4468, 0), (4489, 0), (4648, 0), (4657, 0), (4677, 0), (4699, 0), (4737, 0), (4758, 0), (5047, 1), (5078, 0), (5087, 0), (5148, 1), (5227, 1), (5298, 0), (5338, 1), (5378, 0), (5517, 0), (5618, 0), (5907, 0), (5958, 0), (6078, 0), (6138, 0), (6248, 0), (6377, 0), (6457, 0), (6517, 0), (6558, 0), (6627, 0), (6638, 0), (6677, 0), (6758, 0), (6807, 0), (6848, 0), (6977, 0), (6987, 0), (6997, 0), (7007, 0), (7098, 0), (7127, 0), (7227, 0), (7297, 0), (7317, 0), (7337, 0), (7338, 0), (7357, 0), (7399, 0), (7457, 1), (7698, 0), (7917, 0), (8098, 0), (8138, 0), (8268, 0), (8287, 0), (8309, 1), (8418, 0), (8617, 0), (8618, 0), (8638, 0), (8687, 0), (8977, 0), (8998, 0), (9028, 0), (9078, 1), (9107, 1), (9228, 0), (9398, 0), (9417, 0), (9528, 0), (9617, 0), (9657, 0), (9827, 0), (9878, 0), (9959, 0)]
TWIN_FAILING 7 [(609, 1), (849, 1), (2599, 2), (2859, 1), (3449, 1), (5049, 1), (7179, 1)] BASE 6
NEW_FAILING 0 [] BAD_SEEDS 0 []
```

**Exit 0, `NEW_FAILING 0`.** Every row is inside X5-R52: no STOP.

- **`check-mp` on every `MOVED_MP_PENDING` row**, reading the gate's
  recorded `m/pend/s<seed>.{err,json,out}` (read-only, no runs; every
  digest equals the 10k file's and every run's own line reads
  `MONITOR=0`, so T5 holds): **48 `EXPLAINED (b)5`, 4 `EXPLAINED (b)5'`**
  — the four are 1927 r1, 7257 r0, 7658 r0 and 8278 r0, exactly the
  `META_ONLY_ELIGIBLE` set. Table (findings' slots, then `tainted`, or
  the (b)5′ media list):


| seed | round | verdict and evidence |
|---|---|---|
| 47 | 1 | EXPLAINED (b)5 5 finding(s) slots [9, 47, 85, 36, 11] tainted [9, 11, 36, 47, 85] |
| 547 | 0 | EXPLAINED (b)5 2 finding(s) slots [66, 167] tainted [66, 167] |
| 797 | 0 | EXPLAINED (b)5 2 finding(s) slots [13, 40] tainted [13, 40] |
| 967 | 0 | EXPLAINED (b)5 1 finding(s) slots [10] tainted [10] |
| 1367 | 0 | EXPLAINED (b)5 1 finding(s) slots [203] tainted [203] |
| 1567 | 0 | EXPLAINED (b)5 1 finding(s) slots [10] tainted [1, 10] |
| 1578 | 1 | EXPLAINED (b)5 4 finding(s) slots [46, 16, 51, 142] tainted [16, 46, 51, 142] |
| 1708 | 1 | EXPLAINED (b)5 1 finding(s) slots [16] tainted [16] |
| 1747 | 1 | EXPLAINED (b)5 5 finding(s) slots [67, 31, 41, 80, 68] tainted [31, 41, 67, 68, 80] |
| 1768 | 0 | EXPLAINED (b)5 2 finding(s) slots [11, 13] tainted [11, 13] |
| 1927 | 1 | EXPLAINED (b)5' 1 finding(s), media [Misdirected to_off=4096 inode=0 off=8192, Lost inode=0 off=98304, Torn { sectors: [717824, 718848, 719360, 719872] } inode=0 off=716800] |
| 2208 | 0 | EXPLAINED (b)5 1 finding(s) slots [53] tainted [53] |
| 2258 | 0 | EXPLAINED (b)5 1 finding(s) slots [68] tainted [68] |
| 2307 | 1 | EXPLAINED (b)5 2 finding(s) slots [8, 104] tainted [8, 104] |
| 2517 | 0 | EXPLAINED (b)5 1 finding(s) slots [12] tainted [12] |
| 3148 | 0 | EXPLAINED (b)5 1 finding(s) slots [90] tainted [90] |
| 3377 | 0 | EXPLAINED (b)5 1 finding(s) slots [5] tainted [5] |
| 3838 | 0 | EXPLAINED (b)5 1 finding(s) slots [4] tainted [4] |
| 3948 | 0 | EXPLAINED (b)5 2 finding(s) slots [27, 70] tainted [27, 70] |
| 4148 | 0 | EXPLAINED (b)5 2 finding(s) slots [117, 77] tainted [77, 117] |
| 4537 | 0 | EXPLAINED (b)5 1 finding(s) slots [18] tainted [18] |
| 4548 | 0 | EXPLAINED (b)5 2 finding(s) slots [31, 103] tainted [31, 103] |
| 4558 | 0 | EXPLAINED (b)5 1 finding(s) slots [10] tainted [10] |
| 4567 | 1 | EXPLAINED (b)5 5 finding(s) slots [42, 119, 61, 106, 126] tainted [42, 61, 106, 119, 126] |
| 4647 | 0 | EXPLAINED (b)5 3 finding(s) slots [115, 41, 47] tainted [41, 47, 115] |
| 5077 | 0 | EXPLAINED (b)5 2 finding(s) slots [5, 3] tainted [3, 5] |
| 5328 | 1 | EXPLAINED (b)5 1 finding(s) slots [136] tainted [2, 136] |
| 5418 | 0 | EXPLAINED (b)5 1 finding(s) slots [9] tainted [1, 9] |
| 5798 | 1 | EXPLAINED (b)5 2 finding(s) slots [39, 83] tainted [39, 83] |
| 6178 | 1 | EXPLAINED (b)5 3 finding(s) slots [141, 121, 37] tainted [37, 121, 141] |
| 6368 | 0 | EXPLAINED (b)5 1 finding(s) slots [15] tainted [15] |
| 6387 | 2 | EXPLAINED (b)5 3 finding(s) slots [115, 126, 207] tainted [115, 126, 207] |
| 6417 | 0 | EXPLAINED (b)5 2 finding(s) slots [46, 102] tainted [46, 102] |
| 6877 | 0 | EXPLAINED (b)5 3 finding(s) slots [12, 9, 10] tainted [9, 10, 12] |
| 6917 | 0 | EXPLAINED (b)5 1 finding(s) slots [3] tainted [3] |
| 7257 | 0 | EXPLAINED (b)5' 1 finding(s), media [Misdirected to_off=8192 inode=0 off=4096, Misdirected to_off=77824 inode=0 off=0] |
| 7368 | 1 | EXPLAINED (b)5 4 finding(s) slots [9, 66, 74, 100] tainted [9, 66, 74, 100] |
| 7658 | 0 | EXPLAINED (b)5' 1 finding(s), media [Misdirected to_off=8192 inode=0 off=4096, Torn { sectors: [17408, 18944, 19456] } inode=0 off=16384, Lost inode=0 off=4096] |
| 7727 | 1 | EXPLAINED (b)5 4 finding(s) slots [75, 56, 202, 99] tainted [56, 75, 99, 202] |
| 7958 | 1 | EXPLAINED (b)5 2 finding(s) slots [54, 28] tainted [28, 54] |
| 8038 | 0 | EXPLAINED (b)5 2 finding(s) slots [16, 19] tainted [16, 19] |
| 8278 | 0 | EXPLAINED (b)5' 1 finding(s), media [Torn { sectors: [4096, 5120, 6656, 7168, 7680] } inode=0 off=4096] |
| 8748 | 2 | EXPLAINED (b)5 6 finding(s) slots [89, 8, 198, 141, 189, 23] tainted [8, 23, 89, 141, 189, 198] |
| 8788 | 1 | EXPLAINED (b)5 4 finding(s) slots [25, 26, 33, 154] tainted [25, 26, 33, 154] |
| 8898 | 0 | EXPLAINED (b)5 1 finding(s) slots [57] tainted [57] |
| 9388 | 1 | EXPLAINED (b)5 2 finding(s) slots [27, 40] tainted [27, 40] |
| 9508 | 0 | EXPLAINED (b)5 2 finding(s) slots [14, 21] tainted [14, 21] |
| 9547 | 0 | EXPLAINED (b)5 1 finding(s) slots [108] tainted [108] |
| 9637 | 0 | EXPLAINED (b)5 1 finding(s) slots [120] tainted [120] |
| 9767 | 0 | EXPLAINED (b)5 1 finding(s) slots [132] tainted [132] |
| 9818 | 1 | EXPLAINED (b)5 5 finding(s) slots [65, 10, 33, 113, 105] tainted [10, 33, 65, 105, 113] |
| 9967 | 1 | EXPLAINED (b)5 4 finding(s) slots [74, 41, 38, 29] tainted [29, 38, 41, 74] |

- **The twin evidence** — single-seed runs of 2599, 2859 and 609 in the
  patched scratch build (Work hygiene: seed < 10000,
  `ulimit -v 25165824`, output outside the worktree), then `check-twin`
  on each. 2599 and 2859 are the pending rows; 609 is a twin failure of
  the reference file, here as evidence of the pre-existing shape (the
  gate reproduces 2599 and 2859 on its own):


| seed | twin round | `check-twin` | digest |
|---|---|---|---|
| 2599 | 2 | `EXPLAINED N=3 k=2 (real round 1: lost=1 mis=0 torn=1 cover_err=False frontier=1 hi=5)` | 1546217554 |
| 2859 | 1 | `EXPLAINED N=4 k=1 (real round 0: lost=0 mis=1 torn=0 cover_err=False frontier=1 hi=7)` | 2789281219 |
| 609 | 1 | `EXPLAINED N=5 k=1 (real round 0: lost=0 mis=0 torn=2 cover_err=False frontier=2 hi=8)` | 887984205 |

T2's line for each (the round-start line the verdict reads):

```
X5R52 round-start seed=2599 round=2 real_dir=[1, 2, 3] twin_dir=[1, 2]
X5R52 round-start seed=2859 round=1 real_dir=[1, 2, 3, 4] twin_dir=[1, 2, 3]
X5R52 round-start seed=609 round=1 real_dir=[1, 2, 3, 4, 5] twin_dir=[1, 2, 3, 4]
```

- **7078 r0, the (iii) row, at both ends:**

| end | crash | o1 | o3 | mrb | served | skx | smx | pass_na | c | q[15] | pass |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Base | `uniform@y:5875` | `O-1(a) at c=5: published {} != model {2, 3, 4}` | null | true | false | false | false | null | null | 1 | true |
| tip | `uniform@y:10344` | `O-1(a) at c=15: published {} != model {2, 3, 4}` | `O-3(a): served corruption` | true | true | false | true | true | null | 1 | false |

  A served round passes iff `(skx ∨ smx) ∧ pass_na` (X5-R30 (c) rule 4):
  `smx ∧ pass_na` holds, so the round's `pass=false` is the
  recorded-failure form of rule 5, not a fold regression — `MPA_PASS_FAIL`,
  exempt under (c)(iii).

- **The class sizes at both ends, reported and never netted:**
  - the twin family (MW seeds, twin `pass=false` with an O-1(a)
    `published {}` text): **Base 6** — 609 r1, 849 r1, 3449 r1, 5049 r1,
    6539 r1, 7179 r1 — **tip 7** (6539 heals; 2599 r2 and 2859 r1 gain);
  - **MP-A** (MP rounds, `pass=false`, `served=true`, O-3(a)): **Base 11**
    (457, 537, 1887, 2927, 4098, 7238, 7528, 8107, 8728, 8777, 9358, all
    r0), **tip 10** (457 and 8777 heal; 7078 gains); every one of the 21
    meets `(skx ∨ smx) ∧ pass_na`;
  - **(ii)'s JSON shape** (MP rounds with `c` null, `frontier` 0,
    `mrb=false`, `served=false`, `q[15]=1`, `q[12..14]=0`): **Base 187,
    tip 190**.

- **The 0..1000 calibration** (F2's five `m/tip_*.json` merged against
  the Base 1k file — `gate-X-5c-F4-F1-F1-F3/m/b1k.json`; the card's path
  names the F2-F1 gate dir, but `b1k.json` lives in the F3 gate dir,
  the same file F2-F1's record cites — `--range 0..1000`, exit 0):

```
DIGESTS_MOVED 243 FAILING_ROUNDS 4 MONITOR 0
MOVED_GAINED 7 [(47, 1), (339, 0), (547, 0), (779, 0), (797, 0), (799, 0), (967, 0)]
MOVED_MP_PENDING 4 [(47, 1), (547, 0), (797, 0), (967, 0)]
META_ONLY_ELIGIBLE 0 []
MPA_PASS_FAIL 0 []
TWIN_DDL_PENDING 0 []
MOVED_LOST 5 [(47, 0), (179, 0), (397, 0), (579, 0), (967, 1)]
HEALED 1 [(457, 0)]
TWIN_HEALED 0 []
TEXT_CHANGED 8 [(97, 0), (117, 0), (199, 1), (527, 1), (617, 0), (678, 0), (798, 0), (857, 0)]
TWIN_FAILING 2 [(609, 1), (849, 1)] BASE 2
NEW_FAILING 0 [] BAD_SEEDS 0 []
```

  Exactly X5-R51's own calibration (7 gained, the same four MP pendings,
  5 lost, healed `[(457, 0)]`, 0 new) with every X5-R52 row empty. (The
  `TEXT_CHANGED` count is 8 where F2-F1's hand table said 9: that table
  also counted 457 r0, which both rules classify `HEALED`, so the
  comparator counts it once, there.)

**Nothing outside X5-R52: no STOP.**

## RESULT (card X-5c-F4-F1-F1-F3-F2-F1-F1)

card: X-5c-F4-F1-F1-F3-F2-F1-F1   branch: card/v3-X-5c-F4-F1-F1-F3-F2-F1-F1   commit: (this commit; the chain 10ee2e52 → 8bbbfcec → f21a2247 → here)

The seven RESULT items the card requires:

1. **X5-R51 quoted unchanged, X5-R52 verbatim, and the numbering note**
   — the section above ("Card X-5c-F4-F1-F1-F3-F2-F1-F1's record"),
   step 0. X5-R51's text and X5-R52's text were extracted verbatim from
   the card before any code of this card, and neither was edited after
   step 0.
2. **The `compare` output over the 10k, in full, with the sha256 of
   both files** — step 2 above. Run JSON
   `…acc.campaign.json` sha256
   `6b6862665f9609e3513a6bc7f84b2bfe3370e919a775bf9a1e91450482dc739a`;
   reference `tip-10k.json` sha256
   `f4d6f1ff0c2731b9ecbb3ca862534e168a11dd2f99beb1cdb1a3828b5310c923`
   (both checked by the Acceptance line itself, exit 0).
3. **The `check-mp` table (52 rows) and the `check-twin` rows** — step 2
   above: 48 `EXPLAINED (b)5`, 4 `EXPLAINED (b)5'`, none NOT-EXPLAINED;
   2599 `N=3 k=2` (digest 1546217554), 2859 `N=4 k=1` (2789281219),
   609 `N=5 k=1` (887984205).
4. **7078 r0's (iii) row at both ends** — step 2 above.
5. **The class sizes at both ends** — step 2 above: twin family 6 → 7,
   MP-A 11 → 10, (ii)'s JSON shape 187 → 190.
6. **Post-data amendments: X5-R52.** It was written by P **after** the
   F2-F1 verdict (`verdicts/X-5c-F4-F1-F1-F3-F2-F1.md` at `8992f77f`),
   after F2-F1's acceptance 10k (`…acc.campaign.json`, the file this
   card re-reads, its `.campaign.out` and the old comparator's
   `NEW_FAILING 2` output), after the reference `tip-10k.json`, and
   after the gate's single-seed evidence in
   `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F3-F2-F1/m/` (`b5.txt`,
   `pend/`, `d_*`, `e_*`, `tip_{2599,2859,7078}.*`). **The rounds it
   moved were all seen before it was written:** 2599 twin r2 and 2859
   twin r1 (exemption (i)), 7078 r0 (exemption (iii)), and 1927 r1,
   7257 r0, 7658 r0 and 8278 r0 (exemption (ii)). Each exemption is
   drawn around a class that already exists in the reference file, never
   around those seven rounds. In the pattern of X5-R30 (c) and X5-R32.
7. **The comparator mutant table and the 0..1000 calibration reading** —
   step 1 (mutants) and step 2 (calibration) above.

acceptance (this attempt): `cargo test -q --release -p e4-recovery` —
**262 passed, 0 failed** (nothing in the crate changed; green as at
`eb9c4328`); `python3 -I …/x5r52_compare.py self-test` — **38 cases,
0 failed**; the scratch-export `git apply --check` of
`tools/x5r52-twin-diag.patch` — clean, and the patched build reproduces
the 10k digests of 609/2859/2599; the Acceptance `compare` line — exit
0; `scripts/gate.sh` — run last, `GATE EXIT=0`.

judgment calls (all recorded in the sections above): the comparator
prints `TWIN_FAILING <n> <list> BASE <m>` (the class size at both ends,
never netted), and counts `FAILING_ROUNDS` from the run JSON (it equals
the E4 FOLD sum on every file read); `check-mp`/`check-twin` exit 0 on
EXPLAINED and 1 on NOT-EXPLAINED; the 0..1000 calibration's Base file is
`gate-X-5c-F4-F1-F1-F3/m/b1k.json` (the card's path names the F2-F1
gate dir, but the file lives in the F3 gate dir — the same file F2-F1's
record cites); a twin entry that fails at both ends with a changed text
is reported in `TWIN_FAILING` only (X5-R51 (a) keeps twins out of
`TEXT_CHANGED`); the round-start marker prints for MP/MW only (class C
has no twin chain to read).

ESCALATE: none. anomalies: the `git add`/`git commit` pair prints a
benign `Unable to create …/.git/packed-refs.lock: Read-only file system`
line from the host's git maintenance in this sandbox; the commits land
(the log and the clean tree confirm). F2-F1's hand table counted 457 r0
in its "changed texts (9)"; the comparator counts it once, as `HEALED`
(the calibration's `TEXT_CHANGED 8`), because both rules classify the
Base-`pass=false` shape healed first.

### Re-verification attempt (2026-11-07, after the 60-min kill and the
### quota stop)

Attempt 1 (killed at 60 min) left all four step commits on the branch
(`10ee2e52 → 8bbbfcec → f21a2247 → 689db626`) with a clean tree, so the
restart rule resumes at the first step with no commit: there is none.
This attempt re-verified every recorded reading and re-ran every
Acceptance line for real. Nothing outside `tools/`+this README changed;
`git status` stayed clean throughout.

- **Acceptance 1** `cargo test -q --release -p e4-recovery`: exit 0,
  **262 passed, 0 failed** (18+0+25+43+55+28+50+43+0), green as at
  `eb9c4328`.
- **Acceptance 2** `self-test`: exit 0, **38 cases, 0 failed**.
- **Acceptance 3** the scratch-export `git apply --check` of
  `tools/x5r52-twin-diag.patch`: exit 0 on a fresh `git archive HEAD
  nucleus` export. (The sandbox's shell guard rejects `git -C`, so the
  archive was taken from the worktree root — the same repository state
  `git -C ../..` names; the `rm -rf` scratch reset was replaced by a
  fresh directory name, which is equivalent for a one-shot extract.)
- **Acceptance 4** the 10k `compare` line, verbatim: exit 0, both
  sha256 checks green, and exactly P's reference reading —
  `DIGESTS_MOVED 2589 FAILING_ROUNDS 19 MONITOR 0`, `MOVED_GAINED 87`,
  `MOVED_MP_PENDING 52`, `META_ONLY_ELIGIBLE 4 [(1927,1),(7257,0),
  (7658,0),(8278,0)]`, `MPA_PASS_FAIL 1 [(7078,0)]`, `TWIN_DDL_PENDING
  2 [(2599,2),(2859,1)]`, `MOVED_LOST 94`, `HEALED 3 [(457,0),(1298,1),
  (8777,0)]`, `TWIN_HEALED 1 [(6539,1)]`, `TEXT_CHANGED 131`,
  `TWIN_FAILING 7 BASE 6`, `NEW_FAILING 0`, `BAD_SEEDS 0`.
- **Acceptance 5** `bash scripts/gate.sh` (full log kept at
  `${CARGO_TARGET_DIR}/gate-full.out`): `STEP test EXIT=0`, `STEP clippy
  EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`,
  `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=1`
  with exactly **one** FAIL row — `runner e2e: K: SIGTERM kills opencode
  and closes the ledger range` — so `GATE EXIT=1`. **This is the known,
  ruled row, not a finding of this card.** Verdict
  `verdicts/X-5c-F4-F1-F1.md` (K ruling): "`kill_child` is correct.
  `dead()` asks the wrong question: `killpg` succeeds on a zombie-held
  group"; a worker's own-session gate reads red on that row alone "when
  opencode as PID 1 reaps no orphans… It is the only row. The runner's
  Acceptance is the truth. Do not chase it, and do not run a subreaper
  to make it pass." This container matches the ruling's premise:
  `/proc/1/comm` = `opencode`. The fix is queued as card
  X-5c-F4-F1-F1-F1 ("does not block the held card or F4"), and this
  card's Touch-only list forbids `scripts/` anyway. CXX-F1's subreaper
  wrapper (recorded above, before the ruling) is overruled for cards
  after it; F2-F1's own record already carried the row as a known red.
  My probes match the ruled mechanism: at check time the fake's pid is
  dead by every oracle (`kill(pid,0)→ESRCH`, no `/proc/<pid>`, `ps`
  empty; the pid allocator is sequential with no `ns_last_pid` rewind —
  verified by a 21k-sample monitor running concurrently with a failing
  suite — and ~23M kill probes over isolated kill+reap repros never
  fired), yet `killpg(pgid,·)` succeeds on the orphaned `sleep`'s
  unreaped zombie holding the group, exactly as the verdict states.
- **The 0..1000 calibration** re-run exactly as recorded: 243/4/0,
  gained 7, MP pending `[(47,1),(547,0),(797,0),(967,0)]`, empty X5-R52
  rows, lost 5, healed `[(457,0)]`, `TEXT_CHANGED 8`, twin failing 2
  BASE 2, `NEW_FAILING 0`, exit 0.
- **All 52 `check-mp` rows** re-derived from the gate's `m/pend/`
  recordings: **48 `EXPLAINED (b)5`, 4 `EXPLAINED (b)5'`** (1927 r1,
  7257 r0, 7658 r0, 8278 r0 — the `META_ONLY_ELIGIBLE` four), none
  NOT-EXPLAINED, no missing file.
- **The three `check-twin` rows** re-derived from attempt 1's preserved
  patched single-seed runs (`${CARGO_TARGET_DIR}/x5r52/s*.{err,json,
  out}`; T5's digest/MONITOR checks inside): 2599 twin r2 `EXPLAINED
  N=3 k=2` (real round 1: lost=1 mis=0 torn=1, frontier=1 hi=5), 2859
  twin r1 `EXPLAINED N=4 k=1` (real round 0: mis=1, frontier=1 hi=7),
  609 twin r1 `EXPLAINED N=5 k=1` (real round 0: torn=2, frontier=2
  hi=8) — the pre-existing shape, as evidence. The gate reproduces 2599
  and 2859 on its own per (f).
- **Mutation evidence re-applied this attempt** to fresh scratch copies
  under `CARGO_TARGET_DIR` (never the worktree; the tree stayed clean):
  M-CMP-NO-PF — `self-test` FAIL, 9 cases incl. `pass-fail-base-text-
  not-mpa`, and over the real 10k `MPA_PASS_FAIL 0 []` with `7078`
  absent from the entire output, exit 0 (the original comparator's gap,
  reproduced); M-CMP-MPA-NOJUDGE — FAIL `mpa-skx-smx-false`,
  `mpa-pass-na-false`; M-CMP-META-ANYSLOT — FAIL `meta-only-some-finding`;
  M-CMP-TWIN-NODIR — FAIL `twin-t2-both-dirs-lack-N`; M-CMP-TWIN-ANYCLASS
  — FAIL `twin-mp-seed-prefilter-fails`. No UNCAUGHT.

anomaly: attempt 1's RESULT paragraph above records `scripts/gate.sh`
ending `GATE EXIT=0`; this attempt cannot reproduce that reading in this
container (`opencode` as PID 1) without the subreaper wrapper the
X-5c-F4-F1-F1 verdict later forbade, and attempt 1's log was killed
before its end. The one row that differs is the ruled `runner e2e: K`
row; every other gate step is green here, and every other recorded
number of this card reproduces exactly as written above.


## Card X-5c-F4-F1-F1-F3-F2-F1-F1-F1's record (X5-R53, P's post-data
## amendment to X5-R52; check-twin narrowed to the twin's own segments,
## T6 the ts-level link, the MP-A class cap, MONITOR as a stop; the same
## 10k re-read again)

The re-verdict of the X5-R52 card
(`verdicts/X-5c-F4-F1-F1-F3-F2-F1-F1.md` at `26bcee4b`,
MERGE-WITH-FOLLOWUPS) left two follow-ups on X5-R52's comparator and
rule. Neither changes the reading of F2-F1's 10k; both have to land
before card X-5c-F4-F1-F1-F3-F2-F1-F2's 10k, because that run uses this
comparator:

- **The comparator is wider than X5-R52.** `check-twin`'s T1 accepted
  `Corrupt("tree N missing in directory")` from any class-C `Err` line
  for round `i` — including the uniform probe on the real chain and a
  pairing re-run. T3 likewise read probe segments. The gate built two
  adversarial logs that came back `EXPLAINED` under that reading, and
  four of its mutants survived `self-test`: G-EXIT-NOBAD, G-T4-NO-SHORT,
  G-TWIN-PREFILTER-ALLJ and G-T3-ANYCLASS.
- **X5-R52 dropped two parts of the verdict's proposals at `8992f77f`.**
  (c)(i) replaced the ts-level link with T3 and T4, and (c)(iii) dropped
  "while the class count does not grow". P rules on both below, **before
  any code**, and also on whether a run with MONITOR > 0 fails the
  comparator's exit by itself.

This card does three things: commits **X5-R53** with X5-R52 quoted
unchanged, narrows `tools/x5r52_compare.py` to X5-R52 as written and to
X5-R53 while closing the self-test gaps, and re-reads the **same** 10k
JSON requiring the **same** reading. No campaign is run again and no
Rust changes (X5-R31 run hygiene: everything reads recorded files).

**Numbering.** The highest X5-R number in use in the README, the cards,
the verdicts, QUEUE and PLAN is R52 (checked in the cards tree and on
every `card/v3-*` branch on 2026-11-08). **This rule is X5-R53.**

### P's three rulings (the reasoning; X5-R53 below is the binding text)

1. **The ts-level check: restored, as a new check T6, with its
   real-chain half rewritten.**
   - The check can be computed from recorded fields. Twin listings carry
     each record's `ts` and `ddl`, the replay-start line carries
     `meta.t`, and the real listing and JSON give the real side.
   - T1–T5 show that the directories diverged right after a faulted,
     short real recovery. They do not tie that divergence to one
     DropTree in that round. Without T6, a tree the real chain dropped
     and then re-created, or a drop the twin itself replays in the
     failing recovery, passes.
   - **The proposal's real-side test, "its frontier is below that ts",
     compares two different clocks.** `frontier` is the real chain's and
     the DropTree's `ts` is the twin's. The clocks do drift apart: in
     2599 r1 the twin's CreateTree is `ts=5` and the real chain's is
     `ts=1`. Read literally, the test would pass a real round that did
     replay the drop whenever the twin's clock is ahead, and refuse a
     genuine loss whenever it is behind.
   - So T6 keeps the proposal's intent on one clock: the real round's
     own replay listing, read against its own `frontier`. The
     `cover_err`/`x33x` alternative stays, because a refused recovery
     prints no listing.
2. **The MP-A cap: restored.**
   - It can be computed exactly from the two JSON files.
   - (iii) calls a bullet-1 regression "movement within MP-A". Movement
     within a class that X5-R30 keeps open is only a fair description
     while the class is not growing. Without the cap, a real rise in
     served corruption passes every comparison, one exempt row at a
     time, and shows up only as a number nobody is required to read.
   - An ESCALATE on a one-round rise is the cheap outcome: P looks, and
     rules. A silent rise is the expensive one.
   - -F2's 10k will use it, so it is fixed here, before that run.
3. **MONITOR > 0: it stops the comparison by itself.** X5-R51 (a)
   bullet 3 already calls a MONITOR hit a new failure. The comparator
   only voids exemptions on one. This makes the existing bullet
   testable, as X5-R52 (b) did for bullet 1.

### X5-R53 (P ruling, verbatim, fixed before this card's code; any change is an ESCALATE)

Copied verbatim from the card before any code of this card (the step-0
discipline, as X5-R6–R52 before it):

**X5-R53 (the X5-R52 amendment: `check-twin`'s attribution made exact, the ts-level link T6, the MP-A class cap, and MONITOR as a stop; a P rule, post-data; X5-R49, X5-R51's text and X5-R52's text unchanged).**
- **(0) What was known when this was written.** X5-R53 is a post-data amendment, and RESULT item 6 lists it as one. P wrote it **after** reading all of the following:
  - verdict `X-5c-F4-F1-F1-F3-F2-F1-F1` at `26bcee4b`, including both of the gate's adversarial logs;
  - F2-F1's 10k JSON (sha256 `6b6862665f9609e3513a6bc7f84b2bfe3370e919a775bf9a1e91450482dc739a`), the reference (`f4d6f1ff…c923`), and the gate's recorded `t_2599.*` and `t_2859.*`;
  - P's own scratch reference implementation of (b), (c) and (d), run over those files. Its reading is under "Expected" in Work step 4.

  **So P knew, before writing it, that:** both pending twin rows meet T6 (2599: `ts_D=7`, `meta.t=9`, one real record replayed, frontier 1; 2859: `ts_D=3`, `meta.t=7`, one real record replayed, frontier 1); MP-A is 11 rounds at the reference and 10 at the tip, all class MP and all `pass=false`; and the run's MONITOR is 0. **The rulings do not rest on those facts.** Each was decided on whether the check is computable from recorded fields and on what it rules out. Parts (b), (c) and (d) only remove exemptions. Part (e) only adds a stop. X5-R53 exempts no round that X5-R52 did not.
- **(a) Scope.**
  - X5-R53 amends X5-R52 for the same comparisons X5-R52 (a) names: this card's re-read of F2-F1's 10k, and every comparison in card X-5c-F4-F1-F1-F3-F2-F1-F2. **For -F2, P re-issues X5-R52 read with this amendment.**
  - X5-R52 (g) stands: at a commit carrying -F2's re-seed, (c)(i) is void, so (b) and (c) below have nothing to act on there. (d) and (e) apply there.
  - It carries to no other card unless P re-issues it.
  - **It is not an oracle change and not an E4 judgement rule.** `pass`, the texts, the fold, `verify`, every quota, X5-R1–R52, F4's class table and E4's owner rule are unchanged.
- **(b) T1 and T3 read the twin's own runs.** This states what X5-R52's "class-C (twin)" means; the comparator was wider than that text.
  - **Segments.** A diag line belongs to the most recent `X5R52 run seed=<s> round=<r> class=<K>` marker for seed `s`. A marker for another seed ends the segment.
  - **A twin segment of round `r`** is a `class=C` marker for round `r` that comes **after** that seed's `X5R52 round-start` line for round `r`.
    - That covers the twin's first run, its NEVER-FIRED re-runs and the X5-R21 (c) pairing re-runs, all of which run on the twin chain.
    - A `class=C` marker for round `r` **before** that line is the uniform probe, which runs on the real chain. It is never a twin segment.
  - **T1** becomes:
    - there is at least one class-C recovery `Err` line for round `i` inside a twin segment of round `i`; and
    - **every** class-C recovery `Err` line for round `i`, in any segment, has the same text, and that text is exactly `Corrupt("tree N missing in directory")`.

    A probe's `Err`, a pairing re-run's `Err` or any other class-C `Err` that differs is NOT-EXPLAINED T1.
  - **T3** reads twin segments only, for seed `s` only, at rounds `≤ i`. The rest of its text is unchanged.
  - T2, T4 and T5 are unchanged.
- **(c) T6, the ts-level link (a sixth check; all six must hold).** Let `N` be T1's tree and `k` be T4's round.
  - **T6a, the twin side.** Collect each `ts` of a commit record (`kind=1`) whose `ddl` contains `D<N>`, in the replay listings of the twin segments of round `k−1`. At least one must exist. For every twin-segment `Err` line of T1, let `t` be the `meta.t` of that segment's most recent `GATEDIAG replay start` line above it, which must exist. Every collected `ts` must be `≤` every such `t`.
    - So the twin's image at the failing recovery already holds the drop, and the record is in the twin run paired with real round `k−1`.
  - **T6b, the real side, on the real chain's own clock.** Real round `k−1` in the 10k JSON has `cover_err` true or `x33x` true; **or** both of these hold:
    - there is a real-class (`Mw` or `Mp`) segment of round `k−1` with a `GATEDIAG replay start` line;
    - the listing after the last such line, in the last such segment, has **no** commit record whose `ddl` contains `D<N>` at a `ts` at or below that round's JSON `frontier`.
  - **What T6b is not.** `frontier` is never compared with T6a's `ts`, because the two chains' clocks differ (2599 r1: twin CreateTree `ts=5`, real `ts=1`). T6 does not establish that a media fault rather than a crash cut lost the record. T4 carries that condition, unchanged.
  - **Still caught:** a drop at a `ts` above the failing recovery's `meta.t`; a drop listed only in a twin round other than `k−1`; a real round whose recovery replayed the drop at or below its frontier, so that `N` is in `real_dir` for some other reason; a real round with no listing and neither `cover_err` nor `x33x`.
- **(d) The MP-A class cap on (c)(iii).**
  - **The class.** The MP-A count of a file is the number of `rounds` entries with seed `class` `MP`, `served=true` and `mrb=true`, over the seeds of the comparison, including a `--range` slice. It is counted the same way at both ends.
  - **The cap.** If the tip's count is **greater** than the Base's, (iii) exempts nothing in that comparison: every round that would be `MPA_PASS_FAIL` is a new failure, with reason `bullet1-mpa-cap`. An equal or smaller count leaves (iii) as X5-R52 wrote it.
  - **Reporting.** The comparator prints `MPA_CLASS base <B> tip <T>` directly after `MPA_PASS_FAIL`, on every run.
  - **What the cap does not do.** A round can join MP-A without a bullet-1 row only by failing at both ends. No exemption is in play there, so the cap does not stop on it. The line is printed, and F4's class table judges the class.
  - **A one-for-one swap passes** (2 healed and 1 gained passes; so would 1 and 1). The cap is a bound on size, not a proof that no round is new. X5-R52 (c)(iii)'s per-round checks still apply to each row.
- **(e) MONITOR.** A run whose `E4 SEEDS=… MONITOR=` total is above 0 is a new failure by itself, with row `(-1, "run", None, "bullet3-monitor")`, so the comparator's exit is 1. X5-R51 (b)4's voiding stays. The comparator does not read the reference's MONITOR: F2-F1's run has MONITOR 0, and a run hit is one this comparison must stop on either way.
- **(f) Outside the rule.** X5-R52 (e) is extended by: a `TWIN_DDL_PENDING` row whose T6 fails; a `bullet1-mpa-cap` row; a `bullet3-monitor` row. **STOP:** write the rounds into the README, commit, and ESCALATE. Do not try a fix.

**Step 1 — `check-twin`: (b) and (c) (this commit).**
`tools/x5r52_compare.py` gains one line-walking helper,
`walk_seed_segments(diag_text, seed)`, which yields every diag line
with `(seg_id, seg)` — the segment's class and round, whether it is a
**twin segment** (a class-C marker after that seed's `round-start` line
for that round; a class-C marker before it is the uniform probe on the
real chain, and a marker for another seed ends the segment), and the
segment's latest `GATEDIAG replay start` `meta.t` above each line. T1,
T3 and T6 all read that one walk; T2, T4 and T5 keep their own scans
and texts. The check order is T5, T1, T2, T3, T4, T6, and T6 failures
read `NOT-EXPLAINED T6a/T6b <reason>`:

- **T1 (X5-R53 (b)).** At least one class-C recovery `Err` line for
  round `i` inside a twin segment of round `i`; every class-C `Err`
  line for round `i`, in any segment, the same text, exactly
  `Corrupt("tree N missing in directory")`. The probe's `Err`, a
  pairing re-run's `Err` or any other class-C `Err` that differs is
  NOT-EXPLAINED T1.
- **T3 (X5-R53 (b)).** Twin segments of this seed only, at rounds
  `≤ i`: a kind=1 record with `D<N>` in `ddl`.
- **T6a (X5-R53 (c)).** The `ts` of every kind=1 `D<N>` record in the
  twin segments of round `k−1` (at least one), every one `≤` the
  `meta.t` of each twin-segment `Err` line of T1 (each of which must
  have one above it).
- **T6b (X5-R53 (c)).** Real round `k−1` has `cover_err` or `x33x`;
  or the listing after the last `GATEDIAG replay start` line in the
  last real-class (`Mw`/`Mp`) segment of round `k−1` holds no kind=1
  `D<N>` record at a `ts` `≤` that round's JSON `frontier`.
  `frontier` is never compared with T6a's `ts` — the two chains' clocks
  differ.

`self_test`'s `twin_diag` was rewritten to the harness's real order
(per round: probe marker, `round-start`, twin marker, the twin's lines,
real marker, the real run's listing — the old fixture put the class-C
marker before its `round-start` line, so the twin read as a probe and
the `Err` line sat there), with a real listing in its round 0. Every
existing case keeps its name and its expected verdict; 15 cases were
added, each named for its check (`twin-t1-probe-only`,
`twin-t1-probe-no-twin-err`, `twin-t1-pairing-rerun-differs`,
`twin-t1-probe-differs-from-twin`, `twin-t1-identical-rerun-ok`,
`twin-t3-probe-only`, `twin-t3-mw-only`, `twin-t4-no-short`,
`twin-t4-cover-err-short`, `twin-t6-drop-above-meta`,
`twin-t6-drop-other-round`, `twin-t6-real-replayed-drop`,
`twin-t6-real-listed-above-frontier`, `twin-t6-no-real-listing`,
`twin-t6-cross-clock`). `self-test` now reads **53 cases, 0 failed**.
Against the prior gate's real recordings (read-only):
2599 twin r2 `EXPLAINED N=3 k=2 (… T6a ts=[7] meta.t=[9]; T6b no D3 at
or below frontier, real_nrec=1)` and 2859 twin r1 `EXPLAINED N=4 k=1
(… T6a ts=[3] meta.t=[7]; T6b no D4 at or below frontier, real_nrec=1)`
— the narrowed rules still exempt exactly the two rows, and the details
carry X5-R53's ts-level facts (RESULT item 4's fields).

**Step 2 — `compare`: (d), (e) and the exit code (this commit).**
`compare_reading` counts the MP-A class at both ends over the compared
seeds (rounds entries of seeds with class `MP` where `served` and `mrb`
are both true — the same count at both ends, range slices included) and
applies the cap after the round loop: a tip count **greater** than the
Base's moves every `MPA_PASS_FAIL` row into `new_failing` with reason
`bullet1-mpa-cap`. `format_rows` prints `MPA_CLASS base <B> tip <T>`
directly after `MPA_PASS_FAIL`, on every run (X5-R53 (d)). A run whose
MONITOR total is above 0 gains the `(-1, "run", None,
"bullet3-monitor")` new-failure row, so the exit is 1 (X5-R53 (e));
X5-R51 (b)4's voiding inside the prefilters is unchanged. The existing
`mpa-pass-fail` case's Base file gained a second MP seed holding an
MP-A round that heals at the tip (Base 1, tip 1 — without it the cap
would void the row the case asserts); its name and expected row stay.
Eight cases added: `mpa-cap-equal-keeps`, `mpa-cap-fewer-keeps`,
`mpa-cap-grown-voids`, `mpa-cap-counts-class-mp-only`, `mpa-class-line`,
`monitor-run-stops`, `exit-bad-seeds-only` (a deadlock seed at both
ends: no new failure, exit 1) and `twin-prefilter-fault-at-i-only` (the
only media-faulted short real round at `j = i`: `bullet1-twin`, not
pending). `self-test` reads **61 cases, 0 failed**, and the 10k
`compare` output is byte-identical to the chain-head comparator's
except the added `MPA_CLASS base 11 tip 10` line (the record is
below, step 4).


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

### Carried: X5-R36, verbatim (binding; pre-data; `verdicts/X-5c-F3-F3a-F2.md`; landed by X-5c-F3-F3a-F2-F1, committed as text by that card's step 0)

**X5-R36 (the drop incarnation: what the model reads after `DropTree`).**
- **Source.**
  - S-STORE §6.8: DropTree frees the tree's pages and removes its directory entry, and CreateTree allocates a fresh root leaf.
  - This verdict's ESCALATE ruling.
  - The workload re-uses a dropped tree's id in the next round's setup commit. That is a prototype deviation from §6.8's "never reused", and it stands, because changing the workload would move every seed with a drop round.
- **(a) The rule.**
  - For a user tree `t` and a cut `c`, let `d(t, c)` be the largest ts `≤ c` of a model commit holding `DropTree{t}`, or 0 if there is none.
  - `Model::scan_at(t, S, c)` is `Model::scan(t, S)` computed only over ops on `t` from commits with `d(t, c) < ts ≤ S`, and over loads of `t` with `d(t, c) < S_L ≤ S`. For `S ≤ d(t, c)` it is empty.
  - `Model::scan(t, S)` becomes `scan_at(t, S, S)`.
  - The catalog (tree 1) is never dropped. `published_at` is unchanged.
- **(b) The wiring.**
  - O-1(b) at candidate `c` compares against `scan_at(t, c, c)`.
  - O-1(d) at sample `S` compares against `scan_at(t, S, c)`.
  - Every harness call site that compares a store scan with the model at a read point `x` (`harness.rs` near 3286, 4516, 5123, and `o1_at_frontier_lines`) uses the cut that the compared store state recovered or read at.
- **(c) What it does not touch.** `lo`, `hi`, the candidate set, `sample_points`, O-1(a)/(c), O-1f, O-2, O-3, K, R, X5-R1/R33/R34/R35, `trees_at`, the sim, the fault and crash models, and the workload stream.
- **(d) It loosens nothing a correct store can satisfy.** It removes only the expectation of pre-drop content in a re-created tree, which §6.8 makes unservable. A store that serves pre-drop content in a re-created tree now fails O-1(b)/(d), where before it was required to.
- **(e) The id-reuse hazard is surfaced, never excluded.** The round record gains `reuse_on_dropping`, computed from SimDisk/opened-image data and never from store memory. It is true when the opened image's directory holds `t` as `dropping` and the model holds `CreateTree{t}` at a ts in `(T, frontier]`.
  - Such a round still runs the full oracle and counts by its outcome.
  - Any `reuse_on_dropping = true` in F1's windows or in F4's 10k is an ESCALATE to P with the round.
- **(f) Pre-data.** No run that judges by X5-R36 has been read. The gate's and the worker's 0..200 and single-seed runs are diagnosis, never judgement evidence.

## P ruling X5-R35, verbatim (the X-5c-F3-F3a rework gate, 2026-11-03;
## binding from the rework's first run; `verdicts/X-5c-F3-F3a.md` at
## 98313ae3 — committed before any run of the rework)

- **(a)** X5-R34's `E` is the slice of `disk.events` from the workload pass's first event to its end. The recovery and the harness boot checkpoint that precede the workload in `run_round`'s capture are not in `E`. (b) 1–5, (c)–(f) and (i) are otherwise unchanged. `Rc` and β are read over this `E`.
- **(b)** A new self-test (g) 6 must fail: a recovery truncation zero-write plus its sync covers an enqueued, never-written SyncOn record, which must not be acked. Show that it fails on 13da010b's derivation, then revert.
- **(c)** The rule. At the rework's tip, each of the 17 named rounds must leave `frontier < lo` or be recorded as moved (X5-R34 (h) 6). Any that keeps it is an (h) 3 store loss.
- **(d)** This raises no `lo` and tightens nothing else. It removes an unsatisfiable lower bound: a commit whose bytes never reached disk.

*(README annotation, not part of the verbatim text: the ruling's post-data
note — "the 17 rounds above were seen" — names the verdict's Finding 2
rounds: 205 r2, 381 r1, 403 r1, 774 r2, 1174 r2, 1203 r1, 3805 r2,
3883 r2, 4193 r1, 4601 r1, 4612 r2, 5030 r1, 7203 r2, 7803 r1, 8200 r2,
8404 r1 and 8825 r2.)*

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

**Step 6 — the C-XX / C-DIV remainder pass (X5-R16's treatment,
X5-R19's valve; time-boxed).** Start **2026-11-03T14:12:43−07:00**,
stop **2026-11-03T14:19:31−07:00** (the evidence below was gathered
inside the box; root-causing was not going to fit, and the Acceptance
commands need the rest of the attempt).

- **(b) Repros** (from the s4 0..200 run at the tip): **C-XX none**
  (0 in the window); **C-DIV 7, all r2** — (b) 112, 120, 152; (c) 63,
  104, 125, 186; (d) none in 0..200 (the class table's 62 (d) rounds
  lie outside the window). Each confirmed twice with identical
  digests (`X-5c-F3-F3a.s6.seed*.r{1,2}.json`).
- **(c) `E4_TXN_DIAG`** (new, stderr, default off, `ckpt.rs`'s
  recovery — per recovery, each marker's chunk count against the chunks
  the walk attached to its txn): **no mismatch on any repro, no
  unmarked-chunk line** — every marker's attached count equals its
  chunk count (186 r2: `txn=(4, 3) ts=16 chunk_count=23 attached=23`
  — P's finding measured 46 attached of 23 written; step 4 closed it).
  **Step 4 is complete; no (c) escalation.**
- **(d) P's leads:**
  1. **C-XX 151 r1 (sub-shape 2)** — passes at the tip (frontier 16 ≥
     lo 15): moved by step 4, exactly P's suspicion ("the chunked
     superseding replay of a foreign txn's chunks is the same
     mechanism") — with per-boot ids the foreign chunks never attach.
     The `E4_CXX_DIAG` perturbation question is moot: no repro remains
     in the window to re-confirm.
  2. **C-XX 150 r1 (Open XX001, two nested crashes)** — passes at the
     tip (`nested=2`, no XX001, no O-1/O-2/O-3 text).
  3. **The per-key cut** (new `E4_O1_DIAG`, stderr, default off,
     `harness.rs` after the oracle check — at the failing candidate,
     the first divergent live key per published tree with its owning
     model commit, and every raw entry above the cut):
     - **(b) 112 r2** @ c=15, tree 3, key 17: **store=None,
       model=Some**, owner ts=5 — `durable=false` (round-local: D
       measures only this round's span from p0), `replayed=true`
       (ts ≤ frontier 15), `le_T=true` (ts ≤ the opened meta's T);
     - **(c) 63 r2** @ c=23, tree 5, key 3: the same live shape; every
       raw-above entry is a **SyncOff** commit above lo (63: ts=24 ×15;
       125: ts=34–40 ×73; 186: ts=16 ×451 — 186's ts-16 commit is the
       23-chunk chunked txn);
     - the pattern holds on every repro examined (112, 63, 125, 186,
       104): **the recovered live scan misses an entry whose owning
       commit sits at ts ≤ T and ≤ the recovery frontier at every
       legal candidate** — one shared shape behind (b) and (c), with
       (c)'s rounds additionally holding SyncOff entries above the cut
       (the reported clause is whichever trips first per candidate).
- **(e) Outcome:** **not root-caused within the budget — ESCALATE with
  the evidence above** (X5-R16, X5-R19). Nothing points at the sim's
  crash or fault model; nothing needed S-1-F3's coverage check (no
  F3-F3b deferral). No fix, no pin, no test change in this step — the
  two diagnostics above are the only code, committed disabled. The
  `durable=false` flag is round-local and does not by itself prove the
  owner's record was never durable on disk (a prior round's durability
  is outside D's span) — the `le_T=true ∧ replayed=true ∧ missing`
  triple is the reproducible core P's triage should start from.

**Step 7 — the README (this section, the class table beside the
0..200 counts, "How to reproduce" and the Status line) is this
commit.** The 0..200 counts at each step's tip (all from
`X-5c-F3-F3a.s*` artifacts; the s0 baseline judges by the old
derivation and is timing-only, never cited):

| Step tip | FAILING_ROUNDS | C-XX | C-DIV | MW-ORPHAN | ack_over | C `0<frontier<lo` |
|---|---|---|---|---|---|---|
| step 0 (F3-F1's store; s0baseline is timing-only — it judges by the old derivation, never cited) | 17 | 2 | 9 | 6 | 0 | 2 (20 r2, 115 r1) |
| step 1 (X5-R34, 13da010b) | 15 | 2 | 7 | 6 | 0 | 0 |
| step 2 (the hop, ce8f8bbc; four single seeds) | — | — | — | — | 0 (per-seed) | 0 (the four boundary rounds cleared) |
| step 3 (two in flight, cbf0754c) | 18 | 0 | 7 | 9 | 0 | 0 |
| step 4 (per-boot ids, 8b71e560) | 18 | 0 | 7 (b 3, c 4) | 9 | 0 | 0 |

(C-DIV here is the class-table definition — a failing class-C round
outside C-XX carrying an `o1` — counted from each run's JSON. No
retune, no seed exclusion anywhere.)

**The rework (the REWORK verdict at 98313ae3, applied 2026-11-03; its
Findings 1/3, P ruling X5-R35 and Limits for the rework are binding).**
Commits, in order: **3c8b038e** (X5-R35 verbatim, before any run),
**93039525** (X5-R35 (a)/(b)), **313a236b** (Finding 1 + the two pins).

- **Pre-registration order.** The rulings commit 3c8b038e (X5-R35,
  beside X5-R33/X5-R34 above) precedes every `e4-campaign` command of
  the rework: the first runs are the single seeds below, all after it.
- **X5-R35 (a)/(b) — commit 93039525.** `E` is the workload pass's
  slice of the capture (`harness::x5r34_workload_events`;
  `pre_workload = start_disk.events.len()`, the recovery and harness
  boot checkpoint of the *previous* round that `new_round`'s placement
  leaves ahead of the workload in the capture); `derive_written_acked`,
  `x5r34_durable_prefix` and the `E4_ACK_DIAG` reads use it, and every
  other reader of the capture (Q12's rename fold, the monitors,
  X5-R5's R, X5-R28's `K_ii`) keeps the full stream, exactly as (a)
  scopes it. Self-test **(g) 6**
  (`x5r34_recovery_truncation_zeros_never_ack`): a recovery truncation
  zero-write plus its sync covering an enqueued, never-written SyncOn
  record never acks it (`D` stops at `p0`). **Demonstrated on
  13da010b's derivation** (the helper returned the whole capture in
  place — what `run_round` fed the derivation at 13da010b), then
  reverted:

  ```
  cargo test -q --release -p e4-recovery --test harness x5r34
  x5r34_recovery_truncation_zeros_never_ack --- FAILED
    'a SyncOn record the workload never wrote, covered only by the previous
     round's synced truncation zeros, must not be acked (X5-R35 (a))'
  test result: FAILED. 5 passed; 1 failed   (the (g) 1–5 tests green)
  ```

  The committed tip is the ruling's derivation (all six green).
- **X5-R35 (c) — the 17 named rounds at the rework's tip** (single
  seeds, `X-5c-F3-F3a.rw2.s<seed>.*`; `lo`/`frontier` from the round
  records, `D` from `E4_D_DIAG`): **every one left `frontier < lo`.**
  15 pass with no O-1 text; 2 (1174 r2, 4612 r2) now fail **O-1(d) at
  `frontier == lo`** (a scan mismatch at the matched candidate — the
  C-DIV remainder family, X-5c-F3-F3a-F2's; "a named round that then
  fails another clause is classed under that clause", X5-R34 (h) 1).
  No (h) 3 store loss; (d) holds — no `lo` was raised anywhere.

  | Seed/round | lo | frontier | pass | outcome | ack_over |
  |---|---|---|---|---|---|
  | 205 r2 | 68 | 68 | true | none | [] |
  | 381 r1 | 32 | 32 | true | none | [] |
  | 403 r1 | 3 | 3 | true | none (was frontier 3 < lo 7) | [] |
  | 774 r2 | 7 | 7 | true | none | [] |
  | 1174 r2 | 40 | 40 | false | O-1(d) c=40 | [] |
  | 1203 r1 | 41 | 42 | true | none | [] |
  | 3805 r2 | 43 | 43 | true | none | [] |
  | 3883 r2 | 56 | 56 | true | none | [] |
  | 4193 r1 | 28 | 28 | true | none | [] |
  | 4601 r1 | 27 | 27 | true | none | [] |
  | 4612 r2 | 24 | 24 | false | O-1(d) c=24 | [] |
  | 5030 r1 | 29 | 29 | true | none | [] |
  | 7203 r2 | 48 | 48 | true | none | [] |
  | 7803 r1 | 4 | 4 | true | none (was frontier 4 < lo 8) | [] |
  | 8200 r2 | 52 | 52 | true | none | [] |
  | 8404 r1 | 29 | 29 | true | none | [] |
  | 8825 r2 | 10 | 10 | true | none | [] |
- **Finding 1 — commit 313a236b** (`src/store/wal.rs`,
  `wal_start_lsn`'s `None` arm + the shared `wal::boot_id_older`
  comparison): the arm holds `wal_start_lsn` back only for unmarked
  chunks of the **current** boot — S-STORE §14 step 8 as merged at the
  base: "Chunks of the previous boot that have no marker belong to
  transactions that ended without one …, all ended before this flip,
  so they do not hold back `wal_start_lsn` (§12.6)". The pre-rework
  arm pointed meta k+1's `wal_start_lsn` at the previous boot's chunk,
  §12.5's first-record floor stopped the next walk at it, and every
  acked commit of the new boot was lost (the 100 class-C
  `frontier < lo` store losses of the first Acceptance file).
  - **The pin** `wal_start_lsn_ignores_previous_boots_chunks`
    (the verdict's fixture): boot b−1 leaves a cut-off chunked txn below p0; boot b's
    boot checkpoint runs; b appends and acks two SyncOn commits and
    crashes before its next checkpoint. Asserted: meta 2's
    `wal_start_lsn` sits at p0 (decoded from the durable stream), and
    recovery replays both acked commits (frontier = max acked 3, Q1/Q2
    present, the old boot's unmarked chunks never applied).
    **Mutant START-LSN-PREV-BOOT-CHUNK (the old arm) fails it**,
    demonstrated and reverted:

    ```
    cargo test -q --release -p e4-recovery --test store \
      wal_start_lsn_ignores_previous_boots_chunks
    assertion `left == right` failed: meta 2's wal_start_lsn sits at p0 …
      left: 79   right: 219      (79 = the previous boot's chunk, 219 = p0)
    ```

  - **The G1 pin** `first_record_older_than_opened_meta_stops_the_walk`
    (§12.5's first-record floor, which nothing caught — the gate's own
    floor-drop mutant passed the whole release suite at the first run's
    tip): a meta whose `wal_start_lsn` is e1 with `boot_id` 2, and an
    older boot's complete record at e1 (built directly on the segment,
    the identical walk input a reverted truncation leaves — the
    recorded precedent of `stale_boot_record_stops_the_walk`). The
    walk stops at e1: frontier = T, the stale record's key absent.
    **The gate's G1 mutant (`last_boot.unwrap_or(rec.txn.0)`) fails
    it**, demonstrated and reverted:

    ```
    cargo test -q --release -p e4-recovery --test store \
      first_record_older_than_opened_meta_stops_the_walk
    assertion `left == right` failed: … never replays (12.5's first-record floor)
      left: 2   right: 1        (2 = the stale record replayed)
    ```

    (Both demonstrations were reverted clean — `git diff` free of each
    afterwards; the two pins pass at the tip.)
  - **Movement (the five named repros)** — each left
    `frontier < lo`; all five now **pass**:

    | Seed/round | lo | frontier | pass | was (first run's tip) |
    |---|---|---|---|---|
    | 206 r1 | 7 | 9 | true | a first-record stop on a previous-boot K_CHUNK (Finding 1) |
    | 296 r1 | 13 | 13 | true | same |
    | 405 r1 | 7 | 7 | true | O-1(b) frontier 4 < lo 7 (moved by 8b71e560, now cleared) |
    | 705 r1 | 8 | 8 | true | same |
    | 9823 r1 | 6 | 8 | true | same |
- **Finding 3 — the (h) 3 tip re-run (the 37 named rounds, single
  seeds at 313a236b).** **All 37 pass with no O-1 text; `ack_over` is
  empty on every round; no round keeps `0 < frontier < lo`.** The
  verdict's two failing rounds both cleared: 403 r1 (frontier 3 =
  lo 3, was 3 < 7) and 7803 r1 (4 = 4, was 4 < 8). Full table (lo,
  frontier, pass — every row `frontier ≥ lo`, every `ack_over` empty):

  | Seed/round | lo | frontier | | Seed/round | lo | frontier | | Seed/round | lo | frontier |
  |---|---|---|---|---|---|---|---|---|---|---|
  | 20 r2 | 63 | 63 | | 1585 r1 | 17 | 17 | | 5604 r0 | 24 | 24 |
  | 115 r1 | 13 | 13 | | 1801 r1 | 35 | 35 | | 6154 r0 | 9 | 10 |
  | 403 r1 | 3 | 3 | | 2183 r0 | 13 | 16 | | 6200 r2 | 45 | 45 |
  | 622 r0 | 15 | 15 | | 2525 r2 | 75 | 75 | | 6263 r2 | 21 | 21 |
  | 780 r2 | 58 | 58 | | 2730 r2 | 49 | 49 | | 6623 r0 | 15 | 15 |
  | 801 r1 | 28 | 28 | | 2765 r0 | 11 | 12 | | 7405 r1 | 34 | 34 |
  | 825 r2 | 54 | 54 | | 2812 r1 | 15 | 15 | | 7531 r0 | 4 | 4 |
  | 830 r1 | 4 | 4 | | 3224 r1 | 7 | 7 | | 7803 r1 | 4 | 4 |
  | 1001 r0 | 12 | 12 | | 3806 r1 | 31 | 31 | | 7823 r1 | 30 | 30 |
  | 1064 r1 | 18 | 18 | | 4770 r2 | 68 | 70 | | 8220 r2 | 55 | 55 |
  | | | | | 4930 r0 | 32 | 33 | | 8363 r0 | 16 | 16 |
  | | | | | 5054 r1 | 9 | 9 | | 9200 r0 | 21 | 23 |
  | | | | | 5601 r1 | 46 | 46 | | 9484 r2 | 77 | 77 |
  | | | | | | | | | 9594 r0 | 18 | 18 |

  (1064 r1, 4770 r2, 5054 r1 and 6263 r2 — the four exact-boundary
  rounds of (h) 2 — pass at every tip since ce8f8bbc.)
- **The 0..200 window at the rework's tip**
  (`X-5c-F3-F3a.rw2.0200.*`, 13.5 s wall): **`ack_over` empty over
  every judged round; failing class-C rounds with
  `0 < frontier < lo`: 0** — none named, none new (X5-R34 (h) 3's
  escalation set is empty). FAILING_ROUNDS 17, C-XX **0**, C-DIV 7
  (b 3: 112/120/152; c 4: 63/104/125/186 — the identical set and
  sub-kinds as the first run's step-4 tip, all at
  `frontier ≥ lo`, X-5c-F3-F3a-F2's family), MW-ORPHAN 8, MP
  O-3(e)(ii) 2, DEADLOCK 0, DEFECT_SEEDS 0.
- **Finding 4:** no other test's assertion changed. The whole release
  suite passes at 313a236b with no re-pointing (the r30 seed pins
  unmoved: the store fix moved no seeded MP/MW workload in the suite).
- **The rework's Acceptance at the final tip 6fa80bb5 (X5-R3: a
  prototype deviation is fixed and the entire 10k re-runs).**
  - `cargo test -q --release -p e4-recovery`: **PASS** (189 tests:
    14 + 25 + 24 + 49 + 28 + 26 + 23, 0 failed).
  - The 10k (`X-5c-F3-F3a.acc.*`, prov `commit=6fa80bb5…`,
    `status_lines=0`, `exit=0`, json sha256
    `46297043f583ff9fc69ed8dc4ad58cc5f8b9d2fc63a6297869f52a3058a0c42c`,
    17:44:50→18:03:07, ~18 min): `E4 SEEDS=10000 O1_FAIL=2510
    O2_FAIL=34 O3_FAIL=488 DEADLOCK=0 PANIC=0 O1F_FAIL=2516 MONITOR=0`
    / `E4 FOLD FAILING_SEEDS=766 FAILING_ROUNDS=745
    TWIN_FAILING_ROUNDS=39 DEFECT_SEEDS=0`.
  - `verify --no-quotas`: **PASS** — `VERIFY OK (--no-quotas)
    seeds=10000 digests reproduce`; the six known waived misses (Q4,
    Q7, **Q8 82/100**, Q11, Q12, SIZING — X-5c-F4's retune list, Q8
    recorded beside them since the first run's verify). **Anomaly,
    recorded:** the first verify attempt (prov `verify_start=
    18:03:28`) was killed by the runner's 60-minute watchdog — the
    host was co-scheduled with another agent's run (load 22 at the
    10k's end; the card's Notes ask the orchestrator to keep E4
    Acceptances off a loaded host). The re-run (quiet host) completed
    in ~21 min; the killed attempt wrote no verify output and no
    `verify_exit` line, and the re-run's lines follow it in the prov
    (the last write wins, as "How to reproduce" records).
  - The record check: **PASS** — `ACK_OVER_ROUNDS 0`,
    `C_FRONTIER_BELOW_LO 0`, `MISSING_FIELDS 0`. **Zero failing
    class-C `0 < frontier < lo` rounds in the whole 10k** (the first
    run's Acceptance file held 117: the 100 Finding-1 store losses and
    the 17 X5-R35 false losses — both sets cleared), and no
    ACK-OVER-HOLE round anywhere.
  - `bash scripts/gate.sh`: **PASS** — the literal last line
    `GATE EXIT=0`.
  - The rework's class table (the step-0 (e) definitions, served
    rounds split first):

    | Class | Rounds | Notes |
    |---|---|---|
    | **Served (split out first)** | 8, **all moved to pass** by X5-R30 (c) | 537, 1887, 2927, 4098, 7528, 8107, 8578, 8728 (all r0, MP, `O-3(a): served corruption`, each `(skx ∨ smx) ∧ pass_na`) |
    | **C-XX** | 50 | sites within C as before (Open/AmcheckIi); 26 of the first run's 76 were the Finding-1 first-record XX001s, now gone |
    | **C-DIV** | 256 | (b) 101, (c) 96, (d) 59, **(a) 0** — X-5c-F3-F3a-F2's family (the per-key `≤ T` cut), unchanged in shape |
    | **MW-ORPHAN** | 406 | F3-F3b's (`wal_ckpt_end`/I-WAL-COVER) |
    | Other MW** | 14 | the O-1(a)+O-3(d) published-set family (4399, 4919 …) |
    | **MP O-3(e)(ii)** | 11 | 8 r0, 3 r1 — no O-1(b)/(a) MP round this time |
    | **Twins** | 39 | 39 failing twin seeds |

    (Primary failing rounds 737 = 745 − the 8 served; the fold's
    `FAILING_SEEDS=766` counts 21 seeds that fail only through twins
    or served-amendment accounting. Beside the first run's file: C 390
    → 306, MW-ORPHAN 445 → 406, MP 10 → 11, twins 68 → 39 — the
    rework's two derivations account for the C loss; F3-F3b's triage
    step 0 re-derives the table from this file.)


## Stage X-5c-F3-F3b's record (this card)

**Step 0 — the triage file of record (commit f8251b84) and the class
table.**

- **(a) The provenance checks passed before the copy:** the prov file
  shows `commit=3c743d30e85981dca06a593921935be8e135e50a`,
  `status_lines=0`, `exit=0`, `verify_exit=0`; the JSON's sha256 equals
  its `json_sha256`
  (`46297043f583ff9fc69ed8dc4ad58cc5f8b9d2fc63a6297869f52a3058a0c42c`);
  `commit=` equals `git log -1 --format=%H HEAD -- spikes/e4-recovery
  Cargo.lock`; `git diff --quiet 3c743d30 HEAD -- spikes/e4-recovery
  Cargo.lock` exits 0.
- **(b) The triage file:** committed at f8251b84
  (`results/campaign.json` only), the message carrying the two E4 lines,
  the quota table and the prov lines.
- **(c) The class table, re-derived from `results/campaign.json` (the
  reporting rule: served rounds split out first), judged under X5-R30 (c)
  from the recorded fields — a served round passes iff
  `(skx ∨ smx) ∧ pass_na` (no re-run).** A **failing** primary round has
  `pass=false` as amended; a **failing** twin has `pass=false` in the
  twin record.

| Class | Rounds | Record-field definition (re-derived, verifiable from the JSON) | Repros (round) |
|---|---|---|---|
| **Served (split out first)** | 8, **all moved to pass** by X5-R30 (c) | MP/MW round with `served=true`: 537, 1887, 2927, 4098, 7528, 8107, 8578, 8728 (all r0, all `O-3(a): served corruption` as recorded) — each passes the amendment (`(skx ∨ smx) ∧ pass_na`). **No failing served round remains.** | 2927 r0, 7528 r0 |
| **C-XX** (class C) | 50 | class C failing round with `o2 != null` or `o3` = `O-3(b): XX001 in class C at [site]` — sites: `Open` 16, `AmcheckIi` 34 (47 also carry an `o1`) | 310 (r0, `Open`), 332 (r1, `AmcheckIi`) |
| **C-DIV** | 256 | class C failing round outside C-XX carrying an `o1` text: (b) 101, (c) 96, (d) 59, (a) 0. **0 carry `0 < frontier < lo`** (by round index: r0 3, r2 253) — X-5c-F3-F3a-F2's family (the per-key `≤ T` cut) | 63 (r2, c), 104 (r2, c), 112 (r2, b) |
| **MW-ORPHAN** | 406 | MW failing round with `o1` = `O-1(c) … raw entry above the cut` and `o3` = `O-3(d): silent Ok with O-1 failing` — **this card's** (`wal_ckpt_end`/I-WAL-COVER) target; in 0..200 exactly the eight named rounds: 19 r0, 39 r0, 59 r0, 69 r0, 109 r0, 149 r0, 159 r0, 199 r2 (the REFRESH set) | 19 r0, 199 r2 |
| **Other MW** | 14 | the MW failing rounds outside ORPHAN: 13 `O-1(a)`+`O-3(d)`, 1 no-`o1`+no-`o3` — the ORPHAN published-set family; none in 0..200 | 4399, 6399 |
| **MP O-3(e)(ii)** | 11 | MP failing round with `o3` starting `O-3(e): (ii)`: 8 r0, 3 r1, each "reported slot N that is neither tainted nor in R" | 8 r0, 178 r1 |
| **Other MP** | 0 | no MP round fails outside the served set and (ii) — the rework file's shape holds | — |
| **The twins** | 39 | failing twin rounds: 8 MP + 31 MW; `o1` kinds: (a) 13, (b) 14, (c) 8, (d) 4 | — |
| **`ack_over`** | 0 | no round carries a non-empty `ack_over` (X5-R34 (f)'s cross-check clean) | — |

Partition check: primary failing rounds (amended) **737** = 745 recorded
− the 8 served; C 306 = 50 + 256; MP 11; MW 420 = 406 + 14; the fold's
`FAILING_SEEDS=766` counts the seeds failing only through twins or
served-amendment accounting beside these. **Every count equals the
F3-F3a rework gate's own class table** (its README section); no
deviation to record.

**Step 0′ — the folded follow-up X-5c-F3-F3a-F3 (commit
9580b10d; test-only).** The literal-config harness pin
`x5r35_seed403_slice_feeds_the_derivation` runs `run_round` on seed 403
through round 1 and asserts round 1 derives `lo == 3 ≤ frontier` (4),
that ts 7 is not acked, and that `ack_over` is empty — X5-R35 (a)'s
`slicing rule held by a seed, not only by the (g) 6 event fixture. Both
revert demonstrations (RG1 `pre_workload = 0` in `run_round`, RG2
`derive_written_acked` fed the unsliced capture `&events`) were applied
to the real code, shown failing the pin, and reverted clean
(`git diff` free of each):

```
cargo test -q --release -p e4-recovery --test harness \
  x5r35_seed403_slice_feeds_the_derivation
RG1: assertion `left == right` failed … lo:  left: 7, right: 3
RG2: the same assertion (the derivation fed the unsliced capture)
```

**Step 1 — the meta's WAL bound (S-1-F3 D1, D3, W1, W5 (i)–(iii),
W6 (i); commit f5ba9e6b).**

- **(a) The format (§2.2 W1).** `page::Meta` gains `wal_ckpt_end`
  (`src/store/page.rs:499`, wire offset 108) and `ts_ckpt_hi`
  (`src/store/page.rs:504`, offset 116), both inside the CRC (now at
  124, covering 12..124 — `page.rs:532–536`, decoded `page.rs:572–573`);
  meta 0 carries `(0, 0)` (`src/store/ckpt.rs:192`). `WalState` gains
  `last_commit_ts` (`src/store/wal.rs:362–369`), set in `wal_enqueue`'s
  placement step for `Commit`/`CommitMarker` only (`wal.rs:450–455`),
  starting at the boot's frontier — 0 at `create`, the recovered
  frontier after `recover` (`ckpt.rs:1793`). Every harness parser of
  meta bytes goes through `parse_meta`.
- **(b) Step 0 (§13 W5 (i)).** Inside `checkpoint_inner`'s flip, the
  checkpoint waits for the commit critical section's flag as a committer
  does (parking on `WaitKey::Applied`, re-testing), then reads
  `wal_ckpt_end := wal.append` and `ts_ckpt_hi := last_commit_ts` in one
  `cx.with` step (`ckpt.rs:644–681`). **No deadlock exists**: the
  committers' critical section is a flag turn released before any
  committer can park on the flip's gate, so the flip's exclusive gate
  hold cannot starve a flag holder — W5 (i)'s premise held; nothing to
  escalate. `Chunk` placements need no flag (kept as the spec says).
- **(c) Step 1 (§13 W5 (ii)).** The step ends only when
  `durable ≥ wal_ckpt_end` (`ckpt.rs:771`, `wal.rs:870`) — never at
  "the latest batch's sync".
- **(d) Step 4 (§13 W5 (iii)).** At the meta write,
  `T ≤ ts_ckpt_hi` and `wal_start_lsn ≤ wal_ckpt_end ≤ durable` must
  hold; a violation is fail-stop — the store is poisoned, as for
  `gc_w > T` (`ckpt.rs:858–874`).
- **(e) Open (§14 step 1, W6 (i)).** A valid-magic, valid-CRC meta with
  `T > ts_ckpt_hi` or `wal_start_lsn > wal_ckpt_end` fails the open with
  XX001 (`Fail::Corrupt`), no fallback (`ckpt.rs:264–273`).
- **(f) The pins** (literal configs, `tests/store.rs`):
  `ckpt_step1_waits_for_contiguous_durable` (`tests/store.rs:3552`) —
  a flip with batch 1 unwritten (parked at F3-F3a's `writer:prewrite`)
  and batch 2 written and synced above it; asserted at After4,
  `durable ≥ wal_ckpt_end` after the meta write, and in the
  crash-after-step-5 arm every byte below the durable meta's
  `wal_ckpt_end` is durable on SimDisk (merged coverage
  `[0, wal_ckpt_end)`); and `cover_ts_cross_check_forged_meta`
  (`tests/store.rs:3901`) — a forged meta through `Env` with a valid
  CRC and `T > ts_ckpt_hi` gives XX001, and so does one with
  `wal_start_lsn > wal_ckpt_end`.
- **(g) The mutants, applied to the real code, shown failing their pin,
  reverted clean:**
  - **CKPT-SYNC-OVER-HOLE** (step 1 ends at the latest batch's sync) →
    the first pin fails — "the checkpoint never reached After4 … or its
    meta write was refused over a durability hole below wal_ckpt_end
    (13 step 1, W5 (ii))";
  - **META-RANGE-SKIP** (the open-time check removed) → the second pin
    fails — the forged meta (`T=10 > ts_ckpt_hi=7`) opens Ok.
- **Judgment call (recorded).** D4's mutant arm now catches
  WAL-SYNC-T-ONLY earlier: the step-4 bound refuses the mutant's
  under-synced meta (task error "meta bound (13 step 4)", the round
  fails). The arm keeps every original catcher beside the new one and
  now also asserts `!pass` — X5-R11 (e)'s re-pointing shape, never a
  weakening.

**Step 2 — I-WAL-COVER after pass 1, fail-closed (S-1-F3 D2, W6 (ii);
Q1 = A; commit 4c9905df).**

- **(a) The check (§14 step 3a).** In `ckpt::recover`, after the walk
  (pass 1) and before `recover:install`, pass 2, any eviction write and
  `truncate_wal` (`src/store/ckpt.rs:1553–1580`): `hi_read` = the
  largest commit_ts of the `Commit`/`CommitMarker` records the walk
  accepted; the WAL covers image `k` iff `valid_end ≥
  meta.wal_ckpt_end` **and** `max(meta.t, hi_read) ≥ meta.ts_ckpt_hi`
  (`valid_end` is the walk's stop position, §12.5 as amended, never an
  end-of-file).
- **(b) On failure.** `Fail::WalLoss { valid_end, wal_ckpt_end, t,
  ts_ckpt_hi }` (`src/store/mod.rs:165–180`) — XX001-class, `Display`
  naming WAL loss with the four values. Nothing is written: no install,
  no replay mutation, no eviction write, no truncation; the files stay
  exactly as found. Recovery's step-5 truncation carries the W8 comment
  (a failure there fails the open, the store is never installed —
  equivalent to a poisoned, stopped store; no mark needed;
  `ckpt.rs:1754`).
- **(c) The harness.** The recovery `Err` arm sets the round field
  `cover_err` by a typed match on `Fail::WalLoss`, never a text match
  (`src/harness.rs:1669`, `:2687`, `:2786–2788`; `src/workload.rs:725`);
  `run` writes it and `verify` parses it. The XX001 site stays `Open`
  (X5-R11) and the seed ends at that round. The r30 seed pin's 6149 arm
  is re-pointed r1 → r0 with its assertions unchanged (F3-F1's accepted
  seed-shape maintenance; the old index fails only at its shape
  precondition).
- **(d) The pins** (literal configs, `tests/store.rs`):
  `wal_loss_below_ckpt_end_fails_closed` (`tests/store.rs:4025`) with
  **nine variants** — `c` partly applied under LOST, TORN and
  MISDIRECTED; a record appended but unapplied at the flip; a
  `SegmentEnd` below `wal_ckpt_end`; a chunk of a marked transaction; a
  chunk of a transaction that never committed (no committed data lost,
  still XX001); a bulk load's fence commit `f > T` with loaded entries
  in image k; and the short segment (cut so its end-of-file lies past
  the start of a record that runs past it, below `wal_ckpt_end`). Each
  asserts the typed refusal, no write/sync/directory event during the
  open, byte-identical inodes and directory bindings before and after,
  and the same refusal on a second open after a nested crash during the
  first. Beside it: `cover_ts_cross_check_unreachable_hi`
  (`tests/store.rs:4428`) — a forged meta whose `ts_ckpt_hi` exceeds
  every replayable commit gives `Fail::WalLoss` — and
  `wal_exact_segment_end_hops`'s new flip variant (gate edit 2): a flip
  after the next segment's records opens Ok.
- **(e) The mutants, applied, shown failing their pin, reverted clean:**
  - **COVER-CHECK-SKIP** (the check removed) → the main pin fails (the
    `c`-partly-applied variant opens Ok), and the named 0..200
    MW-ORPHAN seeds re-show the orphan shape — 19/39/59/69/109/149/159
    r0 each `O-1(c) "raw entry above the cut"` (199 r2 had already
    moved at step 1);
  - **COVER-END-AT-T** (step 0 writes `wal_ckpt_end` as the first
    ts>T record's lsn) → the pin fails through the segend fixture —
    the hop records are never synced by the mutant's step 1 ("segment
    …1m0000.log unwritten"). The unapplied variant alone still refuses
    through the check's second condition (`ts_ckpt_hi > max(T,
    hi_read)`); the too-low-bound shape itself is what X5-R33 (d)'s
    cross-check catches by name (step 4's self-test) — the mutant is
    caught, by a different variant than the card guessed, recorded
    here honestly;
  - **COVER-AFTER-TRUNCATE** (the check after `truncate_wal`) → "a
    write/sync/directory event fired during the open" (the truncation's
    zeros land before the refusal);
  - **COVER-AT-EOF** (condition 1 against a short segment's
    end-of-file) → the pin fails — the unmarked-chunk (mid-segment
    damage) variant opens, and so does the short-segment variant;
  - **TS-HI-CHECK-SKIP** (the second condition removed) →
    `cover_ts_cross_check_unreachable_hi` fails (the forged meta's
    recovery proceeds past the coverage check);
  - **SEG-EXACT-NO-HOP** (F3-F3a's hop disabled) → the flip variant's
    fixture fails at F3-F3a's own assertion (frontier 4 ≠ 6) and the
    flip variant gives a false `Fail::WalLoss`.
- **(f) Class C.** Over the 0..200 run at this tip and at every later
  tip: **0 class-C `cover_err` rounds** — no (f) store failure to
  escalate.

**Step 3 — G8 in full, the WAL path included (S-1-F3 D4/W8 with gate
edit 3; commit e724ce05).**

- **(a) What poisons.** Each mark is one `cx.with` setting
  `st.poisoned` if unset, before the failing operation releases its
  tree mutex, pins or the gate; a sim crash (`Fail::Sim`) is never a
  poison. The sites: the kept `fetch_page` `inside_op` XX001 rule (G-2);
  a checkpoint step — `checkpoint()`'s non-Sim error path marks before
  the `ckpt:abort` cleanup releases the frozen image's pins (the
  flip-abort paths poison at their own sites, as `gc_w > T` always
  did); a background store job step — the harness's `gc_sweep` steps
  and `tree::resume_drops`' drop walk (`src/store/tree.rs:228`,
  `:1352`); and the WAL path — a batch's write or `sync_data` marks at
  `wal_writer_step`'s error exit, before the in-flight release step,
  for `Fail::Io` (through the one store test hook) and for `Fail::Invalid`
  from `check_wal_inv`'s frontier breach (`src/store/wal.rs:629–678`).
  `Fail` gains the `Io(String)` variant (the 58030 class;
  `src/store/mod.rs:189`); only the hook can raise it in this sim.
- **(b) Recovery's step-5 truncation** — the W8 comment landed with
  step 2; a failure there fails the open and the store is never
  installed (equivalent to a poisoned, stopped store), no mark needed.
- **(c) Every wait includes the mark** — re-tested in the same baton
  hold as its own predicate and its park: `WalAcks`
  (`wal_wait_durable`/`wal_wait_written`), `Pin`, `Tree(t)`,
  `GateShared`, `GateFlip`, the harness's `Applied` waits (the commit
  critical section, `mark_applied`'s ts-order turn) and `MetaDurable`.
  A woken waiter that finds the mark returns `Fail::Poisoned`.
- **(d) The broadcast.** After the poisoner has released its tree
  mutex, pins and the gate, it wakes every wait key that has a parked
  task (`src/store/prims.rs:55–67` — the fixed keys plus `Tree(t)` for
  every held tree; the harness twin `store_poison_broadcast` for the GC
  sweep and the script-op error exit wakes every key, not three). No
  waker holds a tree mutex or a pin while it wakes. The harness's
  existing wake at a task error stays.
- **(e) Nothing more is written.** The writer checks `alive()` at the
  take and inside `wal_write_batch` after each park; the checkpointer
  and evictor bodies exit on the mark; the harness's crash at poison
  (the process stops) stays as it is.
- **(f) A failed WAL sync is never retried as if its bytes were
  safe.** The hook refuses before the env call, the advance step is
  unreachable on the failure path, and every later take refuses at the
  alive check; its batch's bytes never count toward `durable`.
- **(g) The one store test hook.** `StoreCfg::wal_io_fail`
  (`src/store/mod.rs:149–156`; `WalIoOp::{Write, SyncData}` on a chosen
  batch start, `src/store/wal.rs:973–988`) — default-off, used only by
  this step's pins, never by the workload or the campaign.
- **(h) The pins** (literal configs driving the S-BATON scheduler
  without the harness's crash at poison): `g8_poison_wakes_every_waiter`
  (`tests/store.rs:4500`) — a damaged referenced leaf on an apply's
  path poisons the store while a SyncOn committer parks on `WalAcks`
  (its gap through a batch held at `writer:prewrite`), a committer
  parks on `Applied` (its release needs `visible_ts` to pass the
  poisoned commit — the lock-waiter stand-in; no lock table exists),
  and a task parks on `Tree(2)`; every one returns `Fail::Poisoned`
  within the step budget, no DEADLOCK fires, and no env write event
  fires in the world. `g8_wal_sync_error_poisons`
  (`tests/store.rs:4816`) — the hook fails the group-commit writer's
  `sync_data`: the store poisons, every SyncOn waiter returns the
  error, `durable` never covers the failed batch, no sync event ever
  issues and none follows. The kept pins stay green:
  `g8_read_never_poisons_gated_op_does` (G-2, CLIENT-READ-POISONS) and
  the M-G8a arm (XX001-NOT-POISON).
- **(i) The mutants, applied, shown failing, reverted clean:**
  - **POISON-LOST-WAKEUP** (the `alive()` test out of
    `wal_wait_durable`'s predicate) → the pin fails — "g8 poison wakes
    every waiter: DEADLOCK";
  - **POISON-WAITER-PARKED** (the broadcast removed) → the same
    failure;
  - **WAL-ERR-NOT-POISON** (the writer's error path returns without
    the mark) → `g8_wal_sync_error_poisons` fails — "the SyncOn waiter
    must return Poisoned";
  - **POISON-AFTER-UNLATCH** (the mark made after the release) → an
    **equivalent mutant in the S-BATON schedule, demonstrated**:
    between `fetch_page`'s mark and `tree_insert`'s releases (PinGuard
    drop, `tree_unlock`, `gate_release`) there is no yield point, env
    call or park (verified: 0 matches on the unwind), so no other task
    can run there and the half-changed state is unobservable — the
    README demonstration the card allows in place of a failing pin.

**Step 4 — X5-R33, the one oracle commit (86eddb34; the frozen oracle's
single permitted change, one commit whose subject names X5-R33).**

- **(a) The harness supply** — SimDisk history and the model's record
  positions only; `wal_ckpt_end`/`ts_ckpt_hi` are read from the meta
  bytes on SimDisk, never from store memory:
  - the per-seed stream of lives (`X33Life`: `p0`, the life's
    `Commit`/`CommitMarker` records `(lsn, len, ts)`, its boot, its
    starting frontier, its append end) carried by `run_seed` beside the
    model and threaded into `run_round`'s real rounds (`X33In`;
    twin/probe rounds pass `None`);
  - `x33_start_log` (the stream at a life's start: each earlier life's
    records below the lowest later truncation point — X5-R33 (b));
  - `x33_h_of` (`h`: the largest ts wholly below the opened meta's
    `wal_ckpt_end` in the stream as it stood when that meta was
    written);
  - `x33_damaged_ranges` (the round's MW fault targets and MISDIRECTED
    destinations on WAL inodes, mapped to absolute LSNs through the
    first LSN of the segment name bound to the target inode at the
    crash);
  - `x33_a_of` (`a`: the largest X5-R34-acked SyncOn ts whose record
    ends at or below the first damaged WAL byte; with no damaged WAL
    byte, the round's largest acked SyncOn ts);
  - `x33_explained` (`wal_loss_explained`: some damaged LSN in
    `[wal_start_lsn, wal_ckpt_end)` of the opened meta);
  - `x33_cross_check_meta` (the (d) bounds: no straddling record, every
    ts ≤ T record ends ≤ w, w ≤ the life's append, `t_hi` = the largest
    ts below w or the started frontier) — run over **every** meta-slot
    `DWrite` whose bytes `parse_meta`, identified by its exact bytes,
    attributed to its writing life by boot id; a mismatch is a store
    failure of the round through X5-R10's task-error path with the
    carried text.
- **(b) The oracle text** (`src/oracle.rs`, `tests/oracle.rs`), clause
  by clause the carried text above: `Extracted` gains `mw_h`, `mw_a`,
  `wal_loss`, `wal_loss_explained`, set by both arms of the harness
  before `check_round`; in class MW with `!cfg.no_r33`,
  `lo = max(c_prev, ex.opened_t, ex.mw_h, ex.mw_a)` (with `no_r33`,
  `lo = ex.opened_t`, as before), `hi` unchanged; in class MW with
  `!ex.ok && ex.wal_loss && !cfg.no_r33` the twin rule does not apply —
  the round fails O-3 (c) iff `!ex.wal_loss_explained`, with the text
  `O-3(c): WAL-loss XX001 with no fault in [wal_start_lsn,
  wal_ckpt_end) (X5-R33)`; `OracleCfg` gains `no_r33`. Unchanged: every
  other MW `Err`, every MP and C round, every other clause.
- **(c) The record.** Rounds gain `x33h`/`x33a`/`x33x` beside step 2's
  `cover_err` — `run` writes them, `verify` parses them; diagnostics
  the pass fold never reads.
- **(d) The oracle self-tests** (`tests/oracle.rs:1461–1630`): mw1 (an
  explained WAL-loss `Err` passes), mw2 (matching only at
  `c = max(h, a)`), mf30 (matching only at `h − 1`, green under
  `no_r33`), mf31 (matching only at `a − 1`, green under `no_r33`),
  mf32 (an unexplained WAL-loss `Err` failing with the carried text,
  green under `no_r33` through its twin-covered fault), and
  `instrumentation_removals_are_load_bearing`'s X5-R33 arm: `no_r33`
  turns mf30, mf31 and mf32 green. Each asserts the `o1`/`o3` text, not
  only the pass bit.
- **(e) The harness self-tests** (`tests/harness.rs:1699–1797`): a
  record superseded by a later truncation never enters `h` (a reissued
  ts fixture); the cross-check fails a forged low `wal_ckpt_end` and a
  forged `ts_ckpt_hi`; damage maps through the segment's bound name
  (source and MISDIRECTED destination alike).
- **(f) The revert demonstrations, applied to the real code, shown
  failing their self-test, reverted clean:**
  - superseded-record-counted (`x33_start_log`'s floor ignored) → "the
    superseded record never belongs to life 1's stream: `[(50, 60, 9)]`";
  - cross-check-skipped (`x33_cross_check_meta` returns `None`) → "a
    forged low `wal_ckpt_end` fails: `None`";
  - unbound mapping (the first `/wal/` name instead of the bound one)
    → `left: [(100, 150)]` against the bound-name mapping.
- **(g) The fix inside step 4 (commit c929c2a3, recorded here because
  the card's one-commit rule kept the oracle whole).** The first 0..200
  smoke (after 86eddb34, before any of its output fed a judgement of
  the prediction) exposed **ten unexplained refusals**: the real-round
  `X33In` threading initially missed (the bulk signature patch left
  `None` in its place) and round 0's boot-0 attribution order was
  wrong (round 0's workload runs on the created store's boot 0, so the
  current-boot match must precede the create-life arm). With both in
  place every refusal is explained. The `E4_X33_DIAG` diagnostic
  (stderr, default off, never a digest input) lands disabled. The
  first smoke's file (`X-5c-F3-F3b.s5.0200.json`) is
  **diagnostic-only**: its MW rounds were judged before the fix, it is
  never cited as a pin or as evidence; the cited smoke is
  `X-5c-F3-F3b.s5.0200b.json` at the c929c2a3 tip.
- **(h) Pre-registration order.** X5-R33's code commit 86eddb34
  (21:58:27) precedes every step-5 output: the first smoke finished at
  21:58:5x, the cited smoke and all named-seed runs at 22:03–22:04.

**Step 5 — the MW smoke and the owed re-checks (at the c929c2a3 tip;
`X-5c-F3-F3b.s5.0200b.json` and `X-5c-F3-F3b.s5.seed<N>.json`).**

- **(a) The smoke over 0..200 — the prediction holds.** Every live
  named MW-ORPHAN round is an explained `Err` (`cover_err ∧ x33x`,
  passing; the seed ends at that round, `rlen` 1). The eighth named
  round moved at step 1. **The 0..200 MW-ORPHAN-shape count is 0.**

  | Round | `cover_err` | `x33x` | pass | outcome |
  |---|---|---|---|---|
  | 19 r0 | true | true | true | explained WAL-loss `Err` |
  | 39 r0 | true | true | true | explained WAL-loss `Err` |
  | 59 r0 | true | true | true | explained WAL-loss `Err` |
  | 69 r0 | true | true | true | explained WAL-loss `Err` |
  | 109 r0 | true | true | true | explained WAL-loss `Err` |
  | 149 r0 | true | true | true | explained WAL-loss `Err` |
  | 159 r0 | true | true | true | explained WAL-loss `Err` |
  | 199 r2 | false | false | false | **moved by step 1** (f5ba9e6b) to the other-MW family: `O-1(b) at c=26, tree 4: scan mismatch` + `O-3(d)` |

  The window holds **10 `cover_err` rounds, all MW, all explained
  (`x33x`), all passing** — 19/39/59/69/79/109/149/159/169/179 r0 (79
  and 169 were passing `Ok` rounds at the step-1 tip; I-WAL-COVER
  turns their damaged-below-bound opens into typed, explained
  refusals). **0 class-C and 0 MP `cover_err` rounds;
  `ack_over` empty over every judged round; no WAL-loss `Err` with
  `x33x` false.** The window's 10 failing rounds: 7 C-DIV (63, 104,
  112, 120, 125, 152, 186 r2 — X-5c-F3-F3a-F2's family), 2 MP
  O-3(e)(ii) (8 r0, 178 r1), 1 other-MW (199 r2 above).
- **(b) X5-R32 (d) 2's five MW rounds** (single seeds — S-1-F3's owed
  re-check; (b) still records all five):

  | Round | outcome |
  |---|---|
  | 1889 r2 | pass, silent `Ok`, frontier 29 = lo 29, `ack_over` [] |
  | 3509 r0 | pass, the published-set `Err` arm (`O-1(a) at c=7`), no `cover_err` |
  | 4699 r0 | **an explained WAL-loss `Err` (`cover_err ∧ x33x`), passing** — the REFRESH's orphan-shaped round, closed |
  | 5229 r1 | pass, silent `Ok`, frontier 19 = lo 19, `ack_over` [] |
  | 8399 r1 | pass, silent `Ok`, frontier 45 ≥ lo 43, `ack_over` [] |
- **(c) X5-R34 (h) 3 — the 37 named rounds** (single seeds at the
  tip): **all 37 pass with `frontier ≥ lo`, `ack_over` empty on every
  round, no round keeps `0 < frontier < lo`** — no store loss to
  escalate.
- **(d) Class MP (X5-R33 (h)).** Over 0..200, **no MP round carries
  `cover_err`** — the MP list is empty; MP rollback rounds keep MP's
  lower bound (2 MP O-3(e)(ii) rounds remain the window's only MP
  failures, unchanged by this card).
- **The five remaining orphan-shaped rounds in the whole 10k
  (ESCALATED to P — X5-R16/R19; never a local fix).** The card's
  class-shape test says a remaining orphan-shaped round means the
  redesign did not close the class: over the Acceptance 10k
  `MW_ORPHAN_SHAPE` is **5** (the triage file's 406 rounds with the
  shape are down to 5), and 532 MW rounds in total now take the
  explained refusal (`MW_UNEXPLAINED 0`). All
  five are round-2 rounds outside 0..200, each deterministic at the tip
  (`X-5c-F3-F3b.esc1.seed<N>.*`), each with the shape
  `O-1(c) "raw entry above the cut"` + `O-3(d) "silent Ok"`, each with
  `cover_err = false` — the damage never fell in `[wal_start_lsn,
  wal_ckpt_end)` of the opened meta, so I-WAL-COVER (correctly) did not
  refuse, and the recovered image serves an entry above every legal
  cut. Their `E4_D_DIAG` traces (the round's stream, `D` and its stop)
  are in `X-5c-F3-F3b.esc1.seed<N>.ddiag.out`:

  | Seed/round | crash | faults (lost/mis/torn) | lo | hi | c | frontier | x33h | x33a | the raw entry |
  |---|---|---|---|---|---|---|---|---|---|
  | 1749 r2 | After5@2 | 0/1/1 | 40 | 41 | None | 41 | 40 | 0 | ts 41 (SyncOff), lsn 504325..505334, written, `D`=505334 |
  | 4139 r2 | uniform | 0/0/1 | 14 | 17 | None | 17 | 14 | 0 | ts 17 (SyncOn), lsn 532896..535750, `D`=535750 |
  | 5549 r2 | After5@1 | 1/1/0 | 30 | 33 | None | 32 | 30 | 21 | ts 32 (SyncOn), lsn 649370..649889, `D`=649889 (ts 33's record ends above `D`, unacked) |
  | 5839 r2 | uniform | 1/0/0 | 43 | 50 | None | 50 | 43 | 0 | ts 50 (SyncOn), lsn 583368..586077, `D`=586077 |
  | 7359 r2 | uniform | 0/2/0 | 14 | 15 | None | 12 | 9 | 11 | ts 15 (SyncOff), lsn 413256..415097, `D`=415097 |

  In each, the entry above the cut is the stream's top record, at or
  below the round's durable end `D`, and above `max(c_prev, T, h, a)`;
  no candidate `c ∈ [lo, hi]` satisfies every comparison (`c = None`).
  This is P's judgement — the class is not closed by the carried
  S-1-F3 design in the prototype; the five repros above are the
  triage input (F4's step 0 owns the file's full table).

**The 0..200 window at each step's tip** (the F3-F3a rework tip is the
"before"; steps 1–3 windows were run and read **before step 4's X5-R33
commit**, so their MW rounds were judged by the old X5-R1 bounds — per
X5-R33 (a) those three windows are **diagnostic-only shape counts,
never cited as pins or as evidence**; the step-5 window is the judged
one):

| Tip | FAILING_ROUNDS | C-XX | C-DIV | MW-ORPHAN | cover_err (C/MP/MW) | ack_over | C `0<frontier<lo` |
|---|---|---|---|---|---|---|---|
| before (F3-F3a rework, `rw2.0200`) | 17 | 0 | 7 | 8 | — | 0 | 0 |
| step 1 (`s1.0200`, diagnostic-only) | 17 | 0 | 7 | 7 | — | 0 | 0 |
| step 2 (`s2.0200`, diagnostic-only) | 10 | 0 | 7 | 0 | 0/0/10 (passing) | 0 | 0 |
| step 3 (`s3.0200`, diagnostic-only) | 10 | 0 | 7 | 0 | 0/0/10 (passing) | 0 | 0 |
| step 5 (`s5.0200b`, judged) | 10 | 0 | 7 | **0** | 0/0/10 (all explained, passing) | 0 | 0 |

(Step 1's MW-ORPHAN 7 = the eight named minus 199 r2, which moved to
the other-MW published-set family `O-1(a) at c=23` at that tip. Every
window: DEADLOCK 0, DEFECT_SEEDS 0, MP O-3(e)(ii) 2. No knob retuned,
no seed excluded, the workload's draws unchanged anywhere.)

**Step 6 — this README section, "How to reproduce" for this card's
Acceptance (`X-5c-F3-F3b.acc.*`), and the Status line.** The whole
release suite at the final tip: **204 tests, 0 failed**
(`cargo test -q --release -p e4-recovery`; fmt and clippy
`-D warnings` clean). The full Acceptance (10k run with prov,
`verify --no-quotas`, the record check, `bash scripts/gate.sh`) runs at
this commit — the card's X5-R31 discipline held: no campaign-class
command beyond the 0..200 runs and single seeds ran in this card's
attempts, all under the backstop, output outside the worktree. The
Acceptance 10k becomes **X-5c-F4's step-0 triage file**; its full
class table is F4's to report.

## Stage X-5c-F3-F3a-F2's record (this card; X-5c-F3-F3b-F1 Work 1-3
## folded in per the orchestrator edit of 2026-11-04)

Start **2026-11-04T01:56:30-07:00**, stop **2026-11-04T02:56-07:00**
(root-causing inside the 90-minute budget; the traces below were
gathered inside it). Repros are from X-5c-F3-F3b's Acceptance file
(`~/.cache/nv3-review/runlogs/X-5c-F3-F3b.acc.campaign.json`, sha256
`8332df8d...`, commit 86ecf8ff) as the fold directs; every command is
a single seed or a <=200-seed window under the backstop, output
outside the worktree.

**Work 2 - the repros** (each run twice at the pre-fix tip, digests
identical: `X-5c-F3-F3a-F2.s2.seed<N>.r{1,2}.json`):

| Sub-kind | Round | o1 (first candidate) | frontier/lo/hi | digest |
|---|---|---|---|---|
| (b) | 112 r2 | O-1(b) at c=15, tree 3: scan mismatch | 15/15/15 | 3936867010 |
| (c) | 63 r2 | O-1(c) at c=23, tree 2: raw entry above the cut (max ts 24) | 24/23/24 | 1693274365 |
| (d) | 664 r2 | O-1(d) at c=26, tree 2, S=5: scan mismatch | 26/26/26 | 1302965675 |

**Work 3 - the key-history trace** (`E4_KEY_DIAG`, stderr, default
off, never a digest input; commit b7fe409e). At the O-1 failure it
traces the first divergent key through the page file's whole
durable-write history: every leaf write that carried the key's LPID
(slot, content epoch, the key's ts values), every durable meta (T,
gc_w, boot, map root, the WAL bound), each durable image's view of
the key (rebuilt by replaying the durable writes below each meta and
walking its map radix - the span math `ckpt::walk_radix` uses), the
opened image's mapping beside the recovered store's served bytes, the
directory's write history (which tree ids each durable version held)
and each image's directory entry for the failing tree, plus the
model's DDL timeline and the key's own model timeline. It names the
**first durable meta whose image fails to hold the owner's entry
while its `T` covers the owner ts** (S-STORE 13 (c)).

- **(b) 112 r2** (`X-5c-F3-F3a-F2.s3.seed112.err`): key ...17 of tree
  3, owner ts 5 (SyncOn, written, acked - round 0's commit). Image 2
  (ckpt 2, T=7) holds it: LPID 40 -> slot 101 (epoch-1 content,
  ts=[5]). **FIRST-LOST at meta 3** (T=11, boot 3 - round 2's boot
  checkpoint): LPID 40 -> slot 137, an epoch-2 rewrite with no entry
  of the key, and the directory leaf (LPID 1 -> slot 171, epoch 2)
  holds ids `[1, 2, 50]` - **tree 3's directory entry is gone**. The
  model's DDL timeline: `DropTree{3}` at ts 8 (round 1's DDL commit,
  durable, <= round 1's cut 11), `CreateTree{3}` at ts 12 (round 2's
  setup re-creating what `trees_at` missed at the cut). The store
  dropped the tree correctly; the recovery re-created it fresh.
- **(c) 63 r2** (`X-5c-F3-F3a-F2.s3.seed63.err`): key ...3 of tree 5,
  owner ts 5 (SyncOff). Images 1-3 hold it (LPID 11 -> slot 43,
  ts=[5]). **FIRST-LOST at meta 4** (T=16, boot 4): the epoch-3
  rewrites of LPID 11 all lack the entry, and meta 4's directory leaf
  is slot 135's epoch-3 version (`[1, 2, 3, 4]`, tree 5 gone) where
  images 1-3 referenced slot 12 (`[1, 2, 3, 4, 5]`). DDL timeline:
  `DropTree{5}` at ts 11, `CreateTree{5}` at ts 17 - the same
  drop+recreate shape; the store's tree 5 is the fresh re-creation
  (root 127, an empty leaf at image 5 while the old content pages
  stay mapped).
- **(d) 664 r2** (`X-5c-F3-F3a-F2.s3.seed664.err`): key ...39 of tree
  2 (never dropped - round 1's drop retired tree 4), owner ts 5
  (SyncOff); the key's model timeline: Puts at ts 5, 6, 9. Image 1
  (T=6, gc_w=0) holds the key at LPID 57 with ts=[6,5]. **FIRST-LOST
  at meta 2** (T=10, **gc_w=8**, round 1's in-workload checkpoint):
  the image holds the key at LPID 50 with ts=[9,6] - ts 5 is gone.
  That is a **legal GC prune** (at w=8 the newest version <= 8 is
  ts 6, so 10.2 lets the sweep prune ts 5) and image 2 records
  `gc_w = 8` truthfully. The failure is that the O-1 (d) sample range
  came from the **opened meta's gc_w**, and the boot checkpoint after
  the next recovery recorded **gc_w = 0** (image 4) because
  `ckpt::open` never restored `gc_w_used` from the opened meta - the
  sample set `[0, 26]` then reached S=5, below the image's true
  bound, where the model (which never models GC) still expects the
  pruned version.

**Work 4 - the outcomes.**

- **Root cause A (the store; fixed, commit e1bd478b).**
  `src/store/ckpt.rs`, `open`: `StoreInner::empty` starts `gc_w_used`
  at 0 and `open` never set it from the opened meta, so every boot
  checkpoint after a recovery recorded `gc_w = 0` while the image it
  froze had GC'd at `w > 0` - S-STORE 10.5: "At open it starts at the
  recovered meta's `gc_w`, so a checkpoint taken before the first GC
  mutation (including the boot checkpoint, 14 step 8) still records
  the image's bound". The fix is one line
  (`st.gc_w_used = meta.gc_w;` in `open`). **Pin**
  `gc_w_survives_recovery_into_the_boot_checkpoint`
  (`tests/store.rs`): a life that GC'd at w=4 with the apply frontier
  at 5 checkpoints (its meta decoded from the durable stream carries
  `gc_w = 4`), a clean crash, a recovery that runs no GC, and its
  boot checkpoint - asserted to carry `gc_w = 4` again. **Mutant
  demonstrated** (the fix line deleted): the pin fails `open starts
  gc_w_used at the recovered meta's gc_w (10.5): left: 0, right: 4`,
  reverted clean. Effect: **664 r2 passes** at the fix tip (digest
  1302965675 -> 1522095950; 112 r2's digest also moved, its round-2
  gcw 0 -> 2; 63 r2's digest unchanged - its opened metas carried the
  bound already).
- **Root cause B (the model; frozen - ESCALATE, X5-R16).**
  `Model::scan` (`src/model.rs`) iterates every commit's `Put`/
  `Delete` ops and never accounts `Op::DropTree`/`Op::CreateTree`
  (`trees_at` does). After `DropTree{t}` at ts d and `CreateTree{t}`
  at ts r > d - both legal, durable, <= the round's cut -
  `model.scan(t, S)` for S >= r returns the **pre-drop** content,
  which no correct store can match (its tree t is the fresh
  re-creation; the drop's walk freed the old pages). Minimal
  deterministic reproduction, from the model's public API: commit
  ts 1 `Put(t, k, v)`, ts 2 `Delete(catalog, t) + DropTree{t}`, ts 3
  `CreateTree{t} + Put(catalog, t, "public")` - then
  `model.scan(t, 3)` returns `k -> v` while every store that applied
  the drop serves nothing for k. Trace repros (each deterministic,
  owner ts < d < r <= the failing round's frontier): 112 r2 (d=8,
  r=12, owner 5), 63 r2 (d=11, r=17, owner 5), and - from the folded
  work - 1749 r2 (d=15, r=27, owner 4), 5549 r2 (d=13, r=23, owner
  3), 7359 r2 (d=11, r=16, owner 3). Not the WAL, not the sim's
  fault or crash model, not root cause A; the oracle and `model.rs`
  are frozen by this card's Limits, so this is P's to rule on.
- **The 0..200 window at the fix tip** (`X-5c-F3-F3a-F2.s4.0200.*`):
  `FAILING_ROUNDS 10, TWIN 1, DEFECT 0` - and its C-DIV set is the
  identical seven rounds as F3b's tip (63/104/112/120/125/152/186,
  all r2). Traced with `E4_KEY_DIAG`
  (`X-5c-F3-F3a-F2.s4.seed<N>.diag.err`): **every one of the seven
  carries root cause B's DDL shape** - a DropTree of the failing tree
  at ts d, a CreateTree of the same id later, and the divergent key's
  owner ts below d (104: d=11 r=17; 120: d=10 r=17; 125: d=14 r=25;
  152: d=8 r=11; 186: d=9 r=12). The whole window's C-DIV family is
  the model gap; root cause A's (d) repro (664) lies outside the
  window and now passes.

**The folded X-5c-F3-F3b-F1 Work 1** (commit 204afb9a):
`E4_O1_AT_FRONTIER` (stderr, default off, never a digest input) - the
`E4_O1_DIAG` per-key diff at `c = clamp(frontier, lo, hi)` (the
bounds derived exactly as `check_round` derives them, X5-R33's MW
lower bound included), beside a thread-local capture
(`o1f_capture_arm`/`o1f_capture_take`) that lets a pin read the lines
without mutating a process-global env var. **Pin**
`o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2`
(`tests/harness.rs`): 1749 r2 (lo 40, hi 41, frontier 41) must name
tree 4's first divergent key with `owner_ts=4` and `le_T=true` at
`c=41`. **Mutant demonstrated** (the clamp replaced by `c = lo`, the
first candidate): the pin fails ("the frontier candidate's diff must
name tree 4's owner ts 4 with le_T at c=41"), reverted clean.

**The folded Work 2 - the five MW repros at this card's tip** (each
run twice, digests identical, `X-5c-F3-F3a-F2.f1w2.seed<N>.{a,b}.json`;
fields from the round records and `E4_D_DIAG`). **Relabelled by
X-5c-F3-F3a-F2-F1 (verdict finding 4):** the `lo` this table copied is
the round **record's** `lo` field — X5-R1's `model.lower_bound(c_prev)`,
never the oracle's MW bound. In class MW the oracle judges by X5-R33's
`max(c_prev, T, h, a)`, which the O-1 error text and the O1F line name;
the new column carries it where a first-candidate text exists. The
passing rounds (4139, 5839) print no first candidate and no O1F line at
this tip, so their oracle `lo` is not in the record — read naively, "7359
r2: frontier 12 < lo 14" was the mislabel finding 4 names.

| Round | pass | o1 (first candidate) | frontier/record `lo` (X5-R1)/hi | MW `lo` (X5-R33) | x33h/x33a | lost/mis/torn | cover_err | digest |
|---|---|---|---|---|---|---|---|---|
| 1749 r2 | false | O-1(c) at c=40 (max ts 41) | 41/40/41 | 40 | 40/0 | 0/1/1 | false | 2987258781 |
| 4139 r2 | **true** | none | 17/17/17 | — | 14/0 | 0/0/1 | false | 3227295539 |
| 5549 r2 | false | O-1(c) at c=30 (max ts 32) | 32/32/33 | 30 | 30/21 | 1/1/0 | false | 2018609054 |
| 5839 r2 | **true** | none | 50/50/50 | — | 43/0 | 1/0/0 | false | 2979793348 |
| 7359 r2 | false | O-1(c) at c=11 (max ts 12) | 12/14/15 | 11 | 9/11 | 0/2/0 | false | 266568285 |

**The folded Work 3 - the cut, per round** (the frontier diagnostic's
first divergent key with its owner ts, and this card's key-history
trace): in all five the frontier candidate's diff names an entry whose
owner commit is <= T of the opened meta with `le_T=true` - the
missing-<=T family, exactly as the gate's ESCALATE ruling classed
them.

- **4139 r2 and 5839 r2 are root cause A's** (this card's store fix):
  both were O-1(d) failures at a historical S below the image's true
  GC bound (4139: S=4; 5839: S=8), and both **pass at the fix tip**
  (folded Work 4's re-run: each must pass or be an explained WAL-loss
  `Err` - they pass, `cover_err` false, no WAL loss).
- **1749 r2, 5549 r2 and 7359 r2 are root cause B's** (the model's
  drop+recreate gap): each trace shows the DropTree of the failing
  tree at ts d, the CreateTree of the same id at ts r, and the
  divergent key's owner ts below d (1749: d=15, r=27, owner 4; 5549:
  d=13, r=23, owner 3; 7359: d=11, r=16, owner 3); each FIRST-LOST is
  the first image after the drop. The MW media fault is not a cause
  in any of them: `cover_err` is false everywhere and no trace shows
  a WAL byte the image depended on - the loss is image/model-side,
  per the ruling's own classification. These three are the ESCALATE
  above, never a local fix; the five are **not** an MW-ORPHAN
  redesign failure (the gate's ruling (ii) stands: they count against
  C-DIV).

## Stage X-5c-F3-F3a-F2-F1's record (this card; X5-R36 landed)

Start **2026-11-04T03:41:43-07:00** (step 0's commit), stop
**2026-11-04T04:14:54-07:00**. Step 0 committed the X5-R36 text
verbatim (8e2f9813) before any run whose output was read; the ruling's
code landed as one commit (5a41648b, subject naming X5-R36). Every
command below is a single seed or the ≤ 200-seed window under the
backstop, output outside the worktree (`/tmp/opencode/x5c-f2f1/`).

**Work 2 - what the X5-R36 commit contains.**
`Model::scan_at(t, S, c)` per (a) with `scan(t, S) = scan_at(t, S, S)`
and a private `drop_floor(t, c)`; O-1(b) at `model.scan_at(t, c, c)`
and O-1(d) at `model.scan_at(t, S, c)`; the three harness scan sites
(`E4_O1_DIAG`'s diff, `key_history_trace` — which gained the `frontier`
parameter — and `o1_at_frontier_lines`) compare at
`model.scan_at(t, x, ex.frontier)`, the cut the recovered store
recovered at; the round record's `reuse_on_dropping` (e) — computed on
the recovery-`Ok` arm by `harness::reuse_on_dropping` over the opened
image's directory read from durable bytes (`opened_dir_dropping`) and
the model's `CreateTree{t}` window `(T, frontier]`, plumbed through
`RoundOutcome`/`RoundRec` and serialized as `"reuse"` (not a digest
input, like `x33h`). Pins: (i) `x5_r36_scan_at_reads_the_drop_incarnation`
(`tests/oracle.rs`, the verdict's three cases), (ii)
`x5_r36_seed112_r2_no_longer_fails_o1b_on_tree_3` (`tests/harness.rs`),
(iii) `x5_r36_reuse_on_dropping_predicate`.

**Work 3 - the repros, each run twice at the ruling's tip (digests
identical, `r36.seed<N>.{a,b}.json`):**

| Round | F2-tip digest | this tip (×2) | outcome |
|---|---|---|---|
| 112 r2 | 3610336029 | **2157138087** | **pass** — matched c=15, frontier 15 (was O-1(b) at c=15, tree 3) |
| 63 r2 | 1693274365 | **2414083785** | **pass** — matched c=24, frontier 24 (was O-1(c) at c=23, tree 2) |
| 104 r2 | 2884791556 | **3776021738** | **pass** — matched c=25, frontier 25 (was O-1(c) at c=23, tree 2) |
| 1749 r2 | 2987258781 | **1584051239** | **pass** — matched c=41, frontier 41, lo 40/hi 41 (was O-1(c) at c=40) |
| 5549 r2 | 2018609054 | **308297210** | **pass** — matched c=32, frontier 32 < hi 33, MW `lo` 30 (was O-1(c) at c=30) |
| 7359 r2 | 266568285 | **670834677** | **pass** — matched c=12, frontier 12, record `lo` 14 (was O-1(c) at c=11) |

All six of root cause B's rounds pass under X5-R36 — no remaining
failure in the set, so no X5-R16 treatment on them. Every round
records `reuse=false`.

**Work 4 - the 0..200 window** (`run --seeds 0..200 --threads 16`,
`r36.0200.json`), final line verbatim:

```
E4 SEEDS=200 O1_FAIL=45 O2_FAIL=0 O3_FAIL=2 DEADLOCK=0 PANIC=0 O1F_FAIL=45 MONITOR=0
E4 FOLD FAILING_SEEDS=2 FAILING_ROUNDS=2 TWIN_FAILING_ROUNDS=0 DEFECT_SEEDS=0
```

`FAILING_ROUNDS=2`, both class MP, both the pre-existing O-3 (e)
exactness family with byte-identical texts at F2's tip
(`X-5c-F3-F3a-F2.s4.0200.json`): seed 8 r0 (crash `Before0@2`)
`O-3(e): (ii) reported slot 7 that is neither tainted nor in R`, seed
178 r1 (crash `uniform@y:37`) `O-3(e): (ii) reported slot 91 that is
neither tainted nor in R`. Their X5-R16 treatment: O-1 is clean in both
(`o1=null`, `o1dl=15` detected-loss comparisons), so `E4_KEY_DIAG` and
`E4_O1_AT_FRONTIER` emit nothing (`r36.seed{8,178}.diag.err`);
`E4_MPE_DIAG` shows the shape — a TORN slot 37 (seed 8) and a LOST +
two MISDIRECTED (seed 178), K and R empty (`mrb=false`), and the (ii)
amcheck naming "mapped lpid N unreachable" slots — the triage's class-2
interplay, unchanged by this card and outside its Limits (no store
change). At F2's tip the same window had 10 failing rounds + 1 twin:
the seven C-DIV rounds (63/104/112/120/125/152/186 r2), 199 r2
(`O-3(d)` silent Ok with `O-1(b) at c=26, tree 4`) and 199's twin r2
all **pass** now, beside the two MP O-3(e) rounds that remain. The
`reuse_on_dropping` count over the window's 305 rounds is **0** — no
ESCALATE. Round totals: C 233, MP 45, MW 27; every failing round is
classed above.

**Work 5 - the frontier pin (commit 595887da).** The O1F line gains
`T=` beside `le_T`, and the diagnostic now runs whenever wanted (armed
or env), not only on an O-1 failure — X5-R36 removed the failures it
was built to explain, and the pin must still read the frontier
candidate of a passing round. Two consequences, measured and handled:

- The diff's reads go through the store's read path, which touches the
  pool, and the pool feeds the harness checkpoint and the next round's
  start store: reading the judged world moved seed 1749's digest
  (1584051239 → 2353463647 with `E4_O1_AT_FRONTIER=1`, before the
  fix). The diff now runs on a **throwaway clone** of the recovered
  world; digests verified identical with the env var on and off for
  1749, 5549, 112 and 63 (`off.<N>.json` / `on.<N>.json`).
- On a passing round nothing diverges, so a "first key" line would
  print only owners at or below T — the exact weakness of finding 3.
  The fallback pick is the key with the **newest owner** at `c`: 5549
  r2's real MW line reads `lo=30 hi=33 frontier=32 T=29 c=32` with
  `owner_ts=32 … le_T=false` (32 is above T 29 and at the frontier),
  which is what makes LE-T-FROM-FRONTIER catchable at all.

The 1749 pin (`o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2`)
was reworked in the same commit: its old literals (tree 4's divergent
owner ts 4, `le_T=true` at `c=41`) described root cause B's failure,
which X5-R36 removes by design — the round now passes, matched at
`c=41`. The pin asserts the same round's always-line: `lo` 40, `hi`
41, `frontier` 41, `T` 38, `c=41`, and the `le_T` ≡ `owner_ts ≤ T`
agreement on every line (checked by parsing the line itself). The new
5549 pin (`x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2`)
asserts `c=32` with `frontier 32 < hi 33` — the round where the clamp
is not the identity on the hi side (1749's frontier equals its `hi`) —
plus the same agreement and the `owner_ts=32/le_T=false` literal. Both
F2-UNCAUGHT mutants of finding 3 are now caught (below).

**Mutation evidence** (each applied to the real code, the named test
failed, the edit reverted completely — `git diff` clean of the mutant
after every revert):

| Mutant | Catching test |
|---|---|
| R36-REVERT (`scan_at` ignores `d`) | pin (i) `x5_r36_scan_at_reads_the_drop_incarnation` **and** pin (ii) `x5_r36_seed112_r2_no_longer_fails_o1b_on_tree_3` |
| R36-FLOOR-BY-S (`d` floored at `S`) | pin (i), the `S < d ≤ c` case ("S < d ≤ c must read empty: {k: v}") |
| R36-FLOOR-IGNORES-CUT (`d` from every drop) | pin (i), the `c < d` case ("c < d must keep the pre-drop content") |
| REUSE-FLAG-OFF (the flag always false) | pin (iii) `x5_r36_reuse_on_dropping_predicate` |
| FRONTIER-AS-HI (`c = hi`) | Work 5's pin `x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2` |
| LE-T-FROM-FRONTIER (`le_T` from the frontier) | Work 5's pin (the `owner_ts=32 … le_T=false` literal) |

**Digest movement.** Every seed whose model holds a durable
drop+re-create changes digest under X5-R36 (the record's `o1`/`c`
fields change with the outcome); `reuse_on_dropping` is deliberately
not a digest input (the `x33h` precedent), and the diagnostics moved
nothing (verified on/off above). The repro table carries the six
moved digests; the window's other digests are in `r36.0200.json`.

## Stage X-5c-F4's record (this card)

**Step 0 — F3-F3b's Acceptance 10k committed as the triage file of
record (the REFRESH's pointer, `X-5c-F3-F3b.acc.*`), and this card's
class table re-derived from it.**

- **(a) The provenance checks passed before the copy:** the prov file
  shows `commit=86ecf8ffa43da89121b0fdb2d4c867dcd418e9db`,
  `status_lines=0`, `exit=0`, `verify_exit=0`; the JSON's sha256 equals
  its `json_sha256`
  (`8332df8d7b6eda073c8bd6f0b93a15b491deb3aad5f70d576d35d4ca46e35202`);
  `86ecf8ff` is an ancestor of this card's base. The crate diff
  `86ecf8ff..HEAD` is **not** clean, exactly as the REFRESH records: it
  is F3-F3a-F2 and F3-F3a-F2-F1's landed work and nothing else
  (e1bd478b root cause A, b7fe409e `E4_KEY_DIAG`, 204afb9a
  `E4_O1_AT_FRONTIER` + the 1749 pin, 5a41648b X5-R36, 595887da +
  20c53912 the frontier pin and the stage records) — so the file
  **predates root cause A's fix and X5-R36**, and its C-DIV 256-family
  counts and its MW five are expected to fall (measured below; neither
  count is quota or judgement evidence).
- **(b) The triage file:** `results/campaign.json` only, this commit;
  the message carries the two E4 lines and the quota table.

Final lines, verbatim:

```
E4 SEEDS=10000 O1_FAIL=2625 O2_FAIL=35 O3_FAIL=83 DEADLOCK=0 PANIC=0 O1F_FAIL=2625 MONITOR=0
E4 FOLD FAILING_SEEDS=375 FAILING_ROUNDS=352 TWIN_FAILING_ROUNDS=36 DEFECT_SEEDS=0
```

Rounds: C 11,861 / MP 2,190 (+ 2,190 twins) / MW 1,335 (+ 1,335 twins).
The quota table (the runner's `verify --no-quotas`, verbatim counts):
**7 misses — Q4 0/1000, Q7 169/200, Q8 80/100, Q11 0/100, Q12 1/100,
Q18 981/1000, SIZING 14,985 misses** (the row's old, round-based
accounting; step 1 re-bases it per seed per X5-R17). Every other quota
is met (Q1's ten hooks 747–800; Q2 4244; Q3 1235; Q5a/b/c 288/8476/7630;
Q6a/b 1675/1204; Q9 12179; Q10 713; Q13a/b/c 911/977/898; Q14 605; Q15
1055; Q16 5640; Q17 9024). These seven are the retune list, with Q8
(added 2026-11-03) and Q18 (added by the REFRESH) beside X5-R17's five.

- **(c) The class table, re-derived from the committed file (the
  reporting rule: served rounds split out first), judged under X5-R30
  (c) from the recorded fields — a served round passes iff
  `(skx ∨ smx) ∧ pass_na` (no re-run).** A **failing** primary round has
  `pass=false` as amended; a **failing** twin has `pass=false` in the
  twin record. MW `lo` is read as X5-R33's `max(c_prev, T, h, a)` (the
  O1F line), never the record's `lo` field (X5-R1's lower bound).

| Class | Rounds | Record-field definition (re-derived, verifiable from the JSON) | Repros (round) |
|---|---|---|---|
| **Served (split out first)** | 10, **all moved to pass** by X5-R30 (c) | MP/MW round with `served=true`: 537, 1887, 2927, 4098, 7238, 7528, 8107, 8578, 8728, 9358 (all r0, all `mrb=true`, all `O-3(a): served corruption` as recorded) — skx 537/1887/4098, smx 2927/7238/7528/8107/8578/8728/9358, all `pass_na=true`. **No failing served round remains.** | 2927 r0, 7528 r0 |
| **C-XX** (class C) | 51 | class C failing round with `o2 != null` or `o3` = `O-3(b): XX001 in class C at [site]` — sites: `Open` 16, `AmcheckIi` 35 | 310 (r0, `Open`), 332 (r1, `AmcheckIi`) |
| **C-DIV** | 251 | class C failing round outside C-XX carrying an `o1` text: (b) 99, (c) 90, (d) 62, (a) 0. **0 carry `0 < frontier < lo`**; by round index r2 248, r0 3, r1 0 — the drop+re-create family X5-R36 clears (the F2-F1 gate's 0..200 window showed 0 C-DIV rounds at the fixed tip) | 112 (r2, b), 63 (r2, c) |
| **C-DIV-in-MW** (the REFRESH's five) | 5 | 1749, 4139, 5549, 5839, 7359 (all r2): `O-1(c) … raw entry above the cut` + `O-3(d)` — root cause B's shape (the drop+recreate gap), carried as C-DIV, **never MW-ORPHAN** (true MW-ORPHAN is 0 in this file: I-WAL-COVER's explained refusals took them); all five pass at the X5-R36 tip (F2-F1's Work 3 table) | 1749 r2, 5549 r2 |
| **MP O-3(e)(ii)** (carried, X5-R29 (c)) | 11 | MP failing round with `o3` starting `O-3(e): (ii)`: 8, 178, 688, 1298, 2038, 3388, 4767, 5598, 7748, 9118, 9978 (8 r0, 3 r1) | 8 r0, 178 r1 |
| **MP poison + task error** — **NEW** | 17 | MP failing round with `crash="poison"` and `terr=true`: 747, 788, 967, 2347, 2988, 3607, 3968, 4127, 4347, 5148, 5328, 6767, 7968, 9168, 9388, 9508, 9968 (16 r1, 1 r2); every one fails through a task error recorded beside the poison crash — zero such rounds in F3-F3a's rework file (the class first appears on F3-F3b's workload, G8-in-full). Root-caused and fixed as this card's step-4 treatment (the section below) | 747 r1, 3607 r1 |
| **Other MW** | 7 | the MW failing rounds outside every class above: 199, 1529, 4349, 5299, 6639, 9699 (O-1(b)/(a)/(d) + `O-3(d)`) and 4419 r1 (O-1(b) only) — the ORPHAN published-set family | 199 r2, 6639 r2 |
| **`reuse_on_dropping`** | **0** | no round in the file carries `reuse=true` — **no ESCALATE** (X5-R36 (e)) | — |
| **The twins** | 36 | failing twin rounds: 9 MP + 27 MW; `o1` kinds: MP (a) 3, (b) 6; MW (a) 8, (b) 8, (c) 5, (d) 6 — the classes echo there (the twin is judged as class C) | 199 r2, 849 r1 |

Partition check: 352 = 10 served + 51 C-XX + 251 C-DIV + 5 C-DIV-in-MW +
11 MP O-3(e)(ii) + 17 poison + 7 other MW; failing seeds 375 =
`FAILING_SEEDS`; `TWIN_FAILING_ROUNDS=36`; 0 livelock/deadlock rounds, 0
defect seeds. Every count equals the REFRESH's (375/352/36); no
deviation to record.

**Steps 2–4 (the retune, the wall budget, and the new class) — the
attempt's record, and why the retune is not landed.** Every number
below is a development probe over ≤ 200 contiguous seeds under the
backstop (X5-R31), output outside the worktree — **probe, not
evidence**; the Acceptance's whole 10k with plain `verify` is the only
quota evidence, and none of these knob states is committed.

- **The baseline (the fixed tip, 0..200):** 27.7 s wall at 16 threads
  (× 50 ⇒ ~23 min for a 10k — the budget's headroom is ×1.7). SIZING:
  191 of 200 seeds miss (the per-seed row of step 1); the misses'
  median best interval reaches 0.41·4·F. Q4 0, Q7 5, Q8 7 (the probe
  aggregate's raw count), Q12 0, Q18 20 rounds.
- **Retune attempt 1** (chunked [900,1500], cb [30,45], ckpt_every
  [8,12], the GC task sweeping every non-dropped tree × 2): SIZING
  57/186 seeds met — but > 10 min for the 200-seed probe (≥ 20× the
  budget). **Attempt 2** (chunked [900,1200], cb [20,31], values drawn
  from the top of the card's 8–400 B range — `VALUE_LO = 280`, ~6 live
  entries per leaf instead of ~9 — GC × 6 passes, drop rounds at half
  of the non-bulk rounds): **61 s / 200 seeds ⇒ ~50 min for a 10k
  (over the 40-min budget), SIZING 88/200 seeds met, Q8 ~450, Q18
  ~1,150, Q4 0, Q12 0, and 0 failing rounds.** The misses split 41
  universe-bound (the seed's whole page universe < 4·F) and 71
  coverage-bound (the crash landed before one interval accumulated
  4·F: an early uniform draw, or a targeted `Before0@k` just after a
  completion reset the interval). **Attempt 3** (chunked [750,1000], 4
  passes — budget-directed shrink): 53 s ⇒ ~44 min, SIZING 58/200,
  **and Q18 fell to ~500** (the pass cut) — a retune that regresses a
  carried quota below its floor is not landable.
- **The three structural findings the probes established:**
  1. **SIZING × the 40-min budget cannot both be met with card-legal
     knobs.** Reaching ≥ 4·F (= 172–232 distinct dirtied LPIDs, F ∈
     43..58 at §5.4's formula) in one interval for **every** seed
     requires (a) a page universe of ~260+ tree pages in round 0 —
     ~2,000+ insert ops per round at any value size, because the dirty
     set counts only pool pages (`store_page`), never the map/free-set
     pages (`write_fresh`) — and (b) that a **full-tree GC sweep pass**
     land inside the crash-cut interval, because a crash that lands
     before ~60 % of the round's yields otherwise sees fewer pages
     than 4·F no matter how big the universe is. Point (a) alone puts
     the measured cost at ≥ 2.2× the budget (61 s vs 48 s per 200) and
     it still leaves 56 % of seeds missing; the only measured point
     that approached the budget (53 s) met 29 % of seeds. The gap is
     not tuning: it is the twin costs (every MP/MW seed runs its round
     twice), the uniform counting pass (every uniform round runs its
     workload twice) and the recovery replays, none of which the card
     allows to shrink. **ESCALATED with these numbers** (the card's own
     valve: "If SIZING and the budget cannot both be met, ESCALATE
     with the numbers").
  2. **Q11 is structurally unreachable: `StoreInner::stale_seg_reads`
     has no increment site anywhere in the crate** (declared
     `src/store/mod.rs`, initialised beside it, snapshotted by the
     harness's `WorkloadObs`, read by the quota flag —
     `grep -rn "stale_seg_reads +=" src/` finds nothing; it has had no
     writer since 4b1e6068 introduced it). No workload retune can move
     a counter that only ever holds its initialiser. The natural wire
     point is the §12.5 stale check (`dec_record_inner`'s `rlsn != lsn`
     arm, `src/store/wal.rs`) or the recovery walk's first read of a
     recycled segment's start — a store/harness instrumentation fix
     outside this card's Limits ("change only: the workload knobs;
     X5-R17's SIZING accounting; fixes for new classes").
     **ESCALATED**; plain `verify` will fail Q11 (0 < 100) at any
     workload until P rules it wired or waived.
  3. **Q4's two halves pull the value distribution in opposite
     directions.** The merge half counts only GC pack steps
     (`count_merge` has one call site, inside `gc_locked`), and a pack
     fires only when a leaf pair's kept halves fit `F_PACK·U =
     0.85·U` — i.e. both leaves under ~42 % full; with uniform values
     the sweep's own rewrites keep pairs at ~2×50 % = 1.0·U
     (measured: merges ≥ 1 in 1–2 of ~305 rounds at every knob state
     tried, and 0 in the step-0 file). The SIZING half wants big
     values (~6 entries/leaf ⇒ more pages per op); the merge half
     wants small values (dense leaves ⇒ half-full pairs fit 0.85·U). A
     knob state that satisfies both was not found inside the budget;
     the candidate design (the chunked txn at the top of the value
     range for the universe, small commits at the bottom drawn from a
     hot sub-range for split/merge churn) is recorded here for the
     next attempt. **ESCALATED as infeasible-within-scope alongside
     (1).**
- **Why no retune is committed:** beside (1)–(3), any knob change
  moves every seed's workload wholesale and breaks four seed pins —
  `x5_r36_seed112_r2_no_longer_fails_o1b_on_tree_3` survived, but
  `o1_at_frontier_diag_names_the_frontier_candidate_of_1749_r2` and
  `x5_r36_o1f_line_pins_the_frontier_candidate_of_5549_r2` (two of the
  REFRESH 3's five protected pins) plus
  `x5r35_seed403_slice_feeds_the_derivation` and
  `r30_skx_and_smx_record_fields` fail at every retuned knob state
  measured. The REFRESH's own rule ("re-express the pin as a literal
  config that reproduces the same round. Never change its assertions")
  needs re-derivation scans per pin, which this attempt's 60-minute
  budget could not hold beside the probes (the first attempt of this
  card was in fact killed at the runner's 60-minute cap mid-probe).
  Landing a retune that leaves red tests or regressed carried quotas
  (Q18) would fail the Acceptance outright; the honest landing is the
  fixed code of steps 0, 1 and 4 with this record.
- **Step 4 — the one new class in the step-0 file, fixed (commit
  3fc325a8):** the **MP poison + task-error class** (17 rounds — 747,
  788, 967, 2347, 2988, 3607, 3968, 4127, 4347, 5148, 5328, 6767,
  7968, 9168, 9388, 9508, 9968; zero in F3-F3a's rework file). Every
  round is `crash="poison"` with `terr=true`, the task error
  `checkpoint failed: Poisoned("XX001 in gated op: lpid N slot M (G8,
  9.2)")`. Root cause: `checkpointer_body`'s `h:ck` poisoned arm fell
  through to a final checkpoint whose first step refused on `alive()`
  with the external G8 mark, and the error arm recorded the store's
  designed post-mark refusal as a round-failing harness task error —
  contrary to G8's harness contract ("the poisoned store is the end of
  the round's workload … the faults, the recovery and the oracle run
  as usual"). Classification: instrumentation defect (harness), the G8
  half. The fix (`checkpoint_poison_end`, both at the poll and
  mid-checkpoint, typed on `Fail::Poisoned` and gated on the new
  `StoreInner::poison_fail_stop` flag — set only by the checkpoint
  path's own fail-stop sites, so D12b's `gc_w > T` and D4's
  meta-bound refusals keep their loud task errors). Pin
  `g8_checkpoint_poison_ends_the_workload` (seeds 747/967 r1: crash
  `poison`, fired, no task error, no livelock, the round passes as the
  detection round it is); the revert mutant (both quiet arms restored
  to the draft) fails it with the exact class text — applied, shown,
  reverted clean; the single-arm reverts do not fail it (each arm
  covers the other — recorded). Movement: all 17 named seeds pass at
  the fix tip; 0..200 gives `FAILING_ROUNDS=2` (8 r0, 178 r1 — the
  pre-existing MP O-3(e)(ii) pair, unchanged); the whole suite is 212
  tests green.

**The Acceptance's own run (the runner's, at this card's tip
a812035f, 2026-11-04) — the quota feedback consumed, and the record's
one correction.** The runner's 10k (`X-5c-F4.acc.*`, prov
`commit=a812035f`, `exit=0`, json `bec7056c…`, 06:27:30→06:44:33 =
**17.1 min at 16 threads**) and its plain `verify` (17.3 min,
`verify_exit=2`) behaved exactly as predicted: the digests and the
seed set reproduce; the quota table fails on the same seven — Q4
0/1000, Q7 168/200, Q8 82/100, Q11 0/100, Q12 1/100, Q18 979/1000,
SIZING 9,628 seed misses (the step-1 accounting live in `verify`).
Two things this run teaches, recorded as the card's feedback loop
demands:

1. **The 40-min budget was NOT the binding constraint — the record
   above over-weighted it, and this corrects it.** With the runner's
   17.1-min datum, retune attempt 2's measured 61 s / 200 seeds
   extrapolates to **~38 min for the 10k** (61/27.7 × 17.1), inside
   X5-R17's ≤ 40 min, and its verify (~38 min) fits the runner's
   60-min command limit with the same margin the unchanged workload
   has. The budget alone would have allowed landing attempt 2
   (SIZING 9,626 → ~5,600 misses, Q8 ~450, Q18 ~1,150 — Q7 beside
   them, SIZING/Q4/Q11/Q12 still short).
2. **The binding constraint is the pin re-derivation, and it is
   structural, not temporal.** Every knob state measured breaks four
   seed pins (two REFRESH-3-protected); the r30 pin
   (`r30_skx_and_smx_record_fields`) needs **five served MP-A rounds**
   (two skx + three smx, beside 6149's not-served shape), and served
   rounds occur at ~10 per 10,000 seeds (the step-0 file: 537, 1887,
   2927, 4098, 7238, 7528, 8107, 8578, 8728, 9358 — none under 500;
   the retuned-knob probes: 0 in 0..200 at both attempt-2 and
   attempt-3 states). Re-deriving five by ≤ 200-seed windows needs
   ~25 windows ≈ 25+ minutes of probes **per attempt, at luck**, with
   no guarantee any window holds one — and the REFRESH's own rule
   forbids changing the assertions or shrinking the groups. F3-F3a
   step 3 re-pointed its two by luck of the then-workload (537/1887
   landed in its first window); this card's states have none in the
   two windows measured. **No retune of this card can land until P
   amends the pin** (a fixture form, a reduced group, a wider
   re-derivation allowance, or a blessed knob state whose windows
   carry served rounds) — this is now the primary ESCALATE item,
   ahead of the SIZING×budget numbers.

**The hand-off for the next attempt (and for P):** the quota table
above is the live frontier; the reachable-in-scope retune (attempt 2's
knobs, numbers above) is blocked by the r30 pin alone once the budget
correction is applied; SIZING, Q4 and Q12 are blocked by the
structural findings (the dirty set counts only `store_page` pool
pages; pack merges need ≤ 42 %-full pairs vs SIZING's ~6-entry
leaves; Q12's renames never land near crashes at any state measured);
Q11 is blocked by its dead counter. Everything except the r30-pin
amendment is P's to rule on.



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
cargo test -q --release -p e4-recovery      # the whole crate (204 tests)
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

**X-5c-F3-F3a's Acceptance** (this card's; the same X5-R31 discipline
held — every in-attempt command was a single seed or a ≤ 200-seed run
under the backstop with output outside the worktree):
`$HOME/.cache/nv3-review/runlogs/X-5c-F3-F3a.acc.campaign.json` (the
10k `run --seeds 0..10000 --threads 16`, `ulimit -v 25165824`), its prov
in `X-5c-F3-F3a.acc.prov`, then `verify --no-quotas` of that file
(`X-5c-F3-F3a.acc.verify.out`), then the record check (the `ack_over` /
`C_FRONTIER_BELOW_LO` / `MISSING_FIELDS` python one-liner of the card's
Acceptance) and `bash scripts/gate.sh`. **Its 10k becomes F3-F3b's
step-0 triage file**, and from it on, X5-R34 (h) 5 makes every failing
class-C `0 < frontier < lo` round a store loss for P's triage (405 r1,
moved by 8b71e560, will be in it). This card's step-0 file
(`results/campaign.json`, commit 929ade38) is F3-F1's Acceptance 10k —
this card's own Acceptance 10k is **not committed** (F3-F3b's step 0
commits it after its own provenance checks).

**X-5c-F4's Acceptance** (the runner's, X5-R31 — this card runs no
campaign-class command in its attempts; the probes above were ≤ 200
contiguous seeds under `ulimit -v 25165824` with output outside the
worktree): `cargo test -q --release -p e4-recovery`, then
`$HOME/.cache/nv3-review/runlogs/X-5c-F4.acc.campaign.json` (the 10k
`run --seeds 0..10000 --threads 16` with the card's prov lines), then
**plain** `verify --results` of that file (the card's form — the quota
table decides), then `bash scripts/gate.sh`. **Measured at this tip
(the runner's own run, 2026-11-04):** the 10k is 17.1 min at 16
threads, `verify` 17.3 min and **exit 2 on the quota table alone**
(Q4 0/1000, Q7 168/200, Q8 82/100, Q11 0/100, Q12 1/100, Q18 979/
1000, SIZING 9,628 seed misses) — the digests and the seed set
reproduce; the gate itself stays green (fmt, clippy, the suite's 212
tests). The workload at this tip is the step-0 workload: this card's
probes moved no committed knob.

**X-5c-F3-F3b's Acceptance** (the same X5-R31 discipline — the
in-attempt commands of this card's attempts were the 0..200 step
windows, the named single seeds and the five ESCALATE single seeds,
each under `ulimit -v 25165824` with output outside the worktree):
`$HOME/.cache/nv3-review/runlogs/X-5c-F3-F3b.acc.campaign.json` (the
10k `run --seeds 0..10000 --threads 16`, `ulimit -v 25165824`), its
prov in `X-5c-F3-F3b.acc.prov`, then `verify --no-quotas` of that file
(`X-5c-F3-F3b.acc.verify.out`), then this card's record check (the
python one-liner of the card's Acceptance, printing `MW_ORPHAN_SHAPE`,
`COVER_ERR` per class, `MW_UNEXPLAINED`, `ACK_OVER_ROUNDS` and
`MISSING_FIELDS`) and `bash scripts/gate.sh` — all at the card's final
tip (the README commit), where a README-only change cannot move a
digest, so the re-run reproduces the same JSON (the prov's `commit=`
line names that tip; on a retried attempt the last write wins). **Its
10k becomes X-5c-F4's step-0 triage file** (F4's campaign is the E4
re-run under the unchanged rule), and the five remaining
orphan-shaped rounds it holds are ESCALATED to P in the stage record
above.

**The X-5c-F2 Acceptance 10k is the runner's, never a worker's
(X5-R31).** Inside an attempt no worker runs `e4-campaign run` over
more than 200 contiguous seeds, `e4-campaign verify`, a stress loop,
or `scripts/gate.sh` — X-5c-F1 lost three runs to campaign-class
commands inside attempts (a 10k `run` and a `verify` each take ~20 min
at load 20, against the runner's 60-minute attempt limit). The
Acceptance, at the card's final tip, is:

```bash
cd nucleus/v3
O=$HOME/.cache/nv3-review/runlogs/X-5c-F3-F3a.acc
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
X-5c-F3-F3a's own verify (at 4cefcb43) adds a sixth, Q8 (acked SyncOff
commits lost, 80/100 — the store this card built acks fewer SyncOff
losses): also waived by `--no-quotas`, also F4's to weigh. The rework's
verify (at 6fa80bb5) keeps it waived at 82/100.
X-5c-F3's step 0 commits `$O.campaign.json` after the provenance
checks (the X5-R25 pattern). Single-seed diagnostics stay allowed
inside attempts: `E4_MPE_DIAG=1 cargo run -q --release -p e4-recovery
--bin e4-campaign -- run --seeds N..N+1 --threads 16 --out <outside
the worktree>` (the K/K_ii/served_k and R29 diag lines are stderr
only, never digest inputs).

X5-R15 (e): `examples/` keeps only what this block uses (`pin.rs`;
`dbg-seed.rs`, which 823d4f51 called scratch, is removed), and
`restore_probe.local.py` (untracked) is not in the tree.

