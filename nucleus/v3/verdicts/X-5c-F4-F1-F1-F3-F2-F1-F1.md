Card: X-5c-F4-F1-F1-F3-F2-F1-F1
Class: test
Card-gate: P
Verdict: ESCALATE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-gate-X-5c-F4-F1-F1-F3-F2-F1-F1 (Claude Code session a8aa6954-8b54-40d6-bfd8-f20560378018)
Date: 2026-11-07
Gate-exit: GATE EXIT=1
Commit: ad4fa9991e8e3ddfea88909e3180f87c69d33538
Mutants: M-CMP-NO-PF -> self-test (9 mpa/pass-fail cases) + real 10k (MPA_PASS_FAIL 0, 7078 r0 absent, NEW_FAILING 0), M-CMP-MPA-NOJUDGE -> mpa-skx-smx-false + mpa-pass-na-false, M-CMP-META-ANYSLOT -> meta-only-some-finding, M-CMP-TWIN-NODIR -> twin-t2-both-dirs-lack-N, M-CMP-TWIN-ANYCLASS -> twin-mp-seed-prefilter-fails, G-META-NO-Q13 -> meta-only-referenced-slot, G-EXIT-NOBAD -> UNCAUGHT, G-T4-NO-SHORT -> UNCAUGHT, G-TWIN-PREFILTER-ALLJ -> UNCAUGHT, G-T3-ANYCLASS -> UNCAUGHT
Followups: X-5c-F4-F1-F1-F2-F2, X-5c-F4-F1-F1-F3-F2-F1-F1-F1, X-5c-F4-F1-F1-F3-F2-F1-F1-F2

## Verdict in one paragraph

ESCALATE, **on the gate run alone.** The card's content passes; on content I would write MERGE-WITH-FOLLOWUPS, and no rework is needed.

The fresh gate reads `GATE EXIT=1` twice. The only red is `ack_prefix_two_batches_in_flight` (`STEP test EXIT=101`). That failure is **pre-existing and not caused by this card**:
- it fails identically at eb9c4328, whose `src/` and `tests/` are byte-identical to ad4fa999;
- it has a 30 s wall-clock deadline;
- on this machine, at idle, it needs about **104 s** in debug. So it now fails deterministically (3/3 when run alone), and it is not a load flake.

`merge-card` accepts only `GATE EXIT=0`, so the owner must decide. Either land X-5c-F4-F1-F1-F2-F2 (the deadline card) and then re-run only the fresh gate on this tip, or rule this row out as was done for `runner e2e: K`. The content findings below stand either way.

**Content.** The re-read of F2-F1's 10k under X5-R52 holds:
- I re-derived it independently, from the raw JSON, with my own classifier.
- I reproduced both twin rows in my own patched scratch build.
- I re-checked all 52 `MOVED_MP_PENDING` rows and 7078 r0 from recorded fields.

No round is silently dropped, and nothing falls outside X5-R52. Each of the three exemptions is drawn as narrowly as the card's text in the comparator's compare path. Every adversarial round I built and dressed as an exemption came back a new failure, with one exception: the gate-side `check-twin` T1/T3 check is **wider than the card's text**. It accepts any class-C recovery `Err` line for round `i`, including the uniform probe on the real chain or a twin re-run, not only the twin's own line.

That gap does not reach this card's data. In both pending rows, the round has exactly one class-C `Err` line, and it sits in the twin segment. Under X5-R52 (g), `check-twin` has no further use at -F2's re-seed commits. So the gap is a follow-up, not a block. X5-R52 matches the ruling in 8992f77f with two relaxations of the verdict's proposals, both inert on this data:
- (iii) has no "class count does not grow" cap;
- (i) uses T3/T4 in place of the ts-level link.

## Gate run

- **Scaffold.** `acc81c776183bd9853f7a69e2cec4262d300d89f`, extracted through the `--no-local` shallow clone. No `EXTRACT FAILED` line.
- **Run 1** (concurrent with my e4-campaign build and runs):
  - `fresh-gate: commit ad4fa9991e8e3ddfea88909e3180f87c69d33538`
  - `fresh-gate: base acc81c776183bd9853f7a69e2cec4262d300d89f`
  - `fresh-gate: checks passed (4517 files hashed in 1.99 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - `STEP test EXIT=101`: `ack_prefix_two_batches_in_flight`, "driver timeout: B never completed batch 2 (a_enq=true b_go=true b_wrote=false)"
  - clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests all EXIT=0. `runner e2e: K` is ok.
  - Last line: `GATE EXIT=1`.
- **Run 2** (nothing else running, load average about 2 on 16 cores): identical `fresh-gate:` lines (hashing took 2.00 s). Same single red, same last line, `GATE EXIT=1`.
- **Triage.** Run alone in debug in a clean `git archive` export:
  - ad4fa999: FAILED 3/3, each at 30.00 s;
  - the same in release: ok in 10.4 s;
  - eb9c4328: FAILED identically (`src/` and `tests/` are byte-identical);
  - a scratch copy with the deadline at 300 s: ok in 103.69 s. So this is slowness, not a hang.

  The card changes no Rust. The failure belongs to X-5c-F4-F1-F1-F2-F2 (QUEUE: "30 s wall-clock deadline flakes under load … at base too"). That card should now be marked deterministic in debug on this host. The scratch export was deleted.

## Scope

- `8992f77f..ad4fa999` touches only:
  - `spikes/e4-recovery/README.md`;
  - `tools/x5r52_compare.py`;
  - `tools/x5r52-twin-diag.patch`.

  All three are in Touch only.
- `git diff eb9c4328 ad4fa999 -- src tests Cargo.toml` is empty. No code changed.
- The worktree is clean.
- README:
  - all 35 non-blank lines of X5-R51 and all 105 of X5-R52 appear verbatim;
  - X5-R51 equals its source card's text;
  - the only deleted line is the old Status line.

## Re-read reproduced (question 3)

- **Hashes.** `sha256` matches for the run JSON (`6b686266…739a`) and for the ref (`f4d6f1ff…c923`).
- **The committed comparator.** I extracted it with `git cat-file` from ad4fa999, not from the worktree, and ran it:
  - `compare` exits 0;
  - DIGESTS_MOVED 2589, FAILING_ROUNDS 19, MONITOR 0;
  - gained 87, MP pending 52, META_ONLY 4 [(1927,1),(7257,0),(7658,0),(8278,0)];
  - MPA 1 [(7078,0)], TWIN_DDL 2 [(2599,2),(2859,1)];
  - lost 94, healed 3 [(457,0),(1298,1),(8777,0)];
  - twin-healed 1 [(6539,1)], text-changed 131;
  - twin failing 7 (Base 6), NEW_FAILING 0, BAD_SEEDS 0.

  This is exactly P's reference reading.
- **My independent classifier.** It does not import the comparator. It asserts that both files hold exactly seeds 0..9999, with the same class per seed.
  - Rounds: 15410 at the tip, 15407 at the Base.
  - Categories: same 15092, gained 87 (MP 52, MW 35), changed text 131, text dropped 78, fail→pass 3, pass→fail 1 (7078 r0), fail→fail 18.
  - Tip-missing: 16 with a Base text and 19 text-free.
  - These sum to 15445, which is 15410 tip rounds plus 35 tip-missing. **Every round is accounted for.**
  - The 94 lost rounds are 78 + 16.
  - The 19 text-free tip-missing rounds all belong to tip seeds that `stopped` right after an O-1(a) round. That round is itself classified (gained or twin).
  - Twins: 3520 at the tip, 3517 at the Base. True→False [(2599,2),(2859,1)], False→False 5, False→missing [(6539,1)], True→True 3475, True→missing 34, missing→True 38.
- **Failure paths.**
  - A bad `--run-sha256` exits 2 before any reading.
  - The self-test's range-mismatch and sha-mismatch cases exit 2.
  - A synthetic panic seed at both ends gives `BAD_SEEDS [1]` and exit 1.
- **Calibration.** Over F2's `m/tip_{0..800}` against `b1k.json --range 0..1000`, the reading is:
  - 243/4/0, 7 gained, MP pending [(47,1),(547,0),(797,0),(967,0)];
  - 5 lost, healed [(457,0)], 0 new;
  - every X5-R52 row empty, twins 2/2.

  This equals X5-R51's calibration.

## The gate's single-seed checks (X5-R52 (f))

- **check-mp, all 52 rows.** I read the recorded `gate-…-F3-F2-F1/m/pend/s<seed>.{err,json,out}` from eb9c4328. That is the same code, and T5 holds for each row: digest equal, MONITOR=0.
  - Result: 48 `EXPLAINED (b)5` and 4 `EXPLAINED (b)5'`.
  - The (b)5′ rows are exactly the four META_ONLY rows. Their media hits slot 1/2 by `off` 4096, or by Misdirected `to_off` 4096/8192.
  - Parse check: in every one of the 52 blocks, the count of `Finding {` equals the count of parsed findings. Each block has exactly one header and exactly one `amcheck_i` line. So no finding is silently skipped.
  - From the JSON: all four meta-only rows have `pass=true c=null frontier=0 mrb=false served=false`, `q[12..16]=[0,0,0,1]`, and `o2`/`o3` null.
- **check-twin, from my own build.** Export of ad4fa999, `git apply` of the committed patch, `cargo build --release`, then single-seed runs with `ulimit -v 25165824`. Files are in `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F3-F2-F1-F1/t_*`.
  - **2599 r2:** `EXPLAINED N=3 k=2`. Real r1 has lost=1, torn=1, frontier=1, hi=5. T2 line: `real_dir=[1, 2, 3] twin_dir=[1, 2]`. T3: twin r1 shows `ts=7 ddl=["D3"]`.
  - **2859 r1:** `EXPLAINED N=4 k=1`. Real r0 has mis=1, frontier=1, hi=7. T2 line: `real_dir=[1, 2, 3, 4] twin_dir=[1, 2, 3]`. T3: twin r0 shows `ts=3 ddl=["D4"]`.
  - Both runs have MONITOR=0, and both digests equal the 10k file's.
  - **The verdict's stricter ts-level form also holds.** Each real round `k−1` replays only `ts=1` (the CreateTree record) and loses the later DropTree. Each round `i` has exactly **one** class-C `Err` line, and it comes after the `round-start` line, so it is the twin's, not the probe's.
- **7078 r0 (iii), from the raw JSON.**
  - Base: `uniform@y:5875`, pass, mrb, c null, q15=1, O-1(a) only.
  - Tip: `uniform@y:10344`, pass=false, served, mrb, c null, q15=1, `smx=true`, `pass_na=true`, `o3="O-3(a): served corruption"`, o2 null.
  - MP-A, counted as served∧mrb MP rounds: Base 11, tip 10. Every one meets `(skx∨smx)∧pass_na`.

## Adversarial rounds (question 1)

The harness is `scratchpad/adv.py`, run against the committed script.

| Case | Result |
|---|---|
| Base O-1(b), Base o2, Base o3, Base q15=0 | `new_failing bullet1` |
| tip c≠null, mrb=false, pass_na=None, q15=0, o3 with a trailing space, MONITOR=1 | `new_failing bullet1` |
| Base pass=false → tip fail | no row, correctly: not a regression |
| Meta-only + `served=true` with no o3 | MOVED_MP_PENDING but not META_ONLY, so check-mp applies (b)5 and a `None` finding fails |
| Meta-only + `served=true` with an o3 text | bullet 2 |
| Meta-only + `served=true` with pass=false | bullet 1 |
| Twin JSON prefilter, with the only real fault at `j=i` | new failure |
| Twin T3 with the drop only in a Mw listing | NOT-EXPLAINED T3 |
| Twin with divergence at k=0 | NOT-EXPLAINED T4 |
| Twin with only `WalLoss` | NOT-EXPLAINED T1 |
| Twin whose own `Err` is `WalLoss`, with a class-C probe line in the same round saying `tree 4 missing` | **EXPLAINED (gap)** |
| Twin `tree-missing` followed by a pairing re-run `WalLoss` | **EXPLAINED (gap)** |

The prefilter's O-2-twin corner is moot: twin entries carry only `o1` and `pass`. I checked this on both files.

## Comparator read (question 2)

I read all 1,175 lines. No exception is swallowed into a pass:
- JSON and sha errors exit 2, or raise.
- Missing fields fail closed. Prefilter `frontier`/`hi` default to 0, so `0<0` is false. A missing `pass` at the tip counts as a fail.
- Every run seed is iterated.
- The range check is set equality. A duplicate seed raises.
- Rounds are joined by `(seed, index)` over `max(len)`. Twins are joined by `(seed, index)` in both directions.

Non-blocking points:
- (a) The T1/T3 attribution gap above.
- (b) `seed_is_bad` ignores a seed-level MONITOR. MONITOR is a run-level count, and the exit code does not fail on MONITOR>0 by itself; it only voids exemptions. On this run MONITOR is 0, so this is moot.
- (c) Four of my mutants are UNCAUGHT by `self-test`. The code they mutate is correct as written, so these are coverage holes:
  - BAD_SEEDS-only exit;
  - T4's `cover_err ∨ frontier<hi` half;
  - the prefilter's `j<i`;
  - T3's class-C restriction.

## Rule vs ruling (question 4)

- **(ii) is strictly narrower than proposal 2.** It adds the q15/q13, mrb, served, c and frontier prefilter, the all-`None` "no valid meta" condition, and `tainted {}`.
- **(i) T1–T5 vs proposal 1.** T2/T4 replace "DropTree ts ≤ opened meta t, and the real round lost that ts". That is weaker as text. Both rows meet the stricter form (above), and (g) voids (i) at -F2's re-seed.
- **(iii) vs proposal 3.** It adds the `(skx∨smx)∧pass_na` judgement and `q15`, but drops the proposal's "while the class count does not grow". The count fell here (11→10), so this is inert now. It matters for -F2's re-issue, which is P's call.

## Mutants

The card's five are all caught, and M-CMP-NO-PF over the real 10k drops 7078 r0 (MPA 0, NEW 0, exit 0: the original gap). Of my five, G-META-NO-Q13 is caught. The other four are UNCAUGHT (header). All mutants were scratch copies, and the worktree is untouched.

## Follow-ups

- **X-5c-F4-F1-F1-F2-F2 (existing; raise its priority).** `ack_prefix_two_batches_in_flight` runs about 104 s in debug against a 30 s deadline, even on an idle host. It blocks `GATE EXIT=0` for this card and for every card on the chain until it is fixed or ruled.

- **X-5c-F4-F1-F1-F3-F2-F1-F1-F1 (test/tools, before any reuse of `check-twin`).**
  - In `check-twin`, T1 must take the class-C `Err` line of the twin's own run: the class-C run after round `i`'s `X5R52 round-start` line, its last attempt. It must refuse (NOT-EXPLAINED) when another class-C `Err` line for round `i` differs.
  - T3 must read twin segments only, not probe segments on the real chain.
  - Add `self-test` cases for both adversarial logs above. Also add cases that kill G-EXIT-NOBAD (assert the exit code on a BAD_SEEDS-only reading), G-T4-NO-SHORT, G-TWIN-PREFILTER-ALLJ and G-T3-ANYCLASS.
  - No re-read is needed: both rows here already meet the strict form.
- **X-5c-F4-F1-F1-F3-F2-F1-F1-F2 (P, rule note for -F2's re-issue).**
  - P decides whether X5-R52 (iii), as re-issued for -F2, needs the verdict's "MP-A class count does not grow" cap. Today a growing MP-A class would be reported in the class sizes but never stop a comparison.
  - P also decides whether a MONITOR>0 run should fail the comparator's exit by itself (X5-R51 bullet 3), rather than only voiding exemptions.
