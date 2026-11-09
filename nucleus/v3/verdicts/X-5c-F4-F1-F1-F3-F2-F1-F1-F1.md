Card: X-5c-F4-F1-F1-F3-F2-F1-F1-F1
Class: test
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: Claude Code session a6da87d8-1ef8-4293-80a9-9cbbea44f0b5
Date: 2026-11-08
Gate-exit: GATE EXIT=0
Commit: bc18b545aa86195c4e9758b5c09cc70bbd0c53c9
Mutants: G-EXIT-NOBAD -> exit-bad-seeds-only, G-T4-NO-SHORT -> twin-t4-no-short, G-TWIN-PREFILTER-ALLJ -> twin-prefilter-fault-at-i-only, G-T3-ANYCLASS -> twin-t3-mw-only + twin-t3-probe-only, M-T1-ORIGINAL -> twin-t1-probe-only + twin-t1-probe-no-twin-err + twin-t1-pairing-rerun-differs, M-T1-NO-TWIN-REQ -> twin-t1-probe-no-twin-err, M-T1-NO-DIFFER -> twin-t1-pairing-rerun-differs + twin-t1-probe-differs-from-twin, M-T1-DIFFER-TWIN-ONLY -> twin-t1-probe-differs-from-twin, M-T1-ONE-ERR -> twin-t1-identical-rerun-ok, M-T3-PROBE -> twin-t3-probe-only, M-T6-NO-META -> twin-t6-drop-above-meta, M-T6-ANY-ROUND -> twin-t6-drop-other-round, M-T6-NO-REAL -> twin-t6-real-replayed-drop + twin-t6-no-real-listing, M-T6-ANY-TS -> twin-t6-real-listed-above-frontier, M-T6-CROSS-CLOCK -> twin-t6-cross-clock, M-MPA-NO-CAP -> mpa-cap-grown-voids, M-MPA-CAP-GE -> mpa-cap-equal-keeps, M-MPA-CAP-ANYCLASS -> mpa-cap-counts-class-mp-only, M-MON-NO-STOP -> monitor-run-stops, M-CMP-NO-PF -> 12 self-test cases + real 10k (MPA_PASS_FAIL 0, 7078 r0 absent, exit 0), M-CMP-MPA-NOJUDGE -> mpa-skx-smx-false + mpa-pass-na-false, M-CMP-META-ANYSLOT -> meta-only-some-finding, M-CMP-TWIN-NODIR -> twin-t2-both-dirs-lack-N, M-CMP-TWIN-ANYCLASS -> twin-mp-seed-prefilter-fails, J-WALK-STARTED-ANY -> twin-t1-probe-no-twin-err, J-T6B-LT -> twin-t6-real-replayed-drop, J-T6B-NO-COVER -> twin-t4-cover-err-short, J-MPA-VOID-KEEPS-ROW -> mpa-cap-grown-voids, J-MPA-VOID-NO-FAIL -> mpa-cap-grown-voids, J-MON-GT1 -> monitor-run-stops, J-MPA-LINE-ORDER -> mpa-class-line, J-WALK-OTHERSEED -> UNCAUGHT, J-META-T-FIRST -> UNCAUGHT, J-T6A-ANY-T -> UNCAUGHT, J-T6A-LT -> UNCAUGHT, J-T6B-NO-X33X -> UNCAUGHT, J-T6B-FIRST-LISTING -> UNCAUGHT, J-T6B-ANYKIND -> UNCAUGHT, J-T6A-ANYKIND -> UNCAUGHT, J-T1-SEG-ANYROUND -> UNCAUGHT, J-T4-K-MAX -> UNCAUGHT, J-T3-ANYROUND -> UNCAUGHT, J-MPA-NO-MRB -> UNCAUGHT, J-MPA-NO-SERVED -> UNCAUGHT, J-MPA-BASE-NO-CLASS -> UNCAUGHT, J-MON-LASTFILE -> UNCAUGHT, J-MON-LASTLINE-ONLY -> UNCAUGHT
Followups: X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1, X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F2

## Verdict in one paragraph

MERGE-WITH-FOLLOWUPS. The comparator implements X5-R52 and X5-R53 as written, and the re-read reproduces: 0 new failing, the same exempt rounds, and no STOP. All 24 of the card's mutants are caught. The ts rewrite is correct on the clock question. The cap is computed correctly from recorded fields. MONITOR>0 stops the comparison.

I found no hidden loss in this data. The narrowed checks do leave three rule-level holes in T4/T6 and one code hole in `read_monitor`. The T4/T6 holes cannot reach this card's two twin rows, and `check-twin` is not used again unless P re-issues it. The `read_monitor` hole matters for -F2's five-file comparison, so -F1 below should land first. 16 of my 23 own mutants survive `self-test`. In each case I confirmed that the code under the mutated line behaves as the rule says (or found the rule-level hole), so those survivors are test gaps, as I rule below.

**One correction to the request's framing.** `src/`, `Cargo.toml` and the patch are identical to `ad4fa999`. `tests/` is not: `tests/store.rs` differs by +13/-2 lines. That is 03eb5de1 (the `ack_prefix` deadline, card X-5c-F4-F1-F1-F2-F2), which reaches this card through its Base. Acceptance line 2 does not cover `tests/`. `26bcee4b..bc18b545` touches only the README and `x5r52_compare.py`, so this card changed no test.

## Gate run

- Scaffold `bacc4d1135f616e6dccb0d9bc3c341964dee5a91`, extracted through the `--no-local` shallow clone. No `EXTRACT FAILED`.
- `fresh-gate: commit bc18b545aa86195c4e9758b5c09cc70bbd0c53c9`
- `fresh-gate: base bacc4d1135f616e6dccb0d9bc3c341964dee5a91`
- `fresh-gate: checks passed (4519 files hashed in 1.38 s)`
- `fresh-gate: 2 file(s) matched through their eol conversion`
- `STEP enforcement-tests EXIT=0`, last line `GATE EXIT=0`. The worker's container read `GATE EXIT=1` on the ruled `runner e2e: K` row. Here K is ok.

## Scope

- `git diff 26bcee4b bc18b545 --name-only`: `spikes/e4-recovery/README.md` and `tools/x5r52_compare.py`. Both are in Touch only. The wider `nucleus/v3-scaffold...HEAD` diff is the chain beneath this card.
- `check_mp`, `meta_only_eligible`, `mpa_pass_fail` and `twin_ddl_prefilter` are byte-identical to `26bcee4b` (AST extraction of each function from the committed blobs). Changed: `compare_reading`, `format_rows`, `check_twin`, `self_test`. Added: `walk_seed_segments`. `compare_exit_code` did not need to change.
- README: all 105 non-blank lines of X5-R52 and all 38 of X5-R53 appear verbatim. The only deleted lines are the old Status paragraph.
- Worktree clean. All mutants were scratch copies outside it.

## The re-read (question 5)

Inputs matched their sha256 (`6b686266…739a`, `f4d6f1ff…c923`, and all six `t_2599.*`/`t_2859.*`). I ran the tip and chain-head comparators extracted with `git cat-file`.

- Both exit 0. `diff` shows one added line, `MPA_CLASS base 11 tip 10`.
- DIGESTS_MOVED 2589, FAILING_ROUNDS 19, MONITOR 0, MOVED_MP_PENDING 52, META_ONLY_ELIGIBLE 4 [(1927,1),(7257,0),(7658,0),(8278,0)], MPA_PASS_FAIL 1 [(7078,0)], TWIN_DDL_PENDING 2 [(2599,2),(2859,1)], HEALED 3, TWIN_HEALED 1 [(6539,1)], NEW_FAILING 0, BAD_SEEDS 0. This is the card's Expected.
- `check-twin`: 2599 r2 `EXPLAINED N=3 k=2` (ts_D 7, meta.t 9, one real record, frontier 1). 2859 r1 `EXPLAINED N=4 k=1` (ts_D 3, meta.t 7, one real record, frontier 1).
- `check-mp` over all 52 recorded rows: 48 `(b)5` and 4 `(b)5'`, the (b)5′ rows being exactly the META_ONLY set.

**Independent count** (a script that imports nothing from the comparator; both files hold exactly seeds 0..9999 with equal classes):

- Rounds: 15410 tip, 15407 Base. Twins: 3520 tip, 3517 Base.
- 15092 same (15073 + 19 text-free with no Base round), 87 gained (68 with a Base round + 19 with none), 131 changed text, 78 text dropped, 3 fail→pass, 1 pass→fail (7078 r0), 18 fail→fail, and 35 tip-missing (16 with a text, 19 without). These sum to 15445, which is 15410 + 35. Every round is accounted for.
- Twins: True→False exactly [(2599,2),(2859,1)]; False→missing exactly [(6539,1)].
- MP-A from raw fields: Base 11, tip 10. Joined [(7078,0)], left [(457,0),(8777,0)]. All 21 meet `(skx ∨ smx) ∧ pass_na`.

## 1. `check-twin` and the ts rewrite

The author's clock claim is right. In 2599 r1 the twin's CreateTree is `ts=5` (log line 74) and the real chain's is `ts=1` (line 122). Comparing `frontier` with the twin's `ts` mixes the two clocks.

T6b compares the real listing with the real frontier, on one clock. T6a stays entirely on the twin's clock. The code matches the rule on segments (a class-C marker counts as twin only after that seed's round-start for that round), on the T1 "every class-C `Err` line, any segment" half, on the "most recent replay start above it" reading of `meta.t`, and on the last-listing rule of T6b. The log order the rule assumes is the real order: `t_2859.err` lines 1–3 and 114–116. `run_seed_with` prints the probe before the round-start and the twin and its re-runs after it.

Both adversarial logs from the prior verdict now read NOT-EXPLAINED T1.

**It is narrower than X5-R52, but not fully narrow.** Each of these came back `EXPLAINED` from my synthetic logs. Each is what X5-R53 says, and each is a gap in the rule, not a coding error:

1. **The `cover_err`/`x33x` short-cut skips the listing even when a listing exists.** A real round with `x33x=true` (or `cover_err=true`) whose listing shows `D4` at `ts ≤ frontier` is EXPLAINED. The card justifies the short-cut because "a refused recovery prints no listing". But `x33x` is not limited to refused recoveries: 4 of this 10k's 539 `x33x` rounds have non-null `c`. `harness.rs` sets it on the recovery-`Ok` arm too.
2. **Twin-side re-creation.** `D4` at ts 3, then `C4` at ts 5, both at or below `meta.t`, with the twin's directory lacking 4 is EXPLAINED. T6a never looks for a later create, so a twin that lost a committed create reads as a drop. The workload does reuse ids (`setup` re-creates every missing tree, and X5-R36 (e) says so), so the shape is reachable in principle.
3. **Re-divergence.** T4 and T6 are tied to the *first* divergent round. A log that diverges at k=1 after a faulted real round, converges at 2, and diverges again at i=3 after a real round with no media fault is EXPLAINED.

Also by rule: an empty real listing passes T6b, with T4 carrying the condition.

**None of this reaches the data.** On both rows the real round `k−1` has `cover_err=false` and `x33x=false` and took the listing branch, the listing is one record, no later `C<N>` appears in either twin log, and `k = i`. The rule forbids changing X5-R53 without an ESCALATE, and `check-twin` has no use at -F2's commits under X5-R52 (g). So this is a note for P, in follow-up -F2.

## 2. The cap

Correct from recorded fields: `class == "MP"`, `served is True`, `mrb is True`, counted identically at both ends, over the compared seeds. `cmd_compare` slices `ref_seeds` before the engine sees them.

Real-file perturbations:

| Perturbation | Result |
|---|---|
| `--range 0..1000` | `MPA_CLASS base 2 tip 1`, matching my count (457 and 537 at Base, 537 at tip) |
| Tip with 2 extra MP-A rounds (12 > 11) | `(7078,'round',0,'bullet1-mpa-cap')`, `MPA_PASS_FAIL 0`, exit 1 |
| Tip with 1 extra (11 = 11) | 7078 stays exempt, exit 0, as (d) says |

`served`-only, `mrb`-only and MW rounds do not count at the Base. The self-test does not pin those three facts (below).

## 3. MONITOR

`MONITOR=1` on the real run gives the `(-1,"run",None,"bullet3-monitor")` row and exit 1. The (b)4 voiding still applies: all exemptions drop, for 91 rows. Lines and files sum correctly.

**One hole, in `read_monitor` (not changed by this card, and the card forbade changing it).** Its docstring says each run-out must carry an `E4 SEEDS` line, but it only checks the total over all files. Two files where one is empty returns the other's count, exit 0. `--run` and `--run-out` counts are not tied either. So a truncated `.out` hides that run's MONITOR. X5-R53 (e) now rests on this, and -F2's Work repeats five `.out` files. Hence follow-up -F1.

## 4. Self-test and my mutants

The four the verdict named are now caught, and so is every mutant in the card's table. The README's table has 24 rows; the commit subject's "25" is a miscount, and no row is missing.

Synthetic rounds:

| Round | Result |
|---|---|
| Real round replayed the drop at or below its frontier | NOT-EXPLAINED T6b |
| The same with `ts` exactly at frontier | NOT-EXPLAINED T6b |
| `D4` only in the failing twin segment | NOT-EXPLAINED T6a |
| `Err` in another round's twin segment, or after a foreign-seed marker | NOT-EXPLAINED T1 |
| Meta-only finding with `served=true` or `served=null` | `check-mp` NOT-EXPLAINED (not eligible, so (b)5 applies and fails) |
| Meta-only, `served=true`, `pass=false` | bullet1 |
| Meta-only, `served=true`, O-3 text | bullet2-checks |

**Ruling on the 16 survivors: test gaps, not rework.** The card says a surviving mutant is an ESCALATE or a rework "as the gate rules". All 24 of its named mutants die. For each of mine I confirmed the correct behaviour directly at the tip: `every ts`, `≤ meta.t` at the boundary, most-recent `meta.t`, a chunk-kind `D4`, the last listing, the first-k choice, the round filters, the cap's filters, and the per-file sum. Reworking would add cases and change no reading, and the comparator is frozen for -F2 by that card. Follow-up -F1 adds the cases.

The cost of not doing it now: the cap's `served`/`mrb`/Base-class filters are guarded only by this review.

## Follow-ups

- **X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1 (tools; before -F2's multi-file comparison).** Make `read_monitor` require an `E4 SEEDS … MONITOR=` line in every run-out file and exit 2 otherwise, and refuse a comparison whose `--run` and `--run-out` counts differ. Add `self-test` cases that kill J-MON-LASTFILE, J-MON-LASTLINE-ONLY, J-MPA-NO-MRB, J-MPA-NO-SERVED, J-MPA-BASE-NO-CLASS, J-T6A-ANY-T, J-T6A-LT, J-META-T-FIRST, J-T6B-NO-X33X, J-T6B-FIRST-LISTING, J-T6B-ANYKIND, J-T6A-ANYKIND, J-T1-SEG-ANYROUND, J-T4-K-MAX, J-T3-ANYROUND and J-WALK-OTHERSEED. -F2 forbids editing the comparator, so P decides whether this lands first or whether -F2's five-file step gets its own check that each `.out` has the line.
- **X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F2 (P, rule note; only if `check-twin` is re-issued).** Decide whether T6b's `cover_err`/`x33x` short-cut should apply only when no listing exists (or when `c` is null), whether T6a must also refuse a later `C<N>` at or below `meta.t`, and whether T4/T6 must read the divergence that actually runs up to round `i` and not the first one. All three holes are in the rule's text. No code change can close them without an ESCALATE on X5-R53.
