Card: X-5c-F4-F1-F1-F3-F1
Class: experiment
Card-gate: P
Verdict: ESCALATE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: Claude Code session 1fdcaedc-7ef7-4a27-bcab-c0d780e4f11f
Date: 2026-11-09
Gate-exit: GATE EXIT=1
Commit: 09f9353fb86e4959f0c1315e4783de8e9d073ba2
Mutants: G-SLOT-INERT -> x5_r43_free_home_alloc_is_caught, G-REUSE-ANY -> x5_r42_reuse_counts_window_frees_before_freeze, M-RUPF-NOFREEZE -> x5_r42_reuse_counts_window_frees_before_freeze, M-Q4I-WHOLEWINDOW -> x5_r42_post_freeze_reuse_is_not_q4i, G-SEL-NOTARGET -> x5_r41_uniform_round_not_selected, G-LEDGER-NOREMOVE -> x5_r42_harness_checkpoint_image_is_not_q4i, G-SEEDED-FOLD -> x5_r43_oracle_pass_not_fold, M-R50-DRAW -> x5_r37_base_tuning_reproduces_1b0ce54e, M-R50-SHAREDSTREAM -> x5_r50_prior_window_round_is_q4i, M-R50-SHAREDFLAG -> x5_r50_prior_window_round_is_q4i, M-R50-ANYHOOK -> x5_r50_ineligible_rounds_draw_nothing, M-R50-IDX0 -> x5_r50_ineligible_rounds_draw_nothing, M-FREE-HOME-ALLOC -> x5_r43_free_home_alloc_is_caught, M-R41-WRONGWINDOW -> x5_r41_aligned_round_has_q4_and_reuse, M-R41-AFTERCRASHARM -> x5_r41_aligned_round_has_q4_and_reuse, M-Q4I-LASTCKPT -> x5_r42_q4i_keys_on_the_opened_image, GX-RUPF-INVERT -> x5_r42_reuse_counts_window_frees_before_freeze, GX-R50-WAITIDX -> x5_r50_prior_window_round_is_q4i, GX-R50-ORDER -> x5_r50_prior_window_round_is_q4i, GX-R50-ARMIDX -> x5_r50_prior_window_round_is_q4i, GX-Q4I-LASTROW -> x5_r42_q4i_keys_on_the_opened_image, GX-RUPF-NORESET -> UNCAUGHT, GX-R50-NOSELECT -> UNCAUGHT
Followups: X-5c-F4-F1-F1-F3-F1-F1

## Verdict in one paragraph

ESCALATE, for one reason: the fresh gate reads `GATE EXIT=1`, and `merge-card.sh` will not take a MERGE without `GATE EXIT=0`. The single red step is `deny`, on advisory RUSTSEC-2026-0459. It is not this card's doing, and it is equally red on this card's base and on the scaffold, but it is a real red and its fix lies outside this card's Touch only. How to clear it is the owner's choice, set out below. **The owner's rule itself is MET as written on my own numbers**, X5-R50 is honest, and both planted faults are CAUGHT. Nothing about the experiment needs reworking.

## FOR THE OWNER

**One decision is needed: RUSTSEC-2026-0459 (`cargo deny`).** `libsqlite3-sys` 0.35.0, bundling a SQLite older than 3.54.0 (CVE-2026-119375), is reached only through `rusqlite` 0.37 under `e0-btree` and the E1 spike crates. `e4-recovery` does not depend on it. The advisory's stated exposure is a process opening a database and `-wal` that a less trusted party could have written. Its stated fix is `libsqlite3-sys` ≥ 0.39.0.

- `Cargo.lock` and `deny.toml` are unchanged by this card. `cargo deny check advisories` fails the same way on `af5c8d63` and on scaffold `187cb226`.
- The advisory is new since this morning's -F2-F1-F2 gate. **It will turn every card's gate red until it is dealt with**, not just this one.
- The choices I see are a dependency bump in the E0/E1 spikes, or a reasoned `ignore` entry in `deny.toml`. I have not picked one and have not tested a bump.
- After it clears, this card needs a re-run of the fresh gate on a tip carrying the fix, and a new verdict. The measurements below stand unless the fix moves an `e4-recovery` digest.

**For information, no decision needed:** the CPU limit is met, with less margin under load than at idle. See "Reproduction of the rule".

## Gate run

- Scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, extracted through the `--no-local` shallow clone. No `EXTRACT FAILED`, no `FRESH-GATE REFUSED`.
- `fresh-gate: commit 09f9353fb86e4959f0c1315e4783de8e9d073ba2`; `fresh-gate: base 187cb226…`; `fresh-gate: checks passed (4524 files hashed in 1.32 s)`.
- Steps: test 0, clippy 0, fmt 0, **deny 1**, wasm32 0, core-paths-selftest 0, enforcement-tests 0. Literal last line: `GATE EXIT=1`.
- The worker's second red, `enforcement-tests`, did not reproduce here: it reads 0 outside the worker's container. Its report of `deny` is correct.
- Scope: the diff from `af5c8d63` touches nine files, all under `nucleus/v3/spikes/e4-recovery/`, in Touch only. No `unsafe`, no non-test `unwrap`/`expect`/`panic`, no `std::env`, no `HashMap`, no wall clock in the added source.

## 1. Reproduction of the rule (my runs, clean export of the tip)

Scratch builds of `09f9353f`: BASE as committed, S1 with only `Tuning::FINAL` set to (1000, 500), S0 with (1000, 0). Five 200-seed runs over 0..1000, 16 threads. Runs are deterministic: all six passes produced byte-identical JSON per block and arm.

| | BASE | S0 | S1 | S1 per 10k | Bar | Result |
|---|---|---|---|---|---|---|
| rounds | 1,537 | 1,544 | 1,547 | | | |
| Q4 | 0 | 177 | 186 | 1,860 | ≥ 1,500 | met |
| q4i (pre-freeze) | 3 | 135 | 216 | 2,160 | ≥ 1,500 | met |
| imgreuse ≥ 1 (never decides) | 8 | 158 | 258 | 2,580 | | |
| Q5a | 25 | 26 | 25 | 250 | ≥ min(200, 250) = 200 | met |
| Q7 | 19 | 19 | 19 | 190 | ≥ min(200, 190) = 190 | met, exactly at the floor |
| Q18 | 99 | 500 | 527 | 5,270 | ≥ min(1000, 990) = 990 | met |

Every figure matches the worker's. S0 misses only on the image arm (1,350), so building X5-R50 was what the ladder called for. S1's failing rounds are 178 r1 and 537 r0, both class MP, both failing at BASE with the same text, so neither is new.

**CPU, ΣCPU(S1)/ΣCPU(BASE) − 1, user+sys, nothing else running on the host:**

| pass | start condition | order | BASE s | S1 s | term |
|---|---|---|---|---|---|
| 1 | back to back (own load 4→17) | BASE, S1 | 1,043.8 | 1,685.3 | **+61.46%** |
| 2 | back to back | BASE, S1 | 1,122.2 | 1,740.1 | +55.05% |
| 3 | back to back | BASE, S1 | 1,107.7 | 1,704.9 | +53.92% |
| 4 | back to back | S1, BASE | 1,120.3 | 1,749.2 | +56.14% |
| 5 | back to back | S1, BASE | 1,128.1 | 1,744.5 | +54.64% |
| 6 | back to back | BASE, S1 | 1,105.4 | 1,751.9 | +58.48% |
| C1 | every run from 1-min load < 2 | BASE, S1 | 908.5 | 1,329.4 | +46.33% |
| C2 | every run from 1-min load < 2 | S1, BASE | 909.1 | 1,371.1 | +50.82% |

- **The rule as written is met.** Pass 1 reads inside the +55..+65 band, so the term is the median of the first three passes: **+55.05%** ≤ +60%.
- Six back-to-back passes: median +55.60%, mean +56.61%, sd 2.86, range +53.92 to +61.46.
- **How robust.** At genuinely low load the term is +46 to +51%, well inside the limit. Back to back on a saturated host it sits 4 to 6 points under the limit and one pass in six went over. Pass 1's excess is almost all block 0 (+83.5%, its BASE run started cold while the S1 run did not). The block figures in the other passes run +41 to +71%. So +60% is met by the fixed protocol and not by luck in the median, but the margin under load is thin, and a single back-to-back pass can exceed it.
- The worker's three passes (+55.11, +59.26, +48.94) were taken at load 6.6 to 18.9. They are consistent with mine.

**Seeded faults at S1, my runs:** `seeded-faults --seeds 0..200` exit 0: Joint CAUGHT (29 new-failing, 9 q4sm, 8 q4i_raw), Slot CAUGHT (41, 10, 4). `200..400` exit 0: Joint 31, Slot 38, both CAUGHT. Identical to the worker's.

**10k at the tip.** Files copied to `~/.cache/nv3-review/runlogs/X-5c-F4-F1-F1-F3-F1.acc.*`. JSON sha256 `158ce549c02d8e1367a7b44b43026e2f32450610e0338e2a7edcc353c8d6a69b` matches the `.prov`. Exit 0, `MONITOR=0`, FOLD 19/19/0, `DEFECT_SEEDS=0`, verify OK. My own run of the comparator against the baseline `41c307a4…` with `--no-twin-exemption --max-twin-failing 0`: exit 0, `DIGESTS_MOVED 0 FAILING_ROUNDS 19 MONITOR 0`, `TWIN_FAILING 0 [] BASE 0`, `NEW_FAILING 0 [] BAD_SEEDS 0 []`. The worker's claim was at `25ae82bd`; the runner's run is at the tip, and that is what I read.

## 2. The q4i count

**Honest.** I recounted it three ways.

- From the JSON: q4i 216, q4i_raw 216, every q4i round has `q4ir` and `fired`.
- From raw events. A scratch S1 build logged every reuse at `alloc_lpid`'s site with the freeze flag read there, each `ckpt:begin` reset, each `After5` row and each round's opened meta id. The JSON it wrote is byte-identical to the uninstrumented S1. Sixteen single-threaded processes over 0..1000. A separate script rebuilt the window counts from the events and applied the ledger rule itself: 7,289 rows, 0 mismatches against the events, 1,547 rounds, **0 rounds differ from `q4ir`**, independent q4i **216**. Of 3,619 reuses, 3,547 are pre-freeze.
- By crash position: pre-step-5 targeted 87, post-step-5 targeted 127, uniform 2 (BASE also has 2 uniform). Block A therefore adds only where it should: uniform rounds gain nothing, and the 87 match the Goal's estimate of about 90.

Pre-freeze is read correctly: `image_frozen` is set at step 2's `ckpt:freeze`, and is cleared only at step 5, on recovery, and on step 2's failure path. No leak from block A: its reuses land in window `idx−1`'s own row, and q4i requires the opened image to be that row.

The worker's README cites the 10k's unmoved digests as evidence that `fold_ckpt_windows` is behaviour-identical. That does not show it, because q4i is not a digest input. The evidence that does is the extraction's plain text and my recount above.

## 3. Was the rule followed as written?

Yes.

- Step 0's README text of X5-R50 (`75c5e540`) equals the card's exactly, modulo markup whitespace, and is unchanged at the S0 row (`b6ab9750`) and at HEAD. The S0 row was committed before X5-R50 was built (`4f3fa251`).
- S0 → S1 is the fixed ladder. S1's first pass read +55.11%, in the band, so the two extra passes were owed and run. S1 met the rule, so no S2 ran, as written. I found no choice made after seeing a result that the card did not fix in advance.
- Held-out and MP seeds: the logged `--seeds` values are `0..200`, `200..400`, `0..10000` and block variables over 0..1000. `10000..20000` appears only in card and README text. No command names `MP_PRESSURE`, `--set`, or a 30000-range seed. My own runs used 0..1000 and 0..400.

## 4. X5-R50 itself

- **Targets exactly the rounds it claims**, by reading `build_round`: X5-R41-selected, one of the seven pre-step-5 hooks, `target_ckpt ≥ 1`, one `S_R41P` draw. Block A's content comes only from `S_R41PC`. Block shape, placement and arms match the ruling. The hold and the wait are per index with the stated exit sets.
- **Inert at BASE and FINAL.** Both consts carry `r41p_share: 0`; `r41p_pick` returns `None` at 0 without constructing the stream. The 10k moved no digest, and M-R50-DRAW is caught by X5-R37's literal.
- **Does it inflate q4i without exercising recovery? No.** A q4i round's opened image holds a window whose reuse hit an LPID the image still maps, and recovery ran over it to a live end. The Joint fault, which drops the image entry at exactly that reuse, is caught at S1 with q4i_raw rounds in both seed ranges. At BASE the fault reads NOT CAUGHT (amendment). The block is contrived in the same sense X5-R41 already is, which is X5-R41's own note and within the ruling.

## 5. Mutants and pins

All 16 mutants run from the card's table, F3's re-sited rows and `M-FREE-HOME-ALLOC` were applied to their own export and target of `09f9353f` with the full suite, `--no-fail-fast`. Each is rc 101 with its named pin among the failures.

| Mutant | Result |
|---|---|
| G-SLOT-INERT, G-REUSE-ANY, M-RUPF-NOFREEZE, M-Q4I-WHOLEWINDOW, G-SEL-NOTARGET, G-LEDGER-NOREMOVE, G-SEEDED-FOLD | caught by the named pin |
| M-R50-DRAW, -SHAREDSTREAM, -SHAREDFLAG, -ANYHOOK, -IDX0 | caught by the named pin |
| M-FREE-HOME-ALLOC | caught (`x5_r43_free_home_alloc_is_caught`, among others) |
| M-R41-WRONGWINDOW, M-R41-AFTERCRASHARM, M-Q4I-LASTCKPT (the re-sited three) | caught by the named pin |
| Own: GX-RUPF-INVERT (freeze test inverted), GX-R50-WAITIDX (block A waits on `idx`), GX-R50-ORDER (A after B), GX-R50-ARMIDX (A armed at `idx`), GX-Q4I-LASTROW (F3's original last-row shape in the fold) | caught |
| Own: **GX-RUPF-NORESET** (`ckpt_reuse_pf` never reset) | **UNCAUGHT** |
| Own: **GX-R50-NOSELECT** (`r41p` drops `r41 &&`) | **UNCAUGHT** |

- **Both survivors are pin gaps, not code defects.** The shipped code does reset and does require `r41`: the event recount shows no window mismatch over 7,289 rows, and uniform rounds gain no q4i. They need pins all the same (see the follow-up): NORESET would inflate q4i across windows, and NOSELECT would put block A in unselected rounds.
- **The re-sites are sound.** WRONGWINDOW and AFTERCRASHARM keep their behaviour in the per-slot shape. LASTCKPT moved into the fold as the amendment expects, and my F3-shaped variant is caught too.
- **Step 3b's fallback** is the card's named fallback. I confirmed its premise for S1: over 0..1000 the harness-id removal happened once and changed no round's `q4i_raw`. The worker's claim also covers BASE, which I did not re-measure.
- I did not re-run F3's other rows. Worker's table lists them caught, and `all.txt` shows F3's gate caught them at its tip.

## 6. Before F4's final run

**The F4 REFRESH must carry:**

1. The handed-off setting, in `Tuning::FINAL`: `r41_share` 1000, `r41p_share` 500. BASE stays as it is. No other value of either without a new ruling.
2. The baseline. This card's 10k, `runlogs/X-5c-F4-F1-F1-F3-F1.acc.*`, JSON sha `158ce549…a69b`, moved no digest against `41c307a4…1cd2`. The BASE digest baseline of record stays `41c307a4…1cd2`, with `158ce549…` as the same-digest file that carries `imgrupf`. F4's REFRESH should name which it compares against and why.
3. The comparator: `tools/x5r52_compare.py` with `--no-twin-exemption --max-twin-failing 0`, as here, and `DIGESTS_MOVED 0` required for any commit that should not move BASE.
4. The image arm's bars stated separately. The frozen `Q4I_REQUIRED` row is 1,000; the owner's bar for this choice was 1,500. `imgreuse` never decides.
5. `seeded-faults --seeds 0..200` on F4's own FINAL, Joint and Slot both CAUGHT. At S1 they are; F4's retune can move that.
6. CPU. S1 alone costs +46 to +61% on BASE depending on load. REFRESH 9's run-time limits were set before it, so measure on F4's own FINAL and do not assume.

**Still required before F4 runs:**

- This card: the deny decision above, a fresh `GATE EXIT=0`, a re-gate, then fast-forward `card/v3-X-5c-F4`.
- `X-5c-F4-F1-F1-CXX-F1-F1`: stacks after this card (same files); QUEUE's hard order has it before relaunch.
- `X-5c-F4-F1-F1-F3-F2-F1-F2-F1` (test gaps) and `X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1-F1` (comparator): both listed "before F4's final run".
- `X-5c-F4-F1-F1-F3-F1-F1` (below): tests only, moves no digest, should land before F4's final run.
- F4's `Depends` lists none of -F3-F2-F1-F2, this card, or CXX-F1-F1 yet; the P edit is due with the REFRESH.

## Follow-ups

- **X-5c-F4-F1-F1-F3-F1-F1 (tests only; moves no digest).** Kill the two surviving mutants. Add a directed case to `x5_r42_reuse_counts_window_frees_before_freeze` (or a sibling pin) that runs two checkpoint windows and asserts the second window's `ckpt_reuse_pf` starts at 0, which kills GX-RUPF-NORESET (`ckpt.rs`: drop `st.ckpt_reuse_pf = 0`). Add to `x5_r50_ineligible_rounds_draw_nothing` a literal round where the X5-R41 selection is false, such as a uniform-crash seed at (1000, 1000) with an eligible hook and `target_ckpt ≥ 1`, and assert no block A and no draw, which kills GX-R50-NOSELECT (`let r41p = r41 && …` losing `r41`).
