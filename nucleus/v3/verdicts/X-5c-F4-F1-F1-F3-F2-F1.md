Card: X-5c-F4-F1-F1-F3-F2-F1
Class: test
Card-gate: P
Verdict: ESCALATE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-gate-X-5c-F4-F1-F1-F3-F2-F1-1 (Claude Code session 56a7285f-616e-43c3-92a5-9bd8c77f841e)
Date: 2026-11-07
Gate-exit: GATE EXIT=0
Commit: eb9c432881830a2c6f5a6b2b6dd08037082a77ed
Mutants: F3's 14 rows -> as in the worker's step-5 table (M-FREE-HOME-ALLOC now caught), O-FREE-DROP-IMGREFS -> x5_r49_window_freed_unimaged_home_held_by_image_refs, M-FREE-HOME-ALLOC -> x5_r49_freed_home_frees_at_step6 + x5_r49_window_freed_home_frees_at_next_step6 + x5_r49_window_freed_unimaged_home_held_by_image_refs, G-SLOTFREE-NO-IMGREFS -> x5_r49_window_freed_unimaged_home_held_by_image_refs, G-FREE-NO-FREEZE -> x5_r49_window_freed_home_frees_at_next_step6
Followups: X-5c-F4-F1-F1-F3-F2-F1-F1, X-5c-F4-F1-F1-F3-F2-F1-F2

## Verdict in one paragraph

ESCALATE. X5-R51 (e) fired at the 10k: **seven rounds fall outside the
rule as written.** Three are bullet-1 fold regressions: the two twins the
comparator named, plus 7078 r0, which the comparator cannot see. Four are
`MOVED_MP_PENDING` rounds that fail (b)5. **None of the seven is a loss
caused by X5-R49's slot freeing (I-FREE-SAFE).** Each is a known class
that the moved schedule reaches, and each has a named mechanism (below).
The card's own work is sound:
- the fresh gate is green;
- scope is README plus tests only;
- every mutant is caught;
- the worker's 0..1000 reading reproduces.

The rule was fixed in advance, though, and "any change is an ESCALATE".
Only P can widen it. The card can merge on this same commit once P
amends X5-R51 and the comparator, and the gate re-reads the existing
10k files under that amendment. No code change and no re-run of the 10k
are needed.

## Gate run

- Scaffold `bf355fdc2620ce177368f16a01455a07e4463a47`, extracted through
  the `--no-local` shallow clone.
- `fresh-gate: commit eb9c4328…`, `checks passed (4514 files hashed)`,
  last line `GATE EXIT=0`.
- Scope: `1524e284..HEAD` touches only `spikes/e4-recovery/{README.md,
  tests/harness.rs, tests/store.rs, tests/workload.rs}`. No `src/`
  change, so X5-R49's code is untouched.
- The worktree is clean (`git status` empty).
- All of the gate's builds and diagnostics ran in
  `~/.cache/nv3-review/gate-X-5c-F4-F1-F1-F3-F2-F1/` (scratch exports
  `tip`, `tipd`, `based` and `mt`; Base = `cfd9fa69`, X5-R49's pure-refactor
  parent).

## Finding 1: 2599 twin r2 and 2859 twin r1 are moved twin-divergence rounds, not I-FREE-SAFE

Both reproduce singly at the tip, and both pass at the Base. A scratch
`eprintln!` on recovery's `Err` arm and on replay (export only) names
the shape. In both, the faultless twin's recovery returns
**`Corrupt("tree N missing in directory")`**:

- **2859 twin r1.** The twin chain's r0 WAL carries `ts=3 DropTree 4`.
  Checkpoint 1 (t=7) correctly holds `dir_keys=[1,2,3]`. The r1 record at
  `ts=8` writes into tree 4.
- **2599 twin r2.** The twin chain's r1 WAL carries `ts=7 DropTree 3`.
  Checkpoint 2 (t=9) holds `dir_keys=[1,2]`. The r2 chunk at `ts=12`
  writes into tree 3.

**Mechanism: the twin chain and the real chain diverge in DDL.**
1. The real MW round's WAL fault loses the DropTree. At 2859 r0 the real
   recovery replays only `ts=1`, and its frontier is 1.
2. The next round's script is built from the **real** model
   (`build_round(.., &ch.model, ..)`), in which the tree still exists.
3. The twin runs that script on its own chain, where the tree is gone.
   The harness commit path WAL-appends the record first. The apply then
   waits at `h:applydir`, and the uniform crash lands.
4. The twin's recovery replays a record into a tree its image has
   correctly dropped.

Neither round involves a page slot, a freed home or `image_refs`.

**The Base already has six twin failures of exactly this shape** (609,
849, 3449, 5049, 6539 and 7179, all r1; I re-ran every one at the Base
and at the tip: same error, and the same tree for 609). At the tip,
6539 heals, and 2599 and 2859 gain the shape because the crash points
moved. 2859 r1 moved from `uniform@y:10565` to `6280`. 2599's chain
moved at r1, from `14217` to `1634`.

This is a Work 2.5 pairing artefact in the harness. It belongs to the
C-XX "recovery-Err default" twin family (README "Step 6 — the twins"),
which no one had root-caused before. X5-R51 (a) excludes twins by
design, so the rule correctly stops on them.

## Finding 2: 7078 r0 is a bullet-1 hit that the comparator does not test

- **Base:** `uniform@y:5875`, `pass=true`, with an O-1(a) text.
- **Tip:** `uniform@y:10344`, `pass=false`, `O-3(a): served corruption`.

The comparator only tests rounds that were text-free at the Base, so a
round that already carried a text and flipped pass→fail escapes it.

The shape is **MP-A**:
- `mrb=true`. Both meta slots are hit (slot 1 Torn, slot 2 Lost), so
  open rolls back to ckpt 0.
- Ckpt 0's map root, slot 4, was rewritten at ev[174]. That write came
  after ckpt 1's meta was durable (ev[164]–[170]), which is legal reuse
  of a superseded image.

This class is the one X5-R30 keeps open. It is unchanged in size: 11
rounds at the Base, 10 at the tip (457 and 8777 heal; 7078 gains). It is
movement, not I-FREE-SAFE, but bullet 1 is outside X5-R51 by (a).

## Finding 3: the 52 MOVED_MP_PENDING rounds, all run singly under (b)5

I ran each one at the tip with `E4_MPE_DIAG=1 E4_X33_DIAG=1`. Every
digest equals the 10k file's, and `MONITOR=0` in each run.

- **48 rounds pass (b)5.** Every finding is `Some(n)` with `n` in
  `tainted`. The table is in `m/b5.txt`.
- **Four rounds fail it: 1927 r1, 7257 r0, 7658 r0, 8278 r0.** Each has a
  single finding with `slot: None` ("no valid meta", or its G7
  misplaced-meta form), `image_slots {}`, `tainted {}` and `meta_k None`.
  In each, the media draw hits meta slots 1 and/or 2 (Q14). The crash
  moved:
  - 7257: 37 → 6661;
  - 7658: 16760 → 442;
  - 8278: 10795 → 3665;
  - 1927 r1 is a new round (the Base has one round).

  So open finds no valid meta and reports a detected loss. The texts are
  fully explained by meta-slot faults, but (b)5 says verbatim that "a
  `None` slot … means the round is not explained". Under (f) they are
  new failures.

I also ran the 35 MW moved-gained rounds against (b)5's diag half. All
35 have `x33x`, `cover_err` and a non-empty `damaged` list.

## Finding 4: the quota table is a tuning artefact, not a defect

The table matches X-5c-F4's 10k verify row for row, within schedule
noise. The MISSED set is the same: Q4, Q7, Q8, Q11, Q12, Q18 and SIZING.

- **SIZING** is 9628 → 9629. It is the long-standing label for a Work 5.3
  ≥4·F sizing target that BASE never reaches (X5-R17's retune).
- **Q4-I** (69 against 1000) is F3's new row. Its generator share is 0
  until F3-F1, so this value is expected.

`VERIFY OK (--no-quotas)` and the digests reproduce.

## Finding 5: pins and mutants

- I applied four mutants on a scratch export (`mt`), ran
  `cargo test --test store --no-fail-fast` for each, and reverted to a
  byte-identical tree.
- **O-FREE-DROP-IMGREFS** fails pin 4 only. Pins 1–3 pass, as recorded.
- **M-FREE-HOME-ALLOC** (mut.py's edit) fails pins 1, 2 and 4 in
  `store.rs`. This agrees with the worker's 16 across all test files. The
  row is no longer UNCAUGHT: X5-R49 (a) removes `used_cur`'s mask. That
  direction is favourable and is recorded.
- **My two mutants:**
  - G-SLOTFREE-NO-IMGREFS (`slot_free` ignores `image_refs`) is caught by
    pin 4;
  - G-FREE-NO-FREEZE (`free_lpid` without `freeze_first_touch`) is caught
    by pin 2.
- I did not re-run F3's other 13 rows or F2's rows. The worker's table
  (rc 101, named catchers) is consistent with the code.
- **Record note (non-blocking):** step 6 reports `GEV REUSE 4212`, above
  `GEV FREE 2209`. The diagnostic's `freed_homes` set is apparently not
  cleared on allocation, so later ordinary reuses are counted too. The
  claim the card needs (non-zero) stands.

## What P must change (a P amendment the card carries; X5-R49 untouched)

X5-R51 is not relaxed here. These are proposals for P:

1. **(a′) Twins.** A twin entry that is pass at the Base and fail at the
   tip is **moved** only when all of these hold:
   - the twin's recovery `Err` is `Corrupt("tree N missing in
     directory")`;
   - the twin chain's WAL holds `DropTree N` at a ts at or below the
     opened meta's `t`;
   - the paired real round's recovery lost that ts (its frontier is below
     it, or `cover_err`/`x33x`).

   The evidence comes from a single-seed run, and it needs a diag-only
   print of the recovery `Err` and the replay DDL, which the crate does
   not have today.
2. **(b)5′ Meta-only.** An MP round whose only finding is `slot: None`
   "no valid meta…" is explained when the round's `media[]` hits page
   slot 1 or 2 and `meta_k` is `None`.
3. **Bullet 1 for MP-A.** P decides whether a pass→fail MP round whose
   tip text is `O-3(a)` with `mrb=true` at both ends counts as movement
   within MP-A while the class count does not grow (X5-R30 still open).
   Otherwise 7078 r0 stays an ESCALATE.
4. **The comparator.** Add bullet 1 for every `rounds` and `twin` entry
   (pass at the Base → fail at the tip), whatever text the Base had.
   List the MP-A and twin-divergence candidates, and the `None`-slot MP
   pendings, for the gate to check singly.

After the amendment, the gate re-reads the existing `acc.campaign.json`.
The sha256 is `6b686266…739a`, and the `.prov` file records the commit
and a clean tree.

## Follow-ups

- **X-5c-F4-F1-F1-F3-F2-F1-F1 (P, spec/rule).** The X5-R51 amendment
  above and the corrected comparator, carried into the card and the
  README verbatim. Then a P gate re-read of this card's 10k: the seven
  rounds plus 2599/2859's diag evidence. No worker run.
- **X-5c-F4-F1-F1-F3-F2-F1-F2 (harness, after F4's REFRESH).** Twin DDL
  divergence. When the real round's recovery loses a DDL op the twin
  chain kept, the next round's script (built from the real model) is not
  the twin's script "from the same base" (Work 2.5). Re-seed the twin
  chain from the real round's start whenever the two chains' directories
  differ at round start. Pin it with 2859 (r1) and 609 (r1). It moves
  digests, so X5-R20's campaign re-run applies. This root-causes the
  C-XX twin recovery-Err family (8 rounds at the tip's 10k).
