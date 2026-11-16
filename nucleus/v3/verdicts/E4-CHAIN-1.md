Card: E4-CHAIN-1
Class: experiment
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code (3da7f1e8-e26e-40e7-9519-7cc01d042edd, 2026-11-16)
Date: 2026-11-16
Gate-exit: GATE EXIT=0
Commit: 6d5df7265dc09e66b6048626c831cfd93e1f96fb
Mutants: n/a
Followups: E4-CHAIN-1-F1

## Verdict in one paragraph

MERGE-WITH-FOLLOWUPS. This verdict covers `6d5df726` itself, and through it
the whole stacked range as it was built and run.
- The chain tip gates green: a fresh, hash-verified checkout against today's
  scaffold `d7d41677` gives `GATE EXIT=0`.
- Coverage, re-derived from the refs: each of the 222 commits after X-5c-F4-F1's
  verdict falls in one card's verdict range. Every count the card gives
  reproduces.
- The four ESCALATE-only members were each answered, two by the owner and two
  by P, and continuation cards then passed their gates. The two that have no
  owner line were P matters and do not need the owner (§2.2).
- The three misplaced re-verdicts are their cards' records (§2.3).
- E4-SILENT-1's Class mismatch is a label only (§2.4).
- Of the four follow-ups QUEUE lacked this morning, one is open work. The other
  three were already folded by later chain cards (§2.4).

One follow-up: the chain-merge ruling's standing instruction against stacking
is written nowhere binding (E4-CHAIN-1-F1).

The card did not name one gap, recorded in §2.1: two chain commits are card
X-5c-F4-F1-F1-O1C's work, cherry-picked, and O1C's own P verdict stays off the
chain. The owner point the card leaves to the orchestrator is under "For the
owner". This verdict does not answer it.

## The gate's run (the run of record, once)

- **Extract.** The scaffold's `fresh-gate.sh` and `git-env.sh` came through the
  shallow `--no-local` clone, exactly as GATE-PROMPT step 1 gives it, with no
  `EXTRACT FAILED` line.
  - Scaffold id: `d7d4167777fe24e38b55bb63983b3a9033e72822`.
  - sha256: `fresh-gate.sh` `40a5b232…4899`, `git-env.sh` `8a784782…3977`.
  - `$fg` was under `~/.cache/nv3-review/gate-E4-CHAIN-1/` and was removed
    afterwards.
- **Command.** `cd ~/Workspace/neutron-v3 && PATH=$HOME/.cargo/bin:$PATH
  CARGO_TARGET_DIR=$HOME/.cache/nv3-target/E4-CHAIN-1 bash "$fg/fresh-gate.sh"
  6d5df7265dc09e66b6048626c831cfd93e1f96fb`.
  - The target dir is the one the dispatch named. The card says `gate-E4-CHAIN-1`.
    Either is a new, empty directory, so the name has no bearing on the result.
  - Run once, 11:45:11 to 11:56:28 (-07:00), cold target dir. Host load was
    about 27 from another session's cargo.
  - stdout and stderr were captured apart. stderr is empty. The exit status is 0.
- **Lines, verbatim:**
  ```
  fresh-gate: commit 6d5df7265dc09e66b6048626c831cfd93e1f96fb
  fresh-gate: base d7d4167777fe24e38b55bb63983b3a9033e72822
  fresh-gate: checks passed (4543 files hashed in 2.90 s)
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
  - The literal last line is `GATE EXIT=0`.
  - Seven steps and no `api-surface`: this is the chain's base-era `gate.sh`, as
    the card says.
  - `deny` printed `advisories ok, bans ok, licenses ok, sources ok`.
  - `enforcement-tests` printed 236 `ok` lines and `test-enforcement: all
    passed`.
  - The eol line is the script's one narrow exception (its comment: "the two
    `text eol=crlf` .ps1 files here"). The card's probe quote omits it. Member
    gates on this chain record the same line (for example `1524e284`).
- **The card's checkout of record.** No implementation commit exists. The worktree
  `~/.cache/nv3-cards/E4-CHAIN-1` was clean, at `6d5df726`, on
  `card/v3-E4-CHAIN-1`.
- **Not claimed.** The scaffold's eight-step gate on the merged tree; the final
  pass's `promote-pass.sh` runs it. Its added step, `check-api-surface.sh`,
  scans only `crates/*` and the root manifest, and the chain touches neither.

## Scope

- **The branch.** `card/v3-E4-CHAIN-1` is `6d5df726` plus this commit, which
  changes only `nucleus/v3/verdicts/E4-CHAIN-1.md`.
- **What `nucleus/v3-scaffold...6d5df726` changes:** 72 paths.
  - `spikes/e4-recovery/` (35);
  - `spikes/e0-btree/Cargo.toml`;
  - 36 files under `verdicts/`.

  Nothing under `crates/`, `xtask/`, `docs/`, `scripts/` or `tools/`. Not
  `CORE_PATHS`, `Cargo.toml` or `api-allowlist.txt`. Nothing outside
  `nucleus/v3/`.
- **A sweep of the chain's code, not a re-review.** At the tip, the only `unsafe`
  in `spikes/e4-recovery/src` is a doc comment (`store/page.rs:617`), the same
  as at base.
  - The chain adds three `.expect(` lines to `src/`, all in `campaign.rs`'s
    `#[cfg(test)] mod tests` (lines 3102–3104).
  - The workspace denies `unwrap_used`, `expect_used` and `panic`, and clippy
    passed.
  - Each member's code was gated by its own verdict (§2.1). This card does not
    re-judge E4.
- **Mutants: n/a.** This card changes no code, and the card names none. The
  gate's adversarial work is the re-derivation below, from the refs, plus a dry
  run of the merge in a throwaway clone (§2.6).

## 1. The card's facts, re-checked today (all reproduce)

- **Scaffold and chain.**
  - Scaffold `d7d41677`, merge base `ee8298c3`.
  - Chain head `card/v3-X-5c-F4-F7` is `6d5df726`, and so is origin's
    (`git ls-remote`).
  - 364 chain commits, all first-parent, 0 merges; 360 scaffold commits the
    chain lacks.
  - `git merge-tree --write-tree` of the scaffold and `6d5df726` is clean
    (tree `edd45cec`).
  - No replace refs and no grafts.
- **`scripts/` since `187cb226`.** It changes only `check-api-surface.sh` (new)
  and `gate.sh` (+1, the `api-surface` step). `CORE_PATHS` gained
  `api-allowlist.txt` and `xtask/`.
  - Byte-unchanged: `merge-card.sh`, `enforce-lib.sh`, `audit-merges.sh`,
    `promote-pass.sh`, `fresh-gate.sh`, `test-enforcement.sh`,
    `core-paths-lib.sh`, `git-env.sh`, `queue-check.sh`.
- **The one path both sides change.** `spikes/e0-btree/Cargo.toml` goes from blob
  `82217595` to `ee4a4423` on both sides.
  - On the chain, only DEP-RUSQLITE-1's `59092389` touches it.
  - All 36 chain verdict files are absent at the merge base and on the scaffold.
  - `Cargo.lock` is untracked on both sides (gitignored).
- **nucleus-env's copy.** `6d5df726^` is `48ac9920`.
  `git diff 48ac9920 6d5df726 -- nucleus/v3/spikes/e4-recovery` is empty. The
  tip commit adds only `verdicts/E4-judgement.md` and `verdicts/X-5c-F4-F7.md`.
  The scaffold's `crates/nucleus-env` provenance lines name
  `48ac99203f97d9dd5a53b1370ed141a294c4df0c`.

## 2.1 Coverage

**Method.** The chain was listed `ee8298c3..6d5df726`, oldest first, with
position (pos) = 1-based index. For every commit, its covering verdict is the
nearest verdict commit at or after it, and the verdict file that commit adds or
changes.
- Every commit that touches `verdicts/` touches only verdict files.
- Each verdict file touched twice was touched only by its own card's commits:
  - five REWORK-then-MERGE pairs: X-5c-F3-F3a, X-5c-F4-F1, -CXX, X-5c-PINS-1 and
    E4-CLASS-1;
  - the three re-verdicts.
- No card's commit changed another card's verdict file.
- Each range's work commits carry that card's ID in their subjects. One exception
  is pos 175–176 (below).

**The boundary.** `11aea6a7` is pos 142, X-5c-F4-F1's MERGE-WITH-FOLLOWUPS.
- `ee8298c3..11aea6a7` is 142 commits, covered by the nine verdicts that merge
  card by card first.
- In a throwaway clone, the scaffold's `merge-card.sh` merged those nine, as
  written, onto `d7d41677`: X-5c-F1, -F2, -F3, -F3-F1, -F3-F3a, -F3-F3b,
  -F3-F3a-F2, -F3-F3a-F2-F1 and X-5c-F4-F1. It used today's cards and QUEUE.
- The pass's merge base with `6d5df726` is then `11aea6a720b028e45173c40d9930f402c444df16`.
  222 chain commits remain outside it.

**`11aea6a7..6d5df726`, 222 commits, ranges #1–#25 as the ruling's §1 has them.**
"Commit = last work?" asks whether the verdict's `Commit:` names the commit just
before that verdict, the range's last work commit.

| # | Pos | Card | Verdict commit(s) | Commit = last work? |
|---|---|---|---|---|
| 1 | 143–149 | X-5c-F4-F1-F1 | `a4c1f869` MWF | yes, `e3460d24` |
| 2 | 150–155 | X-5c-F4-F1-F1-F2 | `f8269110` MWF | yes, `5b038ace` |
| 3 | 156–174 | X-5c-F4-F1-F1-CXX | `10567da6` REWORK, then `e3d1bfc3` MWF | yes, `f703c08d`, then `56758f38` |
| 4 | 175–182 | X-5c-F4-F1-F1-O1C-F1 | `86e17a34` MWF | yes, `2ec189e6` |
| 5 | 183–190 | X-5c-F4-F1-F1-CXX-F1 | `5db63057` MWF | yes, `17981efa` |
| 6 | 191–199 | X-5c-F4-F1-F1-F3 | `1180d761` ESCALATE | yes, `d1d9faae` |
| 7 | 200–205 | X-5c-F4-F1-F1-F3-F2 | `1524e284` ESCALATE (`GATE EXIT=1`) | yes, `b5c9a840` |
| 8 | 206–213 | X-5c-F4-F1-F1-F3-F2-F1 | `8992f77f` ESCALATE | yes, `eb9c4328` |
| 9 | 214–219, 222 | X-5c-F4-F1-F1-F3-F2-F1-F1 | `3cef0055` ESCALATE (`GATE EXIT=1`); re-verdict `26bcee4b` MWF | `3cef0055`: yes, `ad4fa999`. `26bcee4b`: **no**, it names `ad4fa999`; its parent is `60f919d7` (§2.3) |
| 10 | 220–221 | X-5c-F4-F1-F1-F2-F2 | `60f919d7` MERGE (S card, S gate) | yes, `03eb5de1` |
| 11 | 223–228 | X-5c-F4-F1-F1-F3-F2-F1-F1-F1 | `991ee5fe` MWF | yes, `bc18b545` |
| 12 | 229–234 | X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1 | `db1e5162` MWF | yes, `6cbbdac0` |
| 13 | 235–245 | X-5c-F4-F1-F1-F3-F2-F1-F2 | `af5c8d63` MWF | yes, `0f091aa4` |
| 14 | 246–261, 266 | X-5c-F4-F1-F1-F3-F1 | `2b4c82b4` ESCALATE (`GATE EXIT=1`); re-verdict `2e8d1b13` MWF | `2b4c82b4`: yes, `09f9353f`. `2e8d1b13`: **no**, it names `09f9353f`; its parent is `b69e0781` (§2.3) |
| 15 | 262–265 | DEP-RUSQLITE-1 | `b69e0781` MERGE | yes, `648a0e5f` |
| 16 | 267–282 | X-5c-PINS-1 | `74f411f0` REWORK, then `0d0f3da4` MERGE | yes, `324a519c`, then `4afeca0e` |
| 17 | 283–288 | X-5c-F4-F2 | `9f96f31e` MWF | yes, `c9c6e8b9` |
| 18 | 289–292 | E4-SILENT-1 | `8d679ccd` MWF | yes, `f9d7ed56` |
| 19 | 293–316 | X-5c-F4-F3 | `2c041aae` ESCALATE | yes, `14c78c42` |
| 20 | 317–323, 324 | X-5c-F4-F4 | `04a269b4` MWF; second verdict `18d2530f` MWF | `04a269b4`: yes, `6af3a6c5`. `18d2530f`: **no**, it names `6af3a6c5`; its parent is `04a269b4` (§2.3) |
| 21 | 325–337 | X-5c-F4-F5 | `ec9f84a6` MWF | yes, `7a1eb285` |
| 22 | 338–347 | X-5c-PINS-2 | `fc275882` MWF | yes, `a95e50f3` |
| 23 | 348–357 | E4-CLASS-1 | `f1dbeb0d` REWORK, then `cb2aca0d` MERGE | yes, `93f21097`, then `ae03fb8e` |
| 24 | 358–359 | X-5c-F4-F6 | `754c6536` MERGE; the same commit adds `X-5c-F4.final.md` (P, ESCALATE, `Commit: cb2aca0d…` = T) | yes, `daee4f53` |
| 25 | 360–364 | X-5c-F4-F7 | `6d5df726` MWF; the same commit adds `E4-judgement.md` (P, no keys) | yes, `48ac9920` |

The counts are 7, 6, 19, 8, 8, 9, 6, 8, 6+1, 2, 6, 6, 11, 16+1, 4, 16, 6, 4, 24,
7+1, 13, 10, 10, 2 and 5, which sum to 222. Every verdict in the table is
`Gate-tier: P` on a `Gate: P` card, except #10, which is S on an S card. Each
card's `Class:` word (merge-card's reading) equals its verdict's, except #18
(§2.4).

**A gap the card does not name: pos 175–176.**
- `43e61833` and `b3e10e53` are card X-5c-F4-F1-F1-O1C's work, not O1C-F1's.
  They carry the same stable patch-ids as `c13299c0` and `82062a02` on
  `card/v3-X-5c-F4-F1-F1-O1C`. That branch is `a4cabc44`, off the chain, and
  forks at `f8269110`.
- O1C's own P verdict (`a4cabc44`, MERGE-WITH-FOLLOWUPS, `Commit: 82062a02…`,
  `GATE EXIT=0`) judged those same patches. It is not in this range, and this
  merge does not carry `verdicts/X-5c-F4-F1-F1-O1C.md`.
- On the chain, the two commits are covered by O1C-F1's `86e17a34`. That card
  Depends on O1C, and its gate's scope line reads: "Scope: `e3d1bfc3..2ec189e6`
  … The ckpt.rs lines are the cherry-picked O1C diagnostic."
- QUEUE's O1C row records "Cherry-picked into O1C-F1 (b3e10e53, 43e61833)".
- **Ruling:** covered, by O1C's P verdict on identical patches, by O1C-F1's range
  and by this verdict. O1C's record of judgement stays at `a4cabc44`, which no
  remote holds (§2.7).

**What covering `6d5df726` adds over the member verdicts.** Each member was
gated at its own tip, on a base that was then an unmerged card, against an older
scaffold (`187cb226`, `504f581c`, …). This verdict adds four things.
- **One fresh gate on the exact stacked tree.** `verify_objects` re-hashed every
  chain commit and tree from today's scaffold base, and the tip's own gate ran
  green in the runner's container.
- **Proof that the stack is exactly the sum of its members.** No commit lies
  outside a member's range. No verdict commit changes anything but verdict
  files. No card changed another card's verdict file. No path outside `spikes/`
  and `verdicts/` changes.
- **Rulings no member verdict could make:** the four ESCALATE members, the three
  misplaced re-verdicts, the two P records, and E4-SILENT-1's Class.
- **The clean merge and the exact add list** against today's scaffold (§2.6).

It adds no re-review of any member's code or measurements, no E4 re-run and no
E4 re-judgement (`E4-judgement.md` judged E4).

## 2.2 The four ESCALATE-only members

These four keep their ESCALATE files, and this verdict is not a re-verdict of
any of them. Their commits reach the scaffold on this verdict.

**1. X-5c-F4-F1-F1-F3 (`1180d761`, `GATE EXIT=0`).**
- **Escalated.** The owner's decision rule (Q4-I and the image arm) was unmet as
  written at all three shares, and the seeded Slot fault was not caught: the
  card's mutant was inert. Options 1, 2 and 3 went to the owner.
- **Answered by** QUEUE "# OWNER DECISION 2026-11-07 (F3 ESCALATE 1180d761)":
  "option 1+2".
- **Discharged by** two continuation lines:
  - -F3-F1, re-verdict `2e8d1b13` MWF: "The owner's rule itself is MET as written
    on the first gate's own numbers … both planted faults are CAUGHT";
  - the store fix (option 2, X5-R49): -F3-F2, then -F3-F2-F1, then -F3-F2-F1-F1
    (`26bcee4b` MWF), -F3-F2-F1-F2 (`af5c8d63` MWF), -F3-F2-F1-F1-F1 (`991ee5fe`
    MWF) and -F3-F2-F1-F1-F1-F1 (`db1e5162` MWF).
- **Ruling:** discharged by the owner's decision.

**2. X-5c-F4-F1-F1-F3-F2 (`1524e284`, `GATE EXIT=1`).**
- **Escalated.** The card's own step-4 movement STOP fired: 7 rounds gained a
  detected-loss text, 5 lost theirs, and 243/1000 digests moved.
  - The gate ruled it "an artefact of schedule movement". X5-R49 is correct as
    written, and I-FREE-SAFE holds (M-NOINPLACE 0 over 1,000 tip seeds).
  - `GATE EXIT=1` is the stale literals the worker declared at its STOP.
  - It asked P, not the owner: "only P can say how the card continues".
- **Answered by** P's ruling X5-R51 (task `nv3-ruling-X-5c-F4-F1-F1-F3-F2-1`).
  It is carried verbatim in the P-written card -F3-F2-F1 and the spike README,
  "fixed before this card's code and before any literal", and scoped "a P rule on
  this card's comparisons only; X5-R49 unchanged".
  - No owner line names `1524e284`. The card itself exists under the owner's
    2026-11-07 option 2 ("FIRST (before -F1)").
- **Discharged by** -F3-F2-F1, which re-derived the literals at `54b7c105` and
  gated green from `eb9c4328` on. Its commits merge through -F3-F2-F1-F1, whose
  re-verdict is `26bcee4b` MWF. That card's README record says "F2-F1's commits
  merge through this card". Its follow-up -F3-F2-F2 was folded (§2.4).
- **Ruling: P's discharge suffices, and this point does not go to the owner.**
  1. What fired was a rule P wrote into its own card. It was not an owner rule,
     a spec or an invariant, and the gate found I-FREE-SAFE held.
  2. GATE-PROMPT keeps ESCALATE for a spec conflict, an invariant break or a
     serialization anomaly. PLAN §7.1 gives ESCALATE gates and experiment
     judgements to P.
  3. P's answer left `pass`, the texts, the fold, `verify`, every quota and
     X5-R1–R50 unchanged, and confined itself to named comparisons.
  4. No owner rule was touched. The owner's 2026-11-07 conditions were "keep the
     rule's numbers … no post-hoc swap to imgreuse and no lowered bar". E4 was
     judged later on the class table frozen before launch, under the owner's
     rulings of 2026-11-10, -11 and -14, not on X5-R51.

**3. X-5c-F4-F1-F1-F3-F2-F1 (`8992f77f`, `GATE EXIT=0`).**
- **Escalated.** X5-R51 (e) fired at the 10k: seven rounds fell outside the rule
  as written.
  - Twin 2599 r2 and twin 2859 r1;
  - 7078 r0, which the comparator could not see;
  - four `MOVED_MP_PENDING` rounds with meta-only findings.

  "None of the seven is a loss caused by X5-R49's slot freeing", and the card's
  own work is sound. "Only P can widen it."
- **Answered by** P's amendment X5-R52, carried by -F3-F2-F1-F1, then X5-R53 in
  -F3-F2-F1-F1-F1. Both are in P-written cards and carried verbatim.
  - X5-R52 says of itself "a P rule, post-data", "It is not an oracle change" and
    "It is not an E4 judgement rule". It names the seven rounds as seen before it
    was written, and draws each exemption "around a class that already exists in
    the reference file".
  - No owner line names `8992f77f`.
- **Discharged by** four continuations:
  - -F3-F2-F1-F1: its ESCALATE `3cef0055` was on the gate run alone, and the
    re-verdict `26bcee4b` MWF reads `GATE EXIT=0` on the stacked tip;
  - -F3-F2-F1-F2 (`af5c8d63` MWF), the twin re-seed that root-causes the twin
    family;
  - -F3-F2-F1-F1-F1 (`991ee5fe` MWF);
  - -F3-F2-F1-F1-F1-F1 (`db1e5162` MWF).
- **Ruling: P's discharge suffices, and this point does not go to the owner**,
  for the same four reasons as member 2. Two facts stay on the record.
  - X5-R52 and X5-R53 are post-data amendments, labelled as such where they
    stand.
  - X5-R52 (c)(iii) applied the already-binding X5-R30 (c) rule 4 to MP-A
    movement, before the owner's own MP-A ruling of 2026-11-10. E4's pass rests
    on that ruling and the frozen table, not on X5-R52.

**4. X-5c-F4-F3 (`2c041aae`, `GATE EXIT=0`).**
- **Escalated.** Four owner rows were unmet at the kept setting: Q7 170 < 200,
  SZC 620 < 1,000, SZI 150 < 300, SZC-MP 14/2,000 < 100. No clean CPU figure
  existed.
- **Answered by** QUEUE "# OWNER 2026-11-10 (F4-F3 ESCALATE 2c041aae)": rung 7.3
  with a ~50-min box; the joint-arm pins (1069, 3669) first; fix SZC-MP's cause
  in E4; a targeted Q7 change.
- **Discharged by** two continuations:
  - X-5c-F4-F4 (`18d2530f` MWF): rung 7.3, the two pins, and it folded
    -F3-F1 and -F3-F2;
  - X-5c-F4-F5 (`ec9f84a6` MWF): "Every exit row holds there together", with
    Q7 442, Q4 1,713, Q4-I 1,921, SZC 2,421 and SZI 777.

  The final run then met every row (`X-5c-F4.final.md`).
- **Ruling:** discharged by the owner's decision.

## 2.3 The three misplaced re-verdicts, accepted as the record

For each one below, the file at `6d5df726` is the re-verdict's own blob, and
that file's last change in the chain is the re-verdict commit.

- **`26bcee4b`** (X-5c-F4-F1-F1-F3-F2-F1-F1, MWF, `Commit: ad4fa999…`; blob
  `ffbaadf5`).
  - **Parent:** `60f919d7`, X-5c-F4-F1-F1-F2-F2's MERGE.
  - **Supersedes `3cef0055`.** That ESCALATE was on the gate run alone:
    `ack_prefix_two_batches_in_flight` has a 30 s deadline and needs about 104 s
    in debug. So it fails at `ad4fa999`, and at `eb9c4328`, whose `src/` and
    `tests/` are byte-identical.
  - **Why it sits there.** The fix belonged to another card, X-5c-F4-F1-F1-F2-F2
    (`03eb5de1`), stacked on this one. Only on that stacked tip could a fresh
    gate read `GATE EXIT=0`. `Commit:` stayed the card's own last commit, and the
    content review stands from the first gate.
- **`2e8d1b13`** (X-5c-F4-F1-F1-F3-F1, MWF, `Commit: 09f9353f…`; blob
  `c163ddb0`).
  - **Parent:** `b69e0781`, DEP-RUSQLITE-1's MERGE.
  - **Supersedes `2b4c82b4`**, an ESCALATE whose only red step was `deny`
    (RUSTSEC-2026-0459).
  - **Why it sits there.** The owner chose the upgrade (2026-11-09), and
    DEP-RUSQLITE-1 made it on top of this card. Once the advisory existed, no
    gate before pos 265 could print `GATE EXIT=0` (ruling §1, Summary). So the
    re-gate ran on `648a0e5f`. In its own words: "This re-verdict changes the
    outcome only."
- **`18d2530f`** (X-5c-F4-F4, MWF, `Commit: 6af3a6c5…`; blob `c495611a`).
  - **Parent:** `04a269b4`, its own first verdict.
  - **Supersedes `04a269b4`** by one line: the `src/workload.rs` stat "+22/−9"
    becomes "+20/−11". The Verdict and Commit are the same.
  - **Re-counted.** `git diff --numstat 2c041aae 6af3a6c5` gives 20 and 11, so
    the second write is the right one.
  - **Why it sits there.** It is a record correction, committed on top instead of
    in place.

**Ruling:** each file at `6d5df726` is that card's record. The earlier verdict
commits `3cef0055`, `2b4c82b4` and `04a269b4` are superseded history, carried as
commits only.

## 2.4 E4-SILENT-1's Class; the carried follow-ups

**The Class.**
- **What differs.** The card reads `Class: core/test`, and merge-card's reader
  (`s/^Class:[[:space:]]*\([a-z]*\).*/\1/p`) takes `core`. The verdict
  (`8d679ccd`) reads `Class: test`.
- **Ruling: only a label.** In `check_merge`, Class counts only for the
  card-and-verdict equality and for `docs/` (Class: spec).
  - E4-SILENT-1 touches no `docs/` and no `CORE_PATHS`.
  - Its verdict is P on a P card, with 12 mutant rows and 3,974 directed cells.
  - So every rule reads the same under either word.
- **The record keeps `Class: test`,** as the carried file says (Limits 2).
- **Noted, nothing turns on it.** The card's leading word `core` fits the work
  better: it fixes the prototype's recovery code, and the chain's other cards
  that change `src/` are `core` (-F1-F1-F2, -CXX, -F3-F2).
- **The other compound lines match.** -F3-F2's `core/test` reads `core`, as its
  verdict says, and -F3-F2-F1-F2's `test/harness` reads `test`, as its verdict
  says.

**The follow-ups.** Re-derived from the `Followups:` lines of all 27 carried
files against today's QUEUE, every named id now has a `- <ID> |` row. The
orchestrator added rows for the card's four on 2026-11-16. Rulings:
- **E4-SILENT-1-F2: queued.** Nothing in the chain carries it. There is no pin
  for the third over-refusal, and the README still holds the joined line
  "folded cards.- **(b)" at the tip (README line 10328). Its row matches the
  verdict.
- **X-5c-F4-F1-F1-F3-F2-F2: folded** into X-5c-F4-F1-F1-F3-F2-F1 at `b53f9ed3`
  ("pin 4 x5_r49_window_freed_unimaged_home_held_by_image_refs (the -F2 follow-up
  folded in)").
  - That card's verdict `8992f77f` records "O-FREE-DROP-IMGREFS ->
    x5_r49_window_freed_unimaged_home_held_by_image_refs" and "fails pin 4 only".
    The test is a plain `#[test]` at the tip (`tests/store.rs:7031`), so it ran in
    this gate's green test step.
  - Its row's `waiting` is wrong: it should read closed (folded).
- **X-5c-F4-F3-F1: folded** into X-5c-F4-F4 (step 5, `6af3a6c5`).
  - Verdict `18d2530f`: "X-5c-F4-F3-F1 (the record): done", with
    G-FINAL-SYNCOFF-BASE and G-FINAL-W11-OFF caught by
    `x5_r37_final_is_the_kept_setting`.
  - Its row should read closed (folded).
- **X-5c-F4-F3-F2: folded** into X-5c-F4-F4 (step 3, `5b73db6d`).
  - Verdict `18d2530f`: "X-5c-F4-F3-F2 (the CPU tool): done", with
    G-CPU-S1-IS-HEAD and G-CPU-BAND-FROM-FINAL caught by
    `x5_cpu_term.sh --self-test`.
  - Its idle-host measuring run is the final run's CPU record (`X-5c-F4.final.md`:
    "CPU_TERM passes=3 terms=[58.61,55.44,54.59] term=55.44 foreign_pct=0.00
    verdict=OK").
  - Its row should read closed (folded).
- None is void.

**Found as well, outside the card's four.** Seven older rows read `waiting` for
work X-5c-PINS-2 did. Its verdict `fc275882` reads:
- "(1) **F5-F1, done.**" and "(2) **F5-F2, done.**";
- "(3) **F4-F4-F1 (a)–(d), done.**" and "(4) **F4-F2-F1, done.**";
- for F4-F2-F2, the OWN-TUNING-ARM pin done; OWN-SZI-NOFOLD went on as
  X-5c-PINS-2-F1 and item 3 as X-5c-PINS-2-F2, both rows merged;
- "(6) **F4-F2-F3, done.**" and "(7) **E4-SILENT-1-F1, done.**"

So E4-SILENT-1-F1, X-5c-F4-F2-F1, -F2-F2, -F2-F3, X-5c-F4-F4-F1, X-5c-F4-F5-F1
and -F5-F2 should close as folded (§2.7). I found no fold in the chain for
X-5c-F4-F1-F1-F1, X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F2,
X-5c-F4-F1-F1-F3-F2-F1-F2-F2 or X-5c-F4-F7-F1. Those stay queued.

## 2.5 The two P records carried under `verdicts/`

- **What each is.**
  - `E4-judgement.md` is P's judgement of E4 (2026-11-14). It has none of the
    README's keys.
  - `X-5c-F4.final.md` is P's final gate of X-5c-F4 (`Verdict: ESCALATE`,
    `Commit: cb2aca0d…`). It was committed in `754c6536` with
    X-5c-F4-F6's verdict.
- **Each matches its cache copy byte for byte** (sha256):
  - `E4-judgement.md` and `~/.cache/nv3-review/E4-judgement.md`;
  - `X-5c-F4.final.md` and `~/.cache/nv3-review/X-5c-F4.final.verdict.md`
    (PLAN §4 cites that path);
  - `X-5c-F4-F7.md` and `X-5c-F4-F6.md` and their `.verdict.md` copies.

  So the judgement's pointer to `~/.cache/nv3-review/X-5c-F4-F7.verdict.md` and
  the committed `verdicts/X-5c-F4-F7.md` say the same thing.
- **No tool reads them as card verdicts.**
  - `merge-card.sh` reads `$branch:$V3/verdicts/$id.md` (in `check_merge`).
  - `audit-merges.sh` takes the id from the merge subject and calls the same
    `check_merge`.
  - `queue-check.sh` reads `nucleus/v3-scaffold:nucleus/v3/verdicts/$id.md` only
    for `# CARD` files with `Status: ready`.
  - Neither file name is a card id. X-5c-F4's card Status is `blocked`, so
    X-5c-F4 does not read as merged.
- **Keeping them is right.** They are the E4 record that PLAN §4 and §8.2 cite.
  Dropping them is the ruling's rejected variant (§2 (b)).

## 2.6 The exact files the merge adds at the pass base

`git diff --name-status 11aea6a7 6d5df726 -- nucleus/v3/verdicts` gives 27 lines,
all `A`. Each file is absent at `ee8298c3` and on the scaffold `d7d41677`, so
"absent at BASE, present at the tip" holds for every one.

- `DEP-RUSQLITE-1.md`, `E4-CLASS-1.md`, `E4-SILENT-1.md`, `E4-judgement.md`
- `X-5c-F4-F1-F1.md`, `X-5c-F4-F1-F1-F2.md`, `X-5c-F4-F1-F1-F2-F2.md`
- `X-5c-F4-F1-F1-CXX.md`, `X-5c-F4-F1-F1-CXX-F1.md`, `X-5c-F4-F1-F1-O1C-F1.md`
- `X-5c-F4-F1-F1-F3.md`, `X-5c-F4-F1-F1-F3-F1.md`, `X-5c-F4-F1-F1-F3-F2.md`
- `X-5c-F4-F1-F1-F3-F2-F1.md`, `X-5c-F4-F1-F1-F3-F2-F1-F1.md`, `X-5c-F4-F1-F1-F3-F2-F1-F2.md`
- `X-5c-F4-F1-F1-F3-F2-F1-F1-F1.md`, `X-5c-F4-F1-F1-F3-F2-F1-F1-F1-F1.md`
- `X-5c-F4-F2.md`, `X-5c-F4-F3.md`, `X-5c-F4-F4.md`, `X-5c-F4-F5.md`, `X-5c-F4-F6.md`, `X-5c-F4-F7.md`
- `X-5c-F4.final.md`, `X-5c-PINS-1.md`, `X-5c-PINS-2.md`

That is 25 card verdict files plus the two P records, which matches the ruling's
27 = 25 + 2. With this card's own `E4-CHAIN-1.md`, the merge adds 28.

**Measured at the pass base** (the throwaway clone's pass after the nine
merges):
- Merging `6d5df726` changes 52 paths:
  - 25 under `spikes/e4-recovery/` (9 `A`, 16 `M`);
  - the same 27 verdict files, all `A`.

  With this card's file, that is 53 paths. The merge is clean (tree `edd45cec`).
- `spikes/e0-btree/Cargo.toml` is already identical on the pass side (blob
  `ee4a4423`), so the merge does not change it.

**The dry run.**
- **Setup.** This file was committed on a clone-only `card/v3-E4-CHAIN-1` on top
  of `6d5df726`, in the same throwaway clone.
- **Without the row.** `merge-card.sh E4-CHAIN-1` refused with exactly 27 lines
  of the form `nucleus/v3/verdicts/<file>: a card may change only its own
  verdict file`, one per file above, and one more:
  `follow-up E4-CHAIN-1-F1 is not a QUEUE entry ('- E4-CHAIN-1-F1 | ...')`.
- **With the row.** Once a `- E4-CHAIN-1-F1 |` row was added to the QUEUE copy,
  the refusal was exactly the 27 lines.
- So, under today's scripts, the only rule this verdict fails is the one
  MERGE-CHAIN-1 exists to change. Card, Class, Card-gate, the tiers,
  `Gate-exit`, the 40-hex `Commit` equal to `tip^`, and the
  tip-changes-only-its-own-file rule all pass on this file.

## 2.7 QUEUE and Status closure

The orchestrator applies these after the final pass is promoted. Items 6 to 8
can be done now.

1. **X-5c and X-5c-F4 (alias branches).**
   - X-5c is `b19ab321` (X-5c-F3-F3a-F2-F1's tip), and X-5c-F4 is `cb2aca0d`
     (E4-CLASS-1's tip). Neither has a mergeable `verdicts/<ID>.md`, and neither
     is merged per card.
   - Set each Status to `merged-into-card/v3-E4-CHAIN-1`.
   - Set the QUEUE rows to `merged`. X-5c's row reads `blocked` today; X-5c-F4's
     already reads `merged`. X-5c-F4's card Status is `blocked` today.
2. **The four ESCALATE cards.**
   - Today: X-5c-F4-F1-F1-F3, -F3-F2 and -F3-F2-F1 are Status `ready`, QUEUE
     `blocked`; X-5c-F4-F3 is Status `ready`, QUEUE `merged`.
   - Set each Status to `merged-into-card/v3-E4-CHAIN-1`, and each QUEUE row to
     `merged` naming this verdict.
   - **Why.** `queue-check.sh` skips a Status-ready card once
     `verdicts/<ID>.md` is on the scaffold, whatever its Verdict says. After the
     merge their ESCALATE files would read as merged. That is true of their
     commits, and the record must say why.
   - Tightening queue-check stays out of scope (ruling §2 (c); the freeze
     stands).
3. **E4-CLASS-1, X-5c-F4-F6 and X-5c-F4-F7.** E4-CLASS-1 is Status `waiting`,
   QUEUE `merged`; F6 and F7 have no Status line. Set all three to
   `merged-into-card/v3-E4-CHAIN-1`.
4. **The rows to close.**
   - `E4-CHAIN-MERGE` becomes `merged`, naming E4-CHAIN-1 and MERGE-CHAIN-1.
   - `E4-CHAIN-1` and `MERGE-CHAIN-1` become `merged`.
   - `scratch/pass153` is marked abandoned, never deleted.
5. **For a plan card.** PLAN §4 "Still open" and §8.2 point (ii) record that T
   `cb2aca0d`, `754c6536` and `6d5df726` are scaffold ancestors.
6. **Follow-up rows.**
   - Close X-5c-F4-F1-F1-F3-F2-F2, X-5c-F4-F3-F1 and X-5c-F4-F3-F2 as folded
     (§2.4).
   - Close the seven PINS-2-folded rows as folded into X-5c-PINS-2 (`fc275882`).
   - Queue **E4-CHAIN-1-F1 before the final pass**: merge-card checks this
     verdict's Followups.
   - **A trap when adding that row.** `QUEUE.local.md`'s last line (the
     AMCHECK-1 row) has no trailing newline. A row appended with `>>` joins that
     line, and merge-card then does not find `^- E4-CHAIN-1-F1 \|`. The dry run
     hit exactly this. Start the row on a new line.
7. **E4-SILENT-1's card.** Note beside its `Class: core/test` line that the
   record keeps `test` (§2.4). Do not edit the verdict file.
8. **Evidence.**
   - Push `card/v3-E4-CHAIN-1` (the card's "For the orchestrator" item 5).
   - Push `card/v3-X-5c-F4-F1-F1-O1C` (`a4cabc44`), under the owner's "push the
     E4 evidence to the git server (yes)". It holds O1C's P verdict for two
     chain commits (§2.1). `git ls-remote origin` shows it is not on the server.

## Anomalies, re-checked 2026-11-16 (all still true)

1. `card/v3-X-5c-F4-F1-F1` → `0d0f3da4`, `card/v3-X-5c-F4-F1-F1-F2-F2` →
   `26bcee4b` and `card/v3-DEP-RUSQLITE-1` → `2e8d1b13` point at another card's
   verdict commit.
   - The three aliases: `card/v3-X-5c` = `b19ab321`, `card/v3-X-5c-F4` =
     `cb2aca0d`, `card/v3-X-5c-F4-F1-F1` = `0d0f3da4`.
   - This gate moved no branch.
2. The three re-verdicts of §2.3.
3. `X-5c-F4.final.md`'s name.
4. `E4-judgement.md`'s form.
5. E4-SILENT-1's Class.
6. The four Status-ready ESCALATE cards.
7. The main checkout's untracked files under `verdicts/`, which are on no
   branch. Today there are 41 (`PL-*`, `S-1-F3-draft*-gate`, `X-5c-*-esc-*`,
   `X-6-F1-F2-F3-F1-esc-1`, and others). The ruling counted 32 on 2026-11-14.
   None has a chain card's verdict file name (`<ID>.md`).
8. `card/v3-X-5c-F4-F1-F1-O1C` (`a4cabc44`) is off the chain. Its two work
   commits are in the chain as cherry-picks (§2.1).

Also, with no effect on any rule:
- `5aa9fcdc`, `648a0e5f` and `7f64cad1` are empty commits.
- `2be66234`'s subject begins "neutronus v3:".

## For the owner (stated plainly; these are not rulings)

1. **One confirming line, before the final pass** (the card's Open point 1). The
   owner's reply of 2026-11-16 answered a list item naming the tooling-freeze
   exception "to merge the E4 chain MERGE-CHAIN-1". That the reply also accepts
   four ESCALATE cards' commits onto the scaffold under one P verdict is a
   reading (PLAN §10 item 20 (d)). This verdict does not settle it. The question,
   exactly:

   > "Merging the E4 chain puts on the scaffold the commits of four cards whose
   > own gates ended ESCALATE: X-5c-F4-F1-F1-F3, X-5c-F4-F1-F1-F3-F2,
   > X-5c-F4-F1-F1-F3-F2-F1 and X-5c-F4-F3. Your decisions of 2026-11-07 and
   > 2026-11-10 answered two of them. P answered the other two by amending rules
   > P had written for those cards, after seeing the data and labelled so; these
   > did not come to you and do not touch E4's pass definition. Continuation
   > cards then passed their gates, and E4 was judged on the result. The four
   > would reach the scaffold on one P verdict over the whole chain
   > (E4-CHAIN-1), not on MERGE verdicts of their own. Do you accept that: yes
   > or no?"

   - **A no** means route (d1). The chain stays off the scaffold, "E4 decided"
     still counts (P0 review 2, point (ii)), and nothing done so far needs
     undoing.
   - **When to ask.** I recommend asking before MERGE-CHAIN-1 is set ready,
     since that tool change has no other use. The card requires the answer
     before the final pass at the latest.
2. **On the reply's condition** ("if thats all best long term … in all ways").
   - For the database itself the merge changes nothing outside
     `spikes/e4-recovery/` and `verdicts/`. It is a record and process decision.
   - The merge is best overall in my view: it makes the E4 evidence scaffold
     ancestry and keeps every verdict record, the ESCALATE ones included.
   - It is not best in every way. Four cards arrive without MERGE verdicts of
     their own, through a one-time, one-entry tool exemption. After the merge
     queue-check counts their ESCALATE files as merged, which only Status edits
     explain (§2.7).
   - The owner should hear this with the question.
3. **Nothing else here needs the owner.** That includes the two ESCALATE members
   without an owner line (§2.2).

## Not verified

- Member code and measurements, beyond the sweep under "Scope". Content rests on
  each member's verdict.
- The output of P's ruling task `nv3-ruling-X-5c-F4-F1-F1-F3-F2-1` as a file:
  none exists under that name in `~/.cache/nv3-review/`. X5-R51 is verified
  only as carried verbatim in its P-written card and the spike README.
- Member `Gate-session` ids against T3 (the README's trust limit).
- That `card/v3-X-5c-F4-F1-F1-O1C` is not on origin rests on one
  `git ls-remote` at gate time.

## Follow-ups

**E4-CHAIN-1-F1** (plan and runbook text; P author, P gate; non-blocking).
Write the chain-merge ruling's standing instruction into binding text: the
runbook `ORCHESTRATOR.local.md` (§2 running a card, §3 gates, §5 merge pass) and
PLAN §7.5 "Card branches". The instruction is in
`~/.cache/nv3-review/E4-chain-merge-ruling.md` §3, last paragraph:
- never fast-forward a `card/v3-<ID>` branch past its own verdict commit, and
  give a chain-head pointer a non-card name;
- never commit a re-verdict on top of another card's verdict commit;
- a card whose base is another unmerged card is stacking, and it needs a merge
  plan the tools support before it launches.

Today it is in that ruling file only: no QUEUE row, card, PLAN text or runbook
text carries it (checked 2026-11-16). This episode's anomalies 1 and 2 come from
exactly those practices. Without the instruction, the next stacked chain would
need another owner freeze exception to merge.
