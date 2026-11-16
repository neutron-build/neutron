Card: S-DELTA
Class: spec
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 8c2a0206-15c1-43a6-aa50-edb6ab78b065 (a fresh gate: this agent wrote neither the card, its edits, verdict 1 nor the rework)
Date: 2026-11-16
Gate-exit: GATE EXIT=0
Commit: cf6162eb4504813772f6ca89a2a1c167952159c7
Mutants: spec text, none in code; each re-walked as text on its owning row at cf6162eb: POOL-MIN-PATH-ONLY -> t_pool_minimum (A-3b), caught (it accepts the pool one frame below the minimum computed with callers' and the build's own threads), RESTART-KEEPS-EMPTY-MAPLEAF -> t_alloc_restart_progress pool-floor variant (A-4a), caught through the tracker (h+1 pins after restart 1, at least h+2 after 2, at least h+3 after 3, with h+2 hooked rounds), and a correct build stays at or below h+1 operation MapLeaf pins under RWD-3, ROOT-PIN-MAP-FIRST -> t_root_name_reuse case F (A-7b), caught (no background evictor, RWD-24), WAL-ERR-NOT-POISON -> t_gated_xx001_poisons WAL variant (A-7c), caught through the read outside the writer (a writer that keeps its own error passes the waits and fails the read), OPEN-PROTECTS-ONE-GEN -> t_two_gen (b) (A-7d), caught when the crash falls after pass 2's and the boot checkpoint's step-2 writes (note 1), UNREAD-PROTECTS-NOTHING -> t_two_gen (c) (A-7d), caught through the write hook, same placement of the crash, FENCE-SKIP -> t_meta_copies fence variants (A-7d), caught ((i) the never-zero form, (ii) both), WALK-XX001-NO-POISON, MISPLACED-NEXT-REFUSES, MISPLACED-IGNORED, CHUNK-SET-KEEPS-ENDED, DROP-MARK-AT-PUBLISHED-W -> unchanged by the rework, as verdict 1 walked them, caught; gate's own: G3 a correct build whose retry holds an emptied pin and also non-emptied pins with enough free LPIDs -> takes its LPIDs under its pins and holds at most n pins (RWD-3 forbids only LPIDs outside its pins), no false alarm, G4 a pin tracker that classifies by page kind only (the card's Notes) on a build whose section-4.1 dirty pins go through the pool's pin call -> fires on a correct build after the first mutation (residual 2; settled by the plan card's A-3c seam, below)
Followups: none

## Verdict in brief

**MERGE.** The rework is exactly verdict 1's RWD-1 to RWD-29, byte for byte, on d493188f's `docs/S-STORE.md`; C-T0 is untouched. Every ruling of verdict 1 is now true in the text, and I re-derived each one rather than take it on trust: `t_two_gen` (b) and (c), the pool-floor variant under the new retry rule, who `threads` counts, WAL-ERR-NOT-POISON at A-7c, the chain pins, case F and the wording items. Each holds.

None of the rework implementer's three residuals or five notes blocks the merge. No test fails on a correct conforming build on the card that owns it. No mutant is unkillable on a conforming build. No sentence leads an implementer to build the wrong thing. Each open detail is a fixture detail or a test seam that a named card settles without contradicting the spec; "What the plan card must say" below lists them. The card's prose is stale in places (residual 1). The card file is not on the branch, so this does not block the merge, but the plan card must be written from this verdict's list, not from the card's "For the plan card" section.

There are no rework blocks and no follow-up cards. After this merge, S-GAMMA's binding condition is discharged. The only S-STORE spec item still able to precede a Phase A card is S-ALPHA-F4's small card, and only if the owner says "yes" (before A-7d).

## Gate run

- **Extract.** A shallow `--no-local` clone of `nucleus/v3-scaffold` gave scaffold id `1084de903b3031e750350f33b0cfd23fe6edb9a4`. It extracted `fresh-gate.sh` and `git-env.sh` with no `EXTRACT FAILED`.
- **Run.** From `~/Workspace/neutron-v3`, `bash "$fg/fresh-gate.sh" cf6162eb4504813772f6ca89a2a1c167952159c7`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-DELTA-2` and stdout and stderr captured to separate files:
  - `fresh-gate: commit cf6162eb4504813772f6ca89a2a1c167952159c7`
  - `fresh-gate: base 1084de903b3031e750350f33b0cfd23fe6edb9a4`
  - `fresh-gate: checks passed (4817 files hashed in 1.79 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - stderr was empty. There were 883 `ok` lines and 56 `test result: ok` lines (467 passed, 0 failed). Every STEP exited 0 (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests). Exit 0; literal last line `GATE EXIT=0`.
- **Afterwards.** `$fg` was removed. Before this verdict's commit, the worktree was at cf6162eb and clean.

## Bytes, scope, names, Acceptance

- **Tip.** `card/v3-S-DELTA` is at cf6162eb, and `git status` is clean. History: 1084de90 → 7dc8cbc5, d493188f (the 115 pairs) → 4d0eb2c0 (verdict 1) → cf6162eb (the rework).
- **The rework is exactly RWD-1 to RWD-29.** My own script parses the 29 blocks out of `git show 4d0eb2c0:nucleus/v3/verdicts/S-DELTA.md` and applies them in order to d493188f's `docs/S-STORE.md`. Each Old occurs exactly once at its turn. The result is byte-identical to cf6162eb's file, sha256 `6197762c979457954dec16c78c813b2f0c66dce704a6691fa90d6f1c54981c14`, which is verdict 1's dry run. C-T0 is unchanged since d493188f (`96caa6cb…a0c0`).
- **Scope.** `git diff 1084de90...HEAD` changes `docs/C-T0-txn-protocol.md`, `docs/S-STORE.md` (the card's Touch only) and `verdicts/S-DELTA.md`. The rework commit touches `docs/S-STORE.md` only (+24/−24).
- **Names.** No mutant, test or invariant token of 1084de90 or of d493188f is missing at HEAD, and HEAD adds none.
- **Placements.** Parsing §15.1 at d493188f and at HEAD, the only change of placement is WAL-ERR-NOT-POISON: {A-6c, B-1c} → {A-7c, B-1c}. No mutant judged on a Phase A row at d493188f is judged only in Phase B now.
- **Acceptance.** The card's Acceptance has 146 commands. Against the pre-rework card, they are the 6 replaced lines and the 23 added lines, exactly verdict 1's 32 lines. The only other change to the card is the Edits line.
  - In a scratch clone at cf6162eb, all 145 commands before `gate.sh` pass.
  - The checker (now line 121) prints `158 rows; 225 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `56 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 here), 56 of them judged on a Phase A row; 47 invariants, 48 tests`, `OK`.
  - Each of the 32 new or replaced lines fails at d493188f.

## Verdict 1's rulings: true in the text, and right

1. **`t_two_gen` (b) and (c): true (RWD-1, -2) and right.** The crash now comes "before the boot checkpoint's step 4, so that no copy of its meta is written and the next open selects meta `k` again".
   - **Correct build.** For any crash point before step 4, the next open selects meta `k`.
     - In (b), the post-sync fault on an image-`k` page that step 2 reads forces a fall-back. Image `k−1` is intact because the first open protected its slots, and its WAL span is intact because nothing recycles before the boot checkpoint's step 6. So (b) passes.
     - In (c), the first open protects every slot below meta `k−1`'s `slot_count`. The read that fails is a free-set read, not a meta place, so no fence runs. No fall-back is attempted, so no 58030. The second open reads the free set and falls back with proof. So (c) passes.
   - **Kill.** OPEN-PROTECTS-ONE-GEN's overwrite in (b), and UNREAD-PROTECTS-NOTHING's hooked write in (c), happen only if pass 2's or the boot checkpoint's step-2 writes have run before the crash. A crash placed early leaves both mutants alive. The text puts the evictions first ("so that pass 2 and the boot checkpoint evict, a crash before …"), and A-7d's gate must see both mutants caught, so a fixture with an early crash cannot pass its own gate. This is under-specification, not a defect. The A-7d card pins the crash point (plan card, below).
2. **The pool-floor variant: true (RWD-3, -5, -6) and right.** Re-derived with `n = h + 1` (a cascading split to the root):
   - **Round 1.** Each insert restarts and pins `h + 1` MapLeafs. S1 flips. Both inserts block in the gate's shared acquire. The seam fires for both, S1 releases, and both retry holding the gate shared.
   - **Each hooked round.** Before the free-set lock, the hook's thread empties every MapLeaf that restart pinned. It works under the gate shared, tree 3's latches and the free-set lock, and it never waits on the held thread, which holds no free-set lock and no tree-3 latch; its evictions use `try_latch`. The retry then holds only emptied pins. RWD-3 makes it restart, whatever the residency of the next MapLeafs. The restart drops every emptied pin before it pins more.
   - **Correct build.** It holds at most `n = h + 1 ≤ h + 2` operation MapLeaf pins, and at most `(h + 2) + (h + 1) ≤ 2(h + 2)` tree-page pins. I checked the mixed case as well (G3): an emptied pin plus kept pins with enough free LPIDs. RWD-3 forbids only LPIDs outside the thread's own pins, so the retry takes its LPIDs and still holds at most `n` pins. The total stays within §5.4's `3h + 6`.
   - **Mutant.** It holds `h + 1` pins after restart 1, at least `h + 2` after restart 2 and at least `h + 3` after restart 3. Restart 3 is within the `h + 2` hooked rounds for every `h ≥ 1`, so the tracker fires at that pin. The kill does not depend on exhausting the pool: there is slack from S1's unused count.
   - **Restart bound.** Every restart after the first is charged to the hook's thread's allocations, or to the other insert's, so both splits finish within §5.6's bound.
3. **Who `threads` counts: true (RWD-4, -7, -9, -10, -27) and right.** Counting S1's flip thread, which pins nothing, over-provisions the floor variant by one thread's count. The variant's kill is through the tracker, not exhaustion, so this is harmless.
4. **WAL-ERR-NOT-POISON at A-7c: true (RWD-11 to -15) and right.**
   - **The closure.** PLAN §9: A-7c's Depends are A-7b, A-6c and A-1b; A-7b's include A-7a; A-7a's is A-3c. So A-7c's closure holds A-6c (the writer) and A-3c (the poison mark and the poison check at pin acquisition). The checker's D map matches PLAN.
   - **The kill.** §9.2 item 1 makes the mark before the failing operation releases anything. Item 2 makes every later call return the error, reads included. Item 3 wakes every wait on the mark. On a correct build, the read after a waiter returns the error therefore fails too. A writer that keeps its own sticky error but leaves the mark unset passes the waits and the appends, and fails through the read.
   - **A-6c.** A-6c no longer claims it, and the Phase-B-only count stays 24.
5. **Wording items (RWD-16 to -23): true and right.** These are `t_flip_chunk_race`'s "full except S3", §15.1's A-7f lead-in, §4.4 item 4, FENCE-SKIP's forms, step 4a's "the places of its own parity" and "§5.4's per-thread count" (×3).
6. **Case F: true (RWD-24, -25) and right.** With no background evictor and no flip, `r` stays resident and never written, so its entry keeps slot 0. A page-table-first pin finds the frame, and ROOT-PIN-MAP-FIRST reads "unmapped" and raises XX001 on a healthy store. The test-only mode is a root-name read that holds no gate.
7. **Chain build and free pins: true (RWD-8) and right.** §6.7 already has the build and every free outside the sweep hold only the leaf. Leaf, two chain pages and two MapLeafs make 5 pins, within `3h + 6`. The path term's arithmetic (`h`, `h + 1` for a pack step, `+ 1` cold, so `h + 2`) is now stated.
8. **The gate seam: true (RWD-5) and right.** It is a wrapper over the RwLock (try, then hook, then block). It does not contradict §5.6's "The gate is a `RwLock`". The hook can tell the inserting threads by thread id, so an evictor's `try` (which never blocks) does not miscount.
9. **The fence's one-copy residual: true (RWD-26, -29).** It is stated in §14 step 1 and in the Changelog, as verdict 1's P-7 asked.

## The residuals and notes: rulings

1. **Residual 1, the card's stale prose: not a spec defect, and not blocking.** The card is `cards/S-DELTA.local.md`, outside the branch, so nothing stale ships. The record needs three things:
   - **(a) A superseded note at the top of the card.** The orchestrator's delegate adds, before or after the merge: "Superseded in part by the rework (verdict 4d0eb2c0, RWD-1–29) and by verdict 2 of S-DELTA. Where this card's prose disagrees with `docs/S-STORE.md` at cf6162eb or with those verdicts, they win." The note lists the stale passages: the title's and Goal item 4's "WAL-ERR-NOT-POISON at A-6c"; D1 (the tracker, the thread count, the seam); D4 (step 5); D7 and D-11 (A-6c); the "§15.1" draft-target bullet; E43 and the other E-blocks naming A-6c for it; Mutation evidence; "For the gate" item 2; Limits 2–6 (they omit RWD-1–29); "the checker (line 118)" (now 121); the Notes' "Due" list; and **the Notes' "For A-4a's test author" bullet**. That bullet is not in the implementer's list, and it now contradicts RWD-6: "the pin tracker classifies a pin by its page's kind" counts §4.1's dirty-page pins.
   - **(b) No in-place edits.** The E-blocks stay as they are: the Edits line already says that HEAD is the pairs followed by RWD-1–29.
   - **(c) "For the plan card".** It is replaced by, or points to, this verdict's "What the plan card must say". The plan card is written from that list.
2. **Residual 2, telling §4.1's dirty pins from an operation's own: real, under-specification, not blocking. The plan card settles it on A-3c.**
   - **Why the tracker needs it.** RWD-6 says what the tracker counts, but nothing makes the pool's pin call carry the distinction. On a build whose §4.1 pin is a plain `pin` by the dirtying thread, a tracker that cannot tell the two kinds apart fires on a correct build after the first mutation. The matching unpin also often runs on another thread (an evictor, or step 2), so the per-thread count drifts as well (G4).
   - **Where it goes.** §4.1's rule ("a MapLeaf stays resident while any page it maps is dirty") is A-3c's code (PLAN A-3c, "dirty eviction … via the page map"). No Phase A card from A-2 on is cut before the plan card. So A-3c's row can require the seam: the pool's pin and unpin hook reports whether a pin is §4.1's dirty-page pin of a MapLeaf, or §4.1's pins do not go through the hooked call. This contradicts nothing, and every conforming build can offer it, because the §4.1 pin is a distinct concept with a distinct release.
   - **Why it is safe.** If it were missed, A-4a's own gate would see the tracker fire on the unmutated build, and the card could not pass. So it cannot slip through silently. It is the same class as verdict 1's note 2, but a plan-row seam suffices because it changes no test or spec obligation.
3. **Residual 3, "no flip is pending": not blocking.** S1 releases the gate once both seam hooks have fired, before any hooked round, so S1 is never pending during one. The only other source of a flip is a checkpointer of the build's own, which is A-7c's code (§13, PLAN A-7c, "meta flip order (steps 0–6)"). It is not in A-4a's closure (A-3c, A-3b, A-3a, A-2, A-0, A-4c0). §15.1 runs the variant on A-4a only, with no later row, and S1 is the flip "before A-7c" by definition. So the owning card cannot deadlock on a correct build.
   - **What remains.** The test stays in the suite after A-7c. If A-7c's store started checkpoints on its own (a timer, or a WAL threshold) during this fixture, the old test could deadlock.
   - **Who settles it.** The plan card notes it on A-7c: any checkpoint trigger of the store's own can be turned off by an open option, and every fixture that names S1 as its flip, `t_alloc_restart_progress`'s pool-floor variant included, opens with it off. The A-4a card states the same premise for its fixture. This contradicts nothing: "no flip is pending" is a fixture premise.
4. **The notes.**
   - **`t_two_gen` (b) and (c), the crash point.** Under-specification, settled by the A-7d card. See ruling 1.
   - **§5.6 "Restart" still says a restart keeps "its reservation's … MapLeafs".** This is consistent with the new rule. A MapLeaf with no free LPID left can no longer serve the reservation, and the same paragraph's later, specific sentence ("Each such restart drops the pin of every MapLeaf left with no free LPID before it pins another") governs. It does not mislead.
   - **§5.4 does not require the build to report its own thread count.** The requirement is in §5.4's own test text ("as the build reports them"). The plan card makes it explicit on A-3b, as verdict 1 already said: the store reports its pinning-thread count, and the open takes the callers' count.
   - **`t_pool_minimum` and N10's cooperative mode.** No test fails because of this. In cooperative mode (embedded, wasm), the store has no threads of its own and runs its background work on callers' threads, so the build reports 0, and the callers' count the test passes is a task count. The A-3b card pins the open option's meaning: callers' threads, or under N10 the tasks that can hold a pin while they wait (RWD-7).
   - **§5.6's gate does not mention the seam.** The seam is in §5.4's test text, which names A-3c. The plan card puts it on A-3c's row (verdict 1 item 5).
   - **A further fixture point, this gate's own.** The WAL variant at A-7c says "a write hook sees … no data-file write after the failure". A dirty eviction that passed its poison checks before the mark was set may finish its write after the failure on a correct build. §9.2 checks the mark at acquisitions, not mid-write. The A-7c card pins the fixture: the pool holds every page and no eviction runs across the failure, as in case F. This contradicts nothing.

## What the plan card must say (this replaces the card's "For the plan card")

Verdict 1's list, "What the plan card must change", items 1–16 at 4d0eb2c0, stands in full: the binding condition, §3.3, A-3a, A-3b, A-3c, A-4a, A-5, A-6c, A-7c, A-2, A-7b, A-7d, A-7f, B-6, §10 item 19 and QUEUE. This verdict adds:

- **A-3b.**
  - The open takes the callers' count: threads, or under N10 the tasks that can hold a pin while they wait.
  - The store reports its own pinning threads (0 in cooperative mode). `t_pool_minimum` adds them.
- **A-3c.**
  - The gate seam (verdict 1).
  - The pool's pin and unpin hook tells §4.1's dirty-page MapLeaf pins apart from an operation's own pins, or §4.1's pins bypass the hooked call (residual 2).
- **A-4a.**
  - The tracker counts as §5.4's floor variant says, not by page kind; the card's "For A-4a's test author" note is superseded.
  - S1 is the fixture's only flip.
- **A-7c.**
  - Any checkpoint trigger of the store's own can be turned off at open, and fixtures that use S1 turn it off (residual 3).
  - The WAL variant's fixture has no eviction in flight across the failure.
- **A-7d.** `t_two_gen` (b) and (c) place the crash after the boot checkpoint's step 2 has written its image (after step 3 is simplest) and before step 4, so that pass 2's and step 2's writes have run.
- **The binding condition.** It is discharged by this merge (below). A-6a no longer waits on the watermark, which is final.

## The record after this merge

- **S-GAMMA's binding condition is discharged.** The condition was: "no card that reads §12.4, §12.6 or §13 step 0 is cut before S-DELTA merges" (verdict 60e71ed0). This card carries S-GAMMA-F1 to F7, and after it nothing pending edits those sections. A-6a, A-6c, A-6d, A-7c and A-7d are free of it once this card merges.
- **Open S-STORE items that must precede a Phase A card.**
  - **S-ALPHA-F4's small card, only on an owner "yes", before A-7d.** It is the appendix's ten blocks. Verdict 1 verified that they apply to the reworked text.
  - **Conditional, P-6.** If the plan card moves the two I-MAP-GROW full forms to A-7f against the recommendation, a two-cell relabel spec card precedes A-7f.
  - **Not S-STORE edits, but still due before their cards:** R-1's spec card (`R_EPOCH`, `R_MAP` and `F`) before A-3b and A-7a, and X-5c-F4-F7-F1 before A-8a.
  - S-1-F3-F1 is C-T6's.
  - QUEUE lists no other open S-STORE spec row.

## Owner points, in plain words

- **The larger minimum pool (P-1).** *Accept.* Each thread that can hold pages, the store's own included, now needs room for three tree paths' worth of pages, not one: about 168 KiB per thread at 8 KiB pages and tree height 5, up from 56 KiB. The smaller floor could deadlock or never finish a split that cascades to the root. A pool below the new floor is refused at open with 53200.
- **The 58030 middle course (P-4).** *Accept.* If the open cannot read the older checkpoint's free-space map, it carries on and treats all of that older checkpoint's space as in use until the next checkpoint. It reports 58030 only when that unreadable map is exactly what stops it falling back. Refusing outright would let one bad sector, in data the next checkpoint discards anyway, lock the database out.
- **The misplaced-copy rule (P-5).** *Accept.* A stray copy of a checkpoint record that is just one checkpoint newer no longer blocks the open, because everything the open needs is still intact. Before, one misdirected write plus a crash refused an intact store. It still refuses a stray copy two or more checkpoints newer.
- **S-ALPHA-F4 (P-8).** *Recommend "yes".* After one unreadable sector under the newest checkpoint, an operator could still read the data from the previous checkpoint and choose to roll back one checkpoint, while the automatic open still refuses. A "yes" is one small spec card, due before A-7d.
- **The 8-byte DropTree watermark (S-BETA-O1).** *Final.* S-GAMMA adopted it, and this card changes no byte format, so A-6a may fix the WAL record format once this card merges.
- **The fence (P-7).** *Accept zeroing, with the residual.* An open that could not read a checkpoint record's place wipes it before writing. In rare two-fault cases, that leaves the current checkpoint with one copy until the next checkpoint rewrites both.

## History

- **Verdict 1: REWORK, commit 4d0eb2c0bee3068563e816b23bcf2e77ec213f42, on d493188f09bf8bc4bd8946f12a815a4330895899.** A P gate in Claude Code session eb411399-913a-49f6-a32f-1be6686be2f9; GATE EXIT=0.
  - **Bytes.** It found the branch exactly the card's 115 pairs, with nothing lost and no Phase A judgement leaving Phase A.
  - **Findings.** It ruled real:
    - `t_two_gen` (b) and (c) could fail on a correct build whose crash fell after step 4;
    - the pool-floor tracker fired on correct builds (§4.1 pins, and G1: a retry pinning more while it kept emptied pins), and the flip's release needed a gate seam;
    - `threads` excluded the build's own threads;
    - WAL-ERR-NOT-POISON was unkillable at A-6c on a writer that keeps its own error;
    - §5.4 left chain build and free pins unbounded;
    - case F failed under a background evictor;
    - eight wording items, and G2's one-copy fence residual.
  - **Fix.** It gave 29 exact blocks, RWD-1 to RWD-29, with 6 replaced and 23 added Acceptance lines, a dry-run sha256 `6197762c…1c14`, and no follow-ups. Its full text, including the blocks, the design review, its open-rule model (`~/.cache/nv3-review/gate-S-DELTA/model`) and the plan-card list, is at `git show 4d0eb2c0:nucleus/v3/verdicts/S-DELTA.md`.
- **The rework: commit cf6162eb4504813772f6ca89a2a1c167952159c7.**
  - **Spec.** It applied RWD-1 to RWD-29 by script to `docs/S-STORE.md` (+24/−24; result sha256 = the dry run). Draft numbers stay 7.14 and 8.7, and C-T0 is untouched.
  - **Card.** It replaced the 6 Acceptance lines and added the 23 (146 commands), and added an Edits line saying that HEAD is the pairs then RWD-1–29. It left the card's prose as it was (residual 1). It reported 145/145 and GATE EXIT=0, and residuals 1–3 with five notes, ruled above.
- **Verdict 2 (this one): MERGE on cf6162eb.**
