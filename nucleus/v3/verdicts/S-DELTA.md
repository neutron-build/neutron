Card: S-DELTA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session eb411399-913a-49f6-a32f-1be6686be2f9 (this agent wrote neither the card nor its commits)
Date: 2026-11-16
Gate-exit: GATE EXIT=0
Commit: d493188f09bf8bc4bd8946f12a815a4330895899
Mutants: spec text, none in code; each walked as text on its owning row at d493188f: POOL-MIN-PATH-ONLY -> t_pool_minimum (A-3b), caught, but a correct build that counts threads of its own refuses the test's pools with 53200 (finding 3; RWD-4, -7, -9, -10), RESTART-KEEPS-EMPTY-MAPLEAF -> t_alloc_restart_progress pool-floor variant (A-4a), caught, but the tracker fires on correct builds and the flip's release needs a gate seam no build is required to have (finding 2, note 2, G1; RWD-3, -5, -6), ROOT-PIN-MAP-FIRST -> t_root_name_reuse case F (A-7b), caught except on a build whose background evictor writes r first (finding 12; RWD-24), WALK-XX001-NO-POISON -> t_root_name_reuse walk-poison variant (A-7d), caught, FENCE-SKIP -> t_meta_copies fence variants (A-7d), caught (the never-zero form by (i) and (ii), the late form by (ii) only; RWD-19), UNREAD-PROTECTS-NOTHING -> t_two_gen (c) (A-7d), caught through the write hook, but (b) and (c) fail on a correct build whose crash falls after step 4 (finding 1; RWD-1, -2), MISPLACED-NEXT-REFUSES -> t_meta_copies table form (A-2) and in-flight variant (A-7d), caught, MISPLACED-IGNORED -> t_meta_copies table form (A-2), caught, CHUNK-SET-KEEPS-ENDED -> t_flip_chunk_race variant ended (A-7d), caught, DROP-MARK-AT-PUBLISHED-W -> t_drop_restart_floor (a) (A-7c), caught, WAL-ERR-NOT-POISON -> t_gated_xx001_poisons WAL variant (A-6c), UNCAUGHT on a conforming writer that keeps its own error (finding 4), caught at A-7c after RWD-11 to -15; gate's own: G1 a correct build whose retry takes LPIDs under resident MapLeafs while it still holds its restart's emptied MapLeaf pins -> holds 2h+2 MapLeaf pins, trips the floor variant's tracker and exceeds §5.4's third term at d493188f (closed by RWD-3), G2 the fence zeroes the selected meta's own unreadable copy, then the boot checkpoint's first copy is misdirected onto its other copy and a crash follows -> XX001 in both modes (18 two-fault states in the model; draft 7.13 refused every one), not a defect, documented by RWD-26
Followups: none

## Verdict in brief

**REWORK.** The bytes are right: the branch is exactly the card's 115 pairs, the riders are S-GAMMA's verdicts' bytes, nothing of 7.13 or 8.6 is lost, and no Phase A judgement leaves Phase A. The design holds up: the three-term minimum is sufficient once two unstated pins are bounded, the page-table-first pin rule is right, the fence closes both traces and never makes an outcome worse than draft 7.13, the middle course for a 58030 on the previous free set is right, and the misplaced-copy change removes the only one-fault refusal of an intact store without opening anything unsafe below four faults.

But four of the implementer's twelve findings, both of its section notes and one finding of this gate make tests that can fail on a correct build or mutants that cannot fail on every conforming build:

- `t_two_gen` (b) and (c) can fail on a correct build (finding 1).
- The pool-floor variant's tracker can fire on a correct build three ways (finding 2 and G1), and its release of the flip needs a gate seam that no build is required to have (note 2).
- `t_pool_minimum` and the floor variant are refused with 53200 on a correct build that has threads of its own (finding 3).
- WAL-ERR-NOT-POISON cannot fail at A-6c on a writer that keeps its own error, because the poison mark is outside A-6c's closure (finding 4).
- §5.4's "counts every pin" does not cover a chain build's or free's in-place pins (note 1), nor a retry that pins new MapLeafs while it keeps emptied restart pins (G1).

The rework is **29 exact blocks** (RWD-1 to RWD-29, below), 6 replaced Acceptance lines and 23 added ones. One mechanical pass closes every finding, the wording ones included, so nothing rides in a later spec card. I applied all 29 to a scratch copy of d493188f's `docs/S-STORE.md` (each Old once at its turn; result sha256 `6197762c979457954dec16c78c813b2f0c66dce704a6691fa90d6f1c54981c14`). I re-read every changed paragraph and re-walked every test the blocks touch. The new Acceptance list passes there: 145 of 145 commands, `gate.sh` aside. Each of its 32 new or replaced lines fails on d493188f. The checker prints `158 rows; 225 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `56 new since 7.10 (...), 56 of them judged on a Phase A row; 47 invariants, 48 tests`, `OK`.

## Gate run

- **Extract.** A shallow `--no-local` clone of `nucleus/v3-scaffold`, scaffold id `1084de903b3031e750350f33b0cfd23fe6edb9a4`. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **Run.** `bash "$fg/fresh-gate.sh" d493188f09bf8bc4bd8946f12a815a4330895899` from `~/Workspace/neutron-v3`, with stdout and stderr captured separately:
  - `fresh-gate: commit d493188f09bf8bc4bd8946f12a815a4330895899`
  - `fresh-gate: base 1084de903b3031e750350f33b0cfd23fe6edb9a4`
  - `fresh-gate: checks passed (4816 files hashed in 1.50 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - stderr empty; 883 `ok` lines; 56 `test result: ok` (467 passed, 0 failed); every `STEP … EXIT=0` (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests); exit 0; literal last line `GATE EXIT=0`.
- `$fg` was removed afterwards. The worktree was at d493188f and clean before this verdict's commit.

## Bytes, scope and names

- **The pairs.** My own script parses the card's 97 headings into 115 Old/New pairs: the 13 riders, E1–E80 with E40a (99 pairs), then RW-F6-1, T1 and T2. It applies them in order to 1084de90's files, which are c6d03599's byte for byte (`86f012fb…ba43`, `ed2bd4e4…b3bd`). Every Old occurs once at its turn. The result equals d493188f's `docs/S-STORE.md` (`7f9ebf20ac3a77476f3ed2df172d393dca9031cb4d2e5d550e7f1bda272ee71b`) and `docs/C-T0-txn-protocol.md` (`96caa6cb73744c0fe03c4b7dee637fd6afc770c07bd3e9ce403d1ef8ad78a0c0`) byte for byte.
- **The date.** With `<date>` = 2026-11-15 the same run gives `ee929687fe1b7d0bb54ec3c096955004667ea07130be1791b2fa494241cb3cbd` and `20cc03e4645d922e1e07cca3eeb0257196d15808667b647f4796e27e8dc64cc3`, the author's dry-run hashes. The only differences from the branch are the four dates (S-STORE lines 3 and 1667, C-T0 lines 3 and 861), one character each.
- **The riders.** Every rider in the card is byte-identical to its verdict's block: RW-F1-1 to RW-F4-5 at 9abe5527 (and at c5414fa6), RW-F5-1 and RW-F6-1 at c5414fa6 and 60e71ed0, RW-F7-1 at 60e71ed0. F7's two Acceptance lines and its Changelog clause are verdict 3's.
- **Scope.** `git diff nucleus/v3-scaffold...HEAD`: `docs/C-T0-txn-protocol.md` and `docs/S-STORE.md`, the card's Touch only. The appendix is not applied (none of its New text, no ROLLBACK-REFUSES-READ-ERROR).
- **Names.** No mutant, test or invariant token of 7.13 or 8.6 is lost, in the whole file or in its body. My own §15/§15.1 parser gives 47 invariants; 215 → 225 mutants (the ten named); 152 → 158 rows; 190 → 201 mutants judged on a Phase A row with none lost; and 25 → 24 Phase-B-only, the difference being WAL-ERR-NOT-POISON. The cards that change are the relabel's A-7d → A-7f moves and WAL-ERR-NOT-POISON's added A-6c.
- **Acceptance.** In a scratch clone at d493188f, the card's 119 commands (`gate.sh` aside) all pass, and the checker prints the card's three lines.

## The implementer's findings: rulings

1. **`t_two_gen` (c), and (b): real defect.** "A crash before the boot checkpoint's step 5" admits a crash after step 4 has written meta `k+1`. The spec's crash model keeps unsynced writes: fence variant (ii) and `t_boot_durable_start` rely on that. So the next open selects meta `k+1`, and the hooked "page of image `k` that step 2 reads" is either its previous free set, where it opens without falling back, or a page that images `k` and `k+1` share, where it falls back to meta `k`. Either way "falls back to meta `k−1` with proof" fails on a correct build. The crash must come before step 4 (RWD-1, RWD-2). The boot checkpoint's evictions are in its step 2, so neither mutant loses its kill.
2. **The pool-floor tracker: real defect, in three parts.**
   - (i) §4.1's dirty-page MapLeaf pins are taken by whichever thread dirties a page, and every thread in the fixture dirties pages under up to `h + 1` MapLeafs per split. A tracker that counts them fires on a correct build (RWD-6).
   - (ii) The finding is right that later rounds need the next free LPIDs under non-resident MapLeafs. This gate found worse (G1). Under 7.14's text a retry "keeps the MapLeaf pins its restart took", and nothing makes it drop the emptied ones before it pins the MapLeafs of LPIDs it takes under other, resident MapLeafs. A correct build then holds `2h + 2` MapLeaf pins and trips the tracker. In normal operation it also exceeds §5.4's third term (`2h + 2 > h + 2`) and, with its path and new pages, the thread's whole count: `h + (h + 1) + 2(h + 1) = 4h + 3 > 3h + 6` for `h ≥ 4`. RWD-3 adds the rule the count needs. A retry that holds an emptied pin restarts instead of pinning more, and a restart drops the emptied pins before it pins another. That restart is charged to other allocations' LPIDs, as §5.6's bound already allows. With it an allocation never holds more than `n` such pins at once, and every hooked round restarts whatever the next MapLeafs' residency, so the fixture needs no more evicted MapLeafs.
   - (iii) "`2h + 2` by its second restart" holds only where that round's LPIDs lie under `h + 1` MapLeafs. §5.6's bound ("restarts at most once in all") does force a restart to pin all it needs at once, but layout still matters. By the third restart the mutant always exceeds `h + 2` (RWD-6).
3. **Who `threads` counts: real defect.** §5.4 counts the store's own threads, but the tests compute the minimum for "one, two and eight threads" and "four threads" only. A correct build with a checkpointer, evictor or background thread of its own refuses those pools with 53200. RWD-4, -7, -9, -10 and -27 make the count explicit: callers' threads plus the store's own, which the build reports, plus, under N10's cooperative mode, every task that can hold a pin while it waits.
4. **WAL-ERR-NOT-POISON at A-6c: real defect. The move into Phase A is right, but A-6c is the wrong card.** The poison mark of §9.2 item 1 is A-3c's code (PLAN A-3c, "the poison check at `try_latch`, gate and pin acquisition"), and A-3c is outside A-6c's closure, which holds no store call but the writer's. A writer that keeps its own sticky error meets every A-6c observable under the mutant (its waits and appends return the error, and nothing more reaches `wal/`), so the mutant cannot fail there on that conforming build. A-7c is the first card whose closure holds the writer, the mark and a call outside the writer. RWD-11 to -15 move the row there: `Needs` A-6c, A-3c, with a read made before any checkpoint or background job runs, which a mark-less writer cannot pass. The checker's placement changes to A-7c, and the Phase-B-only count stays 24. The alternative, an A-6c → A-3c edge, is the plan's to choose. It would need the row's Needs and the checker's Depends map changed too.
5. **`t_flip_chunk_race`'s "full" label: wording.** Variant *ended* ends `X` through S3 (E40a), so by §15.1's convention the form is "full except S3 …". RWD-16 changes no judgement: S3 is a listed stand-in, and CHUNK-SET-KEEPS-ENDED is applied to A-6d's real chunk path.
6. **Five mutants first judged at A-7f: not a placement defect; E56's wording is fixed.** PL-11's gate (check 5) accepted PREV-FIRST-WRONG, WAL-START-PREV-WRONG and WAL-START-PREV-CRC-WRONG being judged in Phase A only at A-7f, since A-7f's closure holds A-7c, whose code they mutate. WAL-START-PAIR-SPLIT-READ and BOOT-PAIR-UNSET are the same row's mutants on the same code, and S-GAMMA's checker accepted A-7d or A-7f for them. A mutant counts where it is applied to the real code it names (§15), and a card with no code of its own can apply it. They need not stay on A-7d. RWD-17 makes E56 say what A-7f runs, and the plan card must add the two to A-7f's list.
7. **§1 and §4.4's list: §1 is consistent, the list is incomplete (wording).** §1 says there is no previous image when the free set fails a read, and E11 agrees. But §4.4's items 1–4 no longer name every slot an open must not write. RWD-18 extends item 4. `t_two_gen` (c) already catches a build that misses it.
8. **FENCE-SKIP's late form passes (i): wording.** (ii) kills both forms. RWD-19 says so.
9. **E33's "whole in its own parity": wording, and wrong in its own case.** In the misplaced scenario meta `k` has only one copy in its own parity. RWD-20.
10. **E18's path bullet adds up to `h + 3`: wording.** The pack step's path stops at `P` (`h + 1` with `L` and `R`), and the cold page brings it to `h + 2`. RWD-8 states the arithmetic.
11. **"§5.4's quota" ×3: wording.** RWD-21 to -23.
12. **Case F: real fixture gap, and a wording point.** A build whose background evictor writes dirty pages proactively, however large the pool, can evict `r`. Case F's premise then fails and ROOT-PIN-MAP-FIRST passes it. RWD-24 requires no background evictor. RWD-25 says the test-only mode is a root-name read that holds no gate, not S4's gated sweep step, which "outside any gated operation" needs.
- **Note 1, chain build and free pins: real.** §5.4 claims to count every pin, but a chain build or free reads pages and MapLeafs in place under its leaf, and nothing bounds how many it keeps. RWD-8 bounds them: at most two chain pages and their MapLeafs at once, each released before a third, within the thread's three terms.
- **Note 2, the gate seam: real.** "A hook in the gate's shared acquire, which fires when an acquirer finds the gate held exclusively" has no point to fire on a plain blocking acquire, so the test cannot be written on every conforming build. RWD-5 requires A-3c's gate to offer the seam: try first, call the hook, then block.

## Design review

### (a) §5.4's minimum

Re-derived from §5–§6 (`h` = the height bound, the directory's included):

| Operation | Path | Reservation pages | MapLeafs | Total ≤ 3h+6 |
|---|---|---|---|---|
| Reader | ≤ 2 coupled + 1 cold | — | — | ✓ |
| Cascading insert | h (+1 cold) | h + 1 | h + 1 (with RWD-3) | 3h + 2 (+1) |
| CreateTree | h (directory) | h + 2 | h + 2 | 3h + 4 |
| Pack step / root collapse | h + 1 (path to `P`, `L`, `R`) | — | 1 | h + 2 |
| DropTree last step | h + 1 (directory leaf, root, emptied path) | — | ≤ h | 2h + 1 |
| Restart waiting on the gate | 1 cold | ≤ h + 2 kept | ≤ h + 2 kept | ✓ |
| Chain build / free | leaf + 2 chain pages | — | 2 | 5 (RWD-8) |
| Checkpointer, evictor | 1–2 frames, copies in `R_EPOCH` | — | dirty pins in `R_MAP` | ✓ |

- **Sufficient** for deadlock freedom once RWD-3 and RWD-8 bound the two pins the text left open.
- **The argument.** Every thread's pins fit its own `3(h + 2)`, latched or not. So whenever a thread below its count needs a frame, at least one frame outside `R_EPOCH` and outside `R_MAP`'s MapLeafs is unpinned. A gated operation past its first mutation can then evict it under its own hold, without waiting on the gate (§5.2). Traces A and B are real at 7.13 and closed here.
- **Size.** Not needlessly large for callers' threads: the tightest per-thread figure is CreateTree's `3h + 4` (+1 cold), against `3h + 6`. It over-counts the store's own threads, which need about three frames each but are charged `3h + 6`. That is about 18 frames (144 KiB) each at `h = 5`. A later per-role refinement could recover it.
- **Cost.** At 8 KiB pages and height 5 the per-thread floor goes from 7 frames (56 KiB) to 21 (168 KiB). At 16 callers' threads that is 0.875 MiB → 2.625 MiB, plus 168 KiB for each of the store's own threads (a checkpointer, an evictor and two jobs make about 3.3 MiB in all), plus `R_EPOCH` and `R_MAP`.
- **Who counts.** Every thread that can hold a pin while it waits, callers' and the store's own, and every such task under N10. The fix is finding 3 above.

### (b) The pin rule and §9.2's XX001

- **Page table first is right.** An eviction sets the entry before the frame leaves the page table, and a pin that sees the frame pins it, which keeps the eviction from dropping it. So no interleaving of a pin's two lookups with an eviction sees "no frame, slot 0" for a mapped page.
- **The other order is not workable.** It would need the LPID bitmap, under the free-set lock that a pin must not take (D-5).
- **§9.2.** The widened XX001 changes no behaviour: every XX001 the file names is one of its three kinds, and §9.2's poison list goes by place.

### (c) The fence, and its trade in plain words

**What it does, in plain words.** A read-write open that could not read a meta place wipes that place, and syncs, before it writes anything. Without that, a meta it could not read might come back later and be opened over pages this open reused (Trace 2), or tie with this open's rewritten checkpoint (Trace 1).

**What it costs.** It destroys a copy that may only have been briefly unreadable.
- **A newer meta.** If the copy was of a meta newer than the one the open chose, nothing is lost that the open had not already given up: the open recovers the same committed state from the older meta and the WAL, which I-TWO-GEN keeps intact. Its own writes, fenced or not, make the newer image unsafe to open later.
- **The chosen meta's other copy.** If the copy was the other copy of the meta the open chose, that meta runs on one copy until its parity's next checkpoint rewrites both. In that window a second fault on the surviving copy can strand the store: an XX001 in both modes, though no data is lost and the WAL is intact. The model shows it with the boot checkpoint's first copy misdirected onto that copy, then a crash.

**Every crash point walked.** From the open's first read to the boot checkpoint's sync, with the retire write, pass-2 evictions and the explicit rollback:
- Refusals in steps 1, 3, 3a and 3b come before any write, so they fence nothing, and a transiently unreadable newest meta is still there for the next open.
- A crash inside the fence leaves only zeros and no other write.
- A crash after the fence and an eviction is Trace 2, closed.
- A crash between the boot checkpoint's two meta writes is Trace 1, closed.
- A fall-back's retire write comes after the fence, and a refused explicit rollback writes no more than the fence and the evictions. The text states each of these outcomes.

**Can it lose acked commits, or force a fall-back?** It never loses an acked commit. It can make a later open select an older meta than the unfenced design would, for instance after a crash right after the fence, but only a meta inside the two generations, whose image and WAL span are intact (I-TWO-GEN), so that open recovers the same committed state. What it loses is that older meta's own fall-back, whose previous meta the fence zeroed; that matters only with a further fault.

**Preconditions.** The design evaluation's preconditions (`header-durability-design-eval.md` §7 Part 3; `header-design-verify.md` §2.4) are all in the text: before any other write; every copy whose read failed, a superset of the abandoned meta's; never on a read-only or refused open; made durable with `sync_all`.

**Ruling.** Accept zeroing (P-7). RWD-26 and RWD-29 write the one-copy window into the residual, so the owner sees it.

### (d) The unread previous free set and 58030

The card's middle course is right. S-ALPHA's third gate recommended "end the open with 58030" only "unless the candidate protection is adopted": with it, "carrying on would also be safe".
- **Why it is safe.** The coarse protection covers every slot image `k−1` can reference, since all of them lie below its `slot_count`. So carrying on spends no protection. The pending list releases at the first step 6, and the file grows by at most one recovery's writes.
- **What refusing would cost.** Ending the open would let one unreadable sector in a structure the next checkpoint discards block every open, read-only included.
- **58030 and XX001.** 58030 is right where that read error is what stops a fall-back (a later open may read it); XX001 is right for an integrity failure.

### (e) The misplaced-copy rule: the model

My own model (`~/.cache/nv3-review/gate-S-DELTA/model/`, not the S-ALPHA gates'):
- **The space.** Four meta places plus page 0; checkpoints 11–13 after a clean base; every crash point (before step 4, between its writes, after it, complete); both write orders. Faults are LOST, TORN or MISDIRECTED meta writes (to any other place), read errors and page 0. Safe metas are those inside two generations, plus an in-flight meta with a copy.

**Single open, 7.14 against 7.13:**

| Faults | 7.14 opens | 7.14 refuses | 7.13 refuses | Unsafe opens |
|---|---|---|---|---|
| 0 | 24 | 0 | 0 | 0 |
| 1 | 588 | 0 | 12 (D5's trace) | 0 |
| 2 | 7,104 | 84 (64 where a safe meta still has a copy) | 448 | 0 |
| 3 | — | — | — | 0 (both rules) |
| 4 | — | — | — | 188 new under 7.14 |

- At one fault, 7.14 never refuses an intact store; 7.13 refused 12, the in-flight misdirect and crash. That is D5's trace, which the card counts as two faults by counting the crash. It is gone.
- At two faults, 7.14's refusals are all "misplaced two above", a strict subset of 7.13's.
- The 188 new unsafe opens at four faults are exactly the card's stated residual (one copy of `k+1` misdirected, its other copy lost, both copies of `k+2` lost), and each is detected at open or at a read.
- **Silent loss.** No commit acked under an honoured sync is lost silently at any fault count up to three.

**Two opens, the fence:** a history, then open A with transient read errors, A's writes and crash points, then open B.
- **One fault:** fence, all OK; without the fence, 3 unsafe (Trace 2).
- **Two faults:** fence, 27 refusals and 0 unsafe; without the fence, 31 refusals (22 equal-`boot_id` ties, the Trace 1 class) and 115 unsafe.
- **The cost.** The fence adds the 18 two-fault refusals of G2.
- **Against 7.13.** Against 7.13 itself, 7.14 (fence and F5 together) is never worse at up to three faults.

### (f) The relabel

- **Against PLAN.** The ten rows match PLAN §9 A-7f's list, and both splits keep DROP-ENTRY-SPLIT and PREV-HOP-WRONG on A-7d. Each moved row needs A-7d, and every Need lies in A-7f's closure (the checker confirms).
- **Beyond the plan's list.** It also moves WAL-START-PAIR-SPLIT-READ, BOOT-PAIR-UNSET and SEAL-STALE-HEAD's re-run, consistently with the split.
- **The five first judged at A-7f.** This is acceptable (finding 6). With E56's wording fixed, it needs no spec change.
- **P-6.** I recommend the plan card keep the two I-MAP-GROW full forms on A-7d, so that no further spec card is needed.

## Tests on their owning rows, with the lock survey

At d493188f:

| Test | Row | Status |
|---|---|---|
| `t_two_gen` (b), (c) | A-7d | cannot pass for a crash after step 4 |
| pool-floor variant | A-4a | cannot pass on a build that counts its own threads; tracker fires (§4.1 pins, G1); release seam missing |
| `t_pool_minimum` | A-3b | cannot pass on a build with its own threads |
| WAL-ERR-NOT-POISON | A-6c | cannot fail on a sticky writer |
| ROOT-PIN-MAP-FIRST | A-7b | not reached where a background evictor writes `r` |
| every other new or changed form | A-2, A-7b, A-7c, A-7d | passable, its mutant caught (see the Mutants line) |

The other forms: `t_meta_copies` table, fence, tie and in-flight; cases C and E; the walk-poison variant; `t_fallback`'s 58030 form; `t_wal_chain_crash`'s hop step; the riders' tests, which S-GAMMA's gates walked.

**Lock survey**, re-walked after the rework:

| Test | What each party holds | Fires on every conforming build |
|---|---|---|
| Pool-floor variant, first restart | Each inserting thread: its pins only. S1: the gate exclusively, no shared holder left. | Yes. The inserts' acquires fire the seam (RWD-5), and S1 releases after both. |
| Pool-floor variant, each hooked retry | Inserting thread: the gate shared, its pins and its own tree's latches, not the free-set lock. Hook's thread: the gate shared (no flip pending), tree 3's latches, the free-set lock, and frames within its count (its evictions under its own hold). | Yes. Nothing waits on a lock its waitee holds, and the retry then restarts by RWD-3. |
| `t_root_name_reuse` C, E, F | A: nothing between its read and its pin. | Yes. No background evictor runs in F (RWD-24). |
| Walk-poison variant | One thread, the test's own step. | Deterministic. |
| Fence, `t_two_gen` (b) and (c), unread, misplaced, in-flight | One open thread, hooks on its own `Env` calls. | Deterministic. |
| WAL variant at A-7c | The writer's own failing `sync_data`, then the test thread's read. | Yes. The read comes after a waiter has returned the error, and no hook waits on an interleaving. |
| `t_flip_chunk_race`, `t_wal_chain_crash` | Unchanged since S-GAMMA. | Yes, by S-GAMMA-F7 (verdict 3's survey). |

## The rework: 29 blocks

Apply on `card/v3-S-DELTA` on top of d493188f, to `docs/S-STORE.md`, in order RWD-1 to RWD-29. Each Old occurs once at its turn (checked by script), and nothing else changes; `docs/C-T0-txn-protocol.md` is untouched. Draft numbers stay 7.14 and 8.7. The fences here are four tildes. The result's sha256 is `6197762c979457954dec16c78c813b2f0c66dce704a6691fa90d6f1c54981c14`.

### RWD-1. S-STORE §4.4, `t_two_gen` (b): the crash comes before the boot checkpoint's step 4 (finding 1).
File: docs/S-STORE.md

Old:
~~~~text
a crash before the boot checkpoint's step 5, then a post-sync fault on a page of image `k` that the next open reads at step 2
~~~~
New:
~~~~text
a crash before the boot checkpoint's step 4, so that no copy of its meta is written and the next open selects meta `k` again, then a post-sync fault on a page of image `k` that the next open reads at step 2
~~~~

### RWD-2. S-STORE §4.4, `t_two_gen` (c): the same (finding 1).
File: docs/S-STORE.md

Old:
~~~~text
and a crash before the boot checkpoint's step 5: the open runs, and a write hook asserts
~~~~
New:
~~~~text
and a crash before the boot checkpoint's step 4, so that no copy of its meta is written and the next open selects meta `k` again: the open runs, and a write hook asserts
~~~~

### RWD-3. S-STORE §5.6 "Restart": a pin of an emptied MapLeaf is dropped before another is pinned, and a retry that holds one restarts (finding 2).
File: docs/S-STORE.md

Old:
~~~~text
The arm makes progress because a pinned MapLeaf stays resident and a free only adds LPIDs: the retry restarts again only if other allocations took free LPIDs under its pins meanwhile, leaving fewer than it needs, and the rest lie under non-resident MapLeafs. Each such restart pins at least one more MapLeaf and drops the pin of any MapLeaf left with no free LPID, so an operation that takes `n` LPIDs holds at most `n` such pins, within §5.4's third per-thread term, not `R_MAP` (mutant RESTART-KEEPS-EMPTY-MAPLEAF, §5.4).
~~~~
New:
~~~~text
The arm makes progress because a pinned MapLeaf stays resident and a free only adds LPIDs: the retry restarts again only if other allocations took free LPIDs under its pins meanwhile, leaving fewer than it needs, and either the rest lie under non-resident MapLeafs or one of its pins has no free LPID left. A retry that holds the pin of a MapLeaf left with no free LPID takes no LPID under a MapLeaf it does not hold pinned: it restarts instead. Each such restart drops the pin of every MapLeaf left with no free LPID before it pins another, and pins at least one more MapLeaf, so an operation that takes `n` LPIDs never holds more than `n` such pins at once, within §5.4's third per-thread term, not `R_MAP` (mutant RESTART-KEEPS-EMPTY-MAPLEAF, §5.4).
~~~~

### RWD-4. S-STORE §5.4, the pool-floor variant: the build's own threads (finding 3).
File: docs/S-STORE.md

Old:
~~~~text
a pool at exactly the minimum for the fixture's four threads at its height `h`, the two inserting threads, the hook's thread and S1's flip thread (`threads` counts every thread that pins frames),
~~~~
New:
~~~~text
a pool at exactly the minimum, at the fixture's height `h`, for its four threads, the two inserting threads, the hook's thread and S1's flip thread, together with the build's own threads that pin frames, if it has any (`threads` counts every thread that pins frames),
~~~~

### RWD-5. S-STORE §5.4, the pool-floor variant: the gate's test seam (the implementer's second note).
File: docs/S-STORE.md

Old:
~~~~text
S1 releases the gate once a hook in the gate's shared acquire, which fires when an acquirer finds the gate held exclusively, has fired for both.
~~~~
New:
~~~~text
S1 releases the gate once a hook in the gate's shared acquire, which fires when an acquirer finds the gate held exclusively, has fired for both; A-3c's gate offers that hook as a test seam: its shared acquire tries the gate first and, when it finds the gate held exclusively, calls the hook before it blocks.
~~~~

### RWD-6. S-STORE §5.4, the pool-floor variant: what the tracker counts, and the kill (finding 2).
File: docs/S-STORE.md

Old:
~~~~text
A pin tracker, a hook on the pool's pin and unpin, asserts at every pin that no thread holds more than `h + 2` MapLeaf pins or more than `2 × (h + 2)` tree-page pins, and both splits finish within the restart bound (§5.6). RESTART-KEEPS-EMPTY-MAPLEAF fails it through the tracker: by its second restart a thread holds `2h + 2` MapLeaf pins.
~~~~
New:
~~~~text
A pin tracker, a hook on the pool's pin and unpin, counts the pins each thread takes for its own operations (their paths, a restart's cold page, their reservations' pages and MapLeafs), not the pin a MapLeaf holds for each dirty page it maps (§4.1, `R_MAP`), whichever thread dirtied the page, nor an epoch-copy frame (`R_EPOCH`); it asserts at every pin that no thread holds more than `h + 2` MapLeaf pins or more than `2 × (h + 2)` tree-page pins, and both splits finish within the restart bound (§5.6). RESTART-KEEPS-EMPTY-MAPLEAF fails it through the tracker: after each hooked round the retry holds only emptied pins and restarts (§5.6), and each such restart keeps them and pins at least one more MapLeaf, so that by its third restart a thread holds more than `h + 2` MapLeaf pins (`2h + 2` at its second where the round's `h + 1` LPIDs lie under `h + 1` MapLeafs).
~~~~

### RWD-7. S-STORE §5.4, Minimum size: who `threads` counts (finding 3).
File: docs/S-STORE.md

Old:
~~~~text
where `threads` counts every thread that pins frames, the checkpointer and the store's background jobs included, and `max tree height`
~~~~
New:
~~~~text
where `threads` counts every thread that pins frames, the checkpointer and the store's background jobs included, the store's own threads as well as its callers', and under N10's cooperative mode every task that can hold a pin while it waits, and `max tree height`
~~~~

### RWD-8. S-STORE §5.4, the path term: its arithmetic, and a chain build's or free's pins (finding 10, the implementer's first note).
File: docs/S-STORE.md

Old:
~~~~text
- **the path:** the latch-coupled path plus the two siblings of a pack step, and the one page a restart pinned cold (§5.3, §5.6 "Restart"), which its retry's descent reaches or releases;
~~~~
New:
~~~~text
- **the path:** the latch-coupled path, at most `h` pages on a tree of `h` levels, or a pack step's path down to `P` with `L` and `R` below it, at most `h + 1`, plus the one page a restart pinned cold (§5.3, §5.6 "Restart"), which its retry's descent reaches or releases: at most `h + 2`. A chain build or free (§6.7), which holds only its leaf, reads its chain's pages and their MapLeafs in place, holding at most two chain pages and their two MapLeafs at once and releasing each before it pins a third, so its pins stay within the thread's three terms;
~~~~

### RWD-9. S-STORE §5.4, `t_pool_minimum`: the thread counts (finding 3).
File: docs/S-STORE.md

Old:
~~~~text
for one, two and eight threads, at every allowed page size and the store's tree-height bound, the test computes the minimum from the formula above with the build's `R_EPOCH` and `R_MAP`, not by asking the build for it;
~~~~
New:
~~~~text
for one, two and eight callers' threads, to each of which it adds the build's own threads that pin frames (its checkpointer, evictors and background jobs, as the build reports them), at every allowed page size and the store's tree-height bound, the test computes the minimum from the formula above with the build's `R_EPOCH` and `R_MAP`, not by asking the build for it;
~~~~

### RWD-10. S-STORE §15.1, `t_pool_minimum`'s row: the thread counts (finding 3).
File: docs/S-STORE.md

Old:
~~~~text
| full: a pool one frame below the minimum and one at it, for one, two and eight threads at every allowed page size | POOL-MIN-PATH-ONLY |
~~~~
New:
~~~~text
| full: a pool one frame below the minimum and one at it, for one, two and eight callers' threads with the build's own added, at every allowed page size | POOL-MIN-PATH-ONLY |
~~~~

### RWD-11. S-STORE §15, I-FAIL-STOP: WAL-ERR-NOT-POISON is judged at A-7c (finding 4).
File: docs/S-STORE.md

Old:
~~~~text
WAL-ERR-NOT-POISON (an error on the WAL path does not poison the store) fails it: at A-6c on the writer's own waits, through a test `Env` whose `sync_data` fails, where every waiter on `written_end` or `durable_end` and every later append returns the error and a write hook sees nothing more written to `wal/`; and at B-1c with commit waiters (§15.1).
~~~~
New:
~~~~text
WAL-ERR-NOT-POISON (an error on the WAL path does not poison the store) fails it: at A-7c, the first card whose Depends closure holds both the group-commit writer (A-6c) and the poison mark (A-3c), through a test `Env` whose `sync_data` fails under the writer, where every waiter on `written_end` or `durable_end`, every later append and a later call into the store that is not the writer's, a read made before any checkpoint or background job runs, return the error and a write hook sees nothing more written, so that a writer that keeps its own error but leaves the mark unset fails it through that read; and at B-1c with commit waiters (§15.1).
~~~~

### RWD-12. S-STORE §15, I-FAIL-STOP's Pinned-by cell (finding 4).
File: docs/S-STORE.md

Old:
~~~~text
A-6c (the WAL variant on the group-commit writer's own waits); 
~~~~
New:
~~~~text
A-7c (the WAL variant: the writer's waits and a later read outside the writer); 
~~~~

### RWD-13. S-STORE §15.1, the WAL variant's row moves from A-6c to A-7c (finding 4).
File: docs/S-STORE.md

Old:
~~~~text
| `t_gated_xx001_poisons` | I-FAIL-STOP | A-6c | — | the WAL variant on the group-commit writer's own waits: a test `Env` whose `sync_data` fails with 58030; every waiter on `written_end` or `durable_end`, and every later append, returns the error, and a write hook sees no write, sync or rename on `wal/` after the failure | WAL-ERR-NOT-POISON |
~~~~
New:
~~~~text
| `t_gated_xx001_poisons` | I-FAIL-STOP | A-7c | A-6c, A-3c | the WAL variant on the store: a test `Env` whose `sync_data` fails with 58030 under the group-commit writer; every waiter on `written_end` or `durable_end`, every later append and a read made before any checkpoint or background job runs return the error, and a write hook sees no write, sync or rename on `wal/` and no data-file write after the failure | WAL-ERR-NOT-POISON |
~~~~

### RWD-14. S-STORE status line (finding 4).
File: docs/S-STORE.md

Old:
~~~~text
the stand-ins tidies, A-6c's judgement of WAL-ERR-NOT-POISON, and ten §15.1 rows relabelled to A-7f
~~~~
New:
~~~~text
the stand-ins tidies, A-7c's judgement of WAL-ERR-NOT-POISON, and ten §15.1 rows relabelled to A-7f
~~~~

### RWD-15. S-STORE Changelog 7.14, the WAL-ERR-NOT-POISON bullet (finding 4).
File: docs/S-STORE.md

Old:
~~~~text
  - **WAL-ERR-NOT-POISON at A-6c** (PL-11-F2 (a), S-15-STANDINS-F3 (f)): §15.1 judges it on the group-commit writer's own waits, as the plan's A-6c row claims, so it leaves the Phase-B-only list.
~~~~
New:
~~~~text
  - **WAL-ERR-NOT-POISON at A-7c** (PL-11-F2 (a), S-15-STANDINS-F3 (f)): §15.1 judges it at A-7c, the first card whose Depends closure holds both the group-commit writer (A-6c) and the poison mark (A-3c), through the writer's waits and a later read outside the writer, so it leaves the Phase-B-only list. At A-6c, which the plan's A-6c row names, a writer that keeps its own error would pass the mutant.
~~~~

### RWD-16. S-STORE §15.1, `t_flip_chunk_race`'s row: its form names S3 (finding 5).
File: docs/S-STORE.md

Old:
~~~~text
| A-7d | A-7c, A-6d, A-1b | full: a flip that races a new chunked transaction's first `Chunk`,
~~~~
New:
~~~~text
| A-7d | A-7c, A-6d, A-1b | full except S3 for the end of `X` in variant *ended* (§15 S3): a flip that races a new chunked transaction's first `Chunk`,
~~~~

### RWD-17. S-STORE §15.1, the lead-in: what A-7f runs (finding 6).
File: docs/S-STORE.md

Old:
~~~~text
A-7f, which the plan split from A-7d (card PL-11), runs the full forms of tests whose earlier forms ran on stand-ins and adds no code, so each of its rows needs A-7d, whose recovery those forms run.
~~~~
New:
~~~~text
A-7f, which the plan split from A-7d (card PL-11), adds no code: it runs the full forms of tests whose earlier forms ran on stand-ins, and the half of `t_wal_chain_crash` that judges A-7c's and A-6c's mutants through recovery, five of them there first, so each of its rows needs A-7d, whose recovery those forms run.
~~~~

### RWD-18. S-STORE §4.4, the slots that may not be written: item 4 names the protection of an unread previous free set (finding 7).
File: docs/S-STORE.md

Old:
~~~~text
4. the previous image (§1): image `k−1` while meta `k` is the durable image, until step 6 of checkpoint `k+1` (I-TWO-GEN).
~~~~
New:
~~~~text
4. the previous image (§1): image `k−1` while meta `k` is the durable image, until step 6 of checkpoint `k+1` (I-TWO-GEN); or, after an open from meta `k` that could not read meta `k−1`'s free set, every slot below meta `k−1`'s `slot_count` that image `k` does not reference, until the first step 6 after that open (§14 step 2).
~~~~

### RWD-19. S-STORE §2.2, FENCE-SKIP: which form each fence variant fails (finding 8).
File: docs/S-STORE.md

Old:
~~~~text
The fence variants fail: in (i) the stale meta `k` sits beside the new one with an equal `boot_id`, and the next open gives XX001; in (ii) the open after the crash selects meta `k+1`.
~~~~
New:
~~~~text
The fence variants fail: (i) its form that never zeroes, since the stale meta `k` sits beside the new one with an equal `boot_id` and the next open gives XX001, while a fence made after the first eviction but before the boot checkpoint passes (i); (ii) both forms, since the open after the crash selects meta `k+1`.
~~~~

### RWD-20. S-STORE §14 step 4a, Retiring meta `k`: what a crash between the two writes leaves (finding 9).
File: docs/S-STORE.md

Old:
~~~~text
The misplaced copies go first, so that a crash between the two writes leaves meta `k` either whole in its own parity, to be decided again, or in no place at all.
~~~~
New:
~~~~text
The misplaced copies go first, so that a crash between the two writes leaves meta `k` either in the places of its own parity that held it at the open, to be decided again, or in no place at all.
~~~~

### RWD-21. S-STORE §6.7, Building: "§5.4's quota" (finding 11).
File: docs/S-STORE.md

Old:
~~~~text
may need more MapLeafs than §5.4's quota pins.
~~~~
New:
~~~~text
may need more MapLeafs than §5.4's per-thread count allows.
~~~~

### RWD-22. S-STORE §6.7, Freeing: "§5.4's quota" (finding 11).
File: docs/S-STORE.md

Old:
~~~~text
and a chain may be longer than §5.4's quota.
~~~~
New:
~~~~text
and a chain may be longer than §5.4's per-thread count.
~~~~

### RWD-23. S-STORE §19 T6: "§5.4's quota" (finding 11).
File: docs/S-STORE.md

Old:
~~~~text
which must keep I-LATCH and §5.4's quota, the constraints
~~~~
New:
~~~~text
which must keep I-LATCH and §5.4's per-thread count, the constraints
~~~~

### RWD-24. S-STORE §15 I-LINK-ATOMIC, `t_root_name_reuse` case F: no background evictor (finding 12).
File: docs/S-STORE.md

Old:
~~~~text
(the pool holds every page, and no flip is pending);
~~~~
New:
~~~~text
(the pool holds every page, no background evictor runs, and no flip is pending);
~~~~

### RWD-25. S-STORE §15 I-LINK-ATOMIC, `t_root_name_reuse`: what the test-only mode is (finding 12).
File: docs/S-STORE.md

Old:
~~~~text
as the test-only stand-in in the other, so no XX001 of A's poisons the store (§9.2);
~~~~
New:
~~~~text
as the test-only stand-in in the other, a root-name read that accepts `dropping` and holds no gate (not S4's sweep step), so no XX001 of A's poisons the store (§9.2);
~~~~

### RWD-26. S-STORE §14 step 1, the fence's residual: a fenced copy of the selected meta (this gate's model).
File: docs/S-STORE.md

Old:
~~~~text
   - *Residual.* A fence write that the device loses after its sync brings the unreadable copy back: the reused slots then fail I-INTEGRITY when read (§3.3), and an equal-`boot_id` tie fails the open with XX001. Neither is silent.
~~~~
New:
~~~~text
   - *Residual.* A fence write that the device loses after its sync brings the unreadable copy back: the reused slots then fail I-INTEGRITY when read (§3.3), and an equal-`boot_id` tie fails the open with XX001. Neither is silent. A fenced place of the selected meta's own parity leaves that meta one copy until its parity's next checkpoint rewrites both: a second fault on that copy meanwhile, such as the boot checkpoint's first copy misdirected onto it with a crash before its second copy, leaves no copy of it and a misplaced copy two `ckpt_id`s above the meta before it, and the next open fails with XX001 in both modes (two faults; draft 7.13 refused the same state).
~~~~

### RWD-27. S-STORE Changelog 7.14, the minimum-pool bullet (findings 2, 3, the first note).
File: docs/S-STORE.md

Old:
~~~~text
`threads` counts the checkpointer and background jobs, and a restart keeps no pin but its cold page and its reservation's (§5.6).
~~~~
New:
~~~~text
`threads` counts every thread that pins frames, the store's own (the checkpointer, evictors and background jobs) included, and under N10 every task that can hold a pin while it waits. A restart keeps no pin but its cold page and its reservation's, and drops an emptied MapLeaf's pin before it pins another; a retry that holds such a pin restarts rather than pin a MapLeaf it does not hold (§5.6); a chain build or free holds at most two chain pages and their MapLeafs at once (§5.4).
~~~~

### RWD-28. S-STORE Changelog 7.14, the S-ALPHA-F2 bullet (findings 1, 7).
File: docs/S-STORE.md

Old:
~~~~text
`t_two_gen` gains (c), and `t_fallback`'s unread variant its 58030 form.
~~~~
New:
~~~~text
`t_two_gen` gains (c), whose crash, like (b)'s now, comes before the boot checkpoint's step 4, so that the next open selects meta `k` again; `t_fallback`'s unread variant gains its 58030 form; and §4.4's list of slots that may not be written names the protection (item 4).
~~~~

### RWD-29. S-STORE Changelog 7.14, the S-ALPHA-F1 bullet (this gate's model).
File: docs/S-STORE.md

Old:
~~~~text
`t_meta_copies` gains two fence variants, and its tie variant now loses the fence's writes too. Mutant FENCE-SKIP.
~~~~
New:
~~~~text
`t_meta_copies` gains two fence variants, and its tie variant now loses the fence's writes too. Mutant FENCE-SKIP. A fenced copy of the meta the open selected leaves that meta one copy until its parity's next checkpoint (§14 step 1, Residual).
~~~~

## Acceptance changes

Six lines of the card's Acceptance list are replaced (numbered as in the card now: 6, 16, 37, 38, 93, 118), and 23 lines are inserted before the file-list line (now 119). The list then has 146 commands, with the file-list line 145 and `gate.sh` last. No command contains a backtick. On the reworked scratch copy the 145 commands before `gate.sh` pass, and the checker prints `OK`. On d493188f, each of the 32 new or replaced lines fails.

**Line 6** (the path bullet) becomes two lines:

~~~~text
- `cd nucleus/v3 && grep -n '^- \*\*the path:\*\* the latch-coupled path, at most .h. pages on a tree of .h. levels, or a pack step.s path down to .P. with .L. and .R. below it, at most .h + 1., plus the one page a restart pinned cold' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'A chain build or free (§6\.7), which holds only its leaf, reads its chain.s pages and their MapLeafs in place, holding at most two chain pages and their two MapLeafs at once and releasing each before it pins a third' docs/S-STORE.md`
~~~~

**Line 16** (the tracker) becomes two lines:

~~~~text
- `cd nucleus/v3 && grep -n 'A pin tracker, a hook on the pool.s pin and unpin, counts the pins each thread takes for its own operations (their paths, a restart.s cold page, their reservations. pages and MapLeafs), not the pin a MapLeaf holds for each dirty page it maps (§4\.1, .R_MAP.), whichever thread dirtied the page, nor an epoch-copy frame (.R_EPOCH.); it asserts at every pin that no thread holds more than .h + 2. MapLeaf pins or more than .2 × (h + 2). tree-page pins' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'after each hooked round the retry holds only emptied pins and restarts (§5\.6), and each such restart keeps them and pins at least one more MapLeaf, so that by its third restart a thread holds more than .h + 2. MapLeaf pins' docs/S-STORE.md`
~~~~

**Line 37** (I-FAIL-STOP's row) becomes:

~~~~text
- `cd nucleus/v3 && sed -n '/^## 15\. /,/^### 15\.1 /p' docs/S-STORE.md | grep '^| I-FAIL-STOP | ' | grep -q 'fails it: at A-7c, the first card whose Depends closure holds both the group-commit writer (A-6c) and the poison mark (A-3c).*A-7c (the WAL variant: the writer.s waits and a later read outside the writer).*A-7d (the drop walk.s XX001: .t_root_name_reuse..s walk-poison variant) | .*WAL-ERR-NOT-POISON, WALK-XX001-NO-POISON |$'`
~~~~

**Line 38** (the WAL variant's §15.1 row) becomes two lines, the second a negative grep:

~~~~text
- `cd nucleus/v3 && sed -n '/^### 15\.1 /,/^## 16\. /p' docs/S-STORE.md | grep -c '^| .t_gated_xx001_poisons. | I-FAIL-STOP | A-7c | A-6c, A-3c | the WAL variant on the store: .* | WAL-ERR-NOT-POISON |$' | grep -qx 1`
- `cd nucleus/v3 && ! sed -n '4,/^## Changelog/p' docs/S-STORE.md | grep -n 'at A-6c on the writer.s own waits\|I-FAIL-STOP | A-6c |\|A-6c (the WAL variant on the group-commit writer'`
~~~~

**Line 93** (§15.1's lead-in) becomes:

~~~~text
- `cd nucleus/v3 && grep -n 'A-7f, which the plan split from A-7d (card PL-11), adds no code: it runs the full forms of tests whose earlier forms ran on stand-ins, and the half of .t_wal_chain_crash. that judges A-7c.s and A-6c.s mutants through recovery, five of them there first' docs/S-STORE.md`
~~~~

**Line 118** (the checker) becomes the same command with its one occurrence of `('WAL-ERR-NOT-POISON','A-6c')` replaced by `('WAL-ERR-NOT-POISON','A-7c')`; in full:

~~~~text
- `cd nucleus/v3 && python3 -c "import re,sys,subprocess as sp;D={'A-0':'','A-1a':'','A-1b-0':'A-1a','A-1b':'A-1a A-1b-0','A-2':'A-0','A-3a':'A-2','A-4c0':'A-0','A-3b':'A-3a A-4c0','A-3c':'A-3b A-4c0','A-4a':'A-3c','A-4b':'A-4a','A-7a':'A-3c','A-4c':'A-4a A-3c A-4c0 A-4b A-7a','A-4e':'A-4b A-4c','A-4d':'A-4e','A-5':'A-4a A-3c','A-6a':'A-1a','A-6b':'A-6a A-1b','A-6c':'A-6b','A-6d':'A-6c','A-7b':'A-7a A-4b A-5 A-4c0','A-7c':'A-7b A-6c A-1b','A-7d':'A-7c A-6d A-8 A-1b','A-7e':'A-6c A-7c','A-7f':'A-7d A-8 A-1b','A-8':'A-4b A-7b','A-8a':'A-1b A-7d A-8'};C={k:set(v.split()) for k,v in D.items()};[C[k].update(*[C[d] for d in list(C[k])]) for i in range(40) for k in C];Bo='B-1a B-1b B-1c B-1d B-2a B-2b B-2c B-2d B-2e B-3 B-4a B-4b B-4c B-4d B-4e B-5 B-6 B-7 B-8 R-1'.split();B=set(Bo);I=lambda s:re.findall(r'[ABR]-[0-9][0-9a-z-]*',s);t=open('docs/S-STORE.md').read();o=sp.run(['git','show','30860cf1:nucleus/v3/docs/S-STORE.md'],capture_output=True,text=True).stdout;V=lambda x:[[y.strip() for y in l.strip().strip('|').split('|')] for l in x[x.index('## 15. '):x.index('## 16. ')].split('### 15.1 ')[0].splitlines() if l.startswith('| I-')];a=t.index('## 15. ');b=t.index('### 15.1 ');c=t.index('## 16. ');R=[[x.strip() for x in l.strip().strip('|').split('|')] for l in t[b:c].splitlines() if l.startswith('| ') and not l.startswith('| Test |')];E=['cycle at %s'%k for k in C if k in C[k]];E+=['short row: %s'%r[0] for r in R if len(r)<6];R=[r for r in R if len(r)>=6];E+=['bad card %s for %s'%(r[2],r[0]) for r in R if r[2] not in C and r[2] not in B];E+=['%s on %s needs %s, not in its Depends closure'%(r[0],r[2],n) for r in R if r[2] in C for n in I(r[3]) if n not in C[r[2]]];E+=['%s needs unknown card %s'%(r[0],n) for r in R for n in I(r[3]) if n not in C and n not in B];N=set(re.findall(r'\bt_[a-z0-9_]+',t[a:b]));P=set(re.findall(r'\bt_[a-z0-9_]+',' '.join(r[0] for r in R)));E+=['no 15.1 row for %s'%x for x in sorted(N-P)];E+=['15.1 names a test not in 15: %s'%x for x in sorted(P-N)];Q=set('t_gated_xx001_poisons t_client_read_xx001_no_poison t_sweep_step_shape t_prune_split_shape t_create_every_cut t_load_every_cut'.split());E+=['no Phase A row for %s'%x for x in sorted(N-Q) if not any(x in r[0] and r[2] in C for r in R)];M=lambda W:[m.strip() for r in W for m in r[3].split(',')];O=V(o);T15=V(t);N10=set(M(T15));N78=set(M(O));NA=set('REPLICA-SKIP COPY-INDEX-OFF TIE-NO-BOOT-ID TIE-EQUAL-OPENS MISPLACED-SHADOWS IDENTITY-SKIP PAGE0-REQUIRED REPLICA-SLOT-FREE FREE-ONE-GEN RECYCLE-ONE-GEN OPEN-PROTECTS-ONE-GEN FALLBACK-OFF FALLBACK-NO-PROOF FALLBACK-TWO-GEN RETIRE-SKIP RETIRE-ONE-COPY ROLLBACK-SKIPS-PREV-CHECKS DEP-CHECK-SKIP DEP-BY-POSITION DEP-FIRST-ONLY DEP-CHECK-LATE DEP-ID-STALE DEP-SALVAGE-PAST-CUT START-SEG-AS-END SEG0-LAZY START-SEG-NOT-ENSURED RECYCLE-UNORDERED MAPINNER-CREATE-UNCONDITIONAL LEVEL-GROW-UNCONDITIONAL MAP-ROOT-CURRENT MAP-POSITION-FROM-ROOT'.split());NS=set('TRUNC-NOT-DURABLE BOOT-DURABLE-AT-VALID-END WAL-START-PAIR-SPLIT-READ BOOT-PAIR-UNSET SEAL-STALE-HEAD SPLICE-MARKER-PAST-CUT SPLICE-CUT-SAME-START-KEEP DROP-NO-GCW DROP-GCW-AT-WALK DROP-GCW-AT-TS REPLAY-DROP-NO-GCW'.split());NG=set('START-LSN-SPLIT-READ APPEND-SECTION-IO REPLAY-DROP-AT-TS DROP-OP-W-ZERO'.split());ND=set('POOL-MIN-PATH-ONLY RESTART-KEEPS-EMPTY-MAPLEAF ROOT-PIN-MAP-FIRST WALK-XX001-NO-POISON FENCE-SKIP UNREAD-PROTECTS-NOTHING MISPLACED-NEXT-REFUSES MISPLACED-IGNORED'.split());NR=set('CHUNK-SET-KEEPS-ENDED DROP-MARK-AT-PUBLISHED-W'.split());NEW=NA|NS|NG|ND|NR;E+=['the new names are %d+%d+%d+%d+%d, not 31+11+4+8+2'%(len(NA),len(NS),len(NG),len(ND),len(NR))] if (len(NA),len(NS),len(NG),len(ND),len(NR),len(NEW))!=(31,11,4,8,2,56) else [];E+=['7.8 table not found'] if len(O)!=40 else [];E+=['mutant set is not 7.8 plus S-ALPHA.s 31, S-BETA.s 11, S-GAMMA.s 4 and this card.s 10: %s'%sorted(N10^(N78|NEW))] if N10!=(N78|NEW) else [];E+=['new mutant already at 7.8: %s'%sorted(NEW&N78)] if NEW&N78 else [];F=lambda s:re.sub(r'\([^()]*fixture only[^()]*\)','',s);H=lambda s:{m for m in N10 if re.search(r'(?<![A-Z0-9+-])'+re.escape(m)+r'(?![A-Z0-9+-])',F(s))};J=lambda p:set().union(*[H(r[5]) for r in R if p(r)]);JA=J(lambda r:r[2] in C);X=set('POISON-IGNORED XX001-NOT-POISON CLIENT-READ-POISONS POISON-WAITER-PARKED WAL-ERR-NOT-POISON CREATE-TREE-SPLIT LOAD-PAGE-SPLIT ROOT-NAME-KEPT'.split());E+=['exception %s is not on a 7.8 row with a Phase A card'%m for m in sorted(X) if not any(m in M([r]) and any(i in C for i in I(r[2])) for r in O)];A78=set(m for r in O if any(i in C for i in I(r[2])) for m in M([r]))-X;E+=['%s: no 15.1 row judges it'%m for m in sorted(N10-J(lambda r:True))];E+=['%s: judged on a Phase A card at 7.8, on no Phase A row now'%m for m in sorted(A78-JA)];E+=['%s: new, judged on no Phase A row'%m for m in sorted(NEW-JA)];E+=['row %d: mutants not named: %s'%(i,r[5]) for i,r in enumerate(R,1) if re.search(r'its own|the same',r[5])];PL={'REPLICA-SKIP':'A-7c A-7d','COPY-INDEX-OFF':'A-7c','TIE-NO-BOOT-ID':'A-2 A-7d','TIE-EQUAL-OPENS':'A-2','MISPLACED-SHADOWS':'A-2 A-7d','IDENTITY-SKIP':'A-2 A-7d','PAGE0-REQUIRED':'A-2 A-7d','REPLICA-SLOT-FREE':'A-3c A-7b','FREE-ONE-GEN':'A-3c A-7c A-7d','RECYCLE-ONE-GEN':'A-7c A-7d','OPEN-PROTECTS-ONE-GEN':'A-7d','FALLBACK-OFF':'A-7d','FALLBACK-NO-PROOF':'A-7d','FALLBACK-TWO-GEN':'A-7d','RETIRE-SKIP':'A-7d','RETIRE-ONE-COPY':'A-7d','ROLLBACK-SKIPS-PREV-CHECKS':'A-7d','DEP-CHECK-SKIP':'A-7d','DEP-BY-POSITION':'A-7d','DEP-FIRST-ONLY':'A-7d','DEP-CHECK-LATE':'A-7d','DEP-ID-STALE':'A-7d','DEP-SALVAGE-PAST-CUT':'A-7d','START-SEG-AS-END':'A-6b A-7d','SEG0-LAZY':'A-6b A-7d','START-SEG-NOT-ENSURED':'A-7c A-7d','RECYCLE-UNORDERED':'A-6b A-7d','MAPINNER-CREATE-UNCONDITIONAL':'A-4a A-7d','LEVEL-GROW-UNCONDITIONAL':'A-4a A-7d','MAP-ROOT-CURRENT':'A-3c A-7d','MAP-POSITION-FROM-ROOT':'A-3c A-7d'};PP=[(m,k) for m,v in sorted(PL.items()) for k in v.split()]+[('GCW-BOUND-SKIP','A-7c'),('FLIP-NO-GATE','A-7f'),('SEG-TAIL-SHORT','A-7d'),('LSN-CHECK-SKIP','A-7d'),('TRUNC-NOT-DURABLE','A-7d'),('BOOT-DURABLE-AT-VALID-END','A-7d'),('WAL-START-PAIR-SPLIT-READ','A-7d A-7f'),('BOOT-PAIR-UNSET','A-7d A-7f'),('SEAL-STALE-HEAD','A-6c'),('SPLICE-MARKER-PAST-CUT','A-7d'),('SPLICE-CUT-SAME-START-KEEP','A-7d'),('DROP-NO-GCW','A-7b'),('DROP-NO-GCW','A-7c'),('DROP-NO-GCW','A-7d'),('DROP-NO-GCW','B-6'),('DROP-GCW-AT-WALK','A-7b'),('DROP-GCW-AT-TS','A-7c'),('REPLAY-DROP-NO-GCW','A-7d'),('REPLAY-DROP-NO-GCW','B-6'),('START-LSN-SPLIT-READ','A-7d A-7f'),('APPEND-SECTION-IO','A-6c'),('APPEND-SECTION-IO','A-7c'),('REPLAY-DROP-AT-TS','A-7d'),('REPLAY-DROP-AT-TS','B-6'),('DROP-OP-W-ZERO','A-6a'),('DROP-OP-W-ZERO','A-7d')]+[('POOL-MIN-PATH-ONLY','A-3b'),('RESTART-KEEPS-EMPTY-MAPLEAF','A-4a'),('ROOT-PIN-MAP-FIRST','A-7b'),('WALK-XX001-NO-POISON','A-7d'),('FENCE-SKIP','A-7d'),('UNREAD-PROTECTS-NOTHING','A-7d'),('MISPLACED-NEXT-REFUSES','A-2'),('MISPLACED-NEXT-REFUSES','A-7d'),('MISPLACED-IGNORED','A-2'),('WAL-ERR-NOT-POISON','A-7c')]+[(m,'A-7f') for m in 'ROOT-MOVES LEAK-ON-PACK MAPLEAF-CREATE-UNCONDITIONAL REPLACE-FREE-UNDER-P DELETE-FREE-UNDER-P CHAIN-BUILD-UNDER-P INPLACE-EVICT STEP2-NO-WAIT IMAGE-RECORD-AFTER-RELEASE IMAGE-SLOT-REUSED MAP-PHASE-DROPS-CLAIMED-COPY MAP-HOME-SLOT-REUSED MAP-PHASE-SKIPS-VICTIM MAP-SOURCE-UNCHECKED MAP-POSITION-AS-LPID ADOPT-LISTED-HOME IMAGE-LIST-GROWS MAP-COPY-NOT-DETACHED RESERVE-DRAW-BLOCKS WAL-SYNC-T-ONLY PREV-FIRST-WRONG WAL-START-PREV-WRONG WAL-START-PREV-CRC-WRONG SEG-ACK-BEFORE-DURABLE CKPT-SYNC-OVER-HOLE'.split()]+[('DROP-ENTRY-SPLIT','A-7d'),('PREV-HOP-WRONG','A-7d')]+[('CHUNK-SET-KEEPS-ENDED','A-7d'),('DROP-MARK-AT-PUBLISHED-W','A-7c')];E+=['placement map misses %s'%sorted(NEW-set(m for m,k in PP))] if NEW-set(m for m,k in PP) else [];E+=['%s not judged on %s'%(m,k) for m,k in PP if m not in J(lambda r,k=k:r[2] in k.split())];PB25=set('CHAIN-DELETE-BY-KEY CLIENT-READ-POISONS CREATE-TREE-SPLIT DEAD-AS-LIVE G-RESURRECT G-TOMB-FIRST G-WATERMARK+1 GC-DROPS-VISIBLE GC-KEEPS-TOMB LOAD-PAGE-SPLIT NEWEST-SKIPS-TOMB ORDINARY-FIRST POISON-IGNORED POISON-WAITER-PARKED PRUNE-ELIDES-OVER-CHAIN PRUNE-EVICT-FREES-CHAIN PRUNE-FIRST-ENTRY PRUNE-FREES-CHAINS PUBLISH-BEFORE-META ROOT-NAME-KEPT STEP-MUTATE-BEFORE-CHAINS SWEEP-CHAINS-UNDER-P UNIQUE-TOMB-LIVE WAL-ERR-NOT-POISON XX001-NOT-POISON'.split());E+=['the Phase-B-only set is not the 25 of draft 7.13 less WAL-ERR-NOT-POISON: %s'%sorted((N10-JA)^(PB25-{'WAL-ERR-NOT-POISON'}))] if (N10-JA)!=(PB25-{'WAL-ERR-NOT-POISON'}) else [];MV=[('I-ROOT-STABLE','(row)','ROOT-MOVES'),('I-LPID-EXACT','(row)','LEAK-ON-PACK'),('I-LINK-ATOMIC','t_alloc_restart_progress','MAPLEAF-CREATE-UNCONDITIONAL'),('I-LINK-ATOMIC','t_replace_free_shape','REPLACE-FREE-UNDER-P'),('I-NO-INPLACE','(row)','INPLACE-EVICT'),('I-CKPT-AT-T','t_step2_waits_for_claimed_copy','STEP2-NO-WAIT'),('I-CKPT-AT-T','(row)','FLIP-NO-GATE'),('I-WAL-CHAIN','t_wal_chain_crash','PREV-FIRST-WRONG'),('I-RO-OPEN','t_seg_ack_durable','SEG-ACK-BEFORE-DURABLE'),('I-ACK-PREFIX','t_ckpt_sync_prefix','CKPT-SYNC-OVER-HOLE')];G=lambda v,x,m,k:[r for r in R if r[1]==v and x in r[0] and r[2]==k and m in H(r[5])];E+=['moved form %s %s %s: %d rows on A-7f with A-7d in Needs'%(v,x,m,n) for v,x,m in MV for n in [len([r for r in G(v,x,m,'A-7f') if 'A-7d' in I(r[3])])] if n!=1];E+=['moved form %s %s %s still on A-7d'%(v,x,m) for v,x,m in MV if G(v,x,m,'A-7d')];E+=['kept half %s %s not on A-7d'%(v,m) for v,x,m in [('I-LPID-EXACT','(row)','DROP-ENTRY-SPLIT'),('I-WAL-CHAIN','t_wal_chain_crash','PREV-HOP-WRONG')] if len(G(v,x,m,'A-7d'))!=1];E+=['A-7f rows: %d, not 10'%n for n in [sum(r[2]=='A-7f' for r in R)] if n!=10];E+=['an A-7f row lacks A-7d in Needs: %s'%r[0] for r in R if r[2]=='A-7f' and 'A-7d' not in I(r[3])];E+=['STEP-MUTATE-BEFORE-CHAINS on %d rows'%n for n in [sum('STEP-MUTATE-BEFORE-CHAINS' in H(r[5]) for r in R)] if n!=1];IV=[r[0] for r in T15];E+=['no 15 row for %s'%v for v in 'I-META-COPIES I-TWO-GEN I-FALLBACK I-CKPT-DEP I-WAL-START I-MAP-GROW I-BOOT-DURABLE'.split() if v not in IV];E+=['new test %s not in 15'%x for x in 't_meta_copies t_replica_region t_two_gen t_fallback t_ckpt_dep t_wal_start t_wal_trunc_durable t_boot_durable_start t_drop_restart_floor t_flip_chunk_race t_append_section_leaf t_pool_minimum'.split() if x not in N];E+=['no I-MAP-GROW row for %s'%x for x in ['t_alloc_restart_progress','t_step2_waits_for_claimed_copy'] if not any(x in r[0] and r[1]=='I-MAP-GROW' for r in R)];E+=['no 15.1 row for invariant %s'%x for x in IV if not any(r[1]==x for r in R)];E+=['row %d: a full form names a stand-in: %s'%(i,r[4]) for i,r in enumerate(R,1) if re.match('full',r[4]) and re.search(r'\bS[1-6]\b',r[4]) and not re.search('full under|full except',r[4])];E+=['row %d: a full form is a page walk: %s'%(i,r[4]) for i,r in enumerate(R,1) if re.match('full',r[4]) and re.search('page.walk',r[4])];E+=['row %d: last form outside the allowed test'%i for i,r in enumerate(R,1) if 'last form' in r[4] and 't_epoch_reserve_two_exhausters' not in r[0]];E+=['15.1 cites N2'] if re.search(r'\bN2\b',t[b:c]) else [];L=lambda s:', '.join(s[:-1])+' and '+s[-1];CB=[x for x in Bo if any(r[2]==x for r in R)];NB=[x for x in Bo if x not in CB and any(x in I(r[3]) for r in R)];s='The Phase B cards in its Card column are %s; its Needs column also names %s.'%(L(CB),L(NB));E+=['15.1 intro lacks: '+s] if s not in t[b:c] else [];print(len(R),'rows;',len(N10),'mutants;',len(N10-JA),'judged only on Phase B cards;',len(A78),'judged on a Phase A card at 7.8');print(len(NEW),'new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 here),',len(NEW&JA),'of them judged on a Phase A row; %d invariants, %d tests'%(len(IV),len(N)));print(chr(10).join(E) or 'OK');sys.exit(1 if E else 0)"`
~~~~

**Inserted before the file-list line** (23 lines, in this order):

~~~~text
- `cd nucleus/v3 && grep -o 'a crash before the boot checkpoint.s step 4, so that no copy of its meta is written and the next open selects meta .k. again' docs/S-STORE.md | wc -l | grep -qx 2`
- `cd nucleus/v3 && ! sed -n '4,/^## Changelog/p' docs/S-STORE.md | grep -n 'crash before the boot checkpoint.s step 5'`
- `cd nucleus/v3 && grep -n 'A retry that holds the pin of a MapLeaf left with no free LPID takes no LPID under a MapLeaf it does not hold pinned: it restarts instead\. Each such restart drops the pin of every MapLeaf left with no free LPID before it pins another, and pins at least one more MapLeaf, so an operation that takes .n. LPIDs never holds more than .n. such pins at once' docs/S-STORE.md`
- `cd nucleus/v3 && ! sed -n '4,/^## Changelog/p' docs/S-STORE.md | grep -n 'LPIDs holds at most .n. such pins\|and the rest lie under non-resident MapLeafs\. Each such restart pins'`
- `cd nucleus/v3 && grep -n 'a pool at exactly the minimum, at the fixture.s height .h., for its four threads, the two inserting threads, the hook.s thread and S1.s flip thread, together with the build.s own threads that pin frames, if it has any' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'A-3c.s gate offers that hook as a test seam: its shared acquire tries the gate first and, when it finds the gate held exclusively, calls the hook before it blocks\.' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'the store.s own threads as well as its callers., and under N10.s cooperative mode every task that can hold a pin while it waits, and .max tree height. counts' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'for one, two and eight callers. threads, to each of which it adds the build.s own threads that pin frames (its checkpointer, evictors and background jobs, as the build reports them)' docs/S-STORE.md`
- `cd nucleus/v3 && sed -n '/^### 15\.1 /,/^## 16\. /p' docs/S-STORE.md | grep -c '^| .t_pool_minimum. | I-POOL-BOUND | A-3b | — | full: a pool one frame below the minimum and one at it, for one, two and eight callers. threads with the build.s own added, at every allowed page size | POOL-MIN-PATH-ONLY |$' | grep -qx 1`
- `cd nucleus/v3 && grep -q 'the stand-ins tidies, A-7c.s judgement of WAL-ERR-NOT-POISON, and ten §15\.1 rows relabelled to A-7f' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n '^  - \*\*WAL-ERR-NOT-POISON at A-7c\*\* (PL-11-F2 (a), S-15-STANDINS-F3 (f)): §15\.1 judges it at A-7c, the first card whose Depends closure holds both the group-commit writer (A-6c) and the poison mark (A-3c)' docs/S-STORE.md`
- `cd nucleus/v3 && sed -n '/^### 15\.1 /,/^## 16\. /p' docs/S-STORE.md | grep -q '^| .t_flip_chunk_race. | I-REDO-COMPLETE | A-7d | A-7c, A-6d, A-1b | full except S3 for the end of .X. in variant \*ended\* (§15 S3): a flip that races'`
- `cd nucleus/v3 && grep -n '^4\. the previous image (§1): image .k−1. while meta .k. is the durable image, until step 6 of checkpoint .k+1. (I-TWO-GEN); or, after an open from meta .k. that could not read meta .k−1..s free set, every slot below meta .k−1..s .slot_count. that image .k. does not reference, until the first step 6 after that open (§14 step 2)\.$' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'The fence variants fail: (i) its form that never zeroes, since the stale meta .k. sits beside the new one with an equal .boot_id. and the next open gives XX001, while a fence made after the first eviction but before the boot checkpoint passes (i); (ii) both forms' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'leaves meta .k. either in the places of its own parity that held it at the open, to be decided again, or in no place at all\.' docs/S-STORE.md`
- `cd nucleus/v3 && ! sed -n '4,/^## Changelog/p' docs/S-STORE.md | grep -n 'whole in its own parity\|§5\.4.s quota'`
- `cd nucleus/v3 && sed -n '4,/^## Changelog/p' docs/S-STORE.md | grep -c '§5\.4.s per-thread count' | grep -qx 3`
- `cd nucleus/v3 && grep -n '(the pool holds every page, no background evictor runs, and no flip is pending); A.s pin finds .r..s frame' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'as the test-only stand-in in the other, a root-name read that accepts .dropping. and holds no gate (not S4.s sweep step), so no XX001 of A.s poisons the store' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'A fenced place of the selected meta.s own parity leaves that meta one copy until its parity.s next checkpoint rewrites both' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'A restart keeps no pin but its cold page and its reservation.s, and drops an emptied MapLeaf.s pin before it pins another; a retry that holds such a pin restarts rather than pin a MapLeaf it does not hold (§5\.6)' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'whose crash, like (b).s now, comes before the boot checkpoint.s step 4, so that the next open selects meta .k. again' docs/S-STORE.md`
- `cd nucleus/v3 && grep -n 'A fenced copy of the meta the open selected leaves that meta one copy until its parity.s next checkpoint (§14 step 1, Residual)\.' docs/S-STORE.md`
~~~~

## Card text the rework must update

The blocks above are the spec change. The card's own prose must follow them:
- **Goal** item 4, **D7** and **D-11**: WAL-ERR-NOT-POISON is judged at A-7c, the first card whose closure holds the writer and the mark (finding 4).
- **D1**: the pool-floor variant's tracker, thread count, seam and kill (RWD-3 to -6); `t_pool_minimum`'s threads (RWD-9).
- **D4**: (b)'s and (c)'s crash before step 4.
- **Limits 2–6**: list RWD-1 to RWD-29 by section:
  - §2.2: RWD-19. §4.4: RWD-1, -2, -18. §5.4: RWD-4 to -10. §5.6: RWD-3. §6.7: RWD-21, -22.
  - §14: RWD-20, -26. §15: RWD-11, -12, -24, -25. §15.1: RWD-10, -13, -16, -17. §19: RWD-23.
  - The status line and the Changelog: RWD-14, -15, -27, -28, -29.
- **Mutation evidence** ("WAL-ERR-NOT-POISON gains A-7c") and the checker's description: WAL-ERR-NOT-POISON on A-7c.
- **"For the gate"**: item 2's WAL variant, the floor variant and `t_two_gen` (c).
- **"For the plan card"** item 5, and the **Notes'** "Due" list: WAL-ERR-NOT-POISON before A-7c, not A-6c.

## Owner points

| Point | Recommendation | What it means |
|---|---|---|
| **P-1.** The larger minimum pool | Accept. | The 7.13 floor could deadlock (Trace A) or never fit a cascading split (Trace B). At 8 KiB pages and tree height 5, each thread now needs 168 KiB instead of 56 KiB: 16 callers' threads go from about 0.9 MiB to 2.6 MiB, and with the store's own threads counted (finding 3) about 3.3 MiB, plus the epoch and map reserves. Below it an open is refused with 53200. A later refinement could charge the store's own threads less. |
| **P-4.** A 58030 on the previous free set | Accept the middle course. | The open carries on, protecting every slot the older image could use, and fails with 58030 only where that read error is what stops a fall-back. Refusing outright would let one bad sector, in a structure the next checkpoint throws away, keep the database from opening at all. |
| **P-5.** The misplaced-copy rule as a change | Accept. | It removes the only one-fault case in which an intact store would not open (a misdirected in-flight copy plus a crash). In my model it never opens anything unsafe below four simultaneous faults, where it matches the residual the card states. |
| **P-7.** The fence zeroes rather than repairs | Accept, with RWD-26's residual written down. | Repairing the chosen meta's own unreadable place would avoid 18 two-fault refusals that zeroing leaves; draft 7.13 refused all of them too. |
| **P-8.** S-ALPHA-F4, the appendix | Say "yes". | Verified: its ten blocks are right and apply once each, in order, both on d493188f and on the reworked text. A "yes" is one tiny spec card (status line, Changelog, ROLLBACK-REFUSES-READ-ERROR judged on A-7d, greps, a PLAN §10 note), due before A-7d. "Yes" means that after one unreadable sector under the newest checkpoint's roots, an operator can still read the data from the previous checkpoint and choose to roll back one checkpoint, while the automatic open still refuses. |
| **The 8-byte DropTree watermark** (S-BETA-O1) | None: it is final. | S-GAMMA took it in 7.13 and this card changes no byte format, so once the reworked card merges nothing pending touches the WAL record format, and A-6a may be cut. |

## What the plan card must change

1. **The binding condition.** S-GAMMA's binding condition (verdict 3: no card that reads S-STORE §12.4, §12.6 or §13 step 0 is cut before S-DELTA merges) is **discharged when this card merges, after its rework**. Until then it still binds A-6a, A-6c, A-6d, A-7c and A-7d.
2. **§3.3, the minimum pool size.** `threads × 3 × (max tree height + 2) + R_EPOCH + R_MAP` (R25). `threads` counts callers' threads, the store's own (checkpointer, evictors, background jobs) and, under N10, every task that can hold a pin while it waits.
3. **A-3a.** Page table first. An LPID is unmapped for a pin only when no frame holds it and its entry has slot 0, and an eviction sets the entry before the frame leaves the page table.
4. **A-3b.**
   - The three terms and `threads`; `t_pool_minimum` (POOL-MIN-PATH-ONLY).
   - The store reports its own pinning-thread count for the test.
   - R-1 sizes `R_MAP` for dirty pages only.
5. **A-3c.**
   - The gate's test seam: the shared acquire tries the gate first and calls a hook before it blocks.
   - The poison mark of §9.2 item 1, as the row already says.
6. **A-4a.**
   - The pool-floor variant (RESTART-KEEPS-EMPTY-MAPLEAF).
   - A restart keeps no pin but its cold page and its reservation's, and drops an emptied MapLeaf's pin before it pins another.
   - A retry that holds such a pin restarts rather than pin another MapLeaf.
7. **A-5.** A chain build or free holds at most two chain pages and their MapLeafs at once, releasing each before it pins a third.
8. **A-6c.**
   - Drop "Also pins WAL-ERR-NOT-POISON at the store level". The writer's error path must reach the store's poison mark once A-7c wires it.
   - S-GAMMA's A-6c items stand: the append takes no lock for a `Chunk` or its hop's `SegmentEnd` (G4's rule).
9. **A-7c.**
   - It wires the group-commit writer's error path to the poison mark, and judges WAL-ERR-NOT-POISON: `t_gated_xx001_poisons`' WAL variant on the store.
   - S-GAMMA-F4's exact (a) (DROP-MARK-AT-PUBLISHED-W).
   - The alternative, an A-6c → A-3c edge with the row's Needs changed, would need a spec card; I do not recommend it.
10. **A-2.** The misplaced threshold `k + 1` (MISPLACED-NEXT-REFUSES, MISPLACED-IGNORED).
11. **A-7b.** `t_root_name_reuse`'s cases C and E non-resident, and case F with no background evictor (ROOT-PIN-MAP-FIRST).
12. **A-7d.**
    - The fence (FENCE-SKIP).
    - The unread free set's protection and its 58030 rule (UNREAD-PROTECTS-NOTHING), with `t_two_gen` (b) and (c) crashing before step 4.
    - The misplaced threshold at open, the walk-poison variant, and `t_wal_chain_crash`'s hop step.
    - CHUNK-SET-KEEPS-ENDED through S3's new case.
13. **A-7f.**
    - Its ten rows, plus WAL-START-PAIR-SPLIT-READ and BOOT-PAIR-UNSET (beyond the plan's list, A-7c's code) and SEAL-STALE-HEAD's re-run.
    - P-6: keep the two I-MAP-GROW full forms on A-7d.
14. **B-6.** `t_wal_loss_salvage` needs B-4a and A-4e; S-GAMMA-F3's AS OF at the op's `w`.
15. **§10 item 19.** 24 mutants judged only in Phase B: the 25 less WAL-ERR-NOT-POISON. The owner is told of P-4, P-5 and P-7. S-ALPHA-F4 stays the owner's.
16. **QUEUE.** The card's rows close when the reworked card merges.

## After this card

Once the reworked card merges, this gate knows of **no open S-STORE defect or follow-up that must precede a Phase A card**, with two conditional items:
- **S-ALPHA-F4.** If the owner says "yes", the appendix's tiny card must precede A-7d.
- **P-6.** If the plan card moves the two I-MAP-GROW full forms to A-7f against this recommendation, a two-cell relabel spec card must precede A-7f.

S-1-F3-F1 is C-T6's.
