Card: S-GAMMA
Class: spec
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 62be307c-e089-4ce6-9eda-687ab3b44319 (fresh gate of the second rework; this agent wrote neither the card, its edits, verdicts 1 and 2 nor either rework)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: c6d03599dc737fa997a2aca2cdb188cbf0137396
Mutants: spec text, none in code; each walked as text against its test on its owning rows at c6d03599: START-LSN-SPLIT-READ -> t_flip_chunk_race (A-7d), both forms, caught on every conforming build whose first `Chunk` does not hop with its `SegmentEnd` in an entry of its own (G2 below; S-GAMMA-F7), verdict 2's Chunk-under-section builds now non-conforming (G3), WAL-START-PAIR-SPLIT-READ -> t_wal_chain_crash racing step (A-7d), chain-head-last form by the false self-pair, caught, chain-head-first form by the hop form, caught except on a build whose store's append takes the I-WAL-TS-ORDER section around a `Chunk`'s hop's separate `SegmentEnd` entry (G1; S-GAMMA-F7), equivalent where a hop is one entry (as stated), APPEND-SECTION-IO -> t_append_section_leaf (A-6c, A-7c), unchanged, caught, REPLAY-DROP-AT-TS -> t_drop_restart_floor (d), (e) (A-7d; B-6), unchanged, caught, DROP-OP-W-ZERO -> t_drop_restart_floor codec form (A-6a) and (d), (e) (A-7d), unchanged, caught, W-FLOOR-META-ONLY -> t_drop_restart_floor (d) full form (B-6), unchanged, caught; gate's own: G1 the store's append takes the I-WAL-TS-ORDER section around the separate `SegmentEnd` entry of a `Chunk`'s hop, with WAL-START-PAIR-SPLIT-READ's chain-head-first form reading both entries inside step 0's hold of that section -> UNCAUGHT at c6d03599 (equivalent; the hop hook blocks and a synchronous one deadlocks), caught once S-GAMMA-F7 lands, G2 the same build, `t_flip_chunk_race` with a fixture whose `X1` hops, START-LSN-SPLIT-READ's step-0 form inside that hold -> UNCAUGHT at c6d03599 (hook blocks on the `SegmentEnd` entry), caught once S-GAMMA-F7 lands, G3 verdict 2's G1/G2 (the section around a `Chunk`'s own reservation) -> caught (the build is non-conforming after RW2-1; residual 1 closed), G4 the store's append waits on the checkpoint's own exclusion for a `Chunk` -> UNCAUGHT, not a natural build, closed as an A-6c plan-card rule (below)
Followups: S-GAMMA-F1, S-GAMMA-F2, S-GAMMA-F3, S-GAMMA-F4, S-GAMMA-F5, S-GAMMA-F6, S-GAMMA-F7

## Verdict in brief

**MERGE-WITH-FOLLOWUPS.** Rework 2 is exactly RW2-1 to RW2-4, and it closes verdict 2's residual 1 for a `Chunk`. The second-rework implementer's finding 1 is **real**: it is a fourth instance of the same gap, the `SegmentEnd` of a `Chunk`'s hop. The text counts that `SegmentEnd` as a reservation of its own kind, and RW2-1 never says "only". The fix is one phrase-level rule in one sentence of §12.4. It changes no other sentence, no test fixture, no mutant definition or placement and no count. It rides in S-DELTA as **S-GAMMA-F7** (exact block below). That is acceptable only under this binding condition:

> **No card that reads S-STORE §12.4, §12.6 or §13 step 0 is cut before S-DELTA merges.** A-6a and A-6c wait for S-DELTA, and so do A-6d, A-7c and A-7d.

This gate's reservation × lock survey turns up nothing else in the spec text. The one theoretical case outside the text, an append that waits on the checkpoint's own exclusion (G4), is closed as an A-6c plan-card rule.

- **Bytes.** My own script re-applies verdict 2's RW2-1 to RW2-4, parsed from `c5414fa6:nucleus/v3/verdicts/S-GAMMA.md`, to c1a9c515's `docs/S-STORE.md`. Each Old occurs once, and the result equals c6d03599's file byte for byte. C-T0 is byte-identical to c1a9c515. `git diff c5414fa6 c6d03599` touches only `docs/S-STORE.md` (+4/−4).
- **Scope.** `git diff nucleus/v3-scaffold...HEAD`: `docs/C-T0-txn-protocol.md`, `docs/S-STORE.md`, `verdicts/S-GAMMA.md`. These are the card's Touch only plus this file.
- **Names.** No token of the form `ABC-DEF…` or `t_…` is lost from either file at 264d44b8, f6549849 or c1a9c515.
- **Acceptance.** The card's 88 commands: 87 run (`gate.sh` aside), 0 fail. The checker prints `152 rows; 215 mutants; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `46 new since 7.10 (31 at 7.11, 11 at 7.12, 4 here), 46 of them judged on a Phase A row; 47 invariants, 47 tests`, `OK`.

## Gate run

- **Extract.** A shallow `--no-local` clone of `nucleus/v3-scaffold`, scaffold id `264d44b818903701ac52c647b2e18869394def13`. `fresh-gate.sh` and `git-env.sh` were extracted with no `EXTRACT FAILED`.
- **Run.** `bash "$fg/fresh-gate.sh" c6d03599dc737fa997a2aca2cdb188cbf0137396`, from `~/Workspace/neutron-v3`, with stdout and stderr captured separately:
  - `fresh-gate: commit c6d03599dc737fa997a2aca2cdb188cbf0137396`
  - `fresh-gate: base 264d44b818903701ac52c647b2e18869394def13`
  - `fresh-gate: checks passed (4816 files hashed in 1.54 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - stderr empty; 883 `ok` lines, 56 `test result: ok`, every `STEP … EXIT=0` (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests); exit 0; literal last line: `GATE EXIT=0`.
- `$fg` was removed afterwards. The worktree was at c6d03599 and clean before this verdict's commit.

## Residual 1 of verdict 2, for a `Chunk`: closed

§12.4 now says: "Nor does the store's append take the I-WAL-TS-ORDER critical section for a `Chunk` … a `Chunk` reserved through the store's append by a thread that holds neither the gate nor that section waits on neither, so it can fall between any two of step 0's entries to the append critical section, even where step 0 holds both around them." §12.6 "Why one entry" and §13 step 0 item 2 now say the same.

Walked with the lock states:
- **Step 0's locks.** Step 0 holds the checkpoint's exclusion (CX), the epoch gate exclusively (G) and the I-WAL-TS-ORDER section (S) around its entries to the append critical section (A).
- **The hook.** The racing hook's thread holds nothing. For its `Chunk` it takes only A, so it enters A between any two of step 0's entries.
- **START-LSN-SPLIT-READ, step-0 form inside S.** The set read is empty, the position read is `q + len(X1)`, and `wal_start_lsn` lies past `q`. Caught.
- **Its second form.** The append cannot hold G or S around a `Chunk`'s two entries, so step 0's entry lands between them. Caught.
- **WAL-START-PAIR-SPLIT-READ, chain head read last.** This gives the false self-pair. Caught.
- **Correct build.** The hook falls before or after the one entry. The pair is consistent, and the test passes; nothing blocks.

Verdict 2's G1 and G2 builds are no longer conforming. One gap remains, below.

## Finding 1, the `SegmentEnd` of a `Chunk`'s hop: real

**The text at c6d03599.**
- §12.4 counts "every reservation, of every kind, `Chunk` and `SegmentEnd` included" as one entry to A.
- §12.5 (WAL-START-PAIR-SPLIT-READ) and the racing step of `t_wal_chain_crash` name "a build that reserves a hop's `SegmentEnd` in an entry of its own (§12.4)".
- RW2-1 rules S out "for a `Chunk`" only. It says the append "may take that section for a `Commit` or `CommitMarker`", never "only".

So a build whose store's append takes S for every reservation that is not a `Chunk` is conforming at c6d03599, and that includes the separate `SegmentEnd` entry of a `Chunk`'s hop. It is a natural reading of the rule as literally coded, and natural in another way too: a `Commit`'s hop's `SegmentEnd` already sits inside the `Commit`'s S. Call this build B.

**Walk 1: WAL-START-PAIR-SPLIT-READ, chain-head-first, on B (my G1).** The mutant reads the chain head in entry E1 and the position and set in entry E2. Both reads sit inside step 0's hold of S, which is where the position read must be ("the same read that gives `wal_ckpt_end`").
- The hop form needs `X1`'s `SegmentEnd` entry between E1 and E2.
- On B that entry takes S, which step 0 holds, so it cannot land there. The mutant is equivalent on B.
- The text claims the opposite. It says this order fails "on a build that reserves a hop's `SegmentEnd` in an entry of its own".
- The hook is told to place the `SegmentEnd` entry between E1 and E2. Its thread blocks on S. A synchronous hook leaves step 0 waiting for the hook and the hook waiting for step 0's S: deadlock.
- An asynchronous hook lands after E2. The pair is then consistent, and the mutant is uncaught.

**Walk 2: the correct build B′ (B with one entry), same hop variant.**
- The hook is "just before step 0's one entry". Read as just before A, which is inside step 0's hold of S, the hook's `SegmentEnd` entry blocks on S. A synchronous hook deadlocks on a correct build.
- This is the reading verdict 2 refused for the `Chunk`, and it is the same here.

**Walk 3: `t_flip_chunk_race` on B, if the fixture's `X1` hops (my G2).** Nothing in the fixture rules a hop out.
- `X1`'s `SegmentEnd` entry must precede its `Chunk` entry.
- Placed between step 0's set read and position read, both inside S, the hook blocks on the `SegmentEnd` entry, or it deadlocks.
- The mutant's kill needs the `Chunk` entry there, behind the `SegmentEnd`.

**Ruling.** The gap is real and of the same class as verdict 1's item 1 and verdict 2's residual 1. It is narrow: it needs B, a hop in the racing reservation, and a hook point inside step 0's hold of S. It is closed the same way, by one phrase:

> The store's append takes S for neither a `Chunk` nor the `SegmentEnd` of a `Chunk`'s hop. It may take S only for a `Commit` or `CommitMarker` and the `SegmentEnd` of its hop.

With that phrase, the three walks change:
- **Walk 1.** The `SegmentEnd` entry waits on neither G nor S and lands between E1 and E2. The mutant is caught, as §12.5 claims.
- **Walk 2.** The hook never blocks, wherever "just before" falls.
- **Walk 3.** The fixture may hop freely.

**(a) or (b): (b).** The fix:
- changes one clause of one sentence (§12.4 "Append");
- leaves every other sentence true as written: RW2-2's "takes neither for a `Chunk`", RW2-3's "does not take it for one", the 7.13 Changelog's "for no `Chunk`" and the hook texts' "a thread that holds no gate (§12.4)";
- changes no test's fixture, no mutant's definition or placement, and no count (S-DELTA's checker passes with it; dry run below).

S-DELTA's author cannot get it wrong, because it is one byte-exact block. Hence MERGE-WITH-FOLLOWUPS with S-GAMMA-F7, under the binding condition above.

**Finding 2 (wording): not a defect, no rider.**
- The status line and the 7.13 Changelog say "The racing hooks of both tests reserve with no gate held". That is true.
- The hook texts say "a thread that holds no gate (§12.4)". A test's hook thread holds no C-T0 sequencer section, and §12.4 (with F7) states that the append takes neither lock.
- S-DELTA's 7.14 Changelog records F7 (text below).

**Finding 3: still open, confirmed.**
- S-GAMMA-F5 (RW-F5-1) applies at c6d03599 and after all of S-DELTA's blocks.
- S-GAMMA-F6 (RW-F6-1) applies to C-T0 8.6 at c6d03599.
- S-DELTA carries neither yet (it carries F1 to F4 only).

**Correction to verdict 2.** Verdict 2 says "all thirteen S-STORE blocks (F1 to F5)". F1 to F5 are **twelve** S-STORE blocks (3 + 1 + 2 + 5 + 1). The thirteenth block, F6, is in C-T0. This is not a defect.

## The reservation × lock survey (once and for all)

**What is listed.**
- **Reservation kinds.** §12.4's list and §12.2's kinds: `Commit`, `CommitMarker`, `Chunk` (a first one, which enters the open-chunk set, or a later one), and `SegmentEnd`, either in the hopping record's entry or in an entry of its own (of a `Commit`/`CommitMarker` hop or of a `Chunk` hop).
- **Locks step 0 holds across its entry and its read of `T`.** These are CX (§13: "At most one checkpoint runs at a time"; the whole checkpoint), G (exclusive, item 1 to item 3, so across `T`'s read and the entry), S (around the entry: "Inside the commit path's I-WAL-TS-ORDER critical section …, read `wal_ckpt_end`"; whether `T`'s read is also inside S is the build's choice) and A (the entry itself).
- **Locks step 0 is neither required nor forbidden to hold.** These are the snapshot registry (a build may read `M` under it, C-T0 §3.6), the SSI mutex (ranked before S, C-T0 §8.4; no text has step 0 take it), frame latches and the root table (recording the map's root), the free-set lock, the catalog lock, the image-record list (step 2, not step 0) and the live-txn registry (a leaf).

In the table, "append" means the store's append and "caller" means the thread that calls it.

| Reservation | G (gate) | S (I-WAL-TS-ORDER) | A (append section) | CX (checkpoint) | Other locks step 0 may hold | Tests and mutants that depend on it |
|---|---|---|---|---|---|---|
| `Commit`, `CommitMarker` | append: **forbidden** (RW-1); caller: allowed (C-T0 §2.6 ranks S after G) | **required**: the reservation's entry lies in S (I-WAL-TS-ORDER; §12.4 "contains its entry"), held by the caller (C-T0 §3.1/§3.2 step 1) or taken by the append ("may take that section") | **required**: it is the entry | not stated; no text has it taken | append: none taken (leaf rule; no reason); caller: SSI mutex **required** for SER (C-T0 §8.4); no stripe, gate or latch acquired while in S (C-T0 §2.6) | step 0's S excludes them: `ts_ckpt_hi`/`wal_ckpt_end` bound (I-WAL-COVER), TS-AFTER-APPEND; the racing fixtures exclude them ("no `Commit` or `CommitMarker` above `T`") |
| `SegmentEnd` of a `Commit`/`CommitMarker` hop, separate entry | append: forbidden (RW-1) | allowed (inside the `Commit`'s S; not required: a record slipping in after the `SegmentEnd` lands in the next segment) | required | not stated | as above | none |
| `Chunk` (first or later), its hop in one entry | append: **forbidden** (RW-1); caller: allowed (gated materialisation, C-T0 §3.2) | append: **forbidden** (RW2-1); caller: allowed (§13 step 0, RW2-3) | required; a first `Chunk`'s set entry is in it (§12.4) | not stated (G4) | append: none taken; caller: lock-table entries, a settle stripe, frame latches allowed (leaf rule; S-GAMMA-F2) | `t_flip_chunk_race` (START-LSN-SPLIT-READ), `t_wal_chain_crash` racing step (WAL-START-PAIR-SPLIT-READ): both need a hook thread whose append takes neither G nor S |
| `SegmentEnd` of a `Chunk` hop, separate entry | append: **forbidden** (RW-1) | append: **allowed at c6d03599: the gap**; forbidden after **S-GAMMA-F7**; caller: allowed | required | not stated (G4) | as for `Chunk` | `t_wal_chain_crash`'s hop form (WAL-START-PAIR-SPLIT-READ chain-head-first, on separate-entry builds); `t_flip_chunk_race` if its `X1` hops |

**Ops carried inside a `Commit` or `Chunk`.** I checked the implementer's survey independently, and it holds.
- **`RequireCkpt`** (§12.8 item 3). Its `Chunk` is written after the forced checkpoint's meta is durable. That is an ordering kept by waiting before the reservation; no lock is held across it.
- **`DropTree`.** Its `w` is read under the snapshot registry by the drop-cleanup job before the commit (C-T0 §9.1). Its mark is a separate gated directory change. Its `Commit` is an ordinary `Commit` row.
- **`CreateTree`, `Ddl`** (C-T6-rehost §5.3, §5.5). The tree's creation is a gated directory change, and the `Chunk` may be reserved inside it or outside it (RW-1).
- None of these gives the append a reason to take any lock that step 0 holds.

**Locks other than G, S and A.**
- The leaf rule forbids every ranked lock *under* A, and nothing in the text gives a reservation a reason to take one *before* A.
- Step 0 holds no lock-table entry, settle stripe or txn wait, so a caller's holding of those cannot block step 0's entries or a hook.

**G4, CX.** No text makes a reservation wait on the checkpoint's own exclusion, and none forbids it before A.
- The text presupposes reservations while a checkpoint runs:
  - §13 step 0 item 3 releases the gate, and step 0 item 1 calls the exclusive hold the generalisation of PLAN-v3's "pause commit-apply", the only pause;
  - `t_flip_chunk_race`'s hook "just after step 0's one entry" must return, and `X`'s chunks and marker follow it.
- A build whose append waited for the checkpoint to end would stall every commit through steps 1 to 6. It would also deadlock that synchronous hook. No natural build does this, so it needs no spec text.
- To converge, it is bound where the code is judged. A-6c's card must state that the store's append, for a `Chunk` or the `SegmentEnd` of a `Chunk`'s hop, takes no lock but the append critical section itself (below).

**Survey result.** The only spec-text gap is the separate-entry `SegmentEnd` of a `Chunk`'s hop × S. S-GAMMA-F7 closes it.

## Dry runs

All dry runs were done in scratch, outside the repository.
- **F7 on c6d03599's S-STORE.**
  - Its Old occurs once.
  - With F7 committed in a scratch clone of the card branch, the card's 87 lines run with one failure: line 81 (AC-15). AC-15 greps RW2-1's old clause; F7 supersedes it, and S-DELTA's Acceptance replaces it (below).
  - The checker's output is unchanged (152 rows, 215 mutants, 47 tests, `OK`).
- **Verdict 2's F1 to F5, then F7, on c6d03599.** Each Old occurs once, with no overlap.
- **S-DELTA as written** (its 109 Old/New pairs: the 11 riders RW-F1-1 to RW-F4-5, then E1 to E80's 98 pairs) applies to c6d03599's S-STORE. Every Old occurs once. S-DELTA was checked against c1a9c515, and RW2 spans none of its Olds.
- **S-DELTA, then F5, then F7.** Each Old occurs once. Applying F7 first and then S-DELTA and F5 gives a byte-identical file, so the order does not matter.
- **S-DELTA's own Acceptance on that file** (with C-T0 at c6d03599): 108 run, 107 pass. The checker (its line 107) passes. The one failure is its line 108, the file list, which cannot pass on a branch where S-GAMMA's C-T0 change is not yet in the scaffold. It is expected and unrelated to F7.
- **F6** applies to C-T0 at c6d03599.

## Follow-ups (all ride in S-DELTA)

- **S-GAMMA-F1 to F4.** These are verdict 1's (9abe5527), repeated byte for byte in verdict 2 (c5414fa6, "Follow-ups"). S-DELTA already carries all four (its riders RW-F1-1 to RW-F4-5; F4 taken, its D-13). They apply at c6d03599 unchanged (dry run above).
- **S-GAMMA-F5: §12.6's "Either"** (verdict 2, residual 3). This is wording and may land at any time. Block RW-F5-1 is repeated below. S-DELTA does not carry it yet.
- **S-GAMMA-F6: C-T0 §3.2's pointer to S-STORE §12.4** (verdict 2, residual 2). It must land before A-6d is cut. Block RW-F6-1 is repeated below. S-DELTA does not carry it yet. Either S-DELTA adds `docs/C-T0-txn-protocol.md` to its Touch only (a C-T0 draft 8.7 status line and Changelog entry), or the next C-T0 card carries it, and A-6d waits for that card.
- **S-GAMMA-F7: the `SegmentEnd` of a `Chunk`'s hop** (this verdict, finding 1). This is the only remaining instance of the "the store's append takes a lock step 0 holds" gap in the text, and it must land before A-6c, A-6d, A-7c or A-7d is cut. Rule: the store's append takes the I-WAL-TS-ORDER section for neither a `Chunk` nor the `SegmentEnd` of a `Chunk`'s hop, and may take it only for a `Commit` or `CommitMarker` and the `SegmentEnd` of its hop. Without it, on a conforming build:
  - the chain-head-first hop form of WAL-START-PAIR-SPLIT-READ is equivalent although §12.5 says it fails;
  - the hop hook of `t_wal_chain_crash`, and `t_flip_chunk_race`'s hook if `X1` hops, can deadlock, even on a correct build.

  It is one block (RW-F7-1), plus two Acceptance lines for S-DELTA, plus one clause in S-DELTA's 7.14 Changelog bullet (all below).

### RW-F7-1. S-STORE §12.4 "Append": the `SegmentEnd` of a `Chunk`'s hop
File: docs/S-STORE.md

Old:
~~~~text
Nor does the store's append take the I-WAL-TS-ORDER critical section for a `Chunk`: it may take that section for a `Commit` or `CommitMarker` (below), and a caller may hold it around a `Chunk`'s reservation or not (§13 step 0), but a `Chunk` reserved through the store's append by a thread that holds neither the gate nor that section waits on neither,
~~~~
New:
~~~~text
Nor does the store's append take the I-WAL-TS-ORDER critical section for a `Chunk`, nor for the `SegmentEnd` of a `Chunk`'s hop where a build reserves it in an entry of its own: it may take that section only for a `Commit` or `CommitMarker` and the `SegmentEnd` of its hop (below), and a caller may hold it around a `Chunk`'s reservation or not (§13 step 0), but a `Chunk` reserved through the store's append by a thread that holds neither the gate nor that section, its hop's `SegmentEnd` included, waits on neither,
~~~~

**S-DELTA's Acceptance for F7.** Two lines, added next to its rider lines. Neither contains a backtick. Each fails at c6d03599 and passes after RW-F7-1. They supersede S-GAMMA's AC-15, which greps the replaced clause:

~~~~text
- `cd nucleus/v3 && grep -n 'nor for the .SegmentEnd. of a .Chunk..s hop where a build reserves it in an entry of its own: it may take that section only for a .Commit. or .CommitMarker. and the .SegmentEnd. of its hop (below)' docs/S-STORE.md`
- `cd nucleus/v3 && ! grep -n 'critical section for a .Chunk.: it may take that section for a .Commit.' docs/S-STORE.md`
~~~~

**S-DELTA's 7.14 Changelog, in its "S-GAMMA's follow-ups" bullet:** add the clause "§12.4 now also says that the store's append takes the I-WAL-TS-ORDER critical section for no `SegmentEnd` of a `Chunk`'s hop, and only for a `Commit` or `CommitMarker` and its hop (S-GAMMA-F7)". Name F5 and F6 there too, if S-DELTA takes them. The checker, the counts (S-DELTA's 158 rows, 225 mutants and 48 tests) and every E-block are unchanged.

### Verdict 2's F5 and F6 blocks, repeated byte for byte (from c5414fa6)

### RW-F5-1. S-STORE §12.6, the transactions that do not count: name the two

File: docs/S-STORE.md

Old:
~~~~text
Either still counts at a flip whose entry precedes the one in which it leaves the open-chunk set (above), which only retains more.
~~~~
New:
~~~~text
An aborted transaction, or one that committed with no ts, still counts at a flip whose entry precedes the one in which it leaves the open-chunk set (above), which only retains more.
~~~~

### RW-F6-1. C-T0 §3.2, "Gate holds": the reservation

File: docs/C-T0-txn-protocol.md

Old:
~~~~text
Each materialization is a gated operation, or a bounded batch of them. A checkpoint flip never waits on more than one bounded batch.
~~~~
New:
~~~~text
Each materialization is a gated operation, or a bounded batch of them. A checkpoint flip never waits on more than one bounded batch. The gate covers the materialization's page mutations, not its `Chunk`'s reservation, which may be made inside the gated operation or outside it: a reservation is not a gated operation, and the store's append takes neither the gate nor, for a `Chunk`, the sequencer critical section (S-STORE §12.4).
~~~~

## Corrections of record (for the orchestrator; this gate edits neither)

- **Verdict 2's ruling (C) extension stands, now complete.** The race needs a first `Chunk` reserved with none of step 0's locks in its way. After RW-1, RW2-1 and S-GAMMA-F7, the store's append takes none for a `Chunk` or its hop, so the tests can always force it.
- **QUEUE.**
  - S-GAMMA's row records MERGE-WITH-FOLLOWUPS at this verdict's commit.
  - S-GAMMA-F5, F6 and F7 join F1 to F4 under S-DELTA. F6 goes there only if S-DELTA takes C-T0; otherwise it waits for the next C-T0 card, and A-6d waits for that card.
  - The binding condition above goes on the S-DELTA row and on the A-6a, A-6c, A-6d, A-7c and A-7d rows.
- **Verdict 2's "thirteen S-STORE blocks (F1 to F5)"** reads "twelve S-STORE blocks (F1 to F5) and one C-T0 block (F6)".

## What S-DELTA must change

1. **Rebase onto c6d03599's 7.13.**
   - Its 109 pairs apply unchanged (dry run above).
   - Its "Rebase points" should cite c6d03599, not c1a9c515.
   - Its Olds must not span RW2-1 to RW2-4's text (verdict 2, "What S-DELTA must change", item 1). None does.
2. **Carry every rider.**
   - **F1 to F4:** carried already.
   - **F5:** RW-F5-1, above.
   - **F6:** RW-F6-1, above, with `docs/C-T0-txn-protocol.md` added to the Touch only and a C-T0 draft 8.7 status line and Changelog entry; or hand it to the next C-T0 card, as above.
   - **F7:** RW-F7-1, its two Acceptance lines and its Changelog clause.

   Apply order: the riders first, as S-DELTA does now (F1 to F5, F7), then E1 to E80. Every order checked gives the same file.
3. **Supersede S-GAMMA's AC-15** with F7's two lines. S-DELTA's checker is unchanged.

## What the plan card must change

Verdict 1's list and verdict 2's additions stand (A-6c, A-6d, A-7c, A-7d, B-6, C-T6-rehost §7.4, QUEUE). Add:
- **A-6a, A-6c, A-6d, A-7c and A-7d are cut only after S-DELTA merges.** This is binding: no card that reads S-STORE §12.4, §12.6 or §13 step 0 is cut before S-DELTA merges. A-6a also waits for the owner's final word on the 8-byte watermark (below).
- **A-6c.** For a `Chunk`, and for the `SegmentEnd` of a `Chunk`'s hop, the store's append takes no lock but the append critical section itself:
  - not the epoch gate (RW-1);
  - not the I-WAL-TS-ORDER section (RW2-1, S-GAMMA-F7);
  - and it never waits on the checkpoint's own exclusion (G4).

  It may take the I-WAL-TS-ORDER section only for a `Commit` or `CommitMarker` and its hop. A-6c's card should test the append for this directly: a reservation, hopping and not, made from a thread with nothing held, while another thread holds the gate exclusively and the I-WAL-TS-ORDER section, returns within the bound that `t_append_section_leaf` already uses.
- **A-7d.** `t_wal_chain_crash`'s hop form and `t_flip_chunk_race` may use a hopping first `Chunk` freely, once F7 lands.

## The owner's points, in plain words

1. **The 8-byte watermark in the drop record (S-BETA-O1).** Unchanged from verdicts 1 and 2, and this gate finds it sound too.
   - *What users gain:* after a crash that follows a DROP or TRUNCATE's cleanup, time-travel (AS OF) queries in the window between "cleanup became allowed" and "cleanup ran" keep working.
   - *Cost:* 8 bytes per dropped table or index, in the cleanup commit only.
   - **It must be final before A-6a is cut,** because A-6a fixes the log's byte format. After that, changing it is a format change: free until A-7e's compatibility verdict, a migration after.
   - Recommendation: keep it.
2. **The race this card fixes.** A crash could lose part of an acknowledged large transaction that began exactly as a checkpoint began. The card fixes it on every build. Three passes have made sure the tests can always provoke the race: writing a log piece never takes the checkpoint's lock (verdict 1), nor the commit-ordering lock (verdict 2), nor that lock for the filler record a log piece writes when it moves to a new log file (this verdict, S-GAMMA-F7). Without the last one, on some correct builds a test could hang, or miss a regression of this exact bug.
3. **Why merge now, with F7 riding.** F7 is one clause, and the next spec card (S-DELTA) carries it with the other six riders. No implementation card can be cut from the text before S-DELTA merges. Nothing else changes for users: no format, behaviour or test-count change in this pass.

## History

- **Verdict 1: REWORK, `9abe55276e3334c02ae880551db9458c96c75191` on `f654984997460c5218629d651d90c124b3fd71c5`** (gate P, claude-opus-5-5, session 099ee6b7-66d5-4dc5-b762-5419bc5a0a2b, GATE EXIT=0).
  - **Findings.** The edit was faithful: the card's 54 blocks reproduce f6549849. It ruled (C) on the race: drafts 7.8 to 7.12 left open whether a `Chunk` reservation holds the epoch gate, so the loss was real on a build whose first-`Chunk` reservation held no gate.
  - **What it asked for:** 18 blocks and 14 Acceptance lines.
    - RW-1, RW-4: a reservation is not a gated operation, and the store's append takes no gate;
    - RW-2, RW-3, RW-12: WAL-START-PAIR-SPLIT-READ's chain-head-first order restored, with a hop form;
    - RW-3, RW-5, RW-9: the racing hooks reserve from a thread that holds no gate;
    - RW-6 to RW-8: `t_append_section_leaf` at A-6c within a bound;
    - RW-10, RW-11: the status line and the Changelog;
    - RW-16 to RW-18: the open-chunk set's leaving rule;
    - RW-13 to RW-15: C-T0 §9.1's unlogged DropTree raise.
  - **Follow-ups:** S-GAMMA-F1 to F4.
- **Rework 1, `c1a9c5152f65561a30ca8170ddb41d5424d32e1a`.** RW-1 to RW-18 applied by script (S-STORE +13/−13, C-T0 +3/−3). Acceptance went to 83 lines (AC-1 replaced, AC-2 to AC-14 added).
- **Verdict 2: REWORK, `c5414fa693ae61c4c3305ff71a08d8aee2203974` on `c1a9c5152f65561a30ca8170ddb41d5424d32e1a`** (gate P, claude-opus-5-5, session 2543ceb9-7772-4b2d-b9a2-5501435b3b9e, GATE EXIT=0).
  - **Finding.** Residual 1 was real: nothing stopped the store's append from taking the I-WAL-TS-ORDER section around a `Chunk`'s reservation.
  - **What it asked for:** RW2-1 to RW2-4 (S-STORE only), saying the append takes no such section for a `Chunk`, and AC-15 to AC-19.
  - **Follow-ups:** S-GAMMA-F5 ("Either") and S-GAMMA-F6 (C-T0 §3.2's pointer).
- **Rework 2, `c6d03599dc737fa997a2aca2cdb188cbf0137396`.** RW2-1 to RW2-4 applied byte for byte (S-STORE +4/−4; C-T0 unchanged). The card's Acceptance went to 88 commands. Its Edits line was updated, and four stale passages were marked superseded. Its implementer reported the `SegmentEnd` instance (finding 1), a wording point (finding 2) and the open F5 and F6.
- **Verdict 3 (this one): MERGE-WITH-FOLLOWUPS on c6d03599.**
  - Finding 1 is real and rides as S-GAMMA-F7.
  - The survey found no further instance in the text; G4 is bound in A-6c's card.
  - Binding condition: no card that reads §12.4, §12.6 or §13 step 0 is cut before S-DELTA merges.
