Card: S-GAMMA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 2543ceb9-7772-4b2d-b9a2-5501435b3b9e (fresh gate of the rework; this agent wrote neither the card, its edits, verdict 1 nor the rework)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: c1a9c5152f65561a30ca8170ddb41d5424d32e1a
Mutants: spec text, none in code; each walked as text against its test on its owning rows at c1a9c515: START-LSN-SPLIT-READ -> t_flip_chunk_race (A-7d), both forms, caught on every build that RW-1 leaves conforming EXCEPT one whose store's append takes the I-WAL-TS-ORDER section around a Chunk's reservation (G1, G2 below; RW2-1 to RW2-3), WAL-START-PAIR-SPLIT-READ -> t_wal_chain_crash racing step (A-7d), chain-head-last form by the false self-pair, chain-head-first form by the hop form on a build that reserves a hop's SegmentEnd in an entry of its own (equivalent, as stated, where a hop is one entry), same exception as START-LSN-SPLIT-READ, APPEND-SECTION-IO -> t_append_section_leaf (A-6c: I/O form through the Env hook, wait form, however it waits, by not returning within the bound; A-7c: deadlock under shuttle), REPLAY-DROP-AT-TS -> t_drop_restart_floor (d), (e) exact check (A-7d; B-6), DROP-OP-W-ZERO -> t_drop_restart_floor codec form (A-6a) and (d), (e) (A-7d), W-FLOOR-META-ONLY -> t_drop_restart_floor (d) full form (B-6); gate's own: G1 the store's append takes the I-WAL-TS-ORDER section around every reservation, a Chunk's included, with START-LSN-SPLIT-READ's second form (the set entry after the reservation, both inside that hold) -> equivalent, and the test's synchronous hook deadlocks on step 0's hold of that section (RW2-1), G2 the same build with START-LSN-SPLIT-READ's step-0 form, both entries inside step 0's hold of that section -> equivalent, hook blocks (RW2-1), G3 the spin form of APPEND-SECTION-IO at A-6c -> caught by RW-6's bound, G4 CHUNK-SET-KEEPS-ENDED -> UNCAUGHT, rule now stated (RW-16), test rides (S-GAMMA-F1), G5 step 0's entry drops a member whose marker is above T -> caught by START-LSN-MARKED-CHUNK's test (A-6d direct, A-7d), G6 the mark raises to the published W in normal operation -> UNCAUGHT, safe (S-GAMMA-F4, optional)
Followups: S-GAMMA-F1, S-GAMMA-F2, S-GAMMA-F3, S-GAMMA-F4, S-GAMMA-F5, S-GAMMA-F6

## Verdict in brief

**REWORK**, one more mechanical pass: 4 exact blocks (RW2-1 to RW2-4, all in `docs/S-STORE.md`) and 5 Acceptance lines (AC-15 to AC-19). Everything else in the rework holds.

- The rework is exactly RW-1 to RW-18: my own script re-applies them to f6549849's files and reproduces c1a9c515 byte for byte; the card's 54 edit blocks reproduce f6549849 from 264d44b8 byte for byte. Scope is the card's Touch only plus this verdict file. No mutant, test or invariant name of 264d44b8 or f6549849 is lost. The card's 83 Acceptance lines pass at HEAD (82 run, `gate.sh` aside; checker `152 rows; 215 mutants; …; OK`), and AC-1 to AC-14 all fail at f6549849.
- Verdict 1's rulings (i)–(iv) are true in the text and right in behaviour, with one gap in (i): **residual 1 is real.** Verdict 1 closed the gate side of "can the racing hook reserve inside step 0?" but not the I-WAL-TS-ORDER side. Nothing stops the store's own append from taking the I-WAL-TS-ORDER section around a `Chunk`'s reservation. On such a build, which is conforming at c1a9c515, both forms of START-LSN-SPLIT-READ and both orders of WAL-START-PAIR-SPLIT-READ can be built so they cannot fail, and the racing hooks, which the text has reserve "through the store's append", block on step 0's hold of that section. A synchronous hook deadlocks. This is the same class of defect as verdict 1's item 1, and the same one-sentence kind of rule closes it (RW2-1).
- Residuals 2–4 are not blocking. They ride in S-DELTA as S-GAMMA-F5 and S-GAMMA-F6 (exact text below), or are card-only notes.

## Gate run

- Extract: shallow `--no-local` clone of `nucleus/v3-scaffold`, scaffold id `264d44b818903701ac52c647b2e18869394def13`; `fresh-gate.sh` and `git-env.sh` extracted, no `EXTRACT FAILED`.
- `bash "$fg/fresh-gate.sh" c1a9c5152f65561a30ca8170ddb41d5424d32e1a`, from `~/Workspace/neutron-v3`, stdout and stderr captured separately:
  - `fresh-gate: commit c1a9c5152f65561a30ca8170ddb41d5424d32e1a`
  - `fresh-gate: base 264d44b818903701ac52c647b2e18869394def13`
  - `fresh-gate: checks passed (4816 files hashed in 4.71 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - stderr empty; 883 `ok` lines, 56 `test result: ok`, every `STEP … EXIT=0` (test, clippy, fmt, deny, wasm32, core-paths-selftest, enforcement-tests);
  - literal last line: `GATE EXIT=0`.
- `$fg` removed afterwards. The worktree was at c1a9c515 and clean before this verdict's commit.

## Scope and bytes (each by this gate's own script)

- Tip `c1a9c5152f65561a30ca8170ddb41d5424d32e1a`, worktree clean; history 264d44b8 → f217212e → f6549849 → 9abe5527 (verdict 1) → c1a9c515 (the rework, `docs/` only: S-STORE +13/−13, C-T0 +3/−3). The verdict file at c1a9c515 is 9abe5527's.
- `git diff nucleus/v3-scaffold...HEAD --stat`: `docs/C-T0-txn-protocol.md`, `docs/S-STORE.md`, `verdicts/S-GAMMA.md`. Merge base is the scaffold tip 264d44b8.
- The card's Edits, parsed by my script (54 Old/New pairs: 49 to S-STORE, 5 to C-T0), applied in order to 264d44b8's files with `<date>` = 2026-11-15: every Old occurs once at its turn, and both results equal f6549849's files.
- Verdict 1's RW blocks, parsed from `git show 9abe5527:nucleus/v3/verdicts/S-GAMMA.md` (18 pairs, in its order RW-1…RW-12, RW-16…RW-18, RW-13…RW-15): every Old occurs once in f6549849's file before any is applied, and the result equals c1a9c515's two files.
- Names: every token of the form `ABC-DEF…` or `t_…` in either file at 264d44b8 or f6549849 is still in the file at c1a9c515.
- Acceptance: the card's 83 lines (AC-1 replaced, AC-2 to AC-14 added). At c1a9c515: 82 run, 0 fail. At f6549849 (a `git archive` tree, so the two git-dependent lines, the checker and the file list, cannot run there): lines 5 and 68–80, i.e. AC-1 to AC-14, all fail.

**Is the card's state acceptable for the record?** Yes. The card's Edits are the record of f217212e and f6549849, verdict 1's RW-1 to RW-18 are the record of c1a9c515, and the two together reproduce HEAD byte for byte (checked both ways). The Acceptance is what checks the current text. The card should say so in one line under "Edits" ("HEAD = these blocks, then `verdicts/S-GAMMA.md`'s RW-1 to RW-18, then RW2-1 to RW2-4"), which the orchestrator can add when it applies this pass; the card is local and non-normative, so this is not a defect.

## Verdict 1's rulings, re-derived at c1a9c515

### (i) A reservation is not a gated operation; the store's append takes no gate

**Consistency across the text.**
- §5.6's list of gated operations names no WAL reservation, and "Each of these holds the gate from its first page mutation to its last" still fits.
- §12.4 (RW-1) states it, with the reason (a `Chunk` may carry ops that change no page: `RequireCkpt`, §12.8; `CreateTree` and `Ddl`, C-T6-rehost §5.3 and §5.5).
- §12.4's leaf rule ("A thread may enter it holding the epoch gate, frame latches (a build may reserve a `Chunk` inside a gated materialisation …)") agrees: a caller may hold the gate, but the append does not take it.
- §12.6 "Why one entry" (RW-4) and §13 step 0 ("`Chunk` reservations need not be made inside the critical section") agree.
- C-T0 §3.2 ("Each materialization is a gated operation") does not contradict it: the gate covers the materialisation's page mutations, and RW-1 explicitly allows the `Chunk`'s reservation inside it. See residual 2.

**The walks, on a build the text allows after RW-1.**
- *`t_flip_chunk_race`, correct build.* The hook thread holds nothing and reserves `X1` through the store's append. Just before step 0's one entry: `X1` is in the set the entry reads, `wal_start_lsn = q`, the pair is `X1`'s own `prev`; recovery replays `X1`…`Xn`, the read at the recovered frontier sees `k1`. Just after: `q` is the position the entry read, `wal_start_lsn = q`, the pair is the chain head. Pass.
- *Under START-LSN-SPLIT-READ, step-0 form.* The hook falls between the set read and the position read. Step 0 holds the gate exclusively there, and the hook's reservation takes no gate, so it lands at `q`; the set read was empty, the position read is `q + len(X1)`, `wal_start_lsn` lies past `q`, and the open returns `Ok` without `k1`. Caught.
- *Under its second form* (set entry after the reservation entry): step 0's entry falls between the hook's two entries, whatever the hook's caller would hold, since the hook calls the store's append directly. Caught.
- *`t_wal_chain_crash`'s racing step, chain head read last:* `X1` at the append position `p` read with the empty set, so `wal_start_lsn = p` and the pair, read afterwards, is `X1` itself, while the walk's first record is `X1`, whose `prev` names `p`'s predecessor: a false splice, XX001. Caught. Correct build: the hook before or after the one entry gives a consistent pair. Pass.
- *Chain head read first, hop form* (a build that reserves a hop's `SegmentEnd` in an entry of its own): the chain head read is `R`; the `SegmentEnd` entry lands at `s`, moving the position to the next segment's first LSN `n`; step 0 then reads position `n` and the empty set, so `wal_start_lsn = n` with the pair naming `R`; `X1`'s own entry follows at `n`, its `prev` the `SegmentEnd`. A splice on a healthy store. Caught. Correct build with the same hook ("otherwise just before step 0's one entry"): position `n`, chain head the `SegmentEnd`, `X1` at `n` with `prev` the `SegmentEnd`: consistent. Pass. Where a hop is one entry this order is equivalent, as RW-2 states.

**The gap: the I-WAL-TS-ORDER section (residual 1, real).** Step 0 makes its one entry to the append section inside its hold of the I-WAL-TS-ORDER section ("the same read that gives `wal_ckpt_end`"; "Inside the commit path's I-WAL-TS-ORDER critical section, read `wal_ckpt_end`"). The text lets a `Chunk` be reserved inside that section ("need not be made inside", §13 step 0; the leaf rule lets a thread enter the append section holding it) and never says the store's append does not take it. And the store's append is a natural owner of that section: I-WAL-TS-ORDER is judged on A-6c's code (§15 row; §15.1 `— (row)` A-6c), so a build whose A-6c append takes the I-WAL-TS-ORDER section around every reservation and then enters the append section inside it is conforming. On that build:
- START-LSN-SPLIT-READ's step-0 form, written with both entries inside step 0's hold of the I-WAL-TS-ORDER section (where the position read must be), cannot fail: no `Chunk` reservation can run between them (my G2). Its second form, the set entry after the reservation entry inside the append's own hold of that section, cannot fail either: step 0 cannot run between them (my G1). The same holds for WAL-START-PAIR-SPLIT-READ in both orders.
- The tests' hooks reserve "through the store's append from a thread that holds no gate". Placed between those entries, or "just before" step 0's entry read as just before the append-section entry, such a hook blocks on step 0's hold; a synchronous hook deadlocks on a correct build too.

That is verdict 1's item 1 again, with the I-WAL-TS-ORDER section in place of the gate, so it is closed the same way: **the store's append takes the I-WAL-TS-ORDER section, if at all, only for a `Commit` or `CommitMarker`; a caller may hold it around a `Chunk`'s reservation or not** (RW2-1, with RW2-2 and RW2-3 making §12.6 and §13 step 0 say the same, and RW2-4 recording it). The hook texts need no change: a hook thread holds neither, and their pointer (§12.4) now covers both. The rule costs nothing: no text puts a `Chunk` under the I-WAL-TS-ORDER section, `Chunk`s carry no ts, and C-T0's callers stay free to hold the section around a `Chunk` (C-T0 v8 §2.6 allows it: the sequencer section is only never held while acquiring a stripe, the gate or a latch).

**The converse danger: does anything rely on step 0's exclusive gate (or its I-WAL-TS-ORDER hold) excluding a `Chunk`'s reservation?** No.
- *The apply frontier `T`:* apply inserts are gated, so no apply runs between items 1 and 3; reservations do not move `T`.
- *I-WAL-TS-ORDER, `ts_ckpt_hi`, the first term:* only `Commit` and `CommitMarker`, which are inside the I-WAL-TS-ORDER section and so excluded by step 0's hold of it, not by the gate.
- *`wal_ckpt_end` "may fall either side":* already written for a `Chunk` racing the read; a `Chunk` reserved during step 0 with no gate gets its provisional entry later, in a gated operation, so in epoch `k+1`; one reserved before has its bytes below `wal_ckpt_end`, synced by step 1. Provisional entries are dead after recovery in either case (C-T0 v8 §2.4).
- *Step 0's "A record appended after the read has no entry in image `k`":* argued through apply inserts being gated, not reservations.
- *The boot checkpoint:* no transaction, so no `Chunk`, before the boot meta is durable (C-T0 v8 §2.5); the set is empty at open.
- *DropTree marks:* the mark is a gated directory change; the op and its `w` ride in the drop-cleanup `Commit`, whose reservation the mark does not depend on.
- *Bulk load `RequireCkpt`:* its `Chunk` is written after the forced checkpoint's meta is durable (§12.8), an ordering the writer keeps, not the gate.

### (ii) The open-chunk set's leaving rule

The text (RW-16 to RW-18) now says: removal is required (a markerless end leaves "in an entry of its own as it ends"; a flip's entry "drops" every member with marker `≤ T`); the set is empty at open; a flip before the leaving entry still counts the member. Re-walked:
- *Abort, or commit with no ts:* no marker ever; before its leaving entry a flip counts it (retains more); after, its chunks may fall below `wal_start_lsn` and recovery ignores them anyway. Safe, and it leaves.
- *Marker `≤ T`:* `T` is the apply frontier, and a large transaction's frontier passes `c` only after its finalize (§12.7, C-T0 v8 §3.5), every conversion gated and so before step 0: image `k` holds it whole as ordinary entries. Dropped, safe.
- *Marker reserved between step 0's taking of `T` and its entry:* `c` is issued above every earlier ts, so `c > T`; the set records it in the marker's own entry; counted.
- *Two consecutive flips:* a drop at flip `k′` needs `c ≤ T_k′ ≤ T_k`.
- *A flip whose meta never becomes durable:* the drop affects only the in-memory set; meta `k−1` counted the member when it had to (it was in the set, or started after that entry, or had `c ≤ T_{k−1}`), and step 6 recycles only on a durable meta. After a crash the set restarts empty and the boot checkpoint's `T` is the recovered frontier `≥ c`.
- *A member whose first `Chunk` hopped:* its own LSN, past the `SegmentEnd`, enters the set; the pair is its own `prev`, the `SegmentEnd`. With a separate `SegmentEnd` entry and step 0's entry between it and the `Chunk`'s, the position read is that LSN and the chunk lands there: counted by the position.
- *For ever:* a member with a marker leaves at the first flip whose `T ≥ c`, which comes once its finalize advances `M`; a markerless end leaves as it ends; a crash empties the set. Only a transaction still open stays, which is the rule's intent.

Nothing in an interleaving of a leaving entry with step 0's one entry drops a member whose chunks are still needed. The rule's mutant and test (CHUNK-SET-KEEPS-ENDED) are still S-GAMMA-F1.

### (iii) `t_append_section_leaf`

- *A-6c* (RW-6 to RW-8): the creation is held until the hopping reservation has returned, and the test fails past a bound. A correct reservation does not wait for the segment, so it returns at once and the hook then releases the creation; the writer creates the segment outside the section. Under APPEND-SECTION-IO the I/O form trips the `Env` hook; the wait form, however it waits (tracked lock, untracked spin), never returns while the creation is held and fails the bound (my G3). In the cooperative mode (N10) the bound is on the reservation's return, before the thread goes on to write, so the form is unchanged.
- *A-7c* (shuttle): unchanged, and sound: under the mutant the hop holds the section waiting on the held creation, step 0 waits on the section holding the gate, the gated operation waits on the gate and the hook on that operation; every schedule deadlocks. A correct build finishes.

### (iv) The DropTree `w` in C-T0

C-T0 §9.1 now says a logged DropTree's mark raises `gc_w_used` to the `w` its op carries (the published `W` the drop-cleanup job read) and an unlogged one's to the `W` published at its mutation; the status line and the 8.6 Changelog say "logged" and record the unlogged case "as before". That matches S-STORE §6.8 step 1 (G3) and §12.3, and C-T0 §3.7 step 3's "raised by every DropTree mark that pass 2 replays" covers only logged ones, as it should.

## The four residuals, ruled

1. **Real; blocking; closed by RW2-1 to RW2-4** (above). The alternative, "state where the mutants are equivalent and how the test behaves there", would leave START-LSN-SPLIT-READ unkillable on a conforming build and a hook that deadlocks, which is what verdict 1 refused for the gate; one sentence of rule is smaller and makes every mutant killable on every build.
2. **Not a defect; rides in S-DELTA as S-GAMMA-F6.** C-T0 §3.2's "Each materialization is a gated operation" is about the materialisation's page mutations, and S-STORE §12.4 (RW-1) explicitly allows the reservation inside it; nothing leads an implementer wrong, and the tests reserve through the store's append directly. A pointer is still worth having. S-DELTA must add `docs/C-T0-txn-protocol.md` to its Touch only (with a C-T0 draft 8.7 status line and Changelog entry) or hand the block to the next C-T0 card.
3. **Wording; rides as S-GAMMA-F5.** After three cases (aborted, marker `≤ T`, committed with no ts), "Either" means the first and third; it is still true read as any of them, so nobody builds anything wrong.
4. **Card prose: not a spec defect, no carrier needed.** "Never weakened" bullet 3, D1's "this split is equivalent (safe), and the mutant is narrowed", D-2's summary, "For the gate" item 2 and "69 lines / 70 commands" are superseded by verdict 1 (RW-2, RW-16) and are non-normative. The orchestrator may mark them superseded when it applies this pass (with the one-line Edits note above); the card is local and does not ship.

## Rework: one list (RW2-1 to RW2-4), then the card's Acceptance

Apply in this order to `docs/S-STORE.md` at c1a9c515 (S-STORE draft 7.13 after RW-1 to RW-18). Each Old occurs exactly once, before any block is applied, and no two Olds overlap (checked by script). C-T0 is untouched. The drafts stay 7.13 and 8.6, and the counts stay 152 rows, 215 mutants and 47 tests. Fences here are four tildes; the card may use three backticks.

**Dry run** (scratch clone of the card branch, outside the repository, with the four blocks committed): the card's 83 Acceptance lines plus AC-15 to AC-19, 87 run (`gate.sh` aside), 0 fail; the checker prints `152 rows; 215 mutants; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `46 new since 7.10 (31 at 7.11, 11 at 7.12, 4 here), 46 of them judged on a Phase A row; 47 invariants, 47 tests`, `OK`. AC-15 to AC-19 all fail at c1a9c515. Verdict 1's S-GAMMA-F1 to F4 blocks and S-GAMMA-F5 still apply after RW2-1 to RW2-4 (each Old once, no overlap), and S-GAMMA-F6 applies to C-T0 at c1a9c515. I re-read §12.4 "Append", §12.6 "When the terms are taken" and "Why one entry", and §13 step 0 item 2 whole with the blocks applied: the leaf rule ("A thread may enter it holding … the I-WAL-TS-ORDER critical section") stays true for the commit path and for a caller that holds the section around a `Chunk`; nothing else names who takes that section for a `Chunk`.

### RW2-1. S-STORE §12.4 "Append": the store's append takes no I-WAL-TS-ORDER section for a `Chunk`
File: docs/S-STORE.md

Old:
~~~~text
and the open-chunk set, not the gate, keeps §12.6's terms right. The section also keeps
~~~~
New:
~~~~text
and the open-chunk set, not the gate, keeps §12.6's terms right. Nor does the store's append take the I-WAL-TS-ORDER critical section for a `Chunk`: it may take that section for a `Commit` or `CommitMarker` (below), and a caller may hold it around a `Chunk`'s reservation or not (§13 step 0), but a `Chunk` reserved through the store's append by a thread that holds neither the gate nor that section waits on neither, so it can fall between any two of step 0's entries to the append critical section, even where step 0 holds both around them. The section also keeps
~~~~

### RW2-2. S-STORE §12.6, "Why one entry": who can fall inside step 0
File: docs/S-STORE.md

Old:
~~~~text
A `Chunk` reserved by a caller that holds the gate cannot fall inside step 0, but one reserved by a caller that holds none can.
~~~~
New:
~~~~text
A `Chunk` reserved by a caller that holds the gate, or the I-WAL-TS-ORDER section, cannot fall inside step 0's hold of it, but one reserved by a caller that holds neither can, since the store's append takes neither for a `Chunk` (§12.4).
~~~~

### RW2-3. S-STORE §13 step 0, item 2
File: docs/S-STORE.md

Old:
~~~~text
`Chunk` reservations need not be made inside the critical section: a chunk appended
~~~~
New:
~~~~text
`Chunk` reservations need not be made inside the critical section, and the store's append does not take it for one (§12.4): a chunk appended
~~~~

### RW2-4. S-STORE Changelog 7.13, first bullet
File: docs/S-STORE.md

Old:
~~~~text
§12.4 now states that a reservation is not a gated operation.
~~~~
New:
~~~~text
§12.4 now states that a reservation is not a gated operation, and that the store's append takes the I-WAL-TS-ORDER critical section for no `Chunk`.
~~~~

### The card's Acceptance

- **AC-15 to AC-19**, added just before the checker line (no backtick in any command; each fails at c1a9c515 and passes after RW2-1 to RW2-4):

  ~~~~text
  - `cd nucleus/v3 && grep -n 'Nor does the store.s append take the I-WAL-TS-ORDER critical section for a .Chunk.: it may take that section for a .Commit. or .CommitMarker. (below)' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'cannot fall inside step 0.s hold of it, but one reserved by a caller that holds neither can, since the store.s append takes neither for a .Chunk. (§12\.4)\.' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n '.Chunk. reservations need not be made inside the critical section, and the store.s append does not take it for one (§12\.4): a chunk appended' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'and that the store.s append takes the I-WAL-TS-ORDER critical section for no .Chunk.\.' docs/S-STORE.md`
  - `cd nucleus/v3 && ! grep -n 'cannot fall inside step 0, but one reserved by a caller that holds none can' docs/S-STORE.md`
  ~~~~
- The card's other 83 lines, the checker line, the file-list line and `gate.sh` are unchanged.

## Follow-ups (ride in S-DELTA)

S-GAMMA-F1 to F4 are verdict 1's, unchanged; their blocks are repeated below byte for byte (from 9abe5527) so that S-DELTA's author has every rider in one file. Each Old occurs once in 7.13 after RW-1 to RW-18 and RW2-1 to RW2-4, and all thirteen S-STORE blocks (F1 to F5) apply together; F6 applies to C-T0 8.6 (checked by script).

- **S-GAMMA-F1: a mutant and a test for the leaving rule** (before A-6d is cut). RW-16 states the rule; nothing tests it. Mutant CHUNK-SET-KEEPS-ENDED, judged at A-7d by `t_flip_chunk_race`'s variant *ended*. Blocks RW-F1-1 to RW-F1-3.
- **S-GAMMA-F2: the leaf rule's entry list** (before A-6c is cut): name the lock-table entries, the settle stripe and the SSI mutex. Block RW-F2-1.
- **S-GAMMA-F3: B-6's second AS OF at the op's `w`** (before B-6). Blocks RW-F3-1, RW-F3-2.
- **S-GAMMA-F4 (optional): pin the one value in normal operation** (before A-7c is cut; may be waived). Blocks RW-F4-1 to RW-F4-5.
- **S-GAMMA-F5: §12.6's "Either"** (residual 3; wording, any time). After three listed cases, "Either" is meant to name the aborted transaction and the one that committed with no ts. Block RW-F5-1.
- **S-GAMMA-F6: C-T0 §3.2's pointer to S-STORE §12.4** (residual 2; before A-6d is cut, since A-6d writes the chunk path). "Each materialization is a gated operation" reads naturally as the reservation being inside the gate; say that it may be either way and why. C-T0 is not in S-DELTA's Touch only as briefed: S-DELTA adds it with a C-T0 draft 8.7 status line and Changelog entry, or hands the block to the next C-T0 card. Block RW-F6-1.

### RW-F1-1. S-STORE §14, START-LSN-SPLIT-READ's test: variant *ended* and its mutant

File: docs/S-STORE.md

Old:
~~~~text
Under START-LSN-SPLIT-READ, meta `k`'s `wal_start_lsn` lies past `q`, pass 2 applies `X`'s later chunks at `c` without the first, and the open returns `Ok` without `k1`.
~~~~
New:
~~~~text
Under START-LSN-SPLIT-READ, meta `k`'s `wal_start_lsn` lies past `q`, pass 2 applies `X`'s later chunks at `c` without the first, and the open returns `Ok` without `k1`. Variant *ended*: with no `Commit` or `CommitMarker` above `T` and no other chunked transaction open, `X` reserves chunks and then aborts, or, in a second run, rolls back to a savepoint that leaves it no data and commits with no ts (C-T0 v8 §3.1 step 0); a flip after `X` has left the open-chunk set records the append position its entry reads as `wal_start_lsn`, past `X`'s chunks.
- **CHUNK-SET-KEEPS-ENDED:** a transaction that ends with no marker never leaves the open-chunk set (§12.6), so every later flip's `wal_start_lsn` stays at its first `Chunk` and no segment from there on is ever recycled: safe, but one aborted large transaction stops the WAL from being recycled. Test `t_flip_chunk_race`, variant *ended* (A-7d).
~~~~

### RW-F1-2. S-STORE §15, I-REDO-COMPLETE's row

File: docs/S-STORE.md

Old:
~~~~text
Test `t_flip_chunk_race` (a flip that races a new chunked transaction's first `Chunk`; §14) | A-6d, A-7d | START-LSN-PAST-CHUNK, START-LSN-MARKED-CHUNK, START-LSN-SPLIT-READ |
~~~~
New:
~~~~text
Test `t_flip_chunk_race` (a flip that races a new chunked transaction's first `Chunk`; §14); a transaction that ends with no marker leaves the set as it ends and stops holding `wal_start_lsn` back (its variant *ended*) | A-6d, A-7d | START-LSN-PAST-CHUNK, START-LSN-MARKED-CHUNK, START-LSN-SPLIT-READ, CHUNK-SET-KEEPS-ENDED |
~~~~

### RW-F1-3. S-STORE §15.1, `t_flip_chunk_race`'s row

File: docs/S-STORE.md

Old:
~~~~text
the acked commit, a power cut and recovery from meta `k` | START-LSN-SPLIT-READ (on A-7c's step 0 and A-6d's chunk path) |
~~~~
New:
~~~~text
the acked commit, a power cut and recovery from meta `k`; variant *ended* | START-LSN-SPLIT-READ (on A-7c's step 0 and A-6d's chunk path), CHUNK-SET-KEEPS-ENDED (on A-6d's chunk path) |
~~~~

### RW-F2-1. S-STORE §12.4, the leaf rule: what a thread may hold on entry

File: docs/S-STORE.md

Old:
~~~~text
A thread may enter it holding the epoch gate, frame latches (a build may reserve a `Chunk` inside a gated materialisation, C-T0 v8 §3.2) and the I-WAL-TS-ORDER critical section, with what C-T0 v8 §2.6 ranks before that section;
~~~~
New:
~~~~text
A thread may enter it holding any lock ranked before it: the lock-table entries that a writer holds logically and a settle stripe (C-T0 v8 §2.6), the epoch gate, frame latches (a build may reserve a `Chunk` inside a gated materialisation, C-T0 v8 §3.2), and the I-WAL-TS-ORDER critical section with the SSI mutex ranked before it (C-T0 v8 §2.6, §8.4);
~~~~

### RW-F3-1. S-STORE §10.5, `t_drop_restart_floor`: the second AS OF in (d) and (e)

File: docs/S-STORE.md

Old:
~~~~text
one at the reopened `W`, which is `gc_w_used` as recovery leaves it (so `w` in (d) and (e)) and lies above `S_ddl`, succeeds
~~~~
New:
~~~~text
one at the reopened `W`, which is `gc_w_used` as recovery leaves it and lies above `S_ddl`, and which in (d) and (e) is taken at the op's `w` itself, the value a correct reopen publishes, so that a floor left above `w` refuses it, succeeds
~~~~

### RW-F3-2. S-STORE §15.1, `t_drop_restart_floor`'s B-6 row

File: docs/S-STORE.md

Old:
~~~~text
and at the reopened `W` (served, without the tree) |
~~~~
New:
~~~~text
and at the reopened `W`, at the op's `w` in (d) and (e) (served, without the tree) |
~~~~

### RW-F4-1. S-STORE §10.5, `t_drop_restart_floor`: exact in (a)

File: docs/S-STORE.md

Old:
~~~~text
After each reopen `gc_w_used` is at least `w` (in (d) and (e) exactly `w`, the larger of `w` and the opened meta's `gc_w`),
~~~~
New:
~~~~text
After each reopen `gc_w_used` is at least `w` (exactly `w` in (a), with `W` published above `w` before the mark, and in (d) and (e), the larger of `w` and the opened meta's `gc_w`),
~~~~

### RW-F4-2. S-STORE §10.5, the mutant

File: docs/S-STORE.md

Old:
~~~~text
    - **DROP-OP-W-ZERO**: the op codec drops `w`, so a replayed op carries 0 and its mark raises nothing, which leaves REPLAY-DROP-NO-GCW's floor.
~~~~
New:
~~~~text
    - **DROP-OP-W-ZERO**: the op codec drops `w`, so a replayed op carries 0 and its mark raises nothing, which leaves REPLAY-DROP-NO-GCW's floor;
    - **DROP-MARK-AT-PUBLISHED-W**: a logged DropTree's mark raises `gc_w_used` to the `W` published at its mutation, not to its op's `w`: safe, but an image that holds the mark then records a floor above `w`, and a restart from it refuses AS OF snapshots in `[w, W)` that a replay of the same drop would admit.
~~~~

### RW-F4-3. S-STORE §10.5, the kill

File: docs/S-STORE.md

Old:
~~~~text
DROP-OP-W-ZERO fails (d) and (e) with the opened meta's `gc_w`, and DROP-GCW-AT-TS fails (f).
~~~~
New:
~~~~text
DROP-OP-W-ZERO fails (d) and (e) with the opened meta's `gc_w`, DROP-MARK-AT-PUBLISHED-W fails (a) with the `W` published at the mark, and DROP-GCW-AT-TS fails (f).
~~~~

### RW-F4-4. S-STORE §15.1, `t_drop_restart_floor`'s A-7c row

File: docs/S-STORE.md

Old:
~~~~text
variant (a) through the flip, meta `k`'s `gc_w ≥ w`; variant (f), the meta write succeeds with `gc_w ≤ T` | DROP-NO-GCW, DROP-GCW-AT-WALK, DROP-GCW-AT-TS |
~~~~
New:
~~~~text
variant (a) through the flip, with S3's `W` above `w` at the mark, meta `k`'s `gc_w` exactly `w`; variant (f), the meta write succeeds with `gc_w ≤ T` | DROP-NO-GCW, DROP-GCW-AT-WALK, DROP-GCW-AT-TS, DROP-MARK-AT-PUBLISHED-W |
~~~~

### RW-F4-5. S-STORE §15, I-GCW's row

File: docs/S-STORE.md

Old:
~~~~text
DROP-GCW-AT-TS, REPLAY-DROP-NO-GCW, REPLAY-DROP-AT-TS, DROP-OP-W-ZERO |
~~~~
New:
~~~~text
DROP-GCW-AT-TS, REPLAY-DROP-NO-GCW, REPLAY-DROP-AT-TS, DROP-OP-W-ZERO, DROP-MARK-AT-PUBLISHED-W |
~~~~

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

- Verdict 1's corrections stand (`verdicts/S-BETA.md` at 5cb70935's qualifier; QUEUE S-BETA-F1's CAUTION). Its ruling (C) is extended, not reversed: the race needs a first `Chunk` reserved with neither the gate nor step 0's I-WAL-TS-ORDER section in its way; after RW2-1 the store's append takes neither for a `Chunk`, so the tests can always force it.
- QUEUE: S-GAMMA's row records REWORK (second) and this verdict's commit; S-GAMMA-F5 and F6 join F1 to F4 under S-DELTA.

## What S-DELTA must change

1. **Rebase onto 7.13 as it stands after RW2-1 to RW2-4.** Besides the card's "Rebase points" and verdict 1's list (RW-1 to RW-18), its Olds must not span: §12.4 "Append" from "and the open-chunk set, not the gate, keeps" to "The section also keeps" (RW2-1); §12.6 "Why one entry", its second sentence (RW2-2); §13 step 0 item 2, "`Chunk` reservations need not be made inside the critical section, …" (RW2-3); the end of the 7.13 Changelog's first bullet (RW2-4).
2. **Carry S-GAMMA-F1, F2, F3 and F5**, and F4 unless it is waived, with the exact blocks above; **carry F6** by adding `docs/C-T0-txn-protocol.md` to its Touch only (C-T0 draft 8.7) or hand it to the next C-T0 card.
3. **Its checker** starts from S-GAMMA's, unchanged by RW2. NG gains CHUNK-SET-KEEPS-ENDED (on A-7d) and, if F4 is taken, DROP-MARK-AT-PUBLISHED-W (on A-7c): 216 or 217 mutants. F1 and F4 change text that four of S-GAMMA's Acceptance greps match (the I-REDO-COMPLETE row, "After each reopen …", "REPLAY-DROP-AT-TS fails (d) and (e) …", the I-GCW row), and F5 changes no grepped text; S-DELTA's own Acceptance supersedes them.

## What the plan card must change

Verdict 1's list stands (A-6c, A-6d, A-7c, A-7d, B-6, C-T6-rehost §7.4, QUEUE), with one addition:
- **A-6c:** the store's append takes no gate, and takes the I-WAL-TS-ORDER section, if at all, only for a `Commit` or `CommitMarker`, never for a `Chunk` (§12.4 after RW-1 and RW2-1). The leaf rule, `t_flip_chunk_race`'s hook and `t_wal_chain_crash`'s racing hook rely on it. A-6c's own I-WAL-TS-ORDER row (TS-AFTER-APPEND) is unaffected.
- **A-6d:** C-T0's materialisation may hold the gate or the sequencer section around its `Chunk`'s reservation; the chunk path's first-`Chunk` set entry is in the store's append either way (S-GAMMA-F6 for C-T0's sentence).

## The owner's points, in plain words

1. **The 8-byte watermark in the drop record (S-BETA-O1).** Unchanged from verdict 1, and this gate also finds it sound. *What users gain:* after a crash that follows a DROP or TRUNCATE's cleanup, time-travel (AS OF) queries in the window between "cleanup became allowed" and "cleanup ran" keep working; under 7.12 they were refused. *Cost:* 8 bytes per dropped table or index, in the cleanup commit only. **Timing: it must be final before A-6a is cut**, because A-6a fixes the log's byte format; after that, changing it is a format change (free until A-7e's compatibility verdict, a migration after). Recommendation: keep it.
2. **The race this card fixes,** in one line: a crash could lose part of an acknowledged large transaction that began exactly as a checkpoint began, on some builds the old text allowed. The card fixes it on every build. Verdict 1 added "writing a log piece never takes the checkpoint's lock" so the tests can force the race; this pass adds the matching sentence for the other lock the checkpoint holds at that moment (the commit-ordering lock), for the same reason. Without it, a test could hang on a correct build or miss a regression.
3. **Nothing else changes for users.** This pass is four sentences and five checks; no format, behaviour or test count changes.

## History

- **Verdict 1: REWORK, `9abe55276e3334c02ae880551db9458c96c75191` on `f654984997460c5218629d651d90c124b3fd71c5`** (gate P, claude-opus-5-5, session 099ee6b7-66d5-4dc5-b762-5419bc5a0a2b, GATE EXIT=0). It found the edit faithful (the card's 54 blocks reproduce f6549849) and ruled (C) on the race: drafts 7.8 to 7.12 left open whether a `Chunk` reservation holds the epoch gate, so the loss was real on a build whose first-`Chunk` reservation held no gate and impossible on one that reserved every `Chunk` inside a gated operation, where START-LSN-SPLIT-READ was equivalent and the racing hooks could not reserve. It asked for 18 blocks and 14 Acceptance lines:
  - RW-1, RW-4: a reservation is not a gated operation and the store's append takes no gate (§12.4, §12.6);
  - RW-2, RW-3, RW-12: WAL-START-PAIR-SPLIT-READ's chain-head-first order restored, with a hop form of the racing step on a build that reserves a hop's `SegmentEnd` in an entry of its own;
  - RW-3, RW-5, RW-9: both racing hooks reserve through the store's append from a thread that holds no gate;
  - RW-6 to RW-8: `t_append_section_leaf` at A-6c holds the creation until the hopping reservation returns, within a bound;
  - RW-10, RW-11: the status line and Changelog name the builds on which the loss was real;
  - RW-16 to RW-18: the open-chunk set's leaving rule (removal required, the set empty at open, "drops" not "may drop");
  - RW-13 to RW-15: C-T0 §9.1's unlogged DropTree raise, and "logged" in its status line and 8.6 entry;
  - follow-ups S-GAMMA-F1 to F4 for S-DELTA (repeated above).
- **The rework, `c1a9c5152f65561a30ca8170ddb41d5424d32e1a`:** RW-1 to RW-18 applied by script (S-STORE +13/−13, C-T0 +3/−3), the card's Acceptance updated (AC-1 replaced, AC-2 to AC-14 added: 83 lines), its 54 edit blocks unchanged. Its implementer reported four residuals, ruled above.
- **Verdict 2 (this one): REWORK** on c1a9c515, RW2-1 to RW2-4 and AC-15 to AC-19, for residual 1; residuals 2 and 3 ride as S-GAMMA-F6 and F5.
