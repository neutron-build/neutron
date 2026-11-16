Card: S-GAMMA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 099ee6b7-66d5-4dc5-b762-5419bc5a0a2b (a fresh gate after the first attempt died on an expired login; this agent wrote neither the card nor any S-GAMMA commit)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: f654984997460c5218629d651d90c124b3fd71c5
Mutants: spec text, none in code; each walked as text against its test on its owning rows: START-LSN-SPLIT-READ -> t_flip_chunk_race (A-7d), both forms, only on a build whose first-Chunk reservation holds no gate (equivalent, and the test's hooks block or deadlock, on a build that reserves every Chunk inside a gated operation: RW-1, RW-5), APPEND-SECTION-IO -> t_append_section_leaf (A-6c: I/O form through the Env hook, wait form only when the wait is tracked; A-7c: deadlock under shuttle), REPLAY-DROP-AT-TS -> t_drop_restart_floor (d), (e) exact check (A-7d with w < c - 1 from S3; B-6 through the exact gc_w_used check), DROP-OP-W-ZERO -> t_drop_restart_floor codec form (A-6a) and (d), (e) (A-7d), WAL-START-PAIR-SPLIT-READ -> t_wal_chain_crash racing step (A-7d), chain head read after the position and the set (with a hook holding no gate, RW-3), W-FLOOR-META-ONLY -> t_drop_restart_floor (d) full form, AS OF at S_ddl - 1 (B-6); gate's own: G1 a build that reserves every Chunk, and records its first, inside a gated operation, with START-LSN-SPLIT-READ -> equivalent, hooks inside step 0's exclusive hold cannot reserve (RW-1, RW-5), G2 WAL-START-PAIR-SPLIT-READ's other order (chain head read first) on a build that reserves a hop's SegmentEnd in an entry of its own -> UNCAUGHT, the text calls it correct (RW-2, RW-3), G3 APPEND-SECTION-IO's wait form as an untracked spin -> UNCAUGHT at A-6c, caught at A-7c (RW-6 to RW-8), G4 CHUNK-SET-KEEPS-ENDED (a transaction that ends with no marker never leaves the set) -> UNCAUGHT, no rule and no test (RW-16 to RW-18; S-GAMMA-F1), G5 DROP-MARK-AT-PUBLISHED-W (the mark raises to the W published at it in normal operation, the P-2 alternative) -> UNCAUGHT, safe (S-GAMMA-F4, optional), G6 the drop job writes c - 1 into the op -> caught at B-6 by (d)'s exact check and by (f)'s fail-stop
Followups: S-GAMMA-F1, S-GAMMA-F2, S-GAMMA-F3, S-GAMMA-F4

## Verdict in brief

**REWORK**, 18 exact blocks (RW-1 to RW-18) and 14 Acceptance lines, one mechanical pass. The edit itself is faithful: my own script re-applies the card's 54 pairs (47 headings) to the 264d44b8 files and reproduces both committed files byte for byte; the 18 rider blocks are the S-BETA verdict's bytes; nothing outside Touch only changes; no invariant, test, mutant, row or 7.12 judgement is lost; the gate run is `GATE EXIT=0`. The defects are in what the text leaves open or claims:

1. **Item 1, ruled (C).** Drafts 7.8 to 7.12 leave open whether a `Chunk` reservation holds the epoch gate. On a build that reserves a first `Chunk` with no gate held and reads §12.6's set apart from the append position, D1's trace is a real silent loss of an acked commit. On a build that reserves every `Chunk`, and records its first, inside a gated operation, it cannot happen. 7.13 keeps it open (G15: "a build may reserve a `Chunk` inside a gated materialisation"), so on that second kind of build START-LSN-SPLIT-READ is equivalent, and `t_flip_chunk_race`'s and `t_wal_chain_crash`'s racing hooks, which sit inside step 0's exclusive hold, cannot reserve (a synchronous hook deadlocks). The choice: keep the card's mechanism and state that a reservation is not a gated operation and the store's append takes no gate (RW-1, RW-4), with the hooks reserving through that append from a thread that holds no gate (RW-3, RW-5, RW-9).
2. **WAL-START-PAIR-SPLIT-READ was narrowed on a false claim.** "A split that reads the chain head first gives a correct pair" fails on a build that reserves a hop's `SegmentEnd` in an entry of its own: a first `Chunk` whose `SegmentEnd` lands between the two reads and whose own entry follows them leaves `wal_start_lsn` at the next segment's first LSN, paired with the record before the `SegmentEnd`, a false splice. That form of the 7.12 mutant is restored with its hook (RW-2, RW-3, RW-12).
3. **A-6c's tracker form cannot catch a wait it does not track** (a spin): the creation is held only "for a while". RW-6 to RW-8 take the implementer's fix.
4. **The open-chunk set's leaving rule** never requires a member that ends with no marker to leave, says the flip "may" drop marked members, leaves the set's state at open unstated, and "a transaction that aborted before the flip does not count" (§12.6) contradicts any removal made after the end. A build that keeps such members never recycles the WAL, and no test sees it. RW-16 to RW-18 state the rule; S-GAMMA-F1 adds its mutant and test.
5. **Wording taken in the same pass** (this card's own C-T0 lines, which have no other carrier): §9.1 lost the unlogged DropTree's raise, and the 8.6 status line and Changelog say "a DropTree's mark" for the logged one (RW-13 to RW-15).

The rest holds: the DropTree `w` (bounds, one value, the codec form, both new mutants on Phase A rows), the splice-cut wording and the tightened read-only fixtures, the leaf rule, the riders, and the placement.

## Gate run

- Extract: shallow `--no-local` clone of `nucleus/v3-scaffold`, scaffold id `264d44b818903701ac52c647b2e18869394def13`; `fresh-gate.sh` and `git-env.sh` extracted; no `EXTRACT FAILED`.
- `bash "$fg/fresh-gate.sh" f654984997460c5218629d651d90c124b3fd71c5`, stdout and stderr captured (stderr empty):
  - `fresh-gate: commit f654984997460c5218629d651d90c124b3fd71c5`
  - `fresh-gate: base 264d44b818903701ac52c647b2e18869394def13`
  - `fresh-gate: checks passed (4815 files hashed in 5.84 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - literal last line: `GATE EXIT=0` (883 `ok` lines, 56 `test result: ok`, `STEP enforcement-tests EXIT=0`).
- `$fg` removed. The worktree was clean at f6549849 before and after; no verdict commit sat on top.

## Scope and bytes (each by this gate's own script)

- `git diff nucleus/v3-scaffold...HEAD --stat`: `docs/C-T0-txn-protocol.md` (8 lines) and `docs/S-STORE.md` (85), the card's Touch only. Base sha256s match the card (`233ad388…53ad`, `11783e36…3c84`).
- The card's Edits parsed by my own script: 54 Old/New pairs under 47 headings, applied in the card's order to the 264d44b8 files with `<date>` = 2026-11-15. Every Old occurs once at its turn, and the result is byte-identical to both committed files.
- The riders: the 18 fenced blocks of "Bundle γ riders" in `verdicts/S-BETA.md` at 264d44b8 equal the card's 18 rider blocks byte for byte.
- Status lines: the text after `on draft 7.12 (card S-BETA, ` and after `on draft 8.5 (card S-BETA, ` is the base line's, byte for byte.
- Placement (own script over §15 and §15.1 at 7.8 = 30860cf1, 7.12 and 7.13): §15 mutants 211 → 215, none removed; §15.1 rows 148 → 152, none removed, no duplicate (test, row, card) key; no (mutant, row) judgement of 7.12 lost; the 25 Phase-B-only mutants are 7.12's 25; of the 152 mutants on a row pinned by a Phase A card at 7.8, the 8 not on a Phase A row now are the checker's existing exceptions (POISON-IGNORED, XX001-NOT-POISON, CLIENT-READ-POISONS, POISON-WAITER-PARKED, WAL-ERR-NOT-POISON, CREATE-TREE-SPLIT, LOAD-PAGE-SPLIT, ROOT-NAME-KEPT). The four new mutants are judged on Phase A rows (A-7d; A-6c, A-7c; A-7d, B-6; A-6a, A-7d).
- Depends, parsed from PLAN-v3 §9 itself (parenthetical notes dropped): no cycle; the four new rows' Needs lie in their card's closure (A-6c ⊇ A-6b; A-7c ⊇ A-6c, A-6b, A-4c0; A-7d ⊇ A-7c, A-6d, A-1b).
- W-FLOOR-META-ONLY judged only at B-6: **acceptable.** It mutates C-T0 §3.7 step 3, B-3's code, and no Phase A card runs C-T0's restart; B-3's own `t_w_floor_after_restart` replays no drop, where `gc_w_used` equals the meta's `gc_w`, so the mutant is equivalent there; its kill needs a replayed drop, B-6's job and B-4a's catalog. It is a C-T0 mutant, outside S-STORE §15's set.
- The card's Acceptance, run read-only in the worktree: all 69 lines but `gate.sh` pass; the checker prints `152 rows; 215 mutants; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `46 new since 7.10 (31 at 7.11, 11 at 7.12, 4 here), 46 of them judged on a Phase A row; 47 invariants, 47 tests`, `OK`.
- On a scratch clone with RW-1 to RW-18 applied (outside the repository): the card's Acceptance with line 5 replaced and AC-2 to AC-14 added passes: 83 lines, all but `gate.sh` run, none fails, and the checker prints the same three lines. AC-1 to AC-14 all fail on f6549849.

## Item 1: does the race exist? Ruling (C)

**Which operations hold the gate when a `Chunk` is reserved, path by path** (drafts 7.12 and 7.8 alike):
- *The gate's own list.* S-STORE §5.6: the gate "is held **shared** by every gated operation: every tree mutation, including its splits; every pack step, root split and root collapse; every GC page mutation and every LPID free; every directory change; every dirty eviction; every apply insert", and "Each of these holds the gate from its first page mutation to its last." No WAL reservation is listed. §12.4: "Every reservation, of every kind, `Chunk` and `SegmentEnd` included, is one entry to the **append critical section** … Any mechanism that makes each reservation one indivisible step will do." No gate.
- *Materialisation* (C-T0 §3.2). "**Materialize(k)** writes `k`'s current top data layer as two things: 1. an op in a `Chunk` record …; 2. `T`'s provisional entry on `k`", and "**Gate holds.** Each materialization is a gated operation, or a bounded batch of them." That gates the materialisation's page mutations. It does not place the record's reservation inside them: a `Chunk{seq u32, ops}` record holds the ops of many materialisations and is reserved once, with its `len` fixed then (§12.2's design note), and "it appends its next batch only after the previous one is synced" (WAL head-of-line) is a wait no build keeps under the gate. A build may reserve the record inside the gated batch, or before or after it.
- *Chunks whose ops change no page.* §12.3: `RequireCkpt` "changes no tree"; §12.8: "A chunked publishing transaction carries the op in a `Chunk` written after that meta is durable." C-T6-rehost §5.3: a DDL runs CreateTree "eagerly, at execution, without a WAL record", and "a large txn logs it in a `Chunk` written **at or before the first chunk that holds an op on the tree**"; §5.5: a large txn puts its `Ddl` ops "in the chunks written at step 1′", and "Recovery ignores it". Such a `Chunk` (a first one included) involves no gated operation at all.
- *Commit and CommitMarker.* Reserved inside the I-WAL-TS-ORDER section, which C-T0 §2.6 says is "never held while acquiring … the epoch gate"; step 0 excludes them by holding that section, not the gate.
- *Replay.* Recovery reserves nothing, and C-T0 §2.5: "No txn starts (and so no `TxnId` is issued and no `Chunk` is appended) until the boot checkpoint's meta is synced." No race at boot.
- *Step 0.* §13: item 1 "Take the epoch gate exclusively", item 3 "Release the gate"; every read of item 2 (`T`, `gc_w`, the pair, the position, `wal_ckpt_end`, `ts_ckpt_hi`, and at 7.13 the set) lies inside that hold. And since 7.8, item 2 itself says: "`Chunk` reservations need not be made inside the critical section: a chunk appended concurrently with the read may fall either side of `wal_ckpt_end`". The read is made under the exclusive gate, so that sentence describes a `Chunk` appended with no gate held.

**So the spec leaves it open, and the answer depends on the build.**
- *A build whose first-`Chunk` reservation holds no gate* (allowed since 7.8, and invited by the `CreateTree`, `Ddl` and `RequireCkpt` chunks), with step 0 reading the set apart from the position (7.12 does not forbid it): step 0 holds the gate **exclusively** from item 1; in item 2 it takes `T` and reads the set, empty; `X`'s thread, **holding no gate**, enters the append section and reserves `X1` at `q = p0`, and `X` enters the set too late; step 0, still holding the gate exclusively and inside its I-WAL-TS-ORDER hold, reads the position `p1 = q + len(X1)` and the chain head `X1`; `wal_start_lsn = p1` with a consistent pair; item 3 releases the gate; steps 1–5 make meta `k` durable; `X` reserves `X2`…`Xn` and its marker at `c > T` and is acked once `durable_end` passes the marker; a power cut; recovery from meta `k` walks from `p1` and replays `X` without `X1` (pass 2 checks no `nchunks`). D1's trace is real here.
- *A build that reserves every `Chunk`, and records its first in the set, inside a gated operation:* that operation cannot run between items 1 and 3, so `X1` lies wholly before step 0's reads (and is in the set) or wholly after them (at or above the position). No loss, whatever order step 0 reads in.

**The record.** S-BETA's verdict at 5cb70935 ("Implementer item 4 is a real silent loss of an acked commit, present since draft 7.8") stands as a finding about the spec: 7.8–7.12 admitted a conforming build that loses an acked commit. Its qualifier ("on a build that reads §12.6's set apart from the append position") is incomplete: the build must also reserve the first `Chunk` with no gate held. The implementer's alternative, that the finding is wrong for every conforming build, does not hold: no text requires a gated reservation, and the `CreateTree`, `Ddl` and `RequireCkpt` chunks have no page mutation to gate. 7.13's Changelog and status line say "could", which is right; RW-10 and RW-11 add the condition.

**The choice, and why.** Keep the card's mechanism (the open-chunk set in the append section, its first `Chunk` entered in the reservation's entry, step 0 reading it in its one entry) and state that a reservation is not a gated operation: the store's append takes no gate, and a caller may hold the gate around a `Chunk`'s or not (RW-1, RW-4).
- §12.6's second term needs every open chunked transaction's first-`Chunk` LSN in every design. The "machinery" is only where that set lives and when it changes, so dropping it saves nothing.
- The one-entry rule is enforced at the one place every reservation passes, whatever its caller holds. The alternative, every `Chunk` reservation and its set entry inside a gated operation, makes the replay start depend on every caller's gate discipline (C-T0's materialisation, C-T6-rehost's `CreateTree` and `Ddl` chunks, §12.8's `RequireCkpt` chunk, and any later path), adds gated operations that mutate no page, against §5.6's definition, and its mutant (an ungated reservation) would be equivalent on any build that reads the set with the position anyway.
- With the store's append gate-free, the A-7d hooks can reserve a first `Chunk` from a thread that holds no gate while step 0 holds the gate exclusively, on every conforming build: START-LSN-SPLIT-READ (both forms) and WAL-START-PAIR-SPLIT-READ are killable everywhere, and no hook blocks or deadlocks on the gate.
- The same applies to `t_wal_chain_crash`'s racing step, which 7.12 already had: on a gated-reservation build its hook could not reserve inside step 0 either. RW-3 fixes both.

## Items 2–6, ruled

**2. The leaving rule.** Sound in every interleaving the implementer traced, which I re-walked: a first `Chunk` before step 0's entry (in the set, kept: its marker is above `T` or absent) or after it (at or above the position read); an abort (no marker ever; its chunks are ignored in recovery); a marker at or below `T` (`c ≤ T = M`, so the image holds `X` whole); a marker reserved between step 0's taking of `T` and its entry (`c > visible_ts ≥ M ≥ T`, counted); two consecutive flips (`M` is monotonic, so a drop at flip `k′` needs `c ≤ T_k′ ≤ T`); a flip whose meta never becomes durable (every later flip has `T ≥ c`, and the previous durable meta counted `X` when it had to; step 6 recycles only below the previous meta's start); a first `Chunk` that hops (its own LSN enters the set; the pair is its own `prev`, the `SegmentEnd`). Real gaps, all fixed by RW-16 to RW-18: nothing requires a member that ends with no marker to leave, so a build that keeps it holds `wal_start_lsn` back for ever and the WAL is never recycled, which no test detects (my own mutant G4); "may drop" lets marked members accumulate; the set's state at open is unstated; and §12.6's "A transaction that aborted before the flip does not count" contradicts a removal made in an entry after the end, the natural build. RW-16 makes a member that ends with no marker leave in an entry of its own as it ends, says a flip before that entry still counts it (safe), makes the flip's entry drop marked members, and makes the set empty at open (C-T0 §2.5: no transaction before the boot meta); RW-17 says the same in the "does not count" paragraph; RW-18 fixes §13 step 0's "may drop". The mutant and its test are S-GAMMA-F1.

**3. `t_append_section_leaf`.** The A-7c shuttle form is sound: under the mutant the hop holds the section waiting on the held creation, step 0 holds the gate and waits on the section, the gated operation waits on the gate and the hook waits on that operation; every schedule deadlocks (a spinning wait exceeds the harness's step bound), and a correct build finishes. The A-6c tracker form is not: a wait that makes no `Env` call and takes no tracked lock (a spin on an atomic) passes, because the creation is held only "for a while" (my own mutant G3). RW-6 to RW-8 hold the creation until the hopping reservation has returned and fail the test if it has not returned within a bound, so every wait form fails A-6c whatever the wait is; the I/O form still trips the `Env` hook.

**4. The DropTree watermark, as WAL format design.** Sound.
- *Bounds* re-derived: `w > S_ddl` (the job reads `w` no earlier than it finds `W > S_job ≥ S_ddl`); `w ≤ T` at every later flip (`w ≤ W` at the mark `≤ M ≤` any later `T`, so GCW-BOUND-SKIP's check is never reached by a real mark); `w < c ≤` the recovered frontier, which the boot checkpoint takes as `T`; the floor after a replay never exceeds the `W` published before the crash; the fall-back and rollback cases hold with `w`.
- *One value (P-2):* safe, and it removes the store's read of `W` inside a logged drop. It is pinned only in replay: a build that keeps 7.12's published `W` in normal operation passes every test (my own mutant G5, safe), hence the optional S-GAMMA-F4.
- *A drop with no logged op (P-5):* it keeps the `W` published at its mutation, is never replayed, and no committed catalog row names its tree; sound. C-T0 §9.1 no longer said so after T1: RW-13.
- *The mutants:* DROP-OP-W-ZERO fails A-6a's round trip and A-7d's (d) and (e) (the opened meta's `gc_w < S_ddl < w`); REPLAY-DROP-AT-TS fails A-7d's (d) and (e) (S3 supplies `w < c − 1`) and B-6's exact `gc_w_used` check. B-6's AS OF "at the reopened `W`" kills it only if pinned at the op's `w`, since the mutant's reopened `W` is `c − 1`, which an AS OF read at the dynamic `W` admits: S-GAMMA-F3. D-11's "user-visible half" needs that.
- *Does the field foreclose anything?* No. A-6a is not cut, and A-6a's, A-2's and A-7c's formats carry no compatibility promise until A-7e. It is 8 bytes per dropped tree, in small drop-cleanup commits only (C-T6-rehost §7.4: "one small commit"). Replicas and CDC see the primary's `w`, which is also a correct floor for them (`w < c`, and they apply `c`). A decoder may treat `w = 0` as corrupt, since `w > S_ddl ≥ 1`. The two-value shape stays available by ignoring `w` in normal operation.
- *Wording:* the 8.6 status line and Changelog say "a DropTree's mark" for the logged one (RW-14, RW-15). C-T6-rehost §7.4's "`DropTree{t}`" is the plan card's (P-4, item 13); §5.6's three "`DropTree{t}`" are pattern notation and need nothing.

**5. The splice cut.** The rule is right as kept: every overwriting record that the walk accepts chains to its predecessor, so no record shows where an overwrite of two or more records began, and the only lower cut is the walk's start, which would discard every intact record above `T` on every splice. The 7.11 text overclaimed; the change is wording, and the residual is §12.5's undetected splice. The tightened read-only fixtures are passable and keep their kills: with the overwrite beginning at `m`, the cut is `m`, every record below it is original, and the replaced commits' partial image entries have ts above every record below the cut, so the oracle "the model over the records below the cut" is exact. SPLICE-SALVAGE-PAST-CUT serves records past the cut; SPLICE-CUT-SAME-START-KEEP (one record overwritten, the walk's predecessor, a `Commit` with data) serves the kept predecessor's data; SPLICE-MARKER-PAST-CUT, with the lower-ts `Commit` and the marker as overwriting records at and past `m` and the chunks below it, applies the chunks and misses the `Commit`. Each fails as at 7.12.

**6. The riders.** R1–R7 are the verdict's bytes, applied first; G8–G12 and G31 touch disjoint text, and "both AS OF snapshots" (R1) stays true. Nothing undoes or contradicts them. R5's B-3 wording matches C-T0 §3.7 step 3; W-FLOOR-META-ONLY's kill is consistent with 7.13's `w` (the meta's `gc_w < S_ddl < w`). Judged only at B-6, acceptably (Scope, above).

**The leaf rule (implementer item 5).** §12.4 is the right home (D-5, P-3): S-STORE states each leaf lock where its state is defined (§6.8's root table, §13 step 2's image-record list), and §4.2 ranks the free-set lock because that lock guards §4.2's state; no §4.2 cross-reference is needed. The entry list ("the epoch gate, frame latches … and the I-WAL-TS-ORDER critical section, with what C-T0 v8 §2.6 ranks before that section") is broad enough read through C-T0 §2.6, which ranks the lock-table entries and the settle stripe before every short mutex, but the card's own gloss narrows it to the SSI mutex; S-GAMMA-F2 lists them. Nothing that 7.12 or 7.13 puts under the section takes a lock or waits, and "how the segment is made instead" adds nothing beyond §12.5's writer rule.

## Tests that cannot pass, and mutants that cannot fail (at f6549849)

- `t_flip_chunk_race` (A-7d) and `t_wal_chain_crash`'s racing step (A-7d): on a conforming build whose `Chunk` reservation holds the gate, their hooks inside step 0's exclusive hold cannot reserve; a synchronous hook deadlocks, so the test cannot pass. START-LSN-SPLIT-READ (both forms) and WAL-START-PAIR-SPLIT-READ are equivalent there. Fixed by RW-1, RW-3, RW-5, RW-9.
- WAL-START-PAIR-SPLIT-READ, other order: dropped from the mutant on a false claim; restored by RW-2 and RW-3, equivalent (stated) where a hop is one entry, as R3 states for a writer with no cut point.
- APPEND-SECTION-IO's wait form at A-6c: survives an untracked wait. Fixed by RW-6 to RW-8.
- No other new or changed test is unpassable: `t_drop_restart_floor`'s six variants and codec form, `t_append_section_leaf` at A-7c, the tightened `t_wal_splice` forms and R3's skip all pass on a correct build.

## Rework: one list (RW-1 to RW-18), then the card's Acceptance

Apply in this order to the files at f6549849 (S-STORE draft 7.13, C-T0 draft 8.6 as committed). Each Old occurs exactly once in its file, also before any other block is applied, and no two Olds overlap (checked by script). The drafts stay 7.13 and 8.6, and the counts stay 152 rows, 215 mutants and 47 tests: no block adds a mutant, a test or a row. Fences here are four tildes; the card may use three backticks.

### RW-1. S-STORE §12.4 "Append": a reservation is not a gated operation

File: docs/S-STORE.md

Old:
~~~~text
Any mechanism that makes each reservation one indivisible step will do. The section also keeps
~~~~
New:
~~~~text
Any mechanism that makes each reservation one indivisible step will do. A reservation is not a gated operation (§5.6), and the store's append takes no gate: a caller may reserve a `Chunk` while it holds the gate shared, inside a gated materialisation (C-T0 v8 §3.2), or while it holds none, since a `Chunk` may carry ops that change no page (a `RequireCkpt`, §12.8; a large transaction's `CreateTree` and `Ddl` ops, C-T6-rehost §5.3, §5.5). So a `Chunk` may be reserved while step 0 holds the gate exclusively, and the open-chunk set, not the gate, keeps §12.6's terms right. The section also keeps
~~~~

### RW-2. S-STORE §12.5, WAL-START-PAIR-SPLIT-READ: the other order of the split

File: docs/S-STORE.md

Old:
~~~~text
A split that reads the chain head first gives a correct pair and is not this mutant: the set, read with the position, counts the chunk, so `wal_start_lsn` is the chunk's own LSN and the pair is its own `prev_lsn` and `prev_crc`;
~~~~
New:
~~~~text
Or step 0 reads the chain head in an earlier entry than the append position and the open-chunk set. Then a `Commit`, `CommitMarker` or `Chunk` reserved between the two reads holds `wal_start_lsn` at or below its own LSN (a first `Chunk` through the set read with the position), so the pair, the chain head read first or the own `prev_lsn` and `prev_crc` of the record at `wal_start_lsn`, is correct, except on a build that reserves a hop's `SegmentEnd` in an entry of its own (§12.4): there a new chunked transaction's first `Chunk` that hops, with its `SegmentEnd`'s entry between the two reads and its own entry after them, can leave `wal_start_lsn` at the next segment's first LSN while the pair names the record before that `SegmentEnd`, which the record there does not name. On a build that reserves each hop in one entry, this order is equivalent;
~~~~

### RW-3. S-STORE §12.5, `t_wal_chain_crash`'s racing step: the hop, and no gate

File: docs/S-STORE.md

Old:
~~~~text
and otherwise just before and just after step 0's one entry (§13 step 0);
~~~~
New:
~~~~text
and otherwise just before and just after step 0's one entry (§13 step 0); on a build that reserves a hop's `SegmentEnd` in an entry of its own, the hook also makes that `Chunk` hop, with its `SegmentEnd`'s entry between step 0's read of the chain head and its read of the append position and the open-chunk set wherever an implementation reads the chain head first, and otherwise just before step 0's one entry, and with the `Chunk`'s own entry after them; the hook reserves through the store's append from a thread that holds no gate (§12.4);
~~~~

### RW-4. S-STORE §12.6, "Why one entry": the gate

File: docs/S-STORE.md

Old:
~~~~text
and a `Chunk` reservation needs neither (§13 step 0).
~~~~
New:
~~~~text
and a `Chunk` reservation needs neither: it is not a gated operation (§12.4), and it need not be made inside that section (§13 step 0). A `Chunk` reserved by a caller that holds the gate cannot fall inside step 0, but one reserved by a caller that holds none can.
~~~~

### RW-5. S-STORE §14, START-LSN-SPLIT-READ's test: the hook holds no gate

File: docs/S-STORE.md

Old:
~~~~text
a hook reserves a new chunked transaction `X`'s first `Chunk`, at `q`,
~~~~
New:
~~~~text
a hook reserves a new chunked transaction `X`'s first `Chunk`, through the store's append from a thread that holds no gate (§12.4), at `q`,
~~~~

### RW-6. S-STORE §15 I-LATCH row: A-6c's held creation

File: docs/S-STORE.md

Old:
~~~~text
whose creation a hook holds for a while;
~~~~
New:
~~~~text
whose creation a hook holds until the hopping reservation has returned, and the test fails if it has not returned within a bound;
~~~~

### RW-7. S-STORE §15 I-LATCH row: how A-6c's form fails the mutant

File: docs/S-STORE.md

Old:
~~~~text
APPEND-SECTION-IO fails A-6c's form through the hook, and deadlocks A-7c's
~~~~
New:
~~~~text
APPEND-SECTION-IO fails A-6c's form, its I/O form through the hook and its wait form, however it waits, by not returning while the creation is held, and deadlocks A-7c's
~~~~

### RW-8. S-STORE §15.1, `t_append_section_leaf`'s A-6c row

File: docs/S-STORE.md

Old:
~~~~text
reservations of every kind across hops into segments whose creation a hook holds | APPEND-SECTION-IO (its I/O and its wait form, through the hook) |
~~~~
New:
~~~~text
reservations of every kind across hops into segments whose creation a hook holds until the hopping reservation has returned, within a bound | APPEND-SECTION-IO (its I/O form through the hook, its wait form by not returning while the creation is held) |
~~~~

### RW-9. S-STORE §15.1, `t_flip_chunk_race`'s row

File: docs/S-STORE.md

Old:
~~~~text
the hook at each point a build separates,
~~~~
New:
~~~~text
the hook at each point a build separates, reserving through the store's append with no gate held,
~~~~

### RW-10. S-STORE status line

File: docs/S-STORE.md

Old:
~~~~text
the append critical section now keeps the open-chunk set,
~~~~
New:
~~~~text
a reservation is not a gated operation, so a `Chunk` reserved with no gate held can fall inside step 0; the append critical section now keeps the open-chunk set,
~~~~

### RW-11. S-STORE Changelog 7.13, first bullet: what was lost, and on which builds

File: docs/S-STORE.md

Old:
~~~~text
Nothing said when step 0 reads §12.6's set of first chunks, so a first `Chunk` reserved between that read and the read of the append position could lie below `wal_start_lsn` uncounted, and a crash after its transaction committed and was acked replayed the transaction without that chunk, silently: a loss of an acknowledged commit present since draft 7.8.
~~~~
New:
~~~~text
Nothing said when step 0 reads §12.6's set of first chunks, and nothing kept a `Chunk`'s reservation out of step 0, so on a build that reserved a first `Chunk` with no gate held, between that read and the read of the append position, the `Chunk` could lie below `wal_start_lsn` uncounted, and a crash after its transaction committed and was acked replayed the transaction without that chunk, silently: a loss of an acknowledged commit present since draft 7.8. A build that reserved every `Chunk`, and recorded a transaction's first one, inside a gated operation could not lose it, since step 0 holds the gate exclusively; §12.4 now states that a reservation is not a gated operation.
~~~~

### RW-12. S-STORE Changelog 7.13, first bullet: the pair's split

File: docs/S-STORE.md

Old:
~~~~text
WAL-START-PAIR-SPLIT-READ and `t_wal_chain_crash`'s racing step now name the split that still fails: the chain head read after the position and the set.
~~~~
New:
~~~~text
WAL-START-PAIR-SPLIT-READ and `t_wal_chain_crash`'s racing step now say how each order of the split fails once the set is read with the position: the chain head read after them pairs a racing first `Chunk` with itself, and read before them it goes stale across a first `Chunk`'s hop whose `SegmentEnd` is an entry of its own. The racing hooks of both tests reserve with no gate held. A transaction leaves the set as it ends with no marker, or at the first flip whose `T` is at or above its marker, and the set is empty at open (§12.6).
~~~~

### RW-16. S-STORE §12.6, "When the terms are taken": how a member leaves, and the set at open

File: docs/S-STORE.md

Old:
~~~~text
that entry may drop it.
~~~~
New:
~~~~text
that entry drops it. One that ends with no marker leaves in an entry of its own as it ends, and a flip whose entry comes before that one still counts it, which is safe. The set is empty at open: no transaction starts before the boot checkpoint's meta is durable (C-T0 v8 §2.5), so no recovered transaction is ever a member.
~~~~

### RW-17. S-STORE §12.6, the transactions that do not count: until they leave the set

File: docs/S-STORE.md

Old:
~~~~text
It writes no marker, ever, so recovery ignores its chunks exactly as it ignores an aborted transaction's, and nothing durable depends on them.
~~~~
New:
~~~~text
It writes no marker, ever, so recovery ignores its chunks exactly as it ignores an aborted transaction's, and nothing durable depends on them. Either still counts at a flip whose entry precedes the one in which it leaves the open-chunk set (above), which only retains more.
~~~~

### RW-18. S-STORE §13 step 0, item 2: the entry drops

File: docs/S-STORE.md

Old:
~~~~text
The entry may drop from the set every transaction whose marker's commit_ts is at or below `T` (§12.6).
~~~~
New:
~~~~text
The entry drops from the set every transaction whose marker's commit_ts is at or below `T` (§12.6).
~~~~

### RW-13. C-T0 §9.1: an unlogged DropTree's raise

File: docs/C-T0-txn-protocol.md

Old:
~~~~text
the published `W` that the drop-cleanup job read (S-STORE §6.8 step 1, §10.5 "Drops", §12.3), and the meta carries it
~~~~
New:
~~~~text
the published `W` that the drop-cleanup job read (S-STORE §6.8 step 1, §10.5 "Drops", §12.3), an unlogged DropTree's mark (C-T6-rehost §5.3) raises it to the `W` published at its mutation (S-STORE §6.8 step 1), and the meta carries it
~~~~

### RW-14. C-T0 status line

File: docs/C-T0-txn-protocol.md

Old:
~~~~text
§9.1 says that a DropTree's mark raises `gc_w_used` to the watermark its op carries
~~~~
New:
~~~~text
§9.1 says that a logged DropTree's mark raises `gc_w_used` to the watermark its op carries
~~~~

### RW-15. C-T0 Changelog 8.6

File: docs/C-T0-txn-protocol.md

Old:
~~~~text
§9.1: a DropTree's mark raises `gc_w_used` to the `w` its op carries, the published `W` that the drop-cleanup job read (S-STORE §12.3).
~~~~
New:
~~~~text
§9.1: a logged DropTree's mark raises `gc_w_used` to the `w` its op carries, the published `W` that the drop-cleanup job read (S-STORE §12.3), and an unlogged one's to the `W` published at its mutation, as before.
~~~~

### The card's Acceptance

- **AC-1.** RW-16 changes "may drop it", so the card's Acceptance line

  ~~~~text
  - `cd nucleus/v3 && grep -n 'A transaction leaves the set only once it has ended with no marker (below), or once a flip.s entry has read it there with a marker whose commit_ts is at or below that flip.s .T.; that entry may drop it\.' docs/S-STORE.md`
  ~~~~
  becomes
  ~~~~text
  - `cd nucleus/v3 && grep -n 'A transaction leaves the set only once it has ended with no marker (below), or once a flip.s entry has read it there with a marker whose commit_ts is at or below that flip.s .T.; that entry drops it\. One that ends with no marker leaves in an entry of its own as it ends' docs/S-STORE.md`
  ~~~~
- **AC-2 to AC-14**, added just before the checker line (no backtick in any command; each fails on f6549849 and passes after RW-1 to RW-18):

  ~~~~text
  - `cd nucleus/v3 && grep -n 'The set is empty at open: no transaction starts before the boot checkpoint.s meta is durable (C-T0 v8 §2\.5)' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'The entry drops from the set every transaction whose marker.s commit_ts is at or below .T. (§12\.6)\.' docs/S-STORE.md`
  - `cd nucleus/v3 && ! grep -n 'may drop' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'A reservation is not a gated operation (§5\.6), and the store.s append takes no gate: a caller may reserve a .Chunk. while it holds the gate shared, inside a gated materialisation (C-T0 v8 §3\.2), or while it holds none' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'and a .Chunk. reservation needs neither: it is not a gated operation (§12\.4), and it need not be made inside that section (§13 step 0)\.' docs/S-STORE.md`
  - `cd nucleus/v3 && test "$(grep -c 'through the store.s append from a thread that holds no gate (§12\.4)' docs/S-STORE.md)" = 2`
  - `cd nucleus/v3 && ! grep -n 'A split that reads the chain head first gives a correct pair' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'Or step 0 reads the chain head in an earlier entry than the append position and the open-chunk set\. .* On a build that reserves each hop in one entry, this order is equivalent;$' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'on a build that reserves a hop.s .SegmentEnd. in an entry of its own, the hook also makes that .Chunk. hop' docs/S-STORE.md`
  - `cd nucleus/v3 && grep -n 'whose creation a hook holds until the hopping reservation has returned, and the test fails if it has not returned within a bound' docs/S-STORE.md`
  - `cd nucleus/v3 && sed -n '/^### 15\.1 /,/^## 16\. /p' docs/S-STORE.md | grep -q '^| .t_append_section_leaf. | I-LATCH | A-6c | A-6b | .* until the hopping reservation has returned, within a bound | APPEND-SECTION-IO (its I/O form through the hook, its wait form by not returning while the creation is held) |$'`
  - `cd nucleus/v3 && grep -n 'an unlogged DropTree.s mark (C-T6-rehost §5\.3) raises it to the .W. published at its mutation (S-STORE §6\.8 step 1)' docs/C-T0-txn-protocol.md`
  - `cd nucleus/v3 && ! grep -n 'says that a DropTree.s mark raises\|§9\.1: a DropTree.s mark raises .gc_w_used. to the .w.' docs/C-T0-txn-protocol.md`
  ~~~~
- The checker line, the file-list line and `gate.sh` are unchanged.

### The card's prose (non-normative, for the orchestrator)

The card's text that RW-2, RW-3 and RW-16 make stale: Goal item 1's "on a build that …" qualifiers stay right; "Never weakened", bullet 3 ("WAL-START-PAIR-SPLIT-READ's text is narrowed … The other order of split was a failure only while the set was read apart from both") and D1's "So this split is equivalent (safe), and the mutant is narrowed to the other order (G17)" are superseded by RW-2: the other order is restored, equivalent only where a hop is one entry. D-2's "A member leaves only at markerless end or at a flip" now reads "leaves as it ends with no marker, or at the first flip at or above its marker, and the set is empty at open". "For the gate" item 2's racing-step bullet gains the hop form.

## Follow-ups (ride in S-DELTA; exact text against 7.13 after RW-1 to RW-18; each Old occurs once there, and all eleven apply together, checked by script)

- **S-GAMMA-F1: a mutant and a test for the leaving rule** (before A-6d is cut; the leave at a markerless end is A-6d's chunk path, the test A-7d's). RW-16 states the rule, but nothing tests it, and a build that keeps a member which ended with no marker stops the WAL from ever being recycled after one aborted large transaction (my own mutant G4). Mutant CHUNK-SET-KEEPS-ENDED, judged at A-7d by `t_flip_chunk_race`'s new variant *ended*; S-DELTA's checker adds it to the new set with that placement. Blocks RW-F1-1 to RW-F1-3 below.
- **S-GAMMA-F2: the leaf rule's entry list** (before A-6c is cut). G15 lists "the epoch gate, frame latches … and the I-WAL-TS-ORDER critical section, with what C-T0 v8 §2.6 ranks before that section", which the card's own gloss reads as the SSI mutex only; every committing writer holds its lock-table entries logically, and a materialising settler may hold a settle stripe. Name them. Block RW-F2-1.
- **S-GAMMA-F3: B-6's second AS OF at the op's `w`** (before B-6). With the AS OF taken at the reopened `W` read at run time, REPLAY-DROP-AT-TS's reopened `W` is `c − 1` and that AS OF is served, so only the exact `gc_w_used` check kills it; pinned at `w`, the mutant's floor refuses it, the user-visible kill D-11 describes. Blocks RW-F3-1, RW-F3-2.
- **S-GAMMA-F4 (optional): pin the one value in normal operation** (before A-7c is cut). G3 makes a logged drop's mark raise `gc_w_used` to the op's `w` in normal operation, but (a)–(c) check only "at least `w`", so a build that keeps 7.12's published `W` there passes every test (my own mutant G5). Safe, so the owner or S-DELTA may waive it; taken, it adds DROP-MARK-AT-PUBLISHED-W, judged at A-7c by an exact (a). Blocks RW-F4-1 to RW-F4-5.

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

## Corrections of record (for the orchestrator; this gate edits neither)

- **`verdicts/S-BETA.md` at 5cb70935**, "Implementer item 4 is a real silent loss of an acked commit, present since draft 7.8": right about the spec, incomplete about the build. The loss needs a first `Chunk` reserved with no gate held, which 7.8–7.12 allowed, as well as a set read apart from the position; a build that reserves every `Chunk`, and records its first, inside a gated operation cannot lose it. The verdict is history and stays; this verdict is the correction of record.
- **QUEUE S-BETA-F1**: the CAUTION can be replaced by: "S-GAMMA gate ruled (C): real on a build whose first-`Chunk` reservation holds no gate and whose step 0 reads the set apart from the position (7.8–7.12 allowed both); impossible where every `Chunk` and its set entry are inside a gated operation. S-GAMMA is reworked to state that a reservation takes no gate (RW-1), so the fix and its tests hold on every build." S-BETA-F1, F2, F3 and O1 close when the reworked S-GAMMA merges.

## What S-DELTA must change

1. **Rebase onto the reworked 7.13.** Besides the card's "Rebase points", the rework changes: §12.4 "Append" (RW-1); §12.5's WAL-START-PAIR-SPLIT-READ and the racing step (RW-2, RW-3); §12.6's "When the terms are taken", "Why one entry" and the "does not count" paragraph (RW-16, RW-4, RW-17); §13 step 0 item 2's last sentence about the set (RW-18); §14's START-LSN-SPLIT-READ test (RW-5); §15's I-LATCH row (RW-6, RW-7); §15.1's `t_append_section_leaf` A-6c row and `t_flip_chunk_race` row (RW-8, RW-9); the status line and the 7.13 Changelog's first bullet (RW-10 to RW-12); and C-T0 §9.1, its status line and its 8.6 entry (RW-13 to RW-15). S-DELTA's Olds must not span those texts.
2. **Carry S-GAMMA-F1, F2 and F3**, and F4 unless it is waived, with the exact blocks above.
3. **Its checker** starts from S-GAMMA's, which the rework leaves unchanged. NG gains CHUNK-SET-KEEPS-ENDED (placed on A-7d) and, if F4 is taken, DROP-MARK-AT-PUBLISHED-W (on A-7c), and the counts become 216 or 217 mutants. F1 and F4 change text that four of S-GAMMA's Acceptance greps match (the I-REDO-COMPLETE row, "After each reopen …", "REPLAY-DROP-AT-TS fails (d) and (e) …" and the I-GCW row), so its own Acceptance supersedes them.

## What the plan card must change

The card's "For the plan card" items 1–13 stand, with these additions:
- **A-6c:** the store's append takes no gate (§12.4, RW-1), which the leaf rule and both racing tests rely on; `t_append_section_leaf`'s tracker form holds the creation until the hopping reservation has returned, within a bound. Depends gains S-GAMMA, and S-DELTA for S-GAMMA-F2.
- **A-6d:** a member that ends with no marker leaves the open-chunk set in an entry of its own as it ends (RW-16). Depends gains S-DELTA for S-GAMMA-F1.
- **A-7c:** step 0's entry drops every member whose marker is at or below `T` (RW-18, "drops", not "may"); F4's exact (a) if taken.
- **A-7d:** `t_flip_chunk_race`'s hook reserves through the store's append from a thread that holds no gate, and F1's variant *ended*; WAL-START-PAIR-SPLIT-READ has two forms, the chain-head-first one killed by the racing step's hop form on a build that reserves a hop's `SegmentEnd` in an entry of its own (equivalent, as stated, where a hop is one entry); the racing step's hook also holds no gate.
- **B-6:** the second AS OF in (d) and (e) at the op's `w` (S-GAMMA-F3).
- **C-T6-rehost §7.4** at its next edit: "`DropTree{t, w}` for each of the row's trees, `w` the `W` the job found above `S` (S-STORE §12.3)" (card item 13, P-4). §5.6's "`DropTree{t}`" is pattern notation and stays.
- **QUEUE:** the S-BETA-F1 correction above; S-GAMMA's row records REWORK and this verdict's commit.

## The owner's points, in plain words

1. **The 8-byte watermark in the drop record (S-BETA-O1; the orchestrator's call, reversible until this card merges).** *What users gain:* if the database crashes after a DROP or TRUNCATE has been cleaned up but before the next checkpoint, time-travel queries (AS OF) at moments between when the cleanup became allowed and when it ran keep working after the restart. Under draft 7.12 they were refused on every table from then on, which with a time-travel window configured could be most of that window. With the field, a crash refuses nothing the database had not already refused before it. *What it costs:* 8 bytes per dropped table or index in the log, written only by the cleanup commit, and nothing else changes. *Timing:* it must be final before A-6a is cut, because A-6a fixes the log's byte format; after that, adding or removing it is a format change (free until A-7e's compatibility verdict, a migration after it). I recommend keeping it, as the card does; this gate found it sound.
2. **The race this card was written for.** Could a crash lose part of an acknowledged large transaction that began at the exact moment a checkpoint began? Under the old text, yes, on one kind of build the text allowed (one that writes the transaction's first log piece without the checkpoint's lock), and never on another kind. The card fixes it for every build: the transaction's start is recorded in the same step that writes its first piece, and the checkpoint reads it in one step. The rework adds the sentence that writing a log piece never takes the checkpoint's lock, so the tests can force the race on any build and catch a regression.
3. **One more thing the rework closes.** On a build that forgot to remove a failed large transaction from that record, the log would never be recycled after the first failure, and the disk would fill. The rework says when the entry must go, and S-GAMMA-F1 adds the test.

## History

- 2026-11-15: first gate attempt died on an expired login before writing anything. This is a fresh gate on f654984997460c5218629d651d90c124b3fd71c5: fresh gate run `GATE EXIT=0`; REWORK with RW-1 to RW-18 and AC-1 to AC-14; follow-ups S-GAMMA-F1 to F4 for S-DELTA.
