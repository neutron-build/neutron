Card: S-BETA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 2ec40280-e518-4700-9c44-6352eaa467c8 (a fresh gate run; this agent wrote neither the card nor any S-BETA commit)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: 9955fe10f735f2cab157a4de70b8fa53a9227edd
Mutants: spec text, none in code; each named mutant checked as text against its test: TRUNC-NOT-DURABLE -> t_wal_trunc_durable (zeroing form; removal form UNCAUGHT as written, RW-6), BOOT-DURABLE-AT-VALID-END -> t_boot_durable_start and its fall-back variant, WAL-START-PAIR-SPLIT-READ -> t_wal_chain_crash racing-chunk step, BOOT-PAIR-UNSET -> t_wal_chain_crash boot step, SEAL-STALE-HEAD -> t_wal_chain_crash two-batch step at A-6c and A-7d (fixture wording RW-5, RW-8), SPLICE-MARKER-PAST-CUT -> t_wal_splice read-only, SPLICE-CUT-SAME-START-KEEP -> t_wal_splice read-only x same-start, DROP-NO-GCW -> t_drop_restart_floor (a)-(c) (only while no GC runs at a w >= S_ddl, RW-2), DROP-GCW-AT-WALK -> t_drop_restart_floor (a) (same), DROP-GCW-AT-TS -> t_drop_restart_floor (f), REPLAY-DROP-NO-GCW -> t_drop_restart_floor (d)-(e) (same); gate's own: G1 step 5 removes F without syncing wal/ -> UNCAUGHT as written (caught by RW-6's first run), G2 the mark's raise made after its gated operation ends -> UNCAUGHT unless A-7c flips there (plan note), G3 START-LSN-SPLIT-READ (implementer item 4) -> UNCAUGHT, no test (S-BETA-F1), G4 durable_end started at meta k's wal_start_lsn after a fall-back -> equivalent (safe)
Followups: S-BETA-F1, S-BETA-F2

## Verdict in brief

**REWORK.** The three fixes are right as durability design. Each failure trace at draft 7.11 is closed by the 7.12 text, and none of the break attempts below reopens it. The defects are in the card's own tests and in two of its sentences:

1. `t_drop_restart_floor`'s "one at `S_ddl` succeeds" cannot pass on a correct build (implementer item 1).
2. The same test lets GC mask DROP-NO-GCW, DROP-GCW-AT-WALK and REPLAY-DROP-NO-GCW, in every form, not only at B-6 (item 5).
3. §12.2's new sealing rule, read with §12.2's own chain-head definition, names the wrong record (item 2).
4. `t_wal_chain_crash` asks for batches "sealed … out of order", which a correct writer cannot do (item 3).
5. `t_wal_trunc_durable` cannot kill TRUNC-NOT-DURABLE on a build that removes segment files (this gate's G1).
6. §6.8 step 1 raises `gc_w_used` unconditionally, while §10.5 and §14 step 4 raise only on `live` → `dropping` (item 6, first point).

Eight exact blocks, RW-1 to RW-8, plus one Acceptance line (RW-A) and two plan notes in the card (RW-C1, RW-C2), fix all six. Dry-run on 9955fe10's `docs/S-STORE.md`: each Old occurs exactly once; with RW-A, every Acceptance line but `gate.sh` passes; the checker prints the same three lines (`148 rows; 211 mutants; 25 …`, `42 new …`, `OK`). No count, name or row changes, so one rework pass is mechanical.

Implementer item 4 is a real silent loss of an acked commit, present since draft 7.8 and not introduced here. It is under-specification that a later card can settle without contradicting 7.12, so it is bound to **S-BETA-F1, before A-6c** (and so before A-6d and A-7c), with the lock-order leaf rule. Item 7, the splice-cut overclaim in 7.11's text, is wording: **S-BETA-F2, before A-7d**.

## Gate run

- Scaffold extracted through the shallow `--no-local` clone: `6ae0c56596b69a5d6ff2c6731e85d0a5a2e20f60`. No `EXTRACT FAILED`.
- `bash "$fg/fresh-gate.sh" 9955fe10f735f2cab157a4de70b8fa53a9227edd`, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-BETA`:
  - `fresh-gate: commit 9955fe10f735f2cab157a4de70b8fa53a9227edd`
  - `fresh-gate: base 6ae0c56596b69a5d6ff2c6731e85d0a5a2e20f60`
  - `fresh-gate: checks passed (4814 files hashed in 1.38 s)`
  - `fresh-gate: 2 file(s) matched through their eol conversion`
  - steps test, clippy, fmt, deny, wasm32, core-paths-selftest and enforcement-tests each `EXIT=0`; stderr empty.
  - Literal last line: `GATE EXIT=0`.
- `$fg` removed afterwards.
- Worktree `~/.cache/nv3-cards/S-BETA`: tip 9955fe10, clean, three commits on 6ae0c565, which is the scaffold tip.

## Scope and bytes

- `git diff nucleus/v3-scaffold...9955fe10 --stat`: `docs/C-T0-txn-protocol.md` (9 lines) and `docs/S-STORE.md` (140), 100 insertions and 49 deletions. Both are the Touch-only paths. No protected path.
- The base files' sha256 match the card (`20283ae8…28e4`, `02d652cc…7aa`).
- **The 65 blocks.** This gate's own parser read every Old/New pair out of the card's Edits section and applied them in order to the 6ae0c565 files: each Old occurs exactly once at its turn (F6-m's first Old exists only after F6-d, as the card says). The results are byte-identical to the tip's two files (sha256 `f1b4a857…7510` and `11783e36…3c84`).
- **The riders** F6-a to F6-m (28 blocks) equal `verdicts/S-ALPHA.md`'s, byte for byte; only the fences differ.
- **Nothing of 7.11 lost** (own script): no invariant, test, mutant, `I-` token, `t_` token or all-caps mutant token of 7.11 is missing; §15 gains I-BOOT-DURABLE (46 → 47 rows), 11 mutants (200 → 211) and 3 tests (42 → 45); every one of 7.11's 141 §15.1 rows is present unchanged except `t_wal_splice` and `t_wal_chain_crash` at A-7d, whose mutant lists only grow; 7 rows are new (148).
- **Limits.** Every changed hunk lies in §1 (F6-e), §2.2 (F6-b, F6-j), §4.4 (F6-k), §6.8, §10.5, §12.2, §12.4, §12.5, §13, §14, §14.1, §15, §15.1, §17, §18, §19 T4 (F6-h) or the Changelog. Both status lines keep the old line after their new head byte for byte (checked by script). The 7.12 and 8.5 status lines and Changelog entries describe what the text does.
- **Acceptance** in a scratch clone at the tip: every line but `gate.sh` passes; the checker prints `148 rows; 211 mutants; 25 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`, `42 new since 7.10 (31 at 7.11, 11 here), 42 of them judged on a Phase A row; 47 invariants, 45 tests`, `OK`.

## The three fixes, traced from first principles

### S-1-F6, first half: the truncation must be durable. **Closed.**

**The failure at 7.11** (class MP with ordinary power cuts, no WAL media fault):
1. Boot `b` recovers from meta `j` to the valid end `V` in segment `S`, with boot id `i`, and syncs its boot meta `j+1`.
2. `b`'s first record, at `V`, sits in an unwritten batch. A later batch holds the rest of `S`, its `SegmentEnd` and records in the next segment `F`; it is written and synced. I-ACK-PREFIX acks nothing past `V`.
3. A power cut, and both copies of meta `j+1` are lost after their sync. The second route is a post-sync fault on a page of image `j+1`, after which the next open falls back to meta `j` with proof: its walk reaches meta `j+1`'s `wal_ckpt_end`, which is `V`.
4. Boot `b′` opens meta `j` and stops at `V`. Its boot id has the same three inputs as `b`'s (§14 step 8): `ckpt_id` `j+1`, meta `j`'s `boot_id`, the same walk. So it is `i`.
5. `b′` truncates. 7.11 does not require the zeroing of `S` past `V`, or of `F`, to be synced: step 1 syncs only below `V`, and `sync_data` of `S` does not reach `F`.
6. `b′` syncs its boot meta, acks `on` commits up to its own synced `SegmentEnd` at `S`'s end, and writes records into `F` without a sync.
7. A power cut. `F` reverts to `b`'s durable records.
8. The next open walks from `V`, crosses `b′`'s records and hops to `F`. `b`'s record there has a good magic, CRC and `lsn`, and id `i` is not older, so it passes every stop check. The chain names `b`'s `SegmentEnd`, not `b′`'s, so a healthy history gives XX001. Where the two boots' records agree byte for byte, `b`'s unacked records replay at ts values `b′` also issued.

**Under 7.12.** E22 makes `F`'s zeroing or removal durable before step 6. At step 7, `F` therefore reverts to zeros, or stays removed. The walk stops at `F`'s first LSN and replays every commit `b′` acked.

**Break attempts:**
- *A real filesystem.* Zeroing is a write plus `sync_data` that does not extend the file, so no metadata change is needed. Removal is unlink plus a sync of `wal/`. Both are the standard durable idioms on POSIX filesystems. A-1b's SimEnv models exactly these: unsynced directory ops persist as a prefix until a directory sync (`crates/nucleus-env/src/sim/env.rs`, `power_cut`).
- *A crash after step 5's syncs, before the first append.* If `b′`'s boot meta is not yet durable, the next open selects meta `j` again and gets id `i` again. Its walk stops at `V` on durable zeros, and its truncation is idempotent. If the boot meta is durable, the next boot gets a fresh id.
- *A crash in the middle of step 5.* Suppose `S` is synced and `F` is not. The walk stops at `V`, on `S`'s zeros, before it can reach `F`, and the next boot truncates `F` again before it appends.
- *The same id reused again and again* (`b`, `b′`, `b″` …). Each boot's truncation is durable before that boot appends. A later power cut can only revert unsynced bytes to the last durable state, which is a truncation's zeros or that boot's own synced records. No earlier boot's record can return.
- *A fall-back or an explicit rollback that reuses an id.* The retire write precedes step 5, and step 5 is the same durable truncation.
- *The Residual* still needs post-sync WAL damage (classes MW and MP), as E12 now states.

The rule needs nothing more. Its test does: `t_wal_trunc_durable` cannot kill the removal form of the mutant (G1, RW-6).

### S-1-F6, second half: where `durable_end` starts. **Closed.**

**The failure at 7.11** (a process crash, then class MP, then a power cut):
1. A life syncs meta `k` (`wal_start_lsn` `s`, `wal_ckpt_end` `e`). It appends records `R` past `e`: the tail of segment `E` and, past a hop, the start of `F`. It does not sync them.
2. The process crashes and the page cache survives. Boot `b`'s walk accepts `R` and stops at `v` in `F`.
3. Step 5 syncs `F` but not `E`. With `durable_end = v`, the boot checkpoint's step 1 syncs nothing.
4. `b` syncs meta `k+1` and acks `on` commits `C` past `v`.
5. A power cut loses `E`'s tail, and both copies of meta `k+1` are lost after their sync.
6. The walk from meta `k` stops in `E`, at or above `e`. I-WAL-COVER passes, and the open returns `Ok` without `C`.

**Under 7.12.** E11 starts `durable_end` at the walk's start. E19's step 1 then syncs every segment holding a byte in `[s, v)` before step 4 writes the boot meta. Step 5 has already synced `v`'s segment and every segment above it. §14 step 8 lets no transaction start before the boot meta is durable, so nothing is acked before that sync.

**Each open path:**
- *Normal.* `[s, v)` is synced by step 1.
- *A fall-back, or an explicit rollback, which falls back as a proven fall-back does.* The span is `[s′, v)`, from meta `k−1`'s start, which is what E11 says. Starting at meta `k`'s `wal_start_lsn` would also be safe: everything below meta `k`'s `wal_ckpt_end` was made durable by checkpoint `k`'s step 1 (G4, equivalent). E11 takes the conservative value.
- *Read-only.* It writes nothing and acks nothing. It serves the state in the page cache and makes no durability claim.
- *Refused opens* (a missing start segment, WAL loss, a splice, a dependency, an unproven fall-back). Nothing is written and no boot starts.

**D-1's class argument holds.** Without a rollback, every later walk starts at or above the boot meta's `wal_start_lsn`, which is `v`. At a boot checkpoint every replayed commit is at most `T`, and the previous boot's unmarked chunks do not count (§12.6, §14 step 8). So `E`'s lost bytes are never read again, and the S-1-F5 gate's proposed test, which loses no meta, passes under the mutant. `t_boot_durable_start` reaches the defect, and its *fall-back* variant is passable and fails under the mutant with the unproven XX001.

### S-1-F5-F1: the start pair, the chain head, sealing and salvage. **Closed, apart from implementer item 4.**

**Every interleaving of a reservation with step 0:**
- *`Commit` and `CommitMarker`* reserve inside the I-WAL-TS-ORDER section, which step 0 also holds for its read. They are serialised with it.
- *A `Chunk`* takes one append-critical-section entry, and so does step 0's read. The chunk falls wholly before the position read, and is then possibly the chain head, or wholly at or after it. Either way, the pair names the record the position follows.
- *A hop.* The `SegmentEnd` and the record after it may be one entry or two; both are correct.
  - Every entry moves the position and the chain head together.
  - A read between the two entries sees the next segment's first LSN with the `SegmentEnd` as the chain head, and the next record names exactly that.
  - For a `Commit`, both entries lie inside its I-WAL-TS-ORDER section, so step 0 cannot fall between them.
- *A boot checkpoint.* The chain head is the recovery's (E8, E11, E18), and the boot's first record names the same record. BOOT-PAIR-UNSET is killed by the boot step with records accepted and with none.
- *Sealing.* With RW-4, each record names the record reserved just before it, which was the chain head when it was reserved, and its `prev_crc` is taken once that predecessor is sealed. Both the fold-at-reservation design and a writer that seals later satisfy it.

**The salvage cut-back (E28)** is sound:
- The marker set, the largest `commit_ts`, the coverage result and step 3b's check are all taken over the records below the cut.
- A `RequireCkpt` that only a record at or after the cut carries is never applied, and neither is one in a chunk whose marker lies at or after the cut. So neither refuses anything.
- A dependency cut computed from records past the splice cut can never lie below the splice cut. So including or excluding those records changes nothing, and "the lowest one does" stands.

**Implementer item 4 is a real loss, outside this card's three charters.**
1. At checkpoint `k`'s flip, no `Commit` lies above `T` and no chunked transaction is open.
2. Step 0 reads §12.6's set of first chunks, which is empty.
3. A new large transaction `X` then reserves its first `Chunk` at `q`.
4. Step 0's one entry reads the position `p = q + len` and the chain head, which is `X`'s chunk. So `wal_start_lsn = p`, and the pair is consistent: no splice.
5. `X` appends more chunks and its `CommitMarker` at `c > T`, and its `on` commit is acked.
6. A crash comes before the next checkpoint's meta.
7. Recovery from meta `k` walks from `p`, applies `X`'s chunks from `p` on at `c`, and never reads the chunk at `q`. Nothing compares `nchunks`.
8. **Wrong outcome:** the acked commit is replayed partly, silently, and not atomically.

E18's one entry covers the position and the chain head only. Nothing at 7.11 or 7.12 says the set is read in that entry, or that a first chunk enters the set inside its own reservation's entry. The card's own D2 item 1 trace ("no chunked transaction is open") assumes that a racing chunk goes uncounted, which is this gap. It is bound to S-BETA-F1, before A-6c.

### S-1-F4: the restart floor across drops. **Closed.**

**The bounds:**
- *In normal operation.* The mark's `w` is the published `W` at its mutation. `W ≤ M` at the mark, and `M` there is at most `M` at any later flip, which the flip takes as `T`. So `gc_w ≤ T`, and real code still never reaches GCW-BOUND-SKIP's check. §15.1 row 50 is unchanged, and so is row 51. `w > S_ddl`, because the drop runs only once `W > S_job ≥ S_ddl`, and `W` is monotonic.
- *In replay.* `c = c_d` was issued above `visible_ts ≥ W > S_job ≥ S_ddl`, so `c − 1 ≥ S_ddl + 1`. And `c` is at most the recovered frontier, which the boot checkpoint takes as `T`, so `c − 1 < T`.
- *The conditional raise.* An entry the recovered image holds as `dropping` or absent was marked before that image's flip, since frees follow the mark and the last step follows them. So that image's `gc_w` already counts the mark.
- *A fall-back or rollback to meta `k−1`.*
  - If image `k−1` holds the mark, meta `k−1`'s `gc_w` counts it.
  - Otherwise `c_d > T_{k−1}`, so its record lies at or above meta `k−1`'s `wal_start_lsn`, and pass 2 replays it and raises the floor to `c − 1`.
  - If salvage cuts before `c_d`, the image holds no mark and so no free either. The tree is then `live` and whole, and a snapshot below `S_ddl` reads it correctly (E27).
- *C1–C4* agree with S-STORE §10.5 "After open", §14 "Afterwards", §14.1 and §17.
  - Every `gc_w` mention in C-T0 was checked, and none states the old floor.
  - I-W-FLOOR is raised. Its owner (B-3), its mutant (W-FLOOR-ZERO) and `t_w_floor_after_restart` are unchanged.
  - `merge-card.sh`'s `docs/*` rule (P tier and Class: spec, per path; `enforce-lib.sh` line 282) is met for both files.

**Does any path still lose an acked commit or admit a bad snapshot?**
- *S-1-F6.* No path loses a commit acked under an honoured sync, through the truncation or through the recovered `durable_end`.
- *S-1-F4.* No path admits a snapshot that can reach freed pages, or a tree that is `dropping` or gone.
- *S-1-F5-F1.* With RW-4's wording, no false splice arises under ordinary crashes from the pair, the boot's chain head or sealing.
- *Still open, and pre-existing:* implementer item 4. An acked chunked transaction can silently lose a chunk reserved during step 0, on a build that reads §12.6's set apart from the append position. Bound to S-BETA-F1, before A-6c.

## The implementer's report, ruled

1. **Real defect: a test that cannot pass.** C-T0 §3.6 admits `t` only when `W ≤ t`, and after every reopen `W ≥ w > S_ddl`, or `W ≥ c − 1 > S_ddl` after a replay. So a snapshot at `S_ddl` gets 72000 on a correct build. RW-3 replaces the check with a snapshot at the reopened `W`, which must be served without the tree; RW-7 fixes B-6's row, and RW-C1 and RW-C2 fix the card's plan notes.
   - *What proves the defect closed.* An AS OF at `S_ddl − 1` lies in `[old floor, S_ddl)` only if no GC ran at a `w ≥ S_ddl`, which RW-2 requires. It must give 72000.
   - *Each mutant against the corrected test:*
     - DROP-NO-GCW fails (a)–(c): `gc_w_used` and meta `gc_w` stay below `w` at A-7b, A-7c and A-7d, and at B-6 `W ≤ S_ddl − 1`, so the snapshot is admitted.
     - DROP-GCW-AT-WALK fails (a): the image's `gc_w` is below `w`.
     - REPLAY-DROP-NO-GCW fails (d) and (e): `gc_w_used` is meta `gc_w`, which is below `S_ddl`.
     - DROP-GCW-AT-TS fails (f): the meta write fail-stops.
   - *A correct build passes:* `W > S_ddl`, and `W ≤` the recovered `visible_ts`.
2. **Real defect, wording that builds the wrong thing.** E8 makes the chain head "the record reserved last", so "the chain head as it stands once the previous batch's last record is sealed" is a later record, even with only two batches. RW-4 fixes it. E14's SEAL-STALE-HEAD ("a chain head read before …") agrees with the corrected rule and stays.
3. **Real defect, a fixture a correct writer cannot produce.** §12.2 fixes the sealing order. RW-5 and RW-8 say what the test controls (write and sync order, with cuts after both batches are synced) and drop "sealed".
4. **Real acked-commit loss, pre-existing since 7.8** (trace above). Bound to S-BETA-F1, before A-6c.
5. **Real, and in every form, not only B-6.** At A-7b, A-7c and A-7d, S3 supplies `gc_w_used`, and a fixture whose S3 value reaches `w` masks DROP-NO-GCW and DROP-GCW-AT-WALK. At A-7d and B-6, any GC at a `w ≥ S_ddl` before the flip masks REPLAY-DROP-NO-GCW as well. RW-2 puts one fixture rule into the test's definition, and RW-7 holds B-3's sweep off at B-6.
6. **The smaller points:**
   - (a) E2 against E6 and E21: a real contradiction for a replayed DropTree that finds the entry already `dropping`. Either reading is safe; an unconditional raise only refuses more snapshots. But the text must agree: RW-1.
   - (b) F6-k against E22 and E12b: **not a defect.** "Intact" is I-TWO-GEN's sense: not recycled, so the span a walk reads. Step 5 changes only bytes past the valid end, which no walk from meta `k−1` accepts; 7.11's non-durable truncation stood in the same relation.
   - (c) §2.2's "the last record appended" (P-3): **wording.** §12.2 and §13 step 0 now bind the boot case. It rides in S-BETA-F2.
   - (d) E10, the hop: **not a defect.** One entry or two are both correct (above).
   - (e) E31's "a splice XX001": **wording.** If the two boots' records were byte-identical, the test's "no record of `b`" would still kill the mutant. RW-6 makes the records differ, so the stated failure is the one that occurs.
7. **Real overclaim, in 7.11's text (S-1-F5's): wording.** Suppose two or more records are overwritten by records of equal length that chain to the walk's predecessor. The walk then follows that chain, by §12.5's transitivity, and the cut keeps a prefix of it. E15's crossing is passable for any number of overwritten records: it checks only the predecessor, and the read-only oracle is the records below the cut as they stand. Bound to S-BETA-F2, before A-7d.

## Lock order

- **Rank.** The order is the epoch gate, then the I-WAL-TS-ORDER section, then the append critical section. Frame latches also come before the append critical section, wherever a build reserves a `Chunk` inside a gated materialisation. Nothing 7.12 requires inverts this. But 7.12 does not say that the append critical section is a leaf. S-BETA-F1 states it: it takes no lock, does no I/O and waits for nothing, so nothing ranks after it.
- **A hop waiting on I/O.**
  - *Not needed.* The append position may sit at a segment's first LSN before that segment is durable: §13's start-segment paragraph says so, and `t_wal_start` checks it at A-7c. The writer makes the segment durable before its first write there (§12.5).
  - *Not forbidden either.* A build that waited on I/O inside the section would make step 0 block on that I/O while holding the gate exclusively, so every gated operation would stall. A build that waited there for step 6's recycling would deadlock with the next step 0.
  - Bound to S-BETA-F1, before A-6c.
- **Reading `W` in the mark.** GC already reads the published `W` inside its gated operations (§10.1 condition 1). C-T0's snapshot-registry mutex is a leaf: §3.6 and §9.1 take no latch, gate or lock under it. So reading `W` under that mutex, while holding the directory leaf and the gate shared, adds no cycle. A lock-free read of the published, monotonic value is enough. Not a defect; a note for A-7b and B-3.

## Tests that cannot pass, and mutants that cannot fail

**Cannot pass on a correct build:**
- `t_drop_restart_floor`'s "one at `S_ddl` succeeds", in §10.5's definition and in B-6's row (RW-3, RW-7).

**A fixture a correct build cannot produce:**
- `t_wal_chain_crash`'s "sealed … out of order", in §12.5 and in A-6c's row (RW-5, RW-8).

**Mutants that cannot fail as written:**
- TRUNC-NOT-DURABLE's removal form, on a build that removes segment files (G1). `b′` writes into `F`, so `F`'s durable re-creation under §12.5's writer rule syncs `wal/` and persists the unlink (RW-6).
- DROP-NO-GCW, DROP-GCW-AT-WALK and REPLAY-DROP-NO-GCW, on fixtures where GC (S3, or B-3's sweep) has reached a `w ≥ S_ddl` (RW-2, RW-7).

**No test at all:**
- G3, START-LSN-SPLIT-READ (S-BETA-F1).

**G2, a note for A-7c's test author.** A raise made after the mark's gated operation ends is caught only by a flip placed between the two. A-7c's variant (a) should sweep flips over every gate release around the mark. E6's "in its own gated operation" already states the rule.

## The eleven mutants: code, fixture and the Phase A card that judges each

| Mutant | Code (card) | Judged on (§15.1) | Fixture | At 7.12 |
|---|---|---|---|---|
| TRUNC-NOT-DURABLE | A-7d, step 5 | A-7d | `t_wal_trunc_durable` | zeroing form killed; removal form not (RW-6) |
| BOOT-DURABLE-AT-VALID-END | A-7d, the writer's start state (A-6c accepts it) | A-7d | `t_boot_durable_start`, and its fall-back variant | killed |
| WAL-START-PAIR-SPLIT-READ | A-7c, step 0 | A-7d, row 100 (A-7f after the relabel) | racing-chunk step | killed on a 7.12 build; S-BETA-F1 re-derives it |
| BOOT-PAIR-UNSET | A-7c | A-7d, row 100 | a boot's first append | killed |
| SEAL-STALE-HEAD | A-6c | A-6c (A-6b's walk), A-7d | two batches in flight | killed once "sealed" is dropped (RW-5, RW-8) |
| SPLICE-MARKER-PAST-CUT | A-7d | A-7d | read-only, marker past the cut | killed |
| SPLICE-CUT-SAME-START-KEEP | A-7d | A-7d | read-only × same-start | killed |
| DROP-NO-GCW | A-7b, the mark | A-7b, A-7c, A-7d, B-6 | variants (a)–(c) | killed with RW-2 |
| DROP-GCW-AT-WALK | A-7b, the placement | A-7b, A-7c, A-7d | variant (a) | killed with RW-2 |
| DROP-GCW-AT-TS | A-7b, which `w` | A-7c | variant (f) | killed |
| REPLAY-DROP-NO-GCW | A-7d, replay | A-7d, B-6 | variants (d), (e) | killed with RW-2 |

**Placement, by this gate's own script.** The script parsed PLAN §9's Depends cells, after removing quoted "dropped" notes and parentheticals.
- The 29 A-cards have no dependency cycle, and the edges match the checker's map.
- Every new mutant is judged on a Phase A row. None is judged only in Phase B.
- In all 148 §15.1 rows, each Needs cell lies inside its card's Depends closure, and no Phase A row needs a Phase B card.

## The riders

F6-a to F6-m are S-ALPHA's verdict text, byte for byte. Taking F6-m is right: it is mechanical, and the Acceptance line counting one `*Unread variant*` holds. None contradicts S-BETA's edits; for F6-k, see 6 (b).

## The orchestrator's calls and the author's open points

**The orchestrator's calls:**
1. **C-T0 folded in: right.** C1–C4 are consistent with S-STORE, and no other C-T0 sentence keeps the old floor.
2. **`c − 1` for a replayed mark: acceptable as the default, but the owner should confirm it before A-6a pins the DropTree op encoding** (see the owner's points below).
3. **The full form on B-6: right.** B-6 has the drop cleanup at a real published `W`, B-4a's catalog and B-3's floor. It must hold B-3's sweep off (RW-7).

**The author's open points:**
- **P-1:** keep the *fall-back* variant. It is passable and fails under BOOT-DURABLE-AT-VALID-END, so it is a second kill.
- **P-2:** confirmed. Step 5 can remove only the start segment of a meta the open did not recover from. That needs a newer meta that the open retired or could not read, whose start lies above the valid end. In the second case, WAL bytes that meta's step 1 synced must already be lost. A later open of that meta refuses with XX001: detected, never silent.
- **P-3:** wording; rides in S-BETA-F2.
- **P-4:** right. F6-h's Old is in §19 T4, and Limits 7 names §19.
- **P-5:** agreed; rides in S-BETA-F2.

## Rework blocks

**How to apply.**
- Apply RW-1 to RW-8 to `docs/S-STORE.md` at 9955fe10. Each Old occurs there exactly once.
- Apply each block also to the card's matching E-block New text: E2, E6 (two blocks), E9, E16, E31, E36 and E39. Each Old occurs exactly once in the card.
- Then make the card-only changes RW-A, RW-C1 and RW-C2.
- Nothing else changes. Counts, names and rows stay as they are, and so does every other Acceptance line.
- This gate dry-ran exactly these blocks on scratch copies. With RW-A, every Acceptance line but `gate.sh` passes, and the checker prints the same three lines.

**RW-1** (§6.8 DropTree step 1, card E2 [implementer item 6, first point]).
Old:
~~~~text
The same mutation sets the root-table entry's state and raises `gc_w_used` to `w`:
~~~~
New:
~~~~text
The same mutation sets the root-table entry's state and, when it changes the entry from `live`, raises `gc_w_used` to `w`:
~~~~

**RW-2** (§10.5 "Drops", `t_drop_restart_floor`'s fixture, card E6 [items 1 and 5]).
Old:
~~~~text
a tree that the catalog names below `S_ddl`, retired at `S_ddl` and dropped at a published `W > S_ddl`, in six variants:
~~~~
New:
~~~~text
a tree that the catalog names below `S_ddl`, retired at `S_ddl` and dropped at a published `W > S_ddl`, with no GC mutation at a `w ≥ S_ddl` in any form (S3's GC `gc_w_used` stays below `S_ddl`, and B-6 holds B-3's sweep off), so that without the drop's raise `gc_w_used`, and the floor after a reopen, would stay at or below `S_ddl − 1`, in six variants:
~~~~

**RW-3** (§10.5 "Drops", `t_drop_restart_floor`'s AS OF checks, card E6 [item 1]).
Old:
~~~~text
an AS OF snapshot at `S_ddl − 1` gives 72000, and one at `S_ddl` succeeds and reads a catalog that does not name the tree; in (f) the meta write succeeds.
~~~~
New:
~~~~text
an AS OF snapshot at `S_ddl − 1` gives 72000, and one at the reopened `W`, which is `gc_w_used` as recovery leaves it and lies above `S_ddl`, succeeds and reads a catalog that does not name the tree (one at `S_ddl` gets 72000 as well, since `W > S_ddl`; C-T0 v8 §3.6); in (f) the meta write succeeds.
~~~~

**RW-4** (§12.2 "Order of sealing", card E9 [item 2]).
Old:
~~~~text
A batch's first record is sealed with the chain head as it stands once the previous batch's last record is sealed, never with a value read before then (mutant SEAL-STALE-HEAD, §12.5).
~~~~
New:
~~~~text
A record's `prev_lsn` and `prev_crc` name the record reserved just before it, which was the chain head when the record was reserved (§12.4), and later reservations move the chain head on. So a batch's first record is sealed with the previous batch's last record's `crc`, taken once that record is sealed, never with a value read before then (mutant SEAL-STALE-HEAD, §12.5).
~~~~

**RW-5** (§12.5 `t_wal_chain_crash`, card E16 [item 3]).
Old:
~~~~text
    - two or more batches in flight, written, sealed and synced out of order, as in `t_ack_prefix` (§12.4);
~~~~
New:
~~~~text
    - two or more batches in flight, batch 2 written and synced before batch 1 is written, as in `t_ack_prefix` (§12.4), with cuts after both are synced as well; the test orders the writes and syncs, and the writer still seals in LSN order (§12.2);
~~~~

**RW-6** (§15 I-BOOT-DURABLE row, `t_wal_trunc_durable`, card E31 [gate finding G1; item 6, last point]).
Old:
~~~~text
writes its boot meta, acks `synchronous_commit=on` commits up to a durable `SegmentEnd` at the end of `V`'s segment, writes records into `F` without a sync, and a power cut that loses every unsynced write follows. The open must succeed with no splice error and replay every commit `b′` acked and no record of `b`; under TRUNC-NOT-DURABLE the cut reverts `b′`'s truncation of `F`, `b`'s record returns there under the same id, and the open fails with a splice XX001.
~~~~
New:
~~~~text
writes its boot meta, and acks `synchronous_commit=on` commits, whose records differ from `b`'s, up to a durable `SegmentEnd` at the end of `V`'s segment, and a power cut that loses every unsynced write follows: in a first run before `b′` syncs `wal/` for any other reason (it creates, recycles or writes no segment past `V`'s), and in a second run after `b′` has written records into `F` without a sync. The open must succeed with no splice error and replay every commit `b′` acked and no record of `b`; under TRUNC-NOT-DURABLE the first run's cut reverts `b′`'s truncation of `F`, whether step 5 zeroed `F` or removed it, `b`'s record returns there under the same id, and the open fails with a splice XX001.
~~~~

**RW-7** (§15.1 `t_drop_restart_floor` at B-6, card E36 [items 1 and 5]).
Old:
~~~~text
| `t_drop_restart_floor` | I-GCW | B-6 | A-7d, B-3, B-4a | full: B-6's drop cleanup at a published `W`, B-4a's catalog and B-3's restart floor; after each reopen the AS OF snapshots at `S_ddl − 1` (72000) and at `S_ddl` | DROP-NO-GCW, REPLAY-DROP-NO-GCW |
~~~~
New:
~~~~text
| `t_drop_restart_floor` | I-GCW | B-6 | A-7d, B-3, B-4a | full: B-6's drop cleanup at a published `W`, with B-3's sweep held off, B-4a's catalog and B-3's restart floor; after each reopen the AS OF snapshots at `S_ddl − 1` (72000) and at the reopened `W` (served, without the tree) | DROP-NO-GCW, REPLAY-DROP-NO-GCW |
~~~~

**RW-8** (§15.1 `t_wal_chain_crash` at A-6c, card E39 [item 3]).
Old:
~~~~text
| `t_wal_chain_crash` | I-WAL-CHAIN | A-6c | A-6b, A-1b | two or more batches in flight, sealed and synced out of order (`t_ack_prefix`'s interleaving), under power cuts, judged by A-6b's walk, which must report no splice | SEAL-STALE-HEAD |
~~~~
New:
~~~~text
| `t_wal_chain_crash` | I-WAL-CHAIN | A-6c | A-6b, A-1b | two or more batches in flight, written and synced out of order (`t_ack_prefix`'s interleaving), under power cuts, some of them after both are synced, judged by A-6b's walk, which must report no splice | SEAL-STALE-HEAD |
~~~~

**RW-A** (the card's Acceptance line 15, which greps E2's sentence; it follows RW-1).
Old:
~~~~text
- `cd nucleus/v3 && grep -n 'The same mutation sets the root-table entry.s state and raises .gc_w_used. to .w.: the watermark C-T0 v8 published at the time of the mutation' docs/S-STORE.md`
~~~~
New:
~~~~text
- `cd nucleus/v3 && grep -n 'The same mutation sets the root-table entry.s state and, when it changes the entry from .live., raises .gc_w_used. to .w.: the watermark C-T0 v8 published at the time of the mutation' docs/S-STORE.md`
~~~~

**RW-C1** (the card's D3, "What changes, card by card", B-6 bullet).
Old:
~~~~text
  - `t_drop_restart_floor` runs in full here: B-6's drop cleanup at a published `W > S_job`, B-4a's catalog, B-3's floor, and after each reopen an AS OF at `S_ddl − 1` (72000) and one at `S_ddl`. It judges DROP-NO-GCW and REPLAY-DROP-NO-GCW.
~~~~
New:
~~~~text
  - `t_drop_restart_floor` runs in full here: B-6's drop cleanup at a published `W > S_job`, with B-3's sweep held off, B-4a's catalog, B-3's floor, and after each reopen an AS OF at `S_ddl − 1` (72000) and one at the reopened `W` (served, without the tree). It judges DROP-NO-GCW and REPLAY-DROP-NO-GCW.
~~~~

**RW-C2** (the card's "For the plan card", item 7).
Old:
~~~~text
   - `t_drop_restart_floor` in full, with its AS OF checks (DROP-NO-GCW, REPLAY-DROP-NO-GCW). It needs B-3 and B-4a.
~~~~
New:
~~~~text
   - `t_drop_restart_floor` in full, with B-3's sweep held off and its AS OF checks at `S_ddl − 1` and at the reopened `W` (DROP-NO-GCW, REPLAY-DROP-NO-GCW). It needs B-3 and B-4a.
~~~~

The card's prose that explains these points may be brought in line at the author's choice (D2 item 3, D3 and the mutation table read correctly as they stand). The card's Notes for A-7d's test author ("sweep seeds until it does") now apply to `t_wal_trunc_durable`'s first run.

## Follow-ups

- **S-BETA-F1: step 0's chunk term, and the append critical section as a leaf.** A P spec card on S-STORE. It must merge before A-6c is cut, and so before A-6d and A-7c. It may ride in bundle γ only if γ lands before A-6c.
  - **(a) The chunk term in the one entry** (implementer item 4; present since draft 7.8).
    - The rule: §12.6's second term is the set of first-chunk LSNs of chunked transactions whose marker is not below `T`. Step 0 reads it in the same entry to the append critical section as the append position (E18's entry). A transaction's first `Chunk` enters the set inside its own reservation's entry (E10).
    - Why: without this, a first `Chunk` reserved after the set is read, and before the position is read, lies below `wal_start_lsn` uncounted. When its transaction commits and is acked, a crash before the next checkpoint replays the transaction without that chunk: silently (no `nchunks` check) and not atomically.
    - New mutant on I-REDO-COMPLETE, proposed name START-LSN-SPLIT-READ: step 0 reads the set apart from the append position, or a first `Chunk` enters the set outside its reservation's entry. It is A-7c's code, judged at A-7d (A-7f after the relabel).
    - Its test, a variant of I-REDO-COMPLETE's A-7d form (crash and replay):
      1. A flip races a new chunked transaction's first `Chunk`. A hook reserves it between the set's read and the position's read, or between the reservation and the set entry, wherever a build separates them.
      2. The transaction commits and is acked.
      3. A crash comes before the next checkpoint.
      4. The transaction must be replayed whole.
    - The card must also re-derive WAL-START-PAIR-SPLIT-READ's kill. S-BETA's D2 item 1 trace assumes the racing chunk is uncounted. Once the set is read in the entry, only a split that reads the position first, and pairs the append position with the chain head read after the hook, gives the false splice. The mutant's text and the racing-chunk step should say that.
  - **(b) The append critical section is a leaf.** It ranks after the epoch gate, the I-WAL-TS-ORDER section and any frame latch. Under it a thread takes no lock, does no I/O and waits for nothing. So a reservation that hops moves the position into the next segment without waiting for that segment. The writer makes the segment durable before its first write there (§12.5; §13's start segment).
  - **(c) Optional.** A DropTree's mark reads the published `W` as GC does (§10.1): lock-free, or under C-T0's registry mutex, which is a leaf.
- **S-BETA-F2: wording.** A P spec card. It must merge before A-7d, and may ride in bundle γ.
  - **(a)** §14 step 3a's "The cut keeps neither" overclaims (implementer item 7; 7.11's text, from S-1-F5). Suppose two or more records were overwritten by records of equal length whose chain agrees with the walk's predecessor. The records below the cut then follow the walk's chain, which may be the overwriting history, and salvage serves a ts-prefix of that history. Say so. No test changes: the read-only oracle is already the records below the cut, and E15's crossing checks only the predecessor.
  - **(b)** §2.2: "for the append position, it is the start and the `crc` field of the last record appended" becomes "… it is the chain head (§12.2)" (P-3).
  - **(c)** START-SEG-AS-END's "a lost meta" becomes "both copies of the newer metas lost" (P-5).

## What the follow-on plan card must change

The card's "For the plan card" items 1–12 stand, with these changes:
1. **Item 7 (B-6).** The full form holds B-3's sweep off. Its AS OF checks are at `S_ddl − 1` (72000) and at the reopened `W` (served), not at `S_ddl` (RW-C2).
2. **A-6c's Depends** gains S-BETA-F1. A-6d and A-7c inherit it through A-6c.
3. **QUEUE** gains the rows S-BETA-F1 (before A-6c) and S-BETA-F2 (before A-7d). S-1-F6, S-1-F5-F1, S-1-F4 and S-ALPHA-F6 close when S-BETA merges, as the card says.
4. **A-7c.**
   - `t_drop_restart_floor`'s forms keep S3's `gc_w_used` below `S_ddl` (RW-2).
   - Variant (a)'s flip sweep should include a flip right after the mark's gated operation (G2).
5. **A-7b and B-3.** The mark reads the published `W` as GC does: lock-free, or under the registry mutex, which is a leaf.
6. **§10 (owner decisions)** records O-2 (`c − 1`) as awaiting the owner's confirmation before A-6a's op codec.
7. **QUEUE row S-1-F5-F1.** The summary of SEAL-STALE-HEAD reads the corrected rule: each record carries its predecessor's final `crc`.

## The owner's points, in plain words

1. **The replay value `c − 1`** (orchestrator call 2). What it costs users:
   - *When.* A crash, or any reopen that has to replay the drop, between a DROP's cleanup commit and the next checkpoint that covers it.
   - *What is lost.* Every AS OF query on any table, at a timestamp between the floor the store would otherwise restart at and that cleanup commit's ts minus one, is then refused with 72000 from then on (the watermark never moves back), although the data at those timestamps is intact.
   - *How much.* With no AS OF retention window this costs almost nothing, because the watermark reaches the present soon after transactions run. With a window configured, it can cut off up to the whole window of time-travel history. The cleanup runs only after the watermark has passed the DROP, and the watermark trails the present by about the window.
   - *It never serves wrong data.*

   This is acceptable as the default. Writing the watermark into the DropTree record (8 bytes per drop) removes the cost entirely. That is nearly free only until card A-6a fixes the record format, so the owner should confirm `c − 1`, or choose the field, before A-6a is cut.
2. **A silent-loss path from draft 7.8 stays open** after this card: implementer item 4. It needs a large transaction to begin exactly while a checkpoint flips. It is bound to S-BETA-F1, which must merge before A-6c, so no code is written against the gap.

Nothing else needs the owner.
