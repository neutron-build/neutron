Card: A-6a
Class: impl
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 14b4367e-ccfc-4cce-9ba7-1eae3ab98cc6, dispatched from T3 thread 45584af2-e651-490e-862c-3402320cffa3 (the independent P gate of A-6a: it wrote neither the card, commits 113801a5, b2440453 and 1890666a, nor any earlier verdict, and is not the orchestrator)
Date: 2026-11-20
Gate-exit: GATE EXIT=0
Commit: 1890666a3eb1aac7d92475f4e2e8e0306d9f3c69
Mutants: on one scratch git-archive export of 1890666a (nucleus/v2 beside nucleus/v3, tar -x -m; debug; -p nucleus-store --test wal_codec, all 13 tests every run), each mutant an exact single-occurrence edit, every run shown to recompile nucleus-store, each file restored by rewriting its pristine bytes (fresh mtime) and byte-compared with git show, proptest regression files deleted before and after every run, a pristine pass first and last; the worktree was never written. The card's eight and two variants: DROP-OP-W-ZERO (encoder) -> t_drop_restart_floor (1) ("w 1", decoded [0]) and wal_golden; DROP-OP-W-ZERO (decoder variant) -> t_drop_restart_floor (1) and wal_golden; REQUIRE-ID-LOW32 -> t_ckpt_dep (1) (ckpt_id 4294967303); REQUIRE-NOT-FIRST (decoder only) -> t_ckpt_dep (2); REQUIRE-NOT-FIRST (the shared rule) -> t_ckpt_dep (2); CHAIN-OUTSIDE-CRC -> wal_frame_checks (byte 12 flipped) and wal_golden; SEAL-NO-SHIFT -> wal_seal_matches_direct (len 53); REVERT-IN-COMMIT -> wal_malformed; SEGEND-N-NONZERO -> wal_malformed (SegmentEnd n 1); SEG-TAIL-SHORT -> wal_place (c) (seg 256 len 53 room 105) and wal_fuzz_place. The gate's own (47): G1-HDR-BOOT-N-SWAP -> wal_golden, wal_frame_checks; G2-HDR-COMMIT-TS-BE -> wal_golden; G3-HDR-PREV-SWAP -> wal_golden, wal_seal_matches_direct; G4-KIND-CHUNK-MARKER-SWAP -> wal_golden, wal_kind_bytes; G5-TAG-PUT-DELETE-SWAP -> wal_golden; G6-PUT-FLAGS-AFTER-VALUE -> wal_golden; G7-DELETE-BITS-SWAP -> wal_golden; G8-PRESENCE-INVERTED -> wal_golden; G9-DROP-FIELDS-SWAP -> wal_golden, t_drop_restart_floor; G10-CREATE-KIND-FIRST -> wal_golden; G11-STRING-LEN-BE -> wal_golden; G12-CRC-SKIPS-LAST-BYTE -> wal_frame_checks, wal_golden; G13-CRC-FROM-BYTE-13 -> wal_frame_checks, wal_golden; G14-RF-CRC-OVER-WHOLE-BUFFER -> wal_frame_checks; G15-COMBINE-TAIL-LEN-PLUS-1 -> wal_seal_matches_direct; G16-COMBINE-SKIP-ODD-STEP -> wal_seal_matches_direct; G17-COMBINE-POLY-IEEE -> wal_seal_matches_direct; G18-COMBINE-NO-PRESQUARE -> wal_seal_matches_direct; G19-COMBINE-ROWS-SHIFTED -> wal_seal_matches_direct; G20-GF2-TIMES-WRONG-BIT -> wal_seal_matches_direct; G21-RF-LEN-52-ACCEPTED -> wal_frame_checks (len 52); G22-RF-LEN-CLAMPED-TO-BUF -> wal_frame_checks (len 143); G23-RF-LEN-BEFORE-MAGIC -> wal_frame_checks (order), wal_fuzz_bytes; G24-RF-MAGIC-BEFORE-SHORT -> wal_frame_checks (order), wal_fuzz_bytes; G25-RF-PAYLOAD-FROM-52 -> wal_frame_checks, wal_golden; G26-DB-NO-FINISH -> wal_malformed (CommitMarker payload 5 bytes); G27-DB-NCHUNKS-UNCHECKED -> wal_malformed (nchunks 0); G28-TS-MAX-ALLOWED (shared rule) -> wal_malformed (Commit commit_ts u64::MAX); G29-DB-NO-HEADER-RULES -> wal_malformed (Commit n 0); G30-KIND-5-AS-SEGEND -> wal_kind_bytes (byte 5); G31-ORDER-DROPTREE-EXEMPT -> wal_op_order ([Put, DropTree]); G32-ORDER-DELETE-NOT-DATA -> wal_op_order ([Delete, Ddl]); G33-DEC-NO-ORDER-CHECK -> wal_op_order, t_ckpt_dep (2), wal_malformed; G34-REQUIRE-SECOND-OK -> t_ckpt_dep (2); G35-ORDER-INDEX-STUCK -> t_ckpt_dep (2); G36-PUT-FLAG-BIT1-OK -> wal_malformed (Put flags 2); G37-DELETE-FLAG-BIT2-OK -> wal_malformed (Delete flags 4); G38-PRESENCE-ANY-NONZERO -> wal_malformed (presence byte 255); G50-REVERT-RULE-OFF (shared rule) -> wal_malformed; G39-PLACE-NO-ROOM-CHECK -> wal_place (f), wal_fuzz_place; G40-PLACE-NO-EXACT-END -> wal_place (a); G41-PLACE-STRICT-53 -> wal_place (b), wal_fuzz_place; G42-PLACE-SEGEND-AT-START -> wal_place (c), wal_fuzz_place; G43-PLACE-MAX-PLUS-1 -> wal_place (e), wal_fuzz_place; G44-PLACE-MIN-52 -> wal_place (e), wal_fuzz_place; G45-PLACE-HOP-INTO-LAST-OK -> wal_place (g), wal_fuzz_place; G48-PLACE-UNCHECKED-ADD -> wal_place (g) and wal_fuzz_place (debug overflow panic at place.rs:71). Every mutant is caught by at least one deterministic test, not only by a proptest driver. None UNCAUGHT
Followups: A-6a-F1

## Verdict in brief

**MERGE-WITH-FOLLOWUPS.** The codec is right, and its tests can tell when it is wrong.

- **The bytes.** I re-derived the 53-byte header, the six ops and every rule from S-STORE §12.2, §12.3 and §7.1 (draft 7.16) and the card's pin. Every offset, width and order in `src/wal/` matches. I recomputed all five golden vectors with my own bitwise CRC32C and my own encoder, written from the tables alone. All five match the card byte for byte, and so does the test file's copy.
- **The oracles.** The tests' oracles do not depend on the code under test: `assemble`, `model_record`, `model_frame`, `check_frame` and the `u128` `model_place` are written from the card's tables. Their one shared dependency is A-2-0's `crc32c`, which A-2-0's test vector and these golden vectors pin.
- **Mutation.** The card's eight mutants are caught, with two variants of them, and so are all 47 of mine. Mine aim at the header and op layouts, the CRC's coverage, the combine, `read_frame`'s truncations and bad lengths, the order and `revert` rules, `RequireCkpt` first only, `DropTree`'s `w`, `place`'s boundaries and integer overflow.
- **The fresh gate is green.** The run before it died of a full disk on this shared host (Gate run, below).

The follow-up, A-6a-F1, is card text for A-6b's re-cut. It does not block.

**Merge timing.** The owner's own words of 2026-11-20 (QUEUE head): "Add A-2-0, A-2 and A-6a to item 13 (e): they may be built, gated and merged before P0 passes." The QUEUE row records the merge hold as lifted.

## Gate run

- **Extract** (GATE-PROMPT step 1). The shallow `--no-local` clone printed scaffold id **0755a61e29858635c2f1fea1fa64214dfe4cce0f**, which is this card's base. There was no `EXTRACT FAILED` line. The sha256 of `fresh-gate.sh` is `40a5b232…5404899` and of `git-env.sh` `8a784782…0b073977`. The extract dir was removed afterwards.
- **Run 1: red, from the environment, not the commit.** The command was GATE-PROMPT's, with `CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-A-6a`. Its four `fresh-gate:` lines were:
  - `commit 1890666a3eb1aac7d92475f4e2e8e0306d9f3c69`
  - `base 0755a61e29858635c2f1fea1fa64214dfe4cce0f`
  - `checks passed (4887 files hashed in 7.42 s)`
  - `2 file(s) matched through their eol conversion`

  Then:
  - `rustc-LLVM ERROR: IO failure on output stream: No space left on device`
  - ``error: failed to write to `/home/tyler/.cache/nv3-target/gate-A-6a/debug/deps/rmetat0FnkB/full.rmeta`: No space left on device (os error 28)``
  - ``error: could not compile `proptest` (lib)``
  - the same error for `nucleus-kv`, `thiserror-impl` and `wasm-bindgen-macro-support`
  - `STEP test EXIT=101`
  - ``error: failed to create directory `…/.fingerprint/e0-btree-1b647825e3fa2e71` … No space left on device (os error 28)``
  - `STEP clippy EXIT=101`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`
  - another ENOSPC from rustc

  The stdout file stops at exactly 4096 bytes, mid-line, because its own write hit ENOSPC. So no `GATE EXIT=` line was recorded. The exit status was 1 and stderr was empty.

  Other projects' cargo builds were filling and freeing this disk. Free space read 53G just after the run and then 94G and 119G minutes later, with load averages of 80 to 100. Nothing in the run names the commit: both red steps failed writing compiler artifacts. I removed only my own 215 MB partial target dir, `gate-A-6a`, and ran the gate once more.
- **Run 2: never ran.** My invocation backgrounded the `fg=` assignment along with the `cd`, so bash was handed `/fresh-gate.sh` and printed `bash: /fresh-gate.sh: No such file or directory` (exit 127). The script never started, so this is not a gate result.
- **Run 3: the gate result.** Run from a wrapper script with the same command and target dir, with free space logged every 15 s (never below 55G). Output:
  - the same four `fresh-gate:` lines, with `checks passed (4887 files hashed in 8.33 s)`
  - `STEP test EXIT=0`, `STEP clippy EXIT=0`, `STEP fmt EXIT=0`, `STEP deny EXIT=0`, `STEP wasm32 EXIT=0`, `STEP core-paths-selftest EXIT=0`, `STEP enforcement-tests EXIT=0`, `STEP api-surface EXIT=0`
  - literal last line **`GATE EXIT=0`**, exit status 0, stderr 0 bytes

  The known `spikes/e1-arm-c` `undo_order.rs` flake did not fire.
- **Why one re-run is not a "re-run loop".** GATE-PROMPT forbids looping on a red gate whose failure may be real. Run 1's failure was ENOSPC in the build itself, on a host whose free space was visibly swinging. I re-ran exactly once. The owner or orchestrator may judge that differently. If so, the remedy is another single fresh-gate run, not a change to this card.

## Scope

- **The diff.** `git diff --stat 0755a61e 1890666a` shows 7 files, +3621/−0:
  - `src/lib.rs` (+1)
  - `src/wal/{mod,op,place,record,seal}.rs`
  - `tests/wal_codec.rs`

  Each is in Touch only. Nothing is in a protected path.
- **`lib.rs`'s one line.** Its only change is `+pub mod wal;`, directly after `pub mod tree;` (Limit 1).
- **The worktree.** It is clean apart from the git-ignored `Cargo.lock`, and `git ls-files -v` shows no assume-unchanged or skip-worktree entry.
- **The merge with A-2.** `git merge-tree --write-tree 1890666a 00d666fd` (A-2's current tip) exits 0 with no conflicted file. The merged `lib.rs` has `pub mod map;` and `pub mod wal;` in their places. A-2's only `codec.rs` change deletes seven attribute lines, so nothing A-6a uses changes. The merged tree was not built (Not verified).

## Acceptance (every line)

- **Lines 1–5**, each in its own `/bin/sh` from the root of my clean scratch export of 1890666a:
  - **Line 1:** exit 0. All ten `nucleus-store` test binaries report `test result: ok`, `wal_codec` among them with 13 passed.
  - **Lines 2, 3 and 4** (`--exact t_drop_restart_floor`, `t_ckpt_dep`, `wal_golden`): each exits 0 with `1 passed; … 12 filtered out`.
  - **Line 5** (wasm32): exit 0.
- **Lines 6 and 7**, run in the worktree, where they only read:
  - **Line 6** (no `std::fs` or the like in `src/`): exit 0.
  - **Line 7** (clean status, files within Touch only, `lib.rs` is `+pub mod wal;`): exit 0, with merge-base 0755a61e.
- **Line 8** (`gate.sh`): the fresh gate's run 3, above.
- **`fuzz_reach_counts(1, 2000)`**: `[[707, 696, 706, 694, 710, 480], [650, 636, 669, 685, 618, 445]]`. DropTree was decoded 696 times from Commits and 636 times from Chunks. RequireCkpt was decoded 480 times from Commits and 445 times from Chunks.

## Correctness review

- **Framing (§12.2).** The fields and offsets are:
  - magic 0..4, `b"NWAL"` in file order (the card's pin of "magic u32 "NWAL"")
  - `len` 4..8 and `crc` 8..12
  - `lsn` 12..20, `prev_lsn` 20..28 and `prev_crc` 28..32
  - `kind` 32, `txn.boot` 33..37, `txn.n` 37..45 and `commit_ts` 45..53

  All are little-endian, and compile-time asserts tie each offset to the one before it. The CRC covers bytes 12..`len`, which is "the bytes after this field", so it covers the chain fields. `PREV_NONE = (u64::MAX, 0)` is "all-ones if none" and "0 if none". A `SegmentEnd` is a bare 53-byte header, as SEG-TAIL-SHORT's "shorter than a `SegmentEnd`" and S-ETA's "four record headers (53 bytes each)" both assume.
- **Kinds' rules.** They follow §12.2 and §7.1:
  - `n ≥ 1` for Commit, Chunk and CommitMarker; `n = 0` for SegmentEnd.
  - `commit_ts` is neither 0 nor `u64::MAX` for Commit and CommitMarker, and is 0 for Chunk and SegmentEnd.
  - `nchunks ≥ 1` and zero ops are the card's pins, under §12.3's delegation.
  - One function per rule serves both encode (`Invalid`) and decode (`Corrupt`).
- **Ops (§12.3 plus the card's pin).**
  - The tags are 1 to 6, and each op's fields come in the spec's order.
  - The widths match the spec: `tree_id` is u64 and the tree `kind` u8 (§6.8), `ckpt_id` is u64 (§2.2), and `w` is u64 (§12.3).
  - A byte string is a `u32` length then its bytes, and `before?` is a presence byte of 0 or 1.
  - The flag bits follow §7.3's provisional flags, with `revert` as bit 1, and every spare bit is refused.
  - The order rule reads §12.3 the card's way: at most one `RequireCkpt`, and then op 0; then CreateTree, DropTree and Ddl in any order; then Put and Delete. `revert` is allowed in a Chunk only.
  - At most one `RequireCkpt` per record is consistent with §14 step 3b ("Every op is checked, not only the first"): that sentence is about the many records pass 2 applies, and DEP-FIRST-ONLY's variant (b) uses two publications.
- **Only canonical records decode.**
  - Every field has exactly one encoding.
  - `len` delimits the record exactly: ops run to the payload's end, a CommitMarker's payload is exactly 4 bytes and a SegmentEnd's is empty.
  - `fuzz_record` re-encodes everything it decodes, through the crate's encoder and through the model.
- **`read_frame`.**
  - It checks, in the card's order: fewer than 53 bytes is `Length`; then the magic; then `len < 53` or `len > buf.len()`; then the CRC.
  - Once a frame reads, no byte at or after `len` has been read.
  - Nit, not a finding: the card's and the doc's sentence "reads no byte at or after `len`" holds for every `len ≥ 53`. For a smaller `len`, the 53-byte header is read first, and when `len < 4` the magic decides between `Magic` and `Length`. The walk never sees such a frame as anything but a stop.
- **`decode_body`** returns only `Corrupt`. `fuzz_record_decoded` checks this, and I traced every error path through `Reader` and `read_list`.
- **Panics, casts and unsafe.** No `unsafe`, `unwrap`, `expect`, `panic!` or indexing appears in `src/wal/`; the crate denies `indexing_slicing`, and the only asserts are `const` ones. Every `as` is a widening of a constant or the `#[repr(u8)]` discriminant.
- **Docs and lints.** Every pub item is documented and cites its §, and clippy `-D warnings` is green.
- **No I/O.** None (Acceptance line 6 and my own reading).
- **Seal.**
  - `Unsealed::new` takes the CRC of bytes 32..`len`.
  - `seal` writes the 20 chain bytes at 12..32 and takes `crc32c_combine(crc32c(0, fields), tail_crc, len − 32)`, written at 8..12.
  - The combine is zlib's `crc32_combine`, line for line, with the reflected Castagnoli polynomial. The zero-bit operator rows are `POLY, 1, 2, …, 1<<30`; it pre-squares twice, alternates even and odd, and returns `crc1 ^ crc2`.
  - **Direct probe** (a `#[cfg(test)]` module appended to `seal.rs` in the scratch copy only, then removed and byte-compared). 1,419 combines equal a direct CRC32C:
    - |A| in {0, 1, 7, 20, 53} × |B| in {0, 1, 2, 3, 8, 21, 31, 32, 33, 255, 256, 4096, 65536, 1 MiB}, zero lengths included;
    - every split of a 300-byte buffer;
    - every tail length that `seal` sees for records of 53 to 1,100 bytes.
- **`place`.** The arithmetic is the card's Work 6:
  - `start`, `room`, the in-place test (`== room` or `+ 53 ≤ room`) and the hop to `start + seg_size`;
  - `Invalid` for a record of under 53 bytes, for one over `seg_size / 4` (so any segment under 212 bytes holds none, which is S-ETA's format floor), and for a room of under 53;
  - every sum is checked.

  After a hop the record always leaves a room of at least 53, since `seg_size / 4 + 53 ≤ seg_size` for every `seg_size ≥ 212`. The card's own "every sum checked" already refuses the last segment below 2^64 for in-place placement. The code also refuses a hop into that segment (ruling 4 (c)).
- **Mutants considered equivalent, and not run.**
  - `start.wrapping_add(seg_size)` in place of the checked add: a wrapped end gives a room of 0, which is `Invalid` either way.
  - A combine that returns `crc1` when `len2 = 0`: `seal` never passes a tail shorter than 21 bytes, and `crc32c(0, [])` is 0 anyway. The probe covers the zero lengths directly.

## Ruling, point 3: the implementer's ESCALATE (A-2-0-F2 not applied)

**Not applying it was right.** The card governs, and the card forbade it:

- Limit 2: "No other existing file changes: not A-2-0's `crc.rs`, `codec.rs`, …".
- Touch only, and Acceptance line 7, which fails on any `codec.rs` hunk.
- A-2-0's verdict folded F2 into "whichever of A-2 and A-6a merges second; **that card's Limits gain this one exception**". A-2's card got that exception (orchestrator amendment 2 to its line 7). This card never did.

An orchestrator's brief cannot widen a card's Limits. A `codec.rs` hunk here would have been a scope failure at this gate (GATE-PROMPT step 2), however harmless its content.

**Who carries the rest.** After A-2 (tip 00d666fd) and this card merge, these remain:

- `Reader::pos`'s `#[allow(dead_code)]` is stale. A-6a's `read_list` now uses `pos`. A-2's tip already deletes `put_u32`'s allow, which A-6a's `encode` uses.
- `Reader::u16`, `Writer::new`, `Writer::u16` and `Writer::len` still have no caller outside `codec.rs`'s own unit tests.

**Carrier: A-6b's re-cut for S-STORE 7.17.** A-6b was written against 7.15 and must be re-cut for S-ETA before it starts, and it is the next card on this lane. Its current Limit 3 forbids every other existing file of the crate, so the re-cut must add the exception explicitly.

- Its Limits gain A-2's amendment-2 exception: `codec.rs` may change only by deleting `#[allow(dead_code)]` lines with their comments, and by deleting helpers that have no non-test caller together with their unit-test lines. For each helper it keeps without a caller, it names the card that will use it (PLAN §6.1).
- It applies the exception to whatever has merged by then: A-6a's merge settles `Reader::pos`, and A-2's merge settles the seven allows its tip deletes.
- If another crate card is cut before A-6b's re-cut, that card takes the same exception instead.

This re-routes A-2-0-F2 itself. No new id is needed, and its QUEUE row should name the carrier.

## Rulings, point 4: the judgment calls and the notes for A-6b

- **(a) `len` written from the bytes: right.** `encoded_len` runs first and refuses anything over `u32::MAX` before a byte is copied. `len` is then set from `bytes.len()`, through a second `u32::try_from`. So §12.2's "bytes of the whole record" holds by construction, even if the length arithmetic and the writer ever drifted. `round_trip` compares the encoder's length with the independent model's.
- **(b) One shared implementation per rule: right, and adequately tested.** It is why only canonical records decode, and why the encoder and decoder cannot drift. The cost is that a mutant of a shared rule moves both sides together, so round trips cannot see it. Explicit tests catch every such mutant I tried: G28, G31, G32, G34, G35, G50 and REQUIRE-NOT-FIRST in the shared rule. They catch it through refusal checks on `encode` and assembled records into `decode_body` (`wal_malformed`, `wal_op_order`, `t_ckpt_dep` (2)).
- **(c) `place` refuses the last segment below 2^64, and a hop into it: consistent with §12. It is not a wrong placement.**
  - §12.1 makes a segment `[first_lsn, first_lsn + wal_seg_size)` over u64 LSNs. In the last segment, the end, the next append position after an exact end and the walk's hop can all be 2^64, which no LSN can name.
  - §12 says nothing about LSN exhaustion. The code fails closed with `Invalid` and never wraps, and every placement it does return keeps "Records never span segments".
  - For in-place placement this is the card's own "every sum checked".
  - The card's literal arithmetic would allow the hop into that segment and then refuse every later append. The code refuses one record earlier, about 16 EiB into the WAL.
  - The tests pin the choice: `wal_place` (g) and the `u128` model. G45 shows the difference is caught.
  - A-6b's walk must keep the same bound with checked sums (A-6a-F1 (b)).
- **(d) Extra API (`RecordKind::from_byte` and `byte`, `Body::kind` and `commit_ts`): acceptable.** Each is small, documented with its §, and used by `encode` or `decode_body`. `from_byte` is what a walk needs for `Frame::kind`. `nucleus-store` has no dependent, so the API-surface lint has nothing to judge, and its step is green.
- **(e) The polynomial repeated in `seal.rs`: acceptable.** Limit 2 forbade the alternative, which is exposing `crc`'s private constant. A wrong constant is caught by eight tests (G17). No follow-up is needed. If A-2-0-F2's carrier takes `crc.rs` as well, it may make the constant `pub(crate)` and drop the copy.
- **(f) `#[expect(clippy::len_without_is_empty)]`: right.** An `Unsealed` is never empty: it holds at least 53 bytes, and the card mandates `len` with no `is_empty`. `expect`, unlike `allow`, turns into a warning the day the lint stops firing, and clippy `-D warnings` is green.
- **Note for A-6b: does `FrameError::Length` need splitting? Not before A-6b.** It does fold three cases together: fewer than 53 bytes, `len` below 53, and `len` past the buffer.
  - Under 7.16, the spec on this base, all three are stops.
  - S-ETA (gated MERGE-WITH-FOLLOWUPS, unmerged) treats short reads by position:
    - at the first LSN of a segment entered by a hop, fewer than 53 bytes is a stop;
    - a record body past the end of file is damage, wherever it lies;
    - any other read past the end of file is damage;
    - "`len` running past the segment" stays a stop.
  - A-6b's card (sha256 `5b33a2c5…4cfebb4de9`, walk steps a–f) already classifies all of these itself, from the room, the end of file and bytes 0..8, before it calls `read_frame` on exactly `len` bytes. There only `Crc` can occur, and its step f says so.
  - So the pinned type is adequate. S-ETA changes how A-6b maps its own steps b and e, not what `read_frame` reports.
  - If A-6b's re-cut would rather have the codec classify, it must split the type under a Limits exception for `record.rs`, which its Limit 3 now forbids (A-6a-F1 (a)).
- **Note: C-T6-rehost's stale "A-2/A-6a enforce it" (the 8 MiB floor): S-ETA-F3 covers it.** It is in S-ETA's verdict, carried through gate 3 (1c3ad006), and QUEUE has the row: "C-T6-rehost still says A-2/A-6a enforce the 8 MiB WAL-segment floor; it is A-7c's. Fold into the C-T6 co-edit card (S-1-F3-F1)."
  - This card enforces only the 212-byte format floor, implicitly, through `place`'s `Invalid`. That matches S-ETA's W5.
  - Nothing in `src/wal/` claims the 8 MiB floor. No A-6a follow-up is needed.

## Follow-ups

**A-6a-F1 (folds into A-6b's re-cut for S-STORE 7.17; card text, before A-6b starts).** Three points for the walk, from this gate:

- **(a)** Map S-ETA's §12.5 "Short segments" onto A-6b's own steps b and e:
  - step b: a stop only at the first LSN of a segment entered by a hop, and damage anywhere else that a read runs past the end of file;
  - step e: damage.

  Keep `FrameError::Length` and `Magic` as "cannot occur" at step f. Never map a `FrameError::Length` to a short-segment outcome, since it conflates three cases. A codec split (for example `Short`, `HeaderLen`, `PastEnd { len }`) is allowed only with an explicit `record.rs` exception in A-6b's Limits.
- **(b)** Use checked arithmetic for `p + len` and for each hop's `s + seg_size`. A stream position that would pass `u64::MAX` is refused, never wrapped. This matches `place`, which puts no record in the last segment below 2^64.
- **(c)** Take the magic, the header length and the `len` offset from A-6a (`RECORD_MAGIC`, `RECORD_HEADER_LEN`, and `record`'s `pub(crate)` offsets, which `wal::walk` can see), not as literals. Then the framing is defined in one place.

None of these blocks this card. A-6a-F1 needs a QUEUE row before the merge, since `merge-card.sh` checks every named follow-up against QUEUE.

**A-2-0-F2, re-routed (not a new id):** see Ruling, point 3.

## Not verified

- **The `u32::MAX` refusals.** The refusals of a byte string, or a whole record, longer than `u32::MAX` have no test: one would need inputs of 4 GiB or more. I checked them by reading only. Three independent checks stand in the way (`string_len`, `write_string`, and the final `u32::try_from(bytes.len())`), so no single mutant gets through.
- **The merged tree.** The tree of A-6a merged with A-2 (00d666fd) was merged textually, not built.
- **The proptest drivers** draw from proptest's per-run seed, and each of my runs is one draw. Every mutant was also caught by a deterministic test.
- **A-6b, A-6c and A-7d do not exist as code.** The adequacy of `FrameError` for A-6b is judged on A-6b's card text (sha256 above) and S-ETA's card (sha256 `80ab3e6e…0986c34b`), not on code.
- **The implementer's report** was read only as QUEUE summarises it. Its scratch dir (`~/.cache/nv3-review/impl-A-6a/`) was not used for any result here.
- **The golden vectors' independence** rests on my Python CRC32C (it passes CRC32C("123456789") = 0xE3069283) and my own encoder of the card's tables. Both are in `~/.cache/nv3-review/gate-A-6a/golden.py`. The fifth vector's full bytes, which the card does not print, are `4e57414c350000006b2145580000000000000000ffffffffffffffff00000000010000000001000000000000000100000000000000`.
