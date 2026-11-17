Card: S-ZETA
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: T3 review sub-agent, Claude Code session 1fd0c91e-03e0-4018-a9b8-755bfca21238 (independent P gate 1 of S-ZETA; it wrote neither the card, its implementation 5c62470b, nor any verdict of A-2-0, and is not the orchestrator)
Date: 2026-11-17
Gate-exit: GATE EXIT=0
Commit: 5c62470b7bf66431f2976b28836449cf60f7c32c
Mutants: spec text, none in code; each walked on the committed text, and the A-2 ones also run in the gate's own executable model of the table form exactly as Z6 lists it (16 symbols, 4 page-0 states, 262,144 cases; ~/.cache/nv3-review/gate-S-ZETA/tools/mutants.py), numbers = cases whose outcome changes: MALFORMED-AS-TORN -> table form (65,876) and fault form later-format runs 1 and 2, IDENTITY-NO-FV-ALG -> table form (36,786), STATICS-AS-FOREIGN -> table form (4,860), IDENTITY-DOMAIN-SKIP -> table form (896) and later-format run 3, FOREIGN-MALFORMED-REFUSES -> table form (3,860) and the foreign copy's second run, UNLOCATED-AS-CORRUPT -> table form (256) and the unlocated variant, META-RANGE-SKIP -> table form (16,193) and t_cover_ts_cross_check, FENCE-BEFORE-REFUSAL -> fault form unlocated variant (byte identity), WRITE-MALFORMED -> write form (forged meta, forged page 0). Gate's own: G-REFUSE-NEWER-ONLY (a refused copy whose ckpt_id is below the newest valid meta's is ignored as stale) -> UNCAUGHT by every test the card specifies (0 of 262,144; 10,124 after RW-3), G-58030-FIRST (the all-unreadable 58030 check runs before page 0's refusal) -> table form (1 case: page 0 format_version 2, U U U U), G2-IDENTITY-NO-FV-ALG (gate 2's code edit, the domain checks kept) -> equivalent (0 cases), G-PAGE0-UUID-FOREIGN (an intact page 0 that is not well-formed is ignored when its uuid is not the replicas') -> UNCAUGHT (no foreign page-0 state; not blocking, RW-1 removes its textual support). Text mutants: each of the 22 blocks left out, and 14 planted errors, each fails its own Acceptance line(s) and line 30 in both locales. All in scratch clones; the worktree was never written
Followups: S-ZETA-F1, S-ZETA-F2

## Verdict in brief

**REWORK, four small text corrections.** The rule is right. Magic and CRC decide that a record exists, the domains decide whether the open may use it, and an intact record of this file that it may not use fails the open in any place. That is the safe long-term design, and nothing below changes it.

What blocks is narrow:

- **A test gap on the rule's central property (RW-3).** The card's rule refuses a record "in any place", because "its `ckpt_id` cannot be relied on". But every refused symbol in Z6's table form sits at a higher `ckpt_id`, and no other specified test has a refused copy below the newest valid meta. So a build that refuses such a record only when it looks newer, and passes over a "stale" one, passes every test. That build is wrong exactly where the rule matters: a later version's `ckpt_id` lies outside the pinned envelope, and a faulty writer's is unreliable. In the gate's model this mutant changes 0 of 262,144 cases. With two of the four refused symbols moved two below the newest, it changes 10,124, and every named mutant stays killable.
- **Two internal conflicts and one wording trap in the new §2.2 text (RW-1, RW-2, RW-4).**
  - RW-1: Z2 calls another file's record "foreign" even over page 0, where Z1, Z4 and §14.2 make it fail the open.
  - RW-2: Z3's 58030 sentence has no exception for a refused page 0.
  - RW-4: the later-format variant expects a detail "naming a place" where the correct detail names page 0. In §2.2's own vocabulary page 0 is not a "place", so a strict test author would write a test a correct build fails.

**Mechanics: all pass.**
- My own parser applies the 22 blocks to the base and rebuilds the committed file byte for byte.
- Only `docs/S-STORE.md` changes.
- The 31 non-gate Acceptance lines pass in both locales, and the checker prints `OK`.
- Every block left out fails its own lines, and so does each of 14 planted errors.
- The scaffold's fresh gate gives `GATE EXIT=0`.

**Point 4 (the P0 question).** At 7.15, under the events 7.15 covers, the text is **not defective**: no covered fault or crash makes the record in question. It is **under-specified**: §2.2 and §14 step 1 are two-minded about intact records with an out-of-range field. It **lacks future-proofing**: a later format version gets a silent rollback with loss after a downgrade. The section below gives the facts the P0 review needs.

## Gate run

- **Extract.** The scaffold's `fresh-gate.sh` and `git-env.sh` came out through GATE-PROMPT step 1's shallow `--no-local` clone.
  - The printed scaffold id was `71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`, with no `EXTRACT FAILED` line.
  - The temporary directory was made under `~/.cache/nv3-review/gate-S-ZETA/` (the brief's scratch root) instead of `/tmp`, and removed afterwards.
- **Run.** From `~/Workspace/neutron-v3`:
  - The command was `PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-S-ZETA bash "$fg/fresh-gate.sh" 5c62470b7bf66431f2976b28836449cf60f7c32c`.
  - Stdout and stderr went to separate files. Stderr was empty (0 bytes), and the script exited 0.
- **`fresh-gate:` lines:**
  - `commit 5c62470b7bf66431f2976b28836449cf60f7c32c`
  - `base 71ecf3e6cd4b328494f6055ad7014efd6d5b2a9b`
  - `checks passed (4874 files hashed in 1.44 s)`
  - `2 file(s) matched through their eol conversion`
- **STEP lines.** `test`, `clippy`, `fmt`, `deny`, `wasm32`, `core-paths-selftest`, `enforcement-tests` and `api-surface`, each `EXIT=0`. The literal last line was `GATE EXIT=0`.
- **The flake.** The implementer's first run failed on the spike test `readers_never_see_past_their_snapshot_during_apply` (QUEUE row E1C-FLAKE-1). This gate did not meet it: one run, green.
  - GATE-PROMPT has no rule for an unrelated flake. Had one occurred, this gate would have recorded both runs.
  - The card changes only a document, so no test outcome can be this card's.
- **Scope.** Against the scaffold, `git diff nucleus/v3-scaffold...HEAD --stat` lists one file: `nucleus/v3/docs/S-STORE.md`, +36/−19.
  - It is in Touch only, and nothing touches a protected path.
  - The commit has one parent, 71ecf3e6.
  - The worktree `~/.cache/nv3-cards/S-ZETA` was clean before and after. Every Acceptance run used a separate `--shared` scratch clone at 5c62470b.

## 1. Mechanics

- **Bytes.**
  - My parser is fence-aware, since E2's Old holds a `## Changelog` line. It took E1, Z1–Z20 and E2 from the card (sha256 `17638fb0…741b` at gating) and applied them in order to the base's 7.15 (`a1376c94…a700f`).
  - Every Old occurs exactly once at its turn.
  - With `<date>` = 2026-11-17 the result is byte-identical to the committed file (`571b861d…030d`).
  - With 2026-11-16 it is the card's recorded `1d086ffd…3206`. The two files differ in exactly two characters, the day digit of the two `<date>` positions, as the implementer reported.
- **Acceptance, both locales.** The 32 commands were extracted with `tools/run_card.py`'s own regexes and with an independent line scan, and the two agree.
  - The 31 before `gate.sh` were run as `/bin/sh -c` from the clone's root, under `LANG=en_CA.UTF-8` and under `LC_ALL=C`, with `/usr/bin/grep` = GNU grep 3.11.
  - Result: 31/31 in each locale. Line 32 is the fresh gate's run.
  - The checker (line 28) printed:
    - `158 rows; 234 mutants; 24 judged only on Phase B cards; 144 judged on a Phase A card at 7.8`
    - `65 new since 7.10 (31 at 7.11, 11 at 7.12, 4 at 7.13, 10 at 7.14, 1 at 7.15, 8 here), 65 of them judged on a Phase A row; 47 invariants, 48 tests`
    - `OK`
- **Omit one block (all 22, not a sample).** Each was committed on its own scratch branch off 71ecf3e6 and run in both locales. The two locales agree on every one. Failing lines:

  | Block left out | Failing lines |
  |---|---|
  | E1 | 1, 2, 29, 30 |
  | Z1 | 7, 25, 29, 30 |
  | Z2 | 8, 25, 27, 29, 30 |
  | Z3 | 9, 25, 29, 30 |
  | Z4 | 10, 25, 29, 30 |
  | Z5 | 11, 25, 29, 30 |
  | Z6 | 12, 25, 29, 30 |
  | Z7 | 13, 26, 29, 30 |
  | Z8 | 14, 25, 30 |
  | Z9 | 14, 26, 27, 30 |
  | Z10 | 15, 27, 29, 30 |
  | Z11 | 16, 25, 27, 29, 30 |
  | Z12 | 17, 25, 29, 30 |
  | Z13 | 18, 26, 27, 29, 30 |
  | Z14 | 19, 25, 29, 30 |
  | Z15 | 20, 25, 30 |
  | Z16 | 20, 26, 27, 28, 30 |
  | Z17 | 21, 25, 27, 28, 29, 30 |
  | Z18 | 22, 25, 27, 28, 29, 30 |
  | Z19 | 23, 25, 27, 28, 29, 30 |
  | Z20 | 24, 25, 27, 29, 30 |
  | E2 | 3, 4, 5, 6, 29, 30 |

- **Planted errors (14, the gate's own, not the card's P1–P31).** Each is one exact substitution on the committed file, committed and run in both locales. Each fails its own line and line 30.

  | Plant | Failing lines |
  |---|---|
  | §14.2's new row's read-only cell `58030` | 19 |
  | Z4 without "whatever the meta places hold" | 10 |
  | Z5's "in any place" → "in its own place" | 11 |
  | Z2's `map_level` "at least 0" | 8 |
  | Z9 "Every read-write open" | 14 |
  | FOREIGN-MALFORMED-REFUSES dropped from the A-7d row | 23, 27, 28 |
  | two mutant names swapped in I-META-COPIES' cell | 20 |
  | a word changed in §14.1 outside every block | 29 |
  | E2's "233 mutants" | 6 |
  | Z3 without "an open that this rule refuses writes nothing" | 9 |
  | Z12 without "with no fallback" | 17 |
  | Z6 without "or 58030" | 12 |
  | Z2's envelope without "at the same offset" | 8 |
  | §14.2's 58030 row read-only cell `XX001` | 19 |

- **Merge orders.** My parser composed this card with R-1-SPEC, S-ETA and S-THETA, each card's blocks parsed from its current file (sha256 `8842b103…`, `ac660c91…`, `f7d7851c…`).
  - Every two-card order applies, and so do the four-card orders tried, with every Old exactly once at its turn.
  - Each pair's two orders are byte-identical from line 4 to `## Changelog`. They differ only at line 3 and in the order of the two top Changelog entries.
  - The four-card result is the same body with this card first or last.
  - This card's checker on the compositions:
    - with R-1-SPEC: `162 rows; 241 mutants; …; 47 invariants, 50 tests`, failing only on R-1-SPEC's seven names and `A-7f rows: 11, not 10`. That is exactly the card's Notes.
    - with S-ETA: `166 rows; 242 mutants; …; 52 tests`, failing only on S-ETA's eight names, as S-ETA's Notes say.
    - with S-THETA: `160 rows; 247 mutants; …; 48 invariants, 49 tests`, failing only on S-THETA's names, as S-THETA's Notes say.
  - S-ETA's Notes were checked against an older S-ZETA card (`a63bc7bc…6539`). The rechecks here use the current one.

## 2. The rule, judged independently

**Method.** I wrote my own executable model of §2.2 at 7.16 from the committed text (`tools/model.py`), beside a model of card A-2-0's "decodes" reading. That second model is A-2-0's code at 53484986 plus A-7d's later range check on the selected meta. I then judged two things:

- **Covered single faults.** Every one I could form from four healthy header states: steady state; a crash after step 4's primary, or after its replica, of the next checkpoint; that checkpoint complete. The faults:
  - on each meta place: a lost, torn, unreadable or misdirected write, the misdirect to every other meta place, to page 0 and to a page slot;
  - page 0 torn or unreadable;
  - both replica blocks torn or unreadable;
  - an 8 KiB slot write misdirected onto the replica region;
  - a foreign copy, format_version 1 or 2, at three `ckpt_id`s, in each replica block.

  That is **200 cases. The 7.16 rule and A-2-0's reading give the same outcome in all 200, and 7.16 refuses none.**
- **The case table below** (`tools/case_table.py`, fixture: page 0 valid, 4 KiB, meta 10 in parity 0, meta 9 in parity 1).

The two tests:
- **(i) No silent rollback:** no open selects an older checkpoint, or serves data as complete, past a newer record of this file.
- **(ii) Availability:** no covered single fault becomes a refusal where 7.15 opened.

**Why no fault within the file makes a refused record (checked against the card's claim and the gate's wider sentence).** Every write into the page file is one of these:
- a whole record of this file, which a correct writer writes well-formed (WRITE-MALFORMED);
- zeros (the fence, the retire write, the rest of a slot or block);
- a page image, whose bytes 4–7 are `kind`, a zero `flags` byte and a zero `u16` (§3.1, kinds 3–9). So it never carries `NUCLEUS\0` or `NUCMETA\0` (bytes 4–7 `EUS\0` / `ETA\0`).

WAL records live in other files and start `NWAL`.

So a lost, torn, misdirected or unreadable write within the file leaves either a record that is not intact or a whole well-formed record of this file. The one exception is a CRC collision, at about 2^-32 per damaged record. There, a torn mix of two of this file's copies could be intact with, for example, `epoch ≠ ckpt_id`: 7.16 refuses it, fail-closed, where A-2-0 passes over it.

- **The card's sentence** ("no fault within the file … with the file's `creation_uuid`") **holds.**
- **The A-2-0 gate's wider sentence** ("no lost, torn, misdirected or unreadable write leaves an intact record that is not well-formed") **is false.** A write misdirected from another file of another version leaves exactly such a record.
- **Two assumptions are worth recording.**
  - The foreign/refused split assumes distinct `creation_uuid`s. A byte copy of the file shares its uuid, the card's case 17: outside the model, and fail-closed here.
  - A lost page-0 write at creation can expose an earlier file's page 0 from reused blocks. It refuses at every reading, as at 7.15.

**The gate's case table** (model outcomes; the read-write and read-only opens are identical at step 1, and a refused open writes nothing in either):

| Case | 7.16 | A-2-0 reading | Right? |
|---|---|---|---|
| One copy of the newest meta, the file's uuid, any of the ten defects (`format_version` 2, `page_size` 12288, `checksum_alg` 2, `wal_seg_size` 0, `copy` 7, `map_level` 0, `epoch ≠ ckpt_id`, `T > ts_ckpt_hi`, `page_size` 8192, `wal_seg_size` 32 MiB) | XX001 | opens 10 (range failure: tie XX001 or the other copy wins) | yes: no covered fault makes it; refusing never rolls back |
| Both copies of the newest, `format_version` 2, `map_level` 0 or `page_size` 8192 | XX001 | **opens 9** | yes; A-2-0 is the card's case 8/9 rollback when the record is a later version's |
| A refused copy at a higher `ckpt_id`, own parity or the other parity's place | XX001 | opens 10 | yes |
| A refused copy at a stale `ckpt_id` (8), or in the previous meta's place (9) | XX001 | opens 10 | yes, but **untested** (RW-3) |
| All four places `format_version` 2, page 0 format 1 | XX001 | XX001 | yes |
| One later-version copy only (a partial upgrade, then a downgrade) | XX001 | opens 10 | yes |
| Both replicas torn or unreadable, a refused copy in a slot | XX001 | opens 9 or 10 | yes |
| A foreign copy (another uuid), well-formed or not, `format_version` 2, page 0 valid | opens 10 | opens 10 | yes: a covered fault, ignored |
| Page 0 intact, the file's uuid, any static field out of domain | XX001 | opens 10 | yes |
| Page 0 intact and foreign, format 1 | XX001 | XX001 | as at 7.15 (not a covered fault for page 0) |
| Page 0 intact and foreign, `format_version` 2 | XX001 | opens 10 | acceptable either way; 7.16's is the uuid-agnostic, safer one (RW-1 makes the text say so) |
| Page 0 intact but not well-formed, all four places unreadable | XX001 | XX001 | yes: no later open can succeed, so 58030's "a later open may not meet it" would be false |
| Page 0 valid, all four places unreadable | 58030 | 58030 | yes |
| Page 0 and both replica blocks unreadable | **58030** | XX001 | yes (S-EPSILON's D35 rule) |
| Page 0 torn with both replicas unreadable, or page 0 unreadable with one replica unreadable and the other torn | XX001 | XX001 | pre-existing (card O-3) |
| Page 0 torn, places sound | opens 10 | opens 10 | yes (covered) |
| Page 0 torn, every copy `format_version` 2 | XX001 | XX001 | yes |
| Page 0 torn, a replica `format_version` 2 or `map_level` 0 | XX001 | opens 10 | yes |
| Page 0 torn, the replicas' page sizes differ | XX001 | XX001 | yes |
| Page 0 torn, a foreign `format_version` 2 copy | XX001 | opens 10 | two faults; fail-closed, as 7.15's literal text |
| Page 0 unreadable, a replica with `page_size` 12288, the other unreadable | XX001 | XX001 | yes |
| Explicit rollback, any step-1 refusal | step 1's XX001 or 58030, never 55000 | (A-7d) | yes |
| Fall-back to meta k−1 from a refused newest record | none (§14 step 4a: "a refusal in step 1" never falls back) | — | yes: a refused record's `ckpt_id` gives no proof |

**No cell of this table is wrong under the card's rule** by (i) and (ii). The card's own table has one inaccurate cell, in card prose and not in the spec: its case 6 says "two identities, XX001 at every reading" for page 0 invalid plus a not-well-formed foreign copy, but A-2-0's reading opens there (the row "Page 0 torn, a foreign `format_version` 2 copy" above).

**The 58030 change and its precedence, against S-EPSILON's read-error rules.**
- S-EPSILON's rule (D35): a read error is not evidence of damage, and a later open may not meet it, so 58030 marks a refusal that a later open could escape.
- **Page 0 and both replica reads failing (58030, was XX001).** The open has seen only read errors, so 58030 is exactly that rule.
- **Nothing is written.**
  - §14 step 1's fence runs only before an open's first write, and "an open refused before its first write fence[s] nothing".
  - FENCE-BEFORE-REFUSAL pins it. A build that fences first would zero both replica blocks and turn a transient 58030 into a later XX001 when page 0 stays unreadable.
- **A refused record takes precedence over 58030.** It makes every later open fail, whatever its reads, so XX001 is the accurate code.
- **The remaining XX001s are pre-existing and outside this card**, where a read error alone stands between the open and success (page 0 torn with both replica reads failed; a valid page 0 with three places unreadable and one torn). The card's O-3 defers them; I agree.

**Outside this card, for the record.** At page sizes of 8 KiB and above, a slot write misdirected onto offset 1 MiB covers both replica blocks. Take parity 1's newest meta, its replica written first:
- its slot keeps the old meta;
- its record lands in parity 0's replica block, as a misplaced copy one above;
- the write's zero tail erases its own replica.

So one fault leaves it no copy in its own parity, and the open selects the meta before it, which recovers the same state (I-TWO-GEN). The model flags this one case of the 200.

That is an exception to I-META-COPIES' wording "never changes which meta opens", not to "what it recovers". Whether a build writes the whole slot is unspecified. It is the same at 7.15 and under A-2-0.

## 3. Future-proofing

**The later-version rule is necessary, correctly stated, and sufficient for what it claims:**
- every build can tell whether a header record of any version is intact;
- every build refuses one with the file's uuid that it cannot use.

It forbids nothing a later version needs. A later version may redefine the meta's bytes 38–162, put additions after the record under their own check, and change domains with a higher `format_version`. A new build reads an old record by the same envelope and knows its layout from `format_version` 1. Neither build can mistake the other's record for a torn one.

**It is not, alone, sufficient for an old build to always refuse a newer file.** Three gaps, all D-8's (§17: "D-6 and D-8 own: … format versioning and upgrades"):
1. **Places.** The rule pins field offsets, not where records live. A later version that moved its metas, and left format-1 metas intact in slots 1–2 or §2.4's replica blocks, would have them opened by an old build.
2. **A single fault after a partial upgrade.** Say the later version wrote format-2 copies of its first checkpoint, and a dropped sync loses both of them (a covered single fault). If page 0 still says format 1, an old build sees only format-1 records. It opens meta k and walks into the later version's WAL. At the first record it cannot read, it stops and truncates there, or, if S-ETA has merged and that record passes every check, it refuses.
   - With page 0 rewritten to the later `format_version` before the later version's first other write, any single fault leaves a record that the old build refuses: page 0, or the later copies when page 0 is damaged.
   - 7.16 already gives that hook: "when page 0 is intact but not well-formed, the open fails with XX001, whatever the meta places hold".
3. **The WAL.** §12.2's framing has no version field, and the rule's list of layouts that `format_version` pins (§2.1, §2.2, §3.1) does not name it. Item 2's page-0 mark covers it.

None of these changes what a 2.0 build does. They are what D-8 must do with the hook 7.16 gives: follow-up S-ZETA-F2.

**A downgrade after an upgrade that wrote only some copies:**
- one later copy, or one of two lost, is refused (XX001);
- both lost with page 0 still format 1 is item 2 above.

**Writer checks.** Creation and §13 step 4 are the only writers of header records, since the fence and the retire write write zeros. Both now check the full domain list, and the write form tests both with a forged value: §15's fault injection, judged on A-7c, where the check's code is.
- The creation hook must inject after any configuration check, or WRITE-MALFORMED's creation half is masked. That is a note for A-7c.
- `gc_w ≤ T` stays a writer-only check (§10.5) and is not in the open's list. That is consistent with 7.15 and not this card's.

## 4. The P0 question: is draft 7.15 defective here?

**Ruling.** Under the events 7.15 claims to cover, **no**:
- 7.15 followed either way has no silent rollback or lost acknowledged data because of an intact malformed record;
- neither has card A-2-0's code following its "decodes" rule.

7.15 is **under-specified** (two-minded), and it **lacks future-proofing** for a later format version. A-2-0's code has the same properties. It has no caller until A-7d.

Event by event:

| Event | Covered by 7.15? | What 7.15 (either reading) and A-2-0's code do | Class |
|---|---|---|---|
| A single lost, torn, misdirected or unreadable write within the file; a crash at any point of the flip | yes | No intact record with the file's uuid that is not well-formed results (the argument and 200-case enumeration in §2); outcomes equal 7.16's | no defect |
| A foreign copy written into a meta place (the fault form's) | yes | ignored at every reading when page 0 is valid; with page 0 also invalid (two faults), the literal reading refuses, and A-2-0 refuses too unless the foreign copy is not well-formed, when it opens | no defect |
| A write from another file over page 0 | no (a foreign format-1 page 0 refuses at every reading) | a later-version foreign page 0: literal reading refuses, A-2-0 opens from this file's replicas | under-specification; no loss |
| A later format version wrote into the file, then this build opens it (downgrade) | no; §17 assigns format versioning and upgrades to D-8; no second format exists | the later copies pass as foreign (literal reading) or as torn (A-2-0), older metas open, and a read-write open then overwrites the later version's places and truncates its WAL: **silent rollback with loss** | **future-proofing only** |
| A faulty format-1 writer | no | literal reading: a malformed copy with the file's identity can be selected; "decodes" reading: a malformed page 0 is masked for the file's life, and malformed metas are passed over as torn, which in every single-bug case I built recovers the same state through I-TWO-GEN | under-specification; defence in depth |
| A deliberate write | no | either | outside any model |

§14 step 1's "A meta whose magic and CRC are valid but whose `T > ts_ckpt_hi` …" is a third two-minded sentence: any copy, or the selected meta (A-2-0's reading). It has the same class: no covered event makes such a copy.

**Facts for the next P0 review (it rules, not this gate):**
- The readings of 7.15 differ only on records that no covered event produces. This gate's enumeration (200 covered single faults, four healthy states) found no outcome difference between 7.16 and A-2-0's reading, and no 7.16 refusal.
- The one silent loss found needs a second format version and a downgrade. 7.15 does not cover that, and assigns format versioning to D-8 (§17).
- The 2.0 build's reaction to a later file cannot be changed after 2.0 ships. So the rule must be in the 2.0 spec before 2.0's open is built (A-7d) and before any 2.0 build ships. That is a due point, not a 7.15 failure under its own model.
- Review 3 §4.3 ruled two comparable items (S-ALPHA-F4, O-3) not defects. The A-2-0 gates called the readings' agreement "on everything I-META-COPIES covers", and this gate confirms that by enumeration.
- The 58030 sub-case changes the code of a refusal, not a safety outcome.

## 5. The card's differences from the A-2-0 gate's text

- **G-1, "another static field": the card is right.** "Another field" would also read as `ckpt_id`.
- **G-2, the domains in §2.2: the card is right.** A refusal now depends on the list, and A-7c's writers must use the same one. It cannot live in a code card.
- **G-3, §14 step 1's range relations in "well-formed": right.** 7.15's words ("A meta whose magic and CRC are valid but …") already read as any intact meta. No covered fault makes such a copy. Under A-2-0's narrower reading, a range-failing copy that loses a tie, or sits in a stale or misplaced place, is passed over silently: the skip this card closes.
  - META-RANGE-SKIP on A-2 is killable: 16,193 cases in the model.
  - §14 step 4a item 2's "runs step 1's range check on meta k−1" becomes redundant and stays true.
  - §14.2's row "The newest meta fails step 1's range check" is now a subset of the new row. That is not wrong.
- **G-4, §14.2's new row limited to the file's uuid: right.** The gate's row would refuse a foreign copy, which is a covered single fault.
- **G-5: right that the gate's sentence is false.** But the replacement's second sentence over-reaches for page 0. That is RW-1.
- **G-6, the unlocated 58030: right** (§2 above). Page 0 torn with both replica reads failed stays XX001, as gate 1 ruled.
- **G-7, the precedence: right in substance.** Z3 still states 58030 unconditionally. That is RW-2.
- **G-8, the detail in the text: right.**
- **G-9, the additions: right.** WRITE-MALFORMED is complete and testable, and FENCE-BEFORE-REFUSAL is killable through the unlocated variant's byte identity. The later-version rule has the limits in §3.
- **G-10, gate 1's "lenient decoder for salvage" dropped: right.** §14.1 salvages no step-1 refusal. A "lenient" reading of a later version's record cannot know its layout. The sentence would promise a path 2.0 does not have. What 2.0 does guarantee instead, that a refused open writes nothing, holds (§2).
- **G-11, "Card A-2" kept: right**, by PL-15's mapping. O-2: keep it.
- **G-12 and O-4, IDENTITY-NO-FV-ALG as a behaviour: right.**
  - With every intact copy's static fields domain-checked, gate 2's code edit (comparisons without `format_version` and `checksum_alg`) is equivalent. Each field has one allowed value, so "equals the identity's" equals "in domain". In the model it changes 0 of 262,144 cases.
  - The behaviour "neither compared nor domain-checked" is killable in every build: 36,786 cases.
  - `checksum_alg` has no table symbol. A skip of that field alone is left to the codec tests, which A-2-0-F1 must keep per field.
- **O-1, a page of image `k` that passes §3.3 but fails its decoder: adopt it, in its own small spec card before A-7d (S-ZETA-F1), not in this card.**
  - Such a page is an XX001 that no path recovers from: no fall-back, no explicit rollback, no read-only salvage. Yet image `k−1` and the WAL may be intact.
  - The proof-guarded fall-back is safe there, unlike case 21, because the selected meta is trusted.
  - The card's one-sentence text is not enough on its own. I-FALLBACK's statement ("on its own only when the newest image fails an integrity check at open"), §14.2's fall-back rows and R18 restate the integrity-only rule and must change with it.
- **The code's home, A-2-0-F1 before A-7c: right.** A-2's Touch only excludes `meta.rs` and `open.rs`, so a dedicated card is "the first card after that spec card that edits `open.rs` or `meta.rs`", which gate 1 allowed. A-7c's writer check needs the shared definition.
- **O-3, a wider 58030 rule: not in this card.** A diagnostics and availability matter (both outcomes refuse, and write nothing); its own card can weigh it.

## 6. Tests, mutants, restatements, merge orders

**Each test passes on a correct build.** All three forms are deterministic, and no form waits on time:
- **The table form** is a pure function of bytes. On the model, a correct rule gives 24,311 opens, 237,576 XX001 and 257 58030 across 262,144 cases.
  - The 257 are page 0 and both replicas unreadable (256), plus page 0 valid with all four places unreadable (1).
  - The size is 16^4 × 4 = 262,144: A-2-0's 11 symbols and 3 page-0 states (43,923, checked against `tests/meta.rs`), plus 5 symbols and 1 state.
- **The write form** forges a value at step 4 and at creation: §15's fault injection.
- **The fault form**:
  - its later-format runs 1 to 3 refuse at step 1, before any write;
  - its foreign copy's second run is ignored;
  - its unlocated variant gives 58030 with nothing written, and the next open selects the faultless meta.

The hooks are named by what they do.

**Each named mutant fails a named test deterministically, on the right Phase A card.**
- The six A-2 mutants and META-RANGE-SKIP each fail the table form (counts in the header). The open rule's code is A-2-0-F1's, labelled "A-2" (PL-15).
- MALFORMED-AS-TORN, IDENTITY-DOMAIN-SKIP, FOREIGN-MALFORMED-REFUSES and UNLOCATED-AS-CORRUPT also fail A-7d's real open.
- FENCE-BEFORE-REFUSAL fails A-7d; the fence is A-7d's code.
- WRITE-MALFORMED fails A-7c; the writers are A-7c's code.

**Two exactness notes.**
- **IDENTITY-DOMAIN-SKIP depends on where the `format_version` 2 symbol sits.** If that symbol always sits in the other parity's place, the table form still kills the mutant, but only through one case (page 0 `format_version` 2, all places unreadable), not by "opening a file whose page 0 and every copy carry `format_version` 2". A-2-0-F1 should put the new symbols in their own parity, like the valid stale copy.
- **G-REFUSE-NEWER-ONLY survives the specified tests:** RW-3.

**Stale restatements.** Outside the Changelog, I read every line with "magic and CRC", "malformed", "decode", "foreign", "identity", "static field", "58030", "page 0", "replica block", "corrupt meta", "range check", "open-time" and "format version" in context.
- None states the old rule.
- Two places are incomplete, without contradiction:
  - §14.2's range-check row (G-3 above);
  - §17's "D-6 and D-8 own: … format versioning and upgrades", which does not point to §2.2's new rule (S-ZETA-F2).
- C-T0's §2.2 references are its own lock table; its only header-related line (248) is about boot ids. C-T6-rehost and `docs/README.md` restate nothing.

**The meaning conflict with S-ETA** (`wal_seg_size` "not 0" here, at least 212 there).
- Once S-ETA lands, "well-formed" must say "at least 212 (§12.1)". This card's own principle is that the domains decide whether the open may use a record, and a segment under 212 bytes is one S-ETA's walk cannot use.
- With both merged and neither change, the file would state two domains for one field: §2.1's and §2.2's.
- S-ETA's Notes carry the fix for both orders:
  - its W34 if this card merges first; its Old (`` `wal_seg_size` not 0; ``) occurs once in this card's result, reworked or not;
  - this card's Z2 New and Acceptance line 8 changing if S-ETA merges first.
- This card's own Notes cover only R-1-SPEC, and should mirror S-ETA's and S-THETA's (card text, below).
- Until S-ETA merges, "not 0" is right for 7.15's format.

## Rework: the one list of corrections

Apply in this order to `nucleus/v3/docs/S-STORE.md` as committed at 5c62470b. Each Old occurs exactly once at its turn, and nothing else changes.

Tried in a scratch clone:
- **The result.** sha256 `6c186f89…25c4` with the commit's date (`70b49911…1bbb` with 2026-11-16). The diff against the scaffold stays `36 19`.
- **The new Acceptance list.** With lines 8, 9, 12, 14 and 30 replaced as given after the blocks, all 31 non-gate lines pass in both locales on that commit.
  - On 5c62470b, the five replaced lines fail.
  - Leaving out any one RW block fails its own line and line 30.
  - Every RW Old still occurs once after R-1-SPEC's, S-ETA's and S-THETA's blocks, in every order tried.
- **The model.** With RW-3's symbols:
  - G-REFUSE-NEWER-ONLY changes 10,124 cases;
  - every named A-2 mutant still changes outcomes: MALFORMED-AS-TORN 65,876, IDENTITY-NO-FV-ALG 36,786, STATICS-AS-FOREIGN 4,860, IDENTITY-DOMAIN-SKIP 896, FOREIGN-MALFORMED-REFUSES 3,860, UNLOCATED-AS-CORRUPT 256, META-RANGE-SKIP 16,193;
  - G2-IDENTITY-NO-FV-ALG stays equivalent.

**RW-1. §2.2, "Intact and well-formed": another file's record over page 0 (Z2; G-5's replacement over-reached) [Acceptance line 8].**

Old:
```text
A write misdirected from another file can leave an intact record that is not well-formed, but that record carries the other file's `creation_uuid` and is foreign (below).
```
New:
```text
A write misdirected from another file can leave an intact record that is not well-formed, but that record carries the other file's `creation_uuid`: in a meta place it is foreign (below), and over page 0 it fails the open, as another file's well-formed page 0 does.
```

**RW-2. §2.2, "At open": the 58030 sentence names the page-0 refusal that precedes it (Z3; G-7) [line 9].**

Old:
```text
if every place's read fails, the open fails with 58030, and so it does when page 0's read and both replica blocks' reads fail
```
New:
```text
if every place's read fails, the open fails with 58030 unless page 0 is intact but not well-formed (below), and so it does when page 0's read and both replica blocks' reads fail
```

**RW-3a. §2.2, `t_meta_copies`' table form: two of the four refused symbols below the newest valid meta (Z6; G-REFUSE-NEWER-ONLY) [line 12].**

Old:
```text
or an intact copy with the file's `creation_uuid` and a higher `ckpt_id` that has `format_version` 2, `map_level` 0, `T` above its `ts_ckpt_hi`, or another `page_size` (four symbols)
```
New:
```text
or an intact copy with the file's `creation_uuid` that has, at a higher `ckpt_id`, `format_version` 2 or `T` above its `ts_ckpt_hi`, or, at a stale `ckpt_id` two below its parity's newest, `map_level` 0 or another `page_size` (four symbols)
```

**RW-3b. §2.2, `t_meta_copies`' table form: the expectation says so (Z6) [line 12].**

Old:
```text
a copy that the rule refuses gives XX001 wherever the open reads it;
```
New:
```text
a copy that the rule refuses gives XX001 wherever the open reads it and whatever its `ckpt_id`;
```

**RW-4. §2.2, the fault form's later-format variant: page 0 is not a "place" in §2.2's vocabulary (Z9) [line 14].**

Old:
```text
fails with XX001, its detail naming a place and `format_version`, and leaves every file as the hook left it.
```
New:
```text
fails with XX001, its detail naming page 0 or a place, and `format_version`, and leaves every file as the hook left it.
```

**Acceptance: replace lines 8, 9, 12, 14 and 30 (the card's numbering from 1) with these.** Every other line is unchanged; line 29 stays `36 19`.

Line 8:

- `cd nucleus/v3 && grep -n '^The rest of the slot, or of the replica block, is zero\. Card A-2 pins the bytes and fuzzes the decoder, which never panics\.$' docs/S-STORE.md && grep -n '^\*\*Intact and well-formed\*\* (card S-ZETA)\. A record, page 0.s or a meta copy.s, is \*\*intact\*\* when its magic and its CRC are valid, and \*\*well-formed\*\* when it is intact and its fields lie in their domains: .format_version. 1, the version these layouts describe; .page_size. one of §2\.1.s; .checksum_alg. 1; .wal_seg_size. not 0; and, in a meta copy, .copy. 0 or 1, .map_level. at least 1, .epoch. the low 32 bits of .ckpt_id., .T ≤ ts_ckpt_hi. and .wal_start_lsn ≤ wal_ckpt_end. (§14 step 1)\. Magic and CRC decide whether a record exists; the domains decide whether the open may use it, and an intact record of this file that it may not use fails the open (below)\. Up to a CRC collision, no fault within the file (a lost, torn or misdirected write, or a read error) leaves an intact record with the file.s .creation_uuid. that is not well-formed or that differs from the file.s identity in another static field: such a record was written whole, by another format version, a faulty writer or on purpose, so the open never takes it for a torn one\. A write misdirected from another file can leave an intact record that is not well-formed, but that record carries the other file.s .creation_uuid.: in a meta place it is foreign (below), and over page 0 it fails the open, as another file.s well-formed page 0 does\. Creation (§2\.3) and §13 step 4 check that each record they write is well-formed before they write it; a record that is not fails creation, and is fail-stop at step 4 (§9\.2), so no correct build leaves one\. Every later format version keeps each record.s first bytes, the magic, .format_version. and, in a meta copy, .copy. and the static fields, at these offsets, and the record.s CRC32C at the same offset over the bytes before it, and puts anything it adds after the record under a check of its own; any change to a layout that .format_version. pins (§2\.1, §2\.2, §3\.1), or to a domain, is a later format version, with a higher .format_version.\. So every build can tell whether a record of any version is intact, and refuses one it cannot use\.$' docs/S-STORE.md`

Line 9:

- `cd nucleus/v3 && grep -n 'A place whose read fails holds an invalid copy; if every place.s read fails, the open fails with 58030 unless page 0 is intact but not well-formed (below), and so it does when page 0.s read and both replica blocks. reads fail, which leaves slots 1 and 2 unlocated (below): the open has seen no damage, only read errors, which a later open may not meet\. A read-write open zeroes every place whose read failed before its first write (§14 step 1, the fence), and an open that this rule refuses writes nothing\. Card A-2 builds the rule' docs/S-STORE.md`

Line 12:

- `cd nucleus/v3 && grep -n '^- \*Table form (A-2)\.\* The rule runs over every combination, for the newest meta and the previous one, of each place holding a valid copy of its parity.s newest meta, the same copy written for its parity.s other place, a valid stale copy, a torn copy, zeros, an unreadable place, a misplaced copy (one .ckpt_id. above the newest valid meta, and two above it), a foreign copy with a higher .ckpt_id., another with .format_version. 2, or an intact copy with the file.s .creation_uuid. that has, at a higher .ckpt_id., .format_version. 2 or .T. above its .ts_ckpt_hi., or, at a stale .ckpt_id. two below its parity.s newest, .map_level. 0 or another .page_size. (four symbols), with page 0 valid, torn, unreadable, and intact with .format_version. 2, and over both tie cases: the newer .boot_id. in either place, and equal .boot_id.s\. Each result is the rule.s: the meta selected, the previous meta, XX001 or 58030\. Among them, page 0.s read and both replica blocks. failing gives 58030, and page 0 torn with both replica blocks. reads failing gives XX001; a copy that the rule refuses gives XX001 wherever the open reads it and whatever its .ckpt_id.; and a file whose page 0 and every intact copy carry .format_version. 2 gives XX001\.$' docs/S-STORE.md`

Line 14:

- `cd nucleus/v3 && grep -n '^- \*Fault form (A-7d)\.\* .*separately, a fault on page 0, on both replica blocks at once, and a foreign copy with a higher .ckpt_id. written into a replica block, in a second run one with .format_version. 2\. Every open selects the meta the faultless run selects and recovers the same state, and every image passes amcheck\. .*\*In-flight variant\* (I-META-SLOT): step 4 of checkpoint .k+1. misdirects its first copy into one of meta .k..s two places, and a crash follows before the second copy.s write; the open selects meta .k. from its other copy and recovers every acked commit\. \*Later-format variant\* (card S-ZETA): a hook takes meta .k..s copy, sets its .ckpt_id. and .epoch. to .k+1. and its .format_version. to 2, re-seals it, and writes it through .Env. into both places of meta .k+1..s parity, as a later format version.s checkpoint would; a second run re-seals only page 0 with .format_version. 2, and a third re-seals page 0 and the copy in every meta place so\. Every open, read-write and read-only, fails with XX001, its detail naming page 0 or a place, and .format_version., and leaves every file as the hook left it\. \*Unlocated variant\* (card S-ZETA): a hook fails every read of page 0 and of both replica blocks at one read-write open, and again at one read-only open; each fails with 58030 and leaves every file byte-identical, and the next open, whose reads succeed, selects the meta the faultless run selects\.$' docs/S-STORE.md`

Line 30:

- `cd nucleus/v3 && test "$(git diff --no-color --diff-algorithm=myers -U0 nucleus/v3-scaffold...HEAD -- docs/S-STORE.md | tail -n +5 | grep -v '^@@' | sed 's/20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/<date>/g' | sha256sum | cut -c1-64)" = '9de849fd3f49b23e331c3a57ed09fc45206b668c9f706eee407048ff045d7666'`

**Card text the rework must update** (the orchestrator's; the implementer edits only `docs/S-STORE.md`):
- Z2's, Z3's, Z6's and Z9's New blocks as RW-1 to RW-4 leave them, so that E1, Z1–Z20 and E2 applied to the base rebuild the reworked file.
- The Acceptance lines above.
- "For the plan card" item 3's alphabet sentence: `format_version` 2 and `T` above `ts_ckpt_hi` at a higher `ckpt_id` in the place's own parity; `map_level` 0 and `page_size` 8192 two below their parity's newest. Still 16 symbols and 262,144 cases.
- Case 6's "XX001 at every reading" (A-2-0's reading opens).
- The dry-run hashes.
- Notes, "If S-ETA merges first" and "If S-THETA merges first", mirroring those cards' Notes, with S-ETA's Z2 change.

## For the plan card

The card's "For the plan card" stands, with these changes:

- **A-2-0-F1.** Its table form follows RW-3: the stale `map_level` 0 and `page_size` 8192 symbols, and the new symbols in their own parity.
  - Its gate re-runs G-REFUSE-NEWER-ONLY and G-58030-FIRST.
  - It keeps per-field codec cases, so that a skip of one field (for example `checksum_alg` or `epoch`) stays killable. Gate 1's RW-2 of A-2-0 is the model.
- **A-7c.** The creation hook of the write form injects after any configuration check.
- **D-8's row** gains S-ZETA-F2.

## Owner points, in plain words (not decided here)

- **OP-1. A file written by a newer version of the database.** When this version opens a file that a newer version has written to, should it refuse with its own error code, 55000 ("object not in prerequisite state"; the card reports that PostgreSQL uses it for incompatible data files), instead of XX001 ("data corrupted")?
  - Either way the open refuses and writes nothing. Only the code differs.
  - With XX001, an operator may restore a backup or run a repair on a file that is merely newer.
  - The card recommends yes, and gives the small spec card it needs.
- **OP-2. A file this version cannot read.** If a file's header holds a record this version cannot use, written by a newer version, a faulty build or on purpose, should the database refuse to open the file at all, read-only included, until a newer version or a repair tool (plan item 15, which does not exist in 2.0) deals with it?
  - The alternative is to open the newest part it can read. That can silently roll back past newer data.
  - Refusing writes nothing, so nothing is lost.
  - There is a cost case: a bug in this version's own writer that damages only bookkeeping fields. Opening the older checkpoint would usually have recovered everything there, and refusing locks the data away until a fix.
  - The writer check added here (WRITE-MALFORMED) is meant to stop this version ever writing such a record.
  - The card recommends yes, with no override in 2.0.

## Merge timing (for the record)

By P0 re-check 1, this card may be built and gated on a branch, but may NOT merge before P0 PASS unless the owner says so in their own words.

## Not verified

- **No code.** No implementation of the 7.16 rule exists yet; it is A-2-0-F1's. Every mutant count above comes from this gate's own model of the text, not from the crate.
  - The model abstracts each record to its fields. It does not reproduce A-2-0's byte layouts or its alphabet's exact `ckpt_id`s.
  - The A-7c and A-7d forms were walked by hand only.
- **The 262,144-case run time** in a debug build was not measured.
- **PostgreSQL.** The card's citation of `ReadControlFile` (card prose, not spec text) was not checked against PostgreSQL's source.
- **Four-card merge orders.** Only the orders named were composed, not all 24.
- **The flake.** The implementer's flake (E1C-FLAKE-1) was not reproduced; this gate's single run was green.

## Follow-ups

- **S-ZETA-F1. O-1's spec card, before A-7d.** A page of image `k` that passes §3.3's four checks but that its decoder refuses (§3.1) counts as failing at open. The proof-guarded fall-back, the explicit rollback and the read-only salvage then reach image `k−1`, where 7.15 leaves no path at all.
  - The card must change, together:
    - §14 step 4a's definition (the card's O-1 block);
    - I-FALLBACK's "fails an integrity check at open";
    - §14.2's fall-back rows;
    - R18;
    - a full restatement search.
  - It adds a test hook that corrupts a page's body before checkpoint `k`'s step 2 seals it, and the mutant FALLBACK-CHECKS-ONLY on `t_fallback`'s A-7d row.
- **S-ZETA-F2. The later-version rule's limits, for D-8 and §17.**
  - **The §17 rider.** With the next S-STORE spec card, §17's D-8 line points to §2.2's rule:

    Old: `**D-6 and D-8 own:** backup, PITR and replica handling of the §12.8 boundary; format versioning and upgrades.`

    New: `**D-6 and D-8 own:** backup, PITR and replica handling of the §12.8 boundary; format versioning and upgrades, within §2.2's rule for later format versions.`

    The Old occurs once at 5c62470b and after the rework, and once in every composition tried.
  - **D-8's plan row**, before D-8 is cut, carries §3's three facts. A later version that writes into a format-1 file:
    - rewrites page 0 with its own `format_version`, durably, before its first other write;
    - keeps writing its header records, in the envelope, to the places format 1 reads, or invalidates them;
    - treats the WAL framing (no version field, and not in the rule's list) the same way, or relies on page 0's mark.

    Otherwise a single covered fault after a partial upgrade still lets an old build roll back.
