# C-T6 re-host: rows, large values, catalog trees and index builds (normative)

Status: draft 1 (card S-3, 2026-10-23). This is a P-authored spec and needs a P gate before it is normative. The row-layer cards implement against this file: B-4a (catalog re-host), B-4b (row format and lazy upgrade), B-4c (TOAST), B-4d (online index build and the bulk-load rule), B-4e (DDL WAL ops), and B-6's drop cleanup. A change to an invariant (`I-*`, §12) is made here first, then in the tests and mutants that pin it, and only then in code.

**What this file is.** It re-hosts two v2 bodies of rules on S-STORE trees and C-T0 v8:
- **C-T6** draft 1 (`nucleus/v2/docs/C-T6-rows-schema-indexes.md` at `nucleus/v2-scaffold` 2e4776fb), together with card C-T6e4fix2's NOT NULL era rule. That rule landed in code (`nucleus-codec/src/row/schema.rs`, 180f9503) but **not** in the v2 file's text, whose §2/§3 NULL clause still states the superseded e4 rule. §3.3 here is normative for it.
- **The C-T5 catalog** as v7.8 §10 states it: catalog rows, storage ids, oids, sequences and the retired-storage record. C-T0 v8 hands every one of these to S-3 (§0.2 "`/sys/catalog/…` rows → catalog trees (S-3)"; §9.3 "its representation belongs to S-3"; §10 tree ids, sequences, catalog).

PLAN-v3 §3.5 asks for this addendum, and §9's S-3 row lists its deliverables: rows, TOAST on overflow pages, and the online index build with the bulk-load rule. S-STORE §17 adds the build's `R`-before-`S` pin, the restart of an unfinished build, and the record of why the pin has no mutant.

**Precedence.** This file refines PLAN-v3 §3.5. If the plan disagrees with this file, this file wins, and §13.3 lists the refinements for PL-1. `docs/S-STORE.md` owns storage and `docs/C-T0-txn-protocol.md` (v8) owns transaction semantics; this file uses both and changes neither. §13.1 and §13.2 list the sentences each should gain at its next edit.

**This file owns:**
- the row and definition bytes;
- where the row layer puts what, inside which tree;
- the catalog trees, the id counters and sequences;
- the retired-tree record;
- TOAST;
- index entry forms, states and builds.

**Error classes** (PostgreSQL SQLSTATEs): XX001 data corrupted, 23502 not-null violation, 25001 active SQL transaction, 53200 out of memory (N16), 54000 program limit exceeded, 54011 too many columns, 55000 object not in prerequisite state, 0A000 feature not supported.

---

## 0. Reading this file

### 0.1 Clause status
| v2 C-T6 | Here | Status |
|---|---|---|
| §1 Layering | §1, §2 | Replaced |
| §2 Row format | §3.1 | Bytes kept verbatim. The NULL clause is replaced (§3.3) |
| §2.1 Value bytes | §3.1 | **Kept verbatim** |
| §3 Table definition | §3.2–§3.4 | Kept with §0.2. The def bytes gain `not_null_since` (§3.2), the decode rule's NOT NULL clause is replaced (§3.3), and the 2704-byte key limit is replaced (§3.4) |
| §3.1 Type tags | §3.1 | **Kept verbatim** |
| §4 Catalog use by DML and reads | §5.2 | Kept with §0.2 |
| §5 Large values | §4 | Replaced. I-TOAST is kept (§4.6) |
| §6 Index definition and states | §2.2, §8 | Kept with §0.2. The entry layout is replaced (§2.2) |
| §6.1 Born-public indexes | §8 | Kept with §0.2 |
| §7 Online index build | §9 | Steps 1–3, the failure and resume rules are kept with §0.2. Steps 4–7 are replaced |
| §7.2 Unique | §9.7 | Kept (0A000) |
| §7.3 Correctness | §9.6 | Replaced. The lock and fence bullets are kept |
| §8 amcheck | §11 | Kept with §0.2, plus I-CAT-TREES and I-TOAST-REGION |
| §9 Open questions and divergences | §14 | Updated |
| v7.8 C-T0 §10: catalog, storage ids, sequences, large values | §5–§7 | Replaced |
| v7.8 C-T0 §12: the C-T6 divergence items (moved here by C-T0 v8 §12) | §14 | Kept, updated |

### 0.2 Substitutions
C-T0 v8 §0.2 applies to every kept clause. In addition, kept C-T6 clauses read with these substitutions. As in C-T0 v8, a kept clause that cannot be read through them is a spec defect: the worker reports it (ESCALATE) and does not resolve it by choice.

| v2 C-T6 text | Reads in v3 as |
|---|---|
| `/t/{sid}/` ‖ pk; `table_lo(sid)`; "the table's storage range" | the relation tree (§2.1); user key `encode_key(pk)` in its row region |
| `/u/{sid}/` ‖ key → row key | the index tree, unique form `0x01 ‖ key` (§2.2) |
| `/i/{sid}/` ‖ key ‖ row key → empty | the index tree, general form `0x02 ‖ key ‖ row key` (§2.2) |
| storage id, `sid`; "map its ranges (C-T5)" | tree id; "create its tree (§5.3)" |
| `table_lo(sid) ‖ 0xFF ‖ toast_id ‖ be32(i)` | the toast region key `0xFF ‖ toast_id ‖ be32(i)` (§4.2) |
| `Core::ingest_versions` | the load of §9 step 6 |
| `insert_key(…, UniqueRule::None)` / `Core::delete_entry` | C-T0 v8 §5.0's key-existence op with no unique rule / entry delete |
| `cleanup_unfinished_builds` | the boot job of §9.5 |
| "header 0x00", "the header key_changed bit" | S-STORE §7.3 tags, through C-T0 v8 §2.3 |
| the 2704-byte encoded-key limit | §3.4 |

---

## 1. Vocabulary

| Term | Meaning |
|---|---|
| relation tree | The one tree of a relation (S-STORE §6.1), of directory kind 2. It holds the relation's rows and their TOAST chunks (§2.1). |
| row region / toast region | The user keys of a relation tree below `0xFF` (rows) and from `0xFF` on (chunks) (§2.1). |
| index tree | The one tree of an index, of directory kind 3 (§2.2). |
| system trees | The catalog trees with fixed ids 1–4, of directory kind 1 (§2.3). |
| entry `E` | An index entry: an index tree's user key and value, computed from one row version (§8). |
| toast id | 20 bytes naming one out-of-line value, unique forever (§4.2). |
| chunk | One `toast_chunk`-sized piece of an out-of-line value, stored as one entry of the toast region (§4.2). |
| created-trees list | A txn's in-memory list of `(seq, tree id)` for each tree it created (§5.3). |
| orphan | A directory tree of kind 2 or 3 that no committed catalog row names (§5.3). |
| counter | A persisted, non-transactional `u64` allocator: tree ids, oids or a sequence (§6). |
| `R`, `f`, `S` (in §9) | The build's pin, its fence commit ts, and its load ts, with `R < f ≤ S` (§9.2). |

---

## 2. Trees: what the row layer puts where

The row layer addresses storage **only by tree id and user key**. It never sees an LPID, a slot or an entry key's ts suffix, except where §9 reads a version's ts through the scan.

### 2.1 Relation trees (kind 2)
A relation tree holds two disjoint regions:

| Region | User key | Entry value (S-STORE §7.3 tag, via C-T0 v8 §2.3) |
|---|---|---|
| rows | `encode_key(pk)`, the C-Q3s §3 composite key; or, for a table with no primary key, the hidden-rowid key `encode_key([bytea ASC], rowid)` (v2 §3, kept) | the row bytes of §3.1: tags 0, 2, 4, 5 (live), 1, 3 (tombstones), 6–8 (provisional) |
| toast | `0xFF ‖ toast_id ‖ be32(i)` (§4.2) | the chunk's bytes: tag 0, or 4 when over `V_INLINE`; tombstone tag 1; provisional tags 6–8 |

- **Why the regions cannot collide.** Every C-Q3s encoded key begins with a marker byte `0x00`–`0x02`, which DESC does not invert (C-Q3s §2). No row key therefore starts with `0xFF`, and the escape of S-STORE §7.2 maps both regions monotonically, so every toast entry sorts after every row entry.
- **Row scans** are bounded to the user range `[∅, 0xFF)`. In encoded form the upper bound is `prefix(0xFF)`, excluded (S-STORE §8.3 bounds table). Mutant **SCAN-INTO-TOAST** (I-TOAST-REGION).
- **Why one tree.** It keeps S-STORE §6.1's one tree per relation. TRUNCATE and DROP retire rows and chunks together (§7). GC, the DropTree walk and the lock table each see one tree. A toast tree per relation would add a tree id to every relation row, and a second CreateTree and DropTree to every TRUNCATE, for no gain in 2.0.

### 2.2 Index trees (kind 3)
An index's entries take one of two forms. The form an entry takes is v2 §6's rule, kept:
- **Unique form:** user key `0x01 ‖ key`, value = the encoded row key (the row's user key in the relation tree). Used by a unique index whose key has no NULL column, or which is `nulls_not_distinct`.
- **General form:** user key `0x02 ‖ key ‖ row key`, with an empty value. Used by every other entry: non-unique indexes, unique indexes for keys with a NULL column, and deferrable unique indexes (v7.8 §5.3).

Here `key` is the C-Q3s encoding of the indexed columns, with their direction and NULL placement. The family byte keeps v2's two ranges (`/u/`, `/i/`) apart inside one tree. A scan of a unique index reads both families, as v2 read both ranges. A deferrable-prefix lock (C-T0 v8 §0.2) covers `[0x02 ‖ p, succ(0x02 ‖ p))`.

**General-form entries are row-specific:** their user key contains the row key. §9.4 relies on this.

Index entries are always inline: a unique-form value is a row key, which is bounded by §3.4.

### 2.3 System trees (kind 1)
| Tree id | Name | User key | Value |
|---|---|---|---|
| 1 | `cat_rel` | `be64(oid)` | RelRow (§5.1) |
| 2 | `cat_idx` | `be64(oid)` | IdxRow (§5.1) |
| 3 | `cat_retired` | `be64(id)` | RetiredRow (§7.1) |
| 4 | `cat_counter` | `be64(counter id)` | `be64(H)` (§6) |

- **Ids.** Tree ids 5–63 are reserved. User trees start at 64 (§6).
- **They are ordinary versioned trees**, not system keys (C-T0 v8 §2.5 has none):
  - they are written through C-T0 v8 §5 by ordinary txns;
  - they are read at registered timestamps;
  - their old versions are removed by S-STORE §10 GC;
  - they are recovered from the WAL like any tree.

### 2.4 Directory kinds
S-STORE §6.8's `kind u8`:
- **Values:** 1 system, 2 relation, 3 index. Any other value is XX001 at open and in amcheck.
- **What the store does with it:** nothing beyond carrying it and dispatching amcheck. The values are a row-layer assignment (§13.1 N2).

---

## 3. Rows

### 3.1 Row bytes (v2 §2, §2.1, §3.1, kept verbatim)
```
row           = 0x52 0x01 be32(schema_version) be16(natts) nullmap datum*
datum         = 0x00 be32(len) value_bytes  |  0x01 toast_pointer
toast_pointer = toast_id(20) be32(raw_len) be32(chunk_count)
```
The nullmap, the value bytes (v2 §2.1) and the type tags (v2 §3.1) are kept verbatim.

**Decoding errors.** Trailing bytes, a short datum, an unknown tag or an undecodable value is XX001, never a panic.

**Column limit (new).** `natts ≤ 1600`. A CREATE TABLE or an ADD COLUMN that would allocate attnum 1601 gets 54011, as in PostgreSQL (`MaxHeapAttributeNumber`). §4.1's row bound depends on this limit.

### 3.2 Definition bytes (v2 §3, with C-T6e4fix2's marker)
```
def   = 0x54 0x01 be32(version) u8(hidden_rowid) be16(ncols) col* be16(npk) be16(pk_attnum)*
col   = be16(attnum) be32(namelen) name type flags(1) [be32(not_null_since)] [be32(len) value_bytes]
flags = bit0 not_null, bit1 dropped, bit2 has_missing, bit4 has_not_null_since
```
**`not_null_since`** is the def version at which the column's NOT NULL constraint last came into force. 0 means the column is nullable.
- **The word's presence.** The `be32` word is present iff the column is live and its `not_null_since` differs from the **derived** value, which is `version` for a NOT NULL column and 0 otherwise. The elision keeps every def a DDL helper produces round-tripping exactly. Reference: `nucleus/v2/crates/nucleus-codec/src/row/schema.rs` at 2e4776fb.
- **Decoding rejects**, with XX001:
  - `not_null` with since 0;
  - a nullable column with since ≠ 0;
  - since > `version`;
  - any unknown flag bit.
- **Maintenance.**
  - CREATE TABLE sets since = 1 on each NOT NULL column, primary-key columns included.
  - ADD COLUMN … NOT NULL and SET NOT NULL set it to the new version. A re-SET after a DROP takes the newer version.
  - DROP NOT NULL resets it to 0.
  - A dropped column carries no era: encode canonicalises it to flags bit1 only.

The rest of v2 §3 is kept with §0.2:
- the hidden rowid;
- attnums are never reused, and `version` is +1 per def-changing DDL;
- ADD COLUMN (fast default stored as `missing`; volatile defaults, `serial` and identity columns → 0A000);
- DROP COLUMN;
- SET NOT NULL's validating scan (latest committed state plus own writes, under the lock; 23502);
- AccessExclusive is taken before the def is read.

A latest-state scan registers `visible_ts` (C-T0 v8 §3.6).

### 3.3 The NULL era rule (replaces v2 §2's NULL clause and §3's "NOT NULL binds at `v == V`")
Take a row with header version `v`, decoded under def `D`. A NULL bit on a live attnum `a ≤ natts` of `D` is **XX001 iff `a.not_null` and `v ≥ a.not_null_since`**. Otherwise the NULL is served.
- **Why it is corruption.** A row encoded at `v ≥ since` went through `encode_row`, which refuses that NULL, so a stray one is corruption.
- **Why the lower range is legal.** A row encoded below `since` may legally carry a pre-constraint NULL that is still visible at an older snapshot (the e4 history: a NULL committed before a concurrent SET NOT NULL, whose validating scan saw the latest state). That holds **whatever DDL came later**.
- **The audit probe.** SET NOT NULL (v2), then ADD COLUMN (v3), then a planted v2 row with a NULL on that column: the read must give XX001. The superseded `v == V` rule served it.

The rule is **one shared helper**, used by every decode path (reads, EPQ base decode, UPDATE/DELETE decode) and by amcheck's I-ROWFMT. The rest of v2 §3's decode rule is kept:
- `v > V` or `n > D.ncols` → XX001;
- attnum `> n` → `missing` if present, else NULL;
- dropped attnums are skipped;
- `decode_row_raw` returns every attnum `≤ n`, dropped ones included.

**Lazy upgrade (kept).** Every write encodes at the current `version` with `natts = D.ncols`. No DDL rewrites rows. Mutant **ENCODE-AT-ROW-ERA**: an UPDATE re-encodes the row at the old row's version and `natts`.

### 3.4 Key limits (replaces v2's 2704 bytes)
**The limit.** A row's user key and an index entry's user key are bounded by S-STORE I-KEY-SIZE: the entry key `|k| + zeros(k) + 10 ≤ K_MAX(page_size)`.

**Where it is checked:**
- **Writes.** It is checked when the write enters the write set, with 54000. That covers an insert, a primary-key change and an index-maintenance insert.
- **Builds.** A build that emits an entry over the limit fails with 54000 (§9 step 5). Mutant **BUILD-SKIPS-LONG-KEY**: the build drops the entry and carries on.
- **Entry deletes.** An entry delete whose key is over the limit is skipped, because no such entry can exist (kept).

The limit now depends on the page size and on the escape (§14 D7).

---

## 4. Large values: TOAST on overflow chains (replaces v2 §5)

### 4.1 When a datum goes out of line
At row encode:
1. **Threshold.** Every non-NULL datum whose value bytes exceed **2048** goes out of line (v2's rule, kept).
2. **Row bound.** Then, while the encoded row exceeds `ROW_LIMIT = 1 MiB`, the largest remaining inline datum goes out of line too. Ties go to the lowest attnum.

The result is deterministic in `(def, values)`.
- **Why the bound always holds.** After step 2 the row is at most `ROW_LIMIT`. Even a 1600-column row of toast pointers is about 46 KB.
- **No compression in 2.0** (§14 D8).

### 4.2 Toast ids and chunks
**Toast id.** `toast_id = be32(epoch) be64(n) be32(seq0) be32(ord)`: the inserting txn's `TxnId`, the statement's `seq0`, and `ord` from the statement's single ordinal counter. That counter is shared with the hidden rowid and is never reset per call (v2, kept).
- **Uniqueness.** A toast id is unique forever, because `TxnId`s never repeat across boots (C-T0 v8 §2.5, I-EPOCH). The hidden rowid relies on the same fact.

**Chunks.** A value of `raw_len` bytes is split into `chunk_count = ⌈raw_len / toast_chunk⌉` chunks.
- **Size.** `toast_chunk = 256 KiB`. Every chunk is full except the last, which is non-empty.
- **Keys.** Chunk `i` is the toast-region user key `0xFF ‖ toast_id ‖ be32(i)` in the relation tree (25 bytes).
- **Storage.** The store keeps a chunk's bytes inline or in an overflow chain (S-STORE §6.7, tags 4 and 8). The row layer never addresses pages: "TOAST on overflow pages" (PLAN-v3 §3.5) means that every chunk is one entry, and the store places its bytes.

### 4.3 Why chunk at all (I-OP-BOUND)
**Every op fits a record.** Every op the row layer writes must fit one WAL record. A record is at most `wal_seg_size / 4` bytes and never spans segments (S-STORE §12.2). A 1 GB datum stored as one entry would be one `Put` that no record can hold. With §4.1's bound and `toast_chunk`, every row-layer op is at most `ROW_LIMIT` plus its key and framing.
- **Requirement on file creation:** `wal_seg_size ≥ 8 MiB`, so that a 1 MiB op fits with room to spare. A-2/A-6a enforce it at file creation (§13.1 N3).
- Mutants:
  - **ROW-LIMIT-OFF:** no step 2 in §4.1;
  - **CHUNK-UNBOUNDED:** one entry per value.

**It bounds the chain free.** It also bounds S-STORE §6.7's chain free per entry to `toast_chunk / page_size` pages (32 at 8 KiB), so TOAST never needs §19 Q6's resumable free.

### 4.4 Writing (v2 §5 kept with §0.2)
- **When chunks are written.** Toast ids are assigned when the new row value is built. The chunks are written in the row's statement, after the row op returned Applied, as key-existence ops with no unique rule (C-T0 v8 §5.0). A skipped row op writes none.
- **Old pointers.** The old pointers of an update or a delete are taken from the **raw** row (`decode_row_raw`, dropped attnums included) of the version the row op acted on (its EPQ base). Each old pointer that is not carried into the new version has every one of its chunks deleted by an entry delete, in the same statement. Mutant **TOAST-DECODED-NOT-RAW**: old pointers are taken from the decoded row, so a dropped column's chunks leak.
- **Carried pointers.** A carried pointer is reused as it is: an UPDATE that leaves a toasted column unchanged writes no chunk. Mutants:
  - **TOAST-CARRIED-DELETED:** a carried pointer's chunks are deleted;
  - **TOAST-OLD-KEPT:** a dropped pointer's chunks are kept.
- **Locks.** Chunk writes lock their keys like every key-existence op and entry delete (C-T0 v8 §2.2). They are not exempt. They **never wait**, by this argument:
  - a chunk key is written by its inserter, whose `TxnId` it contains;
  - and by at most one later txn, the one that replaces the newest version carrying the pointer. That txn holds the row's exclusive lock;
  - pointers are never re-adopted, because a new value gets a new toast id;
  - so row locks serialise the writers of one chunk key, and an acquire of a chunk key never meets a live foreign holder.

  A txn's chunk keys are contiguous, since they begin with its `TxnId`, so escalation (C-T0 v8 §6.2) coalesces them into few runs.
- **Large txns.** A large txn's chunk writes materialise as provisional entries like any write (C-T0 v8 §3.2).

### 4.5 Reading (v2 §5 kept with §0.2)
- **The snapshot.** The chunks of a version with commit ts `t` are read at a registered ts:
  - for a row read at `S`, that is `S`;
  - for an EPQ base version `v`, it is `register_at(v.ts)`, which is admitted because `W ≤ S < v.ts ≤ visible_ts`.
- **Own rows.** The chunks of the txn's own uncommitted rows are read through its own layers (C-T0 v8 §2.1, §4 step 1).
- **No SIREAD.** The row read took the SIREAD, and a chunk's content is a function of the row version.
- **Errors.** Each of these is XX001:
  - a missing chunk;
  - a chunk with `i ≥ chunk_count`;
  - an empty chunk;
  - a chunk other than the last whose length is not `toast_chunk`;
  - chunk lengths that do not sum to `raw_len`.

### 4.6 I-TOAST (kept)
**The invariant.** At every snapshot `S ≥ W`:
- each pointer in a row visible at `S` (raw, dropped attnums included) names chunks `0..chunk_count`, all visible at `S`, summing to `raw_len`;
- every chunk visible at `S` is named by such a pointer.

**GC needs no TOAST rule.** A chunk key has one put, at the inserter's ts `c₀`, and at most one tombstone, at the ts `c` of the version that dropped the pointer. Every row version that names the chunk has its ts in `[c₀, c)`. S-STORE §10.2 therefore removes the chunk only once some `w ≥ c`, and then no snapshot `S ≥ W ≥ c` reads a version that names it.

**DropTree** frees chunks together with their tree (§2.1).

---

## 5. Catalog (the C-T5 re-host)

### 5.1 Catalog rows
v2's bytes (`nucleus-txn/src/catalog/rows.rs` at 2e4776fb) are kept, with the storage id read as a tree id:
```
RelRow = 0x01 be64(oid) kind(1) be64(tree_id) bytes(name) bytes(def)                 # kind 0x01 = table
IdxRow = 0x02 be64(oid) be64(owning_rel) 0x01 be64(tree_id) bytes(def)
bytes(x) = be32(len) x
```
- **`IdxStorage::Range`** (tag `0x02`) is not used in v3, because every index has a tree (§2.2). A decoded `0x02` is XX001. B-4a retires its callers, and B-5's RESULT lists any test retired with it.
- **Keys.** The rows are keyed `be64(oid)` in `cat_rel` and `cat_idx` (§2.3).
- **The `def` payloads** are §3.2's bytes (RelRow) and v2 §6's index def (IdxRow, kept).

### 5.2 Catalog use (v7.8 §10 and v2 C-T6 §4, kept with §0.2)
These are kept:
- transactional DDL;
- AccessExclusive taken first, with the lock-mode exceptions (online build ShareUpdateExclusive per step, born-public Share);
- the SSI map journal, now a tree id → relation oid map (C-T0 v8 §12 Q7). Its boot rebuild scans the committed catalog at `visible_ts` before the first user txn;
- v2 C-T6 §4's statement rules. The RR/SER relation rule compares the **tree id** of the relation row at `S` with that of the latest committed row (40001 on a foreign TRUNCATE). An index serves a read at `S` iff it is Public and `built_at ≤ S`.

### 5.3 Tree lifecycle
**Create.** A DDL that needs a tree (CREATE TABLE, CREATE INDEX of any kind, TRUNCATE) does three things:
1. it allocates a tree id (§6);
2. it runs S-STORE CreateTree **eagerly, at execution, without a WAL record**, and appends `(seq, id)` to its created-trees list;
3. it logs `CreateTree{id, kind}`:
   - a small txn logs it in its `Commit` record, for each tree still on its created-trees list at commit;
   - a large txn logs it in a `Chunk` written **at or before the first chunk that holds an op on the tree** (I-DDL-WAL).

The tree is created eagerly because the txn's own reads, and a large txn's provisional entries, need it to exist before commit.

**`ROLLBACK TO s`.** Every tree created at `seq ≥ s` leaves the created-trees list and is dropped when the txn ends. A large txn's already-written `CreateTree` chunk may then recreate the tree at replay. That tree is an orphan, and the boot job below removes it.

**Abort.** The cleanup continuation (C-T0 v8 §7.1) drops every tree on the created-trees list (S-STORE DropTree, without a WAL record), then finishes its own provisional-entry cleanup, then deregisters. Provisional entries inside a dropped tree are freed by the walk, and §7.5 explains why no reader needs them first. Mutant **ABORT-KEEPS-TREE**.

**Commit.** Created trees that committed catalog rows name live on. A tree created and dropped in one txn is retired through §7 like any other.

**Orphans.** Let `H_boot` be the tree-id counter's recovered `H` (§6). After recovery, a live directory tree of kind 2 or 3 is an **orphan** if:
- its id is below `H_boot`; and
- no committed catalog row at `visible_ts` names it, that is, no current RelRow or IdxRow tree id and no RetiredRow.

The **orphan boot job** (B-6) drops every orphan. It runs after recovery's dropping walks have resumed (S-STORE §14 step 6), and it may run concurrently with txns. Every id this boot allocates is at least `H_boot`, so a tree created by a running DDL is never mistaken for an orphan. Mutant **BOOT-NO-ORPHAN-SWEEP**.

**Why the unlogged directory mutations are sound:**
- **(a)** An image holds an uncommitted tree only as an orphan: it is empty, or holds only provisional entries, which are dead after recovery. The orphan job removes it.
- **(b)** Every committed tree's creation is logged at or before its commit, and CreateTree replay is idempotent (S-STORE §6.8). If the image missed the eager creation, replay creates the tree before any op on it.
- **(c)** S-STORE §12.8.1 requires that a bulk load's tree be created by a logged CreateTree. §9's T0 commits that op before the load.

Mutant **CREATE-NOT-LOGGED**: the `CreateTree` is omitted from the commit. A crash with no checkpoint then leaves the catalog naming an absent tree, which is XX001 at the first read.

S-STORE §6.8 does not yet say that a directory mutation may precede its logged op, or happen without one, for a tree that no committed catalog row names. §13.1 N1 lists that sentence.

**I-CAT-TREES** (quiescent, after the boot jobs):
- the live directory trees of kinds 2 and 3 are exactly the trees named by committed catalog rows, current or retired-and-not-yet-dropped;
- every named tree exists: live, or `dropping` if it is retired.

### 5.4 Bootstrap
S-STORE §2.3 creates the file with an empty tree directory.
- **When it runs.** An open that finds no tree 1 runs the **bootstrap txn** before any user txn, after the boot checkpoint (C-T0 v8 §3.7).
- **What it is.** One synchronous small commit holding `CreateTree{1..4, kind 1}` and the two counter rows of §6: tree ids with `H = 64`, oids with `H = 16384`.
- **Crashes.** It is one record, so after any crash it is present whole or absent whole. If it is absent, the next open runs it again.

### 5.5 The `Ddl` op
S-STORE §12.3 leaves the `Ddl{bytes}` payload to the catalog. Its bytes are:
```
Ddl = 0x01 be32(n) be64(oid)*      # the oids whose cat_rel or cat_idx rows this txn changes, ascending
```
- **Who writes it.** Every txn that writes `cat_rel` or `cat_idx` writes exactly one. A small txn puts it in its `Commit` record. A large txn puts it in the last chunk written at step 1′ (C-T0 v8 §3.2).
- **Recovery ignores it.** The catalog rows replay as ordinary `Put`s.
- **Its consumers** are replicas and CDC (D-6, N14), which use it to invalidate catalog caches.
- Mutant **DDL-OP-MISSING**, caught by a WAL-decode test (B-4e).

### 5.6 Op order (I-DDL-WAL)
**The order rules:**
- In every record, `CreateTree`, `DropTree` and `Ddl` precede `Put` and `Delete` (S-STORE §12.3).
- Across a large txn's chunks, `CreateTree{t}` lies in a chunk at or before the first chunk with an op on `t`.
- No record holds both a `DropTree{t}` and an op on `t`.

**Consequence:** no committed op ever targets a tree that is absent at its replay position. Why:
- **Writes follow creation.** Ops on `t` come only from txns that read a catalog naming `t`. Such a catalog exists only after `t`'s creating commit.
- **Drop follows the last write.** `t` is dropped only once `W >` the retiring DDL's ts (§7.4). By then every txn that read a catalog naming `t` has committed below that DDL or aborted. A large one has also finalized (§7.5).

Replay of an op into an absent tree is therefore XX001. Mutant **CREATE-AFTER-PUT**: a large txn's `CreateTree` chunk follows its first op on the tree.

---

## 6. Counters and sequences

**One mechanism** serves the tree-id counter (counter id 0), the oid counter (counter id 1) and every SQL sequence (counter id = the sequence's oid). The SQL surface of sequences is D-10b's.
- **State.** The persisted state is the `cat_counter` row `be64(id) → be64(H)`, where `H` is the end of the last reserved block. In memory there is a cursor `(next, limit)` behind a per-counter mutex.
- **`alloc()`.** If `next < limit`, return `next++`. Otherwise, **reserve**:
  1. run a **reservation txn**: an internal small txn with `synchronous_commit=on`. It acquires the counter's key exclusively, reads `newest` (S-STORE §8.2; absent means the counter's start), writes `H' = H + B`, commits, and **waits for the ack** (C-T0 v8 §3.4, I-ACK);
  2. only then set `next = H`, `limit = H'`, and return `next++`.

  A failed reservation hands out nothing and leaves the cursor as it was.
- **Block sizes.** `B = 64` for tree ids and oids (v2 `ID_BLOCK`), and `B = 32` for sequences (v2 `seq::BLOCK`).
- **Never repeat** (C-T0 v8 §10, v7.8 kept). Every value handed out is below an acked, sync-on `H`, which is durable (S-STORE I-REDO-COMPLETE). So across any crash, values may skip (an unused block is lost) and never repeat. That includes values whose caller never committed.
- **Non-transactional.** The reservation txn is not the caller's txn. It commits even if the caller aborts, and it is outside the caller's write set, locks and SSI state.
- **Lock order.** The per-counter mutex is held across the reservation commit, so it sits **above** every lock the commit pipeline takes. Nothing that holds a pipeline lock, a page latch or a lock-table partition calls `alloc()`. Only reservation txns lock a counter key, and only under its mutex, so a reservation never waits on another txn and cannot deadlock.
- **Ranges.**
  - Tree ids start at 64 (1–63 are system trees, §2.3).
  - Oids start at 16384, PostgreSQL's `FirstNormalObjectId`. pg_catalog's fixed oids (C-11) stay below that.
  - RetiredRow ids come from the oid counter, as in v7.8.
- **Read-only opens** (N11) never allocate.

**I-SEQ-NO-REPEAT.** No value of a counter is handed out twice, across any sequence of crashes. In particular, no tree id that a WAL record or an image names is ever allocated again. Mutants:
- **HANDOUT-BEFORE-ACK:** the block is used before the reservation's ack;
- **RESERVE-ASYNC:** the reservation commits with `synchronous_commit=off`.

---

## 7. DDL retirement and TRUNCATE (C-T0 v8 §9.3; S-STORE §12.8)

### 7.1 The retired-tree record
```
RetiredRow = 0x01 be64(rel_oid) be32(n) be64(tree_id)*      # v2's bytes, with ranges replaced by tree ids
```
- **Where it lives.** It is in `cat_retired` under `be64(id)`, with `id` from the oid counter. There is one per retiring DDL statement.
- **How it is written.** The DDL's own txn writes it, so it commits, aborts and rolls back to a savepoint with the DDL (C-T0 v8 §9.3).

### 7.2 What retires what
| DDL | Retires |
|---|---|
| DROP TABLE | the relation tree and every index tree |
| DROP INDEX | its index tree |
| TRUNCATE | the relation tree and every index tree, non-public ones included, after creating their replacements (§7.3) |

### 7.3 TRUNCATE (S-STORE §12.8, "not an unlogged load")
Under AccessExclusive, in one txn:
1. allocate a tree id for the relation and for each of its indexes, and create each tree (§5.3: eagerly, with `CreateTree` logged in the commit);
2. rewrite the RelRow and every IdxRow with the new tree ids;
3. write the RetiredRow naming the old tree ids.

That is S-STORE §12.8's three logged steps: steps 1–2 there are this commit, and step 3 there is §7.4. Nothing is unlogged, so no forced checkpoint is needed. Concurrent readers at `S` below the commit keep reading the old trees until `W` passes it (C-T0 v8 §9.3). `RESTART IDENTITY` and owned sequences are D-10b's.

### 7.4 Drop cleanup (B-6; C-B1 semantics)
The drop-cleanup job reads committed RetiredRows at a registered snapshot `S`. For a row visible at `S`, once `W > S`, it runs **one small commit** that holds:
- `DropTree{t}` for each of the row's trees;
- `Delete{cat_retired, id}`.

**What follows from the commit:**
- **The walk.** Apply sets each tree to `dropping` (S-STORE §6.8 step 1). The bounded walk (steps 2–3) then runs as a background job, and the walk resumes at open (S-STORE §14 step 6).
- **Atomicity.** The DropTree ops and the row delete are in one record, so a crash keeps both or neither. v2 needed I-WAL-ORDER's argument for this; one record gives it outright.
- **Idempotence.** A DropTree of an absent or `dropping` tree only resumes the walk.

Mutants:
- **DROP-BEFORE-W:** the drop runs at `W ≥ S`, or at `visible_ts`;
- **DROP-SPLIT-COMMIT:** the row delete commits before the DropTree. A crash between the two leaks the trees (I-CAT-TREES).

### 7.5 Nothing races the DropTree walk
Let `D` be the retiring DDL's commit ts, and `t` a retired tree.
- **No reader.** Every reader with `S ≥ W > D` resolves the catalog at `S` and so uses the replacement trees (or none). Any AS OF `t' ≥ W` does the same. A reader with `S < D` is impossible once `W > S ≥ D`. So no read reaches `t` once its drop runs.
- **No finalize.** Take a large txn `L` with provisional entries in `t`. `L` held RowExclusive, which conflicts with the DDL's AccessExclusive, so `L` ended before the DDL acquired its lock. If `L` committed, at `c`, then `c < D`. The drop needs `W > D > c`, and `W ≤ M` (C-T0 v8 §3.5), so `M ≥ c`: `L` has finalized, and no conversion into `t` remains. Mutant: C-T0 v8's W-ABOVE-M, here caught by `t_drop_waits_finalize`.
- **Abort cleanup.** An aborted `L`'s cleanup may still be deleting its provisional entries when the walk starts. A cleanup delete in a `dropping` or absent tree does nothing, and the walk frees the entry and its chain. Because no reader can reach `t`, deregistering `L` does not wait for the walk (C-T0 v8 I-PROV-LOOKUP is about readers). §13.2 records this for C-T0.
- **No sweep.** GC scheduling (C-T0 v8 §9.4) skips `dropping` trees, and a running sweep stops at its next step.

---

## 8. Index maintenance and states (v2 §6, §6.1, kept with §0.2)

These are kept verbatim through §0.2:
- the index def bytes (`0x49 0x01 flags state be64(built_at) …`);
- the states DeleteOnly, WriteOnly and Public;
- maintenance per state, with entries computed from the row version the row op acted on (its EPQ base);
- entry deletes that are blind, never skip and never raise 40001;
- `key_cols_changed` counting the unique indexes in every state;
- "a unique index that was ever non-public never uses the unique form";
- `index_scan` verifying each entry in its own visibility regime (C-T6e3fix);
- born-public indexes (§6.1: Share lock, emptiness at `S_e`, `built_at = 0` or `S_e`).

The entry forms are §2.2's, and the key limit is §3.4's.

---

## 9. Online index build (non-unique; replaces v2 §7 steps 4–7 and §7.3)

Run by plain `CREATE INDEX` outside a transaction block and by `CREATE INDEX CONCURRENTLY` (25001 inside a block).
- **Step txns.** Each step is its own RC txn holding ShareUpdateExclusive on the relation. Build txns never run at SERIALIZABLE.
- **Concurrent change.** A step that finds the index row absent, or not in the state it expects, fails the build with 55000 (kept).

### 9.1 Steps
1. **T0.** Insert the index row in state DeleteOnly, with a new tree (§5.3). Commit, and wait for the ack.
2. **T1 (kept).** Expect DeleteOnly. `wait_for_lockers(rel, Share)`. Rewrite the state to WriteOnly. Commit, and wait for the ack.
3. **T2, the fence.**
   1. Expect WriteOnly.
   2. `wait_for_lockers(rel, Share)`.
   3. **Register the pin `R`**: a caller-chosen registration (C-T0 v8 §3.6) of `R := visible_ts`, taken under the registry mutex.
   4. Rewrite the index row with its def unchanged.
   5. Commit, and wait for the ack. Call its commit ts `f`.

   `f > R`, because `R ≤ visible_ts`, and `visible_ts` is at most every ts already assigned, all of which are `< f`.
4. **T3 begins.**
   - Expect WriteOnly, and use the rows' **current** tree ids (a TRUNCATE between steps replaces them).
   - Take an ordinary snapshot `S` (`S = visible_ts`, registered in one step). Then `S ≥ f > R`.
   - T3 holds ShareUpdateExclusive until step 8, so no TRUNCATE, DROP or rewrite runs during steps 5–8.
5. **Scan** the relation's row region at `S`, with the scan yielding each version's ts. For each row live at `S` **whose version has ts `< S`**:
   - compute its entry `E` per §8, detoasting indexed toasted columns at `S`;
   - if `E` exceeds §3.4's limit, fail the build with 54000.

   Rows whose live version has ts `= S` are **skipped** (§9.4).

   Sort by user key:
   - in memory up to `build_mem` (R-1);
   - beyond that, in sorted runs spilled to temp files through `Env` (C-T0 v8 §2.1's temp-file rule), then merged k-way;
   - 53200 if neither fits.
6. **Load.** In sorted order, insert each `E` as `entry_key(E, S)` with tag 0, **without a WAL record** (S-STORE §12.8.1), through the store's ordinary insert path. Then release `S` and `R`. `R` stays registered until the last insert has finished (S-STORE §10.1 step 3).
7. **Force a checkpoint** whose flip follows the last insert (S-STORE §12.8.2), and wait until its meta is durable (S-STORE §13 step 5).
8. **T3 publishes.** Re-read the index row, expecting WriteOnly. Rewrite it to Public with `built_at = S`. Commit. This commit is issued only after step 7 (S-STORE I-BULK-PUBLISH; mutant PUBLISH-BEFORE-META).

### 9.2 Why `R` comes before `S` (S-STORE §10.1 condition 2; S-1-F1)
**The problem.** `W` is monotonic, and it equals `visible_ts` when no older ts is registered. A registration at `S − 1` made after `S` is picked therefore cannot hold `W` below `S`.

**The fix.** Registering `R` before `f` is assigned gives `W ≤ R < f ≤ S` for the whole load. The fence supplies a ts above `R` without waiting on any foreign commit, because it is the build's own T2 commit.

Mutant **PIN-AFTER-S**: `R` is registered after T3 takes `S`. It is caught only by §9.3's cfg(test) assertion, not by an observable read.

### 9.3 Why the pin has no read-observable mutant (S-1 gate, ruling 1; recorded per S-STORE §17)
- **What the load writes.** Only live entries, all at `S`, into an unpublished tree.
- **The race.** Suppose a GC with `w = S` races those inserts. Each loaded entry lands in one of two places:
  - behind the sweep's position, where it is newer than everything the sweep kept;
  - or ahead of it, where it is the newest version `≤ w` and is kept.
- **The worst outcome** is under-pruning, and no read at any `S' ≥ W` changes.

So registering `S` (or nothing) instead of `R` is unobservable without a new hook. The condition stays because it is the formal precondition of S-STORE §10.4's carried state. B-4d therefore pins it with a cfg(test) assertion (`W < S` at every load insert, `t_build_pin_below_s`), not with a read-level mutant.

### 9.4 I-LOAD-DISJOINT: no loaded entry shares an entry key with a logged one
**Proof.**
- A logged write of `E` at ts `S` comes only from the txn that committed at `S`.
- `E` is a general-form entry, so its key contains its row key (§2.2). That txn therefore wrote that row at `S`.
- So either the row's live version at `S` has ts `S`, and the scan skipped it, or the row is not live at `S`, and no `E` was emitted.

**The skipped rows are covered anyway.** The writer at `S ≥ f` is WriteOnly-aware, so it wrote `E@S` itself through maintenance.

**Why it matters.** A txn at `S` that deleted and re-inserted `E` (for example, an indexed column changed and changed back) wrote `E` with `key_changed`, which is tag 2 by C-T0 v7.8 §2.1's stickiness. The load writes tag 0. S-STORE I-ENTRY-UNIQUE makes a different value at one entry key XX001, at load time or in a replay over an image that holds the load. v2's ingest tolerated the difference ("the header key_changed bit may differ"); v3's store does not.

Mutant **LOAD-EMITS-AT-S**: rows whose version has ts `S` are emitted too.

### 9.5 Failure, crash and resume (v2 §7 kept with §0.2, plus the load)
- **Failure.** A CONCURRENTLY build leaves the index non-public. Writers keep maintaining it, and readers never use it. A plain CREATE INDEX drops its own index on failure, through a DROP INDEX txn (§7).
- **Crash before publication.** The index tree holds its logged maintenance writes plus whatever an image caught of the load (S-STORE §12.8.4).
  - **Plain build:** a boot job drops it through a DROP INDEX txn, before the open returns (flag bit2 clear; v2 `cleanup_unfinished_builds`). Mutant **BOOT-KEEPS-PLAIN**.
  - **CONCURRENTLY build:** it is left non-public. A resume restarts at step 3, with a **fresh `R` and fence**. It never reuses the lost `S`, which `W` may already have passed. Mutant **RESUME-OLD-S**: the resume loads at the old `S`, so its scan reads a pruned state and I-BUILD-COVER fails.
- **Leftover loaded entries are harmless.**
  - Each was computed from a version live at `S_old`, with ts `< S_old`, and `S_old ≥ f_old`. So every later change to its row was made by a WriteOnly-aware writer, whose blind entry delete covers it.
  - Re-loading at `S_new` adds a newer, equal version of each live entry, which GC folds.
  - **No new commit can collide with a leftover.** A collision would need a new commit at ts `S_old` (§9.4). An image holds a leftover only if its flip followed the insert, and that checkpoint synced every commit `≤ S_old` (§9.6, durable derivation). So the recovered frontier is at least `S_old`, and no ts `≤ S_old` is reused (C-T0 v8 §3.7 steps 2 and 4).
- **Resume points.** From DeleteOnly, resume at step 2. From WriteOnly, resume at step 3 (kept).

### 9.6 Correctness (replaces v2 §7.3)
- **Kept from v2 §7.3 verbatim, with §0.2:**
  - the lock argument: after a state's ack, `wait_for_lockers` leaves only writers that read that state or a later one;
  - the fence argument: every txn with commit ts `≥ f` read WriteOnly or later;
  - "no inserted entry outlives the row version it was computed from".
- **Coverage.** Every row version with ts `< S` that is live at `S` is covered by the load. Versions at ts `S` are covered by their writer's maintenance (§9.4), and so is every version above `S`.
- **No shadowing.** v3 has no intents, so v2's resolve-before-ingest pass retires.
  - A large txn committed at `c < S` has its row and its entries either as provisional entries (read through C-T0 v8 §4) or as ordinary entries.
  - The load's `E@S` sits above the writer's `E@c` as a newer version with the same live value.
  - `c = S` is the skipped case.
- **Durable derivation** (replaces v7.8's "syncs the WAL before the ingest"). No image ever holds a loaded entry without the commits it was derived from, `synchronous_commit=off` ones included, because:
  - every commit `≤ S` was appended before the scan, since `S ≤ visible_ts`;
  - every image whose flip follows an unlogged insert has synced every record appended before that flip (S-STORE §13 step 1).
- **GC.** During the load `W ≤ R < S` (§9.2). Afterwards the tree is GC'd like any other.
- **Readers.** No read uses the index before it is Public with `built_at = S`, and every such read has `S_R ≥` the publishing ts `> S`. AS OF at `t` uses the catalog at `t` (kept).

**I-BUILD-COVER.** After publication, I-IDX holds at every `S' ≥ built_at` (§11). Mutants:
- **NO-FENCE:** `S` is taken right after T1's wait, with no T2. This is v2 §7.3's counter-example: the last DeleteOnly-aware txn commits at `S`, and its row lacks `E`;
- **MAINT-SKIP-WRITEONLY:** a WriteOnly index is not maintained;
- **RESUME-OLD-S.**

### 9.7 Unique builds and in-block builds (v2 §7.2, §9 items 1–2, kept)
- CREATE UNIQUE INDEX outside a block runs §8's born-public path, so it succeeds on an empty table and gives 0A000 on a non-empty pre-existing one.
- CREATE UNIQUE INDEX CONCURRENTLY gives 0A000.
- A plain CREATE INDEX inside a block on a non-empty pre-existing relation gives 0A000.

Follow-on S-3b (§14).

### 9.8 Costs
- **The pin.** `R` holds `W` for the whole build, as any long reader does (C-T0 v8 §9.5).
- **Inserts, not a page build.** The load uses sorted inserts, not a bottom-up page build (§10).
- **Revisit trigger:** the NBench index-build row misses its target, and a profile attributes more than 30% of the load to descents. An Opus card then considers a bottom-up build of the whole tree into a fresh tree, followed by a merge of the concurrent maintenance writes.

---

## 10. The stable root and the row layer (the S-STORE departure handed to S-3)

**Ruling: S-STORE I-ROOT-STABLE (§6.3) does not touch the row layer.** The row layer names trees only by tree id: no catalog row, WAL op or row byte holds an LPID or a root.

Two choices follow, and they are recorded here so that no later card reverses them by accident:
- **TRUNCATE never resets a tree in place.** It creates new trees (§7.3). An in-place reset would change versions under readers at `S` below the DDL, and replay by commit ts could not reproduce it.
- **The build loads by insert.** It inserts under the tree's stable root (§9 step 6), and never builds pages and then swaps a root, which I-ROOT-STABLE forbids.

The row layer needs no test of its own for this beyond A-4a/A-4b's.

---

## 11. amcheck (v2 §8 kept with §0.2; A-8 checks pages and trees, B-4a–e extend it)
At a registered snapshot `S`, with the catalog at `S`:
- **I-ROWFMT:** each visible row decodes under the def at `S` with §3.3's era rule, through the shared helper. `schema_version ≤ def.version`, and the primary-key columns (or the rowid) re-encode to the row's key.
- **I-IDX, I-IDX-WO, I-IDX-DO:** v2 §8, kept.
- **I-TOAST** (§4.6).
- **I-TOAST-REGION:** every user key `≥ 0xFF` in a relation tree decodes as `0xFF ‖ 20 bytes ‖ be32`. Every key below `0xFF` decodes as a row key. Anything else is XX001.
- **I-CAT-TREES** (§5.3), on a quiescent file.

---

## 12. Invariants

Each invariant needs a test and a named mutant that the test catches (PLAN-v3 §7.1). The "Pinned by" column names the card that writes the test. B-4d is "S impl / P tests" (PLAN-v3 §9), so its tests are P-written before its implementation. The other B-4 cards are "S impl, P gate".

| ID | Statement | Pinned by | Named mutants |
|---|---|---|---|
| I-ROWFMT | §3.1–§3.2, §11: rows and defs decode totally (XX001, never a panic) | B-4b (port `nucleus-codec`'s `row_golden`, `row_props`, `row_errors`, `row_defs`) | ROW-DECODE-PANIC |
| I-ERA-NULL | §3.3: a NULL on a NOT NULL attnum is XX001 iff `v ≥ not_null_since`, on every decode path and in amcheck | B-4b (port `t6e4fix2_not_null_since.rs` and the e4 pins of `t6e3_regress.rs`) | CLEAR-ALL, SINCE-OFF-BY-ONE, NO-RESET, AMCHECK-OLD, E4-REGRESS |
| I-LAZY | §3.3: every write encodes at the current version with `natts = ncols`; no DDL rewrites | B-4b | ENCODE-AT-ROW-ERA |
| I-KEY-LIMIT | §3.4: row and entry keys over `K_MAX` → 54000 at write-set entry; a build meeting one fails with 54000 | B-4b (rows), B-4d (builds) | SIZE-CHECK-OFF (S-STORE's), BUILD-SKIPS-LONG-KEY |
| I-TOAST | §4.4–§4.6: pointers name exactly their visible chunks; carried pointers keep their chunks; dropped ones lose them | B-4c (port `t6d_toast.rs`) | TOAST-OLD-KEPT, TOAST-CARRIED-DELETED, TOAST-DECODED-NOT-RAW |
| I-TOAST-REGION | §2.1, §11: row scans stay in `[∅, 0xFF)`; toast keys have the chunk shape | B-4c | SCAN-INTO-TOAST |
| I-OP-BOUND | §4.1, §4.3: every row-layer op is ≤ `ROW_LIMIT` (rows) or `toast_chunk` (chunks) plus framing, so it fits one WAL record | B-4c | ROW-LIMIT-OFF, CHUNK-UNBOUNDED |
| I-CAT-TREES | §5.3: live kind-2/3 trees = trees named by committed catalog rows; named trees exist | B-4a (+ the B-6 orphan job) | ABORT-KEEPS-TREE, BOOT-NO-ORPHAN-SWEEP, CREATE-NOT-LOGGED |
| I-DDL-WAL | §5.5–§5.6: create before use, drop after use, in and across records; one `Ddl` op per catalog-writing txn | B-4e | CREATE-AFTER-PUT, DDL-OP-MISSING |
| I-SEQ-NO-REPEAT | §6: no counter value is handed out twice across crashes; no tree id named by the WAL or an image is reused | B-4a | HANDOUT-BEFORE-ACK, RESERVE-ASYNC |
| I-RETIRE | §7.4–§7.5: retired trees are dropped only once `W >` the row's snapshot; the DropTree ops and the row delete commit together; no finalize races the walk | B-6 (+ B-4a) | DROP-BEFORE-W, DROP-SPLIT-COMMIT, W-ABOVE-M (C-T0's) |
| I-LOAD-DISJOINT | §9.4: no loaded entry shares an entry key with a logged entry | B-4d | LOAD-EMITS-AT-S |
| I-BUILD-PIN | §9.2–§9.3: `R` is registered before `f` is assigned and held through the last insert, so `W < S` during the load | B-4d (cfg(test) assertion; no read-observable mutant, §9.3) | PIN-AFTER-S (assertion only) |
| I-BUILD-COVER | §9.6: after publication, I-IDX holds at every `S' ≥ built_at` | B-4d (port v2's build and interleaving tests; P-written) | NO-FENCE, MAINT-SKIP-WRITEONLY, RESUME-OLD-S |
| I-BUILD-RESUME | §9.5: a crashed plain build is gone after open; a crashed CONCURRENTLY build is non-public, maintained, unused, and resumable from the fence | B-4d | BOOT-KEEPS-PLAIN, RESUME-OLD-S |
| I-BULK-PUBLISH | S-STORE §12.8, used by §9 steps 7–8 | B-4d | PUBLISH-BEFORE-META (S-STORE's) |
| I-IDX, I-IDX-WO, I-IDX-DO | §11 (v2 §8, kept) | B-4d (port `t6d2_amcheck.rs`) | (v2's amcheck mutants, re-run) |

### 12.1 Test list
Each test catches the mutants named in its row above.

| Test | Invariants | Card |
|---|---|---|
| the ported codec suites; `t_era_null_audit_probe`, `t_era_null_drop_reset`, `t_era_null_e4_scene_after_add_column`; `t_lazy_upgrade_add_column_update` | I-ROWFMT, I-ERA-NULL, I-LAZY | B-4b |
| `t_pk_over_kmax_54000` (at every allowed page size) | I-KEY-LIMIT | B-4b |
| ported `t6d_toast.rs`; `t_toast_carry_unchanged` (an UPDATE of another column writes no chunk and keeps the old chunks visible at `S` below it); `t_toast_drop_column_raw` | I-TOAST | B-4c |
| `t_full_scan_skips_toast`; amcheck on a planted `0xFF` key of the wrong shape | I-TOAST-REGION | B-4c |
| `t_value_over_record_limit` (a 64 MiB datum with `wal_seg_size = 8 MiB`, then a crash and replay); `t_wide_row_fits_record` (1,600 × 2,000-byte columns) | I-OP-BOUND | B-4c |
| `t_abort_drops_created_tree`; `t_crash_orphan_swept` (CREATE TABLE in a large txn, a checkpoint, a crash before the marker); `t_create_logged_no_ckpt` (commit, crash before any checkpoint, read) | I-CAT-TREES | B-4a |
| `t_large_create_chunk_order` (WAL decode); `t_ddl_op_lists_oids` | I-DDL-WAL | B-4e |
| `t_counter_crash_no_repeat` (record handed-out values, crash at every step of a reservation, reopen, allocate); `t_tree_id_not_reused_after_orphan` | I-SEQ-NO-REPEAT | B-4a |
| `t_drop_waits_w` (an RR reader below the TRUNCATE keeps reading the old tree); `t_drop_commit_atomic` (crash after the drop commit, then reopen: the walk resumes and nothing leaks); `t_drop_waits_finalize` (a large txn's finalize parked, TRUNCATE, drop cleanup must not run until `M` passes) | I-RETIRE | B-6 |
| `t_load_skips_version_at_s` (a txn committing at exactly `S` deletes and re-inserts its row's entry) | I-LOAD-DISJOINT | B-4d |
| `t_build_pin_below_s` (assertion at every insert) | I-BUILD-PIN | B-4d |
| `t_build_fence` (v2's last-DeleteOnly-commit scene); `t_build_interleavings` (writers at every step boundary); `t_build_resume_after_crash` (crash mid-load and before publication, resume, I-IDX); `t_plain_build_dropped_at_open` | I-BUILD-COVER, I-BUILD-RESUME | B-4d |
| `t_publish_after_meta` (a hook between steps 6 and 7; the publishing commit must not be issued) | I-BULK-PUBLISH | B-4d |

---

## 13. Boundaries and refinements

### 13.1 Sentences S-STORE should gain at its next edit (S-2-F1 or later)
These state what S-STORE already permits in substance. None changes an S-STORE invariant.

| # | S-STORE | Sentence | Why |
|---|---|---|---|
| N1 | §6.8 | "CreateTree and DropTree may run ahead of their logged op, or with none, on a tree no committed catalog row names; an image may hold such a tree, which the row layer reclaims (C-T6-rehost §5.3)." | The eager creation of §5.3 |
| N2 | §6.8 | The `kind` values are the row layer's (C-T6-rehost §2.4) | They were unassigned |
| N3 | §2.1 / §12.2 | `wal_seg_size ≥ 8 MiB` at creation | I-OP-BOUND (§4.3) |
| N4 | §19 Q6 | TOAST values never need a resumable chain free: each chain is ≤ `toast_chunk` | §4.3 |
| N5 | §8.3 | The scan can also yield the chosen version's ts, which is decoded from the entry key, or `c` for a provisional candidate | §9 step 5 needs it (I-LOAD-DISJOINT) |

### 13.2 Notes for C-T0 v8 (S-2a, which writes §5 and §7 in full)
- **§5.0.** TOAST chunk writes are key-existence ops and entry deletes, and they lock their keys. There is no exemption (§4.4).
- **§7.1.** Abort cleanup also drops the txn's created trees, and it skips provisional entries in `dropping` or absent trees. Deregistration does not wait for a DropTree walk (§7.5).
- **§3.6.** The lifetime of a build pin is the build's own, from step 3 to step 6. It is not tied to a txn.

### 13.3 Refinements of PLAN-v3 (for PL-1)
| # | Plan text | Here | Why |
|---|---|---|---|
| P1 | §3.5 "a sorted bulk load into a new tree" | The tree is created at T0 and receives maintenance writes before the load. The load is sorted, unlogged inserts at `S` under the stable root (§9) | WriteOnly maintenance must land in the same tree; I-ROOT-STABLE |
| P2 | §3.5 "a snapshot scan" | A pin `R` registered before the fence, then a snapshot `S ≥ f > R`, skipping versions at `S` (§9.2, §9.4) | S-STORE §10.1; I-ENTRY-UNIQUE |
| P3 | §3.3 "TOAST on overflow pages" | 256 KiB chunks in the relation tree's toast region, each one store entry (§4) | Every op must fit one WAL record |
| P4 | §9 B-4a–e, B-6 | Pin the §12 rows as listed. B-6 also gains the orphan job (§5.3) | — |
| P5 | §3.8 "secondary B+tree: unique, partial, composite, INCLUDE" | Partial, expression and INCLUDE indexes are 0A000 here (§14 D6) | They need a spec and a card before D |

---

## 14. Divergences, open questions and follow-ons

**Divergences from PostgreSQL 17.** Items D1–D5 are moved here from v7.8 §12 by C-T0 v8 §12.
- **D1.** Plain CREATE INDEX inside a transaction block on a non-empty pre-existing relation gives 0A000 (v2 card C-T6h). CREATE INDEX CONCURRENTLY inside a block gives 25001, as in PostgreSQL.
- **D2.** CREATE UNIQUE INDEX on a non-empty pre-existing relation, and CREATE UNIQUE INDEX CONCURRENTLY, give 0A000. Follow-on S-3b.
- **D3.** Rewrite-requiring ALTERs give 0A000: a non-binary-compatible ALTER COLUMN TYPE, a volatile ADD COLUMN default, and a `serial` or identity ADD COLUMN. Follow-on S-3a, required before 2.0.
- **D4.** DROP COLUMN of a primary-key column, and ALTER TABLE … ADD PRIMARY KEY, give 0A000.
- **D5.** A crashed CONCURRENTLY build leaves a non-public index, as in PostgreSQL. A crashed plain build is dropped at the next open. An index born public under a Share lock is not used by its creating RR/SER txn's own reads.
- **D6.** Expression, partial and INCLUDE indexes give 0A000 (§13.3 P5).
- **D7.** The key limit is `K_MAX(page_size)` after the escape (§3.4), not PostgreSQL's ~2704 bytes. At 8 KiB pages it is lower, and keys with many `0x00` bytes lose more. A-2 publishes the values.
- **D8.** There is no TOAST compression (pglz/lz4). Trigger: an NBench space or document row misses target, and a profile attributes more than 20% of bytes to toasted values. An Opus card then adds a compression tag to the toast pointer, which is a format change (D-8).
- **D9.** `natts ≤ 1600` with 54011, as in PostgreSQL (§3.1).

**Open questions:**
- **O1.** Build throughput: sorted inserts against a bottom-up build (§9.8 trigger).
- **O2.** The build pin holds `W` for the whole build (§9.8). Trigger: a G9 soak or NBench operational row in which an index build's retention exhausts disk.

**Follow-on cards** (P spec, each S–M, queued by the orchestrator):
- **S-3a**: table-rewriting ALTER on v3. New trees as in TRUNCATE (§7.3), then a large-txn copy under AccessExclusive, with the old trees retired. It closes D3. It must land before the 2.0 parity rows (D-10).
- **S-3b**: unique index builds on non-empty relations, plain and CONCURRENTLY. Unique-form keys are not row-specific, so §9.4's disjointness argument does not carry over, and a duplicate check against the build snapshot is needed. It closes D2. It must land before C-12's ORM matrix: migrations create unique indexes on populated tables.

---

## Changelog
- **Draft 1** (2026-10-23, card S-3). C-T6 and the C-T5 catalog re-hosted on S-STORE and C-T0 v8:
  - §0: clause status and substitutions for every v2 C-T6 section;
  - §2: relation trees (row and toast regions), index trees (unique and general forms), system trees, directory kinds;
  - §3: row bytes kept; C-T6e4fix2's `not_null_since` and the NULL era rule made normative; key limits on `K_MAX`; `natts ≤ 1600`;
  - §4: TOAST as 256 KiB chunks stored by the store, with I-OP-BOUND;
  - §5: catalog rows on trees, the eager-plus-logged tree lifecycle with the orphan job, bootstrap, `Ddl` bytes, op order;
  - §6: counters and sequences as synchronous reservation txns;
  - §7: the retired-tree record, TRUNCATE, drop cleanup in one commit, and why nothing races the walk;
  - §9: the online build with the pin `R` before the fence, the load skipping versions at `S`, the forced checkpoint and publication, crash and resume, the correctness argument, and the record of S-1 ruling 1;
  - §10: the stable-root ruling;
  - §12: invariants and the test list;
  - §13: notes for S-STORE, C-T0 and PL-1;
  - §14: divergences and the follow-ons S-3a and S-3b.
