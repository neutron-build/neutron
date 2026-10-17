# C-T6: Rows, schema evolution, large values and index builds (normative)

Status: draft 1. Implemented by `nucleus-codec` (§2-§3 bytes) and
`nucleus-catalog` on top of `nucleus-txn`; C-T0 stays normative for every
transactional rule. A change to §2, §3 or §6 bytes is a format change: golden
row corpus update and a format version bump (C-K4).

## 1. Layering
`nucleus-txn` stores opaque values (C-T0 §2.2). This document fixes the
payload of `/t/` versions (§2), `RelRow.def` (§3), `IdxRow.def` (§6), the
large-value layout (§5) and index builds (§6.1, §7). Every write goes through
C-T0 §5 and every read through C-T0 §4 at a registered snapshot or view
(C-T0 §3.1), except `Core::ingest_versions` (§7, C-T0 §10).

## 2. Row format (payload of a live `/t/` version)
    row     = 0x52 0x01 be32(schema_version) be16(natts) nullmap datum*
    nullmap = ceil(natts/8) bytes; attnum a is bit (a-1) % 8 (LSB first) of
              byte (a-1) / 8; 1 = NULL; bits past natts are 0.
    datum   = for each attnum 1..=natts whose bit is 0, in attnum order:
              0x00 be32(len) value_bytes            inline
            | 0x01 toast_pointer                    out of line (§5)
    toast_pointer = toast_id(20) be32(raw_len) be32(chunk_count)
- `natts` = attnums allocated in the definition the row was written under
  (dropped included); dropped attnums are written NULL.
- `value_bytes` is lossless (§2.1), never compared bytewise.
  Canonicalisation (e.g. jsonb input) is the type input function's job.
- Trailing bytes, short datums, unknown tags, undecodable values, or a NULL
  bit on a live NOT NULL attnum `<= natts` are XX001, never a panic.

### 2.1 Value bytes
| type | bytes |
|---|---|
| bool | 0x00 / 0x01 |
| int2 / int4 / int8 | big-endian two's complement |
| float4 / float8 | `to_bits()` big-endian |
| numeric | 0x00 NaN, 0x01 +Inf, 0x02 −Inf, 0x03 neg(1) be32(scale i32) be32(ndigits) digit(1)* |
| text, varchar | UTF-8 |
| bytea | raw |
| date | be32 (i32) |
| time, timestamp, timestamptz | be64 (i64) |
| interval | be32 months, be32 days, be64 micros |
| uuid | 16 bytes |
| jsonb | 0x00 null, 0x01 false, 0x02 true, 0x03 numeric-finite (numeric bytes from its neg byte), 0x04 be32 len utf8, 0x05 be32 n node*, 0x06 be32 n (be32 klen key node)* |
| array(elem) | u8 ndims (be32 len, be32 lower)* be32 nelems nullmap(nelems) (be32 len elem_bytes)* |

## 3. Table definition and schema evolution (`RelRow.def`)
    def   = 0x54 0x01 be32(version) u8(hidden_rowid) be16(ncols) col*
            be16(npk) be16(pk_attnum)*
    col   = be16(attnum) be32(namelen) name type flags(1) [be32(len) value_bytes]
    flags = bit0 not_null, bit1 dropped, bit2 has_missing
- `hidden_rowid` is 1 iff `npk = 0`, else 0 (any other value is XX001).
  With a hidden rowid the row key is `table_lo ‖ encode_key([bytea ASC],
  rowid)`, `rowid = be32(epoch) be64(txn n) be32(seq0) be32(ord)` of the
  inserting txn and statement, `ord` drawn from the statement's single
  ordinal counter (one per `(txn, seq0)`, shared by every call the
  statement makes and by its toast ids, §5; never reset per call):
  unique forever (C-T0 §7.2), no I/O, no lock. The rowid is in the key only,
  never a column, never decoded.
- `cols` lists every attnum ever allocated, 1..=ncols in order; attnums are
  never reused. `version` = 1 at CREATE TABLE, +1 per DDL that changes `def`.
- **ADD COLUMN** appends attnum `ncols+1`. A non-volatile default (immutable
  or stable, PostgreSQL's fast default) is evaluated once by the caller at DDL
  time and stored as `missing`; none leaves it absent (NULL). Volatile
  defaults, `serial` and identity columns → 0A000 (§9). `NOT NULL` without a
  non-NULL default: allowed only if no row is visible at the latest committed
  state plus own writes, checked under the AccessExclusive lock; else 23502.
- **DROP COLUMN** sets `dropped` and clears `not_null`; drops every index
  using the column in the same txn (as PostgreSQL does without CASCADE);
  refused for a primary-key column (0A000, §9).
- **SET NOT NULL**: a validating scan of the latest committed state plus
  own writes under the lock; any NULL (including a NULL `missing`) → 23502;
  else set `not_null`. **DROP NOT NULL** clears it. No rewrite.
- All of these take AccessExclusive **before** reading the def, enumerating
  indexes or scanning; they rewrite `def` through C-T0 §5 and keep every
  storage id (no C-T0 §8.2 DDL-side check).
- **Decode rule.** Row header `(v, n)` under definition `D` (version `V`):
  `v > V` or `n > D.ncols` → XX001. For each live attnum of `D` in order:
  attnum `<= n` → the row's datum or NULL; attnum `> n` → `missing` if
  present, else NULL. Dropped attnums are skipped. `decode_row_raw` returns
  every attnum `<= n` including dropped ones (used to find toast pointers).
- **Lazy upgrade:** every write encodes at the current `version` with
  `natts = D.ncols`; no DDL rewrites rows.
- The encoded primary key is limited to 2704 bytes (54000 on insert and on a
  primary-key change).

### 3.1 Type tags (persisted; never derived from enum order)
| tag | type | tag | type |
|---|---|---|---|
| 0x01 | bool | 0x0A | date |
| 0x02 | int2 | 0x0B | time |
| 0x03 | int4 | 0x0C | timestamp |
| 0x04 | int8 | 0x0D | timestamptz |
| 0x05 | float4 | 0x0E | interval |
| 0x06 | float8 | 0x0F | uuid |
| 0x07 | numeric | 0x10 | jsonb |
| 0x08 | text, then collation byte (0x01 = C) | 0x11 | array, then element type |
| 0x09 | bytea | | |

## 4. Catalog use by DML and reads
- A statement takes its relation lock (AccessShare for reads, RowExclusive
  for DML; C-T0 §6), then reads the relation and index rows at the latest
  committed catalog plus its own txn's earlier statements.
- A statement that writes a relation reads its catalog rows only while
  holding RowExclusive or stronger on it; a catalog read made under a weaker
  lock or in another txn is never used to maintain indexes.
- **RR/SER relation rule.** Compare the storage id of the relation row at
  `S` with the latest **committed** relation row, ignoring the txn's own
  writes. Present at `S` and different (a foreign TRUNCATE or rewrite
  committed after `S`) → 40001. Absent at `S` → proceed (reads empty, as in
  PostgreSQL). A storage id installed by the txn's own DDL is not an error.
  A newer `def.version` is not an error: rows decode under the latest def.
- An index serves a read at snapshot `S` iff `Public` and `built_at <= S`.
  The same holds for latest-state constraint scans (C-T0 §5.3): they use
  only `Public` indexes.

## 5. Large values
- A non-NULL datum whose value bytes exceed 2048 is stored out of line.
  `toast_id = be32(epoch) be64(txn n) be32(seq0) be32(ord)`, `ord` drawn
  from the statement's ordinal counter (§3); unique forever (C-T0 §7.2).
- Chunk `i` (8192 bytes each, last shorter) is the logical key
  `table_lo(sid) ‖ 0xFF ‖ toast_id ‖ be32(i)`, `table_lo(sid) = /t/{be64 sid}/`.
  `0xFF` never starts an encoded key (C-Q3s §2 markers 0x00–0x02, not
  inverted under DESC); chunks sit inside the table's storage range, so
  TRUNCATE/DROP retire them, the SSI map covers them and GC treats them as
  data. Row scans bound to `[table_lo, table_lo ‖ 0xFF)`.
- Toast ids are assigned when the new row value is built; the chunks are
  written with `insert_key(…, UniqueRule::None)` in the row's statement after
  the row op returned `Applied` (same statement, so atomic with it); a
  skipped row op writes none. The old pointers of an update or delete are
  taken from the **raw** row (`decode_row_raw`: every attnum `<= natts`,
  dropped included) of the version the row op acted on; every old pointer
  not carried into the new version has its chunks deleted with
  `Core::delete_entry` in the same statement.
- **Reading chunks.** Chunks of a version with commit ts `t` are read at a
  registered snapshot at `t`: for a row read at `S`, `S`; for an EPQ base
  version `v`, `register_at(v.ts)` (admitted: `v.ts > S >= W`). Chunks of
  the txn's own intent are read with the txn's own ctx. No SIREAD. A missing
  chunk or length mismatch → XX001.
- I-TOAST: at every snapshot `S`, each pointer in a row visible at `S`
  (raw, dropped attnums included) names chunks `0..chunk_count` visible at
  `S` summing to `raw_len`; every chunk visible at `S` is named by such a
  pointer.

## 6. Index definition (`IdxRow.def`) and states
    def   = 0x49 0x01 flags(1) state(1) be64(built_at) be16(ncols)
            (be16(attnum) dir(1) nulls(1))*
    flags = bit0 unique, bit1 nulls_not_distinct, bit2 concurrently
    state = 0x00 DeleteOnly, 0x01 WriteOnly, 0x02 Public
- Entries (C-T0 §5.3): unique with no NULL key column (or
  `nulls_not_distinct`): `/u/{sid}/` ‖ key → encoded row key; otherwise
  `/i/{sid}/` ‖ key ‖ row key → empty.
- **Key limit.** An entry being inserted (maintenance, born-public
  creation, or emitted by a build) whose encoded index key exceeds 2704 bytes
  → 54000; a build that meets one fails with 54000. An entry delete whose key
  exceeds the limit is skipped: no such entry can exist.
- **Maintenance.** DeleteOnly: a delete, or an update that changes the
  entry, deletes the old entry; nothing is inserted. WriteOnly and Public:
  insert → insert entries (`/u/` with `UniqueRule::Unique { same_row:
  Some(row key) }`); delete → delete each entry of the deleted version;
  update that changes an entry → delete old, insert new. Entries are computed
  from the row version the row op actually acted on (its EPQ base).
  Entry deletes run after the row op returned `Applied`, through
  `Core::delete_entry` (C-T0 §5.0): blind (a tombstone is placed whether or
  not the entry exists), no newer-version rule and no EPQ, so they never
  skip and never raise 40001. Entry inserts use the statement's `StmtCtx`.
  `key_cols_changed` (C-T0 §1) counts the unique indexes in every state.
- A unique index whose state was ever DeleteOnly or WriteOnly never uses
  `/u/` (none exists in 2.0: the online path never builds unique indexes).
- `index_scan` verifies each fetched row: an entry whose row is not visible
  at the scan's snapshot, or whose row's key columns do not re-encode to the
  entry's key, is XX001.

### 6.1 Born-public indexes
CREATE INDEX (unique or not) inside a transaction, without §7, when either
(a) the relation was created by this txn (its committed row is absent), or
(b) after `lock_relation(rel, Share)`, no committed row is visible at a
snapshot `S_e` registered after the lock. The index is created `Public` with
`built_at = 0` (a) or `S_e` (b). Entries for the txn's own rows are written
through C-T0 §5; the own rows are those a scan at `S_e` with the txn's own
`ReadCtx` (snapshot `S_e`, the current statement's seq) returns — no
committed row is visible at `S_e`, so these are exactly the txn's own live
writes (case (a): the storage is new, any snapshot). Abort leaves nothing.
Otherwise inside a transaction: 0A000 (interim, §9; C-T0 §12).

## 7. Online index build (non-unique)
Run by plain `CREATE INDEX` outside a transaction block and by
`CREATE INDEX CONCURRENTLY` (25001 inside a block). Each step is its own RC
txn holding `ShareUpdateExclusive` on the relation; build txns never run at
SERIALIZABLE. A step that finds the index row absent, or not in the state it
expects, fails the build with 55000 ("index was dropped or changed
concurrently").
1. **T0:** insert the index row, state `DeleteOnly`, fresh storage id, map
   its ranges (C-T5). Commit; wait for the ack.
2. **T1:** re-read the relation and index rows at the latest committed
   catalog (expect `DeleteOnly`); `wait_for_lockers(rel, Share)`; rewrite
   the state to `WriteOnly`. Commit; wait for the ack.
3. **T2 (fence):** re-read both rows (expect `WriteOnly`);
   `wait_for_lockers(rel, Share)`; rewrite the index row with its def
   unchanged. Commit; wait for the ack; call its commit ts `f`.
4. **T3:** re-read both rows (expect `WriteOnly`) and use their **current**
   storage ids (a TRUNCATE between steps re-allocates them). Register
   snapshot `S` (so `S >= f`); keep it registered until step 6 returns.
5. Scan the table at `S`. For each row live at `S`: the entry `E` per §6
   (indexed toasted columns detoasted at `S`), emitted as `E@S` with header
   0x00. Sort by stored key.
6. `Core::ingest_versions(&S, ranges, entries)` (C-T0 §10: resolves the
   committed and aborted intents in the call's key span, syncs the WAL, then
   ingests every entry at `S`), in sorted non-overlapping chunks.
7. Rewrite the index row: `Public`, `built_at = S`. Commit.
- Resume: from `DeleteOnly` at step 2, from `WriteOnly` at step 3.
- Failure: `CONCURRENTLY` leaves the index non-public (PostgreSQL INVALID);
  writers keep maintaining it, readers never use it; remedy: resume or DROP
  INDEX. Plain CREATE INDEX drops its own index on failure (a DROP INDEX txn,
  retirement through C-T0 §9.2). After a crash, a plain build's non-public
  index is dropped at the next open, by `cleanup_unfinished_builds`, which
  `Db::open` runs before it returns the handle (flag bit2 clear); a
  CONCURRENTLY one is left (§9).

### 7.2 Unique
The online path never builds a unique index. CREATE UNIQUE INDEX outside a
transaction block runs §6.1 in its own txn (Share lock, emptiness checked
under it), so it succeeds on an empty table and returns 0A000 on a non-empty
pre-existing one. CREATE UNIQUE INDEX CONCURRENTLY → 0A000.

### 7.3 Correctness
- A statement reads the catalog once, after its relation lock (§4), and a
  txn holds RowExclusive to its end. So after a state's ack,
  `wait_for_lockers` leaves only writers that read that state or a later
  one. After step 2's wait every writer maintains at least deletes. After
  step 3's wait every writer maintains fully. Every txn with commit ts
  `>= f` read `WriteOnly` or later (a txn that held RowExclusive at step 3's
  collection was waited for and committed before `f`; any other txn that
  read an older state ended before the collection).
- The fence is needed because the last DeleteOnly-aware txn can commit at
  exactly `visible_ts`; a snapshot taken right after step 3's wait could
  equal its commit ts and `E@S` would share a key with its tombstone. With
  `S >= f`, every txn committed at `S` is WriteOnly-aware.
- Entries are inserted only by WriteOnly-aware writers, for the version
  they wrote. Every later change to such a row is made by an at least
  DeleteOnly-aware writer whose entry delete always lands (§6,
  `delete_entry`). So no inserted entry outlives the row version it was
  computed from.
- Every row version with ts `> S` was written by a WriteOnly-aware writer.
  Every version `<= S` is covered by the scan.
- Every write of an `/i/…‖pk` entry `E` is made by a txn that writes
  `/t/pk` at the same ts. For a row live at `S`, a write of `E` at ts `S` is
  therefore made by the WriteOnly-aware txn whose version is the one live at
  `S`; it wrote `E` live with the same payload (the header key_changed bit
  may differ and is not significant). `E@S` never shares a key with a
  tombstone, and writes after `S` sort above it.
- Entries are not emitted at the row version's ts: a DeleteOnly-aware txn
  that deletes and re-inserts a row (or changes a key there and back)
  commits a tombstone of `E` at that same ts, which its resolution would
  write over the ingested version, or a GC DeleteRange computed from it would
  cover.
- An intent of a txn committed at or below `S` would shadow `E@S` (C-T0 §4
  step 1 returns a committed intent's data without reading versions).
  `ingest_versions` resolves such intents in the key span of each call before
  ingesting it, and none can be created afterwards (every txn committed
  `<= S` has ended).
- `S` stays registered until the ingest returns, so `W <= S` and GC never
  removes a version `>= S`. A DeleteRange computed from a tombstone at ts
  `t < S` covers `[E@t, end(E))`, never `E@S`.
- The WAL is synced after the resolutions and before the ingest. Every
  commit `<= S` (including `synchronous_commit = off` ones the scan saw) and
  every resolution is durable before any entry derived from it.
- No §4 read of a published index reads the range before `built_at = S` is
  published, and every such read has `S_R >= S`. Writers' latest-state
  operations on the range (entry deletes, amcheck) are meant to see the
  ingest.

## 8. amcheck (C-T6 tests; C-G7 extends)
At a registered snapshot `S`, catalog at `S`:
- I-ROWFMT: each visible row decodes under the def at `S`, `schema_version
  <= def.version`, pk columns (or rowid) re-encode to its key.
- I-IDX: each `Public` index with `built_at <= S`: visible entries equal the
  entries computed from visible rows (payload compared, header bit ignored);
  unique ones also I-UNIQUE.
- I-IDX-WO: each `WriteOnly` index: every visible entry is computed from a
  visible row.
- I-IDX-DO: a `DeleteOnly` index whose T0 committed at or below `S` has no
  visible entry.
- I-TOAST (§5).

## 9. Open questions and divergences
1. Plain CREATE INDEX inside a transaction block on a non-empty
   pre-existing relation: 0A000 (interim; card C-T6h). CREATE INDEX
   CONCURRENTLY inside a block: 25001 (as PostgreSQL). An index born public
   in §6.1 case (b) is not used by the creating RR/SER txn's own reads
   (`built_at = S_e` is above its snapshot).
2. CREATE UNIQUE INDEX on a non-empty pre-existing relation, and CREATE
   UNIQUE INDEX CONCURRENTLY: 0A000.
3. Rewrite-requiring ALTERs (non-binary-compatible ALTER COLUMN TYPE,
   volatile ADD COLUMN default, serial/identity ADD COLUMN): 0A000 until
   card C-T6r, required before 2.0.
4. DROP COLUMN of a primary-key column, and ALTER TABLE … ADD PRIMARY KEY:
   0A000.
5. A crashed CONCURRENTLY build leaves a non-public index (as PostgreSQL); a
   crashed plain build is dropped at the next open, not at crash time.
6. Expression and partial indexes: 0A000.
7. Builds sort in memory; spill is C-R0's.
8. Column types are the C-Q3s key types only.
