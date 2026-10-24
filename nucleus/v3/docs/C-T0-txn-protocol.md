# C-T0: Transaction protocol (normative)

Status: draft 8.1 (card S-2-F1, 2026-10-23; draft 8.0 was card S-2, increment 1 of 4). This is a P-authored spec edit and needs a P gate before it is normative. Every `nucleus-txn` card implements against this file. A change to an invariant (`I-*`, §11) is made here first, then in the tests and mutants that pin it (and in G0), and only then in code.

**What v8 is.** v8 is C-T0 draft 7.8 (`nucleus/v2/docs/C-T0-txn-protocol.md` at 8095500b) re-hosted on:
- **N2**: a private write set and an in-memory lock table, with no write intents, no resolver and no status table;
- **N3 = (b)**: timestamp-in-key versions, specified in `docs/S-STORE.md`.

PLAN-v3 §3.4 requires that v8 keep every behaviour clause of v7.8 and replace its physical clauses (§2.2 key layout, intents, resolver, status table, compaction GC). §0 says, for every v7.8 section, whether it is kept, kept with substitutions, replaced or retired.

**Increments.** This draft is increment 1 (card S-2). It specifies normatively:
- every new mechanism, including all of X-4's directives to S-2 (PLAN-v3 §4, "E1′ decision"; §8.1 re-gate item 2);
- the full section skeleton;
- the rule (§0) that makes the kept v7.8 clauses normative in v8 terms;
- the invariant table and its test list (§11).

S-2a, S-2b and S-2c replace the by-reference clauses with full text (§0.3). Until each lands, §0 is how its clauses read.

**Precedence.** This file refines PLAN-v3 §3.4, N2 and N5. If the plan disagrees with this file, this file wins, and §13 lists the refinements for PL-1. `docs/S-STORE.md` owns storage. This file owns transaction semantics. The boundary is S-STORE §17. The provisional-entry encoding, its read rule and its recovery treatment are edits of S-STORE made in the same commit as this draft (S-STORE §7.3, §8, §12.7, §13, §14).

**Scope.** Single node. Isolation levels: RC (PostgreSQL semantics), RR = SI, SERIALIZABLE = SSI. 2PC (`PREPARE TRANSACTION`) is refused with 0A000 in 2.0. The target behaviour is PostgreSQL 17, and every deliberate divergence is listed in §12.

**Error classes** (PostgreSQL SQLSTATEs): 40001 serialization failure, 40P01 deadlock, 55P03 lock not available, 23505 unique violation, 23503 FK violation, 21000 cardinality violation, 27000 triggered data change violation, 72000 snapshot too old (AS OF only), 53200 out of memory (N16), 54000 program limit exceeded, 0A000 feature not supported. Storage errors are S-STORE's (XX001, 58030, 53100).

---

## 0. Reading v8

### 0.1 Clause status

| v7.8 | v8 | Status |
|---|---|---|
| §1 Vocabulary | §1 | Replaced. Unchanged terms keep their meaning |
| §2.1 Intent layers | §2.1 | **Layer semantics kept verbatim**; the container is the write set |
| §2.2 Version layout | §2.3 | Replaced by S-STORE §7 (tag mapping in §2.3) |
| §2.3 Persisted system keys | §2.5 | Replaced: no system keys |
| §3 Commit pipeline | §3.1–§3.5 | Replaced |
| §3.1 Snapshot and view registration | §3.6 | Replaced (view counters retire). The lifetime rules are kept |
| §3.2 Commit-visibility window | §3.4 | Kept: committed-not-released counts as a live holder |
| §4 Read path | §4 | Replaced. The AS OF, relation storage-id and `built_at` paragraphs are kept with §0.2 |
| §5.0 Operations and latch keys | §5.0 | Kept with §0.2 |
| §5.1 The loop | §5.1 | Replaced |
| §5.2 RC EvalPlanQual | §5.2 | Kept with §0.2 and the §5.2 changes (S-2a) |
| §5.3 Unique, primary key and FK | §5.3 | Kept with §0.2 and the §5.3 changes (S-2a) |
| §5.3.1 ON CONFLICT arbiter protocol | §5.3.1 | Kept with §0.2 and the §5.3.1 changes (S-2a) |
| §5.4 Own-row rules | §5.4 | Kept verbatim with §0.2 |
| §5.5 Savepoints and cursors | §5.5 | Write-set mechanics replaced; every other rule kept (S-2a) |
| §6 Locks and waiting | §6 | Kept with §0.2, plus the lock table and escalation (§6.1–§6.2; S-2a for §6.3) |
| §7.1 Abort | §7.1 | Replaced |
| §7.2 Crash, §7.3 Intent removal, §7.4 Status truncation | §7.2 | **Retired** |
| §8 SSI | §8 | Kept with §0.2, plus the no-intent rules (§8.1–§8.3; S-2b) |
| §9.1 Watermark | §9.1 | Replaced |
| §9.2 GC rules | §9.2–§9.3 | Replaced by S-STORE §10 and the DDL retirement rule |
| §9.3 Background jobs | §9.4 | Replaced |
| §10 Other rules | §10 | Kept with §0.2, except bulk ingest (replaced) |
| §11 Invariants and G0 | §11 | Replaced. G0 models: S-2c |
| §12 Open questions and divergences | §12 | Updated |

### 0.2 Substitutions
Every clause marked "kept with §0.2" reads with these substitutions. A kept clause that cannot be read through them is a spec defect: a worker reports it (ESCALATE), and does not resolve it by choice.

| v7.8 text | Reads in v8 as |
|---|---|
| `k@INTENT` of T; "T's intent on k" | T's lock-table entry covering k (§2.2) together with T's write-set entry for k (§2.1) |
| a foreign intent's top-layer `data` / `lock` | that entry's data flag / mode (§2.2) |
| placing or writing an intent | the write step of §5.1: hold the entry, then push or modify the write-set layer |
| `latch(latch_key(k))`; "under the latch" | **holding** an exclusive lock-table entry covering k (§5.1). The lock table gives the check-and-place atomicity; C-COMMIT-ORDER (§3.3) gives the stability of committed state |
| `latch_key(k)` = a deferrable unique prefix | a lock-table range entry on the prefix `[p, succ(p))` |
| `status(T)`: Pending / Committed(c) not visible / visible commit / Aborted | owner state (§2.2): Open / Committed(c), not yet released / Released / Aborted |
| removing a foreign visible-committed or aborted intent (§7.3 steps 2–4) | nothing: an ended owner's entry is already free and the acquirer replaces it (§2.2) |
| a missing status for a remembered TxnId | cannot happen: references are counted handles (§2.2) |
| a view; a fresh view of the latest state | a tree read at a registered ts (§3.6); a latest-state read registers `visible_ts` |
| resolution; a resolved version | apply (§3.1) or flip and finalize (§3.2) |
| the write-set log | the key log (§2.1) |
| `intent_count`, `last_removal_counter`, `sweep_counter`, `view_counter` | nothing (retired with §7.2–§7.4) |
| storage id; storage prefix | tree id (S-STORE §6.8) |
| `DeleteRange` of a retired prefix | `DropTree` (S-STORE §6.8), when §9.3 allows |
| `/sys/catalog/…` rows | catalog trees (S-3) |
| the commit thread; its channel | the commit pipeline (§3); its order is ts order |
| unlatch; "unlatch and skip" (v7.8 §5.2, §5.3.1 step 3) | undo-acquire (§5.1): restore or remove `W`'s entry on `k`, and wake if that released anything |
| a failed KV write is fail-stop | a store poison (S-STORE §9.2) |

### 0.3 Increments
| Card | Content | Must land before |
|---|---|---|
| **S-2** (this) | §0–§13 as here; the S-STORE co-edits | P0 re-gate (PLAN-v3 §8.1 item 2), S-3 |
| **S-2a** | Full text of §5.2–§5.5 and §6.3 on the lock table and write set (EPQ, unique/PK/FK including the deferrable prefix, ON CONFLICT, own-row rules, savepoints, waiting, deadlock, cancel, relation and advisory locks, `wait_for_lockers`) | B-2a, B-2b, B-2d, B-2e |
| **S-2b** | Full text of §8 (SIREADs, edges, dangerous structure, pre-commit, writer map, retention) and §8.3's argument as a proof checked by G0-ssi's scope | B-2c |
| **S-2c** | §11.5: the G0 models (commit, write, ssi, gc) re-scoped to v8, the seed table, and the list of retired v7.8 seeds | B-7, B-5's retired-test list |

---

## 1. Vocabulary

| Term | Meaning |
|---|---|
| `Ts` | `u64` commit timestamp from the sequencer. `0` and `u64::MAX` are never commit timestamps (S-STORE §7.1). |
| `TxnId` | `(boot u32, n u64)`: the boot id (§2.5) and a counter dense per boot. |
| `S` | A snapshot ts. A read at `S` sees, per key, the newest committed version with ts `≤ S`. |
| `seq` | The per-txn command counter (PostgreSQL command id), `u32`. Incremented per statement and per savepoint; never decreases, including across `ROLLBACK TO`. |
| write set | A txn's private record of its own writes (§2.1). |
| layer | One entry of a key's own-state stack, v7.8 §2.1. |
| key log | The `(seq, tree, key)` record of every layer change (§2.1). |
| lock table | The in-memory table of row locks, per tree (§2.2). |
| entry (lock) | One lock-table record: a key or range, an owner, a mode and a data flag (§2.2). |
| owner state | `Open`, `Committed(c)`, `Released(c)`, `Released(⊥)` (a commit that took no ts, §3.1 step 0) or `Aborted`, in the owner's shared state (§2.2). |
| live holder | An owner that is `Open` or `Committed(c)` (not yet released). |
| ended | `Released(c)`, `Released(⊥)` or `Aborted`. An ended owner's entries conflict with nothing. |
| small / large txn | Below / above the pre-apply threshold (§2.1). Only large txns write provisional entries. |
| provisional entry | A large txn's own data for one key, materialised in the tree at the head of the key's group (§2.4; S-STORE §7.3 tags 6–8). |
| dead | A provisional entry from a previous boot: its `txn.boot` is not the current boot id (§2.4). |
| settle | Turning a provisional entry of an ended owner into its final form (§2.4). |
| flip | A large txn's commit step that makes all its provisional entries versions at once (§3.2). |
| finalize | A large txn's post-commit conversion of its provisional entries into ordinary versions (§3.2). |
| `visible_ts` | The largest ts `t` such that every commit `≤ t` is visible: applied (small) or flipped (large), and synced if `synchronous_commit=on` (§3.5). |
| `M` | The materialized frontier (§3.5): every commit `≤ M` has its versions as ordinary entries. `M ≤ visible_ts`. It is the store's apply frontier (S-STORE §1). |
| `W` | The GC watermark (§9.1): `W ≤` every registered ts, `W ≤ M`, monotonic. |
| snapshot registry | The one mutex that holds every registered ts and publishes `W` (§3.6). |
| live-txn registry | The in-memory map `TxnId → owner handle` of every large txn of the current boot whose provisional entries may still exist. A txn registers when it becomes large, before it writes its first provisional entry, and deregisters after its finalize or cleanup has deleted the last one (§2.4). It is a leaf lock (§2.6). Distinct from the snapshot registry. |
| boot checkpoint | The checkpoint every read-write open runs after recovery; its meta is durable before the first txn starts (§2.5, §3.7). |
| boot id | The low 32 bits of the boot checkpoint's `ckpt_id` (§2.5). Not S-STORE's checkpoint epoch `e`, which is one higher after the boot checkpoint's flip. |
| key column | As v7.8: a column of any non-partial, non-expression unique index of the relation. |

---

## 2. Data

### 2.1 Write set (N2)
**Content.** Per txn and per tree, an ordered map from user key to that key's **layer stack**.
- **The layer semantics are v7.8 §2.1, verbatim**, with "intent" read as "the txn's write-set entry for the key". They cover `Layer {seq, data_seq, data, lock}`, `Write {value, key_changed} | Delete {moved} | Absent`, the `key_changed` stickiness relative to the newest committed version, lock-only requests that never replace own data, `s = max(seq, top.seq)`, and shared modes never stored in layers.
- **`key_changed`** is decided against the newest committed version, read with the newest probe (S-STORE §8.2, with §4's provisional rule).
- **Widths.** `seq` and `data_seq` are `u32`. A txn that needs more than `2^32 − 2` commands gets 54000, as in PostgreSQL.

**The key log.** One `(seq, tree, key)` record per layer pushed or modified, appended **before** the change (v7.8's write-set log). It serves:
- `ROLLBACK TO` (§5.5);
- apply (§3.1);
- materialization and finalize (§3.2);
- abort cleanup (§7.1);
- the purge of released entries (§3.3).

**Size check.** A write whose entry key would exceed `K_MAX` is refused with 54000 when it enters the write set (S-STORE §3.2, I-KEY-SIZE).

**Small and large.** A txn becomes **large** when its write set holds more than `preapply_rows` data layers (default 4,096) or more than `preapply_bytes` of write-set memory (default 4 MiB). Write-set memory counts **every** layer, top and non-top (savepoints keep non-top layers, v7.8 §5.5), and the key log. It stays large until it ends.
- **Record fit.** `preapply_bytes` must be `≤ wal_seg_size / 4`. A small txn's `Commit` ops are its top layers, which are part of its write-set memory, so they fit in one `Commit` record (S-STORE §12.2).
- **Chunking.** A txn is chunked in the WAL **iff** it is large (§3.2).
- **Bounded apply.** A small txn applies at most `preapply_rows` entries at commit, which bounds the stall of later commits (§3.1).
- **Who sets the numbers.** R-1 owns the numeric defaults, and B-1d measures the stall against them (§12 O2).

**Representation.**
- **Small txns** keep the whole write set in memory. The configuration must give `ws_mem ≥ preapply_bytes`. Since the large threshold counts all write-set memory, a small txn never spills.
- **Large txns** materialise top layers as provisional entries (§2.4, §3.2). A materialised top layer **is** its provisional entry: no second copy of its data is kept. Non-top layers that are still needed (v7.8 §5.5's layer-compaction rule) and the key log stay in memory up to `ws_mem` (R-1; default 64 MiB). Beyond that they spill to sorted runs in a temp file through `Env`. Temp files are deleted at open and are never WAL or tree content.
- **Equivalence.** Every representation answers the same layer-stack queries (I-WS-SPILL). Spilled runs are immutable, and `ROLLBACK TO` marks dead seq intervals in them (§5.5).
- **N16.** Write-set memory is a per-txn account. When nothing can spill and the account is exceeded, the write fails with 53200. W6 measured 117 B/key for an all-in-memory write set (`spikes/e1-bench/README.md` §W6).

**Own reads (I-HALLOWEEN).** A statement with start `seq0` reads, per key, the newest own layer with `seq < seq0` (v7.8 §4 step 1, own case): `Write` gives its value, `Delete` gives absent, and `Absent` or no such layer falls through to the committed read (§4). A large txn's own provisional entry is one of its own layers (it carries `seq` and `data_seq`), never committed data. **I-HALLOWEEN** (kept): a statement never sees its own writes with `seq ≥ seq0`.

### 2.2 Lock table (N2)
**Shape.** In memory, per tree, ordered by user key.
- **An entry** is `{key | range [lo, hi], owner handle, mode, data flag}`. It holds no values: it is v7.8 §2.1's intent reduced to mode, top-layer `Absent`-ness and owner (PLAN-v3 N2).
- **Modes.** KEY SHARE and SHARE (shared); NO KEY UPDATE and UPDATE (exclusive). The conflict matrix and the requested mode of each operation are v7.8 §6. Shared modes now live in this table as well (v7.8 kept them in a separate one). Several holders may share a key under compatible modes.
- **Data flag.** True iff the owner's top layer for the key has data (it is not `Absent`). For a range, it is true if that holds for any key in the range. It is set **before** the owner's SSI writer-side check (§8.2), as a store made **under the entry's partition mutex**, which every lock-table check (§8.1 step 2) also takes. So a check that follows the store observes it (§8.3, event `F`; mutant **FLAG-RELAXED**: a relaxed store outside the mutex). `ROLLBACK TO` recomputes it (§5.5); a range may keep it conservatively.
- **Ranges** arise only from escalation (§6.2) and from deferrable-prefix locks (§0.2). The effective mode of txn `T` on key `k` is the strongest mode among `T`'s entries covering `k`.

**The owner handle.** An entry refers to its owner through a counted handle to the owner's **shared state**:
- `TxnId`, isolation level and owner state;
- the wake generation (§6.3);
- the SSI link (§8).

The shared state outlives every handle, so a lookup through a handle never misses. This replaces v7.8's status table, its truncation and the "missing status means ended" rules.

**Owner states.** The transitions are `Open → Committed(c) → Released(c)`, `Open → Released(⊥)` (a commit with no ts, §3.1 step 0) and `Open → Aborted`.
- **Committed(c)** is set before `visible_ts` reaches `c` (I-VIS).
- **Released(c)** is set only after `visible_ts ≥ c` (§3.3).

**Ended owners are free.** An entry whose owner has ended **conflicts with nothing**, and an acquirer replaces it. This extends v7.8 §6's "conflict checks ignore ended holders" to every entry, and it is what makes release O(1) (§3.3).

**Rules.**
- **Every key-existence and row operation locks its key**, including keys that do not exist yet (N2).
- **A committed owner that is not yet released is a live holder** for every writer-side decision: the writer waits, and release wakes it (v7.8 §3.2).
- **I-LOCK.** No two live holders hold conflicting modes on one key, counting ranges.
- **Budget.** Entry memory is a per-txn N16 account (`lock_mem`, R-1). Escalation (§6.2) keeps it bounded. A txn that still exceeds it after escalation gets 53200.
- **Concurrency (PLAN-v3 §4 decision block, "Contended writes"; S-STORE §19 T3).** An acquire or a release touches one partition of one tree's table, and there is no global mutex on those paths. For ranges this holds only because a range entry never crosses a partition boundary: escalation runs (§6.2) and deferrable-prefix ranges are split at partition boundaries, and a range request is acquired partition by partition in key order. B-1b must honour that. Coalescing (§6.2) must be atomic against concurrent acquires inside the runs it creates (I-LOCK). B-1b designs how; `spikes/e1-mvcc/src/common/lock_table.rs` (the owner-word handshake) is the reference for the whole-table case.

### 2.3 Versions in trees
Committed versions are S-STORE §7 entries. v7.8 §2.2's value headers map onto tags as follows:

| v7.8 header | Tag |
|---|---|
| 0x00 live | 0 (4 in an overflow chain) |
| 0x01 live, `key_changed` | 2 (5 in an overflow chain) |
| 0x02 tombstone | 1 |
| 0x03 moved-tombstone | 3 |

Apply maps `Write {key_changed}` and `Delete {moved}` accordingly. The payload bytes are opaque here (C-T6 and S-3 own them).

### 2.4 Provisional entries (large txns)
**Encoding** (S-STORE §7.1, §7.3, edited with this draft).
- **Key.** The entry key is `entry_key(k, u64::MAX)`, which is the head of k's group and equals `seek_newest(k)`. A tree therefore holds at most one provisional entry per user key.
- **Tags 6–8:**
  - **6, provisional live:** `txn (u32 boot, u64 n)`, `seq u32`, `data_seq u32`, `flags u8` (bit 0 = `key_changed`), then the value bytes;
  - **7, provisional tombstone:** `txn`, `seq`, `data_seq`, `flags` (bit 0 = `moved`);
  - **8, provisional live in an overflow chain:** `txn`, `seq`, `data_seq`, `flags`, `len u64`, `first LPID u64`.

**Meaning.** A provisional entry of owner `T` on `k` is `T`'s current top data layer for `k`.
- **To `T`** it is an own layer (§2.1).
- **To every other reader** it is invisible while `T` is `Open` or `Aborted`. Once `T` is `Committed(c)` or `Released(c)`, it is **a version of `k` at ts `c`**, exactly as `k@c` would be (§4).

**Writing.** Only `T` writes its provisional entries, only while `T` is large, and only while `T` holds an exclusive lock covering `k`.
- **I-PROV-LOCK.** Every provisional entry whose owner is `Open` has its owner holding an exclusive lock-table entry that covers its key.
- **Before writing,** `T` settles any provisional entry already at `k`'s head (rule (b) below). Under I-PROV-LOCK that entry's owner has ended or is dead.
- `T` may replace or delete its own provisional entry, for example on a rewrite or a `ROLLBACK TO` (§5.5). Replacing or deleting a tag-8 entry frees its overflow chain in the same gated operation (S-STORE §6.7), unless the new value is that same chain.

**Dead entries.** A provisional entry whose `txn.boot` differs from the current boot id (§2.5) is **dead**. A read-only open has no boot id, so every provisional entry is dead to it.
- Nobody ever reads a dead entry as data, and that is decided from its bytes alone, with no lookup.
- Every provisional entry in a recovered store is dead, because recovery takes committed data only from the WAL (S-STORE §14).

**Owner lookup (I-PROV-LOOKUP).** A reader that meets a current-boot provisional entry resolves its owner through the **live-txn registry** (§1; `TxnId → handle`), **while still holding the latch of the leaf it read the entry from**.
- The registry is a **leaf lock** (§2.6): it is taken under a page latch, and nothing is acquired while it is held. A lookup is a map read; it never waits on the owner (I-NOBLOCK; mutant **LOOKUP-WAITS**).
- An owner registers when it becomes large, before its first provisional entry, and deregisters only after every one of its provisional entries is gone (§3.2 finalize, §7.1 cleanup).
- Every deletion of a provisional entry holds that leaf's exclusive latch.
- So the lookup never misses. A miss on a current-boot entry is a fatal invariant violation: the engine poisons the store.

**Settle.** `settle(e)` acts on the provisional entry `e` of owner `T` on key `k`, according to `T`'s state:

| T's state | Settle |
|---|---|
| `Committed(c)` or `Released(c)` | **Convert.** Insert `k@c` with the ordinary tag (6 → 0 or 2, 7 → 1 or 3, 8 → 4 or 5, by the flags). Then delete `e` if it is still `T`'s. For tag 8, the chain moves to the new entry and the delete does not free it |
| `Aborted`, or `e` is dead | **Delete** `e` if it is still the same entry (for tag 8, freeing the chain) |
| `Open` | Never settled |

- **Conversion is one gated operation** (S-STORE §5.6): the insert first, then the delete. So no image ever holds both entries (I-CKPT-AT-T), and a reader that misses `e` finds `k@c` (**I-PROV-CONVERT-ORDER**; §4's head-first rule).
- **The settle stripe.** A conversion holds the **settle stripe** of `(tree, k)` for its whole duration: a striped mutex. It is **not** a leaf: it sits above the epoch gate and page latches and below lock-table entries and txn waits (§2.6). A settler takes it while holding no page latch and no lock-table partition mutex (settler (b) may hold its lock-table *entry* on `k`), and never waits on a txn while holding it. Two settlers of one entry therefore serialise, and the second finds `e` gone and does nothing. Mutant **STRIPE-UNDER-LATCH**: a settler takes the stripe while holding a page latch, which can deadlock against a settler that holds the stripe and descends.
- **Idempotence.** An identical re-insert of `k@c` is a no-op (S-STORE §9.1). A delete is conditional on the entry's identity, checked under the exclusive latch.

**Who settles.** There are exactly three settlers:
- **(a)** `T` itself: finalize (§3.2) or abort cleanup (§7.1);
- **(b)** inline: a txn that holds an exclusive lock covering `k` and is about to write its own provisional entry on `k`;
- **(c)** for dead entries only: the GC sweep, page-local prune, or any writer on `k`. Each is a single conditional delete, with no stripe.

There is **no resolver job**, nothing is persisted about `T` beyond its WAL records, and no state about `T` outlives its own finalize or cleanup.

### 2.5 Txn identity, boot id, no system keys
- **TxnId** is S-STORE §12.2's `txn` field. `n` is a counter dense per boot, kept in memory.
- **The boot id** is the low 32 bits of the `ckpt_id` of the **boot checkpoint**: the checkpoint every read-write open runs after recovery (§3.7, S-STORE §14 step 8). It is not S-STORE's checkpoint epoch `e`: after the boot checkpoint's flip, `e` is one higher (S-STORE §1). v8 draft 8.0 called it the "boot epoch".
  - **The boot checkpoint is durable before the first txn.** No txn starts (and so no `TxnId` is issued and no `Chunk` is appended) until the boot checkpoint's meta is synced (S-STORE §13 step 5). Mutant **BOOT-CKPT-NOT-DURABLE**: txns start after step 0; a crash before step 5 leaves the previous meta durable, and the next boot's checkpoint gets the same `ckpt_id`.
  - **It always writes a new meta**, with `ckpt_id` one above the recovered meta's, even when no page is dirty. Mutant **BOOT-CKPT-SKIP-CLEAN**: a clean open skips the checkpoint and reuses the recovered meta's `ckpt_id`, which the previous boot already used.
  - **Why boot ids never repeat.** With both rules, each boot's id comes from a `ckpt_id` above every `ckpt_id` durable before it, so no two boots that issued a `TxnId` share an id (compared for equality only; a repeat would need `2^32` checkpoints).
  - **Why the boot checkpoint is needed.** Without it, a boot that crashes before its first checkpoint is followed by a boot with the same id. Its counter restarts, and its `TxnId`s collide with the dead chunks of the crashed boot that still lie in the replay span. Recovery would then attach those chunks to the new txn's marker (mutant **BOOT-ID-REUSE**, draft 8.0's EPOCH-REUSE).
  - The boot checkpoint also places `wal_start_lsn` past the crashed boot's unmarked chunks (S-STORE §12.6: a transaction aborted before the flip does not count).
  - **Read-only opens (N11)** run no boot checkpoint and have no boot id. They start no write txn and write no provisional entry, so every provisional entry is dead to them (§2.4).
- **No system keys.** v7.8 §2.3's `/sys/txn`, `/sys/epoch`, `/sys/ts_hwm` and `/sys/gc_w` are gone:
  - committed state is the WAL plus the trees;
  - the boot id comes from the boot checkpoint;
  - ts restarts from the recovered frontier (§3.7);
  - `gc_w` lives in the meta (S-STORE §10.5).

  `/sys/ts_clock` (the AS OF wall-clock samples) is held in memory and restarts empty (§9.1).

### 2.6 Lock order
Outermost first. A thread holding a lock acquires only locks further down the list.
1. **Lock-table entries** (held logically, across operations) and **txn waits** (`wait_for`, §6.3).
2. **The settle stripe** (§2.4).
3. **The epoch gate**, shared (S-STORE §5.6).
4. **Page latches**, in S-STORE §5.3's order (top-down, left to right).
5. **Leaf locks:** the live-txn registry (§2.4). Nothing is acquired while a leaf lock is held.

**Short internal mutexes** (lock-table partition mutexes, the SSI mutex, the sequencer critical section and the snapshot registry) are never held while acquiring a stripe, the epoch gate or a page latch, nor while waiting on a txn. Among themselves, the SSI mutex precedes the sequencer critical section (§8.4), and none of them is held while taking another partition mutex.

**Who takes what.** A reader takes page latches, then the live-txn registry under the leaf latch (I-PROV-LOOKUP). A converter (settlers (a) and (b)) takes the stripe, then the gate, then latches. Settler (b) already holds its lock-table entry on `k`. Settler (c) takes no stripe. Writers take their lock-table entry before any stripe, gate or latch (§5.1), and never take a stripe while waiting.

---

## 3. Commit pipeline and visibility

### 3.1 Small txn commit
The order below is binding (**C-COMMIT-ORDER**).
0. **Pre-commit.**
   - Run the deferred constraint checks (v7.8 §5.3), the WITH HOLD materialisation and deferred-trigger fixpoint (v7.8 §3.1), and, for a SERIALIZABLE txn, the SSI pre-commit (§8.4).
   - A txn with no data layers takes no ts and writes no `Commit` or `CommitMarker`. Its owner state goes `Open → Released(⊥)` in one store: ended, so its entries are free, and committed, so SSI treats it as a committed txn with no writes (v7.8 §8). It then wakes its waiters and releases its relation and advisory locks (§3.3 steps 2–4), and acks.
   - A large txn can reach commit with no data layers after `ROLLBACK TO` (§5.5 deleted its provisional entries). It commits as above. Its unmarked chunks are ignored by recovery and count as aborted for S-STORE §12.6, and its cleanup continuation deregisters it (§7.1).
1. **Sequence and append.** In **one critical section**: assign `c` = the next ts, set the owner state to `Committed(c)`, insert `c → TxnId` into the SSI writer map for SER txns (§8.5), and append the `Commit` record to the WAL queue. The record carries the top data layers in key order per tree (S-STORE §12.3).
   - I-WAL-TS-ORDER (S-STORE §12.4); mutant TS-AFTER-APPEND.
2. **Apply.** Insert one entry per top data layer at ts `c` (S-STORE §9.1).
   - It starts after the append (S-STORE §12.4) and runs concurrently with other commits' applies and with the group sync.
   - A physical error is fail-stop (S-STORE §9.2, I-FAIL-STOP).
3. **Make visible.** Once applied, and for `synchronous_commit=on` once synced, mark `c` visible. `visible_ts` advances over the contiguous visible prefix (C-VISIBLE-PREFIX, `spikes/e1-mvcc/src/common/sequencer.rs`).
   - An `off` commit is marked after its append and apply. It still cannot become visible before an earlier `on` commit's sync, because the prefix is contiguous (v7.8 §3 step 4).
4. **Release** (§3.3), only once `visible_ts ≥ c`.
5. **Ack** (§3.4), only once `visible_ts ≥ c`, and for `on` after the sync.

**No abort, no skipping.** After step 1 the pipeline never aborts a txn. A physical failure is fail-stop, and the pipeline never fills a ts hole by skipping.

**The stall is bounded.** A later commit's visibility waits for each earlier in-flight commit's apply. For a small txn that is at most `preapply_rows` inserts, and large txns flip in O(1) (§3.2).

### 3.2 Large txn: pre-apply, flip, finalize
A large txn materialises its data before commit, so that its commit makes it visible in O(1). This is X-4's "Large-txn head-of-line" item: the PLAN-v3 §3.4 trigger is fired.

**Materialize(k)** writes `k`'s current top data layer as two things:
1. an op in a `Chunk` record (S-STORE §12.3);
2. `T`'s provisional entry on `k` (§2.4): inserted, replaced, or deleted. It is deleted if the top layer no longer has data, and the chunk op is then `Delete{revert}` (S-STORE §12.3).

Rules:
- **Settle first.** Before writing its own provisional entry, `T` settles any foreign provisional entry on `k` (§2.4 (b)).
- **When.** Materialization runs whenever the write set would exceed `ws_mem`, as the large txn's way of spilling top layers, and at commit step 1′ for every top layer not yet materialised. The implementation may also run it earlier.
- **Gate holds.** Each materialization is a gated operation, or a bounded batch of them. A checkpoint flip never waits on more than one bounded batch.
- **WAL head-of-line.** `Chunk` records share the WAL queue with every other commit, and a group sync covers every byte appended before it. So a large txn keeps at most `chunk_batch` bytes (default 1 MiB, R-1) of **unsynced** chunks in the queue: it appends its next batch only after the previous one is synced. A small `on` commit's group sync then covers at most one batch per active large txn, in every phase (materialisation, step 1′, the marker). The marker's own sync covers at most one batch plus the marker, so conflicting waiters resume promptly after the flip (`t_w6_shape_hol`). Mutant **CHUNK-QUEUE-UNBOUNDED**: chunks are appended without waiting for the previous batch's sync.
- **Supersede.** A later chunk op on the same `(tree, key)` of the same txn supersedes the earlier one. That happens on a rewrite of the key and on `ROLLBACK TO` (§5.5). Recovery applies the last op per key (S-STORE §14).
- **I-PROV-WAL.** Each provisional entry equals the last chunk op appended for its key.

**Commit of a large txn T.**
0. **Pre-commit**, as in §3.1.
1′. **Materialise** every remaining top layer. `T` is still `Open` and still holds its locks, so nobody waits on `T` longer than they would wait for any open txn.
1. In **one critical section**: assign `c`, set `Committed(c)`, insert the writer-map entry (SER only), and append `CommitMarker{nchunks}` (I-WAL-TS-ORDER).
2. **Flip.** The state store of step 1 is the flip. From then on, every provisional entry of `T` reads as a version at `c` (§2.4), so all of `T`'s writes become versions at once (**I-PROV-ATOMIC**). Mark `c` visible: for `on`, after the sync. This is a large txn's "apply", and it is O(1).
3. **Release** (§3.3) and 4. **Ack** (§3.4), as in §3.1.
5. **Finalize.** Settle (convert) each provisional entry of `T`: walk `T`'s key log in key order per tree, one gated operation per conversion. Then mark `T` finalized, which may advance `M` (§3.5), and deregister `T` from the live-txn registry.
   - **Where it runs.** Finalize is `T`'s continuation: on a job thread in server mode; in embedded and wasm mode, in bounded steps run cooperatively at later commits and checkpoints (N10).
   - **Nobody waits on it.** Readers do not (I-NOBLOCK). Writers of `T`'s keys do not, because `T`'s locks are already released. Unrelated commits do not, because `visible_ts` does not wait for it.

**Cost.** A large txn writes each key to the tree twice (provisional, then converted) and once to the WAL, plus any supersedes. This is accepted. Applying at commit instead would put O(keys) on the ack path of every later commit, once acks wait for visibility (§3.4). The revisit trigger is §12 O3.

### 3.3 Release
- **When.** Only once `visible_ts ≥ c` (C-COMMIT-ORDER; mutant **RELEASE-BEFORE-VISIBLE**). For an aborted txn, see §7.1.
- **What.**
  1. Set the owner state to `Released(c)`, one store.
  2. Bump the wake generation.
  3. Wake every waiter registered on the txn, removing their wait-for edges first (v7.8 §6 "Waking").
  4. Release relation locks and advisory locks, and end deferrable-prefix waits (v7.8 §3 step 5).
- **Cost (I-RELEASE-O1).** O(1) plus O(waiters), independent of how many keys and ranges the txn holds: its entries are free from step 1 on (§2.2).
  - W6 measured 325,680 (b) and 437,257 (c) one-row txns slipping through an O(keys) release tail (`spikes/e1-bench/README.md` §W6).
  - Mutant **RELEASE-OKEYS**: release visits the txn's entries before waking its waiters.
- **Purge.** The released txn's entries are removed from the table afterwards, to return memory: by its continuation for a large txn, or inline after the wake for a small one.
  - A purge changes no conflict outcome, because an ended owner's entry is already free.
  - An acquirer may replace an ended entry before the purge reaches it. The purge removes only entries that still point at its own handle.

### 3.4 Ack and commit visibility
**The rule (I-ACK).** COMMIT returns only once `visible_ts ≥ c`, and for `on` after the sync. This settles the X-3 gate observation, which X-4 assigned here (decision block, "Commit visibility").
- **Read-your-commit.** The session's next snapshot is `S = visible_ts ≥ c`, so it sees its commit.
- **No 40001 against your own delete.** An RR txn that puts a key after the session's own committed delete of it finds the newest version to be that tombstone at `c ≤ S`. The §5.1 RR check therefore does not raise 40001.
- **Across sessions.** Every snapshot taken after the ack sees the commit (v7.8 I-ACK).

**Why not a session floor.** A floor `F > visible_ts` would give the session snapshots that include commits still being applied. A read at such an `S` must then either wait for those applies, which breaks I-NOBLOCK, or miss them, which breaks the guarantee. Waiting for `visible_ts ≥ c` costs at most one bounded apply of an earlier small commit (§3.1), because large txns flip in O(1).

**Tests (B-1c).**
- `t_ack_after_visible`: the apply gate holds a lower commit. The higher COMMIT must not return until the gate is released, and a read-back right after it returns sees it (1,000 of 1,000).
- `t_rr_put_after_own_delete`: no 40001 in 10,000 rounds, with a lower commit held pending at random.
- Mutant **ACK-BEFORE-VISIBLE**: the spike's `commit_with`, which runs `mark_applied` and returns (`spikes/e1-mvcc/src/common/commit.rs`).

**The commit-visibility window is kept (v7.8 §3.2).** Between `Committed(c)` and `Released(c)`, every writer-side decision treats the owner as a live holder. Readers need no rule, because `S ≤ visible_ts`.

### 3.5 Frontiers
- **`visible_ts`**: the largest `t` such that every commit `≤ t` is applied (small) or flipped (large), and synced if `on`.
- **`M`**, the materialized frontier: `M = min(visible_ts, min{c(T) − 1 : T large, committed, not finalized})`.
  - `M` is monotonic: a new flip has `c > visible_ts ≥ M`.
  - Every commit `≤ M` has its versions as ordinary entries in the trees.
- **The store's apply frontier is `M`** (S-STORE §1; §13 step 0 takes `T := M`).
  - So no checkpoint image depends on a provisional entry. Image `k` holds every commit `≤ T` as ordinary entries (I-CKPT-AT-T (a)).
  - A finalizing txn has `c > T`, so after a crash it is replayed from its chunks, which S-STORE §12.6 retains because its marker is not below `T`.
  - Mutant **CKPT-T-VISIBLE**: the checkpoint takes `T := visible_ts` while a large txn finalizes. A crash after that meta loses the txn's unconverted keys, because their provisional entries are dead and replay skips commits `≤ T`.
- **`W ≤ M`** (§9.1). GC never runs at or above an unfinalized large commit. That makes S-STORE A-GC-STABLE (2) hold: conversions insert at `c > M ≥ W`. It also keeps S-STORE §10.5's `gc_w ≤ T`.
  - Mutant **W-ABOVE-M**. Large `T` commits at `c` while its finalize is parked. A later txn deletes `k` at `ct > c`. A sweep at `w ≥ ct` elides that tombstone together with the older versions. The conversion then inserts `k@c` live, and a read at `S ≥ ct` resurrects `k` (a G-RESURRECT variant).
- **Lag.** `M` lags `visible_ts` only while a large txn finalizes. During that time, checkpoints have `T < c` (longer replay, with WAL retained from the txn's first chunk), and GC does not prune at or above `c`.

### 3.6 Snapshots and the snapshot registry
One **snapshot registry** mutex holds every registered ts and publishes `W` (§9.1). It is not the live-txn registry (§1, §2.4).
- **Taking a snapshot:** under the snapshot registry mutex, read `S = visible_ts` and register it, in one step. This is v7.8's I-SNAP-ORDER without views. Mutant **SNAP-TWO-STEP** (v7.8 seed 21).
- **A caller-chosen ts** (AS OF, a bulk-load pin, a segment build): under the mutex, check `W ≤ t ≤ visible_ts`, then register `t`. Otherwise AS OF gets 72000. S-3's bulk-load pin registers first and picks its load ts above the pin (`verdicts/S-1.md`, S-1-F1 item 2).
- **Lifetime: v7.8 §3.1, kept.**
  - RR and SER: until the txn ends. RC: until the statement ends.
  - Cursors and portals: until they close.
  - WITH HOLD cursors are materialised at commit, before the SSI pre-commit, in the deferred-trigger fixpoint loop.
- **Latest-state reads** (deferred constraint scans, FK parent-side checks, the deferrable-prefix check) register `S = visible_ts` for the read's duration. v7.8's view counters and `vts` retire: nothing is truncated, and pages are read under latches, so a registered ts is all a read needs.
- **Stability.** A tree read at a registered `S` is stable. Entries `≤ S` change only through GC with `w ≤ W ≤ S` (S-STORE I-GC-READEQ), through settle (read-equivalent by §2.4) and through S-3's loads into unpublished trees.

### 3.7 Boot
1. Run recovery (S-STORE §14), then the **boot checkpoint** (§2.5; S-STORE §14 step 8). It always writes a new meta, and no txn starts until that meta is synced. A read-only open (N11) skips it (§2.5).
2. `next ts` = the recovered apply frontier + 1, and `visible_ts = M` = the recovered apply frontier. Replay inserts ordinary entries only, so every recovered commit is materialised.
3. **The watermark floor (I-W-FLOOR).** `W` = the recovered meta's `gc_w`, and the store's `gc_w_used` starts at the same value (S-STORE §10.5). The recovered pages were pruned up to `gc_w`, so no registration may be admitted below it.
   - Mutant **W-FLOOR-ZERO**: `W` restarts at 0, so an AS OF at `t < gc_w` is admitted and reads a pruned state.
   - Test `t_w_floor_after_restart`: prune at `w`, checkpoint, crash, reopen; AS OF at `w − 1` must give 72000.
4. **Reused ts values.** The ts of commits lost in a crash may be reused. Nothing durable references them: every entry above `T` in an image has durable records and is replayed (I-CKPT-AT-T (b)).
5. **No boot sweep.** Provisional entries from the previous boot are dead (§2.4), and no sweep exists for them (§12 O1).

### 3.8 Scalability requirements
These are not correctness rules. E7 and B-7 measure them; they address S-STORE §19 T3.
- The per-commit serial sections are §3.1 step 1 and the visible-prefix advance. Neither does per-key work.
- Lock acquire and release touch one partition (§2.2).
- The SSI mutex is taken only on SER paths, as in v7.8.

---

## 4. Read path (readers never block)
To read key `k` (or a range) at snapshot `S`, with statement start `seq0`, inside txn `R`:
1. **Own layer.** Apply §2.1's own-read rule. If it decides the key, stop.
2. **Committed data.** Use S-STORE §8.1 (point) or §8.3 (scan), with the provisional rule (S-STORE §8, as edited):
   - **The head.** The group's head may be a provisional entry `e` of owner `T`:
     - `e` is dead: ignore it;
     - `T = R`: ignore it, because step 1 already used it;
     - otherwise, resolve `T` under the leaf latch (I-PROV-LOOKUP). If `T` is `Committed(c)` or `Released(c)` with `c ≤ S`, `e` is a **candidate version at `c`**.
     - If `T` is `Committed(c)` or `Released(c)` with `c > S`, `e` is a version above `S`: it is skipped, and under SER it gives the edge `R → T` (§8.1 step 3), exactly as a skipped ordinary version would. Release does not end this: a released large txn's entry stays a version at `c` until its finalize converts it, and then the converted `k@c` gives the edge through the writer map. Mutant **RELEASED-EDGE-DROPPED**: only a `Committed(c > S)` owner gives the edge.
     - Otherwise (`Open` or `Aborted`), ignore it. An `Open` owner holds `k`'s lock (I-PROV-LOCK), so a SER reader's §8.1 step 2 already sees it.
   - **The result** is the newer of the candidate and the newest ordinary entry `≤ S`. Both may exist while a finalize or a later small commit is in flight. If both are at `c`, they are the same version. A tombstone reads as absent.
   - **Head first.** The head is examined **before** the ordinary entries of its group. With I-PROV-CONVERT-ORDER, that is sufficient. Mutant **ORDINARY-FIRST**: the ordinary entries are read first, so a conversion that lands between the two reads makes the version vanish.
   - **Fast path.** Each tree keeps a count of registered owners that have provisional entries in it.
     - The count is incremented before an owner's first provisional entry in the tree, and decremented (release ordering) after its finalize or cleanup has deleted its last one there.
     - A point read that reads 0 (acquire ordering) may skip the head check, so S-STORE §8.1's single descent is unchanged when no large txn is active in the tree.
     - **When the count is read.** After `S` is registered (§3.6), and for a SER read after §8.1 step 2. A count of 0 read after `S` means every large txn that flipped at `c ≤ S` has already finalized in this tree (its versions are ordinary entries), and every later one flips at `c > S`. Read before `S`, a large txn could flip at `c ≤ S` between the two and its version would be missed. For SER, a large `W` that is released at `L` incremented the count before `L`, so a count read after `L` sees it unless `W` has finalized, and then the writer map gives the edge (§8.3).
     - Mutants: **COUNT-BEFORE-SNAPSHOT** (the count is read before `S` is registered), **COUNT-DEC-EARLY** (the count is decremented before the last conversion or delete), **COUNT-BEFORE-LOCKTABLE** (a SER read reads the count before §8.1 step 2).
     - Dead entries never affect a point read at `S < u64::MAX`, because `seek(k, S)` lands after them.
   - **Scans** apply the same rule per group.
     - Under SER, each skipped version with ts `> S` records `R → writer(ts)` (v7.8 §4 step 2; §8.5).
     - So a SER scan steps through the entries above `S`, instead of seeking over them. S-STORE §8.3's version-skipping seeks serve non-SER reads, and the part of a group at or below `S`.
3. **Kept clauses.** AS OF reads, the RR/SER relation storage-id rule (40001), the catalog at `t`, and index `built_at`: v7.8 §4's paragraphs are kept with §0.2.

**I-NOBLOCK** (kept). No step waits on another txn. The owner lookup is a live-txn registry read under a latch, never a wait (mutant LOOKUP-WAITS). A SER read also checks the lock table first (§8.1), which is a read, never a wait.

---

## 5. Write path

### 5.0 Operations
v7.8 §5.0 is kept with §0.2: row ops, key-existence ops and entry deletes. The lock target of an operation is `k`, or the prefix range for a deferrable unique entry.

### 5.1 The loop
An operation on key `k` by txn `W`, at snapshot `S`, with current command `seq`, statement start `seq0`, and requested mode `m` (v7.8 §6):
```
loop:
  acquire(k, m)                                                     # §6.1
    for every live holder H ≠ W whose mode conflicts with m:
      key-existence op, every such H has data = false, and newest(k) is a live
        committed version of another row        → 23505, or the §5.3.1 arbiter path  # no wait on lock-only
      NOWAIT → 55P03;  SKIP LOCKED → skip the row
      otherwise wait_for(W → H, gen(H) read where the conflict was seen); continue      # §6.3
    ended entries are replaced; W now holds an entry covering k in mode ≥ m

  # W holds k. No other txn can write k (exclusive modes), and no apply of k is in flight:
  # earlier holders released only after visible_ts ≥ their c (C-COMMIT-ORDER).
  # A conversion in flight is invisible to newest(k), by the head-first rule (§4).

  if W has an own data layer on k: own-row rules (§5.4); skip the newer-version rule
  elif row op (not an entry delete, §5.0):
    base = S, or the EPQ version's ts (§5.2), or v_r.ts (§5.3.1)
    n = newest(k)                       # S-STORE §8.2 with §4's provisional rule (I-NEWEST)
    if n is a data version with n.ts > base:
      RR/SER: undo-acquire; raise 40001    # KEY SHARE: only if a version above base is a
                                           # tombstone, a moved-tombstone or key_changed
      RC:     EPQ (§5.2), still holding k
  key-existence op: unique check (§5.3) on the own top layer, else newest(k); never 40001 from n alone; never EPQ
  shared lock: record it (the entry carries seq); done
  append (seq, tree, k) to the key log; push or modify the layer (§2.1)
  if the layer has data: set the entry's data flag; then the SSI writer-side check (§8.2)
  large W: materialise k per §3.2 (now or later)
```
**Undo-acquire.** An op that acquired or strengthened `W`'s entry on `k`, and then ends with no write and no lock (40001, 23505, 21000, 27000, an EPQ skip), restores `W`'s previous entry on `k`, or removes the entry if there was none. If that released anything, it bumps `W`'s wake generation (v7.8 §5.5's rule). Mutant **ACQUIRE-LEAK**: an EPQ skip keeps the lock, so a later writer waits for the whole txn (I-LIVE).

**Early exits leave nothing behind.** The write set is private, and nothing reaches a tree before commit (or before materialisation, which only a large txn's own reads and §2.4's rules see). So an early exit leaves the trees as they were, and the lock table as undo-acquire leaves it.

**The checks run after the lock (directive (6)).** The RR check and the unique check run **after** the acquire, including after any wait, using the newest probe. That is the spike's order (`spikes/e1-arm-b/src/engine.rs:430–435`).
- **I-WW (kept).** Under RR/SER, a row op (not an entry delete) on a key with no own data never succeeds when the key's newest data version is newer than `S`. Mutant **RR-CHECK-BEFORE-LOCK**: W1 holds `k`; W2 (RR, with `S` before W1's commit) waits; W1 commits; W2 must raise 40001, and with the mutant W2's write succeeds (a lost update). Test `t_rr_check_after_wait`.
- **I-UNIQUE (kept).** Mutant **UNIQUE-BEFORE-LOCK**: a concurrent duplicate insert commits while the second inserter waits; the second must get 23505. Test `t_unique_after_wait`.
- **Shared modes.** The §8.2 precondition (the caller holds `k`'s lock) holds outright for exclusive modes and for SHARE. Under KEY SHARE, a compatible NO KEY UPDATE holder can commit concurrently. It can only produce a live, non-key-changing version, which KEY SHARE's rules ignore, so its race is unobservable.

### 5.2 RC EvalPlanQual
v7.8 §5.2 is kept with §0.2, with these changes (S-2a writes the full text):
- **EPQ runs while `W` holds `k`'s entry.** v7.8 ran it unlatched and without a placement. Since no other txn can write `k` meanwhile, `newest(k)` cannot change, so **EPQ never repeats**. v7.8's repeat rule, and its I-PROGRESS obligation for EPQ, fall away. PostgreSQL likewise locks the newest tuple version before EvalPlanQual.
- **A failed EPQ** skips the row and runs undo-acquire.
- **Kept:** moved rows raise 40001; KEY SHARE EPQ examines every version above `S`; the ON CONFLICT lock never runs EPQ; joined DML stays open (§12).
- **Deadlock.** EPQ may acquire other locks while holding `k`. The wait-for graph covers that (§6.3).

### 5.3 Unique, primary key and FK
v7.8 §5.3 is kept with §0.2 (S-2a writes the full text). Changes:
- The unique check's "current state" is the own top layer if it has data, else `newest(k)`, read while holding `k`'s entry.
- **The deferrable prefix check** holds a range entry on the prefix. A live foreign holder with data in the range is waited on. Committed entries are read through a registered latest-state scan (§3.6).
- I-UNIQUE and I-FK are kept.

### 5.3.1 ON CONFLICT arbiter protocol
v7.8 §5.3.1 is kept with §0.2 (S-2a writes the full text). Changes:
- **The pre-check** reads the lock table:
  - a live foreign holder **with data** on an arbiter key is waited on, and then the attempt restarts;
  - a lock-only holder is ignored.

  It then reads the current state (the own layer, or `newest(a)`). The pre-check is advisory: step 2 or 3 re-verifies it under the lock.
- **Step 2** acquires the attempt's keys in ascending key order.
- **An abandon** is `ROLLBACK TO sa` (§5.5), including undo-acquire of the attempt's entries.

### 5.4 Own-row rules
v7.8 §5.4 is kept verbatim with §0.2 ("an intent on `k`" reads "own layers on `k`").

### 5.5 Savepoints and cursors
v7.8 §5.5 is kept, except that the write-set mechanics below replace its intent mechanics. S-2a writes the full text.

**`ROLLBACK TO SAVEPOINT s`**, for each key the key log records at `seq ≥ s`, **in this order**:
1. drop that key's layers with `seq ≥ s`. A spilled run is immutable, so the rollback records the dead interval `[s, current seq]`, and every reader of the runs ignores layers whose seq falls in a dead interval;
2. **re-materialise a materialised key** (§3.2), still holding the key's lock-table entry unchanged: write new provisional content, or delete the provisional entry, and append a superseding chunk op (`Delete{revert}` if no data remains);
3. only then recompute the entry's mode and data flag from the restored top layer, or remove the entry if no layer remains.

**Why step 2 precedes step 3 (I-PROV-LOCK).** While `T`'s `Open` provisional entry sits at `k`'s head, `T` must hold an exclusive entry covering `k`. If step 3 ran first and removed or downgraded the entry, a large `T2` could acquire `k` in that window and, as settler (b), meet an `Open` provisional entry, which §2.4 forbids it to settle (an implementation may poison on it).

Mutants:
- **ROLLBACK-KEEPS-PROV:** the layer is dropped but the provisional entry stays, so after commit the rolled-back value is visible;
- **ROLLBACK-NO-SUPERSEDE:** the tree is re-materialised but the WAL is not, so replay after a crash shows the rolled-back value;
- **ROLLBACK-UNLOCK-FIRST:** step 3 runs before step 2.

Test `t_prov_rollback_supersede`: a large txn takes a savepoint, updates, rolls back to the savepoint and commits; the read must show the old value, before and after a crash. A second case: the savepoint precedes `T`'s first write of `k`, and a large `T2` waits on `k`; a hook between the rollback's per-key steps lets `T2` run. `T2` must never observe an `Open` provisional entry while it holds `k` (with ROLLBACK-UNLOCK-FIRST, `T2` acquires `k` before `T`'s provisional entry is deleted).

**Kept from v7.8 §5.5:** SIREADs and recorded conflicts survive; portals and cursors opened at `seq ≥ s` close; queued checks and AFTER-trigger events with `seq ≥ s` are discarded; the SSI map journal is undone before relation locks are released; `seq` never goes back; the waiters are woken iff the rollback released something; writes and locks are tagged with the seq current when they execute; the layer-compaction rule.

---

## 6. Locks and waiting

### 6.1 Acquire, conflicts, release
- **Modes and matrix:** v7.8 §6. Shared modes now live in the lock table (§2.2).
- **Ended owners' entries are free** (§2.2).
- **Release:** §3.3 at commit, §7.1 at abort, §5.5 for savepoint rollback, and undo-acquire (§5.1).

### 6.2 Escalation (N2; X-4 ruling R2 item 4)
**Trigger.** When `T`'s entries in one tree reach `lock_escalate_rows`, `T` coalesces them.
- **Default 1,000,000.** That is the budget at which the spike's escalation worked on both arms (`spikes/e1-bench/README.md` §W6).
- The next trigger fires when the count again reaches twice its value after the last coalesce, so escalation is amortised O(1) per acquire.

**Coalescing.** `T`'s entries in the tree are replaced by **runs**.
- **A run** `[lo, hi]` is a maximal sequence of `T`'s entries, in key order, with equal mode and data flag, such that **no live foreign entry** lies in `[lo, hi]`. Ended foreign entries are free (§2.2) and may be covered.
- **A run covers the gaps** between `T`'s keys, including keys that do not exist.
- **Atomicity.** Coalescing may run in bounded batches. Each batch is atomic against acquires inside its runs (I-LOCK).

**Coalescing never waits and is never deferred.**
- A run never covers a live foreign entry, so nothing has to wait.
- This replaces the spike's L-ESCALATE-DEFER, which X-1a-F1 left for S-2 to decide. Its reason stands: an escalant holding a million rows must never wait on a foreign holder that is blocked on one of them.
- Mutant **ESCALATE-OVER-FOREIGN**: a run spans a live foreign entry, so two live holders conflict on one key (I-LOCK).

**Result.** `T`'s entries in the tree are at most (the live foreign entries inside `T`'s key hull) + (the mode and data-flag changes) + 1.
- If `T`'s entry memory still exceeds `lock_mem`, the acquire fails with 53200 (N16).
- After coalescing, an acquire by `T` inside a run of sufficient mode is a no-op. Any other acquire adds an entry.

**What other txns see** (documented behaviour; divergence in §12):
- **Inside a run.** A foreign request on any key inside a run conflicts as if `T` held that key in the run's mode, including keys `T` never touched and keys that do not exist. It waits; with NOWAIT it gets 55P03; with SKIP LOCKED the row is skipped.
- **Outside every run.** Keys there are unaffected. PostgreSQL never escalates. A relation-wide escalation would make SKIP LOCKED skip the whole relation, so runs bound the divergence to `T`'s key hull in that tree.
- **A data run counts as data, conservatively:**
  - for the key-existence no-wait rule (§5.1), an inserter waits for `T` instead of raising 23505 at once, then re-runs and gets the same answer;
  - for SSI, a SER reader inside a data run records `R → T` (§8.1), which may cause an extra 40001 but never loses an edge.

**Tests (B-1b).**
- `t_escalate_1m`: 1,000,001 row locks in one tree leave `T` with at most 1 entry there plus the foreign splits. NOWAIT on an untouched key inside the hull gets 55P03, and SKIP LOCKED skips that key. A key outside the hull is granted. Release stays O(1). The acceptance runs it with `--release`.
- `t_escalate_split_by_foreign`: a foreign holder in the middle of the hull splits the run, and the foreign key is never covered.
- Mutants ESCALATE-OVER-FOREIGN and **NOWAIT-IGNORES-RUN** (the NOWAIT check reads row entries only).

### 6.3 Waiting
v7.8 §6 is kept with §0.2 (S-2a writes the full text). It covers:
- the one wait-for graph over every kind of wait;
- `wait_for(W → T, g)` with the generation re-check made in the graph critical section;
- waking: remove edges, then wake;
- the deadlock DFS after `deadlock_timeout`, with exactly one 40P01 per cycle;
- NOWAIT (55P03), SKIP LOCKED, and `lock_timeout` (55P03);
- cancellation wakes parked sessions;
- relation locks held to txn end, released by `ROLLBACK TO` for those taken after the savepoint;
- advisory locks at xact scope;
- `wait_for_lockers`.

The wake generation lives in the owner's shared state (§2.2). The "missing status" cases disappear, because handles never miss.

---

## 7. Abort and crash

### 7.1 Abort
The owning session acts outside any lock-table critical section, while its state is still `Open`:
1. settle its DDL's SSI map journal (v7.8 §7.1: undo it before `Aborted`);
2. set `Aborted` (one store);
3. wake its waiters, removing their edges first;
4. release its relation and advisory locks.

Its entries are now free (§2.2).
- **Cost.** Steps 1–4 do no per-key work, so waiters resume in O(1) plus O(waiters).
- **What else happens.**
  - The write set is discarded, and its spill files are deleted.
  - **No WAL record** is written: recovery ignores chunks with no marker (S-STORE §14).
  - A large txn's **cleanup continuation** settles (deletes) each of its provisional entries, then deregisters. Until then, readers ignore those entries because the owner is `Aborted`.
  - SSI state follows v7.8 §8.6. The purge follows §3.3.

### 7.2 Crash, and what v8 does not have
- **After a crash,** every txn without a durable `Commit` or `CommitMarker` never committed.
- **Recovery is S-STORE §14 alone,** followed by the boot checkpoint. There are no status records to load, no intents to resolve, no boot sweep and no status truncation.
- **Retired:** v7.8 §7.2–§7.4, together with I-TRUNC, I-COUNT and I-ONE-INTENT (§11.3).

---

## 8. SSI (SERIALIZABLE only)
**Kept** from v7.8 §8 with §0.2 (S-2b writes the full text):
- the algorithm (Cahill, Ports & Grittner, with PostgreSQL's commit-ordering refinements) and the one SSI mutex;
- SIREAD granularity and escalation, relation SIREADs keyed by oid, and SIREADs kept across `ROLLBACK TO`;
- edges carrying `commit_ts`, and "only concurrent txns form edges";
- the dangerous structure, the read-only exception and `earliest_out_conflict_commit`;
- the pre-commit critical section, the writer map, and retention.

N2 removes the pending intents that v7.8's reader-side edges relied on (§4 step 1, §8.2). §8.1–§8.3 replace that mechanism, and PLAN-v3 §3.4 asks S-2 to show that the §8.2 argument still holds; §8.3 does.

### 8.1 The read order (I-SSI-ORDER, v8)
A SER read of key `k`, or of a range `[a, b)`, does these steps **in this order**:
1. register the SIREAD covering it, under the SSI mutex (v7.8 §8.1, including index→row fetches and scans that register the full bound first);
2. **check the lock table**: for every live holder `H ≠ R` with a **data** entry covering `k` (or overlapping `[a, b)`), record `R → H`;
3. **read the tree** (§4), recording `R → writer(t)` for each skipped version `t > S` (§8.5), and `R → T` for each skipped provisional entry whose owner is `Committed(c)` **or `Released(c)`** with `c > S` (the `TxnId` is in the entry). A point read's fast-path count (§4) is read in this step, after step 2.

Mutants:
- **SER-NO-LOCKTABLE-CHECK:** step 2 is skipped (write skew is missed);
- **LOCKTABLE-AFTER-READ:** step 2 runs after step 3;
- **SIREAD-AFTER-READ:** v7.8 seeds 5 and 6;
- **RELEASED-EDGE-DROPPED** and **COUNT-BEFORE-LOCKTABLE** (§4).

### 8.2 Writer side
After a data-changing write of `k` (§5.1, with the entry's data flag set first), `W` checks the SIREADs covering `k` that other SER txns hold, under the SSI mutex. Each gives `R → W`.
- Lock-only and shared acquisitions do not check.
- The DDL side is kept (v7.8 §8.2).
- Mutant **CHECK-BEFORE-FLAG**: `W` checks before it sets the data flag.

### 8.3 Why no edge is lost without intents
Take concurrent SER txns `R` (snapshot `S`) and `W` (commit `c > S`), where `R` reads `k` and `W` writes data on `k`. Name the events:
- `F`: `W` sets its data flag on `k`, a store under the entry's partition mutex (§2.2);
- `A`: `W`'s writer-side check (§8.2), with `F` before `A`;
- `B`: `R`'s SIREAD registration;
- `L`: `R`'s lock-table check (§8.1 step 2), with `B` before `L`;
- `T_R`: `R`'s tree read (§8.1 step 3), with `L` before `T_R`.

`A` and `B` both run under the SSI mutex, so they are ordered.
- **Case `B` before `A`.** `W`'s check sees `R`'s SIREAD, so it records `R → W`.
- **Case `A` before `B`.** Then `F` is before `L`: program order gives `F` before `A`, the SSI mutex gives `A` before `B`, and program order gives `B` before `L`. `L` reads the lock-table partition under the same mutex, so it observes `F` (mutant FLAG-RELAXED breaks this when the store is outside the mutex and unordered with `L`). At `L`, one of two things holds:
  - `W` is still a live holder with data on `k`. Then step 2 records `R → W`.
  - `W` has been released. Release follows `visible_ts ≥ c` (§3.3), which follows `W`'s apply (`k@c` inserted) or flip (its provisional entry reads as a version at `c`). So at `T_R` the tree holds `W`'s version at `c > S`.
    - GC cannot have removed it: GC keeps every version above `w ≤ W ≤ S < c`.
    - A conversion between `L` and `T_R` is invisible, by the head-first rule.
    - So step 3 records `R → W`: through the writer map for an ordinary `k@c`, or through the provisional entry's owner, which is `Released(c)` with `c > S` (§4; this is the case mutant RELEASED-EDGE-DROPPED loses). A point read cannot skip that head on the fast path: `W` incremented its tree count before `L`, and the count is read after `L` (§4).
- **Rolled-back writes.** If `W` rolled back its data on `k` before `L`, no dependency remains, because the rolled-back write is not part of `W`'s committed effects. Edges recorded earlier are kept (v7.8 I-SSI-EDGES counts placements that were later rolled back).
- **Escalated runs** count as data in step 2, so the recorded edges are a superset.
- **Phantoms.** An insert of a new key `x` into `[a, b)` is the same case analysis with `k := x`. Step 2 checks the range, and step 3's scan visits `x`'s group, whose versions all lie above `S`.

**This replaces PLAN-v3 §3.4's pre-commit re-check** "of the SIREAD set over its whole data-changing write set inside the §8.4 critical section" (§13 R4). That re-check would cost O(write set) under the global SSI mutex, and the argument above shows that the write-time check plus step 2 already lose no edge. S-2b writes this argument as a proof, and G0-ssi checks its interleavings.

### 8.4 Pre-commit
v7.8 §8.4 is kept. "Enqueue its commit request" now means §3.1 step 1: the ts assignment and the append run **inside** the SSI critical section, so commit order still equals prepare order.
- **Lock order:** SSI mutex → sequencer critical section.
- **A large SER txn materialises (§3.2 step 1′) before it enters the critical section.** O(keys) work never runs under the SSI mutex. A txn found doomed afterwards aborts and runs its cleanup (§7.1).

### 8.5 Finding `writer(t)`
v7.8 §8.5 is kept. The writer-map entry is inserted in §3.1/§3.2 step 1, before the commit becomes visible. A provisional entry carries its writer's `TxnId` directly.

### 8.6 Retention and abort
v7.8 §8.6 is kept with §0.2.

---

## 9. Watermark and GC

### 9.1 Watermark (directive (4))
**Computation.** `computed = min` over:
- every registered ts (§3.6);
- `M`;
- `visible_ts`;
- the AS OF retention window's lower bound, **only when the configured window is non-zero**: the largest sampled ts whose wall time is at or before the cutoff, or 0 if no sample qualifies (v7.8 §9.1). With a zero window the term is absent.
  - The samples are kept in memory and restart empty at open. So with a non-zero window, after every open no sample qualifies until one ages past the cutoff, and `W` (hence GC) stays at its floor for one window length. This is a known cost (§12 O6), not a correctness issue: it only retains more.

**Publication.** In one snapshot registry critical section, `W := max(W, computed)`.
- `W` is monotonic, and `W ≤ M ≤ visible_ts`. Mutants **W-DECREASE** (v7.8 seed 22) and W-ABOVE-M (§3.5).
- Every caller-chosen registration is checked against the published `W` (§3.6).
- GC mutations take `w` = the published `W` at their start (S-STORE §10.1, A-GC-STABLE (1)). The store records the largest `w` it used (`gc_w_used`), and the meta carries it as `gc_w` (S-STORE §10.5). Nothing else is persisted: the meta's `gc_w` is the durable record that v7.8 kept as `/sys/gc_w`.

**The restart floor:** §3.7 step 3 (I-W-FLOOR).

**No snapshot age cap.** As in PostgreSQL 17, a long txn holds `W` back. §9.5 gives the ruling on disk growth.

### 9.2 What GC removes
S-STORE §10 owns it: the rule, including X-2-F1's tombstone elision and I-GC-ORDER; the page-local prune; the sweep; and `gc_w`.
- **Provisional entries are never versions to GC.** GC never counts, keeps or prunes them as versions. Dead ones may be deleted by any GC mutation (§2.4 (c)).
- I-GC-READEQ holds at every `S ≥ W` because `W ≤ M`.

### 9.3 DDL retirement
DROP, TRUNCATE and table-rewriting ALTER retire the relation's trees and, for TRUNCATE and rewrite, create new ones (S-STORE §12.8: a `CreateTree` and the catalog swap in one commit).
- **When the old trees are dropped.** `DropTree` (S-STORE §6.8) runs on a tree only once `W >` the retiring DDL's commit ts (v7.8 §9.2's rule).
- **The retired-tree record** is a catalog row written in the DDL's own txn, so it commits, aborts and rolls back with the DDL. Its representation belongs to S-3.
- **Drop cleanup** reads committed retired rows at a registered snapshot `S`. A tree from a row visible at `S` may be dropped once `W > S`.

### 9.4 Scheduling (directive (8); background jobs, C-B1 semantics)
**Jobs.**
- GC sweeps; the page-local prune runs inline, at splits and optionally at eviction (S-STORE §10.3);
- drop cleanup (S-STORE §6.8 walks, including their resumption at open);
- SSI retention;
- large-txn continuations: finalize, abort cleanup and purge.

v7.8's resolver and boot sweep no longer exist.

**The sweep trigger.** Each tree keeps a garbage estimate `g`.
- `g` is incremented by every apply or conversion insert into a key that already has a committed version, and by every tombstone. It is reset when a sweep completes.
- A sweep of the tree is scheduled when `g ≥ max(gc_min_versions, gc_ratio × the tree's entry count at its last complete sweep)`, **and** `W` has advanced past the previous sweep's `w`. The defaults are `gc_min_versions = 10,000` and `gc_ratio = 0.1`; R-1 may change them.
- A retired tree's drop is scheduled when `W` passes its DDL (§9.3).

**Execution.**
- **At most one sweep per tree** at a time. Each step is one gated operation (S-STORE §10.4). The token is kept in memory, and a crash restarts sweeps from scratch, which is sound by S-STORE §10.4 "Interruption".
- **Budget.** Steps are throttled by `gc_io_budget` (pages per second; unthrottled by default). In embedded and wasm mode, steps run cooperatively in bounded slices at commits and checkpoints (N10). Budgets throttle steps and never change their outcome (v7.8 §9.3).
- **A physical error** is fail-stop (a store poison).

**Shutdown order.**
1. jobs that commit txns (drop cleanup's catalog txns, analyze);
2. large-txn continuations, so that finalize completes and the final checkpoint has `M = visible_ts`;
3. the commit pipeline;
4. GC and retention.

Test (B-3): `t_gc_schedule_trigger`, beside B-3's W3/W4-shape acceptance (S-STORE §11).

### 9.5 On-disk growth under long readers (X-4 ruling R2, "Not R-1's call")
**Ruling: no bound in 2.0**, as in PostgreSQL 17, which has no snapshot-too-old.
- **What happens.** A long reader holds `W`, and versions accumulate in the trees. They occupy disk, not RAM (S-STORE §5.5).
- **The operator's tools:** `transaction_timeout` and `idle_in_transaction_session_timeout` (PostgreSQL 17 semantics, kept), and the exported metrics: the age of the oldest registered ts, `W`'s lag (`visible_ts − W`), and each tree's garbage estimate.
- **Disk exhaustion** is S-STORE's 53100, which is fail-stop.
- **Revisit trigger.** A G9 soak, an NBench operational row or a user report in which retention under a long reader exhausts the disk before the operator's timeouts can act. An Opus card then considers a bound, for example an opt-in snapshot-too-old.

---

## 10. Other rules
- **Tree ids.** v7.8's storage ids are tree ids (S-STORE §6.8): `u64`, never reused. The catalog maps relation and index oids to tree ids (S-3).
- **Sequences.** v7.8 semantics are kept: non-transactional; values may skip and never repeat. Their persistence belongs to S-3.
- **Catalog.** Transactional DDL, the lock modes and the SSI map journal are kept (v7.8 §10). Catalog rows live in catalog trees (S-3).
- **Bulk ingest.** v7.8's `ingest_versions` is replaced by S-STORE §12.8 (unlogged loads into an unpublished tree, then a forced checkpoint, then publication) and S-3 (the steps). Its `W` pin is §3.6's caller-chosen registration. I-INGEST retires to S-STORE I-BULK-PUBLISH.
- **Large values** live in overflow chains (S-STORE §6.7; tags 4, 5 and 8).
- **Large txns:** §3.2.

---

## 11. Invariants
Each invariant needs a test and a named mutant that the test catches. The "Pinned by" column names the card that writes the test. For B-1 and B-2 these are P-written tests, written before the implementation (PLAN-v3 §7.1).

**B-1d is a new card**, split from B-1c: pre-apply, flip, finalize, settle and the boot id. PL-1 adds its row (§13 R6). B-1c keeps the small-txn commit, visibility, I-NEWEST and I-FAIL-STOP.

### 11.1 Kept from v7.8, with the v8 mechanism
| ID | Statement (v8) | Pinned by | Named mutants |
|---|---|---|---|
| I-HALLOWEEN | §2.1: a statement never sees its own writes with `seq ≥ seq0`, in every write-set representation | B-1a | SPILL-DROPS-SEQ |
| I-WW | §5.1: RR/SER row op with no own data never succeeds over a newer data version; checked after the lock | B-2d | RR-CHECK-BEFORE-LOCK |
| I-LOCK | §2.2/§6.2: no two live holders hold conflicting modes on one key, counting runs | B-1b | ESCALATE-OVER-FOREIGN, NOWAIT-IGNORES-RUN |
| I-UNIQUE | §5.3: committed state `≤ S` never has two live rows with one unique key; checked after the lock | B-2d | UNIQUE-BEFORE-LOCK |
| I-FK | v7.8 §11, unchanged | B-2d | (v7.8 seed 58) |
| I-VIS | §2.2: `Committed(c)` before `visible_ts ≥ c` | B-1c, B-1d | VISIBLE-BEFORE-STATE |
| I-ACK | §3.4: COMMIT returns only once `visible_ts ≥ c` (and after the sync for `on`) | B-1c | ACK-BEFORE-VISIBLE |
| I-SNAP-ORDER | §3.6: `S` is read and registered in one step | B-1c | SNAP-TWO-STEP |
| I-ATOMIC | after any crash, all or none of a txn's writes are visible at `visible_ts` | B-1d, A-7d | CKPT-T-VISIBLE |
| I-DURABLE | an acked `on` commit survives any crash | B-1c, A-7d | (S-STORE I-REDO-COMPLETE mutants) |
| I-NOBLOCK | §4: no read-only path waits on another txn | B-1d | LOOKUP-WAITS |
| I-LIVE | v7.8 §11 (a)–(c); release and undo-acquire wake | B-2b | ACQUIRE-LEAK |
| I-RC-MONO | v7.8 §11, unchanged | B-2d | (v7.8 seed 18) |
| I-PROGRESS | v7.8 §11; EPQ no longer retries (§5.2) | B-2d | (v7.8 seeds 52, 57) |
| I-SER | v7.8 §11 (Elle, G2) | B-2c, B-7 | — |
| I-SSI-ORDER | §8.1: SIREAD, then the lock-table check, then the tree read | B-2c | SER-NO-LOCKTABLE-CHECK, LOCKTABLE-AFTER-READ, SIREAD-AFTER-READ |
| I-SSI-EDGES | v7.8 §11; the footprint adds §8.1's lock-table checks, §8.2's writer checks and released provisional owners (§4) | B-2c | CHECK-BEFORE-FLAG, FLAG-RELAXED, RELEASED-EDGE-DROPPED, COUNT-BEFORE-LOCKTABLE |
| I-SSI-PRECISION | v7.8 §11, unchanged | B-2c | (v7.8 seeds 61, 63) |

### 11.2 New
| ID | Statement | Pinned by | Named mutants |
|---|---|---|---|
| C-COMMIT-ORDER | §3.1–§3.3: append, then apply or flip, then visible, then release, then ack | B-1c | RELEASE-BEFORE-VISIBLE, ACK-BEFORE-VISIBLE |
| I-RELEASE-O1 | §3.3: release does no per-entry work before waking; ended entries are free | B-1c | RELEASE-OKEYS |
| I-WS-SPILL | §2.1/§5.5: every representation answers the same layer queries; dead seq intervals are honoured | B-1a | SPILL-DROPS-SEQ, DEAD-INTERVAL-IGNORED |
| I-ESCALATE | §6.2: coalescing never covers a live foreign entry, never waits, and bounds entries | B-1b | ESCALATE-OVER-FOREIGN, ESCALATE-WAITS |
| I-UNDO-ACQUIRE | §5.1: an op that ends with no write or lock restores the previous entry | B-1b, B-2d | ACQUIRE-LEAK |
| I-PROV-INVISIBLE | §2.4/§4: no reader other than the owner reads an `Open`, `Aborted` or dead provisional entry as data | B-1d | PROV-OPEN-VISIBLE, DEAD-AS-LIVE |
| I-PROV-ATOMIC | §3.2: for every `S ≥ c` all of `T`'s writes are visible, and for `S < c` none are | B-1d | VISIBLE-BEFORE-MATERIALIZED |
| I-PROV-LOOKUP | §2.4: the owner is resolved under the leaf latch and never misses | B-1d | DEREGISTER-EARLY, LOOKUP-AFTER-UNLATCH |
| I-PROV-CONVERT-ORDER | §2.4/§4: conversion inserts before it deletes, in one gated op; readers examine the head first | B-1d | DELETE-BEFORE-INSERT, ORDINARY-FIRST |
| I-PROV-LOCK | §2.4/§5.5: an `Open` owner's provisional entry lies under its exclusive lock, including during `ROLLBACK TO`; `Open` entries are never settled | B-1d | SETTLE-OPEN, ROLLBACK-UNLOCK-FIRST |
| I-PROV-FASTPATH | §4: a point read skips the head check only on a count of 0 read after `S` is registered (after §8.1 step 2 for SER); the count covers every entry until it is deleted | B-1d | COUNT-BEFORE-SNAPSHOT, COUNT-DEC-EARLY |
| I-LOCK-ORDER | §2.6: locks are taken in the stated order; the stripe is never taken under a page latch; the live-txn registry is a leaf | B-1d | STRIPE-UNDER-LATCH |
| I-PROV-WAL | §3.2/§5.5: each provisional entry equals the last chunk op for its key | B-1d, A-7d | ROLLBACK-NO-SUPERSEDE, ROLLBACK-KEEPS-PROV |
| I-M | §3.5: checkpoint `T = M`; `W ≤ M` | B-1d, A-7c | CKPT-T-VISIBLE, W-ABOVE-M |
| I-BOOT-ID | §2.5: boot ids never repeat; the boot checkpoint always writes a new meta, which is durable before the first txn | B-1d, A-7d | BOOT-ID-REUSE, BOOT-CKPT-NOT-DURABLE, BOOT-CKPT-SKIP-CLEAN |
| I-HOL | §3.1–§3.2: an unrelated commit waits for at most one bounded small apply and one chunk batch per active large txn | B-1d | CHUNK-QUEUE-UNBOUNDED |
| I-W-FLOOR | §3.7: after open, `W ≥` the recovered meta's `gc_w` | B-3 | W-FLOOR-ZERO |
| I-W-MONO | §9.1: `W` never decreases | B-3 | W-DECREASE |

### 11.3 Retired
- **I-ONE-INTENT** → I-LOCK.
- **I-TRUNC** and **I-COUNT**: no status table and no intent counts.
- **I-WAL-ORDER** → S-STORE I-WAL-TS-ORDER.
- **I-GC** → S-STORE I-GC-READEQ, with `W` per §9.1.
- **I-GC-QUIESCE** → S-STORE I-GC-COMPLETE.
- **I-INGEST** → S-STORE I-BULK-PUBLISH and S-3.

v7.8 seeds whose bug cannot be expressed without intents, a status table or KV views retire. S-2c lists them, and B-5's RESULT records the retired tests.

### 11.4 Test list
P-written before the implementation for B-1 and B-2 (PLAN-v3 §7.1). Each test catches the mutants named in its row of §11.1 or §11.2.

| Test | Invariants | Card |
|---|---|---|
| `t_ws_spill_halloween`, `t_ws_spill_rollback_to` | I-HALLOWEEN, I-WS-SPILL | B-1a |
| `t_escalate_1m` (acceptance with `--release`), `t_escalate_split_by_foreign`, `t_escalate_nowait_skiplocked` | I-LOCK, I-ESCALATE | B-1b |
| `t_ack_after_visible`, `t_rr_put_after_own_delete`, `t_release_after_visible`, `t_release_o1_counter` (a cfg(test) count of entry visits during release must be 0) | I-ACK, C-COMMIT-ORDER, I-RELEASE-O1, I-VIS | B-1c |
| `t_prov_invisible` (`Open`, `Aborted` and dead owners), `t_prov_flip_atomic` (also with a hook between marking `c` visible and the reader's owner lookup, reader at `S = visible_ts`: with VISIBLE-BEFORE-STATE the owner is still `Open` and the version is missed), `t_prov_lookup_under_latch`, `t_prov_lookup_nowait` (an `Open` owner and a `Committed` owner with finalize parked, each blocked indefinitely: every read of their keys returns within 1 s; LOOKUP-WAITS hangs), `t_prov_fast_path` (a large txn flips between a reader's count read and its registration, and a finalize parked before its last delete), `t_prov_convert_order` (a hook between the insert and the delete, with a reader in between), `t_prov_inline_settle` (under shuttle, with a reader and a finalize racing an inline settler; STRIPE-UNDER-LATCH deadlocks), `t_prov_rollback_supersede` (with a crash, and with a waiting large `T2`, §5.5), `t_ckpt_t_is_m` (a checkpoint, then a crash, during finalize), `t_w_not_above_m`, `t_boot_id_reuse_crash` (a crash before the boot meta's sync, and a clean reopen with nothing dirty: the next boot's `TxnId`s must not collide with the crashed boot's chunks) | I-PROV-*, I-M, I-BOOT-ID, I-ATOMIC, I-VIS, I-NOBLOCK, I-LOCK-ORDER | B-1d |
| `t_w6_shape_hol` (acceptance, n5000, `--release`): W6's 10M-row UPDATE with conflicting and with unrelated 1-row `on` probes. Conflicting probes resume within 10 ms (p99) of the marker's durability (of the flip, for an `off` big txn). Unrelated 1-row commits keep their p99 within 2× of the no-big-txn baseline over the **whole** big txn: materialisation, step 1′, the marker, and finalize. Also measures the stall behind a 4,095-row small commit (§12 O2) | I-HOL, X-4 head-of-line items | B-1d |
| `t_rr_check_after_wait`, `t_unique_after_wait`, `t_undo_acquire_epq_skip` | I-WW, I-UNIQUE, I-UNDO-ACQUIRE | B-2d |
| `t_ssi_locktable_edge`, `t_ssi_no_lost_edge` (shuttle/loom over the `F`/`A`/`B`/`L`/`T_R` interleavings of §8.3, including a large `W` released with its finalize parked, read by a point read and by a scan) | I-SSI-ORDER, I-SSI-EDGES | B-2c |
| `t_w_floor_after_restart`, `t_w_monotonic`, `t_gc_schedule_trigger` | I-W-FLOOR, I-W-MONO | B-3 |

### 11.5 G0
S-2c rewrites v7.8 §11's model section for v8:
- **G0-commit:** small and large commits, flip, finalize parked, release, ack; a crash at every WAL write, sync and checkpoint step; the boot checkpoint and boot id.
- **G0-write:** the lock table, including a coalesced run; the write set with a spill and `ROLLBACK TO`; EPQ under the lock; ON CONFLICT.
- **G0-ssi:** §8.1's order, lock-table edges, pre-commit.
- **G0-gc:** `W`, `M` and the floor; a conversion racing a sweep; AS OF.

Until S-2c lands, the mutants of §11.1–§11.2 are the seeds that the pinning tests must catch.

---

## 12. Open questions and known divergences
**Kept open from v7.8:**
- Q1: RC joined DML, full EPQ or statement retry (G3c).
- Q4: the SERIALIZABLE unique-violation rule (G3c).
- Q5: session-scope advisory locks.
- Q6: `SERIALIZABLE READ ONLY DEFERRABLE` and safe snapshots.
- Q7: relation SIREADs keyed by oid. The catalog hands SSI the tree → relation map.

**Answered:**
- Q3: `synchronous_commit=off` commits are visible before their sync (§3.1 step 3).

**Retired:**
- Q2: no UDT backend.
- Q8: no schema-less intent removers remain.

**New open questions:**
- **O1, dead provisional entries.** After a crash they occupy space until a sweep or a writer reaches their key; there is no boot sweep. Trigger for a meta hint plus a targeted sweep: amcheck reports dead-provisional bytes above 1% of a tree after recovery, in G9 or NBench.
- **O2, thresholds.** `preapply_rows`, `preapply_bytes`, `ws_mem`, `chunk_batch` and `lock_escalate_rows`: R-1 sets the budgets. B-1d measures the unrelated-commit stall against `preapply_rows` (`t_w6_shape_hol`).
- **O3, the double tree write.** Finalize doubles a large txn's tree writes. Trigger: the NBench bulk rows (COPY, bulk UPDATE) miss target, and a profile attributes more than 25% of their time to finalize. An Opus card then considers alternatives.
- **O4, escalation and job queues.** Runs make SKIP LOCKED skip untouched keys inside a large txn's hull. Trigger: the driver/ORM matrix or a user report of a queue workload starving.
- **O5, CDC over supersedes.** Logical decoding of a chunked txn must fold its supersedes (S-STORE §14) before it emits rows. D-6 owns this.
- **O6, AS OF retention after open.** With a non-zero retention window, GC pauses for one window length after every open, because the wall-clock samples are in memory (§9.1). Embedded and browser apps that reopen more often than the window would never GC. Trigger for persisting the samples in the meta (an S-STORE format edit): a G9 soak, an NBench embedded row or a user report of unbounded growth under frequent reopens with AS OF enabled. Until then R-1's embedded and browser defaults should keep the window at 0.

**Known divergences from PostgreSQL 17.** v7.8's list is kept, with §0.2. Its C-T6 items move to S-3. New:
- **Escalated runs** lock the gaps and untouched keys inside a large txn's hull in one tree (NOWAIT gives 55P03, SKIP LOCKED skips). PostgreSQL never escalates.
- **COMMIT waits** for every earlier commit to become visible: at most one bounded small apply (§3.4). PostgreSQL has no such wait. This changes latency only.

---

## 13. Refinements of PLAN-v3 (for PL-1)
This file wins over the plan. PL-1 should bring the plan in line:

| # | Plan text | Here | Why |
|---|---|---|---|
| R1 | §3.4 commit step 6 "release locks" (O(keys) in the spike) | Release is one owner-state store plus waking; entries are freed lazily (§3.3) | X-4: release cheaper than O(keys) |
| R2 | §3.4 "S-2 specifies pre-applying large txns as txn-tagged provisional entries" if triggered | Specified: materialise, flip, finalize, settle (§2.4, §3.2) | Trigger fired (X-4) |
| R3 | §3.4 "advance `visible_ts` in ts order", where the apply frontier = `visible_ts` | `visible_ts` counts flips; the store's apply frontier is `M ≤ visible_ts` (§3.5) | A finalizing txn must be replayed, not read from dead provisional entries |
| R4 | §3.4 SSI "re-checks the SIREAD set over its whole data-changing write set inside the §8.4 critical section" | A write-time check plus the reader's lock-table check (§8.1–§8.3) | O(write set) under the SSI mutex; §8.3 shows no edge is lost |
| R5 | N2 "row locks on a relation escalate to a relation lock" | Coalesced runs that never cover a live foreign entry (§6.2) | No escalation wait; a bounded SKIP LOCKED divergence |
| R6 | §9 B-1c "commit apply + `visible_ts` + fail-stop" | B-1c (small commit, visibility) plus a new B-1d (pre-apply, flip, finalize, settle, boot id) | Keeps each card M-sized |
| R7 | §3.3/§3.4 boot (unstated) | A mandatory boot checkpoint, durable before the first txn, gives the boot id (§2.5) | `TxnId` uniqueness across boots |

---

## Changelog
- **Draft 8.1** (2026-10-23, card S-2-F1, per `verdicts/S-2.md` MERGE-WITH-FOLLOWUPS):
  - **SSI released edge (item 1).** A provisional entry whose owner is `Committed(c)` or `Released(c)` with `c > S` gives `R → T` (§4, §8.1 step 3, §8.3). Mutant RELEASED-EDGE-DROPPED.
  - **`ROLLBACK TO` order (item 2).** Re-materialise before recomputing or removing the entry, so I-PROV-LOCK holds throughout (§5.5). Mutant ROLLBACK-UNLOCK-FIRST.
  - **Lock order (item 4, ruling 1).** New §2.6. The settle stripe sits above the gate and page latches; the live-txn registry is a leaf (§2.4). Row I-LOCK-ORDER, mutant STRIPE-UNDER-LATCH.
  - **Head-of-line (item 5).** At most `chunk_batch` unsynced chunk bytes per large txn in the WAL queue (§3.2); `t_w6_shape_hol` bounds the whole big txn and times conflicting resumes from the marker's durability. Row I-HOL.
  - **Fast path (item 6).** The count is read after `S` is registered, and after §8.1 step 2 for SER (§4). Row I-PROV-FASTPATH; test `t_prov_fast_path`.
  - **Tests (item 7).** VISIBLE-BEFORE-STATE → `t_prov_flip_atomic`; LOOKUP-WAITS → `t_prov_lookup_nowait`.
  - **Boot checkpoint (item 8, ruling 6).** Durable before the first txn, always a new meta, skipped by read-only opens (§2.5, §3.7). "Boot epoch" is renamed **boot id**; I-EPOCH → I-BOOT-ID, EPOCH-REUSE → BOOT-ID-REUSE, `t_epoch_reuse_crash` → `t_boot_id_reuse_crash`; new mutants BOOT-CKPT-NOT-DURABLE and BOOT-CKPT-SKIP-CLEAN.
  - **AS OF samples (item 9).** The window term is absent when the window is 0; the post-open pause is §12 O6.
  - **Small fixes (item 10, ruling 4).** §1 defines the live-txn registry and renames the other "snapshot registry"; §0.2 maps "unlatch"; a no-ts commit ends `Released(⊥)` (§3.1); the large threshold counts all write-set memory, so a small txn never spills (§2.1); a replace frees the replaced chain (§2.4); ranges split at partition boundaries (§2.2); the data flag is stored under the partition mutex (§2.2, §8.3; mutant FLAG-RELAXED).
- **Draft 8.0** (2026-10-23, card S-2, increment 1). C-T0 re-hosted on N2 and N3 = (b):
  - §0: clause status and substitutions for every v7.8 clause.
  - §2: write set, lock table with ended-owner freedom, provisional entries (encoding in S-STORE §7.3), settle, boot epoch.
  - §3: small and large commit pipelines, O(1) flip and release, ack after visibility (the X-3 observation), `M` as the store's apply frontier, the registry, boot.
  - §4: the provisional read rule (head first).
  - §5.1: the loop on the lock table, with the RR and unique checks after the lock.
  - §6.2: escalation by runs.
  - §7: abort; the retired crash machinery.
  - §8.1–§8.3: SSI without intents, and the lost-edge argument.
  - §9: the watermark with `W ≤ M` and the restart floor; GC scheduling; the long-reader ruling.
  - §11: the invariant table and test list.
  - §13: PLAN refinements R1–R7.

  Every X-4 directive to S-2 is addressed: pre-apply (§3.2), release (§3.3), the unrelated-commit stall (§3.1, §3.2), commit visibility (§3.4), the watermark floor (§3.7), write set, lock table and escalation (§2.1, §2.2, §6.2), the RR and unique checks (§5.1), SSI (§8), GC scheduling and the X-2-F1 rule (§9.2, §9.4), and the long-reader ruling (§9.5).
- Drafts 1–7.8: `nucleus/v2/docs/C-T0-txn-protocol.md` (changelog there).
