# C-T0: Transaction protocol (normative)

Status: draft 1. Every `nucleus-txn` card implements against this file. A change to an invariant (`I-*`) needs a spec change first, then the G0 model updated, then code.

Scope: single node. Isolation levels: RC (PostgreSQL semantics), RR = SI, SERIALIZABLE = SSI. 2PC (`PREPARE TRANSACTION`) is refused with 0A000 in 2.0.

## 1. Vocabulary

| Term | Meaning |
|---|---|
| `Ts` | u64 commit timestamp from the sequencer. Wrapped in a struct so it can widen to an HLC later. `Ts::ZERO` is never a commit ts. |
| `TxnId` | `(epoch: u32, n: u64)`. `epoch` = boot epoch. `n` dense per epoch. |
| `S` | A snapshot ts. Snapshot visibility: committed with `commit_ts <= S`. |
| `visible_ts` | Largest ts such that every commit `<= ts` is applied (§3). |
| logical key | KV key without the version suffix (`/t/{tbl}/{pk}`, `/u/{idx}/{key}`, ...). |
| version | `{logical}@{ts}`: committed value or tombstone. |
| intent | `{logical}@INTENT`: at most one per logical key; owned by one txn. Sorts before all versions. |
| `seq` | Per-txn command counter (PostgreSQL command id). Incremented per statement, and per savepoint. |
| latch | Short striped mutex over a hash of the logical key, held only for check-and-place. Never held across I/O waits on other txns. |

## 2. Data

Intent value:
```
Intent { txn: TxnId, kind: Write(bytes) | Delete | LockOnly(RowLockMode),
         seq: u32, history: Vec<(seq, Write(bytes) | Delete | Absent)> }
```
`history` holds earlier own values of the same key in the same txn, newest last. `Absent` = "no own value before this seq; fall through to versions".

Status, persisted under `/sys/txn/{TxnId}`: only `Committed(ts)` is persisted. `Pending` and `Aborted` are in-memory only (§7 explains why that is safe).

`/sys/epoch`: u32, incremented and synced at boot before any txn starts.
`/sys/ts_hwm`: sequencer high-water mark, reserved in blocks (§3).

## 3. Commit pipeline and visibility

One **commit thread** owns the sequencer. Txns submit a commit request (status record, optional `/log` record, sync mode) on a channel. The thread:

1. Drains a group. Assigns `commit_ts` in queue order. If `commit_ts` would exceed `ts_hwm`, first writes `/sys/ts_hwm += BLOCK` in the same batch.
2. Writes the group's records to the KV in `commit_ts` order (one write per request or one batch per group; WAL order must equal `commit_ts` order).
3. For the prefix of `synchronous_commit=off` requests up to the first `on` request: set in-memory status `Committed(ts)`, then advance `visible_ts`, then ack.
4. fsync (never on a tokio worker). Then, for the rest of the group in order: set status, advance `visible_ts`, ack.
5. Wake every waiter on each committed txn (§5).

Failed KV write: the thread stops and the process aborts (fail-stop). It never fills a ts hole by skipping.

Boot: `next_ts = ts_hwm + 1` (anything up to `ts_hwm` may have been handed out).

**I-WAL-ORDER.** KV WAL order of commit records equals `commit_ts` order, and every intent write of a txn precedes its commit record in the WAL. Consequence: a durable commit implies its intents and every earlier commit are durable (crash loses a suffix only).
**I-VIS.** In-memory status of T is `Committed(ts)` before `visible_ts` reaches `ts`.
**I-ACK.** A commit is acknowledged only when `visible_ts >= commit_ts` (read-your-commits across sessions) and, if `synchronous_commit=on`, after fsync.
**I-SNAP-ORDER.** A reader reads `S = visible_ts` first, then opens its KV snapshot. Never the other way round.

Why that is enough: for any committed T with `commit_ts <= S`, T's intents were written before its commit record, which was written before `visible_ts` passed `commit_ts`, which happened before `S` was read, which happened before the KV snapshot opened. So the reader's KV snapshot contains T's writes (as intents or resolved versions).

Backend requirement (disqualifier for C-S1): a single WAL across all keyspaces, sequential, so a sync covers every earlier write.

## 4. Read path (readers never block)

To read logical key `k` at snapshot `S` with statement start `seq0`, inside txn `R`, from KV snapshot `V`:

1. If `k@INTENT` exists in `V` with owner `T`:
   - `T == R`: use the newest of `{intent.kind if intent.seq < seq0} ∪ {history entries with seq < seq0}`. `LockOnly` and `Absent` fall through to step 2. `Write(v)` returns `v`; `Delete` returns not-found.
   - `status(T) == Committed(c)` and `c <= S`: the intent is the newest visible version (`LockOnly` falls through).
   - otherwise (Pending, Aborted, or `c > S`): invisible, fall through. Under SERIALIZABLE, record an rw-antidependency `R -> T` (§8).
2. The first version `k@t` in `V` with `t <= S`. Tombstone = not-found. A version with `t > S` skipped under SERIALIZABLE records `R -> writer(t)` (§8).

Status lookup never blocks and never misses (I-TRUNC, §7).

**I-HALLOWEEN.** A statement never sees its own writes with `seq >= seq0`.

Every scan uses one KV snapshot for its whole duration. Statements in RC take a fresh `S` and `V` per statement; RR/SER take one `S` per txn (a new `V` per statement is fine because versions `<= S` never change, except by GC, which respects §9).

## 5. Write path (all levels)

To write `k` (insert, update, delete, or exclusive row lock) as txn `W` with snapshot `S` and current `seq`:

```
loop:
  latch(k)
  read k@INTENT and the newest version k@t from the LATEST KV state (not V)
  if intent owned by T != W:
     if status(T) == Pending:
        unlatch; wait_for(W -> T)   # §6; may raise 40P01 / 55P03 / lock_timeout
        continue
     if status(T) == Committed(c): treat as version k@c (resolve inline if cheap)
     if status(T) == Aborted: ignore (delete inline if cheap)
  check shared locks on k held by others that conflict with the requested mode (§6);
     if any: unlatch; wait; continue
  newest committed version ts = t*
  if t* > S:
     RR/SER: unlatch; raise 40001
     RC: EPQ (§5.1)
  conflict-specific checks (unique §5.2)
  place or replace own intent (push old own kind into history if old.seq < seq)
  unlatch
  SSI: check SIREAD locks covering k (§8)   # after placement, see I-SSI-ORDER
```

**I-ONE-INTENT.** At most one intent per logical key. Guaranteed because placement happens only under the latch after observing no foreign Pending intent.
**I-WW.** Two txns never both commit a write to `k` where each started its snapshot before the other's commit (RR/SER). RC instead serialises through EPQ.

### 5.1 RC EvalPlanQual
After a wait or when `t* > S`: re-read the newest committed version, re-evaluate the statement's quals for this row against it (PostgreSQL EvalPlanQual). Fail: skip the row. Pass: apply the update computed from the newest version. Joined DML (UPDATE FROM, DELETE USING, MERGE): full EPQ, or an internal statement retry with a fresh snapshot (capped retries, then 40001). Which one is decided by G3c and recorded here.

### 5.2 Unique and FK
- Unique index entry lives at `/u/{idx}/{key}`. An insert or key-changing update writes it through the §5 loop with the latch on `/u/{idx}/{key}`. After the wait loop, if the newest committed version is live and points to a different row, raise 23505. Under SERIALIZABLE, if that version's ts > S, raise 40001 instead (PostgreSQL ≥ 9.6 behaviour). G3c confirms the codes.
- Concurrent duplicate insert: the second inserter sees the first's Pending intent on `/u/...` and waits. First commits: second re-checks and gets 23505. First aborts: second proceeds. **I-UNIQUE**: no two live versions of one `/u/` key at any snapshot.
- Deferrable unique constraints use the non-unique `/i/` layout plus a commit-phase check under the same latch.
- NULL in any key column: the entry goes to `/i/` with pk appended (no uniqueness).
- FK child insert/update: takes KEY SHARE on the parent's logical key (§6), then reads the parent at the latest committed state; not found raises 23503. Parent delete or key change takes UPDATE mode, which conflicts with KEY SHARE.

### 5.3 Savepoints
The txn keeps a write-set log `(seq, logical key)` (spills to disk past a threshold). `ROLLBACK TO SAVEPOINT` at seq `s`: for each key written at seq ≥ `s`, under latch, drop history entries and the current kind with seq ≥ `s`; restore the newest remaining one; if none remains, delete the intent. Shared locks taken after `s` are released. Waiters on this txn are not woken (it is still Pending), except lock waiters on keys whose intent was deleted.

History compaction: an entry may be dropped when no live statement start (current statement, open cursors, portals) and no live savepoint boundary falls between it and the next entry.

## 6. Locks and waiting
- Exclusive row modes (NO KEY UPDATE, UPDATE) are materialised as intents (`LockOnly(mode)` when there is no data change).
- Shared modes (KEY SHARE, SHARE) live in an in-memory table keyed by logical key, checked under the same latch. Safe because a crash aborts every holder.
- Conflict matrix: PostgreSQL's row-lock matrix. A plain UPDATE that changes no key column takes NO KEY UPDATE; one that changes a key column, or a DELETE, takes UPDATE.
- Waiting is on a txn, not a key: `wait_for(W -> T)` parks W until T commits or aborts, then W re-runs the §5 loop.
- NOWAIT: 55P03 instead of waiting. SKIP LOCKED: skip the row. `lock_timeout`: 55P03 on expiry.
- Deadlock: after `deadlock_timeout`, the waiter runs a DFS over the wait-for graph; if it finds a cycle containing itself, it aborts with 40P01. No wait-die.
- Relation locks (AccessShare … AccessExclusive) are a separate in-memory table, held to txn end. After acquiring one, catalog lookups use the latest committed catalog. Under RR/SER, if the relation's schema version is newer than `S`, raise 40001.
- Advisory locks: in-memory, session or xact scope.

## 7. Abort, crash, resolution, status truncation
- **Abort.** Set status `Aborted` in memory, wake waiters, release shared locks, queue an async batch deleting the txn's intents. Nothing is persisted.
- **Crash.** On boot, `epoch += 1` (synced). Load every persisted `Committed` record. For an intent whose owner has `owner.epoch < current` and no `Committed` record: Aborted. That is O(1) per lookup; cleanup is lazy (on encounter under latch, or by the C-B1 sweep).
- **Resolution** of a committed txn T (async, C-B1, plus on encounter): one KV batch per key range: delete `k@INTENT`, put `k@commit_ts` (Write/Delete), or just delete for LockOnly. `intent_count(T) -= n`.
- **Truncation.** The status entry of T may be removed (in memory, and `/sys/txn/T` deleted) only when:
  1. `intent_count(T) == 0` (after a crash the count is unknown: committed records from older epochs stay until one full intent sweep has finished), and
  2. every KV snapshot that was open when T's last resolution batch was written has closed (track a monotonic snapshot-open counter; record the counter at last resolution; truncate when the minimum open counter is greater).

**I-TRUNC.** No reader can observe an intent of T after T's status entry is gone. Condition 2 closes the race where a reader's KV snapshot predates resolution.
The `/sys/txn/T` delete is written after the resolution batches, so by I-WAL-ORDER a crash can never keep the delete and lose the resolution.

## 8. SSI (SERIALIZABLE only)
Algorithm: PostgreSQL's (Cahill, Ports & Grittner) with the commit-ordering refinements.
- **SIREAD** locks are on the KV key ranges actually iterated, gaps included: a scan of `[a, b)` locks `[a, b)`, not just the keys returned. Point reads lock the logical key. Escalation: range → index/table prefix → relation, past `max_pred_locks_per_*`. ANN, FTS, columnar, GIN and graph access paths take relation-level SIREAD.
- **I-SSI-ORDER.** A reader registers its SIREAD range before iterating; a writer checks SIREADs after placing its intent. So for any concurrent reader/writer pair, either the reader's iteration sees the intent (records `R -> W`) or the writer sees the SIREAD (records `R -> W`).
- Reads that skip a version `> S` or a foreign intent record `R -> writer` (§4).
- Dangerous structure `T1 -> T2 -> T3` with T3 committed first: abort with 40001, preferring the txn that has not committed and is not read-only-safe.
- SIREADs and rw-conflict info of a committed txn are kept until every txn concurrent with it has ended.
- Read-only txns: safe-snapshot optimisation and `DEFERRABLE`.

## 9. GC
Watermark `W` = min over: active txn snapshots, open cursors/portals, in-progress segment builds (`built_at`), the `AS OF` window, `now_ts - max_snapshot_age`. Exported as a metric.

Rules:
- Keep, for every logical key, every version `> W` and the newest version `<= W`. Intents are never GC'd.
- **A tombstone may be removed only together with every older version of its key.** With timestamps inside the key (`pk@ts` are distinct KV keys), versions of one logical key can span SST files, so a compaction filter that drops a tombstone can resurrect an older version sitting in another file. Therefore:
  - **Backend with user-defined timestamps (RocksDB UDT + `full_history_ts_low = W`):** the engine owns version GC and keeps one user key's versions together. Preferred if C-S1 confirms UDT works with DeleteRange, iterators at a read ts, and checkpoints.
  - **Otherwise (ts in key: fjall, RocksDB without UDT):** the compaction filter may drop a non-tombstone version only when it has already seen, earlier in the same compaction stream, a newer version `<= W` of the same logical key. It never drops tombstones. The C-B1 GC job removes a tombstone `k@t` (`t <= W`, newest `<= W`) by `DeleteRange(k@t .. k@0]` inclusive of the tombstone, which the engine applies correctly across levels.
- `DROP`/`TRUNCATE`: `DeleteRange` of the relation prefix once `W >` drop ts.

**I-GC.** For every snapshot `S >= W`, the read path (§4) returns the same result before and after any GC step.

Unresolved committed intents are safe: GC only drops versions older than a kept version, and the intent's eventual version is newer than all of them.

## 10. Other rules
- **Sequences:** non-transactional; the high-water mark is persisted (synced) before a value from a new block is handed out. Values may skip, never repeat.
- **Catalog:** rows in `/sys/catalog`, versioned like any table; DDL is transactional and takes AccessExclusive.
- **Large txns:** intents, write-set log and SIREAD state all spill; the 10M-row gate measures commit latency, reader latency during resolution and abort cleanup time.

## 11. Invariants checked by G0
`I-WAL-ORDER`, `I-VIS`, `I-ACK`, `I-SNAP-ORDER`, `I-HALLOWEEN`, `I-ONE-INTENT`, `I-WW`, `I-UNIQUE`, `I-TRUNC`, `I-SSI-ORDER`, `I-GC`, plus:
- **I-ATOMIC.** After any crash, for every txn: all of its writes are visible at `visible_ts`, or none are.
- **I-DURABLE.** An acked `synchronous_commit=on` commit survives any crash.
- **I-SER.** Histories under SERIALIZABLE are serializable (Elle, G2), including predicate/phantom workloads.
- **I-NOBLOCK.** No read-only path (§4) waits on another txn.

The G0 model is a small-scope executable model (`stateright` or a hand-rolled exhaustive explorer) over MemKv with 2–3 txns, 2–3 keys, crash points at every KV write and fsync, and GC steps interleaved. It must find each of these seeded bugs: reader opens V before reading `S`; status truncated without condition 2; tombstone dropped by the compaction filter; intent placed without the latch; SIREAD registered after iterating.

## 12. Open questions (resolve before C-T2 merges)
1. RC joined DML: full EPQ vs statement retry (G3c decides).
2. Exact SQLSTATE for unique conflicts under RR vs SERIALIZABLE (G3c against PG17).
3. RocksDB UDT viability (C-S1); otherwise the ts-in-key GC path is mandatory.
4. Whether `synchronous_commit=off` commits may be visible to `on` sessions before fsync (PostgreSQL: yes). Current spec: yes.
