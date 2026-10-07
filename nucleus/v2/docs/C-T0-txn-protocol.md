# C-T0: Transaction protocol (normative)

Status: draft 2 (round-1 adversarial review applied; changelog at the bottom). Every `nucleus-txn` card implements against this file. A change to an invariant (`I-*`) needs a spec change first, then the G0 model updated, then code.

Scope: single node. Isolation levels: RC (PostgreSQL semantics), RR = SI, SERIALIZABLE = SSI. 2PC (`PREPARE TRANSACTION`) is refused with 0A000 in 2.0. Target behaviour is PostgreSQL 17; every deliberate divergence is listed in §12.

## 1. Vocabulary

| Term | Meaning |
|---|---|
| `Ts` | u64 commit timestamp from the sequencer. Wrapped in a struct so it can widen to an HLC later. `Ts::ZERO` is never a commit ts. |
| `TxnId` | `(epoch: u32, n: u64)`. `epoch` = boot epoch. `n` dense per epoch. |
| `S` | A snapshot ts. Snapshot visibility: committed with `commit_ts <= S`. |
| `visible_ts` | Largest ts such that every commit `<= ts` is applied (§3). |
| logical key | KV key without the version suffix (`/t/{rel}/{pk}`, `/u/{idx}/{key}`, ...). `{rel}` and `{idx}` are **storage ids** (§10), never catalog oids. |
| version | `{logical}@{ts}`: committed value, tombstone, or moved-tombstone (§5.1). **Versions of one logical key sort newest first (descending ts).** |
| intent | `{logical}@INTENT`: at most one per logical key; owned by one txn. Sorts before all versions of its key. |
| `seq` | Per-txn command counter (PostgreSQL command id). Incremented per statement and per savepoint. **Never decreases**, including across `ROLLBACK TO`. |
| view | A KV snapshot (`OrderedKv::snapshot`) or the latest state read under a latch. |
| latch | Short striped mutex over a hash of a latch key (normally the logical key; §5.2 names the exceptions). Held only for check-and-place and for intent removal. Never held across waits on other txns. |
| key column | A column of any non-partial, non-expression unique index of the relation (PostgreSQL's definition for `FOR KEY SHARE` / `NO KEY UPDATE`). |

## 2. Data

### 2.1 Intent
```
Intent { txn: TxnId, layers: Vec<Layer> }          # oldest first, never empty
Layer  { seq: Seq, data: Write(bytes) | Delete | Absent, lock: RowLockMode }
```
Each layer is the **complete own state** of the key as of `seq`: `data` is the txn's own value (`Absent` = no own value; reads fall through to versions), `lock` is the strongest exclusive mode held (`NoKeyUpdate` or `Update`). The top layer is the current state.

- A data write implies a lock: `Delete`, or a `Write` that changes a key column → `Update`; any other `Write` → `NoKeyUpdate`. The layer's `lock` is the max of the implied mode, the previous layer's lock, and any explicit lock request.
- A lock-only request (`SELECT ... FOR UPDATE / NO KEY UPDATE`) copies the previous layer's `data` and raises `lock`. **It never replaces own data.** (A first lock-only request on a key creates a layer with `data: Absent`.)
- New state at command `seq`: if top.seq == seq, modify the top layer in place; otherwise push a new layer.
- Shared modes (KEY SHARE, SHARE) are never stored in intents (§6).

Foreign readers and resolution look only at the **top layer's `data`**. `Absent` means "lock only": no version is produced, nothing becomes visible.

### 2.2 Version key layout (ts-in-key backends)
`L` = the encoded logical key, prefix-free by C-Q3s P-PREFIX, so no other logical key starts with `L`.
```
intent        L ‖ 0x00
version @ts   L ‖ 0x01 ‖ be64(u64::MAX - ts)      # newest first
end(L)        L ‖ 0x02                            # exclusive upper bound of every entry of L
```
`[L ‖ 0x00, end(L))` holds exactly the intent and versions of `L`. The GC range for tombstone `L@t` is `[L ‖ 0x01 ‖ be64(u64::MAX - t), end(L))`. Backends with user-defined timestamps store `L` as the user key, the ts in the engine's timestamp, and the intent as `L ‖ 0x00` with no timestamp.

### 2.3 Persisted system keys
- `/sys/txn/{TxnId}`: only `Committed(ts)` is persisted. `Pending` and `Aborted` are in-memory only (§7).
- `/sys/epoch`: u32, incremented and synced at boot before any txn starts.
- `/sys/ts_hwm`: sequencer high-water mark, reserved in blocks (§3).
- `/sys/gc_w`: the GC watermark `W` (§9). Monotonic.
- `/sys/ts_clock`: sparse `(wall_time, ts)` samples written by the commit thread (at most one per second) for converting wall-time windows to ts (§9).

## 3. Commit pipeline and visibility

One **commit thread** owns the sequencer. Txns submit a commit request (status record, optional `/log` record, sync mode) on a channel. Requests are processed strictly in channel order. The thread:

1. Drains a group. Assigns `commit_ts` in channel order. If `commit_ts` would exceed `ts_hwm`, first writes `/sys/ts_hwm += BLOCK` in the same batch.
2. Writes the group's records to the KV in `commit_ts` order (one write per request or one batch per group; WAL order must equal `commit_ts` order).
3. For the prefix of `synchronous_commit=off` requests up to the first `on` request: set in-memory status `Committed(ts)`, then advance `visible_ts`, then ack.
4. fsync (never on a tokio worker). Then, for the rest of the group in order: set status, advance `visible_ts`, ack.
5. Wake every waiter on each committed txn (§6). Waking happens after `visible_ts >= commit_ts`.

The commit thread never aborts a txn. Once a request is on the channel the txn will commit unless the process dies (SSI decisions are made before enqueueing, §8). Failed KV write: the thread stops and the process aborts (fail-stop). It never fills a ts hole by skipping.

Boot: `next_ts = ts_hwm + 1` (anything up to `ts_hwm` may have been handed out).

### 3.1 Snapshot and view registration
A **registry** (one mutex) holds every live snapshot `S` and every open view's counter.

- **Taking a snapshot:** under the registry mutex, read `S = visible_ts` and insert `S` into the registry. One step; nothing can compute `W` or retire SSI state between the read and the insert.
- **Opening a view:** under the registry mutex, take `c = ++view_counter` and register `c`; release the mutex; then open the KV snapshot/iterator. Unregister `c` when the view closes.
- A txn's snapshot stays registered until the txn ends (RR/SER) or until its statement ends (RC). An open cursor or portal keeps its snapshot registered until it closes.

**I-WAL-ORDER.** KV WAL order of commit records equals `commit_ts` order, and every intent write of a txn precedes its commit record in the WAL. Consequence: a durable commit implies its intents and every earlier commit are durable (crash loses a suffix only).
**I-VIS.** In-memory status of T is `Committed(ts)` before `visible_ts` reaches `ts`.
**I-ACK.** A commit is acknowledged only when `visible_ts >= commit_ts` (read-your-commits across sessions) and, if `synchronous_commit=on`, after fsync.
**I-SNAP-ORDER.** Snapshot taken (`S` read and registered atomically) → view counter registered → KV view opened. Never another order.

Why that is enough: for any committed T with `commit_ts <= S`, T's intents were written before its commit record, which was written before `visible_ts` passed `commit_ts`, which happened before `S` was read, which happened before the view opened. So the view contains T's writes (as intents or resolved versions).

**Commit-visibility window.** Between steps 3/4 setting `Committed(c)` and `visible_ts` reaching `c`, T is committed but not yet visible to new snapshots. Every writer-side decision (§5, §5.2, §6) treats `Committed(c)` with `c > visible_ts` exactly like `Pending`: it waits, and is woken at step 5. Readers need no rule: `S <= visible_ts < c`.

Backend requirement (disqualifier for C-S1): a single WAL across all keyspaces, sequential, so a sync covers every earlier write.

## 4. Read path (readers never block)

To read logical key `k` at snapshot `S` with statement start `seq0`, inside txn `R`, from view `V`:

1. If `k@INTENT` exists in `V` with owner `T`:
   - `T == R`: take the newest layer with `seq < seq0`. If it exists and its `data` is `Write(v)` → `v`; `Delete` → not-found; `Absent`, or no such layer → step 2.
   - `T != R`, top layer `data == Absent` (lock only): step 2. No SSI edge.
   - `T != R`, `status(T) == Committed(c)` and `c <= S`: the top layer's data is the newest visible version.
   - otherwise (Pending, Aborted, or `c > S`): invisible, step 2. Under SERIALIZABLE, record rw-antidependency `R -> T` (§8).
2. The first version `k@t` in `V` with `t <= S`. Tombstone or moved-tombstone = not-found. Each version with `t > S` skipped under SERIALIZABLE records `R -> writer(t)` (§8.5).

Status lookup never blocks and never misses (I-TRUNC, §7). A missing status for a txn of the **current** epoch is a fatal invariant violation (process aborts), never defaulted. A missing status for an older-epoch txn means Aborted.

**I-HALLOWEEN.** A statement never sees its own writes with `seq >= seq0`.

Views: a scan uses one view for its whole duration. RC takes a fresh `S` per statement; RR/SER take one `S` per txn. Versions `<= S` never change except by GC, which respects §9, so several views per statement are allowed. Under SERIALIZABLE every scan opens its **own** view after registering its SIREAD range (§8.1); RC and RR may share one view per statement.

Reads at an explicit ts (`AS OF t`): `t` must be `>= W` and `<= visible_ts`; otherwise error 72000 (`snapshot_too_old`) and no data. The read registers `t` like a snapshot (§3.1), with the `>= W` check made under the registry mutex.

## 5. Write path (all levels)

Operations on a key `k` by txn `W` at snapshot `S`, current command `seq`, statement start `seq0`:

- **Row ops** act on a row the statement found at `S`: UPDATE, DELETE, exclusive row lock (`FOR UPDATE`, `FOR NO KEY UPDATE`), shared row lock (`FOR SHARE`, `FOR KEY SHARE`, §6).
- **Key-existence ops** create a key that must not be live: INSERT of `/t/{rel}/{pk}`, a PK-changing UPDATE's new `/t/` key, and unique entries `/u/{idx}/{key}`.

```
loop:
  latch(k)
  read k@INTENT and the newest DATA version k@t from ONE fresh view of the latest state
  if intent owned by T != W:
     st = status(T)
     if st == Pending or (st == Committed(c) and c > visible_ts):
        unlatch; wait_for(W -> T)        # §6; may raise 40P01 / 55P03 / lock_timeout
        continue
     if st == Committed(c):              # MANDATORY inline resolution (§7.3), joins the placement batch
        if T's top data is Write/Delete: put k@c; the newest data version is now k@c
        delete k@INTENT; intent_count(T) -= 1
     if st == Aborted:                   # MANDATORY removal, joins the placement batch
        delete k@INTENT; intent_count(T) -= 1
  if shared row locks on k held by others conflict with the requested mode (§6):
     unlatch; wait on every conflicting holder; continue
  if own intent top layer has seq == seq0 and this is a row op visiting the row again (§5.4): handle per §5.4
  row op:            t* = ts of the newest DATA version (committed lock-only intents are not versions)
                     if t* > S:
                        RR/SER: unlatch; raise 40001 (KEY SHARE: only if that version is a delete,
                                moved-tombstone or key-changing write; otherwise proceed)
                        RC: EPQ (§5.1)
  key-existence op:  unique check (§5.2); never 40001 from t* alone, never EPQ
  build the new top layer (§2.1); write {inline resolution ops, k@INTENT} as one KV batch
  unlatch
  SSI: if the new layer changed data (not lock-only), check SIREAD locks covering k (§8.2)
```

Shared row locks (KEY SHARE, SHARE) go through the same loop, including the row-op `t* > S` check, but record the lock in the in-memory table (§6) instead of writing an intent.

**I-ONE-INTENT.** At most one intent per logical key. Guaranteed because placement happens only under the latch, after removing or waiting out every foreign intent, in the same batch as the removal.
**I-WW.** Under RR/SER, a row op never succeeds on a row whose newest data version is newer than `S`. RC instead serialises row ops through EPQ. Key-existence ops are governed by I-UNIQUE, not I-WW: inserting over a tombstone newer than `S` is allowed (PostgreSQL behaviour).

### 5.1 RC EvalPlanQual
After a wait or when `t* > S`: re-read the newest committed data version, re-evaluate the statement's quals for this row against it (PostgreSQL EvalPlanQual). Fail: skip the row. Pass: apply the update computed from the newest version. Joined DML (UPDATE FROM, DELETE USING, MERGE): full EPQ, or an internal statement retry with a fresh snapshot (capped retries, then 40001). Which one is decided by G3c and recorded here.

**Moved rows.** An UPDATE that changes the primary key writes, at the old `/t/` key, a **moved-tombstone** (a tombstone with a flag; readers treat it as not-found). EPQ that reaches a moved-tombstone raises 40001 ("tuple to be locked was already moved"). Under RR/SER the row op already raises 40001. Divergence from PostgreSQL, which follows the update chain: §12.

### 5.2 Unique, primary key and FK
**Unique check** (key-existence ops, under the latch on the key): the key's **current state** is the own intent's top-layer data if `W` has an intent on it (any seq, the current statement included), otherwise the newest committed data version.
- Live and belonging to a different row → 23505. If the conflicting live entry was written by `W` in the **current** statement under `INSERT ... ON CONFLICT DO UPDATE` or `MERGE`, raise 21000 instead.
- Under SERIALIZABLE, if that live version has ts `> S` and `W` holds a SIREAD covering the key, raise 40001 instead of 23505 (PostgreSQL ≥ 9.6). Otherwise 23505.
- Not live (absent, tombstone, own `Delete`) → proceed, whatever its ts.
- Concurrent duplicate insert: the second inserter meets the first's Pending intent and waits (§5 loop). First commits: the second re-runs and gets 23505. First aborts: the second proceeds.

**I-UNIQUE.** No two live versions, or a live version and a foreign committed live intent, of one `/u/` or `/t/` key at any snapshot, and no snapshot sees two live rows with equal values of a unique key.

- **Primary key:** `/t/{rel}/{pk}` inserts and PK-changing updates run the same unique check on the new `/t/` key.
- **NULLs:** `NULLS DISTINCT` (default): if any key column is NULL, the entry goes to `/i/{idx}/{key}{pk}` (no uniqueness). `NULLS NOT DISTINCT`: NULL is encoded in the `/u/` key and checked like any value.
- **Deferrable unique constraints** use the non-unique `/i/` layout. The check runs in the commit phase, **before the commit request is enqueued**, under `latch(/i/{idx}/{key})`, which is the prefix of all entries with that key value, not a per-row key. It scans the prefix in the latest state: a foreign Pending (or committed-not-visible) entry → wait on its owner, then re-check; a committed live entry of another row, or a second own live entry → 23505. Every insert of an `/i/` entry for a deferrable constraint takes the same prefix latch.

**Foreign keys** are checked at end of statement (after all of the statement's writes), as PostgreSQL's RI triggers are. Own writes of the current statement count (`seq <= current`).
- **Child side** (insert or FK-changing update of a child): take KEY SHARE on the parent's `/t/` key through the §5 loop (waits on pending parent writers; RR/SER raises 40001 if the parent's newest data version is newer than `S` and is a delete, moved-tombstone or key change). Then read the parent:
  - RC: latest committed state plus own writes.
  - RR/SER: at `S` plus own writes. Under SERIALIZABLE also register a SIREAD on the parent key.
  - Not found → 23503.
- **Parent side** (delete, or key change of a referenced key, NO ACTION/RESTRICT): the delete/update already holds `Update` on the parent (conflicts with KEY SHARE, so pending child inserters wait). At end of statement scan the child index for the old key in the latest committed state plus own writes. A live child → 23503. Under RR/SER, a child live in the latest state but invisible at `S` → 40001 (PostgreSQL `detectNewRows`).
- Referential actions (CASCADE, SET NULL, SET DEFAULT) run as ordinary DML through §5.

### 5.3 Savepoints and cursors
The txn keeps a write-set log `(seq, logical key)` (spills to disk past a threshold).

`ROLLBACK TO SAVEPOINT` at seq `s`:
- For each key written at seq `>= s`: under the latch, re-read `k@INTENT`; if owned by the txn, drop layers with `seq >= s`. If no layer remains, delete the intent (`intent_count -= 1`). The restored top layer restores both data and lock.
- Shared row locks (KEY SHARE, SHARE) taken at seq `>= s` are released.
- **SIREAD locks and recorded rw-conflicts are kept** (the client has seen the data; PostgreSQL keeps them too).
- Portals and cursors opened at seq `>= s` are closed.
- `seq` does not go back: the next command gets a seq greater than every seq used so far.
- **All** waiters on the txn are woken. They re-run §5; spurious wakeups are harmless.

Writes and locks are tagged with the seq current **when they execute**, not with the opening seq of the portal that runs them. A `FOR UPDATE` cursor fetched after `SAVEPOINT s` takes locks at seq `>= s`, so `ROLLBACK TO s` releases them.

**Layer compaction.** Live boundaries are: the current statement's start, every open cursor/portal start, and every live savepoint seq. Layer `i` (with next layer `i+1`) is needed iff some live boundary `r` has `layers[i].seq < r <= layers[i+1].seq`. The top layer is always needed. Others may be dropped.

### 5.4 Revisiting an own row in one statement
If a row op finds the own intent's top layer with `seq == seq0` (the row was already changed by this statement):
- UPDATE / DELETE: skip the row (PostgreSQL `TM_SelfModified`): not counted, no change.
- `INSERT ... ON CONFLICT DO UPDATE`, `MERGE`: raise 21000 ("command cannot affect row a second time").
- Locks: no-op.

## 6. Locks and waiting
- Exclusive row modes (NO KEY UPDATE, UPDATE) live in intent layers (§2.1).
- Shared modes (KEY SHARE, SHARE) live in an in-memory table keyed by logical key, tagged with the acquiring seq, and checked under the same latch. Safe because a crash aborts every holder.
- Conflict matrix: PostgreSQL's row-lock matrix, applied to (requested mode, held mode). An intent holds its top layer's `lock`.
- **Wait-for graph.** One graph covers every kind of wait: row intents, shared row locks (an edge to **every** conflicting holder), relation locks, advisory locks, deferrable-unique prefix waits.
- **Waiting.** `wait_for(W -> T)` = (1) insert the edge into the graph and register W as a waiter on T, (2) re-read `status(T)`; if T is no longer Pending (or is committed and visible), remove the edge and return immediately, (3) park. Commit (§3 step 5), abort (§7.1) and `ROLLBACK TO` (§5.3) wake every registered waiter of T. A woken waiter removes its edges and re-runs §5.
- NOWAIT: 55P03 instead of waiting. SKIP LOCKED: skip the row. `lock_timeout`: 55P03 on expiry.
- **Deadlock.** After `deadlock_timeout`, the waiter takes the graph mutex, runs a DFS, and if it finds a cycle containing itself, removes its own edges before releasing the mutex, then raises 40P01. Exactly one member of a cycle is aborted. No wait-die.
- Relation locks (AccessShare … AccessExclusive) are a separate in-memory table, held to txn end. After acquiring one, catalog lookups use the latest committed catalog. Under RR/SER, if the relation's schema version is newer than `S`, raise 40001.
- Advisory locks: in-memory, session or xact scope.

## 7. Abort, crash, resolution, status truncation

### 7.1 Abort
Set status `Aborted` in memory, wake waiters, release shared locks, queue an async cleanup of the txn's intents (§7.3 rules). Nothing is persisted. SSI state follows §8.6.

### 7.2 Crash
On boot, `epoch += 1` (synced). Load every persisted `Committed` record and `/sys/gc_w`. An intent whose owner has `owner.epoch < current` and no `Committed` record is Aborted. That is O(1) per lookup; cleanup is lazy (on encounter in §5, or by the C-B1 sweep).

### 7.3 Intent removal (every path)
Paths that remove an intent: inline resolution or removal in §5, async resolution (C-B1), abort cleanup, the boot sweep, savepoint rollback. **Every one of them:**
1. takes `latch(k)`,
2. re-reads `k@INTENT` from the latest state, and acts only if it is still owned by the expected txn (and, for savepoint rollback, the expected layers are present),
3. writes its ops as one KV batch: `delete k@INTENT`, plus `put k@commit_ts` with the top layer's data when resolving a committed intent whose data is `Write` or `Delete` (a `Delete` becomes a tombstone version; `Absent` produces no version),
4. decrements `intent_count(T)` only if it removed an intent of T,
5. after the batch write returns, sets `last_removal_counter(T) = view_counter` (read under the registry mutex).

There is no conditional delete in the KV; steps 1-2 replace it. A removal batch that finds the intent gone or re-owned does nothing.

### 7.4 Status truncation
The status entry of T may be removed (in memory, and `/sys/txn/T` deleted) only when:
1. `intent_count(T) == 0`. After a crash the count is unknown: committed records from older epochs stay until one full intent sweep has finished.
2. Every view open when T's last intent was removed has closed: `min(registered view counters) > last_removal_counter(T)`, computed under the registry mutex.

**I-TRUNC.** No reader can observe an intent of T after T's status entry is gone. Condition 2 works because a view registers its counter before opening (§3.1) and the counter is recorded after the removal batch is written (§7.3): a view that could contain T's intent has a counter `<= last_removal_counter(T)`.
The `/sys/txn/T` delete is written after the removal batches, so by I-WAL-ORDER a crash can never keep the delete and lose the resolution.

SSI does not use the status table to map timestamps to txns (§8.5), so status truncation never loses an SSI edge.

## 8. SSI (SERIALIZABLE only)
Algorithm: PostgreSQL's (Cahill, Ports & Grittner) with its commit-ordering refinements. All SSI state is guarded by one **SSI mutex** (may be striped later if G0 is extended to cover it).

### 8.1 SIREAD locks
- SIREADs are on the KV key ranges a scan may iterate, gaps included: a scan of `[a, b)` locks `[a, b)`. Point reads lock the logical key. FK reads lock the parent key (§5.2).
- **I-SSI-ORDER.** A scan registers its SIREAD over the full bound **before opening the view it iterates**. It may shrink the lock to the range actually iterated afterwards, never before.
- Escalation (range → index/table prefix → relation, past `max_pred_locks_per_*`) adds the coarse lock before removing the fine ones. ANN, FTS, columnar, GIN and graph access paths take relation-level SIREAD.
- SIREADs and recorded conflicts survive `ROLLBACK TO SAVEPOINT` (§5.3).

### 8.2 Edges
- Writer side: after placing a data-changing intent (§5), check SIREADs covering `k` held by other SER txns; each gives `reader -> W`. Lock-only placements and shared locks do not check SIREADs.
- Reader side: §4 skipped foreign intents (`R -> owner`) and skipped versions `> S` (`R -> writer(t)`, §8.5).
- Edges to or from a txn that is not SERIALIZABLE are ignored.

Correctness of I-SSI-ORDER: for a concurrent reader R and writer W on `k`, either W's intent placement precedes R's view opening (R's view contains the intent or its resolved version `> S`, so R records the edge), or it follows it, and then it follows R's SIREAD registration, so W's SIREAD check sees it.

### 8.3 Dangerous structure
`T1 -> T2 -> T3` (rw-antidependencies; `T1 == T3` allowed) where T3 commits first. "Commits first" is decided by `commit_ts` for committed txns and by prepare order (§8.4) for prepared ones.
- **Read-only exception:** if T1 is declared `READ ONLY` (or has done no writes and is committing), the structure is dangerous only if `commit_ts(T3) <= S(T1)`.
- Victim: a txn that is not prepared, preferring T2 (the pivot) if it is not prepared, otherwise T1. If the only candidate is the txn running the check, it aborts itself with 40001.

### 8.4 Pre-commit
A SER txn commits by, **in one critical section under the SSI mutex**: run the dangerous-structure check on its own edges; if it passes, mark itself `PREPARED` with `prepare_seq = ++prepare_counter`; enqueue its commit request on the commit thread's channel (non-blocking send). Because enqueue happens under the mutex and the commit thread assigns `commit_ts` in channel order (§3), commit order equals prepare order. A prepared txn can no longer be chosen as a victim. A txn doomed by another checker raises 40001 at its next statement or at commit.

Non-SER txns enqueue without taking the SSI mutex.

### 8.5 Finding `writer(t)`
SSI keeps its own map `commit_ts -> (TxnId, conflict-out summary)` for every SERIALIZABLE txn whose `commit_ts` is greater than the oldest registered SER snapshot. This is independent of the status table (§7.4) and corresponds to PostgreSQL's `OldCommittedSxact` summary. A `t` with no entry was written by a non-SER txn, or by a SER txn older than every live SER snapshot; neither can form a dangerous structure with the reader, so the edge is dropped.

### 8.6 Retention and abort
- The SIREADs and conflict info of a committed SER txn T are kept until every SER txn whose snapshot `S < commit_ts(T)` has ended. "Every" is evaluated against the registry (§3.1), where taking `S` and registering it are one step.
- An aborted txn's SIREADs and edges are removed when it aborts.
- Read-only txns: safe-snapshot optimisation and `DEFERRABLE`.

## 9. GC

### 9.1 Watermark
`W` = min over: registered snapshots (§3.1, txns, cursors, portals, `AS OF` reads), in-progress segment builds (`built_at`), and the `AS OF` retention window converted to ts through `/sys/ts_clock` (rounding down). Computed under the registry mutex. Then:
- `W <= visible_ts`.
- `W` is monotonic: the new `W` is `max(old W, computed)`, persisted to `/sys/gc_w` before any GC step uses it. `set_gc_watermark` returns an error on a decrease and the caller treats that as fatal.
- There is no snapshot age cap (PostgreSQL 17 removed `old_snapshot_threshold`). A long-running txn holds `W` back; it is exported as a metric.

### 9.2 Rules
- Keep, for every logical key, every version `> W` and the newest version `<= W`. Intents are never GC'd.
- **A tombstone may be dropped only if a newer version `<= W` of the same key is kept, or together with every older version of its key.** With timestamps inside the key, versions of one logical key can span SST files, so a compaction filter that drops the newest tombstone `<= W` can resurrect an older version sitting in another file. Therefore:
  - **Backend with user-defined timestamps (RocksDB UDT + `full_history_ts_low = W`):** the engine owns version GC and keeps one user key's versions together. Tombstones must be timestamped `Delete`s, not puts of a tombstone value. Preferred if C-S1 confirms UDT works with DeleteRange, iterators at a read ts, and checkpoints.
  - **Otherwise (ts in key: fjall, RocksDB without UDT):** the compaction filter may drop any version (tombstone or not) when it has already seen, earlier in the same compaction stream, a newer version `<= W` of the same logical key. It never drops the newest version `<= W` it has seen for a key. The C-B1 GC job removes a newest-`<= W` tombstone `k@t` with `DeleteRange([enc(k@t), end(k)))`, where `end(k)` is defined in §2.2. Because versions sort newest first, this range covers `k@t` and every older version.
- **DROP / TRUNCATE** retire the relation's storage ids (table and every index) and, for TRUNCATE, allocate new ones, recorded in the versioned catalog. Once `W >` the DDL's commit ts, `DeleteRange` each retired prefix. Storage ids are never reused.

**I-GC.** For every registered snapshot (all have `S >= W`), the read path (§4) returns the same result before and after any GC step.

Unresolved committed intents are safe: GC only drops versions older than a kept version, and the intent's eventual version is newer than all of them.

## 10. Other rules
- **Storage ids:** u64, allocated from a persisted counter, never reused. Catalog rows map relation and index oids to their current storage ids.
- **Sequences:** non-transactional; the high-water mark is persisted (synced) before a value from a new block is handed out. Values may skip, never repeat.
- **Catalog:** rows in `/sys/catalog`, versioned like any table; DDL is transactional and takes AccessExclusive.
- **Large txns:** intents, write-set log and SIREAD state all spill; the 10M-row gate measures commit latency, reader latency during resolution and abort cleanup time.

## 11. Invariants and the G0 model
Checked by G0: `I-WAL-ORDER`, `I-VIS`, `I-ACK`, `I-SNAP-ORDER`, `I-HALLOWEEN`, `I-ONE-INTENT`, `I-WW`, `I-UNIQUE`, `I-TRUNC`, `I-SSI-ORDER`, `I-GC`, plus:
- **I-ATOMIC.** After any crash, for every txn: all of its writes are visible at `visible_ts`, or none are.
- **I-DURABLE.** An acked `synchronous_commit=on` commit survives any crash.
- **I-SER.** Histories under SERIALIZABLE are serializable (Elle, G2), including predicate/phantom workloads and the read-only anomaly.
- **I-NOBLOCK.** No read-only path (§4) waits on another txn.
- **I-LIVE.** A waiter whose blocker has committed or aborted is eventually woken; every wait-for cycle is broken with exactly one 40P01.

The state space is too large for one exhaustive model, so G0 is four small-scope exhaustive models plus a deterministic simulator. All models run over MemKv, and every actor (txn steps, commit thread, async resolver, abort cleanup, GC job, compaction, crash) is a separate step that can interleave.

| Model | Scope | Invariants |
|---|---|---|
| G0-commit | 3 txns, 2 keys, commit thread, resolver, truncation, crash at every KV write and fsync, sync on/off | WAL-ORDER, VIS, ACK, SNAP-ORDER, TRUNC, ATOMIC, DURABLE |
| G0-write | 2 txns, 2 keys (+1 unique key), 2 statements and 1 savepoint per txn, 1 shared mode, waits + DFS, resolver and abort cleanup actors | ONE-INTENT, WW, UNIQUE, HALLOWEEN, ATOMIC, LIVE |
| G0-ssi | 3 txns (one may be SER READ ONLY), 2 keys + 1 range, 2 statements each, pre-commit, registry | SSI-ORDER, SER (cycle check over the recorded history) |
| G0-gc | 2 txns, 1-2 keys, MemKv in **LSM mode** (≥ 2 levels, per-file compaction streams, range tombstones), GC job, registry, AS OF | GC, ATOMIC |

The deterministic simulator runs the full pipeline with larger scopes under seeded schedules.

**Seeded bugs.** G0 must catch every one of these, each reported by the model the table assigns:
1. Reader opens its view before reading `S` (commit).
2. Status truncated without condition 2 (commit).
3. Tombstone dropped by the compaction filter as the newest `<= W` (gc).
4. Intent placed without the latch (write).
5. SIREAD registered after iterating (ssi).
6. SIREAD registered after the view opened (ssi).
7. `visible_ts` advanced before status is set (commit).
8. Ack before fsync for `synchronous_commit=on` (commit).
9. Commit records written out of `commit_ts` order (commit).
10. `/sys/txn` delete written before resolution (commit).
11. Async resolution or abort cleanup without the latch/owner check (write).
12. Inline resolution of a committed foreign intent skipped (write).
13. `intent_count` decremented on a no-op removal (commit).
14. View counter taken after the view opens (commit).
15. Lock-only request replaces own data (write).
16. Savepoint rollback drops the lock with the data (write).
17. `wait_for` without the re-check (write, I-LIVE).
18. Writer proceeds on `Committed(c)` with `c > visible_ts` (write).
19. Unique check ignores own intents (write).
20. SSI pre-commit check not atomic with enqueue (ssi).
21. Taking `S` and registering it are separate steps (gc, and ssi retention).
22. `W` allowed to decrease (gc).

## 12. Open questions and known divergences
Open (resolve before C-T2 merges):
1. RC joined DML: full EPQ vs statement retry (G3c decides).
2. RocksDB UDT viability (C-S1); otherwise the ts-in-key GC path is mandatory.
3. Whether `synchronous_commit=off` commits may be visible to `on` sessions before fsync (PostgreSQL: yes). Current spec: yes.

Known divergences from PostgreSQL 17:
- A concurrent primary-key change seen by RC EPQ raises 40001 instead of following the update chain (§5.1).

## Changelog
- **draft 2 (2026-10-07), round-1 adversarial review** (`docs/C-T0-review.local.md`, 34 findings):
  - §2.1 Intent is a stack of layers, each holding data and the strongest exclusive lock. Lock-only requests never replace own data; savepoint rollback restores data and lock; write strength is derived from key-column changes (DATA-1, F01, F02, F11). Key column defined (F02).
  - §1 Versions sort newest first; seq never decreases; storage ids in keys (GC-4, F19, GC-3). §2.2 version key layout and GC range bytes (GC-4).
  - §3 Commit thread never aborts; channel order is commit order. §3.1 registry: `S` read and registered atomically; view counters registered before views open (GC-1, TRUNC-2, SSI-2). Committed-but-not-visible is treated as Pending by writers (F07).
  - §4 Readers decide on top-layer data; missing current-epoch status is fatal; AS OF below `W` errors with 72000; SER scans open their own view (TRUNC-2, GC-2, SSI-1).
  - §5 One fresh view for intent + newest version under the latch; inline resolution/removal of foreign intents is mandatory and joins the placement batch; `t*` ignores lock-only intents; key-existence ops use the unique check instead of the `t* > S` rule, so delete-then-reinsert works under RR/SER; shared locks run the version check; SIREAD check skipped for lock-only placements (F04, TRUNC-1, F06, F13, F14, F15, SSI-6).
  - §5.1 PK-moving updates leave a moved-tombstone; EPQ raises 40001 on it (F16).
  - §5.2 Unique check sees own intents; SER 40001 only with a covering SIREAD; PK uses the same check; NULLS NOT DISTINCT; deferrable unique uses a prefix latch and runs before enqueue; FK checks are end-of-statement with RC/RR/SER rules and detectNewRows (F03, F13, F21, F17, F05, SSI-3).
  - §5.3 Rollback rules: re-check owner, keep SIREADs, close later portals, wake all waiters, locks tagged with execution seq; exact layer-compaction rule (F10, F12, F19, SSI-5). §5.4 own-row revisit (F18).
  - §6 One wait-for graph over all lock kinds; register-then-recheck; DFS and edge removal atomic (F08, F09, F20).
  - §7.3 Every intent-removal path latches and checks the owner; counts and removal counters updated only on real removal and after the write (TRUNC-1, F04, TRUNC-2).
  - §8 SIREAD before view; atomic pre-commit + prepare + enqueue; exact read-only exception; T1 == T3; own `commit_ts -> txn` map independent of truncation; non-SER edges ignored (SSI-1, SSI-2, SSI-4, SSI-5, SSI-6).
  - §9 `W` is a pure min, monotonic, persisted, `<= visible_ts`; no snapshot age cap; shadowed tombstones may be dropped; DeleteRange bounds explicit; UDT tombstones are timestamped deletes; DROP/TRUNCATE retire storage ids (GC-1, GC-2, GC-3, GC-4).
  - §11 G0 split into four models with LSM-mode MemKv and interleaved actors; seeded bugs extended from 5 to 22 (G0-SEEDS, G0-1).
  - §12 Q2 (unique SQLSTATEs) resolved in §5.2.
