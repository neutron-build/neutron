# C-T0: Transaction protocol (normative)

Status: draft 7.3 (adversarial review rounds 1-6 applied, G0 representation fixed; changelog at the bottom). Every `nucleus-txn` card implements against this file. A change to an invariant (`I-*`) needs a spec change first, then the G0 model updated, then code.

Scope: single node. Isolation levels: RC (PostgreSQL semantics), RR = SI, SERIALIZABLE = SSI. 2PC (`PREPARE TRANSACTION`) is refused with 0A000 in 2.0. Target behaviour is PostgreSQL 17; every deliberate divergence is listed in §12.

## 1. Vocabulary

| Term | Meaning |
|---|---|
| `Ts` | u64 commit timestamp from the sequencer. Wrapped in a struct so it can widen to an HLC later. `Ts::ZERO` is never a commit ts. |
| `TxnId` | `(epoch: u32, n: u64)`. `epoch` = boot epoch. `n` dense per epoch. |
| `S` | A snapshot ts. Snapshot visibility: committed with `commit_ts <= S`. |
| `visible_ts` | Largest ts such that every commit `<= ts` is applied (§3). |
| visible commit | T is `Committed(c)` and `c <= visible_ts`. A committed txn that is not yet visible is treated like `Pending` by every writer-side decision (§3.2). |
| logical key | KV key without the version suffix (`/t/{rel}/{pk}`, `/u/{idx}/{key}`, ...). `{rel}` and `{idx}` are **storage ids** (§10), never catalog oids. |
| version | `{logical}@{ts}`: a committed value with a header (§2.2). **Versions of one logical key sort newest first (descending ts).** |
| intent | `{logical}@INTENT`: at most one per logical key; owned by one txn. Sorts before all versions of its key. |
| `seq` | Per-txn command counter (PostgreSQL command id). Incremented per statement and per savepoint. **Never decreases**, including across `ROLLBACK TO`. |
| view | A KV snapshot (`OrderedKv::snapshot`), or the latest state read under a latch. |
| latch | Short striped mutex over a hash of `latch_key(k)` (§5.0). Held only for check-and-place, intent removal and shared-lock release. Never held across waits on other txns. A thread never holds two latches. |
| key column | A column of any non-partial, non-expression unique index of the relation (PostgreSQL's definition for `FOR KEY SHARE` / `NO KEY UPDATE`). |

## 2. Data

### 2.1 Intent
```
Intent    { txn: TxnId, layers: Vec<Layer> }        # oldest first, never empty
Layer     { seq: Seq, data_seq: Seq, data: LayerData, lock: RowLockMode }
LayerData = Write { value: bytes, key_changed: bool } | Delete { moved: bool } | Absent
```
Each layer is the **complete own state** of the key as of `seq`. `data` is the txn's own value (`Absent` = no own value; reads fall through to versions). `data_seq` is the seq at which `data` last changed (equal to `seq` for a layer that changed data; copied from the previous layer by lock-only changes; 0 when `data` is `Absent` and never changed). `lock` is the strongest exclusive mode held (`NoKeyUpdate` or `Update`). The top layer is the current state.

- A data write implies a lock: `Delete`, or a `Write` with `key_changed` → `Update`; any other `Write` → `NoKeyUpdate`. The layer's `lock` is the max of the implied mode, the previous layer's lock, and any explicit lock request.
- `key_changed` is relative to the **newest committed version** of the key, not to the own previous layer, and is **sticky**: once a layer of the intent has `key_changed`, every later `Write` layer has it. A `Write` that follows an own `Delete` (delete then re-insert of the same key) has `key_changed`. A `Write` on a key with no live committed version (an insert) has `key_changed = false`.
- `Delete { moved: true }` is written at the old `/t/` key by an UPDATE that changes the primary key (§5.1).
- A lock-only request (`SELECT ... FOR UPDATE / NO KEY UPDATE`) copies the previous layer's `data` and `data_seq` and raises `lock`. **It never replaces own data.** A first lock-only request on a key creates a layer with `data: Absent`.
- New state written by command `seq`: let `s = max(seq, top.seq)`. If `top.seq == s`, modify the top layer in place; otherwise push a new layer with seq `s`. Layers are therefore always ordered by seq. A data change sets the layer's `data_seq` to the writing command's `seq` (not `s`). The case `seq < top.seq` arises when a BEFORE trigger (running at a later seq, §5.3) wrote the key before the main statement's own write at `seq0`; as in PostgreSQL (`es_output_cid`), the main statement's write keeps `data_seq = seq0`.
- Shared modes (KEY SHARE, SHARE) are never stored in intents (§6).

Foreign readers and resolution look only at the **top layer's `data`**. `Absent` means "lock only": no version is produced, nothing becomes visible.

### 2.2 Version key and value layout (ts-in-key backends)
`L` = the encoded logical key, prefix-free by C-Q3s P-PREFIX, so no other logical key starts with `L`.
```
intent        L ‖ 0x00
version @ts   L ‖ 0x01 ‖ be64(u64::MAX - ts) ‖ 0x01   # newest first
end(L)        L ‖ 0x02                                # exclusive upper bound of every entry of L
version value header ‖ payload
  header 0x00 live              payload = row/entry bytes
         0x01 live, key_changed payload = row/entry bytes
         0x02 tombstone         no payload
         0x03 moved-tombstone   no payload
```
`L` is the codec's encoded logical key as-is (order-preserving and prefix-free, C-Q3s), so logical keys keep their SQL order in the KV. `L` cannot be delimited from the left without the schema, so a raw key is split from the right: the last byte is `0x00` for an intent (`L` = all but the last byte) and `0x01` for a version (`L` = all but the last 10 bytes, the byte before the timestamp being `0x01`). The txn layer and the GC filter therefore parse keys without a schema. `[L ‖ 0x00, end(L))` holds exactly the intent and versions of `L`. The GC range for tombstone `L@t` is `[L ‖ 0x01 ‖ be64(u64::MAX - t), end(L))`. Resolution maps `Write{key_changed}` to header 0x00/0x01 and `Delete{moved}` to 0x02/0x03.

The layout for backends with user-defined timestamps (intent placement, tombstone and moved-tombstone representation) is open (§12 Q2) and must be specified before such a backend is adopted.

### 2.3 Persisted system keys
- `/sys/txn/{TxnId}`: only `Committed(ts)` is persisted. `Pending` and `Aborted` are in-memory only (§7).
- `/sys/epoch`: u32, incremented and synced at boot before any txn starts.
- `/sys/ts_hwm`: sequencer high-water mark, reserved in blocks (§3).
- `/sys/gc_w`: the GC watermark `W` (§9). Monotonic. Written with `Durability::Yes` (synced).
- `/sys/ts_clock`: sparse `(wall_time, ts)` samples written by the commit thread (at most one per second) for converting wall-time windows to ts (§9).

## 3. Commit pipeline and visibility

One **commit thread** owns the sequencer. Txns submit a commit request (status record, optional `/log` record, sync mode, and for SERIALIZABLE txns the prepared SSI record, §8.4) on an **unbounded** channel. Requests are processed strictly in channel order. The thread:

1. Drains a group. Assigns `commit_ts` in channel order. If `commit_ts` would exceed `ts_hwm`, first writes `/sys/ts_hwm += BLOCK` in the same batch.
2. Writes the group's records to the KV in `commit_ts` order (one write per request or one batch per group; WAL order must equal `commit_ts` order).
3. For each SERIALIZABLE request, inserts `commit_ts -> TxnId` into the SSI writer map (§8.5). This happens before step 4 sets any status.
4. For the prefix of `synchronous_commit=off` requests up to the first `on` request: set in-memory status `Committed(ts)`, then advance `visible_ts`, then ack. Then fsync (never on a tokio worker), and for the rest of the group in order: set status, advance `visible_ts`, ack.
5. For each committed txn, after `visible_ts >= commit_ts`, in this order: release its in-memory locks (shared row locks, relation locks, xact advisory locks, deferrable-unique prefix waits; shared row locks are released under the key's latch, §6); bump its wake generation and wake every waiter (§6); queue it for async resolution (C-B1); mark it `released` (§7.4).

The commit thread never aborts a txn. Once a request is on the channel the txn will commit unless the process dies (SSI decisions are made before enqueueing, §8.4). Failed KV write: the thread stops and the process aborts (fail-stop). It never fills a ts hole by skipping.

Boot: `next_ts = ts_hwm + 1` and `visible_ts = ts_hwm` (anything up to `ts_hwm` without a persisted `Committed` record never committed). All persisted commits are visible at boot.

### 3.1 Snapshot and view registration
A **registry** (one mutex) holds every live snapshot `S`, every open view's counter, and the published watermark `W` (§9.1).

- **Taking a snapshot:** under the registry mutex, read `S = visible_ts` and insert `S` into the registry. One step; nothing can compute `W` or retire SSI state between the read and the insert.
- **Registering a caller-chosen ts** (`AS OF t`, a segment build's `built_at`): under the registry mutex, check `t >= W` (the published `W`) and `t <= visible_ts`, then insert `t`. Otherwise 72000 (`snapshot_too_old`) for AS OF; a build re-takes its ts.
- **Opening a view:** under the registry mutex, take `c = ++view_counter` and register `(c, vts = visible_ts)`; release the mutex; then open the KV snapshot/iterator. Unregister both when the view closes. `vts` holds `W` back (§9.1) while the view is open, so a latest-state view (one used without a snapshot, e.g. a deferred constraint scan) never sees GC drop a version it would otherwise read: while the view is open `W <= vts`, and GC keeps every version above `W` plus the newest at or below it, so every version the view could return as a key's newest is kept.
- A txn's snapshot stays registered until the txn ends (RR/SER) or until its statement ends (RC). An open cursor or portal keeps its snapshot registered until it closes. A `WITH HOLD` cursor is materialised at commit, before the SSI pre-commit (§8.4). As in PostgreSQL's `CommitTransaction`, firing deferred triggers and constraint checks and materialising holdable portals repeat until neither has anything left (a deferred trigger may declare a new holdable cursor); nothing reads the KV for such a cursor after its txn ends.
- **Every KV read made outside a latch section opens a registered view**: scans, point reads, FK parent and child reads, constraint-check scans, EPQ re-reads, SSI fetches. A read made under `latch(latch_key(k))` may read the latest state of `k` directly: every removal of `k@INTENT` takes the same latch (§7.3), so the intent cannot be removed and its owner's status cannot be truncated while the read runs.

**Lock order.** Latch → registry mutex → SSI mutex → wait-for-graph mutex. A thread holds at most one latch and never acquires an earlier lock in this order while holding a later one. The row-lock, relation-lock and advisory-lock tables, the waiter tables and the status-table mutex are leaves: each may be taken under any lock above, never held while taking another lock, except that status lookups are allowed under a lock-table mutex.

**I-WAL-ORDER.** KV WAL order of commit records equals `commit_ts` order, and every intent write of a txn precedes its commit record in the WAL. Consequence: a durable commit implies its intents and every earlier commit are durable (crash loses a suffix only).
**I-VIS.** In-memory status of T is `Committed(ts)` before `visible_ts` reaches `ts`.
**I-ACK.** A commit is acknowledged only when `visible_ts >= commit_ts` (read-your-commits across sessions) and, if `synchronous_commit=on`, after fsync.
**I-SNAP-ORDER.** Snapshot taken (`S` read and registered atomically) → view counter registered → KV view opened. Never another order.

Why that is enough: for any committed T with `commit_ts <= S`, T's intents were written before its commit record, which was written before `visible_ts` passed `commit_ts`, which happened before `S` was read, which happened before the view opened. So the view contains T's writes (as intents or resolved versions).

### 3.2 Commit-visibility window
Between step 4 setting `Committed(c)` and `visible_ts` reaching `c`, T is committed but not yet visible to new snapshots. Every writer-side decision (§5, §5.2, §6) treats such a T exactly like `Pending`: it waits, and is woken in step 5. Readers need no rule: `S <= visible_ts < c`. Because resolution is queued only in step 5 (and inline resolution only acts on visible commits), no **version** with ts greater than `visible_ts` ever exists; so writers never see a resolved version of a not-yet-visible commit.

Backend requirement (disqualifier for C-S1): a single WAL across all keyspaces, sequential, so a sync covers every earlier write.

## 4. Read path (readers never block)

To read logical key `k` at snapshot `S` with statement start `seq0`, inside txn `R`, from view `V`:

1. If `k@INTENT` exists in `V` with owner `T`:
   - `T == R`: take the newest layer with `seq < seq0`. If it exists and its `data` is `Write` → its value; `Delete` → not-found; `Absent`, or no such layer → step 2.
   - `T != R`, top layer `data == Absent` (lock only): step 2. No SSI edge.
   - `T != R`, `status(T) == Committed(c)` and `c <= S`: the top layer's data is the newest visible version.
   - `T != R`, `status(T)` is `Pending`, or `Committed(c)` with `c > S`: invisible, step 2. Under SERIALIZABLE, record rw-antidependency `R -> T` (§8.2).
   - `T != R`, `status(T) == Aborted`: invisible, step 2. No edge.
2. The first version `k@t` in `V` with `t <= S`. Tombstone or moved-tombstone = not-found. Each version with `t > S` skipped under SERIALIZABLE records `R -> writer(t)` (§8.5).

Status lookup for the owner of an intent found in a view never blocks and never misses (I-TRUNC, §7). There, a missing status for a txn of the **current** epoch is a fatal invariant violation (process aborts), never defaulted. A missing status for an older-epoch txn means Aborted.

Every other status lookup is of a TxnId remembered from earlier (a `wait_for` re-check, a shared-lock or relation-lock holder, a graph edge, an SSI edge). There a missing status means **ended and released**: §7.4 truncates only released txns, so the txn is an aborted one or a visible commit whose step 5 has run.

**I-HALLOWEEN.** A statement never sees its own writes with `seq >= seq0`.

Views: a scan uses one view for its whole duration. RC takes a fresh `S` per statement; RR/SER take one `S` per txn. Versions `<= S` never change except by GC, which respects §9, so several views per statement are allowed. Under SERIALIZABLE the SIREAD ordering rule of §8.1 decides when a fresh view is required.

Reads at an explicit ts (`AS OF t`) are refused with 0A000 inside a SERIALIZABLE txn (they would bypass SSI). Elsewhere they register `t` as in §3.1 and then read as a snapshot at `t`, **with the catalog as of `t`** (relation and index storage ids and `built_at` resolved from catalog versions `<= t`). A relation that did not exist at `t` is 42P01. Because retired prefixes are deleted only once `W` passes the retiring DDL (§9.2) and `t >= W`, the storage the catalog at `t` names still exists.

Relations: a snapshot (other than AS OF) uses the latest committed catalog. Under RR/SER, if a relation's current storage id was created by a DDL with commit ts `> S` (TRUNCATE, table-rewriting ALTER), access raises 40001 (§12). An index whose `built_at > S` is not used by that snapshot.

## 5. Write path (all levels)

### 5.0 Operations and latch keys
Operations on a key `k` by txn `W` at snapshot `S`, current command `seq`, statement start `seq0`:

- **Row ops** act on a row the statement found: UPDATE, DELETE, exclusive row lock (`FOR UPDATE`, `FOR NO KEY UPDATE`), shared row lock (`FOR SHARE`, `FOR KEY SHARE`, §6).
- **Key-existence ops** create a key that must not be live: INSERT of `/t/{rel}/{pk}`, a PK-changing UPDATE's new `/t/` key, and unique entries `/u/{idx}/{key}`.

`latch_key(k)` is defined once and used by **every** path that reads-then-writes `k@INTENT` (placement, every removal in §7.3, savepoint rollback):
- for an `/i/{idx}/{key}{pk}` entry of a **deferrable** unique constraint: the prefix `/i/{idx}/{key}` (all entries with that key value);
- otherwise: `k`.

### 5.1 The loop
```
loop:
  latch(latch_key(k))
  read k@INTENT and the versions of k from ONE fresh view of the latest state
  g = wake_generation(owner) if a foreign intent exists
  if intent owned by T != W:
     st = status(T)
     if st == Committed(c) and c <= visible_ts, or st == Aborted:
        remove it per §7.3 steps 2-4 under the latch already held; unlatch; continue
     # T is Pending or committed-not-visible
     if key-existence op, T's top data is Absent, and the newest committed data version is live:
        unlatch; unique conflict (§5.3: 23505, or the arbiter path of §5.3.1)   # no wait on a lock-only intent
     if requested mode conflicts with top.lock of T's intent (§6 matrix):
        unlatch; wait_for(W -> T, g); continue        # §6; may raise 40P01 / 55P03
     # no conflict (e.g. KEY SHARE vs NoKeyUpdate): T's intent is invisible data; go on
  H = holders of shared row locks on k, other than W, that are neither Aborted nor visible commits
  if some holder in H conflicts with the requested mode (§6):
     g' = their wake generations (read under this latch); unlatch; wait on every conflicting holder; continue
  key-existence ops always run the unique check below, own intent or not
  if W has an intent on k:      row ops follow the own-row rules (§5.4); the newer-version rule below is skipped
  row op (no own intent):
     base = S, or after an EPQ pass the ts of the version v it evaluated (§5.2), or for the ON CONFLICT
            lock the ts of v_r (§5.3.1)
     N = DATA versions of k with ts > base (committed lock-only intents are not versions)
     if N is not empty:
        RR/SER: unlatch; raise 40001   (KEY SHARE: only if some version in N is a tombstone,
                                         moved-tombstone or key_changed write; otherwise proceed)
        RC:     EPQ (§5.2); the ON CONFLICT lock instead unlatches and restarts its arbiter (§5.3.1)
        (with base = v.ts or v_r.ts, N is non-empty only if the row changed again, so these never spin)
  key-existence op: unique check (§5.3); never 40001 from N alone, never EPQ
  shared lock: record it in the lock table (tagged with seq); unlatch; done
  append (s, k) to the write-set log for the layer about to be pushed or modified (§2.1; once per (s, k))
  if placing a new intent: intent_count(W) += 1                (log and count both before the write)
  build the new top layer (§2.1); write k@INTENT as one KV batch   # a failed write is fail-stop: the process aborts
  unlatch
  SSI: if the new layer changed data (not lock-only), check SIREAD locks covering k (§8.2)
```
Every early exit (40001, 23505, 21000, 27000, EPQ skip, NOWAIT, SKIP LOCKED, timeout, cancel) leaves the KV exactly as the removals already written left it; nothing is "planned" without being written. Because every layer change is logged (and every placement counted) before its write and a failed write stops the process, the write-set log names every layer `ROLLBACK TO` must drop and every intent abort cleanup must remove, and `intent_count(W)` never undercounts.

**I-ONE-INTENT.** At most one intent per logical key. Guaranteed because placement happens only under `latch(latch_key(k))` when no foreign intent exists on `k`: visible-committed and aborted ones are removed by a written batch first, and a Pending one always conflicts with any op that places an intent (every exclusive mode conflicts with `NoKeyUpdate`), so only shared locks, which write no intent, pass a foreign intent.
**I-WW.** Under RR/SER, a row op on a row with no own intent never succeeds when the row's newest data version is newer than `S`. RC instead serialises row ops through EPQ. Key-existence ops are governed by I-UNIQUE, not I-WW: inserting over a tombstone newer than `S` is allowed (PostgreSQL behaviour).
**I-LOCK.** No two txns hold conflicting row-lock modes on one key at the same time (intent top-layer `lock` of a Pending or not-yet-visible txn, plus the shared lock table).

### 5.2 RC EvalPlanQual
After a wait or when versions newer than `S` exist: remember the newest committed data version `v` (its ts and header) read under the latch, **unlatch**, re-evaluate the statement's quals for this row against `v` (PostgreSQL EvalPlanQual; this may run subqueries and functions, so it never runs under a latch). Fail: skip the row. Pass: compute the update from `v`, then re-enter §5.1. Under the latch, a foreign intent is handled by the loop as usual (a conflicting one is waited on through `wait_for`, so a deadlock stays visible to the graph; a non-conflicting one is passed). EPQ is repeated **only** if the newest committed data version is no longer `v`; otherwise the intent is placed. The ON CONFLICT lock never runs EPQ (§5.3.1). EPQ for KEY SHARE examines every version newer than `S`, not only the newest (§5.1). Joined DML (UPDATE FROM, DELETE USING, MERGE): full EPQ, or an internal statement retry with a fresh snapshot (capped retries, then 40001). Which one is decided by G3c and recorded here.

**Moved rows.** EPQ that reaches a moved-tombstone (as a version, or as a foreign `Delete{moved: true}` intent) raises 40001 ("tuple to be locked was already moved"). Divergence from PostgreSQL, which follows the update chain: §12.

### 5.3 Unique, primary key and FK
**Unique check** (key-existence ops, under the latch): the key's **current state** is the own intent's top-layer data if `W` has an intent on it and that data is `Write` or `Delete`; otherwise (no own intent, or own top data `Absent`) the newest committed data version.
- Live and belonging to a different row → 23505, except for an `INSERT ... ON CONFLICT` arbiter key, which follows §5.3.1. (A MERGE `WHEN NOT MATCHED THEN INSERT` that hits a unique conflict raises 23505, as in PostgreSQL.)
- Under SERIALIZABLE, if that live version has ts `> S` and `W` holds a SIREAD covering the key, raise 40001 instead of 23505. Otherwise 23505. (To be confirmed against PostgreSQL's isolation tests in G3c.)
- Not live (absent, tombstone, moved-tombstone, own `Delete`) → proceed, whatever its ts.
- Concurrent duplicate insert: the second inserter meets the first's Pending intent (always conflicting: inserts hold `NoKeyUpdate` or stronger) and waits. First commits: the second re-runs and gets 23505. First aborts: the second proceeds.

**I-UNIQUE.** For every ts `S`, the committed versions `<= S` (no txn's own uncommitted writes) contain no two live rows with equal values of a unique key (NULLS DISTINCT keys with a NULL excepted). Index consistency at every such `S`: a live `/u/` entry (or a live `/i/` entry of a deferrable unique index) points to a row live at `S` whose key value is the entry's value, every live row has its entries, and a deferrable prefix `/i/{idx}/{key}` has at most one live entry. (A txn's own view can show a duplicate: an RR txn inserting over a tombstone newer than its `S` still sees the deleted row at `S`, as in PostgreSQL.)

- **Primary key:** `/t/{rel}/{pk}` inserts and PK-changing updates run the same unique check on the new `/t/` key.
- **NULLs:** `NULLS DISTINCT` (default): if any key column is NULL, the entry goes to `/i/{idx}/{key}{pk}` (no uniqueness). `NULLS NOT DISTINCT`: NULL is encoded in the `/u/` key and checked like any value.
- **Deferrable unique constraints** use the non-unique `/i/` layout and the prefix latch key (§5.0). The check scans the prefix in the latest state under that latch, taking each entry's **current state** as defined above (own top-layer data where present, so an own `Delete` on a committed entry counts as not live): a foreign visible-committed or aborted intent → remove it per §7.3 steps 2-4 and re-scan; a foreign Pending or committed-not-visible entry → wait on its owner (it always conflicts), then re-check; two live entries for different rows → 23505. Under `NULLS DISTINCT`, an entry whose key contains a NULL is never checked.
- **Deferrable primary keys** are refused with 0A000 in 2.0 (the `/t/` key is unique by construction and has no deferred layout; §12).

**Constraint timing.** Three timings, as in PostgreSQL:
1. **Per row, immediately:** non-deferrable unique and primary keys (the unique check runs inside §5.1 for each entry), NOT NULL and CHECK.
2. **End of statement:** `DEFERRABLE INITIALLY IMMEDIATE` unique constraints (or deferrable ones set `IMMEDIATE`), and every non-deferred FK check. End-of-statement checks see own writes with `seq <= current`.
3. **Commit:** constraints currently deferred (`INITIALLY DEFERRED`, or `SET CONSTRAINTS ... DEFERRED`). They run in the commit phase **before the commit request is enqueued** (and before the SSI pre-commit, §8.4). `SET CONSTRAINTS ... IMMEDIATE` runs pending deferred checks at that point.

Pending end-of-statement and deferred checks, and queued AFTER-trigger events, are tagged with the seq that queued them; `ROLLBACK TO SAVEPOINT s` discards those with seq `>= s` (§5.5).

### 5.3.1 ON CONFLICT arbiter protocol
`INSERT ... ON CONFLICT` with arbiter unique keys `A` (PostgreSQL's speculative insertion), per proposed row. Each **attempt** starts by taking an internal savepoint at a fresh seq `sa` (§5.5). The **main statement's** writes in the attempt (the proposed row and its index entries) use `sa` as their command seq for layer placement, with `data_seq = seq0` (§2.1, `es_output_cid`). Internal commands run during the attempt (BEFORE triggers, FK checks, constraint checks) take fresh seqs above `sa` and follow §2.1 as usual; so every layer the attempt creates or modifies has seq `>= sa`, and rolling back to `sa` removes exactly the attempt's effects. End-of-statement checks and AFTER-trigger events queued during the attempt are tagged `sa`, so an abandon discards them (§5.5).
1. **Pre-check.** For each arbiter key `a`, under `latch(latch_key(a))`, read `a@INTENT` and its versions from one fresh view, as in §5.1: first remove a foreign visible-committed or aborted intent per §7.3 steps 2-4 and re-read; a foreign Pending or committed-not-visible intent whose top data is not `Absent` → `wait_for` its owner (§6), restart from 1. Otherwise determine the key's current state as in the unique check. A live entry of another row `r` → remember `r` and the newest committed data version `v_r` of `r`'s `/t/` key (read from a registered view, §3.1, since it is outside `r`'s latch), go to 3.
2. **Insert.** No conflict: insert the row and its entries as key-existence ops through §5.1. If any arbiter key's unique check now finds a live entry of another row, or must wait, **abandon** the attempt: roll back to `sa` exactly as `ROLLBACK TO SAVEPOINT` (§5.5: only the layers this attempt pushed or modified are dropped, an intent is removed only if no layer remains, and, if anything was dropped, the wake generation is bumped so waiters on those intents re-run), then restart from 1 (after the wait, if one was needed). A wait at a **non-arbiter** key is an ordinary §5.1 wait: the attempt keeps its layers and waits in place (PostgreSQL `UNIQUE_CHECK_YES`); only an arbiter key's conflict or wait abandons the attempt.
3. **Conflict on row `r`.**
   - Under RR/SER, if `r`'s newest committed version is newer than `S`: 40001 (PostgreSQL `ExecCheckTupleVisible`). This applies to `DO NOTHING` too.
   - `DO NOTHING`: skip the proposed row.
   - `DO UPDATE`: if `r`'s own top layer has `data_seq == seq0` (inserted or updated by this statement): 21000. Otherwise lock `r`'s `/t/` key as a row op through §5.1 with the mode the update needs, **without EPQ**: if under the latch `r`'s newest committed data version is not `v_r` (updated, deleted, moved, or a newer foreign intent was resolved), unlatch and restart from 1 under RC (RR/SER already raised 40001 above). Once locked, `r`'s row value is the own intent's data if it has any, otherwise `v_r` (the version checked under the latch). Evaluate `DO UPDATE ... WHERE` against that value (a false WHERE leaves the lock, as in PostgreSQL), compute the update from it and apply it through §5.4.

Each restart follows either a wait or an observed change of committed state. An abandon that released nothing wakes nobody (§5.5), so two restarting statements that wait on each other leave a stable wait-for cycle for the deadlock check (§6, I-LIVE): restarts cannot spin without another txn making progress.

**Foreign keys.** FK checks, referential actions and AFTER triggers run as internal commands at a **fresh seq** (greater than the statement's), and their reads open registered views (§3.1).
- **Child side** (insert, or FK-changing update, of a child row): first read the parent: RC with a fresh snapshot, RR/SER at `S`, both plus own writes. Not found → 23503. Found → take KEY SHARE on the parent's `/t/` key as a row op through §5.1 (RC: EPQ re-check; RR/SER: 40001 if some data version of the parent newer than `S` is a tombstone, moved-tombstone or key_changed write, as in §5.1). Under SERIALIZABLE register a SIREAD on the parent key before the read. For this lock, an EPQ that fails (the parent is now a tombstone, or no longer matches the referenced key) raises 23503, never skips (PostgreSQL `RI_FKey_check` finds no row); a moved parent raises 40001.
- **Parent side** (delete, or key change of a referenced key): the row op already holds `Update` on the parent (conflicts with KEY SHARE, so pending child inserters wait). At check time, scan the child index for the old key in the latest committed state plus own writes. A live child → for NO ACTION, first look for a live parent with the old key in the same state (`ri_Check_Pk_Match`); if one exists, no error; otherwise 23503. RESTRICT: 23503 without that look. Under RR/SER, a child live in the latest state but invisible at `S` → 40001 (PostgreSQL `detectNewRows`).
- Referential actions (CASCADE, SET NULL, SET DEFAULT) run as ordinary DML through §5.

### 5.4 Own-row rules
When `W` already has an intent on `k`. The revisit and 27000 rules apply only to the statement's own **data-changing row ops**: UPDATE, DELETE, MERGE matched actions, and the ON CONFLICT update (§5.3.1). Lock requests, FK checks, referential actions and triggers (which run at their own fresh seq, §5.3) only build a new layer.
- **Revisit in the same statement** (a row op on a row whose own top layer has `data_seq == seq0`): UPDATE / DELETE skip the row (PostgreSQL `TM_SelfModified`); `INSERT ... ON CONFLICT DO UPDATE` and MERGE matched actions raise 21000. A layer that only changed `lock` at `seq0` (a CTE `FOR UPDATE`, the conflict lock of ON CONFLICT) does not count.
- **Modified by a later command** (`data_seq > seq0`, e.g. a trigger or volatile function of this statement updated the row): raise 27000.
- Otherwise: build the new layer. No `t* > S` check: the own intent has excluded foreign writers since it was placed, and its placement did the check (or was an insert, governed by I-UNIQUE).

### 5.5 Savepoints and cursors
The txn keeps a write-set log `(seq, logical key)` with one entry per layer pushed or modified (§5.1; spills to disk past a threshold). Internal savepoints (one per ON CONFLICT attempt, §5.3.1) follow the same rules as user savepoints.

`ROLLBACK TO SAVEPOINT` at seq `s`:
- For each key written at seq `>= s`: under `latch(latch_key(k))`, re-read `k@INTENT`; if owned by the txn, drop layers with `seq >= s`. If no layer remains, remove the intent per §7.3. The restored top layer restores data, `data_seq` and lock.
- Shared row locks taken at seq `>= s` are released (under each key's latch, §6).
- Pending end-of-statement and deferred constraint checks and queued AFTER-trigger events with seq `>= s` are discarded.
- **SIREAD locks and recorded rw-conflicts are kept** (the client has seen the data; PostgreSQL keeps them too).
- Portals and cursors opened at seq `>= s` are closed.
- `seq` does not go back: the next command gets a seq greater than every seq used so far.
- If the rollback released anything (dropped or removed a layer, or released a shared row, relation or advisory lock), the txn's wake generation is bumped and **all** its waiters are woken. They re-run §5; spurious wakeups are harmless. A rollback that released nothing does not bump: none of its waiters can be unblocked by it, and a bump removes their wait-for edges (§6 Waking), so two restarting ON CONFLICT statements would keep waking each other and never present a stable cycle to the deadlock check (I-LIVE).

Writes and locks are tagged with the seq current **when they execute**, not with the opening seq of the portal that runs them. A `FOR UPDATE` cursor fetched after `SAVEPOINT s` takes locks at seq `>= s`, so `ROLLBACK TO s` releases them.

**Layer compaction.** Live boundaries are: the current statement's start, every open cursor/portal start, and every live savepoint seq. Layer `i` (with next layer `i+1`) is needed iff some live boundary `r` has `layers[i].seq < r <= layers[i+1].seq`. The top layer is always needed. Others may be dropped.

## 6. Locks and waiting
- Exclusive row modes (NO KEY UPDATE, UPDATE) live in intent layers (§2.1).
- Shared modes (KEY SHARE, SHARE) live in an in-memory table keyed by logical key, tagged with the acquiring seq, and checked under the same latch. Safe because a crash aborts every holder.
- Conflict matrix: PostgreSQL's row-lock matrix, applied to (requested mode, held mode). An intent of a Pending or committed-not-visible txn holds its top layer's `lock`. Requested mode of each op: UPDATE without key change → NoKeyUpdate; UPDATE with key change, DELETE, PK change → Update; inserts and unique entries → NoKeyUpdate; explicit locks → their mode.
- **Release.** A txn's in-memory locks are released at abort (§7.1) or in commit step 5 (§3), never earlier. Savepoint rollback releases those taken after the savepoint. A shared row lock on `k` is removed from the lock table only under `latch(latch_key(k))`, and the holder's wake generation is bumped after the removal; so a waiter that read the holder's generation under that latch (§5.1) either saw the lock gone or sees the bump.
- **Conflict checks ignore ended holders.** A holder (of a shared row lock, relation lock or advisory lock) that is Aborted or a visible commit never conflicts, even before its release has run.
- **Wake generation.** Every txn has a counter `gen(T)`, stored in its status entry, bumped on every event that can unblock its waiters: commit step 5, abort, a `ROLLBACK TO` that released something (§5.5; including an abandoned ON CONFLICT attempt that did), any release of an in-memory lock it holds.
- **Wait-for graph.** One graph covers every kind of wait: row intents, shared row locks (an edge to **every** conflicting holder), relation locks, advisory locks, deferrable-unique prefix waits.
- **Waking.** Whoever wakes waiters (commit step 5, §7.1 abort, `ROLLBACK TO`, a lock release) first removes the woken waiters' edges to that txn under the graph mutex, then wakes them, so the deadlock DFS never sees an edge for a wait that has already been satisfied.
- **Waiting.** `wait_for(W -> T, g)` where `g = gen(T)` was read under the latch that observed the conflict: (1) insert the edge into the graph and register W as a waiter on T, (2) in the same graph-mutex critical section as (1), if T's status is missing (ended, §4; checked first, since `gen(T)` lives in the status entry), or T is Aborted, or T is a visible commit, or `gen(T) != g`, or W's cancel flag is set, remove the edge and return immediately, (3) park. A woken waiter removes any remaining edges and re-runs §5. The same pattern (read the generation where the conflict is seen, re-check after registering) is used for relation, advisory and prefix waits.
- NOWAIT: 55P03 instead of waiting. SKIP LOCKED: skip the row. `lock_timeout`: 55P03 on expiry.
- **Deadlock.** After `deadlock_timeout`, the waiter takes the graph mutex, runs a DFS, and if it finds a cycle containing itself, removes its own edges before releasing the mutex, then raises 40P01. Exactly one member of a cycle is aborted. No wait-die.
- Relation locks (AccessShare … AccessExclusive) are a separate in-memory table, held to txn end, except that `ROLLBACK TO` releases those taken after the savepoint (as PostgreSQL does). Lock queues are not fair: a released lock wakes every waiter, and each re-tries. After acquiring one, catalog lookups use the latest committed catalog (§4 for RR/SER storage-id rule).
- Advisory locks: in-memory, xact scope. Session-scope advisory locks are deferred (§12).
- **Cancellation.** Only the owning session sets its own status to `Aborted`, and only outside any latch section. Cancel requests and timeouts set a flag that the session acts on at its next check point. Setting the flag also wakes the session if it is parked (lock wait, deferrable-prefix wait, relation or advisory wait) or blocked on client input inside a txn; every park and idle read re-checks the flag on wake.

## 7. Abort, crash, resolution, status truncation

### 7.1 Abort
The owning session (§6) sets status `Aborted` in memory, releases its in-memory locks, bumps its wake generation and wakes waiters, marks itself `released`, then queues an async cleanup of its intents (§7.3 rules), found through its write-set log. Nothing is persisted. SSI state follows §8.6.

### 7.2 Crash
On boot, `epoch += 1` (synced). Load every persisted `Committed` record and `/sys/gc_w`; set `visible_ts = ts_hwm` (§3). An intent whose owner has `owner.epoch < current` and no `Committed` record is Aborted. That is O(1) per lookup; cleanup is lazy (on encounter in §5, or by the C-B1 sweep).

### 7.3 Intent removal (every path)
Paths that remove an intent: removal of a foreign visible-committed or aborted intent in §5.1, async resolution (C-B1), abort cleanup, the boot sweep, savepoint rollback, and retired-prefix cleanup (§9.2). Resolution of a committed T only ever runs once T is a visible commit. **Every path:**
1. takes `latch(latch_key(k))` (the §5.1 loop already holds it and starts at step 2),
2. re-reads `k@INTENT` from the latest state, and acts only if it is still owned by the expected txn (and, for savepoint rollback, the expected layers are present),
3. writes its ops as one KV batch: `delete k@INTENT`, plus `put k@commit_ts` with the top layer's data when resolving a committed intent whose data is `Write` or `Delete` (§2.2 header; `Absent` produces no version),
4. after the batch write returns, still under the latch, under the registry mutex: `last_removal_counter(T) = view_counter`, **then**, for a current-epoch T, `intent_count(T) -= 1` (older-epoch txns have no count). Only if step 3 removed an intent of T.
5. releases the latch.

There is no conditional delete in the KV; steps 1-2 replace it. A removal that finds the intent gone or re-owned writes nothing and changes no count.

### 7.4 Status truncation
Under the registry mutex, the status entry of T may be removed (in memory, and `/sys/txn/T` deleted) only when:
0. T is `released`: aborted with §7.1 done, or committed with commit step 5 done (so T is a visible commit and holds no in-memory lock). Older-epoch txns are released by definition.
1. `intent_count(T) == 0`. For an older-epoch T the count is unknown; instead the boot sweep must have finished (it removes every older-epoch intent; none can be created afterwards).
2. Every view open when T's last intent was removed has closed: `min(registered view counters) > last_removal_counter(T)`.

A truncated status therefore always means "ended and released" to any later lookup by a remembered TxnId (§4).

Condition 2 applies to older-epoch txns as well: step 4 sets `last_removal_counter(T)` for them too, and the sweep, on finishing, records `sweep_counter = view_counter` under the registry mutex; an older-epoch record is truncated only when `min(registered view counters) > max(last_removal_counter(T), sweep_counter)`.

**I-TRUNC.** No reader can observe an intent of T after T's status entry is gone. Condition 2 works because a view registers its counter before opening (§3.1), the counter is recorded after the removal batch is written (§7.3), and both are read together with the count under one mutex.
The `/sys/txn/T` delete is written after the removal batches, so by I-WAL-ORDER a crash can never keep the delete and lose the resolution.

SSI does not use the status table to map timestamps to txns (§8.5), so status truncation never loses an SSI edge.

## 8. SSI (SERIALIZABLE only)
Algorithm: PostgreSQL's (Cahill, Ports & Grittner) with its commit-ordering refinements. All SSI state is guarded by one **SSI mutex** (may be striped later if G0 is extended to cover it).

### 8.1 SIREAD locks
- SIREADs are on the KV key ranges a scan may iterate, gaps included: a scan of `[a, b)` locks `[a, b)`. Point reads lock the logical key. FK reads lock the parent key (§5.3).
- **I-SSI-ORDER.** For every key `k` a SER txn reads, its SIREAD covering `k` was registered before the view it reads `k` from was opened. There is no other way to satisfy it (an unregistered latest-state read would also break I-TRUNC, §3.1).
  - Scans: register the full bound, then open the scan's own registered view. Shrink the lock to the range actually iterated only afterwards.
  - Point fetches whose key comes out of another scan (index → heap fetch, join probes): register the SIREAD, then open a fresh registered view (batching several fetches per view is allowed if all their SIREADs were registered first). Reusing a view opened before the registration is forbidden.
- Escalation (range → index/table prefix → relation, past `max_pred_locks_per_*`) adds the coarse lock before removing the fine ones. ANN, FTS, columnar, GIN and graph access paths take relation-level SIREAD. **Relation-level SIREADs are keyed by relation oid**, not storage id.
- SIREADs and recorded conflicts survive `ROLLBACK TO SAVEPOINT` (§5.5).

### 8.2 Edges
- Writer side: after placing a data-changing intent (§5), check SIREADs covering `k` held by other SER txns; each gives `reader -> W`. Lock-only placements and shared locks do not check SIREADs.
- DDL side: DROP, TRUNCATE and table-rewriting ALTER of a relation check every SIREAD on that relation (any granularity, any storage id of it) held by other SER txns; each gives `reader -> W` when W is SERIALIZABLE. The check runs **when the DDL executes**, right after it acquires AccessExclusive, under the SSI mutex, while W can still be chosen as a victim (PostgreSQL `CheckTableForSerializableConflictIn`). The promotion of §8.6 (every isolation level) runs in the DDL txn's pre-commit critical section (§8.4).
- Reader side: §4 skipped foreign intents of Pending or `Committed(c > S)` owners (`R -> owner`) and skipped versions `> S` (`R -> writer(t)`, §8.5).
- **Only concurrent txns form edges.** On the writer and DDL sides, a SIREAD holder R that committed with `commit_ts(R) <= S(W)` is not concurrent with W and gives no edge (W saw everything R did). On the reader side, edges only go to writers with `commit_ts > S(R)` or not yet committed, which §4 already ensures.
- Edges to or from a txn that is not SERIALIZABLE, or that has aborted, are ignored.
- An edge `X -> Y` carries `commit_ts(Y)` once Y has committed: set when the edge is recorded if Y already committed, otherwise when Y's commit is processed (§3 step 3). Retiring Y's SSI state (§8.6) never removes or invalidates an edge pointing at Y.

Correctness of I-SSI-ORDER: for a concurrent reader R and writer W on `k`: if W's intent placement precedes the opening of the view R reads `k` from, R sees the intent or its resolved version `> S` and records the edge. Otherwise the placement follows R's SIREAD registration, so W's SIREAD check sees it.

### 8.3 Dangerous structure
`T1 -> T2 -> T3` (rw-antidependencies; `T1 == T3` allowed) where T3 commits first. "Commits first" is decided by `commit_ts` for committed txns and by prepare order (§8.4) for prepared ones.
- **Read-only exception:** if T1 is declared `READ ONLY` (or has done no writes and is committing), the structure is dangerous only if `commit_ts(T3) <= S(T1)`.
- **Committed T2.** When T2 has committed, the `T2 -> T3` half of the structure is tested only through `earliest_out_conflict_commit(T2)` (§8.5): an edge `T1 -> T2` is dangerous if that value is set and (T1 is not read-only, or the value is `<= S(T1)`). This is PostgreSQL's rule (`CheckForSerializableConflictOut`); it is conservative, and it does not depend on T3's SSI state still being kept.
- Victim: a txn that is not prepared, preferring T2 (the pivot) if it is not prepared, otherwise T1. If the only candidate is the txn running the check, it aborts itself with 40001. A victim other than the checker is marked `doomed` (under the SSI mutex).

### 8.4 Pre-commit
After deferred constraint checks (§5.3), a SER txn T commits by, **in one critical section under the SSI mutex**: read its own `doomed` flag (set → 40001); run the dangerous-structure check over every structure containing T in any position, walking edges two hops in both directions (`X -> T -> Y`, `T -> Y -> Z`, `X -> Y -> T`); if it passes, mark itself `PREPARED` with `prepare_seq = ++prepare_counter`; for a DDL txn, run the SIREAD promotion (§8.6; the DDL-side check already ran at execution, §8.2); enqueue its commit request (carrying its TxnId for the writer map) on the commit thread's unbounded channel. Because enqueue happens under the mutex and the commit thread assigns `commit_ts` in channel order (§3), commit order equals prepare order. A prepared txn can no longer be chosen as a victim. A doomed txn raises 40001 at its next statement or at commit; the flag is only read under the SSI mutex.

Non-SER txns enqueue without taking the SSI mutex, except a non-SER DDL txn, which takes it to run the SIREAD promotion and enqueues inside that critical section.

### 8.5 Finding `writer(t)`
SSI keeps its own map `commit_ts -> TxnId` for SERIALIZABLE txns; the txn's SSI state is kept with the entry and retired with it (§8.6). Every SER txn T carries `earliest_out_conflict_commit(T)`: the smallest `commit_ts` of an X with a recorded edge `T -> X` that **committed before T** (`commit_ts(X) < commit_ts(T)`), or unset. It is updated under the SSI mutex: when an edge `T -> X` is recorded to an already-committed X, and when X's commit is processed (§3 step 3) for every T with an edge `T -> X`; in both cases only if `commit_ts(X) < commit_ts(T)`, where an unassigned `commit_ts(T)` counts as infinity. (Commit timestamps are assigned to a whole group in step 1 before step 3 runs, so "T already has a `commit_ts`" is not the test: T3 and T2 in one group, T3 first, must still set T2's value.) An X that commits after T never sets it (PostgreSQL's `earliestOutConflictCommit`, and the "writer committed before T2" exemption of `OnConflict_CheckForSerializationFailure`). SSI state is never summarised in 2.0; it spills to disk under memory pressure (§10). The commit thread inserts the entry in §3 step 3, before status is set, so the entry exists before any version `@t` can (resolution follows visibility). An entry is kept while it can matter (§8.6). A `t > S(R)` with no entry was written by a non-SER txn, and the edge is ignored (§8.2). A SER writer's entry cannot be missing for a live reader R that skips `t > S(R)`: §8.6 keeps it while any SER snapshot below `t` is registered. This map is independent of the status table (§7.4).

### 8.6 Retention and abort
- The SIREADs, conflict info and writer-map entry of a committed SER txn T are kept until **both** `visible_ts >= commit_ts(T)` and every SER txn with a registered snapshot `S < commit_ts(T)` has ended. Evaluated under the registry mutex (§3.1), where taking `S` and registering it are one step; after `visible_ts >= commit_ts(T)` no new snapshot can have `S < commit_ts(T)`.
- An aborted txn's SIREADs and edges are removed when it aborts.
- When a relation's storage is retired (DROP, TRUNCATE, rewrite), finer-grained SIREADs on the retired ids are converted to relation-level SIREADs keyed by the relation oid. The retired ids are taken from the DDL txn's own in-memory catalog changes (no catalog read, so no registry access under the SSI mutex). The conversion runs in the DDL txn's pre-commit critical section under the SSI mutex, before its commit request is enqueued (§8.4), so no SIREAD on a retired id survives into a state where the new ids are in use.
- Read-only txns: the safe-snapshot optimisation and `DEFERRABLE` are not implemented (§12). The dangerous-structure check runs only at pre-commit (§8.4); PostgreSQL also checks when an edge is created, which only fails earlier.

## 9. GC

### 9.1 Watermark
`computed` = min over: registered snapshots and caller-chosen ts (§3.1: txns, cursors, portals, `AS OF` reads, segment builds), the `vts` of every open view (§3.1), `visible_ts`, and the `AS OF` retention window converted to ts through `/sys/ts_clock`: the largest sampled ts whose wall time is at or before the cutoff, or 0 if no sample qualifies (a clock that goes backwards can only lower it). In **one** registry critical section the GC job computes it and publishes `W = max(old W, computed)`. Then:
- `W <= visible_ts`, and `W` is monotonic.
- Every registration with a caller-chosen ts is checked against the published `W` (§3.1).
- The new `W` is persisted to `/sys/gc_w` with `Durability::Yes` (synced) before any GC step (filter, DeleteRange) uses it; the compaction filter is only ever handed a `W` that is already durable. So after a crash, boot loads a `W` at least as large as any `W` a GC step acted on, and AS OF registration (§3.1) can never be admitted below data GC already dropped. `set_gc_watermark` returns an error on a decrease and the caller treats that as fatal.
- There is no snapshot age cap (PostgreSQL 17 removed `old_snapshot_threshold`). A long-running txn holds `W` back; it is exported as a metric.

### 9.2 Rules
- Keep, for every logical key, every version `> W` and the newest version `<= W`. Intents are never removed by GC rules; only by §7.3.
- **A tombstone may be dropped only if a newer version `<= W` of the same key is kept, or together with every older version of its key.** With timestamps inside the key, versions of one logical key can span SST files, so a compaction filter that drops the newest tombstone `<= W` can resurrect an older version sitting in another file. Therefore:
  - **Backend with user-defined timestamps (RocksDB UDT + `full_history_ts_low = W`):** the engine owns version GC and keeps one user key's versions together. Preferred if C-S1 confirms UDT works with DeleteRange, iterators at a read ts, and checkpoints, and §12 Q2 specifies its layout.
  - **Otherwise (ts in key: fjall, RocksDB without UDT):** the compaction filter may drop any version (tombstone or not) when it has already seen, earlier in the same compaction stream, a newer version `<= W` of the same logical key. It never drops the newest version `<= W` it has seen for a key. Filter state never crosses streams or subcompactions. The C-B1 GC job removes a newest-`<= W` tombstone or moved-tombstone `k@t` with `DeleteRange([L ‖ 0x01 ‖ be64(u64::MAX - t), end(L)))` (§2.2), start inclusive. Because versions sort newest first, this range covers `k@t` and every older version.
- **DROP / TRUNCATE / table rewrite** retire the relation's storage ids (table and every index) and, for TRUNCATE and rewrite, allocate new ones, recorded in the versioned catalog. Once `W >` the DDL's commit ts: first remove every intent under each retired prefix through §7.3 (so counts and truncation stay exact), then `DeleteRange` each retired prefix. Storage ids are never reused.

**I-GC.** For every registered snapshot or caller-chosen ts (all are `>= W`), the read path (§4) returns the same result before and after any GC step; and every open view returns the same result for every key before and after any GC step that runs while it is open.

Unresolved committed intents are safe: GC only drops versions older than a kept version, and the intent's eventual version is newer than all of them.

## 10. Other rules
- **Storage ids:** u64, allocated from a persisted counter, never reused. Catalog rows map relation and index oids to their current storage ids.
- **Sequences:** non-transactional; the high-water mark is persisted (synced) before a value from a new block is handed out. Values may skip, never repeat.
- **Catalog:** rows in `/sys/catalog`, versioned like any table; DDL is transactional and takes AccessExclusive. The GC rules (§9.2) apply to data prefixes and `/sys/catalog/` only; every other `/sys/` key is never dropped by the filter, even if its bytes match the version-key pattern.
- **Large txns:** intents, write-set log and SIREAD state all spill; the 10M-row gate measures commit latency, reader latency during resolution and abort cleanup time.

## 11. Invariants and the G0 model
Checked by G0: `I-WAL-ORDER`, `I-VIS`, `I-ACK`, `I-SNAP-ORDER`, `I-HALLOWEEN`, `I-ONE-INTENT`, `I-WW`, `I-LOCK`, `I-UNIQUE`, `I-TRUNC`, `I-SSI-ORDER`, `I-GC`, plus:
- **I-ATOMIC.** After any crash, for every txn: all of its writes are visible at `visible_ts`, or none are.
- **I-DURABLE.** An acked `synchronous_commit=on` commit survives any crash.
- **I-SER.** Histories under SERIALIZABLE are serializable (Elle, G2), including predicate/phantom workloads and the read-only anomaly.
- **I-NOBLOCK.** No read-only path (§4) waits on another txn.
- **I-LIVE.** (a) No waiter stays parked after the releasing actor's sequence completes (commit step 5, the end of §7.1, a `ROLLBACK TO`, a lock release) if none of its blockers then holds a conflicting lock; states inside that sequence are exempt; (b) every wait-for cycle is broken with exactly one 40P01; (c) a 40P01 is raised only on a cycle whose every edge is a current conflict (no stale edge).
- **I-RC-MONO.** An RC statement sees every commit whose effects an earlier statement of the same session observed (via a conflict, a wait or a read).
- **I-PROGRESS.** Every retry of a loop that does not park (§5.1 `continue` after a removal, EPQ repeat, arbiter restart) is preceded by a wake or by a change of committed or intent state made by another actor. G0 checks it by lasso detection over an abstraction of the global state that drops monotone counters (`seq`, `sa`, `view_counter`, `gen`, `prepare_counter`, `ts`): an abstract state revisited inside a retry loop with no intervening foreign step is a violation. As a backstop, a txn that retries more than a fixed bound without a foreign step in between is a violation.
- **I-FK.** For every ts `S`, the committed versions `<= S` contain no live child row that, under the FK's MATCH semantics (SIMPLE: any NULL column exempts the row; FULL: all-NULL exempts, partial NULL is rejected at write), references no live parent row. Applies to validated, enabled FKs (not `NOT VALID` ones).
- **I-COUNT.** For every current-epoch txn T, at every state where no placement or removal of T is between its KV write and its count update: `intent_count(T)` equals the number of `k@INTENT` entries owned by T in the KV, and the write-set log names each of them.
- **I-GC-QUIESCE.** After the async resolver has drained (no intents remain) and the GC job (over the whole keyspace) and a full compaction run with `W` fixed and no txn active, no key has a tombstone or moved-tombstone `<= W` as its newest version `<= W` (GC actually removes what it may, not only never removes too much).
- **I-SSI-EDGES.** Every recorded rw-edge `R -> W` joins two concurrent SER txns (neither committed at or before the other's snapshot), and the recorded edge set equals the set derived from the **SIREAD footprint**, evaluated over time: for each data-changing placement of W (including layers later rolled back), the SIREAD bounds R had registered at the moment that placement ran its SIREAD check (including escalated and relation-level ones, after any shrink that had already happened); for each DDL of W, R's SIREADs on the relation at DDL execution; plus the intents and versions R skipped (§4). Edges involving a txn that aborted are excluded on both sides. G0-ssi computes that set as an oracle and checks both directions. Catches over-eager and missing edges, which I-SER alone cannot.
- **I-SSI-PRECISION.** Every 40001 raised by the dangerous-structure check (§8.3) has a recorded structure `T1 -> T2 -> T3` (T1 == T3 allowed) in which T3 committed or prepared first among the distinct members, with `commit_ts(T3) <= S(T1)` when T1 is read-only, and for a committed T2 the `earliest_out_conflict_commit` that fired is the commit of such a T3. In particular, a schedule where every txn is SERIALIZABLE and no two overlap never raises it. (Other 40001 sources, I-WW and §4, are outside this invariant.)

The state space is too large for one exhaustive model, so G0 is four small-scope exhaustive models plus a deterministic simulator. The exhaustive models are pure state machines whose state is a value (cloneable, hashable, deduplicated by fingerprint), so their KV is an abstract ordered map with exactly the `OrderedKv` semantics the model relies on (batches atomic, unsynced suffix lost at crash, snapshot = a copy); G0-gc's LSM must reproduce MemKv LSM mode's semantics and is checked against the C-K3b LSM conformance cases. The simulator runs the real `nucleus-txn` code over MemKv. All models may start from a preloaded KV state (preloaded data does not count against the txn budget), and every actor is a separate step that can interleave: txn steps, the commit thread with **status-set and visible-advance as separate steps**, async resolver, inline removal, abort cleanup, GC job, compaction, crash.

**Model discipline.** Values that the protocol only compares (LSNs as pending-or-durable, `view_counter` and the counters derived from it) are renumbered by rank after every step, which preserves every guard and check. A step that changes no shared state (a snapshot read through an open view) is not a step: the invariant is checked in every state where the step could run. Each model has a **fixed workload** (the exact statements of each txn, written down in the model's source) rather than generated programs, and partial-order reduction over independent steps. Each run reports its state count, so a change that silently shrinks the explored space shows up. The seed table below names, for every seed, the model and the workload that catches it; a seed whose workload cannot express the bug is a G0 defect. Steps a seed depends on happening late (for example the insert in seed 28) are separate schedulable steps, never fused into an earlier step.

| Model | Scope | Invariants |
|---|---|---|
| G0-commit | 2 writers (W0 sync on over k0,k1; W1 sync off, may abort, over k0 **or**, by an initial choice, the disjoint k2: either two removers of one intent or a two-request commit group) + 2 snapshot readers (one the FK-style unlatched read), commit thread, async resolver + one inline remover, abort cleanup, truncation incl. the `released` condition and older-epoch records, crash at every KV write and fsync with any unsynced suffix lost, boot sweep. A third writer (both behaviours at once) needs a disk-backed frontier; not run in 2.0 | WAL-ORDER, VIS, ACK, SNAP-ORDER, TRUNC, COUNT, ATOMIC, DURABLE |
| G0-write | 2 txns (3 for the KEY SHARE workload: a key-changing then a non-key commit above `S`), 2 keys (+1 unique key, +1 deferrable unique key, 1 parent/child FK pair), status truncation with remembered-TxnId lookups, 2 statements and 1 savepoint per txn (rollback-to releasing data, exclusive and shared locks), 1 shared mode, relation locks, waits + DFS + wake generations + cancel, RC EPQ with the unlatched gap, ON CONFLICT DO UPDATE, commit thread, resolver and abort cleanup | ONE-INTENT, WW, LOCK, UNIQUE, FK, HALLOWEEN, ATOMIC, LIVE, PROGRESS, RC-MONO, TRUNC, COUNT |
| G0-ssi | 3 txns (one may be SER READ ONLY; one of the three may instead run a TRUNCATE), 2 keys + 1 range + 1 index→row fetch, 2 statements each, relation locks and the §4 storage-id rule, pre-commit with the channel send as a separate step, commit thread draining groups of up to 2 requests (status-set and visible-advance per request), resolver, writer map with retention, registry | SSI-ORDER, SER (cycle check over the recorded history), SSI-EDGES, SSI-PRECISION |
| G0-gc | 2 txns + 1 reader, 1-2 keys, MemKv in **LSM mode** (≥ 2 levels, per-file compaction streams, range tombstones; a compaction-filter drop is **visible to already-open snapshots**, the adversarial choice the kv contract allows), GC job, registry, AS OF registration with catalog-at-`t`, one DROP or TRUNCATE, one latest-state view (a deferred FK check) open across compaction, crash between `W` publish and its sync, W recomputation after a simulated restart and a widened AS OF window | GC, GC-QUIESCE, ATOMIC, TRUNC |

The deterministic simulator runs the full pipeline with larger scopes under seeded schedules.

**Seeded bugs.** G0 must catch every one of these, each reported by the model named:
1. Reader opens its view before reading `S` (commit).
2. Status truncated without condition 2 (commit).
3. Compaction filter drops the newest tombstone `<= W` (gc).
4. Intent placed without the latch (write).
5. SIREAD registered after iterating (ssi).
6. SIREAD registered after the scan's view opened (ssi).
7. `visible_ts` advanced before status is set (commit).
8. Ack before fsync for `synchronous_commit=on` (commit).
9. Commit records written out of `commit_ts` order (commit).
10. `/sys/txn` delete written before resolution (commit).
11. Async resolution or abort cleanup without the latch/owner check (write).
12. A foreign visible-committed intent overwritten without first removing it (write).
13. `intent_count` decremented on a removal that wrote nothing (commit).
14. View counter taken after the view opens (commit).
15. Lock-only request replaces own data (write).
16. Savepoint rollback drops the lock with the data (write, I-LOCK).
17. `wait_for` without the generation re-check (write, I-LIVE).
18. Writer proceeds on `Committed(c)` with `c > visible_ts` (write, I-RC-MONO).
19. Unique check ignores own intents (write).
20. SSI pre-commit check not atomic with enqueue (ssi).
21. Taking `S` and registering it are separate steps (gc, and ssi retention).
22. `W` allowed to decrease (gc).
23. `intent_count` decremented before `last_removal_counter` is set (commit).
24. Unique check treats an own `Absent` layer as not live (write).
25. Deferrable `/i/` removal latched on the entry key instead of the prefix (write).
26. Wake lost across `ROLLBACK TO` (status-only re-check) (write, I-LIVE).
27. Async resolution queued before `visible_ts >= commit_ts` (write, I-RC-MONO).
28. Writer-map entry inserted after status is set (ssi).
29. SSI retention ignores `visible_ts` (ssi).
30. Index→row fetch reuses the scan's view (ssi).
31. AS OF registration checked against an unpublished `W` (gc).
32. TRUNCATE without the relation-level SIREAD check (ssi).
33. Compaction-filter state shared across streams (gc).
34. GC DeleteRange with an exclusive start (gc).
35. Retired-prefix DeleteRange without removing intents first (gc, I-TRUNC).
36. Index→row fetch reads the latest state without a registered view (ssi, and commit I-TRUNC).
37. `wait_for` treats a missing status as Pending (write, I-LIVE).
38. Status truncated before the txn is `released` (write).
39. Writer-side edge from a SIREAD holder that committed at or before `S(W)` (ssi, I-SSI-EDGES).
40. Edge `T2 -> T3` dropped when T3's SSI state is retired; committed T2 tested through its edge list (ssi, read-only anomaly).
41. (withdrawn in draft 5: promotion timing is unobservable while the reader holds AccessShare; covered by a unit test of §8.6)
42. Compaction filter handed `W` before `/sys/gc_w` is synced (gc, crash).
43. Pre-commit checks only structures with the committer as pivot (ssi).
44. Unlatched FK parent read without a registered view (commit, I-TRUNC).
45. KEY SHARE checks only the newest version above `S` (write).
46. Shared row lock released without the key's latch (write, I-LIVE).
47. RC EPQ places its intent without re-verifying under the latch (write, I-ONE-INTENT).
48. Cancel does not wake a parked waiter (write, I-LIVE).
49. AS OF resolves the latest catalog instead of the catalog at `t` (gc).
50. Write-set log records only new intents, so `ROLLBACK TO` misses a later layer on an existing intent (write).
51. Older-epoch record truncated without the removal/sweep view-counter condition (commit, I-TRUNC).
52. EPQ repeats on a non-conflicting foreign intent instead of only on a changed version (write, I-LIVE).
53. ON CONFLICT lock runs EPQ and skips a deleted conflicting row (write).
54. Abandoned ON CONFLICT attempt removes whole intents instead of rolling back to its internal savepoint (write).
55. Latest-state view does not hold `W` (gc, I-GC).
56. ON CONFLICT attempt writes at `seq0` instead of `sa`, so abandoning leaves its new arbiter intent (write).
57. Arbiter lock measures newer versions against `S` instead of `v_r` (write, I-PROGRESS).
58. FK child check skips on a failed EPQ instead of raising 23503 (write, I-FK).
59. Waker leaves woken waiters' edges in the graph (write, I-LIVE c).
60. DDL-side SIREAD check run at pre-commit after `PREPARED` instead of at DDL execution (ssi, I-SER).
61. `earliest_out_conflict_commit` set by an X that commits after T (ssi, I-SSI-PRECISION).
62. SER `WITH HOLD` cursor read lazily after commit (ssi, I-SER).
63. `earliest_out_conflict_commit` update skipped because T already has a group-assigned `commit_ts` (ssi, I-SER: T3 and T2 in one group, read-only T1 snapshot between their visible-advances).
64. ON CONFLICT attempt's queued checks or AFTER events tagged `seq0`, surviving an abandon (write).

## 12. Open questions and known divergences
Open:
1. RC joined DML: full EPQ vs statement retry (G3c decides). Deferred to the executor cards: C-T2 implements single-row EPQ (§5.2) only.
2. RocksDB UDT viability (C-S1), and if viable the UDT layout of intents, tombstones and moved-tombstones; otherwise the ts-in-key GC path is mandatory.
3. Whether `synchronous_commit=off` commits may be visible to `on` sessions before fsync (PostgreSQL: yes). Current spec: yes.
4. Confirm the SERIALIZABLE unique-violation rule of §5.3 against PostgreSQL's isolation tests (G3c). C-T2 implements the rule as written; G3c gates release, not C-T2.
5. Session-scope advisory locks: wake generations and the wait-for graph are per txn. Deferred.
6. `SERIALIZABLE READ ONLY DEFERRABLE` and the safe-snapshot optimisation (§8.6). Deferred; `DEFERRABLE` is accepted and ignored.
7. Relation SIREADs (§8.1) are keyed by relation oid, but a writer has only the raw key: the mapping from storage id to relation is maintained by the catalog layer and handed to SSI.

Known divergences from PostgreSQL 17:
- A concurrent primary-key change seen by RC EPQ raises 40001 instead of following the update chain (§5.2).
- Under RR/SER, accessing a relation that was truncated or rewritten after `S` raises 40001; PostgreSQL returns the new (possibly empty) contents, which its documentation calls not MVCC-safe (§4).
- `DEFERRABLE` primary keys are refused with 0A000 (§5.3). Deferrable unique constraints are supported.
- `AS OF` (not a PostgreSQL feature) is refused inside SERIALIZABLE txns (§4).

## Changelog

- **Draft 7.3** (2026-10-08, from drafting C-T2..C-SIM): §3.1 lock-table, waiter-table and status mutexes are leaves. §6 the waiter's re-check runs in the same graph critical section as the edge insert (otherwise a stale edge can give a false 40P01); `ROLLBACK TO` releases relation locks taken after the savepoint; queues are not fair; advisory locks xact scope only. §8.6 safe snapshots and `DEFERRABLE` not implemented; check at pre-commit only. §9.1 `/sys/ts_clock` conversion defined. §10 GC rules skip `/sys/` except the catalog. §12 Q1 deferred to executor cards, Q4 gates release not C-T2, Q5-Q7 added.
- **Draft 7.2** (2026-10-08): §2.2 version keys end in a `0x01` tag byte so a raw key splits from the right without the schema (found in C-T1a review: a length-prefixed `L` broke SQL key order). Ordering, `end(L)` and the GC range are unchanged.
- **draft 7.1 (2026-10-07), G0-commit built:** §11 exhaustive models run over a pure-value abstract KV (MemKv is for the simulator); rank renumbering and read-as-check reductions stated; G0-commit scope is 2 writers with a write-set choice + 2 readers (3 txns at once deferred). 8.39M states, all 11 owned seeds caught.
- **draft 7 (2026-10-07), round-6 delta review** (1 critical, 4 major, 2 minor; 3 round-5 residues):
  - §8.5 `earliest_out_conflict_commit` condition is `commit_ts(X) < commit_ts(T)` with unassigned = infinity, not "T uncommitted" (group-assigned ts) (R6-1, seed 63; G0-ssi groups of 2).
  - §5.3.1 only the main statement's writes use `sa`; internal commands take fresh seqs above it; queued events tagged `sa` (R6-5, seed 64). §3.1 deferred triggers and holdable-portal materialisation loop to a fixpoint (R6-6).
  - §11 I-SSI-PRECISION allows T1 == T3 and prepare order (R6-2); I-SSI-EDGES footprint evaluated at each placement's check time, with DDL edges, aborted txns excluded (R6-3, G0-R5-2); I-PROGRESS lasso over an abstraction without monotone counters plus a retry bound (R6-4, R5W-5); I-UNIQUE restated as uniqueness plus index consistency (R5W-9); I-FK scoped to validated FKs with MATCH semantics (R6-7).
- **draft 6 (2026-10-07), round-5 adversarial review** (10 write/commit findings R5W-*, 4 SSI/GC findings R5S-*, 3 G0 findings):
  - §5.3.1 attempt writes use `sa` for layer placement (always push), `data_seq` stays `seq0` (R5W-1, seed 56); the arbiter lock measures newer versions against `v_r`, read through a registered view (R5W-2, R5W-7, seed 57). §5.1 newer-version base is `S`, the EPQ-evaluated version, or `v_r` (R5W-3); key-existence ops always run the unique check (R5W-10). §5.3 FK child: failed EPQ raises 23503, moved parent 40001 (R5W-6, seed 58, I-FK).
  - §6 wakers remove woken waiters' edges under the graph mutex before waking (R5W-4c, G0-R5-3, seed 59). §11 I-LIVE(a) measured at the end of the releasing actor's sequence (R5W-4a); I-PROGRESS with lasso detection (R5W-5). §7.4 older-epoch txns count as released (R5W-8). I-UNIQUE wording (R5W-9).
  - §8.2 DDL-side SSI check at DDL execution, before the txn can be prepared (R5S-1, seed 60). §8.5 `earliest_out_conflict_commit` counts only X committed before T and freezes at T's commit (R5S-2, seed 61, I-SSI-PRECISION restated). §3.1 WITH HOLD cursors materialised before pre-commit (R5S-3, seed 62). §3.1 `vts` argument restated (R5S-4).
  - §11 G0-gc LSM mode exposes filter drops to open snapshots (G0-R5-1); I-SSI-EDGES oracle over the SIREAD footprint, both directions (G0-R5-2).
- **draft 5 (2026-10-07), round-4 adversarial review** (15 write/commit findings R4W-*, 6 SSI/GC findings R4S-*, 4 G0 findings):
  - §8.2/§8.3 edges carry the target's `commit_ts`; a committed T2 is tested only through `earliest_out_conflict_commit`, so retiring T3 cannot hide the read-only anomaly (R4S-1, seed 40). §8.3/§8.5 summarisation removed; SSI state spills instead (R4S-2). §3.1/§9.1 every open view registers `vts`, which holds `W`; I-GC covers open views (R4S-3, seed 55). §4 AS OF refused under SERIALIZABLE (R4S-4). §8.6 DDL promotion takes retired ids from the txn's own catalog changes (R4S-6). R4S-5 (kv signature/doc) is card C-K3b.
  - §7.3/§7.4 older-epoch removals set the removal counter; the sweep records `sweep_counter`; truncation waits for views past both (R4W-1, seed 51). §5.2 EPQ repeats only when the newest version changed (R4W-2, seed 52). §5.1/§5.5 write-set log entry per layer change (R4W-3, seed 50).
  - §5.3.1 rewritten: each attempt under an internal savepoint, abandon = rollback to it with a wake (R4W-5, R4W-7, seed 54); the arbiter lock never runs EPQ and restarts on any change (R4W-4, seed 53); DO UPDATE evaluates the locked version (R4W-6); the pre-check removes visible-committed and aborted intents first (R4W-8).
  - §2.1 layer seq is `max(command seq, top.seq)` with `data_seq` = the writing command, matching PostgreSQL's `es_output_cid` after BEFORE triggers (R4W-9). §5.3 I-UNIQUE stated over committed state (R4W-10); deferrable prefix check uses each entry's current state (R4W-14); FK child KEY SHARE rule aligned with §5.1 (R3W-3 residue). §6 `gen` lives in the status entry; missing status checked first (R4W-15).
  - §11 I-LIVE strengthened (stale edges, parked past the conflict) (R4W-12); I-SSI-EDGES oracle added and I-SSI-PRECISION restricted to the SSI check (G0-R4-1); I-GC-QUIESCE preconditions (G0-R4-4); G0-write gets 3 txns for KEY SHARE and truncation (R4W-11, R4W-13); G0-ssi gets relation locks and the §4 rule (G0-R4-2); seed 41 withdrawn, seed 50 rewritten, seeds 51-55 added (R4W-13, G0-R4-2, G0-R4-3).
- **draft 4 (2026-10-07), round-3 adversarial review** (15 write/commit findings R3W-*, 9 SSI/GC findings R3S-*, 3 G0 findings):
  - §8.1 The latest-state alternative of I-SSI-ORDER is removed; every SER read uses a view opened after its SIREAD (R3S-1, seed 36). §8.3/§8.5 summarised committed txns keep `earliest_out_conflict_commit`, with PostgreSQL's dangerous-edge rule (R3S-2, seed 40). §8.2 SIREAD holders committed at or before `S(W)` give no edge (R3S-3, seed 39, I-SSI-PRECISION).
  - §8.2/§8.4/§8.6 retired-id SIREAD promotion runs in the DDL txn's pre-commit critical section, for every isolation level (R3S-4). §2.3/§9.1 `/sys/gc_w` synced before the filter sees `W` (R3S-5). §4 AS OF reads the catalog at `t` (R3S-6). §8.4 pre-commit checks every position two hops both ways; `doomed` read under the mutex (R3S-7). §3.1 global lock order (R3S-8). R3S-9 (kv `GcStream` doc) is fixed in code by card C-K3b.
  - §4/§6/§7.4 a missing status for a remembered TxnId means ended; truncation requires `released` (R3W-1, seeds 37, 38). §3.1 every unlatched read registers a view (R3W-2, seed 44).
  - §2.1 `key_changed` relative to the newest committed version and sticky; delete + re-insert counts; §5.1/§5.2 KEY SHARE and EPQ examine every version above `S` (R3W-3, seed 45). §6 shared locks released under the latch (R3W-4, seed 46). §5.3.1 ON CONFLICT arbiter protocol; MERGE inserts raise 23505 (R3W-5). §5.3/§5.4 FK checks, referential actions and triggers run at a fresh seq; own-row rules only for data-changing row ops (R3W-6). §5.3 three constraint timings; deferrable PK refused; NULLs skipped by the deferrable prefix check (R3W-7).
  - §5.1 key-existence ops do not wait on lock-only intents over a live row (R3W-8). §5.2 EPQ runs unlatched and re-verifies (R3W-9, seed 47). §5.1/§7.3 inline removal runs steps 2-4 under the held latch (R3W-10). §6 cancel wakes parked sessions (R3W-11, seed 48). §5.1 placement logged and counted before its write; failed writes are fail-stop (R3W-12, seed 50, I-COUNT). §6 ended holders never conflict (R3W-13). §5.3/§5.5 ROLLBACK TO discards queued checks and trigger events (R3W-14). §7.3 older-epoch removals skip the count update (R3W-15).
  - §11 I-COUNT, I-GC-QUIESCE, I-SSI-PRECISION; fixed workloads, seed→workload mapping, state counts, late-schedulable steps; G0 scopes gain TRUNCATE within the 3-txn budget, relation locks, savepoint lock release, EPQ gap, ON CONFLICT, cancel, `W` sync crash point (G0-R3-1..3). Seeds 36-50.
- **draft 3 (2026-10-07), round-2 adversarial review** (18 write/commit findings R2W-*, 8 SSI/GC findings R2S-*, 5 G0 findings):
  - §5.1 Foreign visible-committed or aborted intents are removed by their own batch under the latch before anything else; nothing is planned without being written; `intent_count += 1` before placement (R2W-1).
  - §7.3 step 4: removal counter set before the count is decremented, both under the registry mutex; §7.4 reads them under the same mutex (R2W-2).
  - §5.3 Unique check treats an own `Absent` layer as "use committed state" (R2W-3).
  - §5.0 One `latch_key(k)` for every path; deferrable `/i/` keys latch the prefix; never two latches (R2W-4).
  - §5.1 Foreign Pending intents are waited on only if the requested mode conflicts with their lock; requested modes listed in §6 (R2W-5, closes F02).
  - §5.3 FK child side reads the parent first, then takes KEY SHARE; constraint timing honours deferral and `SET CONSTRAINTS`; NO ACTION checks for a replacement parent (R2W-6, R2W-7).
  - §2.1 Layers carry `data_seq`; §5.4 own-row rules use it (skip / 21000 / 27000); lock-only layers do not count (R2W-8, R2W-18).
  - §3 step 5: in-memory locks released and resolution queued only after `visible_ts >= commit_ts`; §3.2 no version newer than `visible_ts` exists (R2W-9, R2W-10).
  - §6 Per-txn wake generation read under the latch and re-checked after registering; `wait_for` returns only for aborted or visible commits (R2W-11, R2W-15, closes F08/F10).
  - §5.4 No `t* > S` check on a key with an own intent (R2W-12).
  - §2.1/§2.2 `Write{key_changed}` and `Delete{moved}`, carried into a version value header (R2W-13, closes F16).
  - §4 RR/SER rule for truncated/rewritten relations defined and listed as a divergence; new indexes not used by older snapshots (R2W-14).
  - §3 boot sets `visible_ts = ts_hwm` (R2W-16). §6 only the owning session sets Aborted (R2W-17).
  - §8.6 SSI retention also waits for `visible_ts >= commit_ts` (R2S-1). §8.1 I-SSI-ORDER restated per key read, with index→row fetches and the latest-state alternative (R2S-2). §9.1 `W` published in the registry critical section; caller-chosen ts checked against it (R2S-3). §3 step 3 / §8.5 writer map filled by the commit thread before status; unbounded channel (R2S-4). §8.2 DDL conflict-in, relation SIREADs keyed by oid, retired-id SIREADs promoted (R2S-5). §9.2 retired prefixes: intents removed through §7.3 before DeleteRange (R2S-6). §2.2 UDT layout moved to open question Q2 (R2S-7). §4/§8.2 no edges to aborted txns (R2S-8).
  - §5.1 I-LOCK and §11 I-RC-MONO added; G0 models gain split commit steps, two removers, wake generations, deferrable keys, index→row fetch, TRUNCATE, W recomputation; seeds 23-35 added (G0-a to G0-e).
- **draft 2 (2026-10-07), round-1 adversarial review** (`docs/C-T0-review.local.md`, 34 findings):
  - §2.1 Intent is a stack of layers, each holding data and the strongest exclusive lock. Lock-only requests never replace own data; savepoint rollback restores data and lock; write strength is derived from key-column changes (DATA-1, F01, F02, F11). Key column defined (F02).
  - §1 Versions sort newest first; seq never decreases; storage ids in keys (GC-4, F19, GC-3). §2.2 version key layout and GC range bytes (GC-4).
  - §3 Commit thread never aborts; channel order is commit order. §3.1 registry: `S` read and registered atomically; view counters registered before views open (GC-1, TRUNC-2, SSI-2). Committed-but-not-visible is treated as Pending by writers (F07).
  - §4 Readers decide on top-layer data; missing current-epoch status is fatal; AS OF below `W` errors with 72000; SER scans open their own view (TRUNC-2, GC-2, SSI-1).
  - §5 One fresh view for intent + newest version under the latch; foreign intents removed before placement; `t*` ignores lock-only intents; key-existence ops use the unique check instead of the `t* > S` rule; shared locks run the version check; SIREAD check skipped for lock-only placements (F04, TRUNC-1, F06, F13, F14, F15, SSI-6).
  - §5.1 PK-moving updates leave a moved-tombstone; EPQ raises 40001 on it (F16).
  - §5.2 Unique check sees own intents; SER 40001 only with a covering SIREAD; PK uses the same check; NULLS NOT DISTINCT; deferrable unique uses a prefix latch; FK checks with RC/RR/SER rules and detectNewRows (F03, F13, F21, F17, F05, SSI-3).
  - §5.3 Rollback rules: re-check owner, keep SIREADs, close later portals, wake all waiters, locks tagged with execution seq; exact layer-compaction rule (F10, F12, F19, SSI-5). §5.4 own-row revisit (F18).
  - §6 One wait-for graph over all lock kinds; DFS and edge removal atomic (F08, F09, F20).
  - §7.3 Every intent-removal path latches and checks the owner (TRUNC-1, F04, TRUNC-2).
  - §8 SIREAD before view; atomic pre-commit + prepare + enqueue; exact read-only exception; T1 == T3; own `commit_ts -> txn` map independent of truncation; non-SER edges ignored (SSI-1, SSI-2, SSI-4, SSI-5, SSI-6).
  - §9 `W` is a pure min, monotonic, persisted, `<= visible_ts`; no snapshot age cap; shadowed tombstones may be dropped; DeleteRange bounds explicit; DROP/TRUNCATE retire storage ids (GC-1, GC-2, GC-3, GC-4).
  - §11 G0 split into four models with LSM-mode MemKv and interleaved actors; seeded bugs extended from 5 to 22 (G0-SEEDS, G0-1).
