# C-T0: Transaction protocol (normative)

Status: draft 4 (adversarial review rounds 1-3 applied; changelog at the bottom). Every `nucleus-txn` card implements against this file. A change to an invariant (`I-*`) needs a spec change first, then the G0 model updated, then code.

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
- New state at command `seq`: if `top.seq == seq`, modify the top layer in place; otherwise push a new layer.
- Shared modes (KEY SHARE, SHARE) are never stored in intents (§6).

Foreign readers and resolution look only at the **top layer's `data`**. `Absent` means "lock only": no version is produced, nothing becomes visible.

### 2.2 Version key and value layout (ts-in-key backends)
`L` = the encoded logical key, prefix-free by C-Q3s P-PREFIX, so no other logical key starts with `L`.
```
intent        L ‖ 0x00
version @ts   L ‖ 0x01 ‖ be64(u64::MAX - ts)      # newest first
end(L)        L ‖ 0x02                            # exclusive upper bound of every entry of L
version value header ‖ payload
  header 0x00 live              payload = row/entry bytes
         0x01 live, key_changed payload = row/entry bytes
         0x02 tombstone         no payload
         0x03 moved-tombstone   no payload
```
`[L ‖ 0x00, end(L))` holds exactly the intent and versions of `L`. The GC range for tombstone `L@t` is `[L ‖ 0x01 ‖ be64(u64::MAX - t), end(L))`. Resolution maps `Write{key_changed}` to header 0x00/0x01 and `Delete{moved}` to 0x02/0x03.

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
- **Opening a view:** under the registry mutex, take `c = ++view_counter` and register `c`; release the mutex; then open the KV snapshot/iterator. Unregister `c` when the view closes.
- A txn's snapshot stays registered until the txn ends (RR/SER) or until its statement ends (RC). An open cursor or portal keeps its snapshot registered until it closes.
- **Every KV read made outside a latch section opens a registered view**: scans, point reads, FK parent and child reads, constraint-check scans, EPQ re-reads, SSI fetches. A read made under `latch(latch_key(k))` may read the latest state of `k` directly: every removal of `k@INTENT` takes the same latch (§7.3), so the intent cannot be removed and its owner's status cannot be truncated while the read runs.

**Lock order.** Latch → registry mutex → SSI mutex → wait-for-graph mutex. A thread holds at most one latch and never acquires an earlier lock in this order while holding a later one.

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

Reads at an explicit ts (`AS OF t`) register `t` as in §3.1 and then read as a snapshot at `t`, **with the catalog as of `t`** (relation and index storage ids and `built_at` resolved from catalog versions `<= t`). A relation that did not exist at `t` is 42P01. Because retired prefixes are deleted only once `W` passes the retiring DDL (§9.2) and `t >= W`, the storage the catalog at `t` names still exists.

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
  if W has an intent on k:      own-row rules (§5.4); the newer-version rule below is skipped
  row op (no own intent):
     N = DATA versions of k with ts > S (committed lock-only intents are not versions)
     if N is not empty:
        RR/SER: unlatch; raise 40001   (KEY SHARE: only if some version in N is a tombstone,
                                         moved-tombstone or key_changed write; otherwise proceed)
        RC:     EPQ (§5.2)
  key-existence op: unique check (§5.3); never 40001 from N alone, never EPQ
  shared lock: record it in the lock table (tagged with seq); unlatch; done
  if placing a new intent: append (seq, k) to the write-set log, then intent_count(W) += 1   (both before the write)
  build the new top layer (§2.1); write k@INTENT as one KV batch   # a failed write is fail-stop: the process aborts
  unlatch
  SSI: if the new layer changed data (not lock-only), check SIREAD locks covering k (§8.2)
```
Every early exit (40001, 23505, 21000, 27000, EPQ skip, NOWAIT, SKIP LOCKED, timeout, cancel) leaves the KV exactly as the removals already written left it; nothing is "planned" without being written. Because placement is logged and counted before its write and a failed write stops the process, the write-set log and `intent_count(W)` never undercount W's intents.

**I-ONE-INTENT.** At most one intent per logical key. Guaranteed because placement happens only under `latch(latch_key(k))` when no foreign intent exists on `k`: visible-committed and aborted ones are removed by a written batch first, and a Pending one always conflicts with any op that places an intent (every exclusive mode conflicts with `NoKeyUpdate`), so only shared locks, which write no intent, pass a foreign intent.
**I-WW.** Under RR/SER, a row op on a row with no own intent never succeeds when the row's newest data version is newer than `S`. RC instead serialises row ops through EPQ. Key-existence ops are governed by I-UNIQUE, not I-WW: inserting over a tombstone newer than `S` is allowed (PostgreSQL behaviour).
**I-LOCK.** No two txns hold conflicting row-lock modes on one key at the same time (intent top-layer `lock` of a Pending or not-yet-visible txn, plus the shared lock table).

### 5.2 RC EvalPlanQual
After a wait or when versions newer than `S` exist: remember the newest committed data version `v` (its ts and header) read under the latch, **unlatch**, re-evaluate the statement's quals for this row against `v` (PostgreSQL EvalPlanQual; this may run subqueries and functions, so it never runs under a latch). Fail: skip the row. Pass: compute the update from `v`, then re-enter §5.1; if under the latch the key now has a foreign intent or a newest data version other than `v`, repeat EPQ; otherwise place the intent. EPQ for KEY SHARE examines every version newer than `S`, not only the newest (§5.1). Joined DML (UPDATE FROM, DELETE USING, MERGE): full EPQ, or an internal statement retry with a fresh snapshot (capped retries, then 40001). Which one is decided by G3c and recorded here.

**Moved rows.** EPQ that reaches a moved-tombstone (as a version, or as a foreign `Delete{moved: true}` intent) raises 40001 ("tuple to be locked was already moved"). Divergence from PostgreSQL, which follows the update chain: §12.

### 5.3 Unique, primary key and FK
**Unique check** (key-existence ops, under the latch): the key's **current state** is the own intent's top-layer data if `W` has an intent on it and that data is `Write` or `Delete`; otherwise (no own intent, or own top data `Absent`) the newest committed data version.
- Live and belonging to a different row → 23505, except for an `INSERT ... ON CONFLICT` arbiter key, which follows §5.3.1. (A MERGE `WHEN NOT MATCHED THEN INSERT` that hits a unique conflict raises 23505, as in PostgreSQL.)
- Under SERIALIZABLE, if that live version has ts `> S` and `W` holds a SIREAD covering the key, raise 40001 instead of 23505. Otherwise 23505. (To be confirmed against PostgreSQL's isolation tests in G3c.)
- Not live (absent, tombstone, moved-tombstone, own `Delete`) → proceed, whatever its ts.
- Concurrent duplicate insert: the second inserter meets the first's Pending intent (always conflicting: inserts hold `NoKeyUpdate` or stronger) and waits. First commits: the second re-runs and gets 23505. First aborts: the second proceeds.

**I-UNIQUE.** No snapshot sees two live rows with equal values of a unique key (NULLS DISTINCT keys with a NULL excepted), and no `/u/` or `/t/` key has two live versions at any snapshot.

- **Primary key:** `/t/{rel}/{pk}` inserts and PK-changing updates run the same unique check on the new `/t/` key.
- **NULLs:** `NULLS DISTINCT` (default): if any key column is NULL, the entry goes to `/i/{idx}/{key}{pk}` (no uniqueness). `NULLS NOT DISTINCT`: NULL is encoded in the `/u/` key and checked like any value.
- **Deferrable unique constraints** use the non-unique `/i/` layout and the prefix latch key (§5.0). The check scans the prefix in the latest state under that latch: a foreign Pending or committed-not-visible entry → wait on its owner (it always conflicts), then re-check; a committed live entry of another row, or a second own live entry → 23505. Under `NULLS DISTINCT`, an entry whose key contains a NULL is never checked.
- **Deferrable primary keys** are refused with 0A000 in 2.0 (the `/t/` key is unique by construction and has no deferred layout; §12).

**Constraint timing.** Three timings, as in PostgreSQL:
1. **Per row, immediately:** non-deferrable unique and primary keys (the unique check runs inside §5.1 for each entry), NOT NULL and CHECK.
2. **End of statement:** `DEFERRABLE INITIALLY IMMEDIATE` unique constraints (or deferrable ones set `IMMEDIATE`), and every non-deferred FK check. End-of-statement checks see own writes with `seq <= current`.
3. **Commit:** constraints currently deferred (`INITIALLY DEFERRED`, or `SET CONSTRAINTS ... DEFERRED`). They run in the commit phase **before the commit request is enqueued** (and before the SSI pre-commit, §8.4). `SET CONSTRAINTS ... IMMEDIATE` runs pending deferred checks at that point.

Pending end-of-statement and deferred checks, and queued AFTER-trigger events, are tagged with the seq that queued them; `ROLLBACK TO SAVEPOINT s` discards those with seq `>= s` (§5.5).

### 5.3.1 ON CONFLICT arbiter protocol
`INSERT ... ON CONFLICT` with arbiter unique keys `A` (PostgreSQL's speculative insertion), per proposed row:
1. **Pre-check.** For each arbiter key, under its latch, determine its current state as in the unique check. A foreign Pending or committed-not-visible intent whose top data is not `Absent` → wait on its owner (§6), restart from 1. A live entry of another row → conflict on that row, go to 3.
2. **Insert.** No conflict: insert the row and its entries as key-existence ops through §5.1. If any arbiter key's unique check now finds a live entry of another row, or must wait, abandon this attempt (remove the intents it placed through §7.3 under their latches, adjusting counts) and restart from 1.
3. **Conflict on row `r`.**
   - Under RR/SER, if `r`'s newest committed version is newer than `S`: 40001 (PostgreSQL `ExecCheckTupleVisible`). This applies to `DO NOTHING` too.
   - `DO NOTHING`: skip the proposed row.
   - `DO UPDATE`: lock `r` as a row op through §5.1 with the mode the update needs. If `r`'s own top layer has `data_seq == seq0` (inserted or updated by this statement): 21000. RC: if, after the lock, the arbiter key no longer points at `r` or `r` changed since the pre-check, release nothing (the lock stays, as in PostgreSQL) and restart from 1. Then evaluate `DO UPDATE ... WHERE` against `r` and apply the update through §5.4.

Restarts are unbounded only while each restart follows a wait or an observed change; a restart with no change in between is a bug (G0-write checks the scheduler cannot spin).

**Foreign keys.** FK checks, referential actions and AFTER triggers run as internal commands at a **fresh seq** (greater than the statement's), and their reads open registered views (§3.1).
- **Child side** (insert, or FK-changing update, of a child row): first read the parent: RC with a fresh snapshot, RR/SER at `S`, both plus own writes. Not found → 23503. Found → take KEY SHARE on the parent's `/t/` key as a row op through §5.1 (RC: EPQ re-check; RR/SER: 40001 if the parent's newest data version is newer than `S` and is a tombstone, moved-tombstone or key_changed write). Under SERIALIZABLE register a SIREAD on the parent key before the read.
- **Parent side** (delete, or key change of a referenced key): the row op already holds `Update` on the parent (conflicts with KEY SHARE, so pending child inserters wait). At check time, scan the child index for the old key in the latest committed state plus own writes. A live child → for NO ACTION, first look for a live parent with the old key in the same state (`ri_Check_Pk_Match`); if one exists, no error; otherwise 23503. RESTRICT: 23503 without that look. Under RR/SER, a child live in the latest state but invisible at `S` → 40001 (PostgreSQL `detectNewRows`).
- Referential actions (CASCADE, SET NULL, SET DEFAULT) run as ordinary DML through §5.

### 5.4 Own-row rules
When `W` already has an intent on `k`. The revisit and 27000 rules apply only to the statement's own **data-changing row ops**: UPDATE, DELETE, MERGE matched actions, and the ON CONFLICT update (§5.3.1). Lock requests, FK checks, referential actions and triggers (which run at their own fresh seq, §5.3) only build a new layer.
- **Revisit in the same statement** (a row op on a row whose own top layer has `data_seq == seq0`): UPDATE / DELETE skip the row (PostgreSQL `TM_SelfModified`); `INSERT ... ON CONFLICT DO UPDATE` and MERGE matched actions raise 21000. A layer that only changed `lock` at `seq0` (a CTE `FOR UPDATE`, the conflict lock of ON CONFLICT) does not count.
- **Modified by a later command** (`data_seq > seq0`, e.g. a trigger or volatile function of this statement updated the row): raise 27000.
- Otherwise: build the new layer. No `t* > S` check: the own intent has excluded foreign writers since it was placed, and its placement did the check (or was an insert, governed by I-UNIQUE).

### 5.5 Savepoints and cursors
The txn keeps a write-set log `(seq, logical key)` (spills to disk past a threshold).

`ROLLBACK TO SAVEPOINT` at seq `s`:
- For each key written at seq `>= s`: under `latch(latch_key(k))`, re-read `k@INTENT`; if owned by the txn, drop layers with `seq >= s`. If no layer remains, remove the intent per §7.3. The restored top layer restores data, `data_seq` and lock.
- Shared row locks taken at seq `>= s` are released (under each key's latch, §6).
- Pending end-of-statement and deferred constraint checks and queued AFTER-trigger events with seq `>= s` are discarded.
- **SIREAD locks and recorded rw-conflicts are kept** (the client has seen the data; PostgreSQL keeps them too).
- Portals and cursors opened at seq `>= s` are closed.
- `seq` does not go back: the next command gets a seq greater than every seq used so far.
- The txn's wake generation is bumped and **all** its waiters are woken. They re-run §5; spurious wakeups are harmless.

Writes and locks are tagged with the seq current **when they execute**, not with the opening seq of the portal that runs them. A `FOR UPDATE` cursor fetched after `SAVEPOINT s` takes locks at seq `>= s`, so `ROLLBACK TO s` releases them.

**Layer compaction.** Live boundaries are: the current statement's start, every open cursor/portal start, and every live savepoint seq. Layer `i` (with next layer `i+1`) is needed iff some live boundary `r` has `layers[i].seq < r <= layers[i+1].seq`. The top layer is always needed. Others may be dropped.

## 6. Locks and waiting
- Exclusive row modes (NO KEY UPDATE, UPDATE) live in intent layers (§2.1).
- Shared modes (KEY SHARE, SHARE) live in an in-memory table keyed by logical key, tagged with the acquiring seq, and checked under the same latch. Safe because a crash aborts every holder.
- Conflict matrix: PostgreSQL's row-lock matrix, applied to (requested mode, held mode). An intent of a Pending or committed-not-visible txn holds its top layer's `lock`. Requested mode of each op: UPDATE without key change → NoKeyUpdate; UPDATE with key change, DELETE, PK change → Update; inserts and unique entries → NoKeyUpdate; explicit locks → their mode.
- **Release.** A txn's in-memory locks are released at abort (§7.1) or in commit step 5 (§3), never earlier. Savepoint rollback releases those taken after the savepoint. A shared row lock on `k` is removed from the lock table only under `latch(latch_key(k))`, and the holder's wake generation is bumped after the removal; so a waiter that read the holder's generation under that latch (§5.1) either saw the lock gone or sees the bump.
- **Conflict checks ignore ended holders.** A holder (of a shared row lock, relation lock or advisory lock) that is Aborted or a visible commit never conflicts, even before its release has run.
- **Wake generation.** Every txn has a counter `gen(T)`, bumped on every event that can unblock its waiters: commit step 5, abort, `ROLLBACK TO`, any release of an in-memory lock it holds.
- **Wait-for graph.** One graph covers every kind of wait: row intents, shared row locks (an edge to **every** conflicting holder), relation locks, advisory locks, deferrable-unique prefix waits.
- **Waiting.** `wait_for(W -> T, g)` where `g = gen(T)` was read under the latch that observed the conflict: (1) insert the edge into the graph and register W as a waiter on T, (2) if `gen(T) != g`, or T is Aborted, or T is a visible commit, or T's status is missing (ended, §4), or W's cancel flag is set, remove the edge and return immediately, (3) park. A woken waiter removes its edges and re-runs §5. The same pattern (read the generation where the conflict is seen, re-check after registering) is used for relation, advisory and prefix waits.
- NOWAIT: 55P03 instead of waiting. SKIP LOCKED: skip the row. `lock_timeout`: 55P03 on expiry.
- **Deadlock.** After `deadlock_timeout`, the waiter takes the graph mutex, runs a DFS, and if it finds a cycle containing itself, removes its own edges before releasing the mutex, then raises 40P01. Exactly one member of a cycle is aborted. No wait-die.
- Relation locks (AccessShare … AccessExclusive) are a separate in-memory table, held to txn end. After acquiring one, catalog lookups use the latest committed catalog (§4 for RR/SER storage-id rule).
- Advisory locks: in-memory, session or xact scope.
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
4. after the batch write returns, still under the latch, under the registry mutex: `last_removal_counter(T) = view_counter`, **then** `intent_count(T) -= 1`. Only if step 3 removed an intent of T, and only if T is of the current epoch (older-epoch txns have no counts; §7.4 handles them).
5. releases the latch.

There is no conditional delete in the KV; steps 1-2 replace it. A removal that finds the intent gone or re-owned writes nothing and changes no count.

### 7.4 Status truncation
Under the registry mutex, the status entry of T may be removed (in memory, and `/sys/txn/T` deleted) only when:
0. T is `released`: aborted with §7.1 done, or committed with commit step 5 done (so T is a visible commit and holds no in-memory lock).
1. `intent_count(T) == 0`. After a crash the count is unknown: committed records from older epochs stay until one full intent sweep has finished.
2. Every view open when T's last intent was removed has closed: `min(registered view counters) > last_removal_counter(T)`.

A truncated status therefore always means "ended and released" to any later lookup by a remembered TxnId (§4).

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
- DDL side: DROP, TRUNCATE and table-rewriting ALTER of a relation check every SIREAD on that relation (any granularity, any storage id of it) held by other SER txns; each gives `reader -> W` when W is SERIALIZABLE. The check (SER only) and the promotion of §8.6 (every isolation level) run in the DDL txn's pre-commit critical section under the SSI mutex (§8.4).
- Reader side: §4 skipped foreign intents of Pending or `Committed(c > S)` owners (`R -> owner`) and skipped versions `> S` (`R -> writer(t)`, §8.5).
- **Only concurrent txns form edges.** On the writer and DDL sides, a SIREAD holder R that committed with `commit_ts(R) <= S(W)` is not concurrent with W and gives no edge (W saw everything R did). On the reader side, edges only go to writers with `commit_ts > S(R)` or not yet committed, which §4 already ensures.
- Edges to or from a txn that is not SERIALIZABLE, or that has aborted, are ignored.

Correctness of I-SSI-ORDER: for a concurrent reader R and writer W on `k`: if W's intent placement precedes the opening of the view R reads `k` from, R sees the intent or its resolved version `> S` and records the edge. Otherwise the placement follows R's SIREAD registration, so W's SIREAD check sees it.

### 8.3 Dangerous structure
`T1 -> T2 -> T3` (rw-antidependencies; `T1 == T3` allowed) where T3 commits first. "Commits first" is decided by `commit_ts` for committed txns and by prepare order (§8.4) for prepared ones.
- **Read-only exception:** if T1 is declared `READ ONLY` (or has done no writes and is committing), the structure is dangerous only if `commit_ts(T3) <= S(T1)`.
- **Summarised T2.** When T2 is a committed txn known only through its writer-map entry (§8.5), its out-edges are represented by `earliest_out_conflict_commit(T2)`. An edge `T1 -> T2` is then dangerous if that value is set and (T1 is not read-only, or the value is `<= S(T1)`). This is PostgreSQL's rule for summarised transactions.
- Victim: a txn that is not prepared, preferring T2 (the pivot) if it is not prepared, otherwise T1. If the only candidate is the txn running the check, it aborts itself with 40001. A victim other than the checker is marked `doomed` (under the SSI mutex).

### 8.4 Pre-commit
After deferred constraint checks (§5.3), a SER txn T commits by, **in one critical section under the SSI mutex**: read its own `doomed` flag (set → 40001); run the dangerous-structure check over every structure containing T in any position, walking edges two hops in both directions (`X -> T -> Y`, `T -> Y -> Z`, `X -> Y -> T`); if it passes, mark itself `PREPARED` with `prepare_seq = ++prepare_counter`; for a DDL txn, run the DDL-side check and the SIREAD promotion (§8.2, §8.6); enqueue its commit request (carrying its TxnId for the writer map) on the commit thread's unbounded channel. Because enqueue happens under the mutex and the commit thread assigns `commit_ts` in channel order (§3), commit order equals prepare order. A prepared txn can no longer be chosen as a victim. A doomed txn raises 40001 at its next statement or at commit; the flag is only read under the SSI mutex.

Non-SER txns enqueue without taking the SSI mutex, except a non-SER DDL txn, which takes it to run the SIREAD promotion and enqueues inside that critical section.

### 8.5 Finding `writer(t)`
SSI keeps its own map `commit_ts -> (TxnId, earliest_out_conflict_commit)` for SERIALIZABLE txns. Every SER txn T carries `earliest_out_conflict_commit(T)`: the smallest `commit_ts` of a txn X with a recorded edge `T -> X` whose X has committed, or unset. It is updated under the SSI mutex when such an edge is recorded to an already-committed X, and when X's commit request is processed (§3 step 3) for every T with an edge `T -> X`. The writer-map entry carries the current value and is updated with it; once T's full conflict info is retired (§8.6) the entry is all that remains of T's out-edges. The commit thread inserts the entry in §3 step 3, before status is set, so the entry exists before any version `@t` can (resolution follows visibility). An entry is kept while it can matter (§8.6). A `t` with no entry was written by a non-SER txn, or by a SER txn whose entry was retired under §8.6; neither can form a dangerous structure with the reader, so the edge is dropped. This map is independent of the status table (§7.4) and corresponds to PostgreSQL's `OldCommittedSxact` summary.

### 8.6 Retention and abort
- The SIREADs, conflict info and writer-map entry of a committed SER txn T are kept until **both** `visible_ts >= commit_ts(T)` and every SER txn with a registered snapshot `S < commit_ts(T)` has ended. Evaluated under the registry mutex (§3.1), where taking `S` and registering it are one step; after `visible_ts >= commit_ts(T)` no new snapshot can have `S < commit_ts(T)`.
- An aborted txn's SIREADs and edges are removed when it aborts.
- When a relation's storage is retired (DROP, TRUNCATE, rewrite), finer-grained SIREADs on the retired ids are converted to relation-level SIREADs keyed by the relation oid. The conversion runs in the DDL txn's pre-commit critical section under the SSI mutex, before its commit request is enqueued (§8.4), so no SIREAD on a retired id survives into a state where the new ids are in use.
- Read-only txns: safe-snapshot optimisation and `DEFERRABLE`.

## 9. GC

### 9.1 Watermark
`computed` = min over: registered snapshots and caller-chosen ts (§3.1: txns, cursors, portals, `AS OF` reads, segment builds), `visible_ts`, and the `AS OF` retention window converted to ts through `/sys/ts_clock` (rounding down). In **one** registry critical section the GC job computes it and publishes `W = max(old W, computed)`. Then:
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

**I-GC.** For every registered snapshot or caller-chosen ts (all are `>= W`), the read path (§4) returns the same result before and after any GC step.

Unresolved committed intents are safe: GC only drops versions older than a kept version, and the intent's eventual version is newer than all of them.

## 10. Other rules
- **Storage ids:** u64, allocated from a persisted counter, never reused. Catalog rows map relation and index oids to their current storage ids.
- **Sequences:** non-transactional; the high-water mark is persisted (synced) before a value from a new block is handed out. Values may skip, never repeat.
- **Catalog:** rows in `/sys/catalog`, versioned like any table; DDL is transactional and takes AccessExclusive.
- **Large txns:** intents, write-set log and SIREAD state all spill; the 10M-row gate measures commit latency, reader latency during resolution and abort cleanup time.

## 11. Invariants and the G0 model
Checked by G0: `I-WAL-ORDER`, `I-VIS`, `I-ACK`, `I-SNAP-ORDER`, `I-HALLOWEEN`, `I-ONE-INTENT`, `I-WW`, `I-LOCK`, `I-UNIQUE`, `I-TRUNC`, `I-SSI-ORDER`, `I-GC`, plus:
- **I-ATOMIC.** After any crash, for every txn: all of its writes are visible at `visible_ts`, or none are.
- **I-DURABLE.** An acked `synchronous_commit=on` commit survives any crash.
- **I-SER.** Histories under SERIALIZABLE are serializable (Elle, G2), including predicate/phantom workloads and the read-only anomaly.
- **I-NOBLOCK.** No read-only path (§4) waits on another txn.
- **I-LIVE.** A waiter whose blocker has committed, aborted or released the conflicting lock is eventually woken; every wait-for cycle is broken with exactly one 40P01.
- **I-RC-MONO.** An RC statement sees every commit whose effects an earlier statement of the same session observed (via a conflict, a wait or a read).
- **I-COUNT.** For every current-epoch txn T, at every state where no placement or removal of T is between its KV write and its count update: `intent_count(T)` equals the number of `k@INTENT` entries owned by T in the KV, and the write-set log names each of them.
- **I-GC-QUIESCE.** After the GC job and a full compaction run with `W` fixed and no txn active, no key has a tombstone or moved-tombstone `<= W` as its newest version `<= W` (GC actually removes what it may, not only never removes too much).
- **I-SSI-PRECISION.** A schedule in which no two SER txns overlap never raises 40001 (catches over-eager edges; I-SER alone cannot).

The state space is too large for one exhaustive model, so G0 is four small-scope exhaustive models plus a deterministic simulator. All models run over MemKv, may start from a preloaded KV state (preloaded data does not count against the txn budget), and every actor is a separate step that can interleave: txn steps, the commit thread with **status-set and visible-advance as separate steps**, async resolver, inline removal, abort cleanup, GC job, compaction, crash.

**Model discipline.** Each model has a **fixed workload** (the exact statements of each txn, written down in the model's source) rather than generated programs, and partial-order reduction over independent steps. Each run reports its state count, so a change that silently shrinks the explored space shows up. The seed table below names, for every seed, the model and the workload that catches it; a seed whose workload cannot express the bug is a G0 defect. Steps a seed depends on happening late (for example the insert in seed 28) are separate schedulable steps, never fused into an earlier step.

| Model | Scope | Invariants |
|---|---|---|
| G0-commit | 3 txns, 2 keys, commit thread, async resolver + one inline remover (two removers of one intent), truncation incl. the `released` condition, one FK-style unlatched read, crash at every KV write and fsync, sync on/off | WAL-ORDER, VIS, ACK, SNAP-ORDER, TRUNC, COUNT, ATOMIC, DURABLE |
| G0-write | 2 txns, 2 keys (+1 unique key, +1 deferrable unique key), 2 statements and 1 savepoint per txn (rollback-to releasing data, exclusive and shared locks), 1 shared mode, relation locks, waits + DFS + wake generations + cancel, RC EPQ with the unlatched gap, ON CONFLICT DO UPDATE, commit thread, resolver and abort cleanup | ONE-INTENT, WW, LOCK, UNIQUE, HALLOWEEN, ATOMIC, LIVE, RC-MONO, TRUNC, COUNT |
| G0-ssi | 3 txns (one may be SER READ ONLY; one of the three may instead run a TRUNCATE), 2 keys + 1 range + 1 index→row fetch, 2 statements each, pre-commit with the channel send as a separate step, commit thread, resolver, writer map with summarised entries, registry | SSI-ORDER, SER (cycle check over the recorded history), SSI-PRECISION |
| G0-gc | 2 txns + 1 reader, 1-2 keys, MemKv in **LSM mode** (≥ 2 levels, per-file compaction streams, range tombstones), GC job, registry, AS OF registration with catalog-at-`t`, one DROP or TRUNCATE, crash between `W` publish and its sync, W recomputation after a simulated restart and a widened AS OF window | GC, GC-QUIESCE, ATOMIC, TRUNC |

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
38. Status truncated before the txn is `released` (commit).
39. Writer-side edge from a SIREAD holder that committed at or before `S(W)` (ssi, I-SSI-PRECISION).
40. Edge to a summarised committed T2 ignores `earliest_out_conflict_commit` (ssi).
41. Retired-id SIREADs promoted after the DDL's commit request is enqueued (ssi).
42. Compaction filter handed `W` before `/sys/gc_w` is synced (gc, crash).
43. Pre-commit checks only structures with the committer as pivot (ssi).
44. Unlatched FK parent read without a registered view (commit, I-TRUNC).
45. KEY SHARE checks only the newest version above `S` (write).
46. Shared row lock released without the key's latch (write, I-LIVE).
47. RC EPQ places its intent without re-verifying under the latch (write, I-ONE-INTENT).
48. Cancel does not wake a parked waiter (write, I-LIVE).
49. AS OF resolves the latest catalog instead of the catalog at `t` (gc).
50. Placement counted after its intent write, with a crash or failure in between (commit, I-COUNT).

## 12. Open questions and known divergences
Open (resolve before C-T2 merges):
1. RC joined DML: full EPQ vs statement retry (G3c decides).
2. RocksDB UDT viability (C-S1), and if viable the UDT layout of intents, tombstones and moved-tombstones; otherwise the ts-in-key GC path is mandatory.
3. Whether `synchronous_commit=off` commits may be visible to `on` sessions before fsync (PostgreSQL: yes). Current spec: yes.
4. Confirm the SERIALIZABLE unique-violation rule of §5.3 against PostgreSQL's isolation tests (G3c).

Known divergences from PostgreSQL 17:
- A concurrent primary-key change seen by RC EPQ raises 40001 instead of following the update chain (§5.2).
- Under RR/SER, accessing a relation that was truncated or rewritten after `S` raises 40001; PostgreSQL returns the new (possibly empty) contents, which its documentation calls not MVCC-safe (§4).
- `DEFERRABLE` primary keys are refused with 0A000 (§5.3). Deferrable unique constraints are supported.

## Changelog
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
