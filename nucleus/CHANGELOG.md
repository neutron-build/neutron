# Changelog

Notable changes to the Nucleus engine. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- **An upgrade from a pre-v0.1.2 container image crash-looped instead of
  saying why.** The image has run as uid 10001 since v0.1.2 (the M12 hardening
  pass); v0.1.0 and v0.1.1 ran as root. Nothing re-owns an existing data
  directory on upgrade, so the new process could not open a directory its
  predecessor created. The `chown` in the Dockerfile runs at BUILD time and
  therefore only covers a volume that is still empty on first use.

  The failure surfaced as a panic inside the storage open — exit 101, with no
  mention of permissions — which an orchestrator turns into an endless restart
  loop. Startup now checks that the data directory is writable *before* opening
  anything and exits 1 with the exact `chown` command for both a bind-mount and
  a named volume.

  **This was never announced.** It is a breaking operational change that
  shipped in v0.1.2 with no note in this changelog and none in the upgrade
  runbook, which is where an operator looks before upgrading. Both now carry
  it. Any deployment still on v0.1.1 or earlier needs the `chown` at upgrade
  time; a deployment whose data directory happened to be recreated escaped it
  by luck, not design.

- **A crash mid-COMMIT could leave a transaction durable in part.** On the
  paged engine, a transaction that dirties more pages than the buffer pool
  holds pushes its own uncommitted pages into the data file as the pool steals
  frames. The page WAL logged every image at transaction 0 and recovery
  replayed all of them unconditionally, so nothing could tell a restart that a
  run of page images belonged to a transaction that was never acknowledged.
  Measured: 2 of 3 crash rounds torn at the shipped 32 MB pool, 8 of 8 at a
  small one.

  Recovery can now take those writes back:

  - Page-WAL records carry the real transaction id instead of 0.
  - Before an uncommitted page is written to the data file, its
    pre-transaction image is logged first (a new `PAGE_UNDO` record). The
    write-ahead rule applies to undo exactly as it does to redo.
  - Recovery runs an analysis pass — a transaction with page writes and no
    COMMIT record is a loser — then reconstructs each page from the latest
    committed image, falling back to the before-image.
  - COMMIT is logged after the page images it vouches for, and one sync covers
    both. Read-only transactions still sync nothing.

  **Commit application is now serialized**, which is what makes a whole-page
  before-image safe to restore: two transactions could otherwise mutate the
  same page, and a committed image with an uncommitted transaction's bytes
  baked into it cannot be rolled back a page at a time. Measured cost ~2.4% at
  8 concurrent writers on large transactions and nothing measurable on small
  ones — the apply phase was already close to serial by its own internal
  locking. Transaction bodies are unaffected; only the window between COMMIT
  and its acknowledgement is exclusive.

  `probe_txn_atomicity`, which existed to demonstrate this bug, is now a
  passing gate.

  Page attribution is per SESSION, not merely per window. The apply lock
  serializes commits against each other but not against autocommit statements,
  which bypass it, so a page another connection dirties inside the window is
  left unattributed rather than being handed to the committing transaction —
  otherwise a crash before that transaction committed would undo a write the
  other connection was told succeeded. An unknown session attributes rather
  than skipping, because a missing undo record is the worse failure.

## [0.1.6] - 2026-08-04

The first release since v0.1.2 that carries engine changes; v0.1.3 through
v0.1.5 were release plumbing. Two of the fixes below are data-loss or
migration blockers, and the three changes above them can break SQL that ran
before — read the Changed section before upgrading.

### Changed — three statements that used to succeed now error

All three were accepted and silently not honoured. Erroring is the correction,
but it means SQL that ran yesterday can fail today. Read this before upgrading.

- **`SELECT ... FOR UPDATE SKIP LOCKED` / `NOWAIT` are refused.** The clause was
  parsed into the AST and never read, so it was accepted and discarded — a job
  queue claiming work with it handed the same row to every worker polling at
  that moment, while looking entirely correct. Plain `FOR UPDATE`/`FOR SHARE`
  still pass: they are advisory, and the isolation already provided is stronger
  than dropping them would imply.

  **Migration:** carry the guarantee in a predicate the write re-checks —
  `UPDATE ... WHERE id = (SELECT ... LIMIT 1) AND status = 'pending'`. Two
  workers may select the same row; only one UPDATE matches. This is what
  `neutronjobs` now does, and it is correct on any backend.

- **`CREATE INDEX ... WHERE` is refused.** The predicate was discarded and a
  FULL index was built under the requested name — larger than asked for, and
  usable by the planner for queries the partial index was never meant to serve.

  **Migration:** drop the `WHERE` clause if a full index is acceptable. There is
  no way to express a partial index until the predicate is honoured.

- **Reminder for anyone reading a claim query:** `SKIP LOCKED` is a contention
  optimisation, never a correctness guarantee. If safety can be expressed as a
  predicate, express it there.

### Fixed

- **A table created inside a transaction was unreadable in that transaction.**

  ```sql
  BEGIN;
  CREATE TABLE t (id INT);
  INSERT INTO t VALUES (1);   -- reported "INSERT 0 1"
  SELECT COUNT(*) FROM t;     -- ERROR: table 't' not found in storage
  ```

  `create_table` on the buffered disk engine recorded an op and returned
  without touching the engine, so a read in the same transaction went to an
  engine that had never heard of the table. The INSERT reporting success first
  is what made it expensive: the failure surfaced a statement later, somewhere
  else.

  This is the shape of every migration, and a migration runner that wraps each
  migration in a transaction — the correct thing to do — could not create and
  then populate a table. `ALTER TABLE ... RENAME` is create-new/copy/drop-old
  underneath and failed the same way, so a table rebuild failed too.

  The fix is narrow because the gap was: DML already read its own writes
  through the buffer overlay, and only a table existing solely in the buffer
  had no base to overlay onto. A missing base now reads as empty *only* when
  this transaction created that table, so a genuinely missing table still
  errors rather than silently scanning as empty.

- **PITR reported success while omitting the newest commits.** A WAL segment
  reached the archive only when it FILLED, so the recovery point was the last
  rollover rather than the last commit. At the default 64 MiB segment a
  low-write database can run for days without rolling over, and none of it was
  recoverable; a clean shutdown did not archive the tail either, so a planned
  deploy or failover lost every commit since the segment began. Measured before
  the fix: a restore that recovered 1 of 3 rows printed "PITR restore
  complete".

  `archive_active()` now seals and archives the segment being written — on
  graceful shutdown, and on a timer set by `NUCLEUS_WAL_ARCHIVE_TIMEOUT_SECS`
  (default 60s). **That timeout is the recovery-point objective**, and it
  defaults on rather than off, because configuring an archive at all is a
  statement that you want point-in-time recovery. Empty segments are skipped so
  the timer cannot litter the archive. `restore-pitr` now reports the recovery
  point in wall-clock time and states what is not in it; an LSN alone reads as
  success.

## [0.1.5] - 2026-08-04

### Fixed

- Release plumbing only, no engine changes. The v0.1.4 image is good and
  published; its release job stopped before signing because the smoke test bound
  `0.0.0.0`, which Nucleus refuses without both a cluster and a replication
  token. Opting out of those guards one at a time was the wrong shape for a test
  whose only job is to prove the binary runs and serves — it now binds loopback
  and trips no guard at all.

## [0.1.4] - 2026-08-03

### Fixed

- Release plumbing only, no engine changes. The v0.1.3 image is good — it was
  the first to actually run on the runtime base — but its release job stopped
  before signing because the new smoke test started the server on `0.0.0.0`
  without `NUCLEUS_ALLOW_INSECURE_CLUSTER`, so Nucleus correctly refused a
  non-loopback bind with no cluster token. The test tripped over the product
  behaving properly. Fixed, so this release is signed and attested.

## [0.1.3] - 2026-08-03

### Fixed

- **The v0.1.2 release artefacts could not run.** Linux binaries were built on
  `ubuntu-latest`, which had moved to 24.04 (glibc 2.39), while the runtime
  image is `debian:bookworm-slim` (2.36) — so the published container exited
  immediately with `GLIBC_2.38 not found`, and the standalone tarballs were
  equally unusable on Debian 12. The Linux builders are now pinned to
  ubuntu-22.04, which keeps the binaries runnable on older hosts rather than
  only fixing the container.

  Nothing in the pipeline had ever *run* the image: clippy, tests, SBOM,
  signing and provenance all passed on a container that could not start,
  because each inspects source or metadata rather than behaviour. The release
  now smoke-tests the image — runs it, starts a server, waits for a status
  probe — before signing it.

  v0.1.2 is superseded; use this release. No engine changes.

## [0.1.2] - 2026-08-03

The theme of this release is memory: an instance that had grown could exceed its
limit, refuse writes, and not be recoverable by restarting. All five defects
below were found from one production incident and are fixed together, because
each was only visible once the one before it was.

### Fixed

- **KV memory was accounted in entries, not bytes.** `Pressurable::current_usage`
  returned `dbsize() * 128` — a flat 128 bytes per entry — so a store holding
  31,992 source maps averaging 150 KB reported 4 MB while actually holding
  4.8 GB. The allocator, choosing which subsystem to reclaim from, never picked
  KV no matter how much it held. Usage is now measured from real value sizes.

- **Eviction to the cold tier never triggered.** It fired only when the entry
  count passed `max_hot_entries` (100k). At 32k large keys that never happened,
  so the disk tier sat empty while the hot tier grew without bound. Eviction is
  now driven by a byte budget as well — whichever limit is reached first — and
  spills largest-first, so a target is met by moving a few big values rather
  than thousands of small ones. Configurable with `NUCLEUS_KV_MAX_HOT_MB`
  (default 1 GiB).

- **Memory pressure could not reclaim anything.** The pressure handler only
  swept *expired* entries, so a store whose entries carry no TTL — the normal
  case for anything durable — reported pressure forever while freeing precisely
  nothing, running eviction twice a second indefinitely. It now spills to the
  cold tier, where data stays readable and a cold hit is promoted back on
  access.

- **WAL replay read the entire log into memory before parsing.** A 4.8 GB KV WAL
  cost 4.8 GB of buffer on top of the map being built, so an instance that had
  grown past its memory limit could not be restarted within that limit —
  restarting being exactly what one reaches for. Replay now streams through a
  sliding window that grows only to the largest single item. A checkpointed log
  is a *single* snapshot record containing every live key, so snapshots are
  streamed item by item; record-level streaming alone would still have buffered
  the whole file.

- **Evicted keys could be lost on a crash.** Checkpoint snapshots the hot tier
  and truncates the WAL to it, so an evicted key's last WAL record disappears at
  that moment — while `LsmTree::put` only buffers into an in-memory memtable
  until a 1000-entry threshold. Between the two, an evicted key existed in
  neither the WAL nor on disk. Latent while eviction effectively never fired;
  routine once it did. The cold tier is now flushed before snapshotting.

- **A key asked to be temporary could come back permanent.** `SET` with an expiry
  logged the value and the deadline as two records; replaying only the first
  produced a permanent key. Now written as one atomic record.

### Changed

- **SSTable values live on disk rather than in memory.** An `SSTable` held every
  key *and value* resident and loaded each file in full at open, so the "cold"
  tier only persisted data — it did not offload it. Evicting 3.9 GB left
  resident memory unchanged. Tables now keep keys with a per-key value location
  and read values from the backing file per lookup; loading skips value payloads
  while indexing, writing streams and releases resident copies, and compaction
  materialises one value at a time. Measured on a live instance: **8.34 GB →
  1.16 GB resident**, same data, same query results.

- Removed `TieredKvStore`, a complete second hot/cold KV implementation with no
  callers anywhere in the tree. `KvStore` has its own inline cold tier and is
  what the server constructs. What remains of that module is the value codec it
  always was, now named accordingly.

### Known limitations

- The cold-tier codec carries six type tags and falls back to text for anything
  else — a tradeoff shared with the KV WAL, where a `Bytea` has always returned
  as text across a restart. Eviction now refuses to move values it cannot
  represent, so it never changes a value's type as a side effect of memory
  pressure, but the underlying WAL limitation is unchanged.
- Streaming replay bounds the sliding window, not the parsed map. A store whose
  live dataset genuinely exceeds memory still cannot be opened.

## [0.1.1]

Earlier releases predate this file.
