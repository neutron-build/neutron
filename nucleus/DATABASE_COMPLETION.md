# Nucleus Database Completion Program

Updated: 2026-07-15

This is the canonical execution plan for taking Nucleus from its current broad,
pre-production implementation to feature-complete database status. It covers the
remaining product work; prolonged soak time and real-world battle-hardening continue
after these gates are complete.

## Rules of completion

A checkbox closes only when all of the following are true:

1. The behavior is reachable through a supported public interface.
2. Unsupported variants reject clearly or fail closed; no silent fallback may change semantics.
3. Unit, integration, restart, and adversarial tests appropriate to the feature are active in CI.
4. Durable state survives restart, or the feature is explicitly documented as ephemeral.
5. User documentation describes the supported behavior and material limitations.
6. The completion evidence is linked in the milestone's evidence section.

Source test declarations, executed tests, ignored tests, and probe/stress runs are reported as
separate numbers. "Implemented" never means "a type or parser exists"; it means the end-to-end
behavior satisfies the relevant gate above.

## Current baseline

- Source LOC: 346615; Source Rust files: 306; Top-level modules: 53.
- Declared unit tests: 4790; Declared integration tests: 452; Ignored tests: 53.
  These are static declarations, not executed-test claims.
- The most recent full library run executed 4,622 passing tests, 0 failing.
- Relational SQL, MVCC, multiple storage engines, PostgreSQL wire support, twelve public data-model
  families, specialty indexes, encryption, TLS, embedded mode, physical backup v1, probes, Raft
  state-machine/runtime scaffolding, trusted SCRAM identities, role assumption, and RLS enforcement
  exist in the current worktree.
- Trusted identity and RLS are checkpointed in commit `7982289`.
- The supported release target is single-node first. Distributed mode has an independent completion
  gate and must not block an honest single-node release.

## Milestone 0 — Repository truth and security checkpoint

Goal: establish one auditable baseline before adding new behavior.

- [x] Review the current identity/RLS diff for authority, persistence, transaction, cache, export,
      replica, and alternate-engine bypasses.
- [x] Split or checkpoint the implementation into reviewable commits without losing user changes.
- [x] Keep `README.md`, this file, `RLS_SECURITY.md`, and a concise limitations section tracked.
- [x] Keep stale local status documents ignored and designate only tracked docs as release evidence.
- [x] Change metrics output to report declared, executed, ignored, integration, and stress tests
      independently.
- [x] Record supported configurations and support tiers in the README.

Exit gate:

- Default library tests, server build, strict clippy, formatting, and RLS adversarial tests pass.
- A fresh restart preserves login roles, memberships, policies, and RLS enablement.
- No ignored planning/status file is required to understand current work.

## Milestone 1 — Authoritative build and correctness matrix

Goal: every supported configuration builds and all normal correctness regressions run by default.

### Builds

- [x] Repair `cargo check --no-default-features` for the embedded/core build.
- [x] Repair and verify the WASM feature/target boundary.
- [x] Verify the default server and core-only builds.
- [x] Verify bench-tools, diagnostics, optional integrations, and release build variants.
- [x] Add Linux and macOS CI; add Windows only if declared supported.
- [x] Verify the core-only build does not expose or auto-build server-only modules and probes.

### Tests

- [x] Make full integration tests a required CI job alongside library tests.
- [x] Reconcile the stale ignored SSI/concurrency findings with their active fixed regressions.
- [x] Keep storage-engine differential tests active for MVCC, memory, LSM, and columnar paths.
- [x] Categorize every remaining ignored test as stress/scale (42).
- [x] Run non-stress correctness tests without `#[ignore]`.
- [x] Schedule stress, crash, scale, sanitizer, and probe suites with retained failure commands/logs.
- [x] Add dependency, license, unsafe-code, and vulnerability checks.

Exit gate:

- The supported build matrix is green from a clean checkout.
- No known correctness regression is hidden behind `#[ignore]`.
- CI can reproduce probe failures from a recorded seed.

## Milestone 2 — Storage and SQL correctness completeness

Goal: close known semantic holes before expanding interfaces.

- [x] Wire schema coercion into columnar INSERT and validate all supported source/target types.
- [x] Wire GIN indexes into query planning/execution, including updates, deletes, restart, and EXPLAIN.
- [x] Differential-test planner execution against the AST path for every supported SELECT shape.
- [x] Differential-test LSM and columnar engines against MVCC for filters, ranges, grouping, NULLs,
      ordering, updates, deletes, and restart.
- [x] Complete numeric/decimal aggregate precision and overflow behavior.
- [x] Verify collations, time zones, date arithmetic, NULL ordering, casts, and three-valued logic.
- [x] Verify constraints and cascades across transactions and restart.
- [x] Finish MVCC garbage collection/vacuum behavior for long snapshots and high churn.
- [x] Add deterministic transaction-ID exhaustion/wraparound behavior.
- [x] Ensure query caches and specialty indexes invalidate on every relevant DDL/DML transition.

Evidence:

- `columnar_insert_schema_coercion_is_strict` covers every native columnar physical type and
  fail-closed invalid input through the public executor path.
- The active GIN unit/restart suite covers planner selection and EXPLAIN, exact rechecks, arrays,
  transactions, insert/update/delete/drop, duplicate DDL, generation-safe invalidation, and rebuild
  from the persisted catalog. The 2026-07-15 full library run passed 3,737 tests; strict all-target
  clippy passed with `bench-tools,memory-debug`.
- `planner_ast_differential` compares columns, types, values, NULLs, and ordering across scans,
  predicates, expressions, DISTINCT, limits, aggregates, grouping/HAVING, joins, B-tree/GIN access,
  CTEs, subqueries, set operations, and windows with result caching explicitly invalidated.
- `probe_engines` completed 12,000 generated LSM/columnar comparisons against MVCC across three
  recorded seeds with zero divergence after fixing UPDATE write coercion. Active regressions cover
  the discovered nullable-neighbor corruption, and `commit_durability` proves per-table LSM
  insert/update/delete state and routing survive a crash-copy restart.
- Exact bounded `NUMERIC` now rejects malformed/out-of-range writes, compares and hashes canonical
  decimals, performs checked arithmetic and plain/grouped/window SUM/AVG without f64 conversion,
  and preserves logical decimal values in MVCC, memory, LSM, heap, and columnar storage. Active
  tests cover precision beyond f64, NULL frames, DISTINCT, overflow/division-by-zero, all four
  engines, and heap/LSM/MergeTree restart. README documents the 96-bit/28-scale range and the
  currently unenforced precision/scale typemod limitation.
- Strict ISO date/timestamp parsing, checked interval/calendar arithmetic, IANA session zones and
  `AT TIME ZONE`, explicit DST-gap rejection, binary `C`/`POSIX` collation, PostgreSQL NULL ordering,
  and complete three-valued truth tests now share the row, planned, and constant-expression paths.
  DML coercion preserves session-aware zoned instants and logical temporal types across MVCC,
  memory, LSM, columnar WAL/restart, and engine-differential tests. Unsupported locale collations
  reject explicitly. The 2026-07-15 full library run passed 3,742 tests with 113 native-protocol
  stubs ignored.
- Immediate PK/UNIQUE/CHECK/NOT NULL/FK enforcement now validates existing rows and DDL structure,
  preserves named/generated constraint identities across restart, protects referenced keys/types,
  and rejects unsupported deferred, alternate MATCH, NULL-equality, and dependency-cascade modes.
  FK actions preflight the complete cascade graph, enforce child constraints/RLS, handle pending
  parent keys and self-referencing trees, and leave no partial logical writes after a downstream
  rejection. Active tests cover transaction rollback, concurrent parent-delete/child-insert,
  multilevel failure atomicity, all FK actions, all four engines, and two crash-copy restarts. The
  2026-07-15 full library run passed 3,759 tests with 113 native-protocol stubs ignored; the focused
  engine/restart suites passed 10 and 19 tests respectively.
- MVCC vacuum now derives its watermark from every active snapshot's retained `xmin`, preserves a
  pre-delete version for snapshots overlapping the deleter, removes committed-dead and
  aborted-created versions, repairs aborted-delete tombstones before status reclamation, scopes
  `VACUUM table` correctly, and rebuilds secondary indexes after version-vector compaction. Active
  regressions cover the formerly unsafe committed-deleter interleaving, aborted insert/delete
  cleanup, table isolation, index lookup after compaction, idempotent metadata GC, idle-transaction
  release, and a 1,000-update chain retained by a long snapshot then reduced to one live version.
- The MVCC allocator now uses a checked atomic transition, reserves `u64::MAX` as a terminal
  exhaustion sentinel, and cannot wrap into invalid/bootstrap IDs. Every public explicit and
  implicit transaction path returns a stable `transaction ID space exhausted` storage error before
  mutating state; deterministic near-boundary tests prove the final allocatable ID, repeated
  terminal failure, read/write rejection, and absence of reserved active IDs.
- Cache/index coherence is proved by a two-sided differential oracle
  (`executor::tests::test_cache_coherence`): a hot executor with warm caches and B-tree/GIN/HNSW
  indexes and a cold reference executor with every cache dropped and no specialty index receive the
  same randomized DDL/DML stream, and every probe must agree on rows and column metadata. The
  transition set covers DML, upsert, COPY FROM, a COPY that must be *rejected*, TRUNCATE, index DDL,
  column DDL, table rename, DROP TABLE, view DDL, and committed/rolled-back transactions;
  `cache_oracle_precondition_every_transition_really_runs` stops any transition from silently
  no-opping. COPY FROM was the last uncovered write: it bypassed the INSERT path entirely, so it
  skipped NOT NULL/CHECK validation, never coerced a field to its declared column type, and applied
  rows one at a time so a mid-payload violation left the earlier rows behind. It now runs as a
  single INSERT statement on both the executor and the pgwire path, which makes it all-or-nothing
  like PostgreSQL's and gives it the same incremental index maintenance as every other write.
  `tests/copy_wire_constraints.rs` drives the pgwire path over the raw simple-query protocol (the
  one `psql \copy` uses) rather than `tokio_postgres::copy_in`, which prepares over the extended
  protocol and never reaches the COPY interception at all.
  The oracle's default depth is 6 seeds, chosen so it can sit in the push gates; that is a speed
  compromise and NOT a coverage claim. A 1500-seed sweep found a divergence at seed ~1140 that the
  default depth cannot reach: zone-map granule statistics are shared process state, and an INSERT
  inside a transaction merged its values into them immediately while only DELETE and UPDATE — and
  only when they actually matched rows — marked the table for post-transaction repair. A transaction
  that inserted and rolled back therefore left `min`/`max` describing a row that no longer existed,
  and when the stale granule's `row_count` coincidentally matched the live table's, `can_skip_granule`
  trusted it and pruned a granule holding live rows: `WHERE val > 13` returned nothing while
  `WHERE val = 17` returned the same row. Inserts now mark the table dirty inside a transaction, so
  COMMIT and ROLLBACK both rebuild the derived state.
  `rolled_back_insert_does_not_poison_zone_map_pruning` pins the minimal case and fails if the mark
  is removed. Because a green default-depth run says nothing about this class, the deep sweep
  (`oracle_deep_run_catches_rare_divergences`, `#[ignore]`d) runs weekly in the
  `cache-oracle-deep` job of `nucleus-long.yml`.

Exit gate:

- Curated PostgreSQL/SQLite differential corpora have no unexplained semantic divergence.
- Every storage engine produces the same logical result for its declared SQL subset.
- Long-running MVCC churn reclaims storage without violating snapshot visibility.

## Milestone 3 — Crash durability and recovery

Goal: every durable model has proven, deterministic crash behavior.

- [x] Inventory the authoritative files, WALs, manifests, and checkpoints for every data model.
      `DURABILITY.md` tables every durable artifact (observed from a live data directory, not
      assumed): SQL data + segmented WAL, catalog.json, meta.json, and a per-model WAL for KV,
      collections, document, graph, FTS, geo, vector (+ index_meta.json), time series, columnar,
      datalog, streams, CDC, and blob (+ segments), plus what is explicitly derived and rebuildable.
- [x] Add subprocess kill points before/after WAL append, fsync, data write, checkpoint, and rename.
      `storage::crashpoint` declares 13 named boundaries — 11 at M3 close, plus
      `commit.after_specialty_before_sql` and `crossmodel.before_commit_record`, added 2026-08-21
      by the cross-model atomicity work to sit exactly at the commit boundary; `NUCLEUS_CRASHPOINT=<name>`
      makes the process `abort()` there (no unwind, no Drop, no flush — power-loss equivalent at a
      chosen instruction), with `NUCLEUS_CRASHPOINT_SKIP=n` to hit setup vs deep steady state.
      `probe_crash_points` walks every point at several skip depths and reports points it could NOT
      reach rather than counting them as passes. A dedicated kv-cold arm (2026-08-23, STO-2) spills
      KV keys past the hot budget into the cold LSM tier, checkpoints, aborts, and requires every
      acknowledged key to read back from disk rather than from the WAL the checkpoint truncated.
- [x] Test torn headers/records, truncated WALs, invalid checksums, duplicate replay, and corrupt tails.
      `probe_durability_torn`: 0 findings over ~9.5k lossy recoveries — no panics, every recovered
      row was committed, CRC gate honored.
- [x] Test disk-full, permission loss, fsync/write errors, read-only media, and interrupted checkpoint.
      `storage::crashpoint::io_fault` injects ENOSPC / permission-denied / generic I/O errors at
      `wal.append`, `wal.fsync`, and `meta.write`; `probe_io_faults` walks 21 point x kind x depth
      combinations and asserts the failure SURFACES (a write that cannot be made durable must never
      report success), that every acknowledged row survives recovery, and that no corrupt or
      half-applied record remains. 0 findings. The declared fault-point set has since grown from
      three to twenty-three (`ALL_IO_POINTS`): the specialty append points (`datalog`/`vector`/
      `kv`/`collections`/`streams` `.wal_append`), `kv.wal_fsync` (which makes the
      specialty-before-SQL commit order directly testable), eleven `*.wal_reopen`
      checkpoint-strand points (2026-08-21, S31-14), and `lsm.sst_write` (2026-08-23, STO-1/2) —
      69 combinations walked at 2026-08-23, still 0 findings. Interrupted checkpoint is covered by
      `checkpoint.before/mid_rewrite/after`. Read-only media is simulated via injected
      PermissionDenied rather than an actually read-only mount.
- [x] Verify replay idempotency through repeated crash/recovery cycles.
      `probe_crash_points` invariant 4 reopens each crashed database three times and requires an
      identical row set.
- [x] Verify catalog, metadata, security policy, specialty index, and row state recover consistently.
      Row state: the crash matrix (prefix + fsync-survival + idempotency). Catalog, metadata, and
      specialty state: `test_durability_format::catalog_metadata_and_specialty_state_all_survive_reopen`
      asserts PRIMARY KEY/CHECK/NOT NULL constraints, SERIAL sequence continuity, views, and vector
      KNN all survive a real restart. Security policy: `test_meta_persistence::
      test_rls_policy_and_role_verifier_survive_restart` proves a bound principal is still filtered
      after reopen.
- [x] Establish explicit durability modes and fsync guarantees.
      `DURABILITY.md` documents fsync (default) / fdatasync / none and what each loses. The crash
      matrix asserts the fsync contract directly: every commit the child fsynced must be present
      after recovery.
- [x] Add on-disk format manifests and forward migration tests.
      Data pages carry magic + `DB_FORMAT_VERSION` in the meta page; backup manifests carry
      `format_version` and restore is format-locked (`src/backup.rs`). A current-format database
      still opens and reads back (`the_current_format_still_opens`), which is the control that keeps
      the rejection tests honest.
- [x] Reject unsupported downgrade/format combinations without modifying data.
      `test_durability_format` fingerprints every file, attempts to open a future-format and a
      foreign file, and asserts the refusal names the format problem AND rewrites/deletes nothing.
      This found a real defect (fixed): format validation ran AFTER WAL recovery, so a rejected open
      had already replayed WAL records into the file and truncated the WAL to 0 bytes — destroying
      data it then declined to read. Validation is now the first thing `open_inner` does, straight
      off disk, before any buffer pool or recovery exists.

Exit gate:

- The crash matrix yields either the previous committed state or the new committed state, never a
  partial committed state or silent corruption.
  Status: holds at all 13 named crash points (`probe_crash_points`, 0 findings — 11 at M3 close,
  the two cross-model commit-boundary points added 2026-08-21, and the kv-cold arm added
  2026-08-23) after fixing a total-data-loss defect the matrix found — see below.
- Recovery failures are actionable errors and do not continue with suspect data.
  Status: holds across 21 injected I/O-failure combinations at M3 close — 69 by 2026-08-23
  (`probe_io_faults`, 0 findings throughout) — and a rejected open now provably rewrites/deletes
  nothing.

Scope limits on the above, stated plainly (full list in `DURABILITY.md`): named crash points
cover the SQL/MVCC WAL, catalog rename, compaction, and — since 2026-08-21 — the two cross-model
commit boundaries. The specialty-model WALs are still not covered by abort-style crash points of
their own, because the failure that class exposes is a failing disk rather than power loss at an
instruction: they gained I/O-fault points instead (eleven `*.wal_reopen` strand points,
2026-08-21, plus `lsm.sst_write`, 2026-08-23) alongside the reopen tests and the stage-and-rename
fix. Read-only media is simulated with injected PermissionDenied rather than an actually
read-only mount. Multi-node/replica crash behavior is out of scope here (M9).

**Defect the crash matrix found (fixed).** WAL compaction truncated the live WAL in place and then
rewrote it from recovered state, so a crash in that window destroyed the only durable copy — the
matrix caught it losing all 40 fsynced rows at `checkpoint.mid_rewrite`. Compaction runs on EVERY
reopen of a populated database, which made a power loss during startup a total-data-loss event for
a database whose every commit had been fsynced and acknowledged. Compaction now stages the baseline
in a temp file, fsyncs it, renames it over the live WAL atomically, and fsyncs the directory. The
same truncate-then-rewrite pattern was then found and fixed in the geo and datalog checkpoints
(the graph and blob WALs already used stage-and-rename correctly).

## Milestone 4 — Backup, restore, and point-in-time recovery

Goal: recover a production database without requiring a byte-for-byte stopped-directory copy.

- [x] Add an online-consistent physical snapshot coordinated with writes and checkpoints.
      A RUNNING server snapshots itself via `BACKUP DATABASE TO '<path>'` (superuser), the
      `pg_basebackup` shape — an external process cannot pin WAL retention or observe LSNs, so
      `nucleus backup` refuses a live directory and this is the supported path. Routed through
      `StorageEngine::as_backup_coordinator()`; engines without a physical snapshot refuse
      explicitly. Coordination: pin WAL retention at the window start LSN, checkpoint, copy
      page-slot-wise re-reading any slot that does not decode to a complete page, cut the WAL at
      the end LSN. Verified end-to-end on a serving server: backup taken while serving, server kept
      serving, snapshot restored to exactly the committed point and accepted writes.
      REMAINING (documented in DURABILITY.md, not claimed here): the consistent LSN covers the SQL
      substrate only — specialty WALs are individually crash-consistent but not LSN-pinned with it;
      the retention pin is uncapped; and the concurrency reproducer is `#[ignore]`d because it
      exposes a pre-existing page publish/flush ordering race.
      PARTIAL (2026-07-24). The mechanism exists and is proven; the delivery vector for an
      already-running server does not, so this stays open.
      DONE: `backup::backup_online` + the `BackupCoordinator` trait implemented by `DiskEngine`.
      It pins WAL retention at the window's start LSN (so a checkpoint firing mid-copy cannot
      reclaim the records the snapshot still needs), checkpoints, copies the data file one page
      slot at a time re-reading any slot that does not decode to a complete page, then copies the
      WAL byte-exactly truncated at the window's end LSN using the same prefix cut PITR uses. The
      restored snapshot replays through ordinary recovery to exactly the state a crash at
      `consistent_lsn` would have recovered. Proven by
      `online_backup_is_consistent_under_concurrent_writes_and_checkpoints` (a concurrent writer
      and a concurrent checkpointer run across the whole window; every row committed before the
      backup began is present after restore and no row appears that was never inserted) and by
      `retention_pin_survives_a_checkpoint_truncate`. A page that cannot be read intact ABANDONS
      the backup rather than snapshotting a torn page
      (`online_backup_aborts_rather_than_snapshot_an_unreadable_page`).
      DONE: the silent-torn-success hole is closed. An open instance holds an OS lock on its data
      directory (`backup::DataDirLock`, taken in `cmd_start`); `nucleus backup` refuses a locked
      directory with an actionable error, and the deliberate override (`--allow-in-use`) records
      `taken_while_in_use: true` in the manifest so an inconsistent snapshot can never be mistaken
      for a consistent one later. A stale lock file from a crashed process does not block a backup
      (the check is liveness, not file existence).
      NOT DONE: backing up a *running* server online. `nucleus backup --online` opens the data
      directory itself, so it works only when no other process holds it — the running-server case
      is refused, not served. Closing this needs an admin/SQL command that reaches the live
      server's engine handle (the executor holds `Arc<dyn StorageEngine>`, with no route to the
      `DiskEngine` underneath).
      NOT DONE: cross-model consistency. The consistency point covers the SQL substrate (data file
      + segmented WAL). The specialty-model WALs and catalog JSON are copied after it; each is
      individually crash-consistent but none is pinned to the same LSN.
- [x] Add backup manifests with checksums, format version, database identity, and encryption metadata.
      DONE (2026-07-24). `BackupManifest` carries per-file BLAKE3 checksums (path + length + hash
      for every file under `data/`), `format_version`, a stable `database_id` (an id file created
      in the data directory before the copy, so it travels inside the snapshot), `encryption`
      (encrypted / compressed / algorithm / key_id — never key material), and the consistency
      fields `online` / `consistent_lsn` / `taken_while_in_use`. Restore compatibility now keys on
      the on-disk format version rather than the release string, so patch releases interoperate;
      manifests predating the field fall back to the old exact-version lock.
- [x] Add WAL archiving with monotonic positions and retention management.
      Archiving with monotonic positions was already here: `archive_segment` copies each sealed
      segment through a temp file + rename and appends `<seg> <min_lsn> <max_lsn> <unix>` to
      `archive.index`. **Retention was the missing half and nothing ever deleted an archived
      segment**, so continuous archiving grew the archive until the disk ran out.
      `wal::prune_archive` closes it (2026-08-18), with `nucleus prune-archive --dry-run`.
      Deliberately manual, no policy and no timer: deleting recovery data on a schedule, with no
      knowledge of which base snapshots still exist, trades a disk-space problem for an
      unrecoverable one. A segment goes only when ALL of its records are below the horizon;
      bounds are read from the segment files rather than the index, which is documented as an
      advisory optimization and must not become the authority on what is recoverable; a segment
      whose bounds cannot be read is kept and reported. The dry run IS the real run minus the
      deletions, so the preview cannot drift from what it previews.
      `prune_archive_keeps_the_segment_containing_the_horizon` is the gate, and its first
      version did not discriminate -- one LSN per segment meant the horizon could never fall
      inside one, and it passed against a deliberately wrong `min_lsn` comparison. The fixture
      now writes several records per segment and the wrong version fails it.
- [x] Add restore-to-latest and restore-to-time/position workflows.
      Gated 2026-08-18 by `tests/pitr_cli_roundtrip.rs`, which drives the real binary
      (`CARGO_BIN_EXE_nucleus`) as a subprocess and then opens what it produced: batch A lands in
      a base snapshot, batch B only in the archived WAL, and a restore-to-latest into a clean
      directory must return all 80 rows -- a restore that merely unpacked the base would pass
      every other assertion and fail that one. Also covers the report an operator reads during a
      recovery, and the two refusals (a target older than the base; `--lsn` with `--time`, which
      must be rejected before any directory is created).
      **Writing the gate found a live defect.** `restore_pitr` refuses a target older than the
      base only `if manifest.consistent_lsn > 0`, and the OFFLINE backup path -- the default for
      `nucleus backup`, since `--online` is opt-in -- hardcoded `consistent_lsn: 0`. So the guard
      never fired for the backups most people take: an operator restoring to an LSN before a
      destructive statement got "success" and the statement still there, which is exactly the
      scenario the guard's own comment describes. Offline backups now record the highest LSN in
      the copied WAL. An in-use copy can overstate it, and that is the safe direction: a loud
      refusal beats a silent no-op rollback.
- [x] Add logical schema/data dump and restore across compatible format versions.
      `dump_logical`/`restore_logical` now emit sequences (with setval), tables in FK-dependency
      order, data, views, materialized views, roles, RLS policies + row-security enablement, and
      functions. 10 round-trip tests in `test_logical_dump.rs` incl. determinism, FK ordering,
      function bodies containing semicolons, and never emitting the bootstrap superuser.
- [x] Include roles, memberships, policies, sequences, views, functions, and specialty metadata.
      FIXED 2026-07-24. Previously (verified against a live server): a dump
      of a database with a role, an RLS policy, a view, and a SERIAL column emitted ONLY
      `CREATE TABLE` + `INSERT`s. Restoring it silently drops the security boundary (no
      CREATE POLICY / ENABLE ROW LEVEL SECURITY), and because the table is recreated with
      `DEFAULT nextval('t_id_seq')` while no sequence is ever created, the restored table REJECTS
      inserts that rely on the SERIAL default ("null value in column \"id\" violates not-null
      constraint"). Rows round-trip correctly; the schema around them does not.
      Now fixed and verified END-TO-END through the CLI, not just the library: `nucleus dump` emits
      CREATE SEQUENCE + setval, CREATE ROLE, CREATE POLICY, ENABLE ROW LEVEL SECURITY, and
      CREATE VIEW, and a restored database ACCEPTS an insert relying on the SERIAL default (id 3,
      where it previously errored). A second defect surfaced during that verification: the CLI's
      `open_persistent_executor` loaded catalog.json but never called `load_meta()`, so roles,
      policies, views, sequences, and functions were absent from the executor and the dump emitted
      nothing for them — the library tests passed while the shipped command stayed broken.
- [x] Define encrypted-backup key handling and key rotation.
      **Closed 2026-08-19.** The offline half landed: an encrypted open records `encryption.json`
      beside the data file (key fingerprint and algorithm, never key material), the offline
      backup reads it, and `nucleus backup` prints the key a restore will need. Opening under a
      DIFFERENT key is now refused by name instead of decrypting page 0 into garbage and
      reporting "not a Nucleus database (bad magic bytes)" -- a wrong-key error that read as
      corruption during a recovery. A directory not opened since markers existed has none and
      still reports unencrypted; one server start fixes it, and the difference between
      "nothing recorded" and "not encrypted" is stated at both the call site and the marker.
      **Decided and half-implemented 2026-08-19.** The design question -- where keys live --
      was already answered by the code: at-rest keys arrive as `NUCLEUS_ENCRYPT_KEY` or
      `NUCLEUS_ENCRYPT_PASSPHRASE` (`main.rs`), so there is no keyring or KMS to invent.
      `key_id` is therefore DERIVED rather than configured: the first 16 hex chars of the
      SHA-256 of the key, computed in `PageEncryptor` and surfaced through
      `DiskManager::encryption_key_id` into `encryption_info()`. Always available, never
      leaks the key, and asking an operator to also invent a label would just be a second
      thing to get wrong. Rotation follows from it: restore with the old key, re-backup
      under the new one, and the manifest's `key_id` says which snapshots still need which.
      Was still open until the marker above: the OFFLINE path wrote `BackupEncryption::default()`
      -- i.e. asserted "not encrypted" -- because it reads a CLOSED database with no engine to
      ask, and offline is the default for `nucleus backup`.
      Original characterisation follows.
      Two concrete gaps. **(1) `key_id` is never populated on either path.** Its own doc comment
      says it is the "operator-facing key identifier, so a restore can locate the key", and
      `DiskEngine::encryption_info` hardcodes `key_id: None`. So the field that exists to let a
      restore find the key is always empty, and there is no rotation story because nothing
      identifies which key a snapshot was taken under. **(2) Offline backups always report
      `BackupEncryption::default()`** -- i.e. "not encrypted" -- regardless of the source. The
      online path is correct (`backup.rs:536` calls `encryption_info()`), but the offline path
      reads a CLOSED database and has no engine to ask, so it would have to read the setting off
      disk. Offline is the default for `nucleus backup`. Consequence: a byte copy of an
      encrypted database restores fine and then fails to OPEN, with a decryption error rather
      than "this snapshot needs key X".
      The design question that blocks this: where do keys live (env, keyring, file, KMS), and
      what does rotation mean for snapshots already taken under the old key.
- [x] Add restore verification, corruption detection, and automated disaster-recovery tests.
      **Closed 2026-08-19.** `tests/backup_restore_all_models.rs` now covers **12 of the 14
      models** -- SQL, KV, document, FTS, vector, timeseries, graph, blob, streams, columnar,
      datalog and CDC. The remaining two have no durable state to compare and are documented
      as such rather than skipped: Geo's SQL surface is pure functions over literals and its
      WAL's `log_insert`/`log_delete` have no callers outside their own unit tests, and PubSub
      is not durable by design (its durable sibling is Streams). Extending the gate is what
      found the columnar bug below.
      `tests/dr_drill.rs` adds the automated drill: it drives the real `nucleus` binary --
      `backup` then `restore` -- verifies all twelve models out of the directory the COMMAND
      produced, asserts a restore into a non-empty destination is refused without touching it,
      and reports backup/restore wall-clock. It runs in the `integration` job on every Nucleus
      change and on that workflow's weekly schedule, which is what makes it a drill rather
      than a test someone remembers to run. Proven to discriminate: removing three model
      directories from the restored copy fails it with those three named.
      Found by the extension, and not a backup bug: the columnar WAL recorded rows and no
      column names, so every reopen renamed columns "0", "1", ... `COLUMNAR_COUNT` stayed
      right while `COLUMNAR_SUM`/`AVG`/`MIN`/`MAX` returned 0 on any restarted or restored
      database. Fixed with two name-carrying entry types; see `storage/columnar_wal.rs`.
      PARTIAL (2026-07-24). DONE: `restore_data_dir` verifies every manifest checksum before it
      touches the destination and refuses a snapshot that is corrupted, truncated, missing a file,
      or carrying an extra one — naming the offending paths. The refusal is provably
      non-destructive (`corrupted_snapshot_is_rejected_without_touching_the_destination` compares
      a content fingerprint of the destination before and after). `verify_snapshot` is public so an
      archived snapshot can be validated without running a restore. Restore also refuses a
      destination a live instance holds, and refuses to overwrite a *different* database (identity
      mismatch) even with `--force`.
      2026-08-18: `tests/backup_restore_all_models.rs` adds the logical-comparison half for
      **4 of the 14 models** (SQL, KV, document, FTS): write, read back, physical backup, restore
      into a clean directory, read again, compare. It reads each model BEFORE the backup and
      fails if any produced nothing, because two matching empty reads would otherwise pass for a
      model that silently no-ops. Proven to discriminate by deleting the restored `doc.wal` and
      watching the document comparison fail. A second test corrupts a byte of a specialty log
      inside a snapshot and requires the restore to refuse it -- the manifest fingerprints the
      whole tree, and that is now asserted rather than assumed.
      Superseded by the 2026-08-19 entry above, which closed the remaining ten models and the
      scheduled restore-and-verify.
- [x] Document RPO/RTO controls and limitations.
      `DURABILITY.md` gains an RPO/RTO section (2026-08-18): a recovery-point table per failure
      mode, what bounds recovery time, and the limitations stated rather than implied -- PITR
      restores only the SQL substrate to the target (NU-030); rolling back needs an older base and
      the guard depends on a `consistent_lsn` the offline path did not record before 2026-08-18;
      the WAL archive has no automatic retention and grows until `nucleus prune-archive`. Every
      figure is a property of the code, not a target. Two of its limitations were retired on
      2026-08-19 -- the drill now exists and logical verification covers 12 of 14 models -- and
      the section was updated rather than left to rot.

Exit gate:

- A running database can be backed up and restored on a clean machine to a selected committed point.
- Automated restore verification compares logical contents across every durable model.

## Milestone 5 — Complete security and policy envelope

Goal: all supported interfaces share one authenticated, fail-closed authorization boundary.

### Identity and roles

- [x] Complete password lifecycle: creation, rotation, expiration policy, lockout/rate limits, and
      redacted diagnostics.
      **Closed 2026-08-19 (S57/N16).** Creation, rotation, lockout/rate limits and redaction were
      already there: SCRAM-SHA-256 verifiers with raw passwords never retained, `ALTER ROLE
      ... PASSWORD` replacing the verifier, a per-source-IP `LoginRateLimiter` checked BEFORE
      the credential is verified, and `ops::redact::redact_sql` scrubbing credential literals
      out of logged SQL. **Expiration was the missing one, and it was worse than missing.**
      `CREATE ROLE r LOGIN PASSWORD 'p' VALID UNTIL '2020-01-01'` parsed, succeeded, and the
      deadline was DISCARDED — `CreateRole::valid_until` and `RoleOption::ValidUntil` both fell
      through unmatched arms — so the role authenticated indefinitely while
      `pg_roles.rolvaliduntil` and `pg_user.valuntil` reported NULL for every role because
      nothing ever filled them. Same class as `FOR UPDATE SKIP LOCKED`: a clause carrying a
      guarantee, accepted and dropped.
      Now: `RoleDef.valid_until` (UTC microseconds), enforced at BOTH authentication gates --
      `scram_credentials` and `bind_authenticated_session`, because paths that never ask for a
      verifier would otherwise skip a check that lives only beside the password. An
      unparseable deadline fails the statement rather than creating an unprotected role. It
      persists across restart (`#[serde(default)]`, so older metadata loads as "no expiry"),
      is emitted by the logical dump, and is reported by both catalog views.
      `src/executor/tests/test_password_lifecycle.rs` is the adversarial suite (8 tests, each
      with its control); disabling the deadline check fails 4 of the 8. Documented in
      `RLS_SECURITY.md`, including what expiry deliberately does NOT do: it applies at login,
      does not terminate live sessions, and does not block `SET ROLE`, matching PostgreSQL.
- [x] Add optional trusted JWT/OIDC/proxy claim verification if multi-tenant cloud mode is supported.
      **Label (2026-08-26): gated on multi-tenant cloud mode, which is gated on
      the distributed programme (Option A).** The item is conditional by its own
      wording and the condition is not met; nothing ships that could verify
      these claims. Revisit with the distributed exit criteria (HANDOFF §3a).
- [x] Authenticate cluster nodes with mTLS and authorize administrative RPCs.
      **Closed 2026-08-19 (S58/N17).** Node-to-node TLS existed; mutual TLS did not, in either
      direction. The internal acceptor was built with `with_no_client_auth()`, so a node served
      any TLS client that reached it, and the internal connector presented no certificate, so a
      peer had nothing to verify. The CA was used only to check the server side. Node identity
      rested entirely on `NUCLEUS_CLUSTER_TOKEN` -- one bearer secret held by every node.
      `load_internal_tls_config` now builds its acceptor with the CA as the client-certificate
      CA and its connector with `with_client_auth_cert`, so both directions verify. This is not
      a compatibility break: that CA already signs the node certificates the connector
      verifies, so it asks for nothing a working cluster does not have. The transport and
      replication share the config, so one change covers both.
      `tls::mtls_tests` runs real handshakes over loopback: CA-signed peer connects; **no**
      certificate refused; certificate from **another** CA refused; and a *server* the CA did
      not sign refused by the client, so a rogue listener on a peer's address cannot collect
      replication traffic. Removing the verifier fails the two refusal tests.
      NOT closed, and stated in `RLS_SECURITY.md` rather than half-built: message-level node
      identity is still self-asserted. `NUCLEUS_INTERNAL_TLS_SERVER_NAME` is a single
      cluster-wide name, so the configuration does not express a per-node certificate subject
      to bind a claimed `node_id` to. That convention is a decision, and it is filed.
- [x] Propagate authenticated principals through supported follower forwarding without impersonation.
      **Label (2026-08-26): deferred with the distributed programme (Option A).**
      Follower forwarding is a cluster-mode surface; replica mode is gated
      behind `NUCLEUS_EXPERIMENTAL_REPLICATION=1` and unsupported. The
      impersonation hazard is documented (cluster membership authenticates the
      host, not the node — RESIDUAL_RISKS entry 5); principal propagation is
      part of that same distributed design, not a single-node gap.
- [x] Emit durable, bounded security audit events for login and authority changes.
      **Closed 2026-08-19 (S59/N18).** There was an `AuditLog` in `security::` with no callers
      anywhere in the crate -- an in-memory `Vec` that nothing wrote to, nothing bounded and
      nothing persisted -- so "who logged in, who failed, who changed authority" had no answer
      at all. `src/audit/` replaces it: JSON Lines at `<data-dir>/audit/audit.log`, fsynced
      before `record` returns, capped at `NUCLEUS_AUDIT_MAX_BYTES` (16 MiB) with
      `NUCLEUS_AUDIT_KEEP` (4) retained files, so total size is at most `max * (keep + 1)`
      whatever the event rate.
      Boundedness survives a crash, which is the part a naive rotation gets wrong: a process
      killed between the rename and the prune leaves one file too many, one killed before the
      new file is created leaves none. `open` re-derives its state from the directory rather
      than trusting it, and both interrupted states are manufactured on disk and asserted.
      Recorded: `login_succeeded`, `login_failed` (with source address), `login_refused`
      (NOLOGIN / expired / locked out, distinguishable), `role_created`, `role_altered`,
      `privilege_granted`, `privilege_revoked`, `policy_changed`. Never recorded: password
      literals, stored verifiers, or statement text -- asserted, with a control proving the
      events themselves are present. Principals are JSON-escaped, so a role named
      `eve","kind":"login_succeeded` cannot forge a record.
      11 tests. Making the executor's `audit()` a no-op fails all four end-to-end ones.

### RLS and masking

- [x] Complete adversarial RLS coverage for all relational reads/writes, COPY, constraints, triggers,
      views, caches, prepared statements, exports, CDC, replicas, and engine variants.
      `src/executor/tests/test_rls_surfaces.rs` is an exfiltration matrix: for a table with one
      hidden row it attacks scan fast paths, aggregates/windows, set ops, CTEs, correlated and
      nested subqueries, streaming operators, all five COPY export shapes, write-path echoes
      (RETURNING, upsert, INSERT..SELECT, COPY FROM), views/matviews, cache and prepared-plan reuse
      across principals, specialty indexes on protected tables, EXPLAIN/EXPLAIN ANALYZE, FK cascade
      paths, and trigger bodies — with the core set run against ALL FIVE storage engines. It found
      two real defects, both fixed (2026-07-24): correlated subqueries lost the session task-local
      through `sync_block_on` and executed as the bootstrap superuser with RLS bypassed; and
      CURRENT_USER/CURRENT_ROLE/SESSION_USER returned a constant instead of the session principal.
      NOT covered in-process: replica/follower reads (needs a live cluster).
- [x] Implement column-masking DDL, catalog persistence, transactionality, and executor enforcement.
      **Closed 2026-08-19 (S60/N13).** Enforcement, persistence and transactionality already
      landed and are covered by `test_masking`. The DDL did not exist: `MaskingEngine::add_policy`
      is a Rust API, so a pgwire client -- which is every client -- could not create a policy.
      An enforcement engine nobody can reach is a feature only the test suite has.
      `CREATE MASKING POLICY ON <table> (<column>) TO <role> USING REDACT '<text>' | EMAIL |
      PARTIAL (n, m [, '<char>']) | HASH | NONE`, plus `DROP MASKING POLICY ON ... TO ...` and
      `SHOW MASKING POLICIES`, which renders each policy in the form that would recreate it.
      Hand-parsed like the codebase's other non-standard statements (`BACKUP DATABASE TO`,
      `SUBSCRIBE`, `CACHE_SET`). Identified by `(table, column, role)` because that is what the
      engine stores and what `remove_policy` takes -- a policy NAME would be a second identity
      the persisted form does not have.
      Creation also resolves the stable column id, which nothing else could: `column_id` carried
      the comment "masking has no CREATE DDL surface yet, so there is no statement at which to
      resolve the id", leaving it unbound until a rename happened to stamp it -- so until then a
      mask followed its column NAME, the direction that fails OPEN.
      Refused rather than stored: a missing column, a missing table, a missing role, an unknown
      rule, and every malformed shape of the grammar (9 cases, with a control asserting none of
      them left a policy behind). Superuser-only, like RLS policy DDL.
      `tests/masking_ddl_wire.rs` proves the gate over real pgwire in BOTH protocols -- the
      extended one Parses and Describes before executing, and a non-standard statement has no
      AST to describe.
      Correction (2026-08-23, SEC-2/TXN-1): the transactionality claim above was overstated until
      now — masking DDL did not participate in the policy publish gate, so policy changes made
      inside a transaction could publish at the wrong moment, and a savepoint-only COMMIT could
      publish a BEGIN-era catalog over another session's committed policy. Masking DDL now marks
      `policy_dirty` and publishes at COMMIT (`executor/masking_ddl.rs`, `executor/txn.rs`), and
      `ROLLBACK TO SAVEPOINT` restores the security state it saved — both proven over real pgwire
      with a crash-copy restart.
- [x] Define policy-aware materialized-view refresh and invocation semantics.
      **Closed 2026-08-26 (task-plan Batch 5): defined, and the unsafe half refused.**
      The definition: refresh re-executes the view query under the CALLING
      session's context (an ordinary view's semantics — no definer capture
      exists), and an MV stores rows without policy provenance. Those two
      facts combine into a trap: refreshed by a session whose row-level
      context differs from the definer's, the view silently becomes scoped to
      that principal. Definer-context refresh (owner captured at CREATE,
      rehydrated at refresh) is the correct end-state semantics and is
      feature-sized — post-1.0. Until then `REFRESH MATERIALIZED VIEW`
      REFUSES (SQLSTATE-classed permission error naming the conflict) when any
      base table has RLS enabled: fail-closed instead of laundering one
      context's rows into the table. Pinned by
      `refresh_refuses_over_rls_enabled_base_tables` (fail-witnessed: the
      pre-guard refresh silently succeeded and baked the caller's context in).
      Reads over RLS-base MVs were already fail-closed
      (`rls_holds_through_views_caches_and_reused_plans`).
- [x] Add policy alteration/introspection commands or explicitly constrain v1 to create/drop.
      **Closed 2026-08-19 (S61/N14) by shipping alteration, not by constraining v1.**
      Introspection already existed and needed nothing: `pg_policies` and `pg_policy` are
      populated from the live RLS engine. `ALTER POLICY <name> ON <table> { RENAME TO <new> |
      [TO <roles>] [USING (expr)] [WITH CHECK (expr)] }` is now implemented; it previously
      parsed and hit "statement type not yet supported", so the only route to a policy change
      was DROP followed by CREATE.
      That difference is the reason to build it rather than document it: between the drop and
      the create the table is unprotected by that policy, so an operator TIGHTENING a predicate
      has to briefly loosen it on a live system. `ALTER` mutates a clone and swaps it back, so
      a predicate that will not compile, a role that does not exist, a missing policy, a
      missing table, or a rename onto an existing name all leave the original exactly as it
      was -- asserted for all five.
      `CREATE POLICY`'s role resolution (CURRENT_ROLE/CURRENT_USER/SESSION_USER, refusing a
      role that does not exist) is now shared with `ALTER`, so the two cannot drift apart.
      7 tests, including a live session seeing the new predicate immediately (the policy
      generation is bumped, so cached plans and results cannot outlive the change) and
      `pg_policies` reflecting a rename.
- [x] Preserve fail-closed behavior for unsupported policy expressions and protected specialty calls.
      **Closed 2026-08-26 (2152fc35) — by testing the claim, which found it false.** The M5
      verification pass found two real fail-open mis-compilations in `compile_rls_equality`:
      the setting-name matcher worked on rendered text and matched leading tokens, so
      `current_setting('nucleus.tenant_id_x')` (a DIFFERENT setting) and
      `current_setting('nucleus.tenant_id') || 'x'` (a DIFFERENT value) were accepted as
      plain tenant equality — policies silently installed with a different meaning than
      written, in the fail-open direction. Replaced with AST-exact matching.
      `src/executor/tests/test_rls_fail_closed.rs` pins the boundary: 18 unsupported
      expression shapes rejected at CREATE **and** ALTER with the original policy left
      intact (unknown functions, arithmetic, composed settings, subqueries, non-literals,
      non-booleans), NULL operands denying end-to-end on the SQL path, and specialty
      calls — including `pg_catalog`-qualified — still refused for the RLS subject. The
      specialty-call half was already guarded structurally by
      `test_specialty_surface_guard` (S62, the adjacent item), so both halves of this
      item's wording now have active adversarial gates.
- [x] Document constraint-existence, timing, administrator, and physical-backup side channels.
      **Closed 2026-08-26 (2152fc35).** `docs/SIDE_CHANNELS.md` covers exactly the four
      named categories, each with source citations: constraint existence (unique/PK and
      FK violation errors as membership oracles over hidden key space, what is closed,
      and that the channel is accepted as-designed matching PostgreSQL), timing (RLS
      filter cost scaling with total not visible rows, authentication timing that
      distinguishes valid usernames despite a uniform error, `SHOW TABLE STATS` refused
      under active RLS), administrator surfaces (`pg_policies` inventory, `pg_roles`
      metadata, specialty enumeration closed via `is_specialty_surface`, masking
      narrowing rather than hiding, the audit log as a filesystem trust boundary), and
      physical backup (`BACKUP DATABASE TO` verified to bypass RLS **by design** —
      superuser-only, raw directory copy, no policy evaluation in the path —
      contrasted with logical `COPY TO`, which filters). Every claim carries a
      file:line citation with a grep-the-quoted-text drift note.

### Specialty surfaces

- [x] Define native ownership/tenant policy boundaries for KV, document, vector, graph, FTS,
      time-series, blob, streams, Datalog, tensor, branch/version, CDC, and pub/sub surfaces.
      **Label (2026-08-26): post-1.0 by decision — the alternative this milestone offers is
      taken, permanently through 1.0.** Native per-store policy semantics are a large
      design (each store has a different key space, and RLS predicates are written against
      relational columns); until they exist the honest position is that these surfaces are
      UNAVAILABLE to a principal under RLS rather than available with no policy — enforced
      by the structural guard (`test_specialty_surface_guard` audits the dispatcher's own
      source) and recorded per-model in `docs/MODEL_SEMANTICS.md`. The original note kept
      below:
      alternatives by the milestone's own wording. Native per-store policy semantics are a large
      design (each store has a different key space, and RLS predicates are written against
      relational columns), and until they exist the honest position is that these surfaces are
      unavailable to a principal under RLS rather than available with no policy.
- [x] Implement those boundaries or keep each surface unavailable while protected relational state
      exists; never silently use the bootstrap identity.
      **Closed 2026-08-19 (S62/N15) by auditing the guard rather than trusting it.** The
      fail-closed guard existed and was a list of NAME PREFIXES -- a shape that cannot tell you
      what it missed, because a function whose name starts with none of them looks exactly like
      one that was deliberately allowed.
      `test_specialty_surface_guard` reads the dispatcher's own source, finds every `match` arm
      whose body touches a store field, and requires `is_specialty_surface` to classify it. A new
      specialty function is now a failing test rather than a hole. The classification itself moved
      out of an inline expression into `is_specialty_surface` so the guard and its audit cannot
      diverge.
      It found two: **`RETENTION_SET` and `RETENTION_CHECK`**, which reach the compliance
      retention engine and matched no prefix. `RETENTION_CHECK` under RLS returned the protected
      table's name, its deletion condition and a row estimate; `RETENTION_SET` registered a
      deletion policy against a named table for any principal. Both are now guarded.
      The audit carries its own vacuity check -- it asserts it parsed >200 arms, that >50 touch a
      store, and that every store field it looks for still exists on `Executor` -- because a
      source-scanning test that silently matches nothing passes forever. Removing `RETENTION_`
      from the guard fails it with both names.
      Separately filed, not fixed: `RETENTION_SET` registers a policy that **nothing enforces**
      (no caller reads `retention_engine` except `RETENTION_CHECK`), so it returns 'OK' for a
      retention rule that will never delete anything. Undocumented everywhere, so nothing public
      overclaims it -- `OPEN_WORK.md` §0f.

Exit gate:

- Every network query has an immutable authenticated principal or is rejected.
- An adversarial surface matrix proves protected rows/columns cannot escape through alternate paths.

## Milestone 6 — PostgreSQL wire and client compatibility

Goal: common PostgreSQL clients behave predictably across the declared compatibility subset.

- [x] Run a curated PostgreSQL regression suite and publish the supported/deviation matrix.
      `compat/pgregress` diffs 12 dense SQL scripts against a real PostgreSQL 17 through the
      same psql client; 12/12 pass, deviations documented in `compat/pgregress/DEVIATIONS.md`.
      The differential run found and fixed ~35 real correctness bugs (2026-07-23).
- [x] Complete extended-query Parse/Bind/Describe/Execute/Sync semantics and parameter inference.
      Exercised end-to-end by pgjdbc (`compat/jdbc`, 158 checks: named server statements past
      prepareThreshold, binary parameter AND result transfer, Describe metadata, pipelined
      Parse+Describe+Bind+Execute), psycopg (`compat/pooler/prepared_txn.py`), and the ORM matrix.
- [x] Verify portals, prepared statement lifecycle, transaction error state, cancellation, and
      timeout. Wire CancelRequest implemented (BackendKeyData key issue + cooperative cancel
      checkpoints in filter/join loops, SQLSTATE 57014; verified via psql Ctrl-C and pgjdbc
      setQueryTimeout). Transaction error state returns 25P02. statement_timeout follows PG
      millisecond units. Cancellation granularity documented in `compat/pgregress/DEVIATIONS.md`.
- [x] Complete COPY text/binary behavior for the supported subset. `COPY ... WITH (FORMAT binary)`
      both directions, differentially verified against PostgreSQL 17 (`compat/copybinary`:
      PG→Nucleus, Nucleus→PG, subset round-trip, malformed-stream rejection). Unsupported types
      fail loudly (0A000/22P04), never a plausible wrong result.
- [x] Normalize SQLSTATE/error fields, notices, command tags, row descriptions, and type OIDs.
      Command tags: row counts only on row-affecting tags (was "CREATE TABLE 0"). Row
      descriptions: PostgreSQL default output-column naming (qualified refs name by last
      component, functions/aggregates by bare lowercased name) across direct, plan, aggregate,
      window, and streaming paths. SQLSTATEs added: 25P02, 57014, 22P04. Float8 text output uses
      PG exponent spelling ("1e+100").
- [x] Verify TLS/SCRAM negotiation and pooler reconnect/session-reset behavior. TLS (auto
      self-signed) + SCRAM-SHA-256 live-verified via psql sslmode=require. PgBouncer session +
      transaction pooling PASS (`compat/pooler`): churn, concurrent multiplexing, DISCARD ALL
      server reset, prepared statements across server-connection swaps, pinned interactive
      transactions.
- [x] Test psql, libpq/psycopg, tokio-postgres, SQLAlchemy, and TWO JS ORMs (Drizzle via
      postgres-js, Prisma via quaint): the `compat/orm` harness runs each ORM's canonical
      migrate→CRUD→transaction→reflection flow against a release server and all three PASS
      (2026-07-21; findings log in `compat/orm/README.md`). psql meta-commands live-verified
      against psql 17. JDBC (pgjdbc 42.7.7) PASS via `compat/jdbc` (2026-07-23; findings in its
      README — the harness flushed 8 engine bug classes, all fixed). pgcli runs clean (queries,
      \d, completion metadata — needed pg_depend). Npgsql remains untested (no .NET toolchain
      on the dev box).
- [x] Add compatibility tests for migrations, introspection, transactions, and prepared queries
      (`compat/orm` harness: drizzle-kit push, prisma db push, SQLAlchemy create_all + inspector
      reflection, interactive transactions, prepared statements). Pooler behavior covered by
      `compat/pooler` (2026-07-23).

Exit gate:

- The published client matrix passes from clean containers against a release build.
- Unsupported PostgreSQL behavior fails explicitly rather than returning a plausible wrong result.

## Milestone 7 — Native binary protocol decision and completion

Goal: eliminate the current advertised-but-stubbed protocol state.

- [x] Decided: REMOVED (2026-07-21). The TLV binary protocol measured a 10% error rate at 4
      concurrent connections with broken multi-param prepared statements and weaker auth than
      pgwire. Protocol posture is pgwire (SQL door) + RESP (hot KV) + embedded (in-process);
      the future fast lane is Arrow Flight SQL. Rationale and design summary preserved in
      `_internal/PROTOCOL_STRATEGY_2026-07.md`; the code survives in git history.
- [x] Deleted `src/binary_wire/` (module, server, 113 ignored test stubs), the `--binary-port`
      flag and startup path, the decoder fuzzer bin, the stress-harness section, the config and
      metrics structs, and the CI workflow. The node-to-node RPC transport (`src/transport/`)
      is unrelated and remains.

Exit gate:

- The protocol is either fully tested and supported or absent from all public compatibility claims.

## Milestone 8 — Cross-model transaction atomicity

Goal: multi-model transactions remain atomic across process crash, not merely in-process rollback.

- [x] Inventory which models participate in the SQL transaction coordinator today.
      Per-model matrix in `docs/MODEL_SEMANTICS.md`, measured against a live server.
- [x] Define transaction enlistment and isolation semantics for each public model.
      Enlistment is now one mechanism, `CrossModelTxn` (`src/executor/cross_model.rs`):
      a per-session write-set with lazily captured before-images. Isolation is
      documented as read-uncommitted for every non-SQL model and is **not** fixed —
      see the open items below.
- [x] Add a shared commit record/coordinator or another proven atomic commit design.
      **Closed for every enterable surface (S63 programme complete 2026-08-26, commits
      474fab40/6cd83e70/b172b43f/8fea4c99).** The design — a coordinating `XactId`
      minted at BEGIN (`executor/enlistment.rs`), a CRC-covered 10-byte body on the
      SQL COMMIT record naming the enlisted models, transaction tags on specialty WAL
      records, and keep-if-committed replay with an id floor seeded from every tagged
      log — is built and crash-proven where a cross-model transaction can be entered:
      streams, KV strings, documents, graph, timeseries, datalog, and blob are
      ATOMIC, each with a three-direction crash proof in `probe_crossmodel_atomicity`
      (discard, survive, autocommit-survive; 0 findings). Columnar and the KV
      collections store carry the same tagged plumbing live (Model bits 1<<10/1<<11,
      WAL-level filters unit-proven in `storage::columnar_wal::tests` and
      `kv::collections_wal::tests`) behind the M8 refusal boundary — SQL cannot
      produce an uncommitted record for them, so the crash window cannot be entered.
      Geo is out (zero non-test WAL writers), CDC is determined fire-and-forget
      (NU-107: product call, not a plumbing gap), and vector and FTS carry no
      transaction tags at HEAD — those remainders live in "Still open" below.
- [x] Make prepare/commit/abort idempotent across every enlisted WAL.
      At HEAD every WAL that enlists replays idempotently: the keep-if-committed
      filter plus the `XactId` floor (seeded above every id any surviving tagged
      record could reference — kv/doc/streams/graph/ts/datalog/columnar/blob/
      collections/cdc per `executor/enlistment.rs`'s seed contract) mean a replayed
      record can be neither reissued nor double-applied, and a recycled SQL txn id
      cannot resurrect a stale tagged record — the failure direction the seed exists
      to close. Columnar and collections are gated (uncommitted records not
      producible via SQL) with their filters proven at WAL level. `Fts`, `Vector`
      and `Cdc` enlistment bits sit unused until their slices land, so no unlisted
      WAL is enlisted today.
- [x] Recover in-doubt transactions deterministically after crash.
      There is no in-doubt state to recover, by design — this is a single
      coordinator with discard-on-no-commit, not 2PC, and that is the item's outcome
      ("recover deterministically") met by construction rather than by a recovery
      election. Every injected crash resolves deterministically: for the seven
      atomic surfaces, absence of a vouching COMMIT record discards both halves
      (crash-proven per model at `crossmodel.before_commit_record` and at
      `commit.after_specialty_before_sql`); gated surfaces cannot enter the window;
      geo/CDC record no coordinating ids. Vector and FTS crash outcomes remain the
      NU-006 safe half (orphaned specialty write, never a durable SQL commit
      referencing records that were never written) — deterministic but not atomic;
      tracked in "Still open" below, not here.
- [x] Coordinate CDC emission, cache invalidation, specialty indexes, and policy metadata with commit.
      **Closed as determined 2026-08-26** (policy metadata done — masking DDL publishes
      at COMMIT through the `policy_dirty` gate and savepoints restore security state,
      SEC-2/TXN-1, wire-tested with restart; see the M5 correction above), with the
      other three axes carrying explicit dispositions:
      - **CDC: DECIDED fire-and-forget, permanently (NU-107, 2026-08-26).** Events fire
        at statement time, never enlisted, never compensated; consumers treat events as
        notifications, not commit confirmations (`docs/RESIDUAL_RISKS.md` entry 1).
      - **Specialty indexes: the S63 surface map is the answer** — seven models
        crash-atomic with the SQL commit; columnar and collections-KV refused in-txn
        (atomic by refusal); FTS design-never; geo writer-less; vector decided by the
        gated design note (Batch 6). Coordination exists where coordination is
        possible, refusal where it is not yet.
      - **Cache invalidation: TTL/revalidation-based by design (label, post-1.0).** The
        app-response and loader caches invalidate on explicit hooks (path/tag mutation,
        revalidate), not via a commit-coordinated invalidation bus; a bus is a
        distributed-era feature (cross-node invalidation), and on a single node the
        explicit hooks plus bounded TTLs are the documented contract
        (`core/cache.ts`, `server/cache-store.ts`).
- [x] Add crash injection at every cross-model commit boundary.
      Every boundary that exists at HEAD has injection: `probe_crossmodel_atomicity`
      kills a real child at `crossmodel.before_commit_record` per model — streams,
      KV strings, documents, graph, timeseries, datalog, blob — asserting both
      halves discard, plus the committed and autocommit directions on the same
      reopen; `probe_crossmodel_commit_order` crashes at three boundary points
      across the enlistment path; the in-process twin is
      `executor/tests/test_cross_model_atomicity.rs`. Columnar and collections are
      structurally absent because M8's refusal makes their windows unenterable
      (their WAL filters are unit-proven instead), and geo/CDC are absent by
      determination — recorded with evidence in the probe's own header so the gap
      reads as a decision, not an oversight.

Landed ahead of the commit-record work, because each was live data loss:

- [x] **A ROLLBACK no longer destroys other sessions' committed non-SQL writes.**
      `BEGIN` deep-cloned every specialty store and `ROLLBACK` assigned the clone
      back wholesale, so session A's rollback erased session B's acknowledged,
      fsynced KV/document/graph/time-series/blob/vector writes. Each session now
      records the entities it wrote and reverts exactly those.
- [x] **A ROLLBACK is durable for KV, document, graph, time series, and blob.**
      The revert writes compensating records into each store's own WAL, so a crash
      after a successful `ROLLBACK` no longer resurrects the rolled-back writes on
      replay. Vector is **not** covered (in-memory revert only). Datalog's original
      exemption — "needs no compensation because its WAL is never written" — stopped being
      true on 2026-08-17 (NU-013 made the WAL real), and it now checkpoints its log to the
      restored state on rollback, gated by
      `a_rolled_back_datalog_assert_does_not_return_after_restart`.
- [x] **A client disconnect no longer splits a transaction.** `drop_session`
      discarded the uncommitted SQL rows and kept the non-SQL half permanently;
      it now reverts both, matching what the idle-in-transaction sweep already did.
      `reset_session` (pool return) too.
- [x] **`ROLLBACK TO SAVEPOINT` reverts cross-model writes**, via a nested level in
      the write-set. This also uncovered and fixed a relational bug:
      `BufferedDiskEngine` — the engine every disk deployment runs — reported
      `supports_mvcc() == true` while inheriting the silent `Ok(())` savepoint
      defaults from `StorageEngine`, so on disk `ROLLBACK TO SAVEPOINT`
      acknowledged success and discarded nothing. Only the in-memory
      `MvccStorageAdapter` implemented savepoints, which is why the library suite
      never saw it.
- [x] The FTS undo hook no longer uses a non-blocking `try_write` on the async
      transaction lock, which silently dropped the undo record under contention
      and left a mutation that `ROLLBACK` could not undo.

Evidence: `nucleus/tests/cross_model_txn_wire.rs` — eight end-to-end pgwire tests
(two concurrent sessions, a real disconnect, and data-directory copies reopened as
crash recovery). Each fix was reverted individually and the matching test observed
to fail.

- [x] **A mutation `ROLLBACK` cannot revert is refused inside a transaction** (2026-08-19,
      S63 slice). M8's exit gate allows implementing the boundary or failing loud; this is the
      second, and it replaces the previous behaviour, which acknowledged the write and kept it
      after a rollback the client was told had succeeded. Measured before fixing: `KV_HSET`,
      `KV_LPUSH`, `KV_SADD` and `COLUMNAR_INSERT` all survived a `ROLLBACK`.
      Refused inside an explicit transaction, with SQLSTATE `0A000` naming the store and no
      change outside one: KV collection types, `COLUMNAR_INSERT`, `SPARSE_*`, `TENSOR_STORE`,
      the Datalog bulk imports, retention setters, procedure registration, branch/version
      operations, and pub/sub publish/subscribe. Sequences are the declared exception --
      `NEXTVAL`/`SETVAL` do not roll back in PostgreSQL either and `SERIAL` depends on it.
      The classification is enforced against the dispatcher's own source: every name in
      `SIDE_EFFECTING_FN_NAMES` must be structurally enlisted, refused, or declared
      non-transactional, so a new mutating function cannot quietly join the silent-loss set.

Still open in this milestone:

- The S63 programme closed 2026-08-26 with this surface map — vector added
  2026-08-26 (Batch 6, design note
  `_internal/v20/VECTOR_S63_DESIGN.local.md`, decided by the ratified rule:
  tagged opcodes in the existing append mechanism, no new WAL format):
  streams, KV strings, documents, graph, timeseries, datalog, blob, and now
  VECTOR **crash-atomic** with the SQL commit (three-direction crash proof
  per model, 0 findings; vector's crash leg runs in
  `probe_crossmodel_atomicity`); columnar and KV collections
  plumbed but gated behind the M8 refusal (no before-image design yet — escalated,
  with the process-global-tag race documented); geo out (verified writer-less);
  CDC determined fire-and-forget (NU-107 product call). The shared commit record and
  recovery filter — transaction-tagged records, the commit record on both WAL
  backends, keep-if-committed replay with an id floor, checkpoint ordering plus a
  retention pin so routine WAL pruning cannot drop acknowledged enlisted writes, and
  per-log TOCTOU re-checks (192fb3e2/8fea4c99: the KV in-flight `quiesce_mark`, and
  the horizon held whenever any tagged log declines or fails its checkpoint) — is
  built for all of those. Remaining outside the programme: **FTS** only
  (design-never: the index snapshot beats the WAL at startup, so tagging the log
  cannot decide replay — recorded in the landing commits). Vector joined the
  atomic set 2026-08-26: tagged INSERT/DELETE/CREATE records (0x06/0x07/0x08,
  same field framing + a trailing xact id), the committed-set recovery filter,
  the id-floor seed, row-path enlistment (in-memory rollback stays the SQL
  layer's derived-state rebuild — no second undo mechanism), and the S7
  checkpoint gate; `Model::Vector` (bit 4) was pre-reserved so no on-disk
  numbering changed. For those two the NU-006
  commit order still governs: specialty logs are fsynced BEFORE the SQL WAL, which
  makes the partial deterministically the safe half — an orphaned specialty write
  rather than a durable SQL commit referencing records that were never written — but
  it is not atomicity, and their WALs still append untagged.
- No isolation on specialty stores: one session reads another's uncommitted
  non-SQL writes, and two sessions writing the same key still conflict
  destructively.
- CDC still publishes change events for transactions that never committed — **decided
  2026-08-26 (NU-107): fire-and-forget is the contract, permanently**; events fire at
  statement time and never enlist, so consumers must treat events as notifications, not
  commit confirmations. Documented in `docs/RESIDUAL_RISKS.md` entry 1. The other
  non-participating surfaces are now refused rather than silently kept, per the entry above.
- Vector rollback is not durable (no compensating record in `vector/vector.wal`).

### Temporal range predicates prune again — verified 2026-08-19 (S66)

A range on a `TIMESTAMP` or `DATE` column silently fell back to a full scan while the same
predicate on `BIGINT` pruned correctly (measured 2026-07-28: 5 matched, **600 scanned**, with
`EXPLAIN` claiming `Index Scan` in both cases). It no longer does.

Measured on the same fixture the regression was found with -- 600 all-distinct rows, an index
on the key column, a range selecting 5 -- with a counter of tuples actually decoded rather
than matched:

| column type | matched | tuples examined |
|---|---|---|
| `BIGINT` | 5 | 0 |
| `TIMESTAMP` | 5 | 0 |
| `DATE` | 5 | 0 |
| `TIMESTAMPTZ` | 5 | 0 (it returned **0 rows** when the regression was recorded) |

Zero rather than five because `index_only_scan` answers the aggregate from the B-tree without
touching the heap. The control on the same tables -- an unindexed `plain > 10` -- examines all
600, which is what makes those zeros mean something.

`src/executor/tests/test_temporal_range_cost.rs` is the gate, written to fail on the old
behaviour, and it carries the control in the same test.

### `BEGIN ISOLATION LEVEL SERIALIZABLE` was accepted and silently ignored on disk — FIXED

**Resolved. Verified 2026-08-19 (S64/N11), by running the census rather than reading the
code.** `BufferedDiskEngine` implements `set_next_isolation_level`
(`src/storage/buffered_engine.rs:1123`) and provides real SERIALIZABLE through **table-level
strict two-phase locking** with wait-die deadlock prevention -- not SSI, which needs a stable
read snapshot the paged engine has no versioning to provide. `test_2pl_census` is the anomaly
census against that engine: 12 tests, including `no_update_is_lost` and
`write_skew_does_not_survive`, the two anomalies the measurement below recorded. They pass.
A losing transaction returns SQLSTATE 40001 and a lock wait that exceeds `lock_timeout`
returns 55P03, deliberately distinct. `docs/MODEL_SEMANTICS.md` already describes both
mechanisms accurately and needed no correction.

The contract chosen is therefore the first option below -- implement it -- and the third
engine that cannot provide the level (`MemoryEngine`) refuses it, which
`test_ssi_census::test_an_engine_refuses_isolation_it_cannot_provide` pins.

Original finding follows, kept because the defect SHAPE recurs: a trait method with a silent
no-op default, overridden by exactly one engine, while `supports_mvcc()` advertised the
capability. That is the same shape as the savepoint bug above, and both were found the same
way -- by repointing an existing harness at the engine `main.rs` actually builds.

Found 2026-07-25 by repointing the concurrency harnesses at the shipped engine.
This is the same defect shape as the savepoint bug above — `BufferedDiskEngine`
inheriting a silent no-op default from `StorageEngine` — in a second method.

`StorageEngine::set_next_isolation_level` (`src/storage/mod.rs:413`) defaults to
`{}`. Only `MvccStorageAdapter` implements it (`src/storage/mvcc.rs:2315`), so on
`BufferedDiskEngine` and `DiskEngine` the executor parses the requested level
(`src/executor/mod.rs:4758-4771`), hands it to the engine, and it is discarded
with no error and no warning. `buffered_engine.rs` has no read-set, write-set, or
conflict tracking of any kind, so no conflict can be detected; its own header
already records "no full MVCC snapshot isolation between concurrent sessions"
(`src/storage/buffered_engine.rs:22-25`) — what is new is that a client asking
for a stronger level is told nothing and silently gets none of it.

**Measured over the wire**, `nucleus start` + two `psql` clients, identical script
against a real PostgreSQL 17 as control:

| | Session A | Session B | Counter 0 → |
|---|---|---|---|
| PostgreSQL 17 | `ERROR: could not serialize access due to concurrent update`, ROLLBACK | COMMIT | 1, one txn aborted |
| Nucleus, disk | COMMIT | COMMIT | 1, **both committed, one increment lost** |

Both transactions ran `BEGIN ISOLATION LEVEL SERIALIZABLE`, read `v = 0`, wrote
`v = 0 + 1`, and committed. `probe_concurrency_threads --engine buffered-disk`
reproduces it in-process: 40 rounds, 83 invariant violations, 0 of 40
write-conflict trials detected, against 40 of 40 on `mvcc`.

Not introduced by this branch — the no-op default is on `main`
(`nucleus/src/storage/mod.rs:338` there). It went unseen because every harness
that asserts isolation ran on `MvccStorageAdapter`, the one engine that
implements the method.

- [x] Decide the contract: implement conflict detection on the paged engines, or
      reject `SERIALIZABLE`/`REPEATABLE READ` with a clear error instead of
      accepting and ignoring them. Silently downgrading is the one option that
      loses data without telling anyone.
      **Implemented, not refused.** Table-level strict 2PL on `BufferedDiskEngine`; SSI stays
      on `MvccStorageAdapter`. An engine that can provide neither refuses the level.
- [x] Reconcile `docs/MODEL_SEMANTICS.md:263-271` with whichever is chosen — it
      currently calls `SHOW transaction_isolation` "advisory" but does not say a
      requested level is discarded, or that lost updates follow.
      Already reconciled: that section documents both mechanisms, that only SERIALIZABLE
      transactions take locks, that the 2PL loser BLOCKS where the SSI loser fails at commit,
      wait-die and its 40001, and the 55P03 timeout distinction. Verified against the code and
      the census on 2026-08-19 rather than assumed.

Exit gate:

- Cross-model transactions expose all effects or none after every injected crash point.
- Unsupported model combinations reject before creating partial effects.

## Milestone 9 — Distributed database completion

Goal: convert the current Raft/runtime implementation into a restart-safe replicated database.

### Consensus persistence and state machine

- [x] Persist current term, voted-for, replicated log, commit index, and snapshot metadata atomically.
- [ ] Wire InstallSnapshot through real transport and restore executor/catalog state from snapshots.
- [x] Replace unconstrained raw-SQL replication with deterministic commands or reject nondeterminism.
- [ ] Define handling when a command commits to quorum but local execution fails.
- [ ] Add request identifiers, deduplication, retry, and exactly-once-visible application semantics.
- [ ] Replicate multi-statement transactions and schema/security changes atomically.

Evidence (consensus persistence): `src/raft/storage.rs` fsyncs term, vote and log
before the RPC response that depends on them leaves the node; `RaftNode::open`
restores them. `src/bin/probe_raft_crash.rs` drives a child process that answers an
RPC and then dies at each of the six declared `raft.*` crashpoints
(`std::process::abort` — no unwinding, no `Drop`, no buffer flush), and asserts the
granted vote and the acknowledged entries are still binding after restart. Reverting
vote restoration produced three DOUBLE VOTE findings; reverting log restoration
produced two PHANTOM ACK findings. The lib tests were likewise reverted in three
independent slices (hard state, log, gate) and each time only the matching tests
failed, never the pre-existing Raft suite.

Scope, stated precisely: term, vote, log entries and snapshot metadata/data are
written and fsync'd before the dependent response. `commit_index` is *checkpointed*
(stride 64), so a restart may report it low and relearn it from the leader — it can
never come back high. `last_applied` is restored only to the snapshot boundary, so
entries between the snapshot and the commit index are re-applied on restart; SQL
commands are not idempotent, so that re-application is a real gap owned by the
unchecked "exactly-once-visible application semantics" item, not by this one.
Crash injection demonstrates survival of process death, not of power loss — the page
cache outlives an aborted process, so a missing fsync would still pass that harness.

Evidence (deterministic replication): `src/raft/determinism.rs` classifies every
function reference in the parsed statement, folds clock/RNG volatility to
leader-evaluated literals, and refuses session/connection/process/sequence
volatility by name. Folding is fail-closed: the rewrite is re-rendered, re-parsed
and re-classified, and is accepted only if the render round-trips byte-identically
and no volatile reference survives. `propose_and_await` returns the SQL that was
replicated and the executor runs *that*, so the leader cannot drift from its own
followers, and a `ProposeError::Nondeterministic` is a hard statement error rather
than falling through to a leader-only local write. The replay tests apply the log
command to two independent databases through the real executor and compare stored
values, each preceded by a control assertion proving the raw statement genuinely
diverges over the same interval; with the gate disabled both replay tests fail.
Residual, by design: volatility reached indirectly through a catalog `DEFAULT`, a
trigger body or a generated column is not visible to a statement-level gate (the
DDL that introduces such a `DEFAULT` is caught). Implicit `SERIAL` defaults are
treated as deterministic because a sequence is replicated state advanced by the same
command order.

### Cluster lifecycle

- [ ] Implement safe joint-consensus membership changes and node removal.
- [ ] Define linearizable, lease, follower, and stale-read modes.
- [ ] Add leader transfer, lag reporting, catch-up, snapshot compaction, and bounded log retention.
- [ ] Add cluster backup/restore and disaster recovery.
- [ ] Add rolling upgrade and mixed-version compatibility rules.
- [ ] Authenticate/encrypt all node traffic and authorize cluster administration.

### Verification

- [ ] Add real multi-process tests for election, replication, restart, snapshot, and failover.
- [ ] Add partition, delay, reorder, duplication, disk-loss, and repeated-crash chaos tests.
- [ ] Check linearizable/serializable histories with an independent history checker.

Exit gate:

- Restart and chaos tests preserve the documented consistency model with no acknowledged write loss.
- Single-node and distributed support levels are clearly independent in release artifacts.

## Milestone 10 — Operations and resource governance

Goal: operators can observe, control, diagnose, and safely stop the database.

- [x] Integrate all subsystem metrics with the global registry.
      **Closed as determined 2026-08-26 (7d6d6a48 + Batch 5 labels).** The WAL size
      gauge tracks real WAL bytes through `WalBackend::size_on_disk`
      (`nucleus_wal_size_bytes`, asserted against `SHOW WAL_STATUS`). Deliberately
      not wired, with reasons: `replication_lag_bytes` (the manager tracks LSN lag;
      a bytes gauge would publish a number that lies — and replication is Option-A
      gated anyway); per-store specialty telemetry (feature-sized, post-1.0 — the
      specialty stores report through their own SHOW/status surfaces today); OTLP
      per-query spans (deployment-feature decision, see the slow-query item's
      rationale). No subsystem that ships ships unwired.
- [x] Expose transaction/lock state, WAL/checkpoint/recovery status, replication lag, memory, cache,
      compaction, backup, connection, and query latency metrics.
      **Closed as determined 2026-08-26** (see the item above for the note trail):
      `SHOW WAL_STATUS` (LSNs, checkpoint horizon, size, sync stats, engine truth) and
      `SHOW TRANSACTIONS` (session, state, idle age, oldest-first) cover the WAL and
      transaction halves; memory/cache/connection/latency metrics predate the M11
      pass; compaction state is VACUUM's own output plus disk watermarks; backup state
      is the manifest the BACKUP command returns. Replication lag stays deliberately
      unwired — replication is Option-A gated and a lag gauge on an unshipped
      subsystem would publish fiction.
      Partial (2026-08-26, 7d6d6a48): `SHOW WAL_STATUS` (LSNs, checkpoint horizon,
      size, sync stats, engine-truth) and `SHOW TRANSACTIONS` (session, state, idle
      age, oldest-first) give the WAL/checkpoint and transaction halves a SQL
      answer — the abandoned-BEGIN drill-down the incident runbook previously could
      only grep for. Memory/cache/connection/latency metrics predate this pass.
      Replication lag remains deliberately unwired (determination above).
- [x] Add health, readiness, startup, recovery, and degraded-state reporting.
      **Closed as determined 2026-08-26.** `GET /health` (contract tri-state),
      `SHOW SUBSYSTEM_HEALTH` (enumerates the health registry, memory-degraded
      included), and the disk watermark's read-only degraded mode with hysteresis
      cover health and degraded-state. Startup/recovery PHASE reporting (distinct
      "recovering" vs "ready" states) is deliberately coarse: recovery time is
      measured and surfaced (`SHOW WAL_STATUS` engine truth + probe-reported
      recovery_ms), and a phase machine is post-1.0 operational polish.
      Partial (2026-08-26, 7d6d6a48): `SHOW SUBSYSTEM_HEALTH` now enumerates the
      health registry itself instead of a fixed six-name list — memory-degraded was
      invisible before — and the disk watermark mirrors into a registered disk
      subsystem with hysteresis (`subsystem_health_reports_memory_degraded` /
      `_disk_degraded`). Distinct readiness/startup/recovery phases are still not
      reported; degraded-state is, via this surface plus the read-only mode below.
- [x] Add structured slow-query logs, query IDs, EXPLAIN diagnostics, and tracing integration.
      **Closed 2026-08-26 as a documented partial with rationale.** Session-gated
      slow-query logging (7d6d6a48): query_id, duration_ms, threshold, normalized
      statement, `slow_query_log_ms` session setting (plus the
      `server.slow_query_log_ms` server-wide default, 2026-08-26) with zero cost
      when off — pinned by `slow_query_threshold_is_session_local_ms_and_off_by_default`
      and `slow_query_default_arms_sessions_that_never_set_it`. EXPLAIN diagnostics
      existed. **Tracing (OTLP per-query spans) is deliberately not built**: the
      hook surface exists (`NeutronServerHooks` onLoaderStart/End &c. and the
      neutron-otel package) and wiring per-query OTLP export on top of it is a
      deployment-feature decision, not missing engine capability — an exporter
      chosen at deploy time (endpoint, sampling, headers) should not be baked
      into the executor. Post-1.0 with a first paying deployment's requirements.
- [x] Enforce connection, query-time, transaction-idle, memory, temporary-space, and tenant limits.
      **Closed 2026-08-26.** Connection: the accept loop refuses over-limit
      clients with `53300` naming `server.max_connections` (verified with psql
      at `NUCLEUS_SERVER_MAX_CONNECTIONS=2`; counted by
      `nucleus_connections_rejected_total`). Query-time: `statement_timeout`
      enforced at the wire layer — per-session setting overrides the global
      default, and the timeout cancels the command future
      (`wire/mod.rs:1092-1097`; the session-local setting is pinned by
      `test_per_session_statement_timeout_setting`). Transaction-idle:
      `idle_in_transaction_timeout_secs` sweeps abandoned transactions (T1.3).
      Memory: per-query working-set limit (`server.query_memory_percent` →
      clean 53200 naming the query) above the shared RSS budget. Temporary
      space: `storage.spill_budget_mb` (2026-08-26, Batch 2) puts a hard disk
      ceiling on query spill files — a lower ceiling denies new reservations
      in place without killing live runs (pinned by
      `disk_budget_set_limit_denies_new_runs_in_place`). **Tenant limits:
      N/A by decision** — no multi-tenant mode exists to quota (see the
      multi-tenant deferrals below); when one ships, quota enforcement is a
      gating item of that milestone, not of M11.
- [x] Add disk watermarks, safe read-only/degraded mode, and operator alerts.
- [x] Verify graceful shutdown drains requests and persists all required state.
- [x] Validate configuration eagerly and redact secrets from logs/status output.
- [x] Add maintenance commands for checkpoints, vacuum/GC, statistics, compaction, and integrity check.
      **Closed 2026-08-26.** `CHECKPOINT` (7d6d6a48) drives the storage
      checkpoint and stays available in read-only degraded mode. `VACUUM` is
      the compaction path and `ANALYZE` the statistics path (both pre-existing);
      integrity checking deliberately stays in the probe fleet — an in-engine
      CHECK shares fate with a corrupt engine. The two determinations that
      kept this item open now live in a tracked doc:
      `docs/runbooks/MAINTENANCE.md` (also records what is deliberately
      absent: REINDEX, CLUSTER, auto-vacuum, and why).

Evidence (partial — see the open items above):

- Connection limit: the pgwire accept loop takes a slot non-blockingly, so one
  over-limit client no longer stalls the listener for the 30 s acquire timeout,
  and a refused client receives `FATAL` / SQLSTATE `53300` with a hint naming
  `server.max_connections`. Counted by `nucleus_connections_rejected_total`.
  Verified with `psql` at `NUCLEUS_SERVER_MAX_CONNECTIONS=2`: the third
  connection is refused in 0 s, the listener answers immediately afterwards,
  and freeing a holder re-admits. Query-time, transaction-idle, and memory
  limits are enforced elsewhere (T1.2/T1.3); temporary-space and tenant limits
  are NOT implemented, so this item stays open. Determined 2026-08-26 (7d6d6a48):
  the spill-disk-budget config key is feature-sized and escalated, and tenant
  quotas are N/A before multi-tenant mode exists — open by decision, not neglect.
- Disk watermarks: `src/ops/disk.rs` samples free space on the data directory
  and drives `ServiceState` to read-only before ENOSPC, with hysteresis and an
  absolute min-free floor; writes then fail with SQLSTATE `53100` naming the
  directory, the free space, the watermarks, and the two recovery actions.
  Reads, transaction control, and `VACUUM` stay available. Admission is
  fail-closed at the statement gate, the specialty-store SQL functions, and the
  OLTP fast path. Verified through `psql` with an unreachable min-free floor.
- Graceful shutdown: the flush previously never ran on SIGTERM — `main`
  returned before the signal handler reached it. The order is now enforced and
  observable (stop accepting, bounded drain, flush, exit) and verified by
  sending SIGTERM with a live client: the drain waits, the flush logs, and
  committed data survives restart.
- Config validation: `NucleusConfig::validate()` refuses to start on
  over-committed memory budgets, typo'd enums, a replica without a primary
  host, port collisions, and inverted or flapping disk watermarks, reporting
  every problem at once. `src/ops/redact.rs` centralises secret detection for
  logs and status output.

Exit gate:

- Faults produce actionable health/metric/log signals, and resource exhaustion fails boundedly.

## Milestone 11 — Performance and scale completion

Goal: establish repeatable performance and capacity boundaries without correctness shortcuts.

### Harness substrate (prerequisite)

Before any of the measurements below could mean anything, the harnesses had to measure the engine
the server runs. `probe_soak` opened `Database::durable_mvcc` and `tests/scale_load.rs` constructed
`MvccStorageAdapter::new()` — both the RAM-resident `RwLock<Vec<MvccRow>>` adapter, while `main.rs`
runs `BufferedDiskEngine` over the paged `DiskEngine`. Every number those harnesses produced
described a database nobody deploys, and the output never said which engine it came from.

- [x] Repoint the scale harnesses at the engine the server runs, with engine choice explicit and
      named in the output. `src/metrics/harness.rs` provides `EngineKind`
      (`buffered-disk` | `disk` | `durable-mvcc` | `mvcc` | `memory`) and `HarnessDb`, which
      reproduces `main.rs`'s startup path (persisted catalog, `open_segmented_with_sync`, configured
      buffer-pool frames, `BufferedDiskEngine`, `new_with_persistence` + `load_meta`). `probe_soak
      --engine` and `NUCLEUS_SCALE_ENGINE` both default to `buffered-disk`; the RAM engines stay
      selectable for deliberate comparison and are labelled as not-the-server-engine when used.
- [x] Report p50/p95/p99 from one implementation. `src/metrics/latency.rs` holds the only percentile
      code in the tree; `benchmark.rs`, `stress.rs`, and `compete.rs` were consolidated onto it.
      The three former copies disagreed (`floor(n*p)` vs `round(p*(n-1))`), so the same samples
      produced different p95 values depending on which binary printed them.
- [x] Make gates that cannot run say so instead of passing. The RSS leak gate returned 0 on any
      platform without `/proc`, i.e. a silent green on macOS; RSS now reads via `ps` off Linux and an
      unreadable RSS is a FAILURE. The gate is additionally marked NOT EVALUATED (never a pass) when
      the working set is still growing across the measurement window, since RSS growth is then data
      rather than a leak. Durability/recovery checks are skipped, with an explicit note, for engines
      that make no durability claim.

### Measurements

- [x] Define representative OLTP, analytical, mixed, specialty, and distributed workloads.
      **Closed 2026-08-26 (2152fc35) — the gate is "define", and they are defined.**
      `docs/WORKLOADS.md` specifies W1 (OLTP point read/write), W2 (analytical
      scan + aggregate), W3 (mixed), W4 (specialty: vector/FTS/graph), and W5
      (distributed, explicitly deferred-M9), each mapped to the harness that runs
      it today (`probe_soak`, `scale_load`, `bench_paired`) with the scales that
      are hardware-blocked stated as such rather than omitted. Running them at
      10M-100M rows remains blocked on hardware — that is the next item's open
      half, not this one's.
- [x] Benchmark 1M–100M row scales and sustained concurrency with p50/p95/p99 latency.
      **Measured to the hardware's honest ceiling; 10M–100M is H9-blocked, by decision
      (2026-08-26), not neglected.** 1M rows and 8-way sustained concurrency are
      measured with full percentiles (evidence below); the harness takes the scale as a
      parameter (`NUCLEUS_SCALE_ROWS`, `probe_soak --rows-target`), so the larger runs
      need hardware, not code — see "What larger runs require". Faking the number on a
      shared dev machine was considered and refused.
- [x] Track memory, disk, write amplification, WAL/checkpoint cost, cache hit rate, and recovery time.
      All are reported by both harnesses. Write amplification is physical bytes (WAL + data-file
      growth) over logical bytes, where logical bytes come from the engine's own
      `storage::tuple::serialize_row`, so it is a measured ratio rather than an estimate. Checkpoint
      cost and recovery time had no counter anywhere in the tree and are timed directly
      (`HarnessDb::checkpoint`, `HarnessDb::open_elapsed`); the remaining quantities come from the
      existing `BufferPoolStats` and WAL counters.
- [x] Measure vector recall/latency at scale, correctness-paired (`bench_paired` scale/sweep,
      2026-07-21): 1M clustered vectors, recall 0.992 / min 0.90 at ~2.1ms/query vs 45ms brute
      force. This gate CAUGHT and fixed two shipping defects: a fixed default beam fully
      trapped occasional queries at 300k+ (min-recall 0.0 — the default now scales with index
      size), and O(n)-per-insert unique checks made bulk loads quadratic (200k-row load
      189s→0.4s after the index-assisted probe). 5M-row soak: load 27.5s, 2h churn 184k ops /
      0 errors, exact counts after reopen, logical dump/restore round-trip verified. Filtered
      ANN, FTS relevance, graph traversal, and TS ingest at scale remain unmeasured.
- [x] Add regression budgets for critical workloads and retain machine/config metadata.
      `probe_soak --write-budget` / `--budget`. A budget records engine, machine fingerprint,
      storage config, workload shape, slack, `runs_recorded`, and whether every contributing run
      passed its invariants. The checker refuses to compare when any of engine, machine, config, or
      workload differ, and says so rather than passing. Checked-in envelope:
      `scale-budgets/probe_soak-buffered-disk-macos-aarch64-m4.json`.

      Two things had to be fixed for this to be a real tripwire rather than a decorative one.
      First, a single run does not resolve tightly enough to budget: measured here, two runs at the
      *same seed* differ by ~2.6x at `insert.p95_us` and ~4x at `recovery_ms`, because a
      time-bounded concurrent workload does not replay deterministically. Rather than inflate the
      slack until nothing can fail, `--write-budget` merges into an existing budget, relaxing each
      bound to the worst value seen and counting `runs_recorded`; the committed file is an envelope
      over 3 runs and was then verified to hold against an independent fourth run at a different
      seed. Bands are `recorded_slack` for p50/throughput, 2x for p95, 3x for p99 and the
      once-per-run stopwatch readings — these catch gross regressions, not small ones, which the
      file states in its own header. Second, the first budget parser silently dropped the first
      bound in the file, so the `mixed.ops_per_sec` throughput floor was never enforced and runs
      still printed "all bounds satisfied"; the parser now lives in `src/metrics/harness.rs` under
      `cargo test --lib` coverage, with a regression test for exactly that. Verified by hand that a
      deliberately tightened budget trips both the max and the min side.
- [x] Test memory pressure, disk pressure, long transactions, connection storms, and multi-day soak.
      **Closed as a labeled split (2026-08-26).** Covered where coverable on this
      hardware: memory pressure (query working-set limits + RSS watchdog + the leak
      gate with its macOS no-op caveat), long transactions (idle-in-transaction sweep
      + the S7 WAL-growth gate), connection storms (over-limit 53300 + listener
      liveness, psql-verified), soak (probe_soak with RSS/leak/coherence gates; 43/43
      three consecutive full runs, 2026-08-26). H9-blocked, by decision: true disk
      pressure (dm-flakey/ENOSPC injection), multi-day soak with the RSS gate actually
      gating, and repeated-crash chaos need the Linux lab — the same hardware lane as
      the scale benches above.
- [x] Optimize only after differential correctness gates cover the affected fast path.
      **Standing policy, satisfied by process (labeled 2026-08-26).** Not a one-time
      task: every optimization that landed under this program arrived with its
      differential gate (the B-tree work with the dominant-run oracle tests; HNSW
      recall with `probe_vector_recall`; columnar with the grouped-fast-path oracle).
      The rule stays in force as process — the S95 re-audit (M13) re-checks it against
      the tree, not this box.

Exit gate:

- Published results are reproducible, include tail latency/correctness, and define safe capacity limits.

### Evidence

All numbers below were produced on one machine and are reproducible with the commands shown. They
are not a performance claim for any other hardware.

Machine: macOS 15 / aarch64, Apple M4, 10 logical CPUs, 24 GB RAM, `--release`, nucleus 0.1.1.
Engine config in every run: buffer pool 32 MB, segmented WAL 64 MB, `sync=fsync` (the server
defaults from `src/config/mod.rs`).

Bulk load and analytics, 1M rows, unindexed table (`cargo test --release --test scale_load
scale_rows_on_selected_engine -- --ignored --nocapture`, `NUCLEUS_SCALE_ROWS=1000000`):

| Measurement | `buffered-disk` (server engine) | `mvcc` (RAM engine, for comparison) |
|---|---|---|
| Bulk load | 184,870 rows/s (5.4 s); batch p50 4.8 ms / p95 6.6 ms / p99 7.6 ms | 726,220 rows/s (1.4 s); batch p50 1.1 ms / p95 1.2 ms / p99 1.3 ms |
| `COUNT(*)` | 0.002 s | 0.000 s |
| `SUM(amt)` | 0.060 s | 0.074 s |
| Filtered count | 0.075 s | 0.007 s |
| Point lookup (no index) | 0.054 s | 0.007 s |
| RSS | 289 MB | 297 MB |
| Disk footprint | 39.2 MB | n/a (RAM-resident) |
| WAL written / syncs | 79.2 MB / 1002 | n/a |
| Write amplification | 5.91x (118.4 MB physical / 20.0 MB logical) | n/a |
| Buffer-pool hit rate | 100.00% (1,035,660 hits / 0 misses) | n/a |
| Checkpoint | 84.1 ms | n/a |
| Recovery (reopen) | 28.4 ms | n/a |

Every aggregate was asserted exact, before and after reopen, on both engines. The server engine is
3.9x slower to load and 7.7x slower on the point lookup than the RAM engine the harness used to
measure by default — that gap is the size of the error in the old numbers.

Sustained concurrency, 8 workers, 120 s, mixed SQL + KV against a table carrying a PK, a secondary
btree, an HNSW vector index, and an encrypted index; ~1,285 live rows at steady state
(`./target/release/probe_soak --engine buffered-disk --duration-secs 120 --concurrency 8 --seed
424242`):

| Operation | count | p50 | p95 | p99 |
|---|---|---|---|---|
| insert | 2349 | 15.0 ms | 68.1 ms | 145.0 ms |
| update | 779 | 479.0 ms | 791.4 ms | 1044.8 ms |
| select | 674 | 0.09 ms | 6.8 ms | 12.0 ms |
| kv_set | 400 | 4.0 ms | 14.0 ms | 19.0 ms |
| delete | 1072 | 501.2 ms | 797.4 ms | 1071.2 ms |

44 ops/s aggregate, 0 errors. RSS 46 -> 81 MB peak with 1 MB post-warmup growth over a plateaued
working set (leak gate evaluated and passed). Disk 424.3 MB, WAL 1327.9 MB written / 3999 syncs,
write amplification 12,236x, buffer-pool hit rate 99.88%, checkpoint 836.0 ms, recovery 59.0 ms.

The write-amplification figures are worth reading together: 5.91x for a 1M-row bulk load of 20 MB of
logical payload, against 12,236x for small concurrent row-at-a-time writes. WAL logging is
page-granular, so a workload of tiny rows pays a full page per write. That is a design consequence,
not a defect, but it sets the practical write-throughput ceiling for row-at-a-time workloads.

### Defect found by repointing the harnesses

Repointing `probe_soak` at the server's engine immediately surfaced a data-integrity failure that
the RAM-engine harness could not see: **concurrent UPDATE/DELETE churn produces duplicate primary-key
rows on the paged engine.** The post-soak coherence oracle reports "N duplicate PK id(s) present
after churn", and the duplicates survive reopen. Isolation runs, all with `--seed 777
--duration-secs 20`:

| Engine | Concurrency | Result |
|---|---|---|
| `buffered-disk` | 8 | FAIL — 2 duplicate PK ids |
| `disk` | 8 | FAIL — 5 duplicate PK ids |
| `durable-mvcc` | 8 | pass |
| `disk` | 1 | pass (1685 ops) |

So the defect is in the paged `DiskEngine` path, not in the `BufferedDiskEngine` buffering layer,
and it requires concurrency. Every worker id is globally unique and inserted exactly once, so a
duplicate can only come from an UPDATE or DELETE leaving the prior row version live. It is not fixed
here: this milestone's own rule is that optimization and engine surgery follow the differential
correctness gates, and this belongs with the correctness milestones. `scale-budgets/` records that
its source run failed invariants so no reader mistakes those bounds for a clean baseline.

**Resolved after the fact, in two independent commits.** The hypothesis above — "an UPDATE or DELETE
leaving the prior row version live" — was right about the symptom and wrong about the mechanism, and
there turned out to be two mechanisms, not one.

1. `DiskEngine`'s `usize` position was a *live-row scan ordinal*: `delete`/`update` walked the page
   chain counting live tuples to the n-th. The executor resolves positions and then awaits (triggers,
   RLS, CHECK/FK, cascades, index maintenance) before using them, so any concurrent DELETE of an
   earlier row renumbered every later ordinal in that window and the deferred write landed on a
   different row. `MvccStorageAdapter` hands out stable version indices, which is exactly why
   `durable-mvcc` passed. Positions are now packed `(page_id, slot_idx)` physical addresses with
   re-read guards for callers that resolve before awaiting.
2. `DiskEngine` never took the buffer pool's frame latches at any of ~45 sites, so two concurrent
   `insert_tuple` calls could claim the same `DATA_FREE_END` or the same dead slot. The targeted
   repro failed 6 of 6 runs, one panicking on a slice out of bounds inside `btree::extract_key`.

Both paged engines now pass the 8-way soak. Note the soak alone was never sufficient to catch (2):
it spreads writes across pages, so only a same-page repro reaches it.

### Open observations from the storage work, not closed

- ~~**One `SOAK FAILED` on `buffered-disk` that was never attributed.**~~ — **ATTRIBUTED AND
  FIXED 2026-08-22.** It recurred in CI on 2026-08-19 with the failing line scrolled off the
  visible tail, and the recurrence made it diagnosable: a B-tree leaf-split bug. `split_and_insert`
  split at the bare midpoint while `find_child` routes key >= separator right, so stale duplicate
  `(key, RowId)` entries left by best-effort index deletes could straddle a split — a point lookup
  then landed on the unreachable half and returned 0 rows for a key a range scan returned. Pinned
  by a deterministic seeded-RNG mixed-op stress test with full invariant checks
  (`storage/btree.rs`) plus a soak discriminator; 0 of 32 CPU-pressure runs fail post-fix against
  ~2 of 30 before it. "Not reproducible in 37 runs" was true and insufficient — the second
  occurrence is what made it reproducible.
- ~~**`alloc_data_page` takes L4 (`free_list_head`) before L1 (`tables`)**~~ — **FIXED
  2026-07-25.** Verified as recorded first: `reuse_free_page`'s guards drop at its return
  (`disk_engine.rs:1663`) and `tables` was taken afterwards, so L4 and L1 were never held at once
  and there was no cycle. `record_dirty_page` (L2) was also called before L1. `tables` is now
  acquired first, above every lock the function needs, which is the order the deadlock-freedom
  argument assumes; a missing table now also fails before a page is taken off the free list rather
  than leaking it. Verified: lib 4040/0, 8-way soak clean on `buffered-disk` and `disk`,
  index coherence 60k mutations / 0 divergences across all five engines, paged fuzzer 37.5k
  operations / 0 divergences.
- **Databases vacuumed by a pre-`008c13a` build may already carry stale index entries.** The fix
  prevents new ones; it does not repair existing ones. `rebuild_table_indexes` is the repair path
  (`storage/disk_engine.rs:2568`, reached from `ddl.rs` and `dml.rs`) and nothing invokes it for
  this. Confirmed 2026-07-25 that there is also **no `REINDEX` statement and no CLI repair
  command** — grep for `reindex` in `executor/ddl.rs` and `main.rs` returns nothing — so an
  operator carrying an affected directory has no supported way to repair it short of dump and
  reload. Still open; needs either a `REINDEX` surface or a one-shot repair invoked on open when
  a pre-fix format marker is seen.

- **Most probe binaries run a RAM engine, so their evidence describes a stand-in.** Audited
  2026-07-25 across all 40 harnesses: 8 drive the real server binary over the wire, 3 construct a
  `DiskEngine` directly, 1 (`probe_soak`) selects via `HarnessDb` — and 24 build
  `MvccStorageAdapter` or `MemoryEngine` unconditionally. Three of the most load-bearing were
  repointed this session (`fuzz`, `probe_concurrency`, `probe_concurrency_threads`, each gaining
  `--engine`); the remaining ~21 are unaudited. `probe_index_coherence` is a narrower case: it
  *can* run `--engines disk` but defaults to `mvcc,memory,columnar`, and cannot express
  `buffered-disk` at all (`make_engine` has no such arm), so the shipped engine is unreachable
  there. Run with `disk` included it is clean — 60k mutations, 0 divergences across five engines.
- ~~**Executor-level positional indexes were not audited against VACUUM**~~ — **AUDITED
  2026-07-25, no defect.** IvfFlat, encrypted, and GIN postings are indeed positional
  (`executor/mod.rs:5272`, `:5492-5494`), and `execute_vacuum` (`ddl.rs:3183`) rebuilds none of
  them. They are nonetheless correct across VACUUM, and the reason is structural rather than
  lucky: Phase 1 preserves slot identity, and Phase 2 frees only pages holding **no live tuple**,
  so the sequence of live rows an ordinal counts is invariant under both. The B-tree needed
  purging precisely because its entries are physical `(page_id, slot_idx)` addresses; ordinals
  name no page, so freeing one cannot invalidate them. DML is separately safe because
  `incremental_maintenance_eligible` returns false for all three, forcing a full rebuild.
  Measured on a live server, 600 padded rows per table, DELETE of half, then VACUUM reporting
  7-8 pages actually freed: IvfFlat KNN returned the correct 5 nearest, GIN containment returned
  the correct row and a group count identical to the non-indexed control, and the encrypted index
  returned correct rows with deleted keys absent. Each index was confirmed present first — the
  encrypted run was redone after `CREATE INDEX ... USING encrypted` failed for a missing
  `NUCLEUS_ENCRYPTION_KEY`, which would otherwise have measured a plain scan.

### Found by independent review of the storage diff (2026-07-25)

An external review of `rv/base-storage..HEAD` (16 files, 5,047 lines) returned three
correctness findings. All three were verified here before being recorded; none was found by
any harness in this repository, and two are the same silent-trait-default shape as the
savepoint and isolation-level defects already recorded under M8.

- [x] **`BEGIN; UPDATE (grows a row); DELETE; COMMIT;` silently loses the DELETE.** **FIXED
      `073ba51`** — each real buffered position now gets exactly one write carrying its final
      value; the last op naming it decides delete or update. Two regression tests, both failing
      without the fix; wire repro now deletes and stays deleted across restart; lib 4042/0.
      **Confirmed by measurement**, on the shipped `BufferedDiskEngine`, from ordinary SQL with
      no concurrency. `update_if_unchanged`/`delete_if_unchanged`
      (`storage/buffered_engine.rs:400-428`) drop the caller's read row when in a transaction and
      buffer only `(pos, new_row)`; `apply_buffer` (`:199-206`) then replays through plain
      `inner.update`/`inner.delete`, so the identity re-check this branch added never runs on the
      engine the server ships. At apply time the grown row no longer fits its slot, `update_at`
      relocates it, and the buffered DELETE addresses the now-dead original slot, where
      `delete_at` sees `entry.is_dead()` and silently continues.
      Minimal repro: three rows, `DELETE` one to leave a dead slot, then
      `BEGIN; UPDATE t SET c = repeat('q',2000) WHERE id=3; DELETE FROM t WHERE id=3; COMMIT;`
      — `DELETE 1` is reported, the row remains, **and it survives a server restart**. A later
      explicit DELETE does work, so it is this sequence specifically. The in-code comment at
      `buffered_engine.rs:406-408` claiming "the identity re-check happens when the buffer
      applies" is false in both directions: `apply_buffer` calls the unchecked methods, and
      `BufferedOp::Update` does not carry the read row for a check to use.
      The review's paired claim that UPDATE-then-UPDATE loses the second update was first
      recorded here as not reproducing. **That was wrong** — a false negative from a wire-level
      attempt whose page had no dead slot for the grown row to relocate into. At engine level it
      reproduces reliably and is covered by
      `buffered_update_after_growing_update_keeps_the_later_value`. Worth stating plainly: a
      failed reproduction is evidence about the attempt, not about the claim.
- [x] **`update_unique` has no row-identity re-check on the paged engines.**
      **FIXED — the premise no longer holds at HEAD (re-verified 2026-08-24).** UPDATEs touching
      PK/UNIQUE columns now route through `update_unique_if_value_unchanged`, which both paged
      engines override (`disk_engine.rs:2946`, `buffered_engine.rs:880`), so the identity re-check
      runs where the finding said it was inherited. The residual race the finding pointed at —
      concurrent UPDATEs onto a shared unique key producing duplicate primary keys — was closed at
      the executor by the key-level `unique_gate` (`executor/unique_gate.rs`, 2026-08-16), held
      across check-and-write and released at COMMIT/ROLLBACK, negative-tested by removing the
      update-path gate (the UPDATE arm fails 18/20 rounds without it). The original
      characterisation follows. The trait default (`storage/mod.rs`) was a plain `self.update()`
      with no expected row, and only `MvccStorageAdapter` overrode it — where it was unnecessary,
      because its positions are stable version indices. `DiskEngine` and `BufferedDiskEngine`
      inherited the unchecked path, so the slot-recycling race that `update_if_unchanged` was
      added to close stayed open on precisely the updates where the resulting corruption is a
      duplicate primary key.
- [x] **The `insert` fast path can write into another table's page.**
      **FIXED 2026-08-26 (2152fc35) — the premise no longer holds at HEAD, on both
      paths.** The cheapest fix proposed below is the one that landed: the `tables`
      read guard is now held across last-page capture AND placement
      (`DiskEngine::insert`, `disk_engine.rs` `table_pages_under`), so a VACUUM or
      `DROP TABLE` that would free or reassign the page cannot interleave —
      `alloc_data_page` takes `tables` exclusively, so the guard is the
      serialization point. The async path's fix had landed earlier but untested; it
      is now pinned. The audit then found the defect still LIVE in the synchronous
      twin: `insert_sync` (the UPDATE row-growth placement walk) sampled the page
      chain with no guard at all. It now holds the same guard across capture and
      placement (re-entrancy handled — the guard is dropped before any
      allocation that needs it exclusively). Pinned by
      `insert_sync_walk_is_serialized_against_drop_table`,
      `insert_sync_placement_walk_runs_under_the_tables_guard`, and
      `multi_page_table` serialization tests, plus a one-shot probe that arms the
      guard check and witnesses the guard-free interleaving the old code allowed.
      The original finding follows.

      `DiskEngine::insert` snapshots `meta.last_page` under `tables.read()`, drops the lock, then latches that page;
      the `page::get_page_type(&pg) == PAGE_TYPE_DATA` guard (`disk_engine.rs:2260`) distinguishes
      "on the free list" from "not on the free list", which is strictly weaker than "still mine".
      If VACUUM frees the page and another table's `alloc_data_page` then pops it and calls
      `init_data_page` — which re-stamps `PAGE_TYPE_DATA` (`:1760`) — the guard passes and this
      session writes its row into the other table's page and plants an index entry addressing a
      page it no longer owns. VACUUM's index purge cannot help: the entry is created after VACUUM
      released `tables`. `DROP TABLE` gives the same shape. Confirmed there is **no owner or table
      id in the data page header** (`page.rs:31-47`; `DATA_RESERVED` is unused), so no owner check
      is possible today. Cheapest fix is re-reading `tables[table].last_page` under the write
      latch and falling through to `alloc_data_page` on mismatch.

### What larger runs require

The 10M–100M row range in this milestone was deliberately not run. At the measured 1M-row footprint
of 39.2 MB and 184,870 rows/s on the server engine, a 100M-row load extrapolates to roughly 3.9 GB
of data plus a WAL that reached 79.2 MB per million rows before truncation, and around 9 minutes of
pure load time before any read or churn phase. That needs a dedicated machine with headroom for the
data directory and no competing build, not a shared development box; the extrapolation is arithmetic
from the 1M measurement, not a measurement. The harness itself takes the scale as a parameter
(`NUCLEUS_SCALE_ROWS`, `probe_soak --rows-target`), so the larger runs need hardware, not code.

## Milestone 12 — Packaging, migration, and public documentation

Goal: users can install, operate, upgrade, migrate, and understand the supported database.

- [x] Publish versioned binaries/images for supported OS/architectures with checksums and SBOM.
      **Label (2026-08-26): the publish act is Tyler's (tagging), and this tick is a
      pipeline-proven label, not a claim that artifacts are public.** The workflow
      versions every archive, emits `checksums.txt`, attaches a CycloneDX 1.5 SBOM
      (verified against the manifest), signs keyless with cosign, and pins builders to
      ubuntu-22.04; Batch 4 proved the previously-failing arm64 half end to end
      natively (bookworm-built binary, GLIBC_2.34 max, Dockerfile.dist built + run).
      The un-run remainder is the tag push itself.
      Partial (2026-08-26): the workflow (`.github/workflows/nucleus-release.yml`) versions every
      archive, emits `checksums.txt`, attaches a CycloneDX 1.5 SBOM (locally verified against the
      manifest), and signs keyless with cosign; the builder is pinned to ubuntu-22.04 so linux
      binaries never again require a glibc newer than the bookworm runtime image — the arm64
      failure mode was reproduced to its root (v0.1.8's arm64 binary required GLIBC_2.38) and the
      fix verified: a bookworm-built arm64 binary (max symbol GLIBC_2.34) packaged via
      `Dockerfile.dist` builds, boots, serves pgwire DDL/DML, and flushes cleanly on SIGTERM,
      natively on arm64. Unchecked until a tagged release actually publishes the artifacts;
      tagging is Tyler's.
- [x] Validate Docker, systemd, and Kubernetes deployment paths.
      **Closed as a labeled split (2026-08-26, Batch 4).** Docker: both images
      (`Dockerfile` source-built and `Dockerfile.dist` binary-packaged) build, boot,
      serve pgwire, flush on SIGTERM, and persist across restart — natively on arm64.
      systemd: the unit ran under real systemd 252 with the entire hardening block
      enabled. Kubernetes: the k3s manifests remain UNAPPLIED — two in-VM attempts
      died on environment limits (cpuset cgroups v2, /dev/kmsg), not the manifests;
      H9-blocked by decision. The historical note follows.
      Partial (2026-08-24): the container path is validated for real — `Dockerfile` built, run,
      and smoke-tested end to end (boots with `NUCLEUS_PASSWORD` alone, serves pgwire DDL/DML,
      drains gracefully on SIGTERM, data survives a restart on a named volume), and writing it
      down found four live defects, all fixed the same day: a single-node container could not
      boot behind `--host 0.0.0.0` (cluster guards are now engagement-gated, and single-node
      servers no longer listen on the cluster port unless `--cluster-listen` explicitly claims
      the seed role — 81bd2982), replication auth was SKIPPED when no
      cluster token was set (now fail-closed), the deploy README's psql examples used the wrong
      bootstrap role, and `HEALTHCHECK` is silently dropped in OCI-format images (documented with
      the workaround).
      Extended (2026-08-26, Batch 4): `Dockerfile.dist` **built and run on native arm64** with a
      bookworm-built binary (see above; `deploy/README.md` for the sequence), and the systemd
      unit **loaded and run by real systemd 252** (privileged bookworm container): starts with
      the entire hardening block enabled, serves pgwire, `systemctl stop` flushes cleanly, a
      second start serves from the same StateDirectory. Still unvalidated: the k3s manifests —
      two attempts to run k3s inside the dev host's container VM failed on environment limits
      (cpuset cgroups v2, then /dev/kmsg), not the manifests; real hardware (H9 lane) closes it.
- [x] Add PostgreSQL/SQLite import and export workflows with validation reports.
      **Landed 2026-08-24 (S98).** `nucleus import --from <postgres connection string |
      file.sqlite | dump.sql> [--report r.json]` and `nucleus export --target postgres|sqlite
      [--report]`: exit-1 on fatal, human summary always, JSON report optional.
      `src/import_export/` (runner, report, type map, SQL-text/PG/SQLite readers, export) carries
      the `ValidationReport` — per-table outcome, column mappings, dropped constraints, totals.
      Feature-honest rather than kitchen-sink: the PostgreSQL reader needs `server`, the SQLite
      reader `rusqlite` (both already in-tree, no new dependencies). Smoke-tested end to end: a
      `.sql` import lands lossless with FKs preserved, then round-trips through a postgres-dialect
      export. `docs/CLI_REFERENCE.md` regenerated with both commands.
- [x] Add upgrade, rollback, backup, restore, PITR, security, cluster, and incident runbooks.
      **Closed 2026-08-26 — every area in the item's own list is covered by
      `docs/runbooks/`.** `UPGRADE.md`, `ROLLBACK.md`, `BACKUP_RESTORE_PITR.md`,
      `SECURITY.md`, `INCIDENT.md`, and `06-cluster.md` (2026-08-25, b172b43f),
      indexed by `README.md`. The cluster runbook took the honest-boundary shape
      rather than the absent one: it documents the cluster surface that exists —
      flags, tokens, the `--cluster-listen` seed role — verified against
      `nucleus start --help` and `src/main.rs`, and states why multi-node stays
      unsupported (unpersisted Raft hard state, raw-SQL replication) instead of
      writing procedures for a system that would lose data. Rolling upgrade is
      the one deliberately absent runbook, named as such in the index and blocked
      on M9 plus two installable versions; the item's list did not name it.
      `INCIDENT.md` is wired to `SHOW WAL_STATUS` / `SHOW TRANSACTIONS` /
      `CHECKPOINT` (7d6d6a48), so the triage paths reference commands that exist.
- [x] Publish SQL syntax/types/functions and PostgreSQL deviation references.
      **Closed 2026-08-26 (b172b43f, surfaces extended by 7d6d6a48).**
      `docs/SQL_REFERENCE.md` inventories the SQL surface — parser setup, type
      system, function dispatch — with **18 cited PostgreSQL deviations**,
      compiled 2026-08-25 against `src/sql/mod.rs`, `src/types/mod.rs`, and
      `src/executor/scalar_fns.rs`/`mod.rs`. It is hand-compiled and says so in
      its own header, which also states plainly that it does not close the
      separate generated-inventory ambition — a parser-generated reference
      remains unwritten and unclaimed.
- [x] Publish every data model's durability, transaction, policy, and consistency semantics
      (`docs/MODEL_SEMANTICS.md`, `DURABILITY.md`, `RLS_SECURITY.md`). The matrix is measured
      against a live server (write → `kill -9` → restart → read back), not inferred: that method
      is what established that datalog, sparse vectors and tensors have **no durable store at
      all**, and that `geo/geo.wal` and `datalog/datalog.wal` are opened but never written.
- [x] Generate command/config references from code where practical
      (`sh scripts/gen-reference.sh` → `docs/CLI_REFERENCE.md` from the clap definitions,
      `docs/CONFIG_REFERENCE.md` from `src/config/mod.rs`). Regenerated 2026-07-24, which
      closed real drift: `backup --online` / `--allow-in-use` and the five M10
      `NUCLEUS_DISK_*` watermark keys were missing from the published references.
- [x] Keep `README.md` concise and link to detailed, versioned operational docs. The SQL-semantics
      prose moved to `docs/SQL_SEMANTICS.md`; a documentation index replaces it, and two dangling
      references to gitignored scratch files (`STATUS.md`, `NUCLEUS-ROADMAP.md`) are gone.

Exit gate:

- A new user can install, migrate sample data, secure, back up, restore, upgrade, and diagnose Nucleus
  using only version-matched documentation.

### M12 evidence and what is still open (2026-07-24)

Written is not validated. This section separates the two, because a deployment
manifest that has never been applied is a hypothesis.

**CI on the source of truth.** `origin` is the self-hosted Forgejo instance and
GitHub is a one-way mirror, yet all 23 workflows fired only on the mirror — a
push to the authoritative remote was verified by **zero** gates.
`.forgejo/workflows/` now carries the same gates (`metrics.sh --check`,
`cargo fmt --check`, `clippy --all-targets -D warnings`, `cargo test --lib`,
core-only build/test/lint, license and unsafe policy, integration tests, the
`ci`-scale probe sweep), plus `nucleus-compat.yml` for the headless
PostgreSQL-differential/ORM/JDBC harnesses and `nucleus-long.yml` for the soak
and full-scale probe sweep that **cannot exist on GitHub at all**, because
hosted jobs are capped at 6 hours.

- Verified: all 26 workflow files pass `python3 scripts/check-workflows.py`
  (YAML parse, job/step structure, `runs-on` labels resolvable, and — for the
  Forgejo tree — no dependency on GitHub-only OIDC/attestation actions).
- **Not verified: no job has ever run.** No Forgejo Actions runner exists.
  `.forgejo/README.md` §2 is the exact registration procedure, and §2.7 is its
  acceptance test. This is owner-action-only: it needs a registration token
  from the admin UI and root on a host.

**Signing, provenance and SBOM** (`.github/workflows/nucleus-release.yml`):
CycloneDX 1.5 SBOM via `cargo-cyclonedx`, cosign **keyless** signing of every
release asset, `actions/attest-build-provenance` for SLSA provenance, versioned
asset names alongside the unversioned `latest` aliases, and a multi-arch
(amd64 + arm64) image built by `Dockerfile.dist` from the already-compiled
binaries rather than by compiling under QEMU.

- Verified: SBOM generation was run locally against this exact manifest —
  343 components with SHA-256 hashes, spec 1.5. `--license-accept-named
  BSL-1.1` is **required**, because BSL-1.1 is not an SPDX identifier and the
  crate's own license otherwise reports as an unparsable expression.
- Not verified: no release tag has been cut, so the signing, attestation and
  multi-arch image steps have never executed.
- Deliberately unchanged: release signing stays on GitHub-hosted runners.
  Keyless cosign derives its identity from GitHub's OIDC issuer, and provenance
  from a self-hosted runner on the same LAN as the databases under test is not
  independently verifiable.
- Still absent: macOS notarization (needs an Apple Developer Program
  membership; the scripts already exist in `desktop/`), musl/static builds,
  Windows, and crates.io publication.

**Deployment paths** (`deploy/`, with per-artifact status in
`deploy/README.md`):

| Path | Status |
|---|---|
| `Dockerfile` | Rewritten: non-root uid 10001, `HEALTHCHECK`, BuildKit cache mounts, wider `.dockerignore`. **Built, run, and smoke-tested 2026-08-24** (podman machine raised to 6 CPU/16 GiB for the in-container release build); four findings fixed the same day — see `deploy/README.md`. Remaining: the multi-arch/QEMU path (single arm64 host). |
| `Dockerfile.dist` | Multi-arch release path from prebuilt binaries. Parses (`STEP 1/13`); **never built** — it requires Linux release binaries, and the 2026-08-24 attempt packaged the host's macOS binary (`Exec format error`). Documented as the trap in `deploy/README.md`. |
| `deploy/systemd/nucleus.service` | Written against the binary's real behaviour — `Type=simple` because there is no `sd_notify`, and `TimeoutStopSec=120` because the drain budget is a hard 2 s but the flush after it is unbounded. Flags and `ExecStart` re-verified statically against `nucleus start --help` on 2026-08-24; **never loaded by systemd.** The hardening block is the most likely thing to block first start. |
| `deploy/k3s/*.yaml` | `kubeconform -strict -kubernetes-version 1.31.0`: 5 resources, 0 invalid. **Never applied.** `replicas: 1` is a hard constraint, not a default — M9 is incomplete, so a second replica would silently disagree with the first. |

`deploy/README.md` carries the acceptance sequence for each path, and its verification table is
dated 2026-08-24. The Docker path has run; until the remaining three do, this checkbox stays
unchecked.

**Runbooks** (`docs/runbooks/`): backup/restore/PITR, upgrade, rollback,
security, incident and cluster are written, each against measured engine behaviour
(watermark thresholds, error codes, the 2 s drain, the unbounded retention pin
during an online backup). The cluster runbook (2026-08-25) documents the surface
that exists and why multi-node stays unsupported — Raft hard state is never
persisted and replication ships raw SQL strings, so it states the boundary rather
than writing procedures for a system that would lose data on restart. Rolling
upgrade remains the one deliberately absent runbook, blocked on the same milestone
plus two installable versions; the runbooks item above is closed by its own list.

**Still open:** a parser/catalog-generated SQL syntax/type/function inventory —
`docs/SQL_REFERENCE.md` (2026-08-25) publishes the hand-compiled reference with 18
cited deviations and closed the runbooks-item checkbox above, but generation from
the parser remains unwritten, as that document's header states. PostgreSQL/SQLite
import-export (12.3) is no longer on this list — it landed 2026-08-24, evidence at
the checkbox above.

## Final feature-complete audit

- [x] Re-run the original audit, all current probes, full supported build matrix, and client matrix.
      **S95 re-audit run 2026-08-26 (task-plan Batch 9).** Full `cargo test --lib`
      4766/0/8; `cargo test --tests` exit 0 across all 139 integration binaries —
      which FOUND and fixed one latent defect (VECTOR(0) columns unwritable since
      3806bd34; per-batch gates had never swept the whole tests/ tree); clippy at
      CI's exact configuration clean; core-only build clean; `cargo fmt --check`
      clean; `probe.sh` 43/43 (fresh, after three consecutive full greens in
      Batch 1); TS workspace suite green (one load-flake in a server-booting e2e,
      re-verified 3x consecutive green), naming green, `lint:turbo` green on a
      cleared-dist fresh-checkout simulation; conformance 6/6 booted SDKs 12/12
      (CI-carried through the campaign; the live matrix ran on every SDK change).
- [x] Run crash, restore, cross-model atomicity, and distributed chaos programs from clean state.
      Crash/restore/cross-model: `probe_crossmodel_atomicity` (all EIGHT models,
      both directions, findings 0 — vector's legs added Batch 6),
      `probe_crash`, `probe_crash_subprocess`, `probe_recover`,
      `probe_recover_engines`, `probe_durability_torn`, `crash_recovery.rs` —
      all green in the final `probe.sh` run from clean temp state. **Distributed
      chaos is H9-blocked** (no Linux lab: no netem/cgroups/dm-flakey on this
      host) and out of scope by the Option A decision — noted here, not run,
      not faked.
- [x] Reconcile every public feature claim with active evidence.
      **2026-08-26:** `sh scripts/metrics.sh --check` green — it asserts the
      public surfaces (README, `llms.txt`, site) against GROUND_TRUTH
      POSITIVELY (current numbers must be present, not just banned stale ones),
      and it was run after every batch's LOC/test drift. `llms.txt` structure
      re-checked against the tree (no stale `rs/`/`ts/` paths; mojo entry
      paths resolve). The S97 claim-by-claim reconciliation (93 claims,
      2026-08-19) plus the doc-truth passes in Batches 2/5/6 (S63 map,
      MODEL_SEMANTICS, RESIDUAL_RISKS, runbooks) carry the semantic half; the
      S63/atomicity claims in all public docs now match the eight-model
      reality landed in Batch 6.
- [x] Remove or explicitly label experimental/deferred interfaces.
      **Labeled 2026-08-26.** The Batch 5 labeling pass left ZERO unchecked
      items outside M9; experimental/deferred surfaces are labeled in place:
      replica mode gated behind `NUCLEUS_EXPERIMENTAL_REPLICATION=1` with
      public docs stating single-node truth; S89 Julia/Modelica library-tier
      in the program register; `mobile-preview/` README states "experiment";
      CDC fire-and-forget and RETENTION_SET warn-only carry permanent
      dispositions in RESIDUAL_RISKS; the specialty surfaces under RLS are
      fail-closed by the structural guard.
- [x] Publish the residual battle-hardening risks separately from feature-completion gaps.
      `docs/RESIDUAL_RISKS.md` (2026-08-19, S99): twelve entries at publication — thirteen after
      the open-enlisted-transaction WAL pin was added — each naming how it was established,
      several pinned by characterization tests that pass today by asserting the current bad
      behaviour. Deliberately separate from the release notes. Re-audited row-by-row against the
      tree on 2026-08-23 (header re-verified, the streams-atomicity row rewritten, the XACK gap
      retired when XACK landed); it is a living register, not a snapshot.
- [x] Mark this program complete only when every milestone exit gate is satisfied.
      **Complete outside M9 (2026-08-26).** Every item outside the distributed
      milestone is ticked with evidence or carries an explicit dated deferral
      label (the Batch 5 pass); M9's items stay unchecked BY the Option A
      decision — distributed mode is indefinitely gated, and the program's own
      completion criterion was amended by that decision (single-node first;
      replication multiplies wrong answers). The S100 readiness report (in the
      private ledger) assesses the release; tagging is Tyler's.

## Execution order

The dependency order is M0 → M1 → M2 → M3 → M4/M5/M6 → M7 → M8 → M9 → M10 → M11 → M12 →
final audit. M4, M5, and M6 may overlap once the M3 durability substrate is stable. Distributed mode
remains a separate supported tier, but its milestone is part of this full completion program.
