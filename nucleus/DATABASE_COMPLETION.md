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

- Source LOC: 265610; Source Rust files: 236; Top-level modules: 51.
- Declared unit tests: 3989; Declared integration tests: 335; Ignored tests: 45.
  These are static declarations, not executed-test claims.
- The most recent full library run executed 3,972 passing tests, 0 failing.
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
      `storage::crashpoint` declares 11 named boundaries; `NUCLEUS_CRASHPOINT=<name>` makes the
      process `abort()` there (no unwind, no Drop, no flush — power-loss equivalent at a chosen
      instruction), with `NUCLEUS_CRASHPOINT_SKIP=n` to hit setup vs deep steady state.
      `probe_crash_points` walks every point at several skip depths and reports points it could NOT
      reach rather than counting them as passes.
- [x] Test torn headers/records, truncated WALs, invalid checksums, duplicate replay, and corrupt tails.
      `probe_durability_torn`: 0 findings over ~9.5k lossy recoveries — no panics, every recovered
      row was committed, CRC gate honored.
- [x] Test disk-full, permission loss, fsync/write errors, read-only media, and interrupted checkpoint.
      `storage::crashpoint::io_fault` injects ENOSPC / permission-denied / generic I/O errors at
      `wal.append`, `wal.fsync`, and `meta.write`; `probe_io_faults` walks 21 point x kind x depth
      combinations and asserts the failure SURFACES (a write that cannot be made durable must never
      report success), that every acknowledged row survives recovery, and that no corrupt or
      half-applied record remains. 0 findings. Interrupted checkpoint is covered by
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
  Status: holds at all 11 crash points (`probe_crash_points`, 0 findings) after fixing a
  total-data-loss defect the matrix found — see below.
- Recovery failures are actionable errors and do not continue with suspect data.
  Status: holds across 21 injected I/O-failure combinations (`probe_io_faults`, 0 findings), and a
  rejected open now provably rewrites/deletes nothing.

Scope limits on the above, stated plainly (full list in `DURABILITY.md`): named crash points cover
the SQL/MVCC WAL, catalog rename, and compaction paths — the specialty-model WALs are covered by
reopen tests and by the same stage-and-rename fix, but not yet by crash points of their own.
Read-only media is simulated with injected PermissionDenied rather than an actually read-only
mount. Multi-node/replica crash behavior is out of scope here (M9).

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
- [ ] Add WAL archiving with monotonic positions and retention management.
- [ ] Add restore-to-latest and restore-to-time/position workflows.
      LIKELY DONE, needs an end-to-end gate: `pitr::PitrTarget` implements Lsn / Time / Latest and
      `restore_pitr` rebuilds the WAL dir truncated at the target. Left unchecked until a restore
      is verified from a clean directory rather than in-process.
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
- [ ] Define encrypted-backup key handling and key rotation.
- [ ] Add restore verification, corruption detection, and automated disaster-recovery tests.
      PARTIAL (2026-07-24). DONE: `restore_data_dir` verifies every manifest checksum before it
      touches the destination and refuses a snapshot that is corrupted, truncated, missing a file,
      or carrying an extra one — naming the offending paths. The refusal is provably
      non-destructive (`corrupted_snapshot_is_rejected_without_touching_the_destination` compares
      a content fingerprint of the destination before and after). `verify_snapshot` is public so an
      archived snapshot can be validated without running a restore. Restore also refuses a
      destination a live instance holds, and refuses to overwrite a *different* database (identity
      mismatch) even with `--force`. NOT DONE: automated disaster-recovery tests (scheduled
      restore-and-verify runs), and logical comparison of restored contents across every durable
      model.
- [ ] Document RPO/RTO controls and limitations.

Exit gate:

- A running database can be backed up and restored on a clean machine to a selected committed point.
- Automated restore verification compares logical contents across every durable model.

## Milestone 5 — Complete security and policy envelope

Goal: all supported interfaces share one authenticated, fail-closed authorization boundary.

### Identity and roles

- [ ] Complete password lifecycle: creation, rotation, expiration policy, lockout/rate limits, and
      redacted diagnostics.
- [ ] Add optional trusted JWT/OIDC/proxy claim verification if multi-tenant cloud mode is supported.
- [ ] Authenticate cluster nodes with mTLS and authorize administrative RPCs.
- [ ] Propagate authenticated principals through supported follower forwarding without impersonation.
- [ ] Emit durable, bounded security audit events for login and authority changes.

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
- [ ] Implement column-masking DDL, catalog persistence, transactionality, and executor enforcement.
- [ ] Define policy-aware materialized-view refresh and invocation semantics.
- [ ] Add policy alteration/introspection commands or explicitly constrain v1 to create/drop.
- [ ] Preserve fail-closed behavior for unsupported policy expressions and protected specialty calls.
- [ ] Document constraint-existence, timing, administrator, and physical-backup side channels.

### Specialty surfaces

- [ ] Define native ownership/tenant policy boundaries for KV, document, vector, graph, FTS,
      time-series, blob, streams, Datalog, tensor, branch/version, CDC, and pub/sub surfaces.
- [ ] Implement those boundaries or keep each surface unavailable while protected relational state
      exists; never silently use the bootstrap identity.

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
- [ ] Add a shared commit record/coordinator or another proven atomic commit design.
- [ ] Make prepare/commit/abort idempotent across every enlisted WAL.
- [ ] Recover in-doubt transactions deterministically after crash.
- [ ] Coordinate CDC emission, cache invalidation, specialty indexes, and policy metadata with commit.
- [ ] Add crash injection at every cross-model commit boundary.

Landed ahead of the commit-record work, because each was live data loss:

- [x] **A ROLLBACK no longer destroys other sessions' committed non-SQL writes.**
      `BEGIN` deep-cloned every specialty store and `ROLLBACK` assigned the clone
      back wholesale, so session A's rollback erased session B's acknowledged,
      fsynced KV/document/graph/time-series/blob/vector writes. Each session now
      records the entities it wrote and reverts exactly those.
- [x] **A ROLLBACK is durable for KV, document, graph, time series, and blob.**
      The revert writes compensating records into each store's own WAL, so a crash
      after a successful `ROLLBACK` no longer resurrects the rolled-back writes on
      replay. Vector is **not** covered (in-memory revert only); datalog needs no
      compensation because its WAL is never written at all.
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

Still open in this milestone:

- No model is crash-atomic with the SQL commit. A COMMIT still fsyncs the SQL WAL
  and the specialty logs as two ordered steps, and six specialty logs are not
  fsynced at a commit boundary at all.
- No isolation on specialty stores: one session reads another's uncommitted
  non-SQL writes, and two sessions writing the same key still conflict
  destructively.
- KV collections, the columnar analytics store, streams, and CDC still do not
  participate in transactions at all; their writes survive `ROLLBACK`, and CDC
  publishes change events for transactions that never committed.
- Vector rollback is not durable (no compensating record in `vector/vector.wal`).

Exit gate:

- Cross-model transactions expose all effects or none after every injected crash point.
- Unsupported model combinations reject before creating partial effects.

## Milestone 9 — Distributed database completion

Goal: convert the current Raft/runtime implementation into a restart-safe replicated database.

### Consensus persistence and state machine

- [ ] Persist current term, voted-for, replicated log, commit index, and snapshot metadata atomically.
- [ ] Wire InstallSnapshot through real transport and restore executor/catalog state from snapshots.
- [ ] Replace unconstrained raw-SQL replication with deterministic commands or reject nondeterminism.
- [ ] Define handling when a command commits to quorum but local execution fails.
- [ ] Add request identifiers, deduplication, retry, and exactly-once-visible application semantics.
- [ ] Replicate multi-statement transactions and schema/security changes atomically.

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

- [ ] Integrate all subsystem metrics with the global registry.
- [ ] Expose transaction/lock state, WAL/checkpoint/recovery status, replication lag, memory, cache,
      compaction, backup, connection, and query latency metrics.
- [ ] Add health, readiness, startup, recovery, and degraded-state reporting.
- [ ] Add structured slow-query logs, query IDs, EXPLAIN diagnostics, and tracing integration.
- [ ] Enforce connection, query-time, transaction-idle, memory, temporary-space, and tenant limits.
- [x] Add disk watermarks, safe read-only/degraded mode, and operator alerts.
- [x] Verify graceful shutdown drains requests and persists all required state.
- [x] Validate configuration eagerly and redact secrets from logs/status output.
- [ ] Add maintenance commands for checkpoints, vacuum/GC, statistics, compaction, and integrity check.

Evidence (partial — see the open items above):

- Connection limit: the pgwire accept loop takes a slot non-blockingly, so one
  over-limit client no longer stalls the listener for the 30 s acquire timeout,
  and a refused client receives `FATAL` / SQLSTATE `53300` with a hint naming
  `server.max_connections`. Counted by `nucleus_connections_rejected_total`.
  Verified with `psql` at `NUCLEUS_SERVER_MAX_CONNECTIONS=2`: the third
  connection is refused in 0 s, the listener answers immediately afterwards,
  and freeing a holder re-admits. Query-time, transaction-idle, and memory
  limits are enforced elsewhere (T1.2/T1.3); temporary-space and tenant limits
  are NOT implemented, so this item stays open.
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

- [ ] Define representative OLTP, analytical, mixed, specialty, and distributed workloads.
- [ ] Benchmark 1M–100M row scales and sustained concurrency with p50/p95/p99 latency.
- [ ] Track memory, disk, write amplification, WAL/checkpoint cost, cache hit rate, and recovery time.
- [x] Measure vector recall/latency at scale, correctness-paired (`bench_paired` scale/sweep,
      2026-07-21): 1M clustered vectors, recall 0.992 / min 0.90 at ~2.1ms/query vs 45ms brute
      force. This gate CAUGHT and fixed two shipping defects: a fixed default beam fully
      trapped occasional queries at 300k+ (min-recall 0.0 — the default now scales with index
      size), and O(n)-per-insert unique checks made bulk loads quadratic (200k-row load
      189s→0.4s after the index-assisted probe). 5M-row soak: load 27.5s, 2h churn 184k ops /
      0 errors, exact counts after reopen, logical dump/restore round-trip verified. Filtered
      ANN, FTS relevance, graph traversal, and TS ingest at scale remain unmeasured.
- [ ] Add regression budgets for critical workloads and retain machine/config metadata.
- [ ] Test memory pressure, disk pressure, long transactions, connection storms, and multi-day soak.
- [ ] Optimize only after differential correctness gates cover the affected fast path.

Exit gate:

- Published results are reproducible, include tail latency/correctness, and define safe capacity limits.

## Milestone 12 — Packaging, migration, and public documentation

Goal: users can install, operate, upgrade, migrate, and understand the supported database.

- [ ] Publish versioned binaries/images for supported OS/architectures with checksums and SBOM.
- [ ] Validate Docker, systemd, and Kubernetes deployment paths.
- [ ] Add PostgreSQL/SQLite import and export workflows with validation reports.
- [ ] Add upgrade, rollback, backup, restore, PITR, security, cluster, and incident runbooks.
- [ ] Publish SQL syntax/types/functions and PostgreSQL deviation references.
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
| `Dockerfile` | Rewritten: non-root uid 10001, `HEALTHCHECK`, BuildKit cache mounts, wider `.dockerignore`. Parses (reaches `STEP 1/5`); **never built or run** — the dev machine's podman VM has a faulting overlay store that rejects *every* image, including `debian:bookworm-slim`. |
| `Dockerfile.dist` | Multi-arch release path from prebuilt binaries. Parses (`STEP 1/13`); never built. |
| `deploy/systemd/nucleus.service` | Written against the binary's real behaviour — `Type=simple` because there is no `sd_notify`, and `TimeoutStopSec=120` because the drain budget is a hard 2 s but the flush after it is unbounded. **Never loaded by systemd.** The hardening block is the most likely thing to block first start. |
| `deploy/k3s/*.yaml` | `kubeconform -strict -kubernetes-version 1.31.0`: 5 resources, 0 invalid. **Never applied.** `replicas: 1` is a hard constraint, not a default — M9 is incomplete, so a second replica would silently disagree with the first. |

`deploy/README.md` carries the acceptance sequence for each path. Until those
run, this checkbox stays unchecked.

**Runbooks** (`docs/runbooks/`): backup/restore/PITR, upgrade, rollback,
security and incident are written, each against measured engine behaviour
(watermark thresholds, error codes, the 2 s drain, the unbounded retention pin
during an online backup). **The cluster runbook is deliberately absent** and
the item therefore stays unchecked: Raft hard state is never persisted and
replication ships raw SQL strings, so any cluster procedure written today would
document a system that loses data on restart. Rolling upgrade is blocked on the
same milestone plus two installable versions.

**Still entirely open:** PostgreSQL/SQLite import-export (12.3), and the
generated SQL syntax/type/function inventory (12.5 — `compat/pgregress/DEVIATIONS.md`
and the new `docs/SQL_SEMANTICS.md` cover deviations and designed behaviour,
but nothing is generated from the parser or catalog yet).

## Final feature-complete audit

- [ ] Re-run the original audit, all current probes, full supported build matrix, and client matrix.
- [ ] Run crash, restore, cross-model atomicity, and distributed chaos programs from clean state.
- [ ] Reconcile every public feature claim with active evidence.
- [ ] Remove or explicitly label experimental/deferred interfaces.
- [ ] Publish the residual battle-hardening risks separately from feature-completion gaps.
- [ ] Mark this program complete only when every milestone exit gate is satisfied.

## Execution order

The dependency order is M0 → M1 → M2 → M3 → M4/M5/M6 → M7 → M8 → M9 → M10 → M11 → M12 →
final audit. M4, M5, and M6 may overlap once the M3 durability substrate is stable. Distributed mode
remains a separate supported tier, but its milestone is part of this full completion program.
