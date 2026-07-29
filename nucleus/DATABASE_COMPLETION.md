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

- Source LOC: 280635; Source Rust files: 246; Top-level modules: 51.
- Declared unit tests: 4131; Declared integration tests: 337; Ignored tests: 46.
  These are static declarations, not executed-test claims.
- The most recent full library run executed 4,031 passing tests, 0 failing.
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

### `BEGIN ISOLATION LEVEL SERIALIZABLE` is accepted and silently ignored on disk

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

- [ ] Decide the contract: implement conflict detection on the paged engines, or
      reject `SERIALIZABLE`/`REPEATABLE READ` with a clear error instead of
      accepting and ignoring them. Silently downgrading is the one option that
      loses data without telling anyone.
- [ ] Reconcile `docs/MODEL_SEMANTICS.md:263-271` with whichever is chosen — it
      currently calls `SHOW transaction_isolation` "advisory" but does not say a
      requested level is discarded, or that lost updates follow.

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

- [ ] Define representative OLTP, analytical, mixed, specialty, and distributed workloads.
      Partial: mixed OLTP + specialty-index churn (`probe_soak`) and bulk-load/analytical
      (`scale_load`) exist. No distributed workload.
- [ ] Benchmark 1M–100M row scales and sustained concurrency with p50/p95/p99 latency.
      Partial: 1M rows and 8-way sustained concurrency are measured with full percentiles
      (evidence below). 10M–100M is unrun — see "What larger runs require".
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
- [ ] Test memory pressure, disk pressure, long transactions, connection storms, and multi-day soak.
- [ ] Optimize only after differential correctness gates cover the affected fast path.

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

- **One `SOAK FAILED` on `buffered-disk` that was never attributed.** It occurred on the first run
  after the VACUUM change; the run's output was truncated before the `FAIL:` line was captured, so
  the reason is unknown. Error rate was 0 and row counts matched across reopen, which narrows it to
  coherence or checkpoint. It has not recurred in 37 subsequent runs (21 + 8 by the agent that saw
  it, 8 more independently, including runs under concurrent build load), and base `0de4495` was 6/6
  clean. Recorded rather than closed: "not reproducible in 37 runs" is not the same as explained,
  and it is not claimed to be pre-existing.
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
- [ ] **`update_unique` has no row-identity re-check on the paged engines.** The trait default
      (`storage/mod.rs:138-145`) is a plain `self.update()` with no expected row, and only
      `MvccStorageAdapter` overrides it (`mvcc.rs:2304`) — where it is unnecessary, because its
      positions are stable version indices. `DiskEngine` and `BufferedDiskEngine` inherit the
      unchecked path. Every UPDATE touching a PK or UNIQUE column routes here
      (`dml.rs:2049-2062`, `:2242-2263`), so the slot-recycling race that `update_if_unchanged`
      was added to close is still open on precisely the updates where the resulting corruption is
      a duplicate primary key. Verified by inspection; the concurrency window is real but was not
      reproduced here. The branch's own regression test updates a non-PK column, so it exercises
      only the protected path.
- [ ] **The `insert` fast path can write into another table's page.** `DiskEngine::insert`
      snapshots `meta.last_page` under `tables.read()`, drops the lock, then latches that page;
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
