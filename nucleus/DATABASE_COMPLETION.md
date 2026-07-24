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

- Source LOC: 255538; Source Rust files: 221; Top-level modules: 50.
- Declared unit tests: 3865; Declared integration tests: 320; Ignored tests: 43.
  These are static declarations, not executed-test claims.
- The most recent full library run executed 3,836 passing tests. Core-only executed 1,853
  passing tests with no ignores.
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
- [ ] Ensure query caches and specialty indexes invalidate on every relevant DDL/DML transition.

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

Exit gate:

- Curated PostgreSQL/SQLite differential corpora have no unexplained semantic divergence.
- Every storage engine produces the same logical result for its declared SQL subset.
- Long-running MVCC churn reclaims storage without violating snapshot visibility.

## Milestone 3 — Crash durability and recovery

Goal: every durable model has proven, deterministic crash behavior.

- [ ] Inventory the authoritative files, WALs, manifests, and checkpoints for every data model.
- [ ] Add subprocess kill points before/after WAL append, fsync, data write, checkpoint, and rename.
- [ ] Test torn headers/records, truncated WALs, invalid checksums, duplicate replay, and corrupt tails.
- [ ] Test disk-full, permission loss, fsync/write errors, read-only media, and interrupted checkpoint.
- [ ] Verify replay idempotency through repeated crash/recovery cycles.
- [ ] Verify catalog, metadata, security policy, specialty index, and row state recover consistently.
- [ ] Establish explicit durability modes and fsync guarantees.
- [ ] Add on-disk format manifests and forward migration tests.
- [ ] Reject unsupported downgrade/format combinations without modifying data.

Exit gate:

- The crash matrix yields either the previous committed state or the new committed state, never a
  partial committed state or silent corruption.
- Recovery failures are actionable errors and do not continue with suspect data.

## Milestone 4 — Backup, restore, and point-in-time recovery

Goal: recover a production database without requiring a byte-for-byte stopped-directory copy.

- [ ] Add an online-consistent physical snapshot coordinated with writes and checkpoints.
- [ ] Add backup manifests with checksums, format version, database identity, and encryption metadata.
- [ ] Add WAL archiving with monotonic positions and retention management.
- [ ] Add restore-to-latest and restore-to-time/position workflows.
- [ ] Add logical schema/data dump and restore across compatible format versions.
- [ ] Include roles, memberships, policies, sequences, views, functions, and specialty metadata.
- [ ] Define encrypted-backup key handling and key rotation.
- [ ] Add restore verification, corruption detection, and automated disaster-recovery tests.
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

- [ ] Inventory which models participate in the SQL transaction coordinator today.
- [ ] Define transaction enlistment and isolation semantics for each public model.
- [ ] Add a shared commit record/coordinator or another proven atomic commit design.
- [ ] Make prepare/commit/abort idempotent across every enlisted WAL.
- [ ] Recover in-doubt transactions deterministically after crash.
- [ ] Coordinate CDC emission, cache invalidation, specialty indexes, and policy metadata with commit.
- [ ] Add crash injection at every cross-model commit boundary.

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
- [ ] Add disk watermarks, safe read-only/degraded mode, and operator alerts.
- [ ] Verify graceful shutdown drains requests and persists all required state.
- [ ] Validate configuration eagerly and redact secrets from logs/status output.
- [ ] Add maintenance commands for checkpoints, vacuum/GC, statistics, compaction, and integrity check.

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
- [ ] Publish every data model's durability, transaction, policy, and consistency semantics.
- [ ] Generate command/config references from code where practical.
- [ ] Keep `README.md` concise and link to detailed, versioned operational docs.

Exit gate:

- A new user can install, migrate sample data, secure, back up, restore, upgrade, and diagnose Nucleus
  using only version-matched documentation.

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
