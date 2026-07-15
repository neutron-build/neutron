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

- Source LOC: 239248; Source Rust files: 214; Top-level modules: 50.
- Declared unit tests: 3864; Declared integration tests: 309; Ignored tests: 155;
  Binary-protocol stubs: 113. These are static declarations, not executed-test claims.
- The most recent full library run executed 3,737 passing tests and 113 ignored native-binary
  test stubs. Core-only executed 1,853 passing tests with no ignores.
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
- [x] Categorize every remaining ignored test as stress/scale (42) or binary-protocol defect (113).
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
- [ ] Differential-test planner execution against the AST path for every supported SELECT shape.
- [ ] Differential-test LSM and columnar engines against MVCC for filters, ranges, grouping, NULLs,
      ordering, updates, deletes, and restart.
- [ ] Complete numeric/decimal aggregate precision and overflow behavior.
- [ ] Verify collations, time zones, date arithmetic, NULL ordering, casts, and three-valued logic.
- [ ] Verify constraints and cascades across transactions and restart.
- [ ] Finish MVCC garbage collection/vacuum behavior for long snapshots and high churn.
- [ ] Add deterministic transaction-ID exhaustion/wraparound behavior.
- [ ] Ensure query caches and specialty indexes invalidate on every relevant DDL/DML transition.

Evidence:

- `columnar_insert_schema_coercion_is_strict` covers every native columnar physical type and
  fail-closed invalid input through the public executor path.
- The active GIN unit/restart suite covers planner selection and EXPLAIN, exact rechecks, arrays,
  transactions, insert/update/delete/drop, duplicate DDL, generation-safe invalidation, and rebuild
  from the persisted catalog. The 2026-07-15 full library run passed 3,737 tests; strict all-target
  clippy passed with `bench-tools,memory-debug`.

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

- [ ] Complete adversarial RLS coverage for all relational reads/writes, COPY, constraints, triggers,
      views, caches, prepared statements, exports, CDC, replicas, and engine variants.
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

- [ ] Run a curated PostgreSQL regression suite and publish the supported/deviation matrix.
- [ ] Complete extended-query Parse/Bind/Describe/Execute/Sync semantics and parameter inference.
- [ ] Verify portals, prepared statement lifecycle, transaction error state, cancellation, and timeout.
- [ ] Complete COPY text/binary behavior for the supported subset.
- [ ] Normalize SQLSTATE/error fields, notices, command tags, row descriptions, and type OIDs.
- [ ] Verify TLS/SCRAM negotiation and pooler reconnect/session-reset behavior.
- [ ] Test psql, pgcli, libpq/psycopg, JDBC, Npgsql, tokio-postgres, SQLAlchemy, and one JS ORM.
- [ ] Add compatibility tests for migrations, introspection, transactions, prepared queries, and pools.

Exit gate:

- The published client matrix passes from clean containers against a release build.
- Unsupported PostgreSQL behavior fails explicitly rather than returning a plausible wrong result.

## Milestone 7 — Native binary protocol decision and completion

Goal: eliminate the current advertised-but-stubbed protocol state.

- [ ] Decide to support or remove the native binary protocol from the supported product surface.
- [ ] If supported, implement authenticated handshake, framing, value codecs, typed parameters,
      statement lifecycle, cancellation, limits, errors, and session isolation.
- [ ] Replace all 113 ignored TODO tests with active behavior tests.
- [ ] Add malformed-frame property/fuzz tests and pgwire result-parity tests.
- [ ] Propagate the trusted principal and RLS context; reject bootstrap/principal-less execution.
- [ ] If removed, delete listener/config/public claims while retaining only deliberately internal codecs.

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
- [ ] Measure vector recall/latency, filtered ANN behavior, FTS relevance, graph traversal, and TS ingest.
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
