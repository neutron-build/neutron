# Open audit items

Unresolved findings for this repository from the ChatGPT-led audit series.
Read this before treating related work as done; update it when you close,
defer, or upstream-report an item.

The WebAuthn and versioned session-store deferrals were implemented on
2026-09-30 with explicit migration requirements. Earlier audit findings are
fixed, partially fixed with the remainder scoped, or recorded as false
positives with evidence. The 2026-09-17 through 2026-09-18 re-verifications
resolved the disputed closures, WAL-format and snapshot-lease defects, and
NU-01's bounded compaction subset. The ORM program's N1–N16 findings are
tracked below as verified bounded contracts and remaining unsupported
behavior; their original blanket open status no longer describes the source.

## Resolved 2026-09-18 (round 5 — lease-scope closure + NU-01 compaction subset)

Two work items from the observe backup session's upstream reports and the
round-4 NU-01 note, closed the same day.

### Lease-scope defects (the three 2026-09-18 teploy-observe reports, `Teploy/_internal/UPSTREAM_BUGS.md` newest entries)

All three root-caused on the exact stacks the live probes hit, each with a
fail-before/pass-after regression test in
`nucleus/src/executor/tests/test_snapshot_lease.rs`:

1. **KV/specialty scalar writes bypassed the writer gate.** `SELECT
   kv_set(...)` parses as a Query, so the dispatch gate's DDL/DML shape
   match never saw it — every SQL client's KV writes sailed through the
   lease window (the 8664cbd1 fix had covered only the RESP-wire fast
   path). FIXED: the dispatch walks the statement AST for mutating scalar
   calls while a lease is held
   (`admission::statement_carries_mutating_scalar_fn`; canonicalized like
   the scalar dispatcher, so WHERE-carried and `pg_catalog.`-qualified
   calls gate too). No-lease path keeps its O(matches) cost.
2. **Holder reads were not pinned to the acquire-time snapshot.** A writer
   whose statement predated the lease could COMMIT mid-window (COMMIT is
   not a gated statement) and the holder's next statement saw the new
   rows. FIXED two ways: ACQUIRE now DRAINS — waits, bounded by the
   lease's own TIMEOUT, for every other session's write-bearing
   transaction to end (engine-side via the new
   `StorageEngine::session_has_uncommitted_writes` — BufferedDiskEngine
   buffers, MVCC undo; executor-side via before-images, cross-model
   enlistment, policy/GIN/derived markers) — and versioning engines
   re-take the holder's snapshot at the acquire instant (new
   `StorageEngine::refresh_txn_snapshot`; the MVCC adapter pins it for
   the rest of the transaction, suppressing the READ COMMITTED
   per-statement refresh while held, so the pinned moment is ACQUIRE,
   not BEGIN).
3. **Holder saw another sessions' in-flight uncommitted writes.** The live
   shape was a per-table override engine (mergetree) on the disk stack —
   those engines have no transaction buffering, so the parked INSERT was
   readable outright. FIXED by the same drain: the window can only open
   on resolved state. Acquisition inside a transaction that has already
   written is refused (its moment cannot include its own uncommitted
   work; its write-side resources could deadlock the drain). Honest
   residual, documented in the lease module: a mutation already
   mid-statement at the acquire instant races the drain the same way it
   races the DML gate, and RESP-direct / background specialty writes
   remain outside the gate (unchanged scope note). Engine-side
   equivalents of teploy-observe's backup consistency proofs landed
   (`backup_under_lease_is_one_moment_across_models`: heap + mergetree +
   KV under one window). Commit `4c7c4367`.

### NU-01 in-memory compaction — the safe subset landed, the rest scoped

WAL v2's stable ids unblocked GC compaction; what landed
(commit `6d7c3ffa`) is the conservative subset:

- **Landed — dead-TAIL reclamation.** An all-dead suffix of a table's row
  vector is truncated at VACUUM under gc's own watermark soundness rule.
  No live id moves (mid-vector dead versions stay neutralized in place),
  the mint floor is raised to the pre-truncation horizon before slots
  release (never-rewinds holds; later mints pad invisible fillers to the
  floor, the same shape recovery's `pad_to` produces), and
  `table_version_count` reports max(len, floor) so durable baselines
  cannot understate the horizon. Durable effects: none — replay rebuilds
  a superset, so no new WAL record; proven by WAL-size-unchanged,
  reclaim/mint/reopen, live-id-stability, and torn-tail-after-compaction
  tests, plus a `gc.mid_compaction` crashpoint for the subprocess matrix.
- **Deferred, precisely — mid-vector compaction/renumbering.** Requires
  an id→position map on every addressing path (secondary indexes, pending
  mutations, WAL Delete/Update records, executor position resolution);
  until that indirection exists, moving a live id breaks
  position==durable-id. Rejected for this pass.
- **Deferred, precisely — WAL-space reclamation of dead records.** The
  log retains dead Insert/Delete records until a reopen compacts the
  baseline (which preserves ids and floors but rewrites dead rows too).
  Reclaiming them at runtime needs a new v2 Compaction baseline record
  and a replay floor-reset rule — the one place the never-rewinds
  invariant would be redefined rather than honored. Not taken in this
  pass.

## Round-2 verification (2026-09-17, re-audit at `360c0023`)


An independent re-verification of the 74-finding pass plus twelve new
findings (TS-31/32, GO-25..31, NU-21..23). Disputes it raised against
recorded fixes were re-checked against source; all five were real and are
now repaired:

| Dispute | Verdict | Repair |
|---|---|---|
| TS-07 | Real — epoch advanced only pre-mutation; loader fills unfenced | Completion invalidation advances the epoch again; loader fills re-check a generation fence before publishing |
| TS-29 | Real — stages constructed in order but executed via `Promise.allSettled(map)` | Stages run sequentially with error aggregation; close is memoized/idempotent |
| NU-02 | Real — `created_by != txn_id` guard dropped same-transaction rows on savepoint rollback | Guard removed; ownership proven by observed `deleted_by` CAS |
| NU-03 | Real — `split_off` discarded the undo tail before applying it | Tail cloned, truncated only on success; failed rollback dooms the txn (commit/savepoints refuse until ROLLBACK) |
| NU-05 | Real — failed commit fsync treated as clean abort | Commit IO failure now INDETERMINATE: WAL fenced for all writes until recovery (reopen), explicit error, no Abort record appended |

New-finding resolutions: TS-31 FIXED, TS-32 FIXED-PARTIAL, GO-25/26/27/28
FIXED, GO-29/30/31 FIXED-PARTIAL, NU-21/22/23 FIXED — details folded into the
table rows below. Gates at commit time: typescript 538 tests, go build+test
(+`-race`) all green, nucleus `cargo test --lib` 4900 passed + clippy clean.

## Resolved 2026-09-17 (74-finding pass: TS-30, GO-24, NU-20)

Source: the 2026-09-17 ChatGPT audit of `typescript/`, `go/`, and
`nucleus/`, pinned at `9d8e5505`. Status vocabulary: FIXED (defect closed),
FIXED-PARTIAL (named defect closed; remainder explicitly scoped below),
CONTAINED (unsafe behavior disabled pending the durable redesign),
DEFERRED (needs a format/protocol/API redesign — a product decision),
FALSE-POSITIVE (not reproducible in source; evidence cited).

| Item | Resolution | Commit |
|---|---|---|
| TS-01 | FIXED — global-middleware load failures and required-SSR failures reject createServer; no ungated static fallback | c31736eb |
| TS-02 | FIXED — shared app-cache boundary runs INSIDE the middleware chain; hits execute auth/rate/audit middleware | acf405c0 + c31736eb |
| TS-03 | FIXED — response-level single-flight sharing removed entirely | c31736eb |
| TS-04 | FIXED — cache keys carry origin, Accept-Language, X-Neutron-Data/Routes | c31736eb |
| TS-05 | FIXED — case-insensitive directive parsing, request no-store/no-cache honored, Vary checked against keyed set, TTL capped by s-maxage/max-age | c31736eb |
| TS-06 | FIXED-PARTIAL — byte-exact bodies with per-entry budget and Content-Length validation; aggregate store budget + concurrent-fill cap not added (entry-count bound remains) | c31736eb |
| TS-07 | FIXED — mutation start/completion invalidation advances the backing-store generation; response and loader fills publish only through atomic `setIfGeneration`. Custom server stores without the contract are refused before resources open. Completion always invalidates the mutation path, including actions without an invalidation header. Delayed external publication and during-action GET regressions cover already-published fills and fills through a second server sharing the backing store. | r2 + `cache-atomic-publication.e2e.test.ts` |
| TS-08 | FIXED — segment-based traversal matching the serving path; invalidation matches exact cache-key path fields (no /user sweeping /users); canonical server paths are segment-encoded before passing the raw-path store contract, preserving residual percent signs in automatic and explicit mutation invalidation | c31736eb |
| TS-09 | FIXED — static HTML cache never answers X-Neutron-Data/JSON requests | c31736eb |
| TS-10 | FIXED — backslash/NUL rejected in URL paths; image source resolution is realpath-contained (escaping symlinks refused). @hono/node-server's serveStatic is library surface, not modified here | 67ff9c6b |
| TS-11 | FIXED — remote fetch uses redirect:error and rejects URL credentials | 67ff9c6b |
| TS-12 | FIXED — bounded byte reader enforces the cap as bytes arrive; upstream cancelled on breach; sharp limitInputPixels bounds decode | 67ff9c6b |
| TS-13 | FIXED — fail closed: sharp missing = 503, undecodable input = 415; raw source-byte fallbacks removed | 67ff9c6b |
| TS-14 | FIXED-PARTIAL — atomic temp+rename publication, async IO; content-versioned keys/freshness for mutable sources and disk quota DEFERRED (needs an application-level source-versioning design) | 67ff9c6b |
| TS-15 | FIXED — sharp import promise cached (no first-request race), quality-aware Accept negotiation, strict decimal-integer params | 67ff9c6b |
| TS-16 | FIXED — eviction always makes progress (no floor(0.1n)=0 for n<10); replacement refreshes LRU recency | 67ff9c6b |
| TS-17 | FIXED — session/CSRF middleware rewrap responses before appending Set-Cookie (immutable headers no longer throw post-persistence) | 67ff9c6b |
| TS-18 | FIXED — Set-Cookie values appended (getSetCookie), parent/child cookies preserved | acf405c0 |
| TS-19 | FIXED — middleware requires atomic revision-conditional commit, rotation and revocation. Memory and SQL adapters implement the contract; stale persistence suppresses the original response/cookie with 503. Legacy custom stores are refused. This fences stored state, not already executing application work. | `session.test.ts`, `session-sql.integration.test.ts`; migration: `go/neutronauth/README.md` |
| TS-20 | FIXED — memory session and loader-cache data deep-cloned at ingress and egress | 67ff9c6b |
| TS-21 | FIXED-PARTIAL — live-key cap admits no new keys (controlled 429), options validated, first-request headers emitted; populating context.clientAddress from the transport remains a feature (the missing-address warning is already in place) | 67ff9c6b |
| TS-22 | FIXED — DELETE bodies covered; Content-Length stays as the early check (middleware + adapter). Round 4 landed the actual-byte half: `maxRequestBodyBytes` makes the server adapter wrap every body-bearing request's stream so reading past the cap fails with 413 and cancels the sender — covering chunked (no Content-Length) bodies and lying declared lengths | 67ff9c6b + r4 |
| TS-23 | FIXED — origin comparison includes scheme+port; safe methods uppercased; docs show header submission (form-body token support deliberately not added) | 67ff9c6b |
| TS-24 | FIXED — pull-based streaming with backpressure, cancel propagation, single lock release; first-chunk guard failure cancels the reader | acf405c0 |
| TS-25 | FIXED — layout loaders receive the full matched params | acf405c0 |
| TS-26 | FIXED — loader errors discriminated by property presence (throw null/0/"" render the error path) | acf405c0 |
| TS-27 | FIXED — 405+Allow for unsupported methods and mutations on action-less routes; custom 404 rewrites only 200s | acf405c0 + c31736eb |
| TS-28 | FIXED-PARTIAL — fail-closed pre-upgrade `authorize` hook with 5s deadline added; full transport-middleware/built-in separation is a pipeline redesign | c31736eb |
| TS-29 | FIXED — stages run SEQUENTIALLY (round-2 dispute repaired: they were fired concurrently via allSettled despite the ordered array); drain HTTP before SSR teardown, errors aggregated, close memoized/idempotent | c31736eb + r2 |
| TS-30 | FIXED-PARTIAL — Vary: Origin on allowed/disallowed/no-origin paths. Security-header case-merge half is FALSE-POSITIVE: `Headers.set` replaces case-insensitively, so a lowercase user override applied after the uppercase default always wins (last write, verified in Node) | 67ff9c6b |
| GO-01 | FIXED — binding goes through the generic input; pointer inputs bind, JSON null rejected | 4f743588 |
| GO-02 | FIXED — destination-width parsing with field-specific 400s; named string elements; non-finite floats rejected | 4f743588 |
| GO-03 | FIXED-PARTIAL — single-JSON-value enforcement and DELETE binding (url-encoded DELETE bodies actually decode now — GO-28); an http.MaxBytesReader at the HTTP boundary is server configuration left to deployments | 4f743588 + r2 |
| GO-04 | FIXED — JSON and problem+json marshal before committing headers | 4f743588 |
| GO-05 | FIXED-PARTIAL — InvalidValidationError surfaces as 500; rejected raw values no longer echoed. Nested field paths keep the json-tag name (Namespace paths would change the public error shape) | 4f743588 |
| GO-06 | FIXED — credentialed wildcard CORS panics at construction; origin entries validated | 4f743588 |
| GO-07 | FIXED-PARTIAL — Vary: Origin on every path; only genuine preflights (Origin + ACRM) intercepted. The preflight response still echoes the configured method list rather than validating the request's requested method against it | 4f743588 |
| GO-08 | FIXED — strict q-value parsing; explicit gzip;q=0 never compresses | 4f743588 |
| GO-09 | FIXED — lazy header commitment on final headers, Content-Length removed at commitment, bodyless/range/encoded/upgrade/no-transform skipped, no gzip trailer during panic unwinding. Round-2 writer defects (GO-25) FIXED. Round 4 closed the residual: a panic after the first compressed byte converts to net/http's ErrAbortHandler (connection abort, unfinalized member, no plain-text error appended) and Recover re-panics the sentinel — in-band is impossible once the 200+Content-Encoding are on the wire, and detectable truncation is the honest answer; panics before any byte still answer in-band | 4f743588 + r2 + r4 |
| GO-10 | FIXED — first final status recorded (implicit 200, 1xx forwarded, Flush establishes status) | 4f743588 |
| GO-11 | FIXED — reverse-order rollback of started hooks, combined errors, fresh bounded stop budget, bind failures stop hooks | 4f743588 |
| GO-12 | FIXED — /health prefers a bounded live Ping probe (503 degraded on failure); IsNucleus remains the identity fallback | 4f743588 |
| GO-13 | FIXED-PARTIAL — built-ins yield to method-equivalent user routes; OpenAPI regenerates on a registration-generation mismatch. Mount/Static route-record unification not redesigned (mounts stay opaque to OpenAPI by design) | 4f743588 |
| GO-14 | FIXED — >=32-byte keys, pinned JOSE header, UseNumber decode, duplicate-key and trailing-JSON rejection, mandatory numeric exp checked at now>=exp, nbf honored | 086e0253 |
| GO-15 | FIXED — caller's claims map copied; nil map works; positive-lifetime validation | 086e0253 |
| GO-16 | FIXED — go-webauthn parses and verifies registration/assertion ceremonies with single-consume challenge state and versioned credentials. Legacy registrations require authenticated re-enrollment. Default none attestation does not establish hardware provenance. | `go/neutronauth/webauthn_test.go`; migration: `go/neutronauth/README.md` |
| GO-17 | FIXED — library verification covers challenge, origin, RP ID, presence/verification, signature and counter semantics; failed credential CAS refuses login. Zero counters follow library semantics and are not proof against cloning. | `go/neutronauth/webauthn_test.go` |
| GO-18 | FIXED — token-prefix identity fallback removed; verified userinfo endpoint required; numeric subjects decode with UseNumber; empty subject rejected | 086e0253 |
| GO-19 | FIXED — bounded owned HTTP client, inbound-context outbound requests, trimmed body classification, limit+1 truncation rejection, >=32-byte state secrets, query-preserving authorization URL | 086e0253 |
| GO-20 | FIXED — rotation/revocation failure replaces the response with 503, suppresses body and cookie; onCommitError retained for observability | 086e0253 |
| GO-21 | FIXED — detached cleanup bounded at 5s; informational 1xx no longer finalizes the session | 086e0253 |
| GO-22 | FIXED-PARTIAL — optional server-side HMAC token signing (defeats sibling cookie injection), POST-body-only form fallback, any-unsafe-verb coverage. __Host- cookie prefix not made default (breaking change for existing sessions) | 086e0253 |
| GO-23 | FIXED — errors.Join preserves cancellation, bounded detached rollback context, isolation allowlist before SQL splicing, overflow-safe jitter | 086e0253 |
| GO-24 | FIXED — rate/burst validated at construction, SplitHostPort client IP, Timeout documented cooperative. The capacity defect round 2 found under GO-24 is fixed as GO-27 | 4f743588 |
| NU-01 | FIXED (durable half, round 4) + CONTAINED (in-memory half) — GC never compacts the row vector: dead versions are neutralized in place, so WAL/index/mutation addresses stay stable. The durable half landed with WAL v2: stable 64-bit version ids in every record, per-table id floors, identity-preserving baselines, replay validation. Round 5 landed the unblocked SUBSET: all-dead TAIL slots are reclaimed at VACUUM with ids preserved and the mint floor holding the pre-truncation horizon (no live id moves, never-rewinds holds, no durable effect — see the round-5 section). Mid-vector renumbering and WAL-space reclamation of dead records remain deferred with their reasons recorded there | a0732f5c + r4 + r5 |
| NU-02 | FIXED — savepoints are O(1) marks into a per-transaction undo journal; rollback replays post-mark ops in reverse, preserving identity and duplicate multiplicity | a0732f5c |
| NU-03 | FIXED — rollback writes WAL compensation records (Insert/Delete/Update under the same txn id — existing formats), so replay applies the rollback exactly when the outer transaction commits | a0732f5c |
| NU-04 | FIXED — corruption (CRC/length/tag/decode) fails startup with the byte offset and leaves the file untouched; torn FINAL frames are the explicit accepted case. The round-2 residual (unchecked length prefix) closed with v2 framing (round 4): the checksummed magic+version+length header makes a corrupted length provable damage — including in the final frame — while a valid-header truncated payload stays the accepted torn-tail case (v2 open repairs it; legacy logs keep v1 semantics on their read-only upgrade path) | 6033d56d + r4 |
| NU-05 | FIXED — commit validates under the serializable commit-point lock, durably decides (WAL commit + fsync, S63 marker on the same sync), then publishes. Round-2 dispute repaired: a commit IO failure is now INDETERMINATE — the WAL is fenced for every subsequent write until recovery (reopen), the error says the outcome is unknown, and no Abort record contradicts a possibly-durable Commit | a0732f5c + r2 |
| NU-06 | FIXED — auto-txn drop guards, begin/abort reordering (logical cleanup cannot be skipped by a WAL failure), drop_storage_session aborts the registered transaction. Round-2 residual (create_index early-return leaked its observer) also fixed | a0732f5c + r2 |
| NU-07 | FIXED — multi-row auto-commit statements log real Begin/records/Commit WAL transactions; failures write Abort so replay excludes the prefix | a0732f5c |
| NU-08 | FIXED (round 4) — one checksummed CommitV2 frame carries the txn id and every enlisted coordinating id under a single fsync decision; the two-record window is closed for new writes. Old readers reject a v2 log fail-closed (magic parses as an impossible length, error names it); the v1 decoder names tag 0x14 as a newer-writer record | r4 |
| NU-09 | FIXED — indexed equality returns every visible match with per-candidate key recheck; the early break is gone. (Durable half of its identity clause landed with WAL v2 ids — round 4) | a0732f5c + r4 |
| NU-10 | FIXED — aborted-owner tombstones are CAS-reclaimable from the observed value; committed tombstones still conflict | a0732f5c |
| NU-11 | FIXED — empty predicate reads register the table (both fast paths), and table-level writes leave a marker the read direction discovers; granularity is table-level (conservative, as the audit's containment prescribes) | 3e8afcd9 |
| NU-12 | FIXED — fast COUNT declines inside any explicit transaction | a0732f5c |
| NU-13 | FIXED — integer-vs-integer comparisons are exact; no f64 coercion above 2^53 | a0732f5c |
| NU-14 | FIXED — cached idx.map rows are candidates resolved through a fresh snapshot per version index; the copy is never the authority. (Durable half of its identity clause landed with WAL v2 ids — round 4) | a0732f5c + r4 |
| NU-15 | FIXED — unknown type tags and trailing bytes rejected (fail-closed). Round 4 added lossless versioned type descriptors (vector dims, array element types recursively, UDT names) for v2 records. Honest residual: legacy logs upgrade with parameterized types DEFAULTED — the parameters were never recorded in v1 | 6033d56d + r4 |
| NU-16 | FIXED — conditional updates/deletes carry first-observed expected rows (and constraint sets) through the buffer and re-check atomically at apply; mismatches are write conflicts. In-memory buffer only — no on-disk format touched | 344090ac |
| NU-17 | FIXED — in-transaction index reads decline or merge the union of committed candidates, all overlay updates (including rewrites newly matching the key), and buffered inserts | 344090ac |
| NU-18 | FIXED — table overlays model CREATE/DROP: dropped tables read as gone, created tables inherit no committed rows, drop-then-create starts empty | 344090ac |
| NU-19 | FIXED — RELEASE drops the named savepoint and its descendants; unknown names error | a0732f5c |
| NU-20 | FIXED — vacuum propagates index-rebuild failures (observer committed first) and reports 0 for unmeasured bytes instead of transaction-status counts | a0732f5c |
| TS-31 | FIXED (round 2) — loader-cache keys carry URL origin (placed after the canonical path so `deleteByPath` prefix invalidation still sweeps all origins); cache-control admission is case-insensitive; null/undefined loader results are not stored (a cached null was indistinguishable from a miss) | r2 |
| TS-32 | FIXED-PARTIAL (round 2) — proxy trust now derives the nearest hop from transport-installed socket peer metadata (`installTransportPeer`), never from X-Real-IP/X-Forwarded-For; CIDR masks strictly validated; option docs corrected. Residual: IPv6 matches exact strings only (no v6 CIDR), and adapters that do not install peer metadata get no header trust at all (fail-closed by design) | r2 |
| GO-25 | FIXED (round 2) — gzip writer is a lazy state machine: encoder created only at final-header commitment, closed only if opened (bypassed/empty responses byte-identical), Flush commits before flushing, informational statuses forwarded without finalizing, Content-Type sniffed from original bytes | r2 |
| GO-26 | FIXED (round 2) — statusWriter.Hijack no longer emits an unsolicited 101 (zero header writes, error preserved for unsupported writers) | r2 |
| GO-27 | FIXED (round 2) — hard 100k live-bucket ceiling: new identities refused 429+Retry-After at capacity, existing buckets keep state, expiry sweep throttled to once per 10s (was: full map scan per new key past 100k) | r2 |
| GO-28 | FIXED (round 2) — url-encoded bodies decoded explicitly for every body-bearing method through MaxBytesReader (413 on overflow, 400 on malformed); body takes precedence over query for `form` tags; Go ParseForm ignores DELETE bodies | r2 |
| GO-29 | FIXED (round 2 + round 3) — the whole Migrate/MigrateDown operation (history reads included) is serialized by an in-process gate, closing the appliedVersions→INSERT TOCTOU behind the consumer's 23505. Cross-process runners are now serialized too, by the `_neutron_migration_lock` ledger claim landed with Consumer-1 (round 3). Round 3's 10-minute stale-claim takeover was removed by the ORM program's M04 (2026-09-22): a crashed holder's claim is released only by an explicit `ForceUnlockMigrations` (Go) / `forceUnlockMigrations` (TS), and the heartbeat is diagnostic. Live-engine coverage: `TestConcurrentMigrateBothClientsSucceed`, `TestMigrationLedgerLockNoStaleTakeover` | r2 + r3 + 1482690a |
| GO-30 | FIXED (round 2 + round 3) — plans are copied and prevalidated before any SQL runs (round 2); applied history records a sha256 checksum over version/name/Up, enforced on every run, with legacy rows baselined from the current plan on first new-version run (round 3). History schema carries a nullable `checksum` column, upgraded in place via `ADD COLUMN IF NOT EXISTS`. The ORM program's M04 (2026-09-22) replaced round 3's silent baselining: the checksum is the canonical v2 digest over the up SQL (`contracts/data/MIGRATIONS.md` §3), a history with pre-protocol rows is refused before any mutation, and `AdoptMigrations` graduates it once — rows whose legacy digest reproduces from the supplied file are verified, rows with no recorded checksum or no supplied file of the same version are kept unverified with no checksum, and a recorded checksum that matches no digest of its supplied file refuses the whole adoption (restore the applied SQL or reconcile by hand). Coverage: `TestMigrateRefusesLegacyHistoryUntilAdopted`, `TestMigrateRefusesLegacyDigestHistoryUntilAdopted` | r2 + r3 + 1482690a |
| GO-31 | FIXED (round 2 + round 3) — round 2 kept the Go client's heuristic decoder as a stopgap. Round 3 verified the engine already honors its declared result formats end to end (client-requested formats honored since `1a1b1b41`; text is the default everywhere, binary only on explicit Bind) and pinned it byte-for-byte on the wire in both directions (nucleus `wire::tests_row_description::integer_payloads_honor_the_declared_format`: ASCII decimal incl. `-123`/`i64::MIN` under format 0 for simple, extended-default, and explicit-text Bind; true big-endian int4/int8 under format 1). The Go heuristic (`scanInt`) is removed; `appliedVersions`/`MigrationStatus` scan integers natively, and a live-engine round-trip test pins the client half | r2 + r3 |
| NU-21 | FIXED (round 2) — one shared checked frame encoder for append and compaction: payloads over the 64 MiB replay limit are rejected before the first byte, so the writer can no longer accept a record its own replay refuses | r2 |
| NU-22 | FIXED (round 2) — index scans hold ONE registered observer for the whole statement (snapshot passed into candidate resolution, aborted only after materialization; was: snapshot detached before use while vacuum could reclaim under it, and per-key observers mixed snapshots in one range scan); observer allocation failure declines the optimization instead of a false empty result | r2 |
| NU-23 | FIXED (round 2) — replay validates terminal decisions: Commit+Abort for one txn is corruption (fails recovery with the id, file preserved); identical duplicate markers stay idempotent; terminal records for reserved id 0 rejected | r2 |

## Open deferrals (decisions needed, not work items)

1. ~~**MVCC stable 64-bit version IDs**~~ — resolved 2026-09-18 (round 4,
   WAL v2): see deferral-resolutions below.
2. ~~**Atomic cross-model commit record**~~ — resolved 2026-09-18 (round 4).
3. ~~**Lossless WAL schema codec**~~ — resolved 2026-09-18 (round 4).
4. ~~**Versioned WAL framing**~~ — resolved 2026-09-18 (round 4).
5. ~~**WebAuthn library integration**~~ (GO-16) — resolved 2026-09-30;
   ceremony verification and credential persistence require the documented migration.
6. ~~**Versioned/CAS session store contract**~~ (TS-19 and the fenced half of
   GO-20/GO-21) — resolved 2026-09-30; unversioned stores are refused.

The session/WebAuthn contract migration and operational limits are documented in
[`go/neutronauth/README.md`](go/neutronauth/README.md).

## Resolved deferrals (2026-09-18, round 4 — WAL format v2 + snapshot lease)

Founder-ratified direction, landed as MVCC WAL format v2 (nucleus
`storage/mvcc_wal.rs`) with upgrade-on-open; no dual-format append limbo.
Deferrals 1-4 above resolve as:

1. **Stable 64-bit version IDs** (durable half of NU-01/NU-09/NU-14):
   every v2 record carries a u64 version id (the `as u32` narrowing is
   gone); `CreateTable` and every compaction baseline record a per-table
   id floor; replay advances the floor past every id the log ever
   contained (live, dead, or aborted) and rejects a committed INSERT/UPDATE
   onto a live id as corruption; recovery re-seats each row at its original
   id with invisible filler padding, so the minted-id space never rewinds
   across restarts. In-memory row-vector GC compaction (NU-01's contained
   half) is now UNBLOCKED — the WAL speaks stable ids, not vector
   positions — and deliberately NOT implemented; the neutralize-in-place
   containment stands.
2. **Atomic cross-model commit** (NU-08): one checksummed `CommitV2` frame
   carries the txn id and all enlisted coordinating ids — the two-record
   same-fsync window (Commit + XactCommit) is closed for new writes. Old
   binaries reading a v2 log reject it fail-closed (the frame magic parses
   as an impossible record length; the error names the situation), and the
   v1 decoder names tag 0x14 explicitly as a newer-writer record.
3. **Lossless schema codec** (NU-15 remainder): versioned type descriptors
   — vector dims, array element types recursively, UDT names — round-trip
   exactly through v2 `CreateTable` records; unknown codes still fail
   closed. Honest loss noted: LEGACY logs upgrade with parameterized types
   defaulted (Array→text[], Vector(0), UDT "" ) — the parameters were
   never recorded in v1; v2 is lossless from here on.
4. **Versioned framing** (NU-04 remainder): every v2 frame carries a
   checksummed magic+version+length header. A complete 14-byte header with
   a bad header CRC is provable damage (a crash leaves a header PREFIX,
   handled as torn tail), so a corrupted-but-plausible length is
   corruption, not a torn tail — including in the final frame. Valid
   header + physically truncated payload remains the accepted torn-tail
   case; v2 open repairs it by truncation. A v2 log torn-tail corpus, the
   frozen v1 corpus (generated against the pre-v2 writer), and
   upgrade-idempotence tests pin the whole path; the original of an
   upgraded log is preserved as `mvcc.wal.v1` until the next clean v2
   open retires it.

Consumer-2 (the snapshot capability gap) also closed this round — see the
consumer section below.

## Resolved deferrals (2026-09-18, round 3)

- **Migration history checksum** (was deferral 7, GO-30 remainder):
   resolved as a nullable `checksum` column on `_neutron_migrations`
   (upgraded in place with `ADD COLUMN IF NOT EXISTS`) — see the GO-30 row.
- **Nucleus pgwire integer format** (was deferral 8, GO-31 remainder /
   Nucleus finding #35): the engine has honored client-requested result
   formats since `1a1b1b41` (2026-07-09) — text by default, binary only when
   the client Binds it. The round-2 residual was stale: it assumed the
   declared format still disagreed with the payload. Round 3 pins the
   contract with byte-level wire tests in both directions and removed the Go
   client's heuristic decoder. No engine change was needed.

## Resolved 2026-09-11 (prior series)

All 18 P2 items plus the nucleus residual were verified still present in
current source and fixed. No item was ALREADY FIXED and none was DEFERRED
against a recorded decision.

| Item | Resolution | Commit |
|---|---|---|
| neutron-02 | FIXED — explicit `upgrade` fails when the update check fails | fec68499 |
| neutron-03 | FIXED — cmd-context propagation, documented `--timeout`, strict count parse | 9b9852b7 |
| neutron-05 | FIXED — migration name slugs from a conservative charset, path-like names rejected | 9b9852b7 |
| neutron-06 | FIXED — numeric version ordering (files, status, rollback frontier) | 9b9852b7 |
| neutron-07 | FIXED — status is the union of DB records and local files, missing flagged | 9b9852b7 |
| neutron-08 | FIXED — ZIP extraction for Windows assets + tar.gz/zip fixtures | 5e126cf1 |
| neutron-10 | FIXED — SemVer 2.0.0 precedence; prerelease offered only to prerelease installs; build metadata ignored | 5e126cf1 |
| neutron-11 | FIXED — context-aware bounded downloads, byte caps, status checks, fsynced temps | 5e126cf1 |
| neutron-12 | FIXED — context-aware SMTP dial/deadlines/cancellation-driven close | b9d10536 |
| neutron-13 | FIXED — addresses via net/mail.Address.String + control-char rejection | b9d10536 |
| neutron-16 | FIXED — bearer attached only to explicitly allowed HTTPS origins (all redirect hops) | e4e2c8fc |
| neutron-18 | FIXED — Vary parsed fully; `*` and unrepresented fields never cached | 52e714b4 |
| neutron-19 | FIXED (default contract) — request no-cache bypasses read; response no-cache/max-age=0 not stored; max-age/s-maxage bound the stored TTL. The separately-proposed named opt-in override was deliberately NOT added: responses without freshness metadata already take the middleware TTL, and no route asked for stronger-than-origin behavior | 52e714b4 |
| neutron-20 | FIXED — Unwrap/Flush/Hijack forwarded; flushed/hijacked responses non-cacheable and uncaptured | 52e714b4 |
| neutron-22 | FIXED — exact mount root normalized to the "/" subrequest (one namespace per mount) | 70241b91 |
| neutron-23 | FIXED — problem+json rewrite only for genuine unmatched/method-mismatch outcomes (mux.Handler pattern check); application 404/405 pass through | 70241b91 |
| neutron-14 | FIXED — pathname-index TTL only ever extended (never shortened below a live member) | 14dceb36 |
| neutron-15 | FIXED — invalidation claims the index via atomic RENAME; concurrent writers stay indexed | 14dceb36 |
| nucleus-residual-01 | FIXED — execute_parsed/execute_prepared route through execute_statements_dispatch | 456e4526 |

## Reported by consumers, resolved (2026-09-18 — engine defects from the teploy-observe trust-close session)

Two Nucleus engine defects filed 2026-09-18 from teploy-observe's F12/F19
close-out (its private ledger is `Teploy/_internal/UPSTREAM_BUGS.md`, newest
entries). Both root-caused by direct builds of named revisions; both now
carry lib-suite regression tests
(`nucleus/src/executor/tests/test_upstream_teploy_2026_09_18.rs`).

- **Renamed table invisible to later statements in the same script** —
  fresh installs of teploy-observe failed at migration 027
  (`ALTER TABLE events RENAME TO events_pre027; …; INSERT INTO events
  SELECT … FROM events_pre027`) on repo-built engines with
  `table 'events_pre027' not found in storage`, while the v0.1.8 image
  applied the ladder. Two live defects, both on the disk stack
  (`BufferedDiskEngine` over `DiskEngine` — the server shape with a data
  directory; the in-memory MVCC adapter never had either):
  1. *Committed-table rename inside a transaction* (the pinned-submodule
     5b5d0a3 failure): fixed on main by the 2026-09-17 round-2
     buffered-disk DDL-visibility work (`344090ac`, NU-16..18). Pinned by
     `migration_027_rebuild_shape_in_one_tx`.
  2. *Same-transaction CREATE + RENAME + COMMIT* — still broken at HEAD:
     the buffered `CreateTable` op replayed at COMMIT against
     `DiskEngine::create_table`, which re-asked the CATALOG for the schema
     under the old name — but the RENAME statement had already moved the
     catalog entry, so COMMIT failed `table '<old>' not found in storage`
     and left DDL debris. FIXED: the buffered op now carries the schema
     captured at statement time (`TableSchemaSnapshot`), making COMMIT
     replay self-contained. Pinned by `create_rename_commit_in_one_script`.
  A third, adjacent blind spot fell out of the same investigation:
  RENAME consulted only the `engines.json` sidecar to decide whether a
  table has a per-table engine override, so a memory-mode database (no
  sidecar) or a table created before the sidecar existed took the PLAIN
  rename path for an override-engine table — copying rows into the base
  engine while the routing map still pointed at the override, after which
  the renamed table answered `not found in storage` (the same 027 symptom
  through another door). FIXED: the decision now falls back to the
  catalog's engine spec (sidecar first, catalog second, mirroring
  `restore_table_engines`). Pinned by `mergetree_override_rename_in_tx`
  (which fails on the pre-fix tree with exactly
  `TableNotFound("m_events_pre")`).
  The report's double-wrapped error text
  (`table 'table 'x' not found' not found in storage`) was a second defect
  in the MVCC adapter's error mapping — MvccErrors flattened through
  `StorageError::TableNotFound(e.to_string())`. FIXED with a
  variant-preserving `From<MvccError> for StorageError`.
  Verified: observe's full 001-041 migration ladder applies on a
  repo-built debug binary via the real neutron-go `Migrate` path (Go
  reproducer against a live engine), 5/5 fresh installs.
- **Committed same-key ReplacingMergeTree upsert lost in a multi-table
  transaction** — silent data loss (~6-in-20 on accumulated data; observe's
  replay-session upserts since migration 039). Reproduced 8/20 with the
  exact observe DDL on the v0.1.8 image (= main at `d1384841`,
  2026-08-21) and 8/20 on main at `d4a30583` (2026-08-20); the loss is
  DELAYED — the immediate post-commit read can pass while a later
  MergeTree part merge drops the committed higher-version row, leaving the
  older version's payload stable forever. Root cause window verified by
  direct builds: fixed on main by the 2026-08-25 columnar-atomicity work
  (`b172b43f`, S63 slices 5-8) — the v0.1.8 image was cut four days before
  that fix and still ships it. Every revision tested since (b172b43f,
  a0732f5c, 344090ac, 6dcefaab, HEAD) is clean: 0 lost across 700+
  iterations at 20/30/200/300-iteration scales, immediate AND delayed
  re-verification. Pinned at HEAD by
  `replacing_upsert_multi_table_tx_survives_and_survives_merges` (accumulated
  seed, multi-table txs, merge pressure, then re-verify every key and the
  child rows). **Action needed outside this repo: the published
  `ghcr.io/neutron-build/nucleus` images v0.1.5/v0.1.8 predate both fixes
  (and the arm64 glibc break already on file) — an image rebuild from
  current main is required before the next release; no tag was cut from
  this session.**

## Reported by consumers (2026-09-17), both resolved

Found by teploy-observe's live-engine verification during its 2026-09-17 audit
close-out (its detailed upstream ledger is local to the Teploy umbrella,
`Teploy/_internal/UPSTREAM_BUGS.md`); recorded here so Neutron sessions see
them without that folder:

- **Migration-ledger TOCTOU** — concurrent `Migrate` callers race the
  `appliedVersions`→INSERT sequence and lose with `duplicate key ... (version)`
  (SQLSTATE 23505) at `go/nucleus/migrate.go`; originally reported 2026-08-26,
  reconfirmed 2026-09-17. **Round 2 (GO-29): fixed in-process** via the
  package gate. **Round 3 (Consumer-1): fixed cross-process** — Migrate and
  MigrateDown now claim a `_neutron_migration_lock` ledger row
  (INSERT-first, `ON CONFLICT DO NOTHING`) before touching history; a second
  runner in any process blocks until the holder releases, and a claim whose
  `locked_at` goes unrefreshed for 10 minutes (holder crashed) is stolen by a
  server-side atomic staleness predicate. Advisory-lock verdict, verified in
  engine source before choosing the design: Nucleus has no `pg_advisory_lock`
  (only an honest `pg_advisory_unlock_all` no-op), so no engine-backed lock
  exists to build on — the ledger claim is the contract, not a fake lock.
  Live-engine coverage: `go/nucleus/migrate_integration_test.go`
  (`NEUTRON_TEST_DATABASE_URL`). **RESOLVED 2026-09-22 (ORM program M04,
  `1482690a`)**: the claim carries an owner token and is never taken over on
  a timer (a crashed holder is released with `ForceUnlockMigrations`); two
  concurrent runners produce exactly one effect and one history row on the
  Go SDK, the TS SDK and the CLI (the CLI runs PostgreSQL only, under a
  session advisory lock on a pinned connection). Protocol:
  `contracts/data/MIGRATIONS.md` §5.
- **No cross-table consistent-snapshot boundary** — RESOLVED 2026-09-18
  (round 4, Consumer-2): the engine now has a database-wide snapshot lease.
  `ACQUIRE SNAPSHOT LEASE [TIMEOUT <millis>]` (inside a transaction) pins
  the holder's MVCC moment across every table and blocks every other
  session's SQL mutations at the dispatch gate until release/expiry — so a
  dump reading tables one by one, even on separate connections, cannot mix
  logical moments. Released at COMMIT/ROLLBACK, session drop, explicit
  RELEASE, or lazy timeout expiry. The wire fast paths (SQL OLTP
  interceptor, KV writes) take the same gate. Go consumer surface:
  `Tx.AcquireSnapshotLease/ReleaseSnapshotLease`,
  `Client.SnapshotLease`, with typed `ErrSnapshotLeaseHeld` conflicts —
  live-engine tests in `go/nucleus/snapshot_lease_integration_test.go`.
   Scope note, stated honestly: the gate covers SQL DML/DDL, the
   intercepted fast paths, and (round 5) SELECT-carried specialty scalar
   writes — `SELECT kv_set(...)` and friends now take the gate, since the
   SQL scalar functions are how every SQL client writes KV. Acquisition
   drains write-bearing foreign transactions first and pins the holder's
   snapshot to the ACQUIRE moment (round 5, above). Specialty-model writes
   that route through none of those (RESP-wire direct, streams/CDC appends
   from background tasks) are not lease-gated — a backup of those models
   still relies on their own snapshot/checkpoint paths.

## ORM program findings and remaining boundaries (reported 2026-09-24)

The ORM program's Nucleus defects were originally measured on tree
`3313729a`. X07–X12 repairs and the fresh X13 recording now distinguish
verified bounded contracts from remaining limits. The canonical per-driver
results and source identity are in
`conformance/live/orm/capabilities.nucleus.json`; generated tables and
reproducers are in `conformance/live/orm/ORM_CONFORMANCE.md`.

- **N1 (security) — closed bounded scope.** Transaction-scoped SET LOCAL,
  role reset, rollback/savepoints and session cleanup were repaired in X07.
  X08 additionally supplies implicit simple-message data rollback on the
  server path and explicit COMMIT/ROLLBACK block separation. The old blanket
  non-atomic-message claim is obsolete; transactional DDL and universal
  cancellation/custom-engine parity are not established. set_config remains
  unavailable. RESET ALL also drops the role, a deliberate fail-closed
  PostgreSQL difference.
- **N3 — repaired bounded temporal contracts.** Fresh X13 probes for
  TIMESTAMPTZ explicit offsets and session zones pass on both drivers.
  Ambiguous/nonexistent bare local DST inputs are refused; date infinity
  remains unavailable.
- **N5/N6 — verified bounded column and FK contracts.** Fresh generated,
  identity and deferred-FK probes pass on both drivers after X10, with
  integrated regression coverage. Generated expressions admit only the
  documented scalar/row subset; unverified forms are refused. Deferred
  PRIMARY KEY/UNIQUE remain refused; PostgreSQL 42P17 parity is not claimed.
  DEFAULT VALUES and varchar/char write-length contracts also pass;
  numeric(p,s) typemods remain unenforced.
- **N8/N10/N13 — bounded repairs.** Integer-array result/ANY, JSONB_AGG,
  correlated relational reads and positional derived aliases pass on both
  drivers. Text-array parameters still fail through postgres.js while pg
  passes. Stored multidimensional/interval/vector arrays and overlong alias
  lists are refused. No universal array or scalar-OID parity is claimed.
- **N11 — modes enforced, engine limits retained.** READ ONLY rejects
  writes. Buffered storage supplies READ COMMITTED and explicitly refuses
  REPEATABLE READ/SERIALIZABLE with 0A000; those isolation probes remain
  unsupported. Higher isolation requires MVCC storage.
- **N2/N4/N7/N9/N12 — remaining limits.** DDL catalog changes are not
  transactional. Selected catalog queries pass, but full introspection
  boolean shape, CHECK rendering, regclass output and scalar Text OID 1043
  versus PostgreSQL 25 remain. Advisory locking, LOCK TABLE and SQL sleep/
  cancellation functions are unavailable; the row-lock timeout probe still
  returns XX000 after about 10 seconds. UPDATE FROM/DELETE USING refuse before
  mutation. Numeric range/scale, enum sorting and row comparisons retain
  their recorded limits. None are advertised as PostgreSQL parity.
- **N14/N15/N16 — closed bounded findings in X13.** Fresh X03 checks
  confirm SUM/AVG/MIN/MAX over untyped columnar values all refuse with 0A000;
  typed numeric aggregates remain exact. Documentation correction df718e66
  states attached-WAL/synchronous_commit=on fsync with memory/off and append
  error limits. COLUMNAR_INSERT inside BEGIN refuses before mutation and
  ROLLBACK leaves the fixture count unchanged. Clean restart and SIGKILL
  recovery pass for the tested stores; SIGKILL alone does not prove power-loss
  durability. Columnar-model writes remain outside SQL rollback.
- **Concurrent derived state — bounded physical B-tree repair at X13.** Ordinary
  DML preserves engine-maintained physical postings; controlled before/after
  and structural physical-posting regressions passed. The later 2026-09-30
  repair below adds writer-generation checks, a demonstrated FTS regression
  and zone-map/transaction visibility invariants, and retires encrypted modes.
  These bounded results do not certify universal vector, FTS or zone-map
  scan/publication coherence; a selected passing soak does not do so either.

Advertised family contracts are scoped in
`conformance/live/orm/ORM_CONFORMANCE.md`: relational SQL, KV, documents,
graph, time series, columnar, geo, blob, streams and Datalog retain their
measured limits. CDC/Pub/Sub expose bounded notification or model inspection,
not commit confirmation, replay or exactly-once delivery. pgvector columns
and FTS function probes remain unsupported; native vector/FTS APIs were not
verified by these model legs and are not advertised as verified.

Out-of-repo note: Lullmail's vendored copies of the send.go / bearer-transport
blobs (flagged in neutron-12/13/16 as affected consumers) are NOT fixed here —
that is a separate repo and needs its own sync.

## Cached-function privacy and bounded retention (2026-09-30)

The core `cache()` previously keyed only on a prefix and arguments in a process
map: different functions collided, and authenticated HTTP requests could receive
another request's result. Function identities and Node request-local scopes now
isolate results. Server calls outside a request have no implicit cache; public
process sharing requires `scope: "shared"`. Entry ceilings and complete tag/timer
cleanup bound retention. `request-cache-security.test.ts` checks sequential and
concurrent authenticated requests over HTTP, including within-request
single-flight behavior; `cache.test.ts` covers identity, limits and cleanup.
The request provider, function identities and explicit shared scope are realm-wide
so real SSR and HTTP adapter module graphs share tag invalidation.
`request-cache-ssr.e2e.test.ts` exercises both request deduplication and shared
tag invalidation through the real SSR runtime.

## Durable override failures and retired crypto modes (2026-09-30)

Declared disk-backed columnar and LSM engines now propagate open failures rather
than silently selecting memory storage. CREATE opens its intended storage before
publishing catalog metadata; startup refuses failed override recovery. A real
filesystem obstruction regression verifies refusal and original-row recovery.

Columnar append and checkpoint failures propagate to callers. Operations refused
before WAL publication leave live state unchanged. An I/O failure after encoded
bytes were written can have an uncertain outcome; the engine fences further
reads, writes and durability acknowledgements until reopen. Fault tests distinguish
these outcomes rather than claiming arbitrary-I/O failure atomicity or power-loss
proof.

Detached specialty publication uses writer generations, and readers decline
stale or in-flight images. The FTS regression reproduces a completed concurrent
INSERT missing from indexed results before the fix; generation checks preserve
its authoritative result afterwards. Zone-map and cross-session transaction
visibility tests are additional invariants, not separate demonstrated before
failures.

The encrypted-index prototype exposed plaintext under a chosen-zeroes query.
Public construction and SQL admissions now refuse all legacy encrypted modes;
recovery preserves base rows without rebuilding insecure sidecars. No secure
replacement cryptographic format is implemented or advertised.


Core-only and WASM derived-index generation scopes use poll-scoped thread-local
values instead of requiring Tokio's server runtime. The value is restored after
each poll, including pending, nested and unwinding polls, so suspended embedded
executions do not inherit another future's reader or writer generation. Server
builds retain Tokio task-local scopes. The core-only scope tests exercise
interleaved polls, nesting, panic restoration and cancellation without a runtime.

## Native FTS recovery refusal (2026-09-30)

Native FTS recovery combines a checkpoint with its subsequent WAL tail. A corrupt
or unreadable existing checkpoint, or an unopenable declared FTS WAL, now refuses
persistent executor construction instead of serving an incomplete or volatile
index. `try_new_with_persistence` returns the recovery error; the compatibility
constructor refuses with a clear panic. The server and maintenance opener use
the fallible constructor. Missing checkpoints remain valid for fresh/WAL-only
recovery. Tests reproduce all three unsafe successes before the fix, preserve
the failed files, and verify checkpoint-plus-tail recovery remains writable
across a second reopen. This does not establish power-loss durability.

## Data-protocol responses undeclared variation (2026-10-06, owner-reported)

Reload of a deployed app painted the raw `{"__neutron_serialized__": ...}`
payload as page content. Root cause is NOT the server cache key (TS-04 holds):
the client router's data fetch shares the page URL, and the app-response cache
HIT left the server advertising the framework-synthesized
`Cache-Control: public, max-age=N` with no `Vary`, so the browser HTTP cache
stored the JSON payload under the bare URL and handed it to the next document
navigation. The reload was answered by the browser, never reaching the server.

- TS-33 | FIXED — app responses (both the JSON data protocol and the HTML
  document) declare every representation dimension the server cache keys on
  (`Vary: Accept, Accept-Language, X-Neutron-Data, X-Neutron-Routes`), merged
  with route-declared tokens; stored entries get the same set appended when
  the store path synthesizes shared-cache freshness; the dev-server plugin's
  data and document responses match. Regression:
  `src/server/data-protocol-cache-headers.e2e.test.ts`. Consumers on
  `@neutron-build/core` ≤ 0.2.2 remain exposed until a release carries this
  (0.2.2 predates `c31736eb` entirely: string bodies, substring
  Cache-Control parsing, no keyable-Vary admission, variant key without
  origin/Accept-Language/data-header dimensions).

## Neutron audit pack Pass A — NE-*/MJ-* (2026-10-07, branch `audit/pack-ne-mj`)

Source: the 193-item implementation plan at
`.audits/2026-10-06/neutron-audit-plan/` (pinned `cfa7eefe`, no fixes
applied by the audit). Pass A covers its NE-01..NE-31 and MJ-01..16 /
MJ-K01..K22 clusters (68 items). This section records the pass's work;
two cancelled prior attempts left +1,366/−91 in the tree which this pass
inventoried file-by-file before continuing (all inherited work verified
sound and kept, except two deliberate FAIL-BEFORE probes in
`executor/policy.rs` which this pass completed).

Status vocabulary as above. Every fix carries a fail-before/pass-after
regression unless marked otherwise.

### Engine — transaction/policy correctness

- NE-03 | FIXED — policy COMMIT's three-way merge compared a staged catalog
  cloned at the session's FIRST policy write against a baseline cloned at
  BEGIN; a peer change landing between those moments was classified as this
  transaction's delta and COMMIT reinstalled the peer's outdated entry.
  Baseline and staged copy are now captured together (one clone under one
  read lock, split). Regression:
  `test_policy_merge_moments.rs::peer_tightening_between_begin_and_staging_survives_commit`
  (+ drop variant).
- NE-04 | FIXED — policy publication cloned under a read lock, merged with
  no lock, and installed under a different write lock (two committing
  sessions could each discard the other's delta); failure compensation
  restored the whole pre-publish catalog after an await during which
  another session committed. Clone+merge+install now run under ONE write
  guard (lexical block — the guard never crosses an await) and
  compensation is generation-CAS scoped (`policy_generation`). Regressions:
  `peer_drop_between_begin_and_staging_survives_commit`,
  `concurrent_disjoint_policy_commits_both_survive` (64 barrier rounds ×
  2 threads); masking DDL failure path generation-scoped in
  `masking_ddl.rs`.
- NE-05 | FIXED — RLS comparison guessed numeric semantics from rendered
  strings (`code > '2'` admitted '10' on TEXT; f64 rounding moved exact
  numeric boundaries past 2^53). Domains are bound at CREATE POLICY time
  from the catalog column type (`ColumnDomain::{Heuristic,Text,Numeric}`;
  `bind_column_domains`), TEXT compares lexically, numerics compare exact
  decimal (sign/width/digit-wise, exponent forms fall back), and reloaded
  pre-fix policies keep their recorded Heuristic semantics via serde
  defaults. Regression: `test_rls_typed_domains.rs` (lexical TEXT,
  string-identity IN, >2^53 boundary both directions, decimal scales).
- NE-06 | FIXED — a NULL literal compiled to AlwaysFalse, so `USING (NOT
  NULL)` granted every row. New `AlwaysUnknown` predicate evaluates to
  TriState::Unknown (never grants, including under NOT); dumper renders it
  as `NULL` which recompiles to the same semantics. Regression:
  `null_predicates_never_grant_even_under_not` (NULL, NOT NULL,
  NOT (NULL OR FALSE) deny; TRUE OR NULL grants).

### Engine — wire/security surface

- NE-08 | FIXED — a two-byte non-ASCII RESP prefix panicked the connection
  task inside `&line[1..]` before auth, permanently consuming one of
  max_connections. Parser validates the type byte against `+ - : $ *`
  before any slice; bulk-string CRLF terminators are verified (hardening);
  every accepted connection holds an RAII slot guard (`ConnectionSlotGuard`)
  whose Drop releases on error/cancel/panic. Regressions in
  `resp/parser.rs` + `resp/server.rs` (multibyte sweep; two-slot server
  survives three malformed connections and answers PING).
- NE-09 | FIXED (panic vectors) — `is_large_object_call` sliced
  `&str[..10]` (`SELECT 'aé'` killed the pgwire task mid-transaction); now
  byte-wise prefix match. Email masking indexed `&local[..1]` (multibyte
  first char panicked) and star-counted by bytes; now char-based. Both with
  regressions. DEFERRED (see continuation): the general session-lifecycle
  guard (cleanup-on-unwind for arbitrary handler panics) — the two known
  reachable panic sites are closed; the guard is defense-in-depth.
- NE-10 | FIXED — the RESP listener always started plaintext on :6379 with
  the SQL bootstrap password. Policy: SQL's TLS acceptor is routed into
  `start_resp_server_with_config` whenever TLS is on; on non-loopback binds
  RESP starts only when TLS-protected AND password-gated, else refused with
  an explicit error (override: `NUCLEUS_RESP_INSECURE_AUTH=1`); loopback
  keeps the dev default. AUTH attempts bounded per connection
  (5, independent of pgwire). Regression:
  `test_auth_attempts_are_bounded_per_connection`; loopback gate has
  compile+config-matrix coverage via `main.rs` wiring.
- NE-11 | FIXED — requested mTLS silently downgraded three ways (CA + TLS
  off; CA + half-set cert/key falling back to self-signed; CA-only warning
  without a verifier). `setup_tls_with_client_ca` fails closed on every
  inconsistent form and the generated-certificate path builds a real
  client-CA verifier. Handshake-level regression:
  `mtls_acceptor_rejects_certless_handshake` (real CA-signed client
  admitted, certless client aborted). DEFERRED (see continuation): carrying
  require_tls/client_cert into `on_startup` so a plaintext Startup packet
  is refused before authentication when mTLS is the policy.
- NE-13 | FIXED — a presigned PUT (SignedHeaders=host) plus an UNSIGNED
  `x-amz-copy-source` flipped the upload grant into an arbitrary
  CopyObject. The copy-source header family must be covered by
  SignedHeaders before dispatch. Regression:
  `s3_unsigned_copy_source_cannot_hijack_put` (presigned + header-signed
  refusals, destination untouched, signed positive control).
- NE-15 | FIXED — a multibyte character across byte 8 of `x-amz-date`
  panicked `verify` before signature checking, leaking a gateway slot. The
  complete timestamp parses/validates first; the gateway uses the same RAII
  slot guard as RESP. Regressions: hostile-date unit matrix + S3 server
  guard.
- NE-16 | FIXED — delimiter pagination emitted the CommonPrefix label as
  the continuation token, and raw keys inside the group compare greater
  than the label, so `max-keys=1` re-listed `a/` forever. Resume skips
  every key in the emitted group; max-keys=0 reports not-truncated.
  Regression: `s3_delimiter_pagination_advances` (advancing tokens, full
  coverage, no duplicates, probe + control pages).
- NE-12 | FIXED — the S3 gateway's head reader used unbounded `read_line`
  for the request line and every header, checking the 64 KiB cap only
  AFTER allocation: an unauthenticated peer streaming one line without LF
  grew the buffer without bound (process OOM) and the request line was
  never inside any budget. One aggregate head budget now covers the
  request line + all headers, each line read through `take(remaining+1)`
  so bytes are bounded as they arrive (unterminated-at-budget rejected
  before more allocation); a 256-line header-count cap; duplicate
  Content-Length values that disagree, or Content-Length combined with
  any Transfer-Encoding, are refused as ambiguous framing. Regressions in
  `s3/http.rs` (duplex-fed): overlong unterminated request line and single
  header, 300 short headers, conflicting/ambiguous lengths, mid-header
  EOF, ordinary-head control.
- NE-19 | FIXED — the pgwire accept loop pushed every connection task into
  a JoinSet and never joined: completed entries accumulated for the
  server's whole lifetime (memory growth under connection churn) and a
  panicked handler's result was never observed. The select loop now reaps
  through a guarded `join_next` branch (disabled while the set is empty so
  an empty JoinSet cannot starve accept), logging panics/errors and
  continuing; shutdown drain order unchanged. No dedicated regression
  (needs the binary's accept loop extracted); verified by inspection and
  build.

### Engine — S3 durability boundary

- NE-14 | FIXED — S3 mutators answered 200 while the SQL side refused
  writes under the same degraded state, and no payload/WAL durability was
  forced. Every mutator (create/delete bucket, PUT, COPY, DELETE, batch
  delete, multipart initiate/upload-part/complete/abort) now runs through
  one gate: read-only admission BEFORE any state change (503; GET/HEAD/list
  keep working), then the ordered barrier — payload segment fsync BEFORE
  manifest WAL group-sync — with failures surfaced (503, never 200).
  Multipart completion atomicity is stated in code: one manifest swap;
  part-manifest deletion after the swap can never remove live chunks.
  Regression:
  `read_only_server_refuses_s3_mutations_but_serves_reads` (every mutator
  503 + reads 200 + recovery). The pack's injected mid-write payload/WAL
  fsync fault is not covered by a test injection (needs a durable blob
  fixture; the barrier calls the same sync paths the WAL/buffer tests
  exercise).

### Engine — storage: WALs, LSM, audit

- NE-22 | FIXED — KvWal/ColumnarWal reopened appends BEHIND an unrepaired
  torn tail: the append and its fsync acknowledged, yet the next recovery
  stopped at the tear and omitted them. Open now repairs (truncates to the
  last complete record, durably) before the append handle exists — replay
  reports the true record-boundary prefix (kv's Stop path and columnar's
  framing no longer count partial parses as valid) — and a KvWal append
  that fails after bytes may have moved fences the log until reopen (a
  pre-write injection stays retryable, preserving the edge-triggered
  MISCONF contract). Regressions: byte-by-byte torn-tail corpora for both
  WALs (`torn_tail_is_repaired_*`), `poisoned_log_refuses_further_appends_until_reopen`.
- NE-20 | FIXED — an LSM compaction input whose value could not be read
  back (I/O error, gone file, checksum mismatch) was merged AS A TOMBSTONE
  through the lenient read floor, written durably into the replacement run,
  and the original files deleted: a transient read error became permanent
  data loss. Compaction merges through a fail-closed `entries_checked()`
  and aborts with the tree state and every input file untouched. Point
  lookups keep their documented lenient floor. Regression:
  `corrupt_input_aborts_compaction_instead_of_deleting_data` (checksum
  flip → Err with byte-identical inputs → heal → compact).
- NE-21 | FIXED-PARTIAL — input-file retirement errors were silently
  ignored (`let _ = remove_file`), so a "successful" compaction could leave
  a superseded input live. Deletion failures now propagate with the file
  named. DEFERRED (see continuation): the crash-window half — a crash
  between final-level input deletion and reopen can resurrect a flushed
  deletion because tombstones are dropped at the final level and all
  surviving `.sst` files are authoritative; the real fix is a committed
  live-run manifest (format change).
- NE-26 | FIXED — B-tree insertion never enforced MAX_KEY_SIZE; an
  oversized first key entered split logic with no right-hand entry and
  panicked instead of returning KeyTooLarge. Checked before any page is
  touched. Regression: 257/4096/16349/65536-byte keys refused without
  mutation, MAX_KEY_SIZE boundary accepted.
- NE-27 | FIXED — disk-recovery state transitions used load-decide-store
  sequences: a monitor that had read the pre-operator state could clear a
  just-installed operator hold (`resume_if`'s unconditional swap) or store
  its DiskWatermark over it (`enter_read_only`'s plain store), re-admitting
  writes the operator had frozen, or downgrading the reason so a later
  recovery cleared it. Both transitions are now single compare-and-swap
  loops — the operator-priority check happens INSIDE `enter_read_only`'s
  CAS loop and `resume_if` clears only the exact reason it observed.
  Regression: `concurrent_disk_monitor_transitions_never_clear_an_operator_hold`
  (stress: interleaved monitor/operator transitions; operator hold survives).
- NE-28 | FIXED — an audit event larger than the whole file cap was written
  anyway and rotation carried a copy into every retained file, defeating
  the total bound. Oversized events are refused with InvalidInput BEFORE
  any rotation/write/accounting. Regression:
  `oversized_events_are_refused_without_mutation` (keep=0/1/4, control-char
  expansion, exact boundary accepted).
- NE-29 | FIXED — a torn final audit record (crash/partial write) let the
  next record append mid-line: acknowledged yet unparseable; a tail ending
  inside a UTF-8 character made `read_all` drop EVERY event in that file.
  Open repairs the tail (truncate past the last complete line, durably)
  before appending; a mid-line write failure fences the sink until reopen;
  `read_all` reads bytes and splits on newlines (lossy per line).
  Regressions: full truncated-prefix corpus (incl. mid-codepoint and
  mid-escape), `read_all_survives_a_multibyte_torn_tail`. The mid-write
  injection fault itself has no test hook (fence covered by inspection).
- NE-30 | FIXED — WAL archiving deleted the live segment after a copy that
  was never fsynced (file or directory), and `archive_active` returned
  Ok(true) after rotation's best-effort archive error. The archive copy is
  now fsynced before its rename and the archive directory after it;
  same-size archive copies are verified by CONTENT (torn/corrupt twins are
  replaced); `archive_active` re-verifies the sealed segment durably
  archived and propagates failure; `truncate_before` keeps the live
  segment on any archive failure (as before). Regressions:
  `archive_active_fails_when_the_archive_is_unwritable`,
  `a_corrupt_same_size_archive_copy_is_replaced_not_trusted`,
  `truncate_before_keeps_the_live_segment_when_archiving_fails`. The
  pack's owner-run power-loss filesystem tests remain owner work.

### Engine — backup/PITR

- NE-17 | FIXED — an approved forced restore deleted the destination and
  then copied into it: a halfway copy/rename failure destroyed the old
  database. Restores build the complete image in a staging sibling, fsync
  it, then publish by rename with the old generation retained aside until
  the new one is live and the parent dir is synced (`publish_dir_replace`;
  sync failure propagates and keeps the aside copy). Regressions:
  `a_halfway_copy_failure_keeps_the_previous_database` (legacy-manifest
  fixture + unreadable file), `a_publication_failure_keeps_a_recoverable_generation`
  (read-only parent: old generation at dest or aside). PITR publishes
  through the same helper.
- NE-18 | FIXED — PITR lacked the backup/restore path's overlap fence: a
  forced restore onto the base snapshot's own directory (or an ancestor of
  base/archive) deleted the recovery assets at publication after using
  them; `db_file` joined multi-component/absolute names into the staging
  image. All overlap shapes are refused before any mutation
  (`reject_path_overlap`, message generalized for reuse); `db_file` must be
  exactly one normal relative filename; the destination's directory lock is
  HELD for the whole operation (`DestFence`, undoing what the acquire
  created on failure so a refused restore still leaves the destination
  byte-for-byte unchanged). Regressions:
  `pitr_refuses_destinations_overlapping_the_recovery_assets`,
  `pitr_rejects_non_single_filename_db_file`.

### Engine — page format

- NE-31 | FIXED — the table-directory overflow payload began at byte 4 of
  each overflow page, overlapping the flush-stamped checksum (4..8) and
  LSN (8..16): every WAL log/flush of an overflow page corrupted the first
  12 payload bytes and reopen misread/lost directory entries. v3 overflow
  layout places the payload past the common header (chain pointer stays at
  0..4 — never stamped); `DB_FORMAT_VERSION` bumped to 3; pre-v3
  databases WITH an overflow chain are refused with a named error (their
  payload may already be corrupt — migrating would launder damaged bytes);
  directories without overflow upgrade transparently as before. Physical
  snapshots remain format-locked through the same constant. Regressions:
  `table_directory_overflow_survives_flush_stamp_and_reopens` (800 tables,
  full flush path, two reopens, first pages stable),
  `pre_v3_overflow_directory_is_refused`.

### Mojo (uncompiled — see note)

NOTE: no Mojo toolchain exists on this machine (`mojo` not found; the pack
installed none). The four fixes below are each a provable by-inspection
one-line/one-constant correction against the two signatures involved;
everything else in the MJ clusters is DEFERRED with that stated reason.
They are UNVERIFIED BY COMPILER AND TESTS — the continuation pass must run
`mojo test` before landing them.

- MJ-02 | FIXED (uncompiled) — FP16 subnormal decode initialized the
  normalization exponent at −1, halving every nonzero subnormal
  (0x0001 → 2^−25); e now starts at 0 (`io/binary_reader.mojo`).
- MJ-16 | FIXED (uncompiled) — the CSV parser dropped a trailing empty
  field (`byte_length() > 0` guard); the final field is appended whenever
  the line held any delimiter or content, preserving zero-field blank
  lines (`data/csv_reader.mojo`).
- MJ-K12 | FIXED (uncompiled) — `TensorView.reshape` used the two-argument
  constructor, resetting the storage offset to 0 so reshaping a contiguous
  slice silently re-based it onto the tensor start; it now passes
  `new_shape.strides()` and `self._offset` (`tensor/view.mojo`).
- MJ-K17 | FIXED (uncompiled) — the standalone transformer block called
  `swiglu(gate, up)` while `swiglu(x, gate) = silu(gate)·x`, swapping the
  FFN operands vs the Model/SIMD paths; the call is now
  `swiglu(up, gate)` (`nn/transformer.mojo`).
- MJ-01..15 (excl. 02/16), MJ-K01..K22 (excl. K12/K17) | DEFERRED —
  substantive algorithm/training/kernel work (weight-reader validation,
  GGUF layouts, DLPack contracts, scheduler admission, autograd tape
  semantics, e-graph rebuilds, randn/streaming boundaries). Reason: no
  Mojo toolchain on this machine; blind-editing numerical kernels without
  compilation or tests would be unverifiable. Continuation entry below.

### Deferred remainder (precise)

1. NE-01 (P1) — autocommit page images inherit a buffered transaction's WAL
   identity on unrelated pages (`buffer.rs`/`buffered_engine.rs`/
   `disk_engine.rs`). Needs the per-page WAL owner + apply-window gate
   design from the pack; the existing attribution test
   (`another_sessions_write_is_not_attributed_to_the_applying_txn`) is the
   seed to extend.
2. NE-02 (P1) — a failed buffered COMMIT leaves its applied prefix visible
   (`buffered_engine.rs`). Needs before-image apply contexts restored on
   pre-decision failure; executor-level undo (NU-02/03) does not cover the
   storage-level apply path.
3. NE-21 remainder (P1) — LSM live-run manifest so a crash mid-retirement
   cannot resurrect final-level tombstone-dropped deletions.
4. NE-23/24/25 (P2) — buffer-pool async flush ownership, io_uring short
   write accounting, failed-load frame reclamation. Note the pack's own
   caveat: the inspected DiskEngine constructor never selects the async /
   io_uring backends (file/directory path mismatch), so these are public
   API defects without demonstrated server exposure; fix or document.
5. NE-09 remainder — session-lifecycle guard (cleanup on unwind/cancel).
6. NE-11 remainder — `on_startup` plaintext refusal under a client-cert
   policy (the pack's required native verification branch).
7. MJ-01..15/MJ-K01..K22 remainder — requires a Mojo toolchain; work from
   the pack's per-item proposed changes in
   `.audits/2026-10-06/neutron-audit-plan/findings.json`.

### Pass A gates

nucleus `cargo test --lib` green (full suite; count in DATABASE_COMPLETION
updated via `scripts/metrics.sh --check`, now 0 FAIL), `cargo test --test
s3_gateway` 10/10, targeted suites for every touched module listed in the
per-item regressions above. No web searches were performed; no lookups
beyond the local audit pack and repository. No commits made — tree left
dirty for the orchestrator.

Post-landing verification (same day): the orchestrator landed this pass as
`fa9cc9bb` on `origin/main`. Three fixes present in the diff but missing
from this register on first write (NE-12, NE-19, NE-27) were added as rows
above at that time — the fixed count is 25 NE items (24 full + NE-21
partial), matching the diff. A detached-worktree re-run of the exact commit
reproduced the suite results above (see the pass report).

## Neutron audit pack Pass B — RS-*/TSD-* (2026-10-07, branch `audit/pack-rs-tsd`)

Source: the same 193-item plan (`.audits/2026-10-06/neutron-audit-plan/`,
pinned `cfa7eefe`). Pass B covers RS-01..35 (Rust server runtime,
`rust/crates/*`) + TSD-01..14 (TS data layer: neutron-sql, neutron-nucleus,
neutron-data). STATUS: IMPLEMENTED — FINAL ACCEPTANCE PENDING. All 49 findings have source
fixes; validation results and remaining infrastructure gates are recorded below.

Reconciliation vs landed work: `git diff cfa7eefe..HEAD -- rust/` is EMPTY —
the NA wave's job fencing landed in `go/neutronjobs`, and Pass A touched only
`nucleus/` + `typescript/packages/{neutron,neutron-sql(ast/builder/compile),
create-neutron,neutron-cli}`. None of the 49 Pass B findings' files were
modified, so all are verified on their merits against current code (no
"already-fixed" classifications so far; details per item below).

Status vocabulary as above. Targeted regressions are included; final aggregate
validation is in progress. This continuation did not replay a clean baseline.

### Rust — credential/protocol leaf fixes (implemented in this pass)

- RS-13 | IMPLEMENTED — Stripe webhook HMAC keyed the MAC with a base64-DECODED,
  prefix-stripped secret (`whsec_` removed), so real Stripe signatures never
  verified while self-generated fixtures (using the same helper) passed. The
  verifier now uses the FULL signing-secret string as key bytes and feeds
  timestamp-bytes + "." + raw payload bytes (no UTF-8 round-trip); empty
  secrets are refused. Regressions: an independently generated fixture
  (Python `hmac`, the stripe-node algorithm) verifies; the old decoded-key
  fixture now FAILS (fail-before inverted); empty-secret refusal.
  `neutron-stripe` lib suite green.
- RS-14 | IMPLEMENTED — the shared calendar decomposition (duplicated in
  `neutron-storage/src/sign.rs` + `neutron-jobs/src/cron.rs`) narrowed
  day-of-year to u8 before month conversion (2026-10-07 → signed
  `20260124`) and anchored its 400-year cycle at 1970-01-01 (1972-12-31 →
  1973-01-01). Both copies replaced with Hinnant civil-from-days (wide
  integers until month/day derivation). Regressions: 180-entry fixture table
  (independent Python `datetime` oracle; u8 boundary day-of-year 254-258,
  leap/century/400-year edges through 2400) + the concrete audit date +
  a month/day-restricted cron match. `neutron-storage` + `neutron-jobs`
  suites green.
- RS-15 | IMPLEMENTED — OAuth `fetch_userinfo` fallback invented `user.id` from
  the first 16 access-token chars (the shared JWT header for JWT-shaped
  tokens → distinct users conflated), and the built-in GitHub preset shipped
  without `userinfo_url` (so GitHub ALWAYS hit the fallback). GitHub preset
  now points at `https://api.github.com/user`; the fallback fails closed
  with an actionable error. Regressions: all-presets-have-userinfo + a
  fail-closed fallback test. `neutron-oauth` lib suite green.
- RS-16 | IMPLEMENTED — OAuth callback `parse_query` kept percent-escapes literal
  and `exchange_code` re-encoded them (`code=a%2Fb` double-encoded to
  `a%252Fb` → valid logins failed). Now strict form-decoding (`%HH` must be
  hex, `+`→space, UTF-8 validated) with duplicate-field rejection.
  Regressions: `%2F`/`%2B`/escaped-equals decode, truncated/non-hex/lone-%
  rejection, duplicate code/state rejection.
- RS-17 | IMPLEMENTED — WebAuthn registration rejected the standard
  none-attestation statement map: `skip_cbor_value` refused CBOR major
  type 5 even empty, so every ordinary browser registration failed before
  creating a credential. The parser now captures `attStmt` raw via a proper
  bounded recursive skip (all CBOR majors, definite lengths, depth cap);
  `finish_registration` requires fmt=="none" with an EXACTLY empty attStmt
  map (other formats get a distinct `UnsupportedAttestationFormat` error
  instead of silent acceptance), checks the AT flag, and verifies the
  embedded credential ID against the response `id`. Regressions: complete
  browser-shaped happy-path fixture, missing-AT, cred-id mismatch,
  non-none format, non-empty attStmt. `neutron-webauthn` suite green.

### Rust — runtime-correctness leaf fixes (implemented in this pass)

- RS-31 | IMPLEMENTED — the OpenAPI TypeScript client generator emitted
  `name??:` for optional query properties (invalid TypeScript — a `?` was
  appended after the optionality marker) and made REQUIRED query params
  optional. One-`?` semantics now; generated function arguments reflect required
  query fields, optional query arguments before bodies use `| undefined`, and
  URLSearchParams values stringify scalar values. Real `tsc` compiles generated
  required/optional GET and optional-query POST clients. Regression:
  `ts_codegen_query_param_optionality_is_valid_ts` asserts no `??`,
  required `req: number`, optional `opt?: number`. `neutron` openapi
  tests green.
- RS-34 | IMPLEMENTED — the gRPC adapter dropped successful EMPTY messages
  (`Some(msg) if !msg.is_empty()` skipped framing, so an empty protobuf
  response vanished instead of emitting its 5-byte zero-length frame),
  ignored the compressed-flag and trailing frames on unary requests
  (handing compressed/garbage bytes to the app), and wrote `grpc-message`
  trailers without percent-encoding. Now: every `Some(message)` is framed;
  compressed frames fail closed with UNIMPLEMENTED; extra frames are
  rejected; `grpc-message` is percent-encoded per the gRPC HTTP/2 spec.
  Regressions: empty-message framing, compressed rejection, trailing-frame
  rejection, percent-encode table + é message. `neutron-grpc` suite green.
- RS-21 | IMPLEMENTED — metrics histograms double-accumulated: `observe`
  already increments every containing (cumulative) bucket, and render
  re-cumulated on top — finite buckets could exceed +Inf/count. Render now
  prints each bucket counter directly (both duration and size histograms).
  Regression: `histogram_buckets_render_without_double_counting` asserts
  monotonic non-decreasing buckets, every-bucket-once for a below-bound
  value, and finite ≤ +Inf. `neutron` metrics tests green.
- RS-35 | IMPLEMENTED — the tracing middleware held a `span.enter()` guard
  across the `next.run(req).await`: a suspended request kept its span
  entered, so a concurrent request polled on the same thread recorded its
  events into the FIRST request's span (cross-attributed logs). The span
  is now attached with `Instrument` (enter/exit per poll). Regression:
  `interleaved_requests_keep_their_own_spans` — two barrier-interleaved
  requests on one thread, a recording subscriber maps span→trace_id and
  asserts every event lands in its own span. `neutron` full lib suite
  green (695 tests).
- RS-19 | IMPLEMENTED — the inference SSE decoder ran `from_utf8` per transport
  chunk, so a multibyte character split across chunks (legal — SSE has no
  Unicode alignment guarantee) aborted the stream with "invalid UTF-8";
  CRLF separators, `data:` without a space, and multi-line data were also
  rejected. Now: byte-buffered incremental UTF-8 (incomplete tails held
  back), CRLF + bare-`data:` + multi-line data per spec, 1 MiB event
  bound, EOF flush. Regression: split-at-every-byte-offset Unicode event,
  CRLF/bare-data/multiline, invalid-UTF-8 refusal, unterminated-event
  bound. `neutron-inference` suite green.

### Rust — core runtime (implemented in this pass)

- RS-01 | IMPLEMENTED — default request deduplication shared whole HTTP responses
  across callers with the same `METHOD:path?query` key: two authenticated
  users could receive each other's body/headers/Set-Cookie, and waiters
  skipped their own middleware. Now: credential-bearing requests
  (Authorization/Cookie) are never deduplicated under the default key
  (explicit `key_fn` opts back in); a leader response carrying Set-Cookie /
  WWW-Authenticate / any Vary / private/no-store/no-cache is never
  broadcast — waiters run their OWN chain; a `None` sentinel replaced the
  old "leader failed" 500 (waiters run their own chain instead); the
  pending slot is RAII-owned (`LeaderGuard`) so a cancelled leader cannot
  strand later requests on a dead slot. Regressions: two-principal
  isolation (distinct bodies + 2 handler calls), cookie isolation,
  Set-Cookie non-sharing (waiter gets its own cookie), cancelled-leader
  recovery. `neutron` dedup tests green (14).
- RS-08 | IMPLEMENTED — response-cache flight notifications were lost (lookup of
  the Notify followed by a later `notified()` registration missed
  `notify_waiters`, which retains no permit) and a cancelled leader left
  its in-flight entry stranded forever, hanging every later request for
  the key. Flights are now `watch`-channel slots under the SAME lock as
  entry storage (slot removal + completion send are linearized, so a
  waiter can never miss a notification); leadership is RAII-owned
  (`FlightGuard`): cancellation removes the slot and wakes waiters, which
  re-check the cache and may become the next leader. Regression:
  `cancelled_leader_does_not_strand_waiters`.
- RS-09 | IMPLEMENTED — the response cache keyed only `METHOD:path?query` and
  admitted any 2xx. Now the default key includes authority (Host header
  or URI host), Accept, Accept-Language and Accept-Encoding; only full
  **200** responses are stored (206/204 are not full representations);
  ANY `Vary` disqualifies (the key does not encode arbitrary dimensions;
  `Vary: *` is explicitly uncacheable); `Range` requests and requests
  carrying `Cache-Control: no-cache/no-store` (case-insensitive) bypass
  lookup; response `s-maxage`/`max-age` cap the configured TTL.
  Regressions: host/language isolation, Range bypass (origin hit count),
  206/Vary non-storage, mixed-case request no-cache, s-maxage=1s
  expiring despite a 30s configured TTL.
- RS-10 | IMPLEMENTED — invalidation was not fenced against in-flight stale
  fills: a GET that started before a write could publish its pre-write
  snapshot AFTER the write's invalidation, and the stale value stayed
  until TTL expiry. Entries/generations/in-flights now share one lock;
  each fill captures the path generation before loading, and publication
  is generation-checked (an invalidated fill is discarded). All
  CacheHandle invalidators advance the generation. Regression:
  `invalidation_fences_in_flight_stale_fill` (held-open old fill +
  invalidation + resume → next read sees the post-write value).
  `neutron` cache suite green (28) + full lib suite 708/0.
- RS-33 | IMPLEMENTED — a cancelled half-open probe (e.g. an outer `Timeout`
  dropping the request) never cleared `probe_in_flight`; HalfOpen has no
  expiry transition, so every later request returned 503 permanently.
  The probe permit is now RAII-owned (`ProbePermit`: Drop releases), and
  release happens on all paths including cancellation. Regression:
  `cancelled_half_open_probe_does_not_wedge_the_breaker` (408-cancelled
  probe → next request admitted, succeeds, breaker closes). Suite green.
- RS-03 | IMPLEMENTED — a stale request could resurrect a destroyed session:
  the layer saved unconditionally after the handler, so a request that
  loaded a session before another request destroyed it re-created the
  destroyed session under the same signed ID (restoring stale
  authentication), and concurrent writers silently lost updates.
  `SessionStore` requires atomic `load_with_revision`, fenced `save_if_current`,
  and confirmed `destroy_checked` methods for custom stores too; the layer captures the
  revision at load and saves CONDITIONALLY. `MemoryStore` implements
  save/destroy under one lock with monotonic revisions and tombstoned
  destroyed IDs; `RedisSessionStore` implements them with atomic Lua
  (data + `:rev` counter + 7-day `:tomb` fence, standalone Redis;
  legacy records get a revision atomically at load). A refused save returns 503 (no success-with-
  unsaved-cookie). Regression: genuinely concurrent load→destroy→save
  through the layer (paused handler + logout across two clients) gets
  503 and the session stays destroyed; store-level tombstone and
  concurrent-writer fencing tests. Session and live Redis legacy/expiry fencing regressions passed in final checks.
- RS-04 | IMPLEMENTED — session persistence failures were reported as success:
  `SessionStore` was infallible, so MemoryStore capacity rejections and
  Redis save/destroy failures produced a successful login response with
  an unsaved cookie, and logout cleared the browser cookie while the
  authenticated record stayed usable. The layer now uses the checked
  operations: failed save → 503 with NO cookie; failed destroy → 503
  with the cookie INTACT. Regression:
  `capacity_exhaustion_reports_failure_not_success` +
  `memory_store_save_reports_capacity_failure`. Full neutron lib suite
  721/0; neutron-redis 13/13 including live regressions.
- RS-02 | IMPLEMENTED — `flatten_nests` hoisted a nested router's raw fallback
  to the parent, so an unmatched path ANYWHERE invoked the child fallback
  without the child middleware (an auth-protected fallback under
  /private answered /unrelated anonymously) and a missing route inside
  the prefix also bypassed the child chain. The fallback is now compiled
  as a prefix-scoped catch-all (`{prefix}/{*__neutron_nest_fallback}`
  plus the bare prefix when unclaimed) whose handler is the child
  fallback wrapped with the child middleware chain; first-registered
  nest wins for a shared path (previous tie-break preserved). Regression:
  `nested_fallback_is_scoped_and_authenticated` — /unrelated → parent
  404 (not the child fallback body); /private/missing → 401 without
  credentials, fallback reachable with credentials; real routes intact.
  Router suite 87 green; full lib 713/0.
- RS-23 | IMPLEMENTED — Stripe API calls sent the `Authorization: Bearer
  sk_live_…` credential over a RAW TcpStream regardless of scheme (port
  80, plaintext) and failed real HTTPS endpoints. `execute` is now
  scheme-aware: https dials 443 through a verified rustls connector
  (webpki roots, hostname-checked); plaintext http is refused BEFORE any
  connection attempt unless `StripeConfig::allow_insecure_http` is set
  explicitly (local test doubles); unknown schemes are refused.
  Regressions (real sockets): plaintext base refused with NO connection
  attempted; https to an untrusted certificate fails closed with no
  request delivered; https to a test-CA-signed server succeeds with the
  credential observed on the wire. `neutron-stripe` 36/36.
- RS-24 | IMPLEMENTED — the OTLP exporter dialed port 80 with a raw TcpStream
  for EVERY scheme, silently exporting trace attributes in plaintext for
  https endpoints. Same scheme-aware treatment: https through a verified
  rustls connector (443), http kept as the explicit local-collector
  path, unknown schemes refused. Regression: https endpoint against an
  untrusted certificate fails closed, no payload delivered.
  `neutron-otel` 49/49.
- RS-18 | IMPLEMENTED — cancelling `Db::transaction` after BEGIN was sent but
  before its future resolved dropped the ordinary `PooledConn` (recycled
  to the pool), so the next borrower unknowingly executed inside the
  open transaction. Both `neutron-nucleusdb` and `neutron-postgres` now
  arm the transaction guard (Drop takes the client out → connection
  discarded, never re-pooled) BEFORE sending BEGIN, so cancellation or
  BEGIN failure discards the connection. Suites green (99 + 17). The
  transport-stall regression sends BEGIN to a scripted PostgreSQL-wire
  server, cancels while the reply is pending, then requires a fresh pool
  connection (including max-size one and SSLRequest refusal).








### Remaining implementations and TS data layer (final validation in progress)

- RS-05 | IMPLEMENTED — Configured streaming transport ceilings apply to normal/TLS/worker bodies; strict buffering rechecks previously materialized bytes. Transport chunked-body regression and strict-after-broad regression retained.
- RS-06 | IMPLEMENTED — Worker listeners share one connection cap, clone one listener on macOS/Windows, stop admission, own connection tasks, drain then reverse hooks, abort/join at deadline, and join worker threads through spawn_blocking (current-thread safe). Held-request and forced-deadline TCP regressions added.
- RS-07 | IMPLEMENTED — Normal/TLS connection tasks are owned/reaped through JoinSet; shutdown wins admission selection; TLS handshakes have a timeout/shutdown branch. H3 owns concurrent requests and connection tasks, sends GOAWAY, drains/aborts, runs reverse hooks, and bounds transport teardown in the same deadline.
- RS-11 | IMPLEMENTED — Persistent workers continuously poll configured/named queues as capacity becomes available; enqueue wakes admission, delayed jobs/backlogs/retries remain discoverable. Regression includes 10,001 jobs over two workers.
- RS-12 | IMPLEMENTED — Every durable execution follows an atomic claim. Memory/PG/Redis claims carry monotonic tokens; complete/fail/retry require current token and running state. Custom JobStore transition APIs require tokens. External effects remain at least once.
- RS-20 | IMPLEMENTED — Tower round-trips preserve owned framework request context, including state/extensions/remote/upgrade metadata. Layer service is constructed once, failed readiness and body rejection stop dispatch. Identity/context, construction count, oversized body and failed-readiness regressions added.
- RS-22 | IMPLEMENTED — Redis response cache rejects credentials, HEAD, range, private/no-cache/no-store, Vary, cookies, non-200 and streaming bodies. Public representation keys include authority/Accept dimensions; entries have a v2 namespace, size cap, and response freshness cap. Live principal/Host isolation and policy regressions added.
- RS-25 | IMPLEMENTED — Redis job state/hash/index changes are one Lua transition; claims and terminal/retry/recovery writes are token fenced, with index-type preflight and exhausted-attempt handling. Standalone Redis support is explicit.
- RS-26 | IMPLEMENTED — Redis Script invocation reloads after NOSCRIPT; sequence counters expire with buckets. Backend admission is configurable and defaults to fail closed. Disposable Redis SCRIPT FLUSH regression exercises recovery.
- RS-27 | IMPLEMENTED — WS inbound queue is bounded to 16 messages with a 1 MiB whole-message budget, including fragments. Independent reader/writer tasks preserve partially read frames across outbound writes; dropping halves aborts owned tasks. Sends acknowledge actual write completion. Real partial-frame/outbound regression added.
- RS-28 | IMPLEMENTED — GraphQL negotiates graphql-transport-ws, multiplexes operation streams, drops canceled idle streams, handles ping/pong, duplicate IDs/init and initialization deadline. Real socket test covers simultaneous operations, idle cancellation, ping and confirmed protocol-close transmission.
- RS-29 | IMPLEMENTED — Owned Request extraction and Clone factory bounds let exported GraphQL/OAuth factories mount directly. Consumer integration tests compile and dispatch documented factory shapes, including subscription transport.
- RS-30 | IMPLEMENTED — Advertised optional features explicitly activate required dependency/module edges; all five examples declare their required features (Cargo metadata validated). Full/http3/Tower combinations compiled in the all-feature tests. Standalone feature-matrix acceptance remains pending and is tracked separately from workspace feature unification.
- RS-32 | IMPLEMENTED — OTLP periodic worker flushes low-volume traffic, retains bounded failed batches, accounts for overflow, and provides explicit shutdown/final flush. Cancellation returns the owned batch; HTTP IO driver remains inside the bounded export future; initial tracing fields are recorded. Timer/outage/cancellation/shutdown/attribute regressions added.
- TSD-01 | IMPLEMENTED — Decoder rejection stops cursor ownership before terminal completion, preserving the original error and releasing the transaction/connection.
- TSD-02 | IMPLEMENTED — Capability checks on pinned sessions use session-local executors and savepoint-safe probes; cold single-connection transactions no longer query their occupied outer pool.
- TSD-03 | IMPLEMENTED — Postgres queue claims one execution slot at a time; attempt/worker/active/lease fences protect acknowledgements and renewals, including reused worker IDs. Job payloads bind as JSON objects rather than double-encoded JSON strings.
- TSD-04 | IMPLEMENTED — Native BullMQ unknown-name jobs are durably deferred through delayed disposition; they no longer complete without a handler, including across restart.
- TSD-05 | IMPLEMENTED — Raw/mobile SELECT caching defaults off; caching requires explicit pure-read assertion. Pre-abort checks and write/transaction generation fences protect opted-in reads; actual scalar INCR/SETNX mutators dispatch each time.
- TSD-06 | IMPLEMENTED — Dispatched mutations/BEGIN are not automatically replayed. UnknownOutcome identifies ambiguous responses; offline queue retains only known-undispatched writes. Only caller-asserted pure reads retry.
- TSD-07 | IMPLEMENTED — Cancellation uses an independent channel with immediate rejection observation and target/cancel drain on all outcomes. Pool/PID waits recheck abort. Live PostgreSQL saturates eight main connections and proves cancellation and later reuse.
- TSD-08 | IMPLEMENTED — HTTP signal/deadline covers response body parsing and error bodies; default is 30 seconds, explicit zero disables. Unit and real stalled-body regressions added.
- TSD-09 | IMPLEMENTED — Removed unfenced destructive blob compensation. Store/tag remains explicitly non-atomic; partial metadata errors propagate without deleting a newer writer.
- TSD-10 | IMPLEMENTED — Rollback preflights the persisted newest-first frontier, local migration presence, verified checksum and down SQL before DDL; missing newest/intermediate files cannot skip to older down scripts.
- TSD-11 | IMPLEMENTED — Listener acquisition/close is serialized and generation checked; closing during subscribe releases late subscriptions and prevents post-close notifications/resources.
- TSD-12 | IMPLEMENTED — Redis increment/anchored expiry is atomic Lua. Nucleus TTL increments require an atomic backend primitive; absent support is refused before mutation rather than implementing unsafe split operations.
- TSD-13 | IMPLEMENTED — Cron scheduling arms bounded timers and coalesces missed work into one catch-up after forward/backward clock changes.
- TSD-14 | IMPLEMENTED — Legacy DDL selectively defers cyclic foreign keys and emits canonical driver-aware defaults, including JSON values; known serializers are shared with typed profiles.

Parent-reviewed release fixture correction was applied separately to
`typescript/packages/neutron/src/core/static-gate-preflight.test.ts`; it preserves
malformed-array/undefined-export security cases and public runtime types. It is
not a Pass B finding.

Framework request-cache SSR investigation remains owned by Pass C. The full core
TS test run timed out in its existing real-SSR request-scope test; this test does
not exercise neutron-sql/data/nucleus and was not edited in Pass B.


### Final acceptance checkpoint

Formatting and diff whitespace checks passed. Final durable-job integration tests
passed 8/8, including real PostgreSQL/Redis and the 10,001-job two-worker case.
The all-feature Rust workspace run passed 720 core unit tests and 32
contract/integration tests before UI trybuild exhausted host disk; aggregate
exit 101. Workspace clippy also failed while writing compiler metadata with
ENOSPC. Neither gate is represented as passing. Latest-source libraries compiled: 721 core tests and the audited library suites
passed, and the corrected PostgreSQL/Nucleus DB suites passed 19/19 and 101/101.
Cancellation tests also exposed malformed empty/escaped pool fields: both
connection-string serializers now quote those values correctly, with real
tokio-postgres parser regressions preserving SSL mode and field identity.
Direct execution of the already compiled binaries passed 13 Redis tests including
live regressions, plus all GraphQL/OAuth consumer tests. New heavy compilation
and standalone feature-matrix validation are paused by resource steering.

Nucleus TS unit/build/lint passed (450 passed/15 gated skips); real PostgreSQL
passed 487/510 with 23 engine-specialty skips. The shared engine instance is
owned by another lane and the fixed global specialty fixtures lack safe
isolation, so an owned disposable final-source engine is required for that
acceptance coverage. Data targeted final queue tests passed 23/23 after native JSON binding correction,
and timer/BullMQ tests passed 11/11. SQL aggregate snapshot completed 992 passed/4
failed: two disk-exhaustion fixtures and two timezone assertions. Corrected
targeted reruns passed 6/6 and 2/2; final finite-profile transaction evidence
compile passed and 18/18 targeted profile/db-scope tests passed with the guard
unchanged. These corrected checks do not make the failed aggregate green.

The parent-reviewed fixture-only release patch was applied after git apply
--check; core build passed. It is a release fix, not a Pass B finding. Core's
request-cache-ssr test timed out in a 646-pass/1-fail/1-skip run; this framework
request cache test is outside the data layer and is handed to Pass C without
additional source edits.

## Neutron framework audit NA-01..NA-14 (2026-10-06, GPT pass — wave-2 fixes)

Full report: `.audits/2026-10-06/gpt-reports/neutron-full.md` (the originally
extracted `neutron.md` was truncated at NA-05; the complete 69,011-char report
was recovered from the source conversation). Every finding was re-verified
against `82cb9e28` before fixing; each fix carries a fail-before/pass-after
regression run against a clean HEAD worktree and a live PostgreSQL 17.

Defects fixed (all verified+implemented on branch `audit/na-fixes`):

- NA-01 | FIXED (P1) — every job terminal write (complete/retry/fail/shutdown-
  release) is fenced by a fresh per-claim `claim_token` (`status='running' AND
  claim_token=$n`, rows-affected checked, reaper clears the token); renewal is
  token-fenced, expiry-bounded (`lease_expires_at > NOW()`), and a
  confirmed-through watchdog cancels the handler at the last acknowledged lease
  boundary, so renewal ERRORS no longer extend a handler's lifetime past its
  lease. Same-worker-ID reclaim is explicitly covered by tests. Regressions:
  `go/neutronjobs/queue_integration_test.go`
  (StaleAttemptCannotMutateNewerClaim, LostLeaseCancelsHandler,
  RenewalErrorsBoundHandlerLifetime). Rollout note preserved in code: stop old
  workers before enabling token enforcement — old binaries still issue
  ID-only writes.
- NA-02 | FIXED (P1) — `Process` now runs exactly `concurrency` worker loops,
  each claiming at most one job when ready to execute it; a claim may no
  longer be parked without a heartbeat while waiting for a slot. Regression:
  ClaimRequiresCapacity (B stays `pending`/attempts 0 while the only slot is
  busy).
- NA-03 | FIXED (P2) — `RecoverLegacyRunning(ctx)`: explicit cutover migration
  that backfills an expired lease for NULL-lease running rows (attempts
  unchanged), handing them to the ordinary reaper; NOT run per-boot. Regression:
  LegacyNullLeaseRecovery (reaper ignores them before, recovery applies
  retry/dead-letter policy after, live claims untouched).
- NA-04 | FIXED (P1) — `parseRouteFacts` now parses the module with
  @babel/parser (all declarators incl. destructuring, export specifier names
  incl. string literals, runtime `export *` conservative-true, unparseable →
  conservative-true; regex false-negatives are gone). New shared
  `assertStaticRoutesUngated` (core, exported) checks ACTUAL module exports
  across the layout chain and gates BOTH the standalone renderer and the
  production `neutron-cli build` static loop before anything is written.
  Regressions: `client-tier.test.ts` (9 spellings), `static-gate-preflight.test.ts`,
  existing server-side prebuilt-artifact tests retained.
- NA-05 | FIXED (P2) — `Router.gen` is one shared `*atomic.Uint64` across
  Group trees; route-record append/snapshot and the sites map are behind a
  shared registry mutex (race-detector-verified); `/openapi.json` resolves the
  current spec per request with generation-keyed cached bytes; `OpenAPIJSON`
  remains a public snapshot helper; user-owned `/openapi.json` still wins.
  Regressions: `go/neutron/openapi_generation_regression_test.go`.
- NA-06 | FIXED (P2) — shared `core/route-path.ts` token module;
  `bindPathParams` re-binds parameter names from the WINNING route's pattern
  after trie selection (scratch map no longer escapes); duplicate/wildcard
  positions rejected at insertion. Regressions: `route-param-regression.test.ts`
  (both insertion orders, suffixed/catch-all, backtracking, null-prototype).
- NA-07 | FIXED (P2) — not-found scopes match structurally via the shared
  binder (`findNotFoundMatch`), selection is by structural specificity
  (static>param>wildcard, suffix length, stable id tie-break — never
  parameter-name length), and `matchNotFound` returns the scope's params.
  Regression: `/org/[orgId]/not-found` receives `orgId=acme`; deeper/static
  scope precedence; scope stays non-navigable.
- NA-08 | FIXED (P2) — mixed re-export stripping preserves the original
  statement's prefix and suffix verbatim (source clause, attributes,
  semicolon); string-literal export names are recognized. Regressions in
  `server-only.test.ts` (parse-the-output assertions).
- NA-09 | FIXED (P2) — destination selection is a batch decision driven by
  the TABLE count (`planOutputDestinations`): column count can no longer
  create a directory named `models.go`; multi-table batches get distinct
  destinations, duplicate/explicit-file misuse is rejected with an
  explanation; writes go through temp-sibling+rename. Regressions:
  `cli/cmd/generate_output_test.go`.
- NA-10 | FIXED (P2) — `generateStructs` emits `encoding/json` when any column
  maps to `json.RawMessage`; regression type-checks the generated source with
  the real toolchain (`go/neutroncli/typecheck_test.go`).
- NA-11 | IMPLEMENTED (API improvement) — exported `Server.CallTool(ctx,
  name, args, Principal) ToolResult` (the permission-aware dispatcher; HTTP
  `callTool` is now a shim over it) and exported `ToolResult` (private alias
  retained). External-package tests verify principal context, scope/readonly
  refusal, unknown tool, in-band errors, canceled/nil context, and HTTP-vs-
  in-process parity: `go/neutronmcp/server_external_test.go`.
- NA-12 | FIXED (P2) — `excluded()` builds a semantic `excluded-ref` node
  (ordinary `qual("excluded", …)` references are ordinary identifiers and
  compile/execute — PG really allows a table/schema named `excluded`); the
  validator carries lexical visibility instead of resetting it per subquery:
  scalar subqueries in DO UPDATE SET/WHERE inherit the binding, matching
  PostgreSQL 17 (live fixture `SET body = (SELECT excluded.body || '-seen')`
  → `after-seen`, executed through both drivers). Genuine unbound uses still
  fail before SQL. Regressions: `neutron-sql/src/excluded-scope.test.ts` +
  corrected `conflicts.test.ts` scope case.
- NA-13 | FIXED (P3) — `toPathType` uses `JSON.stringify` for static paths
  (double quotes escape correctly), keeps literal suffixes on dynamic tokens
  (`:id.json` → `` `/${string}.json` ``), and the navigable set is decided by
  the `isNavigableRoute` predicate (no not-found phantom entries, no
  absolute-path `_layout` substring matching). Regressions in
  `route-param-regression.test.ts` incl. a Babel parse of the generated
  declaration.
- NA-14 | FIXED (P2) — `enumerateTables` fails closed on scan/iteration
  errors for BOTH profiles (a partial catalog read is never success); the
  timeout derives from `cmd.Context()`; the legacy path stages the whole batch
  before publishing. Regressions: injected-cursor tests in
  `cli/cmd/generate_output_test.go`.

Retained protections re-verified on this pass (already fixed; no new fix
needed, per the audit's downstream-suspicions table):

- Router JSON-404 passthrough (neutron-23):
  `TestApplication404PassesThroughUntouched` / `...405AndHtml404...` pass.
- Mid-path `[param]` discovery: `manifest.test.ts`
  (`api/runs/[id]/decide.tsx` → `/api/runs/:id/decide`) passes.
- Session regeneration atomic rotation:
  `TestSQLSessionAtomicRevocationRotationAndCAS` (live PG),
  `TestStaleSessionCannotResurrectAfterRevoke`,
  `TestMemorySessionRevisionRotationAndABA` pass.

Suites at the end of this pass: Go `go/` all packages green (jobs + neutron
also under `-race`; the five `go/nucleus` engine-specific tests that require a
Nucleus engine fail identically at HEAD when pointed at plain PG — pre-existing,
env-gated); `cli/` green; TS neutron suite 647 passed / 0 failed / 1 skipped
(request-cache-ssr is the separately tracked flaky pre-existing finding);
neutron-sql 531 unit + 64+ live PG tests green. The one known pre-existing
failure was not chased, per the open item.

Left open from this report: none of NA-01..NA-14 remain unfixed. The report's
"build into staging and publish atomically" hardening for `dist/` (NA-04) and
per-file-rename→whole-directory staging (NA-09) were implemented at the
per-file level only; full atomic-directory publication remains unimplemented
and unrequested.

## Neutron audit pack Pass C — TS-*/NF-* FINAL cluster (2026-10-07, branch `audit/pack-c`)

Third and final pass over the 193-item neutron audit pack (`.audits/2026-10-06/
neutron-audit-plan/`, pinned `cfa7eefe`, dated 2026-10-07 UTC). Pass C covers
the 76 remaining items: TS-F01..21 (TS runtime/build/middleware gating),
NF-NR-01..18 (NeutronWind styling + native runtime device/gesture/platform
adapters), NF-STUDIO-01..14, NF-DESK-01..07, NF-NATIVE-01..06, NF-GAP-01..09
and NF-CI-01. Base: `3380c650` (NA wave + TS-33 + Pass A + Pass B all landed).

Execution note: three earlier Pass C attempts (worktrees `neutron-final-c-{ts,
studio,platform}-20261007`, base `fa9cc9bb`) were killed mid-run by infra
restarts with substantial uncommitted work. Their trees were salvaged (293
files ported with zero textual conflicts; Pass B's only overlap was
`static-gate-preflight.test.ts` and lockfiles, both clean), then every item
was re-verified against current code, completed where unfinished, and
re-tested. NF-NR-01..17 had no prior work and were implemented fresh.

### Reconciliation against earlier landings

- TS-F01 overlaps NA-04 (landed production middleware gate): NA-04 gated
  per-route/layout middleware exports at both static pipelines; the pack adds
  the GLOBAL-middleware case (a `src/middleware.*` file refuses to prerender
  static routes at all) — implemented here as an extension, not a duplicate.
- TS-33 (representation Vary) and TS-04 (variant cache key) were already
  landed before the pack's base; the pack's cache items (TS-F06/07/08) build
  on that state and were verified against it.
- No Pass A/B item overlapped the NF-* clusters.

### Per-item status

TS cluster (typescript/): all 21 fixed. TS-F01/02/03 — global-middleware
static gate in both `build.ts` and `render-static.ts`; generated runtime
boots reject invalid global middleware exports (`normalizeMiddlewareExport`
throws, import injected only when a middleware file exists); Docker adapter
serves ONLY producer-declared `publicArtifacts` from a separate
`.neutron-public` directory with symlink/escape rejection (runtime bundles
no longer reachable). TS-F04/06/07/08 — cache capture separated from
publication: reads and stores happen only after the full middleware chain
completes; middleware and `headers()` callbacks must opt in via
`sharedCacheSafe: true` (safe-by-default: absent flag bypasses shared
caching); request `no-cache`/`no-store` bypass reads; full Accept values are
keyed; body capture is byte-budgeted THROUGH the stream (`cache-capture.ts`)
with bounded concurrent cache operations and quarantine for stores that miss
their atomic deadline (`cache-publication.ts` + `AtomicCachePublication`
opt-in contract). TS-F05 — loader fills fence through mutation completion in
the generated runtime (fail-before/pass-after on literal percent paths).
TS-F09 — `mutableResponse()` wrapper; middleware/handlers mutate real
headers. TS-F10..12 — security package: rate-limit buckets no longer evicted
by quota reset, CSRF origin comparison is scheme-aware, trusted-proxy mode
no longer prioritizes unverifiable vendor identity headers. TS-F13 —
docker/vercel stream writers honor backpressure and abort. TS-F14 —
backslash escapes blocked via the shared `normalizePathname`. TS-F15 — body
cap propagates cancellation and adapter transport context. TS-F16/17 —
client stale-data fencing after async boundaries; client matcher implements
server catch-all semantics (route-parity tests). TS-F18 — returned loader
Responses close their observability spans. TS-F19/20 — live collections
executed with ownership fencing (root-key canonicalization fixes
`/var` vs `/private/var` invalidation misses); MDX `sanitize:true` refuses
renderFactory. TS-F21 — ServerIsland JSON escaping.

Studio cluster: all 14 fixed (NF-STUDIO-01..14 — wire decode, commit-completion
staged-edit preservation, keyboard-repeat duplicate inserts, deep-link
robustness, specialty mutation draft/error handling, document-tree property
paths, namespace-vs-item identity, history-failure isolation, import
preview ordering, prototype collisions, placeholder allocation, schema
designer stale metadata, exact JSON numbers, vector SQL interpolation).

Desktop cluster: all 7 fixed. NF-DESK-01 — biometrics requires a real LAContext
evaluation (policy + localizedReason; no confirmation-dialog stand-in).
NF-DESK-02 — updater authenticates artifacts (signature/size budget) and
installs only the app-owned authenticated stage; persistence is explicit.
NF-DESK-03 — default storage namespaced per app identifier with migration
of legacy shared paths. NF-DESK-04 — the fetch bridge re-issues Requests
preserving method/body/headers/signal (`rebuildRequest`; the ported draft's
`new Request(url, requestObject)` treated the Request as an ignored init and
silently flattened it to GET — the exact defect class). NF-DESK-05/06/07 —
window builder forwards advertised config; fs/notifications report
unsupported instead of no-op success; the dev HTTP bridge requires a dev
token and drops wildcard CORS. NOTE: Rust fixes compile and all 25 cargo test
groups pass + clippy 0, but no macOS/Windows native build was run locally —
biometric/updater paths need a device/toolchain confirmation (flagged).

Native cluster: all 6 fixed. NF-NATIVE-01/02 — OTA client verifies manifests
before download and refuses unauthenticated updates; crash recovery uses a
durable native boot-state store (new `native/modules/neutron-ota` iOS/Android
modules with store + process acceptance fixtures). NF-NATIVE-03 —
TurboModule registry feature-detects `get()` on JSI proxies (no assumption).
NF-NATIVE-04 — Stack/Tabs/Drawer register their screens; navigation is
state-driven through the shared navigator (resetRoot publication + screen
matching). NF-NATIVE-05 — file discovery never invokes components; lazy
loaders wrapped via `lazyRoute` (loader called by React, not discovery).
NF-NATIVE-06 — fallback animated styles subscribe to shared values (covered
by the RN-fallback hook layer + real-React node tests). NOTE: OTA native
modules need Xcode/Gradle runs that don't exist locally — compile-checked by
source review + fixtures only (flagged).

NF-NR cluster (all 18 fixed): NF-NR-01/02/03 — the NeutronWind Babel plugin
was rewritten: only zero-expression templates fold statically; dynamic or
mixed class lists emit ONE explicitly imported `resolveClassName` call over
the FULL original expression (no free `__nw` identifier, evaluation counts
preserved); an existing style prop merges caller-wins with Pressable
function-style preservation; tokens are Node-safe (no react-native import,
screen tokens runtime-only, `leading-*` pairs with a text size into absolute
lineHeight, `numberOfLines`-as-style removed); the Rspack loader parses
TS+JSX itself; the package ships a real dual ESM/CJS build with a verified
exports map. NF-NR-04 — Animated hosts resolve from the SAME provider as the
hooks (Reanimated hosts when installed, RN Animated hosts otherwise;
entering/exiting/layout throw without the peer). NF-NR-05 — RNGH adapter uses
`maxDistance` (not the nonexistent `maxDist`), forwards
simultaneousWith/requireExternalFailure through the provider graph, and
normalizes numeric RNGH states to the string contract. NF-NR-06 — the
PanResponder fallback is a deliberately limited adapter: disabled detectors
never claim touches, Simultaneous over two continuous gestures throws
explicitly, pinch re-baselines at pointer transitions and accumulates (end
RETAINS values), pan/fail offsets and fling direction enforced, long-press
timers cleaned via component lifecycle. NF-NR-07 — SecureStore backend keeps
a durable app-owned key index; `clear()` deletes every indexed key or
rejects with `IncompleteClearError(remainingKeys)`; legacy keys merge
explicitly. NF-NR-08 — camera/location adapters never manufacture permission
grants (platform permission API or undetermined; provider denials reach the
caller; image-picker errorCode handled before didCancel). NF-NR-09 —
notifications route through checkNotifications/requestNotifications on every
platform in single AND batch shapes. NF-NR-10 — web camera stops all tracks
and detaches srcObject on every exit path, awaits a usable frame, video
capture is explicitly unsupported, gallery object URLs are caller-revocable.
NF-NR-11 — web scheduling rejects repeat as unsupported, validates
dates/delays, cleans fired timer records, surfaces constructor errors;
notifee triggers use typed enums. NF-NR-12 — the clipboard listener fetches
text via getStringAsync after relevant contentTypes events, generation-fenced
against removal. NF-NR-13 — the RN→CSS bridge maps axis shorthands,
transforms (per-function units) and length semantics, warns and drops
native-only values. NF-NR-14 — Link uses Linking.openURL for external native
links (accessibilityRole link); disabled web links block the browser
navigation too; Switch is keyboard-operable with aria-disabled; TextInput
normalizes events/labels. NF-NR-15 — Platform detection never invents a host
('unknown' instead of android fallback; documented HermesInternal marker;
macos/windows included in isNative); capability probes documented as
diagnostics. NF-NR-16 — `initOTA`/`disposeOTA` exported through the barrel;
`useOTA` subscribes via useSyncExternalStore; replaced clients are
generation-fenced. NF-NR-17 — Android FocusTrap takes explicit
`backgroundRoots` (hidden while active, restored on release, nested traps
compose); without them it is documented as a modal-content hint only.
NF-NR-18 — the 68 web/module tests and component smoke tests now exercise
the shipped implementations (real react-test-renderer path + registry-level
web tests against actual module objects).

Assurance cluster: NF-CI-01 — the duplicate `services:` keys in the CLI
workflow's `test` job (present at HEAD; GitHub rejects the whole workflow)
removed, passwords aligned to the surviving pgvector block; 38 workflows
strict-parse clean. NF-GAP-01 — schema-shape CI classifies both previously
unclassified invalid fixtures. NF-GAP-02 — SECURITY.md no longer advertises
"Email SOON": it states honestly that no monitored destination is
provisioned and gives the exact enabling steps (OWNER-gated: run the
`gh api --method PUT .../private-vulnerability-reporting` step and update
the doc). NF-GAP-03 — quint CI runs ALL 182 declared scenarios (manifest-
driven, seed-pinned) plus invariant simulation; NF-GAP-04/05/07 — history/
predicate oracles and production-trace conformance added (Rust conformance
tests incl. `production_wal_trace`); NF-GAP-06 — lean4 spec names
strengthened with a property registry + audited axiom output (NOT compiled —
no local Lean toolchain; canary fails closed, flagged). NF-GAP-08 — quint/
lean/verus canaries run positive controls and reject unrelated tool
failures as success. NF-GAP-09 — Verus deferred templates no longer carry
true postconditions (semantic mutant controls; not compiled locally —
flagged).

### Suites at the end of this pass

- typescript: `neutron` tsc clean, vitest 701 passed / 0 failed / 1 skipped
  (was 647 at Pass B); security 53/0; cli 138/0; ops 31/0; otel 16/0.
- studio: vitest 954/954 (74 files) + `tsc --noEmit` + vite build green.
- native: jest 453 passed / 1 skipped (38 suites, two projects) +
  `scripts/test-platform-host.cjs` 27/27 (real React + happy-dom) +
  `pnpm -r typecheck` clean; styling package builds ESM+CJS with verified
  exports.
- desktop: `packages/desktop` vitest 107/107; `cargo check`/`test` 25
  suites green; `cargo clippy` 0 warnings.
- quint: `scripts/ci.sh` fully green (typecheck + simulation + 182
  scenarios + Rust conformance); canary PASS.
- lean4/verus: python-side tests green; toolchain-gated checks fail closed
  locally (no elan/verus installed).

Flagged for follow-up (not closable locally): lean4 elaboration + verus
verification need their toolchains; desktop biometrics/updater and the OTA
native modules need Xcode/Gradle device runs; the SECURITY.md private
reporting destination is owner-gated.

Left open from this pack: none of the 193 items remain unaddressed (Pass A:
NE-01..31 + MJ; Pass B: RS-01..35 + TSD-01..14; Pass C: the 76 above). The
pack's five platform-blocked scopes remain blocked_incomplete per its own
coverage census — device/browser/native release suites were not exercised
beyond the explicit tests listed here.

## 2026-10-07 — ADM-11 private shell command stdin

Source implementation validated in isolated `audit/final-nucleus-stdin` patch
(base `fa9cc9bb`), pending parent review/landing. `shell --command-stdin` is
advertised by help, conflicts with `-c`/`--command`, validates UTF-8 and reads
through EOF before connecting, then passes SQL bytes unchanged to pgwire.
Embedded NUL is refused because pgwire SQL uses a terminated string. JSON
applies to both one-shot modes; plain shell retains its REPL. Stdin command
errors use fixed diagnostics so server/reader errors cannot quote private SQL.

Evidence: existing binary suite 6 passed (including parser, byte preservation,
and partial-read refusal); `scripts/test-shell-command-stdin.py` passed actual
subprocess/pgwire capture and a live in-memory Nucleus KV round trip preserving
literal whitespace, Unicode, newlines, and semicolons. Persistent test startup
on this nearly-full host correctly enters read-only at the standard disk
watermark; the command fixture intentionally uses `--memory`. No engine
storage or authentication semantics changed. Metrics and formatting checked.

Teploy CLI negotiation remains consumer-owned: its existing positive
`shell --help` probe for `--command-stdin` returns before `ExecInput` when
unsupported; there is no argv fallback or new capability protocol. Image
release/version/digest, registry publication, and consumer pin remain separate
parent-owned acceptance steps; this source validation does not assert them.
