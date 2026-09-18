# Open audit items

Unresolved findings for this repository from the ChatGPT-led audit series.
Read this before treating related work as done; update it when you close,
defer, or upstream-report an item.

Open items: **2 deferral clusters** (5 and 6 below — WebAuthn and the
versioned/CAS session store; both need external-library or public-API
decisions). Everything else is fixed, partially fixed with the remainder
scoped, or recorded as a false positive with evidence. The 2026-09-17
round-2 re-verification repaired all five disputed closures and resolved
the twelve new findings it raised; round 3 closed its partials; round 4
(2026-09-18, below) closed the four WAL-format deferral clusters and the
consumer-reported snapshot-capability gap under the founder-ratified
"WAL format v2 + snapshot lease" direction.

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
| TS-07 | FIXED-PARTIAL — epoch advances at mutation START and again at COMPLETION (round-2 dispute repaired); loader fills re-check a generation fence before publishing; for async EXTERNAL cache stores the fence is best-effort (check-then-set is not atomic across a network boundary) | r2 |
| TS-08 | FIXED — segment-based traversal matching the serving path; invalidation matches exact cache-key path fields (no /user sweeping /users) | c31736eb |
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
| TS-19 | DEFERRED — revocation fencing needs a versioned/CAS session-store contract; that is a public API redesign (SessionStorage interface, store schema), not a patch | — |
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
| GO-16 | DEFERRED — a complete WebAuthn registration ceremony requires a full CBOR/WebAuthn library integration (new dependency + store/API redesign). FinishRegistration carries an explicit unsafe-scaffold warning until then | — |
| GO-17 | FIXED-PARTIAL — RP ID hash, minimum authData length, UP flag verified; failed counter persistence fails the login. Full ceremony validation rides the GO-16 deferral | 086e0253 |
| GO-18 | FIXED — token-prefix identity fallback removed; verified userinfo endpoint required; numeric subjects decode with UseNumber; empty subject rejected | 086e0253 |
| GO-19 | FIXED — bounded owned HTTP client, inbound-context outbound requests, trimmed body classification, limit+1 truncation rejection, >=32-byte state secrets, query-preserving authorization URL | 086e0253 |
| GO-20 | FIXED — rotation/revocation failure replaces the response with 503, suppresses body and cookie; onCommitError retained for observability | 086e0253 |
| GO-21 | FIXED — detached cleanup bounded at 5s; informational 1xx no longer finalizes the session | 086e0253 |
| GO-22 | FIXED-PARTIAL — optional server-side HMAC token signing (defeats sibling cookie injection), POST-body-only form fallback, any-unsafe-verb coverage. __Host- cookie prefix not made default (breaking change for existing sessions) | 086e0253 |
| GO-23 | FIXED — errors.Join preserves cancellation, bounded detached rollback context, isolation allowlist before SQL splicing, overflow-safe jitter | 086e0253 |
| GO-24 | FIXED — rate/burst validated at construction, SplitHostPort client IP, Timeout documented cooperative. The capacity defect round 2 found under GO-24 is fixed as GO-27 | 4f743588 |
| NU-01 | FIXED (durable half, round 4) + CONTAINED (in-memory half) — GC never compacts the row vector: dead versions are neutralized in place, so WAL/index/mutation addresses stay stable. The durable half landed with WAL v2: stable 64-bit version ids in every record, per-table id floors, identity-preserving baselines, replay validation. In-memory compaction is now UNBLOCKED and deliberately not implemented | a0732f5c + r4 |
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
| GO-29 | FIXED (round 2 + round 3) — the whole Migrate/MigrateDown operation (history reads included) is serialized by an in-process gate, closing the appliedVersions→INSERT TOCTOU behind the consumer's 23505. Cross-process runners are now serialized too, by the `_neutron_migration_lock` ledger claim landed with Consumer-1 (round 3) | r2 + r3 |
| GO-30 | FIXED (round 2 + round 3) — plans are copied and prevalidated before any SQL runs (round 2); applied history records a sha256 checksum over version/name/Up, enforced on every run, with legacy rows baselined from the current plan on first new-version run (round 3). History schema carries a nullable `checksum` column, upgraded in place via `ADD COLUMN IF NOT EXISTS` | r2 + r3 |
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
5. **WebAuthn library integration** (GO-16): new dependency + ceremony/persistence API.
6. **Versioned/CAS session store contract** (TS-19, and the fenced half of
   GO-20/GO-21): public API redesign across the TS and Go SDKs.

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

## Reported by consumers, open (2026-09-17)

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
  (`NEUTRON_TEST_DATABASE_URL`).
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
  Scope note, stated honestly: the gate covers SQL DML/DDL and the
  intercepted fast paths; specialty-model writes that neither route through
  the executor dispatch nor match the two fast paths (RESP-wire direct,
  streams/CDC appends from background tasks) are not lease-gated — a backup
  of those models still relies on their own snapshot/checkpoint paths.

Out-of-repo note: Lullmail's vendored copies of the send.go / bearer-transport
blobs (flagged in neutron-12/13/16 as affected consumers) are NOT fixed here —
that is a separate repo and needs its own sync.
