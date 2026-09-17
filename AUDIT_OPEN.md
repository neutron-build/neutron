# Open audit items

Unresolved findings for this repository from the ChatGPT-led audit series.
Read this before treating related work as done; update it when you close,
defer, or upstream-report an item.

Open items: **1 deferral cluster** (see the table) plus **2 consumer-reported
items** (see the last section) — every other finding of
the 2026-09-17 pass is fixed, partially fixed with the remainder scoped, or
recorded as a false positive with evidence.

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
| TS-07 | FIXED-PARTIAL — monotonic epoch fences in-process fills and loader writes are GET/HEAD-only; for async EXTERNAL cache stores the fence is best-effort (check-then-set is not atomic across a network boundary) | acf405c0 + c31736eb |
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
| TS-22 | FIXED-PARTIAL — DELETE bodies covered; actual-byte streaming enforcement belongs to the server adapter (the middleware cannot substitute the Request body) — the file documents this and keeps Content-Length as the early check | 67ff9c6b |
| TS-23 | FIXED — origin comparison includes scheme+port; safe methods uppercased; docs show header submission (form-body token support deliberately not added) | 67ff9c6b |
| TS-24 | FIXED — pull-based streaming with backpressure, cancel propagation, single lock release; first-chunk guard failure cancels the reader | acf405c0 |
| TS-25 | FIXED — layout loaders receive the full matched params | acf405c0 |
| TS-26 | FIXED — loader errors discriminated by property presence (throw null/0/"" render the error path) | acf405c0 |
| TS-27 | FIXED — 405+Allow for unsupported methods and mutations on action-less routes; custom 404 rewrites only 200s | acf405c0 + c31736eb |
| TS-28 | FIXED-PARTIAL — fail-closed pre-upgrade `authorize` hook with 5s deadline added; full transport-middleware/built-in separation is a pipeline redesign | c31736eb |
| TS-29 | FIXED — drain HTTP before SSR teardown, all stages run on failure (AggregateError), failed imports retried, real bound port reported | c31736eb |
| TS-30 | FIXED-PARTIAL — Vary: Origin on allowed/disallowed/no-origin paths. Security-header case-merge half is FALSE-POSITIVE: `Headers.set` replaces case-insensitively, so a lowercase user override applied after the uppercase default always wins (last write, verified in Node) | 67ff9c6b |
| GO-01 | FIXED — binding goes through the generic input; pointer inputs bind, JSON null rejected | 4f743588 |
| GO-02 | FIXED — destination-width parsing with field-specific 400s; named string elements; non-finite floats rejected | 4f743588 |
| GO-03 | FIXED-PARTIAL — single-JSON-value enforcement and DELETE binding; an http.MaxBytesReader at the HTTP boundary is server configuration left to deployments | 4f743588 |
| GO-04 | FIXED — JSON and problem+json marshal before committing headers | 4f743588 |
| GO-05 | FIXED-PARTIAL — InvalidValidationError surfaces as 500; rejected raw values no longer echoed. Nested field paths keep the json-tag name (Namespace paths would change the public error shape) | 4f743588 |
| GO-06 | FIXED — credentialed wildcard CORS panics at construction; origin entries validated | 4f743588 |
| GO-07 | FIXED-PARTIAL — Vary: Origin on every path; only genuine preflights (Origin + ACRM) intercepted. The preflight response still echoes the configured method list rather than validating the request's requested method against it | 4f743588 |
| GO-08 | FIXED — strict q-value parsing; explicit gzip;q=0 never compresses | 4f743588 |
| GO-09 | FIXED-PARTIAL — lazy header commitment on final headers, Content-Length removed at commitment, bodyless/range/encoded/upgrade/no-transform skipped, no gzip trailer during panic unwinding. A panic AFTER the first compressed byte still cannot be answered in-band (needs recovery/compress connection-abort coordination) | 4f743588 |
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
| GO-24 | FIXED — rate/burst validated at construction, SplitHostPort client IP, Timeout documented cooperative | 4f743588 |
| NU-01 | CONTAINED — GC never compacts the row vector: dead versions are neutralized in place (payload freed, identity retained), so WAL/index/mutation addresses stay stable. The durable fix (monotonic 64-bit version IDs) is DEFERRED as an on-disk format redesign | a0732f5c |
| NU-02 | FIXED — savepoints are O(1) marks into a per-transaction undo journal; rollback replays post-mark ops in reverse, preserving identity and duplicate multiplicity | a0732f5c |
| NU-03 | FIXED — rollback writes WAL compensation records (Insert/Delete/Update under the same txn id — existing formats), so replay applies the rollback exactly when the outer transaction commits | a0732f5c |
| NU-04 | FIXED — corruption (CRC/length/tag/decode) fails startup with the byte offset and leaves the file untouched; torn FINAL frames are the explicit accepted case (prefix recovered, tail dropped by the next compaction) | 6033d56d |
| NU-05 | FIXED — commit validates under the serializable commit-point lock, durably decides (WAL commit + fsync, S63 marker on the same sync), then publishes; a WAL failure aborts cleanly with nothing ever visible | a0732f5c |
| NU-06 | FIXED — auto-txn drop guards, begin/abort reordering (logical cleanup cannot be skipped by a WAL failure), drop_storage_session aborts the registered transaction | a0732f5c |
| NU-07 | FIXED — multi-row auto-commit statements log real Begin/records/Commit WAL transactions; failures write Abort so replay excludes the prefix | a0732f5c |
| NU-08 | DEFERRED — one atomic CommitV2 record carrying the enlistment id is a WAL format change (new tag, old-reader rejection policy); the two-record same-fsync window is documented | — |
| NU-09 | FIXED — indexed equality returns every visible match with per-candidate key recheck; the early break is gone | a0732f5c |
| NU-10 | FIXED — aborted-owner tombstones are CAS-reclaimable from the observed value; committed tombstones still conflict | a0732f5c |
| NU-11 | FIXED — empty predicate reads register the table (both fast paths), and table-level writes leave a marker the read direction discovers; granularity is table-level (conservative, as the audit's containment prescribes) | 3e8afcd9 |
| NU-12 | FIXED — fast COUNT declines inside any explicit transaction | a0732f5c |
| NU-13 | FIXED — integer-vs-integer comparisons are exact; no f64 coercion above 2^53 | a0732f5c |
| NU-14 | FIXED — cached idx.map rows are candidates resolved through a fresh snapshot per version index; the copy is never the authority | a0732f5c |
| NU-15 | FIXED-PARTIAL — unknown type tags and trailing bytes rejected (fail-closed). Lossless parameterized type descriptors (vector dims, array elements, UDT names) DEFERRED as a WAL format change | 6033d56d |
| NU-16 | FIXED — conditional updates/deletes carry first-observed expected rows (and constraint sets) through the buffer and re-check atomically at apply; mismatches are write conflicts. In-memory buffer only — no on-disk format touched | 344090ac |
| NU-17 | FIXED — in-transaction index reads decline or merge the union of committed candidates, all overlay updates (including rewrites newly matching the key), and buffered inserts | 344090ac |
| NU-18 | FIXED — table overlays model CREATE/DROP: dropped tables read as gone, created tables inherit no committed rows, drop-then-create starts empty | 344090ac |
| NU-19 | FIXED — RELEASE drops the named savepoint and its descendants; unknown names error | a0732f5c |
| NU-20 | FIXED — vacuum propagates index-rebuild failures (observer committed first) and reports 0 for unmeasured bytes instead of transaction-status counts | a0732f5c |

## Open deferrals (decisions needed, not work items)

1. **MVCC stable 64-bit version IDs** (durable half of NU-01/NU-09/NU-14):
   on-disk WAL format redesign — new ID space in every record, recovery
   validation, checkpoint migration. Product decision.
2. **Atomic cross-model commit record** (NU-08): one checksummed record
   carrying txn + enlistment ids; old-reader version rejection policy.
   Product decision.
3. **Lossless WAL schema codec** (NU-15 remainder): versioned type
   descriptors replacing the one-byte tag. Product decision.
4. **WebAuthn library integration** (GO-16): new dependency + ceremony/persistence API.
5. **Versioned/CAS session store contract** (TS-19, and the fenced half of
   GO-20/GO-21): public API redesign across the TS and Go SDKs.

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

## Reported by consumers, open (2026-09-17)

Found by teploy-observe's live-engine verification during its 2026-09-17 audit
close-out (its detailed upstream ledger is local to the Teploy umbrella,
`Teploy/_internal/UPSTREAM_BUGS.md`); recorded here so Neutron sessions see
them without that folder:

- **Migration-ledger TOCTOU** — concurrent `Migrate` callers race the
  `appliedVersions`→INSERT sequence and lose with `duplicate key ... (version)`
  (SQLSTATE 23505) at `go/nucleus/migrate.go:88`; reconfirmed 2026-09-17
  against the original 2026-08-26 report. Consumer workaround in place: run
  Nucleus-backed suites serially (`go test -p 1`).
- **No cross-table consistent-snapshot boundary** — capability gap, not a
  defect: no snapshot/lease API lets a consumer establish a point-in-time view
  or mutation-blocking lease across tables (teploy-observe audit F45 — its
  backup dump reads related tables independently and can mix logical moments).

Out-of-repo note: Lullmail's vendored copies of the send.go / bearer-transport
blobs (flagged in neutron-12/13/16 as affected consumers) are NOT fixed here —
that is a separate repo and needs its own sync.
