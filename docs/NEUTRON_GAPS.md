# Neutron gaps found while building on it

Bugs, missing pieces, and rough edges hit while building real products on the
Neutron stack. Each entry says what was hit, where, and what it cost — so the
fix can be prioritised by actual pain rather than by guess.

Add to this whenever building on Neutron makes you work around something.
An entry is worth writing even if you worked around it: the workaround is the
evidence.

**Status key:** OPEN · FIXED · WONTFIX (with reason)

---

## FIXED

### Rust Nucleus client had no table-attached FTS
**Where:** `rust/crates/neutron-nucleus/src/models/fts.rs`

The client only spoke the doc-id sidecar API (`FTS_INDEX`, `FTS_SEARCH`), which
returns `(doc_id, score)` pairs rather than rows — not joinable, not
filterable, not covered by row-level security. The table-attached index shipped
in `1bb99cc` and the client never caught up.

Fixed: added `create_index`, `drop_index`, `matches` (`@@`), and `bm25`, which
return real primary keys from real tables. The document-store methods stay for
corpora with no table behind them and for fuzzy search, which the
table-attached index does not yet expose; the module doc now says which to
reach for.

Table and column names interpolate into DDL, where bind parameters are not
allowed, so identifiers are validated and rejected rather than quoted — quoting
a hostile identifier still lets it terminate the quote. Tested against the
obvious escapes.


### neutron-oauth could not redeem a refresh token
**Where:** `rust/crates/neutron-oauth/src/token.rs`
**Hit while:** building the mail connector, which needs long-lived provider access.
**Cost:** would have made every OAuth integration die at the first token expiry.

`TokenResponse` parsed `refresh_token` but nothing could redeem one — there was
only `exchange_code`. Fixed in `c06d027`: added `refresh_access_token`, which
also carries the existing token forward when a provider omits it on refresh
(Google issues one only on first consent, so a naive implementation drops the
credential), and maps `invalid_grant` to a distinct `RefreshRejected` error
rather than a confusing missing-field parse failure.

---

## OPEN

### English stemmer: singular and plural of the same noun never match
**Where:** `nucleus/src/fts/mod.rs`, `pub fn stem`
**Severity:** HIGH — silently wrong search results across a huge class of words.

The stemmer applies its rules as a mutually exclusive if/else chain, and the
`-er` comparative rule fires before the plural rule can be reached for the
singular form:

```
"numbers" -> ends with 's'  -> plural rule  -> "number"
"number"  -> ends with "er" -> -er rule     -> "numb"
```

So the two forms of one noun stem to different terms and never match each
other. Reproduced directly over the wire:

```sql
CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT);
INSERT INTO t VALUES (1, 'Quarterly numbers'), (2, 'the number four');

SELECT id FROM t WHERE body @@ 'numbers';  -- {1}      correct
SELECT id FROM t WHERE body @@ 'number';   -- {2}      misses row 1
SELECT id FROM t WHERE body @@ 'numb';     -- {2}      proves "number" -> "numb"
```

The `-er` rule is meant for comparatives ("faster" -> "fast") but is applied to
every word of five or more characters ending in `-er`, which is an enormous set
of ordinary English nouns: **user, server, folder, order, customer, member,
header, provider, filter, owner, number, partner, manager**. For mail search
alone that breaks folder/folders, order/orders, customer/customers.

The `-ly`, `-ed`, and `-est` rules in the same branch have the same shape and
are worth auditing together (`-ed`: "seed" -> "se"; `-ly`: "reply" -> "rep").

A real Porter/Snowball implementation applies measure conditions (only strip a
suffix when the remaining stem has enough syllables) rather than a bare length
check. Either adopt `rust-stemmers`, or gate each rule on a measure function.

**Worked around** in `mail/store.go` with a characterisation test
(`TestIntegrationSearchMatchesOnWordsNotSubstrings`) that pins current
behaviour and fails loudly once this is fixed.

### `nucleus start` has no `--config` flag
**Severity:** low, but it blocks automation.

`src/config/mod.rs` has `Config::load(path)` and `from_toml`, and the config
struct covers real knobs — `disk_readonly_free_pct`, `disk_min_free_mb`,
`buffer_pool_size_mb`. But `nucleus start` exposes none of it and never reads a
config file, so the only way to change a documented setting is to edit source
and rebuild.

**How this was hit:** the dev machine's disk fell below the 3% watermark and
Nucleus correctly went read-only. The documented fix is "raise
`storage.disk_readonly_free_pct`", and there was no way to do that. Worked
around with `--memory`, which meant the store integration suite validated the
executor but never the disk storage path.

### Self-referential symlink in the repo root
**Where:** `Neutron -> /Users/tyler/Documents/Code Projects/Neutron`
**Severity:** low, but it breaks tools.

An untracked symlink at the repo root points at the repo root, making the tree
infinitely deep for anything that walks it recursively. Shows up as `?? Neutron`
in every `git status`. Almost certainly an accident; deleting it is safe but is
the repo owner's call.

### `target/debug` grows without bound
**Severity:** low, but it took the machine to zero bytes free.

`nucleus/target/debug` had reached **60 GB** — every profile, every dependency,
incremental artifacts, and a separate test binary per `probe_*`, `fuzz`,
`bench`, `compete`, and `stress` target. Nothing is wrong with it as such, but
nothing prunes it either, and it silently became the largest thing on the disk.

Worth either a documented `cargo clean` cadence or a CI job that reports it.

---

## Notes that are not gaps

**Nucleus's disk watermark did its job.** It refused writes at 2.6% free with a
precise error naming the setting to change and the current values. That is
exactly right behaviour; the only gap is that the setting was unreachable (see
above).

**Table-attached FTS works, indexed and unindexed.** An earlier draft of this
file claimed `@@` and `BM25()` were broken over pgwire. They are not. The
checked-out `target/release/nucleus` binary was built the day *before* the FTS
merge, so every query was hitting an engine that predated the feature.

Verified against a freshly built binary:

```sql
SELECT id FROM t WHERE body @@ 'quarterly';              -- works with no index
CREATE INDEX t_fts ON t USING FTS (body);
SELECT id, BM25(body,'quarterly') FROM t WHERE body @@ 'quarterly';  -- works
```

**The lesson is worth keeping:** a stale `target/release` binary is
indistinguishable from a missing feature, and it cost real time here. Anything
testing engine behaviour should either rebuild first or assert the binary is
newer than the feature commit. `nucleus version` reporting a build timestamp
and git SHA would make this self-evident — arguably the actual gap.

**FTS requires an integer PRIMARY KEY for the index, and that is documented.**
`mail_messages` is keyed on `(account_id, id)` — text, because message identity
comes from provider IDs and Message-ID headers rather than being minted
locally. So it gets `@@` matching but no index and no `BM25` ranking. That is
the documented trade, not a defect; worth revisiting only if mail search
becomes slow enough to justify a surrogate key.

## No SDK client retries a serialization failure (found 2026-07-30)

`SELECT ... FOR` a serializable transaction can now fail with **SQLSTATE 40001**
on the shipping engine — that is new. Before R6 the disk engine refused
`SERIALIZABLE` outright, so 40001 only ever came from the in-memory MVCC engine
and effectively never reached an application. Strict 2PL with wait-die means a
younger transaction is killed on conflict, and `lock_timeout` adds **55P03
`lock_not_available`** as a second new error class.

Grepped every SDK (`go/`, `python/`, `typescript/`, `rs/`, `elixir/`, `zig/`,
`julia/`) for `40001`: **zero hits.** No client has retry-on-serialization-
failure logic, and `FRAMEWORK_CONTRACT.md` does not mention isolation levels at
all. Every SDK's transaction helper will surface a retryable conflict to
application code as a hard error.

What each client needs:

- Retry the transaction on `40001` with bounded attempts and backoff. This is
  the contract of the error — PostgreSQL drivers do not do it for you, the
  application layer does, and a framework SDK is that layer.
- Do **not** retry `55P03`. The lock is still held; retrying spins against a
  transaction that is not moving. Surface it with the `lock_timeout` hint.
- Document that `SERIALIZABLE` on Nucleus's disk engine is table-level 2PL, so
  a hot table serializes — see `nucleus/docs/MODEL_SEMANTICS.md`.

**STATUS 2026-07-30 — fixed in Go, Python and TypeScript.** Each now ships
`isSerializationFailure` / `isLockNotAvailable` classification (by SQLSTATE, never
by message text) plus a retry helper with jittered exponential backoff:

- **Go** — `client.WithTx(ctx, opts, fn)` in `go/nucleus/retry.go`. The SDK had
  `Begin`/`Commit`/`Rollback` but no managed helper at all, so there was nowhere
  for retry to live; `WithTx` also guarantees rollback on panic, which matters
  more than usual here because an abandoned exclusive lock blocks every other
  serializable transaction on that table until the session drops.
- **Python** — `with_tx(db, fn, isolation=...)` in
  `python/neutron/nucleus/retry.py`.
- **TypeScript** — `withRetry(fn, opts)` in
  `typescript/packages/neutron-nucleus/src/retry.ts`, composed with the existing
  `sql.transaction(fn, { isolationLevel })` which already owned the boundary.

All three treat **55P03 as non-retryable** — the holder is still there, so
retrying spins against a lock that is not moving — and all three have a test
asserting a lock timeout is attempted exactly once. Backoff is full-jitter
because under wait-die the younger transaction loses every round, so a fixed
backoff can starve it indefinitely.

**Still open:** Rust, Elixir, Zig and Julia clients have no retry helper, and
`FRAMEWORK_CONTRACT.md` still does not mention isolation levels or the retry
contract at all — it should, since it is the document every SDK is written
against.

---

## Client navigation still pays a round-trip on a cold click (open)

Fixed in `d76888e`: the prefetch subsystem never ran, wrote to a cache nothing
read, served post-mutation data as a prefetch forever, and its cache-hit path
could be overwritten by a stale in-flight fetch. With prefetch working, a link
the user hovers or scrolls past is warm before the click, so the common case is
now zero-network.

**What is still true:** a *cold* click — a link never in the viewport, never
hovered, chosen straight from the keyboard, or clicked within 65ms — still
fetches, including for a `mode: "static"` route with no loader whose component
is already in the bundle. On a high-RTT deployment that is 200-400ms of waiting
for a payload whose only useful contents are `__head__` and `__css__`.

**Why this is not a client-only fix.** The obvious change — "if the route is
static, skip the fetch and render from the bundle" — is wrong three times over:

1. **The client cannot tell.** `RouteInfo` in `client/hydrate.ts` is
   `{id, path, module, load, parentId, isLayout}`. There is no `mode` and no
   loader flag, and there cannot be one from the module: `stripServerOnlyRouteModule`
   correctly removes `loader`/`action`/`middleware`/`headers` from the client
   build, so every route looks loader-free in the browser. `mode` lives in
   route config, server-side, and reaches the client only through
   `generateManifestModule` — which `hydrate.ts` does not consume.
2. **`<head>` would go stale.** `applyData` only touches the document head when
   the payload carries `__head__`. Render with empty data and the previous
   page's `<title>`, canonical and OG tags persist. Head is not necessarily a
   build-time constant either: `resolveHeadDocument` takes `request`, `params`,
   `context`, `loaderData` and the CSP nonce. For a docs or marketing site that
   is an SEO regression worse than the latency it saves.
3. **It would skip the only point where middleware runs during SPA navigation.**
   A static route behind an auth middleware currently redirects through the
   `!response.ok -> reload()` path. Bypass the fetch and the client renders
   content the server would have refused. Not a server-side data leak — the
   data still requires the server — but it is a visible-content bypass, and it
   is exactly the kind of thing that must not be traded for latency.

**What it would actually take**, in order:

- Emit per-route `hasLoader` and `mode` into the client route table
  (`generateRoutesModule`, `vite/plugin.ts`) — the discovery pass already
  parses exports for stripping, so the information exists at build time.
- Emit the rendered head for routes whose head is statically determinable, and
  mark the ones that are not (head fn reading `request`/`context`) as
  fetch-required.
- Mark routes reachable by global or route middleware as fetch-required. This
  is the hard one: global middleware can gate anything, so the safe default is
  "any app with global middleware opts every route out" unless the middleware
  declares a path matcher.
- Only then: skip the fetch when the whole chain is loader-free, static,
  statically-headed, and ungated, and the CSS is already loaded.

**Measure before building it.** With prefetch working, the remaining exposure
is only the cold-click case; the win may not justify four build-pipeline
changes and a new class of "why is my head stale" bug. The comparison to beat
is not Next.js — its App Router pays an RSC round-trip on the same navigation —
but Astro, which for a static page does a full document load with zero JS and
is very hard to beat on a cold click.
