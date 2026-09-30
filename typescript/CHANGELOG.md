# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

Planned release: core 0.3.0, CLI 0.3.0, create-neutron 0.1.7, Nucleus SDK
0.2.1 and data 0.2.1. Core-dependent adapters receive patch releases for the
new dependency range. Requires Node.js 22 or later.

- Request-local function caches separate authenticated requests, distinguish
  function identities and bound retained entries. Process sharing requires
  explicit `scope: "shared"`.
- Response and loader cache stores must implement atomic generation-conditional
  publication. Custom server stores without that capability are refused.
- Session middleware requires atomic revision-conditional persistence for save,
  rotation and revocation. Legacy stores fail closed; switching stores requires
  fresh login. See [authentication migration](../go/neutronauth/README.md).
- Migration plans snapshot their primitive inputs before waiting for ownership.
  Adoption validates every checksum before upgrading history metadata. Engine
  limits on transactional DDL remain documented.
- The data package's installed SQL consumer no longer needs optional libsql
  declarations merely to import its default entry point.
- New scaffolds pin the released core/CLI pair rather than older compatible
  ranges. Go scaffolds require Go 1.26 and SDK v0.3.0.

## [core 0.2.3, cli 0.2.4, create-neutron 0.1.6, auth 0.1.4, cache-redis 0.1.3, security 0.1.3] - 2026-09-28

**Requires Node.js 22 or later** (`engines.node` is `">=22"`; it was
`">=20"`) for every package released here.

### Fixed

- **An action or loader returning `Response.json(...)` was served as 200 `{}`
  by `neutron-ts start`.** `@hono/node-server` replaces `globalThis.Response`;
  `Response.json()` still returns a native instance, which failed the
  framework's `instanceof Response` checks and was treated as data.
  `isResponse()` now recognizes both, and every loader/action/middleware
  check uses it. Dev now also serves a Response *returned* from a loader
  directly, as production always has.
- **`neutron-ts start` opened a second listener**: Vite's HMR WebSocket on a
  random port on all interfaces. The production SSR runtime now runs with
  `hmr: false, ws: false`; only the configured port listens.
- **`neutron-ts dev` dropped in-flight requests on SIGTERM** (exit 143).
  Dev now drains like `start` (FRAMEWORK_CONTRACT.md §8): stop accepting,
  finish in-flight requests (bounded at 30s), close Vite, exit 0. A second
  SIGTERM/SIGINT exits immediately, in `start` too.
- **`neutron-ts dev` answered 404 for `GET /health`.** It now serves the same
  §7 body and yields to an app-defined `/health` route, as `start` does.
- **`neutron-ts dev` ignored `server.openapi`.** `GET /openapi.json` fell
  through to Vite's index.html (200 `text/html`). With `server.openapi`
  configured, dev now serves `/openapi.json` and `/docs` with the same
  document and content types as `start` (FRAMEWORK_CONTRACT.md §4), and
  yields to an app-defined route at either path. Unconfigured, dev is
  unchanged.
- **`@neutron-build/auth` read a Response from another constructor as
  session data.** Its four `instanceof Response` guards missed native
  Responses under `@hono/node-server` (and cross-realm ones); a Better Auth
  resolver returning one had its `Set-Cookie` forwarded. The guards now use
  the same brand check as core's `isResponse()`, kept local because every
  core in auth's `^0.2.0` range ships the `instanceof`-only version.
- **The `app` and `full` templates' settings page never showed its saved
  state.** Its action returned `Response.json(...)`, which is sent as the
  HTTP response, so the page showed raw JSON instead of `actionData`. It now
  returns a plain object, and every template's `AGENTS.md` states the rule:
  plain object → `props.actionData`; a Response → sent as the response.
- **Server hardening from the framework audit (core).** Startup fails closed:
  a global-middleware load failure or a required SSR runtime failure now
  rejects `createServer` instead of serving an ungated static server. The
  response cache keys on origin, `Accept-Language` and `X-Neutron-Data` /
  `X-Neutron-Routes`, honors request `no-store` / `no-cache`, checks `Vary`
  against the keyed set, caps the TTL by `s-maxage` / `max-age`, stores
  bodies as bytes with a per-entry budget, and no longer shares an in-flight
  Response between requests (which duplicated its `Set-Cookie`). Cache
  invalidation matches exact path fields (`/user` no longer sweeps `/users`).
  A cache hit still runs auth and rate-limit middleware. Unsupported methods
  and mutations on routes with no action answer 405 with `Allow`. A thrown
  `null`, `0` or `""` from a loader renders the error path. Shutdown stages
  run in order and `close()` drains HTTP before the SSR runtime; proxy trust
  comes from the socket peer, never from `X-Forwarded-For` / `X-Real-IP`
  alone. Sessions, CSRF and CORS: session and CSRF cookies survive
  immutable-header redirects, memory sessions evict correctly and are
  deep-cloned, CSRF same-origin compares scheme, host and port, CORS
  responses always carry `Vary: Origin`, the rate limiter refuses new keys at
  its live-key cap, and input limits cover `DELETE` bodies. Images: source
  paths are realpath-contained, remote fetches refuse redirects and URL
  credentials and read a bounded body, a missing `sharp` is 503 and
  undecodable input is 415 (the raw-bytes fallbacks are gone), and cache
  entries publish atomically.
- **`@neutron-build/core` declared `ws` types it did not install.** Its
  declarations import `WebSocketServer` from `ws`; with `skipLibCheck` off a
  consumer got TS7016. `@types/ws` is now a dependency.
- **`@neutron-build/security` rate limiting bucketed every visitor together.**
  `createRateLimitMiddleware` without a `key` function called
  `resolveClientIp(request)` without the proxy options, which always returned
  null, so one abusive client rate-limited the whole site. The options now
  flow through (`RateLimitMiddlewareOptions` extends `TrustedProxyOptions`);
  behind a proxy, set `trustProxy` or supply `key`. With no trusted IP the
  shared bucket remains the fallback.
- **`@neutron-build/cache-redis` shortened an index's TTL and invalidated
  paths non-atomically.** The index TTL is never shortened, and path
  invalidation is rename-atomic.

### Changed

- **Every package requires Node.js 22 or later** (`engines.node` is
  `">=22"`; it was `">=20"`). Node 20 reached end of life on 2026-04-30,
  and CI runs 22 and 24. `@neutron-build/ops`, `@neutron-build/otel` and
  `@neutron-build/mail` have no other change and are not republished in this
  release; their published `engines` still read `>=20` until their next
  release.

- **`dev` and `start` read `NEUTRON_PORT` / `NEUTRON_HOST`**
  (FRAMEWORK_CONTRACT.md §6). Precedence: `--port`/`--host` > env >
  `neutron.config` `server.port`/`server.host` > default (3000; `start` binds
  0.0.0.0, `dev` keeps Vite's localhost). An invalid port from a flag or the
  environment is a startup error. `dev` now also honours `server.port`/`host`
  from the config, and fails instead of silently moving when an explicitly
  configured port is taken.

- **`create-neutron` pins `@neutron-build/core` `^0.2.3` and
  `@neutron-build/cli` `^0.2.4`** for scaffolds created outside the
  workspace.

### Breaking

- **`@neutron-build/core`: `NeutronAppResponseCacheEntry.body` is a
  `Uint8Array`** (it was a `string`; the type is exported from
  `@neutron-build/core` and `@neutron-build/core/server`). The cache is
  byte-exact now, where the string round-trip altered binary and non-UTF-8
  bodies. A custom `NeutronAppCacheStore` must store and return bytes and
  fails to type-check until it does. Entries written as strings by earlier
  versions are still served: the server encodes a string body, and
  `@neutron-build/cache-redis` returns such entries as bytes.
- **`@neutron-build/core`: several behaviours are stricter** (listed in the
  server hardening entry above). Startup rejects instead of serving an ungated
  static server; an image request with no `sharp` answers 503 and undecodable
  input 415, where the raw source bytes were returned; CSRF same-origin
  compares scheme, host and port; configured `ignoredMethods` are upper-cased.
  Apps that relied on the old behaviour need a change.

### Added

- **`maxRequestBodyBytes` on `NeutronServerOptions`.** The server adapter
  counts request-body bytes as they are read: reading past the cap fails the
  read (413 through the app error handler) and cancels the sender, which
  covers chunked bodies with no `Content-Length` and declared lengths that
  lie. The `Content-Length` check stays as the early rejection.

## [nucleus 0.2.0, data 0.2.0, sql 0.1.0] - 2026-09-28

**Requires Node.js 22 or later** (`engines.node` is `">=22"`; it was
`">=20"`) for every package released here.

### Breaking

- **`@neutron-build/nucleus` migrations use history protocol v2**
  (`contracts/data/MIGRATIONS.md`). The checksum is a SHA-256 digest of the
  up SQL; a history containing rows from before the protocol is refused
  before any change until `adoptMigrations` graduates it once (rows whose
  old checksum reproduces from the file are verified; rows with no checksum,
  or with no migration of the same version, are kept unverified; a recorded
  checksum that does not match its migration refuses the adoption). A crashed
  runner's lock claim is no longer taken over after
  ten minutes: release it with `forceUnlockMigrations`, after checking
  `migrationLockInfo`. The same protocol is in the Go SDK (released as
  `go/v0.2.0` with this release) and the CLI.
- **`@neutron-build/nucleus` pub/sub `channels()` takes no pattern.** The
  engine ignored it; filtering on the client would have faked a server
  feature.
- **`@neutron-build/nucleus` datalog `assert`, `retract`, `rule`, `clear`
  and `importGraph` resolve to the engine's reply string** instead of a
  boolean or number. `rule(head, body)` sends the engine's single-argument
  form.

- **`@neutron-build/data` `QueueDriver` gains `schedule()` and
  `unschedule()`.** A custom driver must implement both. The in-memory,
  BullMQ and new Postgres drivers do.
- **`@neutron-build/data` in-memory queue retries.** `InMemoryQueueDriver`
  now runs a failing handler up to three times, then records the job in
  `deadLetters` instead of dropping it on the first throw.
- **`@neutron-build/data` peer range for `@neutron-build/nucleus` is
  `^0.2.0`** (it was `^0.1.2`), following the nucleus release above.

### Changed

- **`@neutron-build/nucleus` model clients were checked against a live
  engine** (Nucleus 1.0.2, `conformance/live/orm/`). Document collections
  and SQL-bound graph traversal sit behind a capability gate, time-series
  queries behind semantic probes; columnar inserts bind numbers with a cast,
  because the engine's aggregates silently answer 0/NULL over untyped
  values; two cancellation defects in the HTTP and pg transports are fixed.
  Interface docs state measured engine behaviour: for example, CDC emits
  INSERT events only, and columnar inserts are refused inside a transaction.

- **`@neutron-build/data` cache TTLs are anchored at creation.**
  `MemoryCacheClient.incr` and the Nucleus cache's `incr` set the expiry only
  on the creating increment, as Redis does; later increments no longer extend
  it.
- **`@neutron-build/data` fixes.** `InMemoryRealtimeBus` logs and skips a
  subscriber that throws instead of aborting delivery to the rest. The Redis
  bus retries a channel whose `SUBSCRIBE` failed and no longer drops
  subscribers that joined meanwhile. The Nucleus storage driver returns the
  stored content type. The queue drivers no longer bundle `luxon` (the
  `cron-parser` dependency is replaced by a built-in five/six-field cron
  parser).
- **`@neutron-build/nucleus` clients match the engine.** Model wrappers were
  aligned with the engine's real functions (for example `kv.scan` uses
  `KV_KEYS`; time-series `query()` throws `NucleusNotSupportedError`). `int8`
  results are coerced to numbers (they arrived as strings under `pg`), so
  `kv.incr()` and the count functions return numbers. `datalog.clear()` and
  `importGraph()` send their argument. `document.update` / `delete` use the
  engine's `DOC_UPDATE` / `DOC_DELETE`. `vector.count` exists.

### Added

- **`@neutron-build/sql` 0.1.0 (first publish, alpha).** First-party
  PostgreSQL ORM: schema in code, one compiler, lossless codecs, relational
  reads, migrations through the CLI. Verified on PostgreSQL 15, 16, 17 and 18
  with `pg` and `postgres` (generated-column expression changes need 17+);
  Nucleus is not claimed. Its README lists the support matrix, the breaking
  corrections made during the alpha, and the upgrade and recovery limits.
- **`@neutron-build/data` Postgres queue driver.**
  `createPostgresQueueDriver` / `PostgresQueueDriver`: a durable queue on
  PostgreSQL with a Nucleus-friendly claim query, and schedules persisted in
  `neutron_schedules`. `schedule(id, pattern, payload)` takes a five- or
  six-field cron pattern on all three drivers (the in-memory driver's
  schedules are development-only).
- **`@neutron-build/data/drizzle` subpath.** The Drizzle interop with real
  `drizzle-orm` result types (Postgres and SQLite overloads). The root
  `createDrizzleDatabase` keeps the loosely typed surface, so importing the
  root needs no `drizzle-orm` types.
- **`@neutron-build/nucleus` PostgreSQL wire transport.** `postgres://` URLs
  connect through `PgTransport` (optional peer `pg`); `http(s)://` still uses
  the gateway transport. Also `withRetry` for serialization failures
  (SQLSTATE 40001), `setNX` with a TTL, and `cdel` / `cexpire` lease
  primitives.

## [agents 0.2.0, ai 0.1.1, mcp 0.1.1, workflow 0.1.1] - 2026-09-28

**Requires Node.js 22 or later** (`engines.node` is `">=22"`; it was
`">=20"`) for every package released here.

### Breaking

- **`@neutron-build/agents` refuses an unauthenticated exec-backed mount.**
  `createAgentHandler` with an `executor` and no `auth` hook now answers 500
  instead of running, because body-supplied `toolApprovals` self-approve.
  Pass `auth` (return a Response to refuse, `null` to continue), or set
  `allowUnauthenticated: true` for local development.
- **`@neutron-build/agents` `LocalExecutor` strips well-known credential
  variables by default** (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
  `GITHUB_TOKEN`, `AWS_*`, `NPM_TOKEN`, `DATABASE_URL` and others). Pass
  `envDenylist: []` to restore full inheritance; explicit `env` values still
  win.

### Fixed

- **`@neutron-build/agents`**: a `LocalExecutor` timeout kills the whole
  process group, not just the shell; the sandbox client bounds each HTTP
  round trip to the daemon and cancels the exec stream on early exit.
- **`@neutron-build/ai`**: a connection that drops mid-stream now ends in an
  error instead of reporting a truncated response as complete; a consumer that
  stops reading no longer leaves the provider connection open or `await
  result.text` pending forever; harness sessions are bounded (LRU, default
  64); the Anthropic adapter can be relabelled for gateway error attribution.
- **`@neutron-build/mcp`**: `tools/call` validates arguments against the
  tool's `inputSchema`.
- **`@neutron-build/workflow`**: fixes from the framework audit, including a
  step timeout that never fired and a sequence-space partition.

### Added

- **`@neutron-build/workflow` `PostgresEventStore`.** A durable event log
  (keyed by run and sequence, first writer wins) with executor leases on
  PostgreSQL, structurally typed against the `postgres` client.

## [Go SDK 0.2.0] - 2026-09-28

Tag `go/v0.2.0` (module `github.com/neutron-build/neutron/go`).

### Breaking

- **Migration history uses protocol v2**: history checksums, a pinned advisory
  lock, and explicit adoption (`AdoptMigrations`). A history from before the
  protocol is refused until adopted; see the `@neutron-build/nucleus` entry
  and `contracts/data/MIGRATIONS.md`.
- **Stricter request handling** from the Go audit: strict binding, a JSON
  commit path, a strict JWT policy, verified OAuth identity, fail-closed
  session commit and CSRF hardening. Apps that relied on the old leniency
  need a change.

### Fixed

- Router group middleware applies to `Mount`, `Static` and `StaticFS` routes;
  an exact mount root is normalized and application 404/405 responses are no
  longer rewritten.
- The HTTP cache honors `Vary` and freshness metadata, keeps streaming, and
  includes the request host in its key.
- CORS and compression correctness, a gzip state machine, a rate-limit bucket
  ceiling, and `DELETE` form bodies.
- Migration ledger lock, history checksums, and native integer scans.
- `Run("")` reads `NEUTRON_HOST` / `NEUTRON_PORT`; OpenAPI schemas match the
  wire; a lifecycle start rollback no longer runs on the success path; vector
  collections satisfy the engine's HNSW DDL gates.

### Added

- A snapshot-lease client surface with typed conflicts.

## [Go CLI 0.3.0] - 2026-09-28

Tag `cli/v0.3.0`. Behaviour changes are listed first; `neutron migrate` and
`neutron db push` users should read them before upgrading.

### Breaking

- **Migration history uses protocol v2.** A history written by an older CLI
  or SDK is refused until `neutron migrate adopt` graduates it once (see the
  `@neutron-build/nucleus` entry above and `contracts/data/MIGRATIONS.md`).
  A crashed runner's lock is not taken over automatically.
- **Migration statements may change only four session settings** (details
  under Changed below).
- **`--allow-destructive` covers more statements.** `TRUNCATE`,
  `DROP MATERIALIZED VIEW`, `DROP DOMAIN ... CASCADE`, and a composite type's
  `ALTER TYPE ... DROP ATTRIBUTE` / `ALTER ATTRIBUTE ... TYPE` now need it, as
  `DROP TABLE` and column drops already did. The `migrate` help and the
  refusal message list them.
- **Studio's legacy `/api/table/update` and `/api/table/delete` endpoints are
  removed**; unknown `/api/` paths answer 404 JSON. Row edits go through the
  identity-checked commit endpoints.

### Changed

- **Plans are ordered so each `up` applies and each `down` reverts.** Shared
  table changes are emitted as renames, plain adds, constraint drops, index
  drops, generated-column drops, attribute changes, generated-column adds,
  constraint adds, index creates, then plain column drops. Shapes that failed
  and rolled back now apply: a new table with a foreign key onto a renamed
  column; dropping a column together with the generated column that reads it;
  the `down` of a column dropped with an index on it; the `down` of tables
  dropped in a foreign-key cycle; changing a column to an enum under a text
  check. Declared views drop dependents first and are created bases first.
  Index key parts compare operator classes (an explicit default class equals
  the implicit one), so a class change rebuilds the index.
- **Changing the type of a column a generated column reads** plans as drop,
  change type and re-add in one migration. It needs `--allow-destructive`
  (values are recomputed) and no longer needs PostgreSQL 17. Changing the type
  of a column an **undeclared view** reads is refused at plan time, with two
  ways out: declare the view, or drop it with `--allow-destructive`.
- **A write that a table rule rewrites is refused in Studio with a 400**
  naming the rule (it was 502). An invalid primary index is no longer taken as
  the key. A rename target containing a dot maps as one name, in Studio and in
  `--rename`.
- **Messages.** The re-baseline and incomplete-chain hints give steps that
  work behind the drift gate; `migrate down` states the plan's actual grade
  for an empty `down`; a first-statement failure is no longer called
  "MID-FILE"; an unslugged `plan.json` refusal names its one-field fix; each
  command error prints once. `INCLUDE` lists compare regardless of order, and
  the v2 validators (Go and TypeScript) refuse a view definition that carries a
  second statement.
- **Platforms.** Native on Linux and macOS (amd64 and arm64). On Windows the
  installer points to WSL; there is no native Windows release. PostgreSQL
  15 to 18 are verified; 14 is not claimed.

- **Enum value additions are their own earlier step.** PostgreSQL
  cannot use an enum value in the transaction that adds it (55P04). When a
  change adds enum values alongside anything else, `db push` applies the
  additions in their own reported transaction first, and `migrate generate`
  writes them as a separate `{version}_{name}_enum_values` migration. A
  migration that adds and uses a value in one file fails with a message
  naming the split. Changing a generated column's expression is refused on
  PostgreSQL 16 before anything runs; snapshot plans record
  `minServerMajor`. A snapshot plan now records the migration's file slug,
  so `--name "Add Users"` produces an appliable migration. With `--rename`,
  `db push` and live `migrate generate` compare a renamed column's generated
  expressions, checks and indexes as PostgreSQL rewrites them, so a rename
  alone plans only `RENAME COLUMN`, and its down file reverts, including
  changed keys, foreign keys and indexes on the column. Offline snapshot
  renames that an expression names are refused with a two-migration path
  instead of a table rewrite whose down file could not run. Snapshot plans
  record what they leave in place, and a declared column order that differs
  from the table's is noted instead of refused, so the chain keeps matching
  the database and a second `db push` after adding a column mid-table
  converges. `schema baseline` leaves out `_neutron_*` tables and refuses
  while migration files are unapplied.

- **Migration statements may change only four session settings.** `neutron migrate` now runs each statement on its own,
  so a setting changed by one statement would apply to how the next is
  read. `SET LOCAL` in a migration may set only `lock_timeout`,
  `statement_timeout`, `maintenance_work_mem` and `work_mem`; any other
  setting (for example `search_path`, `role`, `time zone`) and
  `set_config(...)` anywhere in a statement are refused before anything
  runs, with a message naming the allowed settings. Migration files that
  set other settings must drop those statements or qualify names instead.
  A session whose `client_encoding` is not UTF8 or whose
  `standard_conforming_strings` is off (a database or role default) is
  refused before the first statement.

### Added

- **Snapshot-based planning and schema commands.** `neutron schema export`,
  `pull`, `check` and `baseline`; `neutron migrate generate --mode snapshot`
  plans offline from the last accepted snapshot; `neutron migrate adopt`.
  Introspection and diff cover the schema contract v2 surface
  (`contracts/data/`).
- **Guarded apply.** Migrations run only an allowlist of statement kinds,
  each statement on its own; `neutron migrate resolve <version>` inspects and
  recovers a non-transactional migration interrupted mid-file and never
  replays a statement whose outcome it cannot prove. Operational migrations
  (for example `CREATE INDEX CONCURRENTLY`) are journaled with identity-pinned
  steps.
- **Studio data editor.** Single-row edits by full primary-key identity,
  staged and committed atomically with stale checks, typed editors, virtualized
  large results, lossless import and export.
- **`neutron mcp` inspection tools** (read-only, redacted, per-model limits)
  and a cross-model inspection journey in Studio.
- **Experimental application coordinator.** `neutron project check`, `plan`,
  `run` and `spec` for multi-service applications.

## [core 0.2.2, cli 0.2.3, create-neutron 0.1.5] - 2026-09-07

### Fixed

- **A root `not-found.tsx` could render at `/` in place of the index page —
  on the client only.** A `not-found.tsx` carries its directory's path, so a
  root one is path `/` and collides with the index route. The server trie has
  always skipped such routes; the client route manifest never emitted the
  flag that lets the client do the same, so hydration picked whichever of the
  two the build happened to list first. Ordering was the only thing keeping a
  404 page from rendering at `/`. The manifest now carries `isNotFound` and
  the client skips those routes, mirroring the server. Older route tables
  omit the flag, which reads as false — the previous behaviour.

- **`neutron-ts build` and every other command died at module load if the
  published create-neutron was stale.** cli@0.2.2's init command imports
  named exports from create-neutron; the published 0.1.3 tree had neither
  the exports field nor the built file. init is now a dynamic import, so
  scaffolding code is never on the path of build, dev or preview, and
  create-neutron 0.1.4+ publishes a real entry.

### Changed

- **JSX settings are derived from the declared runtime; `vite.config.ts` is
  optional.** `runtime` is declared once, in neutron.config.ts; the JSX
  transform is now derived from it (resolveRuntimeJsx) instead of being
  restated per project. A project needs no Vite plugin to compile JSX.
  `@preact/preset-vite` remains useful in dev for HMR and devtools.

- **The build warns when a project registers `neutronPlugin()` in its own
  vite.config.** The CLI injects its own configured instance; the project's
  copy resolves from the project's `@neutron-build/core`, which is often
  older than the CLI's, and whichever instance answers first decides route
  semantics. dev drops the duplicate; build only warns for now — silently
  dropping it would move version-skewed projects onto different route
  semantics. Aligning versions first, then the dedupe follows.

- **The five templates are identical and register no plugin, and CI builds
  every one of them.** Previously four templates told you to register
  `neutronPlugin()` and one did not, and nothing ever compiled a scaffold —
  which is how the docs template shipped unbuildable. Templates keep the
  Preact plugin for dev niceties only. External scaffolds also now pin the
  released dependency pair instead of `latest`, which is where every
  floating site came from.

## [core 0.2.1] - 2026-09-07

### Added

- **`not-found.tsx` renders a 404 through the app's layout chain.** A
  `not-found.tsx` in the routes directory is discovered like any other route,
  which is what gives it a layout chain, and the server renders it through the
  same path a normal page takes before forcing the status to 404. One per
  directory is allowed and the deepest one covering the request wins, so a miss
  under `/admin` can look like the admin app rather than the marketing site.

  Two things it deliberately does not do: it is withheld from URL matching, so
  `/not-found` 404s like any other unknown path; and it takes its directory's
  path without being inserted into the trie, so it cannot shadow that
  directory's index route. A `not-found.tsx` that throws falls back to the
  plain `notFound()` response rather than turning every bad URL into a 500.

  Adding one is optional — an app without it keeps the standalone document
  described below.

### Changed

- **`notFound()` returns an HTML document instead of bare text.** It previously
  returned `new Response("Not Found")` with no content type, which browsers
  rendered as two words on a white page — what every app shipped as its 404
  unless the author noticed. The new default respects the reader's colour
  scheme, places a short message inside the shell, passes a full document
  through untouched, and escapes the message (a 404 often echoes part of the URL
  that produced it).

  **Migration:** any test asserting `await res.text() === "Not Found"` needs
  updating to assert on status and content instead. Behaviour for callers
  passing their own full document is unchanged.

  This is the fallback for an app with no `not-found.tsx`; it does not render
  through the layout chain. See the entry above for the route convention that
  does.

### Fixed

- **A navigation across a deploy hung instead of recovering.** Hashed chunks plus
  an emptied output directory mean an open tab's chunks stop existing the moment
  a release ships; the dynamic import rejected, nothing caught it, and the
  navigation never completed — the click did nothing, permanently. It now falls
  back to a full navigation to the target URL, so the user lands where they
  clicked rather than being bounced back.

## [cli 0.2.1] - 2026-08-02

### Fixed

- **The built client entry could be a route chunk instead of the runtime.** The
  build resolved it by globbing `assets/index-*.js`, sorting, and taking the
  last match. Any project with `src/routes/index.tsx` — every project with a
  homepage — emits a route chunk under exactly that name, so which file won was
  decided by which content hash sorted higher.

  When the route chunk won, the page's entire client runtime became that
  route's stripped config module, e.g. `const o={mode:"app"};export{o as
  config};` — 42 bytes, no router.

  The failure was silent. Pages are server-rendered, so they still looked
  correct and nothing errored; the app simply never hydrated. Nothing called
  `markStaticLinks()`, so no anchor got `data-neutron-static`, so the app-tier
  speculation rules scoped to that attribute matched nothing, and every
  navigation was a cold full page load with neither prerender nor client-side
  routing.

  The entry is now resolved from Vite's build manifest, excluding any chunk the
  manifest attributes to a route module. The filename glob remains as a fallback
  for builds without a manifest, with the same exclusion applied.

  **If you are on 0.2.0 or earlier, rebuild.** No source change is needed. To
  check whether you were affected, look at the `<script type="module">` your
  pages load: if it is a few dozen bytes, it was the wrong file.

## [core 0.2.0, cli 0.2.0] - 2026-08-02

### Breaking

- **A `mode: "static"` route no longer hydrates its whole component tree.** It
  ships no client router, so only islands become interactive. Previously the
  router hydrated every route, so a component using hooks in a static page
  worked; now it renders as HTML and stays inert.

  This is why the minor version moved: `^0.1.x` will not pick it up.

  **Migration.** The build now reports this for you, naming the file. Two fixes:
  wrap the interactive component in `<Island>` (ships only that component's
  code, and is why a static page can cost zero JS), or add `hydrate: true` to
  the route's config to restore the old whole-tree hydration.

### Added

- **Static pages navigate instantly with no JavaScript.** Prerendered documents
  and static-tier responses emit `<script type="speculationrules">`, so the
  browser prerenders the next page on pointer intent — already painted when the
  click lands, which no client-side prefetch can match. `eagerness: "moderate"`
  rather than `eager` on purpose: prerendering executes the target page, and
  eager speculation would turn one visitor with a dense nav bar into a dozen
  server renders. `data-neutron-prefetch="false"` opts a link out of both this
  and the JS prefetcher.
- **A build-time check for interactivity that will not run**, described above.
  Conservative: it skips any route declaring an island and stops at the project
  boundary, because a heuristic that warns about correct code gets ignored.
- **Link prefetching that works.** Viewport and pointer-intent warming of the
  navigation payload, declining on `saveData` and 2g, skipping redirected
  responses.

### Fixed

- **The prefetcher fetched into a cache nothing read.** `incremental-prefetch`
  was imported by nothing, so it never ran — and it was built on three server
  headers that do not exist (`X-Neutron-Prefetch-Metadata`,
  `X-Neutron-Skip-Layout`, and a response `X-Neutron-Layout-Id`), so it could
  not have worked if it had. It also wrote to module-local maps while
  navigation read `window.__NEUTRON_PREFETCH_CACHE__`.
- **Post-mutation data was served as a prefetch, forever.** `useSubmit` stored
  its response under the current URL with no expiry and no invalidation, and
  navigation treated that global as a prefetch cache. Navigating away and back
  re-rendered the snapshot with no network, indefinitely — changes made
  anywhere else, including by the same user in another tab, stayed invisible
  until a hard reload. Entries now expire and are consumed on read.
- **A stale in-flight fetch could overwrite a completed navigation.** The
  cache-hit path neither aborted the active request nor claimed a new request
  id, so a slow response for an abandoned page landed afterwards and applied
  its data over the page the user was on. It also skipped the scroll reset the
  fetched path performs.
- **One un-annotated layout forced the router onto every page.** The client
  runtime was gated on `every(route => route.config.hydrate !== false)`, and
  `hydrate` is undefined on every route that does not set it — so a single
  ordinary layout, the default state of every layout, pulled the full router
  into purely static pages. Those pages then intercepted every link click and
  turned it into a data fetch for data that did not exist.
- **Static targets are no longer intercepted by the router.** A `mode: "static"`
  route with a prebuilt file is already served without middleware, so a browser
  navigation to it is both correct and cheaper than a client render.

## [core 0.1.9, cli 0.1.6] - 2026-08-01

### Fixed

- **Production app routes could render without application CSS.** Static routes
  linked Vite's emitted stylesheets, but the CLI discarded those URLs when it
  generated the app-route runtime. Initial SSR documents therefore painted as
  plain HTML, and navigation between static and app routes visibly flashed as
  styles disappeared. The CLI now carries emitted CSS URLs into every runtime,
  core SSR emits render-blocking stylesheet links, JSON navigation responses
  expose the same URLs through `__css__`, and `neutron start` follows the same
  contract.

## [core 0.1.8] - 2026-07-27

### Fixed

- **A `[param]` directory was emitted as a literal path segment
  (`@neutron-build/core`).** Only the last segment of a route was run through
  the param rule, so `api/runs/[id]/decide.tsx` registered as the literal path
  `/api/runs/[id]/decide` with no params — the route existed, but only the URL
  containing the brackets could reach it, and every real request 404'd. There
  was no build error and no warning; the only visible symptom was the generated
  route type, which showed `"/api/runs/[id]/decide"` as a plain string while a
  leaf `runs/[id].tsx` correctly produced `` `/runs/${string}` ``. Directory
  names now go through the same rule as filenames, including `[...name]`
  catch-alls and the `[.]` literal-dot escape.
- **A named catch-all got a literal string route type.** The type generator
  tested for a bare `"*"`, but catch-all segments carry their param name
  (`*slug`), so `/docs/*slug` was typed as the literal `"/docs/*slug"` instead
  of a template with a `${string}` hole.

### Changed

- **A route table that cannot work is now a build error, not a 404 at
  runtime.** `discoverRoutes` rejects three cases that all used to register
  silently: a malformed dynamic segment (a leftover `[` or `]` after
  conversion), a catch-all that is not the last segment (nothing below it can
  ever match), and two files that resolve to the same URL shape (only one can
  ever be matched, and which one was an accident of discovery order — this
  includes `/users/:id` vs `/users/:name`, since the router keeps one param
  name per position). A literal suffix still distinguishes a route, so
  `/docs/*slug` and `/docs/*slug.md` remain separate.

## [0.1.x hardening wave] - 2026-06-04

> DX + ecosystem-hygiene pass. Scaffolded projects now typecheck clean, build
> with no sourcemap warnings, and ship a README. Republishes **all packages**:
> `@neutron-build/core` 0.1.2, `@neutron-build/cli` 0.1.3, `create-neutron`
> 0.1.2, and `@neutron-build/{auth,cache-redis,data,nucleus,ops,otel,security}`
> 0.1.1.

### Ecosystem-wide (all packages)

- **Sourcemaps embed their sources (`inlineSources`).** Every package shipped
  `.js.map` files referencing `src/*.ts` not included in the tarball, so a
  consumer importing any of them saw "points to missing source files" warnings.
  Fixed across all packages.
- **`@neutron-build/*` cross-deps use caret, not exact pins.** Packages depended
  on `core`/`nucleus` via `workspace:*`, which rewrites to an *exact* version on
  publish — so installing e.g. `@neutron-build/auth` alongside a newer `core`
  produced two `core` copies (the same duplicate-instance hazard behind the
  earlier `__H` crash). Now `workspace:^` -> `^0.1.x`, which dedupes to one copy.

### Fixed

- **Scaffolds did not typecheck out of the box.** A fresh project showed
  TypeScript errors on first open:
  - `Cannot find module 'virtual:neutron/routes'` — templates now ship
    `src/neutron-env.d.ts` declaring the Vite virtual module (typed as exactly
    `registerRoutes`'s parameter, so it can't drift).
  - `_layout` props typed `children` as `unknown` — now `ComponentChildren`.
  - The generated `.neutron-*.d.ts` type files were invisible to `tsc`:
    TypeScript's wildcard `include` skips dot-prefixed files, so the template
    `tsconfig.json` now globs `src/**/.neutron-*.d.ts`.
- **Content-collection types never applied (`@neutron-build/core`).** The
  content type generator emitted `declare module "neutron/content"` (a stale
  pre-rename specifier) and omitted a trailing `export {}`, so the
  `ContentCollectionMap` augmentation neither targeted the real module nor
  merged into it — `getEntry()`/`getCollection()` returned `unknown`. Now
  targets `@neutron-build/core/content` and emits `export {}`, so content data
  is properly typed.
- **Sourcemap warnings on every build (`@neutron-build/core`).** Published maps
  referenced `src/*.ts` files not shipped in the package, so consumers saw ~17
  "points to missing source files" warnings per build. Maps now embed their
  sources (`inlineSources`).
- **Edge/worker bundle could crash app-mode inline hook components.** The
  vercel/cloudflare/docker SSR bundle externalized `@neutron-build/core` while
  bundling preact, so an inline `<Link>` (or any inline hook component) in an
  `app`-mode route could hit the two-preact `__H` crash at request time. `core`
  is now bundled into the worker too, matching the build/dev paths. (The node
  production server was already unaffected — it resolves a single deduped preact
  from `node_modules`.)

### Added

- **`head()` supports a `link` field (`@neutron-build/core`).** Lets routes/layouts
  add arbitrary `<link>` tags — favicon, `preconnect`, `manifest`, `alternate`,
  etc. Previously only `canonical` links were possible, so there was no way to
  set a favicon. Multiple same-`rel` links (e.g. several `preconnect`s) are kept.
- **Templates ship and reference a favicon.** Every scaffold now includes a clean
  `public/favicon.svg` wired via `head()`, so pages no longer 404 on `/favicon.ico`
  or show a blank tab.

### Changed

- **`@neutron-build/cli` now depends on `@neutron-build/core` via caret (`^`).**
  `workspace:*` rewrote to an exact pin, so every core patch forced a cli
  republish and could install two core copies (re-triggering the preact-instance
  crash). Now `^0.1.x`.
- **Every template ships a `README.md`** (commands, project structure, docs link).
- **Docs template accessibility:** content is wrapped in a `<main>` landmark and
  pages render a single `<h1>` (the mdx bodies no longer duplicate the
  frontmatter title).

### Testing

- create-neutron regression tests now also assert: each template has a
  `README.md` and `src/neutron-env.d.ts`, the tsconfig globs the generated
  `.neutron-*.d.ts` files, and no layout types `children` as `unknown`.

## [0.1.1 / cli 0.1.2] - 2026-06-04

> Patch wave fixing first-run-experience and SSR bugs found by smoke-testing the
> shipped 0.1.0. Republishes `create-neutron` (0.1.1), `@neutron-build/core`
> (0.1.1) and `@neutron-build/cli` (0.1.2). The other seven packages at 0.1.0 are
> unaffected. NOTE: `@neutron-build/cli@0.1.1` was a broken intermediate publish
> (pinned `core@0.1.0`, which predates an export it needs) — use 0.1.2.

### Fixed

- **`create-neutron`: scaffolded apps failed to run.** Generated `package.json`
  scripts invoked a bare `neutron` binary, but the dev CLI ships the `neutron-ts`
  bin (`@neutron-build/cli`), so a fresh `pnpm dev`/`pnpm build` died with
  `neutron: command not found`. Templates now call `neutron-ts` and include the
  `preact-render-to-string` dependency.
- **`create-neutron` (docs template): broken catch-all URLs.** The `docs`
  `[...slug]` route keyed its `getStaticPaths` params by `"*"` while the router
  names the catch-all param `slug`, so pages rendered at garbage paths like
  `/docs/getting-started/installationslug`. Now keyed by `slug`, producing the
  correct `/docs/getting-started/installation`.
- **`@neutron-build/cli`: inline hook components crashed SSR/pre-render.** Both
  `neutron-ts build` and `neutron-ts dev` left `@neutron-build/core` outside the
  Vite SSR graph while the renderer used the in-graph preact, so core's inline
  `<Link>` (and any inline `useState`/`useEffect` component) crashed with
  "Cannot read properties of null (reading '__H')" — two preact instances, no
  shared hooks dispatcher. `@neutron-build/core` is now in `ssr.noExternal` on
  both paths. (Island components were unaffected — they defer hooks to the
  client.) Surfaced by the `docs` template, the only one using inline `<Link>`.
- **`@neutron-build/cli`: misleading build output.** `neutron-ts build` listed
  `_layout` files as routes, printing duplicates (e.g. `/` twice). The listing
  now shows only page routes; layouts were already correctly excluded from
  rendering, so this is output-only.

### Changed

- **`@neutron-build/core` → 0.1.1.** Ships two fixes that landed after the 0.1.0
  publish: CSS Modules now emit in static builds, and a single preact instance is
  shared during SSR/pre-render (the `CLIENT_ROUTE_QUERY` export the CLI now
  depends on lives here). `cli@0.1.2` pins `core@0.1.1`.

### Known limitations

- The edge/worker bundle (vercel/cloudflare app-route deploys) still externalizes
  `@neutron-build/core`. No template exercises app-mode inline `<Link>` on edge,
  so this is not yet fixed there; static (SSG) and dev paths are.

### Testing

- `create-neutron` gains regression tests that read the real template files and
  assert: scripts only ever call `neutron-ts` (never a bare `neutron`), the
  Neutron deps + `preact-render-to-string` are present, and named catch-all
  routes use the named param key rather than the bare `"*"`.

## [0.1.0] - 2026-05-28

> First coordinated multi-package publish to npm. `@neutron-build/core` and
> `@neutron-build/cli` move from 0.0.1 to 0.1.0; the other eight packages
> publish for the first time, all at 0.1.0. Canonical npm scope is
> `@neutron-build/*` (plus the unscoped `create-neutron`) — see Reality note
> in `docs/rfcs/naming.md`.

### Added

- Core cache-store abstraction for server runtime (`cache.app` + `cache.loader`) with memory defaults and exported cache store types/factories.
- New package `@neutron-build/cache-redis` for distributed Redis/Dragonfly-backed app + loader cache stores.
- New package `@neutron-build/otel` for Neutron hook → OpenTelemetry span/error integration.
- New package `@neutron-build/auth` for auth context middleware, protected-route middleware, and Better Auth/Auth.js style adapters.
- New package `@neutron-build/security` for CSP nonce middleware, CSRF middleware, trusted-proxy IP resolution, rate limiting, and secure cookie defaults.
- New package `@neutron-build/ops` for request-id/trace context middleware, health/readiness middleware, and structured JSON logging hooks.
- New package `@neutron-build/data` (database, cache, sessions, queues, storage, rate limiting).
- New package `@neutron-build/nucleus` — typed Nucleus client (14 data models).
- New package `create-neutron` — project scaffold (`npm create neutron@latest`).
- Enterprise documentation (`docs/enterprise.md`) plus security/support policies (`SECURITY.md`, `SUPPORT.md`).
- Semgrep + `pnpm audit` CI workflow (`.github/workflows/typescript_security.yml`).

### Changed

- `createServer` now supports pluggable cache stores through `NeutronServerOptions.cache`.
- Release docs include explicit support and deprecation policy commitments.
- Naming RFC amended to bless the `@neutron-build/*` org scope (bare `neutron`/`nucleus` and the `@neutron`/`@nucleus` scopes are unavailable on npm).
- Tag format standardized as `ts/vX.Y.Z` (matches the existing `typescript-publish.yml` trigger).

### Security (regression-tested)

- Cross-user app-response cache poisoning closed via credentials gate; CORS-reflected responses no longer cached.
- Rate-limit X-Forwarded-For spoof bypass: default no longer trusts XFF; `trustProxy` opt-in with right-most hop; per-client `context.clientAddress`; key cap.
- CSRF: `crypto.timingSafeEqual` compare; same-origin (Origin/Referer) check; cookie defaults to `HttpOnly` + `SameSite=Strict`; token reuse to stop churn; lazy form-body read.
- Fail-open `X-Forwarded-Proto` removed; new `trustedHosts` server option; `__Host-`/`__Secure-` cookie-prefix enforcement; production-default Secure cookie.
- Bypassable regex HTML sanitizers replaced with trusted-by-default content + optional `sanitize-html` peer dependency; head fragment and island sinks corrected.
- RSS `content:encoded` CDATA-terminator escape; SEO `on*` attribute names blocked; hardened `</script>` head-script guard.
- Request-smuggling rejection (Content-Length + Transfer-Encoding); byte-accurate header limits; opt-in `rejectUnknownLength`.
- Open-redirect closed in route rules (protocol-relative, backslash, tab/newline collapse) and in auth `redirectTo` (same-origin only).
- Default `nosniff` / `X-Frame-Options` / `Referrer-Policy` on static, docker, and generated serverless adapters; docker static path traversal containment + null-byte reject.
- CSP nonce stamped onto framework inline scripts (data, client module, JSON-LD, `headScripts`).
- Dep CVEs: `devalue` ≥5.6.4, `hono` 4.12.x, `@hono/node-server` 1.19.x, `turbo` 2.9.x, `fast-xml-parser` ≥5.7.0 (override).

### Fixed

- Dead Nucleus client specifier in `@neutron-build/data`/drizzle.
- Syntax highlighting restored (Marked v15 async `walkTokens` integration with Shiki).
- `defineCollection` no longer silently drops `live`/`loader`/`cacheTtl`.
- Audit-log IDs use `randomUUID()` (was `Math.random()`).
- Deduplicated `escapeHtml`/`escapeXml` across the codebase into `core/escape.ts`.
- Site install docs corrected to reflect actual published package names and CLI-preset adapter model.

## [0.1.0-dev] - 2026-02-13

### Added

- Server E2E matrix coverage for static/app/islands/forms/errors/streaming routes.
- Content collections recursive file discovery (nested slugs).
- Additional docs for API, benchmarks, migration, release workflow, and examples.
- Deploy presets CI workflow.
- `neutron worker` command with `--entry`, `--mode`, and `--once`.
- `neutron-data` Redis/Dragonfly session driver factory (`createRedisSessionStore`).
- `apps/playground` neutron-data integration profile (`memory` vs `production`) with worker entry and DB migration/seed scripts.
- Server observability hooks (`onRequestStart/End`, loader/action lifecycle, `onError`) for external telemetry adapters.
- Data-profile smoke script + CI lane (`ci:data-profiles`) for `apps/playground` memory profile, with optional production-profile checks when env/services are provided.
- Dedicated example packages: `examples/marketing-reference` and `examples/saas-reference`.
- `create-neutron` templates expanded to `basic`, `marketing`, `app`, and `full`.
- New `neutron release-check` command (build + deploy artifact validation).
- New one-command monorepo release gate: `pnpm run ci:release`.
- New SEO utilities: `buildMetaTags`, `renderMetaTags`, `buildSitemapXml`, `buildRobotsTxt`.
- New i18n routing primitives: `resolveLocalePath`, `withLocalePath`, `stripLocalePrefix`, `createI18nMiddleware`.
- New `Image` component with responsive `srcset` generation and pluggable image loader.
- Benchmark canonical publish workflow: `compare:canonical` and `ci:bench:canonical`.
- Static adapter route coverage test for headers/precompression behavior.
- Deployment guide doc (`docs/deployment.md`) and updated CLI/create-neutron docs for static preset + release checks.

### Changed

- Content collection generated types now emit optional object properties as `?:`.
- E2E islands assertion aligned to production server client-entry injection behavior.
- Route cache config now supports `cache.loaderMaxAge` for loader-data caching.
- App route config now supports `hydrate: false` to disable client runtime/data injection for zero-JS SSR pages.
- Node app runtime now supports loader auto-caching with mutation invalidation.
- Generated adapter runtime bundles now mirror loader auto-cache + invalidation behavior.
- Client navigation protocol now supports `X-Neutron-Data` + optional `X-Neutron-Routes` partial loader requests, with stale-request protection and stronger navigation state handling.
- Content collections now emit clearer contextual errors for parse/schema/MDX failures.
- Route discovery now recognizes `_layout` files across all supported route extensions (`.ts`, `.tsx`, `.js`, `.jsx`, `.mdx`).
- Content config loading now supports `src/content/config.ts` via runtime transpile fallback when Node cannot import TypeScript directly.
- Benchmark harness `neutron-react` lane now runs the same benchmark app as `neutron` (`apps/playground`) with runtime switched by `NEUTRON_RUNTIME` for true renderer parity.
- Benchmark harness now supports load profiles (`BENCH_PROFILE=baseline|stress|saturation`) and payload parity auditing (`BENCH_PAYLOAD_AUDIT`, `BENCH_PAYLOAD_WARN_RATIO`).
- Static adapter output now emits richer static policy metadata, route-level HTML cache header rules, and precompressed artifacts.
- Static benchmark host (`benchmarks/serve-static.mjs`) now uses pre-indexed route resolution, `_headers` parsing, precompressed variant selection, and optional in-memory small-asset serving for more realistic static-host benchmarking.
- Islands hydration path now uses a single runtime path (removed duplicate inline island runtime injection), with stronger island component ID stability and client registration hardening.
- Route discovery now supports route groups (`(group)` directories) without leaking group names into URL paths.
- Vite client route manifest generation now uses lazy route module loading for route-level client code-splitting.
- `neutron build --preset` and `neutron deploy-check --preset` now include `static`.
- `neutron release-check` and docs now define one-command release-grade project validation flow.

### Fixed

- SSR middleware Vite HMR port collision in parallel test runs by using a free port.
- `examples/saas-reference` client route imports now use `neutron/client` for `<Form>` so production client builds do not pull server-only runtime modules.
- Static benchmark fallback cache-control behavior for extensionless HTML routes now defaults correctly to `must-revalidate`.

### Performance

- Completed a fresh full benchmark matrix run and repinned benchmark baselines (`baseline-full.json` and `baseline.json`) from `benchmarks/results/latest.json` for release gating consistency.
- Improved Neutron optimal-static benchmark throughput substantially via benchmark static-host server optimizations (pre-indexed route map + in-memory small-asset serving).
