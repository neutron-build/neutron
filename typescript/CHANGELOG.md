# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

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
