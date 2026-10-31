# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

## [core 0.5.0, cli 0.5.0, create 0.1.11, auth 0.1.6, cache-redis 0.2.1, ops 0.1.5, otel 0.1.5, security 0.1.6] - 2026-10-31

Upgrading from core/cli 0.4.x: read Breaking first; each entry has a
**Migration:** note on what an app must change. Upgrade `core`, `cli` and
the packages under Dependents in one step.

### Breaking

- **`:name*` in `routes` rules matches zero or more segments**, as in Next.
  It required at least one, so a rule written `/builder/:path*` (the Next
  spelling of "this section") silently skipped `/builder` itself, which is
  the document that most needs a section's headers. `/docs/:path*` now also
  matches `/docs`, and an empty catch-all drops its slash in a destination
  (`/v2/docs/:path*` → `/v2/docs`). **Migration:** a redirect or rewrite
  that must not fire on the bare prefix should spell its segments out
  (`/docs/:first/:rest*`). Bare `*` still requires one segment.
- **Global middleware runs before `mount` sub-apps.** `src/middleware.ts`
  never ran for requests a mounted sub-app answered, so an auth gate there
  silently did not cover mounted APIs (tRPC, REST) — FRAMEWORK_CONTRACT §5
  places authentication before every route handler. In ssr mode the global
  chain now runs ahead of each mount, outermost, with the real request; a
  response it returns without calling `next()` (401, redirect, thrown
  `Response`, `ProblemError`) answers instead of the sub-app, and a response
  it substitutes after `next()` replaces the sub-app's. A mount path the
  sub-app does not match falls through to the routes with the chain run once,
  not twice. **Migration:** middleware that assumed it only saw page traffic
  now sees mount traffic too. A sub-app that enforces its own authentication
  can opt out per mount with the new object form:
  `mount: { "/api/trpc": { app: trpcApp, globalMiddleware: false } }`
  (`NeutronMount`). api/raw servers load no global middleware and are
  unchanged.
- **CORS is now opt-in.** `createServer` no longer enables CORS by default.
  Previously, a server created with no `cors` option sent
  `Access-Control-Allow-Origin: *` on every request that carried an `Origin`
  header (plus `Vary: Origin` on every response and a 204 preflight
  short-circuit), in all modes. A wildcard default is the wrong default for
  credentialed apps — browsers refuse credentials against `*`, which masks the
  misconfiguration until something switches to echo-mode. With no `cors`
  option (or `cors: false`) a server in any mode — `ssr`, `api`, `raw` — now
  sends no `Access-Control-*` headers and never short-circuits a preflight.
  **Migration:** pass an explicit config to keep cross-origin access:
  `createServer({ cors: { origin: ["https://app.example.com"], credentials: true } })`.
  An allowlist echoes the allowed origin exactly; `credentials: true` with
  `"*"` is refused at startup. `cors: {}` restores allow-all behavior
  (`origin: "*"`, no credentials) if the old default is genuinely what you
  want. Security headers stay on by default (`securityHeaders: false` to opt
  out) and are unchanged by this flip.
- **Default `Cross-Origin-Opener-Policy` is now `same-origin-allow-popups`**
  (was `same-origin`). `same-origin` severs `window.opener` in popups a page
  opens, which is the channel OAuth providers and payment sheets (Google/Apple
  sign-in, PayPal, wallet popups) use to report back. Every app on the
  defaults had those flows broken until it found and overrode the header.
  `same-origin-allow-popups` keeps the protection COOP exists for: a
  cross-origin page that opens yours gets no handle on it. What it gives up
  is cross-origin isolation, which needs `Cross-Origin-Embedder-Policy` as
  well and breaks third-party embeds, so it has to be configured where it is
  needed anyway. **Migration:** an app that relied on cross-origin
  isolation restores the old value with
  `securityHeaders: { headers: { "Cross-Origin-Opener-Policy": "same-origin" } }`.
  `X-Frame-Options: DENY` is unchanged: framing stays opt-in, and a CSP
  `frame-ancestors` supersedes XFO in browsers that implement CSP 2. The new
  "Security headers" section of the `neutron.config.ts` docs records both
  decisions.
- **Framework components return `AnyJsxElement | null` instead of `any`.**
  `Form`, `Link`, `NavLink`, `ViewTransitions`, `ScrollReveal`, `Island`,
  `Image`, `Picture`, `ServerIsland` and `Show`/`For`/`Switch`/`Match`/
  `Index` declared `any` as their element return so React-typed JSX could
  use them. The exported structural `AnyJsxElement` passes both runtimes'
  JSX checks too (the `type-fixtures/react-*` compiles are unchanged and
  still pass), so the declarations are typed again. Runtime output is
  unchanged. **Migration:** code that assigned a component's return value to a
  preact `VNode` needs a cast. `Await` stays `any`: it returns
  `children(data)` as is, which can be a string or an array.
- **The SSR server's default 404 is content-negotiated.** With no
  application `not-found.tsx`, an unmatched path used to answer bare
  `text/plain` "Not Found". HTML-preferring clients (`Accept: text/html`,
  i.e. browsers) now get the same minimal HTML page `notFound()` serves;
  every other client — `Accept: application/json`, `X-Neutron-Data`, no
  preference, wildcard-only — gets the RFC 7807 `application/problem+json`
  document in exactly the shape api mode serves (`No route matches <path>`).
  An application `not-found.tsx` still answers for every client, and api
  mode's JSON 404 is unchanged. FRAMEWORK_CONTRACT.md §2 states the
  negotiation rule.
  **Migration:** a client or test that matched the bare `Not Found` text
  should check the status code, or the app should add a `not-found.tsx`.
- **`PUBLIC_*` environment variables are inlined into client code.**
  `PUBLIC_` was documented as the public prefix but nothing applied it; the
  new `env.publicPrefixes` (default `["PUBLIC_"]`, see Added) does, so a
  variable with that prefix now reaches browser bundles at build time.
  **Migration:** rename any secret whose name starts with `PUBLIC_` before
  building, or set `env.publicPrefixes` to the prefixes you mean.
- **`ViteAliasEntry.find` is `string | RegExp`, and bare `react` resolves to
  `@neutron-build/core/react-compat` under react-compat** (see the React 19
  `use()` entry under Added). **Migration:** custom Vite setups that read
  `find` as a string, or `mergePreactAliases`' flat map for a bare `react`
  key, must handle the RegExp entry.

### Added

- **`client.unloadRouteStylesheets`: route stylesheets no longer needed are
  unlinked after client navigation (opt-in, F-B5-1).** Client navigation kept
  every earlier route's stylesheets in the document, as Next's App Router
  does, so pages whose sheets redefine the same tokens (Fylun's per-section
  Tailwind entries on `:root`) showed the cascade of whichever loaded last:
  after `/files` → `/studio` → `/files` the studio tokens won. With
  `client: { unloadRouteStylesheets: true }` in `neutron.config.ts`, the
  router unlinks, in the frame after the new page has rendered (its own
  sheets were loaded before it rendered, so nothing flashes), the route
  stylesheets the new route's list does not name. Only links Neutron created
  for routes are candidates — SSR'd route links now carry `data-neutron-css`
  in `start`, dev and prerendered pages — so the client entry's and layouts'
  stylesheets (in every route's list) and `head()` links stay. The default is
  unchanged; the trade-off is a re-request (normally from the HTTP cache)
  when navigating back. `configureRouter()` is exported from
  `@neutron-build/core/client`; the option is applied by the generated
  `virtual:neutron/routes` module.
- **The vercel/netlify/cloudflare/docker runtime matches `start` on security
  headers and serves `src/server.entry.ts` mounts.** The generated handler
  sent 3 of `start`'s 5 security defaults (no `Cross-Origin-Opener-Policy`,
  no `Cross-Origin-Resource-Policy`), ignored `server.securityHeaders`
  (custom headers and `false`), and served no mounts, so a tRPC/REST API
  declared in the server entry 404ed on every edge deploy. It now applies
  `routes.headers` and then the configured defaults with core's own
  functions (the order, overrides and `null` suppression of `start`), on
  every response it answers, and dispatches the entry's mounts with the
  global middleware ahead of them (per-mount opt-out honoured, unmatched
  paths falling through to the routes with the chain run once). What it
  cannot serve fails the build: a server entry `websocket` on any of these
  presets, and mounts (or server actions) in an app with no app route, for
  which no runtime is built; `--preset static` fails on a server entry.
- **Server Actions: `"use server"` modules.** Client code that imported a
  `"use server"` module bundled the module whole into the browser build (the
  Fylun port found Prisma, pg, sharp and Better-Auth in it, F-P3-4, and
  bridged it with its own endpoint). Such a module now compiles, in the client
  graph, to one stub per export that POSTs to `/_neutron/action` under an id
  fixed at build time (a hash of module path and export name). The server
  registers only the modules client code reached — `neutron-ts build` writes
  them to `dist/.neutron-server-actions.json`, `neutron-ts dev` registers what
  Vite transforms for the browser — and answers any other id `404`. Every
  export must be an async function the build can see is one; anything else
  (values, sync functions, re-exports, call results) and inline `"use server"`
  functions are build errors naming the file and line. Requests pass a
  same-origin check (`Sec-Fetch-Site`, else `Origin` against `Host`; `403`),
  a body cap (`server.serverActions.bodySizeLimit`, 1 MiB; `413`), and the
  app's global middleware, which can gate them and fills the `context` the
  action reads through the new `getActionContext()` (with `request` and
  mutable `responseHeaders`: `Set-Cookie`, `x-neutron-invalidate`, which
  evicts the server caches and revalidates the page on screen). Arguments and
  results use the loader-data serializer; `FormData` and `Blob`/`File`
  arguments go multipart. Errors follow the RFC 7807 contract: a
  `ProblemError` answers with its status (the client rejects with a
  `ServerActionError`), a `Response` answers as itself, anything else is a
  generic `500` reported to `hooks.onError` (`source: "action"`). A redirect
  is signalled as `X-Neutron-Redirect` and the client navigates. Served by
  `dev`, `start` (server bundle and Vite fallback), the docker image and the
  vercel/netlify/cloudflare runtime; `--preset static`, or an edge preset
  without an app route, fails the build instead of shipping dead stubs. A
  server-side import of the module is the real module. Under react-compat,
  `<form action={fn}>` now calls `fn(formData)` on submit (React 19, client
  side). The Cloudflare worker no longer sends non-GET requests to the asset
  binding first. See `docs/server-actions.md`.
- **`env.publicPrefixes`: public environment variables for client code,
  including Next's `process.env.NEXT_PUBLIC_*`.** Client builds turned
  `process.env` into `{}` and exposed only Vite's `import.meta.env.VITE_*`,
  so every `process.env.NEXT_PUBLIC_*` read in a Next app's browser code was
  empty in a build and threw `process is not defined` in dev (the Fylun port
  shimmed it). `neutron.config.ts` `env: { publicPrefixes: ["PUBLIC_",
  "NEXT_PUBLIC_"] }` (default `["PUBLIC_"]`) makes those prefixes public: they
  join Vite's `envPrefix` (`import.meta.env.<PREFIX>X`), and
  `process.env.<PREFIX>X` is replaced with its build-time value from `.env`
  files or the process environment in client and server code, in builds and
  in dev. An unset one reads `undefined` in client code and stays a runtime
  read on the server. Dev client code also gets the rest of a client build's
  replacement (`process.env` is `{}`, `NODE_ENV` inlined), so it reads what
  the build reads instead of throwing `process is not defined`. Variables
  without a public prefix are never inlined, and an empty prefix is refused. `PUBLIC_` was already documented as public
  but nothing set it, so `PUBLIC_*` variables now reach client code as the
  docs said. `neutronPublicEnvPlugin` is exported from
  `@neutron-build/core/vite` for custom Vite setups.
- **`src/server.entry.ts`: mounts and WebSocket handlers in the routes'
  module graph.** A custom server had to build its tRPC/REST mounts and its
  WebSocket handler before `createServer`, outside the graph the routes run
  in. Under `neutron-ts start` the routes come from the prebuilt server
  bundle, so every module a mount and a route both imported was loaded twice:
  two Prisma clients, two auth instances, split singletons. The Fylun port
  worked around it with `globalThis` bridges. An app now declares them in
  `src/server.entry.ts` (`export default defineServerEntry({ mount,
  websocket: { path, authorize, attach }, close })`, or a function returning
  that, async allowed). Neutron loads the file wherever it loads routes. It
  is compiled into the `start` server bundle (`neutron-ts build` includes it
  and `createServer` requires it there), loaded through the Vite runtime when
  there is no bundle, and loaded through Vite in `neutron-ts dev`, which now
  serves the entry's mounts (global middleware first, unmatched paths falling
  through, as in production) and its WebSocket upgrades, and reloads the
  entry on edit. Entry mounts merge with `createServer({ mount })` (a prefix
  declared in both is a startup error). `websocket` enables the transport and
  `attach` receives the `WebSocketServer`, and `close` runs on shutdown after
  the drain. A present file that fails to load or exports nothing usable is a
  startup error, like `src/middleware.ts`. The name is not `src/server.ts`:
  that is where apps keep a custom server that calls `createServer`, which
  must not import itself. `createServer({ mount })` is unchanged. The
  vercel/netlify/cloudflare/docker runtime serves the entry's mounts (see
  the adapter-runtime entry below); its `websocket` needs `start`.
- **Security headers can be scoped per route.** `securityHeaders` was one
  site-wide set, so an app that needs cross-origin isolation on one section
  (Fylun: COOP/COEP on `/builder/**` for WebContainer, and no COEP anywhere
  else, since COEP breaks third-party embeds) had to hand-roll a global
  middleware. `routes.headers` rules are now the per-route override, with no
  parallel mechanism. A rule's value is set before the defaults fill in, so
  it overrides them on its paths, and a rule's `null` keeps that header off
  its paths (`{ "X-Frame-Options": null }` on embeddable routes; the
  generated edge handler honours it for its defaults too). For that to hold
  everywhere, two gaps closed. Rule headers were applied only inside the
  page pipeline, and they now reach every response for the path: mounted
  sub-apps, assets and the built-in endpoints as well as app, static and
  resource routes. The dev server never sent security headers at all, and it
  now sends the same ones as `start`, taking `server.securityHeaders` from
  `neutron.config.ts` (`false` still disables them; the dev server now sends
  `X-Frame-Options: DENY` on its pages by default, like production).

- **`defaultTitle` in the head API.** A page whose route chain sets no
  `title` got one guessed from its URL path, templated if a layout set
  `titleTemplate` — `forgot-password | Fylun`, `c - p3-slug | Fylun` — and
  there was no way to name the site's own title once. `SeoMetaInput` now has
  `defaultTitle`, Next's `title.default`: declared in a layout (typically
  the root), it titles every page beneath that sets no `title`, rendered as
  written (the template is not applied), and is inherited and overridable
  like every other field. A page `title` still wins and takes the template.
  The URL guess stays as the last resort when nothing in the chain declares
  a title: HTML requires a `<title>`, and dropping the guess would leave every
  app that never configured titles showing bare URLs in tabs and to screen
  readers.
- **React 19 `use()` under `runtime: "react-compat"`.** preact/compat
  (10.29) has no `use`, so `import { use } from "react"` — how React 19 /
  Next 15 pages read `params` and promises — failed with `use is not a
  function`. Bare `react` now resolves to `@neutron-build/core/react-compat`:
  preact/compat re-exported, plus `use` on the module and on the default
  `React` object. A promise is tracked under React's thenable protocol
  (`status`/`value`/`reason` on the promise): settled, it returns its value
  or throws its reason; pending, it suspends to the nearest `<Suspense>`,
  which re-renders when it settles — on the client and in streaming SSR,
  where the resolved content streams into the document (non-streaming
  `renderToString` cannot suspend). As in React, the promise must be the
  same object across renders. A context is read with `useContext`, so —
  unlike React — not conditionally. `react/*` subpaths and `react-dom` are
  unchanged; `ViteAliasEntry.find` is now `string | RegExp` (the bare-`react`
  entry is an exact match), and `mergePreactAliases`' flat map omits it.
- **`<ServerIsland>` is usable from React-typed JSX — the component surface
  is fully JSX-agnostic.** The last residual named by the control-flow work:
  the server-side deferred-rendering island (exported from the package root,
  `@neutron-build/core`) returned a preact `VNode`, so every usage from a
  module typechecking against `@types/react` (`jsxImportSource: "react"`)
  failed TS2786. Diagnosed with the same tsc-fixture method before designing:
  it fails at the return position only — `id`, `fallback` and `children`
  (React-authored elements) all flow through the untouched
  `ServerIslandProps`. The fix is the repo precedent (Image/Picture, Await,
  Island, the nav/form five, the control-flow five): the element return is
  deliberately `any`, props stay fully typed, pinned by `@ts-expect-error`s
  in both worlds (`type-fixtures/react-server-island/` on the react side,
  the preact-JSX ServerIsland tests on the other). With it, a final sweep of
  the public exports closed the campaign: every component exported from the
  package root or `@neutron-build/core/client` is JSX-agnostic — the
  remaining ones not covered by a dedicated fixture family (`Await`,
  `Image`, `Picture`) are compiled from React-typed JSX by
  `type-fixtures/react-export-sweep/`, and none fails.
- **The control-flow components are usable from React-typed JSX.**
  `<Show>`, `<For>`, `<Switch>`, `<Match>` and `<Index>` (exported from the
  package root, `@neutron-build/core`) now have JSX-agnostic element returns,
  completing the client component surface started by `Image`/`Picture`/
  `Await`/`Island` and the navigation/form five: under
  `runtime: "react-compat"`, apps typechecking against `@types/react`
  (`jsxImportSource: "react"`) got TS2786 on every usage, because the
  components return preact `VNode`s and React's `ReactNode` rejects that
  shape. Diagnosed per component with tsc fixtures before designing: all
  five fail at the return position only — the keyed render callbacks
  (`(item) => …`, `(item, index) => …`, `(item: () => T, index) => …`),
  `fallback` and `key` props needed no changes; React-authored elements and
  callbacks flow through the untouched `ComponentChildren` prop types
  unchanged. Props stay fully typed — mistyped props still fail under either
  import source, pinned by `@ts-expect-error`s in both worlds:
  `type-fixtures/react-control-flow/` (react side) and the preact-JSX
  control-flow tests.
- **The navigation/form components are usable from React-typed JSX.**
  `<Form>`, `<Link>`, `<NavLink>`, `<ViewTransitions>` and `<ScrollReveal>`
  now have JSX-agnostic element returns (the same treatment `Image`,
  `Picture` and `Await` already ship, and `<Island>` received above):
  under `runtime: "react-compat"`, apps typechecking against `@types/react`
  (`jsxImportSource: "react"`) get TS2786 on every usage otherwise, because
  a preact `FunctionalComponent` returns `ComponentChildren` (which includes
  bare `object`) and React's `ReactNode` rejects that. Props stay fully
  typed — a mistyped prop still fails against the same `FormProps`/
  `LinkProps` under either import source, pinned in both worlds by
  `type-fixtures/react-nav-form/` (react) and the preact-JSX component tests.
- **`JsxChildren` / `AnyJsxElement` exported from `@neutron-build/core/client`**
  (and the root entry): the structural element type both JSX runtimes
  produce and accept back, for `children` props on components that must
  compile under both import sources — preact's `ComponentChildren` fails
  React's render position (TS2322: `object` is not a `ReactNode`) and a
  preact-only install cannot import `ReactNode` at all. The scaffolded root
  `_layout.tsx` uses it.
- **`create-neutron --runtime react-compat` scaffolds with
  `jsxImportSource: "react"`.** The tsconfig now derives its import source
  from the runtime token — the same rule the CLI's Vite transform already
  applies (`resolveRuntimeJsx`) — instead of disagreeing with it. All four
  runtime templates also typecheck their string `style` attributes into the
  object form React 19's types require, import hooks from `react` under
  react-compat (`preact/hooks` otherwise), and ship a `typecheck` script
  (`tsc --noEmit`). A create-neutron test compiles every runtime template's
  scaffolded sources under BOTH runtimes, so a scaffolded app typechecks
  out of the box — the criterion that previously kept the scaffold on
  `preact` (settings.tsx uses `<Form>`).
- `docs/react-compat.md` documents the types setup for react-compat apps:
  `react` ships no bundled types, so apps need `@types/react` (the bundler
  never needs the real `react` package — the runtime aliases cover it), and
  the `jsxImportSource` choices with their current limits.
- `create-neutron --runtime react-compat` scaffolds with `@types/react` in
  `devDependencies`. Previously the flag only substituted the config value,
  leaving the scaffold unable to typecheck its first React-ecosystem import.

### Changed

- **`client="visible"` finds the island's boxed content.** core 0.4.1 made
  the `<neutron-island>` marker `display: contents` and observed the marker's
  child elements, hydrating at once when it had none. The runtime now
  observes the content's first boxed descendants, looking through nested
  `display: contents` elements; a marker given a box of its own
  (`neutron-island { display: block }`) is observed itself, and a box-less
  marker with only text inside is represented by its nearest ancestor with a
  box instead of hydrating immediately.
- **Static-tier pages are no longer in the app's client bundle.** The client
  route table had an `import()` for every non-resource route, so a `mode:
  "static"` page's module — stripped only of loader/action/getStaticPaths/
  headers/middleware, not of `head` or its top-level imports — and its whole
  import graph were bundled as browser code, although the router never
  renders such a page: a static page that imported a server-only package
  failed the client build, and everything it imported shipped as chunks. A
  page whose whole chain renders at the static client tier
  (`resolveClientTier` is not `"full"`: every route `mode: "static"` and no
  `hydrate: true`), and any layout only such pages use, is now listed in the
  route table with `clientModule: false` and no `import()`. Its document
  boots like a static site's page — islands hydrate, the router does not
  start — and the router reaches it by a document load, also under an app
  layout. Its stylesheets come from the CSS pass, which now also runs in app
  builds for these pages; a stylesheet both builds emit is linked from the
  client build's copy, so static and app pages share the file. On a fixture
  with two app routes and four static pages each importing ~60 KB of
  content, the client build went from 10 JS files / 276 KB to 6 / 36 KB; an
  app page's initial JS is unchanged (~35 KB — route chunks were already
  loaded lazily). `staticTierRouteIds` is exported from
  `@neutron-build/core/vite`.
- **Pages link only the stylesheets their route needs.** Every page —
  prerendered static pages, SSR'd app pages, and the `__css__` list a client
  navigation loads before it swaps the page — linked every `dist/assets/*.css`
  file, so an app's CSS grew on every page with every section (29 sheets /
  ~2.6 MB per page on the Fylun port), and two routes that style `:root`
  differently fought, the later-sorted file winning on both. `neutron-ts
  build` now writes `dist/.neutron-route-css.json`: per page route (and
  not-found route), the client entry's CSS, then each layout's from the
  outermost in, then the route's own, each being what that module's static
  import graph reaches. CSS shared by several routes comes from the common
  chunk Vite splits it into, so they share one file. `start` (Vite runtime
  and server bundle), the node/docker and worker runtime bundles, and the
  static prerender all link from it; `dev` already linked per route from the
  module graph and is unchanged. A route the table does not know (a dist
  from an older CLI, a route added since the build) still links every
  stylesheet. Stylesheets are now ordered layout-first rather than
  alphabetically, so a route's own rules follow its layouts'. On static-only
  sites the CSS pass is code-split per route: `assets/style-<hash>.css` is
  replaced by one file per route module and shared chunk, recorded in
  `dist/.vite/neutron-css-manifest.json`. Stylesheets a previous page linked
  stay in the document after a client navigation (as in Next's App Router).
- **`neutron-ts start` serves from a prebuilt server bundle instead of
  transforming source through Vite.** `createServer` built a Vite SSR server
  at every boot and transformed each route module, layout, `not-found`,
  resource route and `src/middleware.ts` on first use, so production paid a
  full transform per boot (a large app measured ~18 s CPU and ~2.3 GB RSS)
  and needed Vite at runtime. `neutron-ts build` now compiles that set into
  `dist/server/start/` (one module graph with preact,
  preact-render-to-string and `@neutron-build/core`, so hooks, the renderer
  and router providers share one Preact instance under both runtimes; route
  modules stay lazily loaded) and records it in
  `dist/.neutron-server-bundle.json`. `createServer` loads it natively when
  present and constructs no Vite server. On the reference fixture
  (react-compat page with hooks, layout, resource route, not-found, global
  middleware) boot plus one request per route kind went from ~2.1 s to
  ~0.8 s process CPU and ~200-237 MB to ~120 MB RSS. A dist without a
  current bundle (older CLI, a route added since the build, another
  `runtime`) still runs on the Vite runtime, with a warning when a bundle is
  present but unusable; a bundle that fails to import is a startup error.
  In the bundle, `import.meta.url`/`filename`/`dirname` keep naming each
  module's source file, and dependencies of bundled packages resolve from
  their importer (symlinked aliases under `dist/server/start/node_modules/`)
  as they do under Vite and Node. New: `createServer({ serverBundle: false })`
  forces the Vite runtime (for development servers built on `createServer`),
  `NeutronServer.moduleSource` reports which one is in use, and the boot
  banner prints it. Server islands are now answered by the `@neutron-build/core`
  instance in the routes' module graph, where `<ServerIsland>` registered
  them.
- **Config redirects merge the incoming query into a destination that has
  its own.** A `routes.redirects` rule kept the request's query only when the
  destination had none, so `/old?utm_source=mail` → `/new?tab=info` dropped
  `utm_source`. Incoming parameters are now appended, except keys the
  destination sets — the destination wins per key and its query string is
  kept as written (Next.js semantics); the query goes before any `#fragment`.
  Destinations without a query behave as before. Applies to `start`, `dev`,
  `preview` and the edge runtimes, which share the resolver.

- **`hono` is a peer dependency of core, and mounts are typed
  structurally (F-P6-4).** Core depended on its own `hono`, so an app with
  `hono` in its dependencies could run two copies (always, when core is
  linked or the versions differ), and `NeutronMountApp` was core's `Hono`
  class: a sub-app built with the app's copy failed to type-check (TS2322,
  "Types of property 'get' are incompatible") until cast. `hono`
  (`^4.12.23`) moved to `peerDependencies`, which is how Hono's own
  middleware packages declare it, so an installed app and core share the
  app's copy (npm and pnpm install the peer when the app does not list it).
  `NeutronMountApp` is now the members Neutron routes a sub-app through
  (`fetch`, `route`, `routes`), so a different copy still type-checks;
  routing a foreign instance already worked and is now tested, from
  `createServer` and the adapter runtime. **Migration:** none; drop
  `as unknown as NeutronMountApp` casts.

### Fixed

- **`neutron-ts build` prerenders static pages behind `src/middleware.ts`
  for `neutron-ts start` again.** core/cli 0.4.0 refused any build with a
  global middleware and a `mode: "static"` route, because prebuilt files
  bypass request authorization. Under `start` they do not: its static fast
  path runs global middleware before every prebuilt file it serves. The
  refusal now applies only with an adapter other than `node` (`static`,
  `docker`, `vercel`, `netlify`, `cloudflare`), where dist files are served
  with no Neutron server in front. Resource routes, never prerendered, are
  no longer loaded by that check either.
- **`maxRequestBodyBytes` no longer turns every request into a 500.** The
  0.4.0 body cap copied the incoming request with the native `Request`
  constructor, which throws on `@hono/node-server`'s request object now that
  `serve()` keeps the native globals. It rebuilds the request from its URL,
  method, headers and signal.
- **A nested `<Island>` renders inline after hydration too.** The islands
  runtime hydrated each island outside the context that tells a nested
  `<Island>` it is nested, so the client render emitted a marker the server
  render did not.
- **Dev no longer links a previous build's stylesheets.** With a `dist/`
  present, dev pages (and client navigations, whose stylesheet list comes
  from the same walk) could link `dist/assets/*.css` — minified CSS from an
  old build, stale after any source edit (the Fylun port: `/pricing`,
  `/studio`, a colour-format diff against a fresh load). Dev reads nothing
  from `dist/`; the stylesheets came from the module graph. Tailwind v4
  registers every source file it scans with `addWatchFile`, and where
  `dist/` is not gitignored it scans the build output; Vite records each such
  file as a file-only "asset" entry under the stylesheet that registered it,
  and dev's CSS collection walked those entries as if they were imports.
  It now follows only real imports, never links a file under the build
  output directory, and looks route files up by their real path as well
  (a symlinked path, macOS `/tmp`, no longer misses the route's graph and
  falls back to every stylesheet). The fallbacks are filtered the same way,
  and the last-resort `src/*.css` scan is rooted at the app, not the
  process's working directory.
- **Docker preset: `/` reaches the app, the image context drops `start`'s
  bundle, and files get `start`'s headers.** The image's `server.mjs`
  served `dist/index.html` for `/` before the app handler, so an app route at
  `/` was shadowed by the client build's shell; a prebuilt HTML file now
  answers only where no app route (or redirect rule) claims the path, as in
  `start`. The image context carried `dist/server/start` — `start`'s bundle,
  unused by the image, whose `node_modules` links point into the build
  machine's package store; `.dockerignore` now excludes it and its manifest.
  Files the image serves get the `routes.headers` and security defaults the
  app's responses get, and `/assets/*` the immutable cache header.
- **No `Cannot find module '@jridgewell/sourcemap-codec'` during builds with
  `resolve.preserveSymlinks` (react-compat, pnpm-linked core).** The
  prerender's SSR runtime evaluated core's `magic-string` at core's linked
  path, where its own dependency is invisible; loading core failed (logged)
  and the prerender fell back to the CLI's copy of the router providers.
  `magic-string` joins the deps the SSR runner keeps native
  (`RUNTIME_RUNNER_CJS_EXTERNALS`).

- **`neutron-ts dev` sends every `Set-Cookie` of a response.** Dev copied
  route responses onto the Node response one header entry at a time with
  `setHeader`, which replaces, so of several `Set-Cookie` headers only the
  last reached the browser. Better-Auth's sign-in sets the session token and
  the session data, and only the data arrived, so nobody could sign in under
  dev (F-B6-2). Resource routes, global middleware (after `next()`, answering
  an unmatched path, or throwing a `Response`), route-rule redirects and the
  image endpoint now send each cookie as its own header, as the mount path and
  `start` already did. `start` (`@hono/node-server`) and the docker/vercel
  handlers were checked and were correct.
- **`neutron-ts dev` applies page routes' `headers()` exports.** Dev's
  renderer never called them, so a page's own headers (cookies, Cache-Control)
  were sent by `start` and missing in dev. Dev now merges the route and layout
  `headers()` the way `start` does (Set-Cookie appended across the chain) onto
  both the HTML document and the data (JSON) response.
- **`neutron-ts dev` attaches the server entry's WebSocket handler at
  startup.** Dev created the `WebSocketServer` and called
  `websocket.attach` only once an upgrade had passed `authorize`, while
  `start` attaches at boot. An authorizer that depends on state `attach` sets
  up refused every upgrade in dev (F-B6-1). Dev now loads
  `src/server.entry.ts` and attaches its handler when the server starts
  listening. After an edit to the entry or a module it imports, it closes the
  old entry first (terminates its clients, closes its server, runs its
  `close`) and then loads and attaches the new one, without waiting for a
  request. A load failure is logged and surfaces again on the next request.

- **`--preset docker` builds under pnpm and ships a self-contained context.**
  With pnpm-linked core and `resolve.preserveSymlinks`, the docker runtime
  bundle failed to build: it bundled core's own dependencies, and
  gray-matter's `section-matter` is invisible from core's linked path. Keeping
  them external, as the `start` bundle does, would not have been enough on its
  own, because the image context is `dist/` alone and its `.dockerignore`
  excluded `node_modules`, so the image had nothing to load them from. The
  docker build now asks for a self-contained runtime bundle
  (`ensureRuntimeBundle("node", { selfContained: true })`, new
  `AdapterRuntimeBundleOptions`). Core's dependencies stay external, resolved
  from core's real directory, and every external package the emitted chunks
  import is copied, `pnpm deploy`-style, into the context as real directories
  together with its production dependencies (resolved from each package's
  real path, hoisted to `dist/node_modules` and nested only on a version
  conflict). Core's aliased externals land under
  `dist/server/node/node_modules/@neutron-external/` as copies instead of
  symlinks into the build machine's store. `node_modules` is no longer in the
  generated `.dockerignore`. The vercel/netlify runtime bundles are
  unchanged.

- **A redirected client navigation lands on the redirect target.** The router
  fetched loader data with fetch's default `redirect: "follow"` and never
  looked at the result: when the data request was redirected — a loader's
  `redirect()`, an auth gate in `src/middleware.ts`, a `routes.redirects`
  rule — it rendered the TARGET's data while the address bar kept the URL
  that redirected, and the target's query string was lost (`/open/note/x`
  showed the workspace at `/open/note/x`, without `?open=`). Redirects are
  now part of the data protocol: a 3xx answering an `X-Neutron-Data` request
  is sent as `204` with the location in `X-Neutron-Redirect` (Set-Cookie and
  the other headers kept; `Location` removed), by `createServer` outermost
  (so mounts, middleware, loaders and config rules are all covered), by the
  dev server and by the generated edge handler. Document requests still get
  the plain 3xx. The router navigates to the target, replacing the history
  entry that redirected (the semantics of Next and React Router): a
  same-origin target is a client navigation that keeps its query and hash, a
  cross-origin one is `location.replace`. `response.redirected` (a hop fetch
  followed anyway, e.g. a proxy in front of the app) is honoured the same
  way, and a redirect loop is handed to the browser after 20 hops. The other
  data consumers read the same signal: `<Form>`, `useSubmit` and
  `fetcher.submit` navigate to it as before; `useRevalidator` and fetcher
  revalidation follow it in place; `fetcher.load` navigates; `prefetch()`
  (which previously cached a redirected payload under the linked URL) and
  link prefetching never warm a redirected URL. A client from before this
  release that receives the 204 reloads the page, which follows the redirect.

- **`createServer` resolves once the server is listening.** It resolved as
  soon as `serve()` returned, before the socket was bound, so an immediate
  `close()` threw `ERR_SERVER_NOT_RUNNING`, `url` named the requested port
  rather than the bound one (wrong for `port: 0`), and a bind failure
  (`EADDRINUSE`) surfaced as an unhandled `'error'` event. It now waits for
  `'listening'`, and rejects with the bind error.
- **CommonJS dependencies run in the SSR module runner.** Vite's runner
  evaluates ESM only, so a CommonJS module it inlined — every dependency
  under react-compat's blanket `ssr.noExternal` — failed on its first
  `require` (`require is not defined`). Externalizing them per app
  (`ssr.external: [...]`) worked for plain packages but not for the
  React-ecosystem ones (`use-sync-external-store`, behind zustand, cmdk,
  @tiptap/react, …): natively, their `require("react")` loads real React
  beside Preact. `dev`, `start` on the Vite runtime and the build's prerender
  now rewrite each inlined CommonJS module under `node_modules`: a package
  that declares no react/react-dom/preact dependency is loaded by Node
  (`createRequire`); a React-ecosystem package runs in the runner with its
  own `require`, whose React-family, relative and React-ecosystem requests
  are hoisted to imports — through the react → preact/compat aliases, on the
  page's Preact — and whose other requests go to Node. `module.exports` is
  the module's default export (`.default` for `__esModule` modules) and its
  keys are named exports. Builds (the `start` server bundle, the client)
  were already fine — Rolldown bundles CommonJS itself — and are unchanged.
  Per-app `ssr.external` lists for CommonJS packages are no longer needed.
- **The server bundle builds when core is linked or pnpm-installed under
  `resolve.preserveSymlinks`, and no longer inlines core's dependencies.**
  Under react-compat (`ssr.noExternal: true`) the `start` bundle inlined
  everything core imports — shiki, `@mdx-js/mdx`, `typescript`, zod, hono —
  although none of it shares a module instance with app code. With
  `resolve.preserveSymlinks: true` and core linked or installed by pnpm, the
  resolver stayed on core's symlinked path, where pnpm's siblings of a
  dependency are invisible, so the build failed
  (`failed to resolve import "@shikijs/engine-oniguruma"`) and `start` fell
  back to transforming routes through Vite. Core's own imports of other
  packages are now resolved from core's real directory, as Node does, and
  kept external (aliased under `dist/server/start/node_modules/` when the
  app root resolves them differently); only preact,
  preact-render-to-string and `@preact/signals-core` stay in the bundle's
  graph. On a one-route fixture the bundle's JS went from 8.3 MB (7.8 MB of
  it an inlined `typescript`) to 0.53 MB.
- **`Link` and `NavLink` accept `href` as the destination.** The
  destination prop is `to`; given `href` (the `next/link` and plain-anchor
  name, common in ported code) `Link` spread it onto the `<a>` and then
  overwrote it with the missing `to`, rendering a link with no `href`, and
  `NavLink` was never active. `href` is now an alias for `to`. The props
  type takes exactly one of the two: both, or neither, is a type error.
  `LinkProps` and `NavLinkProps` are now type aliases (a union), not
  interfaces, so code that `extends` them must use an intersection instead.
- **A failed server bundle build no longer leaves its chunks in
  `dist/server/start/`.** Rolldown writes the output before it reports a
  resolve error, so a build that warned and fell back to the Vite runtime
  still left a complete-looking bundle tree under the app root — 135 MB on
  the Fylun port. The Vite runtime that `start` fell back to then scanned it:
  Tailwind's source detection read it on every CSS compile, raising boot RSS
  from ~590 MB to 710–745 MB and the first SSR of 39 app shells from ~10 s
  to 15–17 s CPU. The directory is now removed when the bundle fails.
- **Client builds no longer contain Vite, esbuild, shiki or other server
  code.** Route modules import `Link` and hooks from `@neutron-build/core`,
  whose root entry also re-exports the server, the adapters, content
  collections and the Vite plugins. Tree-shaking dropped their code, but
  the bundler still had to resolve their imports (a `preserveSymlinks`
  build failed on magic-string's dependencies) and kept their dynamic
  imports (`vite`, `ws`, `shiki`, `typescript`) as orphan chunks in
  `dist/assets/` — on a one-route app, under both runtimes, 18 files /
  945 KB of client JS, nearly all of it Vite, esbuild, tsx, postcss, jiti and
  lightningcss; now 5 files / 38 KB. The root entry now has a
  `browser` export condition (`dist/index.browser.js`) with the same export
  names: client modules are re-exported unchanged, node-only functions are
  stubs that throw a "server-only" error if called, and `ServerIsland`
  renders its children as it already did in the browser. Webworker server
  bundles, whose resolver also selects `browser`, keep the full entry.
- **`neutron-ts build` prerenders react-compat pages that import core.**
  Under `runtime: "react-compat"` the prerender's Vite SSR server processes
  every dependency, and it did not keep core's CommonJS-only dependencies
  native the way `dev` and `start` do (`RUNTIME_RUNNER_CJS_EXTERNALS`), so a
  static page whose imports reached `@neutron-build/core`'s root entry
  (`useLocation`, `Link`, …) failed the build with `require is not defined`
  (acorn-jsx, under `@mdx-js/mdx`). The prerender now composes the same
  externals.
- **Starting the server no longer replaces the global `Response` and
  `Request`.** `@hono/node-server`'s `serve()` swaps both for its own
  lightweight classes by default, for the whole process. A `Response`
  subclass defined before `createServer`/`startServer` ran (a Next-compat
  `NextResponse` evaluated at import, for instance) then failed
  `instanceof Response` in app code, as did every `Response.json()` and
  `Response.redirect()` result, so code that branched on `instanceof`
  silently treated real responses as data. Neutron now passes
  `overrideGlobalObjects: false`; `globalThis.Response` stays the native
  class. The framework's own response detection already tolerated both
  classes and is unchanged.
- **Resource-route mutations honour `x-neutron-invalidate`.** A page action's
  response could name other paths to evict from the app-response and
  loader-data caches with the header; a `POST`/`PUT`/`PATCH`/`DELETE`
  resource-route handler's response could not — the header was ignored, so
  an API endpoint that changed what a cached page shows left it stale until
  expiry. Both now go through the same invalidation helper (`self`, `*` and
  explicit paths, as for actions). The edge runtimes do not serve resource
  routes and are unchanged.
- **A static-only build no longer records a deleted file as its client
  entry.** `dist/.neutron-client-entry.json` named the CSS-extraction pass's
  JS entry, which the build deletes right after that pass, so anything that
  injected the client entry pointed at a 404. With no app routes there is no
  client entry, and the file is no longer written.
- **Static-only builds emit fonts and images as files instead of inlining
  them into the stylesheet.** The CSS-extraction pass ran as a Vite library
  build, and library mode inlines every `url()` asset as a base64 `data:` URI
  whatever `build.assetsInlineLimit` says — one reported stylesheet was 798 KB,
  459 KB of it font faces, paid uncached on every first page load. The pass is
  now a plain build: assets are written under `dist/assets/` with hashed names
  and referenced by URL, under the same inline limit as the app client
  build. The stylesheet itself is now `assets/style-<hash>.css` rather than
  `assets/<package name>-<hash>.css`, and the pass no longer fails on a
  project whose `package.json` has no `name`.
- **A static-only build no longer bundles server dependencies as browser
  code.** With no app routes, `neutron-ts build` extracts CSS by bundling
  every route module for the browser, so a static page whose imports reach a
  server-only package (a native addon such as `isolated-vm`) failed the
  build with `UNLOADABLE_DEPENDENCY`/`UNRESOLVED_IMPORT`. In that pass,
  packages under `node_modules` are now external unless they resolve to a
  stylesheet — `import "pkg/style.css"` and packages whose entry is CSS
  (`@fontsource/*`) are still collected — and resource routes, which render
  no page, are not imported at all. A stylesheet a dependency's own
  JavaScript imports internally is no longer picked up by this pass; import
  it from app code instead.
- **`neutron-ts build` prerenders static pages inside the router
  providers.** The CLI's static prerender mounted no router context, so
  `useLocation()` returned `/` (and `useParams()`/`useLoaderData()` their
  defaults) on every prerendered page, baking the home-route branch of any
  pathname-dependent layout into every file. Prerendered pages now get the
  same providers as SSR, with the real pathname, params (from
  `getStaticPaths`) and loader data, resolved through the app's module graph
  like the start server's. Layouts on param-less static routes are also
  created with the app graph's `h` rather than the CLI's own copy.
- **`neutron-ts dev` runs `src/middleware.ts`.** The dev server never loaded
  the app's global middleware, so auth gates ran under `start` and on no path
  under `dev`. Dev now finds the same file (`src/middleware.{ts,tsx,js,mjs}`)
  with the same export rules and runs it outermost (FRAMEWORK_CONTRACT §5)
  for page routes, resource routes and unmatched paths; an unmatched request
  it lets through is answered by Vite as before (the gate sees no request
  body there). The file is resolved per request through Vite's module graph,
  so edits, additions and deletions apply without a restart; a file that
  fails to load is reported as a dev error instead of being skipped.
- **The SSR server serves root-URL `public/` files in production, and only
  those.** Vite copies `public/` into the dist root, and `neutron-ts dev`
  serves it at root URLs (`/favicon.svg`), but `neutron-ts start` only served
  `/assets/*` and `/public/*`, so an app's own favicon 404'd in production.
  The root-asset handler that closed that gap served any extension-bearing
  file in the dist root, which also exposed `/.vite/manifest.json`,
  `/.neutron-client-entry.json` and node/docker preset server bundles under
  `dist/server/`, and let a prebuilt file shadow a resource route at the same
  path. `neutron build` now records the files it copied from `public/` in
  `dist/.neutron-public-files.json`, and the server answers root URLs from
  dist for exactly those paths. A dist without the list (built by an older
  CLI or other tooling) falls back to refusing any dot-segment path and
  anything under `server/`.
- **A resource-route handler's unexpected error no longer reaches the
  client, and is reported.** A handler that threw anything other than a
  `ProblemError` or a `Response` answered a 500 whose `detail` was
  `Resource route handler threw: <error text>` (connection strings, file
  paths, SQL), and the error never reached the server's `onError` hook, so
  nothing logged it. The 500 problem body is now generic, and the error goes
  to `hooks.onError` (`source: "request"`, with the resource route's
  `routeId`/`routePath`) under `neutron-ts start`, and to the terminal and
  dev overlay under `neutron-ts dev`. `dispatchResourceHandler` takes an
  optional `{ onError }` for the same purpose. `ProblemError` and thrown
  `Response` handling is unchanged.
- **`Response.redirect()` works wherever app code can return a Response.**
  Global middleware, loaders (returned or thrown), actions, resource routes
  and mounted sub-apps that answered with `Response.redirect()` got a bare
  500 under `neutron-ts start`. Native `Response.redirect()` (like fetched
  responses) has an immutable header list, and the server sets headers on
  every response on the way out (`x-request-id`, security headers, CORS,
  `Vary`, route-rule headers), so the first write threw `TypeError`. The
  server now copies such a response into one with mutable headers before
  touching it, keeping body, status and headers (through core 0.4.0's
  `mutableResponse()`, which now returns `Response.error()`, status 0, as
  is instead of throwing a `RangeError` copying it). `neutron-ts dev` did not
  500 but silently dropped route-rule headers on these responses; it now
  applies them like `start`. `new Response(null, { status: 307, headers: {
  Location } })` and `redirect()` were never affected.
- **Global middleware runs for unmatched paths whether or not the app has a
  `not-found.tsx`.** `src/middleware.ts` ran for an unmatched path only when
  a `not-found.tsx` existed (its render runs the chain); without one, the
  framework's default 404 answered with the middleware skipped, so an auth
  gate that must see every request (FRAMEWORK_CONTRACT §5: authentication
  precedes the route handler) let unauthenticated clients probe the URL
  space. The default 404, and the 404 for a static route missing from dist,
  now answer through global middleware like the static fast path already
  did; a response it returns (401, redirect) wins.

### Dependents

- `@neutron-build/auth` 0.1.6, `cache-redis` 0.2.1, `ops` 0.1.5, `otel` 0.1.5
  and `security` 0.1.6 are republished unchanged except for their
  `@neutron-build/core` dependency, now `^0.5.0`. Their previous versions
  require `^0.3.0` / `^0.4.0`, which would install a second copy of core
  next to 0.5.0. Upgrade them together with core.
- `@neutron-build/create` 0.1.11 scaffolds external projects on
  `@neutron-build/core` and `@neutron-build/cli` `^0.5.0`.

## [core 0.4.2] - 2026-10-30

### Fixed

- `<Island>` type-checks under the `react-compat` runtime. It returned a
  Preact VNode, which React's JSX types reject (TS2786 on every island). The
  props stay strictly typed; the element type is left to the JSX runtime.
  `apps/playground-react-compat` now runs `tsc` over an island route in CI.
- react-compat builds: `react/jsx-runtime` (and `react-dom/client`,
  `react-dom/server`) resolved through the bare `react`/`react-dom` alias
  into `…/compat.mjs/jsx-runtime`, so the first client-bundled island with
  React-typed JSX failed with "Not a directory" unless the app happened to
  add `@preact/preset-vite`. Runtime aliases are now ordered most specific
  first.

## [core 0.4.1] - 2026-10-30

### Fixed

- Islands: the `<neutron-island>` marker is `display: contents` instead of
  `display: block; width: 100%`. As a flex item the old rule took the whole
  row and collapsed `flex-1` siblings to 0px. `client="visible"` now observes
  the marker's children.
- Islands: an `<Island>` inside another island's component renders inline as
  part of the outer island. It used to emit its own marker, which the
  standalone runtime hydrated a second time (content rendered twice).
- Islands: `<Island component={X}>` resolves `X` from every value-import shape,
  including `import X, { type T } from` and `import X, * as ns from`. Those
  shapes left the marker without `data-src`, so the island never hydrated on
  static pages. The Vite plugin now warns when a local island component cannot
  be resolved.

## [core 0.4.0, cli 0.4.0, create 0.1.10, sql 0.2.0, nucleus 0.3.0, data 0.3.0, ai 0.1.2, security 0.1.5, ops 0.1.4, otel 0.1.4] - 2026-10-26

### Breaking

- **zod 4.** `@neutron-build/core` and `@neutron-build/ai` move to zod 4
  (closes GHSA-mrhx-c7xc-m3w5, which has no zod 3 fix). Content collection
  schemas must be zod 4 schemas: import `z` from `"zod"` and use the zod 4 API
  (`z.email()`, `z.url()`). `@neutron-build/ai` still converts zod 3 schemas
  passed through `zod/v3`.
- **Shared caching is opt-in per middleware.** Middleware and `headers()`
  callbacks must declare `sharedCacheSafe: true` before a response passing
  through them is stored in a shared cache; without it the response bypasses
  shared caching (TS-F04/06/07/08).
- Raw and mobile SELECT caching in the data layer defaults off and requires an
  explicit pure-read assertion (TSD-05). Dispatched mutations and `BEGIN` are no
  longer replayed automatically; ambiguous responses surface as `UnknownOutcome`
  (TSD-06).

### Fixed

- Runtime (TS-F01..21): global middleware is gated in static and production
  builds; the Docker adapter serves only declared public artifacts; cache
  capture runs after the full middleware chain with byte-budgeted body capture;
  `mutableResponse()` lets middleware set headers on immutable responses;
  stream writers honor backpressure and abort; backslash path escapes are
  blocked; body caps propagate cancellation; client data is fenced across async
  boundaries and the client matcher implements server catch-all semantics;
  returned loader Responses close their spans; MDX `sanitize: true` refuses a
  custom render factory; ServerIsland props are JSON-escaped.
- `mutableResponse()` copies only responses whose headers are immutable, and
  the server reassigns Hono's response only when a copy was made. Copying on
  every middleware layer had cut static and login throughput by about 40%.
- The runtime router no longer imports the build-time route manifest, which
  pulled `@babel/parser` and `node:fs` into every server bundle, edge presets
  included (about 17 KB gzipped).
- Security: rate-limit buckets survive quota reset, CSRF origin comparison is
  scheme-aware, trusted-proxy mode ignores unverifiable vendor identity headers.
- Data layer (TSD-01..14): cursor decode failures release their connection;
  pinned-session capability probes no longer deadlock the pool; Postgres queue
  claims are lease- and worker-fenced; unknown BullMQ jobs are deferred instead
  of completed; cancellation drains on every outcome; HTTP deadlines cover body
  parsing (30 s default); blob compensation no longer deletes a newer writer;
  rollback preflights the whole frontier before DDL; listener close during
  subscribe releases late subscriptions; Redis increment with expiry is atomic;
  cron coalesces missed work after clock changes; legacy DDL defers cyclic
  foreign keys.
- `@neutron-build/ops` request-context middleware sets its header through
  `mutableResponse()` and requires core 0.4.0. `@neutron-build/otel` ends
  loader and action spans left open when their request ends.

## [core 0.3.2, cli 0.3.2, create-neutron 0.1.9] - 2026-10-07

### Fixed

- **Both data-protocol representations now declare
  `Vary: Accept, Accept-Language, X-Neutron-Data, X-Neutron-Routes`** (TS-33),
  and cache-stored app responses synthesize the same set before publishing
  `Cache-Control`. Without it a shared cache (browser HTTP cache, CDN) could
  store the JSON payload under a page's bare URL and answer a document reload
  with it — the raw `__neutron_serialized__` envelope painted as page content
  (observed on a deployed app 2026-10-06). The server-side cache key was
  already variant-split; the defect was wire-level. Route-declared `Vary` is
  merged, never replaced; `Vary: *` is untouched.
- **Jobs** (framework audit NA-01..NA-03): claim-token fencing on every
  terminal write so an old attempt can never overwrite a newer one; renewal
  is expiry-bounded and a confirmed-through watchdog bounds handler lifetime;
  jobs are claimed only when a concurrency slot is ready; upgraded running
  rows with NULL leases get an explicit recovery cutover. **Operators must
  stop old workers before deploying this version** — mixed fleets keep the
  old claim semantics against the same queue.
- **Static builds** (NA-04): middleware route facts are parsed conservatively
  (all declarator shapes, star exports, unparseable treated as present) and
  the production build gates on them before any write, closing a path where
  a protected static page skipped its route middleware.
- OpenAPI generation is atomic and generation-keyed; group registration and
  post-build registration no longer leave stale documents (NA-05).
- Shared dynamic route branches bind the winning route's parameter names
  (NA-06); dynamic-directory not-found pages receive scoped parameters with
  a deterministic specificity order (NA-07).
- Client export stripping preserves a surviving re-export's source clause
  (NA-08). Generated route declarations are valid string literals (NA-13).
- Code generation: output destinations decided by table count (a single
  `--out` file no longer becomes a directory, NA-09); `json.RawMessage`
  gains its import via a real `go/types` harness (NA-10); legacy table
  enumeration fails closed on scan errors and stages whole batches before
  publishing (NA-14).
- `@neutron-build/mcp` exports a permission-aware in-process
  `Server.CallTool` dispatcher; the HTTP route becomes a shim over it with
  parity tests (NA-11).
- ORM `excluded` validation is scope-aware (scalar subqueries inherit it;
  ordinary identifiers are no longer rejected), verified against live
  PostgreSQL (NA-12).

### Changed

- `create-neutron` 0.1.9 scaffolds pin `@neutron-build/core`/`cli` `^0.3.2`.

## [core 0.3.1, cli 0.3.1, create-neutron 0.1.8] - 2026-10-03

### Changed

- **The CLI runs on Vite 8** (`vite` `^8.3.2`; it was `^6.0.7`). Projects
  that declare `vite` themselves should move to `^8.3.2` too, so npm installs
  one copy instead of nesting the CLI's; projects that forced Vite 8 onto the
  CLI with an `overrides` entry can drop it. New TypeScript scaffolds (both
  `create` templates and the Go CLI's) pin `vite` `^8.3.2` and
  `@preact/preset-vite` `^2.10.4`, the first release that accepts Vite 8.
- The CLI requires Node.js 22.12 or later (`engines.node` is `">=22.12"`),
  Vite 8's floor on the 22 line.
- The CLI passes JSX settings as `oxc` and build input/output as
  `build.rolldownOptions`, so Vite 8 prints no deprecation warnings for
  Neutron's own config. A project `vite.config` that still sets `esbuild` is
  ignored in favor of the runtime-derived `oxc` options, with Vite's warning.

### Fixed

- `neutronPlugin()` gives Vite 8's dependency optimizer a Rolldown plugin for
  the `virtual:neutron*` externals instead of the esbuild one, which Vite 8
  only converts with a deprecation warning. Vite 6 and 7 still get the esbuild
  plugin.

## [core 0.3.0, cli 0.3.0, create-neutron 0.1.7, nucleus 0.2.1, data 0.2.1, cache-redis 0.2.0, auth 0.1.5, ops 0.1.3, otel 0.1.3, security 0.1.4] - 2026-09-30

Requires Node.js 22 or later.

### Added

- The core exports `createSQLSessionStorage` for application-provided PostgreSQL
  pools. Session records carry unique revisions for atomic replacement,
  ID rotation and revocation.

### Fixed

- Request-local function caches isolate authenticated requests, distinguish
  function identities and bound retained entries. Real SSR and HTTP adapter
  module graphs share request scopes and explicit shared-cache tag invalidation.
- Response and loader fills publish atomically against a backing-store
  generation. Mutation completion invalidates its path even without an explicit
  invalidation header, including fills started while an action was running.
  Percent signs and reserved characters survive canonical-path invalidation.
- Redis Lua scripts atomically publish payloads and pathname indexes, advance
  invalidation generations and switch epochs for clear. Missing pathname indexes
  conservatively invalidate the store rather than leaving orphaned payloads.
- Migration plans snapshot primitive inputs before waiting for ownership.
  Adoption validates every checksum before upgrading history metadata.
- Installed data SQL consumers no longer load optional libsql declarations
  merely to import the default entry point. Strict application typing remains
  supported; the documented upstream Drizzle full-library declaration profile
  remains unsupported.
- New TypeScript scaffolds pin core and CLI to `^0.3.0`. Go scaffolds require
  Go 1.26 and SDK `v0.3.0`.

### Breaking

- Server cache stores must implement atomic `getGeneration`/`setIfGeneration`
  publication and generation-advancing invalidation. Legacy custom stores are
  refused before server resources open. Explicit public function-cache sharing
  requires `scope: "shared"`.
- Session middleware requires atomic revision-conditional `commitSession`.
  Legacy stores fail closed; switching from unversioned stores requires fresh
  login. See [authentication migration](../go/neutronauth/README.md).
- Redis cache clients require `EVAL`. Earlier unversioned entries become cold
  misses. Upgrade all writers sharing a prefix together or use a new prefix;
  standalone Redis is supported, Redis Cluster is not.

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
