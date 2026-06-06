# Neutron TypeScript Framework — Render-Core Audit & Best-in-Industry Plan

Package: `@neutron-build/core` at `typescript/packages/neutron`
Build/CLI: `@neutron-build/cli` at `typescript/packages/neutron-cli`
Re-audit date: 2026-06-06. Supersedes the stale 6.5/10 scaffold.

This document re-verifies the prior audit against the CURRENT code, sets the
best-in-industry bar, and lays out a file-level plan to rewrite the render core.

---

## PHASE A — RE-AUDIT (Currency Table)

Every prior claim re-checked against the live tree. Note: several prior path
references were wrong — the build CLI is NOT in `src/neutron-cli/build.ts`; it
lives in `typescript/packages/neutron-cli/src/commands/build.ts`. The renderer
generator is `generateRuntimeEntrySource()`. The runtime-Vite SSR helper is
`createSsrServer()` in `src/server/index.ts`, not a top-level `createSsrServer`
module.

| # | Prior finding | Verdict | Evidence (current code) |
|---|---------------|---------|--------------------------|
| 1 | Render-path fragmentation: 3 distinct paths | **STILL-REAL** | (a) Dev: `src/server/index.ts` runs Vite middleware via `createSsrServer()` `src/server/index.ts:1700` and renders through `renderAppRouteHtmlResponse()` `src/server/index.ts:1079`. (b) Prod: a SEPARATE handler is *code-generated as a string* by `generateRuntimeEntrySource()` `neutron-cli/src/commands/build.ts:1040` and emitted as `entry.node.ts` / `entry.worker.ts` (see `apps/playground/.neutron/runtime/entry.node.ts`). (c) `renderStatic`/`renderToString` static path `src/core/render-static.ts:106`. Three renderers, three copies of `wrapHtml`, three copies of the loader/middleware loop. |
| 2 | The 3 paths DON'T share streaming/head/error behavior | **STILL-REAL** | Dev streams (table row 3); prod does NOT. Head: dev uses `resolveRouteHeadHtml` + `buildHtmlPrefix` `src/server/index.ts`; prod re-implements `resolveRouteHeadHtml`/`wrapHtml` inside the generated string `entry.node.ts:571,683`. Error: dev `renderErrorResponse` `src/server/index.ts:1791` adds `clientEntryScriptSrc`+`includeClientRuntime`; the generated prod `renderErrorResponse` `entry.node.ts:618` has a different signature and omits the client runtime. Three divergent implementations that drift on every change. |
| 3 | "build.ts uses blocking `renderToString`" did NOT grep-match | **STALE (path) → CONFIRMED TRUE (substance)** | The grep missed because the file is `neutron-cli/src/commands/build.ts`, not `src/neutron-cli/build.ts`. The generated production entry calls **blocking** `renderToString(element)` — `build.ts:1260` (template literal) → materialized at `entry.node.ts:354` and `entry.worker.ts:354`. The `renderToReadableStream` token that appears at the top of the generated entry is part of a shared import string and is **never called** in prod. So: the production artifact does NOT stream. The claim's substance is real. |
| 4 | Islands `island-transform.ts` is regex-based | **STILL-REAL** | `src/vite/island-transform.ts:28` `clientDirectivePattern = /<(\w+)...client:(load|visible|idle|only|media).../g`; props via regex `:62,:73`. No AST. |
| 5 | The directive transform returns code UNMODIFIED | **STILL-REAL (worse than implied)** | `island-transform.ts:30` `let transformedCode = code;` then `:55` `return { code: transformedCode, islands };` — `transformedCode` is never reassigned. It computes `islands[]` and throws them away. Moreover `islandTransform`/`generateIslandMarker` are **imported nowhere** (grep: only self-reference). This module is **dead code**. The `client:load` directive does nothing. |
| 6 | Two competing island systems (directive vs Island component) | **STILL-REAL** | System A — `<Island component={X} client="load">` `src/client/island.tsx:26` — is the REAL one: it renders `<neutron-island>` markers with serialized props and registers components for hydration (`island-runtime.ts`, `island-plugin.ts`). Both build.ts `:704` and the runtime detect islands via `content.includes("<neutron-island")`. System B — the `client:` directive in `island-transform.ts` — is dead (row 5). Two systems, only one wired in. |
| 7 | `createServer` runs Vite in PRODUCTION | **STILL-REAL (dev/preview), with nuance** | `createSsrServer()` `src/server/index.ts:1700` does `await import("vite")` and `vite.createServer({ middlewareMode })`. This is the dev/`neutron preview` server. The *deployed* artifact is the generated `entry.node.ts`, which has NO Vite import — so production deploys do not ship Vite. But the framework's own `createServer` entry path pulls Vite at runtime, which is why there are two worlds. |
| 8 | Streaming SSR (`renderToReadableStream`) only in DEV, not in deployed artifact | **STILL-REAL** | Dev: `getStreamRenderFn()` `src/server/index.ts:1055` lazily imports `preact-render-to-string/stream` and `renderAppRouteHtmlResponse` `:1079` streams via `streamHtmlDocument()` with shell prefix/suffix, falling back to `renderToString` only if the stream import fails. Deployed `entry.node.ts:354`: hard-coded `renderToString`. So streaming exists in dev and is absent in the deployable. |
| 9 | KV/Document comma-split corruption | **STALE / NOT FOUND in this package** | No comma-split serialization in `neutron-data/src` (grep empty) or in core KV/Document paths. The `.split(",")` hits in core are all legitimate HTTP list parsing: `Accept`/`Vary` `cache-utils.ts:24`, `X-Forwarded-*` `session.ts:349,377`, rate-limit IP list `rate-limit.ts:140`, invalidation tokens `index.ts:1532`. This finding is either already fixed or belonged to a different package (likely a Nucleus client path). Not a render-core issue. |

### What is genuinely GOOD today (preserve, do not regress)

The **data layer is strong and ahead of most of the field** — it is the reason
this framework is not a toy:

- **Parallel loaders**: layout+page loaders run in `Promise.all` with per-route
  error isolation — `entry.node.ts:263`, `src/server/index.ts` loader loop.
- **Credential-aware cache**: shared path-keyed cache *refuses* to read, store,
  or single-flight any request carrying `Cookie`/`Authorization` —
  `requestCarriesCredentials` `src/server/index.ts:1453`, loader-cache guard
  `:1468`. This correctly avoids the classic cache-poisoning of personalized data.
- **ETag / 304**: `createEntityTag` + `requestHasMatchingEtag` →
  conditional responses `src/server/index.ts:1368,1395,1608,1664`.
- **SPA partial-data protocol**: `X-Neutron-Data` + `X-Neutron-Routes` lets the
  client refetch only the loaders it needs and validates IDs against the route
  table — `resolveRequestedDataRouteIds` `src/server/index.ts:1764`,
  client `src/client/fetcher.tsx:66`, prefetch `incremental-prefetch.ts:90`.
- **Safe serialization**: `devalue` round-trip with prototype-pollution checks
  `src/core/serialization.ts`, `serializeForInlineScript`.
- **Graceful drain** on SIGTERM/SIGINT with in-flight wait `src/server/index.ts:2100`.

This is the asset the rewrite must NOT touch except to thread it through the
unified pipeline.

---

## PHASE B — BEST-IN-INDUSTRY VERDICT

### How the leaders build the render core

- **Astro (islands)**: a Wasm **compiler** parses `.astro` to an AST and emits
  JS where each `client:*` directive becomes a hydration contract. Server
  streams pure HTML first; each island is its own tree-shaken script bundle;
  hydration is scheduled per-directive (`load`/`idle`/`visible`/`media`/`only`).
  The directive is resolved at **build time via AST**, never regex.
  ([Astro Islands](https://docs.astro.build/en/concepts/islands/),
  [Astro directives](https://docs.astro.build/en/reference/directives-reference/),
  [withastro/compiler](https://deepwiki.com/withastro/compiler))
- **Next App Router**: React Server Components + `<Suspense>` with
  `renderToReadableStream`; the document streams shell-first and flushes
  boundaries as data resolves; only Client Components ship JS.
  ([Qwik vs RSC streaming](https://markaicode.com/qwik-resumable-apps-vs-react-server-components/))
- **Remix**: `defer()` + `<Await>` streams the shell immediately and resolves
  slow loaders in-band over the same response.
- **SvelteKit**: compiler-driven, per-route code-split, streamed responses with
  promises in `load` resolved during the stream.
- **Qwik**: **resumability** — no hydration replay; serialized listeners resume
  on the client, near-zero JS on load.
  ([Qwik resumability](https://qwik.dev/docs/concepts/resumable/),
  [Builder.io](https://www.builder.io/blog/resumability-vs-hydration))

The universal pattern: **ONE render pipeline** shared by dev and prod, **stream
shell-first**, and **resolve the interactive-JS boundary at build time via an
AST**, code-split per island, scheduled by a hydration directive.

### Verdict: is Neutron's framework good?

**Mixed, leaning good — but NOT best-in-industry, and the gap is entirely in the
render core.** Honest split:

- **Data/runtime layer: genuinely good (≈8.5/10).** Parallel loaders,
  credential-aware shared cache, ETag/304, SPA partial-data, devalue safety, and
  graceful drain are correct and competitive with — in the credential-cache
  correctness detail, ahead of — much of the field. This is real engineering.

- **Render core: not good (≈4/10) and below bar.** Three forked renderers that
  drift (dev vs generated-prod vs static), a **production artifact that does not
  stream** while dev does (so dev TTFB lies about prod), a **regex** island
  scanner that is **dead code** returning unmodified source, and **two island
  systems** where only the marker-component one is wired. No per-island
  code-split, no AST, no partial/lazy hydration contract honored at build time.

Composite today: **~6.5/10 stands as an overall number, but it is bimodal** —
great data layer dragged down by a fragmented, non-streaming, regex render core.
To be literally best-in-industry, Neutron MUST: (1) collapse to **one** render
pipeline used by dev and prod; (2) **stream shell-first in the deployable**;
(3) replace the regex island scanner with an **AST transform** that emits
markers + a per-island code-split client manifest with partial + lazy
hydration. Items 1–3 are necessary and, given the existing data layer, sufficient
to put it at the front of the pack.

---

## PHASE C — RENDER-CORE REWRITE PLAN (file-level)

Principle: extract ONE pipeline into core, consume it from dev server AND from
the generated prod entry (the generator emits a thin adapter, not a re-impl).
Preserve the data layer verbatim.

### C1 — Unify the render pipeline (single source of truth)

- **New file** `src/server/render-pipeline.ts`. Export:
  - `renderRoute(ctx): Promise<Response>` — the canonical loader→action→head→
    render→cache flow, lifted from `src/server/index.ts` and the
    `generateRuntimeEntrySource` template. Takes an injected
    `renderHtml(element, shell): Response` so dev and prod pass their own
    streaming strategy but share everything else (routing, parallel loaders,
    cache, ETag, head, error boundaries, mutation invalidation).
  - `buildHtmlShell(pathname, headHtml, loaderData, actionData, clientSrc, nonce)`
    → `{ prefix, suffix }`. Single `wrapHtml` for the whole framework.
- **Edit** `src/server/index.ts`: delete the in-file loader/render/`wrapHtml`/
  `renderErrorResponse` copies; call `render-pipeline`.
- **Edit** `neutron-cli/src/commands/build.ts` `generateRuntimeEntrySource`:
  stop emitting ~600 lines of duplicated logic. Emit only: route tables +
  `import { createNeutronHandler } from "@neutron-build/core/runtime-edge"` and
  `export const handleNeutronRequest = createNeutronHandler({ routes, modules, clientSrc })`.
  - **Why**: kills finding #1/#2 drift at the root; one place to fix bugs.
  - **Tests**: `src/server/render-pipeline.test.ts` — golden HTML equality
    between dev handler and `createNeutronHandler` output for the playground
    routes; head/error/cache parity. Update `e2e-matrix.test.ts`.
  - **Acceptance**: dev and generated-prod produce byte-identical HTML (modulo
    streaming framing) for every playground route; `wrapHtml`/`renderErrorResponse`
    exist exactly once in the codebase (grep proves single definition).

### C2 — Streaming SSR in the deployable

- **Edit** `src/server/render-pipeline.ts`: `createNeutronHandler` uses
  `getStreamRenderFn()` (lift from `src/server/index.ts:1055`) and
  `streamHtmlDocument(stream, prefix, suffix)` by default, falling back to
  `renderToString` only when the stream module is unavailable or for `HEAD`.
- **Edit** `build.ts`: generated entry imports the streaming path (no hard-coded
  `renderToString`). Worker entry uses the WHATWG `ReadableStream` already used
  in dev.
  - **Why**: finding #3/#8 — prod must stream shell-first like dev and the leaders.
  - **Tests**: extend `src/server/streaming-ssr.test.ts` — assert the deployed
    handler returns a streamed body (chunked, shell flushes before slow loader
    resolves); assert `<head>`+open `<body>` arrive before the app subtree.
  - **Acceptance**: a route with an artificially delayed loader flushes the
    shell first byte well before the body in the generated handler (not just dev);
    `renderToString` appears in the prod path ONLY as the no-stream fallback.

### C3 — AST island transform (replace regex), partial + lazy hydration, per-island code-split

- **Replace** `src/vite/island-transform.ts` with an AST transform using the
  already-present deps `@babel/parser` + `magic-string` (no new dep):
  - Parse module to AST; find `<Island component={X} client="..." .../>` JSX
    (the REAL system) AND, for ergonomics, support `client:*` directives by
    rewriting them to the same lowered form.
  - Lower each to the `<neutron-island data-component data-client data-props
    data-import data-media>` marker, recording `{ id, importPath, directive,
    media }` into a per-build **island manifest**.
  - Emit per-island dynamic `import()` so each island is its own Rollup chunk.
- **Delete** the dead exports (`island-transform.ts` old body) and wire the new
  transform into `src/vite/island-plugin.ts` `transform()` (currently regex
  `:29`) and `src/vite/plugin.ts`.
- **Edit** `src/client/island-runtime.ts` / `src/client/hydrate.ts`: schedule
  hydration by directive (`load` now, `idle`→`requestIdleCallback`,
  `visible`→`IntersectionObserver`, `media`→`matchMedia`, `only`→client-render),
  dynamic-importing the island's own chunk so non-visible islands never download.
- **Edit** `build.ts`: write the island manifest next to the client bundle; the
  client entry maps `data-component`→chunk URL for lazy fetch.
  - **Why**: findings #4/#5/#6 — real AST transform, no dead code, one island
    system, true partial + lazy hydration with per-island code-split (Astro-class).
  - **Tests**: `src/vite/island-transform.test.ts` — AST cases regex broke
    (multiline JSX, nested `{}` props, spread props, member-expression
    components); manifest snapshot; runtime scheduling tests asserting a
    `client:visible` island does NOT fetch its chunk until intersection.
  - **Acceptance**: each island is a distinct chunk in `dist/`; a `client:visible`
    island's chunk is absent from the initial network trace until scrolled; the
    old regex paths and dead exports are gone (grep proves no `RegExp` island
    scanning remains).

### C4 — Collapse the static path into the pipeline

- **Edit** `src/core/render-static.ts`: implement SSG as
  `createNeutronHandler` invoked per-route at build, capturing the streamed body
  to a file — so prerender shares head/island/serialization with SSR.
  - **Why**: removes the third renderer (finding #1); SSG and SSR can't drift.
  - **Tests**: prerender a playground route, diff against the SSR response for
    the same path.
  - **Acceptance**: static output for a route is byte-identical to its streamed
    SSR shell (sans dynamic-only headers).

### C5 — Guardrail: single-renderer invariant

- **New test** `src/server/single-renderer.test.ts`: fail CI if `wrapHtml`,
  `renderErrorResponse`, or the loader loop are defined more than once across
  `src/` and the `build.ts` generated template (static string scan of the
  generator output).
  - **Why**: prevents regression to fork-the-renderer.
  - **Acceptance**: test red if anyone reintroduces a second renderer.

### Sequencing

C1 (unify) → C2 (stream in prod, trivial once unified) → C3 (AST islands,
largest) → C4 (fold static) → C5 (guardrail). C1 is the keystone; everything
else is cheap once there is one pipeline.

---

## Sources

- [Astro Islands](https://docs.astro.build/en/concepts/islands/)
- [Astro template directives](https://docs.astro.build/en/reference/directives-reference/)
- [withastro/compiler (DeepWiki)](https://deepwiki.com/withastro/compiler)
- [Qwik resumability](https://qwik.dev/docs/concepts/resumable/)
- [Resumability vs Hydration (Builder.io)](https://www.builder.io/blog/resumability-vs-hydration)
- [Qwik 2.0 vs RSC streaming](https://markaicode.com/qwik-resumable-apps-vs-react-server-components/)
