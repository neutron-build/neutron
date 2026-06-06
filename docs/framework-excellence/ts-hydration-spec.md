# Neutron TS — Hydration-Model Rewrite (zero-JS default + true islands)

Surgical, file-level spec for the TS framework's client-side hydration model.

Status: 2026-06-06. Companion to `ts.md` (render-core audit). Green-after-each-step.

---

## CORRECTION (2026-06-06, empirical) — read this first

The original premise below ("ships a full SPA runtime on every page") was based on
reading the **dev-server** path (`server/index.ts`). Inspecting the actual
**production build output** of the playground overturns it:

- **Pure static content already ships ZERO JS.** Prerendered pages
  (`dist/index.html` 1063B, `about` 776B, all `blog/*`) contain **no `<script>`,
  no `__NEUTRON_DATA_SERIALIZED__`, no island markers** — pure HTML. `build.ts:703`
  already gates the client script on `content.includes("<neutron-island")`. This
  is **already Astro-parity** for the marketing/docs/blog sweet spot.
- **Code-splitting already exists:** `index-*.js` (34KB SPA runtime), a separate
  `islands-*.js` (5.4KB) chunk, and per-route/per-component chunks
  (`todos`/`admin`/`dashboard`/`_slug_`…).
- **The real, narrow remaining gap:** the `islands-*.js` bootstrap is **not
  standalone** — it `import`s `index-*.js`, so any page needing *one* island pulls
  the full 34KB SPA runtime (router + fetcher + nav) it doesn't need. And dynamic
  `mode:app` routes always ship the runtime + serialized data via the server path.

So the giant "invert the default + MPA-nav rewrite" below is **mostly
unnecessary and NOT recommended** (it would risk the 4 live sites for a problem
prod already solves). The justified, bounded work is:

1. **Decouple the islands bootstrap from the SPA runtime** so an islands page
   ships only a tiny scheduler + the per-island component chunks (break
   `island-runtime.ts`/`island.tsx`'s import edge into `hydrate.ts`/router/fetcher).
2. **Client-weight measurement** in the harness (JS bytes shipped per route) so
   this is provable and regressions are caught — this is the genuinely missing piece.
3. (optional) Let dynamic `mode:app` routes opt out of the runtime when they have
   no islands; align the dev path with the prod island-gating.

The MPA-nav default flip and full-tree-hydration removal (S5 below) are **shelved**
unless a future need appears. The sections below are retained for context only.

---

---

## Why (evidence, not aesthetics)

The framework-vs-framework benchmark and a from-source trace established:

- **Server SSR throughput already wins** decisively (static 8262 rps vs Next 2756 /
  Astro 1206; dynamic 7384 vs 1123/355). Preact + the credential-aware cache +
  parallel loaders. **Do not regress this.**
- **session-refresh "loss" (113 rps) was a benchmark artifact** — measured at
  ~41,000 rps / p95 2ms in isolation against the real prod server. No defect.
- **mutate/compute parity is converged physics** (CPU/I-O bound). Not fixable.
- **The only real gap is client weight.** Confirmed in source:
  - `client/hydrate.ts:136` `hydrateApp(data)` does **full-tree hydration** of
    the entire layout+page Preact tree — SPA-style, like React/Next.
  - `client/hydrate.ts:153-168` installs a **global click interceptor** turning
    every same-origin `<a>` into SPA navigation → the page is an SPA.
  - `server/index.ts:691` `includeClientRuntime = allRoutes.every(r => r.config.hydrate !== false)`
    → client runtime ships **by default** for every route.
  - `buildHtmlSuffix` (server/index.ts ~1902-1977) **always** emits
    `<script>window.__NEUTRON_DATA_SERIALIZED__=…</script>` + the client entry
    module `<script>` when `includeClientRuntime`.
  - Islands (`client/island.tsx`, `client/island-runtime.ts`) hydrate **after**
    full-tree hydration and register every component in a global
    `window.__ISLAND_COMPONENTS__` map — so the whole module graph is pulled in,
    not per-island code-split.

Net: a zero-interactivity content page still ships the SPA runtime (~20-40KB) +
serialized loader data + hydrates the whole tree. Astro ships **0 JS** for the
same page. That is the gap, and it is the *only* one worth a rewrite.

---

## Target model: four render modes, "pay only for what the route needs"

| mode | client JS shipped | hydration | navigation | use |
|------|-------------------|-----------|------------|-----|
| `static` | **none** | none | MPA (browser) | pure content, no islands |
| `islands` (NEW default for interactive content) | only island chunks | per-island, lazy | MPA (browser) | content + interactive widgets |
| `spa` (opt-in) | full runtime | full-tree | client router | app-like, client routing |
| `app` (data behaviors) | per spa/islands | — | — | unchanged loader/action/cache semantics |

Default precedence per route, computed on the server:
1. `config.mode === "spa"` → `spa`.
2. route subtree contains ≥1 `<Island>` (or `config.hydrate` explicitly true) → `islands`.
3. otherwise → `static` (zero JS).

`config.hydrate === false` keeps forcing zero-JS (back-compat). The new lever is
that the **default is no longer "full SPA"** — it's "static unless islands/spa".

Preserve verbatim: parallel loaders, credential-aware cache, ETag/304, the
`X-Neutron-Data` partial-data protocol (applies to `spa` mode), devalue safety,
graceful drain. This spec changes **what ships to the client**, not the server
data pipeline.

---

## Steps (each compiles + tests green before the next)

### S1 — Server decides a per-route `clientMode`, not a boolean
- **Edit** `server/index.ts`: replace the `includeClientRuntime` boolean
  (`:691`) with `resolveClientMode(allRoutes, hasIslands): "static" | "islands" | "spa"`.
  `hasIslands` is known at render time: the SSR output contains `"<neutron-island"`
  (already used as the island detector in build.ts and the runtime). Compute it
  from the rendered `content` string before assembling the suffix.
- **Why**: single source of truth for what the client gets; unblocks S2–S4.
- **Test**: unit `resolveClientMode` — spa-config→spa, island-in-output→islands,
  plain→static, `hydrate:false`→static.
- **Acceptance**: function exists; existing e2e still green (defaults unchanged
  until S2 wires it).

### S2 — `buildHtmlSuffix` emits per-mode
- **Edit** `buildHtmlSuffix`/`wrapHtml` (server/index.ts ~1902-1977):
  - `static`: emit **nothing** — no data script, no client entry. Pure
    `</div></body></html>`.
  - `islands`: emit a **tiny islands bootstrap** module script (the island
    scheduler only, not the SPA runtime) + only the props each island needs
    (already in the marker's `data-props`); **no** page-wide
    `__NEUTRON_DATA_SERIALIZED__` unless an island consumes loader data.
  - `spa`: current behavior (data script + full client entry).
- **Why**: kills the per-page weight for static/islands. This is the headline win.
- **Test**: extend `protocol-e2e.test.ts` — a static route response contains **no**
  `<script type="module"` and **no** `__NEUTRON_DATA_SERIALIZED__`; an islands
  route contains the islands bootstrap + `<neutron-island>` but not the SPA
  client entry; a spa route is unchanged.
- **Acceptance**: byte-level assertions above pass; static-page payload drops to
  HTML-only (measure: it should approach Astro's bytes/req).

### S3 — Split the client bootstrap (islands scheduler vs SPA runtime)
- **New** `client/islands-entry.ts`: imports ONLY `initIslands()` from
  `island-runtime.ts`. No `hydrateApp`, no click interceptor, no router/fetcher.
  This is the `islands`-mode entry (target a few KB).
- **Edit** `client/island-runtime.ts` `hydrateIsland()`: instead of reading from
  a global `window.__ISLAND_COMPONENTS__` registry (which forces the whole graph
  in), **dynamic-import the island's own chunk** via `data-import` on the marker
  (Astro model). Keep the directive scheduling (`load`/`idle`/`visible`/`media`)
  that already exists.
- **Keep** `client/hydrate.ts` `init()` as the **`spa`-mode** entry, unchanged.
- **Why**: islands mode ships ~the scheduler + only the island chunks actually
  on the page, lazily. No SPA runtime, no full-tree hydrate.
- **Test**: `island-runtime` test — a `client:visible` island does NOT import its
  chunk until intersection; `client:load` imports immediately; registry path no
  longer required.
- **Acceptance**: islands-mode page executes island hydration with the SPA
  runtime absent from the network trace.

### S4 — Build emits per-island chunks + a manifest
- **Edit** `vite/island-plugin.ts` + `vite/plugin.ts`: ensure each `<Island
  component={X}>` lowers to a marker carrying `data-import="<chunk-url>"`, and
  each island module is its **own Rollup entry/chunk** (dynamic import boundary).
  Write an island manifest (component→chunk URL) next to the client bundle.
- **Edit** `neutron-cli/.../build.ts`: pick the right client entry per route mode
  (none / `islands-entry` / SPA `index`) when generating the prod handler; emit
  the manifest. The generated prod entry must use the **same** `resolveClientMode`
  + `buildHtmlSuffix` path (do not fork — see ts.md C1; this can reuse the
  `runtime-edge` barrel).
- **Why**: real per-island code-split (findings #4-6 in ts.md); no global bundle.
- **Test**: `island-transform`/build test — distinct chunk per island in `dist/`;
  manifest snapshot; a 2-island page emits 2 island chunks.
- **Acceptance**: `grep` shows no global `__ISLAND_COMPONENTS__` requirement on
  the islands path; each island is a separate chunk.

### S5 — MPA navigation for static/islands; SPA nav only in spa mode
- **Edit** `client/hydrate.ts:153-168`: the global click interceptor (SPA nav)
  must only install in `spa` mode. In `static`/`islands` mode, links do normal
  browser navigation (MPA) — like Astro. Optional polish later: prefetch +
  view-transitions for instant MPA nav.
- **Why**: an islands/static page must not pull the router/fetcher just to make
  `<a>` clickable. This is the behavioral crux — content sites are MPA, apps opt
  into SPA.
- **Test**: islands-mode page: clicking `<a>` triggers a real navigation (no
  `__NEUTRON_ROUTER_ACTIVE__`); spa-mode: SPA nav intercept active.
- **Acceptance**: no router code reachable from `islands-entry`.

### S6 — Guardrail
- **New test** `client-weight.test.ts`: for the playground, assert (a) a known
  static route ships 0 module scripts and 0 serialized-data bytes; (b) a known
  islands route ships only island chunks; (c) a known spa route ships the runtime.
  Fail CI if a static route regresses to shipping JS.
- **Acceptance**: red if anyone re-enables default hydration.

---

## Sequencing & risk

S1 → S2 (headline weight win, low risk) → S3 → S4 (the code-split machinery,
largest) → S5 (MPA nav, behavioral) → S6. S1+S2 alone close most of the
client-weight gap (static pages go HTML-only) and are low-risk; S3-S5 deliver
true Astro-class islands.

**Risks:**
- S5 changes navigation semantics for non-spa routes (SPA→MPA). The 4 dogfood
  sites must be checked — any that rely on client-side routing should set
  `mode:"spa"`. Land S1-S4 first (weight win, nav unchanged via temporary
  spa-default), flip the nav default in S5 behind the per-route mode.
- Must not fork the renderer: the generated prod entry shares the
  `resolveClientMode`/`buildHtmlSuffix` path (ties into ts.md C1 unify). If C1
  isn't done, S2/S4 must edit BOTH the server and the build.ts template in lockstep.
- Publish as **0.2.0** (behavior change: default output goes from SPA to
  zero-JS). No users yet, so a clean break — no compat shim.

## Acceptance (whole spec)
1. A no-interactivity playground route ships **0 bytes** of JS/serialized-data
   (Astro parity), verified in the harness's new client-weight metric.
2. An islands route ships only the islands scheduler + the island chunks on the
   page, lazily by directive.
3. A `spa` route is byte-for-byte the current behavior.
4. Server SSR throughput unchanged within the regression gate (RPS drop <20%).
5. One renderer path (dev == generated-prod) for the suffix/mode logic.
