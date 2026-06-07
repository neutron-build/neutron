# Neutron TS — Standalone Islands Implementation (#73)

Goal: an interactive island on an otherwise-static page must ship a **tiny
standalone islands runtime + the island's own chunk**, NOT the 34KB SPA runtime.
Target: `/islands`-type pages drop from ~35KB initial JS to <10KB. Static pages
stay 0KB; SPA/app routes stay exactly as they are.

## Architectural facts (verified in source)
- The hydration entry is the **app's `src/main.tsx`** (`import { init } from
  "@neutron-build/core/client"; init()`) → becomes `index-*.js` (34KB SPA runtime:
  router + fetcher + full-tree hydrate). Resolved by `resolveClientEntryPath`
  (`src/vite/plugin.ts:100`).
- Prerender already gates the static client script on island presence:
  `build.ts:703` `const hasIslands = content.includes("<neutron-island")`.
- Island hydration today (`island-runtime.ts:21`) reads
  `window.__ISLAND_COMPONENTS__[id]`, populated by the `<Island>` component
  running during **full-tree** client render — which is why islands need the SPA
  runtime. Decoupling = obtain the component via dynamic `import()` instead.
- `island-plugin.ts` and `island-transform.ts` are BOTH dead (not wired). Ignore/
  delete; do the transform inside `neutronPlugin` (`src/vite/plugin.ts`).
- Virtual-module pattern to copy: `virtual:neutron/routes` —
  `resolveId` → `\0virtual:neutron/routes` (`plugin.ts:411`), `load` →
  `generateRoutesModule` (`plugin.ts:442`).

## Design (module-path ids + virtual manifest — minification-safe)

Use the island module's **resolved path** as the id (NOT the component name —
names get mangled by minify and would desync SSR vs client). A virtual manifest
maps id → a static `import()` thunk so Rollup code-splits each island into its
own chunk and rewrites the URL in BOTH dev and prod (no manual manifest threading).

### Pieces
1. **`neutronPlugin` client transform** (`src/vite/plugin.ts`, non-ssr `transform`):
   find `<Island ... component={X} ...>`; resolve X's import source to a
   root-relative module id (e.g. `/src/components/Counter.tsx`); inject a prop
   `__src={"<id>"}` into that JSX tag (use `magic-string`; `@babel/parser` is
   available if regex proves fragile, but the existing `component={Name}` +
   import-regex covers the common case). Record every id in plugin `state`.
2. **Virtual `virtual:neutron-islands`** (`neutronPlugin` resolveId/load): emit
   `export const islands = { "<id>": () => import("<id>") , ... };` for all
   recorded ids. Must be populated from a full source scan (walk route files +
   their component imports, or scan all `/src/**` for `<Island component=`), since
   `transform` runs lazily — do an eager scan in `configResolved`/`refreshRoutes`
   so the virtual module is complete at build time.
3. **`island.tsx`**: accept `__src`; stamp `data-src={__src}` on the marker
   (alongside existing `data-component`/`data-props`/`data-client`). Do NOT render
   `__src` as a DOM attribute name. Keep `window.__ISLAND_COMPONENTS__` registration
   for SPA mode (back-compat).
4. **`island-runtime.ts`**: `initIslands(manifest?: Record<string, () =>
   Promise<any>>)`. In `hydrateIsland`: if the registry has the component (SPA
   mode), use it; ELSE if `el.dataset.src` and `manifest[src]`, do
   `const Component = (await manifest[src]()).default` then hydrate. Keep the
   prototype-pollution guard + SSR-content hydrate/render branch.
5. **`src/client/islands-entry.ts`** (NEW, framework-provided standalone entry):
   ```ts
   import { initIslands } from "./island-runtime.js";
   import { islands } from "virtual:neutron-islands";
   initIslands(islands);
   ```
   Export it from `package.json` as `./client/islands-entry` → `dist/client/islands-entry.js`.
   It must NOT import `hydrate.ts`/router/fetcher.
6. **`build.ts`**: add the islands entry as a SECOND Rollup input alongside the
   app's main entry (so it builds as its own chunk graph; it won't pull the SPA
   runtime since it doesn't import it). Capture its output filename. In the
   prerender script-injection (`build.ts:699-706`), when `hasIslands`, emit the
   **islands-entry** chunk URL instead of `clientEntryScriptSrc` (the 34KB index).
   SPA/app routes and the dev server keep `main.tsx` unchanged.

## Constraints
- ADDITIVE: SPA/app routes (full-tree hydration) must be byte-identical after.
  Only prerendered-static-with-islands pages switch to the islands entry.
- Do NOT touch `nucleus/`. Stay on branch `framework/ts-islands`.
- Keep all 225 vitest tests green.

## Verification gates (ALL must pass before merge)
1. `npx vitest run` in `packages/neutron` → 225+ pass.
2. `npx tsc --noEmit` → clean.
3. Rebuild playground (`cd apps/playground && pnpm build`), `pnpm start`, then
   `node benchmarks/client-weight.mjs http://127.0.0.1:3000 "/,/islands,/dashboard"`:
   - `/` stays 0KB.
   - `/islands` (and any island static page) drops from ~35KB to <10KB initial JS,
     and does NOT load `index-*.js` (the 34KB runtime).
4. Island still HYDRATES: fetch `/islands`, confirm the markers + island chunk are
   present; ideally a headless interaction check (or assert the island chunk is
   referenced and the SPA runtime is absent).
5. SPA/app route (`/dashboard` or `/users/1`) initial JS unchanged (still loads the
   SPA runtime — interactivity/nav preserved).
