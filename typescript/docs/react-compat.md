# React Compatibility

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Neutron TypeScript supports two runtime modes:

- `preact` (default): direct Preact runtime.
- `react-compat`: aliases `react` and `react-dom` to `preact/compat`.
- Includes `react-dom/server` compatibility alias to `preact-render-to-string` for SSR/static render paths.

Recommended default:

- Use `preact` for maximum runtime performance and lowest overhead.
- Use `react-compat` when you need React package compatibility during migration.

Set runtime mode in `neutron.config.ts`:

```ts
import { defineConfig } from "@neutron-build/core";

export default defineConfig({
  runtime: "react-compat",
});
```

## SSR and your dependency graph

Under `react-compat`, every command that builds or serves an SSR graph —
`dev`, `build` (static prerender and the runtime bundle) and `start` —
processes **all** `node_modules` dependencies through Vite
(`ssr.noExternal: true`), not just the `react*` alias targets. This is a
correctness requirement, not a preference: an SSR-*externalized* dependency
resolves its own `react` import through native Node, loading real React
next to `preact-render-to-string` — hooks then crash at render time
(observed with Radix: `Cannot read properties of null (reading 'useRef')`).
Which dependencies import react cannot be predicted from package names, so
the default covers all of them.

The cost is real and accepted knowingly: the SSR runtime bundle inlines the
whole reachable dependency graph (larger output, longer builds), and dev SSR
transforms each dependency once on first request (cached afterwards). If
you need a specific heavy package to stay external — a database driver that
must not be bundled, for example — list it in your `vite.config.ts`:

```ts
export default {
  ssr: {
    // Checked before noExternal, so these stay external even under the
    // blanket default. Do NOT list anything that imports react here.
    external: ["pg"],
  },
};
```

An app cannot narrow the default back to an array — Vite's `mergeConfig`
collapses `ssr.noExternal` to `true` when either side is `true` — so
`ssr.external` is the escape hatch.

`dev` and `start` themselves use that escape hatch for a fixed set of
framework deps: `@mdx-js/mdx`, `gray-matter`, `yaml` and `@babel/parser`
(CommonJS-only subtree tops in `@neutron-build/core`'s import graph — the
runner cannot evaluate them, see the limit below). Keeping exactly those
native lets **`@neutron-build/core` itself run inside the SSR graph**, which
is what puts the framework's own hooks (`useLocation`, `useLoaderData`, …)
and your React hooks on one Preact instance during per-request SSR. Your own
`ssr.external` entries merge with (do not replace) that list.

One known limit, stated plainly: a **CommonJS-only** dependency cannot be
processed by Vite's SSR module runner (`neutron-ts start`, and static
prerendering) — it fails loudly with `require is not defined`. `dev` is
unaffected (the dev optimizer pre-bundles it). A CJS React library should
ship as a client-only island instead of server-rendering, the same advice
the [compatibility matrix](./react-compat-matrix.md) gives for libraries
that fail under `preact/compat` generally.

## Compatibility Matrix

| Tier | Status | Notes |
| --- | --- | --- |
| React component model (JSX, hooks, context, memo, refs) | Supported | Via `preact/compat` aliases. |
| App routing/loaders/actions in Neutron | Supported | Same runtime APIs in both modes. |
| Client navigation/forms/hydration | Supported | Covered by runtime-compat smoke lane. |
| Common React libraries that depend on public React APIs | **Tested per package** | See the [compatibility matrix](./react-compat-matrix.md) — real packages, server-rendered, 28 of 29 currently render. |
| Libraries depending on private React internals | Not guaranteed | May fail with `preact/compat`. |
| React Server Components / Next.js RSC model | Not supported | Out of scope for Neutron runtime. |

## Which libraries actually work

**[React library compatibility matrix](./react-compat-matrix.md)** — generated,
not asserted. Each library is mounted for real under `preact/compat` and
server-rendered; a row says yes only if it produced the markup it should.

This page used to answer the question with "usually works — verify per package
in app context", which hands the risk to whoever is evaluating Neutron, at
evaluation time, on their own codebase — and they are usually holding a
Radix-heavy React app when they ask. The matrix answers it instead.

Regenerate after changing a version:

```sh
pnpm --filter @neutron/compat-matrix matrix:write
```

Adding a library is one entry in `compat-matrix/libraries.mjs`: a mount and an
assertion.

## CI Coverage

Two lanes run on every TypeScript workflow build:

- `pnpm run ci:runtime-compat` — the dual-runtime smoke lane. Runs the same
  `@neutron/playground` app under `NEUTRON_RUNTIME=preact` and
  `NEUTRON_RUNTIME=react-compat`, verifying build, server start, HTML responses
  and JSON data transport. This proves the **framework** works in both modes.
- `pnpm run ci:compat-matrix` — the ecosystem lane behind the matrix above. It
  fails if a library that previously rendered stops rendering; a library that
  has never rendered is reported, not fatal, because the matrix is meant to
  state the truth including where the truth is "no".

Both existed as scripts for some time while no workflow invoked them, and this
page claimed the first as CI coverage regardless. They are wired now.
